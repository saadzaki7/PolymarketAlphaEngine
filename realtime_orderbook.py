#!/usr/bin/env python3
import asyncio
import json
import csv
import os
from datetime import datetime, timezone
import websockets

# Constants
INPUT_CSV_FILE = "/Users/saadzaki/Documents/Polymarket/multi_outcome_polymarket_events.csv"
NUM_EVENTS_TO_MONITOR = 2  # Only monitor top 2 events by priority
WEBSOCKET_URL = "wss://clob.polymarket.com/ws"  # Main WebSocket endpoint
RAW_OUTPUT_FILE = "orderbook_data.jsonl"  # File to store raw orderbook updates

# Global data structure to maintain the latest orderbook state
orderbooks = {}  # market_id -> {outcome_token_id -> {bid: {price: size}, ask: {price: size}}}

async def load_market_context():
    """
    Load market context from CSV file and extract tokens for top events.
    Returns a dictionary mapping token IDs to their context.
    """
    print(f"Loading market context from {INPUT_CSV_FILE}")
    token_map = {}
    events = []
    
    # Define the expected column indices based on the CSV structure
    # These indices are 0-based for the columns in the CSV file
    EVENT_ID_INDEX = 0
    EVENT_TITLE_INDEX = 1
    NUM_OUTCOMES_INDEX = 2
    PRIORITY_SCORE_INDEX = 7
    
    # Outcome column indices - each outcome has 4 fields in sequence
    # Starting at column 12 for the first outcome
    OUTCOME_START_INDEX = 12
    OUTCOME_FIELDS_PER_MARKET = 4  # Question, Market ID, Yes Token, No Token
    
    try:
        with open(INPUT_CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                try:
                    # Skip empty rows or rows without enough columns
                    if len(row) < OUTCOME_START_INDEX:
                        continue
                        
                    # Extract basic event information
                    event_id = int(row[EVENT_ID_INDEX])
                    event_title = row[EVENT_TITLE_INDEX]
                    num_outcomes = int(row[NUM_OUTCOMES_INDEX])
                    
                    # Try to get priority score, default to 0 if not available
                    try:
                        priority_score = float(row[PRIORITY_SCORE_INDEX])
                    except (ValueError, IndexError):
                        priority_score = 0
                    
                    # Extract market and token information
                    markets = []
                    for i in range(num_outcomes):
                        # Calculate the starting index for this outcome's data
                        outcome_idx = OUTCOME_START_INDEX + (i * OUTCOME_FIELDS_PER_MARKET)
                        
                        # Ensure we have enough columns for this outcome
                        if len(row) < outcome_idx + OUTCOME_FIELDS_PER_MARKET:
                            continue
                            
                        question = row[outcome_idx].strip()
                        market_id = row[outcome_idx + 1].strip()
                        yes_token = row[outcome_idx + 2].strip()
                        no_token = row[outcome_idx + 3].strip()
                        
                        # Only add if we have all the required data
                        if question and market_id and yes_token and no_token:
                            markets.append({
                                "name": question,
                                "market_id": market_id,
                                "yes_token": yes_token,
                                "no_token": no_token
                            })
                    
                    # Only add events with at least one valid market
                    if markets:
                        events.append({
                            "event_id": event_id,
                            "title": event_title,
                            "priority_score": priority_score,
                            "markets": markets
                        })
                except (ValueError, IndexError) as e:
                    # Print the first 50 chars of the row for debugging
                    row_preview = str(row)[:50] + "..." if len(str(row)) > 50 else str(row)
                    print(f"Error parsing row: {row_preview} - {e}")
                    continue
        
        # Sort events by priority score (descending)
        events.sort(key=lambda x: x.get("priority_score", 0), reverse=True)
        
        # Take only the top N events
        top_events = events[:NUM_EVENTS_TO_MONITOR]
        
        print(f"Selected top {NUM_EVENTS_TO_MONITOR} events:")
        for event in top_events:
            print(f"- {event['title']} (Event ID: {event['event_id']})")
            
            # Initialize orderbooks for each market in this event
            for market in event['markets']:
                market_id = market['market_id']
                yes_token = market['yes_token']
                no_token = market['no_token']
                
                # Initialize orderbook structure for this market
                if market_id not in orderbooks:
                    orderbooks[market_id] = {}
                
                # Initialize orderbook entries for yes and no tokens
                orderbooks[market_id][yes_token] = {"bid": {}, "ask": {}}
                orderbooks[market_id][no_token] = {"bid": {}, "ask": {}}
                
                # Add tokens to the token map
                token_map[yes_token] = {
                    "event_id": event['event_id'],
                    "event_title": event['title'],
                    "market_id": market_id,
                    "market_name": market['name'],
                    "outcome": "YES"
                }
                token_map[no_token] = {
                    "event_id": event['event_id'],
                    "event_title": event['title'],
                    "market_id": market_id,
                    "market_name": market['name'],
                    "outcome": "NO"
                }
                
                print(f"  * Market: {market['name']} (ID: {market_id})")
                print(f"    - YES Token: {yes_token}")
                print(f"    - NO Token: {no_token}")
        
        return token_map
    
    except Exception as e:
        print(f"Error loading market context: {e}")
        return {}

def update_orderbook(token_id, changes):
    """
    Update the internal orderbook based on price change event.
    """
    token_id = str(token_id)  # Ensure token_id is a string for comparison
    
    for market_id, tokens in orderbooks.items():
        if token_id in tokens:
            for change in changes:
                price = change.get("price")
                side = change.get("side")
                size = change.get("size")
                
                if not all([price, side, size]):
                    continue
                
                # Convert to appropriate types
                price = float(price)
                size = float(size)
                
                # Map the side to bid/ask
                book_side = "bid" if side == "BUY" else "ask"
                
                # Update the orderbook
                if size > 0:
                    tokens[token_id][book_side][price] = size
                else:
                    # Remove the price level if size is 0
                    if price in tokens[token_id][book_side]:
                        del tokens[token_id][book_side][price]
            
            # We found and updated the token, no need to check other markets
            return True
    
    # Token not found in our tracked orderbooks
    return False

def get_best_prices(market_id, token_id):
    """
    Get the best bid and ask prices for a token.
    """
    if market_id not in orderbooks or token_id not in orderbooks[market_id]:
        return None, None, None, None
    
    token_book = orderbooks[market_id][token_id]
    
    # Get best bid (highest buy price)
    best_bid = max(token_book["bid"].keys()) if token_book["bid"] else None
    best_bid_size = token_book["bid"].get(best_bid) if best_bid is not None else None
    
    # Get best ask (lowest sell price)
    best_ask = min(token_book["ask"].keys()) if token_book["ask"] else None
    best_ask_size = token_book["ask"].get(best_ask) if best_ask is not None else None
    
    return best_bid, best_bid_size, best_ask, best_ask_size

def print_orderbook_summary():
    """
    Print a summary of the current orderbook state.
    """
    print("\n" + "="*80)
    print(f"ORDERBOOK SUMMARY - {datetime.now(timezone.utc).isoformat()}")
    print("="*80)
    
    for market_id, tokens in orderbooks.items():
        # Get market name from token_map
        market_name = f"Market ID: {market_id}"
        market_info = None
        
        # Find any token from this market to get the market name
        for token_id in tokens.keys():
            for token_info in token_map.values():
                if token_info.get('market_id') == market_id:
                    market_name = f"{token_info.get('market_name', 'Unknown')} (ID: {market_id})"
                    market_info = token_info
                    break
            if market_info:
                break
        
        print(f"\n{market_name}")
        print("-" * 60)
        
        for token_id, book in tokens.items():
            # Find token info in token_map
            token_info = None
            for t_id, t_info in token_map.items():
                if t_id == token_id:
                    token_info = t_info
                    break
            
            outcome = "Unknown"
            if token_info:
                outcome = token_info.get("outcome", "Unknown")
            
            best_bid, best_bid_size, best_ask, best_ask_size = get_best_prices(market_id, token_id)
            
            print(f"  {outcome} Token (ID: {token_id})")
            print(f"    Best Bid: {best_bid if best_bid is not None else 'None'} (Size: {best_bid_size if best_bid_size is not None else 'None'})")
            print(f"    Best Ask: {best_ask if best_ask is not None else 'None'} (Size: {best_ask_size if best_ask_size is not None else 'None'})")
            
            # Calculate mid price if both bid and ask exist
            if best_bid is not None and best_ask is not None:
                mid_price = (best_bid + best_ask) / 2
                print(f"    Mid Price: {mid_price:.4f}")
            
            # Calculate implied probability (assuming price represents probability)
            if best_bid is not None:
                print(f"    Implied Probability: {best_bid:.2%}")
                
        # Check if we have a complete set of outcomes for this market
        # and calculate sum of probabilities
        yes_tokens = []
        no_tokens = []
        for token_id, book in tokens.items():
            for t_id, t_info in token_map.items():
                if t_id == token_id:
                    if t_info.get("outcome") == "YES":
                        yes_tokens.append(token_id)
                    elif t_info.get("outcome") == "NO":
                        no_tokens.append(token_id)
        
        # If we have at least one YES token, calculate sum of best bids
        if yes_tokens:
            sum_yes_bids = 0
            for token_id in yes_tokens:
                best_bid, _, _, _ = get_best_prices(market_id, token_id)
                if best_bid is not None:
                    sum_yes_bids += best_bid
            
            print(f"\n  Sum of YES token bids: {sum_yes_bids:.4f}")
            if abs(sum_yes_bids - 1.0) <= 0.10:  # Within 10% of 1.0
                print(f"  Market appears to be mutually exclusive (sum ≈ 1.0)")
            elif sum_yes_bids < 0.90:  # Less than 0.90
                print(f"  Potential arbitrage opportunity: Long all outcomes")
            elif sum_yes_bids > 1.10:  # Greater than 1.10
                print(f"  Potential arbitrage opportunity: Short all outcomes")
    
    print("\n" + "="*80)

async def data_collector():
    """
    Connect to the Polymarket WebSocket and maintain the orderbook.
    """
    global token_map
    
    # Initialize the last print time
    data_collector.last_print_time = 0
    last_ping = asyncio.get_event_loop().time()
    
    # Ensure the raw output file exists
    if not os.path.exists(RAW_OUTPUT_FILE):
        with open(RAW_OUTPUT_FILE, 'w', encoding='utf-8') as _:
            pass
        print(f"[INFO] Created raw orderbook log file: {RAW_OUTPUT_FILE}")
    
    # Get all token IDs to monitor
    all_token_ids = list(token_map.keys())
    if not all_token_ids:
        print("[INFO] No token IDs to monitor.")
        return
    
    # Create a single subscription message for all tokens
    subscription_message = json.dumps({
        "type": "subscribe",
        "channels": ["market"],
        "assets_ids": all_token_ids
    })
    print(f"[DEBUG] Subscription message: {subscription_message[:100]}...")
    
    while True:
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Connecting to WebSocket at {WEBSOCKET_URL}")
            async with websockets.connect(WEBSOCKET_URL) as websocket:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] WebSocket connection established")
                
                # Send a single subscription message for all tokens
                await websocket.send(subscription_message)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Subscribed to {len(all_token_ids)} tokens")
                
                # Initialize last ping time
                last_ping = asyncio.get_event_loop().time()
                
                try:
                    while True:
                        try:
                            # Receive and process messages with timeout
                            message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                            now = datetime.now().strftime("%H:%M:%S")
                            
                            # Handle PONG responses
                            if message == 'PONG':
                                print(f"[{now}] [INFO] PONG received")
                                continue
                            
                            # Log raw message to file
                            timestamp_iso = datetime.now(timezone.utc).isoformat()
                            try:
                                parsed_msg = json.loads(message)
                                record = {"timestamp": timestamp_iso, "data": parsed_msg}
                                with open(RAW_OUTPUT_FILE, 'a', encoding='utf-8') as raw_f:
                                    json.dump(record, raw_f)
                                    raw_f.write("\n")
                                
                                # Process messages based on their structure
                                if isinstance(parsed_msg, dict):
                                    # Check if this is a market data update
                                    if parsed_msg.get("type") == "market_data":
                                        data = parsed_msg.get("data", {})
                                        asset_id = data.get("asset_id")
                                        changes = data.get("changes", [])
                                        
                                        if asset_id and changes:
                                            # Update our internal orderbook
                                            updated = update_orderbook(asset_id, changes)
                                            
                                            if updated:
                                                print(f"[{now}] Updated orderbook for token {asset_id}")
                                                
                                                # Print the updated orderbook summary every 5 seconds
                                                current_time = datetime.now().timestamp()
                                                if current_time - data_collector.last_print_time >= 5:
                                                    print_orderbook_summary()
                                                    data_collector.last_print_time = current_time
                                
                                    # Handle subscription confirmation
                                    elif parsed_msg.get("type") == "subscribed":
                                        print(f"[{now}] Successfully subscribed to {len(parsed_msg.get('assets_ids', []))} tokens")
                                
                                    # Handle ping/pong
                                    elif parsed_msg.get("type") == "pong":
                                        print(f"[{now}] [INFO] PONG received")
                            
                            except json.JSONDecodeError:
                                print(f"[{now}] [ERROR] Failed to parse message: {message[:100]}...")
                                continue
                        
                        except asyncio.TimeoutError:
                            # No message received within timeout, send a ping
                            current_time = asyncio.get_event_loop().time()
                            now_str = datetime.now().strftime("%H:%M:%S")
                            
                            if current_time - last_ping > 10.0:
                                print(f"[{now_str}] [INFO] No messages received for 10s, sending PING...")
                                await websocket.send("PING")
                                last_ping = current_time
                                
                                # Also print a waiting message every 30 seconds
                                if int(current_time) % 30 < 10:
                                    print(f"[{now_str}] [INFO] Still connected and waiting for price updates...")
                
                finally:
                    # No ping task to cancel in this implementation
                    pass
        
        except (websockets.exceptions.ConnectionClosed, 
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                websockets.exceptions.InvalidStatusCode) as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] WebSocket connection error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Unexpected error: {e}. Reconnecting in 10 seconds...")
            import traceback
            traceback.print_exc()
            await asyncio.sleep(10)

async def ping_websocket(websocket):
    """
    Send periodic pings to keep the WebSocket connection alive.
    """
    while True:
        try:
            await asyncio.sleep(30)
            ping_msg = {"type": "ping", "requestId": f"ping-{datetime.now().timestamp()}"}
            await websocket.send(json.dumps(ping_msg))
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent ping to keep connection alive")
            
            # Also print a status message every 2 minutes
            current_time = datetime.now().timestamp()
            if int(current_time) % 120 < 30:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] [INFO] Still connected and monitoring orderbook...")
                
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Error sending ping: {e}")
            break

async def main():
    """
    Main entry point for the script.
    """
    global token_map
    
    print("=== Polymarket Real-time Orderbook Monitor ===")
    print(f"Monitoring top {NUM_EVENTS_TO_MONITOR} events by priority")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Input CSV file: {INPUT_CSV_FILE}")
    print(f"Raw output file: {RAW_OUTPUT_FILE}")
    print(f"WebSocket URL: {WEBSOCKET_URL}")
    print("==========================================")
    
    # Load market context from CSV
    token_map = await load_market_context()
    
    if not token_map:
        print("No tokens found to monitor. Exiting.")
        return
    
    print(f"Loaded {len(token_map)} tokens for monitoring")
    
    # Start the data collector
    await data_collector()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript terminated by user")
