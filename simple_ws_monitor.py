#!/usr/bin/env python3
import asyncio
import datetime
import json
import os
import websockets
from datetime import datetime, timezone

# Configuration
NUM_EVENTS_TO_MONITOR = 200  # Monitor top 200 events by priority
INPUT_CSV_FILE = 'multi_outcome_polymarket_events.csv'
RAW_OUTPUT_FILE = 'socket_raw_data.jsonl'  # File to store every raw websocket message
WEBSOCKET_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market' # Correct endpoint for market data

# Hardcoded test tokens from your example
TEST_MODE = False  # Set to False to use real data from CSV
TEST_TOKENS = {
    "112691550711713354714525509796202337193505769808087674259394206524293080902258": {"event_id": "test", "market_id": "test", "question": "Will Kamala beat Trump?", "side": "yes"},
    "40934908860656143479579576067633881276941830984503767038689094076378988206631": {"event_id": "test", "market_id": "test", "question": "Will Kamala beat Trump?", "side": "no"},
    # Adding Trump election tokens from your example
    "21742633143463906290569050155826241533067272736897614950488156847949938836455": {"event_id": "test2", "market_id": "test2", "question": "Will Trump win the election?", "side": "yes"},
    "48331043336612883890938759509493159234755048973500640148014422747788308965732": {"event_id": "test2", "market_id": "test2", "question": "Will Trump win the election?", "side": "no"}
}

# --- Step 1: Create a "Context Map" ---
def load_market_context(limit=NUM_EVENTS_TO_MONITOR):
    """Reads the CSV and creates a map from token_id to its full context."""
    token_map = {}
    print(f"Loading context for top {limit} events from '{INPUT_CSV_FILE}'...")
    try:
        with open(INPUT_CSV_FILE, 'r', newline='', encoding='utf-8') as f:
            import csv
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                
                event_id = row.get('Event ID')
                num_outcomes = int(row.get('Number of Outcomes', 0))

                for j in range(1, num_outcomes + 1):
                    market_id = row.get(f'Outcome {j} Market ID')
                    question = row.get(f'Outcome {j} Question')
                    yes_token = row.get(f'Outcome {j} Yes Token ID')
                    no_token = row.get(f'Outcome {j} No Token ID')

                    context = {
                        'event_id': event_id,
                        'market_id': market_id,
                        'question': question,
                    }

                    if yes_token and yes_token.strip():
                        token_map[yes_token] = {**context, 'side': 'yes'}
                    if no_token and no_token.strip():
                        token_map[no_token] = {**context, 'side': 'no'}
        
        print(f"Successfully loaded context for {len(token_map)} unique tokens.")
        return token_map
    except FileNotFoundError:
        print(f"[ERROR] Input file '{INPUT_CSV_FILE}' not found. Please run market_discovery.py first.")
        return {}
    except Exception as e:
        print(f"[ERROR] Failed to load market context: {e}")
        return {}


def setup_output_files():
    """Ensure the raw JSONL log file exists."""
    if not os.path.exists(RAW_OUTPUT_FILE):
        with open(RAW_OUTPUT_FILE, 'w', encoding='utf-8') as _:
            pass
        print(f"[INFO] Created raw websocket log file: {RAW_OUTPUT_FILE}")
    else:
        print(f"[INFO] Using existing raw websocket log file: {RAW_OUTPUT_FILE}")
    """Ensure the raw JSONL log file exists."""
    if not os.path.exists(RAW_OUTPUT_FILE):
        with open(RAW_OUTPUT_FILE, 'w', encoding='utf-8') as _:
            pass
        print(f"[INFO] Created raw websocket log file: {RAW_OUTPUT_FILE}")
    else:
        print(f"[INFO] Using existing raw websocket log file: {RAW_OUTPUT_FILE}")
    # Test file write permissions
    try:
        test_file = "test_write.txt"
        with open(test_file, 'w') as f:
            f.write("Test write permission")
        os.remove(test_file)
        print("[INFO] File write permissions confirmed")
    except Exception as e:
        print(f"[ERROR] File write permission test failed: {e}")

# --- Step 3: Main Websocket Collector ---
async def data_collector(token_map, file_lock):
    """Establishes a single websocket connection for all tokens and handles data collection with keepalive."""
    all_token_ids = list(token_map.keys())
    if not all_token_ids:
        print("[INFO] No token IDs to monitor.")
        return

    print(f"[DEBUG] Using these tokens: {all_token_ids[:5]}...")
    
    subscription_message = json.dumps({
        "type": "market",
        "assets_ids": all_token_ids
    })
    print(f"[DEBUG] Subscription message: {subscription_message[:100]}...")

    while True: # Outer loop for reconnection
        try:
            async with websockets.connect(WEBSOCKET_URL) as websocket:
                print("[INFO] Websocket connected. Subscribing to all market assets...")
                await websocket.send(subscription_message)
                print(f"[INFO] Subscribed to {len(all_token_ids)} assets.")

                last_ping = asyncio.get_event_loop().time()

                while True: # Inner loop for receiving messages
                    try:
                        # Wait for a message with a timeout
                        message = await asyncio.wait_for(websocket.recv(), timeout=10.0)

                        # Debug raw message with timestamp
                        now = datetime.now().strftime("%H:%M:%S")
                        
                        if message == 'PONG':
                            print(f"[{now}] [INFO] PONG received")
                            continue
                            
                        # --- Simple raw logging ---
                        timestamp_iso = datetime.now(timezone.utc).isoformat()
                        try:
                            parsed_msg = json.loads(message)
                        except json.JSONDecodeError:
                            parsed_msg = message  # leave as string if not JSON
                        record = {"timestamp": timestamp_iso, "data": parsed_msg}
                        async with file_lock:
                            with open(RAW_OUTPUT_FILE, 'a', encoding='utf-8') as raw_f:
                                json.dump(record, raw_f)
                                raw_f.write("\n")
                        print(f"[{now}] [RAW] Logged message to {RAW_OUTPUT_FILE}")
                        print(f"[{now}] [DEBUG] Raw message: {str(parsed_msg)[:200]}...")
                        # Skip any further complex processing; raw log is enough
                        continue

                        try:
                            data = json.loads(message)
                            print(f"[DEBUG] Message type: {type(data)}")
                            
                            # Handle different message formats
                            if isinstance(data, list):
                                now = datetime.now().strftime("%H:%M:%S")
                                print(f"[{now}] [DEBUG] Received list data with {len(data)} items")
                                if len(data) == 0:
                                    # Empty list is normal when no updates are available
                                    print(f"[{now}] [INFO] No price updates available yet. Waiting...")
                                elif data and len(data) > 0:
                                    for item in data:
                                        try:
                                            if isinstance(item, dict):
                                                token_id = item.get('asset_id')
                                                server_timestamp = item.get('timestamp')
                                                market = item.get('market')
                                                event_type = item.get('event_type')
                                                
                                                # Print raw item for debugging (first 100 chars)
                                                item_str = str(item)
                                                print(f"[{now}] [DEBUG] Item type: {event_type}, token: {token_id[:8] if token_id else None}")
                                                
                                                # Check the event type
                                                event_type = item.get('event_type')
                                                token_id = item.get('asset_id')
                                        except Exception as e:
                                            print(f"[ERROR] Error parsing item: {e}")
                                            continue
                                            
                                            # Handle different event types
                                            if event_type == 'book':
                                                # Full orderbook data
                                                bids = item.get('bids', [])
                                                asks = item.get('asks', [])
                                                
                                                # Get best bid/ask with sizes
                                                best_bid = float(bids[0]['price']) if bids else None
                                                best_bid_size = float(bids[0]['size']) if bids else None
                                                best_ask = float(asks[0]['price']) if asks else None
                                                best_ask_size = float(asks[0]['size']) if asks else None
                                                
                                                # Calculate mid price and spread
                                                mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else None
                                                spread = (best_ask - best_bid) if best_bid and best_ask else None
                                                
                                                # Print market summary
                                                print(f"[{now}] [BOOK] Asset: {token_id[:8]}... | Bid: {best_bid} ({best_bid_size}) | Ask: {best_ask} ({best_ask_size}) | Spread: {spread if spread else None}")
                                                
                                            elif event_type == 'price_change':
                                                # Price change event
                                                changes = item.get('changes', [])
                                                
                                                # Process each change
                                                for change in changes:
                                                    side = change.get('side')
                                                    price = change.get('price')
                                                    size = change.get('size')
                                                    
                                                    print(f"[{now}] [PRICE] Asset: {token_id[:8]}... | {side}: {price} | Size: {size}")
                                            
                                            # Debug token lookup
                                            if token_id in token_map:
                                                context = token_map[token_id]
                                                timestamp = datetime.now(timezone.utc).isoformat()
                                                server_timestamp = item.get('timestamp')
                                                print(f"[{now}] [INFO] Found token {token_id[:8]}... in context map")
                                            else:
                                                print(f"[{now}] [WARN] Token {token_id[:8]}... not found in context map")
                                                
                                                # Let's save the data anyway with minimal context
                                                context = {
                                                    'event_id': 'unknown',
                                                    'market_id': item.get('market', 'unknown'),
                                                    'question': 'unknown',
                                                    'side': 'unknown'
                                                }
                                                timestamp = datetime.now(timezone.utc).isoformat()
                                                server_timestamp = item.get('timestamp')
                                            
                                            # Save data based on event type
                                            if event_type == 'book':
                                                # 1. Save summary data to CSV
                                                row = [timestamp, context['event_id'], context['market_id'], 
                                                       context['question'], token_id, context['side'], 
                                                       best_bid, best_bid_size, best_ask, best_ask_size, mid_price, spread]
                                                
                                                try:
                                                    async with file_lock:
                                                        with open(OUTPUT_CSV_FILE, 'a', newline='') as f:
                                                            writer = csv.writer(f)
                                                            writer.writerow(row)
                                                    print(f"[{now}] [INFO] Successfully wrote row to CSV")
                                                except Exception as e:
                                                    print(f"[{now}] [ERROR] Failed to write to CSV: {e}")
                                                
                                                # 2. Save full orderbook data to JSON file
                                                orderbook_data = {
                                                    'timestamp': timestamp,
                                                    'server_timestamp': server_timestamp,
                                                    'token_id': token_id,
                                                    'market_id': context['market_id'],
                                                    'event_id': context['event_id'],
                                                    'question': context['question'],
                                                    'side': context['side'],
                                                    'event_type': event_type,
                                                    'summary': {
                                                        'best_bid': best_bid,
                                                        'best_bid_size': best_bid_size,
                                                        'best_ask': best_ask,
                                                        'best_ask_size': best_ask_size,
                                                        'mid_price': mid_price,
                                                        'spread': spread
                                                    },
                                                    'full_orderbook': {
                                                        'bids': bids,
                                                        'asks': asks
                                                    }
                                                }
                                                
                                                # Save to a timestamped JSON file
                                                json_filename = f"{ORDERBOOK_DATA_DIR}/{token_id[:8]}_{int(datetime.now().timestamp())}.json"
                                                try:
                                                    async with file_lock:
                                                        with open(json_filename, 'w') as f:
                                                            json.dump(orderbook_data, f, indent=2)
                                                    print(f"[{now}] [INFO] Successfully saved orderbook JSON to {json_filename}")
                                                except Exception as e:
                                                    print(f"[{now}] [ERROR] Failed to save JSON file {json_filename}: {e}")
                                                
                                                print(f"[DATA] Saved orderbook for {context['question']} ({context['side']})")
                                                
                                            elif event_type == 'price_change':
                                                # Save price change data
                                                changes = item.get('changes', [])
                                                
                                                # Create a structured object with the price change data
                                                price_data = {
                                                    'timestamp': timestamp,
                                                    'server_timestamp': server_timestamp,
                                                    'token_id': token_id,
                                                    'market_id': context['market_id'],
                                                    'event_id': context['event_id'],
                                                    'question': context['question'],
                                                    'side': context['side'],
                                                    'event_type': event_type,
                                                    'changes': changes
                                                }
                                                
                                                # Save to a timestamped JSON file
                                                json_filename = f"{ORDERBOOK_DATA_DIR}/{token_id[:8]}_price_{int(datetime.now().timestamp())}.json"
                                                try:
                                                    async with file_lock:
                                                        with open(json_filename, 'w') as f:
                                                            json.dump(price_data, f, indent=2)
                                                    print(f"[{now}] [INFO] Successfully saved price change JSON to {json_filename}")
                                                except Exception as e:
                                                    print(f"[{now}] [ERROR] Failed to save JSON file {json_filename}: {e}")
                                                
                                                # Also save to CSV for time series analysis
                                                for change in changes:
                                                    change_side = change.get('side')
                                                    change_price = change.get('price')
                                                    change_size = change.get('size')
                                                    
                                                    # Map SELL/BUY to bid/ask for CSV consistency
                                                    if change_side == 'SELL':
                                                        row = [timestamp, context['event_id'], context['market_id'],
                                                              context['question'], token_id, context['side'],
                                                              None, None, change_price, change_size, None, None]
                                                    else:  # BUY
                                                        row = [timestamp, context['event_id'], context['market_id'],
                                                              context['question'], token_id, context['side'],
                                                              change_price, change_size, None, None, None, None]
                                                    
                                                    try:
                                                        async with file_lock:
                                                            with open(OUTPUT_CSV_FILE, 'a', newline='') as f:
                                                                writer = csv.writer(f)
                                                                writer.writerow(row)
                                                        print(f"[{now}] [INFO] Successfully wrote price change to CSV")
                                                    except Exception as e:
                                                        print(f"[{now}] [ERROR] Failed to write price change to CSV: {e}")
                                                
                                                print(f"[DATA] Saved price changes for {context['question']} ({context['side']})")
                                            elif event_type == 'last_trade_price':
                                                # Save last trade price data
                                                last_price = item.get('last_price')
                                                if last_price:
                                                    print(f"[{now}] [TRADE] Last trade price for {token_id[:8]}...: {last_price}")
                                                    # We could save this to CSV if needed
                            elif isinstance(data, dict):
                                token_id = data.get('asset_id')
                                price = data.get('price')
                                if token_id and price and token_id in token_map:
                                    context = token_map[token_id]
                                    timestamp = datetime.now(timezone.utc).isoformat()
                                    row = [timestamp, context['event_id'], context['market_id'], 
                                           context['question'], token_id, context['side'], price]
                                    async with file_lock:
                                        with open(OUTPUT_CSV_FILE, 'a', newline='') as f:
                                            writer = csv.writer(f)
                                            writer.writerow(row)
                                    print(f"[DATA] Saved price update for {context['question']} ({context['side']}): {price}")
                        except json.JSONDecodeError:
                            print(f"[WARN] Received non-JSON message: {message}")
                        except Exception as e:
                            print(f"[ERROR] Error processing message: {e}")
                            import traceback
                            traceback.print_exc()

                    except asyncio.TimeoutError:
                        # No message received, time to send a PING
                        current_time = asyncio.get_event_loop().time()
                        now_str = datetime.now().strftime("%H:%M:%S")
                        
                        if current_time - last_ping > 10.0:
                            print(f"[{now_str}] [INFO] Sending PING to keep connection alive...")
                            await websocket.send("PING")
                            last_ping = current_time
                            
                            # Also print a waiting message every 30 seconds
                            if int(current_time) % 30 < 10:
                                print(f"[{now_str}] [INFO] Still connected and waiting for price updates...")

        except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.ConnectionClosedOK) as e:
            print(f"[WARN] Websocket disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"[ERROR] An unexpected error occurred: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

# --- Step 4: Run the Collector ---
if __name__ == "__main__":
    print("--- Polymarket Arbitrage Data Collector ---")
    
    # Print current working directory for debugging
    print(f"[INFO] Current working directory: {os.getcwd()}")
    
    # Setup output files and directories
    setup_output_files()
    
    # Use test tokens if in test mode
    if TEST_MODE:
        print("[INFO] Using test mode with hardcoded tokens")
        market_context_map = TEST_TOKENS
    else:
        market_context_map = load_market_context()
        print(f"[INFO] Loaded {len(market_context_map)} tokens from market context")
        
    lock = asyncio.Lock()
    try:
        asyncio.run(data_collector(market_context_map, lock))
    except Exception as e:
        print(f"[FATAL] Unhandled exception in main: {e}")
        import traceback
        traceback.print_exc()
