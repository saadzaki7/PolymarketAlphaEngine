#!/usr/bin/env python3
import asyncio
import csv
import datetime
import json
import os
import websockets
from datetime import datetime, timezone

# Configuration
NUM_EVENTS_TO_MONITOR = 1  # Reduced to just 1 event for testing
INPUT_CSV_FILE = 'multi_outcome_polymarket_events.csv'
OUTPUT_CSV_FILE = 'market_data_timeseries.csv'
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

# --- Step 2: Prepare a Data File ---
def setup_output_file():
    """Creates the output CSV with a header if it doesn't exist."""
    if not os.path.exists(OUTPUT_CSV_FILE):
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'event_id', 'market_id', 'question', 'token_id', 'side', 'best_bid', 'best_ask', 'mid_price'])
        print(f"Created output file: {OUTPUT_CSV_FILE}")

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
                            
                        print(f"[{now}] [DEBUG] Raw message: {message[:200]}...")

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
                                        if isinstance(item, dict):
                                            token_id = item.get('asset_id')
                                            price = item.get('price')
                                            market = item.get('market')
                                            timestamp_ms = item.get('timestamp')
                                            
                                            # Extract best bid and ask prices if available
                                            bids = item.get('bids', [])
                                            asks = item.get('asks', [])
                                            best_bid = float(bids[0]['price']) if bids else None
                                            best_ask = float(asks[0]['price']) if asks else None
                                            mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else None
                                            
                                            # Print market summary
                                            print(f"[{now}] [MARKET] Asset: {token_id[:8]}... | Bid: {best_bid} | Ask: {best_ask} | Mid: {mid_price:.4f if mid_price else None}")
                                            
                                            if token_id in token_map:
                                                context = token_map[token_id]
                                                timestamp = datetime.now(timezone.utc).isoformat()
                                                # Save both best bid and ask prices
                                                row = [timestamp, context['event_id'], context['market_id'], 
                                                       context['question'], token_id, context['side'], 
                                                       best_bid, best_ask, mid_price]
                                                async with file_lock:
                                                    with open(OUTPUT_CSV_FILE, 'a', newline='') as f:
                                                        writer = csv.writer(f)
                                                        writer.writerow(row)
                                                print(f"[DATA] Saved price update for {context['question']} ({context['side']}): {price}")
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
    setup_output_file()
    
    # Use test tokens if in test mode
    if TEST_MODE:
        print("[INFO] Using test mode with hardcoded tokens")
        market_context_map = TEST_TOKENS
    else:
        market_context_map = load_market_context()
        
    lock = asyncio.Lock()
    asyncio.run(data_collector(market_context_map, lock))
