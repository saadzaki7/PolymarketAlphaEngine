import csv
import json
import asyncio
import websockets
import datetime

CSV_FILE_PATH = 'multi_outcome_polymarket_events.csv'
WEBSOCKET_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
MARKETS_TO_MONITOR = 5

def get_tokens_from_csv(file_path, num_markets):
    """Reads the CSV file and extracts token IDs to monitor."""
    tokens_to_monitor = []
    market_details = []
    print(f"Reading token IDs from {file_path}...")
    
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Dynamically find outcome columns
                for i in range(1, 100): # Assume max 99 outcomes
                    yes_token_key = f'Outcome {i} Yes Token ID'
                    no_token_key = f'Outcome {i} No Token ID'
                    question_key = f'Outcome {i} Question'

                    if yes_token_key not in row:
                        # No more outcome columns in this row
                        break

                    yes_token = row.get(yes_token_key)
                    no_token = row.get(no_token_key)
                    question = row.get(question_key, 'N/A')

                    if yes_token and no_token:
                        tokens_to_monitor.extend([yes_token, no_token])
                        market_details.append(question)
                        if len(market_details) >= num_markets:
                            print(f"Found {len(market_details)} markets to monitor.")
                            return tokens_to_monitor, market_details
            
            if not market_details:
                print("Could not find any markets with valid token IDs in the CSV.")

    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
    except Exception as e:
        print(f"An error occurred while reading the CSV: {e}")
        
    return tokens_to_monitor, market_details

async def websocket_listener(tokens, details):
    """Connects to the websocket and listens for real-time trades for multiple markets."""
    if not tokens:
        return

    print("\n--- Markets Being Monitored ---")
    for i, detail in enumerate(details):
        print(f"{i+1}. {detail}")
    print(f"\nConnecting to websocket for {len(tokens)} total tokens...")

    try:
        async with websockets.connect(WEBSOCKET_URL) as websocket:
            print("Connection successful. Subscribing to markets...")
            await websocket.send(json.dumps({"assets_ids": tokens, "type": "market"}))
            last_time_pong = datetime.datetime.now()

            while True:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=20)
                    
                    if message == "PONG":
                        last_time_pong = datetime.datetime.now()
                        print(f"PONG received at {last_time_pong.strftime('%H:%M:%S')}")
                        continue

                    data = json.loads(message)
                    print(f"\n--- Real-time Data Received ({datetime.datetime.now().strftime('%H:%M:%S')}) ---")
                    print(json.dumps(data, indent=2))

                    if datetime.datetime.now() - last_time_pong > datetime.timedelta(seconds=15):
                        print("Sending PING to keep connection alive...")
                        await websocket.send("PING")
                        last_time_pong = datetime.datetime.now()

                except asyncio.TimeoutError:
                    print("Websocket timeout. Sending PING...")
                    await websocket.send("PING")
                    last_time_pong = datetime.datetime.now()
                except websockets.exceptions.ConnectionClosed as e:
                    print(f"Websocket connection closed: {e}")
                    break
                except Exception as e:
                    print(f"An error occurred in websocket loop: {e}")
                    break
    except Exception as e:
        print(f"Failed to connect to websocket: {e}")

if __name__ == "__main__":
    tokens, details = get_tokens_from_csv(CSV_FILE_PATH, MARKETS_TO_MONITOR)
    if tokens:
        asyncio.run(websocket_listener(tokens, details))
