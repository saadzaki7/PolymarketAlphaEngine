#!/usr/bin/env python3
"""
Raw data collector for Polymarket WebSocket.
Saves all book updates to a JSON file and prints to console for debugging.
"""
import asyncio
import json
import os
from datetime import datetime
from typing import Dict, Any

# Import our WebSocket client
from poly_websocket import PolyWebSocket

# Configuration
OUTPUT_FILE = 'book_updates.jsonl'  # JSON Lines format
OKC_YES_TOKEN = '83527644927648970835156950007024690327726158617181889316317174894904268227846'
OKC_NO_TOKEN = '57190891280800009123681638917785245658930686322518532172325806346323335059331'

def save_to_jsonl(data: Dict[str, Any]) -> None:
    """Append raw data to JSONL file."""
    with open(OUTPUT_FILE, 'a') as f:
        # Add timestamp for when we received the data
        data['_received_at'] = datetime.utcnow().isoformat()
        f.write(json.dumps(data) + '\n')

async def handle_book_update(data: Dict[str, Any]) -> None:
    """Handle and log raw book update messages."""
    # Save raw data to file
    save_to_jsonl(data)
    
    # Print minimal info for debugging
    print(f"\n[RAW] Book Update - {datetime.utcnow().isoformat()}")
    print(f"Asset: {data.get('asset_id')}")
    print(f"Bids: {len(data.get('bids', []))} levels")
    print(f"Asks: {len(data.get('asks', []))} levels")
    print(f"Trades: {len(data.get('trades', []))} recent")

async def main():
    """Main function to run the WebSocket client."""
    print("Starting Polymarket WebSocket client...")
    print(f"Subscribing to OKC market tokens")
    print(f"Raw data will be saved to: {os.path.abspath(OUTPUT_FILE)}")
    print("Press Ctrl+C to exit\n")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.abspath(OUTPUT_FILE)) or '.', exist_ok=True)
    
    # Initialize WebSocket client
    ws = PolyWebSocket(
        asset_ids=[OKC_YES_TOKEN, OKC_NO_TOKEN],
        on_book=handle_book_update
    )
    
    try:
        # Start the WebSocket client
        await ws.start()
        print("WebSocket client started. Waiting for data...")
        
        # Keep the client running until interrupted
        while True:
            await asyncio.sleep(1)
            
    except asyncio.CancelledError:
        print("\nShutting down...")
    except KeyboardInterrupt:
        print("\nReceived exit signal, shutting down...")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        # Clean up
        await ws.stop()
        print(f"WebSocket client stopped. Data saved to {os.path.abspath(OUTPUT_FILE)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Error: {e}")
        raise
