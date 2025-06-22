#!/usr/bin/env python3
import asyncio
import json
from datetime import datetime
from typing import Dict, Any

# Import our WebSocket client
from poly_websocket import PolyWebSocket

# Oklahoma City Thunder market tokens
OKC_YES_TOKEN = '83527644927648970835156950007024690327726158617181889316317174894904268227846'
OKC_NO_TOKEN = '57190891280800009123681638917785245658930686322518532172325806346323335059331'

async def handle_book_update(data: Dict[str, Any]) -> None:
    """Handle order book update messages."""
    print("\n=== BOOK UPDATE ===")
    print(f"Market: {data.get('market')}")
    print(f"Asset ID: {data.get('asset_id')}")
    print(f"Timestamp: {datetime.fromtimestamp(int(data.get('timestamp', 0))/1000) if data.get('timestamp') else 'N/A'}")
    
    # Print top 3 bids and asks
    print("\nTop 3 Bids (BUY orders):")
    for i, bid in enumerate(data.get('buys', [])[:3], 1):
        print(f"  {i}. Price: {bid['price']} | Size: {bid['size']}")
    
    print("\nTop 3 Asks (SELL orders):")
    for i, ask in enumerate(data.get('sells', [])[:3], 1):
        print(f"  {i}. Price: {ask['price']} | Size: {ask['size']}")
    print("=" * 40)

async def handle_price_change(data: Dict[str, Any]) -> None:
    """Handle price change messages."""
    print("\n=== PRICE CHANGE ===")
    print(f"Market: {data.get('market')}")
    print(f"Asset ID: {data.get('asset_id')}")
    print(f"Timestamp: {datetime.fromtimestamp(int(data.get('timestamp', 0))/1000) if data.get('timestamp') else 'N/A'}")
    
    for change in data.get('changes', []):
        price = change.get('price', 'N/A')
        size = change.get('size', 'N/A')
        side = change.get('side', 'UNKNOWN')
        print(f"{side} - Price: {price} | Size: {size}")
    print("=" * 40)

async def handle_tick_size_change(data: Dict[str, Any]) -> None:
    """Handle tick size change messages."""
    print("\n=== TICK SIZE CHANGE ===")
    print(f"Market: {data.get('market')}")
    print(f"Asset ID: {data.get('asset_id')}")
    print(f"Side: {data.get('side', 'BOTH')}")
    print(f"Old tick size: {data.get('old_tick_size')}")
    print(f"New tick size: {data.get('new_tick_size')}")
    print(f"Timestamp: {datetime.fromtimestamp(int(data.get('timestamp', 0))/1000) if data.get('timestamp') else 'N/A'}")
    print("=" * 40)

async def handle_raw_message(data: Dict[str, Any]) -> None:
    """Handle all raw messages (for debugging)."""
    print(f"\n[RAW] {data.get('event_type', 'UNKNOWN')} message received")

async def main():
    """Main function to run the WebSocket client."""
    print("Starting Polymarket WebSocket client...")
    print("Subscribing to Oklahoma City Thunder NBA Championship market")
    print("Press Ctrl+C to exit\n")
    
    # Create WebSocket client
    # Create WebSocket client for OKC market
    ws = PolyWebSocket(
        asset_ids=[OKC_YES_TOKEN, OKC_NO_TOKEN],
        on_message=handle_raw_message,
        on_book=handle_book_update,
        on_price_change=handle_price_change,
        on_tick_size_change=handle_tick_size_change
    )
    
    try:
        # Start the WebSocket client
        await ws.start()
        
        # Keep the client running
        while True:
            await asyncio.sleep(1)
            
    except asyncio.CancelledError:
        print("\nShutting down...")
    except KeyboardInterrupt:
        print("\nReceived exit signal, shutting down...")
    finally:
        # Clean up
        await ws.stop()
        print("WebSocket client stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as e:
        print(f"Error: {e}")
        raise
