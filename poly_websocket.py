#!/usr/bin/env python3
import asyncio
import json
import websockets
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable, Awaitable, List, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger('poly_websocket')

class PolyWebSocket:
    """
    A WebSocket client for connecting to Polymarket's WebSocket API.
    Handles connection, reconnection, and message processing with Polymarket-specific defaults.
    """
    
    # Default Polymarket WebSocket URL
    DEFAULT_WS_URL = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
    
    # Default ping interval in seconds (Polymarket recommends keeping connection alive)
    PING_INTERVAL = 25  # 25 seconds (less than 30s to avoid timeouts)
    
    def __init__(
        self,
        asset_ids: Union[str, List[str]],
        url: Optional[str] = None,
        on_message: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        on_book: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        on_price_change: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        on_tick_size_change: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        ping_interval: float = 25.0
    ):
        """
        Initialize the Polymarket Market WebSocket client.
        
        Args:
            asset_ids: Single asset ID or list of asset IDs (token IDs) to subscribe to
            url: WebSocket URL (defaults to Polymarket's production endpoint)
            on_message: Callback for all messages
            on_book: Callback for 'book' event_type messages (order book updates)
            on_price_change: Callback for 'price_change' event_type messages
            on_tick_size_change: Callback for 'tick_size_change' event_type messages
            ping_interval: Interval in seconds to send ping messages (default: 25s)
        """
        self.url = url or self.DEFAULT_WS_URL
        self.asset_ids = [asset_ids] if isinstance(asset_ids, str) else asset_ids
        self.ping_interval = ping_interval
        
        # Callbacks
        self.on_message_callback = on_message
        self.on_book = on_book
        self.on_price_change = on_price_change
        self.on_tick_size_change = on_tick_size_change
        
        # Connection state
        self.running = False
        self.websocket = None
        self.reconnect_delay = 1  # Start with 1 second delay
        self.max_reconnect_delay = 30  # Maximum 30 seconds delay
        self.last_ping = 0
        self.ping_task = None
        self.websocket = None
        self.reconnect_delay = 1  # Start with 1 second delay
        self.max_reconnect_delay = 30  # Maximum 30 seconds delay
        
    def _build_subscription_message(self) -> Dict[str, Any]:
        """Build the market subscription message."""
        return {
            'type': 'MARKET',
            'assets_ids': self.asset_ids
        }
    
    async def _handle_message(self, message: str) -> None:
        """Handle incoming WebSocket message."""
        try:
            # Parse the message which could be a list or dict
            parsed = json.loads(message)
            
            # Handle both list and single message cases
            messages = parsed if isinstance(parsed, list) else [parsed]
            
            for data in messages:
                # Log the raw message
                logger.debug(f"Processing message: {data}")
                
                # Call the general message handler if provided
                if self.on_message_callback:
                    await self.on_message_callback(data)
                
                # Route to appropriate handler based on event_type
                if not isinstance(data, dict):
                    logger.warning(f"Skipping non-dict message: {data}")
                    continue
                    
                event_type = data.get('event_type')
                
                # Skip price_change and tick_size_change messages silently
                if event_type in ['price_change', 'tick_size_change']:
                    continue
                    
                # Handle book updates if handler is provided
                if event_type == 'book' and self.on_book:
                    await self.on_book(data)
                # Log other unhandled message types
                elif not self.on_message_callback and event_type not in ['price_change', 'tick_size_change']:
                    logger.info(f"Unhandled message type: {event_type}")
                
        except json.JSONDecodeError:
            logger.warning(f"Received non-JSON message: {message}")
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
    
    async def _send_ping(self) -> None:
        """Send ping message to keep connection alive."""
        if self.websocket and self.running:
            try:
                await self.websocket.ping()
                self.last_ping = asyncio.get_event_loop().time()
                logger.debug("Ping sent")
            except Exception as e:
                logger.warning(f"Failed to send ping: {e}")
    
    async def _keepalive(self) -> None:
        """Background task to send periodic pings."""
        while self.running:
            try:
                await self._send_ping()
                await asyncio.sleep(self.ping_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Keepalive error: {e}")
                await asyncio.sleep(1)  # Prevent tight loop on errors
    
    async def _connect(self) -> None:
        """Establish WebSocket connection and handle reconnection logic."""
        while self.running:
            try:
                logger.info(f"Connecting to {self.url}...")
                async with websockets.connect(
                    self.url,
                    ping_interval=None,  # We'll handle pings manually
                    close_timeout=1,
                    max_queue=1024,
                ) as websocket:
                    self.websocket = websocket
                    self.reconnect_delay = 1  # Reset delay on successful connection
                    
                    # Start keepalive task
                    self.ping_task = asyncio.create_task(self._keepalive())
                    
                    # Build and send subscription message
                    subscription = self._build_subscription_message()
                    if subscription:
                        await websocket.send(json.dumps(subscription))
                        logger.info(f"Subscribed to {len(self.asset_ids or self.markets or [])} items")
                    
                    # Main message loop
                    async for message in websocket:
                        if not self.running:
                            break
                        await self._handle_message(message)
                            
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Connection closed: {e}. Reconnecting in {self.reconnect_delay} seconds...")
                await self._cleanup()
                await asyncio.sleep(self.reconnect_delay)
                # Exponential backoff with max limit
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
                
            except Exception as e:
                logger.error(f"WebSocket error: {e}", exc_info=True)
                await self._cleanup()
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
    
    async def _cleanup(self) -> None:
        """Clean up resources."""
        if self.ping_task and not self.ping_task.done():
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
            self.ping_task = None
            
        if self.websocket:
            await self.websocket.close()
            self.websocket = None
    
    async def start(self) -> None:
        """Start the WebSocket client."""
        if self.running:
            logger.warning("WebSocket client is already running")
            return
            
        if not self.asset_ids:
            raise ValueError("At least one asset_id must be provided")
            
        self.running = True
        await self._connect()
    
    async def stop(self) -> None:
        """Stop the WebSocket client."""
        self.running = False
        await self._cleanup()
        logger.info("WebSocket client stopped")


# Example usage with Polymarket market data
async def main():
    """Example usage of the PolyWebSocket class with Polymarket API."""
    # Example token IDs (replace with actual token IDs you want to monitor)
    token_ids = [
        "65818619657568813474341868652308942079804919287380422192892211131408793125422",  # Example YES token
        "71321045679252212594626385532706912750332728571942532289631379312455583992563"   # Example NO token
    ]
    
    # Define handlers for different message types
    async def on_book(data: Dict[str, Any]) -> None:
        """Handle order book updates."""
        asset_id = data.get('asset_id', 'unknown')[:8]  # First 8 chars for brevity
        market = data.get('market', 'unknown')[:8]
        logger.info(f"Book update for asset {asset_id} (market: {market})")
        
        # Example: Print top of book
        bids = data.get('buys', [])[:1]  # Top bid
        asks = data.get('sells', [])[:1]  # Top ask
        
        if bids:
            logger.info(f"  Best bid: {bids[0].get('price')} @ {bids[0].get('size')}")
        if asks:
            logger.info(f"  Best ask: {asks[0].get('price')} @ {asks[0].get('size')}")
    
    async def on_price_change(data: Dict[str, Any]) -> None:
        """Handle price change events."""
        asset_id = data.get('asset_id', 'unknown')[:8]
        changes = data.get('changes', [])
        logger.info(f"Price change for {asset_id} - {len(changes)} updates")
        
        for change in changes:
            logger.info(f"  {change.get('side')} {change.get('size')} @ {change.get('price')}")
    
    # Create and start the WebSocket client
    logger.info(f"Connecting to Polymarket WebSocket for {len(token_ids)} token(s)")
    
    ws_client = PolyWebSocket(
        asset_ids=token_ids,
        on_book=on_book,
        on_price_change=on_price_change
    )
    
    try:
        logger.info("Starting WebSocket client (Ctrl+C to stop)...")
        await ws_client.start()
        
        # Keep the client running until interrupted
        while True:
            await asyncio.sleep(1)
            
    except asyncio.CancelledError:
        logger.info("Shutdown requested...")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        await ws_client.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nShutting down...")
