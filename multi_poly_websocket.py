#!/usr/bin/env python3
"""
Multi-WebSocket manager for Polymarket connections.
Splits large numbers of token subscriptions across multiple WebSocket connections
to avoid hitting connection limits (~480 tokens per connection).
"""
import asyncio
import logging
from typing import Dict, Any, Optional, Callable, Awaitable, List, Union

from poly_websocket import PolyWebSocket

# Set up logging
logger = logging.getLogger('multi_poly_websocket')

class MultiPolyWebSocket:
    """
    Manages multiple PolyWebSocket connections to handle large numbers of token subscriptions.
    Distributes token IDs across multiple connections but maintains single callback interface.
    """
    
    def __init__(
        self,
        asset_ids: List[str],
        chunk_size: int = 450,
        url: Optional[str] = None,
        on_message: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        on_book: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        on_price_change: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        on_tick_size_change: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None,
        ping_interval: float = 25.0
    ):
        """
        Initialize the Multi-WebSocket manager.
        
        Args:
            asset_ids: List of asset IDs (token IDs) to subscribe to
            chunk_size: Maximum number of token IDs per WebSocket connection
            url: WebSocket URL (defaults to Polymarket's production endpoint)
            on_message: Callback for all messages
            on_book: Callback for 'book' event_type messages (order book updates)
            on_price_change: Callback for 'price_change' event_type messages
            on_tick_size_change: Callback for 'tick_size_change' event_type messages
            ping_interval: Interval in seconds to send ping messages (default: 25s)
        """
        self.asset_ids = asset_ids
        self.chunk_size = chunk_size
        self.url = url
        self.on_message_callback = on_message
        self.on_book = on_book
        self.on_price_change = on_price_change
        self.on_tick_size_change = on_tick_size_change
        self.ping_interval = ping_interval
        
        # Split asset IDs into chunks
        self.asset_id_chunks = self._chunk_asset_ids()
        
        # Create a WebSocket client for each chunk
        self.websocket_clients = []
        for chunk_idx, chunk in enumerate(self.asset_id_chunks):
            client = PolyWebSocket(
                asset_ids=chunk,
                url=self.url,
                on_message=self.on_message_callback,
                on_book=self.on_book,
                on_price_change=self.on_price_change,
                on_tick_size_change=self.on_tick_size_change,
                ping_interval=self.ping_interval
            )
            self.websocket_clients.append(client)
            
        logger.info(f"Created {len(self.websocket_clients)} WebSocket clients for {len(asset_ids)} token IDs")
    
    def _chunk_asset_ids(self) -> List[List[str]]:
        """Split asset IDs into chunks of size chunk_size."""
        return [
            self.asset_ids[i:i + self.chunk_size]
            for i in range(0, len(self.asset_ids), self.chunk_size)
        ]
    
    async def start(self) -> None:
        """Start all WebSocket clients."""
        logger.info(f"Starting {len(self.websocket_clients)} WebSocket clients...")
        
        # Start all clients concurrently
        await asyncio.gather(
            *[client.start() for client in self.websocket_clients]
        )
        
        logger.info(f"All WebSocket clients started")
    
    async def stop(self) -> None:
        """Stop all WebSocket clients."""
        logger.info(f"Stopping {len(self.websocket_clients)} WebSocket clients...")
        
        # Stop all clients concurrently
        await asyncio.gather(
            *[client.stop() for client in self.websocket_clients]
        )
        
        logger.info(f"All WebSocket clients stopped")
