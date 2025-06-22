#!/usr/bin/env python3
"""
Nested Order Book for Polymarket multi-outcome events.
Maintains order books for all outcomes of multi-outcome events
and prints updates periodically.
"""
import asyncio
import json
import logging
import argparse
import signal
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

# Import custom components
from data_models import Event
from orderbook_manager import OrderBookManager
from order_printer import OrderBookPrinter
from poly_websocket import PolyWebSocket

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('poly_orderbook')


class PolyOrderBookApp:
    """
    Main application class for the Polymarket nested order book system.
    Integrates the orderbook manager, printer, and websocket client.
    """
    
    def __init__(
        self,
        event_json: Optional[str] = None,
        event_file: Optional[str] = None,
        print_interval: int = 5,
        event_id_filter: Optional[str] = None
    ):
        """
        Initialize the Polymarket order book application.
        
        Args:
            event_json: JSON string containing event data
            event_file: Path to JSONL file containing event data
            print_interval: Interval in seconds to print order books
            event_id_filter: Only monitor events with this ID (optional)
        """
        self.orderbook_manager = OrderBookManager()
        self.printer = OrderBookPrinter(print_interval=print_interval)
        self.websocket_client = None
        self.event_id_filter = event_id_filter
        self.running = False
        
        # Load events
        if event_json:
            self.event = self.orderbook_manager.load_events_from_json_blob(event_json)
            if not self.event:
                logger.error("Failed to load event from JSON string")
                sys.exit(1)
        elif event_file:
            events = self.orderbook_manager.load_events_from_file(event_file)
            if not events:
                logger.error(f"Failed to load events from file: {event_file}")
                sys.exit(1)
                
            # If filter is specified, only keep that event
            if self.event_id_filter:
                filtered_events = [e for e in events if str(e.event_id) == self.event_id_filter]
                if not filtered_events:
                    logger.error(f"Event with ID {self.event_id_filter} not found")
                    sys.exit(1)
                # Update the events dictionary to only contain the filtered event
                for event in filtered_events:
                    self.orderbook_manager.events = {str(event.event_id): event}
                logger.info(f"Filtered to {len(filtered_events)} events with ID {self.event_id_filter}")
        else:
            logger.error("No event data provided")
            sys.exit(1)
    
    async def handle_book_update(self, data: Dict[str, Any]):
        """
        Handle an order book update from the websocket.
        
        Args:
            data: Order book update data
        """
        processed = self.orderbook_manager.handle_book_update(data)
        if processed:
            logger.debug(f"Processed book update for asset_id {data.get('asset_id', '')[:8]}...")
    
    async def start(self):
        """Start the order book application."""
        if self.running:
            logger.warning("Application already running")
            return
            
        self.running = True
        logger.info("Starting Polymarket Order Book application...")
        
        # Determine which token IDs to subscribe to
        token_ids = []
        if self.event_id_filter:
            token_ids = self.orderbook_manager.get_token_ids_for_event(self.event_id_filter)
            logger.info(f"Subscribing to {len(token_ids)} tokens for event {self.event_id_filter}")
        else:
            token_ids = self.orderbook_manager.get_token_ids()
            logger.info(f"Subscribing to all {len(token_ids)} tokens across all events")
            
        if not token_ids:
            logger.error("No token IDs found to subscribe to")
            return
        
        # Start the printer
        self.printer.start_periodic_printing(self.orderbook_manager.events)
        
        # Initialize WebSocket client
        self.websocket_client = PolyWebSocket(
            asset_ids=token_ids,
            on_book=self.handle_book_update
        )
        
        try:
            # Start the WebSocket client
            await self.websocket_client.start()
            logger.info("WebSocket client started. Waiting for data...")
            
            # Keep the client running until interrupted
            while self.running:
                await asyncio.sleep(1)
                
        except asyncio.CancelledError:
            logger.info("Application shutdown requested...")
            self.running = False
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            self.running = False
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the order book application."""
        logger.info("Stopping Polymarket Order Book application...")
        
        # Stop the printer
        self.printer.stop_periodic_printing()
        
        # Stop the WebSocket client
        if self.websocket_client:
            await self.websocket_client.stop()
            
        self.running = False
        logger.info("Application stopped")


async def main():
    """Main entry point for the application."""
    parser = argparse.ArgumentParser(description="Polymarket Nested Order Book")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--event-json", help="JSON string containing event data")
    group.add_argument("--event-file", help="Path to JSONL file containing event data")
    parser.add_argument("--print-interval", type=int, default=5, help="Interval in seconds to print order books (default: 5)")
    parser.add_argument("--event-id", help="Only monitor events with this ID")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Logging level")
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Create and start the application
    app = PolyOrderBookApp(
        event_json=args.event_json,
        event_file=args.event_file,
        print_interval=args.print_interval,
        event_id_filter=args.event_id
    )
    
    # Handle graceful shutdown on interrupt
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(app.stop()))
        
    try:
        await app.start()
    finally:
        logging.info("Exiting application")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Exiting...")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        sys.exit(1)
