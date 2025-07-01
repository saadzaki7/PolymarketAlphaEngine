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
import os
import gc
from datetime import datetime
from typing import Dict, List, Any, Optional

# Import custom components
from data_models import Event
from orderbook_manager import OrderBookManager
from order_printer import OrderBookPrinter
from poly_websocket import PolyWebSocket
from multi_poly_websocket import MultiPolyWebSocket
from arbitrage_analyzer import ArbitrageAnalyzer
from logging_config import configure_logging

# Initialize logger but don't configure it yet - will be done in main()
logger = logging.getLogger('poly_orderbook')

# Default raw data output file
DEFAULT_RAW_OUTPUT_FILE = 'book_updates.jsonl'

def save_to_jsonl(data: Dict[str, Any], output_file: str) -> None:
    """Append raw data to JSONL file."""
    with open(output_file, 'a') as f:
        # Add timestamp for when we received the data
        data['_received_at'] = datetime.utcnow().isoformat()
        f.write(json.dumps(data) + '\n')


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
        event_id_filter: Optional[str] = None,
        save_raw: bool = False,
        raw_output_file: str = DEFAULT_RAW_OUTPUT_FILE,
        arbitrage_threshold: float = 0.99,
        max_websocket_connections: int = 480,
        disable_print: bool = False
    ):
        """
        Initialize the Polymarket order book application.
        
        Args:
            event_json: JSON string containing event data
            event_file: Path to JSONL file containing event data
            print_interval: Interval in seconds to print order books
            event_id_filter: Only monitor events with this ID (optional)
            save_raw: Whether to save raw book updates to file
            raw_output_file: File to save raw book updates to
        """
        self.orderbook_manager = OrderBookManager()
        self.printer = OrderBookPrinter(print_interval=print_interval)
        self.websocket_client = None
        self.event_id_filter = event_id_filter
        self.running = False
        self.save_raw = save_raw
        self.raw_output_file = raw_output_file
        self.arbitrage_analyzer = ArbitrageAnalyzer(threshold=arbitrage_threshold)
        self.max_websocket_connections = max_websocket_connections
        self.disable_print = disable_print
        logger.info(f"Arbitrage analyzer initialized with threshold {arbitrage_threshold}")
        logger.info(f"Max token IDs per WebSocket connection: {self.max_websocket_connections}")
        
        # Create directory for raw output file if saving raw data
        if self.save_raw:
            os.makedirs(os.path.dirname(os.path.abspath(self.raw_output_file)) or '.', exist_ok=True)
            logger.info(f"Raw book updates will be saved to: {os.path.abspath(self.raw_output_file)}")
        
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
        # Save raw data to file if enabled
        if self.save_raw:
            save_to_jsonl(data, self.raw_output_file)
            asset_id = data.get('asset_id', '')
            logger.debug(f"Saved raw book update for asset_id {asset_id[:8] if asset_id else 'unknown'}")
            
        processed = self.orderbook_manager.handle_book_update(data)
        if processed:
            asset_id = data.get('asset_id', '')
            logger.debug(f"Processed book update for asset_id {asset_id[:8] if asset_id else 'unknown'}")
            
            # Check for arbitrage opportunities
            token_info = self.orderbook_manager.token_mapper.get_outcome_info(asset_id)
            if token_info:
                event_id, outcome_id, side = token_info
                event = self.orderbook_manager.events.get(event_id)
                if event:
                    # Log the number of outcomes in this event
                    outcome_count = len(event.outcomes) if event.outcomes else 0
                    logger.debug(f"Event {event.title} has {outcome_count} outcomes")
                    
                    # Run arbitrage check if we have at least two outcomes
                    # This ensures we have enough data to make a meaningful calculation
                    if outcome_count >= 2:
                        # Run arbitrage check on the updated event
                        self.arbitrage_analyzer.check_event(event)
                    else:
                        logger.debug(f"Skipping arbitrage check for event {event.title} - insufficient outcomes ({outcome_count})")
    
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
        
        # Start the printer with periodic updates if not disabled
        if not self.disable_print:
            self.printer.start_periodic_printing(self.orderbook_manager.events)
        else:
            logger.info("Periodic printing disabled, order books will not be displayed")
        
        # Initialize WebSocket client based on the number of token IDs
        if len(token_ids) > self.max_websocket_connections:
            logger.info(f"Using MultiPolyWebSocket as token count ({len(token_ids)}) exceeds max per connection ({self.max_websocket_connections})")
            self.websocket_client = MultiPolyWebSocket(
                asset_ids=token_ids,
                chunk_size=self.max_websocket_connections,
                on_book=self.handle_book_update
            )
        else:
            logger.info(f"Using single PolyWebSocket for {len(token_ids)} tokens")
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
                # Force garbage collection every 60 seconds to help manage memory
                if int(datetime.now().timestamp()) % 60 == 0:
                    gc.collect()
                    logger.debug("Forced garbage collection")
                
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
    parser.add_argument("--log-file", default="logs/poly_orderbook.log", help="Path to log file (default: logs/poly_orderbook.log)")
    parser.add_argument("--log-max-size", type=int, default=10, help="Maximum log file size in MB before rotation (default: 10)")
    parser.add_argument("--log-backup-count", type=int, default=5, help="Number of backup log files to keep (default: 5)")
    parser.add_argument("--disable-console-log", action="store_true", help="Disable logging to console")
    parser.add_argument("--disable-print", action="store_true", help="Disable printing order books to console")
    parser.add_argument("--save-raw", action="store_true", help="Save raw book updates to file")
    parser.add_argument("--raw-output-file", default=DEFAULT_RAW_OUTPUT_FILE, help=f"File to save raw book updates (default: {DEFAULT_RAW_OUTPUT_FILE})")
    parser.add_argument("--raw-max-size", type=int, default=100, help="Maximum raw output file size in MB before rotation (default: 100)")
    parser.add_argument("--arbitrage-threshold", type=float, default=0.99, help="Threshold value for arbitrage detection (default: 0.99)")
    parser.add_argument("--max-ws-connections", type=int, default=450, help="Maximum number of token IDs per websocket connection (default: 450)")
    args = parser.parse_args()
    
    # Configure centralized logging
    configure_logging(
        log_level=args.log_level,
        log_file=args.log_file,
        max_file_size_mb=args.log_max_size,
        backup_count=args.log_backup_count,
        console_output=not args.disable_console_log
    )
    
    # Create and start the application
    app = PolyOrderBookApp(
        event_json=args.event_json,
        event_file=args.event_file,
        print_interval=args.print_interval,
        event_id_filter=args.event_id,
        save_raw=args.save_raw,
        raw_output_file=args.raw_output_file,
        arbitrage_threshold=args.arbitrage_threshold,
        max_websocket_connections=args.max_ws_connections,
        disable_print=args.disable_print
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
