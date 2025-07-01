#!/usr/bin/env python3
"""
Order book printer for formatting and displaying Polymarket nested order books.
"""
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import time
import threading
import io
from tabulate import tabulate

from data_models import Event, OrderBook, OrderBookLevel

# Initialize logger
logger = logging.getLogger('order_printer')


class OrderBookPrinter:
    """
    Formats and displays nested order books in a clean, readable format.
    Can run in a background thread to print updates at regular intervals.
    """
    
    def __init__(self, print_interval: int = 5, use_console: bool = True):
        """
        Initialize the order book printer.
        
        Args:
            print_interval: Interval in seconds to print order books (default: 5)
            use_console: Whether to print to console (default: True)
        """
        self.print_interval = print_interval
        self.timer_thread = None
        self.running = False
        self.use_console = use_console
        
    def _format_level(self, level: Optional[OrderBookLevel]) -> str:
        """Format a single order book level for display."""
        if not level:
            return "-.-- @ -"
        return f"{float(level.price):.2f} @ {level.size}"
        
    def _format_order_book(self, order_book: OrderBook) -> Dict[str, str]:
        """Format an order book for display."""
        best_bid = self._format_level(order_book.bids.best_level)
        best_ask = self._format_level(order_book.asks.best_level)
        
        if len(order_book.bids.levels) > 1:
            second_bid = self._format_level(order_book.bids.levels[1])
        else:
            second_bid = "-.-- @ -"
            
        if len(order_book.asks.levels) > 1:
            second_ask = self._format_level(order_book.asks.levels[1])
        else:
            second_ask = "-.-- @ -"
            
        last_updated = order_book.last_updated.strftime("%H:%M:%S") if order_book.last_updated else "-"
        
        return {
            "best_bid": best_bid,
            "second_bid": second_bid,
            "best_ask": best_ask,
            "second_ask": second_ask,
            "spread": "-.--" if not (order_book.asks.best_level and order_book.bids.best_level) else 
                    f"{float(order_book.asks.best_level.price) - float(order_book.bids.best_level.price):.2f}",
            "last_updated": last_updated
        }
    
    def print_event_order_books(self, event: Event):
        """
        Print all order books for a single event.
        
        Args:
            event: Event object to print order books for
        """
        # Format the header
        header = f"\n===== {event.title} Order Books - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ====="
        
        table_data = []
        headers = ["Team/Outcome", "Best Bid", "2nd Bid", "Best Ask", "2nd Ask", "Spread", "Updated"]
        
        for market_id, outcome in sorted(event.outcomes.items(), key=lambda x: x[1].title):
            yes_book_fmt = self._format_order_book(outcome.yes.order_book)
            no_book_fmt = self._format_order_book(outcome.no.order_book)
            
            # Format the outcome title to show only the team name
            title = outcome.title
            if "Will the" in title and "win the" in title:
                title = title.split("Will the ")[1].split(" win the")[0]
            
            # Add YES row
            table_data.append([
                f"{title} (YES)",
                yes_book_fmt["best_bid"],
                yes_book_fmt["second_bid"],
                yes_book_fmt["best_ask"],
                yes_book_fmt["second_ask"],
                yes_book_fmt["spread"],
                yes_book_fmt["last_updated"]
            ])
            
            # Add NO row
            table_data.append([
                f"{title} (NO)",
                no_book_fmt["best_bid"],
                no_book_fmt["second_bid"],
                no_book_fmt["best_ask"],
                no_book_fmt["second_ask"],
                no_book_fmt["spread"],
                no_book_fmt["last_updated"]
            ])
            
            # Add separator between outcomes
            table_data.append(["-" * 15] + ["-" * 10] * 6)
        
        # Remove the last separator
        if table_data:
            table_data.pop()
        
        # Format the table
        table = tabulate(table_data, headers=headers, tablefmt="pretty")
        footer = f"Total Outcomes: {len(event.outcomes)}"
        
        # Combine all parts
        output = f"{header}\n{table}\n{footer}"
        
        # Log the formatted output
        if self.use_console:
            # If console output is enabled, print directly
            print(output)
        else:
            # Otherwise just log at INFO level
            logger.info(f"Order book update for {event.title} with {len(event.outcomes)} outcomes")
            # And log the full table at DEBUG level
            logger.debug(f"\n{output}")
        
    def _print_timer(self, events: Dict[str, Event]):
        """Background thread function to print order books at regular intervals."""
        while self.running:
            try:
                for event_id, event in events.items():
                    self.print_event_order_books(event)
            except Exception as e:
                logger.error(f"Error in print timer: {e}", exc_info=True)
            time.sleep(self.print_interval)
    
    def start_periodic_printing(self, events: Dict[str, Event]):
        """
        Start a background thread to periodically print order books.
        
        Args:
            events: Dictionary of events to print order books for
        """
        if self.running:
            logger.warning("Printer already running")
            return
            
        self.running = True
        self.timer_thread = threading.Thread(
            target=self._print_timer,
            args=(events,),
            daemon=True
        )
        self.timer_thread.start()
        logger.info(f"Started periodic printing every {self.print_interval} seconds")
    
    def stop_periodic_printing(self):
        """Stop the background thread for periodic printing."""
        self.running = False
        if self.timer_thread:
            self.timer_thread.join(timeout=1.0)
            self.timer_thread = None
        logger.info("Stopped periodic printing")
