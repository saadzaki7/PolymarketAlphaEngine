#!/usr/bin/env python3
"""
Arbitrage analyzer for Polymarket multi-outcome events.
Checks for arbitrage opportunities where the sum of best ask prices 
across all outcomes is less than a configurable threshold.
"""
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal

from data_models import Event, OrderBookLevel

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('arbitrage_analyzer')


def save_to_jsonl(data: Dict[str, Any], output_file: str) -> None:
    """Append data to a JSONL file."""
    with open(output_file, 'a') as f:
        f.write(json.dumps(data) + '\n')


class ArbitrageAnalyzer:
    """
    Analyzes Polymarket event order books for arbitrage opportunities.
    An opportunity exists when the sum of best ask prices across all outcomes
    is less than a configurable threshold (default 0.99).
    """
    
    def __init__(self, threshold: float = 0.99, output_file: str = "arbitrage_opportunities.jsonl"):
        """
        Initialize the arbitrage analyzer.
        
        Args:
            threshold: Threshold value below which an arbitrage opportunity exists (default: 0.99)
            output_file: Path to the file to save arbitrage opportunities to (default: "arbitrage_opportunities.jsonl")
        """
        self.threshold = threshold
        self.output_file = output_file
        logger.info(f"Initialized ArbitrageAnalyzer with threshold {threshold}")

    @staticmethod
    def _get_decimal_value(level: Optional[OrderBookLevel]) -> Optional[Tuple[Decimal, Decimal]]:
        """
        Convert OrderBookLevel price and size to Decimal objects.
        
        Args:
            level: OrderBookLevel object
            
        Returns:
            Tuple of (price, size) as Decimal objects, or None if level is None
        """
        if not level:
            return None
        try:
            price = Decimal(level.price)
            size = Decimal(level.size)
            return price, size
        except (ValueError, TypeError):
            logger.warning(f"Failed to convert price or size to Decimal: {level}")
            return None
    
    def check_event(self, event: Event) -> Optional[Dict[str, Any]]:
        """
        Check an event for arbitrage opportunities.
        
        An opportunity exists when the sum of best ask prices across all outcomes
        is less than the threshold.
        
        Args:
            event: Event object to check
            
        Returns:
            Dict with arbitrage details if an opportunity exists, None otherwise
        """
        asks_data = []
        cost_sum = Decimal('0')
        count_with_asks = 0
        total_outcomes = len(event.outcomes)
        
        logger.debug(f"Checking arbitrage for event {event.event_id} '{event.title}' with {total_outcomes} outcomes")
        
        # Collect data for each outcome
        for market_id, outcome in event.outcomes.items():
            yes_best_ask = self._get_decimal_value(outcome.yes.order_book.asks.best_level)
            
            # Skip outcomes with no ask data
            if not yes_best_ask:
                logger.debug(f"Skipping outcome '{outcome.title}' with no ask data")
                continue
                
            ask_price, ask_size = yes_best_ask
            count_with_asks += 1
            cost_sum += ask_price
            
            logger.debug(f"Adding outcome '{outcome.title}' with ask price {ask_price} to cost sum (now {cost_sum})")
            
            # Record the data for this outcome
            asks_data.append({
                "outcome_title": outcome.title,
                "best_ask_price": str(ask_price),
                "best_ask_size": str(ask_size),
                "token_id": outcome.yes.token_id,
                "last_updated": outcome.yes.order_book.last_updated.isoformat() if outcome.yes.order_book.last_updated else None
            })
            
        # If we have no outcomes with asks, we can't determine arbitrage
        if count_with_asks == 0:
            logger.debug(f"Event {event.title} has no outcomes with ask data")
            return None
        
        # If we don't have enough outcomes with asks, this might not be a complete event
        if count_with_asks < 2:
            logger.debug(f"Event {event.title} only has {count_with_asks}/{total_outcomes} outcomes with ask data - may be incomplete")
        
        # If the sum of asks is less than the threshold, we have an arbitrage opportunity
        is_arbitrage = cost_sum <= Decimal(str(self.threshold))
        
        logger.debug(f"Event {event.title}: {count_with_asks}/{total_outcomes} outcomes with asks, cost_sum={cost_sum}, threshold={self.threshold}, arbitrage={is_arbitrage}")
        
        if is_arbitrage:
            # Format timestamp
            timestamp = datetime.utcnow().isoformat()
            
            # Create the arbitrage opportunity record
            arbitrage_data = {
                "event_id": event.event_id,
                "event_title": event.title,
                "timestamp": timestamp,
                "threshold": self.threshold,
                "cost_sum": str(cost_sum),
                "is_arbitrage": is_arbitrage,
                "outcomes": asks_data
            }
            
            logger.info(f"Found arbitrage opportunity in '{event.title}': cost_sum={cost_sum}, outcomes={count_with_asks}/{total_outcomes} (threshold={self.threshold})")
            
            # Save to file
            save_to_jsonl(arbitrage_data, self.output_file)
            
            return arbitrage_data
        
        logger.debug(f"No arbitrage found in '{event.title}': cost_sum={cost_sum} (threshold={self.threshold})")
        return None
