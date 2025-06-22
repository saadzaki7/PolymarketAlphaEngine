#!/usr/bin/env python3
"""
Data models for Polymarket nested order book.
Defines the data structures for the nested order book components.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, Any


@dataclass
class OrderBookLevel:
    """Represents a single level in an order book (bid or ask) with price and size."""
    price: str  # Using string to avoid floating point precision issues
    size: str
    
    def __post_init__(self):
        # Ensure consistent types
        if not isinstance(self.price, str):
            self.price = str(self.price)
        if not isinstance(self.size, str):
            self.size = str(self.size)


@dataclass
class OrderBookSide:
    """Represents one side (bids or asks) of an order book with top N levels."""
    levels: List[OrderBookLevel] = field(default_factory=list)
    max_levels: int = 2  # Default to storing top 2 levels
    is_bids: bool = False  # True for bids, False for asks
    
    def update(self, new_levels: List[Dict[str, str]]):
        """Update with new levels data from websocket."""
        if not new_levels:
            self.levels = []
            return
            
        # Convert to OrderBookLevel objects and sort appropriately
        levels = [OrderBookLevel(price=level["price"], size=level["size"]) for level in new_levels]
        
        # Sort by price: descending for bids (highest first), ascending for asks (lowest first)
        sorted_levels = sorted(
            levels, 
            key=lambda x: float(x.price), 
            reverse=self.is_bids  # Reverse for bids (highest first)
        )
        
        # Take top N levels
        self.levels = sorted_levels[:self.max_levels]

    @property
    def best_level(self) -> Optional[OrderBookLevel]:
        """Get the best level (first in the list) if available."""
        return self.levels[0] if self.levels else None
    
    @property
    def best_price(self) -> Optional[str]:
        """Get the best price if available."""
        return self.best_level.price if self.best_level else None


@dataclass
class OrderBook:
    """Represents a complete order book with bids and asks."""
    bids: OrderBookSide = field(default_factory=lambda: OrderBookSide(is_bids=True))
    asks: OrderBookSide = field(default_factory=OrderBookSide)
    last_updated: Optional[datetime] = None
    
    def update(self, data: Dict[str, Any]):
        """Update the order book with new data from websocket."""
        self.bids.update(data.get("bids", []))
        self.asks.update(data.get("asks", []))
        self.last_updated = datetime.utcnow()


@dataclass
class OutcomeSide:
    """Represents one side (YES/NO) of a market outcome."""
    token_id: str
    order_book: OrderBook = field(default_factory=OrderBook)
    
    def update_order_book(self, data: Dict[str, Any]):
        """Update the order book with new data."""
        self.order_book.update(data)


@dataclass
class Outcome:
    """Represents a market outcome with both YES and NO sides."""
    title: str
    market_id: str
    yes: OutcomeSide
    no: OutcomeSide
    
    @classmethod
    def from_outcome_json(cls, outcome_data: Dict[str, Any]):
        """Create an Outcome object from JSON outcome data."""
        websocket_tokens = outcome_data.get("websocket_tokens", {})
        yes_token = websocket_tokens.get("yes", "")
        no_token = websocket_tokens.get("no", "")
        
        return cls(
            title=outcome_data.get("title", ""),
            market_id=outcome_data.get("market_id", ""),
            yes=OutcomeSide(token_id=yes_token),
            no=OutcomeSide(token_id=no_token)
        )


@dataclass
class Event:
    """Represents a multi-outcome event with all its outcomes."""
    event_id: int
    title: str
    outcomes: Dict[str, Outcome] = field(default_factory=dict)
    
    @classmethod
    def from_event_json(cls, event_data: Dict[str, Any]):
        """Create an Event object from JSON event data."""
        event = cls(
            event_id=event_data.get("event_id", 0),
            title=event_data.get("title", "")
        )
        
        # Add all outcomes
        for outcome_data in event_data.get("outcomes", []):
            market_id = outcome_data.get("market_id", "")
            outcome = Outcome.from_outcome_json(outcome_data)
            event.outcomes[market_id] = outcome
            
        return event


class TokenMapper:
    """Utility class to map between token IDs and outcomes."""
    def __init__(self):
        self.token_to_outcome: Dict[str, Tuple[str, str, str]] = {}  # token_id -> (event_id, market_id, side)
        
    def add_event(self, event: Event):
        """Add an event's tokens to the mapping."""
        for market_id, outcome in event.outcomes.items():
            self.token_to_outcome[outcome.yes.token_id] = (str(event.event_id), market_id, "yes")
            self.token_to_outcome[outcome.no.token_id] = (str(event.event_id), market_id, "no")
    
    def get_outcome_info(self, token_id: str) -> Optional[Tuple[str, str, str]]:
        """Get event_id, market_id, and side for a token."""
        return self.token_to_outcome.get(token_id)
