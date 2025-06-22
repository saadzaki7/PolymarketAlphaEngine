#!/usr/bin/env python3
"""
OrderBook Manager for Polymarket nested order books.
Handles updating the order books based on incoming websocket messages.
"""
import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime

from data_models import Event, TokenMapper, OutcomeSide

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('orderbook_manager')


class OrderBookManager:
    """
    Manages the nested order books for multiple events.
    Maps incoming websocket messages to the appropriate order book.
    """
    
    def __init__(self):
        self.events: Dict[str, Event] = {}  # event_id -> Event
        self.token_mapper = TokenMapper()
        
    def add_event(self, event_data: Dict[str, Any]) -> Event:
        """
        Add an event from JSON data.
        
        Args:
            event_data: JSON data for the event
            
        Returns:
            Event object that was created
        """
        event = Event.from_event_json(event_data)
        self.events[str(event.event_id)] = event
        self.token_mapper.add_event(event)
        logger.info(f"Added event: {event.title} (ID: {event.event_id}) with {len(event.outcomes)} outcomes")
        return event
    
    def load_events_from_file(self, file_path: str) -> List[Event]:
        """
        Load events from a JSON file. Handles both JSON array format and JSONL format.
        
        Args:
            file_path: Path to the JSON/JSONL file
            
        Returns:
            List of Event objects created
        """
        events_added = []
        
        try:
            with open(file_path, 'r') as f:
                # First try parsing as a complete JSON array
                try:
                    content = f.read()
                    json_data = json.loads(content)
                    
                    # If it's a list/array, process each item
                    if isinstance(json_data, list):
                        for event_data in json_data:
                            try:
                                event = self.add_event(event_data)
                                events_added.append(event)
                            except Exception as e:
                                logger.error(f"Error processing event: {e}")
                    else:
                        # Single event object
                        try:
                            event = self.add_event(json_data)
                            events_added.append(event)
                        except Exception as e:
                            logger.error(f"Error processing event: {e}")
                            
                except json.JSONDecodeError:
                    # If it fails, try parsing as JSONL (line by line)
                    logger.info(f"File {file_path} is not a valid JSON array, trying JSONL format")
                    f.seek(0)  # Reset file pointer to beginning
                    
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                            
                        try:
                            event_data = json.loads(line)
                            event = self.add_event(event_data)
                            events_added.append(event)
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse JSON line: {line[:100]}...")
                        except Exception as e:
                            logger.error(f"Error processing event: {e}")
                            
        except Exception as e:
            logger.error(f"Error loading events from {file_path}: {e}")
            
        logger.info(f"Loaded {len(events_added)} events from {file_path}")
        return events_added
    
    def load_events_from_json_blob(self, json_blob: str) -> Optional[Event]:
        """
        Load a single event from a JSON blob.
        
        Args:
            json_blob: JSON string containing event data
            
        Returns:
            Event object if successful, None otherwise
        """
        try:
            event_data = json.loads(json_blob)
            event = self.add_event(event_data)
            return event
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON blob: {json_blob[:100]}...")
        except Exception as e:
            logger.error(f"Error processing event from blob: {e}")
        return None
    
    def get_token_ids(self) -> List[str]:
        """
        Get all token IDs across all events.
        
        Returns:
            List of all token IDs
        """
        return list(self.token_mapper.token_to_outcome.keys())
    
    def get_token_ids_for_event(self, event_id: str) -> List[str]:
        """
        Get all token IDs for a specific event.
        
        Args:
            event_id: ID of the event
            
        Returns:
            List of token IDs for the event
        """
        event_token_ids = []
        for token_id, (mapped_event_id, _, _) in self.token_mapper.token_to_outcome.items():
            if mapped_event_id == event_id:
                event_token_ids.append(token_id)
        return event_token_ids
    
    def handle_book_update(self, data: Dict[str, Any]) -> bool:
        """
        Handle an order book update from the websocket.
        
        Args:
            data: Order book update data
            
        Returns:
            True if update was processed, False otherwise
        """
        asset_id = data.get("asset_id")
        if not asset_id:
            logger.warning("Received book update without asset_id")
            return False
            
        # Find the corresponding event, outcome, and side
        outcome_info = self.token_mapper.get_outcome_info(asset_id)
        if not outcome_info:
            logger.debug(f"Received book update for unknown asset ID: {asset_id[:8]}...")
            return False
            
        event_id, market_id, side = outcome_info
        
        # Get the event
        event = self.events.get(event_id)
        if not event:
            logger.warning(f"Event not found for ID: {event_id}")
            return False
            
        # Get the outcome
        outcome = event.outcomes.get(market_id)
        if not outcome:
            logger.warning(f"Outcome not found for market ID: {market_id}")
            return False
            
        # Get the side (YES or NO)
        outcome_side: Optional[OutcomeSide] = getattr(outcome, side, None)
        if not outcome_side:
            logger.warning(f"Invalid side: {side}")
            return False
            
        # Update the order book
        outcome_side.update_order_book(data)
        
        logger.debug(
            f"Updated order book for event '{event.title}', outcome '{outcome.title}', side {side}"
        )
        return True
