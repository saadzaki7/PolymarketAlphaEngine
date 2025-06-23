#!/usr/bin/env python3
"""
Filter and sample Polymarket events by priority score.

This script loads Polymarket events from a JSON file, filters for mutually exclusive events,
sorts them by priority score, and saves a random sample of the top events to a new file.
"""

import json
import random
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional


def load_events(input_file: str) -> List[Dict[str, Any]]:
    """Load events from a JSON file."""
    with open(input_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def filter_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter events to only include mutually exclusive ones."""
    return [event for event in events if event.get('mutually_exclusive', False)]


def sort_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort events by priority score in descending order."""
    return sorted(
        events,
        key=lambda x: x.get('priority_score', float('-inf')),
        reverse=True
    )


def sample_events(
    events: List[Dict[str, Any]],
    top_n: int = 150,
    sample_size: int = 50,
    seed: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Take the top N events and return a random sample of sample_size.
    
    Args:
        events: List of events
        top_n: Number of top events to consider
        sample_size: Number of events to sample
        seed: Random seed for reproducibility
    """
    if seed is not None:
        random.seed(seed)
    
    # Take top N events
    top_events = events[:top_n]
    
    # If sample_size is greater than available events, return all
    if sample_size >= len(top_events):
        return top_events
    
    # Return random sample
    return random.sample(top_events, sample_size)


def save_events(events: List[Dict[str, Any]], output_file: str) -> None:
    """Save events to a JSON file."""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(events, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Filter and sample Polymarket events by priority score.')
    parser.add_argument(
        '--input', 
        type=str, 
        default='polymarket_events.json',
        help='Input JSON file with Polymarket events (default: polymarket_events.json)'
    )
    parser.add_argument(
        '--output', 
        type=str, 
        default='filtered_poly.json',
        help='Output JSON file for filtered events (default: filtered_poly.json)'
    )
    parser.add_argument(
        '--top-n', 
        type=int, 
        default=150,
        help='Number of top events to consider for sampling (default: 150)'
    )
    parser.add_argument(
        '--sample-size', 
        type=int, 
        default=50,
        help='Number of events to randomly sample (default: 50)'
    )
    parser.add_argument(
        '--seed', 
        type=int, 
        default=None,
        help='Random seed for reproducible sampling (default: None)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.top_n <= 0:
        raise ValueError("top-n must be a positive integer")
    if args.sample_size <= 0:
        raise ValueError("sample-size must be a positive integer")
    
    print(f"Loading events from {args.input}...")
    events = load_events(args.input)
    print(f"Loaded {len(events)} total events")
    
    print("Filtering for mutually exclusive events...")
    filtered_events = filter_events(events)
    print(f"Found {len(filtered_events)} mutually exclusive events")
    
    print("Sorting by priority score...")
    sorted_events = sort_events(filtered_events)
    
    print(f"Selecting top {args.top_n} events...")
    print(f"Randomly sampling {args.sample_size} events...")
    sampled_events = sample_events(
        sorted_events,
        top_n=args.top_n,
        sample_size=args.sample_size,
        seed=args.seed
    )
    
    print(f"Saving {len(sampled_events)} events to {args.output}...")
    save_events(sampled_events, args.output)
    print("Done!")


if __name__ == "__main__":
    main()
