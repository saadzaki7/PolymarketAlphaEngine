#!/usr/bin/env python3
"""
Script to parse Polymarket multi-outcome events CSV and convert to JSON format.

Input: multi_outcome_polymarket_events.csv
Output: Formatted JSON with event and outcome data.
"""

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

def parse_bool(value: str) -> bool:
    """Convert string boolean to Python boolean."""
    return value.lower() == 'true' if value else False

def parse_event(row: Dict[str, str]) -> Dict[str, Any]:
    """Parse a single event row into structured data."""
    # Parse event-level fields
    event = {
        'event_id': int(row['Event ID']),
        'title': row['Title'].strip(),
        'number_of_outcomes': int(row['Number of Outcomes']),
        'start_date': row['Start Date'],
        'end_date': row['End Date'],
        'days_to_resolution': float(row['Days to Resolution']),
        'priority_score': float(row['Priority Score']),
        'mutually_exclusive': parse_bool(row['Mutually Exclusive']),
        'sum_of_bids': float(row['Sum Bids'] or 0),
        'arbitrage_opportunity': float(row['Arbitrage Opportunity'] or 0),
        'annualized_return': float(row['Annualized Return'] or 0),
        'has_liquidity': parse_bool(row['Has Liquidity']),
        'outcomes': []
    }
    
    # Parse outcomes (up to 40 possible outcomes per event)
    for i in range(1, 41):
        question_key = f'Outcome {i} Question'
        if not row.get(question_key):
            continue
            
        outcome = {
            'title': row[question_key].strip(),
            'market_id': row[f'Outcome {i} Market ID'],
            'websocket_tokens': {
                'yes': row.get(f'Outcome {i} Yes Token ID', ''),
                'no': row.get(f'Outcome {i} No Token ID', '')
            },
            'liquidity': float(row.get(f'Outcome {i} Liquidity', 0) or 0),
            'volume_24hr': float(row.get(f'Outcome {i} Volume 24hr', 0) or 0),
            'spread': float(row.get(f'Outcome {i} Spread', 0) or 0)
        }
        event['outcomes'].append(outcome)
    
    return event

def main():
    input_file = Path('multi_outcome_polymarket_events.csv')
    output_file = Path('polymarket_events.json')
    
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    events = []
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    event = parse_event(row)
                    events.append(event)
                except Exception as e:
                    print(f"Error parsing row: {e}")
                    continue
        
        # Write output
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(events, f, indent=2, ensure_ascii=False)
            
        print(f"Successfully processed {len(events)} events. Output written to {output_file}")
        
    except Exception as e:
        print(f"Error processing file: {e}")
        raise

if __name__ == '__main__':
    main()
