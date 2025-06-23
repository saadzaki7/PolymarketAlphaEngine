#!/usr/bin/env python3
import requests
import sys
import time
import csv
import concurrent.futures
import re
from datetime import datetime, timezone

# Official Polymarket API endpoint for events
API_URL = "https://gamma-api.polymarket.com/events?closed=false"

# Constants for mutual exclusivity detection
ME_PRICE_THRESHOLD = 0.10  # How close to 1.0 the sum of implied probabilities needs to be
MIN_LIQUIDITY = 10  # Minimum liquidity to consider a market for arbitrage

def verify_market_active_status_batch(event_ids):
    """
    Verifies if markets are truly active by checking the Polymarket website directly in batches.
    Returns a dictionary mapping event_id -> is_active (boolean)
    """
    result = {event_id: True for event_id in event_ids}  # Default all to active
    
    try:
        # Join IDs with commas for the API request
        ids_param = ','.join(map(str, event_ids))
        
        # This endpoint returns market data that includes resolution status
        url = f"https://gamma-api.polymarket.com/markets/market-cards?market-ids={ids_param}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            # Create a mapping of market_id -> resolved status
            market_status = {}
            for market in data:
                market_id = market.get('id')
                if market_id:
                    market_status[market_id] = market.get('resolved', False)
            
            # Update result based on market status
            for event_id in event_ids:
                # If any market for this event is resolved, mark the event as not active
                if str(event_id) in market_status and market_status[str(event_id)]:
                    result[event_id] = False
                    
    except Exception as e:
        print(f"Error verifying batch of markets: {e}")
    
    return result




def verify_events_with_concurrency(events, batch_size=50, max_workers=5):
    """
    Verifies active status of events using concurrent batch processing.
    """
    event_ids = [event.get('id') for event in events]
    active_status = {}
    
    # Split event IDs into batches
    batches = [event_ids[i:i+batch_size] for i in range(0, len(event_ids), batch_size)]
    total_batches = len(batches)
    
    print(f"Verifying {len(event_ids)} events in {total_batches} batches with {max_workers} workers...")
    
    # Process batches with concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batches to the executor
        future_to_batch = {executor.submit(verify_market_active_status_batch, batch): i 
                          for i, batch in enumerate(batches)}
        
        # Process results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_batch):
            batch_index = future_to_batch[future]
            completed += 1
            
            try:
                batch_result = future.result()
                active_status.update(batch_result)
                
                # Print progress every 5 batches or at the end
                if completed % 5 == 0 or completed == total_batches:
                    print(f"  - Verified {completed}/{total_batches} batches ({int(completed/total_batches*100)}%)")
                    
            except Exception as e:
                print(f"  - Error processing batch {batch_index}: {e}")
    
    # Create list of verified active events
    verified_active_events = [event for event in events if active_status.get(event.get('id'), True)]
    inactive_count = len(events) - len(verified_active_events)
    
    print(f"Verification complete: {len(verified_active_events)} truly active events, {inactive_count} resolved/inactive events")
    
    return verified_active_events

def fetch_detailed_events():
    """Fetches active events."""
    try:
        # Step 1: Fetch a list of all active event IDs using pagination
        print("Step 1: Fetching list of all active event IDs (with pagination)...")
        all_event_ids = []
        offset = 0
        limit = 500  # API seems to cap at 500 per request for summaries
        page_num = 1
        while True:
            print(f"  - Fetching page {page_num} (offset {offset}, limit {limit})")
            list_params = {'active': 'true', 'limit': limit, 'offset': offset}
            list_resp = requests.get(API_URL, params=list_params)
            list_resp.raise_for_status()
            event_summaries_page = list_resp.json()
            
            if not event_summaries_page:
                print("  - No more events found on this page. Stopping pagination.")
                break
            
            page_ids = [e['id'] for e in event_summaries_page]
            all_event_ids.extend(page_ids)
            print(f"    - Found {len(page_ids)} events on this page. Total IDs so far: {len(all_event_ids)}")
            
            # # TEMPORARY: Limit to 5 pages for testing
            # if page_num >= 5:
            #     print("  - Reached 5 pages limit (TEMPORARY LIMIT FOR TESTING)")
            #     break
                
            # Continue pagination until there are no more events
            if len(event_summaries_page) < limit:
                print("  - Fetched less than limit, assuming this is the last page.")
                break # Likely the last page
                
            offset += limit
            page_num += 1
            time.sleep(0.5) # Be polite between page fetches

        event_ids = list(set(all_event_ids)) # Remove duplicates just in case
        print(f"\nFound {len(event_ids)} unique active event IDs after pagination.")

        if not event_ids:
            return []

        # Step 2: Fetch the full details for all found event IDs in batches
        # Step 2: Fetch the full details for all found event IDs in batches using threads
        print("\nStep 2: Fetching full details for all event IDs in batches (using threads)...")
        full_events = []
        batch_size = 20  # Max IDs per API call for details
        max_workers = 5  # Reduced number of concurrent threads

        # Prepare all batch_id lists first
        list_of_batch_ids = [event_ids[i:i + batch_size] for i in range(0, len(event_ids), batch_size)]
        total_batches = len(list_of_batch_ids)
        print(f"Preparing to fetch {total_batches} batches with up to {max_workers} concurrent workers.")

        def fetch_batch_details(batch_ids_to_fetch, batch_num):
            print(f"  - Thread fetching batch {batch_num}/{total_batches} (IDs: {batch_ids_to_fetch[0]}...{batch_ids_to_fetch[-1] if len(batch_ids_to_fetch) > 0 else 'N/A'})")
            try:
                detail_params = {'id': batch_ids_to_fetch}
                detail_resp = requests.get(API_URL, params=detail_params, timeout=30) # Added timeout
                detail_resp.raise_for_status()
                data = detail_resp.json()
                time.sleep(0.25) # Small delay to be polite to the API, even within threads
                return data
            except requests.exceptions.RequestException as e:
                print(f"    Error fetching batch {batch_num}: {e}", file=sys.stderr)
                return [] # Return empty list on error for this batch
            except Exception as e:
                print(f"    Unexpected error in thread for batch {batch_num}: {e}", file=sys.stderr)
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks and store futures with their batch numbers for ordered processing or logging
            future_to_batch_num = {executor.submit(fetch_batch_details, batch_ids, idx + 1): idx + 1 
                                   for idx, batch_ids in enumerate(list_of_batch_ids)}
            
            for future in concurrent.futures.as_completed(future_to_batch_num):
                batch_num = future_to_batch_num[future]
                try:
                    batch_data = future.result()
                    if batch_data:
                        full_events.extend(batch_data)
                        print(f"    - Batch {batch_num} completed, {len(batch_data)} events added. Total collected: {len(full_events)}")
                    else:
                        print(f"    - Batch {batch_num} completed with no data or an error.")
                except Exception as exc:
                    print(f"    - Batch {batch_num} generated an exception: {exc}", file=sys.stderr)
        
        print(f"\nSuccessfully fetched full details for {len(full_events)} events in total using threads.")

        # Return all fetched full events
        return full_events

    except requests.exceptions.JSONDecodeError:
        print(f"Error: Failed to decode JSON from response. Response text: {getattr(resp, 'text', 'N/A')}", file=sys.stderr)
        return []
    except requests.exceptions.RequestException as e:
        print(f"Error fetching events: {e}", file=sys.stderr)
        return []

def main():
    print("Starting Polymarket multi-outcome market discovery...")
    # Step 1 & 2: Fetch all event IDs and then their full details
    all_detailed_events = fetch_detailed_events()
    # --- CSV Writing Section ---
    csv_file_name = 'all_polymarket_events.csv' 
    
    if not all_detailed_events:
        print("\nNo detailed events were fetched.")
        print(f"Creating empty CSV: {csv_file_name} with headers only.")
        max_outcomes_for_csv = 0
    else:
        print(f"\nWriting all {len(all_detailed_events)} fetched detailed events to {csv_file_name}...")
        market_lengths = [len(e.get('markets', [])) for e in all_detailed_events if e.get('markets') is not None]
        if market_lengths:
            max_outcomes_for_csv = max(market_lengths)
        else:
            max_outcomes_for_csv = 0
    
    csv_fieldnames = ['Event ID', 'Title', 'Number of Outcomes', 'Active', 'Start Date', 'End Date', 'Volume', 'Volume (24h)', 'Liquidity']
    for i in range(max_outcomes_for_csv):
        csv_fieldnames.append(f'Outcome {i+1} Question')
        csv_fieldnames.append(f'Outcome {i+1} Market ID')
        csv_fieldnames.append(f'Outcome {i+1} Yes Token ID')
        csv_fieldnames.append(f'Outcome {i+1} No Token ID')

    with open(csv_file_name, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_fieldnames)
        writer.writeheader()
        if all_detailed_events:
            for e in all_detailed_events:
                # Helper function to convert NaN to None
                def clean_value(value):
                    import math
                    if value is not None and isinstance(value, float) and math.isnan(value):
                        return None
                    return value
                
                row = {
                    'Event ID': clean_value(e.get('id')),
                    'Title': clean_value(e.get('title')),
                    'Number of Outcomes': clean_value(len(e.get('markets', []))),
                    'Active': clean_value(e.get('active')),
                    'Start Date': clean_value(e.get('startDate')),
                    'End Date': clean_value(e.get('endDate')),
                    'Volume': clean_value(e.get('volume')),
                    'Volume (24h)': clean_value(e.get('volume24hr')),
                    'Liquidity': clean_value(e.get('liquidity'))
                }
                markets_list = e.get('markets', [])
                for i, market in enumerate(markets_list):
                    if i < max_outcomes_for_csv: 
                        row[f'Outcome {i+1} Question'] = clean_value(market.get('question'))
                        row[f'Outcome {i+1} Market ID'] = clean_value(market.get('id'))
                        
                        # Extract CLOB token IDs if available
                        clob_token_ids_str = market.get('clobTokenIds')
                        yes_token_id, no_token_id = '', ''
                        if clob_token_ids_str and isinstance(clob_token_ids_str, str):
                            try:
                                import json
                                clob_token_ids = json.loads(clob_token_ids_str)
                                if isinstance(clob_token_ids, list) and len(clob_token_ids) >= 2:
                                    yes_token_id = clob_token_ids[0]
                                    no_token_id = clob_token_ids[1]
                            except json.JSONDecodeError:
                                # If parsing fails, IDs will remain empty strings
                                pass
                        row[f'Outcome {i+1} Yes Token ID'] = yes_token_id
                        row[f'Outcome {i+1} No Token ID'] = no_token_id
                writer.writerow(row)
    
    if all_detailed_events:
        print(f"Successfully wrote {len(all_detailed_events)} events to {csv_file_name}")
    else:
        # Message for empty CSV already printed
        pass

    # --- Console Reporting Section for Multi-Outcome Events ---
    if not all_detailed_events:
        # This case is already handled for CSV, main function would have returned if fetch_detailed_events was empty initially.
        # However, if fetch_detailed_events returned [], this part won't be reached if we add a return earlier.
        # For robustness, let's assume all_detailed_events might be an empty list here. 
        print("\nNo multi-outcome markets to report as no detailed events were available.")
    # Verify active status of events using concurrent batch processing
    verified_active_events = verify_events_with_concurrency(all_detailed_events)
    
    # Stricter filtering for truly active, tradable multi-outcome events
    print("\nApplying stricter filtering for active, tradable markets with future end dates...")
    truly_active_multi_outcome_events = []
    now = datetime.now(timezone.utc)

    for event in verified_active_events:
        active_markets = []
        for market in event.get('markets', []):
            endDate_str = market.get('endDate')
            
            # Perform all checks to ensure the market is currently tradable
            if (market.get('active') is True and
                market.get('enableOrderBook') is True and
                endDate_str):
                
                try:
                    # Parse the end date and check if it's in the future
                    if endDate_str.endswith('Z'):
                        endDate_str = endDate_str[:-1] + '+00:00'
                    endDate = datetime.fromisoformat(endDate_str)

                    if endDate > now:
                        active_markets.append(market)
                except (ValueError, TypeError):
                    # Ignore markets with invalid date formats
                    continue
        
        # Only keep events that still have more than 2 active markets after filtering
        if len(active_markets) > 2:
            filtered_event = event.copy()
            filtered_event['markets'] = active_markets
            truly_active_multi_outcome_events.append(filtered_event)

    multi_outcome_events = truly_active_multi_outcome_events
    print(f"Found {len(multi_outcome_events)} events containing truly active, tradable multi-outcome markets.")
    
    print(f"\n--- Multi-Outcome Market Report ---")
    if multi_outcome_events:
        print(f"Found {len(multi_outcome_events)} events with more than 2 outcomes (out of {len(all_detailed_events)} processed):\n")
        
        # Save multi-outcome events to a separate CSV file
        multi_outcome_csv_file = 'multi_outcome_polymarket_events.csv'
        print(f"Writing {len(multi_outcome_events)} multi-outcome events to {multi_outcome_csv_file}...")
        
        multi_outcome_csv_file = 'multi_outcome_polymarket_events.csv'
        max_multi_outcomes = max((len(e.get('markets', [])) for e in multi_outcome_events), default=0)

        # --- Calculate Priority Score and Sort ---
        print("\nCalculating priority scores and sorting events...")
        for e in multi_outcome_events:
            max_priority_score = 0
            for market in e.get('markets', []):
                # Ensure values are numeric and handle None
                liquidity = float(market.get('liquidityNum', 0) or 0)
                volume = float(market.get('volume24hr', 0) or 0)
                spread = float(market.get('spread', 0) or 0)
                
                # Priority score rewards high spread multiplied by liquidity and volume (Actionable Inefficiency).
                priority_score = (liquidity + (volume * 0.5)) * spread
                if priority_score > max_priority_score:
                    max_priority_score = priority_score
            e['priority_score'] = max_priority_score

        # Sort events by the calculated priority score in descending order
        multi_outcome_events.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        print("Sorting complete. Top 5 events by priority score:")
        for i, e in enumerate(multi_outcome_events[:5]):
            print(f"  {i+1}. Title: {e.get('title')}, Score: {e.get('priority_score'):.2f}")

        # Define CSV headers for multi-outcome events
        multi_outcome_fieldnames = [
            'Event ID', 'Title', 'Number of Outcomes', 'Active',
            'Start Date', 'End Date', 'Days to Resolution', 'Priority Score', 
            'Mutually Exclusive', 'Sum Bids', 'Arbitrage Opportunity', 
            'Annualized Return', 'Has Liquidity'
        ]

        for i in range(1, max_multi_outcomes + 1):
            multi_outcome_fieldnames.extend([
                f'Outcome {i} Question',
                f'Outcome {i} Market ID',
                f'Outcome {i} Yes Token ID',
                f'Outcome {i} No Token ID',
                f'Outcome {i} Liquidity',
                f'Outcome {i} Volume 24hr',
                f'Outcome {i} Spread'
            ])
        
        # Write multi-outcome events to CSV
        with open(multi_outcome_csv_file, 'w', newline='', encoding='utf-8') as multi_csvfile:
            multi_writer = csv.DictWriter(multi_csvfile, fieldnames=multi_outcome_fieldnames)
            multi_writer.writeheader()
            
            for e in multi_outcome_events:
                # Prepare data for mutual exclusivity detection
                markets = e.get('markets', [])
                num_outcomes = len(markets)
                bids = []
                asks = []
                liquidities = []
                
                # Extract bid/ask prices for mutual exclusivity detection
                for market in markets:
                    best_bid = market.get('bestBid')
                    best_ask = market.get('bestAsk')
                    liquidity = float(market.get('liquidityNum', 0) or 0)
                    
                    if best_bid is not None and best_ask is not None:
                        try:
                            bids.append(float(best_bid))
                            asks.append(float(best_ask))
                            liquidities.append(liquidity)
                        except (ValueError, TypeError):
                            pass
                
                # Calculate mutual exclusivity based on sum of best bids only
                sum_bids = sum(bids) if bids else 0
                
                # Determine mutual exclusivity purely based on price metrics
                # An event is mutually exclusive if:
                # 1. Sum of bids is close to 1.0 (within 0.25)
                # 2. Sum of bids is greater than 0.5 (significant market activity)
                # 3. Sum of bids is not too high (>1.5 suggests non-exclusivity)
                is_mutually_exclusive = (abs(sum_bids - 1.0) <= 0.25 and 
                                        sum_bids > 0.5 and 
                                        sum_bids < 1.5)
                
                # Calculate arbitrage opportunities
                arb_opportunity = 0
                if is_mutually_exclusive and sum_bids < 1.0:
                    # Long arbitrage: buy all outcomes
                    arb_opportunity = 1.0 - sum_bids
                
                # Check for short arbitrage opportunities (selling overpriced outcomes)
                short_arb = 0
                if sum_bids > 1.0:
                    short_arb = sum_bids - 1.0
                
                # Take the better of the two arbitrage opportunities
                arb_opportunity = max(arb_opportunity, short_arb)
                
                # Get event end date (use the earliest end date from all outcomes)
                event_end_date = None
                for market in e.get('markets', []):
                    end_date_str = market.get('endDate')
                    if end_date_str:
                        try:
                            # Parse the end date
                            if end_date_str.endswith('Z'):
                                end_date_str = end_date_str[:-1] + '+00:00'
                            market_end_date = datetime.fromisoformat(end_date_str)
                            
                            # Track the earliest end date
                            if event_end_date is None or market_end_date < event_end_date:
                                event_end_date = market_end_date
                        except:
                            continue
                
                # Calculate days to resolution
                current_date = datetime.now(timezone.utc)
                days_to_resolution = 365  # Default to 1 year if no end date
                event_end_date_str = ''
                if event_end_date:
                    days_to_resolution = max(1, (event_end_date - current_date).days)  # At least 1 day
                    event_end_date_str = event_end_date.isoformat()
                
                # Calculate annualized return (simplified)
                annualized_return = 0
                if is_mutually_exclusive and arb_opportunity > 0 and sum_bids > 0 and days_to_resolution > 0:
                    # Quick estimate of annualized return (ignoring fees for simplicity)
                    annualized_return = (arb_opportunity / sum_bids) * (365 / days_to_resolution)
                
                # Calculate normalized probabilities (forcing sum to 1.0)
                normalized_bids = [bid/sum_bids for bid in bids] if sum_bids > 0 else []
                
                # Calculate priority score based on liquidity and volume
                has_liquidity = any(liq >= MIN_LIQUIDITY for liq in liquidities)
                priority_score = 0
                
                if has_liquidity:
                    # Calculate priority score based on liquidity and volume
                    total_liquidity = sum(liquidities)
                    volume_24hr = sum([market.get('volume24hr', 0) or 0 for market in markets])
                    priority_score = (total_liquidity + (volume_24hr * 0.5))
                    
                # Boost priority for mutually exclusive events with liquidity
                if is_mutually_exclusive and has_liquidity:
                    # First boost for being mutually exclusive
                    priority_score = priority_score * 2.0 if priority_score else 2000
                    
                    # Additional boost based on annualized return for arbitrage opportunities
                    if arb_opportunity > 0 and annualized_return > 0:
                        # Higher annualized return = higher priority
                        # Cap the multiplier at 5x to avoid extreme values
                        annualized_boost = min(5.0, 1.0 + annualized_return * 3.0)
                        priority_score = priority_score * annualized_boost
                
                # Create a row for this event with default values for all fields
                row = {
                    'Event ID': e.get('id', ''),
                    'Title': e.get('title', ''),
                    'Number of Outcomes': num_outcomes,
                    'Active': e.get('active', False),
                    'Start Date': e.get('startDate', ''),
                    'End Date': event_end_date_str,  # Use our calculated end date
                    'Days to Resolution': days_to_resolution,
                    'Priority Score': priority_score,
                    'Mutually Exclusive': is_mutually_exclusive,
                    'Sum Bids': sum_bids,
                    'Arbitrage Opportunity': arb_opportunity,
                    'Annualized Return': annualized_return,
                    'Has Liquidity': has_liquidity
                }
                
                # Initialize all outcome fields with empty values to prevent NaN issues
                for i in range(1, max_multi_outcomes + 1):
                    row[f'Outcome {i} Question'] = ''
                    row[f'Outcome {i} Market ID'] = ''
                    row[f'Outcome {i} Yes Token ID'] = ''
                    row[f'Outcome {i} No Token ID'] = ''
                    row[f'Outcome {i} Liquidity'] = 0
                    row[f'Outcome {i} Volume 24hr'] = 0
                    row[f'Outcome {i} Spread'] = 0
                
                # Add each market question to the row
                markets = e.get('markets', [])
                for i, market in enumerate(markets):
                    if i < max_multi_outcomes:
                        row[f'Outcome {i+1} Question'] = market.get('question', '')
                        row[f'Outcome {i+1} Market ID'] = market.get('id', '')
                        
                        # Extract CLOB token IDs if available
                        clob_token_ids_str = market.get('clobTokenIds')
                        yes_token_id, no_token_id = '', ''
                        if clob_token_ids_str and isinstance(clob_token_ids_str, str):
                            try:
                                import json
                                clob_token_ids = json.loads(clob_token_ids_str)
                                if isinstance(clob_token_ids, list) and len(clob_token_ids) >= 2:
                                    yes_token_id = clob_token_ids[0]
                                    no_token_id = clob_token_ids[1]
                            except json.JSONDecodeError:
                                # If parsing fails, IDs will remain empty strings
                                pass
                        row[f'Outcome {i+1} Yes Token ID'] = yes_token_id
                        row[f'Outcome {i+1} No Token ID'] = no_token_id
                        row[f'Outcome {i+1} Liquidity'] = market.get('liquidityNum', 0) or 0
                        row[f'Outcome {i+1} Volume 24hr'] = market.get('volume24hr', 0) or 0
                        row[f'Outcome {i+1} Spread'] = market.get('spread', 0) or 0
                
                multi_writer.writerow(row)
        
        # Count mutually exclusive events and arbitrage opportunities
        me_count = 0
        arb_count = 0
        liquid_arb_count = 0
        
        for e in multi_outcome_events:
            markets = e.get('markets', [])
            bids = []
            liquidities = []
            
            for market in markets:
                best_bid = market.get('bestBid')
                liquidity = float(market.get('liquidityNum', 0) or 0)
                
                if best_bid is not None:
                    try:
                        bids.append(float(best_bid))
                        liquidities.append(liquidity)
                    except (ValueError, TypeError):
                        pass
            
            sum_bids = sum(bids) if bids else 0
            is_mutually_exclusive = bids and abs(sum_bids - 1.0) <= ME_PRICE_THRESHOLD
            has_liquidity = any(liq >= MIN_LIQUIDITY for liq in liquidities)
            
            if is_mutually_exclusive:
                me_count += 1
            
            # Check for arbitrage opportunities
            arb_opportunity = 0
            if sum_bids < 1.0:
                arb_opportunity = 1.0 - sum_bids
            elif sum_bids > 1.0:
                arb_opportunity = sum_bids - 1.0
                
            if arb_opportunity >= 0.02:  # 2% minimum arbitrage to count
                arb_count += 1
                if has_liquidity:
                    liquid_arb_count += 1
        
        print(f"Successfully wrote {len(multi_outcome_events)} multi-outcome events to {multi_outcome_csv_file}")
        print(f"Found {me_count} mutually exclusive events ({me_count/len(multi_outcome_events)*100:.1f}% of multi-outcome events)")
        print(f"Found {arb_count} arbitrage opportunities ({arb_count/len(multi_outcome_events)*100:.1f}% of events)")
        print(f"Found {liquid_arb_count} liquid arbitrage opportunities with min liquidity {MIN_LIQUIDITY}")
        print(f"Using bid sum threshold of ±{ME_PRICE_THRESHOLD} (sum of bids between {1.0-ME_PRICE_THRESHOLD:.2f} and {1.0+ME_PRICE_THRESHOLD:.2f})")
        
        
        # Print multi-outcome events to console
        for e in multi_outcome_events:
            # Calculate mutual exclusivity for console output
            markets = e.get('markets', [])
            bids = []
            asks = []
            liquidities = []
            
            for market in markets:
                best_bid = market.get('bestBid')
                best_ask = market.get('bestAsk')
                liquidity = float(market.get('liquidityNum', 0) or 0)
                
                if best_bid is not None and best_ask is not None:
                    try:
                        bids.append(float(best_bid))
                        asks.append(float(best_ask))
                        liquidities.append(liquidity)
                    except (ValueError, TypeError):
                        pass
            
            sum_bids = sum(bids) if bids else 0
            is_mutually_exclusive = bids and abs(sum_bids - 1.0) <= ME_PRICE_THRESHOLD
            has_liquidity = any(liq >= MIN_LIQUIDITY for liq in liquidities)
            
            # Calculate arbitrage opportunities
            arb_opportunity = 0
            arb_type = "None"
            if sum_bids < 1.0:
                arb_opportunity = 1.0 - sum_bids
                arb_type = "Long (Buy All)"
            elif sum_bids > 1.0:
                arb_opportunity = sum_bids - 1.0
                arb_type = "Short (Sell All)"
            
            print(f"- Event ID: {e.get('id')}")
            print(f"  Title: {e.get('title')}")
            print(f"  Number of Outcomes: {len(e.get('markets', []))}")
            print(f"  Active: {e.get('active')}")
            print(f"  Mutually Exclusive: {is_mutually_exclusive}")
            print(f"  Sum of Bids: {sum_bids:.3f} (Target: 1.00 ± {ME_PRICE_THRESHOLD})")
            print(f"  Arbitrage: {arb_opportunity:.3f} ({arb_type})")
            print(f"  Has Liquidity: {has_liquidity} (Min: {MIN_LIQUIDITY})")
            print(f"  Priority Score: {e.get('priority_score', 0):.1f}")
            print("  Outcomes (Markets):")
            markets_list_console = e.get('markets', [])
            for market_console in markets_list_console:
                market_id = market_console.get('id', 'N/A')
                clob_token_ids_str = market_console.get('clobTokenIds')
                yes_token, no_token = 'N/A', 'N/A'
                if clob_token_ids_str and isinstance(clob_token_ids_str, str):
                    try:
                        import json
                        clob_token_ids = json.loads(clob_token_ids_str)
                        if isinstance(clob_token_ids, list):
                            if len(clob_token_ids) >= 1:
                                yes_token = str(clob_token_ids[0])
                            if len(clob_token_ids) >= 2:
                                no_token = str(clob_token_ids[1])
                    except json.JSONDecodeError:
                        pass # Keep tokens as 'N/A'
                print(f"    - {market_console.get('question')} (Market ID: {market_id})")
                print(f"      Yes CLOB Token ID: {yes_token}")
                print(f"      No CLOB Token ID: {no_token}")
                liquidity = market_console.get('liquidityNum', 'N/A')
                volume_24hr = market_console.get('volume24hr', 'N/A')
                spread = market_console.get('spread', 'N/A')
                print(f"      Liquidity: {liquidity}")
                print(f"      Volume (24hr): {volume_24hr}")
                print(f"      Spread: {spread}")
            print("") # Print a newline after each event's details
    else:
        print(f"No multi-outcome events found among the {len(all_detailed_events)} processed events.")

if __name__ == "__main__":
    main()
