#!/usr/bin/env python3
import requests
import sys
import time
import csv
import concurrent.futures
from datetime import datetime, timezone

# Official Polymarket API endpoint for events
API_URL = "https://gamma-api.polymarket.com/events?closed=false"

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
            
            # TEMPORARY: Limit to 5 pages for testing
            if page_num >= 5:
                print("  - Reached 5 pages limit (TEMPORARY LIMIT FOR TESTING)")
                break
                
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
    
    csv_fieldnames = ['Event ID', 'Title', 'Number of Outcomes', 'Active', 'Start Date', 'End Date']
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
                row = {
                    'Event ID': e.get('id'),
                    'Title': e.get('title'),
                    'Number of Outcomes': len(e.get('markets', [])),
                    'Active': e.get('active'),
                    'Start Date': e.get('startDate'),
                    'End Date': e.get('endDate')
                }
                markets_list = e.get('markets', [])
                for i, market in enumerate(markets_list):
                    if i < max_outcomes_for_csv: 
                        row[f'Outcome {i+1} Question'] = market.get('question')
                        row[f'Outcome {i+1} Market ID'] = market.get('id')
                        
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
        
        # Find the maximum number of outcomes for header generation
        max_multi_outcomes = max(len(e.get('markets', [])) for e in multi_outcome_events)
        
        # Create fieldnames for multi-outcome CSV
        multi_outcome_fieldnames = ['Event ID', 'Title', 'Number of Outcomes', 'Active', 'Start Date', 'End Date']
        for i in range(max_multi_outcomes):
            multi_outcome_fieldnames.extend([
                f'Outcome {i+1} Question',
                f'Outcome {i+1} Market ID',
                f'Outcome {i+1} Yes Token ID',
                f'Outcome {i+1} No Token ID',
                f'Outcome {i+1} Liquidity',
                f'Outcome {i+1} Volume 24hr',
                f'Outcome {i+1} Spread'
            ])
        
        # Write multi-outcome events to CSV
        with open(multi_outcome_csv_file, 'w', newline='', encoding='utf-8') as multi_csvfile:
            multi_writer = csv.DictWriter(multi_csvfile, fieldnames=multi_outcome_fieldnames)
            multi_writer.writeheader()
            
            for e in multi_outcome_events:
                row = {
                    'Event ID': e.get('id'),
                    'Title': e.get('title'),
                    'Number of Outcomes': len(e.get('markets', [])),
                    'Active': e.get('active'),
                    'Start Date': e.get('startDate'),
                    'End Date': e.get('endDate')
                }
                
                # Add each market question to the row
                markets = e.get('markets', [])
                for i, market in enumerate(markets):
                    if i < max_multi_outcomes:
                        row[f'Outcome {i+1} Question'] = market.get('question')
                        row[f'Outcome {i+1} Market ID'] = market.get('id')
                        
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
                        row[f'Outcome {i+1} Liquidity'] = market.get('liquidityNum')
                        row[f'Outcome {i+1} Volume 24hr'] = market.get('volume24hr')
                        row[f'Outcome {i+1} Spread'] = market.get('spread')
                
                multi_writer.writerow(row)
        
        print(f"Successfully wrote {len(multi_outcome_events)} multi-outcome events to {multi_outcome_csv_file}")
        
        # Print multi-outcome events to console
        for e in multi_outcome_events:
            print(f"- Event ID: {e.get('id')}")
            print(f"  Title: {e.get('title')}")
            print(f"  Number of Outcomes: {len(e.get('markets', []))}")
            print(f"  Active: {e.get('active')}")
            print(f"  Start Date: {e.get('startDate')}")
            print(f"  End Date: {e.get('endDate')}")
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
