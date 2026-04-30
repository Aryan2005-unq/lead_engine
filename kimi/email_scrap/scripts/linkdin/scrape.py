from apify_client import ApifyClient
import json
import csv
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


# List of API keys - Load from environment variables
# Set these in your .env file or environment
api_keys = [
    os.getenv("APIFY_API_KEY_1", ""),
    os.getenv("APIFY_API_KEY_2", ""),
    os.getenv("APIFY_API_KEY_3", ""),
    os.getenv("APIFY_API_KEY_4", ""),
    os.getenv("APIFY_API_KEY_5", "")
]
# Filter out empty keys
api_keys = [key for key in api_keys if key]

# List of keywords to search
keywords = [
    # Category 1: Core Wholesale & Termination (The "Carriers")
    "cli routes provider",
    "ncli routes wholesale",
    "sip trunking wholesale",
    "direct inward dialing wholesale",
    "voip origination provider",
    "international voice termination",
    "voip aggregator",
    "tier 1 voice carrier",
    "voip traffic exchange",
    "wholesale sip termination rates",
    # Category 2: High-Volume & Specialized Traffic (The "Dialers")
    "dialer voip termination",
    "short duration termination",
    "call center routes usa",
    "robocall termination",
    "high cps voip provider",
    "wholesale cc traffic",
    "predictive dialer voip",
    "flat rate voip termination",
    "conversational traffic routes",
    "blasting routes voip",
    # Category 3: Infrastructure & Tech Stack (The "Builders")
    "class 4 softswitch vendor",
    "class 5 softswitch license",
    "voip billing software",
    "session border controller vendor",
    "asterisk solution provider",
    "freeswitch consultant",
    "voip hosting services",
    "mvno platform provider",
    "white label voip platform",
    "webrtc gateway provider",
    # Category 4: SMS & Verification Niche (The "OTP/verify" Market)
    "wholesale a2p sms",
    "bulk sms gateway",
    "sim hosting service",
    "virtual mobile number provider",
    "otp sms service",
    "gsm gateway hardware",
    "sms termination rates",
    "smpp provider",
    "did numbers for verification",
    "wholesale sms routes",
    # Category 5: Business & Reselling (The "Middlemen")
    "voip reseller program white label",
    "become a voip distributor",
    "voip franchise opportunity",
    "telecom consultancy services",
    "hosted pbx reseller",
    "unified communications as a service wholesale",
    "managed voip services",
    "voip startup kit",
    "interconnect billing system",
    "telecom regulatory compliance services"
]

# Split keywords into 5 groups (10 keywords per API key)
keywords_per_api = 10
keyword_groups = [keywords[i:i + keywords_per_api] for i in range(0, len(keywords), keywords_per_api)]

# Ensure we have exactly 5 groups (pad if needed)
while len(keyword_groups) < len(api_keys):
    keyword_groups.append([])

# Function to process one API key
def process_api_key(api_idx, api_key, api_keywords, timestamp):
    """Process all keywords for a single API key"""
    print(f"\n{'='*80}")
    print(f"[THREAD] API KEY {api_idx}/{len(api_keys)} - Starting")
    print(f"{'='*80}\n")
    
    # Initialize client for this API key
    client = ApifyClient(api_key)
    
    if not api_keywords:
        print(f"[API {api_idx}] No keywords assigned. Skipping...")
        return api_idx, 0, []
    
    # Initialize files for this API key
    csv_filename = f"apify_results_api{api_idx}_{timestamp}.csv"
    json_filename = f"apify_results_api{api_idx}_{timestamp}.json"
    dataset_info_filename = f"apify_datasets_api{api_idx}_{timestamp}.json"
    
    all_keys = set()  # Track all field names for this API key
    csv_file_exists = os.path.exists(csv_filename)
    all_results = []  # Results for this API key
    dataset_ids = []  # Dataset IDs for this API key
    
    # Process each keyword for this API key
    for idx, keyword in enumerate(api_keywords, 1):
        print(f"\n[API {api_idx}] [{idx}/{len(api_keywords)}] Processing keyword: {keyword}")
        
        # Prepare the Actor input for this keyword
        run_input = {
            "action": "get-companies",
            "keywords": [keyword],  # Single keyword per request
            "isUrl": False,
            "isName": False,
            "limit": 1500,  # Maximum allowed per request
            "location": [],  # Empty array (required by API)
        }
        
        try:
            # Run the Actor and wait for it to finish
            run = client.actor("od6RadQV98FOARtrp").call(run_input=run_input)
            dataset_id = run["defaultDatasetId"]
            dataset_ids.append({
                "keyword": keyword,
                "dataset_id": dataset_id,
                "run_id": run["id"]
            })
            
            # Fetch results from the run's dataset
            keyword_results = []
            for item in client.dataset(dataset_id).iterate_items():
                keyword_results.append(item)
                all_results.append(item)
                
                # Update keys set for CSV header
                if isinstance(item, dict):
                    all_keys.update(item.keys())
            
            # Immediately append results to CSV after each keyword
            if keyword_results:
                # Update keys from current results
                for item in keyword_results:
                    if isinstance(item, dict):
                        all_keys.update(item.keys())
                
                # If file exists, check for new keys by reading existing header
                has_new_keys = False
                if csv_file_exists:
                    with open(csv_filename, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        existing_keys = set(reader.fieldnames or [])
                        has_new_keys = len(all_keys - existing_keys) > 0
                
                # If file exists and we have new keys, we need to rewrite with new header
                if csv_file_exists and has_new_keys:
                    # Read existing data
                    existing_rows = []
                    with open(csv_filename, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        existing_rows = list(reader)
                    
                    # Rewrite with updated header
                    with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
                        writer.writeheader()
                        
                        # Write existing rows
                        for row in existing_rows:
                            for key in sorted(all_keys):
                                if key not in row:
                                    row[key] = ''
                            writer.writerow(row)
                        
                        # Append new results
                        for item in keyword_results:
                            if isinstance(item, dict):
                                row = {}
                                for key in sorted(all_keys):
                                    value = item.get(key, '')
                                    if isinstance(value, (dict, list)):
                                        row[key] = json.dumps(value)
                                    else:
                                        row[key] = value
                                writer.writerow(row)
                else:
                    # Simple append (new file or no new keys)
                    file_mode = 'a' if csv_file_exists else 'w'
                    with open(csv_filename, file_mode, newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
                        
                        if not csv_file_exists:
                            writer.writeheader()
                            csv_file_exists = True
                        
                        # Append new results
                        for item in keyword_results:
                            if isinstance(item, dict):
                                row = {}
                                for key in sorted(all_keys):
                                    value = item.get(key, '')
                                    if isinstance(value, (dict, list)):
                                        row[key] = json.dumps(value)
                                    else:
                                        row[key] = value
                                writer.writerow(row)
                
                # Also update JSON file progressively after each keyword
                with open(json_filename, 'w', encoding='utf-8') as f:
                    json.dump(all_results, f, indent=2, ensure_ascii=False)
                
                print(f"  ✓ Found {len(keyword_results)} results for '{keyword}'")
                print(f"    Dataset ID: {dataset_id}")
                print(f"    ✓ Appended to CSV: {csv_filename}")
                print(f"    ✓ Updated JSON: {json_filename} (Total: {len(all_results)} results)")
            else:
                print(f"  ✓ No results found for '{keyword}'")
                print(f"    Dataset ID: {dataset_id}")
            
        except Exception as e:
            print(f"  ✗ Error processing '{keyword}': {e}")
            continue
    
    # Save final JSON file and dataset info for this API key
    print(f"\n{'='*80}")
    print(f"[THREAD] API KEY {api_idx} SUMMARY")
    print(f"{'='*80}")
    print(f"Total results collected: {len(all_results)}")
    print(f"{'='*80}\n")
    
    if all_results:
        # Final save to JSON (already saved progressively, this ensures final state)
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"[API {api_idx}] ✓ Final results confirmed in JSON: {json_filename} ({len(all_results)} total results)")
        print(f"[API {api_idx}] ✓ Progressive CSV file: {csv_filename} ({len(all_results)} total results)")
        
        # Save dataset IDs for reference
        if dataset_ids:
            with open(dataset_info_filename, 'w', encoding='utf-8') as f:
                json.dump(dataset_ids, f, indent=2, ensure_ascii=False)
            print(f"[API {api_idx}] ✓ Dataset IDs saved to: {dataset_info_filename}")
        
        print(f"\n[API {api_idx}] Note: Results are also stored in Apify datasets.")
        print(f"[API {api_idx}]       Access them at: https://console.apify.com/storage/datasets")
        if dataset_ids:
            print(f"[API {api_idx}]       Total datasets created: {len(dataset_ids)}")
    else:
        print(f"[API {api_idx}] No results to save.")
    
    return api_idx, len(all_results), dataset_ids

# Process all API keys in parallel
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"\n{'='*80}")
print(f"STARTING PARALLEL PROCESSING - {len(api_keys)} API KEYS")
print(f"{'='*80}\n")

# Check if any API keys are configured
if not api_keys or len(api_keys) == 0:
    print("⚠ WARNING: No API keys configured!")
    print("Please set APIFY_API_KEY_1, APIFY_API_KEY_2, etc. in your environment variables.")
    print("Skipping LinkedIn scraping...")
    print(f"{'='*80}\n")
    exit(1)  # Exit with error code so pipeline knows scraping was skipped

# Use ThreadPoolExecutor to run all API keys in parallel
with ThreadPoolExecutor(max_workers=len(api_keys)) as executor:
    # Submit all tasks
    futures = []
    for api_idx, api_key in enumerate(api_keys, 1):
        api_keywords = keyword_groups[api_idx - 1] if api_idx <= len(keyword_groups) else []
        future = executor.submit(process_api_key, api_idx, api_key, api_keywords, timestamp)
        futures.append(future)
    
    # Wait for all tasks to complete and collect results
    results = []
    for future in as_completed(futures):
        try:
            api_idx, result_count, dataset_ids = future.result()
            results.append((api_idx, result_count, dataset_ids))
            print(f"\n[COMPLETED] API {api_idx} finished with {result_count} results")
        except Exception as e:
            print(f"\n[ERROR] API key processing failed: {e}")

print(f"\n{'='*80}")
print(f"ALL API KEYS COMPLETED")
print(f"{'='*80}\n")

# Final summary across all API keys
print(f"\n{'='*80}")
print(f"FINAL SUMMARY - ALL API KEYS")
print(f"{'='*80}")
print(f"Total API keys processed: {len(api_keys)}")
total_results = sum(count for _, count, _ in results)
print(f"Total results across all API keys: {total_results}")
print(f"\nResults files created:")
for api_idx in range(1, len(api_keys) + 1):
    csv_file = f"apify_results_api{api_idx}_{timestamp}.csv"
    json_file = f"apify_results_api{api_idx}_{timestamp}.json"
    if os.path.exists(csv_file) or os.path.exists(json_file):
        result_count = next((count for idx, count, _ in results if idx == api_idx), 0)
        print(f"  - API {api_idx}: {csv_file}, {json_file} ({result_count} results)")
print(f"{'='*80}\n")