#!/usr/bin/env python3
"""
Fix company names in the merged CSV by matching emails back to source files.

USAGE:
    # Fix a specific merged CSV file (auto-detects output name)
    python fix_company_names.py all_results_merged_20260109_004903.csv
    
    # Specify custom input and output files
    python fix_company_names.py input.csv output.csv
    
    # Use default paths (looks for latest merged file in current directory)
    python fix_company_names.py

WHEN TO RUN:
    - After running all_merged_csv.py and noticing empty company_name values
    - When the merged CSV has emails but missing company/business names
    - To fix case-sensitivity issues in column matching
"""

import csv
import os
import sys
import glob
import argparse

# Column aliases for case-insensitive matching
COMPANY_COLUMNS = [
    "company_name", "company name", "name", "business_name", 
    "Business Name", "Business name", "business Name"
]

EMAIL_COLUMNS = [
    "email", "contact_email", "company email", "company_email"
]

def normalize_email(email):
    """Normalize email to lowercase for matching."""
    if not email:
        return ""
    return email.strip().lower()

def find_column_case_insensitive(row, candidates):
    """Find a column value using case-insensitive matching."""
    row_lower = {k.lower(): v for k, v in row.items()}
    for candidate in candidates:
        candidate_lower = candidate.lower()
        if candidate_lower in row_lower:
            value = row_lower[candidate_lower]
            if value and str(value).strip():
                return str(value).strip()
    return ""

def build_email_to_company_map(csv_file):
    """Build a mapping from email to company name from a CSV file."""
    email_to_company = {}
    
    if not os.path.exists(csv_file):
        print(f"[WARNING] File not found: {csv_file}")
        return email_to_company
    
    print(f"Reading: {csv_file}")
    with open(csv_file, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        rows_processed = 0
        
        for row in reader:
            # Find email (case-insensitive)
            email = find_column_case_insensitive(row, EMAIL_COLUMNS)
            if not email:
                continue
            
            email_normalized = normalize_email(email)
            
            # Find company name (case-insensitive)
            company_name = find_column_case_insensitive(row, COMPANY_COLUMNS)
            
            # Store mapping (keep first non-empty company name found)
            if email_normalized and company_name:
                if email_normalized not in email_to_company or not email_to_company[email_normalized]:
                    email_to_company[email_normalized] = company_name
                rows_processed += 1
    
    print(f"  [OK] Found {len(email_to_company):,} email-company mappings")
    return email_to_company

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Fix company names in merged CSV by matching emails to source files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python fix_company_names.py merged_file.csv
  python fix_company_names.py input.csv output.csv
  python fix_company_names.py  # Auto-finds latest merged file
        """
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        help="Input merged CSV file to fix (default: auto-find latest all_results_merged_*.csv)"
    )
    parser.add_argument(
        "output_file",
        nargs="?",
        help="Output CSV file (default: input_file with '_fixed' suffix)"
    )
    parser.add_argument(
        "--usa-email",
        help="Path to USA email CSV file (default: ../All/fetch_emails/usa_email.csv)"
    )
    parser.add_argument(
        "--non-usa-email",
        help="Path to non-USA email CSV file (default: ../All/fetch_emails/non_usa_email.csv)"
    )
    
    args = parser.parse_args()
    
    # Set up paths
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    # Determine input file
    if args.input_file:
        MERGED_FILE = os.path.abspath(args.input_file)
    else:
        # Auto-find latest merged file
        merged_files = glob.glob(os.path.join(SCRIPT_DIR, "all_results_merged_*.csv"))
        if not merged_files:
            print("[ERROR] No merged CSV files found. Please specify input file.")
            print("Usage: python fix_company_names.py <input_file.csv> [output_file.csv]")
            sys.exit(1)
        MERGED_FILE = max(merged_files, key=os.path.getmtime)
        print(f"[INFO] Auto-selected latest file: {os.path.basename(MERGED_FILE)}")
    
    # Determine output file
    if args.output_file:
        OUTPUT_FILE = os.path.abspath(args.output_file)
    else:
        # Create output filename with _fixed suffix
        base, ext = os.path.splitext(MERGED_FILE)
        OUTPUT_FILE = f"{base}_fixed{ext}"
    
    # Set source email files
    USA_EMAIL_FILE = args.usa_email or os.path.join(SCRIPT_DIR, "..", "All", "fetch_emails", "usa_email.csv")
    NON_USA_EMAIL_FILE = args.non_usa_email or os.path.join(SCRIPT_DIR, "..", "All", "fetch_emails", "non_usa_email.csv")
    
    print("=" * 80)
    print("FIXING COMPANY NAMES IN MERGED CSV")
    print("=" * 80)
    print(f"Input file  : {MERGED_FILE}")
    print(f"Output file : {OUTPUT_FILE}")
    print()
    
    # Build email-to-company mappings from source files
    print("Step 1: Building email-to-company mappings from source files...")
    email_to_company = {}
    
    # Add mappings from USA email file
    usa_map = build_email_to_company_map(USA_EMAIL_FILE)
    email_to_company.update(usa_map)
    
    # Add mappings from non-USA email file
    non_usa_map = build_email_to_company_map(NON_USA_EMAIL_FILE)
    # Update only if company name is missing or empty
    for email, company in non_usa_map.items():
        if email not in email_to_company or not email_to_company[email]:
            email_to_company[email] = company
    
    print(f"\nTotal unique email-company mappings: {len(email_to_company):,}")
    print()
    
    # Read merged file and fix company names
    print("Step 2: Fixing merged CSV file...")
    if not os.path.exists(MERGED_FILE):
        print(f"[ERROR] Merged file not found: {MERGED_FILE}")
        sys.exit(1)
    
    fixed_count = 0
    already_has_name = 0
    no_match_found = 0
    total_rows = 0
    
    with open(MERGED_FILE, "r", encoding="utf-8", errors="replace") as infile:
        reader = csv.DictReader(infile)
        
        # Ensure we have the expected columns
        if "company_name" not in reader.fieldnames or "email" not in reader.fieldnames:
            print(f"[ERROR] Expected columns 'company_name' and 'email' not found.")
            print(f"Found columns: {reader.fieldnames}")
            sys.exit(1)
        
        rows = []
        for row in reader:
            total_rows += 1
            email = normalize_email(row.get("email", ""))
            company_name = row.get("company_name", "").strip()
            
            # If company name is missing, try to find it from mapping
            if not company_name and email:
                if email in email_to_company:
                    company_name = email_to_company[email]
                    fixed_count += 1
                else:
                    no_match_found += 1
            elif company_name:
                already_has_name += 1
            
            rows.append({
                "company_name": company_name,
                "email": row.get("email", "")
            })
    
    # Write fixed CSV
    print(f"\nStep 3: Writing fixed CSV to: {OUTPUT_FILE}")
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["company_name", "email"])
        writer.writeheader()
        writer.writerows(rows)
    
    # Summary
    print("\n" + "=" * 80)
    print("FIXING COMPLETE")
    print("=" * 80)
    print(f"Total rows processed    : {total_rows:,}")
    print(f"Already had company name: {already_has_name:,}")
    print(f"Fixed (added name)      : {fixed_count:,}")
    print(f"No match found          : {no_match_found:,}")
    print(f"\nOutput file: {OUTPUT_FILE}")
    print("=" * 80)
    
    # Ask if user wants to replace original
    if not args.output_file and fixed_count > 0:
        print(f"\n[INFO] To replace the original file, run:")
        print(f"  Copy-Item \"{OUTPUT_FILE}\" \"{MERGED_FILE}\" -Force")

if __name__ == "__main__":
    main()

