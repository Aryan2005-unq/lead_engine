import csv
import os
import glob
from datetime import datetime

# --------------------------------------------------
# 1. Find all email CSV files
# --------------------------------------------------
# Look for multiple patterns to find email CSV files
# Files can be in current directory or in parent directories
patterns = [
    "crwn_*_cleaned.csv",  # Original pattern (in Merging dir)
    os.path.join("..", "All", "fetch_emails", "usa_email.csv"),
    os.path.join("..", "All", "fetch_emails", "non_usa_email.csv"),
    os.path.join("..", "linkdin", "linkedin_leads_with_emails*.csv"),
    # Also check if files are in current directory (if copied)
    "usa_email.csv",
    "non_usa_email.csv",
    "linkedin_leads_with_emails*.csv"
]

csv_files = []
for pattern in patterns:
    found = glob.glob(pattern)
    csv_files.extend(found)

# Remove duplicates and sort
csv_files = sorted(list(set(csv_files)))

# Filter to only existing files and get absolute paths
csv_files = [os.path.abspath(f) for f in csv_files if os.path.exists(f)]

if not csv_files:
    print("⚠️  No CSV files found matching any email file patterns:")
    for pattern in patterns:
        print(f"  - {pattern}")
    print("\nSearched in:")
    print(f"  - Current directory: {os.getcwd()}")
    print(f"  - Script directory: {os.path.dirname(os.path.abspath(__file__))}")
    print("\n⚠️  Creating empty merged file with headers...")
    # Create empty merged file with headers
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"all_results_merged_{timestamp}.csv"
    with open(output_filename, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["company_name", "email"])
        writer.writeheader()
    print(f"✓ Created empty merged file: {output_filename}")
    print("=" * 80 + "\n")
    exit(0)  # Exit successfully with empty file

print(f"Found {len(csv_files)} CSV files to merge:")
for f in csv_files:
    print(f"  - {f}")

# --------------------------------------------------
# 2. Column aliases
# --------------------------------------------------
COMPANY_COLUMNS = [
    "company_name",
    "company name",
    "name",
    "business_name",
    "Business Name",
    "Business name",
    "business Name"
]

EMAIL_COLUMNS = [
    "email",
    "contact_email",
    "company email",
    "company_email"
]

# --------------------------------------------------
# 3. Create output file
# --------------------------------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"all_results_merged_{timestamp}.csv"

total_rows = 0

with open(output_filename, "w", newline="", encoding="utf-8") as outfile:
    writer = csv.DictWriter(outfile, fieldnames=["company_name", "email"])
    writer.writeheader()
    outfile.flush()  # Ensure header is written immediately

    # --------------------------------------------------
    # 4. Merge files
    # --------------------------------------------------
    for csv_file in csv_files:
        print(f"\nProcessing: {csv_file}")
        file_rows = 0

        with open(csv_file, "r", encoding="utf-8") as infile:
            reader = csv.DictReader(infile)

            for row in reader:
                # Extract company name (case-insensitive matching)
                company_name = ""
                row_lower = {k.lower(): v for k, v in row.items()}
                for col in COMPANY_COLUMNS:
                    col_lower = col.lower()
                    if col_lower in row_lower:
                        value = str(row_lower[col_lower]).strip()
                        if value:
                            company_name = value
                            break

                # Extract email (case-insensitive matching)
                email = ""
                for col in EMAIL_COLUMNS:
                    col_lower = col.lower()
                    if col_lower in row_lower:
                        value = str(row_lower[col_lower]).strip()
                        if value:
                            email = value
                            break

                # Write row only if data exists
                if company_name or email:
                    writer.writerow({
                        "company_name": company_name,
                        "email": email
                    })
                    file_rows += 1
                    total_rows += 1
                    
                    # Flush every 100 rows to ensure data is written
                    if total_rows % 100 == 0:
                        outfile.flush()

        # Flush after each file
        outfile.flush()
        print(f"  ✓ Added {file_rows} rows from {os.path.basename(csv_file)}")
        print(f"  [💾] Progress saved: {total_rows:,} total rows written to {output_filename}")

# --------------------------------------------------
# 5. Final summary
# --------------------------------------------------
print("\n" + "=" * 80)
print("MERGED CSV CREATED SUCCESSFULLY")
print("=" * 80)
print(f"Output file   : {output_filename}")
print(f"Total rows    : {total_rows:,}")
print("Total columns : 2 (company_name, email)")
print("=" * 80 + "\n")
