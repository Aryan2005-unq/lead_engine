import csv
import os
import glob
from datetime import datetime

# Find all CSV files matching the pattern
csv_files = sorted(glob.glob("apify_results_api*_*.csv"))

if not csv_files:
    print("No CSV files found matching pattern 'apify_results_api*_*.csv'")
    exit(1)

print(f"Found {len(csv_files)} CSV files to merge:")
for f in csv_files:
    print(f"  - {f}")

# Collect all unique column names from all files
all_columns = set()
file_columns = {}

# First pass: collect all column names
for csv_file in csv_files:
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames
        if columns:
            all_columns.update(columns)
            file_columns[csv_file] = columns

# Sort columns for consistent output
sorted_columns = sorted(all_columns)

print(f"\nTotal unique columns found: {len(sorted_columns)}")
print(f"Columns: {', '.join(sorted_columns[:10])}..." if len(sorted_columns) > 10 else f"Columns: {', '.join(sorted_columns)}")

# Create merged output file
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_filename = f"apify_results_merged_{timestamp}.csv"

total_rows = 0

# Merge all CSV files with incremental saving
with open(output_filename, 'w', newline='', encoding='utf-8') as outfile:
    writer = csv.DictWriter(outfile, fieldnames=sorted_columns)
    writer.writeheader()
    outfile.flush()  # Ensure header is written immediately
    
    for csv_file in csv_files:
        print(f"\nProcessing: {csv_file}")
        file_rows = 0
        
        with open(csv_file, 'r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            
            for row in reader:
                # Ensure all columns are present (fill missing with empty string)
                complete_row = {}
                for col in sorted_columns:
                    complete_row[col] = row.get(col, '')
                writer.writerow(complete_row)
                file_rows += 1
                total_rows += 1
                
                # Flush every 100 rows to ensure data is written
                if total_rows % 100 == 0:
                    outfile.flush()
        
        # Flush after each file
        outfile.flush()
        print(f"  ✓ Added {file_rows} rows from {os.path.basename(csv_file)}")
        print(f"  [💾] Progress saved: {total_rows:,} total rows written to {output_filename}")

print(f"\n{'='*80}")
print(f"MERGED CSV CREATED SUCCESSFULLY")
print(f"{'='*80}")
print(f"Output file: {output_filename}")
print(f"Total rows: {total_rows:,}")
print(f"Total columns: {len(sorted_columns)}")
print(f"{'='*80}\n")





