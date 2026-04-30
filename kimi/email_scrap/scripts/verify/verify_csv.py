import csv
import subprocess
import smtplib
import random
import sys
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Fix Windows console encoding for emoji support
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except (AttributeError, ValueError):
        pass

# ================= CONFIG =================
INPUT_CSV = "../Merging/all_results_merged_20260109_004903.csv"
MAX_WORKERS = 10  # Parallel threads for speedup
# ==========================================


def get_mx_record(domain):
    """Retrieves the MX record solver solver using Windows nslookup natively."""
    try:
        sub = subprocess.run(["nslookup", "-type=mx", domain], capture_output=True, text=True, timeout=5)
        for line in sub.stdout.splitlines():
            if "mail exchanger" in line or "exchanger =" in line:
                # Matches: domain.com  mail exchanger = 10 mx1.server.com
                mx_server = line.split("exchanger =")[-1].replace("\t", " ").strip()
                if " " in mx_server:
                    mx_server = mx_server.split(" ")[-1]
                return mx_server
    except:
        pass
    return None


def verify_email_smtp(email):
    """Connects to MX Server directly and query validation codes natively."""
    if not email or "@" not in email:
        return "INVALID_FORMAT"

    domain = email.split("@")[-1].strip()
    mx_server = get_mx_record(domain)
    
    if not mx_server:
        return "NO_MX_DOMAIN"

    try:
        # 🛡️ Direct SMTP handshake check layout
        s = smtplib.SMTP(mx_server, 25, timeout=10)
        s.helo("gmail.com") 
        s.mail("verify@gmail.com") 
        code, msg = s.rcpt(email)
        s.quit()
        
        if code == 250:
            return "VALID"
        elif code >= 500:
            return "INVALID"
        else:
            return f"UNKNOWN_{code}"

    except Exception as e:
        # Usually connection failure blocks or timeouts
        return f"ERROR ({str(e).strip()})"


def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"verified_email_list_{timestamp}.csv"

    stats = {"valid": 0, "checked": 0, "invalid": 0, "error": 0}

    print(f"\n[INPUT] Input file: {INPUT_CSV}")
    print(f"[OUTPUT] Output file: {output_file}\n")

    # Read rows first to parse list sizes
    if not os.path.exists(INPUT_CSV):
        print(f"❌ ERROR: Input file not found at {INPUT_CSV}")
        return

    with open(INPUT_CSV, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = [row for row in reader if row.get("email")]

    print(f"[TOTAL] Loaded {len(rows)} emails to analyze via Direct SMTP verification.\n")

    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["company_name", "email"])
        writer.writeheader()

        total = len(rows)
        
        # 🚀 Parallel Execution on loaded threads
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_row = {
                executor.submit(verify_email_smtp, row.get("email").strip()): row 
                for row in rows
            }

            print("[STARTING] Launching verification nodes. Output yields live in terminal...")
            for future in as_completed(future_to_row):
                stats["checked"] += 1
                row = future_to_row[future]
                email = row.get("email", "").strip()
                company = row.get("company_name", "").strip()

                try:
                    result = future.result()
                    
                    if result == "VALID":
                        stats["valid"] += 1
                        print(f"[VALID] ({stats['checked']}/{total}) {email} → Saved")
                        writer.writerow({
                            "company_name": company,
                            "email": email
                        })
                        outfile.flush()  # LIVE SAVE
                    elif result == "INVALID" or "INVALID_FORMAT" in result:
                        stats["invalid"] += 1
                        print(f"[INVALID] ({stats['checked']}/{total}) {email} → Skipping")
                    else:
                        stats["error"] += 1
                        # Includes unknown SMTP codes or blocks offsets
                        print(f"[SKIP/LIMIT] ({stats['checked']}/{total}) {email} → {result}")

                except Exception as exc:
                     stats["error"] += 1
                     print(f"[ERROR] ({stats['checked']}/{total}) {email} → {exc}")

    print("\n[COMPLETE] AUTOMATED VERIFICATION COMPLETED")
    print(f"Total checked : {stats['checked']}")
    print(f"Valid emails  : {stats['valid']}")
    print(f"Invalid/Skip  : {stats['invalid']}")
    print(f"Failed/Slow   : {stats['error']}")
    print(f"Saved file   : {output_file}")
    
    # 🗄️ Save validated emails to PostgreSQL
    insert_to_postgres(output_file)


def insert_to_postgres(filepath):
    """Inserts verified emails back to PostgreSQL"""
    import psycopg2
    from dotenv import load_dotenv
    
    # Load .env from root workspace
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
    load_dotenv() # Local fallback

    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "crwm_db")
    DB_USER = os.getenv("DB_USER", "crwm_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "crwm_password")

    print(f"\n[DB] Syncing verified leads to PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")

    try:
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()
        
        count = 0
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                email = row.get("email", "").strip()
                company = row.get("company_name", "").strip()
                if email:
                    try:
                        cursor.execute(
                            "INSERT INTO verified_emails (company_name, email) VALUES (%s, %s) ON CONFLICT (email) DO NOTHING",
                            (company, email)
                        )
                        count += 1
                    except Exception:
                        pass
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[DB] Successfully synced rows: {count}")
    except Exception as e:
        print(f"[DB] PostgreSQL Sync Error: {e}")


if __name__ == "__main__":
    main()

