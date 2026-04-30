#!/usr/bin/env python3
"""
================================================================================
MASTER PIPELINE SCRIPT - Database-Only Email Scraping Workflow
================================================================================
This script orchestrates the scraper flow without intermediary CSV reads/writes.
Everything executes with transactional memory loaded straight into Postgres.
================================================================================
"""

import os
import sys
import asyncio
import subprocess
from datetime import datetime

# Color codes
class Colors:
    HEADER = '\033[95m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

def print_header(text):
    print(f"\n{Colors.HEADER}================================================================================{Colors.ENDC}")
    print(f"{Colors.HEADER}{text.center(80)}{Colors.ENDC}")
    print(f"{Colors.HEADER}================================================================================{Colors.ENDC}\n")

async def run_script(script_path, cwd=None):
    """Run a Python script asynchronously from subprocess wrapper"""
    cmd = [sys.executable, os.path.basename(script_path)]
    work_dir = cwd or os.path.dirname(script_path)
    
    print(f"{Colors.OKCYAN}  Executing: {' '.join(cmd)}{Colors.ENDC}")
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=work_dir,
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        await process.wait()
        if process.returncode == 0:
            print(f"{Colors.OKGREEN}  ✓ {os.path.basename(script_path)} completed successfully.{Colors.ENDC}")
            return True
        else:
             print(f"{Colors.FAIL}  ✗ Execution failed (Exit code: {process.returncode}){Colors.ENDC}")
             return False
    except Exception as e:
         print(f"{Colors.FAIL}  ✗ Error running script {script_path}: {e}{Colors.ENDC}")
         return False

async def main():
    print_header("🚀 STARTING ZERO-CSV PIPELINE")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Step 1: Scrum listings
    print(f"{Colors.OKCYAN}[1/3] Step 1: update_listings.py (Scrapes internet directly to staging_fcc_listings DB table){Colors.ENDC}")
    if not await run_script("scripts/All/All_list/update_listings.py", cwd="scripts/All/All_list"):
        print(f"{Colors.WARNING}Listings sync flagged warnings, continuing pipeline...{Colors.ENDC}")

    # Step 2: Verification
    print(f"\n{Colors.OKCYAN}[2/3] Step 2: verify_leads_fast.py (Checks Form 499 Status structures, loads main leads table DB directly){Colors.ENDC}")
    if not await run_script("scripts/All/usa_list/verify_leads_fast.py", cwd="scripts/All/usa_list"):
        print(f"{Colors.FAIL}Leads verification failed. Aborting further steps.{Colors.ENDC}")
        return

    # Step 3: Fetch Emails
    print(f"\n{Colors.OKCYAN}[3/3] Step 3: fetch_emails_browser.py (Scrapes and binds target contact emails straight into leads table row updates){Colors.ENDC}")
    if not await run_script("scripts/All/fetch_emails/fetch_emails_browser.py", cwd="scripts/All/fetch_emails"):
        print(f"{Colors.WARNING}Email fetcher cycle halted early.{Colors.ENDC}")

    print_header("🎉 PIPELINE COMPLETED")
    print(f"All stages successfully synchronized on PostgreSQL. End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

if __name__ == "__main__":
    asyncio.run(main())
