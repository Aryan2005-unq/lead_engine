#!/usr/bin/env python3
"""
================================================================================
[TEST] SINGLE-PAGE STREAMING ASYNC PIPELINE
================================================================================
This script orchestrates the scraper flow so that it collects ONE page of leads
verifies, fetches emails, and loads into database. Bypasses further pages.
================================================================================
"""

import os
import sys
import asyncio
import time
import re
import random
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup

# Appending workspace folder to load database
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from app.database import get_db_connection

# --- CONFIGURATION ---
FCC_URL = "https://fccprod.servicenowservices.com/rmd?id=rmd_listings"
FCC_FORM_499_URL = "https://apps.fcc.gov/cgb/form499/499results.cfm"

TIMEOUT_MS = 60000 
MAX_CONCURRENT_TASKS = 30  
MAX_CONCURRENT_BROWSERS = 10 

# --- BROWSER IDENTITIES ---
BROWSER_PROFILES = [
    {"impersonate": "chrome120", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
    {"impersonate": "chrome119", "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"},
    {"impersonate": "safari15_5", "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1"},
    {"impersonate": "edge101",   "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Safari/537.36 Edg/101.0.1210.53"}
]

# --- 1. VERIFICATION MODULE ---
async def check_fcc_form499(frn, legal_name=''):
    params = {'FilerID': '', 'frn': frn, 'operational': '', 'comm_type': 'Any Type', 'LegalName': '', 'state': 'Any State', 'R1': 'and', 'XML': 'FALSE'}
    link = f"https://apps.fcc.gov/cgb/form499/499results.cfm?frn={frn}"
    result = {'status': 'Unknown', 'filer_id': 'N/A', 'legal_name': 'N/A', 'dba': 'N/A', 'link': link, 'error': ''}
    
    for attempt in range(3):
        profile = random.choice(BROWSER_PROFILES)
        headers = {'Referer': 'https://apps.fcc.gov/cgb/form499/499a.cfm', 'User-Agent': profile['ua']}
        try:
            async with AsyncSession(impersonate=profile['impersonate'], headers=headers) as session:
                response = await session.get(FCC_FORM_499_URL, params=params, timeout=45)
                if response.status_code == 200:
                    if parse_fcc_results(response.text, result): return result
                    result['status'] = 'Not Found'
                    return result
                elif response.status_code in [429, 500, 502, 503, 504]:
                    await asyncio.sleep(2)
                    continue
        except:
            pass
    return result

def parse_fcc_results(html_content, result_dict):
    if "1 Record Found" in html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for i, row in enumerate(rows):
                cells = row.find_all(['th', 'td'])
                if len(cells) >= 3 and cells[0].get_text(strip=True).isdigit():
                    result_dict['filer_id'] = cells[0].get_text(strip=True)
                    result_dict['legal_name'] = cells[1].get_text(strip=True)
                    result_dict['dba'] = cells[2].get_text(strip=True) if len(cells) > 2 else 'N/A'
                    result_dict['status'] = 'Active'
                    return True
    return False

# --- 2. EMAIL FETCH MODULE ---
async def fetch_email_browser(context, frn, filing_url):
    try:
        page = await context.new_page()
        await page.goto(filing_url, timeout=30000)
        email = None
        try:
             selector = "#sp_formfield_contact_email"
             await page.wait_for_selector(selector, timeout=10000)
             email = await page.input_value(selector)
        except:
             pass
        if not email:
             content = await page.content()
             emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', content)
             valid_emails = [e for e in emails if 'fcc.gov' not in e and 'servicenow' not in e]
             if valid_emails: email = valid_emails[0]
        await page.close()
        return email
    except:
        return None

# --- 3. DATABASE SAVE MODULE ---
def save_lead_to_db(company_name, email, phone, status):
    if not email or '@' not in email: return
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM leads WHERE email = %s", (email,))
        if cursor.fetchone():
             cursor.execute("UPDATE leads SET company_name = %s, phone = %s, verify_status = %s WHERE email = %s", (company_name, phone, status, email))
        else:
             cursor.execute("INSERT INTO leads (company_name, email, phone, verify_status) VALUES (%s, %s, %s, %s)", (company_name, email, phone, status))
        conn.commit()
        cursor.close()
        conn.close()
    except:
        pass

# --- 4. UNIFIED CHAIN PROCESSOR ---
async def process_single_lead(lead_dict, browser_context, browser_semaphore):
    frn = lead_dict.get("FRN", "").strip()
    company_name = lead_dict.get("Business Name", "").strip()
    if not frn: return

    try:
        print(f"  [Verify] Checking FRN: {frn} ({company_name})")
        verify_result = await check_fcc_form499(frn, company_name)

        if 'Active' in verify_result['status']:
             print(f"  [Active] {company_name} - Pulling Email...")
             async with browser_semaphore:
                 # Visit the single listing page where contact_email exists
                 sys_id = lead_dict.get("sys_id", "")
                 if sys_id:
                      filing_url = f"https://fccprod.servicenowservices.com/rmd?id=rmd_single_listing&sys_id={sys_id}"
                      email = await fetch_email_browser(browser_context, frn, filing_url)
                 else:
                      email = None

             if email:
                  print(f"  ✅ [MATCH] {company_name} -> {email}")
                  save_lead_to_db(company_name, email, lead_dict.get("Contact Phone", ""), verify_result['status'])
    except:
         pass

# --- 5. MAIN SCRAPER INITIATOR ---
async def main():
    print("\n🚀 STARTING [TEST] SINGLE-PAGE ASYNC PIPELINE\n")
    from playwright.async_api import async_playwright
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx_list = await browser.new_context()
        page = await ctx_list.new_page()
        ctx_fetch = await browser.new_context(user_agent="Mozilla/5.0 AppleWebKit/537.36")

        print(f"Opening listing index endpoint: {FCC_URL}")
        await page.goto(FCC_URL, timeout=TIMEOUT_MS)
        await page.wait_for_selector("table tbody tr", timeout=45000)

        tasks = []
        browser_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BROWSERS)

        header_els = await page.locator("table thead th").all()
        headers = [await h.inner_text() for h in header_els]
        headers = [h.strip() for h in headers if h.strip()]

        print("\n--- Scanning Page 1 ---")
        rows = await page.locator("table tbody tr").all()
        print(f"  Found {len(rows)} leads on Page 1. Triggering stream...")

        # Extract full Angular dataset for EVERY row at once (including sys_id!)
        items = await page.evaluate("""
            () => {
                const rows = document.querySelectorAll("table tbody tr[ng-repeat]");
                return Array.from(rows).map(tr => {
                    const scope = angular.element(tr).scope();
                    if (!scope || !scope.item) return null;
                    const item = scope.item;
                    return {
                        "Business Name": item.business_name ? item.business_name.display_value : "",
                        "FCC Registration Number (FRN)": item.frn ? item.frn.display_value : "",
                        "FRN": item.frn ? item.frn.display_value : "",
                        "Contact Phone": item.contact_telephone_number ? item.contact_telephone_number.display_value : "",
                        "sys_id": item.sys_id || ""
                    };
                }).filter(i => i !== null);
            }
        """)

        print(f"  Found {len(items)} leads on Page 1. Triggering stream...")
        for row_data in items:
            if not row_data.get("FRN"): continue
            task = asyncio.create_task(process_single_lead(row_data, ctx_fetch, browser_semaphore))
            tasks.append(task)

            if len([t for t in tasks if not t.done()]) >= MAX_CONCURRENT_TASKS:
                await asyncio.sleep(0.5)

        print("\n⏳ [TEST] Scanned Page 1. Awaiting stream processing to complete fully...")
        await asyncio.gather(*tasks)
        await browser.close()
        print("\n🎉 [TEST] PIPELINE STREAM COMPLETE.")

if __name__ == "__main__":
    asyncio.run(main())
