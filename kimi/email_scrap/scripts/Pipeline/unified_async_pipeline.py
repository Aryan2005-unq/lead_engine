#!/usr/bin/env python3
"""
================================================================================
UNIFIED STREAMING PIPELINE - Parallel Details Crawler (High Performance)
================================================================================
This script replaces slow sequential click navigation with parallel background 
contexts execution. 

LOGIC:
1.  **Extract All Links First**: Open listings, evaluate nodes extracting links.
2.  **Parallel Scraper**: Opens direct detail URLs concurrently over context pools.
3.  **Chunked Commit**: Processes 20-50 Batch frames updating conflict buffers.
4.  **Deduplicate Aggregates**: Safely buffers incremental resumes seamlessly.
================================================================================
"""

import os
import sys
import asyncio
import re
import random
import json
import time
from typing import List, Dict, Optional

from curl_cffi.requests import AsyncSession
from playwright.async_api import async_playwright, BrowserContext, TimeoutError as PlaywrightTimeoutError

# --- PATH RESOLUTION ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from system.core.db import get_pool, close_pool
from system.queues.redis_client import (
    get_checkpoint as redis_get_checkpoint,
    save_checkpoint as redis_save_checkpoint,
)

# --- CONFIGURATION ---
FCC_URL = "https://fccprod.servicenowservices.com/rmd?id=rmd_listings"
FCC_FORM_499_URL = "https://apps.fcc.gov/cgb/form499/499results.cfm"
TIMEOUT_MS = 60000
MAX_PAGES = 9999  # Effectively unlimited; loop breaks on missing/disabled Next button

# Concurrency 
NUM_CONTEXTS = 3
CONCURRENT_PAGES = 10
BATCH_SIZE = 30


BROWSER_PROFILES = [
    {"impersonate": "chrome120", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
    {"impersonate": "chrome119", "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/555.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"},
]

# ---------------------------------------------------------------------
# 1. Database Operations
# ---------------------------------------------------------------------

async def init_schema():
    """Adds conflict constraints for safe Upserts triggers."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            constraint_check = await conn.fetchval("""
                SELECT 1 FROM information_schema.table_constraints WHERE table_name = 'leads' AND constraint_name = 'leads_email_key'
            """)
            if not constraint_check:
                await conn.execute("DELETE FROM leads a USING leads b WHERE a.id < b.id AND a.email = b.email")
                await conn.execute("ALTER TABLE leads ADD CONSTRAINT leads_email_key UNIQUE (email)")
        except Exception: pass


async def bulk_insert_leads(leads: List[Dict]):
    """Insert found elements batch aggregate Conflict overlays."""
    if not leads: return
    pool = await get_pool()
    async with pool.acquire() as conn:
         async with conn.transaction():
              stmt = await conn.prepare("""
                  INSERT INTO leads (company_name, email, phone, verify_status) 
                  VALUES ($1, $2, $3, $4)
                  ON CONFLICT (email) DO UPDATE 
                  SET company_name = EXCLUDED.company_name, 
                      phone = COALESCE(NULLIF(EXCLUDED.phone, ''), leads.phone)
              """)
              await stmt.executemany([
                  (l['company_name'], l['email'], l.get('phone', ''), l.get('verify_status', 'Pending')) 
                  for l in leads
              ])


# ---------------------------------------------------------------------
# 2. Background FCC Verify Async
# ---------------------------------------------------------------------

async def check_fcc_form499(frn: str) -> str:
    params = {'frn': frn, 'operational': '', 'comm_type': 'Any Type', 'R1': 'and', 'XML': 'FALSE'}
    for attempt in range(2):
        profile = random.choice(BROWSER_PROFILES)
        headers = {'User-Agent': profile['ua']}
        try:
            async with AsyncSession(impersonate=profile['impersonate'], headers=headers) as session:
                resp = await session.get(FCC_FORM_499_URL, params=params, timeout=30)
                if resp.status_code == 200:
                    if "Access Denied" in resp.text: continue
                    if "Record Found" in resp.text or "Records Found" in resp.text: return "Active"
                    return "Not Found"
        except: pass
    return "Unknown"


async def verify_and_update(company: str, email: str, frn: str):
    """Executes background verification overlaying commits."""
    if not frn: return
    try:
        status = await check_fcc_form499(frn)
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE leads SET verify_status=$1 WHERE email=$2", status, email)
        print(f"  ✅ [FCC] Verification completed for {company}: {status}")
    except: pass


# ---------------------------------------------------------------------
# 3. Crawler Worker 
# ---------------------------------------------------------------------

async def scrape_detail_page(ctx: BrowserContext, item: Dict, sem: asyncio.Semaphore) -> Optional[Dict]:
    """Concurrent worker page.goto evaluating details directly."""
    if not item.get("detail_url"): return None
    
    async with sem:
        page = None
        current_url = item["detail_url"]
        print(f"  [Detail] Navigating to: {current_url}")
        try:
            page = await ctx.new_page()
            await page.goto(current_url, timeout=30000, wait_until="load")
            
            # 1. Extract values
            email = None
            try:
                await page.wait_for_selector("#sp_formfield_contact_email", timeout=12000)
                email = await page.input_value("#sp_formfield_contact_email")
                print(f"  [Detail] {item['company_name']} Email input found: {email}")
            except Exception as e:
                print(f"  [Detail] contact_email selector failed or read-only for {item['company_name']}, trying regex fallback...")

            if not email or '@' not in email:
                content = await page.content()
                emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', content)
                valid = [e for e in emails if 'fcc.gov' not in e and 'servicenow' not in e]
                if valid: 
                    email = valid[0]
                    print(f"  [Detail] Regex fallback found email: {email}")

            if not email or '@' not in email: 
                print(f"  [Detail] ❌ No email found for {item['company_name']}")
                return None

            item["email"] = email.strip()
            
            try: 
                item["company_name"] = await page.input_value("#sp_formfield_business_name") or item["company_name"]
            except: pass
            
            try: 
                item["phone"] = await page.input_value("#sp_formfield_contact_telephone_number") or ""
            except: pass
            
            try: 
                item["frn"] = await page.input_value("#sp_formfield_frn") or item["frn"]
            except: pass
            
            print(f"  [Detail] ✅ Success extraction for {item['company_name']}")
            return item
        except Exception as e: 
            print(f"  [Detail] 💥 Error scraping {item['company_name']}: {str(e)[:100]}")
            return None
        finally:
            if page: await page.close()


# ---------------------------------------------------------------------
# 4. Checkpoint management
# ---------------------------------------------------------------------

# Checkpoint is now stored in Redis via system.queues.redis_client
# See save_checkpoint / get_checkpoint with key "unified_pipeline"


# ---------------------------------------------------------------------
# 5. Core Pipeline Loops
# ---------------------------------------------------------------------

async def goto_page(page, target_page: int):
    """Navigate to listings and click Next until we reach target_page."""
    await page.goto(FCC_URL, timeout=TIMEOUT_MS)
    await page.wait_for_selector("table tbody tr", timeout=30000)
    for i in range(target_page - 1):
        next_btn = page.locator('a[aria-label^="Next page"]')
        if await next_btn.count() > 0:
            await next_btn.click()
            await asyncio.sleep(2)
            await page.wait_for_selector("table tbody tr", timeout=15000)


async def main():
    print("\n🚀 STARTING PARALLEL HIGH-PERFORMANCE STREAMING PIPELINE\n")
    await init_schema()
    
    checkpoint = await redis_get_checkpoint("unified_pipeline", {"page": 1})
    page_num = checkpoint.get("page", 1) if isinstance(checkpoint, dict) else 1
    print(f"📖 Resume: Starting from Page {page_num}")
    
    seen_emails = set()
    bg_tasks = []
    
    # Pre-populate seen filter
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT email FROM leads WHERE email IS NOT NULL")
        seen_emails = {r['email'] for r in rows}
    print(f"Loaded {len(seen_emails)} saved leads for de-duplication.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        # 1. Create Browser Context Pool
        contexts = []
        for _ in range(NUM_CONTEXTS):
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            # Route aborters Speed boosts
            await ctx.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "font", "media"] else route.continue_())
            contexts.append(ctx)

        # listings crawler master page
        page = await browser.new_page()
        print(f"Opening listings master interface...")
        await goto_page(page, page_num)
        
        sem = asyncio.Semaphore(CONCURRENT_PAGES)

        while page_num <= MAX_PAGES:
            print(f"\n--- 📄 Page {page_num} listings extractor ---")
            await page.wait_for_selector("table tbody tr", timeout=15000)
            
            # Fast Extraction listings from Angular Scope (ServiceNow compatible)
            links = await page.evaluate("""() => {
                const results = [];
                const scope = angular.element(document.querySelector('div[ng-controller="listingsController"]')).scope();
                
                if (scope && scope.listings) {
                    scope.listings.forEach(item => {
                        const tbl = item.targetTable || 'x_g_fmc_rmd_robocall_mitigation_database';
                        results.push({
                            company_name: item.u_business_name || '',
                            frn: item.u_fcc_registration_number_frn || '',
                            detail_url: `https://fccprod.servicenowservices.com/rmd?id=rmd_form&table=${tbl}&sys_id=${item.sys_id}`
                        });
                    });
                } else {
                    // Fallback to DOM parsing if Angular holds no scope globally
                    document.querySelectorAll("table tbody tr").forEach(row => {
                        const cells = row.querySelectorAll("td");
                        if (cells.length < 2) return;
                        
                        let sys_id = row.getAttribute('sys_id') || row.getAttribute('data-sys_id');
                        let table = row.getAttribute('table') || row.getAttribute('data-table') || 'x_g_fmc_rmd_robocall_mitigation_database';
                        
                        try {
                            const rowScope = angular.element(row).scope();
                            if (rowScope && rowScope.item && rowScope.item.sys_id) {
                                sys_id = rowScope.item.sys_id;
                                if (rowScope.item.targetTable) table = rowScope.item.targetTable;
                            }
                        } catch(e) {}

                        // Find the actual record link if sys_id fails
                        const rmd_link = Array.from(row.querySelectorAll("a")).find(a => a.href && a.href.includes("rmd_form"));
                        let url = '';
                        
                        if (sys_id) {
                            url = `https://fccprod.servicenowservices.com/rmd?id=rmd_form&table=${table}&sys_id=${sys_id}`;
                        } else if (rmd_link) {
                            url = rmd_link.href;
                        }

                        results.push({
                            company_name: cells[0].innerText.trim(),
                            frn: cells[2] ? cells[2].innerText.trim() : '',
                            detail_url: url
                        });
                    });
                }
                return results;
            }""")

            print(f"  Extracted {len(links)} links on page listings.")
            
            # Chunking detail workers
            for i in range(0, len(links), BATCH_SIZE):
                 batch = links[i:i+BATCH_SIZE]
                 tasks = [scrape_detail_page(random.choice(contexts), l, sem) for l in batch]
                 results = await asyncio.gather(*tasks, return_exceptions=True)

                 insertable = []
                 for res in results:
                     if res and isinstance(res, dict) and res.get("email"):
                         email = res["email"]
                         if email not in seen_emails:
                              seen_emails.add(email)
                              insertable.append(res)
                              if res.get("frn"):
                                   bg_tasks.append(asyncio.create_task(verify_and_update(res["company_name"], email, res["frn"])))

                 if insertable:
                     await bulk_insert_leads(insertable)
                     print(f"  💾 Saved batch aggregate size {len(insertable)} leads.")

            print(f"  ➡️  Completed page {page_num} background processing triggers.")
            await redis_save_checkpoint("unified_pipeline", {"page": page_num + 1})
            
            # Next listings cycle 
            next_btn = page.locator('a[aria-label^="Next page"]')
            if await next_btn.count() > 0:
                classes = await next_btn.get_attribute("class") or ""
                if "disabled" in classes: break
                await next_btn.click()
                await asyncio.sleep(3)
                page_num += 1
            else: break

        if bg_tasks:
            print(f"\n⏳ Waiting for {len(bg_tasks)} background FCC verifications...")
            await asyncio.gather(*bg_tasks, return_exceptions=True)

        await browser.close()

    await close_pool()
    print("\n🎉 PIPELINE COMPLETE!")


if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print("\n⚠️ Manually interrupted.")
print("=" * 50)
