import asyncio
import re
import os
import sys
import logging
import time
import random
from typing import List, Dict, Optional

from playwright.async_api import async_playwright, BrowserContext, TimeoutError as PlaywrightTimeoutError

# --- PATH RESOLUTION ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from system.core.db import get_pool, close_pool

# --- LOGGING CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

# Concurrency & Pool limits
NUM_CONTEXTS = 3            # Number of parallel browser identities
CONCURRENT_PAGES = 6        # Max parallel open tabs total
BATCH_SIZE = 50             # Process buffer triggers
PAGE_TIMEOUT_MS = 15000     # 15s page navigation timeouts


# ---------------------------------------------------------------------
# 1. Database Operations
# ---------------------------------------------------------------------

async def init_schema():
    """Appends resume flag tracker resolving process cycles setup triggers."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("ALTER TABLE leads ADD COLUMN IF NOT EXISTS processed_email BOOLEAN DEFAULT FALSE")


async def fetch_batch(limit: int = BATCH_SIZE) -> List[Dict]:
    """Fetch chunked unprocessed leads conforming to specify filter parameters."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, frn, company_name 
            FROM leads 
            WHERE (verify_status LIKE 'Active%%' OR verify_status IS NULL)
              AND (email IS NULL OR email = '' OR email NOT LIKE '%%@%%')
              AND processed_email = false 
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]


async def bulk_update_emails(rows: List[Dict]):
    """Insert found email buffers bulk into target columns."""
    if not rows: return
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            stmt = await conn.prepare("""
                UPDATE leads SET email = $1
                WHERE id = $2 AND (email IS NULL OR email = '')
            """)
            await stmt.executemany([(r['email'], r['id']) for r in rows])


async def mark_processed(lead_ids: List[int]):
    """Disarms offset checks preventing loops back-end iterations."""
    if not lead_ids: return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE leads SET processed_email = true WHERE id = ANY($1)", lead_ids)


# ---------------------------------------------------------------------
# 2. Enrichment Worker
# ---------------------------------------------------------------------

async def enrich_lead(ctx: BrowserContext, lead: Dict, sem: asyncio.Semaphore) -> Optional[Dict]:
    """
    Open single tab concurrent operation loading FCC portals with safe 
    routing filter overlays. Includes internal fallbacks.
    """
    frn = lead.get("frn")
    if not frn: return None

    # Redirecting Direct Search string servicing ServiceNow detailed routers
    url = f"https://fccprod.servicenowservices.com/rmd?id=rmd_listings&sysparm_search={frn}"
    
    async with sem:
        for attempt in range(3):
            page = None
            try:
                page = await ctx.new_page()
                await page.goto(url, timeout=PAGE_TIMEOUT_MS)
                
                try:
                    selector = "#sp_formfield_contact_email"
                    await page.wait_for_selector(selector, timeout=8000)
                    email = await page.input_value(selector)
                    if email and "@" in email:
                        lead["email"] = email.strip()
                        return lead
                except Exception:
                    pass # Go to fallback regex
                
                # Regex Fallback
                content = await page.content()
                emails = re.findall(r"[\w\.-]+@[\w\.-]+\.\w+", content)
                valid = [e for e in emails if "fcc.gov" not in e and "servicenow" not in e]
                if valid:
                    lead["email"] = valid[0]
                    return lead
                
                break # If no error but simply unfound, stop retry iterations
                
            except (PlaywrightTimeoutError, Exception) as e:
                if attempt == 2:
                    logging.debug(f"Failed lead id {lead['id']} after 3 attempts: {e}")
            finally:
                if page:
                    await page.close()
                    
    return None

# ---------------------------------------------------------------------
# 3. Main Orchestration 
# ---------------------------------------------------------------------

async def main():
    await init_schema()
    print(f"\n--- 🚀 STARTING ASYNC BROWSER EMAIL ENRICHMENT ---\n")
    start_time = time.time()
    
    total_found = 0
    total_processed = 0
    batch_num = 1
    
    sem = asyncio.Semaphore(CONCURRENT_PAGES)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        # 1. Create Browser Context Pool
        contexts = []
        for _ in range(NUM_CONTEXTS):
            ctx = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36"
            )
            # Setup route aborters to isolate images and fonts (for Speed boosts)
            await ctx.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "font", "media"] else route.continue_())
            contexts.append(ctx)

        logging.info(f"Generated pool of {NUM_CONTEXTS} contexts. Processing in buffers size {BATCH_SIZE}...")

        while True:
            batch = await fetch_batch(limit=BATCH_SIZE)
            if not batch:
                print("✅ All leads processed through current filter offset.")
                break

            logging.info(f"Processing Batch {batch_num} ({len(batch)} records)...")
            
            # 2. Schedule Tasks
            tasks = [enrich_lead(random.choice(contexts), l, sem) for l in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # 3. Analyze output
            insertable = []
            for res in results:
                if res and isinstance(res, dict) and res.get("email"):
                    insertable.append(res)

            # 4. Save cycles
            await bulk_update_emails(insertable)
            await mark_processed([l["id"] for l in batch])

            total_found += len(insertable)
            total_processed += len(batch)
            elapsed = time.time() - start_time
            rate = total_processed / elapsed if elapsed > 0 else 0

            print(
                f"📊 Progress: Batch {batch_num} | Found Batch Emails: {len(insertable)} | "
                f"Total Processed: {total_processed} | Total Found: {total_found} | Speed: {rate:.2f} leads/sec"
            )
            batch_num += 1

        await browser.close()

    await close_pool()
    print(f"\n🎉 DONE! Enriched total {total_found} emails across {total_processed} checked records.")

if __name__ == "__main__":
    try:
         asyncio.run(main())
    except KeyboardInterrupt:
         print("\n⚠️ Enrichment workflow interrupted cleanly user.")
    except Exception as e:
         logging.error(f"❌ Core Error Crash: {type(e).__name__}: {e}")
