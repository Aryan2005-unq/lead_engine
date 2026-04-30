#!/usr/bin/env python3
"""
================================================================================
SELF-OPTIMIZING FCC LISTINGS FETCHER (ADAPTIVE CONCURRENCY & BULK POST)
================================================================================
This script scrapes listings pages with dynamically adjusting parallel workers 
hitting ServiceNow's JSON widgets loading routers cleanly avoiding static setups.

FEATURES:
1. **Adaptive Scaling**: Scales workers tasks based on response rates and error sizes.
2. **Bulk DB inserts**: Buffers Conflict triggers upsert buffering efficiently.
3. **Multi-Queue Router**: Loops increment page offsets framing cleanly.
================================================================================
"""

import asyncio
import os
import sys
import json
import time
import random
import logging
from collections import deque
from datetime import datetime
from typing import Set, List, Dict, Any

from curl_cffi.requests import AsyncSession
from playwright.async_api import async_playwright

# --- PATH RESOLUTION ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from system.core.db import get_pool, close_pool

# --- CONFIGURATION ---
API_URL = "https://fccprod.servicenowservices.com/api/now/sp/widget/2ba6f55c1b72a89089df9796bc4bcb10?id=rmd_listings"
MAX_CONCURRENCY = 10
MIN_CONCURRENCY = 3
INITIAL_CONCURRENCY = 5
MAX_PAGES = 1000  

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

class AdaptiveScraper:
    def __init__(self, token: str, cookies: list, recent_ids: Set[str]):
        self.token = token
        self.cookies = cookies
        self.recent_ids = recent_ids
        self.session = None
        
        # Concurrency & Scaling logic state
        self.concurrency = INITIAL_CONCURRENCY
        self.queue = asyncio.Queue()
        self.workers = []
        self.stop_scraping = asyncio.Event()
        
        # Statistics Tracking buffers
        self.stats = {
            "response_times": deque(maxlen=20),
            "errors": 0,
            "success": 0,
            "total_fetched": 0,
            "batches_inserted": 0,
        }
        
        # Batching 
        self.batch_queue = []
        self.batch_size = 150
        self.last_batch_time = time.time()

    async def init_session(self):
        """Builds AsyncSession applying extracted tokens setups."""
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "X-UserToken": self.token,
            "X-Portal": "ac2856301b92681048c6ed7bbc4bcb27",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        self.session = AsyncSession(impersonate="chrome120", headers=headers)
        # Apply cookies inside session trigger frames
        for c in self.cookies:
            self.session.cookies.set(c['name'], c['value'], domain=c['domain'])

    def get_payload(self, page_num: int) -> Dict[str, Any]:
        """Builds ServiceNow listings JSON payload offsetting framing."""
        return {
            "table": "x_g_fmc_rmd_robocall_mitigation_database",
            "filter": "status=Published",
            "p": page_num,
            "o": "sys_updated_on",
            "d": "desc",
            "window_size": 50, # Optimized batch window size
            "view": "service_portal",
            "fields": "business_name,frn,previous_dba_names,business_address"
        }

    async def worker(self, worker_id: int):
        """Continuous pipeline consumer consuming increments."""
        logging.info(f"👷 Worker-{worker_id} started")
        while not self.stop_scraping.is_set():
            try:
                # 1. Pop Next Page
                page_num = await self.queue.get()
                if page_num == "STOP":
                    self.queue.task_done()
                    logging.info(f"👷 Worker-{worker_id} exiting (Scale Down)")
                    break
                
                # 2. Fetch API Data offset
                start = time.time()
                try:
                    resp = await self.session.post(API_URL, json=self.get_payload(page_num), timeout=25)
                    elapsed = time.time() - start
                    self.stats["response_times"].append(elapsed)

                    if resp.status_code == 200:
                        data = resp.json()
                        rows = data.get("result", {}).get("data", {}).get("list", [])
                        
                        if not rows:
                            logging.info(f"[{worker_id}] Page {page_num} returned 0 items.")
                            self.stats["success"] += 1
                            self.queue.task_done()
                            continue

                        # 3. Process Content rows
                        stop_found = False
                        for r in rows:
                            frn = r.get("u_fcc_registration_number_frn") or r.get("frn")
                            if frn: frn = str(frn).strip()

                            # Early Stop Triggered
                            if frn and frn in self.recent_ids:
                                logging.info(f"🚨 [Worker-{worker_id}] Found overlapping sync point FRN='{frn}'. Triggering Early Stop.")
                                self.stop_scraping.set()
                                stop_found = True
                                break
                            
                            if frn:
                                self.batch_queue.append({
                                    "frn": frn,
                                    "business_name": r.get("u_business_name") or r.get("business_name") or "",
                                    "sys_id": r.get("sys_id", ""),
                                    "attachment_link": r.get("u_attachment_link") or "",
                                    "other_data": r
                                })

                        self.stats["success"] += 1
                        self.stats["total_fetched"] += len(rows)
                        
                        if stop_found:
                             self.queue.task_done()
                             break

                    else:
                        logging.warning(f"[{worker_id}] Page {page_num} HTTP Errors: {resp.status_code}")
                        self.stats["errors"] += 1
                        # Push back to queue with retry increment fallback frames
                        await self.queue.put(page_num)

                except Exception as e:
                    logging.error(f"[{worker_id}] Error page {page_num}: {e}")
                    self.stats["errors"] += 1
                    # Push back for retry if not stopped
                    if not self.stop_scraping.is_set():
                         await self.queue.put(page_num)

                self.queue.task_done()
                
                # Jitter rate Protection
                await asyncio.sleep(random.uniform(0.1, 0.4))

                # periodic bulk inserts
                if len(self.batch_queue) >= self.batch_size:
                     await self.flush_batch()

            except asyncio.CancelledError:
                 break

    async def flush_batch(self):
        """Flushes batch_queue into Postgresql bulk inserts buffers."""
        if not self.batch_queue: return
        start = time.time()
        
        current_rows = list(self.batch_queue)
        self.batch_queue.clear()
        
        try:
             pool = await get_pool()
             async with pool.acquire() as conn:
                 async with conn.transaction():
                      stmt = await conn.prepare("""
                          INSERT INTO staging_fcc_listings 
                          (frn, business_name, sys_id, attachment_link, other_data)
                          VALUES ($1, $2, $3, $4, $5::jsonb)
                          ON CONFLICT (frn) DO NOTHING
                      """)
                      await stmt.executemany([
                          (r["frn"], r["business_name"], r["sys_id"], r["attachment_link"], json.dumps(r["other_data"]))
                          for r in current_rows
                      ])
             self.stats["batches_inserted"] += 1
             elapsed = time.time() - start
             
             # Adaptive Batch Adjuster logic
             if elapsed < 0.5 and self.batch_size < 300: self.batch_size += 50
             elif elapsed > 2.0 and self.batch_size > 100: self.batch_size -= 50

        except Exception as e:
             logging.error(f"Batch Insert Failed: {e}")

    async def controller_loop(self):
        """Adaptive monitor weighing Concurrency limits scaling thresholds."""
        while not self.stop_scraping.is_set():
            await asyncio.sleep(5) # Evaluate rate every 5 seconds
            
            success = self.stats["success"]
            errors = self.stats["errors"]
            total = success + errors
            if total == 0: continue

            error_rate = errors / total
            avg_time = sum(self.stats["response_times"]) / len(self.stats["response_times"]) if self.stats["response_times"] else 0
            
            print(f"📊 [STATS] Concurrency: {self.concurrency} | Speed: {success/5:.1f} req/s | Avg Time: {avg_time:.2f}s | Errors: {error_rate:.1%} | Batch Size: {self.batch_size}")
            
            # Reset counters overlaying fresh periods calculations
            self.stats["success"] = 0
            self.stats["errors"] = 0

            # Scale Up: fast responses & no errors
            if avg_time < 1.8 and error_rate < 0.05 and self.concurrency < MAX_CONCURRENCY:
                self.concurrency += 1
                task = asyncio.create_task(self.worker(len(self.workers) + 1))
                self.workers.append(task)
                logging.info(f"🚀 Scaling UP Concurrency -> {self.concurrency}")

            # Scale Down: Slows or Blocked Rates thresholds
            elif (avg_time > 3.5 or error_rate > 0.10) and self.concurrency > MIN_CONCURRENCY:
                self.concurrency -= 1
                await self.queue.put("STOP")
                logging.info(f"🐢 Scaling DOWN Concurrency -> {self.concurrency}")

    async def run(self):
        """Main Orchestrator seeding queues continuous."""
        await self.init_session()
        
        # Seed Queue page references
        for p in range(1, MAX_PAGES + 1):
             await self.queue.put(p)

        # Initial launch worker pools
        self.workers = [asyncio.create_task(self.worker(i + 1)) for i in range(self.concurrency)]
        controller = asyncio.create_task(self.controller_loop())

        # Wait until full queue exhaust or Early Stop Event trigger
        done_task = asyncio.create_task(self.queue.join())
        stop_task = asyncio.create_task(self.stop_scraping.wait())

        await asyncio.wait([done_task, stop_task], return_when=asyncio.FIRST_COMPLETED)
        self.stop_scraping.set() # ensure everything halts trigger frames

        # Cleanup residual tabs
        for task in self.workers: task.cancel()
        controller.cancel()
        
        # Flush final backlogs
        await self.flush_batch()
        await self.session.close()


# ---------------------------------------------------------------------
# Utilities: Token Fetching 
# ---------------------------------------------------------------------

async def extract_session_token():
    logging.info("Starting Playwright context to extract authentication cookie tokens...")
    try:
        async with async_playwright() as p:
            # 🛡 Add sandbox flags for Docker environments Continuous
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            ctx = await browser.new_context()
            page = await ctx.new_page()
            
            logging.info("Navigating to ServiceNow portal...")
            await page.goto("https://fccprod.servicenowservices.com/rmd?id=rmd_listings", timeout=60000)
            
            # ⏳ Reduced Wait time, window.NOW typically populates immediately continuous
            await asyncio.sleep(4) 
            
            token = await page.evaluate("() => window.NOW ? window.NOW.user_token : (window.g_ck || '')")
            cookies = await ctx.cookies()
            
            await browser.close()
            return token, cookies
    except Exception as e:
        logging.error(f"Playwright Token Extraction Exception: {e}")
        return None, []

async def fetch_existing_ids() -> Set[str]:
    """Preloads existing ID triggers safe aggregates."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT frn FROM staging_fcc_listings ORDER BY created_at DESC LIMIT 5000")
            return {r["frn"] for r in rows if r["frn"]}
    except: return set()

async def main():
    print("\n🚀 LAUNCHING ADAPTIVE SELF-OPTIMIZING LISTINGS SCRAPER\n")
    
    token, cookies = await extract_session_token()
    if not token:
        logging.error("Failed to extract ServiceNow User-Token session trigger overlays. Exiting.")
        return

    recent_ids = await fetch_existing_ids()
    logging.info(f"Loaded {len(recent_ids)} historical IDs for Early-Stop caching thresholds.")

    scraper = AdaptiveScraper(token, cookies, recent_ids)
    await scraper.run()

    print(f"\n🎉 SCRAPE COMPLETE! Total leads fetched framing session updates.")
    await close_pool()

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print("\n⚠️ Aborted.")
