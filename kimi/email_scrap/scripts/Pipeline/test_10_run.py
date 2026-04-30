#!/usr/bin/env python3
"""
================================================================================
LIMITED TEST RUN - Scrapes 10 Valid Leads to Verify Database Saves
================================================================================
"""

import os
import sys
import asyncio
import time
import re
import random
import json
from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from app.database import get_db_connection
from scripts.Pipeline.unified_async_pipeline import process_single_lead, FCC_URL, TIMEOUT_MS, MAX_CONCURRENT_BROWSERS

async def main():
    print("\n🚀 STARTING LIMITED (10 LEADS) TEST RUN\n")
    
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
        page_num = 1
        
        while len(tasks) < 10:
             print(f"\n--- Scanning Page {page_num} ---")
             try:
                 await page.wait_for_selector("table tbody tr", timeout=10000)
             except:
                 print("  Table did not load content.")
                 break
                 
             items = await page.evaluate("""
                 () => {
                     const rows = document.querySelectorAll("table tbody tr[ng-repeat]");
                     return Array.from(rows).map(tr => {
                         const scope = angular.element(tr).scope();
                         if (!scope || !scope.item) return null;
                         const item = scope.item;
                         return {
                             "Business Name": item.business_name ? item.business_name.display_value : "",
                             "FRN": item.frn ? item.frn.display_value : "",
                             "Contact Phone": item.contact_telephone_number ? item.contact_telephone_number.display_value : "",
                             "sys_id": item.sys_id || ""
                         };
                     }).filter(i => i !== null);
                 }
             """)

             if not items:
                  print("  No items found.")
                  break

             print(f"  Found {len(items)} leads on Page {page_num}. Triggering streams...")
             for row_data in items:
                  if not row_data.get("FRN"): continue
                  if len(tasks) >= 10:
                       print("\n🎯 Loaded 10 leads queue. Stopping page scan streams.")
                       break
                       
                  print(f"  -> Spawning stream for: {row_data['Business Name']}")
                  task = asyncio.create_task(process_single_lead(row_data, ctx_fetch, browser_semaphore))
                  tasks.append(task)

             if len(tasks) >= 10:
                  break

             next_btn = page.locator('a[aria-label^="Next page"]')
             if await next_btn.count() > 0:
                  classes = await next_btn.get_attribute("class")
                  if classes and "disabled" in classes: break
                  print("  Moving to Next Page...")
                  await next_btn.click()
                  await page.wait_for_timeout(3000) 
                  page_num += 1
             else:
                  break

        print(f"\n⏳ All 10 tasks spawned! Awaiting background lead streams to complete fully...")
        await asyncio.gather(*tasks)
        await browser.close()
        print("\n🎉 LIMITED PIPELINE TEST COMPLETE.")

if __name__ == "__main__":
    asyncio.run(main())
