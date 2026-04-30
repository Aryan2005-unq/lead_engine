import asyncio
import sys
import os
import re
from playwright.async_api import async_playwright

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.Pipeline.unified_async_pipeline import FCC_URL

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        isolated_ctx = await browser.new_context(user_agent="Mozilla/5.0 AppleWebKit/537.36")
        page = await isolated_ctx.new_page()

        filing_url = "https://fccprod.servicenowservices.com/rmd?id=rmd_single_listing&sys_id=885368f59797b61012c3bffce053af82&view=sp"
        
        print("1. Visiting Index to establish session...")
        await page.goto("https://fccprod.servicenowservices.com/rmd?id=rmd_listings", timeout=20000)
        await page.wait_for_timeout(3000)
        
        print("2. Visiting single listing profile...")
        await page.goto(filing_url, timeout=30000)
        await page.wait_for_timeout(10000) # Give it 10 seconds to load AngularJS
        
        selector = "#sp_formfield_contact_email"
        print("3. Waiting for selector...")
        try:
             await page.wait_for_selector(selector, timeout=10000)
             email = await page.input_value(selector)
             print(f"✅ Success! Found email value: '{email}'")
        except Exception as e:
             print(f"❌ wait_for_selector ERROR: {e}")
             
             # Print what is on the page instead
             inputs = await page.locator("input").all()
             print(f"Found {len(inputs)} inputs:")
             for i, inp in enumerate(inputs):
                 id_attr = await inp.get_attribute("id")
                 try: val = await inp.input_value()
                 except: val = "ERR"
                 if "email" in (id_attr or "").lower() or "s2g" in (val or "").lower():
                      print(f"[{i}] {id_attr} -> {val}")

        await browser.close()

asyncio.run(main())
