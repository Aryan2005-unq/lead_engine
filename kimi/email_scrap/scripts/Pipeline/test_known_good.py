import asyncio
import sys
import os
from playwright.async_api import async_playwright

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from scripts.Pipeline.unified_async_pipeline import process_single_lead, MAX_CONCURRENT_BROWSERS

async def main():
    print("\n🚀 RUNNING SINGLE KNOWN LEAD VERIFICATION TEST\n")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx_fetch = await browser.new_context()
        browser_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BROWSERS)

        # 🎯 Known good lead with contact email: s2gadmin@s2g.net
        row_data = {
            "Business Name": "Support Services Group",
            "FRN": "0031375546",
            "Contact Phone": "(254) 299-2700",
            # Sys id from earlier diagnostics
            "sys_id": "885368f59797b61012c3bffce053af82"
        }
        
        print(f"Spawning lead stream for: {row_data['Business Name']}")
        await process_single_lead(row_data, ctx_fetch, browser_semaphore)
        
        await browser.close()
    print("\n🎉 SINGLE LEAD SAVE TEST COMPLETE.")

if __name__ == "__main__":
    asyncio.run(main())
