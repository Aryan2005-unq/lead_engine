import csv
import asyncio
import re
from playwright.async_api import async_playwright
import argparse
import os

async def fetch_email_browser(context, frn, filing_url, row_data):
    """Worker to fetch email using a browser page"""
    page = None
    try:
        # Check if context is still valid
        if context.browser and not context.browser.is_connected():
            row_data['error'] = "Browser context closed"
            return row_data, False
            
        page = await context.new_page()
        
        # Go to the page
        # Wait for the email field to appear. 
        # Based on screenshot, it's an input with name="contact_email" or similar label
        # The debug_page.html showed: id="sp_formfield_contact_email"
        
        # print(f"Navigating to {filing_url}...")
        await page.goto(filing_url, timeout=30000)
        
        # Wait for the contact email field
        try:
            # Wait for the specific input ID from the debug HTML
            selector = "#sp_formfield_contact_email"
            await page.wait_for_selector(selector, timeout=15000)
            
            # Get the value
            email = await page.input_value(selector)
            
            if email:
                row_data['contact_email'] = email
                return row_data, True
            else:
                # Try parsing body text as fallback
                content = await page.content()
                emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', content)
                # Filter out generic fcc emails if any?
                # Just take the first likely candidate
                valid_emails = [e for e in emails if 'fcc.gov' not in e and 'servicenow' not in e]
                if valid_emails:
                    row_data['contact_email'] = valid_emails[0]
                    return row_data, True
                    
        except Exception as e:
            # print(f"Timeout/Error finding selector: {e}")
            pass
            
        row_data['error'] = "Email not found"
        return row_data, False
        
    except Exception as e:
        # Handle browser closed errors gracefully
        error_msg = str(e)
        if "Target page, context or browser has been closed" in error_msg or "TargetClosedError" in error_msg:
            row_data['error'] = "Browser closed during operation"
        else:
            row_data['error'] = error_msg
        return row_data, False
    finally:
        if page:
            try:
                await page.close()
            except:
                pass  # Ignore errors when closing already-closed page

async def main():
    print(f"Loading leads from Postgres database...")
    leads_to_process = []
    
    try:
        import sys
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))
        from app.database import get_db_connection
        
        conn = get_db_connection()
        cursor = conn.cursor()
        # Fetch non-usa leads or everything left that hasn't been fetched
        cursor.execute("""
            SELECT id, frn, company_name, verify_status, phone, email 
            FROM leads 
            WHERE (verify_status LIKE 'Active%%' OR verify_status IS NULL) 
            AND (email IS NULL OR email = '' OR email NOT LIKE '%%@%%')
        """)
        
        for r_row in cursor.fetchall():
            lead_id, frn, company_name, verify_status, phone, email = r_row
            url = f"https://apps.fcc.gov/cgb/form499/499results.cfm?frn={frn}"
            row_dict = {
                'id': lead_id,
                'frn': frn,
                'company_name': company_name,
                'verify_status': verify_status,
                'phone': phone,
                'email': email
            }
            if frn and url:
                 leads_to_process.append((frn, url, row_dict))
                 
        cursor.close()
        conn.close()
        print(f"Loaded {len(leads_to_process)} leads to process...")
    except Exception as e:
        print(f"Error loading from Postgres: {e}")
        return

    # 3. Launch Browser
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        CONCURRENCY = 15
        sem = asyncio.Semaphore(CONCURRENCY)
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        async def worker(frn, url, row):
            async with sem:
                try:
                    return await fetch_email_browser(context, frn, url, row)
                except Exception as e:
                    row['error'] = f"Worker error: {str(e)}"
                    return row, False

        tasks = []
        for frn, url, row in leads_to_process:
             tasks.append(asyncio.create_task(worker(frn, url, row)))
        
        completed_count = 0
        found_count = 0
        results = []
        import time
        start_time = time.time()

        for future in asyncio.as_completed(tasks):
            try:
                row, success = await future
                results.append(row)
                completed_count += 1
                
                if success and row.get('contact_email'):
                    found_count += 1
                    print(f"[FOUND] {row.get('company_name', 'N/A')}: {row['contact_email']}")
                
                if completed_count % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = completed_count / elapsed if elapsed > 0 else 0
                    print(f"Progress: {completed_count}/{len(leads_to_process)} | Found: {found_count} | Speed: {rate:.2f} leads/sec")
                    if results:
                        save_results_incremental(results)
            except Exception as e:
                print(f"Error processing result: {e}")

        await browser.close()
    
    if results:
         save_results_incremental(results)
         print("Done! Final results saved to Postgres.")
         print(f"Total Emails Found: {found_count}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Error: {e}")
