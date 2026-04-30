#!/usr/bin/env python3
"""
========================================================================
END-TO-END 10-LEAD TEST  — Full diagnosis of scrape + save pipeline
========================================================================
"""
import os, sys, asyncio, re
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from app.database import get_db_connection
from playwright.async_api import async_playwright

# -------- STEP A: Test DB save directly --------
def test_db_save():
    print("\n========== STEP A: Direct DB Save Test ==========")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO leads (company_name, email, phone, verify_status) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    ("DB_TEST_COMPANY", "dbtest@example.com", "111-222", "Test"))
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM leads")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"  ✅ DB Save OK. Total leads in DB now: {count}")
        return True
    except Exception as e:
        print(f"  ❌ DB SAVE FAILED: {e}")
        return False

# -------- STEP B: Scrape one email from a known profile via click method --------
async def test_scrape_single_email(browser):
    print("\n========== STEP B: Single Email Scrape Test ==========")
    ctx = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    page = await ctx.new_page()

    print("  1. Visiting listings index...")
    await page.goto("https://fccprod.servicenowservices.com/rmd?id=rmd_listings", timeout=60000)
    await page.wait_for_selector("table tbody tr", timeout=30000)
    print("  2. Table loaded. Clicking first row...")

    row = page.locator("table tbody tr").first
    await row.click()
    await page.wait_for_timeout(8000)

    print(f"  3. Current URL after click: {page.url}")

    email = None
    try:
        sel = "#sp_formfield_contact_email"
        await page.wait_for_selector(sel, timeout=10000)
        email = await page.input_value(sel)
        print(f"  ✅ Email via selector: '{email}'")
    except:
        print("  ⚠️ Selector failed, trying regex fallback...")
        content = await page.content()
        found = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', content)
        valid = [e for e in found if 'fcc.gov' not in e and 'servicenow' not in e]
        if valid:
            email = valid[0]
            print(f"  ✅ Email via regex: '{email}'")
        else:
            print(f"  ❌ No email found on page")

    # Also get company name from page
    company = "Unknown"
    try:
        name_sel = "#sp_formfield_business_name"
        await page.wait_for_selector(name_sel, timeout=3000)
        company = await page.input_value(name_sel)
    except:
        pass

    await page.close()
    await ctx.close()
    return email, company

# -------- STEP C: Full E2E for 10 leads --------
async def test_10_leads(browser):
    print("\n========== STEP C: Full 10-Lead E2E Pipeline ==========")
    ctx = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    page = await ctx.new_page()

    print("  Loading listings index...")
    await page.goto("https://fccprod.servicenowservices.com/rmd?id=rmd_listings", timeout=60000)
    await page.wait_for_selector("table tbody tr", timeout=30000)

    saved_count = 0
    skipped_count = 0
    error_count = 0
    page_num = 1

    while saved_count < 10 and page_num <= 20:
        print(f"\n  --- Page {page_num} ---")
        try:
            await page.wait_for_selector("table tbody tr", timeout=10000)
        except:
            print("  Table not loaded")
            break

        # Get row count on this page
        row_count = await page.locator("table tbody tr").count()
        print(f"  Rows on page: {row_count}")

        for row_idx in range(row_count):
            if saved_count >= 10:
                break

            try:
                # Click the row
                row = page.locator("table tbody tr").nth(row_idx)
                biz_name_text = await row.locator("td").first.inner_text()
                print(f"\n  [{saved_count+1}/10] Clicking: {biz_name_text.strip()[:50]}...")
                await row.click()
                await page.wait_for_timeout(6000)

                # Try to get email
                email = None
                company = biz_name_text.strip()
                phone = ""

                try:
                    sel = "#sp_formfield_contact_email"
                    await page.wait_for_selector(sel, timeout=8000)
                    email = await page.input_value(sel)
                except:
                    # Regex fallback
                    content = await page.content()
                    found = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', content)
                    valid = [e for e in found if 'fcc.gov' not in e and 'servicenow' not in e]
                    if valid:
                        email = valid[0]

                # Get company name from form
                try:
                    company = await page.input_value("#sp_formfield_business_name")
                except:
                    pass

                # Get phone from form
                try:
                    phone = await page.input_value("#sp_formfield_contact_telephone_number")
                except:
                    pass

                if email and '@' in email:
                    print(f"  ✅ FOUND: {company} -> {email}")
                    # Save to DB immediately
                    try:
                        conn = get_db_connection()
                        cur = conn.cursor()
                        cur.execute("SELECT id FROM leads WHERE email = %s", (email,))
                        if cur.fetchone():
                            cur.execute("UPDATE leads SET company_name=%s, phone=%s, verify_status=%s WHERE email=%s",
                                        (company, phone, "Active", email))
                        else:
                            cur.execute("INSERT INTO leads (company_name, email, phone, verify_status) VALUES (%s,%s,%s,%s)",
                                        (company, email, phone, "Active"))
                        conn.commit()
                        cur.close()
                        conn.close()
                        saved_count += 1
                        print(f"  💾 SAVED to DB! (Total saved: {saved_count})")
                    except Exception as e:
                        print(f"  ❌ DB Error: {e}")
                        error_count += 1
                else:
                    print(f"  ⚠️ No email found, skipping")
                    skipped_count += 1

                # Go back to listings
                await page.go_back()
                await page.wait_for_timeout(3000)
                await page.wait_for_selector("table tbody tr", timeout=10000)

            except Exception as e:
                print(f"  ❌ Row error: {e}")
                error_count += 1
                # Try to navigate back to listings
                try:
                    await page.goto("https://fccprod.servicenowservices.com/rmd?id=rmd_listings", timeout=30000)
                    await page.wait_for_selector("table tbody tr", timeout=15000)
                except:
                    break

        if saved_count >= 10:
            break

        # Go to next page
        next_btn = page.locator('a[aria-label^="Next page"]')
        if await next_btn.count() > 0:
            classes = await next_btn.get_attribute("class") or ""
            if "disabled" in classes:
                break
            await next_btn.click()
            await page.wait_for_timeout(3000)
            page_num += 1
        else:
            break

    await page.close()
    await ctx.close()

    print(f"\n  ========== RESULTS ==========")
    print(f"  Saved:   {saved_count}")
    print(f"  Skipped: {skipped_count}")
    print(f"  Errors:  {error_count}")
    return saved_count

async def main():
    print("=" * 60)
    print("  END-TO-END PIPELINE TEST (10 LEADS)")
    print("=" * 60)

    # Step A
    db_ok = test_db_save()
    if not db_ok:
        print("\n🛑 CANNOT CONTINUE — DB connection failed!")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        # Step B
        email, company = await test_scrape_single_email(browser)
        if email:
            print(f"\n  Step B PASSED: {company} -> {email}")
        else:
            print(f"\n  ⚠️ Step B: No email from first row (may be empty profile)")

        # Step C
        count = await test_10_leads(browser)

        await browser.close()

    # Final DB check
    print("\n========== FINAL DB CHECK ==========")
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, company_name, email, verify_status FROM leads ORDER BY id DESC LIMIT 15")
    rows = cur.fetchall()
    print(f"Total leads in DB: {len(rows)}")
    for r in rows:
        print(f"  [{r[0]}] {r[1]} | {r[2]} | {r[3]}")
    cur.close()
    conn.close()

    print("\n🎉 TEST COMPLETE!")

if __name__ == "__main__":
    asyncio.run(main())
