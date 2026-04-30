"""
Intake Worker
Continuously scrapes new FCC listings via Playwright and pushes them
into the normalize_queue for downstream processing.
"""
import asyncio
import json
import re
from typing import Any, Dict, List, Set

from playwright.async_api import async_playwright

from system.core.config import config
from system.core.logger import WorkerLogger
from system.core import db
from system.queues import redis_client

log = WorkerLogger("intake")


async def _get_recent_frns(limit: int = 500) -> Set[str]:
    """Load recent FRNs from DB to detect the sync-stop point."""
    rows = await db.fetch_rows(
        "SELECT frn FROM staging_fcc_listings ORDER BY created_at DESC LIMIT $1",
        limit,
    )
    return {r["frn"] for r in rows}


async def _scrape_pages(page, recent_ids: Set[str]) -> List[Dict[str, Any]]:
    """Evaluates Angular listings bundles page-by-page targeting optimal speeds."""
    collected: List[Dict] = []
    page_num = 1

    try:
        await page.wait_for_selector("table tbody tr", timeout=45000)
    except Exception:
        log.error("Timeout waiting for listings table")
        return []

    while True:
        log.info(f"Scanning page {page_num}...")
        await asyncio.sleep(2) # Stabilize angular DOM loads
        
        # Fast Angular JS Bundle Scraper evaluation
        data = await page.evaluate("""() => {
            const results = [];
            const scope = angular.element(document.querySelector('div[ng-controller="listingsController"]')).scope();
            if (scope && scope.listings) {
                scope.listings.forEach(item => {
                    results.push({
                        frn: item.u_fcc_registration_number_frn || '',
                        business_name: item.u_business_name || '',
                        sys_id: item.sys_id || '',
                        attachment_link: item.u_attachment_link || '',
                        other_data: item
                    });
                });
            } else {
                // Fallback: standard extraction for table loops if scope isn't cleanly available
                document.querySelectorAll("table tbody tr").forEach(row => {
                     const cells = row.querySelectorAll("td");
                     if (cells.length < 3) return;
                     results.push({
                          frn: cells[2] ? cells[2].innerText.trim() : '',
                          business_name: cells[1] ? cells[1].innerText.trim() : '',
                          sys_id: '',
                          other_data: {}
                     });
                });
            }
            return results;
        }""")

        if not data:
            log.warning(f"No angular scope data found on page {page_num}")
            break

        stop = False
        for item in data:
            frn_val = item.get("frn", "").strip()
            if frn_val in recent_ids:
                log.info(f"Sync point hit at FRN={frn_val}")
                stop = True
                break
            
            if frn_val:
                collected.append({
                    "frn": frn_val,
                    "business_name": item.get("business_name", ""),
                    "sys_id": item.get("sys_id", ""),
                    "attachment_link": item.get("attachment_link", ""),
                    "other_data": item.get("other_data", {})
                })

        if stop: break

        next_btn = page.locator('a[aria-label^="Next page"]')
        if await next_btn.count() > 0:
            classes = await next_btn.get_attribute("class") or ""
            if "disabled" in classes: break
            await next_btn.click()
            page_num += 1
        else:
            break

    return collected


async def run():
    """Main intake loop — runs once per cycle, then sleeps."""
    log.info("Starting intake cycle")
    recent_frns = await _get_recent_frns()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await ctx.new_page()
        await page.goto(config.FCC_LISTINGS_URL, timeout=60000)

        new_rows = await _scrape_pages(page, recent_frns)
        await browser.close()

    if not new_rows:
        log.info("No new listings found — DB is up to date")
        return

    # Bulk write to staging
    log.batch_start(len(new_rows))
    await db.bulk_upsert_staging(new_rows)
    log.batch_end(len(new_rows), 0)

    # Push to normalize queue
    await redis_client.push_batch(config.Q_NORMALIZE, new_rows)
    log.info(f"Pushed {len(new_rows)} rows to normalize_queue")


async def loop():
    """Continuous loop wrapper."""
    while True:
        try:
            await run()
        except Exception as e:
            log.error(f"Intake cycle error: {e}")
        await asyncio.sleep(config.IDLE_SLEEP * 12)  # Intake runs less frequently
