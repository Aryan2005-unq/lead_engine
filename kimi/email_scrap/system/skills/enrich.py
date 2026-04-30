"""
Enrich Skill
Playwright-based email scraper with context reuse, image/font blocking,
strict timeouts, and limited concurrency.
"""
import asyncio
import re
from typing import Any, Dict, List, Optional

from playwright.async_api import async_playwright, Browser, BrowserContext


_browser: Optional[Browser] = None
_context: Optional[BrowserContext] = None


async def get_browser_context() -> BrowserContext:
    """Lazily launch a shared browser + context with optimized settings."""
    global _browser, _context
    if _browser is None or not _browser.is_connected():
        pw = await async_playwright().start()
        _browser = await pw.chromium.launch(headless=True)
        _context = await _browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        # Block images, fonts, media for speed
        await _context.route(
            "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,ico,mp4,webm}",
            lambda route: route.abort(),
        )
    return _context


async def close_browser():
    global _browser, _context
    if _browser:
        await _browser.close()
        _browser = None
        _context = None


async def enrich_single(lead: Dict[str, Any], timeout: int = 30000) -> Dict[str, Any]:
    """
    Visit the FCC profile page for a lead and extract the contact email.
    Returns the lead dict with 'enriched_email' set (or empty string).
    """
    frn = lead.get("frn", "")
    if not frn:
        lead["enriched_email"] = ""
        lead["enrich_error"] = "missing frn"
        return lead

    url = f"https://fccprod.servicenowservices.com/rmd?id=rmd_listings&sysparm_search={frn}"
    ctx = await get_browser_context()
    page = None
    try:
        page = await ctx.new_page()
        await page.goto(url, timeout=timeout)

        # Try the known email input field first
        try:
            selector = "#sp_formfield_contact_email"
            await page.wait_for_selector(selector, timeout=15000)
            email = await page.input_value(selector)
            if email and "@" in email:
                lead["enriched_email"] = email.strip()
                return lead
        except Exception:
            pass

        # Fallback: regex scan page content
        content = await page.content()
        emails = re.findall(r"[\w\.\-+]+@[\w\.\-]+\.\w+", content)
        valid = [e for e in emails if "fcc.gov" not in e and "servicenow" not in e]
        lead["enriched_email"] = valid[0] if valid else ""

    except Exception as e:
        lead["enriched_email"] = ""
        lead["enrich_error"] = f"{type(e).__name__}: {str(e)[:80]}"
    finally:
        if page:
            try:
                await page.close()
            except Exception:
                pass

    return lead


async def enrich_batch(
    leads: List[Dict[str, Any]], concurrency: int = 3
) -> List[Dict[str, Any]]:
    """
    Enrich a batch of leads concurrently using a semaphore to cap
    the number of simultaneous browser pages.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _worker(lead: Dict) -> Dict:
        async with sem:
            return await enrich_single(lead)

    return await asyncio.gather(*[_worker(ld) for ld in leads])
