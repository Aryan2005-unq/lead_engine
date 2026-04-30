"""
Lead Extraction Engine -- Browser Utility.

Provides a shared Playwright browser context for all scrapers.
Uses Chromium in headless mode with anti-detection settings.

Usage:
    from browser import fetch_page, fetch_page_with_wait

    # Simple fetch (wait for load)
    html = fetch_page("https://example.com")

    # Fetch with custom wait (for JS-heavy pages)
    html = fetch_page_with_wait("https://example.com", wait_selector="table")
"""

import logging
from contextlib import contextmanager

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout

logger = logging.getLogger(__name__)

# Shared browser args for stealth
BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


@contextmanager
def get_browser():
    """
    Context manager that yields a Playwright browser page.

    Usage:
        with get_browser() as page:
            page.goto("https://example.com")
            html = page.content()
    """
    pw = sync_playwright().start()
    browser = pw.chromium.launch(
        headless=True,
        args=BROWSER_ARGS,
    )
    context = browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        java_script_enabled=True,
    )
    page = context.new_page()

    try:
        yield page
    finally:
        context.close()
        browser.close()
        pw.stop()


def fetch_page(url: str, timeout: int = 30000) -> str | None:
    """
    Fetch a page using Playwright headless browser.
    Returns the full rendered HTML or None on failure.

    Args:
        url: URL to fetch.
        timeout: Max wait time in ms (default 30s).
    """
    try:
        with get_browser() as page:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            # Small delay to let JS render
            page.wait_for_timeout(2000)
            html = page.content()
            logger.debug("Fetched %s (%d chars)", url, len(html))
            return html
    except PwTimeout:
        logger.warning("Timeout fetching %s", url)
        return None
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", url, e)
        return None


def fetch_page_with_wait(
    url: str,
    wait_selector: str = None,
    wait_text: str = None,
    timeout: int = 30000,
) -> str | None:
    """
    Fetch a page and wait for a specific element or text to appear.
    Better for JS-heavy pages that load content dynamically.

    Args:
        url: URL to fetch.
        wait_selector: CSS selector to wait for (e.g., "table", ".results").
        wait_text: Text content to wait for on the page.
        timeout: Max wait time in ms.
    """
    try:
        with get_browser() as page:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout)

            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=10000)
                except PwTimeout:
                    logger.debug("Selector '%s' not found on %s", wait_selector, url)

            if wait_text:
                try:
                    page.wait_for_function(
                        f'document.body.innerText.includes("{wait_text}")',
                        timeout=10000,
                    )
                except PwTimeout:
                    logger.debug("Text '%s' not found on %s", wait_text, url)

            # Extra settle time for dynamic content
            page.wait_for_timeout(2000)
            return page.content()

    except PwTimeout:
        logger.warning("Timeout fetching %s", url)
        return None
    except Exception as e:
        logger.warning("Failed to fetch %s: %s", url, e)
        return None


def fetch_with_form_submit(
    url: str,
    form_data: dict,
    submit_selector: str = None,
    wait_selector: str = None,
    timeout: int = 60000,
) -> str | None:
    """
    Navigate to a page, fill a form, submit it, and return the result.
    Used for FCC 499A search which requires form POST.

    Args:
        url: URL with the form.
        form_data: Dict of {selector: value} to fill.
        submit_selector: CSS selector for the submit button.
        wait_selector: CSS selector to wait for after submit.
        timeout: Max wait time in ms.
    """
    try:
        with get_browser() as page:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            page.wait_for_timeout(2000)

            # Fill form fields
            for selector, value in form_data.items():
                try:
                    page.fill(selector, value)
                except Exception:
                    # Try select dropdown instead
                    try:
                        # First try matching the option label, then value.
                        page.select_option(selector, label=value)
                    except Exception:
                        try:
                            page.select_option(selector, value=value)
                        except Exception:
                            logger.debug("Could not fill %s", selector)

            # Submit
            if submit_selector:
                page.click(submit_selector)
            else:
                page.keyboard.press("Enter")

            # Wait for results
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=30000)
                except PwTimeout:
                    logger.debug("Results selector not found after submit")

            page.wait_for_timeout(3000)
            return page.content()

    except Exception as e:
        logger.warning("Form submit failed on %s: %s", url, e)
        return None


def fetch_multiple_pages(urls: list[str], timeout: int = 30000) -> dict[str, str]:
    """
    Fetch multiple URLs using a single browser instance for efficiency.
    Returns dict of {url: html_content}.
    """
    results = {}
    try:
        with get_browser() as page:
            for url in urls:
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                    page.wait_for_timeout(2000)
                    results[url] = page.content()
                    logger.debug("Fetched %s", url)
                except Exception as e:
                    logger.debug("Failed %s: %s", url, e)
                    results[url] = None
    except Exception as e:
        logger.warning("Browser session failed: %s", e)

    return results
