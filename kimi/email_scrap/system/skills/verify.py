"""
Verify Skill
Async FCC Form 499 status checker using curl_cffi with browser impersonation.
Ported from verify_leads_fast.py — now a pure async function.
"""
import asyncio
import random
import re
from typing import Any, Dict, List

from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

from system.queues import redis_client

FCC_FORM_499_URL = "https://apps.fcc.gov/cgb/form499/499results.cfm"

BROWSER_PROFILES = [
    {"impersonate": "chrome120", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"},
    {"impersonate": "chrome119", "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36"},
    {"impersonate": "chrome110", "ua": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/110.0.0.0 Safari/537.36"},
    {"impersonate": "edge101",   "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Edg/101.0.1210.53"},
    {"impersonate": "safari15_5","ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Version/15.5 Safari/605.1.15"},
]


def _parse_fcc_results(html: str, result: Dict) -> bool:
    """Parse FCC Form 499 HTML for a single-record match."""
    if "1 Record Found" not in html:
        return False
    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        header_found = False
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            texts = [c.get_text(strip=True) for c in cells]
            if "499 Filer ID" in texts:
                header_found = True
                continue
            if header_found and len(cells) >= 2:
                filer_id = cells[0].get_text(strip=True)
                if filer_id.isdigit() and len(filer_id) >= 4:
                    result["filer_id"] = filer_id
                    result["legal_name"] = cells[1].get_text(strip=True)
                    if len(cells) > 2:
                        result["dba"] = cells[2].get_text(strip=True)
                    result["status"] = "Active"
                    return True
    result["status"] = "Active (Parse Fail)"
    return True


async def verify_single(frn: str, company_name: str = "") -> Dict[str, Any]:
    """Verify a single FRN against FCC Form 499. Returns status dict."""
    # Check Redis cache first
    cached = await redis_client.cache_get(f"verify:{frn}")
    if cached:
        return {"frn": frn, "status": cached, "_cached": True}

    params = {
        "FilerID": "", "frn": frn, "operational": "",
        "comm_type": "Any Type", "LegalName": "",
        "state": "Any State", "R1": "and", "XML": "FALSE",
    }
    result = {"frn": frn, "status": "Unknown", "filer_id": "", "legal_name": "", "dba": ""}

    for attempt in range(3):
        profile = random.choice(BROWSER_PROFILES)
        headers = {
            "Referer": "https://apps.fcc.gov/cgb/form499/499a.cfm",
            "User-Agent": profile["ua"],
        }
        try:
            async with AsyncSession(impersonate=profile["impersonate"], headers=headers) as session:
                resp = await session.get(FCC_FORM_499_URL, params=params, timeout=45)
                if resp.status_code == 200:
                    if "Access Denied" in resp.text:
                        if attempt < 2:
                            await asyncio.sleep(random.uniform(1, 3))
                            continue
                        result["status"] = "BLOCKED"
                        break
                    if _parse_fcc_results(resp.text, result):
                        # Cache the positive result for 1 hour
                        await redis_client.cache_set(f"verify:{frn}", result["status"], ttl=3600)
                        break
                    result["status"] = "Not Found"
                    await redis_client.cache_set(f"verify:{frn}", "Not Found", ttl=3600)
                    break
                elif resp.status_code in (429, 500, 502, 503, 504):
                    await asyncio.sleep(random.uniform(2, 5))
                    continue
                else:
                    result["status"] = f"HTTP {resp.status_code}"
                    break
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(random.uniform(1, 3))
                continue
            result["status"] = "Error"
            result["error"] = f"{type(e).__name__}: {str(e)[:100]}"

    return result


async def verify_batch(
    items: List[Dict[str, Any]], concurrency: int = 50
) -> List[Dict[str, Any]]:
    """
    Verify a batch of items concurrently.
    Each item must have 'frn' and optionally 'business_name'.
    Returns list of items enriched with verification results.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _check(item: Dict) -> Dict:
        async with sem:
            await asyncio.sleep(random.uniform(0.01, 0.1))
            res = await verify_single(item["frn"], item.get("business_name", ""))
            item["verify_status"] = res["status"]
            item["filer_id"] = res.get("filer_id", "")
            item["legal_name_499"] = res.get("legal_name", "")
            return item

    tasks = [_check(item) for item in items]
    return await asyncio.gather(*tasks, return_exceptions=False)
