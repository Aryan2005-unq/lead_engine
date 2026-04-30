"""Job Board Mining -- Playwright + BS4."""
import logging, re
from bs4 import BeautifulSoup
import requests
from ingestors.base import BaseIngestor
from browser import fetch_page_with_wait
from config import APOLLO_API_KEY

logger = logging.getLogger(__name__)
QUERIES = ["wholesale voice","carrier relations telecom","route manager VoIP","voice termination","SIP trunking wholesale","interconnect manager telecom"]

class JobBoardIngestor(BaseIngestor):
    @property
    def source_name(self): return "JobBoard"

    def extract(self):
        companies, seen = [], set()
        for q in QUERIES:
            logger.info("Searching Indeed: '%s' via Playwright", q)
            try:
                url = f"https://www.indeed.com/jobs?q={q.replace(' ', '+')}&l=United+States&sort=date"
                html = fetch_page_with_wait(url, wait_selector=".job_seen_beacon,.resultContent,.jobsearch-ResultsList", timeout=30000)
                if not html:
                    logger.warning("No results for '%s'", q)
                    continue

                soup = BeautifulSoup(html, "html.parser")

                # Indeed uses multiple card patterns
                for card in soup.find_all("div", class_=re.compile(r"job_seen|cardOutline|result|slider_container")):
                    # Company name
                    ce = card.find(attrs={"data-testid": "company-name"})
                    if not ce: ce = card.find("span", class_=re.compile(r"company", re.I))
                    if not ce: ce = card.find("span", attrs={"data-testid": re.compile(r"company", re.I)})
                    if not ce: continue
                    name = ce.get_text(strip=True)
                    if not name or len(name) < 2 or name.lower() in seen: continue

                    # Location
                    loc_el = card.find(attrs={"data-testid": "text-location"})
                    location = loc_el.get_text(strip=True) if loc_el else ""

                    # Job title
                    title_el = card.find("h2") or card.find(class_=re.compile(r"title", re.I))
                    job_title = title_el.get_text(strip=True) if title_el else q

                    # Tech stack from snippet
                    snippet_el = card.find("div", class_=re.compile(r"snippet|description|metadata", re.I))
                    snippet = snippet_el.get_text(strip=True) if snippet_el else ""
                    tech = self._extract_tech(snippet)

                    seen.add(name.lower())
                    companies.append({
                        "company_name": name,
                        "company_domain": None,
                        "company_type": None,
                        "country": "USA",
                        "state": location,
                        "tech_stack": tech,
                        "source_id": f"indeed_{name[:50]}",
                        "raw_data": {"job_title": job_title, "location": location, "query": q, "snippet": snippet[:500]},
                    })
                logger.info("  '%s': %d unique companies so far", q, len(seen))
            except Exception as e:
                logger.warning("Indeed error for '%s': %s", q, e)

        # Fallback when Indeed blocks scraping: infer hiring companies via Apollo people search.
        if not companies and APOLLO_API_KEY:
            logger.info("Indeed yielded 0; running Apollo hiring-intent fallback")
            headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
            for q in QUERIES:
                try:
                    resp = requests.post(
                        "https://api.apollo.io/api/v1/mixed_people/api_search",
                        json={"q_keywords": q, "per_page": 30, "page": 1},
                        headers=headers,
                        timeout=30,
                    )
                    resp.raise_for_status()
                    people = (resp.json() or {}).get("people", [])
                    for person in people:
                        org = person.get("organization", {}) or {}
                        name = (org.get("name", "") or "").strip()
                        if not name or name.lower() in seen:
                            continue
                        seen.add(name.lower())
                        companies.append({
                            "company_name": name,
                            "company_domain": None,
                            "company_type": None,
                            "country": None,
                            "state": None,
                            "tech_stack": None,
                            "source_id": f"apollo_job_{name[:50]}",
                            "raw_data": {"query": q, "fallback": "apollo_api_search"},
                        })
                except Exception as e:
                    logger.warning("Apollo job fallback failed for '%s': %s", q, e)
        return companies, []

    @staticmethod
    def _extract_tech(text):
        kws = ["VOS3000","VOS5000","PortaSwitch","PortaBilling","FreeSWITCH","Odin","SBC","Session Border","Odin","Odin Media"]
        found = []
        tl = text.lower()
        for k in kws:
            if k.lower() in tl and k not in found: found.append(k)
        return ", ".join(found) if found else None
