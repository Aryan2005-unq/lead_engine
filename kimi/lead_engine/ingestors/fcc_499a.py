"""FCC 499A Ingestor -- Playwright + BS4."""
import logging, re
from bs4 import BeautifulSoup
from ingestors.base import BaseIngestor
from browser import fetch_page_with_wait, fetch_with_form_submit

logger = logging.getLogger(__name__)
FCC_SEARCH_URL = "https://apps.fcc.gov/cgb/form499/499a.cfm"
FCC_RESULTS_URL = "https://apps.fcc.gov/cgb/form499/499Results.cfm"

class FCC499AIngestor(BaseIngestor):
    @property
    def source_name(self): return "FCC_499A"

    def extract(self):
        all_companies = {}
        max_terms = 2  # Avoid long hangs on unstable FCC endpoint.
        search_terms = [
            ("Interconnected VoIP", "Interconnected VoIP"),
            ("Interexchange", "IXC"),
            ("Competitive Local Exchange", "CLEC"),
            ("Toll Reseller", "Toll Reseller"),
        ]
        for term, ctype in search_terms[:max_terms]:
            logger.info("Searching FCC 499A: '%s'", term)
            try:
                # Use Playwright to submit the FCC search form
                html = fetch_with_form_submit(
                    url=FCC_SEARCH_URL,
                    form_data={
                        'input[name="filerName"]': "%",
                        'select[name="filerType"]': term,
                    },
                    submit_selector='input[type="submit"]',
                    wait_selector="table",
                    timeout=30000,
                )
                if not html:
                    # Fallback: try direct POST via Playwright page fetch
                    html = fetch_page_with_wait(
                        f"{FCC_RESULTS_URL}?filerName=%25&filerType={term.replace(' ', '+')}",
                        wait_selector="table",
                        timeout=30000,
                    )
                if not html:
                    logger.warning("No results for '%s'", term)
                    continue

                results = self._parse_html(html, ctype)
                for c in results:
                    frn = c.get("source_id", "")
                    if frn and frn not in all_companies:
                        all_companies[frn] = c
                logger.info("Found %d for '%s' (total: %d)", len(results), term, len(all_companies))
            except Exception as e:
                logger.warning("FCC search failed for '%s': %s", term, e)
        return list(all_companies.values()), []

    def _parse_html(self, html, comm_type):
        soup = BeautifulSoup(html, "html.parser")
        companies = []
        tables = soup.find_all("table")
        if not tables: return companies
        results_table = max(tables, key=lambda t: len(t.find_all("tr")))
        for row in results_table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if len(cells) < 4: continue
            frn = cells[0].get_text(strip=True)
            filer = cells[1].get_text(strip=True)
            dba = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            city = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            state = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            if not filer or not frn: continue
            companies.append({
                "company_name": dba or filer,
                "company_domain": None,
                "company_type": comm_type,
                "country": "USA",
                "state": state,
                "address": f"{city}, {state}" if city else state,
                "services": comm_type,
                "source_id": frn,
                "raw_data": {"frn": frn, "filer_name": filer, "dba_name": dba},
            })
        return companies
