"""Competitor Partner Pages -- Playwright + BS4."""
import logging, re
from bs4 import BeautifulSoup
from ingestors.base import BaseIngestor
from config import COMPETITOR_DOMAINS
from browser import fetch_multiple_pages

logger = logging.getLogger(__name__)
PATHS = ["/partners","/partner","/about","/our-partners","/case-studies","/customers","/carrier-partners"]
SKIP = ["logo","partner","customer","read more","learn more","view","download","click here","image","icon","close","placeholder","default","banner","hero","background"]

class CompetitorPagesIngestor(BaseIngestor):
    @property
    def source_name(self): return "Competitor"

    def extract(self):
        companies, seen = [], set()
        for domain, comp_name in COMPETITOR_DOMAINS.items():
            logger.info("Scraping competitor: %s via Playwright", comp_name)
            urls = [f"https://www.{domain}{p}" for p in PATHS]
            page_results = fetch_multiple_pages(urls)

            for url, html in page_results.items():
                if not html: continue
                soup = BeautifulSoup(html, "html.parser")
                names = set()

                # Logo alt text
                for img in soup.find_all("img"):
                    alt = (img.get("alt") or "").strip()
                    if alt and 2 < len(alt) < 100 and not any(s in alt.lower() for s in SKIP) and domain.split(".")[0] not in alt.lower():
                        names.add(alt)

                # Partner/customer sections
                for sec in soup.find_all(["div","section"], class_=re.compile(r"partner|customer|client|logo|case", re.I)):
                    for h in sec.find_all(["h3","h4","h5","strong"]):
                        t = h.get_text(strip=True)
                        if t and 2 < len(t) < 100 and not any(s in t.lower() for s in SKIP):
                            names.add(t)

                # Case study links
                for a in soup.find_all("a", href=re.compile(r"case.stud", re.I)):
                    t = a.get_text(strip=True)
                    if t and 2 < len(t) < 100: names.add(t)

                for name in names:
                    if name.lower() not in seen:
                        seen.add(name.lower())
                        companies.append({
                            "company_name": name,
                            "company_domain": None,
                            "source_id": f"competitor_{comp_name}_{name[:40]}",
                            "raw_data": {"competitor": comp_name, "source_url": url},
                        })

            logger.info("  %s: %d partner companies", comp_name, sum(1 for c in companies if comp_name in c.get("source_id","")))
        logger.info("Total competitor partners: %d", len(companies))
        return companies, []
