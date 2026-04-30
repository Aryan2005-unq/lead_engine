"""TelecomsXchange Scraper -- Playwright + BS4."""
import logging, re
from bs4 import BeautifulSoup
from ingestors.base import BaseIngestor
from browser import fetch_page, fetch_multiple_pages

logger = logging.getLogger(__name__)
TCXC_BASE = "https://www.telecomsxchange.com"
TCXC_PAGES = ["/carriers","/partners","/marketplace","/members","/directory","/sellers"]
SKIP_WORDS = ["read more","sign up","login","register","contact us","home","menu","close","search","telecomsxchange","tcxc","cookie","privacy","terms","copyright"]

class TCXCIngestor(BaseIngestor):
    @property
    def source_name(self): return "TCXC"

    def extract(self):
        all_companies, seen = [], set()
        urls = [f"{TCXC_BASE}{p}" for p in TCXC_PAGES] + [TCXC_BASE]
        logger.info("Fetching %d TCXC pages via Playwright", len(urls))
        page_results = fetch_multiple_pages(urls)

        for url, html in page_results.items():
            if not html: continue
            soup = BeautifulSoup(html, "html.parser")
            names = set()

            # Carrier/member list elements
            for elem in soup.find_all(["div","li","span","a","td"], class_=re.compile(r"carrier|member|seller|buyer|company|provider|partner", re.I)):
                n = elem.get_text(strip=True)
                if self._valid(n): names.add(n)

            # Headings in carrier sections
            for sec in soup.find_all(["section","div"], class_=re.compile(r"carrier|member|directory|list", re.I)):
                for h in sec.find_all(["h2","h3","h4","h5","strong"]):
                    n = h.get_text(strip=True)
                    if self._valid(n): names.add(n)

            # Table rows
            for table in soup.find_all("table"):
                for row in table.find_all("tr")[1:]:
                    cells = row.find_all("td")
                    if cells:
                        n = cells[0].get_text(strip=True)
                        if self._valid(n): names.add(n)

            # Logo alt text (homepage)
            for img in soup.find_all("img"):
                alt = (img.get("alt") or "").strip()
                if alt and 3 < len(alt) < 100 and not any(kw in alt.lower() for kw in ["logo","icon","banner","tcxc","telecomsxchange"]):
                    names.add(alt)

            for name in names:
                if name.lower() not in seen:
                    seen.add(name.lower())
                    all_companies.append({
                        "company_name": name,
                        "company_domain": None,
                        "company_type": "Carrier",
                        "country": None,
                        "services": "Wholesale Voice Termination",
                        "source_id": f"tcxc_{name[:50]}",
                        "raw_data": {"marketplace": "TelecomsXchange", "source_url": url},
                    })
            if names: logger.info("  %s: %d carriers", url.split("/")[-1] or "home", len(names))

        logger.info("TCXC total: %d unique carriers", len(all_companies))
        return all_companies, []

    def _valid(self, name):
        return name and 3 < len(name) < 150 and not any(kw in name.lower() for kw in SKIP_WORDS)
