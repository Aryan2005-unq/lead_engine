"""FCC Public Notices -- Playwright + BS4."""
import logging, re
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from ingestors.base import BaseIngestor
from database import get_engine_connection
from browser import fetch_page_with_wait

logger = logging.getLogger(__name__)
FCC_DIGEST_URL = "https://www.fcc.gov/proceedings-actions/daily-digest"
FCC_DIGEST_RSS_URLS = [
    "https://www.fcc.gov/news-events/daily-digest.xml",
    "https://www.fcc.gov/rss/daily-digest.xml",
]
KEYWORDS = ["section 214","authorization granted","voip provider","voice service provider","interconnected voip","international carrier","domestic carrier"]

class FCCNoticesIngestor(BaseIngestor):
    @property
    def source_name(self): return "FCC_Notice"

    def extract(self):
        companies, seen = [], set()
        logger.info("Fetching FCC daily digest via Playwright")
        html = fetch_page_with_wait(FCC_DIGEST_URL, wait_selector="article,.field-item,.node-content")
        if html:
            self._extract_from_html(html, companies, seen)
        else:
            logger.warning("Playwright fetch failed for FCC digest; trying RSS fallback")
            for rss_url in FCC_DIGEST_RSS_URLS:
                rss_text = self._fetch_rss(rss_url)
                if not rss_text:
                    continue
                self._extract_from_rss(rss_text, companies, seen)
                if companies:
                    break

        # Add fcc_new_license signals after dedup runs
        if companies:
            self._add_signals(companies)

        logger.info("Found %d companies from FCC notices", len(companies))
        return companies, []

    def _extract_from_html(self, html, companies, seen):
        soup = BeautifulSoup(html, "html.parser")
        for el in soup.find_all(["p", "li", "td", "div"]):
            text = el.get_text(strip=True)
            if not any(kw in text.lower() for kw in KEYWORDS):
                continue
            self._append_company_matches(text, companies, seen)

    def _extract_from_rss(self, rss_text, companies, seen):
        soup = BeautifulSoup(rss_text, "xml")
        for item in soup.find_all("item"):
            text = " ".join(
                [
                    (item.title.get_text(strip=True) if item.title else ""),
                    (item.description.get_text(" ", strip=True) if item.description else ""),
                ]
            ).strip()
            if not text:
                continue
            if not any(kw in text.lower() for kw in KEYWORDS):
                continue
            self._append_company_matches(text, companies, seen)

    def _append_company_matches(self, text, companies, seen):
        for name in self._extract_names(text):
            lname = name.lower()
            if lname in seen:
                continue
            seen.add(lname)
            companies.append(
                {
                    "company_name": name,
                    "company_domain": None,
                    "company_type": "Interconnected VoIP",
                    "country": "USA",
                    "source_id": f"fcc_notice_{name[:50]}",
                    "raw_data": {"notice_text": text[:500], "detected_at": datetime.utcnow().isoformat()},
                }
            )

    @staticmethod
    def _fetch_rss(url):
        try:
            resp = requests.get(
                url,
                timeout=25,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code != 200:
                return None
            return resp.text
        except Exception:
            return None

    def _extract_names(self, text):
        names = []
        patterns = [
            r"application\s+of\s+(.+?)\s+(?:for|to|under)",
            r"authorization\s+to\s+(.+?)\s+(?:to|for|under)",
            r"granted\s+(.+?)\s+(?:authority|authorization|permission)",
        ]
        for pat in patterns:
            for m in re.findall(pat, text, re.I):
                m = re.sub(r"\s+", " ", m.strip().strip(",."))
                if 3 < len(m) < 150: names.append(m)
        return names

    def _add_signals(self, companies):
        try:
            with get_engine_connection() as conn:
                cursor = conn.cursor()
                for c in companies:
                    cursor.execute("SELECT id FROM companies WHERE company_name ILIKE %s LIMIT 1", (f"%{c['company_name']}%",))
                    m = cursor.fetchone()
                    if m:
                        cursor.execute("SELECT 1 FROM signals WHERE company_id=%s AND signal_type='fcc_new_license'", (m["id"],))
                        if not cursor.fetchone():
                            cursor.execute("INSERT INTO signals (company_id,signal_type,signal_detail,points) VALUES (%s,'fcc_new_license','New FCC authorization detected',12)", (m["id"],))
                conn.commit(); cursor.close()
        except Exception as e:
            logger.debug("Signal insert skipped: %s", e)
