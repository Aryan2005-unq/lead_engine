"""Event Website Scraper -- Playwright + BS4."""
import logging, re
from bs4 import BeautifulSoup
from ingestors.base import BaseIngestor
from browser import fetch_multiple_pages
from utils import classify_seniority

logger = logging.getLogger(__name__)
EVENT_PAGES = {
    "ITW_2026": {
        "name": "International Telecoms Week 2026",
        "urls": [
            "https://internationaltelecomsweek.com/attending-companies",
            "https://internationaltelecomsweek.com/2026-speakers",
            "https://internationaltelecomsweek.com/sponsors/2026-exhibitors",
        ],
    },
    "Capacity_Europe": {
        "name": "Capacity Europe",
        "urls": [
            "https://www.capacityeurope.com/involved/exhibitors",
            "https://www.capacityeurope.com/agenda/speaker-list",
        ],
    },
}

UI_SKIP = {"read more","learn more","view all","register","sign up","contact us","home","menu","close","search","next","previous","copyright"}

class EventScraperIngestor(BaseIngestor):
    @property
    def source_name(self): return "Event_Scrape"

    def extract(self):
        companies, contacts, seen = [], [], set()
        for ek, ec in EVENT_PAGES.items():
            logger.info("Scraping event: %s via Playwright", ec["name"])
            # Fetch all pages for this event in one browser session
            page_results = fetch_multiple_pages(ec["urls"])

            for url, html in page_results.items():
                if not html: continue
                soup = BeautifulSoup(html, "html.parser")
                is_speakers = "speaker" in url.lower()

                if is_speakers:
                    # Speaker pages: extract name + title + company
                    for block in soup.find_all(["div","article","li"], class_=re.compile(r"speaker|paneli|person|bio", re.I)):
                        name_el = block.find(["h2","h3","h4","strong","b"])
                        name = name_el.get_text(strip=True) if name_el else ""
                        details = [p.get_text(strip=True) for p in block.find_all(["p","span","div"]) if p.get_text(strip=True) != name]
                        title = details[0] if details else ""
                        company_name = details[1] if len(details) > 1 else ""
                        co_key = f"{ek}_{company_name[:50]}" if company_name else None
                        if company_name and company_name.lower() not in seen:
                            seen.add(company_name.lower())
                            companies.append({"company_name":company_name,"company_domain":None,"source_id":co_key,"raw_data":{"event":ec["name"]}})
                        if name:
                            contacts.append({
                                "full_name": name,
                                "job_title": title,
                                "seniority": classify_seniority(title),
                                "email": None,
                                "phone": None,
                                "_co_key": co_key,
                                "raw_data": {"event": ec["name"], "company": company_name},
                            })
                else:
                    # Company/exhibitor pages
                    for elem in soup.find_all(["div","li","span","a","h3","h4","h5"], class_=re.compile(r"company|attendee|exhibitor|sponsor|logo|partner", re.I)):
                        name = elem.get_text(strip=True)
                        if name and 3 < len(name) < 200 and name.lower() not in UI_SKIP and name.lower() not in seen:
                            seen.add(name.lower())
                            companies.append({"company_name":name,"company_domain":None,"source_id":f"{ek}_{name[:50]}","raw_data":{"event":ec["name"]}})
                    # Fallback: headings
                    if not companies:
                        for h in soup.find_all(["h3","h4","h5"]):
                            name = h.get_text(strip=True)
                            if name and 3 < len(name) < 200 and name.lower() not in UI_SKIP and name.lower() not in seen:
                                seen.add(name.lower())
                                companies.append({"company_name":name,"company_domain":None,"source_id":f"{ek}_{name[:50]}","raw_data":{"event":ec["name"]}})

        logger.info("Event scraping: %d companies, %d speaker contacts", len(companies), len(contacts))
        return companies, contacts
