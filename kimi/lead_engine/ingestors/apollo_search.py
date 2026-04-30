"""
Apollo.io Search Ingestor.

Unlike apollo_enrich.py (which enriches EXISTING companies),
this ingestor proactively SEARCHES Apollo for new wholesale VoIP
prospects by industry + job title criteria.

This is a discovery tool -- finds companies and contacts we
don't already know about.
"""

import logging
import requests

from ingestors.base import BaseIngestor
from config import APOLLO_API_KEY, APOLLO_RATE_LIMIT, TARGET_TITLES
from utils import (
    RateLimiter,
    extract_domain,
    normalize_email,
    classify_seniority,
    generate_linkedin_search_url,
)

logger = logging.getLogger(__name__)

APOLLO_URL = "https://api.apollo.io/api/v1"
rate_limiter = RateLimiter(max_per_minute=APOLLO_RATE_LIMIT)

# Industry keywords to search for
INDUSTRY_FILTERS = [
    "telecommunications",
    "internet",
    "wireless",
]

# Locations to search (strictly aligned with target market)
LOCATIONS = ["United States", "Canada"]


def _apollo_headers():
    return {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}


class ApolloSearchIngestor(BaseIngestor):
    """
    Proactive Apollo.io search for wholesale VoIP decision-makers.

    Searches by title + industry + location to discover NEW companies
    and contacts not found through other sources.
    """

    @property
    def source_name(self) -> str:
        return "Apollo"

    def extract(self) -> tuple[list[dict], list[dict]]:
        if not APOLLO_API_KEY or APOLLO_API_KEY == "your_apollo_api_key_here":
            logger.warning("Apollo API key not configured. Skipping search.")
            return [], []

        all_companies = {}
        all_contacts = []
        seen_emails = set()

        # Search for each target title in telecom industry
        search_titles = TARGET_TITLES[:6]  # Top 6 most relevant titles

        for title in search_titles:
            for location in LOCATIONS:
                logger.info("Apollo search: '%s' in %s", title, location)
                try:
                    people = self._search_people(title, location)
                    for person in people:
                        # Extract company
                        org = person.get("organization", {}) or {}
                        company_name = org.get("name", "")
                        if not company_name:
                            continue

                        domain = extract_domain(org.get("website_url", ""))
                        company_key = domain or company_name.lower().strip()

                        if company_key not in all_companies:
                            all_companies[company_key] = {
                                "company_name": company_name,
                                "company_domain": domain,
                                "company_type": self._classify_org(org),
                                "company_size": org.get("estimated_num_employees", ""),
                                "country": org.get("country", ""),
                                "state": org.get("state", ""),
                                "about": org.get("short_description", ""),
                                "website_url": org.get("website_url", ""),
                                "tech_stack": None,
                                "source_id": f"apollo_{org.get('id', company_name[:50])}",
                                "raw_data": {
                                    "apollo_org_id": org.get("id"),
                                    "industry": org.get("industry", ""),
                                    "founded_year": org.get("founded_year"),
                                },
                            }

                        # Extract contact (keep even without email so ID-based enrichment can run later).
                        email = normalize_email(person.get("email", ""))
                        if email and email in seen_emails:
                            continue
                        if email:
                            seen_emails.add(email)
                        name = (person.get("name") or "").strip()
                        if not name:
                            # api_search often returns first_name + obfuscated last name.
                            first = (person.get("first_name") or "").strip()
                            last_obf = (person.get("last_name_obfuscated") or "").strip()
                            name = " ".join([p for p in (first, last_obf) if p]).strip()
                        if not name and not person.get("id"):
                            continue
                        job_title = person.get("title", "")
                        phone_numbers = person.get("phone_numbers", [])
                        phone = (
                            phone_numbers[0].get("sanitized_number", "")
                            if phone_numbers
                            else ""
                        )
                        linkedin = person.get("linkedin_url", "")
                        linkedin_search = ""
                        if not linkedin:
                            linkedin_search = generate_linkedin_search_url(
                                name, company_name
                            )

                        all_contacts.append({
                            "full_name": name,
                            "job_title": job_title,
                            "seniority": classify_seniority(job_title),
                            "email": email,
                            "email_verified": bool(email),
                            "email_confidence": 85 if email else 0,
                            "phone": phone,
                            "linkedin_url": linkedin,
                            "_co_key": all_companies[company_key]["source_id"],
                            "raw_data": {
                                "apollo_person_id": person.get("id"),
                                "company_name": company_name,
                                "search_title": title,
                                "search_location": location,
                            },
                        })

                    logger.info(
                        "  Found %d people, %d unique companies so far",
                        len(people), len(all_companies),
                    )

                except Exception as e:
                    logger.warning(
                        "Apollo search failed for '%s' in %s: %s",
                        title, location, e,
                    )

        # Fallback: broader discovery if strict title+location filters return nothing.
        if not all_companies:
            logger.info("Apollo strict search returned 0; running broad fallback search")
            for kw in ("wholesale voice", "carrier", "telecom", "sip trunking"):
                for person in self._search_people_broad(kw):
                    org = person.get("organization", {}) or {}
                    company_name = (org.get("name", "") or "").strip()
                    if not company_name:
                        continue
                    company_key = company_name.lower()
                    if company_key not in all_companies:
                        all_companies[company_key] = {
                            "company_name": company_name,
                            "company_domain": None,
                            "company_type": self._classify_org(org),
                            "company_size": None,
                            "country": None,
                            "state": None,
                            "about": None,
                            "website_url": None,
                            "tech_stack": None,
                            "source_id": f"apollo_{company_name[:50]}",
                            "raw_data": {
                                "apollo_org_name": company_name,
                                "search_keyword": kw,
                            },
                        }
                    # Keep contact candidates for downstream ID-based enrichment.
                    email = normalize_email(person.get("email", ""))
                    if email and email in seen_emails:
                        continue
                    if email:
                        seen_emails.add(email)
                    name = (person.get("name") or "").strip()
                    if not name:
                        first = (person.get("first_name") or "").strip()
                        last_obf = (person.get("last_name_obfuscated") or "").strip()
                        name = " ".join([p for p in (first, last_obf) if p]).strip()
                    if not name and not person.get("id"):
                        continue
                    job_title = person.get("title", "")
                    all_contacts.append({
                        "full_name": name,
                        "job_title": job_title,
                        "seniority": classify_seniority(job_title),
                        "email": email,
                        "email_verified": bool(email),
                        "email_confidence": 85 if email else 0,
                        "phone": None,
                        "linkedin_url": person.get("linkedin_url", ""),
                        "_co_key": all_companies[company_key]["source_id"],
                        "raw_data": {
                            "apollo_person_id": person.get("id"),
                            "company_name": company_name,
                            "search_keyword": kw,
                            "fallback": "broad_api_search",
                        },
                    })

        logger.info(
            "Apollo search complete: %d companies, %d contacts",
            len(all_companies), len(all_contacts),
        )
        return list(all_companies.values()), all_contacts

    def _search_people(self, title: str, location: str, max_pages: int = 3) -> list:
        """
        Search Apollo for people matching title + industry + location.
        Returns list of person dicts from the API.
        """
        all_people = []

        for page in range(1, max_pages + 1):
            rate_limiter.wait()

            payload = {
                "person_titles": [title],
                "q_organization_industry_tag_ids": [],
                "person_locations": [location],
                "organization_industry_tag_ids": [],
                "q_keywords": "wholesale voice OR VoIP OR carrier OR telecom",
                "page": page,
                "per_page": 25,
            }

            try:
                response = requests.post(
                    f"{APOLLO_URL}/mixed_people/api_search",
                    json=payload,
                    headers=_apollo_headers(),
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                people = data.get("people", [])
                if not people:
                    break

                all_people.extend(people)

                # Check pagination
                total = data.get("pagination", {}).get("total_entries", 0)
                if page * 25 >= total:
                    break

            except requests.RequestException as e:
                logger.warning("Apollo API error on page %d: %s", page, e)
                break

        return all_people

    def _search_people_broad(self, keyword: str, max_pages: int = 2) -> list:
        all_people = []
        for page in range(1, max_pages + 1):
            rate_limiter.wait()
            try:
                response = requests.post(
                    f"{APOLLO_URL}/mixed_people/api_search",
                    json={"q_keywords": keyword, "page": page, "per_page": 25},
                    headers=_apollo_headers(),
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()
                people = data.get("people", [])
                if not people:
                    break
                all_people.extend(people)
            except requests.RequestException as e:
                logger.warning("Apollo broad search failed for '%s' page %d: %s", keyword, page, e)
                break
        return all_people

    @staticmethod
    def _classify_org(org: dict) -> str:
        """Classify organization type from Apollo data."""
        industry = (org.get("industry", "") or "").lower()
        keywords = (org.get("keywords", []) or [])
        keyword_str = " ".join(keywords).lower() if keywords else ""
        combined = f"{industry} {keyword_str}"

        if "voip" in combined or "voice over" in combined:
            return "Interconnected VoIP"
        if "telecom" in combined or "carrier" in combined:
            return "Carrier"
        if "call center" in combined or "contact center" in combined:
            return "Call Center"
        if "internet" in combined or "isp" in combined:
            return "ISP"
        return "Telecom"
