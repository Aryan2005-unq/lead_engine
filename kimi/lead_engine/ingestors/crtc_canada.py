"""CRTC Canada Ingestor -- OpenData XML feeds."""
import logging
import re
import requests
import xml.etree.ElementTree as ET
from ingestors.base import BaseIngestor

logger = logging.getLogger(__name__)
CRTC_FEED_INDEX_URL = (
    "https://open.canada.ca/data/organization/crtc"
    "?_keywords_limit=0&collection=primary&keywords=Telecommunications+service+providers"
)
CRTC_OPEN_DATA_FALLBACK_FEEDS = {
    "Reseller": "https://applications.crtc.gc.ca/OpenData/CASP/Telecomlist/TelecomList_Reseller_35.xml",
    "ILEC": "https://applications.crtc.gc.ca/OpenData/CASP/Telecomlist/TelecomList_ILEC_41.xml",
}

NOISE_NAMES = {
    "phone", "news", "internet", "public proceedings", "crtc home page",
    "business and licensing", "canadian content", "consultations and hearings",
}
PERSON_LIKE_SUFFIXES = {"mr", "mrs", "ms", "dr"}
COMPANY_KEYWORDS = {
    "inc", "corp", "corporation", "ltd", "limited", "llc", "group", "communications",
    "telecom", "telecommunications", "wireless", "networks", "network", "solutions",
    "systems", "services", "company", "canada", "technologies", "technology",
    "university", "society", "foundation", "association", "institute",
}

class CRTCIngestor(BaseIngestor):
    @property
    def source_name(self): return "CRTC"

    def extract(self):
        companies = {}
        feeds = self._discover_feeds()
        for list_name, feed_url in feeds.items():
            logger.info("Fetching CRTC OpenData feed: %s", list_name)
            xml_text = self._fetch_xml_utf16(feed_url)
            if not xml_text:
                logger.warning("Failed CRTC feed fetch: %s", feed_url)
                continue
            for entry in self._parse_feed(xml_text, list_name, self._company_type_from_list(list_name), feed_url):
                key = entry["company_name"].lower().strip()
                if key not in companies:
                    companies[key] = entry
        logger.info("Found %d companies from CRTC OpenData", len(companies))
        return list(companies.values()), []

    def _discover_feeds(self) -> dict:
        """Discover TelecomList XML feeds from Open Canada; fallback to known stable feeds."""
        discovered = {}
        try:
            resp = requests.get(CRTC_FEED_INDEX_URL, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                matches = set(
                    re.findall(
                        r"https://applications\.crtc\.gc\.ca/OpenData/CASP/Telecomlist/TelecomList_([A-Za-z]+)_\d+\.xml",
                        resp.text,
                    )
                )
                for list_name in matches:
                    discovered[list_name] = (
                        f"https://applications.crtc.gc.ca/OpenData/CASP/Telecomlist/TelecomList_{list_name}_"
                    )
                # Complete exact URLs from html matches.
                for full_url in set(
                    re.findall(
                        r"https://applications\.crtc\.gc\.ca/OpenData/CASP/Telecomlist/TelecomList_[A-Za-z]+_\d+\.xml",
                        resp.text,
                    )
                ):
                    key = full_url.split("TelecomList_")[-1].split("_")[0]
                    discovered[key] = full_url
        except Exception:
            pass
        if not discovered:
            return CRTC_OPEN_DATA_FALLBACK_FEEDS
        return discovered

    @staticmethod
    def _company_type_from_list(list_name: str) -> str:
        ll = (list_name or "").lower()
        if "clec" in ll:
            return "CLEC"
        if "ilec" in ll:
            return "ILEC"
        if "wireless" in ll:
            return "Wireless Carrier"
        if "reseller" in ll:
            return "Carrier"
        return "Carrier"

    @staticmethod
    def _fetch_xml_utf16(url: str) -> str | None:
        try:
            resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code != 200:
                return None
            # CRTC feed is UTF-16 encoded.
            return resp.content.decode("utf-16", errors="ignore")
        except Exception:
            return None

    def _parse_feed(self, xml_text: str, list_name: str, company_type: str, source_url: str) -> list[dict]:
        out = []
        try:
            root = ET.fromstring(xml_text)
        except Exception as e:
            logger.warning("CRTC XML parse failed for %s: %s", list_name, e)
            return out
        if not list(root):
            return out
        container = list(root)[0]
        for node in list(container):
            attrs = node.attrib or {}
            name = (attrs.get("CompanyName1") or "").strip()
            if not self._looks_like_company(name):
                continue
            addr_1 = (attrs.get("AddressLine1") or "").strip()
            city = (attrs.get("City") or "").strip()
            province = (attrs.get("Province") or "").strip()
            postal = (attrs.get("PostalCode") or "").strip()
            addr = ", ".join([p for p in [addr_1, city, province, postal] if p])
            out.append(
                {
                    "company_name": name,
                    "company_domain": None,
                    "company_type": company_type,
                    "country": "Canada",
                    "state": province,
                    "address": addr,
                    "source_id": f"crtc_{list_name}_{name[:40]}",
                    "raw_data": {
                        "source_feed": source_url,
                        "registration_list": list_name,
                        "city": city,
                        "postal_code": postal,
                    },
                }
            )
        return out

    @staticmethod
    def _looks_like_company(name: str) -> bool:
        if not name or len(name) < 3 or len(name) > 180:
            return False
        lowered = name.lower().strip()
        if lowered in NOISE_NAMES:
            return False
        has_keyword = any(k in lowered for k in COMPANY_KEYWORDS)
        if not has_keyword and re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+$", name.strip()):
            return False
        # Reject obvious person-like names (two tokens with no company keywords).
        tokens = [t for t in re.split(r"\s+", lowered) if t]
        if len(tokens) <= 3:
            if not has_keyword and all(t.isalpha() for t in tokens):
                if tokens[0] not in PERSON_LIKE_SUFFIXES:
                    return False
        return True
