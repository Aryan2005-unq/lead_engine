"""
Lead Extraction Engine -- Utility Functions.
"""

import re
import time
import json
import hashlib
import logging
from urllib.parse import urlparse
from datetime import datetime

logger = logging.getLogger(__name__)


def extract_domain(url: str) -> str | None:
    if not url:
        return None
    url = url.strip().lower()
    if "@" in url and "/" not in url:
        return url.split("@")[-1]
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        domain = re.sub(r"^www\.", "", domain)
        return domain if domain else None
    except Exception:
        return None


def normalize_company_name(name: str) -> str:
    if not name:
        return ""
    name = name.strip()
    suffixes = [
        r"\bLLC\b", r"\bL\.L\.C\.\b", r"\bInc\.?\b", r"\bCorp\.?\b",
        r"\bCorporation\b", r"\bLtd\.?\b", r"\bLimited\b", r"\bLLP\b",
        r"\bL\.P\.\b", r"\bCo\.?\b", r"\bCompany\b", r"\bd/b/a\b", r"\bDBA\b",
    ]
    for suffix in suffixes:
        name = re.sub(suffix, "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s+", " ", name).strip()
    name = name.rstrip(".,- ")
    return name


def normalize_email(email: str) -> str | None:
    if not email:
        return None
    email = email.strip().lower()
    if re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email):
        return email
    return None


def classify_seniority(title: str) -> str:
    if not title:
        return "Unknown"
    title_lower = title.lower()
    if any(kw in title_lower for kw in ["ceo", "cto", "coo", "cfo", "chief", "founder", "president", "owner"]):
        return "C-Level"
    if any(kw in title_lower for kw in ["vp", "vice president", "svp", "evp", "head of"]):
        return "VP"
    if any(kw in title_lower for kw in ["director", "dir."]):
        return "Director"
    if any(kw in title_lower for kw in ["manager", "mgr", "lead", "supervisor"]):
        return "Manager"
    return "Individual"


def generate_linkedin_search_url(name: str, company: str = None) -> str:
    query_parts = [f'"{name}"']
    if company:
        query_parts.append(f'"{company}"')
    query = " ".join(query_parts)
    return f"https://www.linkedin.com/search/results/people/?keywords={query}&origin=GLOBAL_SEARCH_HEADER"


class RateLimiter:
    def __init__(self, max_per_minute: int):
        self.interval = 60.0 / max_per_minute
        self.last_request_time = 0.0

    def wait(self):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self.last_request_time = time.time()


def merge_json_arrays(existing: list, new_items: list) -> list:
    if not existing:
        existing = []
    if not new_items:
        new_items = []
    return sorted(list(set(existing + new_items)))


def pick_best_value(*values):
    for val in values:
        if val and str(val).strip():
            return str(val).strip()
    return None


def hash_record(data: dict) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def now_iso() -> str:
    return datetime.utcnow().isoformat()
