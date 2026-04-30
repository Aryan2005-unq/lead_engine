"""
Normalize Skill
Pure business logic: deduplicate, clean company names, standardize fields.
"""
import re
from typing import Any, Dict, List, Set


def normalize_company_name(name: str) -> str:
    """Standardize a company name for dedup and display."""
    if not name:
        return ""
    # Strip whitespace, collapse multiple spaces
    name = re.sub(r"\s+", " ", name.strip())
    # Title-case while preserving known abbreviations
    abbreviations = {"LLC", "INC", "LLP", "LP", "DBA", "USA", "FCC", "CO"}
    words = name.split()
    result = []
    for w in words:
        if w.upper() in abbreviations:
            result.append(w.upper())
        else:
            result.append(w.capitalize())
    return " ".join(result)


def normalize_batch(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process a batch of staging rows:
    1. Clean company names
    2. Deduplicate by FRN within the batch
    3. Fill defaults for missing fields
    Returns deduplicated, normalized rows.
    """
    seen_frns: Set[str] = set()
    output: List[Dict[str, Any]] = []

    for row in rows:
        frn = str(row.get("frn", "")).strip()
        if not frn or frn in seen_frns:
            continue
        seen_frns.add(frn)

        row["business_name"] = normalize_company_name(
            row.get("business_name", "")
        )
        # Ensure other_data is a dict
        if not isinstance(row.get("other_data"), dict):
            row["other_data"] = {}

        output.append(row)

    return output
