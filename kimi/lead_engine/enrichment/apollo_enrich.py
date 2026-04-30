"""
Apollo.io Enrichment Module -- 5 Endpoint Strategy.

Endpoints used:
1. organizations/enrich      -- Get company details (size, industry, tech stack)
2. mixed_people/organization_top_people -- Find decision-makers at a company
3. people/match              -- Match event speakers to emails
4. organizations/job_postings -- Get job postings as intent signals
5. mixed_people/api_search   -- (used by apollo_search.py ingestor, not here)

Credit tracking prevents overspending on free plans.
"""

import json
import logging
import time
from datetime import date, timedelta

import requests

from config import (
    APOLLO_API_KEY,
    APOLLO_RATE_LIMIT,
    APOLLO_MONTHLY_CREDIT_LIMIT,
    APOLLO_DAILY_CREDIT_LIMIT,
    APOLLO_MAX_CREDITS_PER_RUN,
    TARGET_TITLES,
    ENRICH_BATCH_SIZE,
)
from database import get_engine_connection
from utils import (
    RateLimiter,
    normalize_email,
    classify_seniority,
    generate_linkedin_search_url,
)

logger = logging.getLogger(__name__)

APOLLO_BASE = "https://api.apollo.io/api/v1"
rate_limiter = RateLimiter(max_per_minute=APOLLO_RATE_LIMIT)
TARGET_COUNTRY_ALIASES = (
    "usa", "us", "united states", "united states of america",
    "canada", "ca",
)


def _apollo_headers():
    return {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}


def _check_api_key():
    if not APOLLO_API_KEY or APOLLO_API_KEY == "your_apollo_api_key_here":
        logger.warning("Apollo API key not configured. Set APOLLO_API_KEY in .env")
        return False
    return True


def _clean_org_name(company_name: str, full_name: str) -> str:
    """Normalize noisy event-scrape company strings for people/match."""
    if not company_name:
        return ""
    cleaned = company_name.strip()
    if full_name and cleaned.lower().startswith(full_name.strip().lower()):
        cleaned = cleaned[len(full_name):].strip(" ,:-")
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    if parts:
        # Prefer the right-most segment, which is usually the company token.
        cleaned = parts[-1]
    # Avoid passing obvious title fragments as organization names.
    bad_tokens = {
        "ceo", "coo", "cto", "cfo", "chief", "vp", "svp", "director",
        "manager", "head", "founder", "co-founder", "president", "partner",
    }
    if cleaned.lower() in bad_tokens:
        return ""
    return cleaned


def _best_email_from_person(person: dict) -> str | None:
    email = normalize_email(person.get("email", ""))
    if email:
        return email
    personal = person.get("personal_emails") or []
    if isinstance(personal, list):
        for e in personal:
            ne = normalize_email(e or "")
            if ne:
                return ne
    return None


def _get_credits_used():
    today = date.today()
    period_start = today.replace(day=1)
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COALESCE(SUM(credits_used), 0) FROM api_credits WHERE api_name='apollo' AND period_start=%s",
            (period_start,),
        )
        used = cursor.fetchone()[0]
        cursor.close()
    return used


def _get_daily_credits_used():
    today = date.today()
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COALESCE(SUM(credits_used), 0) FROM api_credits WHERE api_name='apollo_daily' AND period_start=%s",
            (today,),
        )
        used = cursor.fetchone()[0]
        cursor.close()
    return used


def _budget_exhausted(monthly_used, daily_used, run_used):
    if monthly_used >= APOLLO_MONTHLY_CREDIT_LIMIT:
        return "monthly_limit"
    if daily_used >= APOLLO_DAILY_CREDIT_LIMIT:
        return "daily_limit"
    if run_used >= APOLLO_MAX_CREDITS_PER_RUN:
        return "run_limit"
    return None


def _record_credits(count):
    if not count:
        return
    today = date.today()
    period_start = today.replace(day=1)
    # last day of the current month
    next_month = (period_start.replace(day=28) + timedelta(days=4)).replace(day=1)
    period_end = next_month - timedelta(days=1)
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO api_credits (api_name, credits_used, credits_limit, period_start, period_end)
               VALUES ('apollo', %s, 10000, %s, %s)
               ON CONFLICT (api_name, period_start)
               DO UPDATE SET credits_used = api_credits.credits_used + EXCLUDED.credits_used""",
            (count, period_start, period_end),
        )
        cursor.execute(
            """INSERT INTO api_credits (api_name, credits_used, credits_limit, period_start, period_end)
               VALUES ('apollo_daily', %s, %s, %s, %s)
               ON CONFLICT (api_name, period_start)
               DO UPDATE SET credits_used = api_credits.credits_used + EXCLUDED.credits_used""",
            (count, APOLLO_DAILY_CREDIT_LIMIT, today, today),
        )
        conn.commit()
        cursor.close()


def _enrich_apollo_raw_contacts_by_id(cursor, monthly_used, daily_used, run_used, limit):
    """High-confidence enrichment path using Apollo person IDs captured in raw contacts."""
    matched = 0
    cursor.execute(
        """
        SELECT
            rc.id AS raw_contact_id,
            rc.full_name,
            rc.job_title,
            rc.linkedin_url,
            rc.raw_data->>'apollo_person_id' AS apollo_person_id,
            c.id AS company_id
        FROM raw_contacts rc
        LEFT JOIN raw_companies rco ON rco.id = rc.raw_company_id
        LEFT JOIN companies c
            ON (c.company_domain IS NOT NULL AND c.company_domain = rco.company_domain)
            OR (c.company_name IS NOT NULL AND c.company_name = rco.company_name)
        WHERE rc.source = 'Apollo'
          AND rc.raw_data ? 'apollo_person_id'
        ORDER BY rc.id DESC
        LIMIT %s
        """,
        (limit,),
    )
    candidates = cursor.fetchall()

    for row in candidates:
        budget_reason = _budget_exhausted(monthly_used, daily_used, run_used)
        if budget_reason:
            break
        apollo_person_id = row["apollo_person_id"]
        if not apollo_person_id:
            continue
        try:
            rate_limiter.wait()
            resp = requests.post(
                f"{APOLLO_BASE}/people/match",
                json={"id": apollo_person_id, "reveal_personal_emails": True},
                headers=_apollo_headers(),
                timeout=15,
            )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "5") or 5)
                if retry_after > 120:
                    break
                time.sleep(max(1, min(retry_after, 30)))
                resp = requests.post(
                    f"{APOLLO_BASE}/people/match",
                    json={"id": apollo_person_id, "reveal_personal_emails": True},
                    headers=_apollo_headers(),
                    timeout=15,
                )
            resp.raise_for_status()
            person = resp.json().get("person", {})
            monthly_used += 1
            daily_used += 1
            run_used += 1
            email = _best_email_from_person(person or {})
            if not email:
                continue
            cursor.execute(
                """
                INSERT INTO contacts (
                    company_id, full_name, job_title, seniority, email,
                    email_verified, email_confidence, phone, linkedin_url,
                    sources, source_count, enriched
                )
                VALUES (%s,%s,%s,%s,%s,TRUE,85,%s,%s,'["Apollo"]'::jsonb,1,TRUE)
                ON CONFLICT (email) DO UPDATE SET
                    full_name = COALESCE(NULLIF(EXCLUDED.full_name,''), contacts.full_name),
                    job_title = COALESCE(NULLIF(EXCLUDED.job_title,''), contacts.job_title),
                    seniority = COALESCE(NULLIF(EXCLUDED.seniority,''), contacts.seniority),
                    phone = COALESCE(NULLIF(EXCLUDED.phone,''), contacts.phone),
                    linkedin_url = COALESCE(NULLIF(EXCLUDED.linkedin_url,''), contacts.linkedin_url),
                    enriched = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    row["company_id"],
                    person.get("name", "") or row["full_name"],
                    person.get("title", "") or row["job_title"],
                    classify_seniority(person.get("title", "") or row["job_title"] or ""),
                    email,
                    (
                        (person.get("phone_numbers") or [{}])[0].get("sanitized_number", "")
                        if isinstance(person.get("phone_numbers"), list)
                        else ""
                    ),
                    person.get("linkedin_url", "") or row["linkedin_url"],
                ),
            )
            matched += 1
        except Exception as e:
            logger.debug("ID-based Apollo enrichment failed for %s: %s", apollo_person_id, e)
    return matched, monthly_used, daily_used, run_used


# -----------------------------------------------------------------------
# Endpoint 1: organizations/enrich
# Input: company domain -> Output: size, industry, tech stack, about
# -----------------------------------------------------------------------

def enrich_company_details(limit=None):
    """Enrich companies that have a domain but missing details."""
    if not _check_api_key():
        return {"status": "skipped", "reason": "No API key"}

    limit = limit or ENRICH_BATCH_SIZE
    monthly_used = _get_credits_used()
    daily_used = _get_daily_credits_used()
    run_used = 0
    budget_reason = _budget_exhausted(monthly_used, daily_used, run_used)
    if budget_reason:
        logger.warning("Apollo credit budget reached (%s). Skipping.", budget_reason)
        return {"status": "skipped", "reason": budget_reason}

    logger.info("=" * 60)
    logger.info("Enriching company details via organizations/enrich")
    logger.info("=" * 60)

    enriched = 0
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        # Companies with domain but missing key details
        cursor.execute(
            """SELECT id, company_name, company_domain FROM companies
            WHERE company_domain IS NOT NULL
            AND (about IS NULL OR company_size IS NULL OR tech_stack IS NULL)
            ORDER BY source_count DESC LIMIT %s""",
            (limit,),
        )
        companies = cursor.fetchall()

        for co in companies:
            budget_reason = _budget_exhausted(monthly_used, daily_used, run_used)
            if budget_reason:
                logger.warning("Apollo company enrichment stopped by budget (%s).", budget_reason)
                break
            try:
                rate_limiter.wait()
                resp = requests.get(
                    f"{APOLLO_BASE}/organizations/enrich",
                    params={"domain": co["company_domain"]},
                    headers=_apollo_headers(),
                    timeout=15,
                )
                resp.raise_for_status()
                org = resp.json().get("organization", {})
                if not org:
                    continue

                cursor.execute(
                    """UPDATE companies SET
                        about = COALESCE(NULLIF(%s, ''), about),
                        company_size = COALESCE(NULLIF(%s, ''), company_size),
                        tech_stack = COALESCE(NULLIF(%s, ''), tech_stack),
                        linkedin_company_url = COALESCE(NULLIF(%s, ''), linkedin_company_url),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s""",
                    (
                        org.get("short_description", ""),
                        org.get("estimated_num_employees", ""),
                        ", ".join(org.get("technology_names", []) or []),
                        org.get("linkedin_url", ""),
                        co["id"],
                    ),
                )
                enriched += 1
                monthly_used += 1
                daily_used += 1
                run_used += 1
            except Exception as e:
                logger.debug("Enrich failed for %s: %s", co["company_domain"], e)

        conn.commit()
        cursor.close()

    _record_credits(run_used)
    logger.info("Enriched %d company profiles", enriched)
    return {"status": "success", "companies_enriched": enriched}


# -----------------------------------------------------------------------
# Endpoint 2: mixed_people/organization_top_people
# Input: company domain -> Output: top decision-makers with emails
# -----------------------------------------------------------------------

def find_decision_makers(limit=None):
    """Find decision-maker contacts at companies missing contacts."""
    if not _check_api_key():
        return {"status": "skipped", "reason": "No API key"}

    limit = limit or ENRICH_BATCH_SIZE
    monthly_used = _get_credits_used()
    daily_used = _get_daily_credits_used()
    if _budget_exhausted(monthly_used, daily_used, 0):
        logger.warning("Apollo budget exceeded; skipping decision-maker search.")
        return {"status": "skipped", "reason": "Credit limit"}

    logger.info("=" * 60)
    logger.info("Finding decision-makers via organization_top_people")
    logger.info("=" * 60)

    enriched = 0
    contacts_found = 0

    with get_engine_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT c.id, c.company_name, c.company_domain FROM companies c
            WHERE c.company_domain IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM contacts ct WHERE ct.company_id=c.id AND ct.enriched=TRUE)
            ORDER BY c.source_count DESC LIMIT %s""",
            (limit,),
        )
        companies = cursor.fetchall()

        for co in companies:
            if _budget_exhausted(monthly_used, daily_used, 0):
                break
            try:
                rate_limiter.wait()
                resp = requests.post(
                    f"{APOLLO_BASE}/mixed_people/api_search",
                    json={
                        "q_organization_domains_list": [co["company_domain"]],
                        "person_titles": TARGET_TITLES[:5],
                        "per_page": 10,
                    },
                    headers=_apollo_headers(),
                    timeout=30,
                )
                resp.raise_for_status()
                people = resp.json().get("people", [])

                for person in people:
                    email = normalize_email(person.get("email", ""))
                    if not email:
                        continue

                    title = person.get("title", "")
                    name = person.get("name", "")
                    linkedin = person.get("linkedin_url", "")
                    phone_nums = person.get("phone_numbers", [])
                    phone = phone_nums[0].get("sanitized_number", "") if phone_nums else ""

                    cursor.execute(
                        """INSERT INTO contacts
                            (company_id, full_name, job_title, seniority, email,
                             email_verified, email_confidence, phone,
                             linkedin_url, linkedin_search_url,
                             sources, source_count, enriched)
                        VALUES (%s,%s,%s,%s,%s,TRUE,85,%s,%s,%s,'["Apollo"]'::jsonb,1,TRUE)
                        ON CONFLICT (email) DO UPDATE SET
                            full_name = COALESCE(NULLIF(EXCLUDED.full_name,''), contacts.full_name),
                            job_title = COALESCE(NULLIF(EXCLUDED.job_title,''), contacts.job_title),
                            seniority = COALESCE(NULLIF(EXCLUDED.seniority,''), contacts.seniority),
                            phone = COALESCE(NULLIF(EXCLUDED.phone,''), contacts.phone),
                            linkedin_url = COALESCE(NULLIF(EXCLUDED.linkedin_url,''), contacts.linkedin_url),
                            enriched = TRUE, updated_at = CURRENT_TIMESTAMP""",
                        (
                            co["id"], name, title, classify_seniority(title), email,
                            phone, linkedin,
                            generate_linkedin_search_url(name, co["company_name"]) if not linkedin else "",
                        ),
                    )
                    contacts_found += 1

                enriched += 1
                conn.commit()
            except Exception as e:
                logger.debug("Top people failed for %s: %s", co["company_name"], e)

        cursor.close()

    logger.info("Searched %d companies, found %d contacts", enriched, contacts_found)
    return {"status": "success", "companies_searched": enriched, "contacts_found": contacts_found}


# -----------------------------------------------------------------------
# Endpoint 3: people/match
# Input: name + company -> Output: email, phone, LinkedIn
# Used for event speakers found without contact info
# -----------------------------------------------------------------------

def match_contacts(limit=50):
    """Match contacts that have a name but no email (e.g., event speakers)."""
    if not _check_api_key():
        return {"status": "skipped", "reason": "No API key"}

    monthly_used = _get_credits_used()
    daily_used = _get_daily_credits_used()
    run_used = 0
    budget_reason = _budget_exhausted(monthly_used, daily_used, run_used)
    if budget_reason:
        return {"status": "skipped", "reason": budget_reason}

    logger.info("=" * 60)
    logger.info("Matching contacts via people/match")
    logger.info("=" * 60)

    matched = 0
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        # First pass: ID-based enrichment from Apollo raw contacts (highest confidence).
        id_matched, monthly_used, daily_used, run_used = _enrich_apollo_raw_contacts_by_id(
            cursor, monthly_used, daily_used, run_used, limit
        )
        matched += id_matched
        conn.commit()

        # Contacts with name but no email (typically from event scraping)
        cursor.execute(
            """SELECT c.id, c.full_name, co.company_name, co.company_domain, co.country
            FROM contacts c
            LEFT JOIN companies co ON c.company_id = co.id
            WHERE c.email IS NULL AND c.full_name IS NOT NULL
            AND (
                co.country IS NULL
                OR LOWER(TRIM(co.country)) = ANY(%s)
            )
            ORDER BY c.id LIMIT %s""",
            (list(TARGET_COUNTRY_ALIASES), limit),
        )
        contacts = cursor.fetchall()

        for ct in contacts:
            budget_reason = _budget_exhausted(monthly_used, daily_used, run_used)
            if budget_reason:
                logger.warning("Apollo people/match stopped by budget (%s).", budget_reason)
                break
            if not ct["full_name"]:
                continue
            try:
                rate_limiter.wait()
                payload = {
                    "first_name": ct["full_name"].split()[0] if " " in ct["full_name"] else ct["full_name"],
                    "last_name": ct["full_name"].split()[-1] if " " in ct["full_name"] else "",
                    "name": ct["full_name"],
                    # Apollo defaults do NOT reveal personal emails; request explicitly.
                    "reveal_personal_emails": True,
                }
                clean_org = _clean_org_name(ct["company_name"] or "", ct["full_name"] or "")
                if clean_org:
                    payload["organization_name"] = clean_org
                if ct["company_domain"]:
                    payload["domain"] = ct["company_domain"]

                resp = requests.post(
                    f"{APOLLO_BASE}/people/match",
                    json=payload,
                    headers=_apollo_headers(),
                    timeout=15,
                )
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5") or 5)
                    if retry_after > 120:
                        logger.warning(
                            "Apollo rate limit window too long (%ss). Stopping match run early.",
                            retry_after,
                        )
                        break
                    logger.warning(
                        "Apollo rate limit hit on people/match; waiting %ss before retry",
                        retry_after,
                    )
                    time.sleep(max(1, min(retry_after, 30)))
                    resp = requests.post(
                        f"{APOLLO_BASE}/people/match",
                        json=payload,
                        headers=_apollo_headers(),
                        timeout=15,
                    )
                resp.raise_for_status()
                person = resp.json().get("person", {})
                monthly_used += 1
                daily_used += 1
                run_used += 1

                if not person:
                    continue

                email = _best_email_from_person(person)
                # Fallback 1: use stable Apollo person id for a higher-confidence enrichment lookup.
                if not email and person.get("id"):
                    by_id_resp = requests.post(
                        f"{APOLLO_BASE}/people/match",
                        json={"id": person.get("id"), "reveal_personal_emails": True},
                        headers=_apollo_headers(),
                        timeout=15,
                    )
                    if by_id_resp.status_code == 200:
                        person_by_id = by_id_resp.json().get("person", {})
                        email = _best_email_from_person(person_by_id)
                        if person_by_id:
                            person = person_by_id
                    monthly_used += 1
                    daily_used += 1
                    run_used += 1

                # Fallback 2: if LinkedIn exists, try direct LinkedIn enrichment.
                if not email and person.get("linkedin_url"):
                    by_li_resp = requests.post(
                        f"{APOLLO_BASE}/people/match",
                        json={
                            "linkedin_url": person.get("linkedin_url"),
                            "reveal_personal_emails": True,
                        },
                        headers=_apollo_headers(),
                        timeout=15,
                    )
                    if by_li_resp.status_code == 200:
                        person_by_li = by_li_resp.json().get("person", {})
                        email = _best_email_from_person(person_by_li)
                        if person_by_li:
                            person = person_by_li
                    monthly_used += 1
                    daily_used += 1
                    run_used += 1

                if not email:
                    continue

                phone_nums = person.get("phone_numbers", [])
                phone = phone_nums[0].get("sanitized_number", "") if phone_nums else ""

                cursor.execute(
                    """UPDATE contacts SET
                        email = %s, email_verified = TRUE, email_confidence = 85,
                        phone = COALESCE(NULLIF(%s, ''), phone),
                        linkedin_url = COALESCE(NULLIF(%s, ''), linkedin_url),
                        job_title = COALESCE(NULLIF(%s, ''), job_title),
                        seniority = COALESCE(NULLIF(%s, ''), seniority),
                        enriched = TRUE, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s""",
                    (
                        email, phone,
                        person.get("linkedin_url", ""),
                        person.get("title", ""),
                        classify_seniority(person.get("title", "")),
                        ct["id"],
                    ),
                )
                matched += 1
            except Exception as e:
                logger.debug("Match failed for %s: %s", ct["full_name"], e)

        conn.commit()
        cursor.close()

    _record_credits(run_used)
    logger.info("Matched %d contacts with emails", matched)
    return {"status": "success", "matched": matched}


# -----------------------------------------------------------------------
# Endpoint 4: organizations/job_postings
# Input: company domain -> Output: active job postings (intent signals)
# -----------------------------------------------------------------------

def fetch_job_signals(limit=None):
    """Infer hiring intent signals using accessible Apollo people search APIs."""
    if not _check_api_key():
        return {"status": "skipped", "reason": "No API key"}

    limit = limit or ENRICH_BATCH_SIZE
    monthly_used = _get_credits_used()
    daily_used = _get_daily_credits_used()
    if _budget_exhausted(monthly_used, daily_used, 0):
        return {"status": "skipped", "reason": "Credit limit"}

    logger.info("=" * 60)
    logger.info("Inferring hiring intent via mixed_people/api_search")
    logger.info("=" * 60)

    signals_added = 0
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        # Companies with domains, check for relevant job postings
        cursor.execute(
            """SELECT id, company_name, company_domain FROM companies
            WHERE company_domain IS NOT NULL
            ORDER BY source_count DESC LIMIT %s""",
            (limit,),
        )
        companies = cursor.fetchall()

        # Keywords focused on active hiring + wholesale voice relevance.
        intent_query = (
            "hiring OR recruiter OR talent acquisition OR careers "
            "OR wholesale voice OR voip OR interconnect OR carrier"
        )

        for co in companies:
            if _budget_exhausted(monthly_used, daily_used, 0):
                break
            try:
                rate_limiter.wait()
                resp = requests.post(
                    f"{APOLLO_BASE}/mixed_people/api_search",
                    json={
                        "q_organization_domains_list": [co["company_domain"]],
                        "q_keywords": intent_query,
                        "per_page": 3,
                        "page": 1,
                    },
                    headers=_apollo_headers(),
                    timeout=15,
                )
                if resp.status_code in (403, 404):
                    logger.warning(
                        "Apollo people search endpoint unavailable for this key/plan (status=%s). Skipping signal fetch.",
                        resp.status_code,
                    )
                    break
                resp.raise_for_status()
                body = resp.json() or {}
                total_entries = int(body.get("total_entries", 0) or 0)

                if total_entries <= 0:
                    continue

                signal_detail = (
                    f"Apollo people search found {total_entries} matching profiles "
                    f"for hiring/voice-intent query on {co['company_domain']}"
                )
                cursor.execute(
                    "SELECT 1 FROM signals WHERE company_id=%s AND signal_type='hiring_intent' AND signal_detail=%s",
                    (co["id"], signal_detail),
                )
                if not cursor.fetchone():
                    cursor.execute(
                        "INSERT INTO signals (company_id, signal_type, signal_detail, points) VALUES (%s, 'hiring_intent', %s, 10)",
                        (co["id"], signal_detail),
                    )
                    signals_added += 1

            except Exception as e:
                logger.debug("Hiring-intent inference failed for %s: %s", co["company_domain"], e)

        conn.commit()
        cursor.close()

    logger.info("Added %d hiring intent signals", signals_added)
    return {"status": "success", "signals_added": signals_added}


# -----------------------------------------------------------------------
# Master enrichment function -- runs all 4 enrichment steps
# -----------------------------------------------------------------------

def enrich_all(limit=None):
    """Run the full enrichment pipeline: company details -> contacts -> match -> signals."""
    if not _check_api_key():
        return {"status": "skipped", "reason": "No API key"}

    results = {}

    logger.info("")
    logger.info("== Step 1/4: Enrich Company Details ==")
    results["company_details"] = enrich_company_details(limit=limit)

    logger.info("")
    logger.info("== Step 2/4: Find Decision Makers ==")
    results["decision_makers"] = find_decision_makers(limit=limit)

    logger.info("")
    logger.info("== Step 3/4: Match Contacts ==")
    results["match_contacts"] = match_contacts(limit=limit or 50)

    logger.info("")
    logger.info("== Step 4/4: Fetch Job Signals ==")
    results["job_signals"] = fetch_job_signals(limit=limit)

    return results
