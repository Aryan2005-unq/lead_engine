"""Deduplication Module."""
import json, logging
from datetime import datetime
from database import get_engine_connection
from utils import extract_domain, normalize_email, merge_json_arrays, generate_linkedin_search_url

logger = logging.getLogger(__name__)
TARGET_COUNTRIES = {"usa", "us", "united states", "united states of america", "canada", "ca"}

def run_deduplication():
    logger.info("=" * 60)
    logger.info("Starting deduplication process")
    logger.info("=" * 60)
    stats = {"companies_processed":0,"companies_new":0,"companies_updated":0,"contacts_processed":0,"contacts_new":0,"contacts_updated":0}
    _deduplicate_companies(stats)
    _recover_company_domains()
    _mark_verified_companies()
    _deduplicate_contacts(stats)
    _generate_linkedin_urls()
    logger.info("Dedup complete -- Companies: %d new, %d updated | Contacts: %d new, %d updated",
        stats["companies_new"], stats["companies_updated"], stats["contacts_new"], stats["contacts_updated"])
    return stats

def _deduplicate_companies(stats):
    with get_engine_connection() as conn:
        read_cur = conn.cursor()
        write_cur = conn.cursor()
        read_cur.execute("SELECT * FROM raw_companies ORDER BY created_at ASC")
        for row in read_cur.fetchall():
            stats["companies_processed"] += 1
            domain = row["company_domain"]
            existing = None
            if domain:
                write_cur.execute("SELECT * FROM companies WHERE company_domain = %s", (domain,))
                existing = write_cur.fetchone()
            if existing:
                ex_sources = existing["sources"] or []
                if isinstance(ex_sources, str): ex_sources = json.loads(ex_sources)
                new_sources = merge_json_arrays(ex_sources, [row["source"]])
                write_cur.execute("""UPDATE companies SET
                    company_name=COALESCE(NULLIF(%(n)s,''),company_name), company_type=COALESCE(NULLIF(%(t)s,''),company_type),
                    country=COALESCE(NULLIF(%(co)s,''),country), state=COALESCE(NULLIF(%(st)s,''),state),
                    address=COALESCE(NULLIF(%(ad)s,''),address), about=COALESCE(NULLIF(%(ab)s,''),about),
                    services=COALESCE(NULLIF(%(sv)s,''),services), website_url=COALESCE(NULLIF(%(wu)s,''),website_url),
                    sources=%(src)s::jsonb, source_count=%(sc)s, updated_at=%(now)s WHERE id=%(id)s""",
                    {"n":row["company_name"],"t":row["company_type"],"co":row["country"],"st":row["state"],
                     "ad":row["address"],"ab":row["about"],"sv":row["services"],"wu":row["website_url"],
                     "src":json.dumps(new_sources),"sc":len(new_sources),"now":datetime.utcnow(),"id":existing["id"]})
                stats["companies_updated"] += 1
            else:
                write_cur.execute("""INSERT INTO companies (company_name,company_domain,company_type,company_size,country,state,address,about,services,tech_stack,sources,source_count,website_url)
                    VALUES (%(n)s,%(d)s,%(t)s,%(sz)s,%(co)s,%(st)s,%(ad)s,%(ab)s,%(sv)s,%(ts)s,%(src)s::jsonb,1,%(wu)s) ON CONFLICT (company_domain) DO NOTHING""",
                    {"n":row["company_name"],"d":domain,"t":row["company_type"],"sz":row["company_size"],"co":row["country"],"st":row["state"],
                     "ad":row["address"],"ab":row["about"],"sv":row["services"],"ts":row["tech_stack"],"src":json.dumps([row["source"]]),"wu":row["website_url"]})
                stats["companies_new"] += 1
        conn.commit()
        read_cur.close()
        write_cur.close()

def _deduplicate_contacts(stats):
    with get_engine_connection() as conn:
        read_cur = conn.cursor()
        write_cur = conn.cursor()
        read_cur.execute("SELECT * FROM raw_contacts ORDER BY created_at ASC")
        for row in read_cur.fetchall():
            stats["contacts_processed"] += 1
            email = normalize_email(row["email"])
            company_id = _find_company_id(write_cur, row)
            if email:
                write_cur.execute("SELECT * FROM contacts WHERE email = %s", (email,))
                existing = write_cur.fetchone()
            else:
                # Fallback for event contacts that have no email yet; dedupe by name+company.
                if not row["full_name"]:
                    continue
                if company_id:
                    write_cur.execute(
                        "SELECT * FROM contacts WHERE full_name = %s AND company_id = %s LIMIT 1",
                        (row["full_name"], company_id),
                    )
                else:
                    write_cur.execute(
                        "SELECT * FROM contacts WHERE full_name = %s AND company_id IS NULL LIMIT 1",
                        (row["full_name"],),
                    )
                existing = write_cur.fetchone()
            if existing:
                ex_sources = existing["sources"] or []
                if isinstance(ex_sources, str): ex_sources = json.loads(ex_sources)
                new_sources = merge_json_arrays(ex_sources, [row["source"]])
                write_cur.execute("""UPDATE contacts SET full_name=COALESCE(NULLIF(%(n)s,''),full_name),
                    job_title=COALESCE(NULLIF(%(jt)s,''),job_title), phone=COALESCE(NULLIF(%(p)s,''),phone),
                    email_verified=%(ev)s, email_confidence=%(ec)s, company_id=COALESCE(%(ci)s,company_id),
                    sources=%(src)s::jsonb, source_count=%(sc)s, updated_at=%(now)s WHERE id=%(id)s""",
                    {"n":row["full_name"],"jt":row["job_title"],"p":row["phone"],
                     "ev":existing["email_verified"] or row["email_verified"],"ec":max(existing["email_confidence"] or 0, row["email_confidence"] or 0),
                     "ci":company_id,"src":json.dumps(new_sources),"sc":len(new_sources),"now":datetime.utcnow(),"id":existing["id"]})
                stats["contacts_updated"] += 1
            else:
                write_cur.execute("""INSERT INTO contacts (company_id,full_name,job_title,seniority,email,email_verified,email_confidence,phone,linkedin_url,sources,source_count)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,1) ON CONFLICT (email) DO NOTHING""",
                    (company_id,row["full_name"],row["job_title"],row["seniority"],email,row["email_verified"],row["email_confidence"] or 0,row["phone"],row["linkedin_url"],json.dumps([row["source"]])))
                if write_cur.rowcount:
                    stats["contacts_new"] += 1
        conn.commit()
        read_cur.close()
        write_cur.close()

def _find_company_id(cursor, raw_contact):
    raw_co_id = raw_contact.get("raw_company_id")
    if raw_co_id:
        cursor.execute(
            """SELECT c.id FROM companies c
               JOIN raw_companies rc ON rc.company_domain = c.company_domain
               WHERE rc.id = %s LIMIT 1""",
            (raw_co_id,),
        )
        m = cursor.fetchone()
        if m:
            return m["id"]
        # Domain can be NULL for event data; fallback to company_name matching.
        cursor.execute(
            """SELECT c.id FROM companies c
               JOIN raw_companies rc ON rc.company_name = c.company_name
               WHERE rc.id = %s LIMIT 1""",
            (raw_co_id,),
        )
        m = cursor.fetchone()
        if m:
            return m["id"]
    email = raw_contact.get("email")
    if email:
        domain = extract_domain(email)
        if domain:
            cursor.execute("SELECT id FROM companies WHERE company_domain=%s",(domain,))
            m = cursor.fetchone()
            if m: return m["id"]
    return None


def _recover_company_domains():
    """Recover company domains from website URLs and known contact emails."""
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        # Recover from company website URL.
        cursor.execute(
            """
            SELECT id, website_url
            FROM companies
            WHERE (company_domain IS NULL OR company_domain = '')
              AND website_url IS NOT NULL
              AND website_url <> ''
            """
        )
        rows = cursor.fetchall()
        updated_from_web = 0
        for row in rows:
            domain = extract_domain(row["website_url"] or "")
            if not domain:
                continue
            cursor.execute(
                "UPDATE companies SET company_domain=%s, updated_at=CURRENT_TIMESTAMP WHERE id=%s",
                (domain, row["id"]),
            )
            updated_from_web += 1

        # Recover from contact email domains.
        cursor.execute(
            """
            SELECT c.id, ct.email
            FROM companies c
            JOIN contacts ct ON ct.company_id = c.id
            WHERE (c.company_domain IS NULL OR c.company_domain = '')
              AND ct.email IS NOT NULL
              AND ct.email <> ''
            """
        )
        updated_from_email = 0
        for row in cursor.fetchall():
            domain = extract_domain(row["email"] or "")
            if not domain:
                continue
            cursor.execute(
                "UPDATE companies SET company_domain=%s, updated_at=CURRENT_TIMESTAMP WHERE id=%s",
                (domain, row["id"]),
            )
            updated_from_email += 1
        conn.commit()
        cursor.close()
        logger.info(
            "Recovered company domains -- website: %d, email: %d",
            updated_from_web,
            updated_from_email,
        )


def _mark_verified_companies():
    """Mark companies as verified using trusted-source/domain heuristics."""
    trusted_sources = ["FCC_499A", "FCC_Notice", "CRTC", "RMD", "Apollo"]
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE companies
            SET verified_company = TRUE,
                updated_at = CURRENT_TIMESTAMP
            WHERE
                (company_domain IS NOT NULL AND company_domain <> '')
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements_text(COALESCE(companies.sources, '[]'::jsonb)) s
                    WHERE s = ANY(%s)
                )
            """,
            (trusted_sources,),
        )
        marked = cursor.rowcount
        conn.commit()
        cursor.close()
        logger.info("Marked %d companies as verified", marked)

def _generate_linkedin_urls():
    with get_engine_connection() as conn:
        read_cur = conn.cursor()
        write_cur = conn.cursor()
        read_cur.execute("SELECT c.id,c.full_name,co.company_name FROM contacts c LEFT JOIN companies co ON c.company_id=co.id WHERE c.linkedin_url IS NULL AND c.linkedin_search_url IS NULL AND c.full_name IS NOT NULL")
        updated = 0
        for row in read_cur.fetchall():
            url = generate_linkedin_search_url(row["full_name"], row["company_name"])
            write_cur.execute("UPDATE contacts SET linkedin_search_url=%s WHERE id=%s", (url, row["id"]))
            updated += 1
        conn.commit()
        read_cur.close()
        write_cur.close()
        logger.info("Generated LinkedIn URLs for %d contacts", updated)
