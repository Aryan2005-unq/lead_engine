"""CSV Exporter for Sales Platform."""
import csv, json, logging
from datetime import datetime
from pathlib import Path
from database import get_engine_connection
from config import EXPORT_DIR

logger = logging.getLogger(__name__)
CSV_COLUMNS = ["company_name","company_domain","company_type","country","state","about","services","tech_stack","source",
    "contact_name","job_title","seniority","email","email_verified","email_confidence","phone","linkedin_url","linkedin_search_url","lead_score","tier"]

def export_csv(only_new=True, min_tier=None):
    logger.info("=" * 60)
    logger.info("Starting CSV export")
    logger.info("=" * 60)
    where, params = [], []
    if only_new: where.append("c.exported = FALSE")
    # Export only contactable leads.
    where.append("((c.email IS NOT NULL AND c.email <> '') OR (c.phone IS NOT NULL AND c.phone <> ''))")
    # Enforce final output to target geographies only.
    where.append(
        "LOWER(TRIM(COALESCE(co.country, ''))) IN (%s,%s,%s,%s,%s,%s)"
    )
    params.extend(["usa", "us", "united states", "united states of america", "canada", "ca"])
    if min_tier:
        tiers = {"A":["A"],"B":["A","B"],"C":["A","B","C"]}.get(min_tier.upper(),["A","B","C"])
        where.append(f"c.tier IN ({','.join(['%s']*len(tiers))})")
        params.extend(tiers)
    where_sql = " AND ".join(where) if where else "TRUE"
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(f"""SELECT co.company_name,co.company_domain,co.company_type,co.country,co.state,co.about,co.services,co.tech_stack,co.sources AS company_sources,
            c.id AS contact_id,c.full_name,c.job_title,c.seniority,c.email,c.email_verified,c.email_confidence,c.phone,c.linkedin_url,c.linkedin_search_url,c.lead_score,c.tier
            FROM contacts c LEFT JOIN companies co ON c.company_id=co.id WHERE {where_sql} ORDER BY c.lead_score DESC""", params)
        rows = cursor.fetchall()
        if not rows:
            logger.info("No contacts to export.")
            return {"file":None,"rows":0}
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        fp = Path(EXPORT_DIR) / f"leads_{ts}.csv"
        tiers, cids = {"A":0,"B":0,"C":0}, []
        with open(fp,"w",newline="",encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS); w.writeheader()
            for r in rows:
                src = r["company_sources"] or []
                if isinstance(src,str): src = json.loads(src)
                w.writerow({"company_name":r["company_name"] or "","company_domain":r["company_domain"] or "","company_type":r["company_type"] or "",
                    "country":r["country"] or "","state":r["state"] or "","about":r["about"] or "","services":r["services"] or "",
                    "tech_stack":r["tech_stack"] or "","source":", ".join(src),"contact_name":r["full_name"] or "","job_title":r["job_title"] or "",
                    "seniority":r["seniority"] or "","email":r["email"] or "","email_verified":r["email_verified"] or False,
                    "email_confidence":r["email_confidence"] or 0,"phone":r["phone"] or "","linkedin_url":r["linkedin_url"] or "",
                    "linkedin_search_url":r["linkedin_search_url"] or "","lead_score":r["lead_score"] or 0,"tier":r["tier"] or "C"})
                t = r["tier"] or "C"; tiers[t] = tiers.get(t,0)+1; cids.append(r["contact_id"])
        if only_new and cids:
            cursor.execute(f"UPDATE contacts SET exported=TRUE WHERE id IN ({','.join(['%s']*len(cids))})", cids)
            conn.commit()
        cursor.close()
    logger.info("Exported %d contacts to %s -- A:%d B:%d C:%d", len(rows), fp.name, tiers["A"], tiers["B"], tiers["C"])
    return {"file":str(fp),"rows":len(rows),"tier_a":tiers["A"],"tier_b":tiers["B"],"tier_c":tiers["C"]}
