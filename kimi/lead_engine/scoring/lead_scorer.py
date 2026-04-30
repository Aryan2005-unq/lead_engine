"""Lead Scoring Module -- 100-point model."""
import json, logging
from database import get_engine_connection

logger = logging.getLogger(__name__)

def run_scoring():
    logger.info("=" * 60)
    logger.info("Starting lead scoring")
    logger.info("=" * 60)
    scored, tier_counts = 0, {"A":0,"B":0,"C":0}
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""SELECT c.id AS contact_id, c.job_title, c.email_verified, c.email_confidence, c.phone,
            c.source_count AS contact_source_count, co.id AS company_id, co.company_type, co.company_size,
            co.sources AS company_sources, co.source_count AS company_source_count
            FROM contacts c LEFT JOIN companies co ON c.company_id=co.id ORDER BY c.id""")
        contacts = cursor.fetchall()
        cursor.execute("SELECT company_id, signal_type, points FROM signals")
        sigs_by_co = {}
        for s in cursor.fetchall():
            sigs_by_co.setdefault(s["company_id"],[]).append(s)
        for ct in contacts:
            score, tier = _calc(ct, sigs_by_co)
            cursor.execute("UPDATE contacts SET lead_score=%s, tier=%s WHERE id=%s", (score, tier, ct["contact_id"]))
            scored += 1; tier_counts[tier] += 1
        conn.commit(); cursor.close()
    logger.info("Scored %d -- A:%d B:%d C:%d", scored, tier_counts["A"], tier_counts["B"], tier_counts["C"])
    return {"scored":scored, "tier_a":tier_counts["A"], "tier_b":tier_counts["B"], "tier_c":tier_counts["C"]}

def _calc(ct, sigs_by_co):
    score = 0
    ctype = (ct["company_type"] or "").lower()
    csrc = ct["company_sources"] or []
    if isinstance(csrc, str): csrc = json.loads(csrc)
    if any(t in ctype for t in ["ixc","interconnected voip","clec"]): score += 10
    elif any(t in ctype for t in ["carrier","itsp"]): score += 7
    if "FCC_499A" in csrc: score += 5
    if "RMD" in csrc: score += 3
    if "CRTC" in csrc: score += 4
    csize = ct["company_size"] or ""
    if any(s in csize for s in ["51-200","201-500","501-1000"]): score += 3
    title = (ct["job_title"] or "").lower()
    if any(kw in title for kw in ["vp wholesale","head of wholesale","director wholesale"]): score += 30
    elif any(kw in title for kw in ["carrier relations","interconnect","route manager"]): score += 25
    elif any(kw in title for kw in ["voice trading","head of voice"]): score += 20
    elif any(kw in title for kw in ["cto","ceo","vp network","vp carrier"]): score += 15
    elif any(kw in title for kw in ["director","manager"]): score += 10
    elif any(kw in title for kw in ["engineer","analyst"]): score += 5
    intent = sum(min(s.get("points",0),12) for s in sigs_by_co.get(ct["company_id"],[]))
    score += min(intent, 25)
    if ct["email_verified"]: score += 8
    if (ct["email_confidence"] or 0) > 80: score += 4
    if ct["phone"]: score += 4
    sc = ct["contact_source_count"] or 1
    if sc >= 3: score += 4
    elif sc >= 2: score += 2
    score = min(score, 100)
    tier = "A" if score>=70 else ("B" if score>=45 else "C")
    return score, tier
