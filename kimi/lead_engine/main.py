"""
Lead Extraction Engine -- Main CLI Orchestrator.

Usage:
    python main.py --init              Initialize database (first run)
    python main.py --all               Full pipeline run
    python main.py --source fcc_499a   Extract from FCC 499A only
    python main.py --source rmd        Import from existing RMD system
    python main.py --dedup             Merge raw -> master records
    python main.py --enrich            Full Apollo enrichment (4 steps)
    python main.py --enrich-companies  Enrich company profiles only
    python main.py --enrich-contacts   Find decision-makers only
    python main.py --match             Match names to emails (speakers)
    python main.py --signals           Fetch job posting signals
    python main.py --score             Score all contacts
    python main.py --export            Export new leads to CSV
    python main.py --export --tier B   Export Tier A+B leads only
    python main.py --stats             View database statistics
"""

import argparse
import sys
from datetime import datetime

from logger import setup_logger
from database import init_database, get_engine_connection
from dedup.deduplicator import run_deduplication
from scoring.lead_scorer import run_scoring
from export.csv_exporter import export_csv

logger = setup_logger("lead_engine")

SOURCE_REGISTRY = {
    "fcc_499a": {"module": "ingestors.fcc_499a", "class": "FCC499AIngestor", "description": "FCC Form 499-A filer database"},
    "rmd": {"module": "ingestors.fcc_rmd", "class": "RMDIngestor", "description": "Existing RMD system (crwm_db)"},
    "apollo": {"module": "ingestors.apollo_search", "class": "ApolloSearchIngestor", "description": "Apollo.io proactive people search"},
    "crtc": {"module": "ingestors.crtc_canada", "class": "CRTCIngestor", "description": "CRTC Canada telecom providers"},
    "events": {"module": "ingestors.event_scraper", "class": "EventScraperIngestor", "description": "ITW/Capacity Europe attendees"},
    "job_board": {"module": "ingestors.job_board", "class": "JobBoardIngestor", "description": "Indeed job postings (expansion signals)"},
    "tcxc": {"module": "ingestors.tcxc_scraper", "class": "TCXCIngestor", "description": "TelecomsXchange carrier marketplace"},
    "competitors": {"module": "ingestors.competitor_pages", "class": "CompetitorPagesIngestor", "description": "Competitor partner pages"},
    "fcc_notices": {"module": "ingestors.fcc_notices", "class": "FCCNoticesIngestor", "description": "FCC public notices (new licenses)"},
}


def get_ingestor(source_name):
    if source_name not in SOURCE_REGISTRY:
        logger.error("Unknown source: '%s'. Available: %s", source_name, ", ".join(SOURCE_REGISTRY.keys()))
        sys.exit(1)
    entry = SOURCE_REGISTRY[source_name]
    module = __import__(entry["module"], fromlist=[entry["class"]])
    return getattr(module, entry["class"])()


def cmd_init():
    logger.info("Initializing database...")
    init_database()
    logger.info("Database initialized successfully.")


def cmd_source(source_name):
    ingestor = get_ingestor(source_name)
    result = ingestor.run()
    if result["status"] == "success":
        logger.info("[OK] Source '%s' completed successfully.", source_name)
    else:
        logger.error("[FAIL] Source '%s' failed: %s", source_name, result.get("error"))
    return result


def cmd_dedup():
    return run_deduplication()


def cmd_score():
    return run_scoring()


def cmd_export(only_new=True, min_tier=None):
    return export_csv(only_new=only_new, min_tier=min_tier)


def cmd_enrich(limit=None):
    from enrichment.apollo_enrich import enrich_all
    return enrich_all(limit=limit)


def cmd_enrich_companies(limit=None):
    from enrichment.apollo_enrich import enrich_company_details
    return enrich_company_details(limit=limit)


def cmd_enrich_contacts(limit=None):
    from enrichment.apollo_enrich import find_decision_makers
    return find_decision_makers(limit=limit)


def cmd_match(limit=50):
    from enrichment.apollo_enrich import match_contacts
    return match_contacts(limit=limit)


def cmd_signals(limit=None):
    from enrichment.apollo_enrich import fetch_job_signals
    return fetch_job_signals(limit=limit)


def cmd_all(limit=None):
    logger.info("=" * 60)
    logger.info("FULL PIPELINE -- Started at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    for source_name in SOURCE_REGISTRY:
        logger.info("")
        logger.info("-- Source: %s --", source_name)
        try:
            cmd_source(source_name)
        except Exception as e:
            logger.error("Source '%s' failed, continuing: %s", source_name, e)

    logger.info("")
    logger.info("-- Deduplication --")
    cmd_dedup()

    logger.info("")
    logger.info("-- Enrichment --")
    try:
        cmd_enrich(limit=limit)
    except Exception as e:
        logger.warning("Enrichment skipped: %s", e)

    logger.info("")
    logger.info("-- Signals --")
    try:
        cmd_signals(limit=limit)
    except Exception as e:
        logger.warning("Signals skipped: %s", e)

    logger.info("")
    logger.info("-- Scoring --")
    cmd_score()

    logger.info("")
    logger.info("-- Export --")
    result = cmd_export()

    logger.info("")
    logger.info("=" * 60)
    logger.info("FULL PIPELINE -- Completed at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)
    return result


def cmd_stats():
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_companies")
        raw_co = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM raw_contacts")
        raw_ct = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM companies")
        co = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM contacts")
        ct = cursor.fetchone()[0]
        cursor.execute("SELECT tier, COUNT(*) FROM contacts GROUP BY tier ORDER BY tier")
        tiers = {r["tier"]: r["count"] for r in cursor.fetchall()}
        cursor.execute("SELECT source, COUNT(*) as cnt FROM raw_companies GROUP BY source ORDER BY cnt DESC")
        sources = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) FROM contacts WHERE enriched = TRUE")
        enriched = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM contacts WHERE exported = TRUE")
        exported = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM signals")
        signals = cursor.fetchone()[0]
        cursor.execute("SELECT source, status, records_found, records_new, started_at FROM extraction_runs ORDER BY started_at DESC LIMIT 5")
        recent = cursor.fetchall()
        cursor.close()

    print()
    print("=" * 60)
    print("  LEAD EXTRACTION ENGINE -- Database Statistics")
    print("=" * 60)
    print()
    print(f"  Raw Records:     {raw_co:,} companies | {raw_ct:,} contacts")
    print(f"  Master Records:  {co:,} companies | {ct:,} contacts")
    print(f"  Signals:         {signals:,}")
    print()
    print("  -- Tier Breakdown --")
    print(f"    Tier A (>=70):  {tiers.get('A', 0):,}")
    print(f"    Tier B (>=45):  {tiers.get('B', 0):,}")
    print(f"    Tier C (<45):   {tiers.get('C', 0):,}")
    print()
    print(f"  Enriched:  {enriched:,} / {ct:,}")
    print(f"  Exported:  {exported:,} / {ct:,}")
    print()
    print("  -- Sources --")
    for s in sources:
        print(f"    {s['source']:<20s}  {s['cnt']:,} companies")
    print()
    print("  -- Recent Runs --")
    for r in recent:
        icon = "[OK]" if r["status"] == "success" else "[FAIL]"
        print(f"    {icon} {r['source']:<15s} found:{r['records_found'] or 0:>5,}  new:{r['records_new'] or 0:>5,}  at:{r['started_at']}")
    print()
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Lead Extraction Engine -- Wholesale VoIP Lead Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --init              Initialize database (run once)
  python main.py --all               Full pipeline run
  python main.py --source fcc_499a   Extract from FCC 499A only
  python main.py --source rmd        Import from existing RMD system
  python main.py --dedup             Merge raw -> master records
  python main.py --enrich            Full Apollo enrichment (4 steps)
  python main.py --enrich-companies  Enrich company profiles only
  python main.py --enrich-contacts   Find decision-makers only
  python main.py --match             Match speaker names to emails
  python main.py --signals           Fetch job posting intent signals
  python main.py --score             Score all contacts
  python main.py --export            Export new leads to CSV
  python main.py --export --tier A   Export only Tier A leads
  python main.py --stats             View database statistics
        """,
    )
    parser.add_argument("--init", action="store_true", help="Initialize the database schema.")
    parser.add_argument("--source", type=str, metavar="NAME", help="Run a specific source.")
    parser.add_argument("--all", action="store_true", help="Run the full pipeline.")
    parser.add_argument("--dedup", action="store_true", help="Run deduplication.")
    parser.add_argument("--enrich", action="store_true", help="Full Apollo enrichment (4 steps).")
    parser.add_argument("--enrich-companies", action="store_true", help="Enrich company profiles via Apollo.")
    parser.add_argument("--enrich-contacts", action="store_true", help="Find decision-makers via Apollo.")
    parser.add_argument("--match", action="store_true", help="Match contact names to emails via Apollo.")
    parser.add_argument("--signals", action="store_true", help="Fetch job posting signals via Apollo.")
    parser.add_argument("--score", action="store_true", help="Run lead scoring.")
    parser.add_argument("--export", action="store_true", help="Export leads to CSV.")
    parser.add_argument("--tier", type=str, choices=["A", "B", "C"], default=None, help="Min tier (with --export).")
    parser.add_argument("--export-all", action="store_true", help="Include previously exported.")
    parser.add_argument("--limit", type=int, default=None, help="Limit records for enrichment.")
    parser.add_argument("--stats", action="store_true", help="Display statistics.")

    args = parser.parse_args()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    if args.init:
        cmd_init()
    elif args.source:
        cmd_source(args.source)
    elif getattr(args, "all"):
        cmd_all(limit=args.limit)
    elif args.dedup:
        cmd_dedup()
    elif args.enrich:
        cmd_enrich(limit=args.limit)
    elif getattr(args, 'enrich_companies', False):
        cmd_enrich_companies(limit=args.limit)
    elif getattr(args, 'enrich_contacts', False):
        cmd_enrich_contacts(limit=args.limit)
    elif args.match:
        cmd_match(limit=args.limit or 50)
    elif args.signals:
        cmd_signals(limit=args.limit)
    elif args.score:
        cmd_score()
    elif args.export:
        cmd_export(only_new=not args.export_all, min_tier=args.tier)
    elif args.stats:
        cmd_stats()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
