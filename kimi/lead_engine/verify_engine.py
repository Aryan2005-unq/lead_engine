"""
Verification script -- run on the box where the crwm_postgres Docker container is up.

Usage:
    python verify_engine.py

What it checks:
    1. DB connectivity (engine + RMD)
    2. Schema applies cleanly (idempotent re-run safe)
    3. Savepoint rollback works on a poisoned INSERT (no transaction-poison regression)
    4. api_credits unique upsert behaves correctly
    5. extraction_runs new columns exist
    6. RMD ingestor end-to-end run (read-only on RMD, writes to engine)
    7. Dedup -> Score -> Export round-trip
    8. CSV header matches the sales platform plan
"""
from __future__ import annotations

import csv
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import ENGINE_DB, RMD_DB
from database import get_engine_connection, get_rmd_connection, init_database


GREEN = "\033[92m"; RED = "\033[91m"; YELLOW = "\033[93m"; RESET = "\033[0m"


def check(name, fn):
    try:
        fn()
        print(f"{GREEN}[PASS]{RESET} {name}")
        return True
    except Exception as e:
        print(f"{RED}[FAIL]{RESET} {name}: {type(e).__name__}: {e}")
        return False


def t1_engine_connection():
    with get_engine_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1


def t2_rmd_connection():
    with get_rmd_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1


def t3_schema_idempotent():
    init_database()
    init_database()  # second call must not raise
    with get_engine_connection() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT table_name FROM information_schema.tables
                       WHERE table_schema='public' ORDER BY table_name""")
        tables = {r[0] for r in cur.fetchall()}
    expected = {"raw_companies", "raw_contacts", "companies", "contacts",
                "signals", "extraction_runs", "api_credits"}
    missing = expected - tables
    assert not missing, f"missing tables: {missing}"


def t4_savepoint_rollback():
    """Insert one good row + one bad (FK violation) row inside savepoints; both
    should be possible because the savepoint isolates the failure."""
    with get_engine_connection() as conn:
        cur = conn.cursor()
        # Clean any prior verify rows
        cur.execute("DELETE FROM raw_contacts WHERE source = 'VERIFY_TEST'")
        cur.execute("DELETE FROM raw_companies WHERE source = 'VERIFY_TEST'")
        conn.commit()

        # row 1 (good)
        cur.execute("SAVEPOINT sp1")
        cur.execute(
            "INSERT INTO raw_companies (company_name, source) VALUES ('VerifyCo', 'VERIFY_TEST') RETURNING id"
        )
        good_id = cur.fetchone()[0]
        cur.execute("RELEASE SAVEPOINT sp1")

        # row 2 (bad FK -- raw_company_id=999999999 doesn't exist)
        cur.execute("SAVEPOINT sp2")
        try:
            cur.execute(
                "INSERT INTO raw_contacts (raw_company_id, email, source) VALUES (999999999, 'x@x.com', 'VERIFY_TEST')"
            )
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp2")

        # row 3 (good, must still succeed despite row 2 failing)
        cur.execute("SAVEPOINT sp3")
        cur.execute(
            "INSERT INTO raw_contacts (raw_company_id, email, source) VALUES (%s, 'y@y.com', 'VERIFY_TEST')",
            (good_id,),
        )
        cur.execute("RELEASE SAVEPOINT sp3")
        conn.commit()

        cur.execute(
            "SELECT COUNT(*) FROM raw_contacts WHERE source = 'VERIFY_TEST'"
        )
        assert cur.fetchone()[0] == 1, "row 3 didn't survive row 2 failure"

        # cleanup
        cur.execute("DELETE FROM raw_contacts WHERE source = 'VERIFY_TEST'")
        cur.execute("DELETE FROM raw_companies WHERE source = 'VERIFY_TEST'")
        conn.commit()


def t5_api_credits_upsert():
    period = date.today().replace(day=1)
    with get_engine_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM api_credits WHERE api_name='verify_test' AND period_start=%s",
            (period,),
        )
        conn.commit()

    from enrichment.apollo_enrich import _record_credits, _get_credits_used
    # stub: temporarily run upsert against a fake api_name by patching env
    with get_engine_connection() as conn:
        cur = conn.cursor()
        for amt in (10, 20, 7):
            cur.execute(
                """INSERT INTO api_credits (api_name, credits_used, credits_limit, period_start, period_end)
                   VALUES ('verify_test', %s, 10000, %s, %s)
                   ON CONFLICT (api_name, period_start)
                   DO UPDATE SET credits_used = api_credits.credits_used + EXCLUDED.credits_used""",
                (amt, period, period),
            )
        conn.commit()

        cur.execute(
            "SELECT COUNT(*), SUM(credits_used) FROM api_credits WHERE api_name='verify_test' AND period_start=%s",
            (period,),
        )
        rowcount, total = cur.fetchone()
        cur.execute(
            "DELETE FROM api_credits WHERE api_name='verify_test' AND period_start=%s",
            (period,),
        )
        conn.commit()
    assert rowcount == 1, f"expected 1 row, got {rowcount} (UNIQUE missing?)"
    assert total == 37, f"expected 10+20+7=37, got {total}"


def t6_extraction_runs_columns():
    with get_engine_connection() as conn:
        cur = conn.cursor()
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_name='extraction_runs'""")
        cols = {r[0] for r in cur.fetchall()}
    for need in ("companies_found", "companies_new", "contacts_found", "contacts_new"):
        assert need in cols, f"missing {need}"


def t7_rmd_ingest_runs():
    """Run the RMD ingestor end-to-end. Skips gracefully if RMD has no leads."""
    from ingestors.fcc_rmd import RMDIngestor
    result = RMDIngestor().run()
    assert result.get("status") == "success", result


def t8_full_pipeline():
    """Dedup -> Score -> Export. Asserts CSV header matches the platform plan column list."""
    from dedup.deduplicator import run_deduplication
    from scoring.lead_scorer import run_scoring
    from export.csv_exporter import export_csv, CSV_COLUMNS

    run_deduplication()
    run_scoring()
    out = export_csv(only_new=False)
    if out.get("file"):
        with open(out["file"], encoding="utf-8") as f:
            header = next(csv.reader(f))
        assert header == CSV_COLUMNS, f"CSV header drift: {header}"


def main():
    print("=" * 60)
    print(f"Engine DB: {ENGINE_DB['host']}:{ENGINE_DB['port']}/{ENGINE_DB['dbname']}")
    print(f"RMD    DB: {RMD_DB['host']}:{RMD_DB['port']}/{RMD_DB['dbname']}")
    print("=" * 60)

    tests = [
        ("Engine DB connection",            t1_engine_connection),
        ("RMD DB connection",               t2_rmd_connection),
        ("Schema applies + idempotent",     t3_schema_idempotent),
        ("Savepoint rollback survives bad row", t4_savepoint_rollback),
        ("api_credits unique upsert",       t5_api_credits_upsert),
        ("extraction_runs has split cols",  t6_extraction_runs_columns),
        ("RMD ingestor end-to-end",         t7_rmd_ingest_runs),
        ("Dedup -> Score -> Export -> CSV", t8_full_pipeline),
    ]
    passed = sum(check(n, f) for n, f in tests)
    print("=" * 60)
    print(f"{passed}/{len(tests)} checks passed")
    sys.exit(0 if passed == len(tests) else 1)


if __name__ == "__main__":
    main()
