"""
Lead Extraction Engine -- Database Module.

Manages connections to two PostgreSQL databases:
1. Engine DB  -- read/write for extracted + enriched leads
2. RMD DB     -- read-only access to the existing RMD scraper system
"""

import psycopg2
import psycopg2.extras
import logging
import json
import uuid
import time
from contextlib import contextmanager

from config import ENGINE_DB, RMD_DB

logger = logging.getLogger(__name__)

DEBUG_LOG_PATH = "debug-5b7ed9.log"
DEBUG_SESSION_ID = "5b7ed9"


def _debug_log(hypothesis_id, location, message, data):
    payload = {
        "sessionId": DEBUG_SESSION_ID,
        "runId": "init-schema-debug",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": int(time.time() * 1000),
        "id": f"log_{uuid.uuid4().hex}",
    }
    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


@contextmanager
def get_engine_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=ENGINE_DB["host"], port=ENGINE_DB["port"],
            dbname=ENGINE_DB["dbname"], user=ENGINE_DB["user"],
            password=ENGINE_DB["password"],
            cursor_factory=psycopg2.extras.DictCursor,
        )
        yield conn
    except psycopg2.OperationalError as e:
        logger.error("Failed to connect to engine database: %s", e)
        raise
    finally:
        if conn and not conn.closed:
            conn.close()


@contextmanager
def get_rmd_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host=RMD_DB["host"], port=RMD_DB["port"],
            dbname=RMD_DB["dbname"], user=RMD_DB["user"],
            password=RMD_DB["password"],
            cursor_factory=psycopg2.extras.DictCursor,
        )
        conn.set_session(readonly=True)
        yield conn
    except psycopg2.OperationalError as e:
        logger.error("Failed to connect to RMD database: %s", e)
        raise
    finally:
        if conn and not conn.closed:
            conn.close()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS raw_companies (
    id              SERIAL PRIMARY KEY,
    company_name    VARCHAR(500) NOT NULL,
    company_domain  VARCHAR(255),
    company_type    VARCHAR(100),
    company_size    VARCHAR(50),
    country         VARCHAR(100),
    state           VARCHAR(100),
    address         TEXT,
    about           TEXT,
    services        TEXT,
    tech_stack      TEXT,
    source          VARCHAR(100) NOT NULL,
    source_id       VARCHAR(255),
    website_url     TEXT,
    raw_data        JSONB,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_contacts (
    id                  SERIAL PRIMARY KEY,
    raw_company_id      INT REFERENCES raw_companies(id) ON DELETE CASCADE,
    full_name           VARCHAR(255),
    job_title           VARCHAR(255),
    seniority           VARCHAR(50),
    email               VARCHAR(255),
    email_verified      BOOLEAN DEFAULT FALSE,
    email_confidence    INT DEFAULT 0,
    phone               VARCHAR(50),
    linkedin_url        TEXT,
    source              VARCHAR(100) NOT NULL,
    raw_data            JSONB,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS companies (
    id                  SERIAL PRIMARY KEY,
    company_name        VARCHAR(500) NOT NULL,
    company_domain      VARCHAR(255) UNIQUE,
    company_type        VARCHAR(100),
    company_size        VARCHAR(50),
    country             VARCHAR(100),
    state               VARCHAR(100),
    address             TEXT,
    about               TEXT,
    services            TEXT,
    tech_stack          TEXT,
    sources             JSONB DEFAULT '[]'::jsonb,
    source_count        INT DEFAULT 1,
    website_url         TEXT,
    linkedin_company_url TEXT,
    verified_company    BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contacts (
    id                  SERIAL PRIMARY KEY,
    company_id          INT REFERENCES companies(id) ON DELETE CASCADE,
    full_name           VARCHAR(255),
    job_title           VARCHAR(255),
    seniority           VARCHAR(50),
    email               VARCHAR(255) UNIQUE,
    email_verified      BOOLEAN DEFAULT FALSE,
    email_confidence    INT DEFAULT 0,
    phone               VARCHAR(50),
    linkedin_url        TEXT,
    linkedin_search_url TEXT,
    sources             JSONB DEFAULT '[]'::jsonb,
    source_count        INT DEFAULT 1,
    lead_score          INT DEFAULT 0,
    tier                VARCHAR(1) DEFAULT 'C',
    enriched            BOOLEAN DEFAULT FALSE,
    exported            BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS signals (
    id              SERIAL PRIMARY KEY,
    company_id      INT REFERENCES companies(id) ON DELETE CASCADE,
    signal_type     VARCHAR(100) NOT NULL,
    signal_detail   TEXT,
    detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    points          INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS extraction_runs (
    id                 SERIAL PRIMARY KEY,
    source             VARCHAR(100) NOT NULL,
    records_found      INT DEFAULT 0,
    records_new        INT DEFAULT 0,
    records_updated    INT DEFAULT 0,
    records_skipped    INT DEFAULT 0,
    companies_found    INT DEFAULT 0,
    companies_new      INT DEFAULT 0,
    contacts_found     INT DEFAULT 0,
    contacts_new       INT DEFAULT 0,
    status             VARCHAR(20) DEFAULT 'running',
    error              TEXT,
    started_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at       TIMESTAMP
);

ALTER TABLE extraction_runs ADD COLUMN IF NOT EXISTS companies_found INT DEFAULT 0;
ALTER TABLE extraction_runs ADD COLUMN IF NOT EXISTS companies_new   INT DEFAULT 0;
ALTER TABLE extraction_runs ADD COLUMN IF NOT EXISTS contacts_found  INT DEFAULT 0;
ALTER TABLE extraction_runs ADD COLUMN IF NOT EXISTS contacts_new    INT DEFAULT 0;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS verified_company BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS api_credits (
    id              SERIAL PRIMARY KEY,
    api_name        VARCHAR(50) NOT NULL,
    credits_used    INT DEFAULT 0,
    credits_limit   INT DEFAULT 0,
    period_start    DATE NOT NULL,
    period_end      DATE NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_api_credits_period
    ON api_credits(api_name, period_start);

CREATE INDEX IF NOT EXISTS idx_raw_companies_source ON raw_companies(source);
CREATE INDEX IF NOT EXISTS idx_raw_companies_domain ON raw_companies(company_domain);
CREATE INDEX IF NOT EXISTS idx_companies_domain ON companies(company_domain);
CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email);
CREATE INDEX IF NOT EXISTS idx_contacts_company_id ON contacts(company_id);
CREATE INDEX IF NOT EXISTS idx_contacts_tier ON contacts(tier);
CREATE INDEX IF NOT EXISTS idx_contacts_exported ON contacts(exported);
CREATE INDEX IF NOT EXISTS idx_signals_company_id ON signals(company_id);
CREATE INDEX IF NOT EXISTS idx_extraction_runs_source ON extraction_runs(source);
"""


def create_database_if_not_exists():
    try:
        conn = psycopg2.connect(
            host=ENGINE_DB["host"], port=ENGINE_DB["port"],
            dbname="postgres", user=ENGINE_DB["user"],
            password=ENGINE_DB["password"],
        )
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (ENGINE_DB["dbname"],))
        if not cursor.fetchone():
            cursor.execute(f'CREATE DATABASE "{ENGINE_DB["dbname"]}"')
            logger.info("Created database: %s", ENGINE_DB["dbname"])
        else:
            logger.info("Database already exists: %s", ENGINE_DB["dbname"])
        cursor.close()
        conn.close()
    except psycopg2.OperationalError as e:
        logger.error("Cannot connect to PostgreSQL server: %s", e)
        raise


def init_schema():
    with get_engine_connection() as conn:
        cursor = conn.cursor()
        # #region agent log
        cursor.execute("SELECT to_regclass('public.api_credits') IS NOT NULL")
        api_credits_exists = bool(cursor.fetchone()[0])
        _debug_log(
            "H1",
            "database.py:init_schema:precheck",
            "api_credits table existence before schema init",
            {"api_credits_exists": api_credits_exists},
        )
        # #endregion

        if api_credits_exists:
            # #region agent log
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT api_name, period_start
                    FROM api_credits
                    GROUP BY api_name, period_start
                    HAVING COUNT(*) > 1
                ) d
                """
            )
            duplicate_pair_count = int(cursor.fetchone()[0])
            _debug_log(
                "H1",
                "database.py:init_schema:duplicate_check",
                "duplicate api_credits key pairs before unique index",
                {"duplicate_pair_count": duplicate_pair_count},
            )
            # #endregion

            # #region agent log
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname = 'uq_api_credits_period'
                """
            )
            index_exists = bool(cursor.fetchone()[0])
            _debug_log(
                "H2",
                "database.py:init_schema:index_check",
                "uq_api_credits_period index existence before schema init",
                {"index_exists": index_exists},
            )
            # #endregion

            # #region agent log
            if duplicate_pair_count > 0 and not index_exists:
                cursor.execute(
                    """
                    WITH ranked AS (
                        SELECT id,
                               ROW_NUMBER() OVER (
                                   PARTITION BY api_name, period_start
                                   ORDER BY id ASC
                               ) AS rn
                        FROM api_credits
                    )
                    DELETE FROM api_credits
                    WHERE id IN (
                        SELECT id FROM ranked WHERE rn > 1
                    )
                    """
                )
                deleted_rows = cursor.rowcount
                conn.commit()
                _debug_log(
                    "H1",
                    "database.py:init_schema:dedup_cleanup",
                    "removed duplicate api_credits rows before creating unique index",
                    {"deleted_rows": deleted_rows},
                )
            # #endregion

        try:
            cursor.execute(SCHEMA_SQL)
            conn.commit()
            # #region agent log
            _debug_log(
                "H3",
                "database.py:init_schema:execute_schema",
                "SCHEMA_SQL execution succeeded",
                {"schema_execute": "success"},
            )
            # #endregion
            cursor.close()
            logger.info("Engine database schema initialized successfully.")
        except Exception as e:
            conn.rollback()
            # #region agent log
            _debug_log(
                "H3",
                "database.py:init_schema:execute_schema_error",
                "SCHEMA_SQL execution failed",
                {"error_type": type(e).__name__, "error": str(e)},
            )
            # #endregion
            cursor.close()
            raise


def init_database():
    create_database_if_not_exists()
    init_schema()
