"""
Lead Extraction Engine -- Configuration Module.

Loads environment variables and defines constants used across the engine.
All sensitive values (API keys, DB credentials) are loaded from .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# ---------------------------------------------------------------------------
# Engine Database (separate from RMD)
# ---------------------------------------------------------------------------

ENGINE_DB = {
    "host": os.getenv("ENGINE_DB_HOST", "localhost"),
    "port": int(os.getenv("ENGINE_DB_PORT", "5432")),
    "dbname": os.getenv("ENGINE_DB_NAME", "lead_engine_db"),
    "user": os.getenv("ENGINE_DB_USER", "crwm_user"),
    "password": os.getenv("ENGINE_DB_PASSWORD", "crwm_password"),
}

# ---------------------------------------------------------------------------
# RMD Source Database (read-only access to existing system)
# ---------------------------------------------------------------------------

RMD_DB = {
    "host": os.getenv("RMD_DB_HOST", "localhost"),
    "port": int(os.getenv("RMD_DB_PORT", "5432")),
    "dbname": os.getenv("RMD_DB_NAME", "crwm_db"),
    "user": os.getenv("RMD_DB_USER", "crwm_user"),
    "password": os.getenv("RMD_DB_PASSWORD", "crwm_password"),
}

# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")

# ---------------------------------------------------------------------------
# Rate Limits (requests per minute)
# ---------------------------------------------------------------------------

APOLLO_RATE_LIMIT = int(os.getenv("APOLLO_RATE_LIMIT", "40"))
APOLLO_MONTHLY_CREDIT_LIMIT = int(os.getenv("APOLLO_MONTHLY_CREDIT_LIMIT", "8000"))
APOLLO_DAILY_CREDIT_LIMIT = int(os.getenv("APOLLO_DAILY_CREDIT_LIMIT", "200"))
APOLLO_MAX_CREDITS_PER_RUN = int(os.getenv("APOLLO_MAX_CREDITS_PER_RUN", "50"))

# ---------------------------------------------------------------------------
# Enrichment Settings
# ---------------------------------------------------------------------------

ENRICH_BATCH_SIZE = int(os.getenv("ENRICH_BATCH_SIZE", "100"))

# ---------------------------------------------------------------------------
# File Paths
# ---------------------------------------------------------------------------

DATA_DIR = BASE_DIR / "data"
EXPORT_DIR = BASE_DIR / os.getenv("EXPORT_DIR", "data/exports")
DOWNLOAD_DIR = DATA_DIR / "downloads"
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = BASE_DIR / os.getenv("LOG_FILE", "logs/extraction.log")

# Ensure directories exist
for directory in [DATA_DIR, EXPORT_DIR, DOWNLOAD_DIR, LOG_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Scoring Weights (must sum to 100)
# ---------------------------------------------------------------------------

SCORE_WEIGHTS = {
    "firmographics": 25,
    "role_fit": 30,
    "intent_signals": 25,
    "data_quality": 20,
}

# ---------------------------------------------------------------------------
# Target Job Titles for wholesale VoIP decision-makers
# ---------------------------------------------------------------------------

TARGET_TITLES = [
    "VP Wholesale", "Director Wholesale", "Head of Wholesale",
    "Carrier Relations", "Interconnect Manager", "Route Manager",
    "Voice Trading", "Head of Voice", "SIP Engineer",
    "Director of Operations", "VP Network", "VP Carrier", "CTO", "CEO",
]

# ---------------------------------------------------------------------------
# Company types relevant to wholesale VoIP
# ---------------------------------------------------------------------------

RELEVANT_COMPANY_TYPES = [
    "Interconnected VoIP", "IXC", "CLEC", "Carrier",
    "ITSP", "Call Center", "CPaaS",
]

# ---------------------------------------------------------------------------
# Competitor domains for partner page scraping
# ---------------------------------------------------------------------------

COMPETITOR_DOMAINS = {
    "idtexpress.com": "IDT Express",
    "bandwidth.com": "Bandwidth",
    "telnyx.com": "Telnyx",
    "acepeak.ai": "Acepeak",
    "flowroute.com": "Flowroute",
    "viirtue.com": "Viirtue",
    "siptrunk.com": "SIPTrunk",
}

# ---------------------------------------------------------------------------
# FCC 499A Filters
# ---------------------------------------------------------------------------

FCC_499A_ACTIVE_TYPES = [
    "Interconnected VoIP Provider",
    "Interexchange Carrier",
    "Competitive Local Exchange Carrier",
    "Local Reseller",
    "Toll Reseller",
]
