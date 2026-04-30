# Wholesale VoIP Lead Extraction Engine

A fully automated, CLI-driven data pipeline designed to extract, deduplicate, enrich, score, and export high-intent wholesale VoIP leads for the Sales Platform.

## 🏗️ System Architecture

The engine operates in a 5-stage pipeline:

1. **Extraction (9 Sources):** Scrapes raw companies and contacts using Playwright, REST APIs, and database syncs.
2. **Deduplication:** Merges raw records into `companies` and `contacts` master tables based on domains and emails.
3. **Enrichment (Apollo API):** A 4-step waterfall process to grab firmographics, find decision-makers, match emails, and detect hiring signals.
4. **Scoring:** Evaluates each contact out of 100 points (Firmographics, Role, Intent, Data Quality) to assign Tiers (A, B, C).
5. **Export:** Generates structured CSVs ready for immediate ingestion into the Sales Platform.

---

## 🛠️ Prerequisites & Setup

### 1. Environment Requirements
* Python 3.14+
* PostgreSQL 14+ (Running via Docker on port 5433)
* Playwright & Chromium

### 2. Installation
```bash
# 1. Activate virtual environment
.venv\Scripts\activate

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Install Playwright browser binaries
playwright install chromium
```

### 3. Environment Variables (`.env`)
Create a `.env` file in the root directory (copy from `.env.example`).
Ensure your database ports match your Docker configuration (usually `5433` for the engine to avoid native Windows clashes):
```env
ENGINE_DB_PORT=5433
RMD_DB_PORT=5433
APOLLO_API_KEY=your_key_here
```

### 4. Initialize Database
Run this once to create the schema and tables:
```bash
python main.py --init
```

---

## 🚀 CLI Usage Guide

The engine is controlled entirely via `main.py`. 

### The "One-Click" Run
Run the entire pipeline sequentially (Extract -> Dedup -> Enrich -> Score -> Export):
```bash
python main.py --all
```

### 1. Extraction (Ingestors)
Run specific sources individually:
```bash
python main.py --source fcc_499a      # FCC Form 499A Database
python main.py --source crtc          # CRTC Canada
python main.py --source events        # ITW & Capacity Europe
python main.py --source tcxc          # TelecomsXchange
python main.py --source job_board     # Indeed hiring signals
python main.py --source competitors   # Competitor partner pages
python main.py --source fcc_notices   # FCC Daily Digest
python main.py --source apollo        # Apollo Proactive Search
python main.py --source rmd           # Legacy crwm_db sync
```

### 2. Deduplication
Merge all raw data into the master tables:
```bash
python main.py --dedup
```

### 3. Apollo Enrichment
*Apollo handles both data discovery and email verification natively.*
```bash
python main.py --enrich               # Run all 4 steps below sequentially

# Or run individual steps to save credits:
python main.py --enrich-companies     # Step 1: Add size, industry, tech stack
python main.py --enrich-contacts      # Step 2: Find decision-makers at companies
python main.py --match                # Step 3: Match scraped names to verified emails
python main.py --signals              # Step 4: Fetch job posting intent signals
```

### 4. Scoring
Calculate scores (0-100) and assign Tiers (A, B, C):
```bash
python main.py --score
```

### 5. Export
Export CSV files to `data/exports/` for the Sales Platform:
```bash
python main.py --export               # Export all new leads
python main.py --export --tier A      # Export ONLY Tier A leads
python main.py --export --tier B      # Export Tier A and Tier B leads
```

### 6. Analytics
View total records, tier breakdowns, and recent extraction runs:
```bash
python main.py --stats
```

---

## 📊 Scoring Model (100 Points)

Contacts are assigned a Tier based on their total score. 
* **Tier A:** >= 70 points
* **Tier B:** 45 - 69 points
* **Tier C:** < 45 points

**Point Distribution:**
* **Firmographics (Max 25):** Carrier/VoIP type, presence in FCC/CRTC, company size.
* **Role Fit (Max 30):** VIP titles (VP Carrier Relations, CEO, CTO).
* **Intent Signals (Max 25):** Detected at events, newly approved FCC licenses, active VoIP hiring.
* **Data Quality (Max 20):** Email verified by Apollo, LinkedIn URL present, Phone number present.

---

## 🛡️ Credit Protection
The Apollo enrichment module (`apollo_enrich.py`) includes an automatic circuit breaker. If you hit 8,000 credits used in the current calendar month, the engine will safely skip further enrichment to protect your free tier limits (10,000/mo).
