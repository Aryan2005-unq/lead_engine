# 📊 CRWM Email Scraping & Dashboard System

A robust **FastAPI Dashboard** that manages automated web scraping and email verification pipelines using Playwright and asynchronous worker execution. Fully integrated with a **Postgres Database** and **Redis-backed state management** for a true **Zero-CSV footprint**.

---

## 🚀 Key Features

*   **Autonomous Automation Pipeline**: Triggers web scrapers and verification tasks in parallel using elastic background contexts.
*   **Database-Driven ETL**: Intermediate data storage operates purely in local SQL staging tables (`staging_fcc_listings`), removing speed bottlenecks and file hazards.
*   **Intuitive Dashboard UI**: Real-time stats (Total, Verified, Ticked), responsive search filters, and full team-management node structure lists.
*   **Redis Distributed State Controller**: Syncs pipeline checkpoints in memory rather than local disk files. Progress survives container restarts and scales across clusters safely.
*   **Production NGINX Reverse Proxy**: Bundled configurations shielding endpoints providing static asset caching and Rate Limiting automatically.
*   **Role-Based Access Control**: Standard credential encryption protecting Admin dashboards and Member endpoints transparently.

---

## 📂 Project Structure

### 🛠️ Backend (`app/`)
*   **`main.py`**: FastAPI entry point initializing database pools, endpoints, and CORS setups.
*   **`auth.py`**: Handles login cycles, session validation, and hash validations.
*   **`database.py`**: Postgres pooling coordinates initial SQL creates and defaults loading.
*   **`logging_system.py`**: Custom Activity-Log handlers streaming telemetry triggers live to dashboard endpoints.
*   **`routes/`**: Endpoint layer:
    *   `auth_routes.py`: Login/Logout and Employee creation loops triggers.
    *   `pipeline_routes.py`: Standard trigger coordinates schedule cycle locks.
    *   `script_routes.py`: Operates manual subscripts loads individually.
    *   `email_routes.py`: Performs read updates on targets list aggregates.

### 🤖 Scraping Pipelines (`scripts/`)
*   **`Pipeline/` (Unified orchestrations)**:
    *   **`unified_async_pipeline.py`**: *Next-Gen Parallel Detail Crawler*. Uses concurrent browser Context Pools scraping listings and verifying emails simultaneously without intermediate CSV handoff buffers.
    *   **`run_pipeline.py`**: Sequential multi-stage grid workflow execution bundle chain framework.
*   **`All/` (Legacy Sequential Subscripts)**:
    *   `update_listings.py`, `verify_leads_fast.py`, `fetch_emails_browser.py` grouped by task layouts.

---

## ⚙️ The Parallel Streaming Workflow

The `unified_async_pipeline.py` runs a High-Performance loop:
1.  **Extract All Links First**: Evaluates ServiceNow lists collecting targeted sub-grids directly via Angular Scope hooks.
2.  **Parallel Scraper**: Opens direct detail context sub-buffers concurrently over pooled nodes concurrency weights.
3.  **Self-healing Deduplication**: Checks local buffer seen offsets framing incremental loads seamlessly.
4.  **Bulk Updates**: Performs Conflict overlays Upserts triggers to Postgres fast `executemany` triggers.

---

## 🗄️ Database Architecture (Postgres)

#### 1. `users` & `companies`
Standard tenant nodes managing administrative panel visibility guards correctly.

#### 2. `staging_fcc_listings` (The Intermediate Cache)
Absorbs variables dynamic coordinates without breaking static rigid grids models leveraging JSONB buffers.

#### 3. `leads` (Dashboard Aggregate Targets)
Binds `company_name`, `email`, `phone`, and `verify_status` (e.g. "Active", "Verified", "Not Found") for live view management.

---

## 🐳 Setup with Docker Compose (Recommended)

### Step 1: Initialize Environment
Copy the example configuration layout:
```bash
cp .env.example .env
```
_Edit `.env` to supply your Database keys, environments setups, or endpoints ports._

### Step 2: Launch Container Services
Run Docker Compose building isolated worker and dashboard instances nodes transparently setups:
```bash
docker compose up --build -d
```

### Step 3: Access Dashboard
-   Dashboard Host: **`http://localhost:8067`** (Mapped directly or reverse proxies transparently triggers)
-   Default Login: `admin@example.com` / `1234`

---

## 🛠️ Local Development Setup (Manual)

### Pre-requisites
-   Python 3.10+ & PostgreSQL available setups.

```powershell
# 1. Initialize Workspace Venv
python -m venv .venv
.venv\Scripts\activate      # Windows
source .venv/bin/activate   # Mac/Linux

# 2. Install Packages
pip install -r requirements.txt

# 3. Pull Chrome Binaries
playwright install chromium

# 4. Boot Framework
uvicorn app.main:app --host 127.0.0.1 --port 8067 --reload
```

---

## 🛡️ Maintenance & Troubleshooting

*   **Clearing Database Resets Page 1 Progression**: 
    Because pipeline checkpoints are synced to Redis (`pipeline:checkpoint:unified_pipeline`), clearing Postgres database **retains previous progression states**. 
    *   **Fix**: Call endpoint `POST /admin/pipeline/reset-checkpoint` using API client (or interface layouts) explicit triggers to wipe checkpoint memory buffers restarting lists triggers safely from Page 1.
*   **State Aborts Lockouts**: If schedules freeze or overload nodes weights, press **`Terminate All`** inside dashboards forcing active execution streams resets backwards gracefully recursive.
