import asyncio
import time
import random
import json
import os
import sys
import logging
from typing import List, Dict, Optional, Set

from curl_cffi.requests import AsyncSession
from bs4 import BeautifulSoup

# --- PATH RESOLUTION ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", "..", ".."))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from system.core.db import get_pool, close_pool

# --- LOGGING CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

# Safe print helper to prevent Unicode crashes on Windows
def safe_print(*args, **kwargs):
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        message = ' '.join(str(arg) for arg in args)
        replacements = {'💾': '[SAVED]', '📊': '[STATS]', '✅': '[OK]', '❌': '[ERROR]', '⚠️': '[WARNING]'}
        for e, t in replacements.items():
            message = message.replace(e, t)
        print(message.encode('ascii', 'replace').decode('ascii'), **kwargs)

FCC_FORM_499_URL = "https://apps.fcc.gov/cgb/form499/499results.cfm"

# Concurrency & Batch sizes
MAX_CONCURRENT_REQUESTS = 40  # Controlled concurrent workers
BATCH_SIZE = 100              # Number of rows processed per buffer cycle

BROWSER_PROFILES = [
    {"impersonate": "chrome120", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"},
    {"impersonate": "chrome119", "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/119.0.0.0"},
    {"impersonate": "safari15_5", "ua": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) Chrome/120.0 Safari/604.1"},
    {"impersonate": "edge101", "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edg/101.0.1210.53"}
]

# ---------------------------------------------------------------------
# 1. Database Schema Initializer
# ---------------------------------------------------------------------

async def init_schema():
    """Ensures structure triggers are updated to support resume-safe and bulk indices."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        logging.info("Initializing schema locks and indexes...")
        # 1. Soft process tracker
        await conn.execute("ALTER TABLE staging_fcc_listings ADD COLUMN IF NOT EXISTS processed BOOLEAN DEFAULT FALSE")
        
        # 2. Unique constraints on leads(email) to enable ON CONFLICT triggers
        try:
            # Check unique constraints on table leads for 'email'
            constraint_check = await conn.fetchval("""
                SELECT 1 FROM information_schema.table_constraints 
                WHERE table_name = 'leads' AND constraint_name = 'leads_email_key'
            """)
            if not constraint_check:
                logging.info("Adjusting lead schema for unique email keys...")
                # Safe deduplicate before Alter Constraint addition
                await conn.execute("DELETE FROM leads a USING leads b WHERE a.id < b.id AND a.email = b.email")
                await conn.execute("ALTER TABLE leads ADD CONSTRAINT leads_email_key UNIQUE (email)")
        except Exception as e:
            logging.debug(f"Constraint alter debug: {e}")


# ---------------------------------------------------------------------
# 2. FCC API Service (Rotating Agent Proxy)
# ---------------------------------------------------------------------

def parse_fcc_results(html_content: str, result_dict: Dict) -> bool:
    """Helper to parse the result table from HTML."""
    import re
    if "1 Record Found" in html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        text_content = soup.get_text(" ", strip=True)
        
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            header_index = -1
            for i, row in enumerate(rows):
                cells = row.find_all(['th', 'td'])
                cell_texts = [c.get_text(strip=True) for c in cells]
                if '499 Filer ID' in cell_texts:
                    header_index = i; continue
                if header_index != -1 and len(cells) >= 2:
                    filer_id = cells[0].get_text(strip=True)
                    if filer_id.isdigit() and len(filer_id) >= 4:
                        result_dict.update({
                            'filer_id': filer_id,
                            'legal_name': cells[1].get_text(strip=True),
                            'dba': cells[2].get_text(strip=True) if len(cells) > 2 else 'N/A',
                            'status': 'Active'
                        })
                        return True
        result_dict['status'] = 'Active (Parse Fail)'
        ids = re.findall(r'\b\d{6}\b', text_content)
        if ids: result_dict['filer_id'] = ids[0] + "?"
        return True
    return False


async def check_fcc_form499(frn: str, legal_name: str = '') -> Dict:
    """Queries FCC database using impersonate rotations with high tolerance."""
    params = {'FilerID': '', 'frn': frn, 'operational': '', 'comm_type': 'Any Type', 'LegalName': '', 'state': 'Any State', 'R1': 'and', 'XML': 'FALSE'}
    res = {'status': 'Not Found', 'filer_id': 'N/A', 'legal_name': 'N/A', 'dba': 'N/A', 'link': f"https://apps.fcc.gov/cgb/form499/499results.cfm?frn={frn}", 'error': ''}

    for attempt in range(3):
        profile = random.choice(BROWSER_PROFILES)
        headers = {'Referer': 'https://apps.fcc.gov/cgb/form499/499a.cfm', 'User-Agent': profile['ua']}
        try:
            async with AsyncSession(impersonate=profile['impersonate'], headers=headers) as s:
                resp = await s.get(FCC_FORM_499_URL, params=params, timeout=45)
                if resp.status_code == 200:
                    if "Access Denied" in resp.text or "Akamai" in resp.text:
                        await asyncio.sleep(random.uniform(1.5, 3.0))
                        continue
                    if parse_fcc_results(resp.text, res):
                         return res

                    # Name search fallback
                    if legal_name and len(legal_name) > 3:
                        params_name = params.copy()
                        params_name['frn'] = ''; params_name['LegalName'] = legal_name 
                        await asyncio.sleep(0.3)
                        resp_name = await s.get(FCC_FORM_499_URL, params=params_name, timeout=45)
                        if resp_name.status_code == 200 and parse_fcc_results(resp_name.text, res):
                             res.update({'status': 'Active (Matched by Name)', 'link': f"{FCC_FORM_499_URL}?LegalName={legal_name}"})
                             return res
                    return res
                elif resp.status_code in [429, 503]:
                    await asyncio.sleep(random.uniform(3.0, 5.0))
        except Exception as e:
            await asyncio.sleep(random.uniform(1.0, 2.5))
            res['error'] = f"{type(e).__name__}: {str(e)[:50]}"
    return res


# ---------------------------------------------------------------------
# 3. Main Processor Functions
# ---------------------------------------------------------------------

async def fetch_batch(limit: int = BATCH_SIZE) -> List[Dict]:
    """Fetch unprocessed staging rows flattening other_data bindings."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT frn, business_name, sys_id, other_data 
            FROM staging_fcc_listings 
            WHERE processed = false 
            LIMIT $1
        """, limit)
        
        results = []
        for r in rows:
            item = dict(r)
            other = item.pop("other_data") or {}
            # Flatten to simplify row parsing fallback lists
            item.update(other)
            results.append(item)
        return results


async def verify_lead(row: Dict, sem: asyncio.Semaphore) -> Optional[Dict]:
    """Verifies singular listing against constraints maintaining Semaphore concurrency."""
    async with sem:
        await asyncio.sleep(random.uniform(0.01, 0.1))
        
        # Standardize fallback keys
        frn = (row.get('FRN') or row.get('frn') or row.get('FCC Registration Number (FRN)') or '').strip()
        if not frn: return None
        
        country = (row.get('Country') or row.get('country') or '').strip().lower()
        if country and country not in ['united states', 'usa', 'us', 'puerto rico', '']:
            return None

        company_name = (row.get('Business Name') or row.get('business_name') or '').strip()
        
        # Verify
        v_res = await check_fcc_form499(frn, company_name)
        
        if 'Active' in v_res['status']:
            email = (row.get('Contact Email') or row.get('contact_email') or '').strip()
            phone = (row.get('Contact Telephone Number') or row.get('Contact Phone Number') or 
                     row.get('contact_telephone_number') or row.get('contact_phone_number') or '').strip()
                     
            return {
                'company_name': company_name,
                'email': email,
                'phone': phone,
                'verify_status': v_res['status']
            }
        return None


async def bulk_insert(rows: List[Dict]):
    """Insert into leads with dynamic updates resolving email overlaps."""
    if not rows: return
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            stmt = await conn.prepare("""
                INSERT INTO leads (company_name, email, phone, verify_status)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (email) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    phone = COALESCE(NULLIF(EXCLUDED.phone, ''), leads.phone),
                    verify_status = EXCLUDED.verify_status
            """)
            await stmt.executemany([
                (r['company_name'], r['email'], r.get('phone', ''), r.get('verify_status', 'Active'))
                for r in rows
            ])


async def mark_processed(frns: List[str]):
    """Update staging identifiers to bypass processed cycles."""
    if not frns: return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE staging_fcc_listings SET processed = true WHERE frn = ANY($1)", frns)


# ---------------------------------------------------------------------
# 4. Orchestration Entry Point
# ---------------------------------------------------------------------

async def process_batch_cycle(batch: List[Dict], sem: asyncio.Semaphore) -> (int, int):
    """Schedules concurrent batches resolving insertion constraints."""
    tasks = [verify_lead(row, sem) for row in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    insertable = []
    for res in results:
        if res and isinstance(res, dict):
            email = res.get("email")
            # Only save records containing valid email strings into the leads board
            if email and "@" in email:
                insertable.append(res)
    
    if insertable:
        await bulk_insert(insertable)
        
    await mark_processed([r["frn"] for r in batch])
    return len(batch), len(insertable)


async def main():
    await init_schema()
    safe_print(f"Starting async verification cycle with max concurrency {MAX_CONCURRENT_REQUESTS}...")
    
    start_time = time.time()
    processed_count = 0
    verified_count = 0
    batch_num = 1
    
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    while True:
        batch = await fetch_batch(limit=BATCH_SIZE)
        if not batch:
            safe_print("✅ No more unprocessed listings found. Database synchronization synchronized.")
            break
            
        logging.info(f"Processing Batch {batch_num} with {len(batch)} records...")
        cycle_start = time.time()
        
        proc, ins = await process_batch_cycle(batch, sem)
        
        processed_count += proc
        verified_count += ins
        elapsed = time.time() - start_time
        rate = processed_count / elapsed if elapsed > 0 else 0
        
        safe_print(
            f"📊 Progress: Batch {batch_num} | Processed: {proc} | Inserted Leads: {ins} | "
            f"Total Processed: {processed_count} | Total Leads: {verified_count} | Speed: {rate:.2f} req/sec"
        )
        batch_num += 1

    logging.info(f"Finished processing. Total Checked: {processed_count}, Leads Added: {verified_count} in {time.time()-start_time:.2f}s")
    await close_pool()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        safe_print("\n⚠️ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        safe_print(f"❌ Script Crash: {type(e).__name__}: {str(e)[:150]}")
        sys.exit(1)