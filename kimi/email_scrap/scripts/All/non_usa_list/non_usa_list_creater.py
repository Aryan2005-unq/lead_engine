#!/usr/bin/env python3
"""
-----------------------------------------------------------------------
FCC NON-USA FILTER CREATOR (DATABASE-DRIVEN)
-----------------------------------------------------------------------
This script replaces slow CSV-based list comparison filters with high-performance 
PostgreSQL speed update joins. It marks records as USA-based for fast filtering.

LOGIC:
1.  **Schema Update**: Ensures `staging_fcc_listings` contains `is_usa` column.
2.  **Constraint Check**: Handles indices transparently leveraging existing unique tables keys.
3.  **Fast update Join**: Matches `staging_fcc_listings` with `usa_frn_table` in-memory lookup joins.
4.  **Logging Aggregates**: Queries summary updates resolving state profiles.

USAGE:
    python3 non_usa_list_creater.py
-----------------------------------------------------------------------
"""

import sys
import os
import asyncio
import logging
import time

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

async def setup_schema():
    """Defines columns and indexes conforming to bulk updates design specs."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        logging.info("Optimizing table indices and schemas...")
        
        # 1. Add is_usa soft filter column and tracker
        await conn.execute("ALTER TABLE staging_fcc_listings ADD COLUMN IF NOT EXISTS is_usa BOOLEAN DEFAULT FALSE")
        
        # 2. Add speed indices safely where necessary
        # Note: staging_fcc_listings has a UNIQUE constraint on `frn`, meaning a index is already online automatically.
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_fcc_is_usa ON staging_fcc_listings(is_usa)")
        
        # 3. Create target specs for usa list supporting specifications
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS usa_frn_table (
                frn VARCHAR(100) PRIMARY KEY
            )
        """)


async def execute_filter():
    """Executes the inner speed join marking USA records inside the pipeline table."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        logging.info("Executing speed filter update workflow joining against usa_frn_table...")
        cycle_start = time.time()
        
        # Fast SQL UPDATE Join
        # Marks is_usa = TRUE where matches are loaded in usa_frn_table
        stmt = """
            UPDATE staging_fcc_listings s
            SET is_usa = TRUE
            FROM usa_frn_table u
            WHERE s.frn = u.frn AND s.is_usa = FALSE
        """
        result = await conn.execute(stmt)
        # Extract row count updated (Example: "UPDATE 1540")
        updated = int(result.split()[-1]) if result and "UPDATE" in result else 0
        
        logging.info(f"Speed Update finished in {time.time()-cycle_start:.2f}s. Marked {updated} rows matching USA index.")
        return updated


async def print_statistics():
    """Aggregates and prints summary report profiling pipeline coverage."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_usa = TRUE THEN 1 ELSE 0 END) as matches_usa,
                SUM(CASE WHEN is_usa = FALSE THEN 1 ELSE 0 END) as non_usa
            FROM staging_fcc_listings
        """)
        
        total = stats["total"] or 0
        usa = stats["matches_usa"] or 0
        non_usa = stats["non_usa"] or 0
        
        print("\n--- 📊 FILTERING DATABASE SUMMARY ---")
        print(f"Total FCC Listings Found : {total}")
        print(f"USA Matched Rows (Filtered) : {usa}")
        print(f"NON-USA Records Remaining   : {non_usa}\n")


async def main():
    print(f"\n--- 🚀 RUNNING DATABASE FILTER JOIN WORKFLOW ---\n")
    start = time.time()
    
    # 1. Init schema offsets
    await setup_schema()
    
    # 2. Update stats
    await execute_filter()
    
    # 3. Print Logs
    await print_statistics()
    await close_pool()
    
    print(f"✅ DONE in {time.time()-start:.2f} seconds.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        import traceback
        logging.error(f"❌ Script Crash: {type(e).__name__}: {e}")
        # print stacktrace if detail required
        sys.exit(1)
