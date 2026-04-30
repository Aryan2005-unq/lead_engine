"""
Normalize Worker
Pulls raw rows from normalize_queue, cleans/deduplicates, writes to DB,
and pushes cleaned items into verify_queue.
"""
import asyncio

from system.core.config import config
from system.core.logger import WorkerLogger
from system.core import db
from system.queues import redis_client
from system.skills.normalize import normalize_batch

log = WorkerLogger("normalize")


async def run_cycle():
    """Process one batch from the normalize queue with SQL index filter joins."""
    items = await redis_client.pop_batch(config.Q_NORMALIZE, config.NORMALIZE_BATCH)
    if not items:
        return False  # queue empty

    log.batch_start(len(items))
    cleaned = normalize_batch(items)

    # 1. Bulk upsert cleaned rows into staging
    await db.bulk_upsert_staging(cleaned)

    # 2. SQL Bulk Filter Join (Requirement #2: Filter USA records using DB join)
    pool = await db.get_pool()
    async with pool.acquire() as conn:
         # Speeds updates triggering soft exclusion filters
         await conn.execute("""
            UPDATE staging_fcc_listings s
            SET is_usa = TRUE
            FROM usa_frn_table u
            WHERE s.frn = u.frn AND s.is_usa = FALSE
         """)

    # 3. Push to verify queue
    await redis_client.push_batch(config.Q_VERIFY, cleaned)
    log.batch_end(len(cleaned), len(items) - len(cleaned))
    return True


async def loop():
    """Continuous loop."""
    log.info("Normalize worker started")
    while True:
        try:
            had_work = await run_cycle()
            if not had_work:
                log.idle()
                await asyncio.sleep(config.IDLE_SLEEP)
        except Exception as e:
            log.error(f"Normalize cycle error: {e}")
            await asyncio.sleep(config.IDLE_SLEEP)
