"""
Enrich Worker
Pulls leads from enrich_queue, runs Playwright email scraping in
limited concurrency, and bulk-updates the leads table.
"""
import asyncio

from system.core.config import config
from system.core.logger import WorkerLogger
from system.core import db
from system.queues import redis_client
from system.skills.enrich import enrich_batch, close_browser

log = WorkerLogger("enrich")


async def run_cycle():
    """Process one batch from the enrich queue."""
    items = await redis_client.pop_batch(config.Q_ENRICH, config.ENRICH_BATCH)
    if not items:
        return False

    log.batch_start(len(items))

    results = await enrich_batch(items, concurrency=config.ENRICH_CONCURRENCY)

    # Collect successful enrichments for bulk DB update
    updates = []
    success = 0
    failed = 0
    for r in results:
        email = r.get("enriched_email", "")
        lead_id = r.get("lead_id") or r.get("id")
        if email and "@" in email and lead_id:
            updates.append({"lead_id": lead_id, "email": email})
            success += 1
        elif r.get("enrich_error"):
            r["_retry_count"] = r.get("_retry_count", 0) + 1
            r["_origin_queue"] = config.Q_ENRICH
            await redis_client.push_batch(config.Q_RETRY, [r])
            failed += 1
        else:
            failed += 1

    if updates:
        await db.bulk_update_lead_emails(updates)

    log.batch_end(success, failed)
    return True


async def loop():
    """Continuous loop."""
    log.info("Enrich worker started")
    try:
        while True:
            try:
                had_work = await run_cycle()
                if not had_work:
                    log.idle()
                    await asyncio.sleep(config.IDLE_SLEEP)
            except Exception as e:
                log.error(f"Enrich cycle error: {e}")
                await asyncio.sleep(config.IDLE_SLEEP)
    finally:
        await close_browser()
        log.shutdown()
