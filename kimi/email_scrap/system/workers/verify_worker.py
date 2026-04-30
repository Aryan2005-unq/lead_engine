"""
Verify Worker
Pulls items from verify_queue, runs FCC Form 499 checks in parallel,
writes active leads to DB, pushes enrichment candidates to enrich_queue.
"""
import asyncio

from system.core.config import config
from system.core.logger import WorkerLogger
from system.core import db
from system.queues import redis_client
from system.skills.verify import verify_batch

log = WorkerLogger("verify")


async def run_cycle():
    """Process one batch from the verify queue."""
    items = await redis_client.pop_batch(config.Q_VERIFY, config.VERIFY_BATCH)
    if not items:
        return False

    log.batch_start(len(items))

    # Run verification with concurrency limit
    results = await verify_batch(items, concurrency=config.VERIFY_CONCURRENCY)

    # Separate active vs inactive
    active = []
    failed = []
    for r in results:
        status = r.get("verify_status", "")
        if "Active" in status:
            active.append(r)
        elif status in ("Error", "BLOCKED", "Unknown"):
            r["_retry_count"] = r.get("_retry_count", 0) + 1
            r["_origin_queue"] = config.Q_VERIFY
            failed.append(r)

    # Bulk upsert active leads into the leads table
    if active:
        lead_rows = [
            {
                "company_name": a.get("business_name", a.get("company_name", "")),
                "email": a.get("contact_email", ""),
                "phone": a.get("contact_phone", ""),
                "verify_status": a.get("verify_status", ""),
            }
            for a in active
        ]
        await db.bulk_upsert_leads(lead_rows)

    # Push active leads missing email to enrich queue
    needs_email = [
        a for a in active
        if not a.get("contact_email") or "@" not in a.get("contact_email", "")
    ]
    if needs_email:
        await redis_client.push_batch(config.Q_ENRICH, needs_email)

    # Push failures to retry queue
    if failed:
        await redis_client.push_batch(config.Q_RETRY, failed)

    log.batch_end(len(active), len(failed))
    return True


async def loop():
    """Continuous loop."""
    log.info("Verify worker started")
    while True:
        try:
            had_work = await run_cycle()
            if not had_work:
                log.idle()
                await asyncio.sleep(config.IDLE_SLEEP)
        except Exception as e:
            log.error(f"Verify cycle error: {e}")
            await asyncio.sleep(config.IDLE_SLEEP)
