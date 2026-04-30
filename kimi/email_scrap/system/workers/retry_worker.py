"""
Retry Worker
Pulls items from retry_queue, applies exponential backoff, and re-routes
them to their origin queue. After MAX_RETRIES, items go to dead-letter.
"""
import asyncio

from system.core.config import config
from system.core.logger import WorkerLogger
from system.queues import redis_client

log = WorkerLogger("retry")


async def run_cycle():
    """Process one batch from the retry queue."""
    items = await redis_client.pop_batch(config.Q_RETRY, config.RETRY_BATCH)
    if not items:
        return False

    log.batch_start(len(items))

    requeued = 0
    dead = 0

    for item in items:
        retry_count = item.get("_retry_count", 0)
        origin = item.get("_origin_queue", "")

        if retry_count >= config.MAX_RETRIES:
            await redis_client.move_to_dead_letter(
                item, reason=f"exceeded {config.MAX_RETRIES} retries"
            )
            dead += 1
            continue

        # Exponential backoff sleep
        delay = config.RETRY_BACKOFF_BASE ** retry_count
        await asyncio.sleep(min(delay, 30))

        # Increment counter and push back to origin
        item["_retry_count"] = retry_count + 1
        if origin:
            await redis_client.push_batch(origin, [item])
        else:
            # Default to verify queue if origin unknown
            await redis_client.push_batch(config.Q_VERIFY, [item])
        requeued += 1

    log.batch_end(requeued, dead)
    return True


async def loop():
    """Continuous loop."""
    log.info("Retry worker started")
    while True:
        try:
            had_work = await run_cycle()
            if not had_work:
                log.idle()
                await asyncio.sleep(config.IDLE_SLEEP * 2)
        except Exception as e:
            log.error(f"Retry cycle error: {e}")
            await asyncio.sleep(config.IDLE_SLEEP)
