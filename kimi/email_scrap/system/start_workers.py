"""
Pipeline Worker Launcher
Starts all workers concurrently via asyncio.gather.
Supports --workers flag for horizontal scaling, e.g.:
    python -m system.start_workers --workers verify=3 enrich=2
"""
import argparse
import asyncio
import signal
import sys

from system.core.config import config
from system.core.logger import WorkerLogger
from system.core import db
from system.queues import redis_client

# Workers
from system.workers import (
    intake_worker,
    normalize_worker,
    verify_worker,
    enrich_worker,
    retry_worker,
)

log = WorkerLogger("launcher")
_shutdown = False


def _handle_sigint(*_):
    global _shutdown
    _shutdown = True
    log.info("Shutdown signal received — stopping workers gracefully")


async def main():
    parser = argparse.ArgumentParser(description="Pipeline Worker Launcher")
    parser.add_argument(
        "--workers",
        nargs="*",
        default=[],
        help="Scale workers: verify=3 enrich=2",
    )
    parser.add_argument("--dry-run", action="store_true", help="Boot check only")
    args = parser.parse_args()

    # Parse scaling overrides
    scale = {"intake": 1, "normalize": 1, "verify": 1, "enrich": 1, "retry": 1}
    for pair in args.workers:
        if "=" in pair:
            name, count = pair.split("=", 1)
            if name in scale:
                scale[name] = int(count)

    # ── Boot checks ──
    log.info("Connecting to PostgreSQL...")
    pool = await db.get_pool()
    log.info(f"DB pool ready (min={config.DB_POOL_MIN}, max={config.DB_POOL_MAX})")

    log.info("Connecting to Redis...")
    r = await redis_client.get_redis()
    await r.ping()
    log.info("Redis connected")

    # Ensure indexes
    await db.ensure_indexes()
    log.info("DB indexes ensured")

    # Print queue sizes
    sizes = await redis_client.get_all_queue_sizes()
    log.info(f"Queue sizes: {sizes}")

    if args.dry_run:
        log.info("Dry-run complete — all connections OK")
        await db.close_pool()
        await redis_client.close_redis()
        return

    # ── Build worker task list ──
    signal.signal(signal.SIGINT, _handle_sigint)

    worker_map = {
        "intake": intake_worker.loop,
        "normalize": normalize_worker.loop,
        "verify": verify_worker.loop,
        "enrich": enrich_worker.loop,
        "retry": retry_worker.loop,
    }

    tasks = []
    for name, loop_fn in worker_map.items():
        count = scale.get(name, 1)
        for i in range(count):
            label = f"{name}#{i}" if count > 1 else name
            log.info(f"Launching worker: {label}")
            tasks.append(asyncio.create_task(loop_fn()))

    log.info(f"All {len(tasks)} workers running — press Ctrl+C to stop")

    # Monitor loop — print queue sizes every 60s and update heartbeat every 10s
    async def _monitor():
        try:
            r_any = await redis_client.get_redis()
            while not _shutdown:
                # Set heartbeat with 30s TTL
                await r_any.setex("pipeline:workers:status", 30, "running")
                for w_name in worker_map.keys():
                    await r_any.setex(f"pipeline:worker:{w_name}:status", 30, "running")
                
                await asyncio.sleep(10)
        except Exception as e:
            log.error(f"Monitor heartbeat failed: {e}")

    tasks.append(asyncio.create_task(_monitor()))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    finally:
        log.shutdown()
        await db.close_pool()
        await redis_client.close_redis()


if __name__ == "__main__":
    asyncio.run(main())
