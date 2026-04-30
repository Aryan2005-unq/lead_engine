"""
Redis Queue Client
Provides push/pop batch operations, checkpoint persistence,
queue size monitoring, and dead-letter management.
"""
import json
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

from system.core.config import config

_redis: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    """Return (and lazily create) the async Redis connection."""
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            config.REDIS_URL, decode_responses=True
        )
    return _redis


async def close_redis():
    global _redis
    if _redis:
        await _redis.close()
        _redis = None


# ── Queue operations ──

async def push_batch(queue: str, items: List[Dict[str, Any]]):
    """Push a batch of JSON-serializable dicts onto a Redis list."""
    if not items:
        return
    r = await get_redis()
    pipe = r.pipeline()
    for item in items:
        pipe.rpush(queue, json.dumps(item))
    await pipe.execute()


async def pop_batch(queue: str, count: int) -> List[Dict[str, Any]]:
    """Pop up to `count` items from the left of a Redis list."""
    r = await get_redis()
    items: List[Dict[str, Any]] = []
    pipe = r.pipeline()
    for _ in range(count):
        pipe.lpop(queue)
    results = await pipe.execute()
    for raw in results:
        if raw is not None:
            items.append(json.loads(raw))
    return items


async def queue_size(queue: str) -> int:
    r = await get_redis()
    return await r.llen(queue)


# ── Checkpoint persistence ──

_CHECKPOINT_PREFIX = "pipeline:checkpoint:"


async def save_checkpoint(worker_name: str, value: Any):
    r = await get_redis()
    await r.set(f"{_CHECKPOINT_PREFIX}{worker_name}", json.dumps(value))


async def get_checkpoint(worker_name: str, default: Any = None) -> Any:
    r = await get_redis()
    raw = await r.get(f"{_CHECKPOINT_PREFIX}{worker_name}")
    if raw is None:
        return default
    return json.loads(raw)


# ── Dead letter ──

async def move_to_dead_letter(item: Dict[str, Any], reason: str = ""):
    """Move a permanently failed item to the dead-letter queue."""
    r = await get_redis()
    item["_dead_reason"] = reason
    await r.rpush(config.Q_DEAD, json.dumps(item))


# ── Cache helpers ──

_CACHE_PREFIX = "pipeline:cache:"


async def cache_get(key: str) -> Optional[str]:
    r = await get_redis()
    return await r.get(f"{_CACHE_PREFIX}{key}")


async def cache_set(key: str, value: str, ttl: int = 3600):
    r = await get_redis()
    await r.set(f"{_CACHE_PREFIX}{key}", value, ex=ttl)


# ── Monitoring ──

async def get_all_queue_sizes() -> Dict[str, int]:
    """Return sizes of all pipeline queues for monitoring."""
    queues = [
        config.Q_NORMALIZE,
        config.Q_VERIFY,
        config.Q_ENRICH,
        config.Q_RETRY,
        config.Q_DEAD,
    ]
    r = await get_redis()
    sizes = {}
    for q in queues:
        sizes[q.split(":")[-1]] = await r.llen(q)
    return sizes
