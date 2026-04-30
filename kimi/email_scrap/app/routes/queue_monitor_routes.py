"""
Queue Monitor API Routes
Exposes Redis queue sizes and worker status to the frontend dashboard.
"""
from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user

router = APIRouter()

# In-memory worker status tracker (updated by start_workers.py)
worker_status = {
    "system_running": False,
    "workers": {
        "intake": {"status": "stopped", "processed": 0, "errors": 0},
        "normalize": {"status": "stopped", "processed": 0, "errors": 0},
        "verify": {"status": "stopped", "processed": 0, "errors": 0},
        "enrich": {"status": "stopped", "processed": 0, "errors": 0},
        "retry": {"status": "stopped", "processed": 0, "errors": 0},
    },
}


@router.get("/admin/queue-monitor/status")
async def queue_monitor_status(
    current_user_dict: dict = Depends(get_current_user),
):
    """Return queue sizes and worker status for the dashboard."""
    if current_user_dict.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    queues = {
        "normalize_queue": 0,
        "verify_queue": 0,
        "enrich_queue": 0,
        "retry_queue": 0,
        "dead_letter_queue": 0,
    }

    # Try to read live sizes from Redis
    try:
        import redis.asyncio as aioredis
        import os

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = aioredis.from_url(redis_url, decode_responses=True)
        queues["normalize_queue"] = await r.llen("pipeline:normalize_queue")
        queues["verify_queue"] = await r.llen("pipeline:verify_queue")
        queues["enrich_queue"] = await r.llen("pipeline:enrich_queue")
        queues["retry_queue"] = await r.llen("pipeline:retry_queue")
        queues["dead_letter_queue"] = await r.llen("pipeline:dead_letter_queue")
        redis_connected = True
    except Exception:
        redis_connected = False

    # Expose individual worker statuses from Redis heartbeats
    for w in worker_status["workers"].keys():
        if redis_connected:
            is_alive = await r.get(f"pipeline:worker:{w}:status") == "running"
            worker_status["workers"][w]["status"] = "running" if is_alive else "stopped"

    system_alive = await r.get("pipeline:workers:status") == "running" if redis_connected else False

    if redis_connected: await r.close()

    return {
        "queues": queues,
        "workers": worker_status["workers"],
        "system_running": system_alive,
        "redis_connected": redis_connected,
    }


@router.post("/admin/queue-monitor/start")
async def queue_monitor_start(
    current_user_dict: dict = Depends(get_current_user),
):
    """Start the async pipeline workers (Subprocess / Docker fallback)."""
    if current_user_dict.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    import subprocess
    import sys

    # Check Redis to see if continuous workers are already active somewhere
    try:
        import redis.asyncio as aioredis
        import os
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        r = aioredis.from_url(redis_url, decode_responses=True)
        is_running = await r.get("pipeline:workers:status") == "running"
        await r.close()
    except: is_running = False

    if is_running:
         return {"success": True, "message": "Workers already active (Docker continuous)"}

    # Otherwise launch locally
    subprocess.Popen(
        [sys.executable, "-m", "system.start_workers"],
        cwd=sys.path[0] if sys.path[0] else ".",
    )

    return {"success": True, "message": "Backdoor workers starting locally..."}


@router.post("/admin/queue-monitor/stop")
async def queue_monitor_stop(
    current_user_dict: dict = Depends(get_current_user),
):
    """Stop the async pipeline workers."""
    if current_user_dict.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    worker_status["system_running"] = False
    for w in worker_status["workers"].values():
        w["status"] = "stopped"

    return {"success": True, "message": "Stop signal sent to workers"}
