"""
Service logic for managing the Automation Pipeline (Data Scraping/Verification).
Supports Manual triggers and Autonomous schedule loops with json state persistence.
"""
from app.database import get_db_connection
import asyncio
import json
import os
import subprocess
from datetime import datetime

from typing import Dict, Any

import redis

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"), 
    port=int(os.getenv("REDIS_PORT", 6379)), 
    db=0, 
    decode_responses=True
)

active_subprocess = None

class PipelineStatusProxy:

    def __getitem__(self, key):
        if key == "is_running":
            return redis_client.get("pipeline:is_running") == "True"
        elif key == "is_active":
            return redis_client.get("pipeline:is_active") == "True"
        elif key == "interval_seconds":
            return int(redis_client.get("pipeline:interval_seconds") or 3600)
        elif key in ["error", "last_run", "next_run"]:
            return redis_client.get(f"pipeline:{key}")
        elif key == "logs":
            return redis_client.lrange("pipeline:logs", 0, -1)
        return None

    def __setitem__(self, key, value):
        if key in ["is_running", "is_active"]:
            redis_client.set(f"pipeline:{key}", "True" if value else "False")
        elif key in ["interval_seconds", "error", "last_run", "next_run"]:
            redis_client.set(f"pipeline:{key}", str(value) if value is not None else "")

    def setdefault(self, key, default):
         return self[key]

    def get(self, key, default=None):
        val = self[key]
        # For logs, return empty list if val is empty/None for dictionary fallback matching
        if key == "logs" and not val:
            return []
        return val if val is not None else default

# Globally replace dict with Proxy

pipeline_status = PipelineStatusProxy()

def load_pipeline_state():
    return {
        "is_running": pipeline_status["is_running"],
        "interval_seconds": pipeline_status["interval_seconds"],
        "last_run": pipeline_status["last_run"]
    }

def save_pipeline_state():
    # Variables already saved directly through setitem trigger buffers
    pass



def stop_active_pipeline():
    # Force state reset into Redis as absolute fail-safe fallback
    redis_client.set("pipeline:is_active", "False")
    
    if active_subprocess:
        try:
             active_subprocess.terminate()
             active_subprocess.kill()
             return {"success": True, "message": "Pipeline execution stopped forcefully."}
        except Exception as e:
             return {"success": False, "message": f"Failed to stop process: {e}"}
             
    return {"success": True, "message": "Forced state reset to idle completed."}


def reset_pipeline_checkpoint():
    """Reset pipeline checkpoint so next run starts from page 1."""
    try:
        # Clear Redis checkpoint key
        redis_client.delete("pipeline:checkpoint:unified_pipeline")
        
        # Remove legacy JSON checkpoint file if it exists
        legacy_file = os.path.join("scripts", "Pipeline", "pipeline_checkpoint.json")
        if os.path.exists(legacy_file):
            os.remove(legacy_file)
        
        return {"success": True, "message": "Pipeline checkpoint reset. Next run will start from page 1."}
    except Exception as e:
        return {"success": False, "message": f"Failed to reset checkpoint: {e}"}

async def start_autonomous_pipeline(interval: int = 3600):
    if pipeline_status["is_running"]:
        return {"success": True, "message": "Pipeline Schedule is already running."}
    
    pipeline_status["is_running"] = True
    pipeline_status["interval_seconds"] = interval
    save_pipeline_state()
    asyncio.create_task(_pipeline_worker_loop())
    return {"success": True, "message": "Autonomous Pipeline Schedule Started."}


async def stop_autonomous_pipeline():
    pipeline_status["is_running"] = False
    save_pipeline_state()
    return {"success": True, "message": "Autonomous Pipeline Schedule Stopped."}


# 🛠️ Allowlist of individual subscripts with safe environments
SUPPORTED_SCRIPTS = {
    "update_listings": {
        "title": "Listings Updater",
        "script": "update_listings.py",
        "cwd": os.path.join("scripts", "All", "All_list"),
        "args": []
    },
    "verify_leads": {
        "title": "Verify Leads",
        "script": "verify_leads_fast.py",
        "cwd": os.path.join("scripts", "All", "usa_list"),
        "args": []
    },
    "fetch_emails": {
        "title": "Fetch Emails",
        "script": "fetch_emails_browser.py",
        "cwd": os.path.join("scripts", "All", "fetch_emails"),
        "args": []
    },
    "fetch_emails_non_usa": {
        "title": "Fetch Emails Non-USA",
        "script": "fetch_emails_browser_non_usa.py",
        "cwd": os.path.join("scripts", "All", "fetch_emails"),
        "args": []
    },
    "unified_pipeline": {
        "title": "Unified Parallel Pipeline",
        "script": "unified_async_pipeline.py",
        "cwd": os.path.join("scripts", "Pipeline"),
        "args": []
    }
}

async def trigger_manual_pipeline(script_key: str = None):
    if pipeline_status["is_active"]:
        return {"success": False, "message": "Pipeline is already active and running."}
    
    # Run script in background without blocking the loop
    asyncio.create_task(_run_pipeline_script(script_key))
    message = "Pipeline started manually" if not script_key else f"Script '{script_key}' started manually"
    return {"success": True, "message": message}


async def _run_pipeline_script(script_key: str = None):
    if pipeline_status["is_active"]:
        return
    
    pipeline_status["is_active"] = True
    pipeline_status["last_run"] = datetime.now().isoformat()
    save_pipeline_state()
    
    # 🔍 Configure Script Target Environment
    target_script = os.path.join("scripts", "Pipeline", "unified_async_pipeline.py")
    target_cwd = None
    target_args = []
    target_title = "Master Pipeline"

    if script_key and script_key in SUPPORTED_SCRIPTS:
         config = SUPPORTED_SCRIPTS[script_key]
         target_script = os.path.join(config["cwd"], config["script"])
         target_cwd = config["cwd"]
         target_args = config.get("args", [])
         target_title = config.get("title", script_key)

    print(f"--- Executing {target_title} ---")
    
    try:
        if not os.path.exists(target_script):
             pipeline_status["error"] = f"{target_script} not found."
             print(f"Error: {target_script} not found at path.")
             return

        # Run synchronous subprocess inside safe thread wrapper to bypass Windows loop limitations
        def run_sync_subprocess():
             import subprocess as sync_subprocess
             
             # Force UTF-8 on Windows for outputs to prevent 'charmap' emoji crashes
             env = os.environ.copy()
             env["PYTHONIOENCODING"] = "utf-8"
             env["PYTHONUTF8"] = "1"
             
             script_runner = os.path.basename(target_script) if target_cwd else target_script
             proc = sync_subprocess.Popen(
                 ["python", script_runner] + target_args,
                 stdout=sync_subprocess.PIPE,
                 stderr=sync_subprocess.STDOUT,
                 text=True,
                 encoding='utf-8',
                 errors='ignore',
                 env=env,
                 cwd=target_cwd
             )
             
             import app.services.pipeline_service as dev_service
             
             dev_service.active_subprocess = proc
                          # Logs operations inside Redis list-items
             redis_client.delete("pipeline:logs")
             redis_client.rpush("pipeline:logs", f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] --- Execution Started: {target_title} ---")
             
             while True:
                 line = proc.stdout.readline()
                 if not line: break
                 line_str = line.strip()
                 redis_client.rpush("pipeline:logs", line_str)
                 redis_client.ltrim("pipeline:logs", -500, -1)
             proc.wait()
             redis_client.rpush("pipeline:logs", f"--- Finished with code {proc.returncode} ---")
             return proc.returncode


        loop = asyncio.get_running_loop()
        returncode = await loop.run_in_executor(None, run_sync_subprocess)
        print(f"--- {target_title} Finished with code {returncode} ---")
    except Exception as e:
        pipeline_status["error"] = str(e)
        print(f"Execution crashed: {e}")
    finally:
        pipeline_status["is_active"] = False


async def _pipeline_worker_loop():
    print("Autonomous Pipeline Task Activated.")
    
    while pipeline_status["is_running"]:
        try:
             if not pipeline_status["is_active"]:
                  print("Autonomous Pipeline: Triggering Scheduled Run...")
                  await _run_pipeline_script()
             else:
                  print("Autonomous Pipeline: Script already running. Skipping triggers overlapping.")

        except Exception as e:
             print(f"Pipeline Loop exception iteration failed: {e}")

        pipeline_status["next_run"] = datetime.now().timestamp() + pipeline_status["interval_seconds"]
        await asyncio.sleep(float(pipeline_status["interval_seconds"]))

    print("Autonomous Pipeline Task Deactivated.")

# Boot logic setup init
def init_pipeline_state():
    state = load_pipeline_state()
    pipeline_status["is_running"] = state.get("is_running", False)
    pipeline_status["interval_seconds"] = state.get("interval_seconds", 3600)
    pipeline_status["last_run"] = state.get("last_run")
    if pipeline_status["is_running"]:
        asyncio.create_task(_pipeline_worker_loop())

