from fastapi import APIRouter, Depends, HTTPException
from app.auth import get_current_user
from app.services.pipeline_service import (
    start_autonomous_pipeline,
    stop_autonomous_pipeline,
    trigger_manual_pipeline,
    pipeline_status,
    reset_pipeline_checkpoint
)

router = APIRouter()

@router.post("/admin/pipeline/start")
async def admin_start_pipeline(
    interval: int = 3600,
    current_user_dict: dict = Depends(get_current_user)
):
    """Start autonomous background pipeline continuous cycle"""
    if current_user_dict.get("role") != 'admin':
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    return await start_autonomous_pipeline(interval)


@router.post("/admin/pipeline/stop")
async def admin_stop_pipeline(
    current_user_dict: dict = Depends(get_current_user)
):
    """Stop autonomous background pipeline schedule loop"""
    if current_user_dict.get("role") != 'admin':
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    return await stop_autonomous_pipeline()


@router.post("/admin/pipeline/trigger")
async def admin_trigger_pipeline(
    script_key: str = None,
    current_user_dict: dict = Depends(get_current_user)
):
    """Trigger pipeline execution manually now once or run specific script"""
    if current_user_dict.get("role") != 'admin':
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    return await trigger_manual_pipeline(script_key)


@router.post("/admin/pipeline/kill")
async def admin_kill_pipeline(
    current_user_dict: dict = Depends(get_current_user)
):
    """Stop/Kill active running pipeline immediately"""
    if current_user_dict.get("role") != 'admin':
         raise HTTPException(status_code=403, detail="Requires Admin privileges")

    from app.services.pipeline_service import stop_active_pipeline
    return stop_active_pipeline()

@router.post("/admin/pipeline/reset-checkpoint")
async def admin_reset_checkpoint(
    current_user_dict: dict = Depends(get_current_user)
):
    """Reset pipeline checkpoint so next run starts from page 1"""
    if current_user_dict.get("role") != 'admin':
         raise HTTPException(status_code=403, detail="Requires Admin privileges")

    return reset_pipeline_checkpoint()

@router.get("/admin/pipeline/status")

async def admin_pipeline_status(
    current_user_dict: dict = Depends(get_current_user)
):
    """Get status of autonomous background pipelines process"""
    if current_user_dict.get("role") != 'admin':
         raise HTTPException(status_code=403, detail="Requires Admin privileges")

    return {
        "is_running": pipeline_status["is_running"],
        "is_active": pipeline_status["is_active"],
        "interval_seconds": pipeline_status["interval_seconds"],
        "logs": pipeline_status.get("logs", []),
        "error": pipeline_status.get("error")
    }
