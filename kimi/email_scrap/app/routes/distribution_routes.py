"""
FastAPI Endpoints for Lead Distribution Engine
Modified to use direct pymysql cursor queries.
"""
from fastapi import APIRouter, Depends, HTTPException, status, Request
from pydantic import BaseModel
from typing import List

from app.auth import get_current_user
from app.database import get_db_connection
from app.services.distribution_service import (
    distribute_leads_service, 
    get_member_leads, 
    start_autonomous_distribution, 
    stop_autonomous_distribution, 
    distribution_status
)

router = APIRouter()

# ============================================
# Pydantic Schemas For Serialization Validators
# ============================================

class AllocationItem(BaseModel):
    company_id: int
    lead_count: int

class DistributionRequest(BaseModel):
    company_allocations: List[AllocationItem]


@router.post("/admin/distribute-leads")
async def admin_distribute_leads(
    request: DistributionRequest, 
    current_user_dict: dict = Depends(get_current_user)
):
    """
    Admin distributes leads across multiple companies split offset accurately loops sequentially.
    """
    if current_user_dict.get("role") != 'admin':
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    allocs = [{"company_id": item.company_id, "lead_count": item.lead_count} for item in request.company_allocations]
    return await distribute_leads_service(allocs)


@router.get("/member/leads")
async def member_leads(
    current_user_dict: dict = Depends(get_current_user)
):
    """
    Fetch all assigned email leads for the current acting member node safely.
    """
    return await get_member_leads(current_user_dict["id"])


@router.get("/admin/distribution-summary")
async def admin_distribution_summary(
    current_user_dict: dict = Depends(get_current_user)
):
    """
    Provides table summary statistics regarding companies assignments counts splits workloads accurately properly.
    """
    if current_user_dict.get("role") != 'admin':
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id, name FROM companies")
        companies = cursor.fetchall()

        summary = []
        for comp in companies:
            # Count Members
            cursor.execute("SELECT COUNT(*) as cnt FROM users WHERE company_id = %s", (comp["id"],))
            members_cnt = cursor.fetchone()["cnt"] or 0

            # Count leads Total assigned
            cursor.execute("SELECT COUNT(*) as cnt FROM assigned_leads WHERE assigned_user_id IN (SELECT id FROM users WHERE company_id = %s)", (comp["id"],))
            leads_cnt = cursor.fetchone()["cnt"] or 0

            summary.append({
                "name": comp["name"],
                "members": members_cnt,
                "leads_assigned": leads_cnt
            })

        return summary
    finally:
         cursor.close()
         conn.close()


@router.post("/admin/distribute/start")
async def start_autonomous(
    count: int = 400,
    interval: int = 604800,
    current_user_dict: dict = Depends(get_current_user)
):
    """Start autonomous background leads distribution continuous cycle"""
    if current_user_dict.get("role") != 'admin':
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    return await start_autonomous_distribution(count, interval)



@router.post("/admin/distribute/stop")
async def stop_autonomous(
    current_user_dict: dict = Depends(get_current_user)
):
    """Stop autonomous background distribution cycle"""
    if current_user_dict.get("role") != 'admin':
        raise HTTPException(status_code=403, detail="Requires Admin privileges")

    return await stop_autonomous_distribution()


@router.get("/admin/distribute/status")
async def get_autonomous_status(
    current_user_dict: dict = Depends(get_current_user)
):
    """Get status of autonomous distribution continuous process"""
    if current_user_dict.get("role") != 'admin':
         raise HTTPException(status_code=403, detail="Requires Admin privileges")

    return {
        "is_running": distribution_status["is_running"],
        "lead_count_per_company": distribution_status["lead_count_per_company"],
        "error": distribution_status.get("error")
    }
