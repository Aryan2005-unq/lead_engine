"""
Shared dependencies for FastAPI routes
"""
from fastapi import Depends, HTTPException, status, Request
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.auth import get_current_user
from typing import Optional, Dict, Any


async def get_db() -> AsyncSession:
    """
    Dependency to get database session
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_current_user_optional(
    request: Request
) -> Optional[Dict[str, Any]]:
    """
    Optional dependency to get current user (doesn't raise error if not authenticated)
    """
    try:
        return await get_current_user(request)
    except HTTPException:
        return None


async def get_current_user_required(
    request: Request
) -> Dict[str, Any]:
    """
    Required dependency to get current user (raises error if not authenticated)
    """
    return await get_current_user(request)
