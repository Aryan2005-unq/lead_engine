"""
Async Database Layer (asyncpg)
Provides connection pool, bulk upsert, and batch-fetch helpers.
"""
import asyncio
import json
from typing import Any, Dict, List, Optional

import asyncpg

from system.core.config import config


_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Return (and lazily create) the asyncpg connection pool."""
    global _pool
    if _pool is None or _pool._closed:
        _pool = await asyncpg.create_pool(
            dsn=config.dsn,
            min_size=config.DB_POOL_MIN,
            max_size=config.DB_POOL_MAX,
        )
    return _pool


async def close_pool():
    global _pool
    if _pool and not _pool._closed:
        await _pool.close()
        _pool = None


# ── Schema bootstrap ──
async def ensure_indexes():
    """Create performance indexes if they don't exist."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_leads_verify_status "
            "ON leads(verify_status)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_staging_created "
            "ON staging_fcc_listings(created_at)"
        )


# ── Generic helpers ──
async def fetch_rows(query: str, *args, limit: int = 200) -> List[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def execute(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)


# ── Bulk upsert for staging_fcc_listings ──
async def bulk_upsert_staging(rows: List[Dict[str, Any]]):
    """Insert/update a batch of staging rows in one round-trip."""
    if not rows:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Use a prepared statement inside a transaction
        async with conn.transaction():
            stmt = await conn.prepare("""
                INSERT INTO staging_fcc_listings
                    (frn, business_name, sys_id, attachment_link, other_data)
                VALUES ($1, $2, $3, $4, $5::jsonb)
                ON CONFLICT (frn) DO UPDATE SET
                    business_name   = EXCLUDED.business_name,
                    sys_id          = EXCLUDED.sys_id,
                    attachment_link = EXCLUDED.attachment_link,
                    other_data      = EXCLUDED.other_data
            """)
            await stmt.executemany([
                (
                    r["frn"],
                    r.get("business_name", ""),
                    r.get("sys_id", ""),
                    r.get("attachment_link", ""),
                    json.dumps(r.get("other_data", {})),
                )
                for r in rows
            ])


# ── Bulk upsert for leads ──
async def bulk_upsert_leads(rows: List[Dict[str, Any]]):
    """Insert/update a batch of lead rows in one round-trip."""
    if not rows:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            stmt = await conn.prepare("""
                INSERT INTO leads
                    (company_name, email, phone, verify_status)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE SET
                    company_name  = EXCLUDED.company_name,
                    email         = COALESCE(NULLIF(EXCLUDED.email, ''), leads.email),
                    phone         = COALESCE(NULLIF(EXCLUDED.phone, ''), leads.phone),
                    verify_status = EXCLUDED.verify_status
            """)
            await stmt.executemany([
                (
                    r.get("company_name", ""),
                    r.get("email", ""),
                    r.get("phone", ""),
                    r.get("verify_status", ""),
                )
                for r in rows
            ])


async def bulk_update_lead_emails(updates: List[Dict[str, Any]]):
    """Update email column for a batch of lead IDs."""
    if not updates:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            stmt = await conn.prepare("""
                UPDATE leads SET email = $1
                WHERE id = $2 AND (email IS NULL OR email = '')
            """)
            await stmt.executemany([
                (u["email"], u["lead_id"])
                for u in updates
            ])


# ── Fetch batches ──
async def fetch_unprocessed_staging(checkpoint_id: int, limit: int) -> List[asyncpg.Record]:
    """Fetch staging rows after a given checkpoint ID."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT id, frn, business_name, sys_id, attachment_link, other_data "
            "FROM staging_fcc_listings WHERE id > $1 ORDER BY id LIMIT $2",
            checkpoint_id,
            limit,
        )


async def fetch_unverified_leads(limit: int) -> List[asyncpg.Record]:
    """Fetch leads that haven't been verified yet."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT id, frn, company_name, email, phone "
            "FROM staging_fcc_listings s "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM leads l WHERE l.company_name = s.business_name"
            ") "
            "ORDER BY s.id LIMIT $1",
            limit,
        )


async def fetch_leads_needing_email(limit: int) -> List[asyncpg.Record]:
    """Fetch verified leads that are missing an email."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT id, company_name, verify_status, phone "
            "FROM leads "
            "WHERE verify_status LIKE 'Active%%' "
            "  AND (email IS NULL OR email = '' OR email NOT LIKE '%%@%%') "
            "ORDER BY id LIMIT $1",
            limit,
        )
