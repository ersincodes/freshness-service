"""Freshness monitoring API routes."""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException

from ...config import get_settings
from ...domain import ErrorCode
from ...freshness import (
    FreshnessReportResponse,
    FreshnessStatus,
    SingleSourceFreshnessResponse,
    check_all_sources_freshness,
    check_source_freshness,
    get_enabled_sources,
    get_source_by_name,
    load_sources_config,
)

router = APIRouter(tags=["freshness"])


@router.get("/api/freshness", response_model=FreshnessReportResponse)
async def get_freshness_report() -> FreshnessReportResponse:
    return await asyncio.to_thread(check_all_sources_freshness, default_db_path=get_settings().db_path)


@router.get("/api/freshness/{source_id}", response_model=SingleSourceFreshnessResponse)
async def get_source_freshness(source_id: str) -> SingleSourceFreshnessResponse:
    src = get_source_by_name(source_id)
    if not src:
        raise HTTPException(404, {"code": ErrorCode.SOURCE_NOT_FOUND, "message": f"Source '{source_id}' not found"})
    detail = await asyncio.to_thread(check_source_freshness, src, default_db_path=get_settings().db_path)
    return SingleSourceFreshnessResponse(detail=detail, is_fresh=detail.status == FreshnessStatus.FRESH)


@router.get("/api/freshness/sources/list")
async def list_freshness_sources() -> dict:
    sources = get_enabled_sources()
    return {
        "total": len(sources),
        "sources": [
            {
                "name": s.name,
                "type": s.type.value,
                "ttl_minutes": s.ttl_minutes,
                "description": s.description,
                "enabled": s.enabled,
            }
            for s in sources
        ],
    }


@router.post("/api/freshness/reload")
async def reload_freshness_config() -> dict:
    cfg = load_sources_config(force_reload=True)
    return {"status": "ok", "message": "Configuration reloaded", "sources_count": len(cfg.sources)}
