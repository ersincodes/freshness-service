"""Root and legacy compatibility routes."""
from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Query

from ..deps import chat_service
from ..schemas import FreshnessReport, LegacyFreshnessResponse
from ...config import get_settings

router = APIRouter(tags=["legacy"])


@router.get("/")
async def root() -> dict:
    s = get_settings()
    return {
        "service": "freshness-service",
        "status": "ok",
        "timestamp": dt.datetime.utcnow().isoformat(),
        "offline_retrieval_mode": s.offline_retrieval_mode,
        "model_name": s.model_name,
    }


@router.get("/freshness", response_model=LegacyFreshnessResponse)
async def legacy_freshness(query: str = Query(..., min_length=1)) -> LegacyFreshnessResponse:
    result = await chat_service().get_answer(query)
    return LegacyFreshnessResponse(
        query=query,
        mode=result.mode,
        reports=[
            FreshnessReport(
                source_name=c.url,
                is_fresh=c.is_fresh,
                last_updated=c.timestamp_iso,
                latency_seconds=c.latency_seconds,
            )
            for c in result.contexts
        ],
        answer=result.answer,
    )
