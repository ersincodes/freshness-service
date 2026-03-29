"""Health check endpoint."""
from __future__ import annotations

from fastapi import APIRouter

from ..deps import health_service
from ..schemas import HealthResponse, HealthStatus

router = APIRouter(tags=["health"])


@router.get("/api/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    r = await health_service().check_all()
    return HealthResponse(
        backend=HealthStatus(status=r.backend.status, message=r.backend.message, latency_ms=r.backend.latency_ms),
        lm_studio=HealthStatus(status=r.lm_studio.status, message=r.lm_studio.message, latency_ms=r.lm_studio.latency_ms),
        brave_search=HealthStatus(
            status=r.brave_search.status,
            message=r.brave_search.message,
            latency_ms=r.brave_search.latency_ms,
        ),
    )
