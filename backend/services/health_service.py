"""
Health check service.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal

from ..integrations import LLMClient, BraveClient

HealthStatusType = Literal["ok", "error", "unavailable"]


@dataclass(frozen=True)
class ServiceHealth:
    status: HealthStatusType
    message: str | None = None
    latency_ms: int | None = None


@dataclass(frozen=True)
class HealthReport:
    backend: ServiceHealth
    lm_studio: ServiceHealth
    brave_search: ServiceHealth


class HealthService:
    """Service for checking health of all system components."""
    
    def __init__(self, llm_client: LLMClient, brave_client: BraveClient) -> None:
        self._llm = llm_client
        self._brave = brave_client
    
    async def check_all(self) -> HealthReport:
        lm, brave = await asyncio.gather(self._check_lm(), self._check_brave())
        return HealthReport(ServiceHealth("ok", "Backend is running"), lm, brave)
    
    async def _check_lm(self) -> ServiceHealth:
        ok, msg, lat = await self._llm.check_health()
        if ok:
            return ServiceHealth("ok", msg, lat)
        return ServiceHealth("unavailable" if "Cannot connect" in msg else "error", msg)
    
    async def _check_brave(self) -> ServiceHealth:
        ok, msg, lat = await self._brave.check_health()
        if ok:
            return ServiceHealth("ok", msg, lat)
        return ServiceHealth("unavailable" if "not configured" in msg else "error", msg)
