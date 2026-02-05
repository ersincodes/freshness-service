from __future__ import annotations

import datetime as dt
from typing import Literal

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

from . import archive
from .config import get_settings, update_settings
from .main import SourceContext, ask_llm_async

app = FastAPI(
    title="Freshness Service",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


class FreshnessReport(BaseModel):
    source_name: str
    is_fresh: bool
    last_updated: str
    latency_seconds: float


class FreshnessResponse(BaseModel):
    query: str
    mode: str
    reports: list[FreshnessReport]
    answer: str


class ConfigUpdate(BaseModel):
    max_search_results: int | None = Field(None, ge=1)
    request_timeout_s: int | None = Field(None, ge=1)
    max_chars_per_source: int | None = Field(None, ge=100)
    offline_retrieval_mode: Literal["keyword", "semantic"] | None = None
    semantic_top_k: int | None = Field(None, ge=1)
    lm_studio_base_url: str | None = None
    model_name: str | None = None
    brave_api_key: str | None = None


def _payload_to_dict(payload: BaseModel) -> dict:
    try:
        return payload.model_dump(exclude_unset=True)
    except AttributeError:
        return payload.dict(exclude_unset=True)


def _to_report(context: SourceContext) -> FreshnessReport:
    return FreshnessReport(
        source_name=context.url,
        is_fresh=context.is_fresh,
        last_updated=context.timestamp_iso,
        latency_seconds=context.latency_seconds,
    )


@app.on_event("startup")
async def startup() -> None:
    settings = get_settings()
    archive.init_db(settings.db_path)


@app.get("/")
async def root() -> dict:
    settings = get_settings()
    return {
        "service": "freshness-service",
        "status": "ok",
        "timestamp": dt.datetime.utcnow().isoformat(),
        "offline_retrieval_mode": settings.offline_retrieval_mode,
        "model_name": settings.model_name,
    }


@app.get("/freshness", response_model=FreshnessResponse)
async def freshness(query: str = Query(..., min_length=1)) -> FreshnessResponse:
    answer, mode, contexts = await ask_llm_async(query)
    reports = [_to_report(context) for context in contexts]
    return FreshnessResponse(query=query, mode=mode, reports=reports, answer=answer)


@app.post("/config")
async def update_config(payload: ConfigUpdate) -> dict:
    updates = _payload_to_dict(payload)
    updated = update_settings(updates)
    return {
        "status": "ok",
        "settings": {
            "lm_studio_base_url": updated.lm_studio_base_url,
            "model_name": updated.model_name,
            "max_search_results": updated.max_search_results,
            "offline_retrieval_mode": updated.offline_retrieval_mode,
            "semantic_top_k": updated.semantic_top_k,
            "request_timeout_s": updated.request_timeout_s,
            "max_chars_per_source": updated.max_chars_per_source,
            "brave_api_key_set": bool(updated.brave_api_key),
        },
    }
