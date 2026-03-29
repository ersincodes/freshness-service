"""Runtime settings and config update endpoints."""
from __future__ import annotations

from fastapi import APIRouter

from ..schemas import ConfigUpdate, SettingsResponse
from ...config import get_settings, update_settings

router = APIRouter(tags=["settings"])


@router.get("/api/settings", response_model=SettingsResponse)
async def get_api_settings() -> SettingsResponse:
    s = get_settings()
    return SettingsResponse(
        brave_api_key_set=bool(s.brave_api_key),
        lm_studio_base_url=s.lm_studio_base_url,
        model_name=s.model_name,
        offline_retrieval_mode=s.offline_retrieval_mode,
        max_search_results=s.max_search_results,
        request_timeout_s=s.request_timeout_s,
        llm_request_timeout_s=s.llm_request_timeout_s,
        max_chars_per_source=s.max_chars_per_source,
        semantic_top_k=s.semantic_top_k,
        web_top_k=s.web_top_k,
        doc_semantic_top_k=s.doc_semantic_top_k,
        doc_keyword_top_k=s.doc_keyword_top_k,
        web_max_chars=s.web_max_chars,
        doc_max_chars=s.doc_max_chars,
        total_context_budget=s.total_context_budget,
        web_budget_fraction=s.web_budget_fraction,
    )


@router.post("/api/config")
async def update_config(payload: ConfigUpdate) -> dict:
    updates = {k: v for k, v in payload.model_dump().items() if v is not None}
    u = update_settings(updates)
    return {
        "status": "ok",
        "settings": {
            "lm_studio_base_url": u.lm_studio_base_url,
            "model_name": u.model_name,
            "max_search_results": u.max_search_results,
            "offline_retrieval_mode": u.offline_retrieval_mode,
            "semantic_top_k": u.semantic_top_k,
            "request_timeout_s": u.request_timeout_s,
            "llm_request_timeout_s": u.llm_request_timeout_s,
            "max_chars_per_source": u.max_chars_per_source,
            "brave_api_key_set": bool(u.brave_api_key),
            "web_top_k": u.web_top_k,
            "doc_semantic_top_k": u.doc_semantic_top_k,
            "doc_keyword_top_k": u.doc_keyword_top_k,
            "web_max_chars": u.web_max_chars,
            "doc_max_chars": u.doc_max_chars,
            "total_context_budget": u.total_context_budget,
            "web_budget_fraction": u.web_budget_fraction,
        },
    }
