from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

if load_dotenv:
    load_dotenv()


def _getenv_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _getenv_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _normalize_mode(raw: str | None) -> str:
    value = (raw or "keyword").strip().lower()
    return value if value in {"keyword", "semantic"} else "keyword"


@dataclass(frozen=True)
class Settings:
    brave_api_key: str | None
    lm_studio_base_url: str
    model_name: str
    db_path: str
    max_search_results: int
    offline_retrieval_mode: str
    chroma_dir: str
    embed_model_name: str
    semantic_top_k: int
    request_timeout_s: int
    max_chars_per_source: int
    # Document upload settings
    upload_dir: str
    max_upload_mb: int
    # Decoupled RAG settings
    web_top_k: int
    doc_semantic_top_k: int
    doc_keyword_top_k: int
    web_max_chars: int
    doc_max_chars: int
    total_context_budget: int
    web_budget_fraction: float
    # Tabular analytics settings
    enable_tabular_analytics: bool
    analytics_groupby_top_n_default: int


settings = Settings(
    brave_api_key=os.getenv("BRAVE_API_KEY"),
    lm_studio_base_url=os.getenv("LM_STUDIO_BASE_URL", "http://localhost:1111/v1"),
    model_name=os.getenv("MODEL_NAME", "rnj-1"),
    db_path=os.getenv("DB_PATH", "knowledge.db"),
    max_search_results=_getenv_int("MAX_SEARCH_RESULTS", 3),
    offline_retrieval_mode=_normalize_mode(os.getenv("OFFLINE_RETRIEVAL_MODE")),
    chroma_dir=os.getenv("CHROMA_DIR", "chroma_db"),
    embed_model_name=os.getenv(
        "EMBED_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2"
    ),
    semantic_top_k=_getenv_int("SEMANTIC_TOP_K", 3),
    request_timeout_s=_getenv_int("REQUEST_TIMEOUT_S", 10),
    max_chars_per_source=_getenv_int("MAX_CHARS_PER_SOURCE", 2000),
    upload_dir=os.getenv("UPLOAD_DIR", "uploads"),
    max_upload_mb=_getenv_int("MAX_UPLOAD_MB", 25),
    # Decoupled RAG settings
    web_top_k=_getenv_int("WEB_TOP_K", 3),
    doc_semantic_top_k=_getenv_int("DOC_SEMANTIC_TOP_K", 12),
    doc_keyword_top_k=_getenv_int("DOC_KEYWORD_TOP_K", 20),
    web_max_chars=_getenv_int("WEB_MAX_CHARS", 2000),
    doc_max_chars=_getenv_int("DOC_MAX_CHARS", 0),
    total_context_budget=_getenv_int("TOTAL_CONTEXT_BUDGET", 14000),
    web_budget_fraction=_getenv_float("WEB_BUDGET_FRACTION", 0.4),
    enable_tabular_analytics=os.getenv("ENABLE_TABULAR_ANALYTICS", "true").strip().lower() in {"true", "1", "yes"},
    analytics_groupby_top_n_default=_getenv_int("ANALYTICS_GROUPBY_TOP_N_DEFAULT", 50),
)

_RUNTIME_OVERRIDES: dict[str, Any] = {}


def get_settings() -> Settings:
    if not _RUNTIME_OVERRIDES:
        return settings
    base = settings
    return Settings(
        brave_api_key=_RUNTIME_OVERRIDES.get("brave_api_key", base.brave_api_key),
        lm_studio_base_url=_RUNTIME_OVERRIDES.get(
            "lm_studio_base_url", base.lm_studio_base_url
        ),
        model_name=_RUNTIME_OVERRIDES.get("model_name", base.model_name),
        db_path=_RUNTIME_OVERRIDES.get("db_path", base.db_path),
        max_search_results=_RUNTIME_OVERRIDES.get(
            "max_search_results", base.max_search_results
        ),
        offline_retrieval_mode=_RUNTIME_OVERRIDES.get(
            "offline_retrieval_mode", base.offline_retrieval_mode
        ),
        chroma_dir=_RUNTIME_OVERRIDES.get("chroma_dir", base.chroma_dir),
        embed_model_name=_RUNTIME_OVERRIDES.get(
            "embed_model_name", base.embed_model_name
        ),
        semantic_top_k=_RUNTIME_OVERRIDES.get("semantic_top_k", base.semantic_top_k),
        request_timeout_s=_RUNTIME_OVERRIDES.get(
            "request_timeout_s", base.request_timeout_s
        ),
        max_chars_per_source=_RUNTIME_OVERRIDES.get(
            "max_chars_per_source", base.max_chars_per_source
        ),
        upload_dir=_RUNTIME_OVERRIDES.get("upload_dir", base.upload_dir),
        max_upload_mb=_RUNTIME_OVERRIDES.get("max_upload_mb", base.max_upload_mb),
        web_top_k=_RUNTIME_OVERRIDES.get("web_top_k", base.web_top_k),
        doc_semantic_top_k=_RUNTIME_OVERRIDES.get(
            "doc_semantic_top_k", base.doc_semantic_top_k
        ),
        doc_keyword_top_k=_RUNTIME_OVERRIDES.get(
            "doc_keyword_top_k", base.doc_keyword_top_k
        ),
        web_max_chars=_RUNTIME_OVERRIDES.get("web_max_chars", base.web_max_chars),
        doc_max_chars=_RUNTIME_OVERRIDES.get("doc_max_chars", base.doc_max_chars),
        total_context_budget=_RUNTIME_OVERRIDES.get(
            "total_context_budget", base.total_context_budget
        ),
        web_budget_fraction=_RUNTIME_OVERRIDES.get(
            "web_budget_fraction", base.web_budget_fraction
        ),
        enable_tabular_analytics=_RUNTIME_OVERRIDES.get(
            "enable_tabular_analytics", base.enable_tabular_analytics
        ),
        analytics_groupby_top_n_default=_RUNTIME_OVERRIDES.get(
            "analytics_groupby_top_n_default", base.analytics_groupby_top_n_default
        ),
    )


def update_settings(overrides: dict[str, Any]) -> Settings:
    normalized: dict[str, Any] = {}
    int_keys = {
        "max_search_results",
        "semantic_top_k",
        "request_timeout_s",
        "max_chars_per_source",
        "web_top_k",
        "doc_semantic_top_k",
        "doc_keyword_top_k",
        "web_max_chars",
        "doc_max_chars",
        "total_context_budget",
        "analytics_groupby_top_n_default",
    }
    float_keys = {"web_budget_fraction"}
    bool_keys = {"enable_tabular_analytics"}
    for key, value in overrides.items():
        if value is None:
            continue
        if key == "offline_retrieval_mode":
            normalized[key] = _normalize_mode(str(value))
        elif key in int_keys:
            normalized[key] = int(value)
        elif key in float_keys:
            normalized[key] = float(value)
        elif key in bool_keys:
            normalized[key] = bool(value)
        else:
            normalized[key] = value
    _RUNTIME_OVERRIDES.update(normalized)
    return get_settings()
