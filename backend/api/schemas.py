"""HTTP request and response Pydantic models for the API layer."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class SourceLocation(BaseModel):
    page: int | None = None
    sheet: str | None = None
    row_start: int | None = None
    row_end: int | None = None


class Source(BaseModel):
    url: str
    snippet: str
    retrieval_type: Literal["online", "offline_keyword", "offline_semantic", "document_keyword", "document_semantic"]
    timestamp: str | None = None
    url_hash: str | None = None
    source_type: Literal["web", "document"] = "web"
    filename: str | None = None
    location: SourceLocation | None = None
    source_kind: Literal["web", "document", "analytics", "archive"] | None = None
    document_id: str | None = None
    display_name: str | None = None
    sheet_name: str | None = None


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1)
    conversation_id: str | None = None
    prefer_mode: Literal["ONLINE", "OFFLINE"] | None = None
    include_web: bool = True
    include_documents: bool = False
    document_ids: list[str] | None = None


class TimingInfo(BaseModel):
    search_ms: int = 0
    scrape_ms: int = 0
    llm_ms: int = 0
    total_ms: int = 0


class ForecastResponseModel(BaseModel):
    document: str | None = None
    document_id: str
    sheet: str
    measure: str
    time_column: str
    horizon: int
    point: list[float]
    lower: list[float]
    upper: list[float]
    model: str


class ChatResponse(BaseModel):
    conversation_id: str
    answer: str
    mode: Literal["ONLINE", "OFFLINE_ARCHIVE", "LOCAL_WEIGHTS"]
    sources: list[Source]
    timing: TimingInfo
    forecast: ForecastResponseModel | None = None
    chart: dict[str, Any] | None = None


class ArchiveEntryModel(BaseModel):
    url_hash: str
    url: str
    timestamp: str
    excerpt: str


class ArchiveSearchResponse(BaseModel):
    entries: list[ArchiveEntryModel]
    total: int
    cursor: str | None = None


class ArchivePageResponse(BaseModel):
    url_hash: str
    url: str
    content: str
    timestamp: str


class SettingsResponse(BaseModel):
    brave_api_key_set: bool
    lm_studio_base_url: str
    model_name: str
    offline_retrieval_mode: Literal["keyword", "semantic"]
    max_search_results: int
    request_timeout_s: int
    llm_request_timeout_s: int
    max_chars_per_source: int
    semantic_top_k: int
    web_top_k: int
    doc_semantic_top_k: int
    doc_keyword_top_k: int
    web_max_chars: int
    doc_max_chars: int
    total_context_budget: int
    web_budget_fraction: float


class HealthStatus(BaseModel):
    status: Literal["ok", "error", "unavailable"]
    message: str | None = None
    latency_ms: int | None = None


class HealthResponse(BaseModel):
    backend: HealthStatus
    lm_studio: HealthStatus
    brave_search: HealthStatus


class ConfigUpdate(BaseModel):
    max_search_results: int | None = Field(None, ge=1)
    request_timeout_s: int | None = Field(None, ge=1)
    llm_request_timeout_s: int | None = Field(None, ge=15)
    max_chars_per_source: int | None = Field(None, ge=100)
    offline_retrieval_mode: Literal["keyword", "semantic"] | None = None
    semantic_top_k: int | None = Field(None, ge=1)
    lm_studio_base_url: str | None = None
    model_name: str | None = None
    brave_api_key: str | None = None
    web_top_k: int | None = Field(None, ge=1)
    doc_semantic_top_k: int | None = Field(None, ge=1)
    doc_keyword_top_k: int | None = Field(None, ge=1)
    web_max_chars: int | None = Field(None, ge=100)
    doc_max_chars: int | None = Field(None, ge=0)
    total_context_budget: int | None = Field(None, ge=1000)
    web_budget_fraction: float | None = Field(None, ge=0.0, le=1.0)


class DocumentResponse(BaseModel):
    document_id: str
    filename: str
    doc_type: Literal["pdf", "xlsx", "xls"]
    size_bytes: int
    status: Literal["pending", "processing", "ready", "error"]
    uploaded_at: str
    error_message: str | None = None
    chunk_count: int = 0


class DocumentListResponse(BaseModel):
    documents: list[DocumentResponse]
    total: int


class DocumentUploadResponse(BaseModel):
    document_id: str
    filename: str
    status: Literal["pending", "processing", "ready", "error"]
    message: str


class FreshnessReport(BaseModel):
    source_name: str
    is_fresh: bool
    last_updated: str
    latency_seconds: float


class LegacyFreshnessResponse(BaseModel):
    query: str
    mode: str
    reports: list[FreshnessReport]
    answer: str
