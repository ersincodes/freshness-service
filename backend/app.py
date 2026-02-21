"""
FastAPI application for the Freshness Service.

Routes delegate business logic to services layer.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import AsyncGenerator, Literal

from fastapi import FastAPI, Query, HTTPException, UploadFile, File, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from .config import get_settings, update_settings
from .domain import ErrorCode, SourceContext
from .repositories import ArchiveRepository, DocumentRepository, AnalyticsRepository
from .repositories.document_repository import DocumentInfo
from .documents import DocumentType, DocumentStatus, generate_document_id, get_document_type_from_filename, validate_mime_type, sanitize_filename, process_document, hash_chunk_id, ingest_excel_to_sqlite
from .integrations import LLMClient, BraveClient
from .services import ChatService, HealthService
from .freshness import FreshnessReportResponse, SingleSourceFreshnessResponse, FreshnessStatus, check_all_sources_freshness, check_source_freshness, get_source_by_name, get_enabled_sources, load_sources_config
from .vector_store import upsert_document_chunk, delete_document_chunks_from_vector_store
from . import archive

logger = logging.getLogger(__name__)

app = FastAPI(title="Freshness Service", version="1.0.0", docs_url="/docs", redoc_url="/redoc", openapi_url="/openapi.json")
app.add_middleware(CORSMiddleware, allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


# ============================================================================
# Pydantic Models
# ============================================================================

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


class ChatResponse(BaseModel):
    conversation_id: str
    answer: str
    mode: Literal["ONLINE", "OFFLINE_ARCHIVE", "LOCAL_WEIGHTS"]
    sources: list[Source]
    timing: TimingInfo


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
    max_chars_per_source: int
    semantic_top_k: int
    # Decoupled RAG settings
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
    max_chars_per_source: int | None = Field(None, ge=100)
    offline_retrieval_mode: Literal["keyword", "semantic"] | None = None
    semantic_top_k: int | None = Field(None, ge=1)
    lm_studio_base_url: str | None = None
    model_name: str | None = None
    brave_api_key: str | None = None
    # Decoupled RAG settings
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


# ============================================================================
# Service Factories
# ============================================================================

def _archive_repo() -> ArchiveRepository:
    return ArchiveRepository(get_settings().db_path)


def _doc_repo() -> DocumentRepository:
    s = get_settings()
    return DocumentRepository(s.db_path, s.upload_dir)


def _chat_service() -> ChatService:
    s = get_settings()
    return ChatService(
        s,
        LLMClient(s.lm_studio_base_url, s.model_name, s.request_timeout_s),
        BraveClient(s.brave_api_key, s.request_timeout_s, s.max_search_results),
        _archive_repo(),
        _doc_repo(),
        analytics_repo=_analytics_repo(),
    )


def _health_service() -> HealthService:
    s = get_settings()
    return HealthService(LLMClient(s.lm_studio_base_url, s.model_name, s.request_timeout_s),
                         BraveClient(s.brave_api_key, s.request_timeout_s, s.max_search_results))


def _source_to_model(d: dict) -> Source:
    loc = SourceLocation(**d["location"]) if d.get("location") else None
    return Source(url=d["url"], snippet=d["snippet"], retrieval_type=d["retrieval_type"], timestamp=d.get("timestamp"),
                  url_hash=d.get("url_hash"), source_type=d.get("source_type", "web"), filename=d.get("filename"), location=loc)


def _doc_to_response(d: DocumentInfo) -> DocumentResponse:
    return DocumentResponse(document_id=d.document_id, filename=d.filename, doc_type=d.doc_type.value, size_bytes=d.size_bytes,
                            status=d.status.value, uploaded_at=d.uploaded_at, error_message=d.error_message, chunk_count=d.chunk_count)


# ============================================================================
# Migrations + Startup
# ============================================================================

def _run_analytics_migrations(db_path: str) -> None:
    """Execute SQL migration files for tabular analytics schema."""
    migration_file = Path(__file__).parent / "migrations" / "001_tabular_analytics.sql"
    if not migration_file.exists():
        logger.warning("Analytics migration file not found: %s", migration_file)
        return
    sql = migration_file.read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(sql)
    logger.info("Analytics migrations applied from %s", migration_file.name)


def _get_analytics_connection(db_path: str) -> sqlite3.Connection:
    """Return a long-lived SQLite connection for analytics operations."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


_analytics_conn: sqlite3.Connection | None = None


def _analytics_repo() -> AnalyticsRepository | None:
    if _analytics_conn is None:
        return None
    return AnalyticsRepository(_analytics_conn)


def _retroactive_analytics_ingestion(db_path: str, upload_dir: str) -> None:
    """Ingest any existing Excel documents that are missing from analytics tables.

    File naming convention: uploads/{document_id}_{filename}
    """
    conn = _get_analytics_connection(db_path)
    repo = AnalyticsRepository(conn)
    already_ingested = set(repo.list_all_document_ids())

    doc_repo = DocumentRepository(db_path, upload_dir)
    all_docs = doc_repo.list_documents()
    excel_types = {DocumentType.XLSX, DocumentType.XLS}
    ingested_count = 0

    for doc in all_docs:
        if doc.doc_type not in excel_types:
            continue
        if doc.status != DocumentStatus.READY:
            continue
        if doc.document_id in already_ingested:
            continue

        file_path = os.path.join(upload_dir, f"{doc.document_id}_{doc.filename}")
        if not os.path.isfile(file_path):
            logger.warning("Retroactive ingestion: file not found at %s", file_path)
            continue

        try:
            ingest_excel_to_sqlite(
                excel_path=file_path,
                document_id=doc.document_id,
                sqlite_connection=conn,
            )
            ingested_count += 1
            logger.warning("Retroactively ingested analytics for document %s (%s)", doc.document_id, doc.filename)
        except Exception as exc:
            logger.warning("Retroactive ingestion failed for %s: %s", doc.document_id, exc)

    if ingested_count:
        logger.warning("Retroactive analytics ingestion complete: %d document(s)", ingested_count)

    conn.close()


@app.on_event("startup")
async def startup() -> None:
    global _analytics_conn
    s = get_settings()
    archive.init_db(s.db_path)
    _run_analytics_migrations(s.db_path)
    if s.enable_tabular_analytics:
        _retroactive_analytics_ingestion(s.db_path, s.upload_dir)
        _analytics_conn = _get_analytics_connection(s.db_path)


# ============================================================================
# Chat Routes
# ============================================================================

@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest) -> ChatResponse:
    start = time.perf_counter()
    conv_id = request.conversation_id or str(uuid.uuid4())
    try:
        result = await _chat_service().get_answer(request.query, request.prefer_mode, request.include_web, request.include_documents, request.document_ids)
    except Exception as e:
        raise HTTPException(500, {"code": ErrorCode.LLM_ERROR, "message": str(e)})
    sources = [_source_to_model(s) for s in _chat_service().convert_contexts_to_sources(result.contexts, result.mode)]
    return ChatResponse(conversation_id=conv_id, answer=result.answer, mode=result.mode, sources=sources, timing=TimingInfo(total_ms=int((time.perf_counter() - start) * 1000)))


@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    conv_id = request.conversation_id or str(uuid.uuid4())
    async def gen() -> AsyncGenerator[str, None]:
        async for ev in _chat_service().stream_answer(request.query, conv_id, request.prefer_mode, request.include_web, request.include_documents, request.document_ids):
            yield f"event: {ev.event_type}\ndata: {json.dumps(ev.data)}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})


# ============================================================================
# Archive Routes
# ============================================================================

@app.get("/api/archive/search", response_model=ArchiveSearchResponse)
async def archive_search(q: str = Query(""), limit: int = Query(20, ge=1, le=100), cursor: str | None = Query(None)) -> ArchiveSearchResponse:
    r = await _archive_repo().search_pages_async(q, limit, cursor)
    return ArchiveSearchResponse(entries=[ArchiveEntryModel(url_hash=e.url_hash, url=e.url, excerpt=e.excerpt, timestamp=e.timestamp) for e in r.entries], total=r.total, cursor=r.cursor)


@app.get("/api/archive/page/{url_hash}", response_model=ArchivePageResponse)
async def archive_page(url_hash: str) -> ArchivePageResponse:
    p = await _archive_repo().get_page_async(url_hash)
    if not p:
        raise HTTPException(404, {"code": ErrorCode.NOT_FOUND, "message": f"Archive page not found: {url_hash}"})
    return ArchivePageResponse(url_hash=p.url_hash, url=p.url, content=p.content, timestamp=p.timestamp)


# ============================================================================
# Document Routes
# ============================================================================

async def _process_doc_bg(doc_id: str, file_path: str, doc_type_val: str, filename: str) -> None:
    s = get_settings()
    repo = _doc_repo()
    doc_type = DocumentType(doc_type_val)
    try:
        await repo.update_status_async(doc_id, DocumentStatus.PROCESSING)
        chunks = await asyncio.to_thread(process_document, file_path, doc_type)
        if not chunks:
            await repo.update_status_async(doc_id, DocumentStatus.ERROR, "No content could be extracted")
            return
        await repo.save_chunks_async(doc_id, [(c.chunk_index, c.content, c.metadata) for c in chunks])
        if s.offline_retrieval_mode == "semantic":
            for c in chunks:
                try:
                    await asyncio.to_thread(upsert_document_chunk, s.chroma_dir, s.embed_model_name, hash_chunk_id(doc_id, c.chunk_index), doc_id, filename, c.content, c.metadata, dt.datetime.utcnow().isoformat())
                except Exception:
                    pass

        if s.enable_tabular_analytics and doc_type in {DocumentType.XLSX, DocumentType.XLS}:
            try:
                conn = _get_analytics_connection(s.db_path)
                await asyncio.to_thread(
                    ingest_excel_to_sqlite,
                    excel_path=file_path,
                    document_id=doc_id,
                    sqlite_connection=conn,
                )
            except Exception as exc:
                logger.warning("Tabular analytics ingestion failed for %s: %s", doc_id, exc)

        await repo.update_status_async(doc_id, DocumentStatus.READY)
    except Exception as e:
        await repo.update_status_async(doc_id, DocumentStatus.ERROR, str(e))


@app.post("/api/documents/upload", response_model=DocumentUploadResponse)
async def upload_document(background_tasks: BackgroundTasks, file: UploadFile = File(...)) -> DocumentUploadResponse:
    s = get_settings()
    if not file.filename:
        raise HTTPException(400, {"code": ErrorCode.INVALID_FILENAME, "message": "Filename is required"})
    doc_type = get_document_type_from_filename(file.filename)
    if not doc_type:
        raise HTTPException(400, {"code": ErrorCode.UNSUPPORTED_TYPE, "message": "Unsupported file type. Allowed: .pdf, .xlsx, .xls"})
    content = await file.read()
    if len(content) > s.max_upload_mb * 1024 * 1024:
        raise HTTPException(413, {"code": ErrorCode.FILE_TOO_LARGE, "message": f"File exceeds {s.max_upload_mb}MB"})
    doc_id, safe_name = generate_document_id(), sanitize_filename(file.filename)
    repo = _doc_repo()
    file_path = repo.save_file(doc_id, safe_name, content)
    await repo.save_document_async(doc_id, safe_name, DocumentType(doc_type.value), len(content), DocumentStatus.PENDING)
    background_tasks.add_task(_process_doc_bg, doc_id, file_path, doc_type.value, safe_name)
    return DocumentUploadResponse(document_id=doc_id, filename=safe_name, status="pending", message="Document uploaded. Processing started.")


@app.get("/api/documents", response_model=DocumentListResponse)
async def get_documents() -> DocumentListResponse:
    docs = await _doc_repo().list_documents_async()
    return DocumentListResponse(documents=[_doc_to_response(d) for d in docs], total=len(docs))


@app.get("/api/documents/{document_id}", response_model=DocumentResponse)
async def get_document_status(document_id: str) -> DocumentResponse:
    d = await _doc_repo().get_document_async(document_id)
    if not d:
        raise HTTPException(404, {"code": ErrorCode.NOT_FOUND, "message": f"Document not found: {document_id}"})
    return _doc_to_response(d)


@app.delete("/api/documents/{document_id}")
async def delete_document_endpoint(document_id: str) -> dict:
    s, repo = get_settings(), _doc_repo()
    d = await repo.get_document_async(document_id)
    if not d:
        raise HTTPException(404, {"code": ErrorCode.NOT_FOUND, "message": f"Document not found: {document_id}"})
    if s.offline_retrieval_mode == "semantic":
        try:
            await asyncio.to_thread(delete_document_chunks_from_vector_store, s.chroma_dir, s.embed_model_name, document_id)
        except Exception:
            pass
    await repo.delete_document_async(document_id)
    await repo.delete_document_file_async(document_id)
    return {"status": "ok", "message": f"Document {document_id} deleted"}


# ============================================================================
# Settings & Health Routes
# ============================================================================

@app.get("/api/settings", response_model=SettingsResponse)
async def get_api_settings() -> SettingsResponse:
    s = get_settings()
    return SettingsResponse(
        brave_api_key_set=bool(s.brave_api_key),
        lm_studio_base_url=s.lm_studio_base_url,
        model_name=s.model_name,
        offline_retrieval_mode=s.offline_retrieval_mode,
        max_search_results=s.max_search_results,
        request_timeout_s=s.request_timeout_s,
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


@app.post("/api/config")
async def update_config(payload: ConfigUpdate) -> dict:
    updates = {k: v for k, v in (payload.model_dump() if hasattr(payload, 'model_dump') else payload.dict()).items() if v is not None}
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


@app.get("/api/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    r = await _health_service().check_all()
    return HealthResponse(backend=HealthStatus(status=r.backend.status, message=r.backend.message, latency_ms=r.backend.latency_ms),
                          lm_studio=HealthStatus(status=r.lm_studio.status, message=r.lm_studio.message, latency_ms=r.lm_studio.latency_ms),
                          brave_search=HealthStatus(status=r.brave_search.status, message=r.brave_search.message, latency_ms=r.brave_search.latency_ms))


# ============================================================================
# Freshness Routes
# ============================================================================

@app.get("/api/freshness", response_model=FreshnessReportResponse)
async def get_freshness_report() -> FreshnessReportResponse:
    return await asyncio.to_thread(check_all_sources_freshness, default_db_path=get_settings().db_path)


@app.get("/api/freshness/{source_id}", response_model=SingleSourceFreshnessResponse)
async def get_source_freshness(source_id: str) -> SingleSourceFreshnessResponse:
    src = get_source_by_name(source_id)
    if not src:
        raise HTTPException(404, {"code": ErrorCode.SOURCE_NOT_FOUND, "message": f"Source '{source_id}' not found"})
    detail = await asyncio.to_thread(check_source_freshness, src, default_db_path=get_settings().db_path)
    return SingleSourceFreshnessResponse(detail=detail, is_fresh=detail.status == FreshnessStatus.FRESH)


@app.get("/api/freshness/sources/list")
async def list_freshness_sources() -> dict:
    sources = get_enabled_sources()
    return {"total": len(sources), "sources": [{"name": s.name, "type": s.type.value, "ttl_minutes": s.ttl_minutes, "description": s.description, "enabled": s.enabled} for s in sources]}


@app.post("/api/freshness/reload")
async def reload_freshness_config() -> dict:
    cfg = load_sources_config(force_reload=True)
    return {"status": "ok", "message": "Configuration reloaded", "sources_count": len(cfg.sources)}


# ============================================================================
# Legacy Routes
# ============================================================================

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


@app.get("/")
async def root() -> dict:
    s = get_settings()
    return {"service": "freshness-service", "status": "ok", "timestamp": dt.datetime.utcnow().isoformat(), "offline_retrieval_mode": s.offline_retrieval_mode, "model_name": s.model_name}


@app.get("/freshness", response_model=LegacyFreshnessResponse)
async def legacy_freshness(query: str = Query(..., min_length=1)) -> LegacyFreshnessResponse:
    result = await _chat_service().get_answer(query)
    return LegacyFreshnessResponse(query=query, mode=result.mode, reports=[FreshnessReport(source_name=c.url, is_fresh=c.is_fresh, last_updated=c.timestamp_iso, latency_seconds=c.latency_seconds) for c in result.contexts], answer=result.answer)
