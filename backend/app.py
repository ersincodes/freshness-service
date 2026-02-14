from __future__ import annotations

import asyncio
import datetime as dt
import time
import uuid
from typing import AsyncGenerator, Literal

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from . import archive
from .config import get_settings, update_settings
from .main import SourceContext, ask_llm_async, get_online_context, get_offline_context
from .freshness import (
    FreshnessReportResponse,
    SingleSourceFreshnessResponse,
    FreshnessStatus,
    check_all_sources_freshness,
    check_source_freshness,
    get_source_by_name,
    get_enabled_sources,
    load_sources_config,
)

app = FastAPI(
    title="Freshness Service",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)

# Enable CORS for frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Pydantic Models
# ============================================================================

class Source(BaseModel):
    """A source used to generate the answer."""
    url: str
    snippet: str
    retrieval_type: Literal["online", "offline_keyword", "offline_semantic"]
    timestamp: str | None = None
    url_hash: str | None = None


class ChatRequest(BaseModel):
    """Request body for chat endpoint."""
    query: str = Field(..., min_length=1, description="User's question")
    conversation_id: str | None = Field(None, description="Optional conversation ID for context")
    prefer_mode: Literal["ONLINE", "OFFLINE"] | None = Field(
        None,
        description="Optional retrieval preference. ONLINE forces online retrieval, OFFLINE forces archive retrieval.",
    )


class TimingInfo(BaseModel):
    """Timing metrics for the request."""
    search_ms: int = 0
    scrape_ms: int = 0
    llm_ms: int = 0
    total_ms: int = 0


class ChatResponse(BaseModel):
    """Response from chat endpoint."""
    conversation_id: str
    answer: str
    mode: Literal["ONLINE", "OFFLINE_ARCHIVE", "LOCAL_WEIGHTS"]
    sources: list[Source]
    timing: TimingInfo


class ChatStreamEvent(BaseModel):
    """SSE event for streaming chat."""
    event: Literal["meta", "token", "done", "error"]
    data: dict


class ArchiveEntry(BaseModel):
    """Archive entry for list view."""
    url_hash: str
    url: str
    timestamp: str
    excerpt: str


class ArchiveSearchResponse(BaseModel):
    """Response from archive search."""
    entries: list[ArchiveEntry]
    total: int
    cursor: str | None = None


class ArchivePageResponse(BaseModel):
    """Response from archive page detail."""
    url_hash: str
    url: str
    content: str
    timestamp: str


class SettingsResponse(BaseModel):
    """Non-secret configuration values."""
    brave_api_key_set: bool
    lm_studio_base_url: str
    model_name: str
    offline_retrieval_mode: Literal["keyword", "semantic"]
    max_search_results: int
    request_timeout_s: int
    max_chars_per_source: int
    semantic_top_k: int


class HealthStatus(BaseModel):
    """Health status for a service."""
    status: Literal["ok", "error", "unavailable"]
    message: str | None = None
    latency_ms: int | None = None


class HealthResponse(BaseModel):
    """Health check response."""
    backend: HealthStatus
    lm_studio: HealthStatus
    brave_search: HealthStatus


class ConfigUpdate(BaseModel):
    """Request body for config update."""
    max_search_results: int | None = Field(None, ge=1)
    request_timeout_s: int | None = Field(None, ge=1)
    max_chars_per_source: int | None = Field(None, ge=100)
    offline_retrieval_mode: Literal["keyword", "semantic"] | None = None
    semantic_top_k: int | None = Field(None, ge=1)
    lm_studio_base_url: str | None = None
    model_name: str | None = None
    brave_api_key: str | None = None


class ErrorResponse(BaseModel):
    """Error response."""
    code: str
    message: str


# ============================================================================
# Helper Functions
# ============================================================================

def _payload_to_dict(payload: BaseModel) -> dict:
    try:
        return payload.model_dump(exclude_unset=True)
    except AttributeError:
        return payload.dict(exclude_unset=True)


def _context_to_source(context: SourceContext, retrieval_type: str) -> Source:
    """Convert SourceContext to Source model."""
    return Source(
        url=context.url,
        snippet=context.text[:500] if context.text else "",
        retrieval_type=retrieval_type,
        timestamp=context.timestamp_iso,
        url_hash=archive.hash_url(context.url) if context.url != "N/A" else None,
    )


def _determine_retrieval_type(mode: str, settings) -> str:
    """Determine retrieval type based on mode and settings."""
    if mode == "ONLINE":
        return "online"
    elif mode == "OFFLINE_ARCHIVE":
        if settings.offline_retrieval_mode == "semantic":
            return "offline_semantic"
        return "offline_keyword"
    return "offline_keyword"


# ============================================================================
# Startup Event
# ============================================================================

@app.on_event("startup")
async def startup() -> None:
    settings = get_settings()
    archive.init_db(settings.db_path)


# ============================================================================
# API Routes - Chat
# ============================================================================

@app.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest) -> ChatResponse:
    """
    Process a chat query and return an answer with sources.
    
    This endpoint orchestrates:
    1. Online retrieval (Brave Search + scraping) if available
    2. Offline retrieval (SQLite keyword or Chroma semantic) as fallback
    3. LLM response generation via LM Studio
    """
    start_time = time.perf_counter()
    settings = get_settings()
    
    conversation_id = request.conversation_id or str(uuid.uuid4())
    
    # Track timing
    search_start = time.perf_counter()
    
    try:
        answer, mode, contexts = await ask_llm_async(
            request.query, prefer_mode=request.prefer_mode
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"code": "LLM_ERROR", "message": str(e)}
        )
    
    total_time = time.perf_counter() - start_time
    
    # Convert contexts to sources
    retrieval_type = _determine_retrieval_type(mode, settings)
    sources = [
        _context_to_source(ctx, retrieval_type)
        for ctx in contexts
        if ctx.url != "N/A"
    ]
    
    timing = TimingInfo(
        search_ms=int((time.perf_counter() - search_start) * 1000),
        total_ms=int(total_time * 1000),
    )
    
    return ChatResponse(
        conversation_id=conversation_id,
        answer=answer,
        mode=mode,
        sources=sources,
        timing=timing,
    )


@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    """
    Stream chat response via Server-Sent Events (SSE).
    
    Event types:
    - meta: mode, sources, conversation_id
    - token: partial assistant text
    - done: finalization event
    - error: error details
    """
    import json
    
    async def generate_events() -> AsyncGenerator[str, None]:
        settings = get_settings()
        conversation_id = request.conversation_id or str(uuid.uuid4())
        
        try:
            # Gather contexts first
            if request.prefer_mode == "OFFLINE":
                contexts = await get_offline_context(request.query)
                if contexts:
                    mode = "OFFLINE_ARCHIVE"
                else:
                    mode = "LOCAL_WEIGHTS"
                    contexts = []
            elif request.prefer_mode == "ONLINE":
                contexts = await get_online_context(request.query)
                if contexts:
                    mode = "ONLINE"
                else:
                    mode = "LOCAL_WEIGHTS"
                    contexts = []
            else:
                contexts = await get_online_context(request.query)
                mode = "ONLINE"

                if not contexts:
                    contexts = await get_offline_context(request.query)
                    if contexts:
                        mode = "OFFLINE_ARCHIVE"
                    else:
                        mode = "LOCAL_WEIGHTS"
                        contexts = []
            
            # Convert contexts to sources
            retrieval_type = _determine_retrieval_type(mode, settings)
            sources = [
                _context_to_source(ctx, retrieval_type).model_dump()
                for ctx in contexts
                if ctx.url != "N/A"
            ]
            
            # Send meta event
            meta_data = {
                "mode": mode,
                "sources": sources,
                "conversation_id": conversation_id,
            }
            yield f"event: meta\ndata: {json.dumps(meta_data)}\n\n"
            
            # Build context for LLM
            if contexts:
                context_str = "\n---\n".join(
                    [f"SOURCE: {ctx.url}\nCONTENT: {ctx.text}" for ctx in contexts]
                )
            else:
                context_str = "No sources available."
            
            system_prompt = (
                "You are a helpful AI that answers ONLY from provided context.\n"
                f"Current Mode: {mode}\n"
                "Instructions: Use the provided context to answer. "
                "If the context is empty or does not contain the exact answer, "
                "say you could not verify it and ask to try again.\n"
                "Always cite the URL for factual claims.\n\n"
                "CONTEXT:\n"
                f"{context_str}"
            )
            
            # Try streaming from LM Studio
            import requests
            
            payload = {
                "model": settings.model_name,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": request.query},
                ],
                "temperature": 0.2,
                "stream": True,
            }
            
            full_response = ""
            
            try:
                response = requests.post(
                    f"{settings.lm_studio_base_url}/chat/completions",
                    json=payload,
                    timeout=settings.request_timeout_s,
                    stream=True,
                )
                response.raise_for_status()
                
                for line in response.iter_lines():
                    if line:
                        line_str = line.decode("utf-8")
                        if line_str.startswith("data: "):
                            data_str = line_str[6:]
                            if data_str == "[DONE]":
                                break
                            try:
                                data = json.loads(data_str)
                                delta = data.get("choices", [{}])[0].get("delta", {})
                                content = delta.get("content", "")
                                if content:
                                    full_response += content
                                    yield f"event: token\ndata: {json.dumps({'text': content})}\n\n"
                            except json.JSONDecodeError:
                                continue
                
            except Exception as e:
                # Fallback to non-streaming
                try:
                    payload["stream"] = False
                    response = requests.post(
                        f"{settings.lm_studio_base_url}/chat/completions",
                        json=payload,
                        timeout=settings.request_timeout_s,
                    )
                    response.raise_for_status()
                    full_response = response.json()["choices"][0]["message"]["content"]
                    yield f"event: token\ndata: {json.dumps({'text': full_response})}\n\n"
                except Exception as fallback_error:
                    yield f"event: error\ndata: {json.dumps({'code': 'LLM_UNAVAILABLE', 'message': str(fallback_error)})}\n\n"
                    return
            
            # Send done event
            yield f"event: done\ndata: {json.dumps({'final_text': full_response})}\n\n"
            
        except Exception as e:
            yield f"event: error\ndata: {json.dumps({'code': 'STREAM_ERROR', 'message': str(e)})}\n\n"
    
    return StreamingResponse(
        generate_events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ============================================================================
# API Routes - Archive
# ============================================================================

@app.get("/api/archive/search", response_model=ArchiveSearchResponse)
async def archive_search(
    q: str = Query("", description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Number of results"),
    cursor: str | None = Query(None, description="Pagination cursor"),
) -> ArchiveSearchResponse:
    """
    Search the archive for pages matching the query.
    Returns paginated list of archive entries with excerpts.
    """
    settings = get_settings()
    
    import sqlite3
    
    with sqlite3.connect(settings.db_path) as conn:
        cur = conn.cursor()
        
        if q:
            search_term = f"%{q.lower()}%"
            cur.execute(
                """
                SELECT url_hash, url, content, timestamp
                FROM pages
                WHERE url LIKE ? OR content LIKE ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (search_term, search_term, limit + 1),
            )
        else:
            cur.execute(
                """
                SELECT url_hash, url, content, timestamp
                FROM pages
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (limit + 1,),
            )
        
        rows = cur.fetchall()
        
        # Get total count
        if q:
            cur.execute(
                """
                SELECT COUNT(*) FROM pages
                WHERE url LIKE ? OR content LIKE ?
                """,
                (search_term, search_term),
            )
        else:
            cur.execute("SELECT COUNT(*) FROM pages")
        total = cur.fetchone()[0]
    
    # Check if there are more results
    has_more = len(rows) > limit
    if has_more:
        rows = rows[:limit]
    
    entries = [
        ArchiveEntry(
            url_hash=row[0],
            url=row[1],
            excerpt=row[2][:200] + "..." if len(row[2]) > 200 else row[2],
            timestamp=str(row[3]),
        )
        for row in rows
    ]
    
    return ArchiveSearchResponse(
        entries=entries,
        total=total,
        cursor=entries[-1].url_hash if has_more and entries else None,
    )


@app.get("/api/archive/page/{url_hash}", response_model=ArchivePageResponse)
async def archive_page(url_hash: str) -> ArchivePageResponse:
    """
    Get detailed view of an archived page by its URL hash.
    """
    settings = get_settings()
    
    import sqlite3
    
    with sqlite3.connect(settings.db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT url_hash, url, content, timestamp
            FROM pages
            WHERE url_hash = ?
            """,
            (url_hash,),
        )
        row = cur.fetchone()
    
    if not row:
        raise HTTPException(
            status_code=404,
            detail={"code": "NOT_FOUND", "message": f"Archive page not found: {url_hash}"}
        )
    
    return ArchivePageResponse(
        url_hash=row[0],
        url=row[1],
        content=row[2],
        timestamp=str(row[3]),
    )


# ============================================================================
# API Routes - Settings & Health
# ============================================================================

@app.get("/api/settings", response_model=SettingsResponse)
async def get_api_settings() -> SettingsResponse:
    """
    Get non-secret configuration values.
    """
    settings = get_settings()
    
    return SettingsResponse(
        brave_api_key_set=bool(settings.brave_api_key),
        lm_studio_base_url=settings.lm_studio_base_url,
        model_name=settings.model_name,
        offline_retrieval_mode=settings.offline_retrieval_mode,
        max_search_results=settings.max_search_results,
        request_timeout_s=settings.request_timeout_s,
        max_chars_per_source=settings.max_chars_per_source,
        semantic_top_k=settings.semantic_top_k,
    )


@app.post("/api/config")
async def update_config(payload: ConfigUpdate) -> dict:
    """
    Update runtime configuration.
    """
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


@app.get("/api/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Check health of backend, LM Studio, and Brave Search.
    """
    import requests
    
    settings = get_settings()
    
    # Backend is always ok if we reach this point
    backend_status = HealthStatus(status="ok", message="Backend is running")
    
    # Check LM Studio
    lm_studio_status: HealthStatus
    try:
        start = time.perf_counter()
        response = requests.get(
            f"{settings.lm_studio_base_url}/models",
            timeout=5,
        )
        latency = int((time.perf_counter() - start) * 1000)
        
        if response.status_code == 200:
            lm_studio_status = HealthStatus(
                status="ok",
                message="LM Studio is reachable",
                latency_ms=latency,
            )
        else:
            lm_studio_status = HealthStatus(
                status="error",
                message=f"LM Studio returned status {response.status_code}",
            )
    except requests.exceptions.ConnectionError:
        lm_studio_status = HealthStatus(
            status="unavailable",
            message=f"Cannot connect to LM Studio at {settings.lm_studio_base_url}",
        )
    except Exception as e:
        lm_studio_status = HealthStatus(
            status="error",
            message=str(e),
        )
    
    # Check Brave Search
    brave_status: HealthStatus
    if not settings.brave_api_key:
        brave_status = HealthStatus(
            status="unavailable",
            message="Brave API key not configured",
        )
    else:
        try:
            start = time.perf_counter()
            headers = {
                "Accept": "application/json",
                "X-Subscription-Token": settings.brave_api_key,
            }
            response = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers=headers,
                params={"q": "test", "count": 1},
                timeout=5,
            )
            latency = int((time.perf_counter() - start) * 1000)
            
            if response.status_code == 200:
                brave_status = HealthStatus(
                    status="ok",
                    message="Brave Search is reachable",
                    latency_ms=latency,
                )
            elif response.status_code == 401:
                brave_status = HealthStatus(
                    status="error",
                    message="Brave API key is invalid",
                )
            else:
                brave_status = HealthStatus(
                    status="error",
                    message=f"Brave Search returned status {response.status_code}",
                )
        except Exception as e:
            brave_status = HealthStatus(
                status="error",
                message=str(e),
            )
    
    return HealthResponse(
        backend=backend_status,
        lm_studio=lm_studio_status,
        brave_search=brave_status,
    )


# ============================================================================
# API Routes - Freshness (TTL-based)
# ============================================================================

@app.get("/api/freshness", response_model=FreshnessReportResponse)
async def get_freshness_report() -> FreshnessReportResponse:
    """
    Get a detailed freshness report for all configured data sources.
    
    Returns a breakdown of which sources are fresh vs stale based on their
    configured TTL (Time-to-Live) thresholds.
    
    The response includes:
    - Overview with counts of fresh/stale/error sources
    - Detailed information for each source including:
      - Last modified timestamp
      - Age in minutes/seconds
      - Time until stale (negative if already stale)
      - TTL configuration
    """
    settings = get_settings()
    report = await asyncio.to_thread(
        check_all_sources_freshness,
        default_db_path=settings.db_path,
    )
    return report


@app.get("/api/freshness/{source_id}", response_model=SingleSourceFreshnessResponse)
async def get_source_freshness(source_id: str) -> SingleSourceFreshnessResponse:
    """
    Get freshness status for a specific data source.
    
    Args:
        source_id: The unique identifier of the source (as defined in sources.yaml)
        
    Returns:
        Detailed freshness information including:
        - Current status (fresh/stale/error/unknown)
        - Age and time until stale
        - Last modified timestamp
        
    Raises:
        404: If the source is not found in configuration
    """
    source = get_source_by_name(source_id)
    
    if source is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "SOURCE_NOT_FOUND",
                "message": f"Source '{source_id}' not found in configuration",
            },
        )
    
    settings = get_settings()
    detail = await asyncio.to_thread(
        check_source_freshness,
        source,
        default_db_path=settings.db_path,
    )
    
    return SingleSourceFreshnessResponse(
        detail=detail,
        is_fresh=detail.status == FreshnessStatus.FRESH,
    )


@app.get("/api/freshness/sources/list")
async def list_freshness_sources() -> dict:
    """
    List all configured freshness sources.
    
    Returns the source configurations without checking their freshness status.
    Useful for discovering available sources before querying them.
    """
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


@app.post("/api/freshness/reload")
async def reload_freshness_config() -> dict:
    """
    Reload the freshness configuration from sources.yaml.
    
    Use this endpoint after modifying sources.yaml to apply changes
    without restarting the service.
    """
    config = load_sources_config(force_reload=True)
    return {
        "status": "ok",
        "message": "Configuration reloaded",
        "sources_count": len(config.sources),
    }


# ============================================================================
# Legacy Routes (for backward compatibility)
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


def _to_report(context: SourceContext) -> FreshnessReport:
    return FreshnessReport(
        source_name=context.url,
        is_fresh=context.is_fresh,
        last_updated=context.timestamp_iso,
        latency_seconds=context.latency_seconds,
    )


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


@app.get("/freshness", response_model=LegacyFreshnessResponse)
async def legacy_freshness(query: str = Query(..., min_length=1)) -> LegacyFreshnessResponse:
    """
    Legacy freshness endpoint for backward compatibility.
    
    Note: This endpoint is deprecated. Use /api/freshness for TTL-based
    freshness checks or /api/chat for query-based responses.
    """
    answer, mode, contexts = await ask_llm_async(query)
    reports = [_to_report(context) for context in contexts]
    return LegacyFreshnessResponse(query=query, mode=mode, reports=reports, answer=answer)
