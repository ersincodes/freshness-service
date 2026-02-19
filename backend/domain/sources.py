"""
Source context and mapping logic.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from .types import RetrievalType, RetrievalMode, DOC_URL_PREFIX, FALLBACK_SOURCE_URL, FALLBACK_SOURCE_TEXT


@dataclass(frozen=True)
class SourceContext:
    """Represents a source of context information for RAG retrieval."""
    url: str
    text: str
    timestamp_iso: str
    is_fresh: bool
    latency_seconds: float
    filename: str | None = None
    metadata: dict[str, Any] | None = None

    def is_document_source(self) -> bool:
        return self.url.startswith(DOC_URL_PREFIX)

    @classmethod
    def create_fallback(cls) -> "SourceContext":
        return cls(FALLBACK_SOURCE_URL, FALLBACK_SOURCE_TEXT, dt.datetime.utcnow().isoformat(), False, 0.0)


def determine_retrieval_type(mode: RetrievalMode, offline_mode: str, is_document: bool = False) -> RetrievalType:
    """Determine retrieval type based on mode and settings."""
    if is_document:
        return "document_semantic" if offline_mode == "semantic" else "document_keyword"
    if mode == "ONLINE":
        return "online"
    if mode == "OFFLINE_ARCHIVE":
        return "offline_semantic" if offline_mode == "semantic" else "offline_keyword"
    return "offline_keyword"


def context_to_source_dict(ctx: SourceContext, retrieval_type: RetrievalType, url_hash_fn: callable | None = None) -> dict[str, Any]:
    """Convert SourceContext to API response dict."""
    is_doc = ctx.is_document_source()
    result: dict[str, Any] = {
        "url": ctx.url,
        "snippet": ctx.text[:500] if ctx.text else "",
        "retrieval_type": retrieval_type,
        "timestamp": ctx.timestamp_iso,
        "source_type": "document" if is_doc else "web",
        "url_hash": None if is_doc else (url_hash_fn(ctx.url) if url_hash_fn and ctx.url != FALLBACK_SOURCE_URL else None),
        "filename": ctx.filename if is_doc else None,
        "location": {"page": ctx.metadata.get("page"), "sheet": ctx.metadata.get("sheet"),
                     "row_start": ctx.metadata.get("row_start"), "row_end": ctx.metadata.get("row_end")} if is_doc and ctx.metadata else None,
    }
    return result


def build_context_string(contexts: list[SourceContext]) -> str:
    """Build formatted context string for LLM prompts."""
    if not contexts:
        return "No sources available."
    return "\n---\n".join(f"SOURCE: {c.url}\nCONTENT: {c.text}" for c in contexts)


def build_location_string(metadata: dict[str, Any] | None) -> str:
    """Build human-readable location string from metadata."""
    if not metadata:
        return ""
    parts = []
    if metadata.get("page"):
        parts.append(f"Page {metadata['page']}")
    if metadata.get("sheet"):
        parts.append(f"Sheet: {metadata['sheet']}")
    if metadata.get("row_start") and metadata.get("row_end"):
        parts.append(f"Rows {metadata['row_start']}-{metadata['row_end']}")
    return ", ".join(parts)
