"""RAG context gathering: web archive, online search, and document chunks."""
from __future__ import annotations

import asyncio
import datetime as dt
import time
from dataclasses import dataclass

from ...config import Settings
from ...domain import DOC_URL_PREFIX, SourceContext, build_location_string
from ...integrations import BraveClient
from ...repositories import ArchiveRepository, DocumentRepository
from ...scraper import get_clean_text
from ...vector_store import query_document_chunks_similar, query_similar, upsert_page
from .intents import QueryIntent, detect_query_intent


@dataclass(frozen=True)
class ChatRetrievalDeps:
    """Dependencies for retrieval without coupling to ChatService."""

    settings: Settings
    archive: ArchiveRepository
    docs: DocumentRepository
    brave: BraveClient


def allocate_budget(
    deps: ChatRetrievalDeps,
    web_ctx: list[SourceContext],
    doc_ctx: list[SourceContext],
) -> list[SourceContext]:
    """Merge and prune contexts based on budget settings."""
    s = deps.settings
    total_budget = s.total_context_budget
    web_budget = int(total_budget * s.web_budget_fraction)
    doc_budget = total_budget - web_budget

    result: list[SourceContext] = []
    web_used = 0

    for ctx in web_ctx:
        max_chars = s.web_max_chars
        truncated_text = ctx.text[:max_chars] if max_chars > 0 else ctx.text
        ctx_len = len(truncated_text)

        if web_used + ctx_len <= web_budget:
            if truncated_text != ctx.text:
                ctx = SourceContext(
                    ctx.url,
                    truncated_text,
                    ctx.timestamp_iso,
                    ctx.is_fresh,
                    ctx.latency_seconds,
                    ctx.filename,
                    ctx.metadata,
                )
            result.append(ctx)
            web_used += ctx_len

    doc_budget += web_budget - web_used

    doc_used = 0
    doc_max = s.doc_max_chars
    min_useful = 200

    for ctx in doc_ctx:
        remaining = doc_budget - doc_used
        if remaining < min_useful:
            break

        text = ctx.text if doc_max == 0 else ctx.text[:doc_max]
        ctx_len = len(text)

        if ctx_len <= remaining:
            if text != ctx.text:
                ctx = SourceContext(
                    ctx.url,
                    text,
                    ctx.timestamp_iso,
                    ctx.is_fresh,
                    ctx.latency_seconds,
                    ctx.filename,
                    ctx.metadata,
                )
            result.append(ctx)
            doc_used += ctx_len
        else:
            truncated = text[:remaining]
            result.append(
                SourceContext(
                    ctx.url,
                    truncated,
                    ctx.timestamp_iso,
                    ctx.is_fresh,
                    ctx.latency_seconds,
                    ctx.filename,
                    ctx.metadata,
                )
            )
            doc_used += len(truncated)

    return result


async def fetch_source(
    deps: ChatRetrievalDeps,
    query: str,
    url: str,
    fallback: str,
) -> SourceContext | None:
    s = deps.settings
    start = time.perf_counter()
    try:
        text = await asyncio.wait_for(get_clean_text(url), timeout=s.request_timeout_s)
    except asyncio.TimeoutError:
        text = None
    latency = time.perf_counter() - start
    if not text:
        if not fallback:
            return None
        text = fallback
    truncated = text[: s.max_chars_per_source]
    await deps.archive.save_page_async(query, url, text)
    ts = dt.datetime.utcnow().isoformat()
    if s.offline_retrieval_mode == "semantic":
        try:
            await asyncio.to_thread(
                upsert_page,
                s.chroma_dir,
                s.embed_model_name,
                deps.archive.hash_url(url),
                url,
                text,
                ts,
            )
        except Exception:
            pass
    return SourceContext(url, truncated, ts, True, latency)


async def get_online_context(deps: ChatRetrievalDeps, query: str) -> list[SourceContext]:
    if not deps.brave.is_configured:
        return []
    try:
        results = await deps.brave.search(query)
    except Exception:
        return []
    tasks = [
        asyncio.create_task(
            fetch_source(deps, query, r.url, f"SEARCH_SNIPPET:\n{r.snippet}" if r.snippet else "")
        )
        for r in results
    ]
    return [c for c in await asyncio.gather(*tasks) if c]


async def get_offline_context(deps: ChatRetrievalDeps, query: str) -> list[SourceContext]:
    s = deps.settings
    top_k = s.web_top_k
    if s.offline_retrieval_mode == "semantic":
        try:
            rows = await asyncio.to_thread(
                query_similar, s.chroma_dir, s.embed_model_name, query, top_k
            )
        except Exception:
            rows = await deps.archive.search_offline_async(query, top_k)
        if not rows:
            rows = await deps.archive.search_offline_async(query, top_k)
    else:
        rows = await deps.archive.search_offline_async(query, top_k)
    return [
        SourceContext(url, text[: s.max_chars_per_source], str(ts), False, 0.0)
        for url, text, ts in rows
    ]


async def get_document_context(
    deps: ChatRetrievalDeps,
    query: str,
    doc_ids: list[str] | None = None,
    intent: QueryIntent | None = None,
) -> list[SourceContext]:
    """Hybrid document retrieval: column-value + filename + row-targeted + semantic + keyword."""
    s = deps.settings
    seen_chunk_ids: set[str] = set()
    all_chunks: list[tuple[str, str, str, dict, str, str, bool]] = []

    def _collect(chunks: list, targeted: bool = True) -> int:
        added = 0
        for c in chunks:
            if c.chunk_id not in seen_chunk_ids:
                seen_chunk_ids.add(c.chunk_id)
                all_chunks.append(
                    (
                        c.chunk_id,
                        c.document_id,
                        c.content,
                        c.metadata,
                        c.filename or "",
                        c.timestamp,
                        targeted,
                    )
                )
                added += 1
        return added

    should_use_fallbacks = True
    exact_hits = 0

    if intent and intent.column_value:
        cv = intent.column_value
        cv_terms = [f"{cv.column_name}={cv.value}"]
        try:
            exact_hits += _collect(
                await deps.docs.search_chunks_by_terms_async(cv_terms, doc_ids, limit=5),
                targeted=True,
            )
        except Exception:
            pass

    if intent and intent.filename_pattern:
        filename_limit = 1 if intent.wants_last else s.doc_keyword_top_k
        try:
            _collect(
                await deps.docs.search_chunks_by_filename_async(
                    intent.filename_pattern,
                    doc_ids,
                    limit=filename_limit,
                    last_chunks=intent.wants_last,
                ),
                targeted=True,
            )
        except Exception:
            pass

    if intent and intent.row_intent:
        row_terms = [f"Row {intent.row_intent.row_number}:", f"Row {intent.row_intent.row_number}"]
        try:
            exact_hits += _collect(
                await deps.docs.search_chunks_by_terms_async(row_terms, doc_ids, limit=5),
                targeted=True,
            )
        except Exception:
            pass

    if intent and (
        (intent.column_value and exact_hits > 0)
        or (intent.row_intent and exact_hits > 0)
        or (intent.wants_last and intent.filename_pattern)
    ):
        should_use_fallbacks = False

    if should_use_fallbacks and s.offline_retrieval_mode == "semantic":
        try:
            rows = await asyncio.to_thread(
                query_document_chunks_similar,
                s.chroma_dir,
                s.embed_model_name,
                query,
                s.doc_semantic_top_k,
                doc_ids,
            )
            for chunk_id, doc_id, content, meta, filename in rows:
                if chunk_id not in seen_chunk_ids:
                    seen_chunk_ids.add(chunk_id)
                    all_chunks.append(
                        (
                            chunk_id,
                            doc_id,
                            content,
                            meta,
                            filename or "",
                            dt.datetime.utcnow().isoformat(),
                            False,
                        )
                    )
        except Exception:
            pass

    if should_use_fallbacks:
        try:
            _collect(
                await deps.docs.search_chunks_keyword_async(query, doc_ids, s.doc_keyword_top_k),
                targeted=False,
            )
        except Exception:
            pass

    if intent and intent.column_value:
        cv_marker = f"{intent.column_value.column_name}={intent.column_value.value}"
        all_chunks.sort(key=lambda x: (not x[6], cv_marker.lower() not in x[2].lower()))
        if exact_hits > 0:
            all_chunks = [x for x in all_chunks if cv_marker.lower() in x[2].lower()]
    elif intent and intent.row_intent:
        row_marker = f"Row {intent.row_intent.row_number}"
        all_chunks.sort(key=lambda x: (not x[6], row_marker not in x[2]))
        if exact_hits > 0:
            all_chunks = [x for x in all_chunks if f"{row_marker}:" in x[2]]

    contexts: list[SourceContext] = []
    for _chunk_id, doc_id, content, meta, filename, ts, _is_row_match in all_chunks:
        filtered_content = content
        if intent and intent.column_value and exact_hits > 0:
            cv_marker_lc = f"{intent.column_value.column_name}={intent.column_value.value}".lower()
            matching_lines = [line for line in content.splitlines() if cv_marker_lc in line.lower()]
            if matching_lines:
                filtered_content = "\n".join(matching_lines)
        elif intent and intent.row_intent and exact_hits > 0:
            row_prefix = f"Row {intent.row_intent.row_number}:"
            matching_lines = [line for line in content.splitlines() if line.startswith(row_prefix)]
            if matching_lines:
                filtered_content = "\n".join(matching_lines)
        elif intent and intent.wants_last and intent.filename_pattern:
            row_lines = [line for line in content.splitlines() if line.startswith("Row ")]
            if row_lines:
                filtered_content = row_lines[-1]

        loc = build_location_string(meta)
        contexts.append(
            SourceContext(
                f"{DOC_URL_PREFIX}{doc_id}",
                f"[{filename}] {loc}\n{filtered_content}",
                ts,
                False,
                0.0,
                filename,
                meta,
            )
        )

    return contexts


async def gather_contexts(
    deps: ChatRetrievalDeps,
    query: str,
    prefer_mode: str | None,
    include_web: bool,
    include_docs: bool,
    doc_ids: list[str] | None,
) -> tuple[str, list[SourceContext]]:
    web_ctx: list[SourceContext] = []
    doc_ctx: list[SourceContext] = []
    mode = "LOCAL_WEIGHTS"

    if prefer_mode == "OFFLINE":
        ctx = await get_offline_context(deps, query)
        if ctx:
            mode, web_ctx = "OFFLINE_ARCHIVE", ctx
    elif include_web:
        if prefer_mode == "ONLINE":
            ctx = await get_online_context(deps, query)
            if ctx:
                mode, web_ctx = "ONLINE", ctx
        else:
            ctx = await get_online_context(deps, query)
            if ctx:
                mode, web_ctx = "ONLINE", ctx
            else:
                ctx = await get_offline_context(deps, query)
                if ctx:
                    mode, web_ctx = "OFFLINE_ARCHIVE", ctx

    if include_docs:
        intent = detect_query_intent(query)
        doc_ctx = await get_document_context(deps, query, doc_ids, intent)
        if doc_ctx and (not include_web or mode == "LOCAL_WEIGHTS"):
            mode = "OFFLINE_ARCHIVE"

    all_ctx = allocate_budget(deps, web_ctx, doc_ctx)
    if not all_ctx:
        fallback_ctx = [SourceContext.create_fallback()]
        if prefer_mode == "OFFLINE":
            return ("OFFLINE_ARCHIVE", fallback_ctx)
        return ("LOCAL_WEIGHTS", fallback_ctx)
    return (mode, all_ctx)
