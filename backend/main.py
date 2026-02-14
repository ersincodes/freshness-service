from __future__ import annotations

import asyncio
import datetime as dt
import json
import time
from dataclasses import dataclass

import requests

from . import archive
from .config import get_settings
from .scraper import get_clean_text
from .vector_store import query_similar, upsert_page

# Import answer caching functions
from .archive import save_answer, get_cached_answer


@dataclass(frozen=True)
class SourceContext:
    url: str
    text: str
    timestamp_iso: str
    is_fresh: bool
    latency_seconds: float


def _parse_json_response(raw: str) -> dict | None:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        try:
            return json.loads(raw[start : end + 1])
        except json.JSONDecodeError:
            return None


def _build_context(blocks: list[SourceContext]) -> str:
    return "\n---\n".join(
        [f"SOURCE: {block.url}\nCONTENT: {block.text}" for block in blocks]
    )


def _fetch_search_results(query: str, settings) -> list[dict]:
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": settings.brave_api_key,
    }
    params = {"q": query, "count": settings.max_search_results}
    resp = requests.get(
        "https://api.search.brave.com/res/v1/web/search",
        headers=headers,
        params=params,
        timeout=settings.request_timeout_s,
    )
    resp.raise_for_status()
    return resp.json().get("web", {}).get("results", [])


async def get_online_context(query: str) -> list[SourceContext]:
    settings = get_settings()
    if not settings.brave_api_key:
        print("Missing BRAVE_API_KEY; skipping online search.")
        return []

    try:
        results = await asyncio.to_thread(_fetch_search_results, query, settings)
    except Exception as exc:
        print(f"Online search failed: {exc}")
        return []

    tasks: list[asyncio.Task[SourceContext | None]] = []
    for res in results:
        url = res.get("url")
        if not url:
            continue
        title = res.get("title") or ""
        description = res.get("description") or ""
        snippet = "\n".join(part for part in [title.strip(), description.strip()] if part)
        fallback_text = f"SEARCH_SNIPPET:\n{snippet}" if snippet else ""
        tasks.append(asyncio.create_task(_fetch_source_context(query, url, fallback_text)))

    contexts: list[SourceContext] = []
    for result in await asyncio.gather(*tasks):
        if result is not None:
            contexts.append(result)
    return contexts


async def _fetch_source_context(
    query: str, url: str, fallback_text: str
) -> SourceContext | None:
    settings = get_settings()
    start = time.perf_counter()
    try:
        text = await asyncio.wait_for(
            get_clean_text(url), timeout=settings.request_timeout_s
        )
    except asyncio.TimeoutError:
        text = None
    latency = time.perf_counter() - start
    if not text:
        if not fallback_text:
            return None
        text = fallback_text

    truncated = text[: settings.max_chars_per_source]
    await asyncio.to_thread(archive.save_to_archive, settings.db_path, query, url, text)
    timestamp = dt.datetime.utcnow().isoformat()

    if settings.offline_retrieval_mode == "semantic":
        try:
            await asyncio.to_thread(
                upsert_page,
                settings.chroma_dir,
                settings.embed_model_name,
                archive.hash_url(url),
                url,
                text,
                timestamp,
            )
        except Exception as exc:
            print(f"Chroma upsert failed; continuing without vector store: {exc}")

    return SourceContext(
        url=url,
        text=truncated,
        timestamp_iso=timestamp,
        is_fresh=True,
        latency_seconds=latency,
    )


async def get_offline_context(query: str) -> list[SourceContext]:
    settings = get_settings()
    if settings.offline_retrieval_mode == "semantic":
        try:
            rows = await asyncio.to_thread(
                query_similar,
                settings.chroma_dir,
                settings.embed_model_name,
                query,
                settings.semantic_top_k,
            )
        except Exception as exc:
            print(
                "Semantic retrieval failed; falling back to keyword search. "
                f"Error: {exc}"
            )
            rows = await asyncio.to_thread(
                archive.search_offline, settings.db_path, query, settings.semantic_top_k
            )
    else:
        rows = await asyncio.to_thread(
            archive.search_offline, settings.db_path, query, settings.semantic_top_k
        )

    contexts: list[SourceContext] = []
    for url, text, timestamp in rows:
        contexts.append(
            SourceContext(
                url=url,
                text=text[: settings.max_chars_per_source],
                timestamp_iso=str(timestamp),
                is_fresh=False,
                latency_seconds=0.0,
            )
        )
    return contexts


async def _gather_contexts(
    user_query: str, prefer_mode: str | None = None
) -> tuple[str, list[SourceContext]]:
    if prefer_mode == "OFFLINE":
        print("Offline mode: Checking local archive...")
        contexts = await get_offline_context(user_query)
        if contexts:
            return "OFFLINE_ARCHIVE", contexts
        return (
            "LOCAL_WEIGHTS",
            [
                SourceContext(
                    url="N/A",
                    text="No offline information found.",
                    timestamp_iso=dt.datetime.utcnow().isoformat(),
                    is_fresh=False,
                    latency_seconds=0.0,
                )
            ],
        )

    if prefer_mode == "ONLINE":
        contexts = await get_online_context(user_query)
        if contexts:
            return "ONLINE", contexts
        return (
            "LOCAL_WEIGHTS",
            [
                SourceContext(
                    url="N/A",
                    text="No online information found.",
                    timestamp_iso=dt.datetime.utcnow().isoformat(),
                    is_fresh=False,
                    latency_seconds=0.0,
                )
            ],
        )

    contexts = await get_online_context(user_query)
    mode = "ONLINE"

    if not contexts:
        print("Offline mode: Checking local archive...")
        contexts = await get_offline_context(user_query)
        if contexts:
            mode = "OFFLINE_ARCHIVE"
        else:
            mode = "LOCAL_WEIGHTS"
            contexts = [
                SourceContext(
                    url="N/A",
                    text="No fresh information found.",
                    timestamp_iso=dt.datetime.utcnow().isoformat(),
                    is_fresh=False,
                    latency_seconds=0.0,
                )
            ]

    return mode, contexts


def _post_llm(payload: dict, settings) -> str:
    response = requests.post(
        f"{settings.lm_studio_base_url}/chat/completions",
        json=payload,
        timeout=settings.request_timeout_s,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]


async def _extract_with_llm(
    user_query: str, context_blocks: list[SourceContext], settings
) -> dict | None:
    system_prompt = (
        "You are a strict information extraction engine.\n"
        "Use ONLY the provided context. Return a JSON object with keys:\n"
        '- "answer": string or null\n'
        '- "citation_url": string or null\n'
        '- "evidence_quote": string or null\n'
        "If the answer is not explicitly present in the context, "
        "set all fields to null.\n"
        "Do NOT add any extra text.\n\n"
        "CONTEXT:\n"
        f"{_build_context(context_blocks)}"
    )
    payload = {
        "model": settings.model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query},
        ],
        "temperature": 0.0,
    }
    raw = await asyncio.to_thread(_post_llm, payload, settings)
    return _parse_json_response(raw)


async def ask_llm_async(
    user_query: str, prefer_mode: str | None = None
) -> tuple[str, str, list[SourceContext]]:
    settings = get_settings()
    mode, context_blocks = await _gather_contexts(user_query, prefer_mode=prefer_mode)
    
    # In offline mode, first check if we have a cached answer
    if mode == "OFFLINE_ARCHIVE":
        cached = await asyncio.to_thread(
            get_cached_answer, settings.db_path, user_query
        )
        if cached:
            answer_text, citation_url, evidence, timestamp = cached
            response = f"{answer_text}\n\nSource: {citation_url or 'cached answer'}"
            if evidence:
                response = f"{response}\nEvidence: {evidence}"
            response = f"{response}\n(Cached from: {timestamp})"
            return response, mode, context_blocks
    
    extraction = await _extract_with_llm(user_query, context_blocks, settings)
    if extraction:
        answer = extraction.get("answer")
        citation_url = extraction.get("citation_url")
        evidence = extraction.get("evidence_quote")
        # Accept answer even if citation_url is missing (more lenient)
        if answer:
            # Try to infer citation from context if not provided
            if not citation_url and context_blocks:
                citation_url = context_blocks[0].url
            
            response = f"{answer}\n\nSource: {citation_url or 'extracted from context'}"
            if evidence:
                response = f"{response}\nEvidence: {evidence}"
            
            # Save successful answer to cache for future offline use
            if mode == "ONLINE":
                await asyncio.to_thread(
                    save_answer,
                    settings.db_path,
                    user_query,
                    answer,
                    citation_url,
                    evidence,
                )
            
            return response, mode, context_blocks
    
    if mode in {"OFFLINE_ARCHIVE", "LOCAL_WEIGHTS"}:
        if mode == "OFFLINE_ARCHIVE":
            response = (
                "I could not verify the answer from the offline archive. "
                "Please try online mode or add a relevant source."
            )
        else:
            response = (
                "I do not have any sources to answer this question. "
                "Please try online mode or add sources to the archive."
            )
        return response, mode, context_blocks
    system_prompt = (
        "You are a helpful AI that answers ONLY from provided context.\n"
        f"Current Mode: {mode}\n"
        "Instructions: Use the provided context to answer. "
        "If the context is empty or does not contain the exact answer, "
        "say you could not verify it and ask to try again.\n"
        "Always cite the URL for factual claims.\n\n"
        "CONTEXT:\n"
        f"{_build_context(context_blocks)}"
    )

    payload = {
        "model": settings.model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_query},
        ],
        "temperature": 0.2,
    }

    answer = await asyncio.to_thread(_post_llm, payload, settings)
    
    # Save successful answer from fallback LLM call
    if answer and mode == "ONLINE":
        # Extract first URL from context as citation
        first_url = context_blocks[0].url if context_blocks else None
        await asyncio.to_thread(
            save_answer,
            settings.db_path,
            user_query,
            answer,
            first_url,
            None,
        )
    
    return answer, mode, context_blocks


def ask_llm(user_query: str) -> str:
    answer, _, _ = asyncio.run(ask_llm_async(user_query))
    return answer


def run_cli() -> None:
    settings = get_settings()
    archive.init_db(settings.db_path)
    print("Freshness Service Started.")
    while True:
        q = input("\nUser: ")
        if q.lower() in {"exit", "quit"}:
            break
        answer = ask_llm(q)
        print(f"\nAI: {answer}")


if __name__ == "__main__":
    run_cli()
