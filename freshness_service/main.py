from __future__ import annotations

import datetime as dt
import requests

from . import archive
from .config import settings
from .scraper import get_clean_text
from .vector_store import query_similar, upsert_page


def _build_context(blocks: list[tuple[str, str]]) -> str:
    return "\n---\n".join(
        [f"SOURCE: {url}\nCONTENT: {text}" for url, text in blocks]
    )


def get_online_context(query: str) -> list[tuple[str, str]]:
    if not settings.brave_api_key:
        print("Missing BRAVE_API_KEY; skipping online search.")
        return []

    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": settings.brave_api_key,
    }
    params = {"q": query, "count": settings.max_search_results}

    try:
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers=headers,
            params=params,
            timeout=settings.request_timeout_s,
        )
        resp.raise_for_status()
        results = resp.json().get("web", {}).get("results", [])
    except Exception as exc:
        print(f"Online search failed: {exc}")
        return []

    context_blocks: list[tuple[str, str]] = []
    for res in results:
        url = res.get("url")
        if not url:
            continue
        text = get_clean_text(url)
        if not text:
            continue

        truncated = text[: settings.max_chars_per_source]
        archive.save_to_archive(settings.db_path, query, url, text)

        if settings.offline_retrieval_mode == "semantic":
            timestamp = dt.datetime.utcnow().isoformat()
            upsert_page(
                settings.chroma_dir,
                settings.embed_model_name,
                archive.hash_url(url),
                url,
                text,
                timestamp,
            )

        context_blocks.append((url, truncated))

    return context_blocks


def _get_offline_context(query: str) -> list[tuple[str, str]]:
    if settings.offline_retrieval_mode == "semantic":
        return query_similar(
            settings.chroma_dir,
            settings.embed_model_name,
            query,
            settings.semantic_top_k,
        )
    return archive.search_offline(settings.db_path, query, settings.semantic_top_k)


def ask_llm(user_query: str) -> str:
    context_blocks = get_online_context(user_query)
    mode = "ONLINE"

    if not context_blocks:
        print("Offline mode: Checking local archive...")
        offline_blocks = _get_offline_context(user_query)
        if offline_blocks:
            mode = "OFFLINE_ARCHIVE"
            context_blocks = [
                (url, text[: settings.max_chars_per_source])
                for url, text in offline_blocks
            ]
        else:
            mode = "LOCAL_WEIGHTS"
            context_blocks = [("N/A", "No fresh information found.")]

    system_prompt = (
        "You are a helpful AI with access to real-time web data.\n"
        f"Current Mode: {mode}\n"
        "Instructions: Use the provided context to answer. If the context is empty, "
        "rely on your training data but warn the user.\n"
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
        "temperature": 0.7,
    }

    response = requests.post(
        f"{settings.lm_studio_base_url}/chat/completions",
        json=payload,
        timeout=settings.request_timeout_s,
    )
    response.raise_for_status()
    return response.json()["choices"][0]["message"]["content"]


def run_cli() -> None:
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
