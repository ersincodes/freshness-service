"""
Brave Search API client.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import requests


@dataclass(frozen=True)
class BraveSearchResult:
    """A single search result from Brave."""
    url: str
    title: str
    description: str
    
    @property
    def snippet(self) -> str:
        parts = [p for p in [self.title.strip(), self.description.strip()] if p]
        return "\n".join(parts)


class BraveClientError(Exception):
    pass


class BraveClient:
    """Async client for Brave Search API."""
    
    SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"
    
    def __init__(self, api_key: str | None, timeout_seconds: int = 10, max_results: int = 3) -> None:
        self._api_key = api_key
        self._timeout = timeout_seconds
        self._max_results = max_results
    
    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)
    
    def _headers(self) -> dict[str, str]:
        return {"Accept": "application/json", "X-Subscription-Token": self._api_key or ""}
    
    async def search(self, query: str, count: int | None = None) -> list[BraveSearchResult]:
        """Perform a web search."""
        if not self.is_configured:
            return []
        
        def _search():
            resp = requests.get(self.SEARCH_URL, headers=self._headers(), params={"q": query, "count": count or self._max_results}, timeout=self._timeout)
            resp.raise_for_status()
            return resp.json().get("web", {}).get("results", [])
        
        raw = await asyncio.to_thread(_search)
        return [BraveSearchResult(r.get("url", ""), r.get("title", ""), r.get("description", "")) for r in raw if r.get("url")]
    
    async def check_health(self) -> tuple[bool, str, int | None]:
        """Check if Brave Search API is healthy."""
        if not self.is_configured:
            return False, "Brave API key not configured", None
        try:
            start = time.perf_counter()
            resp = await asyncio.to_thread(lambda: requests.get(self.SEARCH_URL, headers=self._headers(), params={"q": "test", "count": 1}, timeout=5))
            latency_ms = int((time.perf_counter() - start) * 1000)
            if resp.status_code == 200:
                return True, "Brave Search is reachable", latency_ms
            elif resp.status_code == 401:
                return False, "Brave API key is invalid", None
            return False, f"Brave Search returned status {resp.status_code}", None
        except Exception as e:
            return False, str(e), None
