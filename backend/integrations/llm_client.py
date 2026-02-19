"""
LLM client for LM Studio integration.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import AsyncIterator, Any

import requests


@dataclass(frozen=True)
class LLMResponse:
    """Response from a non-streaming LLM call."""
    content: str
    model: str


@dataclass(frozen=True)
class LLMStreamChunk:
    """A chunk from a streaming LLM response."""
    content: str
    is_done: bool = False


class LLMClientError(Exception):
    pass


class LLMClient:
    """Async client for LM Studio LLM service."""
    
    def __init__(self, base_url: str, model_name: str, timeout_seconds: int = 30) -> None:
        self._base_url = base_url.rstrip("/")
        self._model_name = model_name
        self._timeout = timeout_seconds
    
    def _post(self, payload: dict) -> str:
        resp = requests.post(f"{self._base_url}/chat/completions", json=payload, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    
    async def complete(self, system_prompt: str, user_message: str, temperature: float = 0.2) -> LLMResponse:
        """Send a non-streaming chat completion request."""
        payload = {
            "model": self._model_name,
            "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_message}],
            "temperature": temperature,
        }
        content = await asyncio.to_thread(self._post, payload)
        return LLMResponse(content=content, model=self._model_name)
    
    async def extract_json(self, system_prompt: str, user_message: str) -> dict[str, Any] | None:
        """Send request expecting JSON response."""
        resp = await self.complete(system_prompt, user_message, temperature=0.0)
        raw = resp.content
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            start, end = raw.find("{"), raw.rfind("}")
            if start != -1 and end > start:
                try:
                    return json.loads(raw[start:end + 1])
                except json.JSONDecodeError:
                    pass
        return None
    
    async def stream(self, system_prompt: str, user_message: str, temperature: float = 0.2) -> AsyncIterator[LLMStreamChunk]:
        """Stream chat completion."""
        payload = {
            "model": self._model_name,
            "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_message}],
            "temperature": temperature,
            "stream": True,
        }
        resp = await asyncio.to_thread(lambda: requests.post(f"{self._base_url}/chat/completions", json=payload, timeout=self._timeout, stream=True))
        resp.raise_for_status()
        
        for line in resp.iter_lines():
            if line:
                line_str = line.decode("utf-8")
                if line_str.startswith("data: "):
                    data_str = line_str[6:]
                    if data_str == "[DONE]":
                        yield LLMStreamChunk(content="", is_done=True)
                        break
                    try:
                        data = json.loads(data_str)
                        content = data.get("choices", [{}])[0].get("delta", {}).get("content", "")
                        if content:
                            yield LLMStreamChunk(content=content)
                    except json.JSONDecodeError:
                        continue
    
    async def check_health(self) -> tuple[bool, str, int | None]:
        """Check if LLM service is healthy."""
        try:
            start = time.perf_counter()
            resp = await asyncio.to_thread(lambda: requests.get(f"{self._base_url}/models", timeout=5))
            latency_ms = int((time.perf_counter() - start) * 1000)
            if resp.status_code == 200:
                return True, "LM Studio is reachable", latency_ms
            return False, f"LM Studio returned status {resp.status_code}", None
        except requests.exceptions.ConnectionError:
            return False, f"Cannot connect to LM Studio at {self._base_url}", None
        except Exception as e:
            return False, str(e), None
