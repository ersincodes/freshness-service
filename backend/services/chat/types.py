"""Shared datatypes for the chat pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...domain import SourceContext


@dataclass(frozen=True)
class ChatResult:
    answer: str
    mode: str
    contexts: list[SourceContext]
    attached_sources: list[dict[str, Any]] | None = None
    forecast: dict[str, Any] | None = None
    chart: dict[str, Any] | None = None


@dataclass(frozen=True)
class StreamEvent:
    event_type: str
    data: dict[str, Any]
