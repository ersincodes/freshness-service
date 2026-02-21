from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class AnalyticsRoutingDecision:
    use_analytics: bool
    reason: str


class AnalyticsRouter:
    """Heuristic router: route only obvious aggregation questions to analytics."""

    _AGG_PATTERNS = (
        re.compile(r"\bhow many\b", re.IGNORECASE),
        re.compile(r"\bcount\b", re.IGNORECASE),
        re.compile(r"\bnumber of\b", re.IGNORECASE),
        re.compile(r"\bdistinct\b", re.IGNORECASE),
        re.compile(r"\bunique\b", re.IGNORECASE),
        re.compile(r"\bbreakdown\b", re.IGNORECASE),
        re.compile(r"\bgroup by\b", re.IGNORECASE),
    )

    def decide(self, user_query: str) -> AnalyticsRoutingDecision:
        query = user_query.strip()
        if not query:
            return AnalyticsRoutingDecision(use_analytics=False, reason="empty_query")

        if any(pattern.search(query) for pattern in self._AGG_PATTERNS):
            return AnalyticsRoutingDecision(use_analytics=True, reason="aggregation_intent")

        return AnalyticsRoutingDecision(use_analytics=False, reason="default_rag")
