from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class AnalyticsRoutingDecision:
    use_analytics: bool
    reason: str


class AnalyticsRouter:
    """Heuristic router: route aggregation and filtered list queries to analytics."""

    # Requires at least one of these plus _METRIC_HINT so generic prose does not hit analytics.
    _METRIC_HINT = re.compile(
        r"\b(revenue|sales?|profit|profitable|margin|quantity|units?|qty|volume|"
        r"category|categories|segment|country|countries|region|regions|channel|"
        r"product|products|order|orders|customer|customers|discount|price|amount|total|"
        r"online|offline|spreadsheet|excel|dataset|tabular|sheet|forecast)\b",
        re.IGNORECASE,
    )

    _BI_CONTEXT_PATTERNS = (
        re.compile(r"\btop\b", re.IGNORECASE),
        re.compile(r"\bmost\b", re.IGNORECASE),
        re.compile(r"\bbest\b", re.IGNORECASE),
        re.compile(r"\bworst\b", re.IGNORECASE),
        re.compile(r"\brank(?:ing|s)?\b", re.IGNORECASE),
        re.compile(r"\btrend\b", re.IGNORECASE),
        re.compile(r"\bover time\b", re.IGNORECASE),
        re.compile(r"\bmonthly\b", re.IGNORECASE),
        re.compile(r"\byearly\b", re.IGNORECASE),
        re.compile(r"\bseason(?:al|ality)?\b", re.IGNORECASE),
        re.compile(r"\bcompare[sd]?\b", re.IGNORECASE),
        re.compile(r"\bvs\.?\b", re.IGNORECASE),
        re.compile(r"\bversus\b", re.IGNORECASE),
        re.compile(r"\bcorrelation\b", re.IGNORECASE),
        re.compile(r"\bperformance\b", re.IGNORECASE),
    )

    _AGG_PATTERNS = (
        re.compile(r"\bhow many\b", re.IGNORECASE),
        re.compile(r"\bcount\b", re.IGNORECASE),
        re.compile(r"\bnumber of\b", re.IGNORECASE),
        re.compile(r"\bdistinct\b", re.IGNORECASE),
        re.compile(r"\bunique\b", re.IGNORECASE),
        re.compile(r"\bbreakdown\b", re.IGNORECASE),
        re.compile(r"\bgroup by\b", re.IGNORECASE),
        re.compile(r"\baverage\b", re.IGNORECASE),
        re.compile(r"\bmean\b", re.IGNORECASE),
        re.compile(r"\bsum\b", re.IGNORECASE),
        re.compile(r"\btotal\b", re.IGNORECASE),
        re.compile(r"\bmin(?:imum)?\b", re.IGNORECASE),
        re.compile(r"\bmax(?:imum)?\b", re.IGNORECASE),
        re.compile(r"\blowest\b", re.IGNORECASE),
        re.compile(r"\bhighest\b", re.IGNORECASE),
    )

    _LIST_PATTERNS = (
        re.compile(r"\blist\b", re.IGNORECASE),
        re.compile(r"\bshow\b", re.IGNORECASE),
        re.compile(r"\bfind\b", re.IGNORECASE),
        re.compile(r"\bget\b", re.IGNORECASE),
        re.compile(r"\bwhat are\b", re.IGNORECASE),
        re.compile(r"\bwho are\b", re.IGNORECASE),
        re.compile(r"\bwho\s+(?:has|had|is|was)\b", re.IGNORECASE),
        re.compile(r"\bwhich\b", re.IGNORECASE),
        re.compile(r"\bfilter\b", re.IGNORECASE),
        re.compile(r"\bfrom\s+\w+\b", re.IGNORECASE),
        re.compile(r"\bwhere\b", re.IGNORECASE),
        re.compile(r"\bcustomers?\s+(?:from|in|with|where)\b", re.IGNORECASE),
        re.compile(r"\b(?:names?|emails?|addresses?)\s+of\b", re.IGNORECASE),
    )

    def decide(self, user_query: str) -> AnalyticsRoutingDecision:
        query = user_query.strip()
        if not query:
            return AnalyticsRoutingDecision(use_analytics=False, reason="empty_query")

        if any(pattern.search(query) for pattern in self._AGG_PATTERNS):
            return AnalyticsRoutingDecision(use_analytics=True, reason="aggregation_intent")

        if any(pattern.search(query) for pattern in self._LIST_PATTERNS):
            return AnalyticsRoutingDecision(use_analytics=True, reason="list_filter_intent")

        if self._METRIC_HINT.search(query) and any(
            pattern.search(query) for pattern in self._BI_CONTEXT_PATTERNS
        ):
            return AnalyticsRoutingDecision(use_analytics=True, reason="bi_metric_intent")

        return AnalyticsRoutingDecision(use_analytics=False, reason="default_rag")
