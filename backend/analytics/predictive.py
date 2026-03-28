"""Keyword-based predictive intent and query-aware forecast resolution."""
from __future__ import annotations

import re
from collections.abc import Callable

from .forecast_repository import ForecastArtifactRow
from .models import (
    DatasetProfile,
    ForecastChatPayload,
    ForecastUnavailable,
    HistoricalPoint,
)

PREDICTIVE_KEYWORDS = (
    "predict",
    "forecast",
    "next month",
    "next quarter",
    "next year",
    "future",
    "projection",
    "expect",
)

_MEASURE_SYNONYMS: dict[str, list[str]] = {
    "revenue": ["revenue", "sales", "income", "earnings"],
    "profit": ["profit", "margin", "earnings", "net"],
    "cost": ["cost", "expense", "spending"],
    "units": ["units", "quantity", "volume", "count", "sold"],
    "price": ["price", "rate", "unit price"],
}


def is_predictive_intent(query: str) -> bool:
    q = query.lower()
    if "will" in q and any(w in q for w in ("sales", "revenue", "grow", "trend", "value")):
        return True
    return any(kw in q for kw in PREDICTIVE_KEYWORDS)


def _score_measure_against_query(measure_column: str, query: str) -> float:
    """Score how well a measure column name matches the user query.

    Higher is better. Returns 0.0 for no match.
    """
    q_lower = query.lower()
    m_lower = measure_column.lower()
    m_tokens = set(re.split(r"[\s_\-]+", m_lower))
    q_tokens = set(re.split(r"[\s_\-]+", q_lower))

    score = 0.0

    direct_overlap = m_tokens & q_tokens
    score += len(direct_overlap) * 3.0

    for canonical, synonyms in _MEASURE_SYNONYMS.items():
        query_has = any(s in q_lower for s in synonyms)
        measure_has = any(s in m_lower for s in synonyms)
        if query_has and measure_has:
            score += 2.0

    if any(tok in m_lower for tok in ("total", "sum")):
        score += 0.5

    return score


def resolve_forecast_for_chat(
    forecast_rows: list[ForecastArtifactRow],
    *,
    get_filename: Callable[[str], str | None] | None = None,
    user_query: str = "",
) -> ForecastChatPayload | ForecastUnavailable:
    if not forecast_rows:
        return ForecastUnavailable(reason="no_forecast_available")

    if user_query:
        scored = [
            (row, _score_measure_against_query(row.measure_column, user_query))
            for row in forecast_rows
        ]
        scored.sort(key=lambda x: -x[1])
        row = scored[0][0]
    else:
        row = forecast_rows[0]

    fc = row.forecast
    filename = None
    if get_filename:
        try:
            filename = get_filename(row.document_id)
        except Exception:
            filename = None

    raw_historical = fc.get("historical", [])
    historical = [
        HistoricalPoint(date=str(h["date"]), value=float(h["value"]))
        for h in raw_historical
        if isinstance(h, dict) and "date" in h and "value" in h
    ]

    return ForecastChatPayload(
        document=filename,
        document_id=row.document_id,
        sheet=row.sheet_name,
        measure=row.measure_column,
        time_column=row.time_column,
        horizon=int(fc.get("horizon", 0)),
        point=[float(x) for x in fc.get("point", [])],
        lower=[float(x) for x in fc.get("lower", [])],
        upper=[float(x) for x in fc.get("upper", [])],
        model=str(fc.get("model", "linear_trend")),
        frequency=str(fc.get("frequency", "unknown")),
        historical=historical,
        forecast_dates=[str(d) for d in fc.get("forecast_dates", [])],
    )


# ------------------------------------------------------------------
# Filter-intent detection for on-demand forecasting
# ------------------------------------------------------------------

_NOISE_TOKENS = {
    "forecast", "predict", "next", "month", "months", "quarter", "year",
    "years", "future", "projection", "expect", "will", "for", "the",
    "what", "how", "much", "many", "of", "in", "by", "and", "or",
    "a", "an", "to", "is", "are", "be", "do", "does", "can", "could",
    "my", "me", "this", "that", "it", "its", "about", "from", "with",
    "sales", "revenue", "profit", "cost", "units", "price", "income",
    "earnings", "margin", "quantity", "volume", "total", "sum", "average",
    "growth", "trend", "value", "amount", "spending", "expense",
    "3", "6", "12",
}


def query_has_filter_intent(
    query: str,
    profile: DatasetProfile | None,
) -> bool:
    """Detect whether the user query references specific categorical values.

    Compares non-noise query tokens against known ``top_values`` of string
    columns in the dataset profile.  Returns True if any match is found,
    signalling that the forecast should be computed on a filtered slice
    rather than the whole sheet.
    """
    if profile is None:
        return False

    q_lower = query.lower()
    q_tokens = set(re.split(r"[\s,;:!?\"'()]+", q_lower)) - {""}

    content_tokens = q_tokens - _NOISE_TOKENS

    if not content_tokens:
        return False

    for col_name, col_profile in profile.columns.items():
        if col_profile.logical_type != "string":
            continue
        if col_profile.top_values is None:
            continue
        known_values = {v.lower() for v in col_profile.top_values}
        for token in content_tokens:
            for known in known_values:
                if token in known or known in token:
                    return True

    return False
