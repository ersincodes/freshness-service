"""Keyword-based predictive intent and forecast resolution."""
from __future__ import annotations

from collections.abc import Callable

from .forecast_repository import ForecastArtifactRow
from .models import ForecastChatPayload, ForecastUnavailable

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


def is_predictive_intent(query: str) -> bool:
    q = query.lower()
    if "will" in q and any(w in q for w in ("sales", "revenue", "grow", "trend", "value")):
        return True
    return any(kw in q for kw in PREDICTIVE_KEYWORDS)


def resolve_forecast_for_chat(
    forecast_rows: list[ForecastArtifactRow],
    *,
    get_filename: Callable[[str], str | None] | None = None,
) -> ForecastChatPayload | ForecastUnavailable:
    if not forecast_rows:
        return ForecastUnavailable(reason="no_forecast_available")
    row = forecast_rows[0]
    fc = row.forecast
    filename = None
    if get_filename:
        try:
            filename = get_filename(row.document_id)
        except Exception:
            filename = None
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
    )
