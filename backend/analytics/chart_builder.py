"""Structured chart specs for chat responses."""
from __future__ import annotations

from typing import Any

from .models import ForecastChatPayload


def select_chart_type(user_query: str) -> str:
    q = user_query.lower()
    if any(k in q for k in ("trend", "over time", "forecast", "predict")):
        return "line_chart"
    if any(k in q for k in ("compare", "distribution", "by category")):
        return "bar_chart"
    return "line_chart"


def build_forecast_line_chart(payload: ForecastChatPayload) -> dict[str, Any]:
    """Line chart with forecast horizon (no in-chat historical series yet)."""
    historical: list[dict[str, str | float]] = []
    forecast_data = [
        {"date": f"H{i + 1}", "value": v} for i, v in enumerate(payload.point)
    ]
    lower_data = [
        {"date": f"H{i + 1}", "value": v} for i, v in enumerate(payload.lower)
    ]
    upper_data = [
        {"date": f"H{i + 1}", "value": v} for i, v in enumerate(payload.upper)
    ]
    title = f"{payload.measure} ({payload.sheet})"
    if payload.document:
        title = f"{payload.document} · {title}"
    return {
        "type": "line_chart",
        "title": title,
        "x_label": "Horizon step",
        "y_label": payload.measure,
        "series": [
            {"name": "Historical", "style": "solid", "data": historical},
            {
                "name": "Forecast",
                "style": "dashed",
                "data": forecast_data,
                "confidence_band": {
                    "lower": lower_data,
                    "upper": upper_data,
                },
            },
        ],
        "forecast_start": "H1",
    }
