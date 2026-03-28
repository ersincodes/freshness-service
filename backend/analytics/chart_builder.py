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


def _freq_label(freq: str) -> str:
    return {
        "monthly": "Monthly",
        "quarterly": "Quarterly",
        "yearly": "Yearly",
    }.get(freq, "")


def build_forecast_line_chart(payload: ForecastChatPayload) -> dict[str, Any]:
    """Line chart with historical data, forecast, and shaded confidence band."""
    historical_data: list[dict[str, str | float]] = [
        {"date": h.date, "value": h.value} for h in payload.historical
    ]

    forecast_dates = payload.forecast_dates
    if not forecast_dates:
        forecast_dates = [f"H{i + 1}" for i in range(payload.horizon)]

    forecast_data = [
        {"date": forecast_dates[i], "value": v}
        for i, v in enumerate(payload.point)
    ]
    lower_data = [
        {"date": forecast_dates[i], "value": v}
        for i, v in enumerate(payload.lower)
    ]
    upper_data = [
        {"date": forecast_dates[i], "value": v}
        for i, v in enumerate(payload.upper)
    ]

    bridge_point: list[dict[str, str | float]] = []
    bridge_band: list[dict[str, str | float]] = []
    if historical_data and forecast_data:
        last_hist = historical_data[-1]
        bridge_point = [{"date": str(last_hist["date"]), "value": last_hist["value"]}]
        bridge_band = [{"date": str(last_hist["date"]), "value": last_hist["value"]}]

    freq_label = _freq_label(payload.frequency)
    measure_part = payload.measure
    if payload.filter_label:
        measure_part = f"{payload.filter_label} — {measure_part}"
    title = f"{measure_part} ({payload.sheet})"
    if payload.document:
        title = f"{payload.document} · {title}"

    subtitle = ""
    if freq_label:
        subtitle = f"{freq_label} aggregation · Linear trend forecast"

    forecast_start_label = forecast_dates[0] if forecast_dates else "H1"

    return {
        "type": "line_chart",
        "title": title,
        "subtitle": subtitle,
        "x_label": payload.time_column,
        "y_label": payload.measure,
        "series": [
            {
                "name": "Historical",
                "style": "solid",
                "data": historical_data,
            },
            {
                "name": "Forecast",
                "style": "dashed",
                "data": bridge_point + forecast_data,
                "area_band": {
                    "lower": bridge_band + lower_data,
                    "upper": bridge_band + upper_data,
                },
            },
        ],
        "forecast_start": forecast_start_label,
    }
