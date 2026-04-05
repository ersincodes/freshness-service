"""Tests for multi-period forecast horizon (next N months) and trim safety net."""
from __future__ import annotations

import sqlite3

import pandas as pd
import pytest

from backend.analytics.forecaster import _trim_forecast_window, compute_filtered_forecast
from backend.analytics.models import ColumnMetadata, ForecastPlan
from backend.services.chat.analytics_planning import (
    infer_forecast_horizon_from_query,
    repair_forecast_plan_horizon,
)


def _monthly_sales_conn() -> tuple[sqlite3.Connection, dict[str, ColumnMetadata]]:
    """In-memory DB: monthly revenue Jan 2024 – Dec 2025 (24 points)."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE sheet (c_date INTEGER, c_rev REAL)")
    rows: list[tuple[int, float]] = []
    for i, ts in enumerate(pd.date_range("2024-01-01", periods=24, freq="MS")):
        sec = int(ts.timestamp())
        rows.append((sec, float(10_000 + i * 100)))
    conn.executemany("INSERT INTO sheet VALUES (?, ?)", rows)
    conn.commit()
    meta = {
        "OrderDate": ColumnMetadata(
            column_name="OrderDate",
            logical_type="date",
            sqlite_type="INTEGER",
            nullable=False,
            original_name="OrderDate",
            safe_name="c_date",
        ),
        "Revenue": ColumnMetadata(
            column_name="Revenue",
            logical_type="float",
            sqlite_type="REAL",
            nullable=False,
            original_name="Revenue",
            safe_name="c_rev",
        ),
    }
    return conn, meta


def test_trim_monthly_labels_narrow_requested_end_keeps_only_first_month() -> None:
    """Document trim behavior: end of January excludes Feb/Mar period starts."""
    pts = [1.0, 2.0, 3.0]
    lo = [0.5, 1.5, 2.5]
    hi = [1.5, 2.5, 3.5]
    dates = ["Jan 2026", "Feb 2026", "Mar 2026"]
    out = _trim_forecast_window(pts, lo, hi, dates, None, "2026-01-31")
    assert out[0] == [1.0] and out[3] == ["Jan 2026"]


def test_compute_filtered_forecast_restores_full_horizon_after_over_trim() -> None:
    """Narrow requested_end must not drop below plan.horizon future points."""
    conn, meta = _monthly_sales_conn()
    try:
        plan = ForecastPlan(
            document_id="doc",
            sheet_name="S",
            measure_column="Revenue",
            time_column="OrderDate",
            horizon=3,
            requested_end="2026-01-31",
        )
        payload = compute_filtered_forecast(
            conn,
            "sheet",
            meta,
            plan,
            filename="test.xlsx",
            sheet_name="S",
        )
        assert payload.horizon == 3
        assert payload.forecast_dates == ["Jan 2026", "Feb 2026", "Mar 2026"]
        assert len(payload.point) == 3
    finally:
        conn.close()


@pytest.mark.parametrize(
    "q,expected",
    [
        ("What will sales look like next 3 months?", 3),
        ("the next 12 months", 12),
        ("Next three months outlook", 3),
        ("forecast for the next 1 month", 1),
    ],
)
def test_infer_forecast_horizon_from_query(q: str, expected: int) -> None:
    assert infer_forecast_horizon_from_query(q) == expected


def test_repair_forecast_plan_horizon_raises_and_clears_dates() -> None:
    plan = ForecastPlan(
        document_id="d",
        measure_column="Revenue",
        time_column="OrderDate",
        horizon=1,
        requested_start="2026-01-01",
        requested_end="2026-01-31",
    )
    repaired = repair_forecast_plan_horizon(
        plan, "What will sales look like next 3 months?"
    )
    assert repaired.horizon == 3
    assert repaired.requested_start is None
    assert repaired.requested_end is None


def test_repair_forecast_plan_horizon_no_change_without_month_phrase() -> None:
    plan = ForecastPlan(
        document_id="d",
        measure_column="Revenue",
        horizon=2,
        requested_end="2026-06-30",
    )
    repaired = repair_forecast_plan_horizon(plan, "predict revenue for 2026")
    assert repaired.horizon == 2
    assert repaired.requested_end == "2026-06-30"
