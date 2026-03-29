"""Deterministic baseline forecasts (linear trend) with temporal aggregation."""
from __future__ import annotations

import logging
import sqlite3
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from .forecast_repository import ForecastRepository, PIPELINE_VERSION_FORECAST
from .models import ColumnMetadata, ForecastChatPayload, ForecastPlan, HistoricalPoint
from .profiler import apply_missing_policy
from .sql_compiler import _compile_where

logger = logging.getLogger(__name__)

_FREQ_THRESHOLDS = {
    "monthly": (25, 35),
    "quarterly": (80, 100),
    "yearly": (330, 400),
}


def _detect_frequency(dt_index: pd.DatetimeIndex) -> str:
    """Heuristic frequency detection from median gap between sorted timestamps."""
    if len(dt_index) < 2:
        return "unknown"
    gaps = dt_index.to_series().diff().dropna()
    median_days = gaps.dt.days.median()
    for freq, (lo, hi) in _FREQ_THRESHOLDS.items():
        if lo <= median_days <= hi:
            return freq
    if median_days < 25:
        return "monthly"
    if median_days < 100:
        return "quarterly"
    return "yearly"


def _pick_agg_rule(freq: str) -> str:
    """Map detected frequency to pandas resample rule."""
    return {"monthly": "MS", "quarterly": "QS", "yearly": "YS"}.get(freq, "MS")


def _format_date_label(ts: pd.Timestamp, freq: str) -> str:
    if freq == "yearly":
        return ts.strftime("%Y")
    if freq == "quarterly":
        return f"Q{(ts.month - 1) // 3 + 1} {ts.year}"
    return ts.strftime("%b %Y")


def _parse_time_column(time_series: pd.Series) -> pd.Series:
    """Convert a time column to datetime, handling epoch-second integers.

    During ingestion, date columns are stored as epoch seconds (INTEGER) in
    SQLite.  ``pd.to_datetime`` on bare integers treats them as *nanoseconds*
    which maps everything to ~1970.  Detect this and use ``unit='s'``.
    """
    numeric = pd.to_numeric(time_series, errors="coerce")
    non_null = numeric.dropna()
    if (
        len(non_null) > 0
        and non_null.dtype.kind in ("i", "f")
        and (non_null.abs() > 1e8).all()
        and (non_null.abs() < 1e11).all()
    ):
        return pd.to_datetime(numeric, unit="s", errors="coerce", utc=True)
    return pd.to_datetime(time_series, errors="coerce")


def prepare_timeseries(
    time_series: pd.Series,
    value_series: pd.Series,
) -> tuple[pd.Series, str]:
    """Parse dates, aggregate by detected frequency, return (aggregated_series, freq).

    The returned series has a DatetimeIndex and summed values per period.
    """
    dt_parsed = _parse_time_column(time_series)
    mask = dt_parsed.notna() & value_series.notna()
    dt_clean = dt_parsed[mask]
    val_clean = pd.to_numeric(value_series[mask], errors="coerce").dropna()
    idx = dt_clean.loc[val_clean.index]

    combined = pd.DataFrame({"dt": idx.values, "val": val_clean.values})
    combined = combined.sort_values("dt")
    combined = combined.set_index("dt")
    combined.index = pd.DatetimeIndex(combined.index)
    if combined.index.tz is not None:
        combined.index = combined.index.tz_localize(None)

    raw_freq = _detect_frequency(combined.index)

    rule = _pick_agg_rule(raw_freq)
    aggregated = combined["val"].resample(rule).sum()
    aggregated = aggregated[aggregated != 0]

    if len(aggregated) < 3:
        aggregated = combined["val"]
        raw_freq = "raw"

    return aggregated, raw_freq


def forecast_series(
    series: pd.Series,
    horizon: int = 3,
    frequency: str = "unknown",
) -> dict[str, Any]:
    """Linear regression trend forecast with Gaussian bands.

    Accepts a series with DatetimeIndex (from prepare_timeseries) or plain
    numeric index (legacy path). Returns historical points, forecast points
    with date labels, and confidence bands.
    """
    y_raw = pd.to_numeric(series, errors="coerce").dropna()
    y = y_raw.values.astype(float)
    if len(y) < 2:
        raise ValueError("insufficient_points")

    has_dt_index = isinstance(y_raw.index, pd.DatetimeIndex)

    x = np.arange(len(y), dtype=float).reshape(-1, 1)
    model = LinearRegression()
    model.fit(x, y)
    fitted = model.predict(x)
    residuals = y - fitted
    std = float(np.std(residuals)) if len(residuals) else 0.0

    future_x = np.arange(len(y), len(y) + horizon, dtype=float).reshape(-1, 1)
    point = model.predict(future_x).tolist()
    lower = [float(p - 1.96 * std) for p in point]
    upper = [float(p + 1.96 * std) for p in point]

    historical: list[dict[str, Any]] = []
    forecast_dates: list[str] = []

    if has_dt_index:
        dt_idx = y_raw.index
        for ts, val in zip(dt_idx, y):
            historical.append({
                "date": _format_date_label(ts, frequency),
                "value": round(float(val), 2),
            })

        last_ts = dt_idx[-1]
        freq_map = {"monthly": "MS", "quarterly": "QS", "yearly": "YS"}
        rule = freq_map.get(frequency, "MS")
        future_dates = pd.date_range(
            start=last_ts, periods=horizon + 1, freq=rule
        )[1:]
        forecast_dates = [_format_date_label(d, frequency) for d in future_dates]
    else:
        for i, val in enumerate(y):
            historical.append({"date": str(i), "value": round(float(val), 2)})
        forecast_dates = [f"H{i + 1}" for i in range(horizon)]

    return {
        "horizon": horizon,
        "point": [round(float(p), 4) for p in point],
        "lower": [round(float(x), 4) for x in lower],
        "upper": [round(float(x), 4) for x in upper],
        "model": "linear_trend",
        "backtest_std": round(std, 4),
        "frequency": frequency,
        "historical": historical,
        "forecast_dates": forecast_dates,
    }


def generate_sheet_forecasts(
    df: pd.DataFrame,
    column_types: dict[str, ColumnMetadata],
    document_id: str,
    sheet_name: str,
    time_col_original: str,
    measure_rows: list[dict[str, Any]],
    repo: ForecastRepository,
) -> None:
    """Persist one artifact per eligible measure on a sheet."""
    tmeta = column_types[time_col_original]
    tsafe = tmeta.safe_name
    for row in measure_rows:
        if not row.get("eligible"):
            continue
        mname = row["name"]
        if mname not in column_types:
            continue
        msafe = column_types[mname].safe_name
        if tsafe not in df.columns or msafe not in df.columns:
            continue
        sub = df[[tsafe, msafe]].copy()
        sub = sub.sort_values(tsafe)

        try:
            agg_series, freq = prepare_timeseries(sub[tsafe], sub[msafe])
        except Exception as exc:
            logger.warning(
                "Time-series preparation skipped for %s %s %s: %s",
                document_id, sheet_name, mname, exc,
            )
            continue

        if freq == "raw":
            y = agg_series
            mf = float(y.isna().mean()) if len(y) else 1.0
            y2, pol = apply_missing_policy(y, mf)
            if pol == "ineligible":
                continue
        else:
            y2 = agg_series

        try:
            result = forecast_series(y2, horizon=3, frequency=freq)
        except Exception as exc:
            logger.warning(
                "Forecast skipped for %s %s %s: %s",
                document_id, sheet_name, mname, exc,
            )
            continue
        repo.save_artifact(
            document_id=document_id,
            sheet_name=sheet_name,
            measure_column=mname,
            time_column=time_col_original,
            forecast=result,
            pipeline_version=PIPELINE_VERSION_FORECAST,
        )


# ------------------------------------------------------------------
# On-demand filtered forecast
# ------------------------------------------------------------------


def compute_filtered_forecast(
    conn: sqlite3.Connection,
    table_name: str,
    column_metadata: dict[str, ColumnMetadata],
    plan: ForecastPlan,
    *,
    filename: str | None = None,
    sheet_name: str = "",
) -> ForecastChatPayload:
    """Run prepare_timeseries + forecast_series on a dynamically filtered slice.

    Uses the existing SQL compiler to translate ``plan.filters`` into a WHERE
    clause, fetches the time + measure columns from SQLite, and returns a
    fully-formed ``ForecastChatPayload``.

    Supports ``requested_start`` / ``requested_end`` on the plan to trim
    forecast output to a specific date window.
    """
    original_to_safe = {
        m.original_name: m.safe_name for m in column_metadata.values()
    }

    time_col = plan.time_column
    if time_col is None:
        for col_name, meta in column_metadata.items():
            if meta.logical_type == "date" and not col_name.startswith("_"):
                time_col = col_name
                break
    if time_col is None:
        raise ValueError("No time column identified for forecast")

    measure_col = plan.measure_column
    if measure_col not in column_metadata:
        raise ValueError(f"Unknown measure column: {measure_col}")

    t_safe = original_to_safe[time_col]
    m_safe = original_to_safe[measure_col]

    where_sql, params = _compile_where(
        plan.filters, column_metadata, original_to_safe
    )

    sql = f"SELECT {t_safe}, {m_safe} FROM {table_name} {where_sql} ORDER BY {t_safe};"
    cursor = conn.execute(sql, tuple(params))
    rows = cursor.fetchall()

    if not rows:
        raise ValueError("No data after applying filters")

    time_values = pd.Series([r[0] for r in rows])
    measure_values = pd.Series([r[1] for r in rows])

    agg_series, freq = prepare_timeseries(time_values, measure_values)

    if freq == "raw":
        mf = float(agg_series.isna().mean()) if len(agg_series) else 1.0
        agg_series, pol = apply_missing_policy(agg_series, mf)
        if pol == "ineligible":
            raise ValueError("Too many missing values after filtering")

    result = forecast_series(agg_series, horizon=plan.horizon, frequency=freq)

    forecast_points = [float(x) for x in result.get("point", [])]
    forecast_lower = [float(x) for x in result.get("lower", [])]
    forecast_upper = [float(x) for x in result.get("upper", [])]
    forecast_dates = [str(d) for d in result.get("forecast_dates", [])]

    if plan.requested_start or plan.requested_end:
        forecast_points, forecast_lower, forecast_upper, forecast_dates = (
            _trim_forecast_window(
                forecast_points, forecast_lower, forecast_upper,
                forecast_dates, plan.requested_start, plan.requested_end,
            )
        )

    historical = [
        HistoricalPoint(date=str(h["date"]), value=float(h["value"]))
        for h in result.get("historical", [])
        if isinstance(h, dict) and "date" in h and "value" in h
    ]

    return ForecastChatPayload(
        document=filename,
        document_id=plan.document_id,
        sheet=sheet_name,
        measure=measure_col,
        time_column=time_col,
        horizon=len(forecast_points),
        point=forecast_points,
        lower=forecast_lower,
        upper=forecast_upper,
        model=str(result.get("model", "linear_trend")),
        frequency=str(result.get("frequency", "unknown")),
        historical=historical,
        forecast_dates=forecast_dates,
        filter_label=plan.filter_label,
    )


def _trim_forecast_window(
    points: list[float],
    lower: list[float],
    upper: list[float],
    dates: list[str],
    requested_start: str | None,
    requested_end: str | None,
) -> tuple[list[float], list[float], list[float], list[str]]:
    """Trim forecast arrays to only include the requested date window.

    Date labels are strings like 'Jan 2026', 'Feb 2026'. We parse them
    back to comparable values for filtering.
    """
    if not dates or (not requested_start and not requested_end):
        return points, lower, upper, dates

    from datetime import datetime

    def _parse_label(label: str) -> datetime | None:
        for fmt in ("%b %Y", "%Y", "Q%q %Y", "%Y-%m"):
            try:
                return datetime.strptime(label, fmt)
            except ValueError:
                continue
        try:
            return pd.to_datetime(label).to_pydatetime()
        except Exception:
            return None

    start_dt = None
    end_dt = None
    if requested_start:
        try:
            start_dt = datetime.strptime(requested_start[:10], "%Y-%m-%d")
        except ValueError:
            pass
    if requested_end:
        try:
            end_dt = datetime.strptime(requested_end[:10], "%Y-%m-%d")
        except ValueError:
            pass

    if start_dt is None and end_dt is None:
        return points, lower, upper, dates

    indices: list[int] = []
    for i, label in enumerate(dates):
        dt = _parse_label(label)
        if dt is None:
            indices.append(i)
            continue
        if start_dt and dt < start_dt:
            continue
        if end_dt and dt > end_dt:
            continue
        indices.append(i)

    if not indices:
        return points, lower, upper, dates

    return (
        [points[i] for i in indices if i < len(points)],
        [lower[i] for i in indices if i < len(lower)],
        [upper[i] for i in indices if i < len(upper)],
        [dates[i] for i in indices],
    )


def sanity_check_forecast(
    historical_values: list[float],
    forecast_points: list[float],
) -> list[str]:
    """Check forecast values against historical data for reasonableness.

    Returns a list of warning strings (empty if all checks pass).
    """
    warnings: list[str] = []
    if not historical_values or not forecast_points:
        return warnings

    hist_arr = np.array(historical_values)
    hist_max = float(hist_arr.max())
    hist_min = float(hist_arr.min())
    hist_mean = float(hist_arr.mean())
    all_positive = bool((hist_arr >= 0).all())

    for i, val in enumerate(forecast_points):
        if all_positive and val < 0:
            warnings.append(
                f"Forecast point {i+1} is negative ({val:.2f}) but all "
                f"historical values are non-negative."
            )
        if hist_max > 0 and abs(val) > 3 * hist_max:
            warnings.append(
                f"Forecast point {i+1} ({val:.2f}) exceeds 3x the "
                f"historical maximum ({hist_max:.2f})."
            )

    if warnings:
        warnings.insert(0, "Low confidence: forecast values may be unreliable.")

    return warnings
