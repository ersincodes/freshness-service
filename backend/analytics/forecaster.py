"""Deterministic baseline forecasts (linear trend) for eligible series."""
from __future__ import annotations

import logging
from typing import Any
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from .forecast_repository import ForecastRepository, PIPELINE_VERSION_FORECAST
from .models import ColumnMetadata
from .profiler import apply_missing_policy

logger = logging.getLogger(__name__)


def forecast_series(series: pd.Series, horizon: int = 3) -> dict[str, Any]:
    """Linear regression trend forecast with simple Gaussian-ish bands from residual std."""
    y_raw = pd.to_numeric(series, errors="coerce").dropna()
    y = y_raw.values.astype(float)
    if len(y) < 2:
        raise ValueError("insufficient_points")
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

    return {
        "horizon": horizon,
        "point": [round(float(p), 4) for p in point],
        "lower": [round(float(x), 4) for x in lower],
        "upper": [round(float(x), 4) for x in upper],
        "model": "linear_trend",
        "backtest_std": round(std, 4),
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
        y = sub[msafe]
        mf = float(y.isna().mean()) if len(y) else 1.0
        y2, pol = apply_missing_policy(y, mf)
        if pol == "ineligible":
            continue
        try:
            result = forecast_series(y2, horizon=3)
        except Exception as exc:
            logger.warning(
                "Forecast skipped for %s %s %s: %s",
                document_id,
                sheet_name,
                mname,
                exc,
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
