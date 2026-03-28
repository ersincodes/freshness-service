"""Compute dataset profiles post-ingestion."""
from __future__ import annotations

import json
import logging
from typing import Any

import pandas as pd

from .models import ColumnMetadata, ColumnProfile, DatasetProfile

logger = logging.getLogger(__name__)

PIPELINE_VERSION_PROFILE = "profile_v2"
PIPELINE_VERSION_TS = "ts_v1"


def profile_dataframe(
    df: pd.DataFrame,
    column_types: dict[str, ColumnMetadata],
) -> DatasetProfile:
    """Build a DatasetProfile from a normalized DataFrame and its column metadata.

    The DataFrame should already contain normalized values (epoch ints for dates,
    0/1 for booleans, etc.) so min/max are computed on the stored representation.
    """
    row_count = len(df)
    columns: dict[str, ColumnProfile] = {}

    for col_name in sorted(column_types.keys()):
        meta = column_types[col_name]
        if col_name.startswith("_"):
            continue
        safe = meta.safe_name
        if safe not in df.columns:
            continue

        series = df[safe]
        total = len(series)
        null_count = int(series.isna().sum())
        null_ratio = null_count / total if total else 0.0
        non_null = series.dropna()
        distinct_count = int(non_null.nunique())
        dtype_str = str(series.dtype)

        min_value: Any = None
        max_value: Any = None
        mean_value: float | None = None
        median_value: float | None = None
        std_value: float | None = None
        top_values: dict[str, int] | None = None

        if meta.logical_type in ("integer", "float", "date") and not non_null.empty:
            try:
                numeric = pd.to_numeric(non_null, errors="coerce").dropna()
                if not numeric.empty:
                    min_value = _to_json_safe(numeric.min())
                    max_value = _to_json_safe(numeric.max())
                    if meta.logical_type in ("integer", "float"):
                        mean_value = round(float(numeric.mean()), 4)
                        median_value = round(float(numeric.median()), 4)
                        std_val = float(numeric.std())
                        std_value = None if pd.isna(std_val) else round(std_val, 4)
            except Exception:
                pass
        else:
            if not non_null.empty:
                vc = non_null.astype(str).value_counts().head(5)
                top_values = {str(k): int(v) for k, v in vc.items()}

        columns[col_name] = ColumnProfile(
            logical_type=meta.logical_type,
            null_ratio=round(null_ratio, 6),
            distinct_count=distinct_count,
            min_value=min_value,
            max_value=max_value,
            dtype=dtype_str,
            missing_count=null_count,
            mean_value=mean_value,
            median_value=median_value,
            std_value=std_value,
            top_values=top_values,
        )

    return DatasetProfile(row_count=row_count, columns=columns)


def _to_json_safe(val: Any) -> int | float | str | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if isinstance(val, float) and val == int(val):
            return int(val)
        return val
    try:
        f = float(val)
        return int(f) if f == int(f) else f
    except (ValueError, TypeError):
        return str(val)


def apply_missing_policy(
    series: pd.Series, missing_fraction: float
) -> tuple[pd.Series, str]:
    if missing_fraction <= 0.1:
        return series.ffill().bfill(), "forward_fill"
    if missing_fraction <= 0.3:
        return series.ffill().bfill(), "forward_fill_warned"
    return series, "ineligible"


def detect_time_column(
    df: pd.DataFrame,
    column_types: dict[str, ColumnMetadata],
) -> str | None:
    """Return original column name of the best time column, if any."""
    for col_name in sorted(column_types.keys()):
        if col_name.startswith("_"):
            continue
        meta = column_types[col_name]
        safe = meta.safe_name
        if safe not in df.columns:
            continue
        s = df[safe]
        if meta.logical_type == "date":
            return col_name
        if pd.api.types.is_datetime64_any_dtype(s):
            return col_name
    for col_name in sorted(column_types.keys()):
        if col_name.startswith("_"):
            continue
        meta = column_types[col_name]
        if meta.logical_type != "string":
            continue
        safe = meta.safe_name
        if safe not in df.columns:
            continue
        s = df[safe]
        try:
            parsed = pd.to_datetime(s, errors="coerce")
            if len(parsed) and parsed.notna().mean() > 0.8:
                return col_name
        except Exception:
            continue
    return None


_ID_LIKE_TOKENS = {"id", "index", "code", "key", "number", "num", "no", "hash"}
_BUSINESS_METRIC_TOKENS = {
    "revenue", "profit", "sales", "cost", "total", "units",
    "amount", "price", "quantity", "income", "margin", "earnings",
}


def _measure_sort_key(
    col_name: str, df: pd.DataFrame, column_types: dict[str, ColumnMetadata]
) -> tuple[int, int, str]:
    """Return a sort key that ranks business metrics first, IDs last.

    Tuple: (id_penalty, -business_bonus, col_name)
    Lower sorts first.
    """
    name_lower = col_name.lower()
    tokens = set(name_lower.replace("_", " ").replace("-", " ").split())

    is_id_like = bool(tokens & _ID_LIKE_TOKENS)
    is_business = bool(tokens & _BUSINESS_METRIC_TOKENS)

    if not is_business:
        safe = column_types[col_name].safe_name
        if safe in df.columns:
            s = df[safe].dropna()
            if len(s) > 0 and int(s.nunique()) == len(s):
                is_id_like = True

    id_penalty = 1 if is_id_like else 0
    business_bonus = 1 if is_business else 0
    return (id_penalty, -business_bonus, col_name)


def detect_measures(
    df: pd.DataFrame,
    column_types: dict[str, ColumnMetadata],
    time_col_original: str,
) -> list[str]:
    """Numeric measure columns (original names), excluding the time column.

    Returned in ranked order: business metrics first, ID-like columns last.
    """
    candidates: list[str] = []
    for col_name in sorted(column_types.keys()):
        if col_name.startswith("_") or col_name == time_col_original:
            continue
        meta = column_types[col_name]
        if meta.logical_type not in ("integer", "float"):
            continue
        safe = meta.safe_name
        if safe not in df.columns:
            continue
        s = df[safe]
        if s.notna().mean() <= 0.5:
            continue
        candidates.append(col_name)
    candidates.sort(key=lambda c: _measure_sort_key(c, df, column_types))
    return candidates


def build_timeseries_record(
    df: pd.DataFrame,
    column_types: dict[str, ColumnMetadata],
) -> tuple[str | None, list[dict[str, Any]], int, str | None]:
    """Compute time-series detection for one sheet (deterministic JSON-friendly rows)."""
    time_col = detect_time_column(df, column_types)
    if time_col is None:
        return None, [], 0, "no_time_column"

    tmeta = column_types[time_col]
    ts = df[tmeta.safe_name]
    time_missing = float(ts.isna().mean()) if len(ts) else 1.0
    if time_missing > 0.3:
        return time_col, [], 0, "time_column_high_missing"

    measures = detect_measures(df, column_types, time_col)
    measure_entries: list[dict[str, Any]] = []
    for m in measures:
        ms = df[column_types[m].safe_name]
        mmf = float(ms.isna().mean())
        _, pol = apply_missing_policy(ms, mmf)
        eligible = pol != "ineligible"
        measure_entries.append(
            {
                "name": m,
                "missing_fraction": round(mmf, 4),
                "policy": pol,
                "eligible": eligible,
            }
        )

    sheet_eligible = 1 if any(x["eligible"] for x in measure_entries) else 0
    reason = None if sheet_eligible else "no_eligible_measure"
    return time_col, measure_entries, sheet_eligible, reason


def measures_json_dumps(measures: list[dict[str, Any]]) -> str:
    return json.dumps(measures, sort_keys=True, separators=(",", ":"))
