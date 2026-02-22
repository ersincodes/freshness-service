"""Compute dataset profiles post-ingestion."""
from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from .models import ColumnMetadata, ColumnProfile, DatasetProfile

logger = logging.getLogger(__name__)


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

    for col_name, meta in column_types.items():
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

        min_value: Any = None
        max_value: Any = None

        if meta.logical_type in ("integer", "float", "date") and not non_null.empty:
            try:
                numeric = pd.to_numeric(non_null, errors="coerce").dropna()
                if not numeric.empty:
                    min_value = _to_json_safe(numeric.min())
                    max_value = _to_json_safe(numeric.max())
            except Exception:
                pass

        columns[col_name] = ColumnProfile(
            logical_type=meta.logical_type,
            null_ratio=round(null_ratio, 6),
            distinct_count=distinct_count,
            min_value=min_value,
            max_value=max_value,
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
