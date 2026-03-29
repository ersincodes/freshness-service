"""Unified dataset context for downstream consumers.

Builds a single DatasetSummary from metadata + profile that the
QueryDecomposer, forecaster, and validator all consume. Avoids
scattering metadata lookups across the codebase.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .metadata_repository import MetadataRepository
from .models import ColumnMetadata, ColumnProfile, DatasetProfile

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetSummary:
    """Everything downstream needs to know about an ingested dataset."""

    document_id: str
    sheet_name: str
    table_name: str
    row_count: int
    columns: dict[str, ColumnMetadata]
    profile: DatasetProfile
    time_column: str | None = None
    date_range: tuple[str, str] | None = None
    frequency: str = "unknown"
    eligible_measures: list[str] = field(default_factory=list)
    categorical_values: dict[str, list[str]] = field(default_factory=dict)


def build_dataset_summary(
    meta_repo: MetadataRepository,
    conn: sqlite3.Connection,
    document_id: str,
    sheet_name: str | None = None,
) -> DatasetSummary | None:
    """Assemble a DatasetSummary from persisted metadata.

    Returns None if the document/sheet has no analytics metadata.
    """
    resolved_sheet = sheet_name or meta_repo.resolve_default_sheet_name(document_id)
    if resolved_sheet is None:
        return None

    table_name = meta_repo.get_table_name(document_id, resolved_sheet)
    if table_name is None:
        return None

    columns = meta_repo.get_columns(document_id, resolved_sheet)
    if not columns:
        return None

    profile = meta_repo.get_profile(document_id, resolved_sheet)
    if profile is None:
        return None

    time_column = meta_repo.get_timeseries_time_column(document_id, resolved_sheet)

    date_range = _compute_date_range(conn, table_name, columns, time_column)
    frequency = _detect_frequency_from_profile(date_range, profile.row_count)
    eligible_measures = _extract_eligible_measures(columns, profile)
    categorical_values = _extract_categorical_values(profile)

    return DatasetSummary(
        document_id=document_id,
        sheet_name=resolved_sheet,
        table_name=table_name,
        row_count=profile.row_count,
        columns=columns,
        profile=profile,
        time_column=time_column,
        date_range=date_range,
        frequency=frequency,
        eligible_measures=eligible_measures,
        categorical_values=categorical_values,
    )


def _compute_date_range(
    conn: sqlite3.Connection,
    table_name: str,
    columns: dict[str, ColumnMetadata],
    time_column: str | None,
) -> tuple[str, str] | None:
    """Query min/max of the time column, return as ISO date strings."""
    if time_column is None:
        return None

    meta = columns.get(time_column)
    if meta is None or meta.logical_type != "date":
        return None

    safe_col = meta.safe_name
    try:
        cur = conn.execute(
            f"SELECT MIN({safe_col}), MAX({safe_col}) FROM {table_name} "
            f"WHERE {safe_col} IS NOT NULL;"
        )
        row = cur.fetchone()
        if row is None or row[0] is None or row[1] is None:
            return None

        min_epoch, max_epoch = int(row[0]), int(row[1])
        min_date = datetime.fromtimestamp(min_epoch, tz=timezone.utc).strftime("%Y-%m-%d")
        max_date = datetime.fromtimestamp(max_epoch, tz=timezone.utc).strftime("%Y-%m-%d")
        return (min_date, max_date)
    except Exception as exc:
        logger.warning("Failed to compute date range for %s: %s", table_name, exc)
        return None


def _detect_frequency_from_profile(
    date_range: tuple[str, str] | None,
    row_count: int,
) -> str:
    """Heuristic frequency from date span and row count."""
    if date_range is None or row_count < 2:
        return "unknown"

    try:
        from datetime import datetime as _dt
        start = _dt.strptime(date_range[0], "%Y-%m-%d")
        end = _dt.strptime(date_range[1], "%Y-%m-%d")
        span_days = (end - start).days
        if span_days <= 0:
            return "unknown"
        avg_gap = span_days / row_count
        if avg_gap < 2:
            return "daily"
        if avg_gap < 10:
            return "weekly"
        if avg_gap < 50:
            return "monthly"
        if avg_gap < 120:
            return "quarterly"
        return "yearly"
    except Exception:
        return "unknown"


def _extract_eligible_measures(
    columns: dict[str, ColumnMetadata],
    profile: DatasetProfile,
) -> list[str]:
    """Numeric columns suitable for aggregation/forecasting."""
    measures: list[str] = []
    for col_name, meta in columns.items():
        if col_name.startswith("_"):
            continue
        if meta.logical_type not in ("integer", "float"):
            continue
        col_prof = profile.columns.get(col_name)
        if col_prof and col_prof.null_ratio < 0.5:
            measures.append(col_name)
    return measures


def _extract_categorical_values(
    profile: DatasetProfile,
) -> dict[str, list[str]]:
    """Top values for string columns from the profile."""
    result: dict[str, list[str]] = {}
    for col_name, col_prof in profile.columns.items():
        if col_prof.logical_type == "string" and col_prof.top_values:
            result[col_name] = list(col_prof.top_values.keys())
    return result
