"""Data standardization for spreadsheet ingestion.

Responsible for: type inference, cell normalization, safe column naming,
SQLite table creation, and bulk data insertion. Extracted from documents.py
to enforce single-responsibility.
"""
from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from dataclasses import dataclass
from typing import Any

import pandas as pd

from .models import ColumnMetadata, LogicalType, SQLITE_TYPE_MAP

logger = logging.getLogger(__name__)

_CURRENCY_STRIP_RE = re.compile(r"[\$,€£¥₹\u00a0\u202f]")


@dataclass(frozen=True)
class StandardizedSheet:
    """Result of standardizing a single spreadsheet sheet."""

    document_id: str
    sheet_name: str
    table_name: str
    dataframe: pd.DataFrame
    column_metadata: dict[str, ColumnMetadata]
    row_count: int


# ============================================================================
# Type Inference
# ============================================================================


def infer_logical_type(series: pd.Series) -> LogicalType:
    """Infer a LogicalType for a pandas Series.

    Priority: date > boolean > integer > float > string.
    """
    import datetime as _dt

    non_null = series.dropna()
    if non_null.empty:
        return "string"

    if pd.api.types.is_datetime64_any_dtype(series):
        return "date"
    sample = non_null.iloc[0]
    if isinstance(sample, (_dt.datetime, _dt.date, pd.Timestamp)):
        return "date"
    is_string_dtype = non_null.dtype == object or pd.api.types.is_string_dtype(non_null)

    if is_string_dtype:
        try:
            parsed = pd.to_datetime(non_null, errors="coerce")
            success_ratio = int(parsed.notna().sum()) / len(non_null)
            if success_ratio >= 0.8:
                return "date"
        except Exception:
            pass

    _BOOL_VALS = {"true", "false", "yes", "no", "0", "1"}
    if non_null.dtype == bool or (
        is_string_dtype
        and all(str(v).strip().lower() in _BOOL_VALS for v in non_null)
    ):
        return "boolean"

    if pd.api.types.is_integer_dtype(series):
        return "integer"
    if pd.api.types.is_float_dtype(series):
        if (non_null == non_null.astype(int)).all():
            return "integer"
        return "float"
    if is_string_dtype:
        coerced = non_null.map(_coerce_loose_numeric)
        if coerced.notna().sum() / len(non_null) >= 0.9:
            if (coerced.dropna() == coerced.dropna().astype(int)).all():
                return "integer"
            return "float"

    return "string"


# ============================================================================
# Cell Normalization
# ============================================================================


def strip_currency(value: Any) -> str | None:
    """Normalize currency strings for numeric parsing.

    Handles $, commas, spaces, NBSP, and (1234.56) accounting negatives.
    """
    if value is None:
        return None
    if pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()
    s = _CURRENCY_STRIP_RE.sub("", s)
    s = s.replace(",", "").replace(" ", "")
    if s.endswith("%"):
        s = s[:-1].strip()
    if not s or s in {"-", ".", "-."}:
        return None
    if neg:
        if s.startswith("-"):
            s = s[1:]
        s = "-" + s
    return s


def _coerce_loose_numeric(value: Any) -> float:
    """Single-cell numeric coercion for type inference; NaN if not numeric."""
    t = strip_currency(value)
    if t is None:
        return float("nan")
    try:
        return float(t)
    except ValueError:
        return float("nan")


def normalize_cell(x: Any, logical_type: LogicalType) -> Any:
    """Normalize a cell value according to its logical type.

    - date -> epoch seconds (int, UTC)
    - boolean -> 0/1 (int)
    - integer -> int
    - float -> float
    - string -> str (trimmed)
    """
    if x is None:
        return None
    if pd.isna(x):
        return None

    import datetime as _dt
    from datetime import timezone as _tz

    if logical_type == "date":
        if isinstance(x, pd.Timestamp):
            if x.tzinfo is None:
                x = x.tz_localize("UTC")
            return int(x.timestamp())
        if isinstance(x, _dt.datetime):
            if x.tzinfo is None:
                x = x.replace(tzinfo=_tz.utc)
            return int(x.timestamp())
        if isinstance(x, _dt.date):
            return int(_dt.datetime(x.year, x.month, x.day, tzinfo=_tz.utc).timestamp())
        try:
            parsed = pd.to_datetime(x)
            if parsed.tzinfo is None:
                parsed = parsed.tz_localize("UTC")
            return int(parsed.timestamp())
        except Exception:
            return None

    if logical_type == "boolean":
        s = str(x).strip().lower()
        return 1 if s in {"true", "yes", "1", "1.0"} else 0

    if logical_type == "integer":
        try:
            if isinstance(x, str):
                stripped = strip_currency(x)
                if stripped is None:
                    return None
                x = stripped
            return int(float(x))
        except (ValueError, TypeError):
            return None

    if logical_type == "float":
        try:
            if isinstance(x, str):
                stripped = strip_currency(x)
                if stripped is None:
                    return None
                x = stripped
            return float(x)
        except (ValueError, TypeError):
            return None

    return str(x).strip()


# ============================================================================
# Column Naming
# ============================================================================


def build_safe_column_mapping(original_headers: list[str]) -> dict[str, str]:
    """Map original column names to SQLite-safe identifiers."""
    used: set[str] = set()
    mapping: dict[str, str] = {}

    for raw in original_headers:
        base = re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw).strip().lower())
        base = re.sub(r"_+", "_", base).strip("_")
        base = base or "col"

        candidate = f"col_{base}"
        if candidate[0].isdigit():
            candidate = f"col_{candidate}"

        unique = candidate
        suffix = 2
        while unique in used:
            unique = f"{candidate}_{suffix}"
            suffix += 1

        used.add(unique)
        mapping[str(raw)] = unique

    return mapping


def build_table_name(*, document_id: str, sheet_name: str) -> str:
    """Deterministic SQLite table name from document + sheet."""
    doc_part = re.sub(r"[^a-zA-Z0-9_]+", "_", document_id)[:24].strip("_") or "doc"
    sheet_hash = hashlib.sha1(sheet_name.encode("utf-8")).hexdigest()[:10]
    return f"doc_{doc_part}__{sheet_hash}"


# ============================================================================
# SQLite Operations
# ============================================================================


def create_typed_table(
    conn: sqlite3.Connection,
    table_name: str,
    columns: list[ColumnMetadata],
) -> None:
    """DROP + CREATE a typed SQLite table with useful indices."""
    columns_ddl = ", ".join(f"{c.safe_name} {c.sqlite_type}" for c in columns)

    with conn:
        conn.execute(f"DROP TABLE IF EXISTS {table_name};")
        conn.execute(f"CREATE TABLE {table_name} ({columns_ddl});")

        for col in columns:
            if "source_row_number" in col.safe_name:
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}__rownum "
                    f"ON {table_name} ({col.safe_name});"
                )
            elif col.logical_type == "date":
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}__{col.safe_name} "
                    f"ON {table_name} ({col.safe_name});"
                )
            elif any(kw in col.original_name.lower() for kw in ("_id", "id", "code", "index")):
                conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{table_name}__{col.safe_name} "
                    f"ON {table_name} ({col.safe_name});"
                )


def bulk_insert(
    conn: sqlite3.Connection,
    table_name: str,
    safe_columns: list[str],
    rows: Any,
) -> None:
    """Insert rows into a SQLite table."""
    placeholders = ",".join(["?"] * len(safe_columns))
    cols_sql = ",".join(safe_columns)
    sql = f"INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders});"

    with conn:
        conn.executemany(sql, list(rows))


# ============================================================================
# DataStandardizer
# ============================================================================


class DataStandardizer:
    """Standardizes raw spreadsheet data into typed, analysis-ready SQLite tables.

    Orchestrates: type inference -> normalization -> table creation -> insertion.
    Does NOT handle profiling or forecasting (those are separate concerns).
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def standardize_sheet(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        document_id: str,
    ) -> StandardizedSheet:
        """Full standardization pipeline for a single sheet."""
        if df is None or df.empty:
            raise ValueError(f"Sheet '{sheet_name}' is empty")

        original_headers = [str(c) for c in df.columns]
        augmented_headers = ["_source_row_number", *original_headers]

        type_map = self._infer_types(df, original_headers)

        df_normalized = self._normalize_dataframe(
            df, augmented_headers, type_map
        )

        table_name = build_table_name(
            document_id=document_id, sheet_name=sheet_name
        )
        original_to_safe = build_safe_column_mapping(augmented_headers)
        df_normalized.columns = [original_to_safe[h] for h in augmented_headers]

        col_meta = self._build_column_metadata(
            augmented_headers, type_map, original_to_safe
        )
        col_meta_dict = {m.original_name: m for m in col_meta}

        self._persist_to_sqlite(table_name, col_meta, original_to_safe, augmented_headers, df_normalized)

        logger.info(
            "Sheet '%s' column types: %s",
            sheet_name,
            {h: type_map[h] for h in original_headers},
        )

        return StandardizedSheet(
            document_id=document_id,
            sheet_name=sheet_name,
            table_name=table_name,
            dataframe=df_normalized,
            column_metadata=col_meta_dict,
            row_count=len(df_normalized),
        )

    def _infer_types(
        self, df: pd.DataFrame, original_headers: list[str]
    ) -> dict[str, LogicalType]:
        type_map: dict[str, LogicalType] = {"_source_row_number": "integer"}
        for header in original_headers:
            type_map[header] = infer_logical_type(df[header])
        return type_map

    def _normalize_dataframe(
        self,
        df: pd.DataFrame,
        augmented_headers: list[str],
        type_map: dict[str, LogicalType],
    ) -> pd.DataFrame:
        df2 = df.copy()
        df2.insert(0, "_source_row_number", range(1, len(df2) + 1))
        df2 = df2.astype(object).where(pd.notnull(df2), None)

        for header in augmented_headers:
            ltype = type_map[header]
            df2[header] = df2[header].map(lambda x, lt=ltype: normalize_cell(x, lt))

        return df2

    def _build_column_metadata(
        self,
        augmented_headers: list[str],
        type_map: dict[str, LogicalType],
        original_to_safe: dict[str, str],
    ) -> list[ColumnMetadata]:
        result: list[ColumnMetadata] = []
        for h in augmented_headers:
            ltype = type_map[h]
            sqlite_type = SQLITE_TYPE_MAP.get(ltype, "TEXT")
            nullable = h != "_source_row_number"
            result.append(ColumnMetadata(
                column_name=h,
                logical_type=ltype,
                sqlite_type=sqlite_type,
                nullable=nullable,
                original_name=h,
                safe_name=original_to_safe[h],
            ))
        return result

    def _persist_to_sqlite(
        self,
        table_name: str,
        col_meta: list[ColumnMetadata],
        original_to_safe: dict[str, str],
        augmented_headers: list[str],
        df: pd.DataFrame,
    ) -> None:
        create_typed_table(self._conn, table_name, col_meta)
        safe_cols = [original_to_safe[h] for h in augmented_headers]
        bulk_insert(
            self._conn,
            table_name,
            safe_cols,
            df.itertuples(index=False, name=None),
        )
