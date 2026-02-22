"""Compile an AnalyticsPlan into parameterized SQL + params.

Key invariant: all date filtering is compiled to epoch ranges by
this module. The LLM never specifies boundary predicates.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Mapping

from .errors import AnalyticsCompilationError
from .models import (
    AnalyticsFilter,
    AnalyticsPlan,
    ColumnMetadata,
    DATE_ONLY_OPS,
    NUMERIC_ONLY_OPS,
    STRING_ONLY_OPS,
)


@dataclass(frozen=True)
class CompiledSql:
    sql: str
    parameters: list[Any]


# ------------------------------------------------------------------
# Epoch conversion helpers
# ------------------------------------------------------------------

def _iso_to_epoch(iso_date: str) -> int:
    """Parse YYYY-MM-DD to UTC epoch seconds (midnight)."""
    try:
        d = date.fromisoformat(iso_date.strip())
    except ValueError as exc:
        raise AnalyticsCompilationError(f"Invalid ISO date: {iso_date!r}") from exc
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def compile_year_equals(safe_col: str, year: int) -> tuple[str, list[Any]]:
    start = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp())
    end = int(datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp())
    return f"({safe_col} >= ? AND {safe_col} < ?)", [start, end]


def compile_month_equals(safe_col: str, year: int, month: int) -> tuple[str, list[Any]]:
    start = int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp())
    if month == 12:
        end = int(datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp())
    else:
        end = int(datetime(year, month + 1, 1, tzinfo=timezone.utc).timestamp())
    return f"({safe_col} >= ? AND {safe_col} < ?)", [start, end]


def compile_between_dates(safe_col: str, start_date: str, end_date: str) -> tuple[str, list[Any]]:
    """Inclusive of both endpoints (half-open: end_date + 1 day)."""
    start_epoch = _iso_to_epoch(start_date)
    end_d = date.fromisoformat(end_date.strip())
    end_epoch = int(datetime(end_d.year, end_d.month, end_d.day, tzinfo=timezone.utc).timestamp()) + 86400
    return f"({safe_col} >= ? AND {safe_col} < ?)", [start_epoch, end_epoch]


# ------------------------------------------------------------------
# Filter compilation
# ------------------------------------------------------------------

def _parse_year_month(value: Any) -> tuple[int, int]:
    """Parse 'YYYY-MM' or int month from filter value."""
    s = str(value).strip()
    if "-" in s:
        parts = s.split("-")
        return int(parts[0]), int(parts[1])
    raise AnalyticsCompilationError(
        f"month_equals requires 'YYYY-MM' format, got: {value!r}"
    )


def _compile_single_filter(
    filt: AnalyticsFilter,
    columns: dict[str, ColumnMetadata],
    original_to_safe: Mapping[str, str],
) -> tuple[str, list[Any]]:
    if filt.column not in columns:
        raise AnalyticsCompilationError(f"Unknown column: {filt.column}")

    meta = columns[filt.column]
    safe_col = original_to_safe[filt.column]
    op = filt.operator

    if op in {"is_null", "is_not_null"}:
        kw = "IS NOT NULL" if op == "is_not_null" else "IS NULL"
        return f"{safe_col} {kw}", []

    if filt.value is None:
        raise AnalyticsCompilationError(f"Value required for operator '{op}'")

    # Type-operator compatibility checks
    if op in NUMERIC_ONLY_OPS and meta.logical_type not in ("integer", "float", "date"):
        raise AnalyticsCompilationError(
            f"Operator '{op}' not valid for {meta.logical_type} column '{filt.column}'"
        )
    if op in STRING_ONLY_OPS and meta.logical_type != "string":
        raise AnalyticsCompilationError(
            f"Operator '{op}' not valid for {meta.logical_type} column '{filt.column}'"
        )
    if op in DATE_ONLY_OPS and meta.logical_type != "date":
        raise AnalyticsCompilationError(
            f"Operator '{op}' not valid for {meta.logical_type} column '{filt.column}'"
        )

    # Date operators → epoch boundary compilation
    if op == "year_equals":
        return compile_year_equals(safe_col, int(filt.value))

    if op == "month_equals":
        year, month = _parse_year_month(filt.value)
        return compile_month_equals(safe_col, year, month)

    if op == "between_dates":
        if not isinstance(filt.value, list) or len(filt.value) != 2:
            raise AnalyticsCompilationError(
                "between_dates requires a list of two ISO date strings"
            )
        return compile_between_dates(safe_col, str(filt.value[0]), str(filt.value[1]))

    # Comparison operators
    sql_op_map = {
        "eq": "=", "neq": "!=",
        "gt": ">", "gte": ">=",
        "lt": "<", "lte": "<=",
    }
    if op in sql_op_map:
        return f"{safe_col} {sql_op_map[op]} ?", [filt.value]

    # String operators
    if op == "contains":
        return f"{safe_col} LIKE ?", [f"%{filt.value}%"]
    if op == "startswith":
        return f"{safe_col} LIKE ?", [f"{filt.value}%"]

    raise AnalyticsCompilationError(f"Unsupported operator: {op}")


def _compile_where(
    filters: list[AnalyticsFilter],
    columns: dict[str, ColumnMetadata],
    original_to_safe: Mapping[str, str],
) -> tuple[str, list[Any]]:
    if not filters:
        return "", []

    clauses: list[str] = []
    params: list[Any] = []
    for f in filters:
        clause, p = _compile_single_filter(f, columns, original_to_safe)
        clauses.append(clause)
        params.extend(p)

    return "WHERE " + " AND ".join(clauses), params


# ------------------------------------------------------------------
# Plan compilation
# ------------------------------------------------------------------

def compile_plan(
    plan: AnalyticsPlan,
    *,
    table_name: str,
    column_metadata: dict[str, ColumnMetadata],
) -> CompiledSql:
    """Compile a validated AnalyticsPlan into parameterized SQL."""
    original_to_safe = {m.original_name: m.safe_name for m in column_metadata.values()}

    where_sql, params = _compile_where(plan.filters, column_metadata, original_to_safe)

    if plan.operation == "count_rows":
        sql = f"SELECT COUNT(1) AS count FROM {table_name} {where_sql};"
        return CompiledSql(sql=sql, parameters=params)

    if plan.operation == "count_distinct":
        _require_target(plan)
        safe_col = _safe_col(plan.target_column, column_metadata, original_to_safe)
        sql = f"SELECT COUNT(DISTINCT {safe_col}) AS count_distinct FROM {table_name} {where_sql};"
        return CompiledSql(sql=sql, parameters=params)

    if plan.operation == "sum":
        _require_target(plan)
        safe_col = _safe_col(plan.target_column, column_metadata, original_to_safe)
        sql = f"SELECT SUM({safe_col}) AS sum_value FROM {table_name} {where_sql};"
        return CompiledSql(sql=sql, parameters=params)

    if plan.operation == "avg":
        _require_target(plan)
        safe_col = _safe_col(plan.target_column, column_metadata, original_to_safe)
        sql = f"SELECT AVG({safe_col}) AS avg_value FROM {table_name} {where_sql};"
        return CompiledSql(sql=sql, parameters=params)

    if plan.operation == "min":
        _require_target(plan)
        safe_col = _safe_col(plan.target_column, column_metadata, original_to_safe)
        sql = f"SELECT MIN({safe_col}) AS min_value FROM {table_name} {where_sql};"
        return CompiledSql(sql=sql, parameters=params)

    if plan.operation == "max":
        _require_target(plan)
        safe_col = _safe_col(plan.target_column, column_metadata, original_to_safe)
        sql = f"SELECT MAX({safe_col}) AS max_value FROM {table_name} {where_sql};"
        return CompiledSql(sql=sql, parameters=params)

    if plan.operation == "groupby_count":
        group_col_name = plan.group_by or plan.target_column
        if not group_col_name:
            raise AnalyticsCompilationError("groupby_count requires group_by or target_column")
        safe_col = _safe_col(group_col_name, column_metadata, original_to_safe)

        order_sql = {
            "count_desc": "cnt DESC",
            "count_asc": "cnt ASC",
            "key_asc": f"{safe_col} ASC",
            "key_desc": f"{safe_col} DESC",
        }[plan.order]

        top_n = max(1, min(plan.top_n, 1000))

        sql = (
            f"SELECT {safe_col} AS key, COUNT(1) AS cnt "
            f"FROM {table_name} "
            f"{where_sql} "
            f"GROUP BY {safe_col} "
            f"ORDER BY {order_sql} "
            f"LIMIT {top_n};"
        )
        return CompiledSql(sql=sql, parameters=params)

    if plan.operation == "select_rows":
        if plan.select_columns:
            for col in plan.select_columns:
                if col not in column_metadata:
                    raise AnalyticsCompilationError(f"Unknown select column: {col}")
            safe_cols = [original_to_safe[c] for c in plan.select_columns]
            aliases = [f"{s} AS '{c}'" for c, s in zip(plan.select_columns, safe_cols)]
            select_clause = ", ".join(aliases)
        else:
            visible = [(k, v) for k, v in column_metadata.items() if not k.startswith("_")]
            select_clause = ", ".join(f"{v.safe_name} AS '{k}'" for k, v in visible)

        limit = max(1, min(plan.limit, 500))

        sql = f"SELECT {select_clause} FROM {table_name} {where_sql} LIMIT {limit};"
        return CompiledSql(sql=sql, parameters=params)

    raise AnalyticsCompilationError(f"Unhandled operation: {plan.operation}")


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _require_target(plan: AnalyticsPlan) -> None:
    if not plan.target_column:
        raise AnalyticsCompilationError(
            f"target_column is required for operation '{plan.operation}'"
        )


def _safe_col(
    col_name: str | None,
    columns: dict[str, ColumnMetadata],
    original_to_safe: Mapping[str, str],
) -> str:
    if col_name is None:
        raise AnalyticsCompilationError("Column name is required")
    if col_name not in columns:
        raise AnalyticsCompilationError(f"Unknown column: {col_name}")
    return original_to_safe[col_name]
