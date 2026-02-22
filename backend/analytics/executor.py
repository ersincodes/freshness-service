"""Execute compiled analytics SQL against SQLite and format results."""
from __future__ import annotations

import logging

from .errors import (
    AnalyticsExecutionError,
    AnalyticsPlanValidationError,
    AnalyticsRoutingError,
)
from .metadata_repository import MetadataRepository
from .models import AnalyticsPlan, AnalyticsResult, DatasetProfile
from .sql_compiler import compile_plan
from .validator import validate_plan, validate_result

logger = logging.getLogger(__name__)


class AnalyticsExecutor:
    """Resolves metadata, validates, compiles, executes, and formats analytics results."""

    def __init__(self, metadata_repo: MetadataRepository) -> None:
        self._meta = metadata_repo

    @property
    def metadata_repo(self) -> MetadataRepository:
        return self._meta

    def execute(self, plan: AnalyticsPlan) -> AnalyticsResult:
        document_id = plan.document_id
        sheet_name = plan.sheet_name or self._meta.resolve_default_sheet_name(document_id)
        if sheet_name is None:
            raise AnalyticsRoutingError("No sheet specified and no default sheet registered")

        table_name = self._meta.get_table_name(document_id, sheet_name)
        if table_name is None:
            raise AnalyticsRoutingError("No ingested table registered for document_id + sheet")

        column_metadata = self._meta.get_columns(document_id, sheet_name)
        if not column_metadata:
            raise AnalyticsRoutingError("No column metadata registered for document_id + sheet")

        validate_plan(plan, column_metadata)

        compiled = compile_plan(plan, table_name=table_name, column_metadata=column_metadata)

        try:
            import sqlite3
            self._meta._conn.row_factory = sqlite3.Row
            cursor = self._meta._conn.execute(compiled.sql, tuple(compiled.parameters))
            rows = cursor.fetchall()
        except Exception as exc:
            raise AnalyticsExecutionError(str(exc)) from exc

        result_data = self._format_result(plan, rows)

        profile = self._meta.get_profile(document_id, sheet_name)
        try:
            validate_result(result_data, profile)
        except Exception as exc:
            logger.warning("Result validation warning: %s", exc)

        return AnalyticsResult(
            summary=self._build_summary(plan, result_data),
            sql=compiled.sql,
            parameters=list(compiled.parameters),
            data=result_data,
        )

    def _format_result(self, plan: AnalyticsPlan, rows: list) -> dict:
        if plan.operation == "count_rows":
            count = int(rows[0]["count"]) if rows else 0
            return {"count": count}

        if plan.operation == "count_distinct":
            val = int(rows[0]["count_distinct"]) if rows else 0
            return {"count_distinct": val}

        if plan.operation == "sum":
            val = rows[0]["sum_value"] if rows else 0
            return {"sum": val if val is not None else 0}

        if plan.operation == "avg":
            val = rows[0]["avg_value"] if rows else None
            return {"avg": round(float(val), 4) if val is not None else None}

        if plan.operation == "min":
            val = rows[0]["min_value"] if rows else None
            return {"min": val}

        if plan.operation == "max":
            val = rows[0]["max_value"] if rows else None
            return {"max": val}

        if plan.operation == "groupby_count":
            out_rows = [{"key": r["key"], "count": int(r["cnt"])} for r in rows]
            return {"rows": out_rows}

        if plan.operation == "select_rows":
            out_rows = [dict(r) for r in rows]
            return {"rows": out_rows, "row_count": len(out_rows)}

        raise AnalyticsPlanValidationError(f"Unhandled operation: {plan.operation}")

    def _build_summary(self, plan: AnalyticsPlan, data: dict) -> str:
        op = plan.operation
        if op == "count_rows":
            return f"Counted {data['count']} rows."
        if op == "count_distinct":
            return f"Counted {data['count_distinct']} distinct values in '{plan.target_column}'."
        if op == "sum":
            return f"Sum of '{plan.target_column}' is {data['sum']}."
        if op == "avg":
            return f"Average of '{plan.target_column}' is {data['avg']}."
        if op == "min":
            return f"Minimum of '{plan.target_column}' is {data['min']}."
        if op == "max":
            return f"Maximum of '{plan.target_column}' is {data['max']}."
        if op == "groupby_count":
            col = plan.group_by or plan.target_column
            return f"Computed group-by counts for '{col}' (top {plan.top_n})."
        if op == "select_rows":
            return f"Retrieved {data['row_count']} matching row(s)."
        return f"Executed {op}."
