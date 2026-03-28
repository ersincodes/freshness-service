"""Execute compiled analytics SQL against SQLite and format results."""
from __future__ import annotations

import json
import logging

from .errors import (
    AnalyticsExecutionError,
    AnalyticsPlanValidationError,
    AnalyticsRoutingError,
)
from .filter_value_normalizer import normalize_analytics_plan_filters
from .forecast_repository import ForecastRepository
from .metadata_repository import MetadataRepository
from .models import AnalyticsPlan, AnalyticsResult, DatasetProfile
from .sql_compiler import compile_plan
from .validator import validate_plan, validate_result

logger = logging.getLogger(__name__)


class AnalyticsExecutor:
    """Resolves metadata, validates, compiles, executes, and formats analytics results."""

    def __init__(self, metadata_repo: MetadataRepository) -> None:
        self._meta = metadata_repo
        self._forecast_repo = ForecastRepository(metadata_repo._conn)

    @property
    def metadata_repo(self) -> MetadataRepository:
        return self._meta

    @property
    def forecast_repo(self) -> ForecastRepository:
        return self._forecast_repo

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

        profile = self._meta.get_profile(document_id, sheet_name)
        plan = normalize_analytics_plan_filters(
            plan,
            column_metadata,
            profile,
            self._meta._conn,
            table_name,
        )

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
            document_id=document_id,
            sheet_name=sheet_name,
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
            out_rows: list[dict] = []
            for r in rows:
                d = dict(r)
                if "time_bucket" in d and "key" in d:
                    out_rows.append(
                        {
                            "time_bucket": d["time_bucket"],
                            "key": d["key"],
                            "count": int(d["cnt"]),
                        }
                    )
                elif "time_bucket" in d:
                    out_rows.append(
                        {
                            "time_bucket": d["time_bucket"],
                            "count": int(d["cnt"]),
                        }
                    )
                else:
                    out_rows.append({"key": d["key"], "count": int(d["cnt"])})
            return {"rows": out_rows}

        if plan.operation in ("groupby_sum", "groupby_ratio"):
            out_rows = []
            for r in rows:
                d = dict(r)
                v = d["value"]
                val = 0 if v is None else v
                if "time_bucket" in d and "key" in d:
                    out_rows.append(
                        {"time_bucket": d["time_bucket"], "key": d["key"], "value": val}
                    )
                elif "time_bucket" in d:
                    out_rows.append({"time_bucket": d["time_bucket"], "value": val})
                else:
                    out_rows.append({"key": d["key"], "value": val})
            return {"rows": out_rows}

        if plan.operation == "select_rows":
            out_rows = [dict(r) for r in rows]
            # JSON round-trip ensures plain dict/list scalars (avoids sqlite3.Row or odd types in chat formatting).
            out_rows = [json.loads(json.dumps(row, default=str)) for row in out_rows]
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
            if plan.time_grain and plan.time_grain != "none":
                g = plan.group_by or plan.target_column or "(time only)"
                return (
                    f"Computed row counts by {plan.time_grain} and '{g}' (top {plan.top_n})."
                )
            col = plan.group_by or plan.target_column
            return f"Computed group-by counts for '{col}' (top {plan.top_n})."
        if op == "groupby_sum":
            if plan.time_grain and plan.time_grain != "none":
                g = plan.group_by or "(time only)"
                return (
                    f"Computed sums of '{plan.target_column}' over {plan.time_grain} "
                    f"buckets by '{g}' (top {plan.top_n})."
                )
            return (
                f"Computed group-by sums of '{plan.target_column}' by "
                f"'{plan.group_by}' (top {plan.top_n})."
            )
        if op == "groupby_ratio":
            return (
                f"Computed ratio SUM({plan.target_column})/SUM({plan.denominator_column}) "
                f"by '{plan.group_by}' (top {plan.top_n})."
            )
        if op == "select_rows":
            return f"Retrieved {data['row_count']} matching row(s)."
        return f"Executed {op}."
