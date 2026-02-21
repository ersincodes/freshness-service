from __future__ import annotations

from .errors import (
    AnalyticsExecutionError,
    AnalyticsPlanValidationError,
    AnalyticsRoutingError,
)
from .models import AnalyticsPlan, AnalyticsResult
from .sql_compiler import compile_plan
from ..repositories.analytics_repository import AnalyticsRepository


class AnalyticsExecutor:
    """Resolves table+columns, compiles a restricted plan, executes it, formats results."""

    def __init__(self, analytics_repository: AnalyticsRepository) -> None:
        self._repo = analytics_repository

    def execute(self, plan: AnalyticsPlan) -> AnalyticsResult:
        document_id = plan.table.document_id
        sheet_name = plan.table.sheet_name or self._repo.resolve_default_sheet_name(document_id)
        if sheet_name is None:
            raise AnalyticsRoutingError("No sheet specified and no default sheet registered")

        table_name = self._repo.resolve_table_name(document_id, sheet_name)
        if table_name is None:
            raise AnalyticsRoutingError("No ingested table registered for document_id+sheet")

        mappings = self._repo.fetch_column_mappings(document_id, sheet_name)
        if not mappings:
            raise AnalyticsRoutingError("No column mapping registered for document_id+sheet")

        original_to_safe = {m.original_name: m.safe_name for m in mappings}

        compiled = compile_plan(plan, table_name=table_name, original_to_safe=original_to_safe)

        try:
            rows = self._repo.execute_parameterized_sql(compiled.sql, compiled.parameters)
        except Exception as exc:
            raise AnalyticsExecutionError(str(exc)) from exc

        if plan.type == "count_rows":
            count = int(rows[0]["count"]) if rows else 0
            return AnalyticsResult(
                summary=f"Counted {count} rows.",
                sql=compiled.sql,
                parameters=list(compiled.parameters),
                data={"count": count},
            )

        if plan.type == "count_distinct":
            count_distinct = int(rows[0]["count_distinct"]) if rows else 0
            return AnalyticsResult(
                summary=f"Counted {count_distinct} distinct values in '{plan.column}'.",
                sql=compiled.sql,
                parameters=list(compiled.parameters),
                data={"count_distinct": count_distinct},
            )

        if plan.type == "groupby_count":
            out_rows = [{"key": r["key"], "count": int(r["cnt"])} for r in rows]
            return AnalyticsResult(
                summary=f"Computed group-by counts for '{plan.group_by}' (top {plan.top_n}).",
                sql=compiled.sql,
                parameters=list(compiled.parameters),
                data={"rows": out_rows},
            )

        raise AnalyticsPlanValidationError(f"Unhandled plan type: {plan.type}")
