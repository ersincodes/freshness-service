from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .errors import AnalyticsCompilationError
from .models import AnalyticsPlan, WherePredicate


@dataclass(frozen=True)
class CompiledSql:
    sql: str
    parameters: list[Any]


def compile_where_clause(
    where: list[WherePredicate],
    allowed_original_columns: set[str],
    original_to_safe: Mapping[str, str],
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    parameters: list[Any] = []

    for predicate in where:
        if predicate.column not in allowed_original_columns:
            raise AnalyticsCompilationError(f"Unknown column in where: {predicate.column}")

        safe_col = original_to_safe[predicate.column]

        if predicate.op in {"is_null", "is_not_null"}:
            clauses.append(f"{safe_col} IS {'NOT ' if predicate.op == 'is_not_null' else ''}NULL")
            continue

        if predicate.value is None:
            raise AnalyticsCompilationError(f"Predicate value required for op={predicate.op}")

        if predicate.op in {"=", "!=", "<", "<=", ">", ">="}:
            clauses.append(f"{safe_col} {predicate.op} ?")
            parameters.append(predicate.value)
        elif predicate.op == "contains":
            clauses.append(f"{safe_col} LIKE ?")
            parameters.append(f"%{predicate.value}%")
        elif predicate.op == "startswith":
            clauses.append(f"{safe_col} LIKE ?")
            parameters.append(f"{predicate.value}%")
        elif predicate.op == "endswith":
            clauses.append(f"{safe_col} LIKE ?")
            parameters.append(f"%{predicate.value}")
        else:
            raise AnalyticsCompilationError(f"Unsupported op: {predicate.op}")

    if not clauses:
        return "", []

    return "WHERE " + " AND ".join(clauses), parameters


def compile_plan(
    plan: AnalyticsPlan,
    *,
    table_name: str,
    original_to_safe: Mapping[str, str],
) -> CompiledSql:
    allowed = set(original_to_safe.keys())

    if plan.type == "count_rows":
        where_sql, params = compile_where_clause(plan.where, allowed, original_to_safe)
        sql = f"SELECT COUNT(1) AS count FROM {table_name} {where_sql};"
        return CompiledSql(sql=sql, parameters=params)

    if plan.type == "count_distinct":
        if plan.column not in allowed:
            raise AnalyticsCompilationError(f"Unknown column: {plan.column}")
        safe_col = original_to_safe[plan.column]
        where_sql, params = compile_where_clause(plan.where, allowed, original_to_safe)
        sql = f"SELECT COUNT(DISTINCT {safe_col}) AS count_distinct FROM {table_name} {where_sql};"
        return CompiledSql(sql=sql, parameters=params)

    if plan.type == "groupby_count":
        if plan.group_by not in allowed:
            raise AnalyticsCompilationError(f"Unknown column: {plan.group_by}")
        safe_col = original_to_safe[plan.group_by]
        where_sql, params = compile_where_clause(plan.where, allowed, original_to_safe)

        order_sql = {
            "count_desc": "cnt DESC",
            "count_asc": "cnt ASC",
            "key_asc": f"{safe_col} ASC",
            "key_desc": f"{safe_col} DESC",
        }[plan.order]

        top_n = int(plan.top_n)
        if top_n < 1 or top_n > 1000:
            raise AnalyticsCompilationError("top_n out of range (1..1000)")

        sql = (
            f"SELECT {safe_col} AS key, COUNT(1) AS cnt "
            f"FROM {table_name} "
            f"{where_sql} "
            f"GROUP BY {safe_col} "
            f"ORDER BY {order_sql} "
            f"LIMIT {top_n};"
        )
        return CompiledSql(sql=sql, parameters=params)

    raise AnalyticsCompilationError(f"Unhandled plan type: {plan.type}")
