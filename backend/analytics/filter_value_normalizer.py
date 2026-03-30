"""Repair string filter values against profile and DISTINCT data (no extra deps)."""
from __future__ import annotations

import difflib
import sqlite3
from typing import Any

from .models import AnalyticsFilter, AnalyticsPlan, ColumnMetadata, DatasetProfile


_DISTINCT_CAP = 200
_CLOSE_MATCH_CUTOFF = 0.72


def _profile_keys_for_column(
    profile: DatasetProfile | None, column: str
) -> list[str]:
    if profile is None or column not in profile.columns:
        return []
    tv = profile.columns[column].top_values
    if not tv:
        return []
    return list(tv.keys())


def _fetch_distinct_strings(
    conn: sqlite3.Connection,
    table_name: str,
    safe_column: str,
    limit: int = _DISTINCT_CAP,
) -> list[str]:
    q = (
        f"SELECT DISTINCT {safe_column} AS v FROM {table_name} "
        f"WHERE {safe_column} IS NOT NULL LIMIT ?"
    )
    cur = conn.execute(q, (limit,))
    out: list[str] = []
    for (v,) in cur.fetchall():
        if v is None:
            continue
        out.append(str(v))
    return out


def _best_string_match(user_value: str, candidates: list[str]) -> str | None:
    if not candidates:
        return None
    u = user_value.strip()
    if not u:
        return None
    u_lower = u.lower()
    for c in candidates:
        if c == user_value:
            return c
    for c in candidates:
        if c.lower() == u_lower:
            return c
    for c in candidates:
        cl, ul = c.lower(), u_lower
        if ul in cl or cl in ul:
            return c
    matches = difflib.get_close_matches(u, candidates, n=1, cutoff=_CLOSE_MATCH_CUTOFF)
    if matches:
        return matches[0]
    matches_l = difflib.get_close_matches(u_lower, [x.lower() for x in candidates], n=1, cutoff=_CLOSE_MATCH_CUTOFF)
    if matches_l:
        ml = matches_l[0]
        for c in candidates:
            if c.lower() == ml:
                return c
    return None


def _normalize_scalar_value(
    column: str,
    value: Any,
    logical_type: str,
    profile: DatasetProfile | None,
    conn: sqlite3.Connection | None,
    table_name: str | None,
    original_to_safe: dict[str, str],
) -> Any:
    if logical_type != "string" or not isinstance(value, str):
        return value
    candidates = _profile_keys_for_column(profile, column)
    seen: set[str] = set()
    ordered: list[str] = []
    for x in candidates:
        if x not in seen:
            seen.add(x)
            ordered.append(x)
    if conn is not None and table_name and column in original_to_safe:
        safe = original_to_safe[column]
        for d in _fetch_distinct_strings(conn, table_name, safe):
            if d not in seen:
                seen.add(d)
                ordered.append(d)
    matched = _best_string_match(value, ordered)
    return matched if matched is not None else value


def _year_equals_to_eq_on_integer(
    filt: AnalyticsFilter,
    column_metadata: dict[str, ColumnMetadata],
) -> AnalyticsFilter | None:
    """Map year_equals on an integer column to eq(year): validators only allow year_equals on dates."""
    if filt.operator != "year_equals" or filt.value is None:
        return None
    meta = column_metadata.get(filt.column)
    if meta is None or meta.logical_type != "integer":
        return None
    try:
        y = int(filt.value)
    except (TypeError, ValueError):
        return None
    return AnalyticsFilter(column=filt.column, operator="eq", value=y)


def normalize_analytics_plan_filters(
    plan: AnalyticsPlan,
    column_metadata: dict[str, ColumnMetadata],
    profile: DatasetProfile | None,
    conn: sqlite3.Connection | None,
    table_name: str | None,
) -> AnalyticsPlan:
    """Return a copy of plan with repaired filters.

    - year_equals on integer columns (e.g. Year) becomes eq; planners often emit
      year_equals for calendar-year filters, but validation only allows it on date columns.
    - String eq/neq values are aligned to actual cell text where possible.
    """
    original_to_safe = {m.original_name: m.safe_name for m in column_metadata.values()}
    new_filters: list[AnalyticsFilter] = []
    changed = False
    for filt in plan.filters:
        repaired = _year_equals_to_eq_on_integer(filt, column_metadata)
        if repaired is not None:
            changed = True
            filt = repaired

        if filt.operator not in ("eq", "neq"):
            new_filters.append(filt)
            continue
        if filt.column not in column_metadata:
            new_filters.append(filt)
            continue
        meta = column_metadata[filt.column]
        nv = _normalize_scalar_value(
            filt.column,
            filt.value,
            meta.logical_type,
            profile,
            conn,
            table_name,
            original_to_safe,
        )
        if nv != filt.value:
            changed = True
            new_filters.append(
                AnalyticsFilter(column=filt.column, operator=filt.operator, value=nv)
            )
        else:
            new_filters.append(filt)
    if not changed:
        return plan
    return plan.model_copy(update={"filters": new_filters})
