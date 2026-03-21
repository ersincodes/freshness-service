"""Format deterministic analytics results as markdown for chat UI (GFM tables)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from .models import AnalyticsResult, ColumnMetadata


def _coerce_row_mapping(row: Any) -> dict[str, Any] | None:
    """Build a plain dict from sqlite3.Row, dict, or other dict()-compatible rows."""
    if isinstance(row, dict):
        return row
    if isinstance(row, (str, bytes, bytearray)):
        return None
    try:
        mapped = dict(row)
    except (TypeError, ValueError):
        return None
    if not isinstance(mapped, dict):
        return None
    return mapped


def _normalize_row_list(rows_raw: Any) -> list[dict[str, Any]] | None:
    """Parse `data['rows']` into plain dict rows (handles JSON string, sqlite3.Row, etc.)."""
    if rows_raw is None:
        return None
    if isinstance(rows_raw, str):
        try:
            rows_raw = json.loads(rows_raw)
        except json.JSONDecodeError:
            return None
    if isinstance(rows_raw, tuple):
        rows_raw = list(rows_raw)
    if not isinstance(rows_raw, list) or not rows_raw:
        return None
    out: list[dict[str, Any]] = []
    for r in rows_raw:
        m = _coerce_row_mapping(r)
        if m is None:
            return None
        out.append(m)
    return out


def _escape_md_cell(text: str) -> str:
    return text.replace("\n", " ").replace("|", "\\|")


def _format_unix_date(ts: int) -> str:
    try:
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (OSError, ValueError, OverflowError):
        return str(ts)


def _format_scalar_for_cell(
    column_name: str | None,
    value: Any,
    meta: ColumnMetadata | None,
) -> str:
    if value is None:
        return ""
    if meta is not None:
        lt = meta.logical_type
        if lt == "boolean":
            if value in (0, 1, True, False):
                return "Yes" if bool(value) else "No"
        if lt == "date" and isinstance(value, int):
            return _format_unix_date(value)
        if lt == "float" and isinstance(value, (int, float)):
            x = float(value)
            if x.is_integer():
                return f"{int(x):,}"
            s = f"{x:,.4f}".rstrip("0").rstrip(".")
            return s if s else "0"
        if lt == "integer" and isinstance(value, int):
            return f"{value:,}"
    if (
        column_name
        and isinstance(value, int)
        and "date" in column_name.lower()
        and 946_684_800 <= value <= 4_102_444_800
    ):
        return _format_unix_date(value)
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}"
        s = f"{value:,.4g}"
        return s
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _ordered_columns(
    sample_row: dict[str, Any],
    columns_meta: dict[str, ColumnMetadata],
) -> list[str]:
    meta_order = [
        k
        for k in columns_meta
        if k in sample_row and not k.startswith("_")
    ]
    rest = [k for k in sample_row if k not in meta_order and not k.startswith("_")]
    return meta_order + rest


def _markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    esc = _escape_md_cell
    lines = [
        "| " + " | ".join(esc(h) for h in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(esc(c) for c in row) + " |")
    return "\n".join(lines)


def _fallback_json(summary: str, data: dict[str, Any]) -> str:
    payload = json.dumps(data, default=str)
    block = f"**Data:** {payload}"
    if summary:
        return f"{summary}\n\n{block}"
    return block


def format_analytics_result_markdown(
    result: AnalyticsResult,
    columns_meta: dict[str, ColumnMetadata] | None = None,
) -> str:
    """Summary plus a GFM table for tabular analytics; scalars stay summary-only."""
    meta = columns_meta or {}
    data = result.data
    summary = (result.summary or "").strip()

    rows_raw = data.get("rows")
    rows_norm = _normalize_row_list(rows_raw)
    if isinstance(rows_raw, list) and rows_raw and rows_norm is None:
        return _fallback_json(summary, data)

    if rows_norm:
        first = rows_norm[0]
        keys = set(first.keys())

        if keys == {"key", "count"}:
            headers = ["key", "count"]
            table_rows: list[list[str]] = []
            for r in rows_norm:
                table_rows.append(
                    [
                        _format_scalar_for_cell("key", r.get("key"), None),
                        str(int(r.get("count", 0))),
                    ]
                )
            return f"{summary}\n\n{_markdown_table(headers, table_rows)}"

        if keys == {"key", "value"}:
            headers = ["key", "value"]
            table_rows = []
            for r in rows_norm:
                table_rows.append(
                    [
                        _format_scalar_for_cell("key", r.get("key"), None),
                        _format_scalar_for_cell("value", r.get("value"), None),
                    ]
                )
            return f"{summary}\n\n{_markdown_table(headers, table_rows)}"

        col_order = _ordered_columns(first, meta)
        table_rows = []
        for r in rows_norm:
            table_rows.append(
                [_format_scalar_for_cell(c, r.get(c), meta.get(c)) for c in col_order]
            )
        return f"{summary}\n\n{_markdown_table(col_order, table_rows)}"

    if summary:
        return summary
    return _fallback_json("", data)
