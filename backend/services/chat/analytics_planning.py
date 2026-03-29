"""Analytics plan parsing, row-limit hints, and post-LLM plan repairs."""
from __future__ import annotations

import json
import re

from ...analytics.models import AnalyticsPlan, DatasetProfile

_SELECT_ROWS_FIRST_TOP_RE = re.compile(r"\b(?:first|top)\s+(\d+)\b", re.IGNORECASE)
_SELECT_ROWS_LIMIT_ONLY_RE = re.compile(r"\b(?:limit|only|just)\s+(\d+)\b", re.IGNORECASE)
_SELECT_ROWS_NUM_ROWS_RE = re.compile(r"\b(\d+)\s+rows?\b", re.IGNORECASE)
_SELECT_ROWS_LIMIT_CAP = 500


def infer_select_rows_limit_from_query(query: str) -> int | None:
    """Best-effort row count for select_rows from natural language (first/top/N rows).

    Does not treat bare numbers in filenames (e.g. '100 Sales Record') as a row cap.
    """
    if not (query and query.strip()):
        return None
    m = _SELECT_ROWS_FIRST_TOP_RE.search(query)
    if m:
        return min(int(m.group(1)), _SELECT_ROWS_LIMIT_CAP)
    m = _SELECT_ROWS_LIMIT_ONLY_RE.search(query)
    if m:
        return min(int(m.group(1)), _SELECT_ROWS_LIMIT_CAP)
    m = _SELECT_ROWS_NUM_ROWS_RE.search(query)
    if m:
        return min(int(m.group(1)), _SELECT_ROWS_LIMIT_CAP)
    return None


def format_analytics_numeric_hints(profile: DatasetProfile | None) -> str:
    """One line per numeric column from the stored profile (helps pick revenue vs quantity)."""
    if profile is None:
        return ""
    lines: list[str] = []
    for name in sorted(profile.columns.keys()):
        cp = profile.columns[name]
        if cp.logical_type not in ("integer", "float"):
            continue
        bits: list[str] = []
        if cp.mean_value is not None:
            bits.append(f"mean≈{cp.mean_value}")
        if cp.min_value is not None:
            bits.append(f"min={cp.min_value}")
        if cp.max_value is not None:
            bits.append(f"max={cp.max_value}")
        if bits:
            lines.append(f"  - {name}: {', '.join(bits)}")
    return "\n".join(lines) if lines else ""


def _first_volume_like_numeric_column(
    column_names: list[str],
    column_types: dict[str, str],
) -> str | None:
    vol_tokens = ("quantity", "units", "qty", "volume")
    for c in column_names:
        if column_types.get(c) not in ("integer", "float"):
            continue
        n = c.lower().replace("_", " ")
        if any(t in n for t in vol_tokens):
            return c
    return None


def _volume_question_prefers_sum_not_rowcount(query: str) -> bool:
    """True when 'most X' means total units/revenue, not COUNT(*)."""
    q = query.lower()
    if re.search(
        r"\b(how many|number of|count of)\b.{0,40}\b(order|transaction|row|record)s?\b",
        q,
    ):
        return False
    if re.search(r"\bmost\s+(orders?|transactions?|rows?|records?)\b", q):
        return False
    if re.search(
        r"\b(buys?|bought|purchase|purchased|ordered|order(s)?\s+(of|for))\b",
        q,
    ):
        return True
    if re.search(
        r"\b(most|largest|highest|greatest|biggest)\b.{0,60}\b"
        r"(quantity|quantities|units|qty|volume|fruit|fruits|vegetable|product)\b",
        q,
    ):
        return True
    if re.search(
        r"\b(quantity|quantities|units|qty|volume)\b.{0,40}\b(most|largest|highest)\b",
        q,
    ):
        return True
    return False


def repair_rowcount_plan_to_quantity_sum(
    plan: AnalyticsPlan,
    user_query: str,
    column_names: list[str],
    column_types: dict[str, str],
) -> AnalyticsPlan:
    """If the model used row counts for a volume-style question, switch to groupby_sum."""
    if plan.operation != "groupby_count":
        return plan
    if not _volume_question_prefers_sum_not_rowcount(user_query):
        return plan
    vol = _first_volume_like_numeric_column(column_names, column_types)
    if not vol:
        return plan
    group_dim = plan.group_by or plan.target_column
    if not group_dim:
        return plan
    return plan.model_copy(
        update={
            "operation": "groupby_sum",
            "target_column": vol,
            "group_by": group_dim,
            "order": "value_desc",
        }
    )


def _superlative_which_who_groupby_query(query: str) -> bool:
    """True for which/who + superlative + a plausible aggregate measure in one question."""
    q = query.lower()
    if not re.search(r"\b(which|who)\b", q):
        return False
    if not re.search(
        r"\b(most|highest|best|top|largest|greatest|biggest)\b",
        q,
    ):
        return False
    return bool(
        re.search(
            r"\b(profit|revenue|sales|margin|quantity|units|qty|volume|amount|income)\b",
            q,
        )
    )


def _normalize_header_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.lower().replace("_", " ")).strip()


def _header_matches_salesperson_dimension(norm: str) -> bool:
    """norm is lowercase header with spaces (underscores collapsed)."""
    compact = norm.replace(" ", "")
    if "salesperson" in norm or "sales person" in norm:
        return True
    if "salesrep" in compact:
        return True
    if re.search(r"\bsales\s+rep\b", norm):
        return True
    if "representative" in norm and "product" not in norm:
        return True
    if re.search(r"\bemployee\b", norm) and "count" not in norm:
        return True
    if "associate" in norm and "product" not in norm:
        return True
    return False


def _first_salesperson_like_string_column(
    column_names: list[str],
    column_types: dict[str, str],
) -> str | None:
    for c in column_names:
        if column_types.get(c) != "string":
            continue
        if _header_matches_salesperson_dimension(_normalize_header_name(c)):
            return c
    return None


def _first_numeric_column_matching_name_tokens(
    column_names: list[str],
    column_types: dict[str, str],
    tokens: tuple[str, ...],
) -> str | None:
    for c in column_names:
        if column_types.get(c) not in ("integer", "float"):
            continue
        n = _normalize_header_name(c)
        if any(t in n for t in tokens):
            return c
    return None


def _first_money_like_numeric_column(
    column_names: list[str],
    column_types: dict[str, str],
) -> str | None:
    vol_tokens = ("quantity", "units", "qty", "volume")
    money_tokens = (
        "revenue",
        "sales",
        "profit",
        "amount",
        "cost",
        "price",
        "margin",
        "total",
        "subtotal",
        "line total",
    )
    for c in column_names:
        if column_types.get(c) not in ("integer", "float"):
            continue
        n = _normalize_header_name(c)
        if any(t in n for t in vol_tokens):
            continue
        if any(t in n for t in money_tokens):
            return c
    return None


def _pick_superlative_measure_column(
    user_query: str,
    column_names: list[str],
    column_types: dict[str, str],
) -> str | None:
    q = user_query.lower()
    if re.search(r"\b(profit|profitable|margin)\b", q):
        m = _first_numeric_column_matching_name_tokens(
            column_names,
            column_types,
            ("profit", "margin", "net income", "ebitda"),
        )
        if m:
            return m
    if re.search(
        r"\b(quantity|quantities|units|qty|volume)\b.{0,60}\b(most|highest|top|largest|greatest|biggest)\b",
        q,
    ) or re.search(
        r"\b(most|highest|top|largest|greatest|biggest)\b.{0,60}\b(quantity|quantities|units|qty|volume)\b",
        q,
    ):
        v = _first_volume_like_numeric_column(column_names, column_types)
        if v:
            return v
    return (
        _first_money_like_numeric_column(column_names, column_types)
        or _first_volume_like_numeric_column(column_names, column_types)
    )


def repair_select_rows_to_groupby_superlative(
    plan: AnalyticsPlan,
    user_query: str,
    column_names: list[str],
    column_types: dict[str, str],
) -> AnalyticsPlan:
    """If the model used select_rows for a which/who + superlative + metric question, use groupby_sum."""
    if plan.operation != "select_rows":
        return plan
    if not _superlative_which_who_groupby_query(user_query):
        return plan
    measure = _pick_superlative_measure_column(
        user_query,
        column_names,
        column_types,
    )
    group_dim = _first_salesperson_like_string_column(column_names, column_types)
    if not measure or not group_dim:
        return plan
    top_n = 1
    return plan.model_copy(
        update={
            "operation": "groupby_sum",
            "target_column": measure,
            "group_by": group_dim,
            "order": "value_desc",
            "top_n": top_n,
            "select_columns": None,
        }
    )


def format_suggested_measure_picks(
    column_names: list[str],
    column_types: dict[str, str],
) -> str:
    """Name-based hints so 'buys the most' maps to SUM(quantity) not COUNT(rows)."""
    vol_tokens = ("quantity", "units", "qty", "volume")
    money_tokens = (
        "revenue",
        "sales",
        "profit",
        "amount",
        "cost",
        "price",
        "margin",
        "total",
        "subtotal",
        "line total",
    )
    vol_col: str | None = None
    money_col: str | None = None
    for c in column_names:
        if column_types.get(c) not in ("integer", "float"):
            continue
        n = c.lower().replace("_", " ")
        is_volumeish = any(t in n for t in vol_tokens)
        if vol_col is None and is_volumeish:
            vol_col = c
        if money_col is None and not is_volumeish:
            if any(t in n for t in money_tokens):
                money_col = c
    lines: list[str] = []
    if vol_col:
        lines.append(
            f"  - For how much product / units / 'buys the most' / ordered quantity: "
            f"use operation groupby_sum with target_column={vol_col!r} — never use "
            f"groupby_count for that intent (counts rows, not units)."
        )
    if money_col:
        lines.append(
            f"  - For spend / revenue / sales / money totals: use groupby_sum with "
            f"target_column={money_col!r}."
        )
    if not lines:
        return ""
    return (
        "\n\nSUGGESTED MEASURE COLUMNS (superlatives over customers/regions/products):\n"
        + "\n".join(lines)
    )


def apply_select_rows_limit_from_user_query(plan: AnalyticsPlan, user_query: str) -> AnalyticsPlan:
    """When the user explicitly asks for N rows, override planner limit (capped at 500)."""
    if plan.operation != "select_rows":
        return plan
    inferred = infer_select_rows_limit_from_query(user_query)
    if inferred is None:
        return plan
    return plan.model_copy(update={"limit": inferred})


def parse_analytics_plan_json(plan_json_text: str) -> AnalyticsPlan:
    """Validate raw JSON text from the LLM into a typed AnalyticsPlan."""
    raw = plan_json_text.strip()
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        start, end = raw.find("{"), raw.rfind("}")
        if start != -1 and end > start:
            obj = json.loads(raw[start : end + 1])
        else:
            raise
    return AnalyticsPlan.model_validate(obj)


__all__ = [
    "apply_select_rows_limit_from_user_query",
    "format_analytics_numeric_hints",
    "format_suggested_measure_picks",
    "infer_select_rows_limit_from_query",
    "parse_analytics_plan_json",
    "repair_rowcount_plan_to_quantity_sum",
    "repair_select_rows_to_groupby_superlative",
]
