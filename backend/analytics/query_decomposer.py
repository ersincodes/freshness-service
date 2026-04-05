"""Unified LLM-powered query understanding.

Replaces the fragile keyword-based routing (is_predictive_intent,
AnalyticsRouter.decide, query_has_filter_intent) with a single LLM call
that classifies intent and produces a structured execution plan.
"""
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from pydantic import ValidationError

from .dataset_summary import DatasetSummary
from .models import QueryPlan

if TYPE_CHECKING:
    from ..integrations.llm_client import LLMClient

logger = logging.getLogger(__name__)


class QueryDecomposer:
    """Classifies user intent and generates structured execution plans.

    Single responsibility: take a user query + dataset context, return a QueryPlan.
    Does NOT execute anything.
    """

    def __init__(self, llm_client: LLMClient) -> None:
        self._llm = llm_client

    async def decompose(
        self,
        query: str,
        summary: DatasetSummary,
    ) -> QueryPlan:
        """Decompose a user question into an actionable QueryPlan."""
        system_prompt = self._build_system_prompt(summary)
        try:
            resp = await self._llm.complete(system_prompt, query, temperature=0.0)
            return self._parse_response(resp.content, summary.document_id)
        except Exception as exc:
            logger.warning("QueryDecomposer failed: %s", exc)
            return QueryPlan(intent="rag", reason=f"decomposition_error: {exc}")

    def _parse_response(self, raw: str, document_id: str) -> QueryPlan:
        """Parse LLM JSON output into a validated QueryPlan."""
        raw = raw.strip()
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            start, end = raw.find("{"), raw.rfind("}")
            if start != -1 and end > start:
                obj = json.loads(raw[start : end + 1])
            else:
                return QueryPlan(intent="rag", reason="unparseable_response")

        try:
            plan = QueryPlan.model_validate(obj)
        except ValidationError as exc:
            logger.warning("QueryPlan validation failed: %s", exc)
            return QueryPlan(intent="rag", reason=f"validation_error: {exc}")

        if plan.analytics_plan and not plan.analytics_plan.document_id:
            plan.analytics_plan = plan.analytics_plan.model_copy(
                update={"document_id": document_id}
            )
        if plan.forecast_plan and not plan.forecast_plan.document_id:
            plan.forecast_plan = plan.forecast_plan.model_copy(
                update={"document_id": document_id}
            )

        return plan

    # ------------------------------------------------------------------
    # Prompt construction
    # ------------------------------------------------------------------

    def _build_system_prompt(self, summary: DatasetSummary) -> str:
        columns_block = self._format_columns(summary)
        values_block = self._format_categorical_values(summary)
        date_block = self._format_date_context(summary)
        numeric_block = self._format_numeric_hints(summary)
        measure_block = self._format_measure_picks(summary)

        return (
            "You are a data analysis planner. Given a user question about a spreadsheet dataset, "
            "you must classify the intent and produce a structured JSON plan.\n\n"
            "You MUST output ONLY valid JSON — no markdown fences, no commentary.\n\n"
            "DATASET CONTEXT:\n"
            f"  - Document ID: {json.dumps(summary.document_id)}\n"
            f"  - Sheet: {summary.sheet_name}\n"
            f"  - Row count: {summary.row_count}\n"
            f"{date_block}"
            f"\n\nAVAILABLE COLUMNS:\n{columns_block}"
            f"{values_block}"
            f"{numeric_block}"
            f"{measure_block}"
            "\n\n"
            "OUTPUT FORMAT — return exactly ONE JSON object with this shape:\n"
            "{\n"
            '  "intent": "<analytics | forecast | rag | cannot_answer>",\n'
            '  "reason": "<brief explanation of why this intent was chosen>",\n'
            '  "analytics_plan": { ... } or null,\n'
            '  "forecast_plan": { ... } or null,\n'
            '  "cannot_answer_reason": "<explanation>" or null\n'
            "}\n\n"
            + self._analytics_plan_schema(summary)
            + self._forecast_plan_schema(summary)
            + self._intent_rules()
        )

    def _format_columns(self, summary: DatasetSummary) -> str:
        lines: list[str] = []
        for col_name, meta in summary.columns.items():
            if col_name.startswith("_"):
                continue
            lines.append(f"  - {col_name} (type: {meta.logical_type})")
        return "\n".join(lines)

    def _format_categorical_values(self, summary: DatasetSummary) -> str:
        if not summary.categorical_values:
            return ""
        parts: list[str] = []
        for col, vals in sorted(summary.categorical_values.items()):
            parts.append(f"  - {col}: {', '.join(vals)}")
        return (
            "\n\nKNOWN CATEGORICAL VALUES (use exact spelling for eq filters):\n"
            + "\n".join(parts)
        )

    def _format_date_context(self, summary: DatasetSummary) -> str:
        parts: list[str] = []
        if summary.time_column:
            parts.append(f"  - Time column: {summary.time_column}")
        if summary.date_range:
            parts.append(f"  - Data spans: {summary.date_range[0]} to {summary.date_range[1]}")
        if summary.frequency != "unknown":
            parts.append(f"  - Detected frequency: {summary.frequency}")
        if summary.eligible_measures:
            parts.append(f"  - Forecastable measures: {', '.join(summary.eligible_measures)}")
        if not parts:
            return ""
        return "\n" + "\n".join(parts)

    def _format_numeric_hints(self, summary: DatasetSummary) -> str:
        lines: list[str] = []
        for name in sorted(summary.profile.columns.keys()):
            cp = summary.profile.columns[name]
            if cp.logical_type not in ("integer", "float"):
                continue
            bits: list[str] = []
            if cp.mean_value is not None:
                bits.append(f"mean={cp.mean_value}")
            if cp.min_value is not None:
                bits.append(f"min={cp.min_value}")
            if cp.max_value is not None:
                bits.append(f"max={cp.max_value}")
            if bits:
                lines.append(f"  - {name}: {', '.join(bits)}")
        if not lines:
            return ""
        return "\n\nNUMERIC COLUMN HINTS:\n" + "\n".join(lines)

    def _format_measure_picks(self, summary: DatasetSummary) -> str:
        col_types = {
            c: m.logical_type for c, m in summary.columns.items()
            if not c.startswith("_")
        }
        vol_tokens = ("quantity", "units", "qty", "volume")
        money_tokens = (
            "revenue", "sales", "profit", "amount", "cost",
            "price", "margin", "total", "subtotal",
        )
        vol_col: str | None = None
        money_col: str | None = None
        for c, lt in col_types.items():
            if lt not in ("integer", "float"):
                continue
            n = c.lower().replace("_", " ")
            if vol_col is None and any(t in n for t in vol_tokens):
                vol_col = c
            if money_col is None and not any(t in n for t in vol_tokens):
                if any(t in n for t in money_tokens):
                    money_col = c
        lines: list[str] = []
        if vol_col:
            lines.append(
                f"  - For units/quantity/volume questions: use groupby_sum with "
                f"target_column={vol_col!r}"
            )
        if money_col:
            lines.append(
                f"  - For revenue/sales/money questions: use groupby_sum with "
                f"target_column={money_col!r}"
            )
        if not lines:
            return ""
        return "\n\nSUGGESTED MEASURE COLUMNS:\n" + "\n".join(lines)

    def _analytics_plan_schema(self, summary: DatasetSummary) -> str:
        time_hint = ""
        if summary.time_column:
            time_hint = (
                f"DETECTED DATE COLUMN: {summary.time_column!r}\n"
                "Use it as time_column with time_grain month/year/week for trends.\n\n"
            )

        return (
            "ANALYTICS PLAN (when intent=analytics):\n"
            "analytics_plan must have this shape:\n"
            "{\n"
            f'  "document_id": {json.dumps(summary.document_id)},\n'
            '  "operation": "<count_rows | count_distinct | sum | avg | min | max | '
            'groupby_count | groupby_sum | groupby_avg | groupby_min | groupby_max | '
            'groupby_ratio | select_rows>",\n'
            '  "target_column": "<column or null>",\n'
            '  "group_by": "<column or null>",\n'
            '  "time_column": "<date column or null>",\n'
            '  "time_grain": "<none | month | year | week>",\n'
            '  "filters": [{"column": "...", "operator": "...", "value": ...}],\n'
            '  "order": "count_desc",\n'
            '  "top_n": 50,\n'
            '  "limit": 50,\n'
            '  "select_columns": null,\n'
            '  "denominator_column": null\n'
            "}\n\n"
            f"{time_hint}"
            "ANALYTICS RULES:\n"
            "- You must NEVER generate SQL.\n"
            "- Allowed filter operators: eq, neq, gt, gte, lt, lte, contains, startswith, "
            "year_equals (int year), month_equals (YYYY-MM), between_dates ([start, end]), "
            "is_null, is_not_null.\n"
            "- target_column is REQUIRED for count_distinct, sum, avg, min, max, groupby_sum, groupby_avg, "
            "groupby_min, groupby_max, groupby_ratio.\n"
            "- groupby_ratio always needs target_column (numerator), denominator_column, and group_by; "
            "return rate by region → SUM(return-like)/SUM(order-or-qty-like) grouped by Region; use order ratio_desc "
            "to rank highest rate first.\n"
            "- groupby_count counts ROWS — use it only for 'how many orders/transactions'. "
            "For 'who buys the most', use groupby_sum on a quantity/revenue column.\n"
            "- For 'which/who + superlative + metric', use groupby_sum with order=value_desc, top_n=1.\n"
            "- For 'average <metric> by <dimension>' or 'which <dimension> has the highest/lowest/min/max "
            "average <metric>', use groupby_avg with target_column=<metric>, group_by=<dimension>. "
            "Use order=value_asc for lowest/min average, order=value_desc for highest/max average.\n"
            "- For fastest/shortest/min time per group (e.g. shipping time by warehouse), use groupby_min; "
            "for slowest/longest use groupby_max.\n"
            "- time_grain is only valid with groupby_sum, groupby_avg, groupby_min, groupby_max, or groupby_count. "
            "For scalar totals (e.g. total revenue this year) use sum/avg with filters, time_grain none.\n"
            "- Column names must be ORIGINAL header names from the list above.\n"
            "- Map user phrases to KNOWN CATEGORICAL VALUES (e.g. 'fruits' -> 'Fruits').\n"
            "- Total or sum for a calendar year: use operation sum on the money column; if there is an "
            "integer Year column, filter with operator eq and integer value (e.g. 2025). Use year_equals "
            "only on columns typed as date in AVAILABLE COLUMNS (e.g. OrderDate). Never use year_equals "
            "on integer or string columns.\n"
            "- Example (replace names with actual headers): total revenue in 2025 → intent analytics, "
            "analytics_plan with operation sum, target_column set to the revenue column, "
            'filters [{"column":"Year","operator":"eq","value":2025}] when Year is integer.\n\n'
        )

    def _forecast_plan_schema(self, summary: DatasetSummary) -> str:
        horizon_guidance = ""
        if summary.date_range:
            horizon_guidance = (
                f"HORIZON CALCULATION:\n"
                f"Data ends at {summary.date_range[1]}. "
                f"Frequency is {summary.frequency}. "
                "You MUST compute horizon as the number of periods from the last data date "
                "to the end of the user's requested window.\n"
                "Example: if data ends 2025-12-30 and user asks for 'year 2026' with monthly frequency, "
                "horizon = 12 (Jan-Dec 2026), requested_start = '2026-01-01', requested_end = '2026-12-31'.\n"
                "Example: if user asks for 'next 6 months', horizon = 6.\n"
                "Maximum horizons: monthly=36, quarterly=12, yearly=5. "
                "If the user's request exceeds this, set intent=cannot_answer.\n\n"
            )

        return (
            "FORECAST PLAN (when intent=forecast):\n"
            "forecast_plan must have this shape:\n"
            "{\n"
            f'  "document_id": {json.dumps(summary.document_id)},\n'
            '  "measure_column": "<numeric column to forecast>",\n'
            '  "time_column": "<date column or null>",\n'
            '  "filters": [{"column": "...", "operator": "...", "value": ...}],\n'
            '  "horizon": <integer>,\n'
            '  "filter_label": "<human label or null>",\n'
            '  "requested_start": "<YYYY-MM-DD or null>",\n'
            '  "requested_end": "<YYYY-MM-DD or null>"\n'
            "}\n\n"
            f"{horizon_guidance}"
            "FORECAST RULES:\n"
            "- measure_column must be a numeric column (integer or float).\n"
            "- If the user mentions a category/product/region, add a filter.\n"
            "- If no specific measure is mentioned, default to the most relevant "
            "business metric (Revenue > Profit > Quantity).\n"
            "- Column names must be ORIGINAL header names.\n"
            "- If the user asks for the next N months with monthly frequency, set horizon=N and "
            "either set requested_end to on or after the last day of the N-th future month "
            "(e.g. data ends Dec 2025, N=3 → end >= 2026-03-31) or set requested_start and "
            "requested_end to null and rely on horizon alone.\n\n"
        )

    def _intent_rules(self) -> str:
        return (
            "INTENT CLASSIFICATION RULES:\n"
            "1. intent=forecast: user asks about future values, predictions, projections, "
            "trends forward, 'what will happen', 'next month/year', 'prediction for 2026', etc.\n"
            "2. intent=analytics: user asks about existing data — aggregations (total, average, "
            "count, sum), comparisons, rankings, listings, breakdowns, trends over historical periods.\n"
            "3. intent=cannot_answer: the question cannot be answered from this dataset. Examples:\n"
            "   - Question is unrelated to the data (e.g. 'what is the weather?')\n"
            "   - Requested forecast horizon exceeds maximum\n"
            "   - Required column/data does not exist\n"
            "   - The computation is not supported (e.g. complex statistical tests)\n"
            "   Set cannot_answer_reason to a clear, user-friendly explanation.\n"
            "4. intent=rag: the question is about the dataset but cannot be answered with "
            "analytics or forecast operations (e.g. 'describe this dataset', 'what insights can you find').\n\n"
            "CRITICAL: Never fabricate data values. If you cannot compute the answer "
            "deterministically, use cannot_answer.\n"
        )
