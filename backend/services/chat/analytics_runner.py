"""Analytics and forecast plan generation, execution, and answer formatting for chat."""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from typing import Any

from pydantic import ValidationError

from ...analytics.chart_builder import build_forecast_line_chart
from ...analytics.dataset_summary import DatasetSummary
from ...analytics.display_markdown import format_analytics_result_markdown
from ...analytics.errors import AnalyticsError
from ...analytics.executor import AnalyticsExecutor
from ...analytics.forecaster import compute_filtered_forecast
from ...analytics.models import (
    AnalyticsPlan,
    AnalyticsResult,
    AnalyticsUnavailable,
    DatasetProfile,
    ForecastChatPayload,
    ForecastPlan,
)
from ...analytics.planner import effective_analytics_document_ids
from ...analytics.predictive import validate_horizon
from ...analytics.router import AnalyticsRouter
from ...analytics.validator import validate_forecast_result
from ...integrations import LLMClient
from ...repositories import DocumentRepository
from .analytics_planning import (
    apply_select_rows_limit_from_user_query,
    format_analytics_numeric_hints,
    format_suggested_measure_picks,
    parse_analytics_plan_json,
    repair_rowcount_plan_to_quantity_sum,
    repair_select_rows_to_groupby_superlative,
)
from .prompts import build_analytics_system_prompt, build_forecast_system_prompt
from .types import ChatResult

logger = logging.getLogger(__name__)


class AnalyticsChatRunner:
    """Runs tabular analytics and forecast flows used by ChatService."""

    def __init__(
        self,
        llm: LLMClient,
        analytics_executor: AnalyticsExecutor,
        document_repo: DocumentRepository,
        analytics_router: AnalyticsRouter,
    ) -> None:
        self._llm = llm
        self._analytics_executor = analytics_executor
        self._docs = document_repo
        self._analytics_router = analytics_router

    def analytics_source_payload(self, ar: AnalyticsResult) -> dict[str, Any]:
        filename: str | None = None
        if ar.document_id:
            info = self._docs.get_document(ar.document_id)
            if info:
                filename = info.filename
        return {
            "url": "analytics://tabular",
            "snippet": (ar.summary[:500] if ar.summary else ""),
            "retrieval_type": "document_keyword",
            "source_type": "document",
            "source_kind": "analytics",
            "document_id": ar.document_id,
            "sheet_name": ar.sheet_name,
            "display_name": filename or ar.document_id or "Analytics",
        }

    def forecast_source_payload(self, fc: ForecastChatPayload) -> dict[str, Any]:
        return {
            "url": "forecast://artifact",
            "snippet": f"{fc.measure} forecast ({fc.model})",
            "retrieval_type": "document_keyword",
            "source_type": "document",
            "source_kind": "analytics",
            "document_id": fc.document_id,
            "sheet_name": fc.sheet,
            "display_name": fc.document or fc.document_id,
        }

    def format_deterministic_analytics_answer(self, ar: AnalyticsResult) -> str:
        cols: dict = {}
        if ar.document_id:
            cols = self._analytics_executor.metadata_repo.get_columns(
                ar.document_id, ar.sheet_name
            )
        body = format_analytics_result_markdown(ar, cols)
        return f"{body}\n\nSource: deterministic analytics"

    def build_forecast_chat_result(
        self, fc_res: ForecastChatPayload
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        chart_spec = build_forecast_line_chart(fc_res)
        payload_dict = dataclasses.asdict(fc_res)
        source_payload = self.forecast_source_payload(fc_res)
        return payload_dict, chart_spec, source_payload

    async def forecast_narration(self, payload_json: str, query: str) -> str:
        narr = await self._llm.complete(
            "You are a helpful analyst. In 2–4 sentences, summarize the baseline "
            "linear-trend forecast below for the user. Note that intervals are "
            "approximate (residual std × 1.96).",
            f"Forecast:\n{payload_json}\n\nUser question: {query}",
        )
        return (narr.content or "").strip()

    async def execute_forecast_plan(
        self,
        plan: ForecastPlan,
        summary: DatasetSummary,
    ) -> ChatResult | None:
        valid, reason = validate_horizon(plan.horizon, summary.frequency)
        if not valid:
            return ChatResult(
                answer=f"I cannot produce this forecast. {reason}",
                mode="OFFLINE_ARCHIVE",
                contexts=[],
            )

        meta = self._analytics_executor.metadata_repo
        table_name = meta.get_table_name(summary.document_id, summary.sheet_name)
        if table_name is None:
            return None

        columns = meta.get_columns(summary.document_id, summary.sheet_name)
        if not columns:
            return None

        filename: str | None = None
        info = self._docs.get_document(summary.document_id)
        if info:
            filename = info.filename

        try:
            payload = await asyncio.to_thread(
                compute_filtered_forecast,
                self._analytics_executor.metadata_repo._conn,
                table_name,
                columns,
                plan,
                filename=filename,
                sheet_name=summary.sheet_name,
            )
        except Exception as exc:
            logger.warning("Forecast execution failed: %s", exc)
            return None

        validation = validate_forecast_result(payload)

        payload_dict, chart_spec, source_payload = self.build_forecast_chat_result(payload)
        payload_json = json.dumps(payload_dict, indent=2, sort_keys=True)
        body = await self.forecast_narration(payload_json, "")

        if validation.warnings:
            body += "\n\n*Note: " + " ".join(validation.warnings[:2]) + "*"

        answer = f"{body}\n\n**Forecast (baseline)**\n```json\n{payload_json}\n```\n"
        return ChatResult(
            answer=answer,
            mode="OFFLINE_ARCHIVE",
            contexts=[],
            attached_sources=[source_payload],
            forecast=payload_dict,
            chart=chart_spec,
        )

    async def execute_analytics_plan(self, plan: AnalyticsPlan) -> ChatResult:
        try:
            result = await asyncio.to_thread(self._analytics_executor.execute, plan)
            answer = self.format_deterministic_analytics_answer(result)
            return ChatResult(
                answer=answer,
                mode="OFFLINE_ARCHIVE",
                contexts=[],
                attached_sources=[self.analytics_source_payload(result)],
            )
        except AnalyticsError as exc:
            logger.warning("Analytics execution failed: %s", exc)
            return ChatResult(
                answer=(
                    "I could not run that analysis on your spreadsheet. "
                    "Try rephrasing the question or check that filters match the file columns."
                ),
                mode="OFFLINE_ARCHIVE",
                contexts=[],
                attached_sources=None,
            )

    async def generate_analytics_plan(
        self, *, user_query: str, document_id: str
    ) -> AnalyticsPlan | None:
        meta = self._analytics_executor.metadata_repo
        sheet_name = meta.resolve_default_sheet_name(document_id)
        if sheet_name is None:
            return None

        columns = meta.get_columns(document_id, sheet_name)
        if not columns:
            return None

        column_names = [c for c in columns if not c.startswith("_")]
        column_types = {c: m.logical_type for c, m in columns.items() if not c.startswith("_")}
        profile = meta.get_profile(document_id, sheet_name)
        profile_values: dict[str, list[str]] = {}
        if profile:
            for col_name, col_prof in profile.columns.items():
                if col_prof.logical_type == "string" and col_prof.top_values:
                    profile_values[col_name] = list(col_prof.top_values.keys())
        time_hint = meta.get_timeseries_time_column(document_id, sheet_name)
        numeric_hints = format_analytics_numeric_hints(profile)
        measure_pick_hints = format_suggested_measure_picks(column_names, column_types)
        system_prompt = build_analytics_system_prompt(
            column_names,
            document_id,
            column_types,
            profile_values=profile_values or None,
            suggested_time_column=time_hint,
            numeric_hints=numeric_hints,
            measure_pick_hints=measure_pick_hints,
        )

        try:
            resp = await self._llm.complete(system_prompt, user_query, temperature=0.0)
            plan = parse_analytics_plan_json(resp.content)
            plan = apply_select_rows_limit_from_user_query(plan, user_query)
            plan = repair_rowcount_plan_to_quantity_sum(
                plan, user_query, column_names, column_types
            )
            plan = repair_select_rows_to_groupby_superlative(
                plan, user_query, column_names, column_types
            )
            return plan
        except (json.JSONDecodeError, ValidationError, Exception) as exc:
            logger.warning("Analytics plan generation/validation failed: %s", exc)
            return None

    async def generate_forecast_plan(
        self, *, user_query: str, document_id: str
    ) -> ForecastPlan | None:
        meta = self._analytics_executor.metadata_repo
        sheet_name = meta.resolve_default_sheet_name(document_id)
        if sheet_name is None:
            return None

        columns = meta.get_columns(document_id, sheet_name)
        if not columns:
            return None

        column_names = [c for c in columns if not c.startswith("_")]
        column_types = {
            c: m.logical_type for c, m in columns.items() if not c.startswith("_")
        }

        profile = meta.get_profile(document_id, sheet_name)
        profile_values: dict[str, list[str]] = {}
        if profile:
            for col_name, col_prof in profile.columns.items():
                if col_prof.logical_type == "string" and col_prof.top_values:
                    profile_values[col_name] = list(col_prof.top_values.keys())

        system_prompt = build_forecast_system_prompt(
            column_names, column_types, document_id, profile_values
        )

        try:
            resp = await self._llm.complete(system_prompt, user_query, temperature=0.0)
            raw = resp.content.strip()
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                start, end = raw.find("{"), raw.rfind("}")
                if start != -1 and end > start:
                    obj = json.loads(raw[start : end + 1])
                else:
                    raise
            return ForecastPlan.model_validate(obj)
        except (json.JSONDecodeError, ValidationError, Exception) as exc:
            logger.warning("Forecast plan generation failed: %s", exc)
            return None

    async def try_on_demand_forecast(
        self,
        query: str,
        document_ids: list[str],
    ) -> ForecastChatPayload | None:
        meta = self._analytics_executor.metadata_repo
        resolved = effective_analytics_document_ids(meta, document_ids)
        if not isinstance(resolved, list) or not resolved:
            return None

        for doc_id in resolved:
            plan = await self.generate_forecast_plan(user_query=query, document_id=doc_id)
            if plan is None:
                continue

            sheet_name = plan.sheet_name or meta.resolve_default_sheet_name(doc_id)
            if sheet_name is None:
                continue

            table_name = meta.get_table_name(doc_id, sheet_name)
            if table_name is None:
                continue

            columns = meta.get_columns(doc_id, sheet_name)
            if not columns:
                continue

            filename: str | None = None
            info = self._docs.get_document(doc_id)
            if info:
                filename = info.filename

            try:
                payload = await asyncio.to_thread(
                    compute_filtered_forecast,
                    self._analytics_executor.metadata_repo._conn,
                    table_name,
                    columns,
                    plan,
                    filename=filename,
                    sheet_name=sheet_name,
                )
                return payload
            except Exception as exc:
                logger.warning("On-demand forecast failed for %s: %s", doc_id, exc)
                continue

        return None

    async def try_analytics(
        self, query: str, doc_ids: list[str] | None
    ) -> AnalyticsResult | AnalyticsUnavailable | None:
        decision = self._analytics_router.decide(query)
        if not decision.use_analytics:
            return None

        meta = self._analytics_executor.metadata_repo
        resolved = effective_analytics_document_ids(meta, doc_ids)
        if isinstance(resolved, AnalyticsUnavailable):
            return resolved
        if resolved is None:
            return None
        effective_ids = resolved

        for doc_id in effective_ids:
            try:
                plan = await self.generate_analytics_plan(user_query=query, document_id=doc_id)
                if plan is None:
                    continue
                result = await asyncio.to_thread(self._analytics_executor.execute, plan)
                return result
            except AnalyticsError as exc:
                logger.warning("Analytics execution failed for doc %s: %s", doc_id, exc)
                continue

        return None

    def get_profile_for_documents(
        self, document_ids: list[str]
    ) -> tuple[str | None, DatasetProfile | None]:
        meta = self._analytics_executor.metadata_repo
        for doc_id in document_ids:
            sheet = meta.resolve_default_sheet_name(doc_id)
            if sheet is None:
                continue
            profile = meta.get_profile(doc_id, sheet)
            if profile is not None:
                return doc_id, profile
        return None, None
