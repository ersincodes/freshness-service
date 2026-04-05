"""
Chat service for RAG-based question answering with deterministic analytics path.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
from typing import AsyncIterator, Any

from ..analytics.dataset_summary import DatasetSummary, build_dataset_summary
from ..analytics.executor import AnalyticsExecutor
from ..analytics.metadata_repository import MetadataRepository
from ..analytics.models import (
    AnalyticsPlan,
    AnalyticsResult,
    AnalyticsUnavailable,
    DatasetProfile,
    ForecastChatPayload,
    ForecastPlan,
    ForecastUnavailable,
    QueryPlan,
)
from ..analytics.predictive import (
    is_predictive_intent,
    query_has_filter_intent,
    resolve_forecast_for_chat,
)
from ..analytics.query_decomposer import QueryDecomposer
from ..analytics.router import AnalyticsRouter
from ..config import Settings
from ..domain import (
    DOC_URL_PREFIX,
    SourceContext,
    determine_retrieval_type,
    context_to_source_dict,
    FALLBACK_SOURCE_URL,
    ErrorCode,
)
from ..integrations import LLMClient, BraveClient
from ..repositories import ArchiveRepository, DocumentRepository

from .chat.analytics_planning import (
    apply_select_rows_limit_from_user_query,
    infer_select_rows_limit_from_query,
    repair_forecast_plan_horizon,
    repair_rowcount_plan_to_quantity_sum,
    repair_select_rows_to_groupby_superlative,
)
from .chat.analytics_runner import AnalyticsChatRunner
from .chat.context import ChatRetrievalDeps, gather_contexts
from .chat.intents import strip_filename_from_query
from .chat.prompts import answer_prompt, extraction_prompt, has_usable_context
from .chat.types import ChatResult, StreamEvent

logger = logging.getLogger(__name__)

_repair_rowcount_plan_to_quantity_sum = repair_rowcount_plan_to_quantity_sum
_repair_select_rows_to_groupby_superlative = repair_select_rows_to_groupby_superlative


class ChatService:
    """Service for RAG-based chat functionality with analytics path."""
    
    def __init__(
        self,
        settings: Settings,
        llm_client: LLMClient,
        brave_client: BraveClient,
        archive_repo: ArchiveRepository,
        document_repo: DocumentRepository,
        metadata_repo: MetadataRepository | None = None,
    ) -> None:
        self._s = settings
        self._llm = llm_client
        self._brave = brave_client
        self._archive = archive_repo
        self._docs = document_repo
        self._analytics_router = AnalyticsRouter()
        self._analytics_executor = AnalyticsExecutor(metadata_repo) if metadata_repo else None
        self._runner = (
            AnalyticsChatRunner(
                llm_client,
                self._analytics_executor,
                document_repo,
                self._analytics_router,
            )
            if self._analytics_executor
            else None
        )
        self._decomposer = QueryDecomposer(llm_client) if metadata_repo else None

    def _rag_deps(self) -> ChatRetrievalDeps:
        return ChatRetrievalDeps(self._s, self._archive, self._docs, self._brave)

    # ------------------------------------------------------------------
    # Analytics path
    # ------------------------------------------------------------------

    def _can_use_analytics(self) -> bool:
        return (
            self._s.enable_tabular_analytics
            and self._analytics_executor is not None
        )

    def _can_use_decomposer(self) -> bool:
        return self._can_use_analytics() and self._decomposer is not None

    def _get_dataset_summary(self, document_ids: list[str]) -> DatasetSummary | None:
        """Build a DatasetSummary for the first eligible document."""
        if self._analytics_executor is None:
            return None
        meta = self._analytics_executor.metadata_repo
        conn = meta._conn
        for doc_id in document_ids:
            summary = build_dataset_summary(meta, conn, doc_id)
            if summary is not None:
                return summary
        return None

    def _augment_contexts_with_document_summary(
        self,
        contexts: list[SourceContext],
        include_documents: bool,
        document_ids: list[str] | None,
    ) -> list[SourceContext]:
        """When RAG returns no real sources but a scoped spreadsheet exists, inject metadata outline."""
        if not include_documents or not document_ids:
            return contexts
        if has_usable_context(contexts):
            return contexts
        summary = self._get_dataset_summary(document_ids)
        if summary is None:
            return contexts
        info = self._docs.get_document(summary.document_id)
        filename = info.filename if info else None
        col_lines = [
            f"  - {name} ({meta.logical_type})"
            for name, meta in sorted(summary.columns.items())
            if not name.startswith("_")
        ]
        parts = [
            "Attached spreadsheet (metadata outline; use for high-level advice, not exact cells):",
            f"File: {filename or summary.document_id}",
            f"Sheet: {summary.sheet_name}",
            f"Row count: {summary.row_count}",
        ]
        if summary.date_range:
            parts.append(f"Date span: {summary.date_range[0]} — {summary.date_range[1]}")
        if summary.time_column:
            parts.append(f"Primary date column: {summary.time_column}")
        if summary.eligible_measures:
            em = summary.eligible_measures[:20]
            parts.append(f"Numeric measures: {', '.join(em)}")
        parts.append("Columns:")
        parts.extend(col_lines[:80])
        text = "\n".join(parts)
        ctx = SourceContext(
            f"{DOC_URL_PREFIX}{summary.document_id}",
            text,
            dt.datetime.now(dt.UTC).isoformat(),
            False,
            0.0,
            filename,
            None,
        )
        rest = [c for c in contexts if c.url != FALLBACK_SOURCE_URL]
        return [ctx] + rest

    async def _execute_forecast_plan(
        self,
        plan: ForecastPlan,
        summary: DatasetSummary,
    ) -> ChatResult | None:
        if self._runner is None:
            return None
        return await self._runner.execute_forecast_plan(plan, summary)

    async def _execute_analytics_plan(
        self,
        plan: AnalyticsPlan,
    ) -> ChatResult | None:
        if self._runner is None:
            return None
        return await self._runner.execute_analytics_plan(plan)

    async def _generate_analytics_plan(
        self, *, user_query: str, document_id: str
    ) -> AnalyticsPlan | None:
        if self._runner is None:
            return None
        return await self._runner.generate_analytics_plan(user_query=user_query, document_id=document_id)

    async def _generate_forecast_plan(
        self, *, user_query: str, document_id: str
    ) -> ForecastPlan | None:
        if self._runner is None:
            return None
        return await self._runner.generate_forecast_plan(user_query=user_query, document_id=document_id)

    async def _try_on_demand_forecast(
        self,
        query: str,
        document_ids: list[str],
    ) -> ForecastChatPayload | None:
        if self._runner is None:
            return None
        return await self._runner.try_on_demand_forecast(query, document_ids)

    async def _try_analytics(
        self, query: str, doc_ids: list[str] | None
    ) -> AnalyticsResult | AnalyticsUnavailable | None:
        if not self._can_use_analytics() or self._runner is None:
            return None
        return await self._runner.try_analytics(query, doc_ids)

    def _build_forecast_chat_result(
        self, fc_res: ForecastChatPayload
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        assert self._runner is not None
        return self._runner.build_forecast_chat_result(fc_res)

    async def _forecast_narration(self, payload_json: str, query: str) -> str:
        assert self._runner is not None
        return await self._runner.forecast_narration(payload_json, query)

    def _get_profile_for_documents(
        self, document_ids: list[str]
    ) -> tuple[str | None, DatasetProfile | None]:
        if self._runner is None:
            return None, None
        return self._runner.get_profile_for_documents(document_ids)

    def _analytics_source_payload(self, ar: AnalyticsResult) -> dict[str, Any]:
        assert self._runner is not None
        return self._runner.analytics_source_payload(ar)

    def _format_deterministic_analytics_answer(self, ar: AnalyticsResult) -> str:
        assert self._runner is not None
        return self._runner.format_deterministic_analytics_answer(ar)

    def _forecast_source_payload(self, fc: ForecastChatPayload) -> dict[str, Any]:
        assert self._runner is not None
        return self._runner.forecast_source_payload(fc)

    # ------------------------------------------------------------------
    # Unified query decomposition path
    # ------------------------------------------------------------------

    async def _handle_decomposed_query(
        self,
        query: str,
        document_ids: list[str],
    ) -> ChatResult | None:
        """Route through the QueryDecomposer for intelligent intent classification."""
        if self._decomposer is None:
            return None

        summary = self._get_dataset_summary(document_ids)
        if summary is None:
            return None

        cleaned_query = strip_filename_from_query(query)

        try:
            plan = await self._decomposer.decompose(cleaned_query, summary)
        except Exception as exc:
            logger.warning("Query decomposition failed: %s", exc)
            return None

        if plan.intent == "cannot_answer":
            reason = plan.cannot_answer_reason or "This question cannot be answered from the available data."
            return ChatResult(
                answer=reason,
                mode="OFFLINE_ARCHIVE",
                contexts=[],
            )

        if plan.intent == "forecast" and plan.forecast_plan:
            fc_plan = repair_forecast_plan_horizon(plan.forecast_plan, cleaned_query)
            result = await self._execute_forecast_plan(fc_plan, summary)
            if result is not None:
                return result

        if plan.intent == "analytics" and plan.analytics_plan:
            assert self._runner is not None
            repaired = self._runner.refine_analytics_plan(
                plan.analytics_plan, cleaned_query, summary
            )
            result = await self._execute_analytics_plan(repaired)
            if result is not None:
                return result

        return None

    async def _handle_legacy_analytics(
        self,
        query: str,
        document_ids: list[str],
    ) -> ChatResult | str | None:
        """Legacy keyword-based routing fallback.

        Returns ChatResult on success, a prefix string for hints, or None.
        """
        analytics_prefix = ""

        if is_predictive_intent(query):
            _, profile = self._get_profile_for_documents(document_ids)
            use_on_demand = query_has_filter_intent(query, profile)

            fc_res: ForecastChatPayload | ForecastUnavailable | None = None

            if use_on_demand:
                od_result = await self._try_on_demand_forecast(query, document_ids)
                if od_result is not None:
                    fc_res = od_result

            if fc_res is None and not use_on_demand:
                rows = self._analytics_executor.forecast_repo.list_for_documents(  # type: ignore[union-attr]
                    document_ids
                )

                def _doc_filename(did: str) -> str | None:
                    info = self._docs.get_document(did)
                    return info.filename if info else None

                fc_res = resolve_forecast_for_chat(
                    rows, get_filename=_doc_filename, user_query=query
                )

            if isinstance(fc_res, ForecastChatPayload):
                payload_dict, chart_spec, source_payload = (
                    self._build_forecast_chat_result(fc_res)
                )
                payload_json = json.dumps(payload_dict, indent=2, sort_keys=True)
                body = await self._forecast_narration(payload_json, query)
                answer = (
                    f"{body}\n\n**Forecast (baseline)**\n```json\n{payload_json}\n```\n"
                )
                return ChatResult(
                    answer=answer,
                    mode="OFFLINE_ARCHIVE",
                    contexts=[],
                    attached_sources=[source_payload],
                    forecast=payload_dict,
                    chart=chart_spec,
                )
            if isinstance(fc_res, ForecastUnavailable):
                analytics_prefix = f"*{fc_res.hint}*\n\n"

        analytics_out = await self._try_analytics(query, document_ids)
        if isinstance(analytics_out, AnalyticsUnavailable):
            analytics_prefix = f"*{analytics_out.hint}*\n\n"
        elif isinstance(analytics_out, AnalyticsResult):
            answer = self._format_deterministic_analytics_answer(analytics_out)
            return ChatResult(
                answer=answer,
                mode="OFFLINE_ARCHIVE",
                contexts=[],
                attached_sources=[self._analytics_source_payload(analytics_out)],
            )

        return analytics_prefix if analytics_prefix else None

    async def _stream_decomposed_query(
        self,
        query: str,
        document_ids: list[str],
        conversation_id: str,
    ) -> list[StreamEvent] | None:
        """Stream-compatible decomposed query handler.

        Returns a list of StreamEvents to yield, or None to fall through.
        """
        if self._decomposer is None:
            return None

        summary = self._get_dataset_summary(document_ids)
        if summary is None:
            return None

        cleaned_query = strip_filename_from_query(query)

        try:
            plan = await self._decomposer.decompose(cleaned_query, summary)
        except Exception as exc:
            logger.warning("Stream query decomposition failed: %s", exc)
            return None

        if plan.intent == "cannot_answer":
            reason = plan.cannot_answer_reason or "This question cannot be answered from the available data."
            return [
                StreamEvent("meta", {"mode": "OFFLINE_ARCHIVE", "sources": [], "conversation_id": conversation_id}),
                StreamEvent("token", {"text": reason}),
                StreamEvent("done", {"final_text": reason}),
            ]

        if plan.intent == "forecast" and plan.forecast_plan:
            fc_plan = repair_forecast_plan_horizon(plan.forecast_plan, cleaned_query)
            result = await self._execute_forecast_plan(fc_plan, summary)
            if result is not None:
                events: list[StreamEvent] = []
                source_list = result.attached_sources or []
                events.append(StreamEvent(
                    "meta",
                    {
                        "mode": result.mode,
                        "sources": source_list,
                        "conversation_id": conversation_id,
                        "forecast": result.forecast,
                        "chart": result.chart,
                    },
                ))
                events.append(StreamEvent("token", {"text": result.answer}))
                events.append(StreamEvent(
                    "done",
                    {
                        "final_text": result.answer,
                        "forecast": result.forecast,
                        "chart": result.chart,
                    },
                ))
                return events

        if plan.intent == "analytics" and plan.analytics_plan:
            assert self._runner is not None
            repaired = self._runner.refine_analytics_plan(
                plan.analytics_plan, cleaned_query, summary
            )
            result = await self._execute_analytics_plan(repaired)
            if result is not None:
                source_list = result.attached_sources or []
                return [
                    StreamEvent("meta", {"mode": result.mode, "sources": source_list, "conversation_id": conversation_id}),
                    StreamEvent("token", {"text": result.answer}),
                    StreamEvent("done", {"final_text": result.answer}),
                ]

        return None

    async def get_answer(self, query: str, prefer_mode: str | None = None, include_web: bool = True, include_documents: bool = False, document_ids: list[str] | None = None) -> ChatResult:
        doc_scope = document_ids if document_ids else None
        analytics_prefix = ""

        if include_documents and document_ids and self._can_use_decomposer():
            result = await self._handle_decomposed_query(query, document_ids)
            if result is not None:
                return result

        elif include_documents and document_ids and self._can_use_analytics():
            fallback = await self._handle_legacy_analytics(query, document_ids)
            if isinstance(fallback, ChatResult):
                return fallback
            if isinstance(fallback, str):
                analytics_prefix = fallback

        mode, contexts = await gather_contexts(
            self._rag_deps(),
            query,
            prefer_mode,
            include_web,
            include_documents,
            doc_scope,
        )
        contexts = self._augment_contexts_with_document_summary(
            contexts, include_documents, doc_scope
        )

        if mode == "OFFLINE_ARCHIVE":
            cached = await self._archive.get_cached_answer_async(query)
            if cached:
                resp = f"{cached.answer}\n\nSource: {cached.citation_url or 'cached answer'}"
                if cached.evidence_quote:
                    resp += f"\nEvidence: {cached.evidence_quote}"
                return ChatResult(
                    f"{analytics_prefix}{resp}\n(Cached from: {cached.timestamp})",
                    mode,
                    contexts,
                )
        
        extraction = await self._llm.extract_json(extraction_prompt(contexts), query)
        if extraction and extraction.get("answer"):
            ans, cite, ev = extraction["answer"], extraction.get("citation_url") or (contexts[0].url if contexts else None), extraction.get("evidence_quote")
            resp = f"{ans}\n\nSource: {cite or 'extracted from context'}"
            if ev:
                resp += f"\nEvidence: {ev}"
            if mode == "ONLINE":
                await self._archive.save_answer_async(query, ans, cite, ev)
            return ChatResult(f"{analytics_prefix}{resp}", mode, contexts)
        
        if mode in {"OFFLINE_ARCHIVE", "LOCAL_WEIGHTS"} and not has_usable_context(contexts):
            msg = "I could not verify the answer from the offline archive. Please try online mode or add a relevant source." if mode == "OFFLINE_ARCHIVE" else "I do not have any sources to answer this question. Please try online mode or add sources to the archive."
            return ChatResult(f"{analytics_prefix}{msg}", mode, contexts)
        
        llm_resp = await self._llm.complete(answer_prompt(mode, contexts, include_documents), query)
        if llm_resp.content and mode == "ONLINE":
            await self._archive.save_answer_async(query, llm_resp.content, contexts[0].url if contexts else None, None)
        body = llm_resp.content or ""
        return ChatResult(f"{analytics_prefix}{body}", mode, contexts)
    
    async def stream_answer(self, query: str, conversation_id: str, prefer_mode: str | None = None, include_web: bool = True, include_documents: bool = False, document_ids: list[str] | None = None) -> AsyncIterator[StreamEvent]:
        try:
            doc_scope = document_ids if document_ids else None
            analytics_unavailable: dict[str, str] | None = None
            stream_prefix = ""

            if include_documents and document_ids and self._can_use_decomposer():
                decomposed = await self._stream_decomposed_query(
                    query, document_ids, conversation_id
                )
                if decomposed is not None:
                    for event in decomposed:
                        yield event
                    return

            elif include_documents and document_ids and self._can_use_analytics():
                if is_predictive_intent(query):
                    _, profile = self._get_profile_for_documents(document_ids)
                    use_on_demand = query_has_filter_intent(query, profile)

                    fc_res: ForecastChatPayload | ForecastUnavailable | None = None

                    if use_on_demand:
                        od_result = await self._try_on_demand_forecast(query, document_ids)
                        if od_result is not None:
                            fc_res = od_result

                    if fc_res is None and not use_on_demand:
                        rows = self._analytics_executor.forecast_repo.list_for_documents(  # type: ignore[union-attr]
                            document_ids
                        )

                        def _fn(did: str) -> str | None:
                            info = self._docs.get_document(did)
                            return info.filename if info else None

                        fc_res = resolve_forecast_for_chat(
                            rows, get_filename=_fn, user_query=query
                        )

                    if isinstance(fc_res, ForecastChatPayload):
                        payload_dict, chart_spec, source_payload = (
                            self._build_forecast_chat_result(fc_res)
                        )
                        payload_json = json.dumps(payload_dict, indent=2, sort_keys=True)
                        body = await self._forecast_narration(payload_json, query)
                        answer = (
                            f"{body}\n\n**Forecast (baseline)**\n```json\n{payload_json}\n```\n"
                        )
                        yield StreamEvent(
                            "meta",
                            {
                                "mode": "OFFLINE_ARCHIVE",
                                "sources": [source_payload],
                                "conversation_id": conversation_id,
                                "forecast": payload_dict,
                                "chart": chart_spec,
                            },
                        )
                        yield StreamEvent("token", {"text": answer})
                        yield StreamEvent(
                            "done",
                            {
                                "final_text": answer,
                                "forecast": payload_dict,
                                "chart": chart_spec,
                            },
                        )
                        return
                    if isinstance(fc_res, ForecastUnavailable):
                        stream_prefix = f"*{fc_res.hint}*\n\n"

                analytics_out = await self._try_analytics(query, document_ids)
                if isinstance(analytics_out, AnalyticsUnavailable):
                    analytics_unavailable = {
                        "reason": analytics_out.reason,
                        "hint": analytics_out.hint,
                    }
                    hint = f"*{analytics_out.hint}*\n\n"
                    stream_prefix = stream_prefix + hint if stream_prefix else hint
                elif isinstance(analytics_out, AnalyticsResult):
                    answer = self._format_deterministic_analytics_answer(analytics_out)
                    yield StreamEvent(
                        "meta",
                        {
                            "mode": "OFFLINE_ARCHIVE",
                            "sources": [self._analytics_source_payload(analytics_out)],
                            "conversation_id": conversation_id,
                        },
                    )
                    yield StreamEvent("token", {"text": answer})
                    yield StreamEvent("done", {"final_text": answer})
                    return

            mode, contexts = await gather_contexts(
                self._rag_deps(),
                query,
                prefer_mode,
                include_web,
                include_documents,
                doc_scope,
            )
            contexts = self._augment_contexts_with_document_summary(
                contexts, include_documents, doc_scope
            )
            sources = [context_to_source_dict(c, determine_retrieval_type(mode, self._s.offline_retrieval_mode, c.is_document_source()), self._archive.hash_url) for c in contexts if c.url != FALLBACK_SOURCE_URL]
            meta_payload: dict[str, Any] = {
                "mode": mode,
                "sources": sources,
                "conversation_id": conversation_id,
            }
            if analytics_unavailable is not None:
                meta_payload["analytics_unavailable"] = analytics_unavailable

            if mode == "OFFLINE_ARCHIVE":
                cached = await self._archive.get_cached_answer_async(query)
                if cached:
                    resp = f"{cached.answer}\n\nSource: {cached.citation_url or 'cached answer'}"
                    if cached.evidence_quote:
                        resp += f"\nEvidence: {cached.evidence_quote}"
                    final_text = f"{stream_prefix}{resp}\n(Cached from: {cached.timestamp})"
                    yield StreamEvent("meta", meta_payload)
                    yield StreamEvent("token", {"text": final_text})
                    yield StreamEvent("done", {"final_text": final_text})
                    return

            extraction = await self._llm.extract_json(extraction_prompt(contexts), query)
            if extraction and extraction.get("answer"):
                ans, cite, ev = extraction["answer"], extraction.get("citation_url") or (contexts[0].url if contexts else None), extraction.get("evidence_quote")
                resp = f"{ans}\n\nSource: {cite or 'extracted from context'}"
                if ev:
                    resp += f"\nEvidence: {ev}"
                if mode == "ONLINE":
                    await self._archive.save_answer_async(query, ans, cite, ev)
                final_text = f"{stream_prefix}{resp}" if stream_prefix else resp
                yield StreamEvent("meta", meta_payload)
                yield StreamEvent("token", {"text": final_text})
                yield StreamEvent("done", {"final_text": final_text})
                return

            if mode in {"OFFLINE_ARCHIVE", "LOCAL_WEIGHTS"} and not has_usable_context(contexts):
                msg = (
                    "I could not verify the answer from the offline archive. Please try online mode or add a relevant source."
                    if mode == "OFFLINE_ARCHIVE"
                    else "I do not have any sources to answer this question. Please try online mode or add sources to the archive."
                )
                final_text = f"{stream_prefix}{msg}" if stream_prefix else msg
                yield StreamEvent("meta", meta_payload)
                yield StreamEvent("token", {"text": final_text})
                yield StreamEvent("done", {"final_text": final_text})
                return

            yield StreamEvent("meta", meta_payload)

            full_resp = ""
            model_resp = ""
            prefix_sent = False
            try:
                async for chunk in self._llm.stream(answer_prompt(mode, contexts, include_documents), query):
                    if chunk.content:
                        text = chunk.content
                        model_resp += text
                        if stream_prefix and not prefix_sent:
                            text = stream_prefix + text
                            prefix_sent = True
                        full_resp += text
                        yield StreamEvent("token", {"text": text})
                    if chunk.is_done:
                        break
            except Exception:
                resp = await self._llm.complete(answer_prompt(mode, contexts, include_documents), query)
                body = resp.content or ""
                model_resp = body
                if stream_prefix and not prefix_sent:
                    body = stream_prefix + body
                    prefix_sent = True
                full_resp = body
                yield StreamEvent("token", {"text": body})
            if stream_prefix and not prefix_sent:
                full_resp = stream_prefix + full_resp
                yield StreamEvent("token", {"text": stream_prefix})
            if mode == "ONLINE" and model_resp.strip():
                await self._archive.save_answer_async(
                    query, model_resp.strip(), contexts[0].url if contexts else None, None
                )
            yield StreamEvent("done", {"final_text": full_resp})
        except Exception as e:
            yield StreamEvent("error", {"code": ErrorCode.STREAM_ERROR, "message": str(e)})
    
    def convert_contexts_to_sources(self, contexts: list[SourceContext], mode: str) -> list[dict[str, Any]]:
        return [context_to_source_dict(c, determine_retrieval_type(mode, self._s.offline_retrieval_mode, c.is_document_source()), self._archive.hash_url) for c in contexts if c.url != FALLBACK_SOURCE_URL]
