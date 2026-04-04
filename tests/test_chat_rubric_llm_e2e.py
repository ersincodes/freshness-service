"""LLM end-to-end tests for the chat QA rubric (tests/qa_rubric.json).

Enable with RUN_CHAT_LLM_E2E=1 and place a workbook at tests/fixtures/Advanced_Sales_Dataset.xlsx
or set FRESHNESS_QA_WORKBOOK to an .xlsx path. Requires LM Studio (or compatible) per backend settings.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

_RUBRIC_PATH = Path(__file__).resolve().parent / "qa_rubric.json"
_QUESTIONS: list[dict[str, Any]] = json.loads(
    _RUBRIC_PATH.read_text(encoding="utf-8")
)["questions"]

_OFFLINE_NO_SOURCE = "I could not verify the answer from the offline archive"
_NO_SOURCES = "I do not have any sources to answer"


def _has_analytics_source(result: Any) -> bool:
    for src in result.attached_sources or []:
        if src.get("source_kind") == "analytics":
            return True
    return False


def _answer_has_markdown_table(answer: str) -> bool:
    return "|" in answer and "---" in answer


def _assert_structured_analytics(result: Any, qid: str) -> None:
    assert result.answer.strip(), f"{qid}: empty answer"
    if _has_analytics_source(result) or _answer_has_markdown_table(result.answer):
        return
    pytest.fail(
        f"{qid}: expected analytics source or markdown table; "
        f"attached_sources={result.attached_sources!r}"
    )


def _assert_structured_forecast(result: Any, qid: str) -> None:
    assert result.answer.strip(), f"{qid}: empty answer"
    if result.forecast is not None:
        return
    if "**Forecast (baseline)**" in result.answer or "```json" in result.answer:
        return
    for src in result.attached_sources or []:
        url = str(src.get("url") or "")
        if url.startswith("forecast://"):
            return
    pytest.fail(f"{qid}: expected forecast payload or forecast markers in answer")


def _assert_narrative_ok(result: Any, qid: str) -> None:
    text = result.answer.strip()
    assert len(text) >= 50, f"{qid}: answer too short ({len(text)} chars)"
    assert _OFFLINE_NO_SOURCE not in text, f"{qid}: offline archive failure message"
    assert _NO_SOURCES not in text, f"{qid}: no sources message"


def _assert_unsupported_or_narrative(result: Any, qid: str) -> None:
    text = result.answer.strip()
    assert len(text) >= 25, f"{qid}: answer too short ({len(text)} chars)"


@pytest.mark.llm_e2e
@pytest.mark.parametrize("case", _QUESTIONS, ids=[q["id"] for q in _QUESTIONS])
def test_rubric_question(case: dict[str, Any], llm_e2e_context: dict[str, Any]) -> None:
    """Sync wrapper so system pytest works without pytest-asyncio (uses asyncio.run)."""
    from backend.api.deps import chat_service

    document_id = llm_e2e_context["document_id"]
    query = str(case["text"])
    expectation = str(case["expectation"])
    qid = str(case["id"])

    async def _ask() -> Any:
        svc = chat_service()
        return await svc.get_answer(
            query,
            prefer_mode="OFFLINE",
            include_web=False,
            include_documents=True,
            document_ids=[document_id],
        )

    result = asyncio.run(_ask())

    if expectation == "structured_analytics":
        _assert_structured_analytics(result, qid)
    elif expectation == "structured_forecast":
        _assert_structured_forecast(result, qid)
    elif expectation == "narrative_ok":
        _assert_narrative_ok(result, qid)
    elif expectation == "unsupported_expect_narrative_or_cannot":
        _assert_unsupported_or_narrative(result, qid)
    else:
        pytest.fail(f"{qid}: unknown expectation {expectation!r}")
