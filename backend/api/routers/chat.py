"""Chat HTTP endpoints."""
from __future__ import annotations

import json
import time
import uuid
from typing import AsyncGenerator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from ..converters import source_dict_to_model
from ..deps import chat_service
from ..schemas import ChatRequest, ChatResponse, ForecastResponseModel, TimingInfo
from ...domain import ErrorCode

router = APIRouter(tags=["chat"])


@router.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest) -> ChatResponse:
    start = time.perf_counter()
    conv_id = request.conversation_id or str(uuid.uuid4())
    svc = chat_service()
    try:
        result = await svc.get_answer(
            request.query,
            request.prefer_mode,
            request.include_web,
            request.include_documents,
            request.document_ids,
        )
    except Exception as e:
        raise HTTPException(500, {"code": ErrorCode.LLM_ERROR, "message": str(e)})
    if result.attached_sources is not None:
        sources = [source_dict_to_model(s) for s in result.attached_sources]
    else:
        sources = [source_dict_to_model(s) for s in svc.convert_contexts_to_sources(result.contexts, result.mode)]
    forecast_out = (
        ForecastResponseModel.model_validate(result.forecast)
        if result.forecast is not None
        else None
    )
    return ChatResponse(
        conversation_id=conv_id,
        answer=result.answer,
        mode=result.mode,
        sources=sources,
        timing=TimingInfo(total_ms=int((time.perf_counter() - start) * 1000)),
        forecast=forecast_out,
        chart=result.chart,
    )


@router.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    conv_id = request.conversation_id or str(uuid.uuid4())

    async def gen() -> AsyncGenerator[str, None]:
        async for ev in chat_service().stream_answer(
            request.query,
            conv_id,
            request.prefer_mode,
            request.include_web,
            request.include_documents,
            request.document_ids,
        ):
            yield f"event: {ev.event_type}\ndata: {json.dumps(ev.data)}\n\n"

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )
