"""
Core type definitions and constants.
"""
from __future__ import annotations

from typing import Literal

RetrievalType = Literal["online", "offline_keyword", "offline_semantic", "document_keyword", "document_semantic"]
RetrievalMode = Literal["ONLINE", "OFFLINE_ARCHIVE", "LOCAL_WEIGHTS"]
PreferMode = Literal["ONLINE", "OFFLINE"] | None
SourceType = Literal["web", "document"]
SSEEventType = Literal["meta", "token", "done", "error"]

DOC_URL_PREFIX = "doc://"
FALLBACK_SOURCE_URL = "N/A"
FALLBACK_SOURCE_TEXT = "No information found."


class ErrorCode:
    LLM_ERROR = "LLM_ERROR"
    LLM_UNAVAILABLE = "LLM_UNAVAILABLE"
    STREAM_ERROR = "STREAM_ERROR"
    NOT_FOUND = "NOT_FOUND"
    INVALID_FILENAME = "INVALID_FILENAME"
    UNSUPPORTED_TYPE = "UNSUPPORTED_TYPE"
    FILE_TOO_LARGE = "FILE_TOO_LARGE"
    SOURCE_NOT_FOUND = "SOURCE_NOT_FOUND"
