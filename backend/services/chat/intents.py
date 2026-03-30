"""Natural-language intent detection for document retrieval."""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class RowIntent:
    """Detected row-specific query intent."""

    row_number: int
    confidence: float


@dataclass(frozen=True)
class ColumnValueIntent:
    """Detected column-value lookup intent (e.g., 'Index=1000')."""

    column_name: str
    value: str
    confidence: float


@dataclass(frozen=True)
class QueryIntent:
    """Parsed query intent for document retrieval."""

    row_intent: RowIntent | None = None
    filename_pattern: str | None = None
    wants_last: bool = False
    column_value: ColumnValueIntent | None = None


_ROW_PATTERNS = [
    (re.compile(r"\brow\s+(\d+)\b", re.IGNORECASE), 1.0),
    (re.compile(r"#(\d+)\b"), 0.9),
    (re.compile(r"\b(\d+)(?:st|nd|rd|th)\s+(?:row|customer|entry|record|item)\b", re.IGNORECASE), 0.95),
    (re.compile(r"\b(?:customer|entry|record|item)\s+#?(\d+)\b", re.IGNORECASE), 0.85),
]

_COLUMN_VALUE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?:has|with|where)\s+(?:value\s+)?(\S+)\s+in\s+(?:the\s+)?(\w+)\s+(?:column|field)", re.IGNORECASE), "value_first"),
    (re.compile(r"\b(\d[\d.]*)\s+in\s+(?:the\s+)?(\w+)\s+(?:column|field)", re.IGNORECASE), "value_first"),
    (re.compile(r"\b(\w+)\s+(?:column|field)\s+(?:is|=|equals)\s+(\S+)", re.IGNORECASE), "column_first"),
    (re.compile(r"where\s+(?:the\s+)?(\w+)\s+(?:is|=|equals)\s+(\S+)", re.IGNORECASE), "column_first"),
    (re.compile(r"\b(index|id|code|number|num|no)\s+(\d+)\b", re.IGNORECASE), "column_first"),
]

_FILENAME_FROM_PATTERN = re.compile(
    r"from\s+(?:the\s+)?['\"]?([a-zA-Z0-9_\-]+(?:-\d+)?(?:\.[a-zA-Z0-9]+)?)['\"]?\s*(?:file|document)?",
    re.IGNORECASE,
)
_FILENAME_IN_PATTERN = re.compile(
    r"in\s+(?:the\s+)?['\"]?([a-zA-Z0-9_\-]+(?:-\d+)?(?:\.[a-zA-Z0-9]+)?)['\"]?\s+(?:file|document)",
    re.IGNORECASE,
)

_LAST_PATTERN = re.compile(r"\b(?:last|final|latest|most recent|bottom)\b", re.IGNORECASE)

_STRIP_FILENAME_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"\b(?:in|from|of)\s+(?:the\s+)?['\"]?"
        r"[a-zA-Z0-9_\-]+(?:-\d+)?(?:\.[a-zA-Z0-9]+)?"
        r"['\"]?\s+(?:file|document|spreadsheet|workbook|sheet|dataset)\s*\??",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bfrom\s+(?:the\s+)?['\"]?"
        r"[a-zA-Z0-9_\-]+(?:-\d+)?(?:\.[a-zA-Z0-9]+)?"
        r"['\"]?\s*(?:file|document|spreadsheet|workbook|sheet|dataset)?",
        re.IGNORECASE,
    ),
]


def strip_filename_from_query(query: str) -> str:
    """Remove filename references so the LLM decomposer sees only the analytical question.

    The document is already selected via ``document_ids``; leaving the
    filename in the prompt causes the LLM to misinterpret it as a filter
    or column reference.
    """
    cleaned = query
    for pat in _STRIP_FILENAME_PATTERNS:
        cleaned = pat.sub("", cleaned)
    cleaned = re.sub(r"\s{2,}", " ", cleaned).strip().rstrip("?").strip()
    if cleaned:
        cleaned += "?"
    return cleaned if cleaned else query


def _detect_filename(query: str) -> str | None:
    """Extract filename from query, preferring 'from FILE' over 'in FILE file'."""
    m = _FILENAME_FROM_PATTERN.search(query)
    if m:
        return m.group(1)
    m = _FILENAME_IN_PATTERN.search(query)
    return m.group(1) if m else None


def detect_row_intent(query: str) -> RowIntent | None:
    """Parse user query for row-specific addressing."""
    for pattern, confidence in _ROW_PATTERNS:
        match = pattern.search(query)
        if match:
            try:
                row_num = int(match.group(1))
                if row_num > 0:
                    return RowIntent(row_number=row_num, confidence=confidence)
            except ValueError:
                continue
    return None


def detect_column_value_intent(query: str) -> ColumnValueIntent | None:
    """Detect 'value V in column C' style lookups.

    Maps to the Header=Value format produced by row-to-text chunking, enabling
    precise term search against chunk content.
    """
    for pattern, order in _COLUMN_VALUE_PATTERNS:
        match = pattern.search(query)
        if match:
            if order == "value_first":
                value, column = match.group(1), match.group(2)
            else:
                column, value = match.group(1), match.group(2)
            return ColumnValueIntent(column_name=column, value=value, confidence=0.9)
    return None


def detect_query_intent(query: str) -> QueryIntent:
    """Parse query for document retrieval hints (row, filename, last, column-value)."""
    row_intent = detect_row_intent(query)
    column_value = detect_column_value_intent(query)
    filename_pattern = _detect_filename(query)
    wants_last = bool(_LAST_PATTERN.search(query))

    return QueryIntent(
        row_intent=row_intent,
        filename_pattern=filename_pattern,
        wants_last=wants_last,
        column_value=column_value,
    )
