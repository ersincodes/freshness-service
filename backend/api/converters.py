"""Map domain / dict payloads to API response models."""
from __future__ import annotations

from .schemas import DocumentResponse, Source, SourceLocation
from ..repositories.document_repository import DocumentInfo


def source_dict_to_model(d: dict) -> Source:
    loc = SourceLocation(**d["location"]) if d.get("location") else None
    return Source(
        url=d["url"],
        snippet=d["snippet"],
        retrieval_type=d["retrieval_type"],
        timestamp=d.get("timestamp"),
        url_hash=d.get("url_hash"),
        source_type=d.get("source_type", "web"),
        filename=d.get("filename"),
        location=loc,
    )


def document_info_to_response(d: DocumentInfo) -> DocumentResponse:
    return DocumentResponse(
        document_id=d.document_id,
        filename=d.filename,
        doc_type=d.doc_type.value,
        size_bytes=d.size_bytes,
        status=d.status.value,
        uploaded_at=d.uploaded_at,
        error_message=d.error_message,
        chunk_count=d.chunk_count,
    )
