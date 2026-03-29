"""Document upload and management HTTP endpoints."""
from __future__ import annotations

import asyncio
import datetime as dt
import logging

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, UploadFile

from ..converters import document_info_to_response
from ..deps import doc_repo
from ..schemas import DocumentListResponse, DocumentResponse, DocumentUploadResponse
from ...config import get_settings
from ...documents import (
    DocumentStatus,
    DocumentType,
    generate_document_id,
    get_document_type_from_filename,
    hash_chunk_id,
    ingest_excel_to_sqlite,
    process_document,
    sanitize_filename,
)
from ...domain import ErrorCode
from ...lifecycle import get_metadata_repository, open_analytics_connection
from ...vector_store import delete_document_chunks_from_vector_store, upsert_document_chunk

logger = logging.getLogger(__name__)

router = APIRouter(tags=["documents"])


async def process_doc_bg(doc_id: str, file_path: str, doc_type_val: str, filename: str) -> None:
    s = get_settings()
    repo = doc_repo()
    doc_type = DocumentType(doc_type_val)
    try:
        await repo.update_status_async(doc_id, DocumentStatus.PROCESSING)
        chunks = await asyncio.to_thread(process_document, file_path, doc_type)
        if not chunks:
            await repo.update_status_async(doc_id, DocumentStatus.ERROR, "No content could be extracted")
            return
        await repo.save_chunks_async(doc_id, [(c.chunk_index, c.content, c.metadata) for c in chunks])
        if s.offline_retrieval_mode == "semantic":
            for c in chunks:
                try:
                    await asyncio.to_thread(
                        upsert_document_chunk,
                        s.chroma_dir,
                        s.embed_model_name,
                        hash_chunk_id(doc_id, c.chunk_index),
                        doc_id,
                        filename,
                        c.content,
                        c.metadata,
                        dt.datetime.utcnow().isoformat(),
                    )
                except Exception:
                    pass

        if s.enable_tabular_analytics and doc_type in {DocumentType.XLSX, DocumentType.XLS}:
            try:
                conn = open_analytics_connection(s.db_path)
                await asyncio.to_thread(
                    ingest_excel_to_sqlite,
                    excel_path=file_path,
                    document_id=doc_id,
                    sqlite_connection=conn,
                )
            except Exception as exc:
                logger.warning("Tabular analytics ingestion failed for %s: %s", doc_id, exc)

        await repo.update_status_async(doc_id, DocumentStatus.READY)
    except Exception as e:
        await repo.update_status_async(doc_id, DocumentStatus.ERROR, str(e))


@router.post("/api/documents/upload", response_model=DocumentUploadResponse)
async def upload_document(background_tasks: BackgroundTasks, file: UploadFile = File(...)) -> DocumentUploadResponse:
    s = get_settings()
    if not file.filename:
        raise HTTPException(400, {"code": ErrorCode.INVALID_FILENAME, "message": "Filename is required"})
    doc_type = get_document_type_from_filename(file.filename)
    if not doc_type:
        raise HTTPException(
            400,
            {"code": ErrorCode.UNSUPPORTED_TYPE, "message": "Unsupported file type. Allowed: .pdf, .xlsx, .xls"},
        )
    content = await file.read()
    if len(content) > s.max_upload_mb * 1024 * 1024:
        raise HTTPException(413, {"code": ErrorCode.FILE_TOO_LARGE, "message": f"File exceeds {s.max_upload_mb}MB"})
    doc_id, safe_name = generate_document_id(), sanitize_filename(file.filename)
    repo = doc_repo()
    file_path = repo.save_file(doc_id, safe_name, content)
    await repo.save_document_async(doc_id, safe_name, DocumentType(doc_type.value), len(content), DocumentStatus.PENDING)
    background_tasks.add_task(process_doc_bg, doc_id, file_path, doc_type.value, safe_name)
    return DocumentUploadResponse(
        document_id=doc_id,
        filename=safe_name,
        status="pending",
        message="Document uploaded. Processing started.",
    )


@router.get("/api/documents", response_model=DocumentListResponse)
async def get_documents() -> DocumentListResponse:
    docs = await doc_repo().list_documents_async()
    return DocumentListResponse(documents=[document_info_to_response(d) for d in docs], total=len(docs))


@router.get("/api/documents/{document_id}", response_model=DocumentResponse)
async def get_document_status(document_id: str) -> DocumentResponse:
    d = await doc_repo().get_document_async(document_id)
    if not d:
        raise HTTPException(404, {"code": ErrorCode.NOT_FOUND, "message": f"Document not found: {document_id}"})
    return document_info_to_response(d)


@router.delete("/api/documents/{document_id}")
async def delete_document_endpoint(document_id: str) -> dict:
    s, repo = get_settings(), doc_repo()
    d = await repo.get_document_async(document_id)
    if not d:
        raise HTTPException(404, {"code": ErrorCode.NOT_FOUND, "message": f"Document not found: {document_id}"})
    if s.offline_retrieval_mode == "semantic":
        try:
            await asyncio.to_thread(
                delete_document_chunks_from_vector_store,
                s.chroma_dir,
                s.embed_model_name,
                document_id,
            )
        except Exception:
            pass
    meta = get_metadata_repository()
    if meta is not None:
        try:
            await asyncio.to_thread(meta.delete_document, document_id)
        except Exception as exc:
            logger.warning("Analytics cleanup failed for %s: %s", document_id, exc)
    await repo.delete_document_async(document_id)
    await repo.delete_document_file_async(document_id)
    return {"status": "ok", "message": f"Document {document_id} deleted"}
