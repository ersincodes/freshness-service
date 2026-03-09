"""
Document repository for managing uploaded document data.

Encapsulates SQLite and filesystem operations for documents and chunks.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import sqlite3
from dataclasses import dataclass
from typing import Any

from ..documents import DocumentType, DocumentStatus, hash_chunk_id


@dataclass(frozen=True)
class DocumentInfo:
    """Document metadata."""
    document_id: str
    filename: str
    doc_type: DocumentType
    size_bytes: int
    status: DocumentStatus
    uploaded_at: str
    error_message: str | None = None
    chunk_count: int = 0


@dataclass(frozen=True)
class DocumentChunk:
    """A chunk with location metadata."""
    chunk_id: str
    document_id: str
    chunk_index: int
    content: str
    metadata: dict[str, Any]
    timestamp: str
    filename: str | None = None


class DocumentRepository:
    """Repository for document data access operations."""
    
    def __init__(self, db_path: str, upload_dir: str) -> None:
        self._db_path = db_path
        self._upload_dir = upload_dir
    
    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path)
    
    def _ensure_upload_dir(self) -> str:
        os.makedirs(self._upload_dir, exist_ok=True)
        return self._upload_dir
    
    def save_document(self, document_id: str, filename: str, doc_type: DocumentType, size_bytes: int,
                      status: DocumentStatus = DocumentStatus.PENDING, error_message: str | None = None) -> None:
        """Save document metadata."""
        now = dt.datetime.now().isoformat()
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO documents (document_id, filename, doc_type, size_bytes, status, uploaded_at, error_message) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (document_id, filename, doc_type.value, size_bytes, status.value, now, error_message),
            )
            conn.commit()
    
    def update_status(self, document_id: str, status: DocumentStatus, error_message: str | None = None) -> None:
        """Update document status."""
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE documents SET status = ?, error_message = ? WHERE document_id = ?",
                        (status.value, error_message, document_id))
            conn.commit()
    
    def get_document(self, document_id: str) -> DocumentInfo | None:
        """Get document by ID."""
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT d.document_id, d.filename, d.doc_type, d.size_bytes, d.status, d.uploaded_at, d.error_message,
                   (SELECT COUNT(*) FROM document_chunks WHERE document_id = d.document_id) FROM documents d WHERE d.document_id = ?""",
                (document_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        return DocumentInfo(row[0], row[1], DocumentType(row[2]), row[3], DocumentStatus(row[4]), row[5], row[6], row[7])
    
    def list_documents(self) -> list[DocumentInfo]:
        """List all documents."""
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT d.document_id, d.filename, d.doc_type, d.size_bytes, d.status, d.uploaded_at, d.error_message,
                   (SELECT COUNT(*) FROM document_chunks WHERE document_id = d.document_id) FROM documents d ORDER BY d.uploaded_at DESC"""
            )
            rows = cur.fetchall()
        return [DocumentInfo(r[0], r[1], DocumentType(r[2]), r[3], DocumentStatus(r[4]), r[5], r[6], r[7]) for r in rows]
    
    def delete_document(self, document_id: str) -> bool:
        """Delete document and chunks from database."""
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM document_chunks WHERE document_id = ?", (document_id,))
            cur.execute("DELETE FROM documents WHERE document_id = ?", (document_id,))
            conn.commit()
            return cur.rowcount > 0
    
    def delete_document_file(self, document_id: str) -> None:
        """Delete document file from filesystem."""
        upload_dir = self._ensure_upload_dir()
        for filename in os.listdir(upload_dir):
            if filename.startswith(f"{document_id}_"):
                try:
                    os.remove(os.path.join(upload_dir, filename))
                except OSError:
                    pass
    
    def save_chunks(self, document_id: str, chunks: list[tuple[int, str, dict[str, Any]]]) -> None:
        """Save document chunks."""
        now = dt.datetime.now().isoformat()
        with self._conn() as conn:
            cur = conn.cursor()
            for chunk_index, content, metadata in chunks:
                chunk_id = hash_chunk_id(document_id, chunk_index)
                cur.execute(
                    "INSERT OR REPLACE INTO document_chunks (chunk_id, document_id, chunk_index, content, meta_json, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                    (chunk_id, document_id, chunk_index, content, json.dumps(metadata), now),
                )
            conn.commit()
    
    _STOP_WORDS = frozenset({
        "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "with", "by", "from", "is", "it", "its", "that", "this",
        "are", "was", "were", "be", "been", "being", "have", "has", "had",
        "do", "does", "did", "will", "would", "could", "should", "may",
        "can", "you", "me", "my", "your", "who", "what", "how", "where",
        "when", "which", "give", "get", "tell", "show", "find", "please",
    })

    @staticmethod
    def _rows_to_chunks(rows: list[tuple[Any, ...]]) -> list[DocumentChunk]:
        return [DocumentChunk(r[0], r[1], r[2], r[3], json.loads(r[4]), r[5], r[6]) for r in rows]

    @staticmethod
    def _tokenize_query(query: str) -> list[str]:
        tokens = [t for t in query.lower().split() if len(t) > 2 and t not in DocumentRepository._STOP_WORDS]
        if not tokens:
            tokens = [query.lower().strip()]
        return tokens[:10]

    @staticmethod
    def _scope_clause(document_ids: list[str] | None) -> tuple[str, tuple[str, ...]]:
        if document_ids:
            placeholders = ",".join("?" for _ in document_ids)
            return f"dc.document_id IN ({placeholders})", tuple(document_ids)
        return "d.status = 'ready'", ()

    def _search_chunks_by_like_clauses(
        self,
        *,
        like_clauses: str,
        like_params: list[str],
        document_ids: list[str] | None,
        limit: int,
        order: str = "dc.chunk_index ASC",
        extra_where: str = "",
        extra_params: tuple[Any, ...] = (),
    ) -> list[DocumentChunk]:
        scope_where, scope_params = self._scope_clause(document_ids)
        where_sql = f"{scope_where} AND ({like_clauses})"
        if extra_where:
            where_sql = f"{where_sql} AND {extra_where}"

        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"""SELECT dc.chunk_id, dc.document_id, dc.chunk_index, dc.content, dc.meta_json, dc.timestamp, d.filename
                    FROM document_chunks dc JOIN documents d ON dc.document_id = d.document_id
                    WHERE {where_sql}
                    ORDER BY {order} LIMIT ?""",
                (*scope_params, *like_params, *extra_params, limit),
            )
            rows = cur.fetchall()
        return self._rows_to_chunks(rows)

    def fetch_chunks(
        self,
        query: str,
        top_k: int = 10,
        document_ids: list[str] | None = None,
    ) -> list[DocumentChunk]:
        """Canonical chunk retrieval API with optional document scoping."""
        tokens = self._tokenize_query(query)
        like_clauses = " OR ".join("LOWER(dc.content) LIKE ?" for _ in tokens)
        like_params = [f"%{t}%" for t in tokens]
        return self._search_chunks_by_like_clauses(
            like_clauses=like_clauses,
            like_params=like_params,
            document_ids=document_ids,
            limit=top_k,
        )

    def search_chunks_keyword(self, query: str, document_ids: list[str] | None = None, top_k: int = 5) -> list[DocumentChunk]:
        """Search chunks by keyword tokens with OR logic.
        
        Tokenizes the query and matches individual terms rather than the
        full sentence, which would never appear as a substring in data chunks.
        """
        return self.fetch_chunks(query=query, top_k=top_k, document_ids=document_ids)
    
    def search_chunks_by_terms(
        self, terms: list[str], document_ids: list[str] | None = None, limit: int = 10
    ) -> list[DocumentChunk]:
        """Search chunks for multiple row-target terms (OR logic)."""
        if not terms:
            return []
        
        like_clauses = " OR ".join("dc.content LIKE ?" for _ in terms)
        like_params = [f"%{t}%" for t in terms]
        return self._search_chunks_by_like_clauses(
            like_clauses=like_clauses,
            like_params=like_params,
            document_ids=document_ids,
            limit=limit,
        )
    
    def search_chunks_by_filename(
        self, filename_pattern: str, document_ids: list[str] | None = None, limit: int = 10, last_chunks: bool = False
    ) -> list[DocumentChunk]:
        """Search chunks by filename pattern. Optionally get last chunks (highest chunk_index)."""
        term = f"%{filename_pattern.lower()}%"
        order = "dc.chunk_index DESC" if last_chunks else "dc.chunk_index ASC"
        return self._search_chunks_by_like_clauses(
            like_clauses="1 = 1",
            like_params=[],
            document_ids=document_ids,
            limit=limit,
            order=order,
            extra_where="LOWER(d.filename) LIKE ?",
            extra_params=(term,),
        )
    
    def save_file(self, document_id: str, filename: str, content: bytes) -> str:
        """Save uploaded file to disk."""
        upload_dir = self._ensure_upload_dir()
        file_path = os.path.join(upload_dir, f"{document_id}_{filename}")
        with open(file_path, "wb") as f:
            f.write(content)
        return file_path
    
    # Async wrappers
    async def save_document_async(self, document_id: str, filename: str, doc_type: DocumentType, size_bytes: int,
                                  status: DocumentStatus = DocumentStatus.PENDING, error_message: str | None = None) -> None:
        return await asyncio.to_thread(self.save_document, document_id, filename, doc_type, size_bytes, status, error_message)
    
    async def update_status_async(self, document_id: str, status: DocumentStatus, error_message: str | None = None) -> None:
        return await asyncio.to_thread(self.update_status, document_id, status, error_message)
    
    async def get_document_async(self, document_id: str) -> DocumentInfo | None:
        return await asyncio.to_thread(self.get_document, document_id)
    
    async def list_documents_async(self) -> list[DocumentInfo]:
        return await asyncio.to_thread(self.list_documents)
    
    async def delete_document_async(self, document_id: str) -> bool:
        return await asyncio.to_thread(self.delete_document, document_id)
    
    async def delete_document_file_async(self, document_id: str) -> None:
        return await asyncio.to_thread(self.delete_document_file, document_id)
    
    async def save_chunks_async(self, document_id: str, chunks: list[tuple[int, str, dict[str, Any]]]) -> None:
        return await asyncio.to_thread(self.save_chunks, document_id, chunks)
    
    async def search_chunks_keyword_async(self, query: str, document_ids: list[str] | None = None, top_k: int = 5) -> list[DocumentChunk]:
        return await asyncio.to_thread(self.search_chunks_keyword, query, document_ids, top_k)

    async def fetch_chunks_async(
        self, query: str, top_k: int = 10, document_ids: list[str] | None = None
    ) -> list[DocumentChunk]:
        return await asyncio.to_thread(self.fetch_chunks, query, top_k, document_ids)
    
    async def search_chunks_by_terms_async(
        self, terms: list[str], document_ids: list[str] | None = None, limit: int = 10
    ) -> list[DocumentChunk]:
        return await asyncio.to_thread(self.search_chunks_by_terms, terms, document_ids, limit)
    
    async def search_chunks_by_filename_async(
        self, filename_pattern: str, document_ids: list[str] | None = None, limit: int = 10, last_chunks: bool = False
    ) -> list[DocumentChunk]:
        return await asyncio.to_thread(self.search_chunks_by_filename, filename_pattern, document_ids, limit, last_chunks)
