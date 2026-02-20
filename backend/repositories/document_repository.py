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

    def search_chunks_keyword(self, query: str, document_ids: list[str] | None = None, top_k: int = 5) -> list[DocumentChunk]:
        """Search chunks by keyword tokens with OR logic.
        
        Tokenizes the query and matches individual terms rather than the
        full sentence, which would never appear as a substring in data chunks.
        """
        tokens = [t for t in query.lower().split() if len(t) > 2 and t not in self._STOP_WORDS]
        if not tokens:
            tokens = [query.lower().strip()]
        tokens = tokens[:10]

        like_clauses = " OR ".join("LOWER(dc.content) LIKE ?" for _ in tokens)
        like_params = [f"%{t}%" for t in tokens]

        with self._conn() as conn:
            cur = conn.cursor()
            if document_ids:
                placeholders = ",".join("?" for _ in document_ids)
                cur.execute(
                    f"""SELECT dc.chunk_id, dc.document_id, dc.chunk_index, dc.content, dc.meta_json, dc.timestamp, d.filename
                        FROM document_chunks dc JOIN documents d ON dc.document_id = d.document_id
                        WHERE dc.document_id IN ({placeholders}) AND ({like_clauses})
                        ORDER BY dc.chunk_index ASC LIMIT ?""",
                    (*document_ids, *like_params, top_k),
                )
            else:
                cur.execute(
                    f"""SELECT dc.chunk_id, dc.document_id, dc.chunk_index, dc.content, dc.meta_json, dc.timestamp, d.filename
                       FROM document_chunks dc JOIN documents d ON dc.document_id = d.document_id
                       WHERE d.status = 'ready' AND ({like_clauses})
                       ORDER BY dc.chunk_index ASC LIMIT ?""",
                    (*like_params, top_k),
                )
            rows = cur.fetchall()
        return [DocumentChunk(r[0], r[1], r[2], r[3], json.loads(r[4]), r[5], r[6]) for r in rows]
    
    def search_chunks_by_terms(
        self, terms: list[str], document_ids: list[str] | None = None, limit: int = 10
    ) -> list[DocumentChunk]:
        """Search chunks for multiple row-target terms (OR logic)."""
        if not terms:
            return []
        
        like_clauses = " OR ".join("dc.content LIKE ?" for _ in terms)
        like_params = [f"%{t}%" for t in terms]
        
        with self._conn() as conn:
            cur = conn.cursor()
            if document_ids:
                doc_placeholders = ",".join("?" for _ in document_ids)
                cur.execute(
                    f"""SELECT dc.chunk_id, dc.document_id, dc.chunk_index, dc.content, dc.meta_json, dc.timestamp, d.filename
                        FROM document_chunks dc JOIN documents d ON dc.document_id = d.document_id
                        WHERE dc.document_id IN ({doc_placeholders}) AND ({like_clauses})
                        ORDER BY dc.chunk_index ASC LIMIT ?""",
                    (*document_ids, *like_params, limit),
                )
            else:
                cur.execute(
                    f"""SELECT dc.chunk_id, dc.document_id, dc.chunk_index, dc.content, dc.meta_json, dc.timestamp, d.filename
                        FROM document_chunks dc JOIN documents d ON dc.document_id = d.document_id
                        WHERE d.status = 'ready' AND ({like_clauses})
                        ORDER BY dc.chunk_index ASC LIMIT ?""",
                    (*like_params, limit),
                )
            rows = cur.fetchall()
        return [DocumentChunk(r[0], r[1], r[2], r[3], json.loads(r[4]), r[5], r[6]) for r in rows]
    
    def search_chunks_by_filename(
        self, filename_pattern: str, document_ids: list[str] | None = None, limit: int = 10, last_chunks: bool = False
    ) -> list[DocumentChunk]:
        """Search chunks by filename pattern. Optionally get last chunks (highest chunk_index)."""
        term = f"%{filename_pattern.lower()}%"
        order = "dc.chunk_index DESC" if last_chunks else "dc.chunk_index ASC"
        
        with self._conn() as conn:
            cur = conn.cursor()
            if document_ids:
                doc_placeholders = ",".join("?" for _ in document_ids)
                cur.execute(
                    f"""SELECT dc.chunk_id, dc.document_id, dc.chunk_index, dc.content, dc.meta_json, dc.timestamp, d.filename
                        FROM document_chunks dc JOIN documents d ON dc.document_id = d.document_id
                        WHERE dc.document_id IN ({doc_placeholders}) AND LOWER(d.filename) LIKE ?
                        ORDER BY {order} LIMIT ?""",
                    (*document_ids, term, limit),
                )
            else:
                cur.execute(
                    f"""SELECT dc.chunk_id, dc.document_id, dc.chunk_index, dc.content, dc.meta_json, dc.timestamp, d.filename
                        FROM document_chunks dc JOIN documents d ON dc.document_id = d.document_id
                        WHERE d.status = 'ready' AND LOWER(d.filename) LIKE ?
                        ORDER BY {order} LIMIT ?""",
                    (term, limit),
                )
            rows = cur.fetchall()
        return [DocumentChunk(r[0], r[1], r[2], r[3], json.loads(r[4]), r[5], r[6]) for r in rows]
    
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
    
    async def search_chunks_by_terms_async(
        self, terms: list[str], document_ids: list[str] | None = None, limit: int = 10
    ) -> list[DocumentChunk]:
        return await asyncio.to_thread(self.search_chunks_by_terms, terms, document_ids, limit)
    
    async def search_chunks_by_filename_async(
        self, filename_pattern: str, document_ids: list[str] | None = None, limit: int = 10, last_chunks: bool = False
    ) -> list[DocumentChunk]:
        return await asyncio.to_thread(self.search_chunks_by_filename, filename_pattern, document_ids, limit, last_chunks)
