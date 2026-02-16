"""
Document extraction and processing module.

Handles PDF and Excel file ingestion with chunking for RAG retrieval.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

# PDF extraction
try:
    from pypdf import PdfReader
except ImportError:
    PdfReader = None

# Excel extraction
try:
    import openpyxl
except ImportError:
    openpyxl = None

try:
    import xlrd
except ImportError:
    xlrd = None


class DocumentType(str, Enum):
    PDF = "pdf"
    XLSX = "xlsx"
    XLS = "xls"


class DocumentStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    READY = "ready"
    ERROR = "error"


@dataclass
class DocumentChunk:
    """A chunk of document content with location metadata."""
    chunk_index: int
    content: str
    metadata: dict[str, Any]  # page, sheet, row_start, row_end


@dataclass
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


# ============================================================================
# Database Schema
# ============================================================================

CREATE_DOCUMENTS = """
CREATE TABLE IF NOT EXISTS documents (
    document_id TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    doc_type TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    uploaded_at DATETIME NOT NULL,
    error_message TEXT
)
"""

CREATE_DOCUMENT_CHUNKS = """
CREATE TABLE IF NOT EXISTS document_chunks (
    chunk_id TEXT PRIMARY KEY,
    document_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    meta_json TEXT NOT NULL,
    timestamp DATETIME NOT NULL,
    FOREIGN KEY (document_id) REFERENCES documents(document_id) ON DELETE CASCADE
)
"""

CREATE_DOCUMENT_CHUNKS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_document_chunks_document_id 
ON document_chunks(document_id)
"""


def init_document_tables(db_path: str) -> None:
    """Initialize document-related tables in the database."""
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(CREATE_DOCUMENTS)
        cur.execute(CREATE_DOCUMENT_CHUNKS)
        cur.execute(CREATE_DOCUMENT_CHUNKS_INDEX)
        conn.commit()


# ============================================================================
# Document CRUD Operations
# ============================================================================

def generate_document_id() -> str:
    """Generate a unique document ID."""
    return str(uuid.uuid4())


def hash_chunk_id(document_id: str, chunk_index: int) -> str:
    """Generate a deterministic chunk ID."""
    raw = f"{document_id}:{chunk_index}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def save_document(
    db_path: str,
    document_id: str,
    filename: str,
    doc_type: DocumentType,
    size_bytes: int,
    status: DocumentStatus = DocumentStatus.PENDING,
    error_message: str | None = None,
) -> None:
    """Save document metadata to the database."""
    now = dt.datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO documents 
            (document_id, filename, doc_type, size_bytes, status, uploaded_at, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (document_id, filename, doc_type.value, size_bytes, status.value, now, error_message),
        )
        conn.commit()


def update_document_status(
    db_path: str,
    document_id: str,
    status: DocumentStatus,
    error_message: str | None = None,
) -> None:
    """Update document status."""
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE documents 
            SET status = ?, error_message = ?
            WHERE document_id = ?
            """,
            (status.value, error_message, document_id),
        )
        conn.commit()


def get_document(db_path: str, document_id: str) -> DocumentInfo | None:
    """Get document by ID."""
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT d.document_id, d.filename, d.doc_type, d.size_bytes, 
                   d.status, d.uploaded_at, d.error_message,
                   (SELECT COUNT(*) FROM document_chunks WHERE document_id = d.document_id) as chunk_count
            FROM documents d
            WHERE d.document_id = ?
            """,
            (document_id,),
        )
        row = cur.fetchone()
    
    if not row:
        return None
    
    return DocumentInfo(
        document_id=row[0],
        filename=row[1],
        doc_type=DocumentType(row[2]),
        size_bytes=row[3],
        status=DocumentStatus(row[4]),
        uploaded_at=row[5],
        error_message=row[6],
        chunk_count=row[7],
    )


def list_documents(db_path: str) -> list[DocumentInfo]:
    """List all documents."""
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT d.document_id, d.filename, d.doc_type, d.size_bytes, 
                   d.status, d.uploaded_at, d.error_message,
                   (SELECT COUNT(*) FROM document_chunks WHERE document_id = d.document_id) as chunk_count
            FROM documents d
            ORDER BY d.uploaded_at DESC
            """
        )
        rows = cur.fetchall()
    
    return [
        DocumentInfo(
            document_id=row[0],
            filename=row[1],
            doc_type=DocumentType(row[2]),
            size_bytes=row[3],
            status=DocumentStatus(row[4]),
            uploaded_at=row[5],
            error_message=row[6],
            chunk_count=row[7],
        )
        for row in rows
    ]


def delete_document(db_path: str, document_id: str) -> bool:
    """Delete document and its chunks."""
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        # Delete chunks first (FK constraint)
        cur.execute(
            "DELETE FROM document_chunks WHERE document_id = ?",
            (document_id,),
        )
        # Delete document
        cur.execute(
            "DELETE FROM documents WHERE document_id = ?",
            (document_id,),
        )
        conn.commit()
        return cur.rowcount > 0


def save_document_chunks(
    db_path: str,
    document_id: str,
    chunks: list[DocumentChunk],
) -> None:
    """Save document chunks to the database."""
    now = dt.datetime.now().isoformat()
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        for chunk in chunks:
            chunk_id = hash_chunk_id(document_id, chunk.chunk_index)
            cur.execute(
                """
                INSERT OR REPLACE INTO document_chunks 
                (chunk_id, document_id, chunk_index, content, meta_json, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    chunk_id,
                    document_id,
                    chunk.chunk_index,
                    chunk.content,
                    json.dumps(chunk.metadata),
                    now,
                ),
            )
        conn.commit()


def get_document_chunks(
    db_path: str,
    document_id: str,
) -> list[tuple[str, str, int, str, dict, str]]:
    """
    Get all chunks for a document.
    
    Returns: List of (chunk_id, document_id, chunk_index, content, metadata, timestamp)
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT chunk_id, document_id, chunk_index, content, meta_json, timestamp
            FROM document_chunks
            WHERE document_id = ?
            ORDER BY chunk_index
            """,
            (document_id,),
        )
        rows = cur.fetchall()
    
    return [
        (row[0], row[1], row[2], row[3], json.loads(row[4]), row[5])
        for row in rows
    ]


def search_document_chunks_keyword(
    db_path: str,
    query: str,
    document_ids: list[str] | None = None,
    top_k: int = 5,
) -> list[tuple[str, str, str, dict, str, str]]:
    """
    Search document chunks by keyword.
    
    Returns: List of (chunk_id, document_id, content, metadata, timestamp, filename)
    """
    search_term = f"%{query.lower()}%"
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        
        if document_ids:
            placeholders = ",".join("?" for _ in document_ids)
            cur.execute(
                f"""
                SELECT dc.chunk_id, dc.document_id, dc.content, dc.meta_json, 
                       dc.timestamp, d.filename
                FROM document_chunks dc
                JOIN documents d ON dc.document_id = d.document_id
                WHERE dc.document_id IN ({placeholders})
                  AND (LOWER(dc.content) LIKE ? OR LOWER(d.filename) LIKE ?)
                ORDER BY dc.timestamp DESC
                LIMIT ?
                """,
                (*document_ids, search_term, search_term, top_k),
            )
        else:
            cur.execute(
                """
                SELECT dc.chunk_id, dc.document_id, dc.content, dc.meta_json, 
                       dc.timestamp, d.filename
                FROM document_chunks dc
                JOIN documents d ON dc.document_id = d.document_id
                WHERE d.status = 'ready'
                  AND (LOWER(dc.content) LIKE ? OR LOWER(d.filename) LIKE ?)
                ORDER BY dc.timestamp DESC
                LIMIT ?
                """,
                (search_term, search_term, top_k),
            )
        
        rows = cur.fetchall()
    
    return [
        (row[0], row[1], row[2], json.loads(row[3]), row[4], row[5])
        for row in rows
    ]


# ============================================================================
# PDF Extraction
# ============================================================================

def _require_pypdf() -> None:
    if PdfReader is None:
        raise RuntimeError(
            "pypdf is not installed. Install with `pip install pypdf`."
        )


def extract_pdf_text(file_path: str) -> list[tuple[int, str]]:
    """
    Extract text from PDF, page by page.
    
    Returns: List of (page_number, text) tuples (1-indexed page numbers)
    """
    _require_pypdf()
    
    reader = PdfReader(file_path)
    pages: list[tuple[int, str]] = []
    
    for i, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        if text:
            pages.append((i, text))
    
    return pages


def chunk_pdf_pages(
    pages: list[tuple[int, str]],
    chunk_size: int = 2000,
) -> list[DocumentChunk]:
    """
    Chunk PDF pages into smaller pieces.
    
    Each chunk maintains page number metadata.
    """
    chunks: list[DocumentChunk] = []
    chunk_index = 0
    
    for page_num, text in pages:
        # Split page into chunks if needed
        if len(text) <= chunk_size:
            chunks.append(DocumentChunk(
                chunk_index=chunk_index,
                content=text,
                metadata={"page": page_num},
            ))
            chunk_index += 1
        else:
            # Split by sentences or words
            words = text.split()
            current_chunk = []
            current_length = 0
            
            for word in words:
                if current_length + len(word) + 1 > chunk_size and current_chunk:
                    chunks.append(DocumentChunk(
                        chunk_index=chunk_index,
                        content=" ".join(current_chunk),
                        metadata={"page": page_num},
                    ))
                    chunk_index += 1
                    current_chunk = []
                    current_length = 0
                
                current_chunk.append(word)
                current_length += len(word) + 1
            
            if current_chunk:
                chunks.append(DocumentChunk(
                    chunk_index=chunk_index,
                    content=" ".join(current_chunk),
                    metadata={"page": page_num},
                ))
                chunk_index += 1
    
    return chunks


# ============================================================================
# Excel Extraction
# ============================================================================

def _require_openpyxl() -> None:
    if openpyxl is None:
        raise RuntimeError(
            "openpyxl is not installed. Install with `pip install openpyxl`."
        )


def _require_xlrd() -> None:
    if xlrd is None:
        raise RuntimeError(
            "xlrd is not installed. Install with `pip install xlrd`."
        )


def extract_xlsx_data(file_path: str) -> list[tuple[str, list[list[Any]]]]:
    """
    Extract data from XLSX file, sheet by sheet.
    
    Returns: List of (sheet_name, rows) tuples where rows is a list of row values
    """
    _require_openpyxl()
    
    workbook = openpyxl.load_workbook(file_path, data_only=True)
    sheets: list[tuple[str, list[list[Any]]]] = []
    
    for sheet_name in workbook.sheetnames:
        sheet = workbook[sheet_name]
        rows: list[list[Any]] = []
        
        for row in sheet.iter_rows(values_only=True):
            # Filter out completely empty rows
            if any(cell is not None for cell in row):
                rows.append(list(row))
        
        if rows:
            sheets.append((sheet_name, rows))
    
    workbook.close()
    return sheets


def extract_xls_data(file_path: str) -> list[tuple[str, list[list[Any]]]]:
    """
    Extract data from XLS file (legacy format), sheet by sheet.
    
    Returns: List of (sheet_name, rows) tuples
    """
    _require_xlrd()
    
    workbook = xlrd.open_workbook(file_path)
    sheets: list[tuple[str, list[list[Any]]]] = []
    
    for sheet_idx in range(workbook.nsheets):
        sheet = workbook.sheet_by_index(sheet_idx)
        rows: list[list[Any]] = []
        
        for row_idx in range(sheet.nrows):
            row = sheet.row_values(row_idx)
            if any(cell for cell in row):
                rows.append(row)
        
        if rows:
            sheets.append((sheet.name, rows))
    
    return sheets


def _row_to_text(row: list[Any], headers: list[Any] | None = None) -> str:
    """Convert a row to text representation."""
    if headers:
        parts = []
        for header, value in zip(headers, row):
            if value is not None and str(value).strip():
                header_str = str(header) if header else f"Col{len(parts)+1}"
                parts.append(f"{header_str}={value}")
        return ", ".join(parts)
    else:
        return ", ".join(str(v) for v in row if v is not None and str(v).strip())


def chunk_excel_sheets(
    sheets: list[tuple[str, list[list[Any]]]],
    rows_per_chunk: int = 50,
) -> list[DocumentChunk]:
    """
    Chunk Excel sheets into smaller pieces.
    
    Each chunk maintains sheet name and row range metadata.
    """
    chunks: list[DocumentChunk] = []
    chunk_index = 0
    
    for sheet_name, rows in sheets:
        if not rows:
            continue
        
        # Use first row as headers if it looks like headers
        headers = rows[0] if rows else None
        data_rows = rows[1:] if headers else rows
        
        # Chunk by row ranges
        for i in range(0, len(data_rows), rows_per_chunk):
            chunk_rows = data_rows[i:i + rows_per_chunk]
            row_start = i + 2 if headers else i + 1  # 1-indexed, account for header
            row_end = row_start + len(chunk_rows) - 1
            
            # Convert rows to text
            text_lines = []
            for j, row in enumerate(chunk_rows):
                row_num = row_start + j
                row_text = _row_to_text(row, headers)
                if row_text:
                    text_lines.append(f"Row {row_num}: {row_text}")
            
            if text_lines:
                chunks.append(DocumentChunk(
                    chunk_index=chunk_index,
                    content="\n".join(text_lines),
                    metadata={
                        "sheet": sheet_name,
                        "row_start": row_start,
                        "row_end": row_end,
                    },
                ))
                chunk_index += 1
    
    return chunks


# ============================================================================
# High-Level Processing Functions
# ============================================================================

def process_document(
    file_path: str,
    doc_type: DocumentType,
    chunk_size: int = 2000,
    rows_per_chunk: int = 50,
) -> list[DocumentChunk]:
    """
    Process a document and return chunks.
    
    Args:
        file_path: Path to the document file
        doc_type: Type of document (pdf, xlsx, xls)
        chunk_size: Max characters per chunk for PDF
        rows_per_chunk: Max rows per chunk for Excel
        
    Returns:
        List of DocumentChunk objects
    """
    if doc_type == DocumentType.PDF:
        pages = extract_pdf_text(file_path)
        return chunk_pdf_pages(pages, chunk_size)
    
    elif doc_type == DocumentType.XLSX:
        sheets = extract_xlsx_data(file_path)
        return chunk_excel_sheets(sheets, rows_per_chunk)
    
    elif doc_type == DocumentType.XLS:
        sheets = extract_xls_data(file_path)
        return chunk_excel_sheets(sheets, rows_per_chunk)
    
    else:
        raise ValueError(f"Unsupported document type: {doc_type}")


def get_document_type_from_filename(filename: str) -> DocumentType | None:
    """Determine document type from filename extension."""
    ext = Path(filename).suffix.lower()
    
    if ext == ".pdf":
        return DocumentType.PDF
    elif ext == ".xlsx":
        return DocumentType.XLSX
    elif ext == ".xls":
        return DocumentType.XLS
    
    return None


def validate_mime_type(content_type: str | None, doc_type: DocumentType) -> bool:
    """Validate MIME type matches expected document type."""
    if not content_type:
        return True  # Allow if no content type provided
    
    valid_types = {
        DocumentType.PDF: [
            "application/pdf",
        ],
        DocumentType.XLSX: [
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ],
        DocumentType.XLS: [
            "application/vnd.ms-excel",
            "application/x-msexcel",
        ],
    }
    
    return content_type in valid_types.get(doc_type, [])


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal and other issues."""
    # Remove path components
    filename = os.path.basename(filename)
    # Remove potentially dangerous characters
    filename = re.sub(r'[^\w\-_\. ]', '', filename)
    # Limit length
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        filename = name[:255 - len(ext)] + ext
    return filename
