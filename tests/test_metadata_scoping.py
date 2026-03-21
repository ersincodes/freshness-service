"""Tests for document-scoped metadata reads (STEP 02)."""
from __future__ import annotations

import sqlite3

import pytest

from backend.analytics.metadata_repository import MetadataRepository
from backend.analytics.models import ColumnMetadata


@pytest.fixture
def meta_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE documents (
            document_id TEXT PRIMARY KEY,
            filename TEXT,
            doc_type TEXT,
            size_bytes INTEGER,
            status TEXT,
            uploaded_at TEXT,
            error_message TEXT
        );
        CREATE TABLE document_tables (
            document_id TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (document_id, sheet_name),
            UNIQUE (table_name)
        );
        CREATE TABLE document_table_columns (
            document_id TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            original_name TEXT NOT NULL,
            safe_name TEXT NOT NULL,
            inferred_type TEXT NOT NULL,
            logical_type TEXT NOT NULL DEFAULT 'string',
            sqlite_type TEXT NOT NULL DEFAULT 'TEXT',
            nullable INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (document_id, sheet_name, ordinal),
            UNIQUE (document_id, sheet_name, safe_name)
        );
        INSERT INTO documents VALUES ('id-1', 'a.xlsx', 'xlsx', 100, 'ready', '2020-01-01', NULL);
        INSERT INTO documents VALUES ('id-2', 'b.xlsx', 'xlsx', 100, 'ready', '2020-01-01', NULL);
        INSERT INTO document_tables VALUES
            ('id-1', 'Sheet1', 't_id1_s1', 5, datetime('now'), datetime('now')),
            ('id-2', 'Data', 't_id2_d', 10, datetime('now'), datetime('now'));
        INSERT INTO document_table_columns VALUES
            ('id-1', 'Sheet1', 0, 'A', 'a', 'string', 'string', 'TEXT', 0),
            ('id-1', 'Sheet1', 1, 'B', 'b', 'integer', 'integer', 'INTEGER', 0),
            ('id-2', 'Data', 0, 'X', 'x', 'string', 'string', 'TEXT', 1);
        """
    )
    yield conn
    conn.close()


def test_list_tables_for_documents_empty_ids(meta_db: sqlite3.Connection) -> None:
    repo = MetadataRepository(meta_db)
    assert repo.list_tables_for_documents([]) == []


def test_list_tables_for_documents_single_doc(meta_db: sqlite3.Connection) -> None:
    repo = MetadataRepository(meta_db)
    rows = repo.list_tables_for_documents(["id-1"])
    assert rows == [("id-1", "Sheet1", "t_id1_s1", 5)]


def test_list_tables_for_documents_unknown_doc(meta_db: sqlite3.Connection) -> None:
    repo = MetadataRepository(meta_db)
    assert repo.list_tables_for_documents(["id-99"]) == []


def test_list_columns_for_documents_empty_ids(meta_db: sqlite3.Connection) -> None:
    repo = MetadataRepository(meta_db)
    assert repo.list_columns_for_documents([]) == {}


def test_list_columns_for_documents_scoped(meta_db: sqlite3.Connection) -> None:
    repo = MetadataRepository(meta_db)
    cols = repo.list_columns_for_documents(["id-1"])
    assert list(cols.keys()) == ["id-1"]
    assert len(cols["id-1"]) == 2
    assert cols["id-1"][0].original_name == "A"
    assert cols["id-1"][1].original_name == "B"


def test_list_all_tables_joins_ready_documents(meta_db: sqlite3.Connection) -> None:
    repo = MetadataRepository(meta_db)
    rows = repo.list_all_tables()
    assert len(rows) == 2
    assert ("id-1", "Sheet1", "t_id1_s1", 5) in rows
