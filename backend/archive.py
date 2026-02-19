"""
Archive database initialization and utilities.

This module provides database schema initialization and URL hashing.
All data access operations are in repositories/archive_repository.py.
"""
from __future__ import annotations

import hashlib
import sqlite3


CREATE_PAGES = """
CREATE TABLE IF NOT EXISTS pages (
    url_hash TEXT PRIMARY KEY,
    url TEXT,
    content TEXT,
    timestamp DATETIME
)
"""

CREATE_HISTORY = """
CREATE TABLE IF NOT EXISTS search_history (
    query TEXT,
    url_hash TEXT,
    timestamp DATETIME
)
"""

CREATE_ANSWERS = """
CREATE TABLE IF NOT EXISTS answers (
    query TEXT PRIMARY KEY,
    answer TEXT,
    citation_url TEXT,
    evidence_quote TEXT,
    timestamp DATETIME
)
"""

CREATE_SOURCE_METADATA = """
CREATE TABLE IF NOT EXISTS source_metadata (
    source_name TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    last_checked DATETIME,
    last_modified DATETIME,
    status TEXT,
    ttl_minutes INTEGER,
    error_message TEXT
)
"""


def hash_url(url: str) -> str:
    """Generate MD5 hash for a URL."""
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def init_db(db_path: str) -> None:
    """Initialize all database tables."""
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(CREATE_PAGES)
        cur.execute(CREATE_HISTORY)
        cur.execute(CREATE_ANSWERS)
        cur.execute(CREATE_SOURCE_METADATA)
        conn.commit()
    
    from .documents import init_document_tables
    init_document_tables(db_path)
