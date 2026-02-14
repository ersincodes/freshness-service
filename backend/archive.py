from __future__ import annotations

import datetime as dt
import hashlib
import sqlite3
from typing import Iterable


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
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def init_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(CREATE_PAGES)
        cur.execute(CREATE_HISTORY)
        cur.execute(CREATE_ANSWERS)
        cur.execute(CREATE_SOURCE_METADATA)
        conn.commit()


def save_to_archive(db_path: str, query: str, url: str, content: str) -> None:
    url_hash = hash_url(url)
    now = dt.datetime.now()
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO pages VALUES (?, ?, ?, ?)",
            (url_hash, url, content, now),
        )
        cur.execute(
            "INSERT INTO search_history VALUES (?, ?, ?)",
            (query.lower(), url_hash, now),
        )
        conn.commit()


def search_offline(
    db_path: str, query: str, top_k: int = 3
) -> list[tuple[str, str, str]]:
    search_term = f"%{query.lower()}%"
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT p.url, p.content, p.timestamp
            FROM pages p
            JOIN search_history s ON p.url_hash = s.url_hash
            WHERE s.query LIKE ? OR p.content LIKE ?
            ORDER BY p.timestamp DESC
            LIMIT ?
            """,
            (search_term, search_term, top_k),
        )
        rows: Iterable[tuple[str, str, str]] = cur.fetchall()
    return list(rows)


def save_answer(
    db_path: str,
    query: str,
    answer: str,
    citation_url: str | None = None,
    evidence_quote: str | None = None,
) -> None:
    """Save a successful answer to the cache for offline retrieval."""
    now = dt.datetime.now()
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO answers (query, answer, citation_url, evidence_quote, timestamp)
            VALUES (?, ?, ?, ?, ?)
            """,
            (query.lower().strip(), answer, citation_url, evidence_quote, now),
        )
        conn.commit()


def get_cached_answer(
    db_path: str, query: str
) -> tuple[str, str | None, str | None, str] | None:
    """
    Retrieve a cached answer for the given query.
    
    Returns: (answer, citation_url, evidence_quote, timestamp) or None if not found.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT answer, citation_url, evidence_quote, timestamp
            FROM answers
            WHERE query = ?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (query.lower().strip(),),
        )
        row = cur.fetchone()
    return row if row else None


# ============================================================================
# Source Metadata Functions (for TTL-based freshness tracking)
# ============================================================================

def save_source_metadata(
    db_path: str,
    source_name: str,
    source_type: str,
    last_modified: dt.datetime | None,
    status: str,
    ttl_minutes: int,
    error_message: str | None = None,
) -> None:
    """
    Save or update source metadata for freshness tracking.
    
    Args:
        db_path: Path to SQLite database
        source_name: Unique source identifier
        source_type: Type of source (postgres, s3, api, file, archive)
        last_modified: Timestamp of last data modification
        status: Freshness status (fresh, stale, error, unknown)
        ttl_minutes: Configured TTL in minutes
        error_message: Optional error details
    """
    now = dt.datetime.now()
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO source_metadata 
            (source_name, source_type, last_checked, last_modified, status, ttl_minutes, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (source_name, source_type, now, last_modified, status, ttl_minutes, error_message),
        )
        conn.commit()


def get_source_metadata(
    db_path: str, source_name: str
) -> tuple[str, str, str | None, str | None, str, int, str | None] | None:
    """
    Retrieve metadata for a specific source.
    
    Returns: (source_name, source_type, last_checked, last_modified, status, ttl_minutes, error_message)
             or None if not found.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT source_name, source_type, last_checked, last_modified, status, ttl_minutes, error_message
            FROM source_metadata
            WHERE source_name = ?
            """,
            (source_name,),
        )
        row = cur.fetchone()
    return row if row else None


def get_all_source_metadata(
    db_path: str,
) -> list[tuple[str, str, str | None, str | None, str, int, str | None]]:
    """
    Retrieve metadata for all tracked sources.
    
    Returns: List of (source_name, source_type, last_checked, last_modified, status, ttl_minutes, error_message)
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT source_name, source_type, last_checked, last_modified, status, ttl_minutes, error_message
            FROM source_metadata
            ORDER BY last_checked DESC
            """
        )
        rows = cur.fetchall()
    return list(rows)


def get_archive_stats(db_path: str) -> dict:
    """
    Get statistics about the archive database.
    
    Returns:
        Dictionary with page count, oldest/newest timestamps, and total size
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        
        # Get page count
        cur.execute("SELECT COUNT(*) FROM pages")
        page_count = cur.fetchone()[0]
        
        # Get timestamp range
        cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM pages")
        row = cur.fetchone()
        oldest_timestamp = row[0] if row else None
        newest_timestamp = row[1] if row else None
        
        # Get answer count
        cur.execute("SELECT COUNT(*) FROM answers")
        answer_count = cur.fetchone()[0]
    
    return {
        "page_count": page_count,
        "answer_count": answer_count,
        "oldest_page": str(oldest_timestamp) if oldest_timestamp else None,
        "newest_page": str(newest_timestamp) if newest_timestamp else None,
    }
