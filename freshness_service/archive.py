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


def hash_url(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def init_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(CREATE_PAGES)
        cur.execute(CREATE_HISTORY)
        cur.execute(CREATE_ANSWERS)
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
