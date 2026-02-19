"""
Archive repository for managing web page archive data.

Encapsulates all SQLite operations for pages, search_history, and answers tables.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import sqlite3
from dataclasses import dataclass

from ..archive import hash_url


@dataclass(frozen=True)
class ArchivePage:
    """Represents an archived web page."""
    url_hash: str
    url: str
    content: str
    timestamp: str


@dataclass(frozen=True)
class ArchiveEntry:
    """Archive entry for list views (with excerpt)."""
    url_hash: str
    url: str
    excerpt: str
    timestamp: str


@dataclass(frozen=True)
class ArchiveSearchResult:
    """Result of an archive search operation."""
    entries: list[ArchiveEntry]
    total: int
    cursor: str | None


@dataclass(frozen=True)
class CachedAnswer:
    """Represents a cached LLM answer."""
    query: str
    answer: str
    citation_url: str | None
    evidence_quote: str | None
    timestamp: str


class ArchiveRepository:
    """Repository for archive data access operations."""
    
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
    
    @staticmethod
    def hash_url(url: str) -> str:
        return hash_url(url)
    
    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self._db_path)
    
    def search_pages(self, query: str = "", limit: int = 20, cursor: str | None = None) -> ArchiveSearchResult:
        """Search archive pages."""
        with self._conn() as conn:
            cur = conn.cursor()
            if query:
                term = f"%{query.lower()}%"
                cur.execute(
                    "SELECT url_hash, url, content, timestamp FROM pages WHERE url LIKE ? OR content LIKE ? ORDER BY timestamp DESC LIMIT ?",
                    (term, term, limit + 1),
                )
            else:
                cur.execute("SELECT url_hash, url, content, timestamp FROM pages ORDER BY timestamp DESC LIMIT ?", (limit + 1,))
            rows = cur.fetchall()
            
            if query:
                term = f"%{query.lower()}%"
                cur.execute("SELECT COUNT(*) FROM pages WHERE url LIKE ? OR content LIKE ?", (term, term))
            else:
                cur.execute("SELECT COUNT(*) FROM pages")
            total = cur.fetchone()[0]
        
        has_more = len(rows) > limit
        rows = rows[:limit] if has_more else rows
        entries = [ArchiveEntry(r[0], r[1], r[2][:200] + "..." if len(r[2]) > 200 else r[2], str(r[3])) for r in rows]
        return ArchiveSearchResult(entries, total, entries[-1].url_hash if has_more and entries else None)
    
    def get_page(self, url_hash: str) -> ArchivePage | None:
        """Get archived page by hash."""
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT url_hash, url, content, timestamp FROM pages WHERE url_hash = ?", (url_hash,))
            row = cur.fetchone()
        return ArchivePage(row[0], row[1], row[2], str(row[3])) if row else None
    
    def save_page(self, query: str, url: str, content: str) -> str:
        """Save page to archive."""
        url_hash = hash_url(url)
        now = dt.datetime.now()
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO pages VALUES (?, ?, ?, ?)", (url_hash, url, content, now))
            cur.execute("INSERT INTO search_history VALUES (?, ?, ?)", (query.lower(), url_hash, now))
            conn.commit()
        return url_hash
    
    def search_offline(self, query: str, top_k: int = 3) -> list[tuple[str, str, str]]:
        """Keyword search in archive."""
        term = f"%{query.lower()}%"
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """SELECT DISTINCT p.url, p.content, p.timestamp FROM pages p
                   JOIN search_history s ON p.url_hash = s.url_hash
                   WHERE s.query LIKE ? OR p.content LIKE ? ORDER BY p.timestamp DESC LIMIT ?""",
                (term, term, top_k),
            )
            return list(cur.fetchall())
    
    def save_answer(self, query: str, answer: str, citation_url: str | None = None, evidence_quote: str | None = None) -> None:
        """Cache an answer."""
        now = dt.datetime.now()
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT OR REPLACE INTO answers (query, answer, citation_url, evidence_quote, timestamp) VALUES (?, ?, ?, ?, ?)",
                (query.lower().strip(), answer, citation_url, evidence_quote, now),
            )
            conn.commit()
    
    def get_cached_answer(self, query: str) -> CachedAnswer | None:
        """Get cached answer."""
        with self._conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT query, answer, citation_url, evidence_quote, timestamp FROM answers WHERE query = ? ORDER BY timestamp DESC LIMIT 1",
                (query.lower().strip(),),
            )
            row = cur.fetchone()
        return CachedAnswer(row[0], row[1], row[2], row[3], str(row[4])) if row else None
    
    # Async wrappers
    async def search_pages_async(self, query: str = "", limit: int = 20, cursor: str | None = None) -> ArchiveSearchResult:
        return await asyncio.to_thread(self.search_pages, query, limit, cursor)
    
    async def get_page_async(self, url_hash: str) -> ArchivePage | None:
        return await asyncio.to_thread(self.get_page, url_hash)
    
    async def save_page_async(self, query: str, url: str, content: str) -> str:
        return await asyncio.to_thread(self.save_page, query, url, content)
    
    async def search_offline_async(self, query: str, top_k: int = 3) -> list[tuple[str, str, str]]:
        return await asyncio.to_thread(self.search_offline, query, top_k)
    
    async def save_answer_async(self, query: str, answer: str, citation_url: str | None = None, evidence_quote: str | None = None) -> None:
        return await asyncio.to_thread(self.save_answer, query, answer, citation_url, evidence_quote)
    
    async def get_cached_answer_async(self, query: str) -> CachedAnswer | None:
        return await asyncio.to_thread(self.get_cached_answer, query)
