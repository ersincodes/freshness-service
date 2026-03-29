"""Application startup: database init, analytics migrations, and process-wide analytics SQLite handle."""
from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path

from . import archive
from .analytics.metadata_repository import MetadataRepository
from .config import get_settings
from .documents import DocumentStatus, DocumentType, ingest_excel_to_sqlite
from .repositories import DocumentRepository

logger = logging.getLogger(__name__)

_analytics_conn: sqlite3.Connection | None = None


def open_analytics_connection(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection configured for analytics (WAL, check_same_thread=False)."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def get_process_analytics_connection() -> sqlite3.Connection | None:
    """Long-lived connection used by ChatService metadata after startup."""
    return _analytics_conn


def _set_process_analytics_connection(conn: sqlite3.Connection | None) -> None:
    global _analytics_conn
    _analytics_conn = conn


def run_analytics_migrations(db_path: str) -> None:
    """Execute all SQL migration files for tabular analytics schema in order."""
    migrations_dir = Path(__file__).resolve().parent / "migrations"
    if not migrations_dir.is_dir():
        logger.warning("Migrations directory not found: %s", migrations_dir)
        return
    migration_files = sorted(migrations_dir.glob("*.sql"))
    with sqlite3.connect(db_path) as conn:
        for mf in migration_files:
            try:
                sql = mf.read_text(encoding="utf-8")
                conn.executescript(sql)
                logger.info("Migration applied: %s", mf.name)
            except sqlite3.OperationalError as exc:
                if "duplicate column" in str(exc).lower():
                    logger.debug("Migration %s: column already exists, skipping", mf.name)
                else:
                    logger.warning("Migration %s failed: %s", mf.name, exc)


def get_metadata_repository() -> MetadataRepository | None:
    if _analytics_conn is None:
        return None
    return MetadataRepository(_analytics_conn)


def cleanup_orphaned_analytics(conn: sqlite3.Connection) -> None:
    """Remove analytics metadata for documents that no longer exist."""
    meta_repo = MetadataRepository(conn)
    cursor = conn.execute(
        "SELECT DISTINCT dt.document_id FROM document_tables dt "
        "LEFT JOIN documents d ON dt.document_id = d.document_id "
        "WHERE d.document_id IS NULL;"
    )
    orphaned = [str(r[0]) for r in cursor.fetchall()]
    for doc_id in orphaned:
        try:
            meta_repo.delete_document(doc_id)
            logger.info("Cleaned up orphaned analytics for document %s", doc_id)
        except Exception as exc:
            logger.warning("Orphan cleanup failed for %s: %s", doc_id, exc)
    if orphaned:
        logger.info("Cleaned up %d orphaned analytics entries", len(orphaned))


def retroactive_analytics_ingestion(db_path: str, upload_dir: str) -> None:
    """Ingest existing Excel documents missing from analytics; re-forecast outdated pipelines."""
    conn = open_analytics_connection(db_path)

    cleanup_orphaned_analytics(conn)

    meta_repo = MetadataRepository(conn)
    already_ingested = set(meta_repo.list_all_document_ids())

    doc_repo = DocumentRepository(db_path, upload_dir)
    all_docs = doc_repo.list_documents()
    excel_types = {DocumentType.XLSX, DocumentType.XLS}
    ingested_count = 0

    for doc in all_docs:
        if doc.doc_type not in excel_types:
            continue
        if doc.status != DocumentStatus.READY:
            continue
        if doc.document_id in already_ingested:
            continue

        file_path = os.path.join(upload_dir, f"{doc.document_id}_{doc.filename}")
        if not os.path.isfile(file_path):
            logger.warning("Retroactive ingestion: file not found at %s", file_path)
            continue

        try:
            ingest_excel_to_sqlite(
                excel_path=file_path,
                document_id=doc.document_id,
                sqlite_connection=conn,
            )
            ingested_count += 1
            logger.info("Retroactively ingested analytics for document %s (%s)", doc.document_id, doc.filename)
        except Exception as exc:
            logger.warning("Retroactive ingestion failed for %s: %s", doc.document_id, exc)

    if ingested_count:
        logger.info("Retroactive analytics ingestion complete: %d document(s)", ingested_count)

    upgrade_outdated_forecasts(conn, doc_repo, upload_dir)

    conn.close()


def upgrade_outdated_forecasts(
    conn: sqlite3.Connection,
    doc_repo: DocumentRepository,
    upload_dir: str,
) -> None:
    """Re-forecast documents that still have v1 (or older) pipeline artifacts."""
    from .analytics.forecast_repository import ForecastRepository, PIPELINE_VERSION_FORECAST

    fc_repo = ForecastRepository(conn)
    outdated_ids = fc_repo.list_outdated_document_ids(PIPELINE_VERSION_FORECAST)
    if not outdated_ids:
        return

    logger.info("Found %d document(s) with outdated forecast artifacts, re-forecasting...", len(outdated_ids))
    upgraded = 0
    for doc_id in outdated_ids:
        info = doc_repo.get_document(doc_id)
        if info is None:
            continue
        file_path = os.path.join(upload_dir, f"{doc_id}_{info.filename}")
        if not os.path.isfile(file_path):
            continue
        try:
            fc_repo.delete_for_document(doc_id)
            ingest_excel_to_sqlite(
                excel_path=file_path,
                document_id=doc_id,
                sqlite_connection=conn,
            )
            upgraded += 1
            logger.info("Re-forecasted document %s (%s)", doc_id, info.filename)
        except Exception as exc:
            logger.warning("Re-forecast failed for %s: %s", doc_id, exc)

    if upgraded:
        logger.info("Forecast upgrade complete: %d document(s)", upgraded)


async def on_startup() -> None:
    """FastAPI startup: archive DB, migrations, optional analytics ingestion and singleton connection."""
    s = get_settings()
    archive.init_db(s.db_path)
    run_analytics_migrations(s.db_path)
    if s.enable_tabular_analytics:
        retroactive_analytics_ingestion(s.db_path, s.upload_dir)
        _set_process_analytics_connection(open_analytics_connection(s.db_path))
