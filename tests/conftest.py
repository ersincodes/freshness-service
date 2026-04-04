"""Pytest configuration and LLM E2E fixtures for chat QA rubric tests."""
from __future__ import annotations

import os
import shutil
import threading
import uuid
from pathlib import Path
from typing import Any

import pytest

_lm_health_lock = threading.Lock()
_lm_health_verified = False

# Intentionally no top-level `backend` imports: system pytest without venv would fail
# conftest collection before any skip. Imports run inside `llm_e2e_context` when needed.


def _run_chat_llm_e2e_enabled() -> bool:
    raw = os.getenv("RUN_CHAT_LLM_E2E", "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _default_workbook_path() -> Path:
    env = os.getenv("FRESHNESS_QA_WORKBOOK", "").strip()
    if env:
        return Path(env).resolve()
    return (Path(__file__).resolve().parent / "fixtures" / "Advanced_Sales_Dataset.xlsx").resolve()


def _apply_llm_env_to_overrides(overrides: dict[str, Any]) -> None:
    """Refresh LM settings from the current process env when the E2E fixture runs.

    Ensures shell exports (and IDE test env) apply even if `backend.config` was imported earlier.
    """
    url = os.getenv("LM_STUDIO_BASE_URL", "").strip()
    if url:
        overrides["lm_studio_base_url"] = url
    model = os.getenv("MODEL_NAME", "").strip()
    if model:
        overrides["model_name"] = model
    raw_to = os.getenv("LLM_REQUEST_TIMEOUT_S", "").strip()
    if raw_to:
        try:
            overrides["llm_request_timeout_s"] = int(raw_to)
        except ValueError:
            pass


def _verify_lm_studio_reachable() -> None:
    """One-time GET /v1/models so failures explain URL/port/WSL instead of opaque chat errors."""
    import requests

    from backend.config import get_settings

    s = get_settings()
    base = s.lm_studio_base_url.rstrip("/")
    timeout = min(25, max(5, s.llm_request_timeout_s))
    try:
        resp = requests.get(f"{base}/models", timeout=timeout)
    except requests.exceptions.RequestException as exc:
        pytest.fail(
            f"Cannot reach LM Studio at {base} ({exc!s}).\n\n"
            f"Checklist:\n"
            f"  • LM Studio → Local Server: server must be **Running**; copy the shown base URL (usually ends with /v1).\n"
            f"  • Port is often **1234**, while this repo's default is **1111**. Try:\n"
            f"      export LM_STUDIO_BASE_URL=http://100.112.3.138:1111/v1\n"
            f"  • **WSL2**: if LM Studio runs on Windows, `localhost` from Linux may not reach it. Use the Windows\n"
            f"    host IP (first `nameserver` in /etc/resolv.conf), e.g. http://172.x.x.x:1234/v1\n"
            f"  • Set **MODEL_NAME** to an id listed under that server (see /v1/models in a browser or curl).\n\n"
            f"To skip this probe: SKIP_LLM_HEALTHCHECK=1\n",
        )
    if resp.status_code != 200:
        pytest.fail(
            f"LM Studio at {base}/models returned HTTP {resp.status_code}. "
            f"Fix LM_STUDIO_BASE_URL or start the local server."
        )


def _verify_lm_studio_reachable_once() -> None:
    global _lm_health_verified
    if os.getenv("SKIP_LLM_HEALTHCHECK", "").strip().lower() in ("1", "true", "yes", "on"):
        return
    with _lm_health_lock:
        if _lm_health_verified:
            return
        _verify_lm_studio_reachable()
        _lm_health_verified = True


@pytest.fixture
def llm_e2e_workbook_path() -> Path:
    """Path to the Excel workbook; skips when E2E is disabled or file is missing."""
    if not _run_chat_llm_e2e_enabled():
        pytest.skip("Set RUN_CHAT_LLM_E2E=1 to run LLM end-to-end chat rubric tests.")
    path = _default_workbook_path()
    if not path.is_file():
        pytest.skip(
            f"QA workbook not found: {path}. Set FRESHNESS_QA_WORKBOOK or add the file under tests/fixtures/."
        )
    return path


@pytest.fixture
def llm_e2e_context(tmp_path: Path, llm_e2e_workbook_path: Path) -> Any:
    """Isolated DB, uploaded workbook, tabular ingestion, document chunks, and metadata connection."""
    try:
        from backend import archive
        from backend.config import reset_runtime_settings, update_settings
        from backend.documents import DocumentStatus, DocumentType, ingest_excel_to_sqlite, process_document
        from backend.lifecycle import (
            open_analytics_connection,
            run_analytics_migrations,
            set_process_metadata_connection,
        )
        from backend.repositories.document_repository import DocumentRepository
    except ModuleNotFoundError as exc:
        pytest.skip(
            f"Missing dependency {exc.name!r}. Install project requirements in a virtualenv, then use that "
            f"interpreter's pytest (not system /usr/bin/pytest). Example: "
            f"python3 -m venv .venv && .venv/bin/pip install -r requirements.txt && "
            f"RUN_CHAT_LLM_E2E=1 .venv/bin/pytest -q tests/test_chat_rubric_llm_e2e.py"
        )

    db_path = str(tmp_path / "e2e.db")
    upload_dir = str(tmp_path / "uploads")
    os.makedirs(upload_dir, exist_ok=True)
    chroma_dir = str(tmp_path / "chroma")

    overrides: dict[str, Any] = {
        "db_path": db_path,
        "upload_dir": upload_dir,
        "chroma_dir": chroma_dir,
        "enable_tabular_analytics": True,
        "offline_retrieval_mode": "keyword",
    }
    _apply_llm_env_to_overrides(overrides)
    update_settings(overrides)
    _verify_lm_studio_reachable_once()

    archive.init_db(db_path)
    run_analytics_migrations(db_path)

    document_id = str(uuid.uuid4())
    filename = llm_e2e_workbook_path.name
    stored_path = os.path.join(upload_dir, f"{document_id}_{filename}")
    shutil.copy2(llm_e2e_workbook_path, stored_path)

    size_bytes = os.path.getsize(stored_path)
    doc_repo = DocumentRepository(db_path, upload_dir)
    doc_repo.save_document(
        document_id,
        filename,
        DocumentType.XLSX,
        size_bytes,
        status=DocumentStatus.READY,
    )

    chunks = process_document(stored_path, DocumentType.XLSX)
    doc_repo.save_chunks(
        document_id,
        [(c.chunk_index, c.content, c.metadata) for c in chunks],
    )

    conn = open_analytics_connection(db_path)
    ingest_excel_to_sqlite(
        excel_path=stored_path,
        document_id=document_id,
        sqlite_connection=conn,
    )
    set_process_metadata_connection(conn)

    ctx = {
        "document_id": document_id,
        "db_path": db_path,
        "workbook_path": str(llm_e2e_workbook_path),
    }
    try:
        yield ctx
    finally:
        try:
            conn.close()
        except Exception:
            pass
        set_process_metadata_connection(None)
        reset_runtime_settings()
