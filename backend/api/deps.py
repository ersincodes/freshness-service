"""Dependency factories for services and repositories used by route handlers."""
from __future__ import annotations

from ..config import get_settings
from ..integrations import BraveClient, LLMClient
from ..lifecycle import get_metadata_repository
from ..repositories import ArchiveRepository, DocumentRepository
from ..services import ChatService, HealthService


def archive_repo() -> ArchiveRepository:
    return ArchiveRepository(get_settings().db_path)


def doc_repo() -> DocumentRepository:
    s = get_settings()
    return DocumentRepository(s.db_path, s.upload_dir)


def chat_service() -> ChatService:
    s = get_settings()
    return ChatService(
        s,
        LLMClient(s.lm_studio_base_url, s.model_name, s.llm_request_timeout_s),
        BraveClient(s.brave_api_key, s.request_timeout_s, s.max_search_results),
        archive_repo(),
        doc_repo(),
        metadata_repo=get_metadata_repository(),
    )


def health_service() -> HealthService:
    s = get_settings()
    return HealthService(
        LLMClient(s.lm_studio_base_url, s.model_name, s.llm_request_timeout_s),
        BraveClient(s.brave_api_key, s.request_timeout_s, s.max_search_results),
    )
