"""Scoped analytics document resolution (STEP 03)."""
from __future__ import annotations

from .metadata_repository import MetadataRepository
from .models import AnalyticsUnavailable


def effective_analytics_document_ids(
    metadata_repo: MetadataRepository,
    document_ids: list[str] | None,
) -> list[str] | AnalyticsUnavailable | None:
    """Resolve which document IDs may run tabular analytics.

    Returns:
        list[str]: ordered ids to attempt (preserves order of ``document_ids`` when explicit).
        AnalyticsUnavailable: non-empty ``document_ids`` but no registered tables in scope.
        None: analytics should not run (no tabular docs in global scope).
    """
    explicit = document_ids is not None and len(document_ids) > 0
    if explicit:
        tables = metadata_repo.list_tables_for_documents(document_ids)
        if not tables:
            return AnalyticsUnavailable(
                reason="no_eligible_table_in_selected_documents",
                document_ids=list(document_ids),
            )
        ids_with_tables = {t[0] for t in tables}
        ordered = [d for d in document_ids if d in ids_with_tables]
        if not ordered:
            return AnalyticsUnavailable(
                reason="no_eligible_table_in_selected_documents",
                document_ids=list(document_ids),
            )
        return ordered

    all_ready = metadata_repo.list_all_document_ids()
    return all_ready if all_ready else None
