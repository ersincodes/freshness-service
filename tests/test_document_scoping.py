from __future__ import annotations

from pathlib import Path

from backend.documents import DocumentStatus, DocumentType, init_document_tables
from backend.repositories.document_repository import DocumentRepository
from backend.vector_store import query_document_chunks_similar


def _build_repo(tmp_path: Path) -> DocumentRepository:
    db_path = tmp_path / "scoping.db"
    upload_dir = tmp_path / "uploads"
    init_document_tables(str(db_path))
    return DocumentRepository(str(db_path), str(upload_dir))


def _seed_two_docs(repo: DocumentRepository) -> None:
    repo.save_document("id-1", "doc-1.pdf", DocumentType.PDF, 10, DocumentStatus.READY)
    repo.save_document("id-2", "doc-2.pdf", DocumentType.PDF, 10, DocumentStatus.READY)

    repo.save_chunks(
        "id-1",
        [
            (0, "alpha match from document one", {"page": 1}),
            (1, "alpha second line from document one", {"page": 2}),
        ],
    )
    repo.save_chunks(
        "id-2",
        [
            (0, "alpha match from document two", {"page": 1}),
            (1, "beta only line from document two", {"page": 2}),
        ],
    )


def test_fetch_chunks_scoped_returns_only_requested_document(tmp_path: Path) -> None:
    repo = _build_repo(tmp_path)
    _seed_two_docs(repo)

    rows = repo.fetch_chunks("alpha", top_k=50, document_ids=["id-1"])

    assert rows
    assert {r.document_id for r in rows} == {"id-1"}


def test_fetch_chunks_without_document_ids_returns_global_results(tmp_path: Path) -> None:
    repo = _build_repo(tmp_path)
    _seed_two_docs(repo)

    rows = repo.fetch_chunks("alpha", top_k=50)

    assert rows
    assert {r.document_id for r in rows} == {"id-1", "id-2"}


def test_fetch_chunks_empty_document_ids_behaves_like_global(tmp_path: Path) -> None:
    repo = _build_repo(tmp_path)
    _seed_two_docs(repo)

    rows_none = repo.fetch_chunks("alpha", top_k=50, document_ids=None)
    rows_empty = repo.fetch_chunks("alpha", top_k=50, document_ids=[])

    assert {r.chunk_id for r in rows_empty} == {r.chunk_id for r in rows_none}


def test_semantic_fallback_never_drops_document_scope(monkeypatch) -> None:
    class FakeCollection:
        def __init__(self) -> None:
            self.calls: list[dict | None] = []

        def query(self, *, query_texts, n_results, where=None):
            self.calls.append(where)

            # Simulate an "$in" query failure so fallback path is exercised.
            if isinstance(where, dict) and isinstance(where.get("document_id"), dict):
                raise RuntimeError("Unsupported where filter")

            if where is None:
                raise AssertionError("Unscoped fallback query must not be used")

            doc_id = str(where["document_id"])
            return {
                "documents": [[f"chunk text for {doc_id}"]],
                "metadatas": [[{
                    "chunk_id": f"chunk-{doc_id}",
                    "document_id": doc_id,
                    "filename": f"{doc_id}.pdf",
                }]],
            }

    fake_collection = FakeCollection()

    def _fake_get_collection(persist_dir: str, embed_model_name: str):
        return fake_collection

    monkeypatch.setattr("backend.vector_store.get_document_chunks_collection", _fake_get_collection)

    rows = query_document_chunks_similar(
        persist_dir="unused",
        embed_model_name="unused",
        query="alpha",
        top_k=5,
        document_ids=["id-1", "id-2"],
    )

    assert {r[1] for r in rows} == {"id-1", "id-2"}
    assert None not in fake_collection.calls
