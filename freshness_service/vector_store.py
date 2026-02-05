from __future__ import annotations

from typing import Any, Iterable

try:
    import chromadb
    from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
except ImportError as exc:  # pragma: no cover - optional dependency
    chromadb = None
    SentenceTransformerEmbeddingFunction = None
    _CHROMA_IMPORT_ERROR = exc


def _require_chromadb() -> None:
    if chromadb is None:
        raise RuntimeError(
            "chromadb is not installed. Install with "
            "`pip install chromadb sentence-transformers`."
        ) from _CHROMA_IMPORT_ERROR


def _build_client(persist_dir: str) -> Any:
    try:
        return chromadb.PersistentClient(
            path=persist_dir,
            tenant="default_tenant",
            database="default_database",
        )
    except TypeError:
        return chromadb.PersistentClient(path=persist_dir)


def _ensure_tenant_database(client: Any) -> None:
    if hasattr(client, "get_or_create_tenant"):
        client.get_or_create_tenant("default_tenant")
    elif hasattr(client, "create_tenant"):
        try:
            client.create_tenant("default_tenant")
        except Exception:
            pass

    if hasattr(client, "get_or_create_database"):
        client.get_or_create_database("default_database", tenant="default_tenant")
    elif hasattr(client, "create_database"):
        try:
            client.create_database("default_database", tenant="default_tenant")
        except Exception:
            pass


def get_collection(persist_dir: str, embed_model_name: str) -> Any:
    _require_chromadb()
    client = _build_client(persist_dir)
    _ensure_tenant_database(client)
    embed_fn = SentenceTransformerEmbeddingFunction(model_name=embed_model_name)
    try:
        return client.get_or_create_collection(
            name="pages",
            embedding_function=embed_fn,
            metadata={"hnsw:space": "cosine"},
            tenant="default_tenant",
            database="default_database",
        )
    except TypeError:
        return client.get_or_create_collection(
            name="pages",
            embedding_function=embed_fn,
            metadata={"hnsw:space": "cosine"},
        )


def upsert_page(
    persist_dir: str,
    embed_model_name: str,
    url_hash: str,
    url: str,
    content: str,
    timestamp_iso: str,
) -> None:
    col = get_collection(persist_dir, embed_model_name)
    col.upsert(
        ids=[url_hash],
        documents=[content],
        metadatas=[{"url": url, "timestamp": timestamp_iso, "url_hash": url_hash}],
    )


def query_similar(
    persist_dir: str,
    embed_model_name: str,
    query: str,
    top_k: int = 3,
) -> list[tuple[str, str, str]]:
    col = get_collection(persist_dir, embed_model_name)
    res: dict[str, Iterable] = col.query(query_texts=[query], n_results=top_k)
    documents = res.get("documents", [[]])[0]
    metadatas = res.get("metadatas", [[]])[0]
    out: list[tuple[str, str, str]] = []
    for doc, meta in zip(documents, metadatas):
        out.append((meta.get("url", ""), doc, meta.get("timestamp", "")))
    return out
