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


# ============================================================================
# Document Chunks Collection
# ============================================================================

def get_document_chunks_collection(persist_dir: str, embed_model_name: str) -> Any:
    """Get or create the document_chunks collection."""
    _require_chromadb()
    client = _build_client(persist_dir)
    _ensure_tenant_database(client)
    embed_fn = SentenceTransformerEmbeddingFunction(model_name=embed_model_name)
    try:
        return client.get_or_create_collection(
            name="document_chunks",
            embedding_function=embed_fn,
            metadata={"hnsw:space": "cosine"},
            tenant="default_tenant",
            database="default_database",
        )
    except TypeError:
        return client.get_or_create_collection(
            name="document_chunks",
            embedding_function=embed_fn,
            metadata={"hnsw:space": "cosine"},
        )


def upsert_document_chunk(
    persist_dir: str,
    embed_model_name: str,
    chunk_id: str,
    document_id: str,
    filename: str,
    content: str,
    metadata: dict,
    timestamp_iso: str,
) -> None:
    """Upsert a document chunk to the vector store."""
    col = get_document_chunks_collection(persist_dir, embed_model_name)
    
    # Merge location metadata with standard fields
    full_metadata = {
        "document_id": document_id,
        "filename": filename,
        "timestamp": timestamp_iso,
        "chunk_id": chunk_id,
        **metadata,  # page, sheet, row_start, row_end
    }
    
    col.upsert(
        ids=[chunk_id],
        documents=[content],
        metadatas=[full_metadata],
    )


def delete_document_chunks_from_vector_store(
    persist_dir: str,
    embed_model_name: str,
    document_id: str,
) -> None:
    """Delete all chunks for a document from the vector store."""
    col = get_document_chunks_collection(persist_dir, embed_model_name)
    
    # Query to find all chunks for this document
    try:
        # Get all chunks with this document_id
        results = col.get(
            where={"document_id": document_id},
            include=["metadatas"],
        )
        
        if results and results.get("ids"):
            col.delete(ids=results["ids"])
    except Exception:
        # If where clause fails, try alternative approach
        pass


def query_document_chunks_similar(
    persist_dir: str,
    embed_model_name: str,
    query: str,
    top_k: int = 5,
    document_ids: list[str] | None = None,
) -> list[tuple[str, str, str, dict, str]]:
    """
    Query similar document chunks.
    
    Returns: List of (chunk_id, document_id, content, metadata, filename) tuples
    """
    col = get_document_chunks_collection(persist_dir, embed_model_name)
    
    # Build where clause if filtering by document IDs
    where_clause = None
    if document_ids:
        if len(document_ids) == 1:
            where_clause = {"document_id": document_ids[0]}
        else:
            where_clause = {"document_id": {"$in": document_ids}}
    
    try:
        if where_clause:
            res: dict[str, Iterable] = col.query(
                query_texts=[query],
                n_results=top_k,
                where=where_clause,
            )
        else:
            res = col.query(query_texts=[query], n_results=top_k)
    except Exception:
        # Fallback without where clause
        res = col.query(query_texts=[query], n_results=top_k)
    
    documents = res.get("documents", [[]])[0]
    metadatas = res.get("metadatas", [[]])[0]
    
    out: list[tuple[str, str, str, dict, str]] = []
    for doc, meta in zip(documents, metadatas):
        chunk_id = meta.get("chunk_id", "")
        document_id = meta.get("document_id", "")
        filename = meta.get("filename", "")
        # Extract location metadata
        location_meta = {
            k: v for k, v in meta.items()
            if k in ("page", "sheet", "row_start", "row_end")
        }
        out.append((chunk_id, document_id, doc, location_meta, filename))
    
    return out
