from __future__ import annotations

from dataclasses import dataclass
import os

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None

if load_dotenv:
    load_dotenv()


def _getenv_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _normalize_mode(raw: str | None) -> str:
    value = (raw or "keyword").strip().lower()
    return value if value in {"keyword", "semantic"} else "keyword"


@dataclass(frozen=True)
class Settings:
    brave_api_key: str | None
    lm_studio_base_url: str
    model_name: str
    db_path: str
    max_search_results: int
    offline_retrieval_mode: str
    chroma_dir: str
    embed_model_name: str
    semantic_top_k: int
    request_timeout_s: int
    max_chars_per_source: int


settings = Settings(
    brave_api_key=os.getenv("BRAVE_API_KEY"),
    lm_studio_base_url=os.getenv("LM_STUDIO_BASE_URL", "http://localhost:1111/v1"),
    model_name=os.getenv("MODEL_NAME", "rnj-1"),
    db_path=os.getenv("DB_PATH", "knowledge.db"),
    max_search_results=_getenv_int("MAX_SEARCH_RESULTS", 3),
    offline_retrieval_mode=_normalize_mode(os.getenv("OFFLINE_RETRIEVAL_MODE")),
    chroma_dir=os.getenv("CHROMA_DIR", "chroma_db"),
    embed_model_name=os.getenv(
        "EMBED_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2"
    ),
    semantic_top_k=_getenv_int("SEMANTIC_TOP_K", 3),
    request_timeout_s=_getenv_int("REQUEST_TIMEOUT_S", 10),
    max_chars_per_source=_getenv_int("MAX_CHARS_PER_SOURCE", 2000),
)
