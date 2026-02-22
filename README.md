# Freshness Service

Local-first RAG system with:

- FastAPI backend
- React + Vite frontend
- LM Studio (OpenAI-compatible) for generation
- Brave Search for fresh web retrieval
- SQLite + optional ChromaDB for offline retrieval
- Deterministic tabular analytics for Excel documents

## Latest Project Changes

- Added deterministic tabular analytics for uploaded Excel files (`.xlsx`, `.xls`).
- Added analytics metadata migrations in `backend/migrations/`.
- Added typed analytics schema and dataset profiling support.
- Integrated analytics routing inside `ChatService`:
  - heuristic query routing (aggregation/list/filter intent),
  - restricted JSON planning,
  - validated execution over ingested sheet tables.
- Refactored backend into clearer layers: `domain`, `integrations`, `repositories`, `services`, and `analytics`.
- Expanded decoupled context-budget controls for web/document retrieval blending.

## What This Service Does

- Retrieves fresh web context at query time.
- Archives sources and supports offline recall.
- Lets you upload and chat with PDF/Excel documents.
- Streams chat responses over SSE.
- Runs deterministic spreadsheet analytics when document queries are tabular.

## Requirements

- Python 3.10+ (3.11 recommended)
- Node.js 18+ (for frontend)
- LM Studio with local server enabled
- Brave Search API key (for online retrieval)

Install backend dependencies:

```bash
pip install -r requirements.txt
playwright install
```

## Quick Start

1) Create and activate virtual environment:

```bash
# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

# macOS/Linux
python -m venv .venv
source .venv/bin/activate
```

2) Set environment variables (via shell or `.env` file in repo root).

3) Start backend:

```bash
uvicorn backend.app:app --host 0.0.0.0 --port 8000
```

4) Start frontend:

```bash
cd frontend
npm install
npm run dev
```

Backend docs:

- Swagger: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI: `http://localhost:8000/openapi.json`

## Architecture (Current)

```text
backend/
  app.py                  # FastAPI routes + startup + migrations
  config.py               # Environment settings + runtime overrides
  archive.py              # SQLite archive initialization
  documents.py            # PDF/Excel extraction + chunking + ingestion
  scraper.py              # Web scraping/clean text extraction
  freshness.py            # Freshness source checks
  vector_store.py         # Chroma upsert/query helpers
  domain/                 # Shared domain models/utilities
  integrations/           # LM Studio + Brave clients
  repositories/           # Archive/document/analytics data access
  services/               # Chat + health orchestration
  analytics/              # Routing, planning, validation, SQL compile, execution
  migrations/             # SQL migrations for analytics metadata

frontend/
  src/components/         # Chat/archive/documents/settings UI
  src/lib/                # API client + hooks + shared types/utilities
  src/store/              # Chat state
```

## Environment Variables

Core:

- `BRAVE_API_KEY`
- `LM_STUDIO_BASE_URL` (default: `http://localhost:1111/v1`)
- `MODEL_NAME` (default: `rnj-1`)
- `DB_PATH` (default: `knowledge.db`)
- `REQUEST_TIMEOUT_S` (default: `10`)

Retrieval:

- `MAX_SEARCH_RESULTS` (default: `3`)
- `MAX_CHARS_PER_SOURCE` (default: `2000`)
- `OFFLINE_RETRIEVAL_MODE` (`keyword` or `semantic`, default: `keyword`)
- `SEMANTIC_TOP_K` (default: `3`)
- `CHROMA_DIR` (default: `chroma_db`)
- `EMBED_MODEL_NAME` (default: `sentence-transformers/all-MiniLM-L6-v2`)

Decoupled RAG budgets:

- `WEB_TOP_K` (default: `3`)
- `DOC_SEMANTIC_TOP_K` (default: `12`)
- `DOC_KEYWORD_TOP_K` (default: `20`)
- `WEB_MAX_CHARS` (default: `2000`)
- `DOC_MAX_CHARS` (default: `0`, unlimited)
- `TOTAL_CONTEXT_BUDGET` (default: `14000`)
- `WEB_BUDGET_FRACTION` (default: `0.4`)

Document processing:

- `UPLOAD_DIR` (default: `uploads`)
- `MAX_UPLOAD_MB` (default: `25`)

Tabular analytics:

- `ENABLE_TABULAR_ANALYTICS` (default: `true`)
- `ANALYTICS_GROUPBY_TOP_N_DEFAULT` (default: `50`)

## API Endpoints

Core/chat:

- `GET /` - service info
- `POST /api/chat` - non-streaming chat response
- `POST /api/chat/stream` - SSE chat stream (`meta`, `token`, `done`, `error`)

Archive:

- `GET /api/archive/search?q=...`
- `GET /api/archive/page/{url_hash}`

Documents:

- `POST /api/documents/upload`
- `GET /api/documents`
- `GET /api/documents/{document_id}`
- `DELETE /api/documents/{document_id}`

Settings/health:

- `GET /api/settings`
- `POST /api/config`
- `GET /api/health`

Freshness:

- `GET /api/freshness`
- `GET /api/freshness/{source_id}`
- `GET /api/freshness/sources/list`
- `POST /api/freshness/reload`

Legacy compatibility:

- `GET /freshness?query=...`

## Chat Request Shape

```json
{
  "query": "How many users signed up in 2020?",
  "conversation_id": "optional-id",
  "prefer_mode": "ONLINE",
  "include_web": true,
  "include_documents": true,
  "document_ids": ["optional-doc-id"]
}
```

Notes:

- If `include_documents=true`, analytics routing can short-circuit normal RAG flow for spreadsheet-style questions.
- If no context is available, mode falls back to `LOCAL_WEIGHTS`.

## Runtime Data

- `knowledge.db` (SQLite archive + document + analytics metadata)
- `knowledge.db-wal` / `knowledge.db-shm` (SQLite WAL sidecar files)
- `uploads/` (uploaded files)
- `chroma_db/` (if semantic mode is used)

## Testing

Run all tests:

```bash
pytest -q
```

Deterministic analytics contract tests:

```bash
pytest -q tests/test_analytics_deterministic.py
```

## Security Notes

- Treat web/document content as untrusted input.
- Prompt-injection defenses are applied in prompts, but source text can still be adversarial.
- Do not upload sensitive files unless your local machine/storage is secured.

## Troubleshooting

- LM Studio unavailable: verify `LM_STUDIO_BASE_URL` and local server status.
- Brave search failures: verify `BRAVE_API_KEY`.
- Empty web extraction: target page may block scraping or require heavy JS.
- Upload errors: verify file extension and `MAX_UPLOAD_MB`.
- Analytics not triggering: ensure `ENABLE_TABULAR_ANALYTICS=true` and query has tabular intent (count/list/filter/grouping).
