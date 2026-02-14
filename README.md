# Freshness Service (LM Studio + Web Search + Auto-Archive)

A small Python orchestrator that gives a **local LM Studio model** (e.g., `rnj-1`) access to **fresh web information** at query time, while building an **offline “brain”** by archiving fetched pages. 

This repo now supports **two offline retrieval modes**:

1. **Keyword (SQLite-only)** — zero extra components.
2. **Semantic (Vector search via ChromaDB)** — better recall when wording differs (e.g., “feline” → “cat”).

## What this is (and is not)

- ✅ **Freshness via retrieval**: The model answers with “today” info by reading fetched sources.
- ✅ **Offline resilience**: If internet goes down later, you can still retrieve previously archived pages.
- ❌ **Not weight updates**: This does not retrain or permanently update the model’s weights.

## Requirements

- Python 3.10+ recommended
- LM Studio running with **Local Server** enabled (OpenAI-compatible endpoint)
- A web search API key (currently **Brave Search API**)

Python packages:
- `requests`
- `beautifulsoup4`
- `python-dotenv` (loads `.env` for local development)
- `fastapi`
- `uvicorn`
- `playwright` (JS-rendered pages)

Optional (for semantic offline retrieval):
- `chromadb`
- an embedding model (default via `sentence-transformers`)

### Virtual environment (recommended)

A `venv` is not strictly required, but it is strongly recommended to avoid
dependency conflicts and keep this project isolated.

```bash
# Windows (PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

# macOS/Linux
python -m venv .venv
source .venv/bin/activate
```

Install base deps:

```bash
pip install requests beautifulsoup4 python-dotenv fastapi uvicorn playwright
playwright install
```

Install semantic deps:

```bash
pip install chromadb sentence-transformers
```

## Project Layout

```
backend/
  __init__.py      # Package entry
  __main__.py      # python -m backend
  config.py        # Environment-driven settings
  archive.py       # SQLite storage + offline retrieval (keyword)
  vector_store.py  # ChromaDB store + offline retrieval (semantic)
  scraper.py       # URL fetch + HTML -> clean text extraction
  main.py          # Orchestrator CLI (online retrieval + offline fallback + LLM call)
  app.py           # FastAPI application with REST API endpoints

frontend/          # React + TypeScript chat application
  src/
    components/    # UI components (chat, archive, settings)
    lib/           # API client, types, utilities
    store/         # Chat state management
  README.md        # Frontend-specific documentation
```

Runtime data:
- `knowledge.db` (SQLite) — created automatically on first run.
- `chroma_db/` (ChromaDB persistent store) — created automatically when enabled.

## Configuration

All configuration is read from environment variables:

- `BRAVE_API_KEY`: Brave Search API token
- `LM_STUDIO_BASE_URL`: default `http://localhost:1111/v1`
- `MODEL_NAME`: model identifier LM Studio expects (example: `rnj-1`)
- `DB_PATH`: default `knowledge.db`
- `MAX_SEARCH_RESULTS`: how many URLs to fetch per query (default `3`)
- `REQUEST_TIMEOUT_S`: HTTP timeout in seconds (default `10`)
- `MAX_CHARS_PER_SOURCE`: truncate per source for prompt size (default `2000`)

Vector / semantic retrieval settings:
- `OFFLINE_RETRIEVAL_MODE`: `"keyword"` or `"semantic"` (default `"keyword"`)
- `CHROMA_DIR`: persistent path for ChromaDB (default `"chroma_db"`)
- `EMBED_MODEL_NAME`: default `"sentence-transformers/all-MiniLM-L6-v2"`
- `SEMANTIC_TOP_K`: number of offline hits to inject (default `3`)

### Recommended: use a `.env` file (Option A)

```bash
# Copy template and edit values
cp .env.example .env

# Windows (PowerShell)
Copy-Item .env.example .env
```

The app auto-loads `.env` if `python-dotenv` is installed.  
Do not commit `.env` (see `.gitignore`).

### Option B: set environment variables directly

```bash
# macOS/Linux
export BRAVE_API_KEY="..."

# Windows (PowerShell)
$env:BRAVE_API_KEY="..."
```

## How it Works

### Online path
1. `main.py:get_online_context(query)` calls Brave Search:
   - Endpoint: `https://api.search.brave.com/res/v1/web/search`
2. For each returned URL (up to `MAX_SEARCH_RESULTS`):
   - `scraper.py:get_clean_text(url)` downloads HTML and extracts visible text.
   - Archive:
     - Always stores in SQLite via `archive.save_to_archive(...)`.
     - If semantic mode enabled, also upserts into Chroma via `vector_store.upsert(...)`.
3. Build context blocks:
   - `SOURCE: <url>`
   - `CONTENT: <first N chars>` (currently truncated to limit prompt size)
4. Send messages to LM Studio `/chat/completions`.

### Offline fallback
If online search fails:
- **Keyword mode**: `archive.search_offline(...)` (SQLite `LIKE` matching)
- **Semantic mode**: `vector_store.query_similar(...)` (Chroma cosine similarity over embeddings)

## SQLite Schema

Created in `archive.init_db(db_path)`:

### `pages`
- `url_hash` (PRIMARY KEY)
- `url` (original URL)
- `content` (clean extracted text)
- `timestamp` (when it was first stored)

### `search_history`
- `query` (lowercased)
- `url_hash` (reference to `pages.url_hash`)
- `timestamp` (when this query->page link was recorded)

Deduplication:
- Page content stored with `INSERT OR IGNORE` by `url_hash`.

## ChromaDB (Vector Store)

- Persistent directory: `CHROMA_DIR` (default `chroma_db/`)
- Collection: `pages`
- Each page is stored as a document with metadata (URL, timestamp, url_hash).

## Running

```bash
python -m backend
```

Exit with `exit` or `quit`.

## Running as an API

```bash
uvicorn backend.app:app --host 0.0.0.0 --port 8000
```

Example requests:

```bash
curl "http://localhost:8000/"
curl "http://localhost:8000/freshness?query=What%20is%20new%20in%20Python%203.13"
```

API docs:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

### New Chat API Endpoints

The following endpoints are available for the chat frontend:

- `POST /api/chat` - Send a chat message and receive a structured response with sources
- `POST /api/chat/stream` - Stream chat responses via Server-Sent Events (SSE)
- `GET /api/archive/search?q=...` - Search archived pages
- `GET /api/archive/page/{url_hash}` - Get archived page details
- `GET /api/settings` - Get current configuration (non-secret values)
- `GET /api/health` - Check health of backend, LM Studio, and Brave Search

## Running the Frontend

```bash
cd frontend
npm install
npm run dev
```

The frontend will be available at `http://localhost:5173`.

Make sure the backend is running on `http://localhost:8000` (or configure `VITE_API_BASE_URL` in frontend `.env`).

## Operational Notes / Limitations

- **Extraction quality**: BeautifulSoup + basic tag removal is “good enough” but not perfect.
- **JS-heavy sites**: May return little content without a headless browser.
- **Prompt injection risk**: Web pages can contain adversarial instructions.
  - Treat retrieved text as untrusted. The system prompt should instruct the model to ignore instructions inside sources.
- **Citations**: Prompt requests citations, but the model may fail unless enforced.

## Roadmap (optional improvements)

- Convert CLI to **FastAPI**.
- Add caching + TTL.
- Add reranking (cross-encoder).
- Switch extraction to readability / trafilatura.
- Add better citation enforcement.

## Troubleshooting

- LM Studio errors: ensure server is enabled and `LM_STUDIO_BASE_URL` is correct.
- Brave API errors: verify key and plan limits.
- Empty page content: site might block scrapers or be JS-rendered.

## Security / Privacy

- Archive is stored locally (`knowledge.db` and optionally `chroma_db/`).
- Avoid archiving sensitive pages unless your disk is secured.
