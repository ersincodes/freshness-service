# Freshness Service (LM Studio + Web Search + Auto-Archive + Document Chat)

A small Python orchestrator that gives a **local LM Studio model** (e.g., `rnj-1`) access to **fresh web information** at query time, while building an **offline "brain"** by archiving fetched pages. 

This repo now supports:

1. **Two offline retrieval modes**:
   - **Keyword (SQLite-only)** — zero extra components.
   - **Semantic (Vector search via ChromaDB)** — better recall when wording differs (e.g., "feline" → "cat").

2. **Document Upload & Chat** (NEW):
   - Upload **PDF**, **XLSX**, and **XLS** files
   - Chat with your documents alongside web sources
   - Precise citations with page numbers (PDF) or sheet/row ranges (Excel)

## What this is (and is not)

- ✅ **Freshness via retrieval**: The model answers with "today" info by reading fetched sources.
- ✅ **Offline resilience**: If internet goes down later, you can still retrieve previously archived pages.
- ✅ **Document chat**: Upload and query your own PDF/Excel files locally.
- ❌ **Not weight updates**: This does not retrain or permanently update the model's weights.

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
- `python-multipart` (file uploads)

Document processing:
- `pypdf` (PDF extraction)
- `openpyxl` (XLSX extraction)
- `xlrd` (XLS extraction)

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

Install all deps:

```bash
pip install -r requirements.txt
playwright install
```

Or install manually:

```bash
# Base deps
pip install requests beautifulsoup4 python-dotenv fastapi uvicorn playwright python-multipart
playwright install

# Document processing
pip install pypdf openpyxl xlrd

# Semantic search (optional)
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
  documents.py     # Document upload, extraction, and chunking (PDF/Excel)

frontend/          # React + TypeScript chat application
  src/
    components/    # UI components (chat, archive, documents, settings)
    lib/           # API client, types, utilities
    store/         # Chat state management
  README.md        # Frontend-specific documentation
```

Runtime data:
- `knowledge.db` (SQLite) — created automatically on first run.
- `chroma_db/` (ChromaDB persistent store) — created automatically when enabled.
- `uploads/` — uploaded document files (PDF/Excel).

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

Document upload settings:
- `UPLOAD_DIR`: directory for uploaded files (default `"uploads"`)
- `MAX_UPLOAD_MB`: maximum file size in MB (default `25`)

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

### Document Chat (NEW)
1. Upload PDF/Excel via `POST /api/documents/upload`
2. Documents are processed asynchronously:
   - **PDF**: Text extracted per page, chunked to ~2000 chars
   - **Excel**: Data extracted per sheet, normalized to text with row metadata
3. Chunks stored in SQLite (`document_chunks` table) and optionally ChromaDB
4. Chat with `include_documents=true` to search document chunks alongside web sources
5. Citations include page numbers (PDF) or sheet/row ranges (Excel)

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

### `documents` (NEW)
- `document_id` (PRIMARY KEY)
- `filename` (sanitized filename)
- `doc_type` (pdf, xlsx, xls)
- `size_bytes` (file size)
- `status` (pending, processing, ready, error)
- `uploaded_at` (timestamp)
- `error_message` (optional)

### `document_chunks` (NEW)
- `chunk_id` (PRIMARY KEY)
- `document_id` (FK to documents)
- `chunk_index` (order within document)
- `content` (extracted text)
- `meta_json` (page/sheet/row metadata)
- `timestamp`

Deduplication:
- Page content stored with `INSERT OR IGNORE` by `url_hash`.

## ChromaDB (Vector Store)

- Persistent directory: `CHROMA_DIR` (default `chroma_db/`)
- Collections:
  - `pages` — archived web pages
  - `document_chunks` — uploaded document chunks (NEW)
- Each item is stored as a document with metadata (URL/filename, timestamp, location info).

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

### Chat API Endpoints

The following endpoints are available for the chat frontend:

- `POST /api/chat` - Send a chat message and receive a structured response with sources
- `POST /api/chat/stream` - Stream chat responses via Server-Sent Events (SSE)
- `GET /api/archive/search?q=...` - Search archived pages
- `GET /api/archive/page/{url_hash}` - Get archived page details
- `GET /api/settings` - Get current configuration (non-secret values)
- `GET /api/health` - Check health of backend, LM Studio, and Brave Search

### Document API Endpoints (NEW)

- `POST /api/documents/upload` - Upload a PDF/Excel file (multipart/form-data)
- `GET /api/documents` - List all uploaded documents
- `GET /api/documents/{id}` - Get document status and details
- `DELETE /api/documents/{id}` - Delete a document and its chunks

### Chat Request Options (Extended)

The chat endpoints now support additional options:

```json
{
  "query": "What does the Q3 report say about revenue?",
  "include_web": true,
  "include_documents": true,
  "document_ids": ["optional-specific-doc-id"]
}
```

- `include_web` (default: true) - Include web sources in retrieval
- `include_documents` (default: false) - Include uploaded documents in retrieval
- `document_ids` (optional) - Limit document search to specific documents

## Running the Frontend

```bash
cd frontend
npm install
npm run dev
```

The frontend will be available at `http://localhost:5173`.

Make sure the backend is running on `http://localhost:8000` (or configure `VITE_API_BASE_URL` in frontend `.env`).

### Frontend Features

- **Chat View**: Send messages with mode selection (Online/Offline)
- **Documents View** (NEW): Upload and manage PDF/Excel files
- **Archive View**: Browse and search archived web pages
- **Settings View**: Configure backend settings

The chat interface includes toggles for:
- **Web**: Include web sources in retrieval
- **Documents**: Include uploaded documents in retrieval

## Operational Notes / Limitations

- **Extraction quality**: BeautifulSoup + basic tag removal is "good enough" but not perfect.
- **JS-heavy sites**: May return little content without a headless browser.
- **Prompt injection risk**: Web pages and documents can contain adversarial instructions.
  - Treat retrieved text as untrusted. The system prompt instructs the model to ignore instructions inside sources.
- **Citations**: Prompt requests citations, but the model may fail unless enforced.
- **Document size**: Large PDFs/Excel files may take time to process.

## Troubleshooting

- LM Studio errors: ensure server is enabled and `LM_STUDIO_BASE_URL` is correct.
- Brave API errors: verify key and plan limits.
- Empty page content: site might block scrapers or be JS-rendered.
- Document upload fails: check file size (max 25MB) and format (.pdf, .xlsx, .xls).
- Document stuck in "processing": check backend logs for extraction errors.

## Security / Privacy

- Archive is stored locally (`knowledge.db` and optionally `chroma_db/`).
- Uploaded documents are stored in `uploads/` directory.
- Avoid archiving sensitive pages or uploading confidential documents unless your disk is secured.
- Document content is treated as untrusted input to prevent prompt injection.
