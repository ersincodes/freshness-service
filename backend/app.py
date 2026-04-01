"""
FastAPI application for the Freshness Service.

Routes live under `api.routers`; business logic remains in services and domain packages.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.routers import archive, chat, documents, freshness, health, legacy, settings as settings_router
from .config import settings
from .lifecycle import on_startup

app = FastAPI(title="Freshness Service", version="1.0.0", docs_url="/docs", redoc_url="/redoc", openapi_url="/openapi.json")
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(settings.cors_origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chat.router)
app.include_router(archive.router)
app.include_router(documents.router)
app.include_router(settings_router.router)
app.include_router(health.router)
app.include_router(freshness.router)
app.include_router(legacy.router)


@app.on_event("startup")
async def startup() -> None:
    await on_startup()
