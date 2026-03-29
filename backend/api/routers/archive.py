"""Archive search HTTP endpoints."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from ..deps import archive_repo
from ..schemas import ArchiveEntryModel, ArchivePageResponse, ArchiveSearchResponse
from ...domain import ErrorCode

router = APIRouter(tags=["archive"])


@router.get("/api/archive/search", response_model=ArchiveSearchResponse)
async def archive_search(
    q: str = Query(""),
    limit: int = Query(20, ge=1, le=100),
    cursor: str | None = Query(None),
) -> ArchiveSearchResponse:
    r = await archive_repo().search_pages_async(q, limit, cursor)
    return ArchiveSearchResponse(
        entries=[
            ArchiveEntryModel(url_hash=e.url_hash, url=e.url, excerpt=e.excerpt, timestamp=e.timestamp)
            for e in r.entries
        ],
        total=r.total,
        cursor=r.cursor,
    )


@router.get("/api/archive/page/{url_hash}", response_model=ArchivePageResponse)
async def archive_page(url_hash: str) -> ArchivePageResponse:
    p = await archive_repo().get_page_async(url_hash)
    if not p:
        raise HTTPException(404, {"code": ErrorCode.NOT_FOUND, "message": f"Archive page not found: {url_hash}"})
    return ArchivePageResponse(url_hash=p.url_hash, url=p.url, content=p.content, timestamp=p.timestamp)
