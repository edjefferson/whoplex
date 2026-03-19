from typing import Optional

import aiosqlite
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from server.config import settings
from server.database import get_db, get_all_media, get_media_by_id
from server.models import LibraryResponse, MediaItem
from server.services import scanner

router = APIRouter(prefix="/library", tags=["library"])


def _build_item(row: dict) -> MediaItem:
    return MediaItem(**{**row, "base_url": settings.base_url})


@router.get("", response_model=LibraryResponse)
async def list_library(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    q: Optional[str] = Query(None),
    conn: aiosqlite.Connection = Depends(get_db),
):
    rows = await get_all_media(conn, page=page, page_size=page_size, query=q)
    # Total count (separate query)
    if q:
        cursor = await conn.execute(
            "SELECT COUNT(*) FROM media_items WHERE is_missing = 0 AND title LIKE ?",
            (f"%{q}%",),
        )
    else:
        cursor = await conn.execute(
            "SELECT COUNT(*) FROM media_items WHERE is_missing = 0"
        )
    total = (await cursor.fetchone())[0]
    return LibraryResponse(
        items=[_build_item(r) for r in rows],
        page=page,
        page_size=page_size,
        total=total,
    )


@router.get("/{media_id}", response_model=MediaItem)
async def get_media(
    media_id: int, conn: aiosqlite.Connection = Depends(get_db)
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")
    return _build_item(row)


async def _run_scan():
    from server.database import get_db as _get_db
    async for conn in _get_db():
        result = await scanner.scan_all(settings, conn)
        print(f"[scan] {result}")


@router.post("/scan", status_code=202)
async def trigger_scan(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_scan)
    return {"message": "Scan started"}
