import aiosqlite
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from server.config import settings
from server.database import get_db, get_media_by_id
from server.services import bitmap_sub_service

router = APIRouter(prefix="/bsubs", tags=["bsubs"])


def _base():
    return settings.db_path.parent


@router.get("/{media_id}/{track}/status")
async def get_status(
    media_id: int, track: int, conn: aiosqlite.Connection = Depends(get_db)
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")

    if bitmap_sub_service.is_ready(_base(), media_id, track):
        return {"status": "ready"}
    if bitmap_sub_service.is_extracting(media_id, track):
        return {"status": "extracting"}
    return {"status": "not_started"}


@router.post("/{media_id}/{track}/extract")
async def extract(
    media_id: int, track: int, conn: aiosqlite.Connection = Depends(get_db)
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")

    status = await bitmap_sub_service.start_extraction(
        media_id, row["file_path"], track, _base()
    )
    return {"status": status}


@router.get("/{media_id}/{track}/cues.json")
async def get_cues(media_id: int, track: int):
    base = _base()
    if not bitmap_sub_service.is_ready(base, media_id, track):
        raise HTTPException(status_code=404, detail="Bsubs not ready")

    cues_path = bitmap_sub_service.sub_dir(base, media_id, track) / "cues.json"
    return FileResponse(cues_path, media_type="application/json")


@router.get("/{media_id}/{track}/{frame}")
async def get_frame(media_id: int, track: int, frame: str):
    if not frame.endswith(".png"):
        raise HTTPException(status_code=404, detail="Not found")

    base = _base()
    path = bitmap_sub_service.sub_dir(base, media_id, track) / frame
    if not path.exists():
        raise HTTPException(status_code=404, detail="Frame not found")

    return FileResponse(path, media_type="image/png")
