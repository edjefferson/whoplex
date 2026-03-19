import aiosqlite
from fastapi import APIRouter, Depends, HTTPException

from server.config import settings
from server.database import get_db, get_media_by_id
from server.models import CastDevice, CastPlayRequest, CastPlayResponse
from server.services import chromecast_service

router = APIRouter(prefix="/cast", tags=["cast"])

MIME_MAP = {
    ".mp4": "video/mp4",
    ".m4v": "video/mp4",
    ".mkv": "video/x-matroska",
    ".avi": "video/x-msvideo",
    ".mov": "video/quicktime",
}


@router.get("/devices", response_model=list[CastDevice])
async def list_devices():
    devices = await chromecast_service.discover_devices(timeout=5.0)
    return [CastDevice(**d) for d in devices]


@router.post("/play", response_model=CastPlayResponse)
async def cast_play(
    body: CastPlayRequest, conn: aiosqlite.Connection = Depends(get_db)
):
    row = await get_media_by_id(conn, body.media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")

    import os
    ext = os.path.splitext(row["file_path"])[1].lower()
    content_type = MIME_MAP.get(ext, "video/mp4")

    stream_url = f"{settings.base_url}/stream/{body.media_id}"
    thumb_url = f"{settings.base_url}/thumb/{body.media_id}"

    try:
        await chromecast_service.cast_url(
            device_uuid=body.device_uuid,
            url=stream_url,
            content_type=content_type,
            title=row["title"],
            thumb_url=thumb_url,
            current_time=body.current_time,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Cast failed: {exc}")

    return CastPlayResponse(success=True, message=f"Casting '{row['title']}'")
