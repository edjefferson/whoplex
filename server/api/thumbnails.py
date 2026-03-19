import aiosqlite
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from server.config import settings
from server.database import get_db, get_media_by_id, set_thumb_ready
from server.services import ffmpeg_service

router = APIRouter(prefix="/thumb", tags=["thumbnails"])


@router.get("/{media_id}")
async def get_thumbnail(
    media_id: int, conn: aiosqlite.Connection = Depends(get_db)
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")

    thumb_path = settings.thumb_dir / f"{media_id}.jpg"

    if not thumb_path.exists():
        try:
            await ffmpeg_service.generate_thumbnail(
                file_path=row["file_path"],
                output_path=thumb_path,
                seek_seconds=settings.thumb_seek_seconds,
            )
            await set_thumb_ready(conn, media_id)
        except Exception as exc:
            raise HTTPException(
                status_code=500, detail=f"Thumbnail generation failed: {exc}"
            )

    return FileResponse(thumb_path, media_type="image/jpeg")
