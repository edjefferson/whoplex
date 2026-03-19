import mimetypes
import os
from typing import AsyncGenerator, Optional

import aiofiles
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from server.database import get_db, get_media_by_id
from server.services import ffmpeg_service

router = APIRouter(prefix="/stream", tags=["stream"])

MIME_MAP = {
    ".mp4": "video/mp4",
    ".m4v": "video/mp4",
    ".mkv": "video/x-matroska",
    ".avi": "video/x-msvideo",
    ".mov": "video/quicktime",
}

CHUNK_SIZE = 1024 * 1024  # 1 MB


async def _file_chunks(
    file_path: str, start: int, end: int
) -> AsyncGenerator[bytes, None]:
    async with aiofiles.open(file_path, "rb") as f:
        await f.seek(start)
        remaining = end - start + 1
        while remaining > 0:
            chunk = await f.read(min(CHUNK_SIZE, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk


@router.get("/{media_id}")
async def stream_video(
    media_id: int,
    request: Request,
    audio: Optional[int] = Query(None, description="Audio track index for remux"),
    start: float = Query(0.0, description="Start offset in seconds (remux only)"),
    conn=Depends(get_db),
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")

    file_path: str = row["file_path"]
    if not os.path.isfile(file_path):
        raise HTTPException(status_code=404, detail="File not found on disk")

    ext = os.path.splitext(file_path)[1].lower()
    content_type = MIME_MAP.get(ext) or mimetypes.guess_type(file_path)[0] or "application/octet-stream"

    # ── Remux path: ffmpeg pipes selected audio track ──────────────────────
    if audio is not None:
        headers = {
            "Content-Type": "video/x-matroska",
            "Accept-Ranges": "none",
            "Cache-Control": "no-cache",
        }
        return StreamingResponse(
            ffmpeg_service.remux_audio_stream(file_path, audio, start_seconds=start),
            status_code=200,
            headers=headers,
            media_type="video/x-matroska",
        )

    # ── Normal path: direct file with range requests ────────────────────────
    file_size = os.path.getsize(file_path)

    range_header = request.headers.get("range")
    if range_header:
        try:
            range_val = range_header.strip().replace("bytes=", "")
            range_start, range_end = range_val.split("-")
            byte_start = int(range_start)
            byte_end = int(range_end) if range_end else file_size - 1
        except (ValueError, AttributeError):
            raise HTTPException(status_code=416, detail="Invalid Range header")
    else:
        byte_start = 0
        byte_end = file_size - 1

    if byte_start > byte_end or byte_start >= file_size or byte_end >= file_size:
        raise HTTPException(
            status_code=416,
            detail="Range Not Satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    content_length = byte_end - byte_start + 1
    headers = {
        "Content-Range": f"bytes {byte_start}-{byte_end}/{file_size}",
        "Content-Length": str(content_length),
        "Accept-Ranges": "bytes",
        "Content-Type": content_type,
    }

    return StreamingResponse(
        _file_chunks(file_path, byte_start, byte_end),
        status_code=206,
        headers=headers,
        media_type=content_type,
    )
