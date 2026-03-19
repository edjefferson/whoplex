import asyncio

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from server.config import settings
from server.database import get_db, get_media_by_id
from server.services import bitmap_sub_service, ffmpeg_service, hls_service

router = APIRouter(tags=["tracks"])

# Subtitle codecs that cannot be converted to WebVTT (bitmap-based)
_IMAGE_SUBS = {"hdmv_pgs_subtitle", "dvd_subtitle", "dvdsub", "pgssub"}


@router.get("/tracks/{media_id}")
async def get_tracks(media_id: int, conn: aiosqlite.Connection = Depends(get_db)):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")

    try:
        info = await ffmpeg_service.probe(row["file_path"])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Probe failed: {exc}")

    audio = []
    subtitles = []
    audio_idx = sub_idx = 0

    for stream in info.get("streams", []):
        tags = stream.get("tags", {})
        lang = tags.get("language", "")
        title = tags.get("title", "")
        codec = stream.get("codec_name", "")
        kind = stream.get("codec_type", "")

        if kind == "audio":
            label = title or lang or f"Track {audio_idx + 1}"
            audio.append({
                "index": audio_idx,
                "codec": codec,
                "language": lang,
                "label": label,
                "channels": stream.get("channels", 0),
            })
            audio_idx += 1

        elif kind == "subtitle":
            label = title or lang or f"Sub {sub_idx + 1}"
            subtitles.append({
                "index": sub_idx,
                "codec": codec,
                "language": lang,
                "label": label,
                "type": "bitmap" if codec in _IMAGE_SUBS else "text",
            })
            sub_idx += 1

    base = settings.db_path.parent
    for sub in subtitles:
        if sub["type"] == "bitmap":
            asyncio.create_task(
                bitmap_sub_service.start_extraction(
                    media_id, row["file_path"], sub["index"], base, sub["codec"]
                )
            )

    hls_status_url = None
    if len(audio) > 1:
        asyncio.create_task(
            hls_service.start_generation_all(
                media_id, row["file_path"], audio, base
            )
        )
        track_indices = ",".join(str(a["index"]) for a in audio)
        hls_status_url = f"/hls/{media_id}/status?tracks={track_indices}"

    return {"audio": audio, "subtitles": subtitles, "hls_status_url": hls_status_url}


@router.get("/subtitles/{media_id}/{track_index}")
async def get_subtitle(
    media_id: int, track_index: int, conn: aiosqlite.Connection = Depends(get_db)
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")

    sub_dir = settings.db_path.parent / "subtitles"
    vtt_path = sub_dir / f"{media_id}_{track_index}.vtt"

    if not vtt_path.exists():
        try:
            await ffmpeg_service.extract_subtitle_webvtt(
                row["file_path"], track_index, vtt_path
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Subtitle extraction failed: {exc}")

    return FileResponse(vtt_path, media_type="text/vtt")
