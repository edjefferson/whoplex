import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, PlainTextResponse

from server.config import settings
from server.database import get_db, get_media_by_id
from server.services import hls_service

router = APIRouter(prefix="/hls", tags=["hls"])


def _base():
    return settings.db_path.parent


# ── Split-stream endpoints (specific paths first to avoid /{track} capture) ───

@router.get("/{media_id}/status")
async def get_split_status(
    media_id: int,
    tracks: str = Query(""),
    conn: aiosqlite.Connection = Depends(get_db),
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")
    track_indices = [int(t) for t in tracks.split(",") if t.strip().isdigit()]
    return hls_service.get_status(_base(), media_id, track_indices)


@router.get("/{media_id}/master.m3u8")
async def get_master_playlist(
    media_id: int,
    tracks: str = Query(""),
    conn: aiosqlite.Connection = Depends(get_db),
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")
    track_indices = [int(t) for t in tracks.split(",") if t.strip().isdigit()]
    status = hls_service.get_status(_base(), media_id, track_indices)
    if not status["master_ready"]:
        raise HTTPException(status_code=503, detail="HLS not ready yet")
    audio_tracks = [{"index": i, "label": f"Track {i}", "language": ""} for i in track_indices]
    bit_rate = row.get("bit_rate", 0) or 0
    content = hls_service.build_master_playlist(_base(), media_id, audio_tracks, bit_rate)
    return PlainTextResponse(content, media_type="application/vnd.apple.mpegurl")


@router.get("/{media_id}/video/playlist.m3u8")
async def get_video_playlist(media_id: int):
    path = hls_service.video_hls_dir(_base(), media_id) / "playlist.m3u8"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Video HLS not ready")
    return FileResponse(path, media_type="application/vnd.apple.mpegurl")


@router.get("/{media_id}/video/{segment}")
async def get_video_segment(media_id: int, segment: str):
    if not segment.endswith(".ts"):
        raise HTTPException(status_code=404, detail="Not found")
    path = hls_service.video_hls_dir(_base(), media_id) / segment
    if not path.exists():
        raise HTTPException(status_code=404, detail="Segment not found")
    return FileResponse(path, media_type="video/mp2t")


@router.get("/{media_id}/audio/{track}/playlist.m3u8")
async def get_audio_playlist(media_id: int, track: int):
    path = hls_service.audio_hls_dir(_base(), media_id, track) / "playlist.m3u8"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Audio HLS not ready")
    return FileResponse(path, media_type="application/vnd.apple.mpegurl")


@router.get("/{media_id}/audio/{track}/{segment}")
async def get_audio_segment(media_id: int, track: int, segment: str):
    if not segment.endswith(".ts"):
        raise HTTPException(status_code=404, detail="Not found")
    path = hls_service.audio_hls_dir(_base(), media_id, track) / segment
    if not path.exists():
        raise HTTPException(status_code=404, detail="Segment not found")
    return FileResponse(path, media_type="video/mp2t")


# ── Legacy combined-stream endpoints ──────────────────────────────────────────

@router.get("/{media_id}/{track}/status")
async def get_status(
    media_id: int, track: int, conn: aiosqlite.Connection = Depends(get_db)
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")

    if hls_service.is_ready(_base(), media_id, track):
        return {"status": "ready"}
    if hls_service.is_generating(media_id, track):
        return {"status": "generating"}
    return {"status": "not_started"}


@router.post("/{media_id}/{track}/generate")
async def generate(
    media_id: int, track: int, conn: aiosqlite.Connection = Depends(get_db)
):
    row = await get_media_by_id(conn, media_id)
    if not row:
        raise HTTPException(status_code=404, detail="Media not found")

    status = await hls_service.start_generation(
        media_id, row["file_path"], track, _base()
    )
    return {"status": status}


@router.get("/{media_id}/{track}/playlist.m3u8")
async def get_playlist(
    media_id: int, track: int, conn: aiosqlite.Connection = Depends(get_db)
):
    if not hls_service.is_ready(_base(), media_id, track):
        raise HTTPException(status_code=404, detail="HLS not ready")

    path = hls_service.hls_dir(_base(), media_id, track) / "playlist.m3u8"
    return FileResponse(path, media_type="application/vnd.apple.mpegurl")


@router.get("/{media_id}/{track}/{segment}")
async def get_segment(media_id: int, track: int, segment: str):
    if not segment.endswith(".ts"):
        raise HTTPException(status_code=404, detail="Not found")

    path = hls_service.hls_dir(_base(), media_id, track) / segment
    if not path.exists():
        raise HTTPException(status_code=404, detail="Segment not found")

    return FileResponse(path, media_type="video/mp2t")
