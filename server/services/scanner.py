import os
import re
from pathlib import Path
from typing import Optional

import aiosqlite

from server.config import Settings
from server.services import ffmpeg_service


def _derive_title(path: Path) -> str:
    """Strip year/quality tokens from filename to form a human-readable title."""
    stem = path.stem
    # Remove common quality/release tokens
    stem = re.sub(
        r"[\.\-_](720p|1080p|2160p|4k|bluray|bdrip|webrip|web-dl|dvdrip|hdtv|x264|x265|hevc|avc|aac|ac3|dts|"
        r"xvid|divx|h264|h265|10bit|hdr|sdr|remux|proper|repack|extended|theatrical|"
        r"directors\.cut|unrated|dubbed|subbed|\d{4}).*$",
        "",
        stem,
        flags=re.IGNORECASE,
    )
    # Replace dots/underscores/dashes with spaces
    stem = re.sub(r"[\.\-_]+", " ", stem).strip()
    return stem if stem else path.stem


async def probe_file(file_path: str) -> Optional[dict]:
    """Probe a media file and return extracted metadata, or None on failure."""
    try:
        info = await ffmpeg_service.probe(file_path)
    except Exception:
        return None

    video_stream = next(
        (s for s in info.get("streams", []) if s.get("codec_type") == "video"), {}
    )
    audio_stream = next(
        (s for s in info.get("streams", []) if s.get("codec_type") == "audio"), {}
    )
    fmt = info.get("format", {})

    duration = float(fmt.get("duration") or video_stream.get("duration") or 0)
    bit_rate = int(fmt.get("bit_rate") or 0)

    return {
        "duration_seconds": duration,
        "video_codec": video_stream.get("codec_name", ""),
        "audio_codec": audio_stream.get("codec_name", ""),
        "width": int(video_stream.get("width") or 0),
        "height": int(video_stream.get("height") or 0),
        "bit_rate": bit_rate,
    }


def _path_info(file_path: str, media_dir: str) -> dict:
    """Extract season, story, and depth from a file path relative to its media_dir."""
    parts = Path(os.path.relpath(file_path, media_dir)).parts
    depth = len(parts)
    return {
        "season": parts[0] if depth > 1 else "",
        "story":  parts[1] if depth > 2 else "",
        "path_depth": depth,
    }


async def scan_all(settings: Settings, conn: aiosqlite.Connection) -> dict:
    """Walk media dirs, upsert new/changed files, mark missing rows."""
    extensions = settings.media_extensions_set
    dirs = settings.media_dirs_list

    print(f"[scan] media_dirs={dirs}")
    print(f"[scan] extensions={extensions}")

    # Collect current DB rows for change detection (include path_depth for backfill)
    cursor = await conn.execute(
        "SELECT file_path, file_mtime, path_depth FROM media_items WHERE is_missing = 0"
    )
    existing = {row[0]: (row[1], row[2]) for row in await cursor.fetchall()}

    scanned = added = skipped = 0
    found_paths: list[str] = []
    batch: list[dict] = []

    for media_dir in dirs:
        exists = os.path.isdir(media_dir)
        print(f"[scan] checking dir '{media_dir}' → exists={exists}")
        if not exists:
            continue
        for root, _dirs, files in os.walk(media_dir):
            for filename in files:
                ext = Path(filename).suffix.lower()
                if ext not in extensions:
                    if ext:
                        print(f"[scan] skipping '{filename}' (ext={ext})")
                    continue
                file_path = os.path.join(root, filename)
                scanned += 1
                found_paths.append(file_path)

                try:
                    stat = os.stat(file_path)
                except OSError:
                    continue

                mtime = stat.st_mtime
                if file_path in existing:
                    existing_mtime, existing_depth = existing[file_path]
                    if abs(existing_mtime - mtime) < 0.01:
                        # Backfill path info for rows added before browse columns existed
                        if existing_depth == 0:
                            info = _path_info(file_path, media_dir)
                            await conn.execute(
                                "UPDATE media_items SET season=:season, story=:story, "
                                "path_depth=:path_depth WHERE file_path=:file_path",
                                {**info, "file_path": file_path},
                            )
                            await conn.commit()
                        skipped += 1
                        continue

                metadata = await probe_file(file_path)
                if metadata is None:
                    continue

                item = {
                    "file_path": file_path,
                    "file_size_bytes": stat.st_size,
                    "file_mtime": mtime,
                    "title": _derive_title(Path(filename)),
                    **metadata,
                    **_path_info(file_path, media_dir),
                }
                await conn.execute(
                    """
                    INSERT INTO media_items
                        (file_path, file_size_bytes, file_mtime, title, duration_seconds,
                         video_codec, audio_codec, width, height, bit_rate,
                         season, story, path_depth, is_missing, updated_at)
                    VALUES
                        (:file_path, :file_size_bytes, :file_mtime, :title, :duration_seconds,
                         :video_codec, :audio_codec, :width, :height, :bit_rate,
                         :season, :story, :path_depth, 0, datetime('now'))
                    ON CONFLICT(file_path) DO UPDATE SET
                        file_size_bytes  = excluded.file_size_bytes,
                        file_mtime       = excluded.file_mtime,
                        title            = excluded.title,
                        duration_seconds = excluded.duration_seconds,
                        video_codec      = excluded.video_codec,
                        audio_codec      = excluded.audio_codec,
                        width            = excluded.width,
                        height           = excluded.height,
                        bit_rate         = excluded.bit_rate,
                        season           = excluded.season,
                        story            = excluded.story,
                        path_depth       = excluded.path_depth,
                        is_missing       = 0,
                        updated_at       = datetime('now')
                    """,
                    item,
                )
                await conn.commit()
                added += 1
                print(f"[scan] added ({added}): {item['title']}")

    # Mark rows whose files were not found
    missing = 0
    if found_paths:
        placeholders = ",".join("?" * len(found_paths))
        cursor = await conn.execute(
            f"UPDATE media_items SET is_missing = 1, updated_at = datetime('now') "
            f"WHERE file_path NOT IN ({placeholders}) AND is_missing = 0",
            found_paths,
        )
        missing = cursor.rowcount
        await conn.commit()
    else:
        # No files found at all — mark everything missing
        cursor = await conn.execute(
            "UPDATE media_items SET is_missing = 1, updated_at = datetime('now') "
            "WHERE is_missing = 0"
        )
        missing = cursor.rowcount
        await conn.commit()

    return {"scanned": scanned, "added": added, "skipped": skipped, "missing": missing}
