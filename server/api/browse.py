import re

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException

from server.config import settings
from server.database import get_db

router = APIRouter(prefix="/browse", tags=["browse"])


def _season_sort_key(name: str) -> int:
    """Extract numeric part from 'Season 3' → 3 for correct ordering."""
    m = re.search(r"\d+", name)
    return int(m.group()) if m else 0


def _story_sort_key(name: str) -> tuple:
    """Sort by leading number e.g. '003 - Daleks' → (3, 'Daleks')."""
    m = re.match(r"(\d+)", name.strip())
    num = int(m.group(1)) if m else 9999
    return (num, name)


@router.get("/seasons")
async def list_seasons(conn: aiosqlite.Connection = Depends(get_db)):
    cursor = await conn.execute(
        """
        SELECT season,
               COUNT(DISTINCT story) AS story_count,
               COUNT(*)              AS episode_count,
               (SELECT id FROM media_items m2
                WHERE  m2.season = m.season AND m2.path_depth = 3 AND m2.is_missing = 0
                ORDER  BY m2.story, m2.file_path LIMIT 1) AS thumb_id
        FROM   media_items m
        WHERE  is_missing = 0 AND season != '' AND path_depth = 3
        GROUP  BY season
        """
    )
    rows = await cursor.fetchall()
    result = [
        {"name": r[0], "story_count": r[1], "episode_count": r[2], "thumb_id": r[3]}
        for r in rows
    ]
    result.sort(key=lambda x: _season_sort_key(x["name"]))
    return result


@router.get("/seasons/{season}")
async def list_stories(season: str, conn: aiosqlite.Connection = Depends(get_db)):
    cursor = await conn.execute(
        """
        SELECT story, COUNT(*) AS episode_count
        FROM   media_items
        WHERE  is_missing = 0 AND season = ? AND story != '' AND path_depth = 3
        GROUP  BY story
        """,
        (season,),
    )
    rows = await cursor.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="Season not found")
    result = [{"name": r[0], "episode_count": r[1]} for r in rows]
    result.sort(key=lambda x: _story_sort_key(x["name"]))
    return result


@router.get("/seasons/{season}/stories/{story}")
async def list_episodes(
    season: str, story: str, conn: aiosqlite.Connection = Depends(get_db)
):
    cursor = await conn.execute(
        """
        SELECT id, title, duration_seconds, video_codec, file_path
        FROM   media_items
        WHERE  is_missing = 0 AND season = ? AND story = ? AND path_depth = 3
        ORDER  BY file_path
        """,
        (season, story),
    )
    rows = await cursor.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="Story not found")
    return [
        {
            "id": r[0],
            "title": r[1],
            "duration_seconds": r[2],
            "video_codec": r[3],
            "stream_url": f"/stream/{r[0]}",
            "thumb_url": f"/thumb/{r[0]}",
        }
        for r in rows
    ]
