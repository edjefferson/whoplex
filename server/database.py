from pathlib import Path
from typing import AsyncGenerator, Optional
import aiosqlite

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS media_items (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path        TEXT    NOT NULL UNIQUE,
    file_size_bytes  INTEGER NOT NULL,
    file_mtime       REAL    NOT NULL,
    title            TEXT    NOT NULL,
    duration_seconds REAL    NOT NULL DEFAULT 0.0,
    video_codec      TEXT    NOT NULL DEFAULT '',
    audio_codec      TEXT    NOT NULL DEFAULT '',
    width            INTEGER NOT NULL DEFAULT 0,
    height           INTEGER NOT NULL DEFAULT 0,
    bit_rate         INTEGER NOT NULL DEFAULT 0,
    is_missing       INTEGER NOT NULL DEFAULT 0,
    thumb_ready      INTEGER NOT NULL DEFAULT 0,
    season           TEXT    NOT NULL DEFAULT '',
    story            TEXT    NOT NULL DEFAULT '',
    path_depth       INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT    NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_media_title ON media_items (title COLLATE NOCASE);
"""

# Applied to existing DBs; safe to re-run (errors ignored)
_MIGRATIONS = [
    "ALTER TABLE media_items ADD COLUMN season     TEXT    NOT NULL DEFAULT ''",
    "ALTER TABLE media_items ADD COLUMN story      TEXT    NOT NULL DEFAULT ''",
    "ALTER TABLE media_items ADD COLUMN path_depth INTEGER NOT NULL DEFAULT 0",
    # Index created after columns exist
    "CREATE INDEX IF NOT EXISTS idx_media_browse ON media_items (season, story, path_depth)",
]

_db_path: Optional[Path] = None


async def init_db(db_path: Path) -> None:
    global _db_path
    _db_path = db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(db_path, timeout=30) as conn:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.executescript(CREATE_TABLE_SQL)
        for sql in _MIGRATIONS:
            try:
                await conn.execute(sql)
            except Exception:
                pass  # column already exists
        await conn.commit()


async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    assert _db_path is not None, "Database not initialized"
    async with aiosqlite.connect(_db_path, timeout=30) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        yield conn


async def upsert_media_item(conn: aiosqlite.Connection, item: dict) -> int:
    await conn.execute(
        """
        INSERT INTO media_items
            (file_path, file_size_bytes, file_mtime, title, duration_seconds,
             video_codec, audio_codec, width, height, bit_rate, is_missing, updated_at)
        VALUES
            (:file_path, :file_size_bytes, :file_mtime, :title, :duration_seconds,
             :video_codec, :audio_codec, :width, :height, :bit_rate, 0, datetime('now'))
        ON CONFLICT(file_path) DO UPDATE SET
            file_size_bytes  = excluded.file_size_bytes,
            file_mtime       = excluded.file_mtime,
            title            = excluded.title,
            duration_seconds = excluded.duration_seconds,
            video_codec      = excluded.video_codec,
            audio_codec      = excluded.audio_codec,
            width            = excluded.width,
            height            = excluded.height,
            bit_rate         = excluded.bit_rate,
            is_missing       = 0,
            updated_at       = datetime('now')
        """,
        item,
    )
    cursor = await conn.execute(
        "SELECT id FROM media_items WHERE file_path = ?", (item["file_path"],)
    )
    row = await cursor.fetchone()
    return row["id"]


async def get_all_media(
    conn: aiosqlite.Connection,
    page: int = 1,
    page_size: int = 50,
    query: Optional[str] = None,
) -> list[dict]:
    offset = (page - 1) * page_size
    if query:
        cursor = await conn.execute(
            "SELECT * FROM media_items WHERE is_missing = 0 AND title LIKE ? "
            "ORDER BY title COLLATE NOCASE LIMIT ? OFFSET ?",
            (f"%{query}%", page_size, offset),
        )
    else:
        cursor = await conn.execute(
            "SELECT * FROM media_items WHERE is_missing = 0 "
            "ORDER BY title COLLATE NOCASE LIMIT ? OFFSET ?",
            (page_size, offset),
        )
    rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def get_media_by_id(
    conn: aiosqlite.Connection, media_id: int
) -> Optional[dict]:
    cursor = await conn.execute(
        "SELECT * FROM media_items WHERE id = ?", (media_id,)
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def mark_missing(conn: aiosqlite.Connection, file_paths: list[str]) -> int:
    if not file_paths:
        return 0
    placeholders = ",".join("?" * len(file_paths))
    cursor = await conn.execute(
        f"UPDATE media_items SET is_missing = 1, updated_at = datetime('now') "
        f"WHERE file_path NOT IN ({placeholders}) AND is_missing = 0",
        file_paths,
    )
    return cursor.rowcount


async def set_thumb_ready(conn: aiosqlite.Connection, media_id: int) -> None:
    await conn.execute(
        "UPDATE media_items SET thumb_ready = 1, updated_at = datetime('now') WHERE id = ?",
        (media_id,),
    )
    await conn.commit()
