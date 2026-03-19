import asyncio
from contextlib import asynccontextmanager

import aiosqlite
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from server.config import settings
from server.database import init_db
from server.api import library, stream, thumbnails, cast, browse, tracks, hls, bsubs


async def _scan_background():
    from server.services import scanner

    async with aiosqlite.connect(settings.db_path) as conn:
        result = await scanner.scan_all(settings, conn)
        print(f"[startup scan] {result}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db(settings.db_path)
    settings.thumb_dir.mkdir(parents=True, exist_ok=True)

    if settings.scan_on_startup:
        asyncio.create_task(_scan_background())

    yield


app = FastAPI(title="Whoplex Media Server", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(library.router)
app.include_router(browse.router)
app.include_router(tracks.router)
app.include_router(hls.router)
app.include_router(bsubs.router)
app.include_router(stream.router)
app.include_router(thumbnails.router)
app.include_router(cast.router)

app.mount("/static", StaticFiles(directory="server/static"), name="static")


@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}
