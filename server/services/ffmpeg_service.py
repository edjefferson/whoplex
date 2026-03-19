import asyncio
from pathlib import Path
from typing import Any

import ffmpeg


async def probe(file_path: str) -> dict[str, Any]:
    """Async wrapper around ffmpeg.probe (synchronous)."""
    return await asyncio.to_thread(ffmpeg.probe, file_path)


async def generate_thumbnail(
    file_path: str,
    output_path: Path,
    seek_seconds: float = 180.0,
    width: int = 320,
) -> None:
    """Extract a single JPEG frame at seek_seconds into the file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Clamp seek time: for videos shorter than the desired seek, use 25% in instead
    info = await probe(file_path)
    duration = float(info.get("format", {}).get("duration") or 0)
    if duration > 0:
        if duration <= seek_seconds:
            seek_seconds = duration * 0.25
        seek_seconds = max(min(seek_seconds, duration - 2), 0)

    def _run() -> None:
        (
            ffmpeg
            .input(file_path, ss=seek_seconds)
            .filter("scale", width, -1)
            .output(str(output_path), vframes=1, format="image2")
            .overwrite_output()
            .run(quiet=True)
        )

    await asyncio.to_thread(_run)


async def remux_audio_stream(
    file_path: str,
    audio_track: int,
    start_seconds: float = 0.0,
    chunk_size: int = 65536,
):
    """
    Async generator: remux file with a single audio track via ffmpeg subprocess.
    Yields raw bytes suitable for a StreamingResponse.
    """
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
    if start_seconds > 0:
        cmd += ["-ss", str(start_seconds)]
    cmd += [
        "-i", file_path,
        "-map", "0:v:0",
        "-map", f"0:a:{audio_track}",
        "-c", "copy",
        "-f", "matroska",
        "pipe:1",
    ]
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        while True:
            chunk = await process.stdout.read(chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        if process.returncode is None:
            process.terminate()
            await process.wait()


async def extract_subtitle_webvtt(
    file_path: str, subtitle_index: int, output_path: Path
) -> None:
    """Extract subtitle stream at subtitle_index to a WebVTT file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    def _run() -> None:
        inp = ffmpeg.input(file_path)
        (
            ffmpeg
            .output(inp[f"s:{subtitle_index}"], str(output_path), f="webvtt")
            .overwrite_output()
            .run(quiet=True)
        )

    await asyncio.to_thread(_run)
