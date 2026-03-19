import asyncio
import shutil
from pathlib import Path

from server.services import ffmpeg_service, proc_registry

# (media_id, track) pairs currently being generated
_generating: set[tuple[int, int]] = set()

# ── New split-stream state ─────────────────────────────────────────────────────
_gen_video: set[int] = set()             # media_id
_gen_audio: set[tuple[int, int]] = set() # (media_id, track)


def hls_dir(base: Path, media_id: int, track: int) -> Path:
    return base / "hls" / f"{media_id}_t{track}"


def video_hls_dir(base: Path, media_id: int) -> Path:
    return base / "hls" / f"{media_id}_v"


def audio_hls_dir(base: Path, media_id: int, track: int) -> Path:
    return base / "hls" / f"{media_id}_a{track}"


def is_ready(base: Path, media_id: int, track: int) -> bool:
    return (hls_dir(base, media_id, track) / ".done").exists()


def is_generating(media_id: int, track: int) -> bool:
    return (media_id, track) in _generating


async def start_generation(
    media_id: int, file_path: str, track: int, base: Path
) -> str:
    """Kick off background HLS generation. Returns current status string."""
    if is_ready(base, media_id, track):
        return "ready"
    if is_generating(media_id, track):
        return "generating"

    _generating.add((media_id, track))
    asyncio.create_task(_generate(media_id, file_path, track, base))
    return "generating"


async def _generate(media_id: int, file_path: str, track: int, base: Path) -> None:
    out_dir = hls_dir(base, media_id, track)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Copy audio if it's already AAC (much faster); transcode otherwise
    try:
        info = await ffmpeg_service.probe(file_path)
        audio_streams = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
        codec = audio_streams[track].get("codec_name", "") if track < len(audio_streams) else ""
    except Exception:
        codec = ""

    audio_args = ["-c:a", "copy"] if codec == "aac" else ["-c:a", "aac", "-b:a", "192k"]

    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", file_path,
        "-map", "0:v:0",
        "-map", f"0:a:{track}",
        "-c:v", "copy",
        *audio_args,
        "-f", "hls",
        "-hls_time", "10",
        "-hls_playlist_type", "vod",
        "-hls_flags", "independent_segments",
        "-hls_segment_filename", str(out_dir / "seg%03d.ts"),
        str(out_dir / "playlist.m3u8"),
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stderr=asyncio.subprocess.DEVNULL
        )
        proc_registry.register(proc)
        returncode = await proc.wait()
        if returncode == 0:
            (out_dir / ".done").touch()
        else:
            shutil.rmtree(out_dir, ignore_errors=True)
    except Exception:
        shutil.rmtree(out_dir, ignore_errors=True)
    finally:
        proc_registry.unregister(proc)
        _generating.discard((media_id, track))


# ── Split-stream generation ────────────────────────────────────────────────────

async def generate_video_hls(media_id: int, file_path: str, base: Path) -> str:
    """Kick off video-only HLS generation. Returns current status string."""
    vdir = video_hls_dir(base, media_id)
    if (vdir / ".done").exists():
        return "ready"
    if media_id in _gen_video:
        return "generating"
    _gen_video.add(media_id)
    asyncio.create_task(_generate_video(media_id, file_path, base))
    return "generating"


async def _generate_video(media_id: int, file_path: str, base: Path) -> None:
    out_dir = video_hls_dir(base, media_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", file_path,
        "-map", "0:v:0",
        "-c:v", "copy",
        "-an",
        "-f", "hls",
        "-hls_time", "10",
        "-hls_playlist_type", "vod",
        "-hls_flags", "independent_segments",
        "-hls_segment_filename", str(out_dir / "seg%03d.ts"),
        str(out_dir / "playlist.m3u8"),
    ]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stderr=asyncio.subprocess.DEVNULL)
        proc_registry.register(proc)
        returncode = await proc.wait()
        if returncode == 0:
            (out_dir / ".done").touch()
            print(f"[hls] {media_id} video: done")
        else:
            shutil.rmtree(out_dir, ignore_errors=True)
    except Exception:
        shutil.rmtree(out_dir, ignore_errors=True)
    finally:
        proc_registry.unregister(proc)
        _gen_video.discard(media_id)


async def generate_audio_hls(media_id: int, file_path: str, track: int, base: Path) -> str:
    """Kick off audio-only HLS generation. Returns current status string."""
    adir = audio_hls_dir(base, media_id, track)
    if (adir / ".done").exists():
        return "ready"
    key = (media_id, track)
    if key in _gen_audio:
        return "generating"
    _gen_audio.add(key)
    asyncio.create_task(_generate_audio(media_id, file_path, track, base))
    return "generating"


async def _generate_audio(media_id: int, file_path: str, track: int, base: Path) -> None:
    out_dir = audio_hls_dir(base, media_id, track)
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        info = await ffmpeg_service.probe(file_path)
        audio_streams = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
        codec = audio_streams[track].get("codec_name", "") if track < len(audio_streams) else ""
    except Exception:
        codec = ""
    audio_args = ["-c:a", "copy"] if codec == "aac" else ["-c:a", "aac", "-b:a", "192k"]
    cmd = [
        "ffmpeg", "-hide_banner", "-loglevel", "error",
        "-i", file_path,
        "-map", f"0:a:{track}",
        "-vn",
        *audio_args,
        "-f", "hls",
        "-hls_time", "10",
        "-hls_playlist_type", "vod",
        "-hls_flags", "independent_segments",
        "-hls_segment_filename", str(out_dir / "seg%03d.ts"),
        str(out_dir / "playlist.m3u8"),
    ]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stderr=asyncio.subprocess.DEVNULL)
        proc_registry.register(proc)
        returncode = await proc.wait()
        if returncode == 0:
            (out_dir / ".done").touch()
            print(f"[hls] {media_id} audio {track}: done")
        else:
            shutil.rmtree(out_dir, ignore_errors=True)
    except Exception:
        shutil.rmtree(out_dir, ignore_errors=True)
    finally:
        proc_registry.unregister(proc)
        _gen_audio.discard((media_id, track))


async def start_generation_all(
    media_id: int, file_path: str, audio_tracks: list, base: Path
) -> None:
    """Single ffmpeg pass: video HLS + all audio HLS tracks simultaneously."""
    vdir = video_hls_dir(base, media_id)
    track_indices = [t["index"] for t in audio_tracks]

    need_video = not (vdir / ".done").exists() and media_id not in _gen_video
    need_audio = [
        i for i in track_indices
        if not (audio_hls_dir(base, media_id, i) / ".done").exists()
        and (media_id, i) not in _gen_audio
    ]

    if not need_video and not need_audio:
        return

    if need_video:
        _gen_video.add(media_id)
    for i in need_audio:
        _gen_audio.add((media_id, i))

    asyncio.create_task(_generate_all(media_id, file_path, audio_tracks, base, need_video, need_audio))


async def _generate_all(
    media_id: int,
    file_path: str,
    audio_tracks: list,
    base: Path,
    do_video: bool,
    do_audio: list[int],
) -> None:
    """Single ffmpeg invocation producing video + audio HLS outputs."""
    try:
        info = await ffmpeg_service.probe(file_path)
        audio_streams = [s for s in info.get("streams", []) if s.get("codec_type") == "audio"]
    except Exception:
        audio_streams = []

    vdir = video_hls_dir(base, media_id)
    adirs: list[tuple[int, Path]] = []

    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", file_path]

    if do_video:
        vdir.mkdir(parents=True, exist_ok=True)
        cmd += [
            "-map", "0:v:0", "-c:v", "copy", "-an",
            "-f", "hls", "-hls_time", "10",
            "-hls_playlist_type", "vod",
            "-hls_flags", "independent_segments",
            "-hls_segment_filename", str(vdir / "seg%03d.ts"),
            str(vdir / "playlist.m3u8"),
        ]

    for t in audio_tracks:
        idx = t["index"]
        if idx not in do_audio:
            continue
        adir = audio_hls_dir(base, media_id, idx)
        adir.mkdir(parents=True, exist_ok=True)
        adirs.append((idx, adir))
        codec = audio_streams[idx].get("codec_name", "") if idx < len(audio_streams) else ""
        audio_args = ["-c:a", "copy"] if codec == "aac" else ["-c:a", "aac", "-b:a", "192k"]
        cmd += [
            "-map", f"0:a:{idx}", "-vn",
            *audio_args,
            "-f", "hls", "-hls_time", "10",
            "-hls_playlist_type", "vod",
            "-hls_flags", "independent_segments",
            "-hls_segment_filename", str(adir / "seg%03d.ts"),
            str(adir / "playlist.m3u8"),
        ]

    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stderr=asyncio.subprocess.DEVNULL)
        proc_registry.register(proc)
        returncode = await proc.wait()
        if returncode == 0:
            if do_video:
                (vdir / ".done").touch()
                print(f"[hls] {media_id} video: done")
            for idx, adir in adirs:
                (adir / ".done").touch()
                print(f"[hls] {media_id} audio {idx}: done")
        else:
            if do_video:
                shutil.rmtree(vdir, ignore_errors=True)
            for _, adir in adirs:
                shutil.rmtree(adir, ignore_errors=True)
    except Exception:
        if do_video:
            shutil.rmtree(vdir, ignore_errors=True)
        for _, adir in adirs:
            shutil.rmtree(adir, ignore_errors=True)
    finally:
        proc_registry.unregister(proc)
        if do_video:
            _gen_video.discard(media_id)
        for idx, _ in adirs:
            _gen_audio.discard((media_id, idx))


def build_master_playlist(base: Path, media_id: int, audio_tracks: list, bit_rate: int = 0) -> str:
    """Build an HLS master playlist string with audio renditions."""
    bandwidth = bit_rate if bit_rate and bit_rate > 0 else 5_000_000
    lines = ["#EXTM3U", "#EXT-X-VERSION:3", ""]
    for i, t in enumerate(audio_tracks):
        lang = t.get("language") or "und"
        name = t.get("label") or f"Track {i + 1}"
        default = "YES" if i == 0 else "NO"
        uri = f"/hls/{media_id}/audio/{t['index']}/playlist.m3u8"
        lines.append(
            f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="audio",'
            f'NAME="{name}",LANGUAGE="{lang}",DEFAULT={default},'
            f'URI="{uri}"'
        )
    lines.append("")
    lines.append(
        f'#EXT-X-STREAM-INF:BANDWIDTH={bandwidth},'
        f'CODECS="avc1.640028,mp4a.40.2",AUDIO="audio"'
    )
    lines.append(f"/hls/{media_id}/video/playlist.m3u8")
    return "\n".join(lines) + "\n"


def get_status(base: Path, media_id: int, track_indices: list[int]) -> dict:
    """Return generation status for video, each audio track, and master readiness."""
    vdir = video_hls_dir(base, media_id)
    if (vdir / ".done").exists():
        video_status = "ready"
    elif media_id in _gen_video:
        video_status = "generating"
    else:
        video_status = "not_started"

    audio_status: dict[int, str] = {}
    for idx in track_indices:
        adir = audio_hls_dir(base, media_id, idx)
        if (adir / ".done").exists():
            audio_status[idx] = "ready"
        elif (media_id, idx) in _gen_audio:
            audio_status[idx] = "generating"
        else:
            audio_status[idx] = "not_started"

    master_ready = (
        video_status == "ready"
        and all(s == "ready" for s in audio_status.values())
    )

    return {
        "video": video_status,
        "audio": audio_status,
        "master_ready": master_ready,
    }
