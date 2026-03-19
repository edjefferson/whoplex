import re
import shutil
from pathlib import Path


def cleanup_except(base: Path, keep_id: int) -> None:
    """Delete all generated data except for keep_id. Called when switching episodes."""

    # data/hls/{id}_v  |  {id}_a{N}  |  {id}_t{N}
    hls_dir = base / "hls"
    if hls_dir.exists():
        for entry in hls_dir.iterdir():
            m = re.match(r'^(\d+)_', entry.name)
            if m and int(m.group(1)) != keep_id:
                shutil.rmtree(entry, ignore_errors=True)

    # data/subtitles/{id}_{track}.vtt
    subs_dir = base / "subtitles"
    if subs_dir.exists():
        for entry in subs_dir.iterdir():
            m = re.match(r'^(\d+)_', entry.name)
            if m and int(m.group(1)) != keep_id:
                entry.unlink(missing_ok=True)

    # data/bsubs/{id}_{track}/
    bsubs_dir = base / "bsubs"
    if bsubs_dir.exists():
        for entry in bsubs_dir.iterdir():
            m = re.match(r'^(\d+)_', entry.name)
            if m and int(m.group(1)) != keep_id:
                shutil.rmtree(entry, ignore_errors=True)

    print(f"[cleanup] kept {keep_id}, removed everything else")
