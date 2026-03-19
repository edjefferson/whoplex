import asyncio
import json
import shutil
import struct
import zlib
from pathlib import Path

_extracting: set[tuple[int, int]] = set()

_PGS_CODECS = {"hdmv_pgs_subtitle", "pgssub"}

# PGS segment type constants
_SEG_PDS = 0x14  # Palette Definition Segment
_SEG_ODS = 0x15  # Object Definition Segment
_SEG_PCS = 0x16  # Presentation Composition Segment
_SEG_WDS = 0x17  # Window Definition Segment
_SEG_END = 0x80  # End of Display Set


def sub_dir(base: Path, media_id: int, track: int) -> Path:
    return base / "bsubs" / f"{media_id}_{track}"


def is_ready(base: Path, media_id: int, track: int) -> bool:
    return (sub_dir(base, media_id, track) / ".done").exists()


def is_extracting(media_id: int, track: int) -> bool:
    return (media_id, track) in _extracting


async def start_extraction(
    media_id: int, file_path: str, track: int, base: Path, codec: str = ""
) -> str:
    if is_ready(base, media_id, track):
        return "ready"
    if is_extracting(media_id, track):
        return "extracting"
    _extracting.add((media_id, track))
    asyncio.create_task(_extract(media_id, file_path, track, base, codec))
    return "extracting"


async def _extract(
    media_id: int, file_path: str, track: int, base: Path, codec: str
) -> None:
    out_dir = sub_dir(base, media_id, track)
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        if codec not in _PGS_CODECS:
            raise NotImplementedError(
                f"Bitmap subtitle codec '{codec}' is not yet supported "
                f"(only PGS/hdmv_pgs_subtitle is supported)"
            )

        sup_path = out_dir / "_subtitle.sup"

        # Copy the PGS bitstream out as a raw .sup file
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-i", file_path,
            "-map", f"0:s:{track}",
            "-c:s", "copy",
            str(sup_path),
            stderr=asyncio.subprocess.PIPE,
        )
        _, err = await proc.communicate()
        if err:
            print(f"[bsubs] ffmpeg: {err.decode().strip()}")

        if not sup_path.exists() or sup_path.stat().st_size == 0:
            raise RuntimeError("PGS extraction produced no output")

        sup_data = sup_path.read_bytes()
        sup_path.unlink()

        # Parse in a thread so we don't block the event loop
        cues = await asyncio.get_running_loop().run_in_executor(
            None, _parse_pgs, sup_data, out_dir
        )

        (out_dir / "cues.json").write_text(json.dumps(cues))
        (out_dir / ".done").touch()
        print(f"[bsubs] {media_id} track {track}: {len(cues)} cues extracted")

    except Exception as e:
        print(f"[bsubs] extraction failed: {e}")
        shutil.rmtree(out_dir, ignore_errors=True)
    finally:
        _extracting.discard((media_id, track))


# ── PGS (.sup) parser ────────────────────────────────────────────────────────

def _ycbcr_to_rgba(y: int, cb: int, cr: int, alpha: int) -> tuple:
    r = y + 1.402 * (cr - 128)
    g = y - 0.344136 * (cb - 128) - 0.714136 * (cr - 128)
    b = y + 1.772 * (cb - 128)
    return (
        max(0, min(255, round(r))),
        max(0, min(255, round(g))),
        max(0, min(255, round(b))),
        alpha,
    )


def _rle_decode(data: bytes, width: int, height: int) -> bytearray:
    """Decode PGS run-length encoded bitmap to palette-index bytearray."""
    out = bytearray(width * height)
    pos = x = y = 0
    while pos < len(data) and y < height:
        c = data[pos]; pos += 1
        if c:
            if x < width:
                out[y * width + x] = c
            x += 1
        else:
            if pos >= len(data):
                break
            c2 = data[pos]; pos += 1
            if c2 == 0:
                x = 0; y += 1
            elif (c2 & 0xC0) == 0x00:
                x += c2  # short transparent run
            elif (c2 & 0xC0) == 0x40:
                if pos >= len(data):
                    break
                c3 = data[pos]; pos += 1
                x += ((c2 & 0x3F) << 8) | c3  # long transparent run
            elif (c2 & 0xC0) == 0x80:
                if pos >= len(data):
                    break
                col = data[pos]; pos += 1
                for _ in range(c2 & 0x3F):
                    if x < width and y < height:
                        out[y * width + x] = col
                    x += 1
            else:  # 0xC0 — long colored run
                if pos + 1 >= len(data):
                    break
                c3 = data[pos]; pos += 1
                col = data[pos]; pos += 1
                for _ in range(((c2 & 0x3F) << 8) | c3):
                    if x < width and y < height:
                        out[y * width + x] = col
                    x += 1
    return out


def _write_png(path: Path, width: int, height: int, rgba: bytes) -> None:
    """Write a raw RGBA PNG using only stdlib (struct + zlib)."""
    def chunk(tag: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack('>I', len(data)) + tag + data + struct.pack('>I', crc)

    ihdr = struct.pack('>IIBBBBB', width, height, 8, 6, 0, 0, 0)

    stride = width * 4
    raw = bytearray()
    for y in range(height):
        raw.append(0)  # filter type None
        raw += rgba[y * stride:(y + 1) * stride]

    path.write_bytes(
        b'\x89PNG\r\n\x1a\n'
        + chunk(b'IHDR', ihdr)
        + chunk(b'IDAT', zlib.compress(bytes(raw)))
        + chunk(b'IEND', b'')
    )


def _parse_pgs(data: bytes, out_dir: Path) -> list:
    """
    Parse a PGS .sup file and write frame_NNNN.png for each subtitle image.
    Returns a list of cue dicts: [{start, end, frame}, ...].
    """
    pos = 0
    n = len(data)

    palette: dict[int, tuple] = {}
    objects: dict[int, bytearray] = {}
    obj_size: dict[int, tuple] = {}
    compositions: list = []
    pcs_pts = 0.0

    cues: list = []
    frame_idx = 0
    last_cue_closed = True  # tracks whether the last cue's end time is finalized

    while pos + 13 <= n:
        if data[pos:pos + 2] != b'PG':
            pos += 1
            continue

        pts_raw = struct.unpack('>I', data[pos + 2:pos + 6])[0]
        pts = pts_raw / 90000.0
        seg_type = data[pos + 10]
        seg_len = struct.unpack('>H', data[pos + 11:pos + 13])[0]
        if pos + 13 + seg_len > n:
            break
        seg = data[pos + 13:pos + 13 + seg_len]
        pos += 13 + seg_len

        if seg_type == _SEG_PCS:
            pcs_pts = pts
            if len(seg) >= 8:
                comp_state = seg[7]
                if comp_state == 0x80:  # Epoch Start — reset palette
                    palette = {}
            compositions = []
            if len(seg) >= 11:
                for i in range(seg[10]):
                    b = 11 + i * 8
                    if b + 8 <= len(seg):
                        oid = struct.unpack('>H', seg[b:b + 2])[0]
                        x = struct.unpack('>H', seg[b + 4:b + 6])[0]
                        y = struct.unpack('>H', seg[b + 6:b + 8])[0]
                        compositions.append((oid, x, y))

        elif seg_type == _SEG_PDS:
            for i in range(2, len(seg) - 3, 5):
                palette[seg[i]] = _ycbcr_to_rgba(
                    seg[i + 1], seg[i + 2], seg[i + 3], seg[i + 4]
                )

        elif seg_type == _SEG_ODS:
            if len(seg) < 4:
                continue
            oid = struct.unpack('>H', seg[0:2])[0]
            seq = seg[3]
            if seq & 0x40:  # first (or only) fragment — contains width/height
                if len(seg) < 11:
                    continue
                w = struct.unpack('>H', seg[7:9])[0]
                h = struct.unpack('>H', seg[9:11])[0]
                obj_size[oid] = (w, h)
                objects[oid] = bytearray(seg[11:])
            else:  # continuation fragment
                if oid in objects:
                    objects[oid].extend(seg[4:])

        elif seg_type == _SEG_END:
            if compositions:
                # New subtitle — if no clear event closed the previous cue, close it now
                if cues and not last_cue_closed:
                    cues[-1]['end'] = pcs_pts

                # Render each composition object
                for oid, cx, cy in compositions:
                    if oid not in obj_size or oid not in objects:
                        continue
                    w, h = obj_size[oid]
                    if w <= 0 or h <= 0:
                        continue

                    indices = _rle_decode(bytes(objects[oid]), w, h)

                    rgba = bytearray(w * h * 4)
                    for i in range(w * h):
                        r, g, b, a = palette.get(indices[i], (0, 0, 0, 0))
                        base = i * 4
                        rgba[base] = r
                        rgba[base + 1] = g
                        rgba[base + 2] = b
                        rgba[base + 3] = a

                    frame_idx += 1
                    frame_file = f"frame_{frame_idx:04d}.png"
                    _write_png(out_dir / frame_file, w, h, bytes(rgba))

                    cues.append({
                        'start': pcs_pts,
                        'end': pcs_pts + 30.0,  # placeholder — overwritten by clear event
                        'frame': frame_file,
                    })
                    last_cue_closed = False
                    break  # one PNG per display set
            else:
                # Clear event — finalize the previous cue's end time
                if cues and not last_cue_closed:
                    cues[-1]['end'] = pcs_pts
                    last_cue_closed = True

            # Reset per-display-set state (palette persists across sets)
            compositions = []
            objects = {}
            obj_size = {}

    return cues
