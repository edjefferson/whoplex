#!/usr/bin/env python3
"""whoplex.py — Extract MKV subtitles and produce an EPUB per video file."""

import argparse
import html
import json
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class TrackKind(Enum):
    DIALOGUE = "dialogue"
    SIGNS    = "signs"


@dataclass
class SubEntry:
    start_ms: int
    end_ms:   int
    text:     str
    kind:     TrackKind


# ---------------------------------------------------------------------------
# Step 1: Inspect MKV tracks
# ---------------------------------------------------------------------------

def get_track_info(mkv_path: str) -> list:
    """Run mkvmerge -J and return the tracks list."""
    try:
        result = subprocess.run(
            ["mkvmerge", "-J", mkv_path],
            capture_output=True, text=True, check=True
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            "mkvmerge not found. Install with: brew install mkvtoolnix"
        )
    data = json.loads(result.stdout)
    return data.get("tracks", [])


# ---------------------------------------------------------------------------
# Step 2: Identify subtitle tracks by name keywords
# ---------------------------------------------------------------------------

DIALOGUE_KEYWORDS = {"subtitle", "dialog", "dialogue", "sdh"}
SIGNS_KEYWORDS    = {"sign", "signs", "forced", "info", "caption"}
DIALOGUE_EXCLUDE  = {"sign", "signs", "forced"}


def identify_tracks(tracks: list):
    """Return (dialogue_track, signs_track) from the tracks list."""
    subtitle_tracks = [
        t for t in tracks
        if t.get("type") == "subtitles"
    ]

    if not subtitle_tracks:
        return None, None

    if len(subtitle_tracks) == 1:
        return subtitle_tracks[0], None

    dialogue_candidates = []
    signs_candidates    = []

    for t in subtitle_tracks:
        props = t.get("properties", {})
        name  = (props.get("track_name") or "").lower()
        words = set(re.split(r"[\s_\-/]+", name))

        has_dialogue_kw = bool(words & DIALOGUE_KEYWORDS)
        has_signs_kw    = bool(words & SIGNS_KEYWORDS)
        has_exclude_kw  = bool(words & DIALOGUE_EXCLUDE)

        if has_dialogue_kw and not has_exclude_kw:
            dialogue_candidates.append(t)
        elif has_signs_kw:
            signs_candidates.append(t)

    # Resolve
    if dialogue_candidates and signs_candidates:
        return dialogue_candidates[0], signs_candidates[0]

    if dialogue_candidates and not signs_candidates:
        remaining = [t for t in subtitle_tracks if t not in dialogue_candidates]
        signs = remaining[0] if remaining else None
        return dialogue_candidates[0], signs

    if signs_candidates and not dialogue_candidates:
        remaining = [t for t in subtitle_tracks if t not in signs_candidates]
        dialogue = remaining[0] if remaining else None
        return dialogue, signs_candidates[0]

    # Positional fallback
    return subtitle_tracks[0], subtitle_tracks[1] if len(subtitle_tracks) > 1 else None


# ---------------------------------------------------------------------------
# Step 3: Extract tracks to temp files
# ---------------------------------------------------------------------------

CODEC_SUFFIX_MAP = {
    "S_TEXT/UTF8":   ".srt",
    "S_TEXT/ASCII":  ".srt",
    "S_SRT":         ".srt",
    "S_TEXT/ASS":    ".ass",
    "S_TEXT/SSA":    ".ass",
    "S_ASS":         ".ass",
    "S_SSA":         ".ass",
    "S_VOBSUB":      ".sub",
    "S_HDMV/PGS":    ".sup",
    "S_DVBSUB":      ".dvbsub",
}


def codec_to_suffix(codec: str) -> str:
    return CODEC_SUFFIX_MAP.get(codec.upper(), ".sub")


def extract_track(mkv_path: str, track_id: int, suffix: str, tmp_dir: str) -> str:
    """Extract a single track to a temp file. Returns the output path."""
    out_path = os.path.join(tmp_dir, f"track_{track_id}{suffix}")
    try:
        subprocess.run(
            ["mkvextract", "tracks", mkv_path, f"{track_id}:{out_path}"],
            capture_output=True, text=True, check=True
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            "mkvextract not found. Install with: brew install mkvtoolnix"
        )
    return out_path


# ---------------------------------------------------------------------------
# Step 4: Parse subtitle files
# ---------------------------------------------------------------------------

def srt_time_to_ms(ts: str) -> int:
    """Convert HH:MM:SS,mmm (or HH:MM:SS.mmm) to milliseconds."""
    ts = ts.strip().replace(".", ",")
    # HH:MM:SS,mmm
    m = re.match(r"(\d+):(\d+):(\d+)[,.](\d+)", ts)
    if not m:
        return 0
    h, mn, s, ms = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    # ms field may be 1-3 digits; normalize to ms
    ms_str = m.group(4)
    ms = int(ms_str.ljust(3, "0")[:3])
    return h * 3_600_000 + mn * 60_000 + s * 1_000 + ms


def strip_srt_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text)


def parse_srt(path: str, kind: TrackKind) -> list[SubEntry]:
    with open(path, encoding="utf-8-sig") as f:
        content = f.read()

    entries = []
    blocks  = re.split(r"\n{2,}", content.strip())

    for block in blocks:
        lines = block.strip().splitlines()
        if len(lines) < 3:
            continue
        # First line: sequence number (skip)
        # Second line: timestamps
        # Rest: text
        time_line = lines[1]
        m = re.match(
            r"([\d:,. ]+)\s*-->\s*([\d:,. ]+)",
            time_line
        )
        if not m:
            continue

        start_ms = srt_time_to_ms(m.group(1))
        end_ms   = srt_time_to_ms(m.group(2))

        raw_text = " ".join(lines[2:])
        text     = strip_srt_tags(raw_text).strip()
        if not text:
            continue

        entries.append(SubEntry(start_ms=start_ms, end_ms=end_ms, text=text, kind=kind))

    return entries


def ass_time_to_ms(ts: str) -> int:
    """Convert H:MM:SS.cc (centiseconds) to milliseconds."""
    m = re.match(r"(\d+):(\d+):(\d+)\.(\d+)", ts.strip())
    if not m:
        return 0
    h, mn, s, cs = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    return h * 3_600_000 + mn * 60_000 + s * 1_000 + cs * 10


def strip_ass_tags(text: str) -> str:
    # Strip drawing commands {\p1}...{\p0}
    text = re.sub(r"\{\\p\d+\}.*?\{\\p0\}", "", text, flags=re.DOTALL)
    # Strip override tags
    text = re.sub(r"\{[^}]*\}", "", text)
    # ASS line break
    text = text.replace("\\N", " ").replace("\\n", " ").replace("\\h", " ")
    return text


def parse_ass(path: str, kind: TrackKind) -> list[SubEntry]:
    with open(path, encoding="utf-8-sig") as f:
        content = f.read()

    # Find [Events] section
    events_match = re.search(r"\[Events\](.*?)(?=\[|\Z)", content, re.DOTALL)
    if not events_match:
        return []

    events_section = events_match.group(1)
    lines = events_section.splitlines()

    # Find Format line
    columns = []
    for line in lines:
        if line.startswith("Format:"):
            columns = [c.strip() for c in line[len("Format:"):].split(",")]
            break

    if not columns:
        return []

    try:
        start_idx  = columns.index("Start")
        end_idx    = columns.index("End")
        text_idx   = columns.index("Text")
        layer_idx  = columns.index("Layer") if "Layer" in columns else None
    except ValueError:
        return []

    entries = []
    for line in lines:
        if not line.startswith("Dialogue:"):
            continue
        # Split on comma, but only up to len(columns)-1 times
        # so the Text field (last) can contain commas
        parts = line[len("Dialogue:"):].split(",", len(columns) - 1)
        if len(parts) < len(columns):
            continue

        start_ms = ass_time_to_ms(parts[start_idx])
        end_ms   = ass_time_to_ms(parts[end_idx])
        raw_text = parts[text_idx]
        text     = strip_ass_tags(raw_text).strip()
        if not text:
            continue

        entries.append(SubEntry(start_ms=start_ms, end_ms=end_ms, text=text, kind=kind))

    return entries


SUBTITLE_EDIT = "/Applications/Subtitle Edit.app/Contents/MacOS/SubtitleEdit"


def ocr_vobsub_track(sub_path: str, language: str = "eng") -> str:
    """OCR a VobSub .sub/.idx pair to SRT using SubtitleEdit. Returns output SRT path."""
    idx_path = str(Path(sub_path).with_suffix(".idx"))
    if not os.path.exists(idx_path):
        raise FileNotFoundError(f"VobSub .idx not found alongside .sub: {idx_path}")
    if not os.path.exists(SUBTITLE_EDIT):
        raise FileNotFoundError(
            f"SubtitleEdit not found at {SUBTITLE_EDIT!r}. "
            "Install from: https://www.nikse.dk/subtitleedit"
        )
    subprocess.run(
        [SUBTITLE_EDIT, "/convert", idx_path, "SubRip",
         "/ocrengine:tesseract", f"/language:{language}"],
        capture_output=True, text=True, check=True
    )
    srt_path = str(Path(sub_path).with_suffix(".srt"))
    return srt_path


def ocr_pgs_track(sup_path: str, language: str = "eng") -> str:
    """OCR a PGS .sup file to SRT using pgsrip. Returns output SRT path."""
    try:
        subprocess.run(
            ["pgsrip", "--language", language, sup_path],
            capture_output=True, text=True, check=True
        )
    except FileNotFoundError:
        raise FileNotFoundError(
            "pgsrip not found. Install with: pip install pgsrip  (also needs: brew install tesseract)"
        )
    srt_path = str(Path(sup_path).with_suffix(".srt"))
    return srt_path


def parse_subtitle_file(path: str, kind: TrackKind, codec: str) -> list[SubEntry]:
    suffix = codec_to_suffix(codec).lower()
    if suffix == ".srt":
        return parse_srt(path, kind)
    elif suffix in (".ass", ".ssa"):
        return parse_ass(path, kind)
    else:
        return []


# ---------------------------------------------------------------------------
# Step 5: Interleave
# ---------------------------------------------------------------------------

def interleave(entries: list[SubEntry]) -> list[SubEntry]:
    # "dialogue" < "signs" lexicographically → dialogue wins ties
    return sorted(entries, key=lambda e: (e.start_ms, e.kind.value))


# ---------------------------------------------------------------------------
# Step 6: Build EPUB
# ---------------------------------------------------------------------------

CSS = """\
body {
    font-family: Georgia, "Times New Roman", serif;
    margin: 2em;
    line-height: 1.6;
}
h1 {
    font-size: 1.4em;
    margin-bottom: 1.5em;
}
p {
    margin: 0.4em 0;
}
.ts {
    font-family: "Courier New", Courier, monospace;
    font-size: 0.7em;
    color: #888;
    margin-right: 0.5em;
}
"""


def ms_to_hms(ms: int) -> str:
    s  = ms // 1000
    h  = s // 3600
    s %= 3600
    m  = s // 60
    s %= 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def build_epub(entries: list[SubEntry], video_stem: str, output_dir: str) -> str:
    try:
        from ebooklib import epub
    except ImportError:
        raise ImportError("ebooklib not found. Install with: pip install ebooklib")

    book = epub.EpubBook()
    book.set_identifier(f"whoplex-{video_stem}")
    book.set_title(video_stem)
    book.set_language("en")

    # Build chapter HTML
    paragraphs = []
    for entry in entries:
        ts_html   = f'<small class="ts">{html.escape(ms_to_hms(entry.start_ms))}</small>'
        body_text = html.escape(entry.text)
        if entry.kind == TrackKind.DIALOGUE:
            body_html = f"<em>{body_text}</em>"
        else:
            body_html = body_text
        paragraphs.append(f"<p>{ts_html}{body_html}</p>")

    chapter_body = "\n".join(paragraphs)
    chapter_html = f"""\
<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <title>{html.escape(video_stem)}</title>
  <link rel="stylesheet" type="text/css" href="../Styles/style.css"/>
</head>
<body>
  <h1>{html.escape(video_stem)}</h1>
  {chapter_body}
</body>
</html>
"""

    chapter = epub.EpubHtml(
        title=video_stem,
        file_name="Text/chapter.xhtml",
        lang="en"
    )
    chapter.content = chapter_html

    css_item = epub.EpubItem(
        uid="style",
        file_name="Styles/style.css",
        media_type="text/css",
        content=CSS.encode("utf-8")
    )

    book.add_item(css_item)
    book.add_item(chapter)
    book.toc    = (epub.Link("Text/chapter.xhtml", video_stem, "chapter"),)
    book.spine  = ["nav", chapter]
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())

    out_path = os.path.join(output_dir, f"{video_stem}.epub")
    epub.write_epub(out_path, book)
    return out_path


# ---------------------------------------------------------------------------
# Step 7: Orchestration
# ---------------------------------------------------------------------------

def process_file(mkv_path: str, output_dir: str, verbose: bool, ocr_lang: str = "eng") -> bool:
    """Process a single MKV file. Returns True on success."""
    p = Path(mkv_path)
    if not p.exists():
        print(f"[ERROR] File not found: {mkv_path}", file=sys.stderr)
        return False

    print(f"Processing: {p.name}")

    try:
        tracks = get_track_info(mkv_path)
    except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"  [ERROR] Could not read track info: {e}", file=sys.stderr)
        return False

    dialogue_track, signs_track = identify_tracks(tracks)

    if dialogue_track is None:
        print(f"  [WARN] No subtitle tracks found, skipping.", file=sys.stderr)
        return False

    if verbose:
        d_name = (dialogue_track.get("properties", {}).get("track_name") or "(unnamed)")
        print(f"  Dialogue track: id={dialogue_track['id']}  name={d_name!r}")
        if signs_track:
            s_name = (signs_track.get("properties", {}).get("track_name") or "(unnamed)")
            print(f"  Signs track:    id={signs_track['id']}  name={s_name!r}")
        else:
            print("  Signs track:    (none)")

    all_entries: list[SubEntry] = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        for track, kind in [(dialogue_track, TrackKind.DIALOGUE), (signs_track, TrackKind.SIGNS)]:
            if track is None:
                continue

            props  = track.get("properties", {})
            codec  = props.get("codec_id", "")
            suffix = codec_to_suffix(codec)

            if suffix == ".dvbsub":
                print(f"  [WARN] Track {track['id']} is image-based ({codec}), skipping.", file=sys.stderr)
                continue

            try:
                out_path = extract_track(mkv_path, track["id"], suffix, tmp_dir)
            except (FileNotFoundError, subprocess.CalledProcessError) as e:
                print(f"  [ERROR] Extraction failed for track {track['id']}: {e}", file=sys.stderr)
                continue

            if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
                print(f"  [WARN] Extracted file is empty for track {track['id']}, skipping.", file=sys.stderr)
                continue

            if suffix in (".sup", ".sub"):
                ocr_label = "PGS" if suffix == ".sup" else "VobSub"
                if verbose:
                    print(f"  OCR-ing {ocr_label} track {track['id']} (lang={ocr_lang}) ...")
                try:
                    if suffix == ".sup":
                        out_path = ocr_pgs_track(out_path, ocr_lang)
                    else:
                        out_path = ocr_vobsub_track(out_path, ocr_lang)
                except FileNotFoundError as e:
                    print(f"  [ERROR] {e}", file=sys.stderr)
                    continue
                except subprocess.CalledProcessError as e:
                    print(f"  [ERROR] OCR failed for track {track['id']}: {e}", file=sys.stderr)
                    continue
                if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
                    print(f"  [WARN] OCR produced no output for track {track['id']}, skipping.", file=sys.stderr)
                    continue
                codec = "S_TEXT/UTF8"

            if verbose:
                print(f"  Parsing {kind.value} ({codec_to_suffix(codec)}) ...")

            try:
                entries = parse_subtitle_file(out_path, kind, codec)
            except Exception as e:
                print(f"  [ERROR] Parse failed for track {track['id']}: {e}", file=sys.stderr)
                continue

            if verbose:
                print(f"    → {len(entries)} entries")

            all_entries.extend(entries)

    if not all_entries:
        print("  [WARN] No subtitle entries found, skipping EPUB generation.", file=sys.stderr)
        return False

    merged = interleave(all_entries)

    if verbose:
        print(f"  Total entries after interleave: {len(merged)}")

    try:
        epub_path = build_epub(merged, p.stem, output_dir)
        print(f"  → {epub_path}")
        return True
    except ImportError as e:
        print(f"  [ERROR] {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"  [ERROR] EPUB build failed: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Extract MKV subtitles and produce an EPUB per video file."
    )
    parser.add_argument(
        "files",
        nargs="+",
        metavar="FILE",
        help="MKV file(s) to process"
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=".",
        metavar="DIR",
        help="Directory to write EPUB files (default: current directory)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print detailed progress"
    )
    parser.add_argument(
        "--ocr-lang",
        default="eng",
        metavar="LANG",
        help="Tesseract language code for PGS OCR (default: eng)"
    )
    args = parser.parse_args()

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    success_count = 0
    fail_count    = 0

    for f in args.files:
        ok = process_file(f, output_dir, args.verbose, args.ocr_lang)
        if ok:
            success_count += 1
        else:
            fail_count += 1

    total = success_count + fail_count
    if total > 1:
        print(f"\nDone: {success_count}/{total} file(s) succeeded.")

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
