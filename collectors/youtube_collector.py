import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in os.sys.path:
    os.sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings, ensure_dirs
from db.file_store import attach_file

log = logging.getLogger(__name__)


def _run_ytdlp(args: List[str], timeout: int = 120) -> Tuple[int, str, str]:
    try:
        result = subprocess.run(
            ["yt-dlp"] + args,
            capture_output=True, text=True, timeout=timeout,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except FileNotFoundError:
        return -2, "", "yt-dlp not found"
    except Exception as e:
        return -3, "", str(e)


def _get_video_metadata(url: str) -> Optional[Dict]:
    code, out, err = _run_ytdlp([
        "--dump-json",
        "--no-download",
        "--no-playlist",
        "--playlist-end", "1",
        "--flat-playlist",
        url,
    ], timeout=60)

    if code != 0:
        log.warning("yt-dlp metadata failed for %s: %s", url, err[:200])
        return None

    try:
        return json.loads(out)
    except json.JSONDecodeError:
        log.warning("yt-dlp output not JSON for %s", url)
        return None


def _get_channel_videos(channel_url: str, limit: int = 50) -> List[Dict]:
    code, out, err = _run_ytdlp([
        "--dump-json",
        "--no-download",
        "--flat-playlist",
        "--playlist-end", str(limit),
        channel_url,
    ], timeout=180)

    if code != 0:
        log.warning("yt-dlp channel list failed for %s: %s", channel_url, err[:200])
        return []

    videos = []
    for line in out.strip().split("\n"):
        if not line.strip():
            continue
        try:
            data = json.loads(line)
            videos.append(data)
        except json.JSONDecodeError:
            continue

    return videos


def _download_video(video_url: str, output_dir: Path, format_spec: str = "bestvideo[height<=720]+bestaudio/best[height<=720]") -> Optional[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_template = str(output_dir / "%(id)s.%(ext)s")

    code, out, err = _run_ytdlp([
        "--format", format_spec,
        "--output", out_template,
        "--merge-output-format", "mp4",
        "--no-playlist",
        video_url,
    ], timeout=600)

    if code != 0:
        log.warning("yt-dlp download failed for %s: %s", video_url, err[:200])
        return None

    for line in out.strip().split("\n"):
        if "Destination" in line or "Merging" in line:
            match = re.search(r'[\s:]([^\s]+\.(?:mp4|mkv|webm))', line)
            if match:
                return Path(match.group(1))

    candidates = sorted(output_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _download_subtitle(video_url: str, output_dir: Path, lang: str = "ru") -> Optional[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_template = str(output_dir / "%(id)s")

    code, _, _ = _run_ytdlp([
        "--write-auto-sub",
        "--sub-lang", lang,
        "--skip-download",
        "--output", out_template,
        "--convert-subs", "srt",
        video_url,
    ], timeout=60)

    srt_files = sorted(output_dir.glob("*.srt"), key=lambda p: p.stat().st_mtime, reverse=True)
    return srt_files[0] if srt_files else None


def _video_id_from_data(data: Dict) -> str:
    for key in ("id", "video_id", "display_id"):
        val = data.get(key)
        if val:
            return str(val)
    url = data.get("url", data.get("webpage_url", ""))
    if url:
        m = re.search(r'(?:v=|/)([\w-]{11})(?:$|[?&])', url)
        if m:
            return m.group(1)
    return hashlib.sha256(json.dumps(data, default=str).encode()).hexdigest()[:16]


def _srt_to_plain_text(srt_path: Path) -> str:
    lines = []
    time_re = re.compile(r'^\d{2}:\d{2}:\d{2}')
    seq_re = re.compile(r'^\d+$')
    for line in srt_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or seq_re.match(line) or time_re.match(line) or '-->' in line:
            continue
        lines.append(line)
    return " ".join(lines)


def process_channel_videos(source_id: int, channel_url: str, conn: sqlite3.Connection, settings: dict, limit: int = 50, download: bool = False):
    full_url = channel_url
    if not full_url.startswith("http"):
        full_url = f"https://www.{full_url}"

    log.info("Fetching channel videos: %s (limit=%d)", full_url, limit)
    videos = _get_channel_videos(full_url, limit=limit)

    if not videos:
        log.warning("No videos found for %s", full_url)
        return 0

    new_count = 0
    for video_data in videos:
        vid = _video_id_from_data(video_data)
        external_id = f"yt_{vid}"

        existing = conn.execute(
            "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=?",
            (source_id, external_id),
        ).fetchone()
        if existing:
            continue

        title = video_data.get("title", video_data.get("fulltitle", ""))
        description = video_data.get("description", "")
        published = video_data.get("upload_date", "")
        if published and len(published) == 8:
            published = f"{published[:4]}-{published[4:6]}-{published[6:8]}"
        duration = video_data.get("duration", 0)
        view_count = video_data.get("view_count", 0)
        channel = video_data.get("channel", video_data.get("uploader", ""))

        payload = {
            "video_id": vid,
            "title": title,
            "channel": channel,
            "duration": duration,
            "view_count": view_count,
            "description_preview": (description or "")[:500],
        }
        raw_json = json.dumps(payload, ensure_ascii=False, default=str)
        raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

        cur = conn.execute(
            """INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
               VALUES(?,?,?,?,?,1)""",
            (source_id, external_id, raw_json, datetime.now().isoformat(), raw_hash),
        )
        raw_id = cur.lastrowid

        webpage_url = video_data.get("webpage_url", f"https://www.youtube.com/watch?v={vid}")

        cur2 = conn.execute(
            """INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, published_at, collected_at, url, status)
               VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')""",
            (source_id, raw_id, external_id, "video", title, description or "", published or datetime.now().isoformat(), datetime.now().isoformat(), webpage_url),
        )
        content_id = cur2.lastrowid

        if download:
            video_dir = Path(settings.get("processed_youtube", "")) / channel[:50].replace(" ", "_") / (published[:7] if published else "unknown")
            video_path = _download_video(webpage_url, video_dir)
            if video_path:
                from collectors.watch_folder import file_hash
                sha = file_hash(video_path)
                attach_file(
                    conn,
                    content_id,
                    raw_id,
                    video_path,
                    "video",
                    original_url=webpage_url,
                    mime_type="video/mp4",
                    hash_sha256=sha,
                    file_size=video_path.stat().st_size,
                    metadata={"youtube_video_id": vid},
                )

            srt_path = _download_subtitle(webpage_url, video_dir)
            if srt_path:
                plain = _srt_to_plain_text(srt_path)
                if plain:
                    conn.execute(
                        "UPDATE content_items SET body_text=? WHERE id=?",
                        (plain[:50000], content_id),
                    )
                from collectors.watch_folder import file_hash
                srt_sha = file_hash(srt_path)
                attach_file(
                    conn,
                    content_id,
                    raw_id,
                    srt_path,
                    "subtitle",
                    original_url=f"{webpage_url}#subtitle",
                    mime_type="text/srt",
                    hash_sha256=srt_sha,
                    file_size=srt_path.stat().st_size,
                    metadata={"youtube_video_id": vid},
                )

        conn.commit()
        new_count += 1

    conn.execute("UPDATE sources SET last_checked_at=? WHERE id=?", (datetime.now().isoformat(), source_id))
    conn.commit()

    log.info("Channel %s: %d new videos out of %d", channel_url, new_count, len(videos))
    return new_count


def collect_youtube(settings: dict = None, download: bool = False, limit: int = 50):
    if settings is None:
        settings = load_settings()

    ensure_dirs(settings)
    conn = get_db(settings)

    sources = conn.execute(
        "SELECT id, name, url, access_method FROM sources WHERE category='youtube' AND is_active=1"
    ).fetchall()

    if not sources:
        log.info("No active YouTube sources configured")
        conn.close()
        return

    total_new = 0
    for src in sources:
        if src["access_method"] not in ("yt_dlp", "yt-dlp", None, ""):
            continue
        url = src["url"]
        if not url:
            continue

        try:
            count = process_channel_videos(src["id"], url, conn, settings, limit=limit, download=download)
            total_new += count
        except Exception as e:
            log.error("Error processing %s: %s", src["name"], e)

    log.info("YouTube collection done: %d new videos total", total_new)
    conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--download", action="store_true", help="Download video files")
    parser.add_argument("--limit", type=int, default=50, help="Max videos per channel")
    args = parser.parse_args()
    collect_youtube(download=args.download, limit=args.limit)


if __name__ == "__main__":
    main()
