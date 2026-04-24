import logging
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in os.sys.path:
    os.sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

_pipeline = None


def get_pipeline(settings: dict = None):
    global _pipeline
    if _pipeline is not None:
        return _pipeline

    if settings is None:
        settings = load_settings()

    try:
        import torch
        from faster_whisper import WhisperModel, BatchedInferencePipeline

        model_name = settings.get("whisper_model", "large-v3")
        device = "cuda" if torch.cuda.is_available() else "cpu"
        compute_type = "float16" if device == "cuda" else "int8"

        if device == "cuda":
            torch.backends.cudnn.benchmark = True
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True

        base_model = WhisperModel(model_name, device=device, compute_type=compute_type)
        _pipeline = BatchedInferencePipeline(model=base_model)
        log.info("Whisper loaded: %s on %s (%s)", model_name, device.upper(), compute_type)
        return _pipeline
    except ImportError:
        log.error("faster_whisper or torch not installed")
        return None
    except Exception as e:
        log.error("Failed to load Whisper: %s", e)
        return None


def transcribe_file(audio_path: str, settings: dict = None) -> dict:
    pipeline = get_pipeline(settings)
    if pipeline is None:
        return {"text": "", "segments": [], "language": "ru"}

    try:
        t0 = time.time()
        segments_gen, info = pipeline.transcribe(
            audio_path,
            beam_size=5,
            batch_size=8,
            language="ru",
            vad_filter=True,
        )
        all_segments = []
        full_text = []
        for seg in segments_gen:
            all_segments.append({
                "start": seg.start,
                "end": seg.end,
                "text": seg.text.strip(),
            })
            full_text.append(seg.text.strip())

        elapsed = time.time() - t0
        log.info("Transcribed %s: %d segments in %.1fs", os.path.basename(audio_path), len(all_segments), elapsed)

        return {
            "text": " ".join(full_text),
            "segments": all_segments,
            "language": info.language,
        }
    except Exception as e:
        log.error("Transcription failed for %s: %s", audio_path, e)
        return {"text": "", "segments": [], "language": "ru"}


def extract_audio(video_path: str) -> Optional[str]:
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", video_path, "-vn", "-ac", "1", "-ar", "16000", tmp.name],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=300,
        )
        return tmp.name
    except Exception as e:
        log.error("Audio extraction failed for %s: %s", video_path, e)
        try:
            os.unlink(tmp.name)
        except Exception:
            pass
        return None


def transcribe_video(video_path: str, settings: dict = None) -> dict:
    audio_path = extract_audio(video_path)
    if not audio_path:
        return {"text": "", "segments": [], "language": "ru"}
    try:
        return transcribe_file(audio_path, settings)
    finally:
        try:
            os.unlink(audio_path)
        except Exception:
            pass


def _fmt_time(seconds: float) -> str:
    m = int(seconds // 60)
    s = int(seconds % 60)
    return f"{m:02d}:{s:02d}"


def process_untranscribed_videos(settings: dict = None):
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)
    rows = conn.execute(
        """
        SELECT c.id, a.file_path, a.attachment_type
        FROM content_items c
        JOIN attachments a ON a.content_item_id = c.id
        WHERE c.content_type = 'video'
          AND (c.body_text IS NULL OR c.body_text = '')
          AND a.attachment_type = 'video'
          AND a.file_path != ''
        LIMIT 50
        """
    ).fetchall()

    if not rows:
        log.info("No untranscribed videos")
        conn.close()
        return

    for row in rows:
        content_id = row["id"]
        video_path = row["file_path"]

        if not os.path.exists(video_path):
            log.warning("Video not found: %s", video_path)
            continue

        log.info("Transcribing: %s", os.path.basename(video_path))
        result = transcribe_video(video_path, settings)

        if result["text"]:
            conn.execute(
                "UPDATE content_items SET body_text=? WHERE id=?",
                (result["text"], content_id),
            )

            for seg in result["segments"]:
                ts_start = _fmt_time(seg["start"])
                ts_end = _fmt_time(seg["end"])
                conn.execute(
                    """INSERT INTO quotes(content_item_id, quote_text, timecode_start, timecode_end, is_flagged)
                       VALUES(?,?,?,?,0)""",
                    (content_id, seg["text"], ts_start, ts_end),
                )

        conn.commit()

    conn.close()
    log.info("Transcription batch complete")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    process_untranscribed_videos()


if __name__ == "__main__":
    main()
