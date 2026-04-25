import logging
import os
import sqlite3
import sys
from pathlib import Path
from typing import List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in os.sys.path:
    os.sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings
from runtime.state import record_dead_letter, update_source_sync_state

log = logging.getLogger(__name__)

_ocr_engine = None
_ocr_backend = None
_ocr_failed_backends: set[str] = set()


def _ocr_source_key(source_id: int | None) -> str:
    return f"ocr:{int(source_id or 0)}"


def _ocr_preferences(settings: dict | None = None) -> tuple[str, bool]:
    settings = settings or {}
    preferred = str(settings.get("ocr_engine", "auto") or "auto").strip().lower()
    allow_fallback = bool(settings.get("ocr_allow_fallback", True))
    return preferred, allow_fallback


def _effective_ocr_settings(conn: sqlite3.Connection, settings: dict | None = None) -> dict:
    effective = dict(settings or {})
    preferred, _ = _ocr_preferences(effective)
    if preferred not in {"auto", "", "none"}:
        return effective

    rows = conn.execute(
        """
        SELECT state, last_error, metadata_json
        FROM source_sync_state
        WHERE source_key LIKE 'ocr:%'
        ORDER BY COALESCE(last_success_at, last_attempt_at, '') DESC, id DESC
        LIMIT 5
        """
    ).fetchall()
    for row in rows:
        metadata = {}
        try:
            import json

            metadata = json.loads(row["metadata_json"] or "{}")
        except Exception:
            metadata = {}
        if str(metadata.get("backend") or "").strip().lower() == "rapidocr" and str(row["state"] or "").lower() == "ok":
            effective["ocr_engine"] = "rapidocr"
            return effective
        last_error = str(row["last_error"] or "")
        if "ConvertPirAttribute2RuntimeAttribute" in last_error or "oneDNN" in last_error:
            effective["ocr_engine"] = "rapidocr"
            return effective
    return effective


def _backend_order(settings: dict | None = None) -> list[str]:
    preferred, allow_fallback = _ocr_preferences(settings)
    if preferred == "rapidocr":
        return ["rapidocr", "paddleocr"] if allow_fallback else ["rapidocr"]
    if preferred == "paddleocr":
        return ["paddleocr", "rapidocr"] if allow_fallback else ["paddleocr"]
    return ["paddleocr", "rapidocr"] if allow_fallback else ["paddleocr"]


def _load_paddle_engine():
    from paddleocr import PaddleOCR

    try:
        return PaddleOCR(use_angle_cls=True, lang="ru", show_log=False)
    except TypeError as exc:
        if "show_log" not in str(exc):
            raise
        return PaddleOCR(use_angle_cls=True, lang="ru")
    except Exception as exc:
        if "show_log" not in str(exc):
            raise
        return PaddleOCR(use_angle_cls=True, lang="ru")


def _load_rapidocr_engine():
    from rapidocr_onnxruntime import RapidOCR

    return RapidOCR()


def _remember_engine(engine, backend: str):
    global _ocr_engine, _ocr_backend
    _ocr_engine = engine
    _ocr_backend = backend
    return engine


def _clear_engine_cache(*, failed_backend: str | None = None):
    global _ocr_engine, _ocr_backend
    _ocr_engine = None
    _ocr_backend = None
    if failed_backend:
        _ocr_failed_backends.add(failed_backend)


def get_ocr_engine(settings: dict = None):
    if _ocr_engine is not None:
        return _ocr_engine

    for backend in _backend_order(settings):
        if backend in _ocr_failed_backends:
            continue
        try:
            if backend == "paddleocr":
                engine = _load_paddle_engine()
                log.info("PaddleOCR loaded (lang=ru)")
            elif backend == "rapidocr":
                engine = _load_rapidocr_engine()
                log.info("RapidOCR loaded (onnxruntime backend)")
            else:
                continue
            return _remember_engine(engine, backend)
        except ImportError:
            log.error("%s backend not installed", backend)
            _ocr_failed_backends.add(backend)
        except Exception as e:
            log.error("Failed to load %s: %s", backend, e)
            _ocr_failed_backends.add(backend)
    return None


def ocr_image(image_path: str, settings: dict = None) -> str:
    text, _ = _ocr_image_with_error(image_path, settings)
    return text


def _ocr_image_with_error(image_path: str, settings: dict = None) -> tuple[str, str | None]:
    engine = get_ocr_engine(settings)
    if engine is None:
        return "", "OCR engine unavailable"

    try:
        text, error_message = _run_backend_ocr(engine, _ocr_backend or "unknown", image_path)
        if error_message and (_ocr_backend or "") == "paddleocr" and "rapidocr" not in _ocr_failed_backends:
            fallback_engine = _switch_to_backend("rapidocr")
            if fallback_engine is not None:
                text, fallback_error = _run_backend_ocr(fallback_engine, "rapidocr", image_path)
                if fallback_error is None:
                    return text, None
                return "", fallback_error
        return text, error_message
    except Exception as e:
        log.warning("OCR failed for %s: %s", image_path, e)
        return "", f"{type(e).__name__}: {e}"


def _switch_to_backend(backend: str):
    if backend in _ocr_failed_backends:
        return None
    _clear_engine_cache()
    try:
        if backend == "rapidocr":
            return _remember_engine(_load_rapidocr_engine(), backend)
        if backend == "paddleocr":
            return _remember_engine(_load_paddle_engine(), backend)
    except ImportError:
        _ocr_failed_backends.add(backend)
    except Exception as exc:
        log.error("Failed to switch OCR backend to %s: %s", backend, exc)
        _ocr_failed_backends.add(backend)
    return None


def _extract_rapidocr_text(result) -> str:
    texts = []
    for line in result or []:
        if not line or len(line) < 2:
            continue
        text = line[1]
        if isinstance(text, (list, tuple)):
            text = text[0] if text else ""
        texts.append(str(text).strip())
    return "\n".join(item for item in texts if item)


def _extract_paddle_text(result) -> str:
    texts = []
    if result and result[0]:
        for line in result[0]:
            if line and len(line) >= 2:
                text = line[1][0] if isinstance(line[1], (list, tuple)) else str(line[1])
                texts.append(text.strip())
    return "\n".join(item for item in texts if item)


def _run_backend_ocr(engine, backend: str, image_path: str) -> tuple[str, str | None]:
    try:
        if backend == "rapidocr":
            result, _ = engine(image_path)
            return _extract_rapidocr_text(result), None
        try:
            result = engine.ocr(image_path, cls=True)
        except TypeError as exc:
            if "cls" not in str(exc):
                raise
            result = engine.ocr(image_path)
        except Exception as exc:
            if "cls" not in str(exc):
                raise
            result = engine.ocr(image_path)
        return _extract_paddle_text(result), None
    except Exception as exc:
        log.warning("%s OCR failed for %s: %s", backend, image_path, exc)
        return "", f"{type(exc).__name__}: {exc}"


def ocr_pdf(pdf_path: str, settings: dict = None) -> str:
    try:
        import fitz
        doc = fitz.open(pdf_path)
        texts = []
        for page_num in range(min(len(doc), 50)):
            page = doc[page_num]
            text = page.get_text("text").strip()
            if text and len(text) > 20:
                texts.append(text)
            else:
                pix = page.get_pixmap(dpi=200)
                img_path = pdf_path + f"_page_{page_num}.png"
                pix.save(img_path)
                ocr_text = ocr_image(img_path, settings)
                if ocr_text:
                    texts.append(ocr_text)
                try:
                    os.unlink(img_path)
                except Exception:
                    pass
        doc.close()
        return "\n\n".join(texts)
    except ImportError:
        log.error("PyMuPDF not installed. Run: pip install PyMuPDF")
        return ""
    except Exception as e:
        log.error("PDF extraction failed for %s: %s", pdf_path, e)
        return ""


def process_unprocessed_ocr(settings: dict = None):
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)
    ocr_settings = _effective_ocr_settings(conn, settings)

    rows = conn.execute(
        """
        SELECT a.id, a.file_path, a.attachment_type, a.ocr_text, a.hash_sha256, c.id as content_id, c.source_id
        FROM attachments a
        JOIN content_items c ON c.id = a.content_item_id
        LEFT JOIN dead_letter_items d
          ON d.attachment_id = a.id
         AND d.resolved_at IS NULL
         AND d.failure_stage IN ('ocr_missing_attachment', 'ocr_runtime')
        WHERE (
                (a.attachment_type IN ('keyframe','scan','photo') AND (a.ocr_text IS NULL OR a.ocr_text = ''))
             OR (a.attachment_type = 'pdf' AND c.body_text = '')
        )
          AND d.id IS NULL
        LIMIT 100
        """
    ).fetchall()

    if not rows:
        log.info("No unprocessed OCR targets")
        conn.close()
        return {"ok": True, "items_seen": 0, "items_new": 0, "items_updated": 0, "dead_letters": 0}

    seen = len(rows)
    text_updates = 0
    processed = 0
    dead_letters = 0

    for row in rows:
        att_id = row["id"]
        file_path = row["file_path"]
        att_type = row["attachment_type"]
        content_id = row["content_id"]
        source_id = row["source_id"]
        hash_sha256 = row["hash_sha256"]
        source_key = _ocr_source_key(source_id)

        if not os.path.exists(file_path):
            dead_letters += 1
            record_dead_letter(
                conn,
                failure_stage="ocr_missing_attachment",
                source_key=source_key,
                source_id=source_id,
                attachment_id=att_id,
                content_item_id=content_id,
                error_type="FileNotFoundError",
                error_message=f"Attachment file not found: {file_path}",
                payload={
                    "file_path": file_path,
                    "attachment_type": att_type,
                },
            )
            update_source_sync_state(
                conn,
                source_key=source_key,
                source_id=source_id,
                success=False,
                last_cursor=str(att_id),
                last_external_id=str(att_id),
                last_hash=hash_sha256,
                transport_mode="ocr",
                last_error=f"FileNotFoundError: {file_path}",
                metadata={
                    "attachment_type": att_type,
                    "file_path": file_path,
                    "status": "missing_attachment",
                },
            )
            continue

        log.info("OCR: %s (%s)", os.path.basename(file_path), att_type)
        text = ""

        if att_type == "pdf":
            text = ocr_pdf(file_path, ocr_settings)
            if text:
                conn.execute("UPDATE content_items SET body_text=? WHERE id=?", (text, content_id))
                conn.execute("UPDATE attachments SET ocr_text=? WHERE id=?", (text, att_id))
                text_updates += 1
        elif att_type in ("keyframe", "scan", "photo"):
            text, error_message = _ocr_image_with_error(file_path, ocr_settings)
            if error_message:
                dead_letters += 1
                record_dead_letter(
                    conn,
                    failure_stage="ocr_runtime",
                    source_key=source_key,
                    source_id=source_id,
                    attachment_id=att_id,
                    content_item_id=content_id,
                    error_type=error_message.split(":", 1)[0],
                    error_message=error_message,
                    payload={
                        "attachment_type": att_type,
                        "file_path": file_path,
                    },
                )
                update_source_sync_state(
                    conn,
                    source_key=source_key,
                    source_id=source_id,
                    success=False,
                    last_cursor=str(att_id),
                    last_external_id=str(att_id),
                    last_hash=hash_sha256,
                    transport_mode="ocr",
                    last_error=error_message,
                    metadata={
                        "attachment_type": att_type,
                        "file_path": file_path,
                        "status": "runtime_failed",
                        "backend": _ocr_backend,
                    },
                )
                continue
            if text:
                conn.execute("UPDATE attachments SET ocr_text=? WHERE id=?", (text, att_id))
                text_updates += 1

        processed += 1
        update_source_sync_state(
            conn,
            source_key=source_key,
            source_id=source_id,
            success=True,
            last_cursor=str(att_id),
            last_external_id=str(att_id),
            last_hash=hash_sha256,
            transport_mode="ocr",
            metadata={
                "attachment_type": att_type,
                "file_path": file_path,
                "status": "processed",
                "text_updated": bool(text),
                "backend": _ocr_backend,
            },
        )

    conn.commit()
    conn.close()
    log.info("OCR batch complete")
    return {
        "ok": True,
        "items_seen": seen,
        "items_new": text_updates,
        "items_updated": processed,
        "dead_letters": dead_letters,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    process_unprocessed_ocr()


if __name__ == "__main__":
    main()
