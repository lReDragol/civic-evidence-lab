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
_ocr_engine_failed = False


def _ocr_source_key(source_id: int | None) -> str:
    return f"ocr:{int(source_id or 0)}"


def get_ocr_engine(settings: dict = None):
    global _ocr_engine, _ocr_engine_failed
    if _ocr_engine is not None:
        return _ocr_engine
    if _ocr_engine_failed:
        return None

    try:
        from paddleocr import PaddleOCR
        try:
            _ocr_engine = PaddleOCR(use_angle_cls=True, lang="ru", show_log=False)
        except TypeError as exc:
            if "show_log" not in str(exc):
                raise
            _ocr_engine = PaddleOCR(use_angle_cls=True, lang="ru")
        except Exception as exc:
            if "show_log" not in str(exc):
                raise
            _ocr_engine = PaddleOCR(use_angle_cls=True, lang="ru")
        log.info("PaddleOCR loaded (lang=ru)")
        _ocr_engine_failed = False
        return _ocr_engine
    except ImportError:
        log.error("paddleocr not installed. Run: pip install paddleocr")
        _ocr_engine_failed = True
        return None
    except Exception as e:
        log.error("Failed to load PaddleOCR: %s", e)
        _ocr_engine_failed = True
        return None


def ocr_image(image_path: str, settings: dict = None) -> str:
    text, _ = _ocr_image_with_error(image_path, settings)
    return text


def _ocr_image_with_error(image_path: str, settings: dict = None) -> tuple[str, str | None]:
    engine = get_ocr_engine(settings)
    if engine is None:
        return "", "OCR engine unavailable"

    try:
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
        texts = []
        if result and result[0]:
            for line in result[0]:
                if line and len(line) >= 2:
                    text = line[1][0] if isinstance(line[1], (list, tuple)) else str(line[1])
                    texts.append(text.strip())
        return "\n".join(texts), None
    except Exception as e:
        log.warning("OCR failed for %s: %s", image_path, e)
        return "", f"{type(e).__name__}: {e}"


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
            text = ocr_pdf(file_path, settings)
            if text:
                conn.execute("UPDATE content_items SET body_text=? WHERE id=?", (text, content_id))
                conn.execute("UPDATE attachments SET ocr_text=? WHERE id=?", (text, att_id))
                text_updates += 1
        elif att_type in ("keyframe", "scan", "photo"):
            text, error_message = _ocr_image_with_error(file_path, settings)
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
