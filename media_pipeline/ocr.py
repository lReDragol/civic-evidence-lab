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

log = logging.getLogger(__name__)

_ocr_engine = None


def get_ocr_engine(settings: dict = None):
    global _ocr_engine
    if _ocr_engine is not None:
        return _ocr_engine

    try:
        from paddleocr import PaddleOCR
        _ocr_engine = PaddleOCR(use_angle_cls=True, lang="ru", show_log=False)
        log.info("PaddleOCR loaded (lang=ru)")
        return _ocr_engine
    except ImportError:
        log.error("paddleocr not installed. Run: pip install paddleocr")
        return None
    except Exception as e:
        log.error("Failed to load PaddleOCR: %s", e)
        return None


def ocr_image(image_path: str, settings: dict = None) -> str:
    engine = get_ocr_engine(settings)
    if engine is None:
        return ""

    try:
        result = engine.ocr(image_path, cls=True)
        texts = []
        if result and result[0]:
            for line in result[0]:
                if line and len(line) >= 2:
                    text = line[1][0] if isinstance(line[1], (list, tuple)) else str(line[1])
                    texts.append(text.strip())
        return "\n".join(texts)
    except Exception as e:
        log.warning("OCR failed for %s: %s", image_path, e)
        return ""


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
        SELECT a.id, a.file_path, a.attachment_type, a.ocr_text, c.id as content_id
        FROM attachments a
        JOIN content_items c ON c.id = a.content_item_id
        WHERE (a.attachment_type IN ('keyframe','scan','photo') AND (a.ocr_text IS NULL OR a.ocr_text = ''))
           OR (a.attachment_type = 'pdf' AND c.body_text = '')
        LIMIT 100
        """
    ).fetchall()

    if not rows:
        log.info("No unprocessed OCR targets")
        conn.close()
        return

    for row in rows:
        att_id = row["id"]
        file_path = row["file_path"]
        att_type = row["attachment_type"]
        content_id = row["content_id"]

        if not os.path.exists(file_path):
            continue

        log.info("OCR: %s (%s)", os.path.basename(file_path), att_type)

        if att_type == "pdf":
            text = ocr_pdf(file_path, settings)
            if text:
                conn.execute("UPDATE content_items SET body_text=? WHERE id=?", (text, content_id))
                conn.execute("UPDATE attachments SET ocr_text=? WHERE id=?", (text, att_id))
        elif att_type in ("keyframe", "scan", "photo"):
            text = ocr_image(file_path, settings)
            if text:
                conn.execute("UPDATE attachments SET ocr_text=? WHERE id=?", (text, att_id))

    conn.commit()
    conn.close()
    log.info("OCR batch complete")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    process_unprocessed_ocr()


if __name__ == "__main__":
    main()
