import sqlite3
import tempfile
import unittest
from pathlib import Path

from collectors import telegram_public_fallback


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


class FakeResponse:
    def __init__(self, text="", content=b"", status_code=200, headers=None):
        self.text = text
        self.content = content
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


class FakeSession:
    def __init__(self, html):
        self.html = html

    def get(self, url, timeout=20, headers=None):
        if url.startswith("https://t.me/s/"):
            return FakeResponse(self.html, self.html.encode("utf-8"), 200, {"content-type": "text/html"})
        if url.startswith("https://cdn.example/"):
            return FakeResponse("", b"jpeg-bytes", 200, {"content-type": "image/jpeg"})
        return FakeResponse("", b"", 404)


class TelegramPublicFallbackTests(unittest.TestCase):
    def test_public_fallback_collects_relevant_yep_documents_and_skips_duplicates(self):
        html = """
        <html><body>
          <div class="tgme_widget_message" data-post="yep_news/27072">
            <a class="tgme_widget_message_date"><time datetime="2026-04-30T12:00:00+00:00"></time></a>
            <a class="tgme_widget_message_photo_wrap" style="background-image:url('https://cdn.example/rkn.jpg')"></a>
            <div class="tgme_widget_message_text">
              Штрафы до 15 млн рублей начал выписывать РКН владельцам сайтов зоны .RU.
              В требовании Роскомнадзора указаны 152-ФЗ, политика конфиденциальности
              и трансграничная передача персональных данных.
            </div>
          </div>
          <div class="tgme_widget_message" data-post="yep_news/27073">
            <div class="tgme_widget_message_text">
              Google should allow third-party search engines access to data, EU says.
              Reuters reports on Brussels and Google.
            </div>
          </div>
        </body></html>
        """
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "news.db"
            processed = tmp_path / "processed"
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
                conn.execute(
                    """
                    INSERT INTO sources(id, name, category, subcategory, url, access_method, is_active, credibility_tier)
                    VALUES(1, 'YEP', 'telegram', 'media', 'https://t.me/yep_news', 'telegram', 1, 'B')
                    """
                )
                conn.commit()
            finally:
                conn.close()

            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "processed_telegram": str(processed),
                "telegram_store_mode": "negative_only",
                "telegram_public_fallback_urls": ["t.me/yep_news"],
            }
            first = telegram_public_fallback.collect_public_fallback(settings, session=FakeSession(html), limit=10)
            second = telegram_public_fallback.collect_public_fallback(settings, session=FakeSession(html), limit=10)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                content = conn.execute("SELECT external_id, title FROM content_items WHERE source_id=1").fetchall()
                votes = conn.execute("SELECT tag_name, signal_layer FROM content_tag_votes ORDER BY tag_name").fetchall()
                reviews = conn.execute("SELECT queue_key, suggested_action FROM review_tasks").fetchall()
                attachments = conn.execute("SELECT attachment_type, mime_type FROM attachments").fetchall()
            finally:
                conn.close()

            self.assertTrue(first["ok"])
            self.assertEqual(first["items_new"], 1)
            self.assertEqual(second["items_new"], 0)
            self.assertEqual([row["external_id"] for row in content], ["27072"])
            self.assertTrue(any(row["tag_name"] == "document/screenshot" for row in votes))
            self.assertIn(("documents", "verify_document_screenshot"), [tuple(row) for row in reviews])
            self.assertEqual([tuple(row) for row in attachments], [("photo", "image/jpeg")])


if __name__ == "__main__":
    unittest.main()
