import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

RSS_FEED_OVERRIDES = {
    "tass.ru": "https://tass.ru/rss/v2.xml",
    "ria.ru": "https://ria.ru/export/rss2/index.xml",
    "rbc.ru": "https://rssexport.rbc.ru/rbcnews/news/30/full.rss",
    "kommersant.ru": "https://www.kommersant.ru/RSS/news.xml",
    "vedomosti.ru": "https://www.vedomosti.ru/rss/news",
    "interfax.ru": "https://www.interfax.ru/rss.asp",
    "meduza.io": "https://meduza.io/rss/all",
    "pnp.ru": "https://www.pnp.ru/rss/index.xml",
    "iz.ru": "https://iz.ru/xml/rss/all.xml",
}


def _feed_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    normalized = url.lower().removeprefix("https://").removeprefix("http://").removeprefix("www.").rstrip("/")
    if normalized in RSS_FEED_OVERRIDES:
        return RSS_FEED_OVERRIDES[normalized]
    if any(x in url for x in ("rss", "atom", "feed", ".xml")):
        return url
    if not url.startswith("http"):
        url = f"https://{url}"
    return url.rstrip("/") + "/rss"


def _parse_feed(url: str, limit: int = 50) -> List[Dict]:
    try:
        import feedparser
        feed = feedparser.parse(url)
        entries = []
        for entry in feed.entries[:limit]:
            published = ""
            for field in ("published", "updated", "created"):
                val = getattr(entry, field, "")
                if val:
                    published = val
                    break

            title = getattr(entry, "title", "")
            summary = getattr(entry, "summary", "")
            link = getattr(entry, "link", "")
            entry_id = getattr(entry, "id", link or title)

            entries.append({
                "title": title,
                "summary": summary,
                "link": link,
                "entry_id": entry_id,
                "published": published,
            })
        return entries
    except Exception as e:
        log.warning("Feed parse failed for %s: %s", url, e)
        return []


def _fetch_full_article(url: str) -> Optional[str]:
    try:
        import requests
        from readability import Document
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200 and len(resp.text) > 200:
            doc = Document(resp.text)
            return doc.summary()
    except Exception as e:
        log.debug("Article fetch failed for %s: %s", url, e)
    return None


def _html_to_plain(html: str) -> str:
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator="\n", strip=True)
    except Exception:
        return re.sub(r'<[^>]+>', ' ', html)


def collect_rss(settings: dict = None, fetch_articles: bool = False, limit: int = 50):
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    sources = conn.execute(
        "SELECT id, name, url FROM sources WHERE category='media' AND is_active=1 AND access_method IN ('rss','atom','rss_atom','')"
    ).fetchall()

    if not sources:
        log.info("No active RSS media sources")
        conn.close()
        return

    try:
        import feedparser
    except ImportError:
        log.warning("feedparser not installed — run: pip install feedparser")
        conn.close()
        return

    total_new = 0
    for src in sources:
        url = src["url"]
        if not url:
            continue

        feed_url = _feed_url(url)
        if not feed_url:
            continue

        entries = _parse_feed(feed_url, limit=limit)
        if not entries:
            log.warning("RSS %s: no entries from %s", src["name"], feed_url)
            continue

        for entry in entries:
            ext_id = hashlib.sha256((entry["entry_id"] or entry["link"]).encode()).hexdigest()[:32]

            existing = conn.execute(
                "SELECT id FROM raw_source_items WHERE source_id=? AND external_id=?",
                (src["id"], ext_id),
            ).fetchone()
            if existing:
                continue

            body = entry["summary"] or ""
            if fetch_articles and entry["link"]:
                article_html = _fetch_full_article(entry["link"])
                if article_html:
                    body = _html_to_plain(article_html)

            raw_json = json.dumps(entry, ensure_ascii=False)
            raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()

            cur = conn.execute(
                """INSERT INTO raw_source_items(source_id, external_id, raw_payload, collected_at, hash_sha256, is_processed)
                   VALUES(?,?,?,?,?,1)""",
                (src["id"], ext_id, raw_json, datetime.now().isoformat(), raw_hash),
            )
            raw_id = cur.lastrowid

            conn.execute(
                """INSERT INTO content_items(source_id, raw_item_id, external_id, content_type, title, body_text, published_at, collected_at, url, status)
                   VALUES(?,?,?,?,?,?,?,?,?,'raw_signal')""",
                (src["id"], raw_id, ext_id, "article", entry["title"], body[:50000], entry["published"], datetime.now().isoformat(), entry["link"]),
            )

            total_new += 1

        conn.commit()
        log.info("RSS %s: %d entries", src["name"], len(entries))

    conn.execute(
        "UPDATE sources SET last_checked_at=? WHERE category='media'",
        (datetime.now().isoformat(),),
    )
    conn.commit()

    log.info("RSS collection done: %d new articles", total_new)
    conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    collect_rss()


if __name__ == "__main__":
    main()
