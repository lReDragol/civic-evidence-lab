import argparse
import csv
import hashlib
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup, Tag
from news_tagging import infer_tags, retag_rows


NEWS_KEYWORDS = [
    "президент",
    "правитель",
    "министр",
    "госдум",
    "парламент",
    "закон",
    "санкц",
    "инфляц",
    "курс",
    "доллар",
    "евро",
    "рубл",
    "эконом",
    "бирж",
    "рынк",
    "акци",
    "банк",
    "крипт",
    "биткоин",
    "переговор",
    "договор",
    "подписал",
    "подписали",
    "заявил",
    "заявили",
    "штраф",
    "блокиров",
    "суд",
    "прокуратур",
    "арест",
    "задерж",
    "авари",
    "пожар",
    "взрыв",
    "теракт",
    "погиб",
    "умер",
    "войн",
    "обстрел",
    "мобилиз",
    "роскомнадзор",
    "ркн",
    "фас",
    "сша",
    "росси",
    "украин",
    "белорус",
    "ес",
    "евросоюз",
    "китай",
    "telegram",
    "twitch",
    "discord",
    "youtube",
    "google",
    "roblox",
]

NON_NEWS_CHAT = [
    "подруб",
    "стрим",
    "конфа",
    "всем хорошего дня",
    "сегодня без основы",
    "пошел спать",
]

NEWS_DOMAINS = [
    "rbc.ru",
    "tass.ru",
    "ria.ru",
    "lenta.ru",
    "interfax",
    "kommersant",
    "vedomosti",
    "meduza",
    "reuters",
    "bbc",
    "dw.com",
    "bloomberg",
    "wsj",
    "ft.com",
]

AD_MARKERS = [
    "реклама. рекламодатель",
    "erid:",
    "партнерский материал",
    "партнерский пост",
    "winline",
    "букмекер",
    "промокод",
]


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("ё", "е").lower()).strip()


def natural_key(name: str):
    return [int(p) if p.isdigit() else p.lower() for p in re.split(r"(\d+)", name)]


def parse_datetime(raw: str):
    m = re.search(r"(\d{2}\.\d{2}\.\d{4}) (\d{2}:\d{2}:\d{2})", raw or "")
    if not m:
        return None
    return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%d.%m.%Y %H:%M:%S")


def first_sentence(text: str, limit: int = 220) -> str:
    txt = re.sub(r"\s+", " ", (text or "")).strip()
    if not txt:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", txt)
    head = (parts[0] if parts else txt).strip(" -")
    if len(head) <= limit:
        return head
    return head[: limit - 3].rstrip() + "..."


def detect_tags(text: str) -> List[str]:
    return infer_tags(text)


def is_ad(text: str) -> bool:
    t = norm(text)
    if any(k in t for k in AD_MARKERS):
        return True
    if "розыгрыш" in t and ("для участия" in t or "кнопк" in t or "нажм" in t):
        return True
    if ("рефераль" in t or "рефк" in t) and ("ссылка" in t or "код" in t):
        return True
    return False


def score_message(text: str, links: List[str], photo_count: int) -> Tuple[int, str]:
    t = norm(text)
    if not t and photo_count == 0:
        return -5, "empty"
    if is_ad(text):
        return -4, "ad"

    news_hits = sum(1 for k in NEWS_KEYWORDS if k in t)
    non_hits = sum(1 for k in NON_NEWS_CHAT if k in t)
    has_news_link = any(any(d in (ln or "").lower() for d in NEWS_DOMAINS) for ln in links)
    length_bonus = 1 if len(t) >= 80 else 0
    photo_bonus = 1 if photo_count > 0 else 0

    score = news_hits + length_bonus + photo_bonus + (2 if has_news_link else 0) - non_hits
    if score >= 2:
        return score, "news_like"
    if score <= -1:
        return score, "low_signal"
    return score, "review"


def parse_export_messages(export_dir: Path) -> List[dict]:
    files = sorted(export_dir.glob("messages*.html"), key=lambda p: natural_key(p.name))
    rows = []
    seq = 0

    for f in files:
        with open(f, encoding="utf-8") as fh:
            soup = BeautifulSoup(fh, "html.parser")
        for msg in soup.select("div.message.default"):
            seq += 1
            msg_id = msg.get("id", "")
            date_div = msg.select_one("div.pull_right.date.details")
            dt_raw = date_div.get("title", "") if date_div else ""
            dt = parse_datetime(dt_raw)

            text_div = msg.select_one("div.text")
            text = text_div.get_text(" ", strip=True) if text_div else ""
            links = [a.get("href") for a in msg.select("div.text a[href]") if a.get("href")]
            photos = [a.get("href") for a in msg.select("a.photo_wrap[href]") if a.get("href")]

            score, reason = score_message(text, links, len(photos))
            auto_decision = "keep" if score >= 0 else "drop"
            if reason == "review":
                auto_decision = "keep"

            rows.append(
                {
                    "row_id": seq,
                    "source_file": f.name,
                    "message_id": msg_id,
                    "datetime_raw": dt_raw,
                    "date": dt.strftime("%Y-%m-%d") if dt else "",
                    "time": dt.strftime("%H:%M:%S") if dt else "",
                    "text": text,
                    "headline": first_sentence(text),
                    "links": "; ".join(links),
                    "photo_count": len(photos),
                    "photos": "; ".join(photos),
                    "has_video": 1 if msg.select("div.media_video") else 0,
                    "auto_score": score,
                    "auto_reason": reason,
                    "auto_decision": auto_decision,
                }
            )
    return rows


def write_csv(path: Path, rows: List[dict]):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def prepare(export_dir: Path):
    rows = parse_export_messages(export_dir)
    review_dir = export_dir / "review_output"
    review_dir.mkdir(parents=True, exist_ok=True)

    write_csv(review_dir / "prepared_messages.csv", rows)

    candidate_rows = [r for r in rows if r["auto_score"] >= 0 or r["photo_count"] > 0]
    write_csv(review_dir / "review_news_with_photos.csv", candidate_rows)

    decisions = []
    for r in candidate_rows:
        decisions.append(
            {
                "row_id": r["row_id"],
                "message_id": r["message_id"],
                "source_file": r["source_file"],
                "datetime_raw": r["datetime_raw"],
                "decision": r["auto_decision"],  # keep/drop; можно редактировать
                "note": "",
            }
        )
    write_csv(review_dir / "decisions.csv", decisions)

    print(f"[prepare] export={export_dir}")
    print(f"[prepare] total_messages={len(rows)}")
    print(f"[prepare] review_candidates={len(candidate_rows)}")
    print(f"[prepare] files: {review_dir}")


def read_prepared(export_dir: Path) -> List[dict]:
    p = export_dir / "review_output" / "prepared_messages.csv"
    if not p.exists():
        raise FileNotFoundError(f"Не найден {p}. Сначала запусти prepare.")
    return list(csv.DictReader(open(p, encoding="utf-8-sig")))


def read_decisions(export_dir: Path) -> Dict[str, str]:
    p = export_dir / "review_output" / "decisions.csv"
    if not p.exists():
        return {}
    out = {}
    for r in csv.DictReader(open(p, encoding="utf-8-sig")):
        out[str(r["row_id"])] = (r.get("decision", "") or "").strip().lower()
    return out


def build_calendar_md(keep_rows: List[dict]) -> str:
    by_date = defaultdict(list)
    for r in keep_rows:
        by_date[r["date"]].append(r)

    lines = ["# Календарь новостей", ""]
    for d in sorted(by_date):
        day_rows = by_date[d]
        tag_count = Counter()
        for r in day_rows:
            for t in (r.get("tags", "") or "").split(","):
                t = t.strip()
                if t:
                    tag_count[t] += 1
        top_tags = ", ".join(x[0] for x in tag_count.most_common(3)) if tag_count else "Прочее"
        lines.append(f"## {d}")
        lines.append(f"1. Событий: {len(day_rows)}")
        lines.append(f"2. Темы: {top_tags}")
        shown = 0
        seen = set()
        for r in day_rows:
            h = (r.get("headline") or "").strip()
            if not h:
                continue
            k = norm(h)
            if k in seen:
                continue
            seen.add(k)
            shown += 1
            lines.append(f"{shown + 2}. {h}")
            if shown >= 3:
                break
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def apply_source_cleanup(export_dir: Path, keep_rows: List[dict]):
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = export_dir / f"backup_before_source_cleanup_{stamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)

    keep_by_file = defaultdict(set)
    keep_photo_names = set()
    for r in keep_rows:
        keep_by_file[r["source_file"]].add(r["message_id"])
        for p in [x.strip() for x in (r.get("kept_photos", "") or "").split(";") if x.strip()]:
            name = os.path.basename(p)
            keep_photo_names.add(name)
            if name.endswith(".jpg"):
                keep_photo_names.add(name[:-4] + "_thumb.jpg")

    # HTML
    for html_name, keep_ids in keep_by_file.items():
        html_path = export_dir / html_name
        if not html_path.exists():
            continue
        backup_path = backup_dir / html_name
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(html_path, backup_path)

        soup = BeautifulSoup(open(html_path, encoding="utf-8"), "html.parser")
        history = soup.select_one("div.history")
        if history is None:
            continue

        new_nodes = []
        pending_service = []
        for child in list(history.children):
            if not isinstance(child, Tag):
                continue
            classes = child.get("class", [])
            cid = child.get("id", "")
            if child.name == "a" and "pagination" in classes:
                new_nodes.append(child)
                continue
            if child.name == "div" and "message" in classes:
                if "service" in classes:
                    pending_service.append(child)
                    continue
                if cid in keep_ids:
                    if pending_service:
                        new_nodes.extend(pending_service)
                        pending_service = []
                    new_nodes.append(child)

        history.clear()
        for n in new_nodes:
            history.append(n)
            history.append("\n")

        with open(html_path, "w", encoding="utf-8") as f:
            f.write(str(soup))

    # Photos
    photos_dir = export_dir / "photos"
    removed_dir = backup_dir / "photos_removed"
    removed_dir.mkdir(parents=True, exist_ok=True)
    if photos_dir.exists():
        for p in photos_dir.iterdir():
            if not p.is_file():
                continue
            if p.name not in keep_photo_names:
                shutil.move(str(p), str(removed_dir / p.name))

    return backup_dir


def finalize(export_dir: Path, apply_source: bool = False):
    prepared = read_prepared(export_dir)
    decisions = read_decisions(export_dir)

    keep_rows = []
    removed_rows = []

    for r in prepared:
        decision = decisions.get(str(r["row_id"]), (r.get("auto_decision") or "keep")).lower()
        keep = decision in {"keep", "news", "ok", "1", "yes", "y"}

        photos = [x.strip() for x in (r.get("photos") or "").split(";") if x.strip()]
        headline = (r.get("headline") or "").strip()
        if not headline and photos:
            headline = f"Новость в изображении ({os.path.basename(photos[0])})"
        tags = detect_tags(f"{headline}\n{r.get('text','')}")

        out = {
            "source_file": r["source_file"],
            "message_id": r["message_id"],
            "date": r["date"],
            "time": r["time"],
            "datetime_raw": r["datetime_raw"],
            "headline": headline,
            "text": r.get("text", ""),
            "links": r.get("links", ""),
            "kept_photos": "; ".join(photos),
            "tags": ", ".join(tags),
            "decision": "keep" if keep else "drop",
            "auto_reason": r.get("auto_reason", ""),
        }
        if keep:
            keep_rows.append(out)
        else:
            removed_rows.append(out)

    out_dir = export_dir / "news_output"
    out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(out_dir / "news_messages.csv", keep_rows)
    write_csv(out_dir / "removed_messages.csv", removed_rows)
    with open(out_dir / "news_calendar.md", "w", encoding="utf-8") as f:
        f.write(build_calendar_md(keep_rows))
    with open(out_dir / "summary.txt", "w", encoding="utf-8") as f:
        f.write(f"Всего сообщений в исходнике: {len(prepared)}\n")
        f.write(f"Оставлено сообщений (новости): {len(keep_rows)}\n")
        f.write(f"Удалено как мусор/реклама: {len(removed_rows)}\n")

    backup_dir = None
    if apply_source:
        backup_dir = apply_source_cleanup(export_dir, keep_rows)

    print(f"[finalize] export={export_dir}")
    print(f"[finalize] keep={len(keep_rows)} drop={len(removed_rows)}")
    print(f"[finalize] output={out_dir}")
    if backup_dir:
        print(f"[finalize] backup={backup_dir}")


def init_db(conn: sqlite3.Connection):
    conn.executescript(
        """
        PRAGMA journal_mode=DELETE;
        CREATE TABLE IF NOT EXISTS exports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            export_dir TEXT UNIQUE NOT NULL,
            export_name TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            total_messages INTEGER DEFAULT 0,
            kept_messages INTEGER DEFAULT 0,
            removed_messages INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            export_id INTEGER NOT NULL,
            source_file TEXT,
            message_id TEXT,
            date TEXT,
            time TEXT,
            datetime_raw TEXT,
            headline TEXT,
            text TEXT,
            links TEXT,
            tags TEXT,
            decision TEXT,
            kept_photos TEXT,
            UNIQUE(export_id, message_id),
            FOREIGN KEY(export_id) REFERENCES exports(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_db_id INTEGER NOT NULL,
            photo_rel_path TEXT NOT NULL,
            photo_basename TEXT,
            exists_on_disk INTEGER DEFAULT 0,
            image_blob BLOB,
            image_ext TEXT,
            image_size INTEGER DEFAULT 0,
            image_sha256 TEXT,
            UNIQUE(message_db_id, photo_rel_path),
            FOREIGN KEY(message_db_id) REFERENCES messages(id) ON DELETE CASCADE
        );
        """
    )
    ensure_photo_columns(conn)
    conn.commit()


def ensure_photo_columns(conn: sqlite3.Connection):
    existing = {row[1] for row in conn.execute("PRAGMA table_info(photos)")}
    alter_sql = []
    if "image_blob" not in existing:
        alter_sql.append("ALTER TABLE photos ADD COLUMN image_blob BLOB")
    if "image_ext" not in existing:
        alter_sql.append("ALTER TABLE photos ADD COLUMN image_ext TEXT")
    if "image_size" not in existing:
        alter_sql.append("ALTER TABLE photos ADD COLUMN image_size INTEGER DEFAULT 0")
    if "image_sha256" not in existing:
        alter_sql.append("ALTER TABLE photos ADD COLUMN image_sha256 TEXT")
    for sql in alter_sql:
        conn.execute(sql)
    conn.commit()


def resolve_photo_file(export_dir: Path, rel_path: str):
    p1 = export_dir / rel_path
    if p1.exists():
        return p1
    p2 = export_dir / "news_output" / "photos" / os.path.basename(rel_path)
    if p2.exists():
        return p2
    return None


def load_photo_blob(path: Path):
    if not path or not path.exists() or path.is_dir():
        return None, "", 0, ""
    data = path.read_bytes()
    ext = path.suffix.lower().lstrip(".")
    if not ext:
        mime, _ = mimetypes.guess_type(path.name)
        ext = (mime.split("/")[-1] if mime else "") or "bin"
    sha = hashlib.sha256(data).hexdigest()
    return data, ext, len(data), sha


def load_news_csv(csv_path: Path) -> List[dict]:
    if not csv_path.exists():
        return []
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8-sig")))
    for r in rows:
        if "decision" not in r:
            r["decision"] = "keep"
        if "tags" not in r or not r["tags"]:
            r["tags"] = ", ".join(detect_tags(f"{r.get('headline','')}\n{r.get('text','')}"))
    return rows


def upsert_export(conn: sqlite3.Connection, export_dir: Path, rows: List[dict]):
    exp_dir = str(export_dir.resolve())
    exp_name = export_dir.name
    total = len(rows)
    kept = sum(1 for r in rows if (r.get("decision", "keep") or "keep") == "keep")
    removed = total - kept

    conn.execute(
        """
        INSERT INTO exports(export_dir, export_name, updated_at, total_messages, kept_messages, removed_messages)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(export_dir) DO UPDATE SET
            export_name=excluded.export_name,
            updated_at=excluded.updated_at,
            total_messages=excluded.total_messages,
            kept_messages=excluded.kept_messages,
            removed_messages=excluded.removed_messages
        """,
        (exp_dir, exp_name, now_iso(), total, kept, removed),
    )
    exp_id = conn.execute("SELECT id FROM exports WHERE export_dir=?", (exp_dir,)).fetchone()[0]
    return exp_id


def sync_one_export_to_db(conn: sqlite3.Connection, export_dir: Path):
    csv_path = export_dir / "news_output" / "news_messages.csv"
    rows = load_news_csv(csv_path)
    if not rows:
        return 0

    exp_id = upsert_export(conn, export_dir, rows)
    conn.execute("DELETE FROM photos WHERE message_db_id IN (SELECT id FROM messages WHERE export_id=?)", (exp_id,))
    conn.execute("DELETE FROM messages WHERE export_id=?", (exp_id,))

    for r in rows:
        cur = conn.execute(
            """
            INSERT INTO messages(
                export_id, source_file, message_id, date, time, datetime_raw,
                headline, text, links, tags, decision, kept_photos
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                exp_id,
                r.get("source_file", ""),
                r.get("message_id", ""),
                r.get("date", ""),
                r.get("time", ""),
                r.get("datetime_raw", ""),
                r.get("headline", ""),
                r.get("text", ""),
                r.get("links", ""),
                r.get("tags", ""),
                r.get("decision", "keep"),
                r.get("kept_photos", ""),
            ),
        )
        msg_db_id = cur.lastrowid
        photos = [x.strip() for x in (r.get("kept_photos") or "").split(";") if x.strip()]
        for p in photos:
            file_path = resolve_photo_file(export_dir, p)
            exists = int(file_path is not None and file_path.exists() and file_path.is_file())
            blob, ext, size, sha = load_photo_blob(file_path) if exists else (None, "", 0, "")
            conn.execute(
                """
                INSERT INTO photos(
                    message_db_id, photo_rel_path, photo_basename, exists_on_disk,
                    image_blob, image_ext, image_size, image_sha256
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (msg_db_id, p, os.path.basename(p), exists, blob, ext, size, sha),
            )
    retag_messages(conn, export_id=exp_id)
    conn.commit()
    return len(rows)


def retag_messages(conn: sqlite3.Connection, export_id=None) -> int:
    sql = """
        SELECT id, date, time, message_id, headline, text
        FROM messages
        WHERE COALESCE(decision, 'keep') = 'keep'
    """
    params = []
    if export_id is not None:
        sql += " AND export_id=?"
        params.append(export_id)
    sql += " ORDER BY date, time, id"

    rows = [
        {
            "id": row[0],
            "date": row[1] or "",
            "time": row[2] or "",
            "message_id": row[3] or "",
            "headline": row[4] or "",
            "text": row[5] or "",
        }
        for row in conn.execute(sql, params)
    ]
    updates = retag_rows(rows)
    if not updates:
        return 0

    conn.executemany(
        "UPDATE messages SET tags=? WHERE id=?",
        [(tags, msg_id) for msg_id, tags in updates],
    )
    return len(updates)


def discover_exports_with_news(root: Path) -> List[Path]:
    found = []
    for dirpath, dirnames, filenames in os.walk(root):
        if "news_output" in dirnames:
            candidate = Path(dirpath)
            if (candidate / "news_output" / "news_messages.csv").exists():
                found.append(candidate)
    # keep shortest paths first
    found = sorted(set(found), key=lambda p: (len(str(p)), str(p).lower()))
    return found


def sync_db(root: Path, db_path: Path):
    conn = sqlite3.connect(db_path)
    init_db(conn)
    exports = discover_exports_with_news(root)
    total = 0
    for exp in exports:
        n = sync_one_export_to_db(conn, exp)
        if n:
            print(f"[sync-db] {exp} -> {n} rows")
            total += n
    conn.close()
    print(f"[sync-db] db={db_path} total_rows={total} exports={len(exports)}")


def prune_root(root: Path, db_path: Path):
    keep = {
        "news_unified_pipeline.py",
        "news_calendar_pyside6.py",
        "news_tagging.py",
        db_path.name,
    }

    removed_files = 0
    removed_dirs = 0
    for item in root.iterdir():
        if item.name in keep:
            continue
        if item.is_file():
            item.unlink()
            removed_files += 1
        elif item.is_dir():
            shutil.rmtree(item)
            removed_dirs += 1
    print(f"[prune-root] removed_files={removed_files} removed_dirs={removed_dirs}")


def launch_calendar(db_path: Path) -> int:
    script_dir = Path(__file__).resolve().parent
    calendar_script = script_dir / "news_calendar_pyside6.py"
    if not calendar_script.exists():
        print(f"[run] calendar script not found: {calendar_script}")
        return 1

    db_abs = db_path.resolve()
    cmd = [sys.executable, str(calendar_script), "--db", str(db_abs)]
    print(f"[run] launch calendar: {' '.join(cmd)}")
    return subprocess.call(cmd)


def main():
    parser = argparse.ArgumentParser(description="Pipeline: экспорт Telegram -> review -> clean -> unified DB")
    sub = parser.add_subparsers(dest="cmd")

    p_run = sub.add_parser("run", help="Режим по умолчанию: обновить БД и открыть календарь")
    p_run.add_argument("--root", default=".", help="Корень, где лежат экспорты (по умолчанию текущая папка)")
    p_run.add_argument("--db", default="news_unified.db", help="Путь к SQLite БД")
    p_run.add_argument("--no-sync", action="store_true", help="Не обновлять БД перед запуском календаря")

    p_prepare = sub.add_parser("prepare", help="Собрать удобные review-файлы из экспорта")
    p_prepare.add_argument("--export-dir", required=True, help="Папка экспорта (где messages*.html)")

    p_finalize = sub.add_parser("finalize", help="Применить decisions, собрать news_output и опционально почистить исходник")
    p_finalize.add_argument("--export-dir", required=True)
    p_finalize.add_argument("--apply-source", action="store_true", help="Перезаписать messages*.html и photos с бэкапом")

    p_sync = sub.add_parser("sync-db", help="Собрать все news_output в одну SQLite БД")
    p_sync.add_argument("--root", default=".", help="Корень, где лежат экспорты")
    p_sync.add_argument("--db", default="news_unified.db", help="Путь к SQLite БД")

    p_retag = sub.add_parser("retag-db", help="Пересчитать теги в существующей SQLite БД")
    p_retag.add_argument("--db", default="news_unified.db", help="Путь к SQLite БД")

    p_full = sub.add_parser("full", help="prepare + finalize + sync-db для одного экспорта")
    p_full.add_argument("--export-dir", required=True)
    p_full.add_argument("--root", default=".")
    p_full.add_argument("--db", default="news_unified.db")
    p_full.add_argument("--apply-source", action="store_true")

    p_calendar = sub.add_parser("calendar", help="Открыть календарь из существующей БД")
    p_calendar.add_argument("--db", default="news_unified.db", help="Путь к SQLite БД")

    args = parser.parse_args()
    if not args.cmd:
        args.cmd = "run"
        args.root = "."
        args.db = "news_unified.db"
        args.no_sync = False

    if args.cmd == "run":
        script_dir = Path(__file__).resolve().parent
        root = Path(args.root).resolve()
        if args.root == "." and not discover_exports_with_news(root):
            script_db = (script_dir / args.db).resolve()
            if script_db.exists() or discover_exports_with_news(script_dir):
                root = script_dir

        db_path = Path(args.db)
        if not db_path.is_absolute():
            db_path = (root / db_path).resolve()

        if not args.no_sync:
            sync_db(root, db_path)
        exit_code = launch_calendar(db_path)
        if exit_code != 0:
            raise SystemExit(exit_code)
        return
    if args.cmd == "prepare":
        prepare(Path(args.export_dir))
        return
    if args.cmd == "finalize":
        finalize(Path(args.export_dir), apply_source=args.apply_source)
        return
    if args.cmd == "sync-db":
        sync_db(Path(args.root), Path(args.db))
        return
    if args.cmd == "retag-db":
        db_path = Path(args.db)
        conn = sqlite3.connect(db_path)
        updated = retag_messages(conn)
        conn.commit()
        conn.close()
        print(f"[retag-db] db={db_path} updated={updated}")
        return
    if args.cmd == "full":
        exp = Path(args.export_dir)
        prepare(exp)
        finalize(exp, apply_source=args.apply_source)
        sync_db(Path(args.root), Path(args.db))
        return
    if args.cmd == "calendar":
        script_dir = Path(__file__).resolve().parent
        db_path = Path(args.db)
        if not db_path.is_absolute():
            db_path = db_path.resolve()
            if not db_path.exists():
                alt = (script_dir / args.db).resolve()
                if alt.exists():
                    db_path = alt
        exit_code = launch_calendar(db_path)
        if exit_code != 0:
            raise SystemExit(exit_code)
        return


if __name__ == "__main__":
    main()
