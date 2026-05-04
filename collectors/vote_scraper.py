import json
import logging
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

BASE_URL = "https://vote.duma.gov.ru"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}


FACTION_MAP = {
    "72100024": "ЕР",
    "72100004": "КПРФ",
    "72100008": "ЛДПР",
    "72100019": "СР",
    "72100028": "НЛ",
    "72100012": "Родина",
    "72100032": "Партия роста",
    "72100036": "Гражданская платформа",
    "0": "Без фракции",
}

RESULT_MAP = {
    "for": "за",
    "against": "против",
    "abstained": "воздержался",
    "absent": "не голосовал",
    "no_vote": "не голосовал",
    "none": "отсутствовал",
}


def _parse_deputies_data(html_text: str) -> List[Dict]:
    match = re.search(r'deputiesData\s*=\s*(\[.*?\]);', html_text, re.DOTALL)
    if not match:
        return []
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    deputies = []
    for d in data:
        name = d.get("sortName", "")
        faction = d.get("faction", "")
        result_raw = d.get("result", "")
        result = RESULT_MAP.get(result_raw, result_raw)
        url = d.get("url", "")
        duma_id = None
        id_m = re.search(r'deputy=(\d+)', url)
        if id_m:
            duma_id = int(id_m.group(1))
        deputies.append({
            "name": name,
            "faction": faction,
            "result": result,
            "duma_id": duma_id,
        })
    return deputies


def _session():
    import requests
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def _parse_vote_date(value: str | None) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except ValueError:
            continue
    match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
    if match:
        try:
            return date(int(match.group(3)), int(match.group(2)), int(match.group(1)))
        except ValueError:
            return None
    return None


def _vote_external_id(vote_url: str | None) -> str:
    match = re.search(r"/vote/(\d+)", str(vote_url or ""))
    return match.group(1) if match else ""


def _table_columns(conn, table_name: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
    except Exception:
        return set()


def _ensure_vote_schema(conn) -> None:
    columns = _table_columns(conn, "bill_vote_sessions")
    if "external_vote_id" not in columns:
        conn.execute("ALTER TABLE bill_vote_sessions ADD COLUMN external_vote_id TEXT")
    if "source_url" not in columns:
        conn.execute("ALTER TABLE bill_vote_sessions ADD COLUMN source_url TEXT")
    if "updated_at" not in columns:
        conn.execute("ALTER TABLE bill_vote_sessions ADD COLUMN updated_at TEXT")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_bill_vote_sessions_external_vote_id
        ON bill_vote_sessions(external_vote_id)
        WHERE external_vote_id IS NOT NULL AND external_vote_id != ''
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bill_vote_sessions_source_url ON bill_vote_sessions(source_url)")
    vote_columns = _table_columns(conn, "bill_votes")
    if "external_vote_id" not in vote_columns:
        conn.execute("ALTER TABLE bill_votes ADD COLUMN external_vote_id TEXT")
    if "source_url" not in vote_columns:
        conn.execute("ALTER TABLE bill_votes ADD COLUMN source_url TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bill_votes_external_vote_id ON bill_votes(external_vote_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bill_votes_source_url ON bill_votes(source_url)")


def _ensure_source(conn, name: str, url: str, category: str, credibility_tier: str = "A"):
    row = conn.execute("SELECT id FROM sources WHERE url=? AND category=?", (url, category)).fetchone()
    if row:
        conn.execute("UPDATE sources SET last_checked_at=datetime('now') WHERE id=?", (row[0],))
        return row[0]
    cur = conn.execute(
        "INSERT INTO sources(name, category, url, is_official, credibility_tier, is_active) VALUES(?,?,?,?,?,1)",
        (name, category, url, 1, credibility_tier),
    )
    return cur.lastrowid


def _find_entity_by_name(conn, name: str) -> Optional[int]:
    if not name or len(name) < 3:
        return None
    row = conn.execute(
        "SELECT entity_id FROM entity_aliases WHERE alias LIKE ? LIMIT 1",
        (f"{name[:20]}%",),
    ).fetchone()
    if row:
        return row[0]
    row = conn.execute(
        "SELECT id FROM entities WHERE entity_type='person' AND canonical_name LIKE ? LIMIT 1",
        (f"%{name[:20]}%",),
    ).fetchone()
    if row:
        return row[0]
    return None


def _find_bill_by_number(conn, vote_subject: str) -> Optional[int]:
    bill_number = ""
    for m in re.finditer(r"№\s*(\d{5,}[-–]\d+[-–]\d+|\d{5,})", vote_subject):
        bill_number = m.group(1).replace("–", "-")
        break
    if not bill_number:
        return None
    row = conn.execute("SELECT id FROM bills WHERE number=?", (bill_number,)).fetchone()
    return row[0] if row else None


def _scrape_vote_list_result(session, page: int = 1, convocation: str = "VIII") -> tuple[List[Dict], str | None]:
    from bs4 import BeautifulSoup
    votes = []
    try:
        params = {}
        if page > 1:
            params["page"] = page
        r = session.get(f"{BASE_URL}/", params=params, timeout=20)
        if r.status_code != 200:
            return votes, f"http_status:{r.status_code}"

        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a[href*='/vote/']"):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if not href or not text or len(text) < 10:
                continue
            vote_id_m = re.search(r"/vote/(\d+)", href)
            if not vote_id_m:
                continue
            vote_id = vote_id_m.group(1)
            stage = ""
            stage_m = re.match(r"\(([^)]+)\)", text)
            if stage_m:
                stage = stage_m.group(1)
                text = re.sub(r"^\([^)]+\)\s*", "", text)

            bill_number = ""
            for m in re.finditer(r"№\s*(\d{5,}[-–]\d+[-–]\d+|\d{5,})", text):
                bill_number = m.group(1).replace("–", "-")
                break

            votes.append({
                "vote_id": vote_id,
                "subject": text,
                "stage": stage,
                "bill_number": bill_number,
                "url": f"{BASE_URL}/vote/{vote_id}",
            })

    except Exception as e:
        log.warning("Vote list page %d failed: %s", page, e)
        return votes, f"{type(e).__name__}:{e}"
    return votes, None


def scrape_vote_list(session, page: int = 1, convocation: str = "VIII") -> List[Dict]:
    votes, _error = _scrape_vote_list_result(session, page=page, convocation=convocation)
    return votes


def scrape_vote_detail(session, vote_url: str) -> Dict:
    from bs4 import BeautifulSoup
    detail = {"url": vote_url, "external_vote_id": _vote_external_id(vote_url)}
    try:
        r = session.get(vote_url, timeout=20)
        if r.status_code != 200:
            return detail
        soup = BeautifulSoup(r.text, "lxml")

        h1 = soup.select_one("h1")
        if h1:
            detail["subject"] = h1.get_text(strip=True)

        date_el = soup.select_one(".date-p")
        if date_el:
            date_text = date_el.get_text(strip=True)
            date_m = re.search(r"(\d{2}\.\d{2}\.\d{4})", date_text)
            detail["vote_date"] = date_m.group(1) if date_m else date_text[:20]

        stat_el = soup.select_one(".statis")
        if stat_el:
            stat_text = stat_el.get_text(strip=True)
            for m in re.finditer(r"За:\s*(\d+)", stat_text):
                detail["total_for"] = int(m.group(1))
            for m in re.finditer(r"Против:\s*(\d+)", stat_text):
                detail["total_against"] = int(m.group(1))
            for m in re.finditer(r"Воздержалось:\s*(\d+)", stat_text):
                detail["total_abstained"] = int(m.group(1))
            for m in re.finditer(r"Не голосовало:\s*(\d+)", stat_text):
                detail["total_absent"] = int(m.group(1))
            for m in re.finditer(r"Кворум:\s*(\d+)", stat_text):
                detail["quorum"] = int(m.group(1))

        faction_results = []
        for box in soup.select(".e-pers-box"):
            frac_el = box.select_one(".ld-frac-n")
            fname = frac_el.get_text(strip=True) if frac_el else ""
            if not fname:
                continue
            fr = {"faction": fname}
            for cls_name, label in [("ep-green","za"),("ep-red","protiv"),("ep-blue","vozderzhan"),("ep-gray","otsutstvoval")]:
                el = box.select_one("." + cls_name)
                if el:
                    val_text = el.get_text(strip=True)
                    num_m = re.search(r"(\d+)\s*гол", val_text)
                    fr[label] = int(num_m.group(1)) if num_m else 0
                else:
                    fr[label] = 0
            faction_results.append(fr)
        detail["faction_results"] = faction_results

        bill_number = ""
        for m in re.finditer(r"№\s*(\d{5,}[-–]\d+[-–]\d+|\d{5,})", detail.get("subject", "")):
            bill_number = m.group(1).replace("–", "-")
            break
        detail["bill_number"] = bill_number

        stage = ""
        if detail.get("subject"):
            stage_m = re.search(r"\(([^)]*(?:чтени|поправк|основу|целом)[^)]*)\)", detail["subject"])
            if stage_m:
                stage = stage_m.group(1)
        detail["stage"] = stage

        result = ""
        stat_text = stat_el.get_text(strip=True) if stat_el else ""
        if "Принят" in stat_text:
            result = "принят"
        elif "Отклонен" in stat_text:
            result = "отклонен"
        detail["result"] = result

        individual = _parse_deputies_data(r.text)
        if individual:
            detail["individual_votes"] = individual

    except Exception as e:
        log.warning("Vote detail failed for %s: %s", vote_url, e)
    return detail


def store_vote_session(conn, bill_id: Optional[int], vote_data: Dict, *, return_stats: bool = False):
    _ensure_vote_schema(conn)
    vote_date = vote_data.get("vote_date", "")
    stage = vote_data.get("stage", "")
    url = vote_data.get("url", "")
    external_vote_id = str(vote_data.get("external_vote_id") or _vote_external_id(url) or "").strip()

    existing = None
    if external_vote_id:
        existing = conn.execute(
            "SELECT id FROM bill_vote_sessions WHERE external_vote_id=?",
            (external_vote_id,),
        ).fetchone()
    if not existing and url:
        existing = conn.execute(
            "SELECT id FROM bill_vote_sessions WHERE source_url=?",
            (url,),
        ).fetchone()
    if not existing and bill_id:
        existing = conn.execute(
            "SELECT id FROM bill_vote_sessions WHERE bill_id=? AND vote_date=? AND vote_stage=?",
            (bill_id, vote_date, stage),
        ).fetchone()

    if not existing and not bill_id:
        existing = conn.execute(
            "SELECT id FROM bill_vote_sessions WHERE vote_date=? AND vote_stage=? AND bill_id IS NULL",
            (vote_date, stage),
        ).fetchone()

    total_present = vote_data.get("total_for", 0) + vote_data.get("total_against", 0) + vote_data.get("total_abstained", 0)
    raw_data = json.dumps(vote_data, ensure_ascii=False, default=str)
    if existing:
        vs_id = int(existing[0])
        conn.execute(
            """
            UPDATE bill_vote_sessions
            SET bill_id=COALESCE(?, bill_id),
                vote_date=COALESCE(NULLIF(?, ''), vote_date),
                vote_stage=COALESCE(NULLIF(?, ''), vote_stage),
                total_for=?, total_against=?, total_abstained=?, total_absent=?,
                total_present=?, result=?, raw_data=?, external_vote_id=COALESCE(NULLIF(?, ''), external_vote_id),
                source_url=COALESCE(NULLIF(?, ''), source_url), updated_at=?
            WHERE id=?
            """,
            (
                bill_id,
                vote_date,
                stage,
                vote_data.get("total_for", 0),
                vote_data.get("total_against", 0),
                vote_data.get("total_abstained", 0),
                vote_data.get("total_absent", 0),
                total_present,
                vote_data.get("result", ""),
                raw_data,
                external_vote_id,
                url,
                datetime.now().isoformat(),
                vs_id,
            ),
        )
        created = False
    else:
        cur = conn.execute(
            "INSERT INTO bill_vote_sessions(bill_id, vote_date, vote_stage, total_for, total_against, "
            "total_abstained, total_absent, total_present, result, raw_data, external_vote_id, source_url, updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (bill_id, vote_date, stage,
             vote_data.get("total_for", 0), vote_data.get("total_against", 0),
             vote_data.get("total_abstained", 0), vote_data.get("total_absent", 0),
             total_present, vote_data.get("result", ""),
             raw_data, external_vote_id, url, datetime.now().isoformat()),
        )
        vs_id = cur.lastrowid
        created = True

    votes_written = 0
    if vote_data.get("faction_results") or vote_data.get("individual_votes"):
        conn.execute("DELETE FROM bill_votes WHERE vote_session_id=?", (vs_id,))

    for fr in vote_data.get("faction_results", []):
        faction = fr.get("faction", "")
        if not faction:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO bill_votes(vote_session_id, deputy_name, faction, vote_result, external_vote_id, source_url, raw_data) VALUES(?,?,?,?,?,?,?)",
            (vs_id, f"Фракция: {faction}", faction,
             f"за={fr.get('za',0)} против={fr.get('protiv',0)} воздерж={fr.get('vozderzhan',0)} отсутств={fr.get('otsutstvoval',0)}",
             external_vote_id, url,
             json.dumps(fr, ensure_ascii=False)),
        )
        votes_written += 1

    for dv in vote_data.get("individual_votes", []):
        name = dv.get("name", "")
        faction = dv.get("faction", "")
        result = dv.get("result", "")
        if not name or not result:
            continue
        entity_id = _find_entity_by_name(conn, name)
        if not entity_id and dv.get("duma_id"):
            row = conn.execute(
                "SELECT entity_id FROM deputy_profiles WHERE duma_id=?",
                (dv["duma_id"],),
            ).fetchone()
            if row:
                entity_id = row[0]
        try:
            conn.execute(
                "INSERT OR IGNORE INTO bill_votes(vote_session_id, entity_id, deputy_name, faction, vote_result, external_vote_id, source_url, raw_data) VALUES(?,?,?,?,?,?,?,?)",
                (vs_id, entity_id, name, faction, result,
                 external_vote_id, url,
                 json.dumps(dv, ensure_ascii=False)),
            )
            votes_written += 1
        except Exception as e:
            log.warning("Failed to store individual vote for %s: %s", name, e)

    if return_stats:
        return {"id": vs_id, "created": created, "votes_written": votes_written}
    return vs_id


def collect_votes(settings=None, pages: int = 10, fetch_details: bool = True):
    if settings is None:
        settings = load_settings()
    conn = get_db(settings)
    session = _session()
    _ensure_vote_schema(conn)

    _ensure_source(conn, "Голосования ГД (vote.duma.gov.ru)", BASE_URL, "votes")
    conn.commit()

    total = 0
    for page in range(1, pages + 1):
        votes, fetch_error = _scrape_vote_list_result(session, page=page)
        log.info("Vote list page %d: found %d votes", page, len(votes))
        if fetch_error:
            break
        if not votes:
            break

        for v in votes:
            bill_id = _find_bill_by_number(conn, v.get("subject", "")) if v.get("bill_number") else None
            if not bill_id and v.get("bill_number"):
                row = conn.execute("SELECT id FROM bills WHERE number=?", (v["bill_number"],)).fetchone()
                bill_id = row[0] if row else None

            if fetch_details:
                detail = scrape_vote_detail(session, v["url"])
                time.sleep(0.3)
            else:
                detail = v

            vs_id = store_vote_session(conn, bill_id, detail)
            if vs_id:
                total += 1

        conn.commit()
        time.sleep(0.5)

    conn.commit()
    conn.close()
    log.info("Vote sessions: %d stored", total)
    return total


def collect_votes_since(
    settings=None,
    *,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    years: int = 2,
    max_pages: int = 180,
    fetch_details: bool = True,
    stop_after_old_pages: int = 2,
):
    """Collect roll-call votes in a bounded date window."""
    if settings is None:
        settings = load_settings()
    today = date.today()
    if isinstance(end_date, datetime):
        window_end = end_date.date()
    elif isinstance(end_date, date):
        window_end = end_date
    elif isinstance(end_date, str) and end_date.strip():
        window_end = _parse_vote_date(end_date) or today
    else:
        window_end = today

    if isinstance(start_date, datetime):
        window_start = start_date.date()
    elif isinstance(start_date, date):
        window_start = start_date
    elif isinstance(start_date, str) and start_date.strip():
        window_start = _parse_vote_date(start_date) or (window_end - timedelta(days=365 * years))
    else:
        window_start = window_end - timedelta(days=365 * years)

    conn = get_db(settings)
    session = _session()
    _ensure_vote_schema(conn)
    source_id = _ensure_source(conn, "Голосования ГД (vote.duma.gov.ru)", BASE_URL, "votes")
    conn.commit()

    sessions_created = 0
    sessions_updated = 0
    votes_written = 0
    seen = 0
    skipped_old = 0
    skipped_future = 0
    warnings: list[str] = []
    consecutive_old_pages = 0
    last_external_id = ""

    for page in range(1, int(max_pages or 1) + 1):
        votes, fetch_error = _scrape_vote_list_result(session, page=page)
        log.info("Vote list page %d: found %d votes", page, len(votes))
        if fetch_error:
            warnings.append(f"vote_list_fetch_failed:{page}:{fetch_error}")
            break
        if not votes:
            break

        page_dated = 0
        page_old = 0
        page_stored = 0
        for v in votes:
            seen += 1
            detail = scrape_vote_detail(session, v["url"]) if fetch_details else dict(v)
            if fetch_details:
                time.sleep(0.25)
            vote_date = _parse_vote_date(detail.get("vote_date"))
            if vote_date:
                page_dated += 1
                if vote_date > window_end:
                    skipped_future += 1
                    continue
                if vote_date < window_start:
                    skipped_old += 1
                    page_old += 1
                    continue
            else:
                warnings.append(f"missing_vote_date:{v.get('vote_id') or v.get('url')}")
                continue

            bill_id = _find_bill_by_number(conn, detail.get("subject", "") or v.get("subject", ""))
            if not bill_id and (detail.get("bill_number") or v.get("bill_number")):
                row = conn.execute(
                    "SELECT id FROM bills WHERE number=?",
                    (detail.get("bill_number") or v.get("bill_number"),),
                ).fetchone()
                bill_id = row[0] if row else None

            stored = store_vote_session(conn, bill_id, detail, return_stats=True)
            if stored:
                page_stored += 1
                last_external_id = str(detail.get("external_vote_id") or v.get("vote_id") or last_external_id)
                if stored.get("created"):
                    sessions_created += 1
                else:
                    sessions_updated += 1
                votes_written += int(stored.get("votes_written") or 0)

        conn.commit()
        if page_dated and page_old == page_dated and page_stored == 0:
            consecutive_old_pages += 1
        else:
            consecutive_old_pages = 0
        if consecutive_old_pages >= max(1, int(stop_after_old_pages or 1)):
            break
        time.sleep(0.4)

    from runtime.state import update_source_sync_state

    fetch_failed = any(w.startswith("vote_list_fetch_failed:") for w in warnings)
    ok = not fetch_failed
    update_source_sync_state(
        conn,
        source_key="votes",
        source_id=source_id,
        success=ok,
        last_external_id=last_external_id or None,
        transport_mode="vote.duma.gov.ru",
        failure_class="timeout" if fetch_failed else None,
        metadata={
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "max_pages": int(max_pages),
            "sessions_created": sessions_created,
            "sessions_updated": sessions_updated,
            "votes_written": votes_written,
            "warnings": warnings[:20],
        },
    )
    conn.commit()
    conn.close()
    return {
        "ok": ok,
        "items_seen": seen,
        "items_new": sessions_created,
        "items_updated": sessions_updated,
        "warnings": warnings[:100],
        "retriable_errors": [w for w in warnings if w.startswith("vote_list_fetch_failed:")][:20],
        "artifacts": {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "sessions_created": sessions_created,
            "sessions_updated": sessions_updated,
            "votes_written": votes_written,
            "skipped_old": skipped_old,
            "skipped_future": skipped_future,
            "max_pages": int(max_pages),
        },
    }


def collect_votes_last_years(settings=None, *, years: int = 2, max_pages: int | None = None):
    if settings is None:
        settings = load_settings()
    resolved_pages = int(max_pages or settings.get("duma_votes_recent_max_pages", 180) or 180)
    resolved_years = int(years or settings.get("duma_votes_recent_years", 2) or 2)
    return collect_votes_since(settings, years=resolved_years, max_pages=resolved_pages, fetch_details=True)


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Collect Duma votes from vote.duma.gov.ru")
    parser.add_argument("--pages", type=int, default=10)
    parser.add_argument("--no-details", action="store_true")
    parser.add_argument("--since-years", type=int, default=0)
    args = parser.parse_args()

    if args.since_years:
        count = collect_votes_last_years(years=args.since_years, max_pages=args.pages)
    else:
        count = collect_votes(pages=args.pages, fetch_details=not args.no_details)
    print(f"Collected: {count} vote sessions")


if __name__ == "__main__":
    main()
