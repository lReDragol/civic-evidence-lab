from __future__ import annotations

import logging
import re
from typing import Any

log = logging.getLogger(__name__)

GARBAGE_PATTERNS_AD = [
    re.compile(r"подпис[а-яё]+", re.I),
    re.compile(r"реклам[а-яё]*", re.I),
    re.compile(r"промокод", re.I),
    re.compile(r"купить", re.I),
    re.compile(r"скидк[а-яё]*", re.I),
    re.compile(r"партнёрск", re.I),
    re.compile(r"спонсорск", re.I),
    re.compile(r"заработ[а-яё]+", re.I),
    re.compile(r"кэшбэк", re.I),
    re.compile(r"бонус[а-яё]*", re.I),
    re.compile(r"переходи\s+по\s+ссылк", re.I),
    re.compile(r"телеграм[\-\s]?канал", re.I),
    re.compile(r"чат[\-\s]?бот", re.I),
    re.compile(r"бот\s+для", re.I),
    re.compile(r"сервис\s+для", re.I),
    re.compile(r"приложени[ея]\s", re.I),
    re.compile(r"VPN\s", re.I),
    re.compile(r"впн\s", re.I),
    re.compile(r"курс\s+по", re.I),
    re.compile(r"обучени[ея]\s", re.I),
    re.compile(r"ставь\s+лайк", re.I),
    re.compile(r"поделись", re.I),
    re.compile(r"репост", re.I),
    re.compile(r"голосован[а-яё]+\s+в\s+коммент", re.I),
]

GARBAGE_PATTERNS_PERSONAL = [
    re.compile(r"кружоч[а-яё]+", re.I),
    re.compile(r"селфи", re.I),
    re.compile(r"фотк[а-яё]*", re.I),
    re.compile(r"обед[а-яё]*", re.I),
    re.compile(r"прогулк[а-яё]*", re.I),
    re.compile(r"отпуск[а-яё]*", re.I),
    re.compile(r"выходн[а-яё]*", re.I),
    re.compile(r"доброе\s+утро", re.I),
    re.compile(r"спокойной\s+ночи", re.I),
    re.compile(r"с\s+дн[а-яё]+\s+рожден", re.I),
    re.compile(r"с\s+праздник", re.I),
    re.compile(r"настроени[ея]", re.I),
    re.compile(r"мое\s+утро", re.I),
    re.compile(r"я\s+сегодня", re.I),
    re.compile(r"личн[а-яё]+\s+(опыг|истор|блог)", re.I),
]

GARBAGE_PATTERNS_TEMPLATE = [
    re.compile(r"читайте\s+также", re.I),
    re.compile(r"читайте\s+в\s+нашем", re.I),
    re.compile(r"подпишитесь\s+на\s+наш", re.I),
    re.compile(r"подпишитесь\s+на\s+канал", re.I),
    re.compile(r"присоединяйтесь\s+к\s+нам", re.I),
    re.compile(r"пишите\s+в\s+коммент", re.I),
    re.compile(r"оставляйт[а-яё]+\s+коммент", re.I),
    re.compile(r"смотрите\s+на\s+нашем\s+канал", re.I),
    re.compile(r"подробнее\s+на\s+нашем\s+сайте", re.I),
    re.compile(r"перейд[а-яё]+\s+по\s+ссылк", re.I),
    re.compile(r"ссылк[а-яё]+\s+в\s+(био|профил|шапк)", re.I),
    re.compile(r"товар[а-яё]*\s+по\s+ссылк", re.I),
]

ALL_PATTERNS = GARBAGE_PATTERNS_AD + GARBAGE_PATTERNS_PERSONAL + GARBAGE_PATTERNS_TEMPLATE

NEWS_SIGNAL_PATTERNS = [
    re.compile(r"(?:сообщ[а-яё]+|заяв[а-яё]+|объяв[а-яё]+|сообщил[а-яё]*|сказал[а-яё]*|поясн[а-яё]+|отмет[а-яё]+)", re.I),
    re.compile(r"(?:закон[а-яё]*|указ[а-яё]*|постановлен[а-яё]+|решен[а-яё]+|голосован[а-яё]+)", re.I),
    re.compile(r"(?:депутат[а-яё]*|сенатор[а-яё]*|министр[а-яё]*|президент[а-яё]*|правительств[а-яё]*)", re.I),
    re.compile(r"(?:арест[а-яё]*|задерж[а-яё]+|обыск[а-яё]*|суд[а-яё]*|приговор[а-яё]*)", re.I),
    re.compile(r"(?:коррупц[а-яё]+|взятк[а-яё]*|хищен[а-яё]+|мошенничеств[а-яё]+)", re.I),
    re.compile(r"(?:ФСБ|МВД|Следственн[а-яё]+|Прокуратур[а-яё]+|Роскомнадзор[а-яё]*|ФАС)", re.I),
    re.compile(r"(?:Государственн[а-яё]+\s+Дум[а-яё]*|Совет\s+Федераци[а-яё]*)", re.I),
    re.compile(r"(?:бюджет[а-яё]*|налог[а-яё]*|штраф[а-яё]*|санкци[а-яё]+)", re.I),
    re.compile(r"(?:вооруж[а-яё]+|арм[а-яё]+|СВО|мобилиз[а-яё]+)", re.I),
    re.compile(r"(?:иноагент[а-яё]*|иностранны[а-яё]+\s+агент[а-яё]*)", re.I),
    re.compile(r"\d{1,2}\s+(?:январ[яь]|феврал[яь]|март[аь]|апрел[яь]|ма[яь]|ию[няь]|ию[ляь]|август[аь]|сентябр[яь]|октябр[яь]|ноябр[яь]|декабр[яь])", re.I),
]


def regex_filter(text: str) -> str:
    if not text or len(text.strip()) < 10:
        return "garbage_empty"

    word_count = len(text.split())

    ad_hits = sum(1 for p in GARBAGE_PATTERNS_AD if p.search(text))
    personal_hits = sum(1 for p in GARBAGE_PATTERNS_PERSONAL if p.search(text))
    template_hits = sum(1 for p in GARBAGE_PATTERNS_TEMPLATE if p.search(text))

    news_hits = sum(1 for p in NEWS_SIGNAL_PATTERNS if p.search(text))

    total_garbage = ad_hits + personal_hits + template_hits

    if total_garbage >= 3 and news_hits == 0:
        return "garbage_ad" if ad_hits >= 2 else ("garbage_personal" if personal_hits >= 2 else "garbage_template")

    if total_garbage >= 2 and word_count < 50 and news_hits == 0:
        return "garbage_short"

    if personal_hits >= 2 and ad_hits == 0 and news_hits == 0:
        return "garbage_personal"

    if ad_hits >= 3 and news_hits <= 1 and word_count < 150:
        return "garbage_ad"

    if template_hits >= 2 and ad_hits >= 1 and news_hits == 0:
        return "garbage_template"

    return "ok"


def llm_classify_garbage(text: str, settings: dict = None, conn: Any = None) -> str:
    from llm.key_pool import choose_key_for_stage, bootstrap_provider_catalog, import_keys_from_file, record_key_failure, record_key_success
    from llm.provider_router import run_ai_task
    from config.db_utils import get_db, load_settings
    from pathlib import Path

    if settings is None:
        settings = load_settings()

    snippet = text[:800] if text else ""
    if not snippet or len(snippet.strip()) < 20:
        return "garbage_empty"

    prompt = (
        "Классифицируй текст Telegram-поста одной буквой:\n"
        "A — факт-новость (событие, закон, арест, решение, данные)\n"
        "B — аналитика/мнение/комментарий (оценка, прогноз, критика)\n"
        "C — реклама/продажи (товар, услуга, промокод, подписка на платное)\n"
        "D — личное/мусор (кружки, фото, селфи, настроение, чат, не новостной контент)\n\n"
        f"Текст:\n{snippet}\n\nОтвет: одна буква (A, B, C или D)"
    )

    own_conn = conn is None
    key_info = None
    try:
        if own_conn:
            conn = get_db(settings)
            key_file = str(Path(settings.get("project_root", ".")) / "key.json")
            try:
                import_keys_from_file(conn, key_file)
            except Exception:
                pass
            bootstrap_provider_catalog(conn)

        key_info = choose_key_for_stage(conn, stage="triage")
        if not key_info:
            return "ok"

        model_name = key_info.get("model_name") or key_info.get("model") or ""
        if not model_name:
            return regex_filter(text)

        result = run_ai_task(
            conn=conn,
            provider=key_info["provider"],
            model=model_name,
            api_key=key_info["api_key"],
            task={"stage": "triage", "input_text": snippet, "prompt_override": prompt},
        )

        record_key_success(conn, key_info["key_id"])

        answer = (result.get("output_text") or "").strip().upper()
        if answer.startswith("A"):
            return "ok"
        elif answer.startswith("B"):
            return "ok"
        elif answer.startswith("C"):
            return "garbage_ad"
        elif answer.startswith("D"):
            return "garbage_personal"
        return "ok"

    except Exception as e:
        log.warning("LLM garbage filter failed, defaulting to regex result: %s", e)
        try:
            if conn and key_info:
                record_key_failure(conn, key_info["key_id"], failure_kind="llm_call", error_text=str(e))
        except Exception:
            pass
        return regex_filter(text)
    finally:
        if own_conn:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


def filter_content(text: str, use_llm: bool = False, settings: dict = None, conn: Any = None) -> tuple[str, bool]:
    regex_result = regex_filter(text)

    if regex_result == "ok":
        if use_llm and text and len(text.strip()) > 30:
            llm_result = llm_classify_garbage(text, settings, conn=conn)
            if llm_result.startswith("garbage_"):
                return llm_result, False
        return "ok", True

    if regex_result.startswith("garbage_"):
        news_hits = sum(1 for p in NEWS_SIGNAL_PATTERNS if p.search(text or ""))
        if news_hits >= 2:
            return "ok_mixed", True
        if use_llm and text and len(text.strip()) > 30:
            llm_result = llm_classify_garbage(text, settings, conn=conn)
            if llm_result == "ok":
                return "ok_rescued", True
        return regex_result, False

    return "ok", True


def filter_telegram_post(text: str, settings: dict = None) -> tuple[str, bool]:
    return filter_content(text, use_llm=False, settings=settings)


def filter_content_with_llm(text: str, settings: dict = None, conn: Any = None) -> tuple[str, bool]:
    return filter_content(text, use_llm=True, settings=settings, conn=conn)


def run_llm_garbage_filter(settings: dict = None, batch_size: int = 200):
    import sqlite3
    from llm.key_pool import bootstrap_provider_catalog, import_keys_from_file
    from config.db_utils import get_db, load_settings
    from pathlib import Path

    if settings is None:
        settings = load_settings()

    conn = get_db(settings)
    try:
        key_file = str(Path(settings.get("project_root", ".")) / "key.json")
        try:
            import_keys_from_file(conn, key_file)
        except Exception:
            pass
        bootstrap_provider_catalog(conn)
    except Exception:
        pass

    rows = conn.execute(
        "SELECT id, body_text, title, status FROM content_items "
        "WHERE status NOT IN ('suppressed_garbage', 'suppressed_promo', 'suppressed_template') "
        "AND (garbage_checked IS NULL OR garbage_checked = 0) "
        "AND length(body_text) > 30 "
        "ORDER BY id DESC LIMIT ?",
        (batch_size,),
    ).fetchall()

    if not rows:
        conn.close()
        return {"checked": 0, "suppressed": 0}

    checked = 0
    suppressed = 0
    for row in rows:
        content_id = row[0]
        text = f"{row[2] or ''}\n{row[1] or ''}"

        conn.commit()
        label, keep = filter_content_with_llm(text, settings=settings, conn=conn)
        checked += 1

        if not keep:
            conn.execute("UPDATE content_items SET status='suppressed_garbage', garbage_checked=1 WHERE id=?", (content_id,))
            suppressed += 1
        else:
            conn.execute("UPDATE content_items SET garbage_checked=1 WHERE id=?", (content_id,))

        if checked % 20 == 0:
            conn.commit()

    conn.commit()
    conn.close()
    log.info("Garbage filter LLM: %d checked, %d suppressed", checked, suppressed)
    return {"checked": checked, "suppressed": suppressed}
