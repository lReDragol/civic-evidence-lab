from __future__ import annotations

from typing import Callable


JOB_DEFS = [
    {"id": "watch_folder", "name": "Inbox-сканер", "group": "Сбор", "default_interval": 60},
    {"id": "telegram", "name": "Telegram", "group": "Сбор", "default_interval": 300},
    {"id": "youtube", "name": "YouTube", "group": "Сбор", "default_interval": 86400},
    {"id": "rss", "name": "RSS/СМИ", "group": "Сбор", "default_interval": 3600},
    {"id": "official", "name": "Офиц. реестры", "group": "Сбор", "default_interval": 86400},
    {"id": "playwright_official", "name": "Офиц. JS-сайты", "group": "Сбор", "default_interval": 86400},
    {"id": "duma_bills", "name": "Законопроекты Думы", "group": "Сбор", "default_interval": 86400},
    {"id": "minjust", "name": "Минюст (иноагенты)", "group": "Сбор", "default_interval": 86400},
    {"id": "zakupki", "name": "Госзакупки", "group": "Сбор", "default_interval": 86400},
    {"id": "gov", "name": "Кремль/Правительство", "group": "Сбор", "default_interval": 86400},
    {"id": "votes", "name": "Голосования Думы", "group": "Сбор", "default_interval": 86400},
    {"id": "senators", "name": "Сенаторы", "group": "Сбор", "default_interval": 604800},
    {"id": "fas_ach_sk", "name": "ФАС/Счётная/СК", "group": "Сбор", "default_interval": 86400},
    {"id": "executive_directory", "name": "Руководство органов", "group": "Сбор", "default_interval": 604800},
    {"id": "tagger", "name": "Тегирование", "group": "Анализ", "default_interval": 21600},
    {"id": "llm", "name": "LLM-классификатор", "group": "Анализ", "default_interval": 43200},
    {"id": "asr", "name": "ASR (Whisper)", "group": "Медиа", "default_interval": 3600},
    {"id": "ocr", "name": "OCR (PaddleOCR)", "group": "Медиа", "default_interval": 3600},
    {"id": "ner", "name": "NER (Natasha)", "group": "Анализ", "default_interval": 7200},
    {"id": "entity_resolve", "name": "Разрешение сущностей", "group": "Анализ", "default_interval": 43200},
    {"id": "quotes", "name": "Извлечение цитат", "group": "Анализ", "default_interval": 7200},
    {"id": "claims", "name": "Заявления/верификация", "group": "Анализ", "default_interval": 21600},
    {"id": "evidence_link", "name": "Привязка свидетельств", "group": "Анализ", "default_interval": 43200},
    {"id": "negation", "name": "Негация/опровержения", "group": "Анализ", "default_interval": 43200},
    {"id": "authenticity", "name": "Модель подлинности", "group": "Верификация", "default_interval": 86400},
    {"id": "structural_links", "name": "Структурные связи", "group": "Аналитика", "default_interval": 86400},
    {"id": "entity_relation_builder", "name": "Построение связей", "group": "Аналитика", "default_interval": 86400},
    {"id": "l4_tags", "name": "L4 аналитические теги", "group": "Анализ", "default_interval": 43200},
    {"id": "re_verifier", "name": "Повторная верификация", "group": "Верификация", "default_interval": 43200},
    {"id": "contradiction_detector", "name": "Детекция противоречий", "group": "Верификация", "default_interval": 86400},
    {"id": "cases", "name": "Построение дел", "group": "Дела", "default_interval": 86400},
    {"id": "accountability", "name": "Индекс подотчётности", "group": "Дела", "default_interval": 86400},
    {"id": "risk_patterns", "name": "Детекция рисков", "group": "Дела", "default_interval": 86400},
    {"id": "relations", "name": "Связи сущностей", "group": "Анализ", "default_interval": 86400},
    {"id": "backup", "name": "Бэкап БД", "group": "Система", "default_interval": 86400},
]


INTERVAL_KEYS = {
    "watch_folder": "watch_folder_interval_seconds",
    "telegram": "telegram_collect_interval_seconds",
    "youtube": "youtube_interval_seconds",
    "rss": "rss_interval_seconds",
    "official": "official_interval_seconds",
    "playwright_official": "playwright_interval_seconds",
    "duma_bills": "duma_bills_interval_seconds",
    "minjust": "minjust_interval_seconds",
    "zakupki": "zakupki_interval_seconds",
    "gov": "gov_interval_seconds",
    "votes": "votes_interval_seconds",
    "senators": "senators_interval_seconds",
    "fas_ach_sk": "fas_ach_sk_interval_seconds",
    "executive_directory": "executive_directory_interval_seconds",
    "tagger": "classification_interval_seconds",
    "llm": "llm_interval_seconds",
    "asr": None,
    "ocr": None,
    "ner": "ner_interval_seconds",
    "entity_resolve": "entity_resolve_interval_seconds",
    "quotes": "quotes_interval_seconds",
    "claims": "claims_interval_seconds",
    "evidence_link": "evidence_link_interval_seconds",
    "negation": "negation_interval_seconds",
    "authenticity": "authenticity_interval_seconds",
    "structural_links": "structural_links_interval_seconds",
    "entity_relation_builder": "entity_relation_builder_interval_seconds",
    "l4_tags": "l4_tags_interval_seconds",
    "re_verifier": "re_verifier_interval_seconds",
    "contradiction_detector": None,
    "cases": "cases_interval_seconds",
    "accountability": "accountability_interval_seconds",
    "risk_patterns": "risk_interval_seconds",
    "relations": "relations_interval_seconds",
    "backup": "backup_interval_seconds",
}


def _load_settings():
    from config.db_utils import load_settings

    return load_settings()


def _get_db():
    from config.db_utils import get_db

    return get_db(_load_settings())


JOB_FUNC_MAP: dict[str, Callable[[], object]] = {
    "watch_folder": lambda: __import__("collectors.watch_folder", fromlist=["scan_all_inboxes"]).scan_all_inboxes(),
    "telegram": lambda: __import__("asyncio").run(__import__("collectors.telegram_collector", fromlist=["run_collect"]).run_collect()),
    "youtube": lambda: __import__("collectors.youtube_collector", fromlist=["collect_youtube"]).collect_youtube(),
    "rss": lambda: __import__("collectors.rss_collector", fromlist=["collect_rss"]).collect_rss(),
    "official": lambda: __import__("collectors.official_scraper", fromlist=["collect_all_official"]).collect_all_official(),
    "playwright_official": lambda: __import__("collectors.playwright_scraper", fromlist=["collect_all_playwright"]).collect_all_playwright(),
    "duma_bills": lambda: __import__("collectors.playwright_scraper_v2", fromlist=["collect_bills_playwright"]).collect_bills_playwright(pages=2, detail_limit=20, headless=True),
    "minjust": lambda: (
        __import__("collectors.minjust_scraper", fromlist=["collect_foreign_agents"]).collect_foreign_agents(),
        __import__("collectors.minjust_scraper", fromlist=["collect_undesirable_orgs"]).collect_undesirable_orgs(),
    ),
    "zakupki": lambda: __import__("collectors.zakupki_scraper", fromlist=["collect_contracts_recent"]).collect_contracts_recent(pages=3, per_page=20),
    "gov": lambda: (
        __import__("collectors.gov_scraper", fromlist=["collect_kremlin_acts"]).collect_kremlin_acts(pages=5),
        __import__("collectors.gov_scraper", fromlist=["collect_government_news"]).collect_government_news(pages=3),
    ),
    "votes": lambda: __import__("collectors.vote_scraper", fromlist=["collect_votes"]).collect_votes(pages=3, fetch_details=True),
    "senators": lambda: __import__("collectors.senators_scraper", fromlist=["collect_senators"]).collect_senators(fetch_profiles=True),
    "fas_ach_sk": lambda: (
        __import__("collectors.fas_ach_sk_scraper", fromlist=["collect_fas"]).collect_fas(pages=3, fetch_details=True, detail_limit=20),
        __import__("collectors.fas_ach_sk_scraper", fromlist=["collect_ach"]).collect_ach(fetch_details=True, detail_limit=15),
        __import__("collectors.fas_ach_sk_scraper", fromlist=["collect_sk"]).collect_sk(pages=2, fetch_details=True, detail_limit=15),
    ),
    "executive_directory": lambda: __import__("collectors.executive_directory_scraper", fromlist=["collect_executive_directories"]).collect_executive_directories(_load_settings()),
    "tagger": lambda: __import__("classifier.tagger_v2", fromlist=["tag_content_items"]).tag_content_items(),
    "llm": lambda: __import__("classifier.llm_classifier", fromlist=["classify_content"]).classify_content(batch_size=20),
    "asr": lambda: __import__("media_pipeline.asr", fromlist=["process_untranscribed_videos"]).process_untranscribed_videos(),
    "ocr": lambda: __import__("media_pipeline.ocr", fromlist=["process_unprocessed_ocr"]).process_unprocessed_ocr(),
    "ner": lambda: __import__("ner.extractor", fromlist=["process_content_entities"]).process_content_entities(batch_size=200),
    "entity_resolve": lambda: (
        __import__("ner.entity_resolver", fromlist=["resolve_deputies"]).resolve_deputies(),
        __import__("ner.entity_resolver", fromlist=["resolve_all_persons"]).resolve_all_persons(),
    ),
    "quotes": lambda: __import__("claims.quote_extractor", fromlist=["process_content_quotes"]).process_content_quotes(batch_size=200),
    "claims": lambda: __import__("verification.engine", fromlist=["process_claims_for_content"]).process_claims_for_content(),
    "evidence_link": lambda: (
        __import__("verification.evidence_linker", fromlist=["auto_link_evidence"]).auto_link_evidence(),
        __import__("verification.evidence_linker", fromlist=["auto_link_by_content_type"]).auto_link_by_content_type(),
    ),
    "negation": lambda: __import__("classifier.negation_handler", fromlist=["process_negations"]).process_negations(),
    "authenticity": lambda: __import__("verification.authenticity_model", fromlist=["recompute_all"]).recompute_all(),
    "structural_links": lambda: __import__("cases.structural_links", fromlist=["run_all_structural_links"]).run_all_structural_links(_load_settings()),
    "entity_relation_builder": lambda: __import__("analysis.entity_relation_builder", fromlist=["run_all"]).run_all(),
    "l4_tags": lambda: __import__("classifier.analytical_tags", fromlist=["compute_l4_tags_batch"]).compute_l4_tags_batch(_get_db(), limit=1000),
    "re_verifier": lambda: __import__("verification.re_verifier", fromlist=["run_reverification"]).run_reverification(limit=200),
    "contradiction_detector": lambda: __import__("verification.contradiction_detector", fromlist=["run_contradiction_detection"]).run_contradiction_detection(entity_limit=200),
    "cases": lambda: __import__("cases.builder", fromlist=["build_cases_from_entities"]).build_cases_from_entities(min_claims=2),
    "accountability": lambda: __import__("cases.accountability", fromlist=["compute_all_indices"]).compute_all_indices(),
    "risk_patterns": lambda: __import__("cases.risk_detector", fromlist=["detect_all_patterns"]).detect_all_patterns(),
    "relations": lambda: (
        __import__("ner.relation_extractor", fromlist=["extract_co_occurrence_relations"]).extract_co_occurrence_relations(),
        __import__("ner.relation_extractor", fromlist=["extract_head_role_relations"]).extract_head_role_relations(),
    ),
    "backup": lambda: __import__("db.backup", fromlist=["backup_database"]).backup_database(),
}


def get_job_def(job_id: str):
    return next((item for item in JOB_DEFS if item["id"] == job_id), None)


def interval_for_job(settings: dict, job_id: str) -> int:
    job_def = get_job_def(job_id)
    if not job_def:
        return 60
    interval_key = INTERVAL_KEYS.get(job_id)
    if not interval_key:
        return int(job_def["default_interval"])
    return int(settings.get(interval_key, job_def["default_interval"]))


def serialize_jobs(settings: dict, running_jobs: set[str] | None = None) -> list[dict]:
    running_jobs = running_jobs or set()
    items = []
    for job in JOB_DEFS:
        items.append(
            {
                "id": job["id"],
                "name": job["name"],
                "group": job["group"],
                "default_interval": int(job["default_interval"]),
                "interval": interval_for_job(settings, job["id"]),
                "running": job["id"] in running_jobs or bool(settings.get(f"job_{job['id']}_running", False)),
            }
        )
    return items
