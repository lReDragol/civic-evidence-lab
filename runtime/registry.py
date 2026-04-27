from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from config.db_utils import PROJECT_ROOT, get_db, load_settings
from graph.relation_candidates import rebuild_and_promote_relation_candidates


@dataclass(frozen=True)
class JobSpec:
    id: str
    name: str
    group: str
    default_interval: int
    interval_key: str | None
    stage: str
    timeout_seconds: int = 3600
    retry_limit: int = 3
    retry_backoff_seconds: int = 60
    scheduled: bool = True
    visible: bool = True
    source_keys: tuple[str, ...] = ()
    runner: Callable[[dict[str, Any]], Any] | None = None


def _watch_folder(settings: dict[str, Any]):
    return __import__("collectors.watch_folder", fromlist=["scan_all_inboxes"]).scan_all_inboxes()


def _telegram(settings: dict[str, Any]):
    return __import__("asyncio").run(
        __import__("collectors.telegram_collector", fromlist=["run_collect"]).run_collect(settings)
    )


def _youtube(settings: dict[str, Any]):
    return __import__("collectors.youtube_collector", fromlist=["collect_youtube"]).collect_youtube()


def _rss(settings: dict[str, Any]):
    return __import__("collectors.rss_collector", fromlist=["collect_rss"]).collect_rss()


def _official(settings: dict[str, Any]):
    return __import__("collectors.official_scraper", fromlist=["collect_all_official"]).collect_all_official()


def _playwright_official(settings: dict[str, Any]):
    return __import__("collectors.playwright_scraper", fromlist=["collect_all_playwright"]).collect_all_playwright()


def _duma_bills(settings: dict[str, Any]):
    return __import__("collectors.playwright_scraper_v2", fromlist=["collect_bills_playwright"]).collect_bills_playwright(
        pages=2, detail_limit=20, headless=True
    )


def _minjust(settings: dict[str, Any]):
    scraper = __import__("collectors.minjust_scraper", fromlist=["collect_foreign_agents"])
    return {
        "foreign_agents": scraper.collect_foreign_agents(),
        "undesirable_orgs": scraper.collect_undesirable_orgs(),
    }


def _zakupki(settings: dict[str, Any]):
    return __import__("collectors.zakupki_scraper", fromlist=["collect_contracts_recent"]).collect_contracts_recent(
        pages=3, per_page=20
    )


def _gov(settings: dict[str, Any]):
    module = __import__("collectors.gov_scraper", fromlist=["collect_kremlin_acts"])
    return {
        "kremlin": module.collect_kremlin_acts(pages=5),
        "government": module.collect_government_news(pages=3),
    }


def _votes(settings: dict[str, Any]):
    return __import__("collectors.vote_scraper", fromlist=["collect_votes"]).collect_votes(pages=3, fetch_details=True)


def _deputies(settings: dict[str, Any]):
    results: dict[str, Any] = {}
    html_module = __import__("collectors.deputy_profiles_scraper", fromlist=["collect_deputies"])
    page_limit = max(1, int(settings.get("deputies_html_pages", 3) or 3))
    fetch_details = bool(settings.get("deputies_fetch_details", False))
    used_primary = 0
    try:
        if settings.get("duma_api_token"):
            results["collect_deputies_api"] = html_module.collect_deputies_api(settings) or 0
            used_primary = int(results["collect_deputies_api"] or 0)
        else:
            results["collect_deputies_html"] = html_module.collect_deputies_html(
                settings,
                fetch_details=fetch_details,
                max_pages=page_limit,
            ) or 0
            used_primary = int(results["collect_deputies_html"] or 0)
    except Exception as error:
        results["collect_deputies_error"] = str(error)

    if not used_primary and bool(settings.get("deputies_playwright_fallback")):
        try:
            results["playwright_fallback"] = __import__(
                "collectors.playwright_scraper_v2",
                fromlist=["collect_deputies_playwright"],
            ).collect_deputies_playwright(settings=settings, headless=True, max_pages=min(page_limit, 3)) or 0
        except Exception as error:
            results["playwright_fallback_error"] = str(error)
            results["playwright_fallback"] = 0

    try:
        results["import_sponsors"] = __import__(
            "tools.import_deputies_from_sponsors",
            fromlist=["import_sponsors_as_deputies"],
        ).import_sponsors_as_deputies(settings) or 0
    except Exception as error:
        results["import_sponsors_error"] = str(error)

    conn = get_db(settings)
    try:
        backfill = __import__("tools.backfill_vote_entities", fromlist=["backfill_vote_entities"])
        matched, created = backfill.backfill_vote_entities(conn)
        results["vote_entity_backfill"] = {"matched": matched, "created": created}
    finally:
        conn.close()
    items_new = int(
        results.get("collect_deputies_api")
        or results.get("collect_deputies_html")
        or results.get("playwright_fallback")
        or 0
    ) + int(results.get("import_sponsors") or 0) + int((results.get("vote_entity_backfill") or {}).get("created") or 0)
    items_updated = int((results.get("vote_entity_backfill") or {}).get("matched") or 0)
    if items_new <= 0 and items_updated <= 0:
        return {
            "ok": False,
            "retriable_errors": ["deputies_collected_zero"],
            "artifacts": results,
        }
    return {
        "ok": True,
        "items_new": items_new,
        "items_updated": items_updated,
        "artifacts": results,
    }


def _senators(settings: dict[str, Any]):
    result = __import__("collectors.senators_scraper", fromlist=["collect_senators"]).collect_senators(fetch_profiles=True)
    if int(result or 0) <= 0:
        return {
            "ok": False,
            "retriable_errors": ["senators_collected_zero"],
            "artifacts": {"result": int(result or 0)},
        }
    return {
        "ok": True,
        "items_new": int(result or 0),
        "artifacts": {"result": int(result or 0)},
    }


def _fas_ach_sk(settings: dict[str, Any]):
    module = __import__("collectors.fas_ach_sk_scraper", fromlist=["collect_fas"])
    return {
        "fas": module.collect_fas(pages=3, fetch_details=True, detail_limit=20),
        "ach": module.collect_ach(fetch_details=True, detail_limit=15),
        "sk": module.collect_sk(pages=2, fetch_details=True, detail_limit=15),
    }


def _executive_directory(settings: dict[str, Any]):
    return __import__(
        "collectors.executive_directory_scraper",
        fromlist=["collect_executive_directories"],
    ).collect_executive_directories(settings)


def _profiles_enrichment(settings: dict[str, Any]):
    return __import__("enrichment.profiles_enrichment", fromlist=["run_profiles_enrichment"]).run_profiles_enrichment(settings)


def _photo_backfill(settings: dict[str, Any]):
    module = __import__("enrichment.photo_backfill", fromlist=["run_photo_backfill"])
    limit = int(settings.get("photo_backfill_limit", 400) or 400)
    return module.run_photo_backfill(settings, limit=limit)


def _anticorruption_disclosures(settings: dict[str, Any]):
    return __import__(
        "enrichment.anticorruption_scraper",
        fromlist=["run_anticorruption_disclosures"],
    ).run_anticorruption_disclosures(settings)


def _company_registry_enrichment(settings: dict[str, Any]):
    module = __import__(
        "enrichment.company_registry_enrichment",
        fromlist=["run_company_registry_enrichment"],
    )
    limit = int(settings.get("company_registry_enrichment_limit", 250) or 250)
    return module.run_company_registry_enrichment(settings, limit=limit)


def _state_company_reports(settings: dict[str, Any]):
    return __import__("enrichment.state_company_reports", fromlist=["run_state_company_reports"]).run_state_company_reports(settings)


def _restriction_corpus(settings: dict[str, Any]):
    return __import__("enrichment.restriction_corpus", fromlist=["build_restriction_corpus"]).build_restriction_corpus(settings)


def _content_dedupe(settings: dict[str, Any]):
    return __import__("enrichment.content_dedupe", fromlist=["run_content_dedupe"]).run_content_dedupe(settings)


def _review_pack_export(settings: dict[str, Any]):
    module = __import__("enrichment.review_packs", fromlist=["export_review_pack"])
    export_dir = Path(settings.get("review_export_dir", str(PROJECT_ROOT / "reports" / "review_packs")))
    export_dir.mkdir(parents=True, exist_ok=True)
    queues = settings.get(
        "review_export_queues",
        ["content_duplicates", "entity_duplicates", "assets_affiliations", "restrictions_justifications"],
    )
    results: dict[str, Any] = {}
    total = 0
    for queue_key in queues:
        csv_path = export_dir / f"{queue_key}.csv"
        result = module.export_review_pack(settings, queue_key=queue_key, csv_path=csv_path)
        results[queue_key] = result
        total += int(result.get("items_new") or 0)
    return {"ok": True, "items_new": total, "artifacts": results}


def _review_pack_import(settings: dict[str, Any]):
    module = __import__("enrichment.review_packs", fromlist=["import_review_pack"])
    import_dir = Path(settings.get("review_import_dir", str(PROJECT_ROOT / "reports" / "review_packs")))
    if not import_dir.exists():
        return {"ok": True, "items_seen": 0, "items_updated": 0, "warnings": [f"missing:{import_dir}"]}
    results: dict[str, Any] = {}
    updated = 0
    for csv_path in sorted(import_dir.glob("*.csv")):
        result = module.import_review_pack(settings, csv_path=csv_path)
        results[csv_path.name] = result
        updated += int(result.get("items_updated") or 0)
    return {"ok": True, "items_updated": updated, "artifacts": results}


def _tagger(settings: dict[str, Any]):
    module = __import__("classifier.tagger_v3", fromlist=["classify_content_items"])
    batch_size = max(100, int(settings.get("classifier_v3_batch_size", 1000) or 1000))
    max_batches = max(1, int(settings.get("classifier_v3_max_batches", 100) or 100))
    processed = 0
    tags_written = 0
    votes_written = 0
    cleanup_deleted = 0
    batches = 0
    warnings: list[str] = []

    for _ in range(max_batches):
        result = module.classify_content_items(settings, batch_size=batch_size)
        if not result.get("ok", True):
            return result
        batches += 1
        processed += int(result.get("processed") or 0)
        tags_written += int(result.get("tags_written") or 0)
        votes_written += int(result.get("votes_written") or 0)
        cleanup_deleted += int(result.get("cleanup_deleted") or 0)
        if int(result.get("processed") or 0) <= 0:
            break
    else:
        warnings.append(f"classifier_v3_max_batches_reached:{max_batches}")

    return {
        "ok": True,
        "items_seen": processed,
        "items_new": tags_written,
        "items_updated": votes_written,
        "warnings": warnings,
        "artifacts": {
            "processed": processed,
            "tags_written": tags_written,
            "votes_written": votes_written,
            "cleanup_deleted": cleanup_deleted,
            "batches": batches,
            "batch_size": batch_size,
        },
    }


def _llm(settings: dict[str, Any]):
    return __import__("classifier.llm_classifier_v2", fromlist=["classify_content"]).classify_content(settings=settings, batch_size=20)


def _semantic_index(settings: dict[str, Any]):
    return __import__("classifier.semantic_index", fromlist=["build_semantic_index"]).build_semantic_index(settings)


def _event_pipeline(settings: dict[str, Any]):
    return __import__("analysis.event_pipeline", fromlist=["build_event_pipeline"]).build_event_pipeline(settings)


def _asr(settings: dict[str, Any]):
    return __import__("media_pipeline.asr", fromlist=["process_untranscribed_videos"]).process_untranscribed_videos()


def _ocr(settings: dict[str, Any]):
    return __import__("media_pipeline.ocr", fromlist=["process_unprocessed_ocr"]).process_unprocessed_ocr()


def _ner(settings: dict[str, Any]):
    return __import__("ner.extractor", fromlist=["process_content_entities"]).process_content_entities(batch_size=200)


def _entity_resolve(settings: dict[str, Any]):
    module = __import__("ner.entity_resolver", fromlist=["resolve_deputies"])
    module.resolve_deputies()
    return module.resolve_all_persons()


def _quotes(settings: dict[str, Any]):
    return __import__("claims.quote_extractor", fromlist=["process_content_quotes"]).process_content_quotes(batch_size=200)


def _claims(settings: dict[str, Any]):
    module = __import__("verification.engine", fromlist=["process_claims_for_content"])
    return module.process_claims_for_content(
        settings=settings,
        content_limit=max(200, int(settings.get("claims_content_limit", 3000) or 3000)),
        verification_limit=max(20, int(settings.get("claims_verification_limit", 200) or 200)),
        external_checks=bool(settings.get("claims_external_checks", False)),
    )


def _claim_cluster(settings: dict[str, Any]):
    return __import__("verification.claim_normalizer", fromlist=["sync_claim_clusters"]).sync_claim_clusters(settings)


def _evidence_link(settings: dict[str, Any]):
    module = __import__("verification.evidence_linker", fromlist=["auto_link_evidence"])
    return {
        "auto_link_evidence": module.auto_link_evidence(),
        "auto_link_by_content_type": module.auto_link_by_content_type(),
        "backfill_evidence_classes": module.backfill_evidence_classes(settings),
    }


def _negation(settings: dict[str, Any]):
    return __import__("classifier.negation_handler", fromlist=["process_negations"]).process_negations()


def _authenticity(settings: dict[str, Any]):
    return __import__("verification.authenticity_model", fromlist=["reverify_all_claims"]).reverify_all_claims(settings)


def _structural_links(settings: dict[str, Any]):
    return __import__("cases.structural_links", fromlist=["run_all_structural_links"]).run_all_structural_links(settings)


def _entity_relation_builder(settings: dict[str, Any]):
    return __import__("analysis.entity_relation_builder", fromlist=["run_all"]).run_all(settings)


def _l4_tags(settings: dict[str, Any]):
    module = __import__("classifier.analytical_tags", fromlist=["compute_l4_tags_batch"])
    conn = get_db(settings)
    try:
        stats = module.compute_l4_tags_batch(conn, limit=1000)
        conn.commit()
        return stats
    finally:
        conn.close()


def _re_verifier(settings: dict[str, Any]):
    return __import__("verification.re_verifier", fromlist=["run_reverification"]).run_reverification(limit=200)


def _contradiction_detector(settings: dict[str, Any]):
    return __import__("verification.contradiction_detector", fromlist=["run_contradiction_detection"]).run_contradiction_detection(entity_limit=200)


def _cases(settings: dict[str, Any]):
    return __import__("cases.builder", fromlist=["build_cases_from_entities"]).build_cases_from_entities(min_claims=2)


def _accountability(settings: dict[str, Any]):
    return __import__("cases.accountability", fromlist=["compute_all_indices"]).compute_all_indices()


def _risk_patterns(settings: dict[str, Any]):
    return __import__("cases.risk_detector", fromlist=["detect_all_patterns"]).detect_all_patterns()


def _relations(settings: dict[str, Any]):
    snapshot_tools = __import__("tools.build_analysis_snapshot", fromlist=["normalize_contracts"])
    conn = get_db(settings)
    try:
        contract_stats = snapshot_tools.normalize_contracts(conn)
    finally:
        conn.close()
    extractor = __import__("ner.relation_extractor", fromlist=["extract_head_role_relations"])
    return {
        "normalized_contracts": contract_stats,
        "head_role_relations": extractor.extract_head_role_relations(settings),
        "candidate_relations": rebuild_and_promote_relation_candidates(settings),
    }


def _relation_rebuild_enriched(settings: dict[str, Any]):
    return __import__("enrichment.relation_rebuild", fromlist=["run_relation_rebuild_enriched"]).run_relation_rebuild_enriched(settings)


def _classifier_audit(settings: dict[str, Any]):
    return __import__("classifier.audit", fromlist=["build_classifier_audit"]).build_classifier_audit(settings)


def _quality_gate(settings: dict[str, Any]):
    return __import__("quality.pipeline_gate", fromlist=["build_quality_gate"]).build_quality_gate(settings)


def _backup(settings: dict[str, Any]):
    return __import__("db.backup", fromlist=["backup_database"]).backup_database()


def _source_health(settings: dict[str, Any]):
    return __import__("tools.check_official_sources", fromlist=["check_sources"]).check_sources(
        timeout=int(settings.get("health", {}).get("timeout_seconds", 8)),
        settings=settings,
    )


def _build_analysis_snapshot(settings: dict[str, Any]):
    module = __import__("tools.build_analysis_snapshot", fromlist=["build_analysis_snapshot"])
    source_db = Path(settings.get("db_path", str(PROJECT_ROOT / "db" / "news_unified.db")))
    target_db = Path(settings.get("analysis_db_path", str(PROJECT_ROOT / "db" / "news_analysis.db")))
    report_path = Path(settings.get("analysis_report_path", str(PROJECT_ROOT / "reports" / "analysis_snapshot_latest.json")))
    return module.build_analysis_snapshot(source_db=source_db, target_db=target_db, report_path=report_path)


def _obsidian_export(settings: dict[str, Any]):
    module = __import__("tools.export_obsidian", fromlist=["export_obsidian"])
    db_path = Path(settings.get("analysis_db_path", str(PROJECT_ROOT / "db" / "news_analysis.db")))
    vault = Path(settings.get("obsidian_export_dir", str(PROJECT_ROOT / "obsidian_export_graph")))
    return module.export_obsidian(db_path=db_path, vault=vault, copy_media=True, mode="graph")


def _maintenance(settings: dict[str, Any]):
    conn = get_db(settings)
    try:
        checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        conn.execute("PRAGMA optimize")
        return {
            "checkpoint": [tuple(row) for row in checkpoint],
            "integrity_check": integrity,
        }
    finally:
        conn.close()


JOB_SPECS = [
    JobSpec("watch_folder", "Inbox-сканер", "Сбор", 60, "watch_folder_interval_seconds", "collect", timeout_seconds=180, runner=_watch_folder),
    JobSpec("telegram", "Telegram", "Сбор", 300, "telegram_collect_interval_seconds", "collect", timeout_seconds=1800, source_keys=("telegram",), runner=_telegram),
    JobSpec("youtube", "YouTube", "Сбор", 86400, "youtube_interval_seconds", "collect", timeout_seconds=3600, source_keys=("youtube",), runner=_youtube),
    JobSpec("rss", "RSS/СМИ", "Сбор", 3600, "rss_interval_seconds", "collect", timeout_seconds=1800, source_keys=("rss",), runner=_rss),
    JobSpec("official", "Офиц. реестры", "Сбор", 86400, "official_interval_seconds", "collect", timeout_seconds=3600, source_keys=("official",), runner=_official),
    JobSpec("playwright_official", "Офиц. JS-сайты", "Сбор", 86400, "playwright_interval_seconds", "collect", timeout_seconds=3600, source_keys=("playwright_official",), runner=_playwright_official),
    JobSpec("duma_bills", "Законопроекты Думы", "Сбор", 86400, "duma_bills_interval_seconds", "collect", timeout_seconds=3600, source_keys=("duma_bills",), runner=_duma_bills),
    JobSpec("minjust", "Минюст (иноагенты)", "Сбор", 86400, "minjust_interval_seconds", "collect", timeout_seconds=3600, source_keys=("minjust",), runner=_minjust),
    JobSpec("zakupki", "Госзакупки", "Сбор", 86400, "zakupki_interval_seconds", "collect", timeout_seconds=3600, source_keys=("zakupki",), runner=_zakupki),
    JobSpec("gov", "Кремль/Правительство", "Сбор", 86400, "gov_interval_seconds", "collect", timeout_seconds=3600, source_keys=("kremlin", "government"), runner=_gov),
    JobSpec("votes", "Голосования Думы", "Сбор", 86400, "votes_interval_seconds", "collect", timeout_seconds=3600, source_keys=("votes",), runner=_votes),
    JobSpec("deputies", "Депутаты ГД", "Сбор", 604800, "deputies_interval_seconds", "collect", timeout_seconds=5400, source_keys=("deputies",), runner=_deputies),
    JobSpec("senators", "Сенаторы", "Сбор", 604800, "senators_interval_seconds", "collect", timeout_seconds=3600, source_keys=("senators",), runner=_senators),
    JobSpec("fas_ach_sk", "ФАС/Счётная/СК", "Сбор", 86400, "fas_ach_sk_interval_seconds", "collect", timeout_seconds=3600, source_keys=("fas", "ach", "sk"), runner=_fas_ach_sk),
    JobSpec("executive_directory", "Руководство органов", "Сбор", 604800, "executive_directory_interval_seconds", "collect", timeout_seconds=3600, source_keys=("executive_directory",), runner=_executive_directory),
    JobSpec("profiles_enrichment", "Profiles enrichment", "Обогащение", 604800, "profiles_enrichment_interval_seconds", "enrichment", timeout_seconds=7200, runner=_profiles_enrichment),
    JobSpec("photo_backfill", "Photo backfill", "Обогащение", 86400, "photo_backfill_interval_seconds", "enrichment", timeout_seconds=7200, runner=_photo_backfill),
    JobSpec("anticorruption_disclosures", "Декларации/доходы", "Обогащение", 86400, "anticorruption_disclosures_interval_seconds", "enrichment", timeout_seconds=10800, runner=_anticorruption_disclosures),
    JobSpec("company_registry_enrichment", "Бизнес/аффилиации", "Обогащение", 86400, "company_registry_enrichment_interval_seconds", "enrichment", timeout_seconds=10800, runner=_company_registry_enrichment),
    JobSpec("state_company_reports", "Госкомпании/отчёты", "Обогащение", 604800, "state_company_reports_interval_seconds", "enrichment", timeout_seconds=10800, runner=_state_company_reports),
    JobSpec("restriction_corpus", "Ограничения/оправдания", "Обогащение", 86400, "restriction_corpus_interval_seconds", "enrichment", timeout_seconds=7200, runner=_restriction_corpus),
    JobSpec("content_dedupe", "Контент dedupe", "Обогащение", 43200, "content_dedupe_interval_seconds", "enrichment", timeout_seconds=7200, runner=_content_dedupe),
    JobSpec("review_pack_export", "Review pack export", "Обогащение", 86400, "review_pack_export_interval_seconds", "enrichment", timeout_seconds=3600, scheduled=False, runner=_review_pack_export),
    JobSpec("review_pack_import", "Review pack import", "Обогащение", 86400, "review_pack_import_interval_seconds", "enrichment", timeout_seconds=3600, scheduled=False, runner=_review_pack_import),
    JobSpec("source_health", "Source health", "Система", 1800, "source_health_interval_seconds", "health", timeout_seconds=600, source_keys=("source_health",), runner=_source_health),
    JobSpec("tagger", "Classifier v3", "Анализ", 21600, "classification_interval_seconds", "analysis", timeout_seconds=3600, runner=_tagger),
    JobSpec("llm", "LLM-классификатор", "Анализ", 43200, "llm_interval_seconds", "analysis", timeout_seconds=7200, scheduled=False, visible=False, runner=_llm),
    JobSpec("semantic_index", "Semantic index", "Анализ", 43200, "semantic_index_interval_seconds", "analysis", timeout_seconds=7200, runner=_semantic_index),
    JobSpec("event_pipeline", "Event pipeline", "Анализ", 43200, "event_pipeline_interval_seconds", "analysis", timeout_seconds=7200, runner=_event_pipeline),
    JobSpec("asr", "ASR (Whisper)", "Медиа", 3600, None, "media", timeout_seconds=7200, runner=_asr),
    JobSpec("ocr", "OCR (PaddleOCR)", "Медиа", 3600, None, "media", timeout_seconds=7200, runner=_ocr),
    JobSpec("ner", "NER (Natasha)", "Анализ", 7200, "ner_interval_seconds", "analysis", timeout_seconds=3600, runner=_ner),
    JobSpec("entity_resolve", "Разрешение сущностей", "Анализ", 43200, "entity_resolve_interval_seconds", "analysis", timeout_seconds=3600, runner=_entity_resolve),
    JobSpec("quotes", "Извлечение цитат", "Анализ", 7200, "quotes_interval_seconds", "analysis", timeout_seconds=3600, runner=_quotes),
    JobSpec("claims", "Заявления/верификация", "Анализ", 21600, "claims_interval_seconds", "verification", timeout_seconds=7200, runner=_claims),
    JobSpec("claim_cluster", "Claim clustering", "Анализ", 43200, "claim_cluster_interval_seconds", "verification", timeout_seconds=7200, runner=_claim_cluster),
    JobSpec("evidence_link", "Привязка свидетельств", "Анализ", 43200, "evidence_link_interval_seconds", "verification", timeout_seconds=7200, runner=_evidence_link),
    JobSpec("negation", "Негация/опровержения", "Анализ", 43200, "negation_interval_seconds", "analysis", timeout_seconds=3600, runner=_negation),
    JobSpec("authenticity", "Модель подлинности", "Верификация", 86400, "authenticity_interval_seconds", "verification", timeout_seconds=7200, runner=_authenticity),
    JobSpec("structural_links", "Структурные связи", "Аналитика", 86400, "structural_links_interval_seconds", "graph", timeout_seconds=3600, runner=_structural_links),
    JobSpec("entity_relation_builder", "Построение связей", "Аналитика", 86400, "entity_relation_builder_interval_seconds", "graph", timeout_seconds=3600, runner=_entity_relation_builder),
    JobSpec("l4_tags", "L4 аналитические теги", "Анализ", 43200, "l4_tags_interval_seconds", "analysis", timeout_seconds=3600, runner=_l4_tags),
    JobSpec("re_verifier", "Повторная верификация", "Верификация", 43200, "re_verifier_interval_seconds", "verification", timeout_seconds=7200, runner=_re_verifier),
    JobSpec("contradiction_detector", "Детекция противоречий", "Верификация", 86400, "contradiction_detector_interval_seconds", "verification", timeout_seconds=3600, runner=_contradiction_detector),
    JobSpec("cases", "Построение дел", "Дела", 86400, "cases_interval_seconds", "cases", timeout_seconds=3600, runner=_cases),
    JobSpec("accountability", "Индекс подотчётности", "Дела", 86400, "accountability_interval_seconds", "cases", timeout_seconds=3600, runner=_accountability),
    JobSpec("risk_patterns", "Детекция рисков", "Дела", 86400, "risk_interval_seconds", "cases", timeout_seconds=3600, runner=_risk_patterns),
    JobSpec("relations", "Связи сущностей", "Анализ", 86400, "relations_interval_seconds", "graph", timeout_seconds=3600, runner=_relations),
    JobSpec("relation_rebuild_enriched", "Enriched relation rebuild", "Обогащение", 86400, "relation_rebuild_enriched_interval_seconds", "graph", timeout_seconds=7200, runner=_relation_rebuild_enriched),
    JobSpec("classifier_audit", "Classifier audit / drift gate", "Система", 86400, "classifier_audit_interval_seconds", "quality", timeout_seconds=3600, scheduled=False, runner=_classifier_audit),
    JobSpec("quality_gate", "QA quality gate", "Система", 86400, "quality_gate_interval_seconds", "quality", timeout_seconds=3600, scheduled=False, runner=_quality_gate),
    JobSpec("analysis_snapshot", "Analysis snapshot", "Система", 86400, "analysis_snapshot_interval_seconds", "snapshot", timeout_seconds=10800, scheduled=False, runner=_build_analysis_snapshot),
    JobSpec("obsidian_export", "Obsidian graph export", "Система", 86400, "obsidian_export_interval_seconds", "export", timeout_seconds=10800, scheduled=False, runner=_obsidian_export),
    JobSpec("backup", "Бэкап БД", "Система", 86400, "backup_interval_seconds", "maintenance", timeout_seconds=3600, runner=_backup),
    JobSpec("maintenance", "DB maintenance", "Система", 604800, "maintenance_interval_seconds", "maintenance", timeout_seconds=3600, visible=False, runner=_maintenance),
]

JOB_BY_ID = {spec.id: spec for spec in JOB_SPECS}

JOB_DEFS = [
    {
        "id": spec.id,
        "name": spec.name,
        "group": spec.group,
        "default_interval": spec.default_interval,
        "stage": spec.stage,
        "scheduled": spec.scheduled,
        "visible": spec.visible,
    }
    for spec in JOB_SPECS
]

INTERVAL_KEYS = {spec.id: spec.interval_key for spec in JOB_SPECS}
JOB_FUNC_MAP = {spec.id: spec.runner for spec in JOB_SPECS if spec.runner}


PIPELINE_JOB_IDS = {
    "incremental": [
        "watch_folder",
        "telegram",
        "youtube",
        "rss",
        "official",
        "playwright_official",
        "duma_bills",
        "minjust",
        "zakupki",
        "gov",
        "votes",
        "deputies",
        "profiles_enrichment",
        "photo_backfill",
        "fas_ach_sk",
        "executive_directory",
        "content_dedupe",
        "asr",
        "ocr",
        "tagger",
        "ner",
        "entity_resolve",
        "quotes",
        "claims",
        "claim_cluster",
        "semantic_index",
        "event_pipeline",
        "evidence_link",
        "negation",
        "authenticity",
        "re_verifier",
        "contradiction_detector",
    ],
    "nightly": [
        "source_health",
        "profiles_enrichment",
        "photo_backfill",
        "anticorruption_disclosures",
        "company_registry_enrichment",
        "state_company_reports",
        "restriction_corpus",
        "content_dedupe",
        "tagger",
        "ner",
        "entity_resolve",
        "quotes",
        "claims",
        "claim_cluster",
        "semantic_index",
        "event_pipeline",
        "evidence_link",
        "negation",
        "authenticity",
        "structural_links",
        "entity_relation_builder",
        "relations",
        "relation_rebuild_enriched",
        "cases",
        "accountability",
        "risk_patterns",
        "classifier_audit",
        "quality_gate",
        "analysis_snapshot",
        "obsidian_export",
        "review_pack_export",
    ],
    "weekly_maintenance": [
        "source_health",
        "backup",
        "maintenance",
    ],
}


def get_job_spec(job_id: str) -> JobSpec | None:
    return JOB_BY_ID.get(job_id)


def get_job_def(job_id: str):
    spec = get_job_spec(job_id)
    if not spec:
        return None
    return {
        "id": spec.id,
        "name": spec.name,
        "group": spec.group,
        "default_interval": spec.default_interval,
        "stage": spec.stage,
        "scheduled": spec.scheduled,
        "visible": spec.visible,
    }


def _nested_interval(settings: dict[str, Any], job_id: str) -> int | None:
    scheduler_cfg = settings.get("scheduler", {})
    intervals = scheduler_cfg.get("intervals", {}) if isinstance(scheduler_cfg, dict) else {}
    value = intervals.get(job_id)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def interval_for_job(settings: dict[str, Any], job_id: str) -> int:
    spec = get_job_spec(job_id)
    if not spec:
        return 60
    nested = _nested_interval(settings, job_id)
    if nested is not None:
        return nested
    if spec.interval_key:
        try:
            return int(settings.get(spec.interval_key, spec.default_interval))
        except (TypeError, ValueError):
            return int(spec.default_interval)
    return int(spec.default_interval)


def serialize_jobs(settings: dict[str, Any], running_jobs: set[str] | None = None) -> list[dict[str, Any]]:
    running_jobs = running_jobs or set()
    items = []
    for spec in JOB_SPECS:
        if not spec.visible:
            continue
        items.append(
            {
                "id": spec.id,
                "name": spec.name,
                "group": spec.group,
                "default_interval": int(spec.default_interval),
                "interval": interval_for_job(settings, spec.id),
                "running": spec.id in running_jobs or bool(settings.get(f"job_{spec.id}_running", False)),
                "stage": spec.stage,
                "scheduled": spec.scheduled,
            }
        )
    return items


def run_job_callable(job_id: str, settings: dict[str, Any] | None = None):
    spec = get_job_spec(job_id)
    if not spec or spec.runner is None:
        raise KeyError(f"Unknown job: {job_id}")
    return spec.runner(settings or load_settings())
