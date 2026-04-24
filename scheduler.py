import logging
import os
import sys
from pathlib import Path
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in os.sys.path:
    os.sys.path.insert(0, sys_path)

from config.db_utils import load_settings, ensure_dirs, setup_logging

log = logging.getLogger(__name__)


def job_watch_folder():
    try:
        from collectors.watch_folder import scan_all_inboxes
        scan_all_inboxes()
    except Exception as e:
        log.error("watch_folder job failed: %s", e)


def job_telegram():
    try:
        from collectors.telegram_collector import run_collect
        import asyncio
        asyncio.run(run_collect())
    except Exception as e:
        log.error("telegram job failed: %s", e)


def job_tagger():
    try:
        from classifier.tagger_v2 import tag_content_items
        tag_content_items()
    except Exception as e:
        log.error("tagger job failed: %s", e)


def job_asr():
    try:
        from media_pipeline.asr import process_untranscribed_videos
        process_untranscribed_videos()
    except Exception as e:
        log.error("asr job failed: %s", e)


def job_ocr():
    try:
        from media_pipeline.ocr import process_unprocessed_ocr
        process_unprocessed_ocr()
    except Exception as e:
        log.error("ocr job failed: %s", e)


def job_ner():
    try:
        from ner.extractor import process_content_entities
        process_content_entities(batch_size=200)
    except Exception as e:
        log.error("ner job failed: %s", e)


def job_quotes():
    try:
        from claims.quote_extractor import process_content_quotes
        process_content_quotes(batch_size=200)
    except Exception as e:
        log.error("quotes job failed: %s", e)


def job_claims():
    try:
        from verification.engine import process_claims_for_content
        process_claims_for_content()
    except Exception as e:
        log.error("claims job failed: %s", e)


def job_cases():
    try:
        from cases.builder import build_cases_from_entities
        build_cases_from_entities(min_claims=2)
    except Exception as e:
        log.error("cases job failed: %s", e)


def job_youtube():
    try:
        from collectors.youtube_collector import collect_youtube
        collect_youtube()
    except Exception as e:
        log.error("youtube job failed: %s", e)


def job_rss():
    try:
        from collectors.rss_collector import collect_rss
        collect_rss()
    except Exception as e:
        log.error("rss job failed: %s", e)


def job_llm():
    try:
        from classifier.llm_classifier import classify_content
        classify_content(batch_size=20)
    except Exception as e:
        log.error("llm_classifier job failed: %s", e)


def job_accountability():
    try:
        from cases.accountability import compute_all_indices
        compute_all_indices()
    except Exception as e:
        log.error("accountability job failed: %s", e)


def job_official():
    try:
        from collectors.official_scraper import collect_all_official
        collect_all_official()
    except Exception as e:
        log.error("official scraper job failed: %s", e)


def job_entity_resolve():
    try:
        from ner.entity_resolver import resolve_deputies, resolve_all_persons
        resolve_deputies()
        resolve_all_persons()
    except Exception as e:
        log.error("entity_resolver job failed: %s", e)


def job_playwright_official():
    try:
        from collectors.playwright_scraper import collect_all_playwright
        collect_all_playwright()
    except Exception as e:
        log.error("playwright_official job failed: %s", e)


def job_duma_bills():
    try:
        from collectors.playwright_scraper_v2 import collect_bills_playwright
        collect_bills_playwright(pages=2, detail_limit=20, headless=True)
    except Exception as e:
        log.error("duma_bills job failed: %s", e)


def job_minjust():
    try:
        from collectors.minjust_scraper import collect_foreign_agents, collect_undesirable_orgs
        collect_foreign_agents()
        collect_undesirable_orgs()
    except Exception as e:
        log.error("minjust job failed: %s", e)


def job_zakupki():
    try:
        from collectors.zakupki_scraper import collect_contracts_recent
        collect_contracts_recent(pages=3, per_page=20)
    except Exception as e:
        log.error("zakupki job failed: %s", e)


def job_gov():
    try:
        from collectors.gov_scraper import collect_kremlin_acts, collect_government_news
        collect_kremlin_acts(pages=5)
        collect_government_news(pages=3)
    except Exception as e:
        log.error("gov scraper job failed: %s", e)


def job_votes():
    try:
        from collectors.vote_scraper import collect_votes
        collect_votes(pages=3, fetch_details=True)
    except Exception as e:
        log.error("votes job failed: %s", e)


def job_negation():
    try:
        from classifier.negation_handler import process_negations
        process_negations()
    except Exception as e:
        log.error("negation job failed: %s", e)


def job_authenticity():
    try:
        from verification.authenticity_model import recompute_all
        recompute_all()
    except Exception as e:
        log.error("authenticity job failed: %s", e)


def job_relations():
    try:
        from ner.relation_extractor import extract_co_occurrence_relations, extract_head_role_relations
        extract_co_occurrence_relations()
        extract_head_role_relations()
    except Exception as e:
        log.error("relations job failed: %s", e)


def job_risk_patterns():
    try:
        from cases.risk_detector import detect_all_patterns
        detect_all_patterns()
    except Exception as e:
        log.error("risk_patterns job failed: %s", e)


def job_evidence_link():
    try:
        from verification.evidence_linker import auto_link_evidence, auto_link_by_content_type
        auto_link_evidence()
        auto_link_by_content_type()
    except Exception as e:
        log.error("evidence_link job failed: %s", e)


def job_backup():
    try:
        from db.backup import backup_database
        backup_database()
    except Exception as e:
        log.error("backup job failed: %s", e)


def job_senators():
    try:
        from collectors.senators_scraper import collect_senators
        collect_senators(fetch_profiles=True)
    except Exception as e:
        log.error("senators job failed: %s", e)


def job_structural_links():
    try:
        from cases.structural_links import run_all_structural_links
        settings = load_settings()
        run_all_structural_links(settings)
    except Exception as e:
        log.error("structural_links job failed: %s", e)


def job_entity_relation_builder():
    try:
        from analysis.entity_relation_builder import run_all
        run_all()
    except Exception as e:
        log.error("entity_relation_builder job failed: %s", e)


def job_l4_tags():
    try:
        from classifier.analytical_tags import compute_l4_tags_batch
        settings = load_settings()
        conn = get_db(settings)
        stats = compute_l4_tags_batch(conn, limit=1000)
        conn.commit()
        conn.close()
        log.info("L4 tags: %s", stats)
    except Exception as e:
        log.error("l4_tags job failed: %s", e)


def job_re_verifier():
    try:
        from verification.re_verifier import run_reverification
        run_reverification(limit=200)
    except Exception as e:
        log.error("re_verifier job failed: %s", e)


def job_fas_ach_sk():
    try:
        from collectors.fas_ach_sk_scraper import collect_fas, collect_ach, collect_sk
        collect_fas(pages=3, fetch_details=True, detail_limit=20)
        collect_ach(fetch_details=True, detail_limit=15)
        collect_sk(pages=2, fetch_details=True, detail_limit=15)
    except Exception as e:
        log.error("fas_ach_sk job failed: %s", e)


def main():
    settings = load_settings()
    setup_logging(settings)
    ensure_dirs(settings)

    scheduler = BackgroundScheduler()

    scheduler.add_job(
        job_watch_folder,
        IntervalTrigger(seconds=settings.get("watch_folder_interval_seconds", 60)),
        id="watch_folder",
        name="Scan inbox folders",
    )
    log.info("Scheduled: watch_folder every %ds", settings.get("watch_folder_interval_seconds", 60))

    if settings.get("telegram_api_id"):
        scheduler.add_job(
            job_telegram,
            IntervalTrigger(seconds=settings.get("telegram_collect_interval_seconds", 300)),
            id="telegram",
            name="Collect Telegram posts",
        )
        log.info("Scheduled: telegram every %ds", settings.get("telegram_collect_interval_seconds", 300))

    scheduler.add_job(
        job_tagger,
        IntervalTrigger(seconds=settings.get("classification_interval_seconds", 21600)),
        id="tagger",
        name="Classify content tags",
    )
    log.info("Scheduled: tagger every %ds", settings.get("classification_interval_seconds", 21600))

    scheduler.add_job(
        job_asr,
        IntervalTrigger(seconds=3600),
        id="asr",
        name="Transcribe videos",
    )
    log.info("Scheduled: ASR every 3600s")

    scheduler.add_job(
        job_ocr,
        IntervalTrigger(seconds=3600),
        id="ocr",
        name="OCR documents",
    )
    log.info("Scheduled: OCR every 3600s")

    scheduler.add_job(
        job_ner,
        IntervalTrigger(seconds=settings.get("ner_interval_seconds", 7200)),
        id="ner",
        name="NER entity extraction",
    )
    log.info("Scheduled: NER every 7200s")

    scheduler.add_job(
        job_quotes,
        IntervalTrigger(seconds=settings.get("quotes_interval_seconds", 7200)),
        id="quotes",
        name="Quote & rhetoric extraction",
    )
    log.info("Scheduled: quotes every 7200s")

    scheduler.add_job(
        job_claims,
        IntervalTrigger(seconds=settings.get("claims_interval_seconds", 21600)),
        id="claims",
        name="Claim extraction & verification",
    )
    log.info("Scheduled: claims every 21600s")

    scheduler.add_job(
        job_cases,
        IntervalTrigger(seconds=settings.get("cases_interval_seconds", 86400)),
        id="cases",
        name="Case builder",
    )
    log.info("Scheduled: cases every 86400s")

    scheduler.add_job(
        job_youtube,
        IntervalTrigger(seconds=settings.get("youtube_interval_seconds", 86400)),
        id="youtube",
        name="YouTube channel collector",
    )
    log.info("Scheduled: YouTube every 86400s")

    scheduler.add_job(
        job_rss,
        IntervalTrigger(seconds=settings.get("rss_interval_seconds", 3600)),
        id="rss",
        name="RSS media collector",
    )
    log.info("Scheduled: RSS every 3600s")

    scheduler.add_job(
        job_llm,
        IntervalTrigger(seconds=settings.get("llm_interval_seconds", 43200)),
        id="llm",
        name="LLM classifier (Ollama)",
    )
    log.info("Scheduled: LLM classifier every 43200s")

    scheduler.add_job(
        job_accountability,
        IntervalTrigger(seconds=settings.get("accountability_interval_seconds", 86400)),
        id="accountability",
        name="Accountability Index",
    )
    log.info("Scheduled: Accountability Index every 86400s")

    scheduler.add_job(
        job_official,
        IntervalTrigger(seconds=settings.get("official_interval_seconds", 86400)),
        id="official",
        name="Official registries scraper",
    )
    log.info("Scheduled: Official registries every 86400s")

    scheduler.add_job(
        job_entity_resolve,
        IntervalTrigger(seconds=settings.get("entity_resolve_interval_seconds", 43200)),
        id="entity_resolve",
        name="Entity resolution (deputies + all persons)",
    )
    log.info("Scheduled: Entity resolution every 43200s")

    scheduler.add_job(
        job_playwright_official,
        IntervalTrigger(seconds=settings.get("playwright_interval_seconds", 86400)),
        id="playwright_official",
        name="Playwright JS-heavy gov scrapers",
    )
    log.info("Scheduled: Playwright scrapers every 86400s")

    scheduler.add_job(
        job_duma_bills,
        IntervalTrigger(seconds=settings.get("duma_bills_interval_seconds", 86400)),
        id="duma_bills",
        name="Duma bills (Playwright)",
    )
    log.info("Scheduled: Duma bills every 86400s")

    scheduler.add_job(
        job_minjust,
        IntervalTrigger(seconds=settings.get("minjust_interval_seconds", 86400)),
        id="minjust",
        name="Minjust registries (foreign agents, undesirable orgs)",
    )
    log.info("Scheduled: Minjust every 86400s")

    scheduler.add_job(
        job_zakupki,
        IntervalTrigger(seconds=settings.get("zakupki_interval_seconds", 86400)),
        id="zakupki",
        name="Zakupki government contracts",
    )
    log.info("Scheduled: Zakupki every 86400s")

    scheduler.add_job(
        job_gov,
        IntervalTrigger(seconds=settings.get("gov_interval_seconds", 86400)),
        id="gov",
        name="Kremlin + Government acts/news",
    )
    log.info("Scheduled: Gov scrapers every 86400s")

    scheduler.add_job(
        job_votes,
        IntervalTrigger(seconds=settings.get("votes_interval_seconds", 86400)),
        id="votes",
        name="Duma vote sessions (vote.duma.gov.ru)",
    )
    log.info("Scheduled: Votes every 86400s")

    scheduler.add_job(
        job_negation,
        IntervalTrigger(seconds=settings.get("negation_interval_seconds", 43200)),
        id="negation",
        name="Negation & rebuttal detection",
    )
    log.info("Scheduled: Negation every 43200s")

    scheduler.add_job(
        job_authenticity,
        IntervalTrigger(seconds=settings.get("authenticity_interval_seconds", 86400)),
        id="authenticity",
        name="Authenticity model recompute",
    )
    log.info("Scheduled: Authenticity every 86400s")

    scheduler.add_job(
        job_relations,
        IntervalTrigger(seconds=settings.get("relations_interval_seconds", 86400)),
        id="relations",
        name="Entity relation extraction",
    )
    log.info("Scheduled: Entity relations every 86400s")

    scheduler.add_job(
        job_risk_patterns,
        IntervalTrigger(seconds=settings.get("risk_interval_seconds", 86400)),
        id="risk_patterns",
        name="Risk pattern detection",
    )
    log.info("Scheduled: Risk patterns every 86400s")

    scheduler.add_job(
        job_evidence_link,
        IntervalTrigger(seconds=settings.get("evidence_link_interval_seconds", 43200)),
        id="evidence_link",
        name="Automated evidence linking",
    )
    log.info("Scheduled: Evidence linking every 43200s")

    scheduler.add_job(
        job_backup,
        IntervalTrigger(seconds=settings.get("backup_interval_seconds", 86400)),
        id="backup",
        name="Database backup",
    )
    log.info("Scheduled: Backup every 86400s")

    scheduler.add_job(
        job_senators,
        IntervalTrigger(seconds=settings.get("senators_interval_seconds", 604800)),
        id="senators",
        name="Senators (council.gov.ru)",
    )
    log.info("Scheduled: Senators every 604800s (weekly)")

    scheduler.add_job(
        job_structural_links,
        IntervalTrigger(seconds=settings.get("structural_links_interval_seconds", 86400)),
        id="structural_links",
        name="Structural data linking (votes→entities, positions→relations)",
    )
    log.info("Scheduled: Structural links every 86400s")

    scheduler.add_job(
        job_entity_relation_builder,
        IntervalTrigger(seconds=settings.get("entity_relation_builder_interval_seconds", 86400)),
        id="entity_relation_builder",
        name="Entity relation builder (structural relations)",
    )
    log.info("Scheduled: Entity relation builder every 86400s")

    scheduler.add_job(
        job_l4_tags,
        IntervalTrigger(seconds=settings.get("l4_tags_interval_seconds", 43200)),
        id="l4_tags",
        name="L4 analytical tags computation",
    )
    log.info("Scheduled: L4 tags every 43200s")

    scheduler.add_job(
        job_re_verifier,
        IntervalTrigger(seconds=settings.get("re_verifier_interval_seconds", 43200)),
        id="re_verifier",
        name="Re-verify unverified claims with new evidence",
    )
    log.info("Scheduled: Re-verifier every 43200s")

    scheduler.add_job(
        job_fas_ach_sk,
        IntervalTrigger(seconds=settings.get("fas_ach_sk_interval_seconds", 86400)),
        id="fas_ach_sk",
        name="FAS + Accounts Chamber + Investigative Committee",
    )
    log.info("Scheduled: FAS/ACH/SK every 86400s")

    scheduler.start()
    log.info("Scheduler started. Press Ctrl+C to exit.")

    try:
        import time
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        log.info("Shutting down...")
        scheduler.shutdown()


if __name__ == "__main__":
    main()
