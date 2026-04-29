import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from config.db_utils import SCHEMA_PATH


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'TG', 'telegram', 'https://t.me/test', 1),
                (2, 'Docs', 'official_registry', 'https://example.test/doc', 1);

            INSERT INTO content_items(
                id, source_id, external_id, content_type, title, body_text, published_at, url, status
            ) VALUES
                (11, 1, 'tg-11', 'post', 'Пост 11', 'Короткий шумный пост', '2026-04-20T09:00:00', 'https://t.me/test/11', 'raw_signal'),
                (12, 2, 'doc-12', 'restriction_record', 'Документ 12', 'Официальный документ о блокировке', '2026-04-20T08:00:00', 'https://example.test/doc/12', 'official_document'),
                (13, 1, 'tg-13', 'post', 'Пост 13', 'Обновление о блокировке', '2026-04-21T10:00:00', 'https://t.me/test/13', 'raw_signal'),
                (14, 1, 'tg-14', 'post', 'Одиночный пост', 'Отдельный материал без кластера', '2026-04-22T10:00:00', 'https://t.me/test/14', 'raw_signal');

            INSERT INTO content_clusters(
                id, cluster_key, cluster_type, canonical_content_id, canonical_title, item_count, similarity_score,
                representative_score, first_seen_at, last_seen_at, status
            ) VALUES
                (101, 'cluster:telegram-block', 'story', 12, 'Блокировка Telegram', 3, 0.95, 0.96, '2026-04-20T08:00:00', '2026-04-21T10:00:00', 'active');

            INSERT INTO content_cluster_items(cluster_id, content_item_id, similarity_score, reason, is_canonical) VALUES
                (101, 11, 0.92, 'story-merge', 0),
                (101, 12, 1.0, 'canonical', 1),
                (101, 13, 0.90, 'story-update', 0);

            INSERT INTO events(
                id, canonical_title, event_type, summary_short, summary_long, status, event_date_start, event_date_end,
                first_observed_at, last_observed_at, importance_score, confidence
            ) VALUES
                (201, 'Блокировка Telegram', 'internet_block', 'Короткое summary', 'Длинное summary', 'active',
                 '2026-04-20T08:00:00', '2026-04-21T10:00:00', '2026-04-20T08:00:00', '2026-04-21T10:00:00', 0.8, 0.9);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (301, 'organization', 'Правительство РФ'),
                (302, 'organization', 'Telegram'),
                (303, 'person', 'Случайный комментатор');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (301, 11, 'issuer', 1.0),
                (302, 11, 'target', 1.0),
                (301, 12, 'issuer', 1.0),
                (302, 12, 'target', 1.0),
                (302, 13, 'target', 1.0),
                (303, 14, 'subject', 1.0);

            INSERT INTO event_entities(event_id, entity_id, role, confidence) VALUES
                (201, 301, 'issuer', 0.95),
                (201, 302, 'target', 0.95);

            INSERT INTO review_tasks(
                task_key, queue_key, subject_type, subject_id, candidate_payload, suggested_action, confidence,
                machine_reason, status
            ) VALUES
                ('relation:1:low_specificity', 'relations', 'relation_candidate', 1, '{"candidate_id": 1}', 'reject', 0.9, 'low_specificity_entity', 'open');
            """
        )
        conn.commit()
    finally:
        conn.close()


class AiSweepTests(unittest.TestCase):
    def test_detect_failure_kind_separates_rate_from_auth_and_invalid_model(self):
        from analysis.ai_sweep import _detect_failure_kind

        self.assertEqual(_detect_failure_kind('429 {"error":{"message":"Rate limit reached"}}'), "rate")
        self.assertEqual(_detect_failure_kind('400 {"message":"WebSearchTool connector is not supported","type":"invalid_tools"}'), "unsupported_tool")
        self.assertEqual(_detect_failure_kind('400 {"error":{"message":"openrouter:auto:online is not a valid model ID"}}'), "invalid_model")
        self.assertEqual(_detect_failure_kind("provider returned bad response shape: missing output_json"), "bad_response_shape")
        self.assertEqual(_detect_failure_kind("invalid json response from provider"), "invalid_output")
        self.assertEqual(_detect_failure_kind("schema_violation: source-only stage returned external_context"), "schema_violation")
        self.assertEqual(_detect_failure_kind('401 invalid api key'), "auth")

    def test_run_provider_budget_keeps_transient_failures_local_to_item(self):
        from analysis.ai_sweep import RunProviderBudget

        budget = RunProviderBudget(max_failures_per_provider_stage=2)
        priority = ["mistral", "groq", "openrouter"]

        self.assertEqual(budget.allowed_priority("structured_extract", priority), priority)
        for _ in range(10):
            budget.record_failure("structured_extract", "mistral", "timeout")
            budget.record_failure("structured_extract", "mistral", "rate")

        self.assertEqual(budget.allowed_priority("structured_extract", priority), priority)
        self.assertEqual(budget.allowed_priority("tag_reasoning", priority), priority)

    def test_run_provider_budget_keeps_schema_violations_local_to_item(self):
        from analysis.ai_sweep import RunProviderBudget

        budget = RunProviderBudget(max_failures_per_provider_stage=5)
        priority = ["mistral", "groq", "openrouter"]

        for _ in range(20):
            budget.record_failure("structured_extract", "mistral", "schema_violation")

        self.assertEqual(budget.allowed_priority("structured_extract", priority), priority)

    def test_run_provider_budget_hard_skips_unsupported_tool_provider(self):
        from analysis.ai_sweep import RunProviderBudget

        budget = RunProviderBudget(max_failures_per_provider_stage=25)
        priority = ["mistral", "perplexity", "groq"]

        budget.record_failure("event_link_hint", "mistral", "unsupported_tool")

        self.assertEqual(budget.allowed_priority("event_link_hint", priority), ["perplexity", "groq"])
        self.assertEqual(budget.allowed_priority("structured_extract", priority), priority)

    def test_stage_provider_priority_keeps_web_models_out_of_source_only_tags(self):
        from analysis.ai_sweep import _stage_provider_priority

        priority = _stage_provider_priority("tag_reasoning", {})

        self.assertNotIn("perplexity", priority)
        self.assertNotIn("openai", priority)
        self.assertLess(priority.index("mistral"), priority.index("groq"))

    def test_choose_key_for_stage_filters_to_allowed_provider_priority(self):
        from llm.key_pool import bootstrap_provider_catalog, choose_key_for_stage

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                bootstrap_provider_catalog(conn)
                conn.execute(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status)
                    VALUES('perplexity', 'pplx-key', 'pplx-hash', 'active')
                    """
                )
                conn.commit()
                blocked = choose_key_for_stage(conn, stage="structured_extract", provider_priority=["mistral", "groq"])
                allowed = choose_key_for_stage(conn, stage="relation_reasoning", provider_priority=["perplexity"])
            finally:
                conn.close()

            self.assertIsNone(blocked)
            self.assertIsNotNone(allowed)
            self.assertEqual(allowed["provider"], "perplexity")

    def test_backfill_failure_kinds_reclassifies_legacy_provider_model_rows(self):
        from analysis.ai_sweep import backfill_ai_attempt_failure_kinds

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.executescript(
                    """
                    INSERT INTO ai_work_items(id, unit_kind, unit_key, stage, prompt_version, status)
                    VALUES(7001, 'content_item', 'content:12', 'structured_extract', 'ai-sweep-v3-extract', 'completed');
                    INSERT INTO ai_task_attempts(work_item_id, provider, model_name, status, failure_kind, error_text)
                    VALUES(
                        7001,
                        'mistral',
                        'mistral-medium',
                        'failed',
                        'provider_model',
                        '400 {"message":"WebSearchTool connector is not supported","type":"invalid_tools"}'
                    );
                    """
                )
                conn.commit()

                updated = backfill_ai_attempt_failure_kinds(conn)
                row = conn.execute("SELECT failure_kind FROM ai_task_attempts WHERE work_item_id=7001").fetchone()
            finally:
                conn.close()

            self.assertEqual(updated, 1)
            self.assertEqual(row["failure_kind"], "unsupported_tool")

    def test_ai_sweep_doctor_reports_provider_stage_health_without_mutating(self):
        from analysis.ai_sweep import build_ai_sweep_doctor

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status)
                    VALUES
                        ('mistral', 'key-a', 'hash-a', 'active'),
                        ('mistral', 'key-b', 'hash-b', 'removed'),
                        ('groq', 'key-c', 'hash-c', 'cooldown');
                    INSERT INTO ai_work_items(id, unit_kind, unit_key, stage, prompt_version, status)
                    VALUES
                        (7001, 'content_item', 'content:12', 'structured_extract', 'ai-sweep-v3-extract', 'pending'),
                        (7002, 'content_item', 'content:13', 'tag_reasoning', 'ai-sweep-v3-tags', 'failed');
                    INSERT INTO ai_task_attempts(work_item_id, provider, model_name, status, failure_kind, error_text)
                    VALUES
                        (7001, 'mistral', 'mistral-medium', 'failed', 'timeout', 'timeout');
                    """
                )
                conn.commit()
                before = conn.total_changes
                report = build_ai_sweep_doctor({"db_path": str(db_path), "ensure_schema_on_connect": False})
                after = conn.total_changes
            finally:
                conn.close()

            self.assertEqual(before, after)
            self.assertEqual(report["active_keys"]["mistral"], 1)
            self.assertEqual(report["removed_keys"]["mistral"], 1)
            self.assertEqual(report["cooldown_keys"]["groq"], 1)
            self.assertEqual(report["pending_work_items"], 1)
            self.assertEqual(report["failed_work_items"], 1)
            self.assertEqual(report["provider_stage_failures"]["mistral:structured_extract"]["timeout"], 1)

    def test_focused_event_linking_sampling_prioritizes_multi_item_clusters(self):
        from analysis.ai_sweep import canonicalize_units, ensure_ai_sweep_campaign

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.executescript(
                    """
                    INSERT INTO content_items(id, source_id, external_id, content_type, title, body_text, published_at, url, status)
                    VALUES
                        (21, 1, 'tg-21', 'post', 'Ещё один одиночный пост', 'Нет общего события', '2026-04-25T09:00:00', 'https://t.me/test/21', 'raw_signal'),
                        (22, 1, 'tg-22', 'post', 'Повтор истории', 'Та же блокировка', '2026-04-20T11:00:00', 'https://t.me/test/22', 'raw_signal'),
                        (23, 1, 'tg-23', 'post', 'Повтор истории 2', 'Та же блокировка', '2026-04-20T12:00:00', 'https://t.me/test/23', 'raw_signal');
                    INSERT INTO content_cluster_items(cluster_id, content_item_id, similarity_score, reason, is_canonical)
                    VALUES (101, 22, 0.91, 'story-update', 0), (101, 23, 0.90, 'story-update', 0);
                    UPDATE content_clusters SET item_count=5 WHERE id=101;
                    """
                )
                conn.commit()
                units = canonicalize_units(conn)
                campaign = ensure_ai_sweep_campaign(
                    conn,
                    {
                        "ai_sweep": {
                            "campaign_seed": "focused",
                            "campaign_key": "pilot:focused",
                            "pilot_target_units": 2,
                            "pilot_distribution": {"content_cluster": 1, "content_item": 1},
                            "sample_strategy": "focused_event_linking",
                        }
                    },
                    units,
                )
            finally:
                conn.close()

            self.assertEqual(campaign["selection"][0]["unit_kind"], "content_cluster")
            self.assertEqual(campaign["selection"][0]["unit_key"], "cluster:telegram-block")

    def test_source_only_stage_rejects_ungrounded_external_context(self):
        from analysis.ai_sweep import _validate_stage_result

        with self.assertRaisesRegex(ValueError, "schema_violation"):
            _validate_stage_result(
                "clean_factual_text",
                {
                    "output_text": "Текст переписан.",
                    "output_json": {
                        "source_facts": ["Факт из документа"],
                        "external_context": ["Модель добавила факт из поиска"],
                    },
                    "confidence": 0.8,
                },
            )

    def test_worker_uses_deterministic_fallback_when_source_only_stage_has_no_allowed_key(self):
        from analysis.ai_sweep import RunProviderBudget, _worker_run

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO ai_work_items(id, campaign_id, unit_kind, unit_key, stage, prompt_version, input_hash, status, payload_json)
                    VALUES(900, 1, 'content_item', 'content:14', 'structured_extract', 'ai-sweep-v3-extract', 'hash-1', 'running', '{}')
                    """
                )
                conn.commit()
            finally:
                conn.close()

            payload = {
                "title": "Одиночный пост",
                "body_text": "Отдельный материал без кластера",
                "content_type": "post",
                "candidate_events": [],
            }
            result = _worker_run(
                {"db_path": str(db_path), "ensure_schema_on_connect": False},
                {"unit_kind": "content_item", "unit_key": "content:14", "content_item_id": 14},
                "structured_extract",
                "ai-sweep-v3-extract",
                "hash-1",
                payload,
                900,
                RunProviderBudget(max_failures_per_provider_stage=3),
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["provider"], "deterministic")
            self.assertEqual(result["model"], "source-only-fallback")
            self.assertEqual(result["result"]["output_json"]["fallback_reason"], "no_active_keys_for_stage")
            self.assertIn("Одиночный пост", result["result"]["output_text"])

    def test_worker_preserves_provider_failures_before_deterministic_fallback(self):
        from analysis.ai_sweep import RunProviderBudget, _worker_run
        from llm.key_pool import bootstrap_provider_catalog

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                bootstrap_provider_catalog(conn)
                conn.execute(
                    """
                    INSERT INTO llm_keys(provider, api_key, key_hash, status)
                    VALUES('mistral', 'test-key', 'mistral-test-key', 'active')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO ai_work_items(id, campaign_id, unit_kind, unit_key, stage, prompt_version, input_hash, status, payload_json)
                    VALUES(902, 1, 'content_item', 'content:14', 'structured_extract', 'ai-sweep-v3-extract', 'hash-3', 'running', '{}')
                    """
                )
                conn.commit()
            finally:
                conn.close()

            payload = {
                "title": "Одиночный пост",
                "body_text": "Отдельный материал без кластера",
                "content_type": "post",
                "candidate_events": [],
            }

            with patch("analysis.ai_sweep.run_ai_task", side_effect=ValueError("schema_violation: source-only stage returned external_context")):
                result = _worker_run(
                    {"db_path": str(db_path), "ensure_schema_on_connect": False, "ai_sweep": {"max_failures_per_provider_per_item": 1}},
                    {"unit_kind": "content_item", "unit_key": "content:14", "content_item_id": 14},
                    "structured_extract",
                    "ai-sweep-v3-extract",
                    "hash-3",
                    payload,
                    902,
                    RunProviderBudget(max_failures_per_provider_stage=10),
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["provider"], "deterministic")
            self.assertEqual(result["attempts"][0]["provider"], "mistral")
            self.assertEqual(result["attempts"][0]["status"], "failed")
            self.assertEqual(result["attempts"][0]["failure_kind"], "schema_violation")
            self.assertEqual(result["attempts"][-1]["provider"], "deterministic")

    def test_worker_does_not_use_deterministic_fallback_for_relation_reasoning(self):
        from analysis.ai_sweep import RunProviderBudget, _worker_run

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    """
                    INSERT INTO ai_work_items(id, campaign_id, unit_kind, unit_key, stage, prompt_version, input_hash, status, payload_json)
                    VALUES(901, 1, 'review_task', 'review:1', 'relation_reasoning', 'ai-sweep-v3-relations', 'hash-2', 'running', '{}')
                    """
                )
                conn.commit()
            finally:
                conn.close()

            result = _worker_run(
                {"db_path": str(db_path), "ensure_schema_on_connect": False},
                {"unit_kind": "review_task", "unit_key": "review:1", "review_task_id": 1},
                "relation_reasoning",
                "ai-sweep-v3-relations",
                "hash-2",
                {"review_payload": {"candidate_id": 1}},
                901,
                RunProviderBudget(max_failures_per_provider_stage=3),
            )

            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "no_active_keys_for_stage")

    def test_prompt_review_uses_current_prompt_versions_instead_of_stale_v2_text(self):
        from analysis.ai_sweep import PROMPT_VERSIONS, build_ai_sweep_prompt_review

        before = {"campaign_key": "pilot:test", "selected_counts": {"content_item": 1}, "sample_units": []}
        after = {"campaign_key": "pilot:test", "selected_counts": {"content_item": 1}, "sample_units": []}
        diff = {"strict_generic_tag_count_before": 0, "strict_generic_tag_count_after": 0}

        text = build_ai_sweep_prompt_review(before, after, diff)

        self.assertIn(PROMPT_VERSIONS["clean_factual_text"], text)
        self.assertIn(PROMPT_VERSIONS["structured_extract"], text)
        self.assertIn(PROMPT_VERSIONS["tag_reasoning"], text)
        self.assertIn(PROMPT_VERSIONS["event_link_hint"], text)
        self.assertNotIn("ai-sweep-v2-tags", text)

    def test_campaign_sampling_is_deterministic_and_reuses_same_selection(self):
        from analysis.ai_sweep import canonicalize_units, ensure_ai_sweep_campaign

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "ai_sweep": {
                    "campaign_seed": "ai-pilot-2026-04-27",
                    "campaign_key": "pilot:ai-pilot-2026-04-27",
                    "pilot_target_units": 232,
                    "pilot_distribution": {
                        "content_item": 120,
                        "content_cluster": 40,
                        "event": 40,
                        "review_task": 32,
                    },
                },
            }

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                units = canonicalize_units(conn)
                campaign_a = ensure_ai_sweep_campaign(conn, settings, units)
                campaign_b = ensure_ai_sweep_campaign(conn, settings, units)
            finally:
                conn.close()

            self.assertEqual(campaign_a["campaign_id"], campaign_b["campaign_id"])
            self.assertEqual(campaign_a["selection"], campaign_b["selection"])
            self.assertEqual({entry["sample_bucket"] for entry in campaign_a["selection"]}, {"content_item", "content_cluster", "event", "review_task"})
            self.assertEqual(len(campaign_a["selection"]), 4)

    def test_effective_worker_count_autoscales_with_backlog_and_active_keys(self):
        from analysis.ai_sweep import _effective_worker_count
        from llm.key_pool import bootstrap_provider_catalog

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                bootstrap_provider_catalog(conn)
                for index in range(18):
                    conn.execute(
                        """
                        INSERT INTO llm_keys(provider, api_key, key_hash, status, failure_count)
                        VALUES('mistral', ?, ?, 'active', 0)
                        """,
                        (f"mistral-{index}", f"hash-{index}"),
                    )
                conn.commit()

                workers = _effective_worker_count(
                    conn,
                    {
                        "ai_sweep": {
                            "default_worker_count": 12,
                            "min_parallel_workers": 10,
                            "max_parallel_workers": 24,
                        }
                    },
                    pending_items=80,
                )
            finally:
                conn.close()

        self.assertGreaterEqual(workers, 18)

    def test_canonicalize_units_collects_clusters_singletons_events_and_review_tasks(self):
        from analysis.ai_sweep import canonicalize_units

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                units = canonicalize_units(conn)
            finally:
                conn.close()

            kinds = [(unit["unit_kind"], unit["unit_key"]) for unit in units]
            self.assertIn(("content_cluster", "cluster:telegram-block"), kinds)
            self.assertIn(("content_item", "content:14"), kinds)
            self.assertIn(("event", "event:201"), kinds)
            self.assertIn(("review_task", "review:1"), kinds)

    def test_enqueue_ai_work_items_creates_stage_rows_without_duplicates(self):
        from analysis.ai_sweep import enqueue_ai_work_items
        from llm.key_pool import bootstrap_provider_catalog

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                bootstrap_provider_catalog(conn)
                conn.close()

                settings = {
                    "db_path": str(db_path),
                    "ensure_schema_on_connect": True,
                    "ai_sweep": {
                        "campaign_seed": "ai-pilot-2026-04-27",
                        "campaign_key": "pilot:ai-pilot-2026-04-27",
                    },
                }
                first = enqueue_ai_work_items(settings)
                second = enqueue_ai_work_items(settings)

                conn = sqlite3.connect(db_path)
                rows = conn.execute(
                    "SELECT unit_kind, stage, COUNT(*) FROM ai_work_items GROUP BY unit_kind, stage ORDER BY unit_kind, stage"
                ).fetchall()
                work_item_meta = conn.execute(
                    "SELECT campaign_id, prompt_version, input_hash, sample_bucket FROM ai_work_items ORDER BY id LIMIT 1"
                ).fetchone()
            finally:
                conn.close()

            self.assertGreater(first["items_new"], 0)
            self.assertEqual(second["items_new"], 0)
            self.assertIsNotNone(work_item_meta[0])
            self.assertTrue(str(work_item_meta[1]).startswith("ai-sweep-v"))
            self.assertTrue(work_item_meta[2])
            self.assertTrue(work_item_meta[3])
            self.assertTrue(any(row[0] == "content_cluster" and row[1] == "clean_factual_text" for row in rows))
            self.assertTrue(any(row[0] == "event" and row[1] == "event_synthesis" for row in rows))

    def test_enqueue_ai_work_items_skips_completed_stage_for_same_campaign_and_prompt(self):
        from analysis.ai_sweep import enqueue_ai_work_items

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "ai_sweep": {
                    "campaign_seed": "ai-pilot-2026-04-27",
                    "campaign_key": "pilot:ai-pilot-2026-04-27",
                },
            }

            first = enqueue_ai_work_items(settings)
            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT id, prompt_version, input_hash
                    FROM ai_work_items
                    WHERE unit_kind='content_item' AND stage='clean_factual_text'
                    ORDER BY id LIMIT 1
                    """
                ).fetchone()
                conn.execute(
                    """
                    UPDATE ai_work_items
                    SET status='completed', completed_at='2026-04-27T12:00:00'
                    WHERE id=?
                    """,
                    (int(row[0]),),
                )
                conn.commit()
            finally:
                conn.close()

            second = enqueue_ai_work_items(settings)
            conn = sqlite3.connect(db_path)
            try:
                refreshed = conn.execute(
                    "SELECT status, prompt_version, input_hash, completed_at FROM ai_work_items WHERE id=?",
                    (int(row[0]),),
                ).fetchone()
            finally:
                conn.close()

            self.assertGreater(first["items_new"], 0)
            self.assertEqual(second["items_new"], 0)
            self.assertEqual(refreshed[0], "completed")
            self.assertEqual(refreshed[1], row[1])
            self.assertEqual(refreshed[2], row[2])
            self.assertEqual(refreshed[3], "2026-04-27T12:00:00")

    def test_enqueue_ai_work_items_versions_stage_when_prompt_version_changes(self):
        from analysis.ai_sweep import enqueue_ai_work_items

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "ai_sweep": {
                    "campaign_seed": "ai-pilot-2026-04-27",
                    "campaign_key": "pilot:ai-pilot-2026-04-27",
                },
            }
            enqueue_ai_work_items(settings)

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT id, prompt_version
                    FROM ai_work_items
                    WHERE unit_kind='content_item' AND stage='clean_factual_text'
                    ORDER BY id LIMIT 1
                    """
                ).fetchone()
                conn.execute(
                    """
                    UPDATE ai_work_items
                    SET status='completed', completed_at='2026-04-27T12:00:00'
                    WHERE id=?
                    """,
                    (int(row[0]),),
                )
                conn.commit()
            finally:
                conn.close()

            changed_settings = {
                **settings,
                "ai_sweep": {
                    **settings["ai_sweep"],
                    "prompt_versions": {"clean_factual_text": "ai-sweep-v4-cleaner"},
                },
            }
            changed = enqueue_ai_work_items(changed_settings)

            conn = sqlite3.connect(db_path)
            try:
                original = conn.execute(
                    "SELECT status, prompt_version, completed_at FROM ai_work_items WHERE id=?",
                    (int(row[0]),),
                ).fetchone()
                versions = conn.execute(
                    """
                    SELECT status, prompt_version, completed_at
                    FROM ai_work_items
                    WHERE unit_kind='content_item' AND stage='clean_factual_text'
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertGreater(changed["items_new"], 0)
            self.assertEqual(original[0], "completed")
            self.assertEqual(original[1], row[1])
            self.assertEqual(original[2], "2026-04-27T12:00:00")
            self.assertTrue(any(version[1] == "ai-sweep-v4-cleaner" and version[0] == "pending" for version in versions))

    def test_enqueue_ai_work_items_isolates_attempts_by_campaign(self):
        from analysis.ai_sweep import enqueue_ai_work_items

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "ai_sweep": {
                    "campaign_seed": "ai-pilot-2026-04-27",
                    "campaign_key": "pilot:ai-pilot-2026-04-27",
                },
            }
            enqueue_ai_work_items(settings)
            conn = sqlite3.connect(db_path)
            try:
                first_row = conn.execute(
                    """
                    SELECT id, campaign_id
                    FROM ai_work_items
                    WHERE unit_kind='content_item' AND stage='clean_factual_text'
                    ORDER BY id LIMIT 1
                    """
                ).fetchone()
                conn.execute(
                    """
                    INSERT INTO ai_task_attempts(work_item_id, provider, model_name, status, failure_kind, error_text)
                    VALUES(?, 'mistral', 'mistral-test', 'failed', 'timeout', 'timeout')
                    """,
                    (int(first_row[0]),),
                )
                conn.commit()
            finally:
                conn.close()

            second_settings = {
                **settings,
                "ai_sweep": {
                    **settings["ai_sweep"],
                    "campaign_seed": "ai-pilot-2026-04-29",
                    "campaign_key": "pilot:ai-pilot-2026-04-29",
                },
            }
            enqueue_ai_work_items(second_settings)

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT id, campaign_id
                    FROM ai_work_items
                    WHERE unit_kind='content_item' AND stage='clean_factual_text'
                    ORDER BY id
                    """
                ).fetchall()
                attempt_campaign = conn.execute(
                    """
                    SELECT aw.campaign_id
                    FROM ai_task_attempts ata
                    JOIN ai_work_items aw ON aw.id=ata.work_item_id
                    WHERE ata.id=1
                    """
                ).fetchone()[0]
            finally:
                conn.close()

            campaign_ids = {int(row[1]) for row in rows}
            self.assertGreaterEqual(len(campaign_ids), 2)
            self.assertEqual(int(attempt_campaign), int(first_row[1]))

    def test_enqueue_ai_work_items_keeps_event_synthesis_completed_when_only_generated_summary_changed(self):
        from analysis.ai_sweep import enqueue_ai_work_items

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "ai_sweep": {
                    "campaign_seed": "ai-pilot-2026-04-27",
                    "campaign_key": "pilot:ai-pilot-2026-04-27",
                },
            }
            enqueue_ai_work_items(settings)

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT id
                    FROM ai_work_items
                    WHERE unit_kind='event' AND stage='event_synthesis'
                    ORDER BY id LIMIT 1
                    """
                ).fetchone()
                conn.execute(
                    """
                    UPDATE ai_work_items
                    SET status='completed', completed_at='2026-04-27T12:00:00'
                    WHERE id=?
                    """,
                    (int(row[0]),),
                )
                conn.execute(
                    """
                    UPDATE events
                    SET summary_short='Новая сводка', summary_long='Новая длинная сводка'
                    WHERE id=201
                    """
                )
                conn.commit()
            finally:
                conn.close()

            second = enqueue_ai_work_items(settings)

            conn = sqlite3.connect(db_path)
            try:
                refreshed = conn.execute(
                    "SELECT status, completed_at FROM ai_work_items WHERE id=?",
                    (int(row[0]),),
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(second["items_reset"], 0)
            self.assertEqual(refreshed[0], "completed")
            self.assertEqual(refreshed[1], "2026-04-27T12:00:00")

    def test_run_ai_full_sweep_writes_derivations_event_candidates_and_metadata(self):
        from analysis.ai_sweep import run_ai_full_sweep
        from llm.key_pool import bootstrap_provider_catalog, import_keys_from_file

        def fake_run_ai_task(*, conn, provider, model, api_key, task):
            stage = task["stage"]
            unit = task["unit"]
            if stage == "clean_factual_text":
                return {
                    "provider": provider,
                    "model": model,
                    "output_text": f"Чистый factual текст для {unit['unit_key']}",
                    "output_json": {"cleaned": True},
                    "confidence": 0.91,
                    "citations": ["https://example.test/doc/12"],
                }
            if stage == "structured_extract":
                return {
                    "provider": provider,
                    "model": model,
                    "output_text": "",
                    "output_json": {
                        "actors": [{"name": "Правительство РФ", "role": "issuer"}],
                        "organizations": [{"name": "Telegram"}],
                        "actions": ["block_start"],
                    },
                    "confidence": 0.88,
                    "citations": ["https://example.test/doc/12"],
                }
            if stage == "event_link_hint":
                return {
                    "provider": provider,
                    "model": model,
                    "output_text": "merge into telegram block event",
                    "output_json": {
                        "action": "link_existing_event",
                        "event_id": 201,
                        "reason": "same legal anchor",
                    },
                    "confidence": 0.84,
                    "citations": ["https://example.test/doc/12"],
                }
            if stage == "tag_reasoning":
                return {
                    "provider": provider,
                    "model": model,
                    "output_text": "restriction tags supported by official document",
                    "output_json": {"tags": ["restriction/internet", "document/official"]},
                    "confidence": 0.83,
                    "citations": ["https://example.test/doc/12"],
                }
            if stage == "relation_reasoning":
                return {
                    "provider": provider,
                    "model": model,
                    "output_text": "official restriction path",
                    "output_json": {"bridge_types": ["Event", "RestrictionEvent", "OfficialDocument"]},
                    "confidence": 0.85,
                    "citations": ["https://example.test/doc/12"],
                }
            if stage == "event_synthesis":
                return {
                    "provider": provider,
                    "model": model,
                    "output_text": "Событие синтезировано",
                    "output_json": {
                        "summary_short": "Правительство ограничило доступ к Telegram.",
                        "summary_long": "Правительство приняло решение об ограничении доступа, после чего начались жалобы пользователей.",
                        "timeline": [
                            {"date": "2026-04-20T08:00:00", "title": "Принято решение"},
                            {"date": "2026-04-21T10:00:00", "title": "Пошли жалобы"},
                        ],
                        "participants": [{"name": "Правительство РФ", "role": "issuer"}],
                    },
                    "confidence": 0.89,
                    "citations": ["https://example.test/doc/12"],
                }
            raise AssertionError(f"Unexpected stage: {stage}")

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            key_path = Path(tmp) / "key.json"
            create_db(db_path)
            key_path.write_text(
                "{\n"
                "  }\n"
                "\"keys\": [\n"
                "  {\"provider\": \"groq\", \"api_key\": \"fake-groq-key-1\", \"status\": \"active\"},\n"
                "  {\"provider\": \"perplexity\", \"api_key\": \"fake-perplexity-key-1\", \"status\": \"active\"},\n"
                "  {\"provider\": \"openai\", \"api_key\": \"fake-openai-key-1\", \"status\": \"active\"}\n"
                "]\n"
                "}\n",
                encoding="utf-8",
            )

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                bootstrap_provider_catalog(conn)
                import_keys_from_file(conn, key_path)
            finally:
                conn.close()

            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "project_root": str(Path(tmp)),
                "ai_sweep": {
                    "campaign_seed": "ai-pilot-2026-04-27",
                    "campaign_key": "pilot:ai-pilot-2026-04-27",
                    "key_file": str(key_path),
                    "default_worker_count": 2,
                    "min_parallel_workers": 2,
                    "max_parallel_workers": 4,
                    "max_units_per_run": 4,
                },
            }

            with patch("analysis.ai_sweep.run_ai_task", side_effect=fake_run_ai_task):
                result = run_ai_full_sweep(settings)

            conn = sqlite3.connect(db_path)
            try:
                derivation_types = conn.execute(
                    "SELECT DISTINCT derivation_type FROM content_derivations ORDER BY derivation_type"
                ).fetchall()
                event_candidate = conn.execute(
                    """
                    SELECT suggested_event_id, candidate_state, suggestion_json
                    FROM event_candidates
                    WHERE candidate_state='link_existing'
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                event_row = conn.execute(
                    "SELECT summary_short, summary_long FROM events WHERE id=201"
                ).fetchone()
                metadata_row = conn.execute(
                    "SELECT value_json FROM runtime_metadata WHERE key='ai_sweep_latest_report'"
                ).fetchone()
                attempts = conn.execute(
                    "SELECT COUNT(*) FROM ai_task_attempts"
                ).fetchone()[0]
                tag_votes = conn.execute(
                    """
                    SELECT tag_name, vote_value, signal_layer
                    FROM content_tag_votes
                    WHERE voter_name LIKE 'ai_sweep:%'
                    ORDER BY tag_name
                    """
                ).fetchall()
                final_ai_tags = conn.execute(
                    "SELECT COUNT(*) FROM content_tags WHERE COALESCE(decision_source, '') LIKE 'ai_sweep:%'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertGreaterEqual(result["items_seen"], 4)
            self.assertIn(("clean_factual_text",), derivation_types)
            self.assertIn(("structured_extract",), derivation_types)
            self.assertIn(("tag_reasoning",), derivation_types)
            self.assertIn(("relation_reasoning",), derivation_types)
            self.assertEqual(event_candidate[0], 201)
            self.assertEqual(event_candidate[1], "link_existing")
            self.assertIn("same legal anchor", event_candidate[2])
            self.assertIn("Telegram", event_row[0] or "")
            self.assertIn("жалобы", event_row[1] or "")
            self.assertIsNotNone(metadata_row)
            self.assertGreater(attempts, 0)
            self.assertIn(("document/official", "supported", "cleaned"), tag_votes)
            self.assertIn(("restriction/internet", "supported", "cleaned"), tag_votes)
            self.assertEqual(final_ai_tags, 0)

    def test_current_derivation_selector_prefers_latest_ready_prompt_without_losing_history(self):
        from analysis.ai_sweep import current_derivations_for_content

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute(
                    """
                    INSERT INTO content_derivations(
                        content_item_id, derivation_type, model_provider, model_name, prompt_version,
                        input_hash, output_text, output_json, confidence, status, is_current, updated_at
                    ) VALUES(14, 'tag_reasoning', 'perplexity', 'sonar', 'ai-sweep-v1-tags',
                             'hash-v1', 'old noisy', '{"tags":["technology"]}', 0.9, 'ready', 0, '2026-04-27T10:00:00')
                    """
                )
                conn.execute(
                    """
                    INSERT INTO content_derivations(
                        content_item_id, derivation_type, model_provider, model_name, prompt_version,
                        input_hash, output_text, output_json, confidence, status, is_current, updated_at
                    ) VALUES(14, 'tag_reasoning', 'mistral', 'medium', 'ai-sweep-v2-tags',
                             'hash-v2', 'strict current', '{"tags":[]}', 0.5, 'ready', 1, '2026-04-27T11:00:00')
                    """
                )
                conn.commit()

                current = current_derivations_for_content(conn, 14)
                all_rows = conn.execute(
                    "SELECT COUNT(*) FROM content_derivations WHERE content_item_id=14"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(all_rows, 2)
            self.assertEqual(current["tag_reasoning"]["prompt_version"], "ai-sweep-v2-tags")
            self.assertEqual(current["tag_reasoning"]["output_text"], "strict current")

    def test_record_attempts_persists_failure_kind_for_provider_health_audit(self):
        from analysis.ai_sweep import _record_attempts

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    "INSERT INTO llm_keys(id, provider, api_key, key_hash, status) VALUES(1, 'groq', 'g', 'g-hash', 'active')"
                )
                conn.execute(
                    """
                    INSERT INTO ai_work_items(id, unit_kind, unit_key, stage, prompt_version, status)
                    VALUES(1, 'content_item', 'content:14', 'tag_reasoning', 'ai-sweep-v2-tags', 'running')
                    """
                )
                conn.commit()

                _record_attempts(
                    conn,
                    1,
                    [
                        {
                            "provider": "groq",
                            "model": "groq/compound",
                            "key_id": 1,
                            "status": "failed",
                            "error_text": "429 rate limit",
                            "failure_kind": "rate",
                        }
                    ],
                )
                row = conn.execute(
                    "SELECT provider, status, failure_kind FROM ai_task_attempts WHERE work_item_id=1"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(tuple(row), ("groq", "failed", "rate"))

    def test_record_attempts_backfills_missing_failure_kind_from_error_text(self):
        from analysis.ai_sweep import _record_attempts

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            try:
                conn.execute(
                    "INSERT INTO llm_keys(id, provider, api_key, key_hash, status) VALUES(1, 'mistral', 'm', 'm-hash', 'active')"
                )
                conn.execute(
                    """
                    INSERT INTO ai_work_items(id, unit_kind, unit_key, stage, prompt_version, status)
                    VALUES(2, 'content_item', 'content:14', 'structured_extract', 'ai-sweep-v3-extract', 'running')
                    """
                )
                conn.commit()

                _record_attempts(
                    conn,
                    2,
                    [
                        {
                            "provider": "mistral",
                            "model": "mistral-medium",
                            "key_id": 1,
                            "status": "failed",
                            "error_text": "ReadTimeout: request timed out after 30 seconds",
                        }
                    ],
                )
                row = conn.execute(
                    "SELECT provider, status, failure_kind FROM ai_task_attempts WHERE work_item_id=2"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(tuple(row), ("mistral", "failed", "timeout"))

    def test_event_link_hint_normalizes_merge_review_and_enqueues_event_review_task(self):
        from analysis.ai_sweep import _persist_event_candidate

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                updated = _persist_event_candidate(
                    conn,
                    {"unit_kind": "content_item", "unit_key": "content:14", "content_item_id": 14},
                    "ai-sweep-v2-event-link",
                    {
                        "provider": "mistral",
                        "model": "mistral-medium",
                        "confidence": 0.77,
                        "output_json": {
                            "action": "merge_review",
                            "event_id": 201,
                            "reason": "same actors and legal anchor but weak time window",
                        },
                    },
                )
                conn.commit()
                candidate = conn.execute(
                    "SELECT candidate_state, suggested_event_id FROM event_candidates ORDER BY id DESC LIMIT 1"
                ).fetchone()
                review = conn.execute(
                    """
                    SELECT queue_key, subject_type, suggested_action, machine_reason
                    FROM review_tasks
                    WHERE queue_key='events'
                    ORDER BY id DESC LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(updated, 1)
            self.assertEqual(tuple(candidate), ("merge_review", 201))
            self.assertEqual(tuple(review), ("events", "event_candidate", "needs_review", "same actors and legal anchor but weak time window"))

    def test_event_link_hint_demotes_link_existing_when_deterministic_gates_fail(self):
        from analysis.ai_sweep import _persist_event_candidate

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                updated = _persist_event_candidate(
                    conn,
                    {"unit_kind": "content_item", "unit_key": "content:14", "content_item_id": 14},
                    "ai-sweep-v3-event-link",
                    {
                        "provider": "mistral",
                        "model": "mistral-medium",
                        "confidence": 0.91,
                        "output_json": {
                            "action": "link_existing_event",
                            "event_id": 201,
                            "reason": "same broad topic",
                        },
                    },
                )
                conn.commit()
                candidate = conn.execute(
                    "SELECT candidate_state, suggested_event_id, suggestion_json FROM event_candidates ORDER BY id DESC LIMIT 1"
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(updated, 1)
            self.assertEqual(candidate["candidate_state"], "standalone")
            self.assertIsNone(candidate["suggested_event_id"])
            self.assertIn("gate_failed", candidate["suggestion_json"])

    def test_event_link_hint_promotes_create_candidate_to_existing_when_candidate_context_passes_gates(self):
        from analysis.ai_sweep import _build_unit_context, _candidate_events_for_unit, _persist_event_candidate

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                unit = {
                    "unit_kind": "content_cluster",
                    "unit_key": "cluster:telegram-block",
                    "cluster_id": 101,
                    "canonical_content_id": 12,
                }
                context = _build_unit_context(conn, unit)
                payload = {
                    "candidate_events": _candidate_events_for_unit(conn, unit, context),
                    "title": context["content_row"]["title"],
                    "body_text": context["content_row"]["body_text"],
                    "published_at": context["content_row"]["published_at"],
                }
                updated = _persist_event_candidate(
                    conn,
                    unit,
                    "ai-sweep-v3-event-link",
                    {
                        "provider": "mistral",
                        "model": "mistral-medium",
                        "confidence": 0.72,
                        "output_json": {
                            "action": "create_event_candidate",
                            "reason": "model was conservative",
                            "external_context": [],
                        },
                    },
                    payload=payload,
                )
                conn.commit()
                candidate = conn.execute(
                    "SELECT candidate_state, suggested_event_id, suggestion_json FROM event_candidates ORDER BY id DESC LIMIT 1"
                ).fetchone()
                suggestion = json.loads(candidate["suggestion_json"])
            finally:
                conn.close()

            self.assertEqual(updated, 1)
            self.assertEqual(candidate["candidate_state"], "link_existing")
            self.assertEqual(candidate["suggested_event_id"], 201)
            self.assertTrue(suggestion["deterministic_override"]["accepted"])
            self.assertEqual(suggestion["deterministic_override"]["from_action"], "create_event_candidate")

    def test_candidate_events_include_timeline_roles_facts_and_document_context(self):
        from analysis.ai_sweep import _build_unit_context, _candidate_events_for_unit

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.executescript(
                    """
                    INSERT INTO event_timeline(event_id, timeline_date, title, description, sort_order)
                    VALUES(201, '2026-04-20T08:00:00', 'Решение о блокировке', 'Официальное решение', 1);
                    INSERT INTO event_facts(event_id, fact_type, canonical_text, confidence)
                    VALUES(201, 'restriction', 'Правительство ограничило доступ к Telegram', 0.9);
                    INSERT INTO fact_evidence(fact_id, content_item_id, evidence_class, source_strength)
                    SELECT id, 12, 'hard', 1.0 FROM event_facts WHERE event_id=201;
                    """
                )
                conn.commit()
                unit = {
                    "unit_kind": "content_item",
                    "unit_key": "content:12",
                    "content_item_id": 12,
                    "canonical_content_id": 12,
                    "title": "Документ 12",
                    "published_at": "2026-04-20T08:00:00",
                }
                context = _build_unit_context(conn, unit)
                candidates = _candidate_events_for_unit(conn, unit, context)
            finally:
                conn.close()

            self.assertTrue(candidates)
            candidate = candidates[0]
            self.assertEqual(candidate["event_id"], 201)
            self.assertIn("timeline_anchors", candidate)
            self.assertIn("entity_roles", candidate)
            self.assertIn("facts", candidate)
            self.assertIn("official_docs", candidate)

    def test_candidate_events_do_not_drop_relevant_late_event_after_first_200_rows(self):
        from analysis.ai_sweep import _build_unit_context, _candidate_events_for_unit

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                for index in range(300):
                    event_id = 1000 + index
                    conn.execute(
                        """
                        INSERT INTO events(
                            id, canonical_title, event_type, summary_short, status,
                            event_date_start, event_date_end, first_observed_at, last_observed_at, confidence
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            event_id,
                            f"Нерелевантное старое событие {index}",
                            "background",
                            "Нет совпадений",
                            "active",
                            "2026-01-01T00:00:00",
                            "2026-01-01T01:00:00",
                            "2026-01-01T00:00:00",
                            "2026-01-01T01:00:00",
                            0.5,
                        ),
                    )
                conn.execute(
                    """
                    INSERT INTO events(
                        id, canonical_title, event_type, summary_short, status,
                        event_date_start, event_date_end, first_observed_at, last_observed_at, confidence
                    ) VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        5000,
                        "Отдельный материал без кластера",
                        "public_statement",
                        "Случайный комментатор сделал отдельное заявление",
                        "active",
                        "2026-04-22T10:00:00",
                        "2026-04-22T10:00:00",
                        "2026-04-22T10:00:00",
                        "2026-04-22T10:00:00",
                        0.8,
                    ),
                )
                conn.execute(
                    "INSERT INTO event_entities(event_id, entity_id, role, confidence) VALUES(5000, 303, 'commentator', 0.9)"
                )
                conn.commit()

                unit = {
                    "unit_kind": "content_item",
                    "unit_key": "content:14",
                    "content_item_id": 14,
                    "canonical_content_id": 14,
                    "title": "Одиночный пост",
                    "published_at": "2026-04-22T10:00:00",
                }
                context = _build_unit_context(conn, unit)
                candidates = _candidate_events_for_unit(conn, unit, context)
            finally:
                conn.close()

            self.assertTrue(any(candidate["event_id"] == 5000 for candidate in candidates))

    def test_run_ai_full_sweep_tolerates_invalid_event_link_id_from_model(self):
        from analysis.ai_sweep import run_ai_full_sweep
        from llm.key_pool import bootstrap_provider_catalog, import_keys_from_file

        def fake_run_ai_task(*, conn, provider, model, api_key, task):
            stage = task["stage"]
            if stage == "event_link_hint":
                return {
                    "provider": provider,
                    "model": model,
                    "output_text": "link to missing event",
                    "output_json": {
                        "action": "link_existing_event",
                        "event_id": 999999,
                        "reason": "bad model guess",
                    },
                    "confidence": 0.81,
                }
            return {
                "provider": provider,
                "model": model,
                "output_text": "ok",
                "output_json": {},
                "confidence": 0.6,
            }

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "ai.db"
            key_path = Path(tmp) / "key.json"
            create_db(db_path)
            key_path.write_text(
                "{\n"
                "\"keys\": [\n"
                "  {\"provider\": \"groq\", \"api_key\": \"fake-groq-key-1\", \"status\": \"active\"}\n"
                "]\n"
                "}\n",
                encoding="utf-8",
            )

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                bootstrap_provider_catalog(conn)
                import_keys_from_file(conn, key_path)
            finally:
                conn.close()

            settings = {
                "db_path": str(db_path),
                "ensure_schema_on_connect": True,
                "project_root": str(Path(tmp)),
                "reports_dir": str(Path(tmp)),
                "ai_sweep": {
                    "campaign_seed": "ai-pilot-2026-04-27",
                    "campaign_key": "pilot:ai-pilot-2026-04-27",
                    "key_file": str(key_path),
                    "default_worker_count": 1,
                    "min_parallel_workers": 1,
                    "max_parallel_workers": 1,
                    "max_units_per_run": 1,
                },
            }

            with patch("analysis.ai_sweep.run_ai_task", side_effect=fake_run_ai_task):
                result = run_ai_full_sweep(settings)

            conn = sqlite3.connect(db_path)
            try:
                candidate = conn.execute(
                    """
                    SELECT suggested_event_id, candidate_state, suggestion_json
                    FROM event_candidates
                    ORDER BY id DESC
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertIsNotNone(candidate)
            self.assertIsNone(candidate[0])
            self.assertIn("invalid_event_id", candidate[2])


if __name__ == "__main__":
    unittest.main()
