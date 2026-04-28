import sqlite3
import tempfile
import unittest
from pathlib import Path

from quality.pipeline_gate import build_quality_gate
from runtime.state import set_runtime_metadata


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_quality_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class QualityGateTests(unittest.TestCase):
    def test_quality_gate_fails_on_zero_support_duplicates_and_degraded_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quality.db"
            report_path = Path(tmp) / "qa_quality_latest.json"
            create_quality_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, url, is_active) VALUES
                        (1, 'Official', 'official_site', 'https://official.example.test', 1);

                    INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                        (101, 1, 'restriction_record', 'Restriction', 'Restriction body', 'raw_signal'),
                        (102, 1, 'post', 'CTA', 'Подпишитесь на наш канал', 'suppressed_template');

                    INSERT INTO content_tags(
                        id, content_item_id, tag_level, tag_name, namespace, normalized_tag,
                        confidence, confidence_calibrated, tag_source, decision_source
                    ) VALUES
                        (301, 101, 0, 'technology', 'topic', 'technology', 0.9, 0.9, 'classifier_v3', 'classifier_v3');

                    INSERT INTO claims(id, content_item_id, claim_text, status) VALUES
                        (401, 102, 'Политический лозунг', 'unverified');

                    INSERT INTO relation_candidates(
                        id, entity_a_id, entity_b_id, candidate_type, origin, score,
                        support_items, support_sources, support_domains, candidate_state, promotion_state
                    ) VALUES
                        (501, 1, 2, 'same_case_cluster', 'candidate_builder:hybrid', 0.55, 0, 0, 0, 'review', 'review');

                    INSERT INTO source_sync_state(source_key, state, quality_state, failure_class, last_error)
                    VALUES('government_news', 'degraded', 'degraded', 'timeout', 'ConnectTimeout: https://government.ru/news/');

                    INSERT INTO job_runs(job_id, trigger_mode, requested_by, owner, attempt_no, status, started_at, warnings_json)
                    VALUES('photo_backfill', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[\"map.svg\"]');
                    """
                )
                set_runtime_metadata(conn, "classifier_audit_last_status", "ok")
                set_runtime_metadata(conn, "classifier_audit_last_report", {"reviewed_baseline_ready": True})
                conn.commit()
            finally:
                conn.close()

            result = build_quality_gate(
                {
                    "db_path": str(db_path),
                    "ensure_schema_on_connect": True,
                    "quality_gate": {
                        "report_path": str(report_path),
                        "max_degraded_sources": 0,
                        "max_zero_support_review_candidates": 0,
                        "max_duplicate_leakage": 0,
                        "strict_gate": True,
                    },
                }
            )

            self.assertFalse(result["ok"])
            self.assertIn("relation_quality_gate_failed", result["fatal_errors"])
            self.assertIn("dedupe_leak_gate_failed", result["fatal_errors"])
            self.assertIn("source_health_gate_failed", result["fatal_errors"])
            self.assertTrue(report_path.exists())
            self.assertEqual(result["artifacts"]["relation_quality"]["zero_support_review_candidates"], 1)
            self.assertEqual(result["artifacts"]["dedupe_leakage"]["total"], 1)
            self.assertEqual(result["artifacts"]["source_health"]["degraded_count"], 1)
            self.assertEqual(result["artifacts"]["top_false_positive_tags"][0]["tag_name"], "technology")

    def test_quality_gate_passes_when_quality_signals_are_clean(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quality.db"
            report_path = Path(tmp) / "qa_quality_latest.json"
            create_quality_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO job_runs(job_id, trigger_mode, requested_by, owner, attempt_no, status, started_at, warnings_json)
                    VALUES('tagger', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[]');
                    """
                )
                set_runtime_metadata(conn, "classifier_audit_last_status", "ok")
                set_runtime_metadata(conn, "classifier_audit_last_report", {"reviewed_baseline_ready": True})
                conn.commit()
            finally:
                conn.close()

            result = build_quality_gate(
                {
                    "db_path": str(db_path),
                    "ensure_schema_on_connect": True,
                    "quality_gate": {
                        "report_path": str(report_path),
                        "strict_gate": True,
                    },
                }
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["fatal_errors"], [])
            self.assertEqual(result["artifacts"]["relation_quality"]["zero_support_review_candidates"], 0)
            self.assertEqual(result["artifacts"]["dedupe_leakage"]["total"], 0)
            self.assertEqual(result["artifacts"]["source_health"]["degraded_count"], 0)

    def test_quality_gate_fails_on_latest_critical_warning_job_even_without_degraded_source_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quality.db"
            report_path = Path(tmp) / "qa_quality_latest.json"
            create_quality_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO job_runs(job_id, trigger_mode, requested_by, owner, attempt_no, status, started_at, warnings_json)
                    VALUES
                        ('photo_backfill', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[\"map.svg\"]'),
                        ('tagger', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[]');
                    """
                )
                set_runtime_metadata(conn, "classifier_audit_last_status", "ok")
                set_runtime_metadata(conn, "classifier_audit_last_report", {"reviewed_baseline_ready": True})
                conn.commit()
            finally:
                conn.close()

            result = build_quality_gate(
                {
                    "db_path": str(db_path),
                    "ensure_schema_on_connect": True,
                    "quality_gate": {
                        "report_path": str(report_path),
                        "strict_gate": True,
                    },
                }
            )

            self.assertFalse(result["ok"])
            self.assertIn("source_health_gate_failed", result["fatal_errors"])
            self.assertEqual(len(result["artifacts"]["critical_warning_jobs"]), 1)
            self.assertEqual(result["artifacts"]["critical_warning_jobs"][0]["failure_class"], "bad_asset")

    def test_quality_gate_allows_non_required_source_warning_when_manifest_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quality.db"
            report_path = Path(tmp) / "qa_quality_latest.json"
            manifest_path = Path(tmp) / "source_health_manifest.json"
            create_quality_db(db_path)
            manifest_path.write_text(
                """
                {
                  "state_company_reports:Газпром": {
                    "acceptance_mode": "direct_only",
                    "required_for_gate": false,
                    "warning_match": ["Газпром:", "www.gazprom.ru"]
                  }
                }
                """,
                encoding="utf-8",
            )

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO source_sync_state(source_key, state, quality_state, failure_class, last_error)
                    VALUES('state_company_reports:Газпром', 'degraded', 'ok', '', 'ConnectTimeout: https://www.gazprom.ru/about/management/');

                    INSERT INTO job_runs(job_id, trigger_mode, requested_by, owner, attempt_no, status, started_at, warnings_json)
                    VALUES
                        ('state_company_reports', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[\"Газпром: ConnectTimeout on https://www.gazprom.ru/about/management/\"]'),
                        ('tagger', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[]');
                    """
                )
                set_runtime_metadata(conn, "classifier_audit_last_status", "ok")
                set_runtime_metadata(conn, "classifier_audit_last_report", {"reviewed_baseline_ready": True})
                conn.commit()
            finally:
                conn.close()

            result = build_quality_gate(
                {
                    "db_path": str(db_path),
                    "ensure_schema_on_connect": True,
                    "source_health_manifest_path": str(manifest_path),
                    "quality_gate": {
                        "report_path": str(report_path),
                        "strict_gate": True,
                    },
                }
            )

            self.assertTrue(result["ok"])
            self.assertEqual(result["fatal_errors"], [])
            self.assertEqual(len(result["artifacts"]["critical_warning_jobs"]), 1)
            self.assertEqual(result["artifacts"]["critical_warning_jobs"][0]["source_key"], "state_company_reports:Газпром")
            self.assertTrue(result["artifacts"]["critical_warning_jobs"][0]["resolved_by_source_policy"])
            self.assertEqual(result["artifacts"]["unresolved_warning_jobs"], [])

    def test_quality_gate_flags_generic_promoted_relation_and_fake_domain_diversity(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quality.db"
            report_path = Path(tmp) / "qa_quality_latest.json"
            create_quality_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO entities(id, entity_type, canonical_name) VALUES
                        (1, 'location', 'России'),
                        (2, 'person', 'Василий Пискарёв');

                    INSERT INTO relation_candidates(
                        id, entity_a_id, entity_b_id, candidate_type, origin, score, support_items,
                        support_sources, support_domains, candidate_state, promotion_state, explain_path_json, evidence_mix_json
                    ) VALUES
                        (
                            900,
                            1,
                            2,
                            'same_case_cluster',
                            'candidate_builder:co_occurrence',
                            0.82,
                            3,
                            2,
                            1,
                            'promoted',
                            'promoted',
                            '[{"node_type":"Case","ids":[801]}]',
                            '{"domains":["source:28"],"content_clusters":{"cluster_ids":[]}}'
                        );

                    INSERT INTO relation_features(candidate_id, dedupe_support_score, calibrated_score, updated_at)
                    VALUES(900, 0.91, 0.82, '2026-04-26T00:00:00');

                    INSERT INTO relation_support(candidate_id, support_kind, support_class, source_id, domain, category)
                    VALUES
                        (900, 'content', 'evidence', 28, 'source:28', 'official_registry'),
                        (900, 'content', 'evidence', 80, 'telegram-export', 'telegram');

                    INSERT INTO job_runs(job_id, trigger_mode, requested_by, owner, attempt_no, status, started_at, warnings_json)
                    VALUES('tagger', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[]');
                    """
                )
                set_runtime_metadata(conn, "classifier_audit_last_status", "ok")
                set_runtime_metadata(conn, "classifier_audit_last_report", {"reviewed_baseline_ready": True})
                conn.commit()
            finally:
                conn.close()

            result = build_quality_gate(
                {
                    "db_path": str(db_path),
                    "ensure_schema_on_connect": True,
                    "quality_gate": {
                        "report_path": str(report_path),
                        "strict_gate": True,
                    },
                }
            )

            self.assertFalse(result["ok"])
            relation_quality = result["artifacts"]["relation_quality"]
            self.assertEqual(relation_quality["promoted_with_generic_entity"], 1)
            self.assertEqual(relation_quality["promoted_same_case_cluster"], 1)
            self.assertEqual(relation_quality["promoted_with_location_entity"], 1)
            self.assertEqual(relation_quality["promoted_with_fake_domain_diversity"], 1)
            self.assertEqual(relation_quality["promoted_without_nonseed_bridge"], 1)
            self.assertEqual(relation_quality["promoted_without_event_fact_or_official_bridge"], 1)

    def test_quality_gate_creates_relation_review_task_for_blocked_official_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "quality.db"
            report_path = Path(tmp) / "qa_quality_latest.json"
            create_quality_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO entities(id, entity_type, canonical_name) VALUES
                        (10, 'organization', 'Министерство юстиции Российской Федерации'),
                        (11, 'person', 'Панферов Константин Юрьевич');

                    INSERT INTO relation_candidates(
                        id, entity_a_id, entity_b_id, candidate_type, origin, score, support_items,
                        support_sources, support_domains, support_hard_evidence_count,
                        candidate_state, promotion_state, promotion_block_reason, evidence_mix_json, explain_path_json
                    ) VALUES
                        (
                            901,
                            10,
                            11,
                            'likely_association',
                            'candidate_builder:hybrid',
                            0.72,
                            1,
                            1,
                            1,
                            1,
                            'review',
                            'review',
                            'official_bridge_missing',
                            '{"official_bridge_count":1,"official_content_types":["restriction_record"],"bridge_types":["Content","OfficialDocument"]}',
                            '[{"node_type":"Content","ids":[17395]},{"node_type":"OfficialDocument","ids":[17395]}]'
                        );

                    INSERT INTO relation_features(
                        candidate_id, dedupe_support_score, bridge_diversity_score, calibrated_score, updated_at
                    ) VALUES(901, 1.0, 0.82, 0.72, '2026-04-26T00:00:00');

                    INSERT INTO job_runs(job_id, trigger_mode, requested_by, owner, attempt_no, status, started_at, warnings_json)
                    VALUES('tagger', 'manual', 'test', 'owner', 1, 'ok', '2026-04-26T00:00:00', '[]');
                    """
                )
                set_runtime_metadata(conn, "classifier_audit_last_status", "ok")
                set_runtime_metadata(conn, "classifier_audit_last_report", {"reviewed_baseline_ready": True})
                conn.commit()
            finally:
                conn.close()

            result = build_quality_gate(
                {
                    "db_path": str(db_path),
                    "ensure_schema_on_connect": True,
                    "quality_gate": {
                        "report_path": str(report_path),
                        "strict_gate": True,
                    },
                }
            )

            self.assertTrue(result["ok"])
            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT queue_key, subject_type, subject_id, suggested_action, machine_reason
                    FROM review_tasks
                    WHERE queue_key='relations'
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(row, ("relations", "relation_candidate", 901, "needs_more_docs", "blocked_official_bridge"))


if __name__ == "__main__":
    unittest.main()
