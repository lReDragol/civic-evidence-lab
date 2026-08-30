import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from config.db_utils import SCHEMA_PATH


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class AgentMasTests(unittest.TestCase):
    def test_agent_task_bus_is_idempotent_and_records_messages_and_artifacts(self):
        from agents.bus import (
            complete_agent_task,
            enqueue_agent_task,
            lease_agent_task,
            record_agent_artifact,
            record_agent_message,
        )

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "mas.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                first = enqueue_agent_task(
                    conn,
                    task_type="search_request",
                    requester_group="relation_audit",
                    target_group="search_classify",
                    subject_type="relation_candidate",
                    subject_id=42,
                    payload={"query": "РКН штраф ООО example"},
                    acceptance={"required_sources": ["official"]},
                    input_hash="hash-1",
                )
                second = enqueue_agent_task(
                    conn,
                    task_type="search_request",
                    requester_group="relation_audit",
                    target_group="search_classify",
                    subject_type="relation_candidate",
                    subject_id=42,
                    payload={"query": "РКН штраф ООО example"},
                    acceptance={"required_sources": ["official"]},
                    input_hash="hash-1",
                )

                self.assertEqual(first["task_id"], second["task_id"])
                self.assertTrue(first["created"])
                self.assertFalse(second["created"])

                leased = lease_agent_task(conn, target_group="search_classify", lease_owner="worker-1")
                self.assertIsNotNone(leased)
                self.assertEqual(leased["id"], first["task_id"])
                self.assertEqual(leased["status"], "running")

                message_id = record_agent_message(
                    conn,
                    task_id=first["task_id"],
                    message_type="search_request",
                    sender_group="relation_audit",
                    recipient_group="search_classify",
                    payload={"needed_evidence": "official URL"},
                )
                artifact_id = record_agent_artifact(
                    conn,
                    task_id=first["task_id"],
                    artifact_type="search_result",
                    payload={"urls": ["https://official.example.test/doc"]},
                    confidence=0.82,
                )
                complete_agent_task(conn, first["task_id"], result={"status": "needs_review"})

                repeat_completed = enqueue_agent_task(
                    conn,
                    task_type="search_request",
                    requester_group="relation_audit",
                    target_group="search_classify",
                    subject_type="relation_candidate",
                    subject_id=42,
                    payload={"query": "РКН штраф ООО example"},
                    acceptance={"required_sources": ["official"]},
                    input_hash="hash-1",
                )
                changed_input = enqueue_agent_task(
                    conn,
                    task_type="search_request",
                    requester_group="relation_audit",
                    target_group="search_classify",
                    subject_type="relation_candidate",
                    subject_id=42,
                    payload={"query": "РКН штраф ООО example updated"},
                    acceptance={"required_sources": ["official"]},
                    input_hash="hash-2",
                )

                self.assertEqual(repeat_completed["task_id"], first["task_id"])
                self.assertFalse(repeat_completed["created"])
                self.assertNotEqual(changed_input["task_id"], first["task_id"])
                self.assertGreater(message_id, 0)
                self.assertGreater(artifact_id, 0)
            finally:
                conn.close()

    def test_search_evidence_dedupes_citations_without_writing_truth_layers(self):
        from agents.bus import enqueue_agent_task
        from agents.search import persist_search_result

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "mas.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                task = enqueue_agent_task(
                    conn,
                    task_type="search_request",
                    requester_group="news_logic",
                    target_group="search_classify",
                    subject_type="content_item",
                    subject_id=10,
                    payload={"query": "Роскомнадзор требование 152-ФЗ"},
                    input_hash="search-hash",
                )
                result = {
                    "provider": "perplexity",
                    "model": "sonar-reasoning-pro",
                    "output_json": {
                        "search_results": [
                            {
                                "url": "https://rkn.gov.ru/doc/123",
                                "title": "Требование Роскомнадзора",
                                "snippet": "Официальное требование по 152-ФЗ",
                                "source_tier": "official",
                                "confidence": 0.91,
                            },
                            {
                                "url": "https://rkn.gov.ru/doc/123",
                                "title": "Дубликат",
                                "snippet": "Повтор",
                                "source_tier": "official",
                                "confidence": 0.8,
                            },
                        ]
                    },
                    "citations": [{"url": "https://rkn.gov.ru/doc/123"}],
                }

                written = persist_search_result(conn, task["task_id"], result)

                self.assertEqual(written["search_evidence_written"], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM search_evidence").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM content_tags").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM entity_relations").fetchone()[0], 0)
            finally:
                conn.close()

    def test_agent_search_worker_processes_mas_tasks_without_truth_layer_writes(self):
        from agents.bus import enqueue_agent_task
        from ner.agent_search import run_agent_search
        from unittest.mock import ANY, patch

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "mas.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                task = enqueue_agent_task(
                    conn,
                    task_type="search_request",
                    requester_group="relation_audit",
                    target_group="search_classify",
                    subject_type="relation_candidate",
                    subject_id=55,
                    payload={
                        "query": "официальный документ РКН штраф ООО Example 152-ФЗ",
                        "needed_evidence": "official_or_documentary_bridge",
                    },
                    input_hash="agent-search-worker-hash",
                )
            finally:
                conn.close()

            fake_key = {
                "key_id": 1,
                "provider": "perplexity",
                "api_key": "fake",
                "model_name": "sonar-pro",
            }
            fake_result = {
                "provider": "perplexity",
                "model": "sonar-pro",
                "output_text": "Найден официальный документ.",
                "output_json": {
                    "search_results": [
                        {
                            "url": "https://rkn.gov.ru/doc/55",
                            "title": "Документ РКН",
                            "snippet": "Официальный документ по 152-ФЗ",
                            "source_tier": "official",
                            "confidence": 0.93,
                        }
                    ],
                    "external_context": [],
                },
                "confidence": 0.93,
            }

            with patch("ner.agent_search.choose_key_for_stage", return_value=fake_key) as choose_key, \
                 patch("ner.agent_search.run_ai_task", return_value=fake_result) as run_task, \
                 patch("ner.agent_search.record_key_success") as record_success:
                result = run_agent_search({"db_path": str(db_path), "ensure_schema_on_connect": True})

            self.assertTrue(result["ok"])
            self.assertEqual(result["items_seen"], 1)
            self.assertEqual(result["items_new"], 1)
            choose_key.assert_called_once()
            self.assertEqual(choose_key.call_args.kwargs["stage"], "agent_search")
            self.assertTrue(choose_key.call_args.kwargs["requires_web_search"])
            self.assertEqual(run_task.call_args.kwargs["task"]["stage"], "agent_search")
            record_success.assert_called_once_with(ANY, 1)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                status = conn.execute("SELECT status FROM agent_tasks WHERE id=?", (task["task_id"],)).fetchone()["status"]
                self.assertEqual(status, "completed")
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM search_evidence").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM agent_artifacts WHERE artifact_type='search_result'").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM content_items WHERE content_type='agent_search_result'").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM entity_relations WHERE relation_type='agent_discovered'").fetchone()[0], 0)
            finally:
                conn.close()

    def test_ocr_document_review_enqueues_official_source_search_task(self):
        from media_pipeline.ocr import _update_document_review_after_ocr

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "mas.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.executescript(
                    """
                    INSERT INTO sources(id, name, category, url, is_active)
                    VALUES(1, 'Telegram', 'telegram', 'https://t.me/yep_news', 1);
                    INSERT INTO content_items(id, source_id, external_id, content_type, title, body_text)
                    VALUES(10, 1, 'yep-10', 'post', 'РКН требование', 'Скриншот документа');
                    INSERT INTO review_tasks(
                        task_key, queue_key, subject_type, subject_id, candidate_payload,
                        suggested_action, confidence, machine_reason, status
                    ) VALUES(
                        'document:10', 'documents', 'content_item', 10, '{}',
                        'verify_document_screenshot', 0.8, 'document screenshot', 'open'
                    );
                    """
                )
                _update_document_review_after_ocr(
                    conn,
                    content_id=10,
                    text="Роскомнадзор № 207 от 30.03.2026. Требование по ст. 12 152-ФЗ.",
                )
                conn.commit()

                row = conn.execute(
                    "SELECT task_type, requester_group, target_group, subject_type, subject_id, payload_json FROM agent_tasks"
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["task_type"], "search_request")
                self.assertEqual(row["requester_group"], "data_structuring")
                self.assertEqual(row["target_group"], "search_classify")
                self.assertEqual(row["subject_type"], "content_item")
                self.assertEqual(row["subject_id"], 10)
                payload = json.loads(row["payload_json"])
                self.assertIn("Роскомнадзор", payload["document_identifiers"]["issuer"])
                self.assertIn("official_source_search", payload["needed_evidence"])
            finally:
                conn.close()

    def test_relation_review_sync_enqueues_relation_gap_agent_task(self):
        from quality.pipeline_gate import _sync_relation_review_tasks

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "mas.db"
            create_db(db_path)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                active = _sync_relation_review_tasks(
                    conn,
                    [
                        {
                            "candidate_id": 77,
                            "issue": "blocked_official_bridge",
                            "candidate_type": "likely_association",
                            "candidate_state": "review",
                            "promotion_block_reason": "official_bridge_missing",
                            "entity_a_id": 1,
                            "entity_b_id": 2,
                            "entity_a_name": "РКН",
                            "entity_b_name": "ООО Example",
                            "bridge_types": ["Event", "Fact"],
                            "evidence_mix": {"official_bridge_count": 0},
                            "source_links": ["https://t.me/yep_news/10"],
                        }
                    ],
                )

                self.assertIn("relation:77:blocked_official_bridge", active)
                row = conn.execute(
                    """
                    SELECT task_type, requester_group, target_group, subject_type, subject_id, payload_json
                    FROM agent_tasks
                    WHERE subject_type='relation_candidate' AND subject_id=77
                    """
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["task_type"], "relation_gap")
                self.assertEqual(row["target_group"], "search_classify")
                payload = json.loads(row["payload_json"])
                self.assertTrue(payload["reject_if_only_telegram"])
                self.assertEqual(payload["needed_evidence"], "official_or_documentary_bridge")
            finally:
                conn.close()

    def test_provider_router_only_allows_web_for_search_or_relation_stages(self):
        from llm.provider_router import _stage_allows_web

        self.assertFalse(_stage_allows_web("clean_factual_text"))
        self.assertFalse(_stage_allows_web("structured_extract"))
        self.assertFalse(_stage_allows_web("tag_reasoning"))
        self.assertFalse(_stage_allows_web("event_link_hint"))
        self.assertTrue(_stage_allows_web("relation_reasoning"))
        self.assertTrue(_stage_allows_web("agent_search"))


if __name__ == "__main__":
    unittest.main()
