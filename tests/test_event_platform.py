import sqlite3
import tempfile
import unittest
from pathlib import Path

from analysis.event_pipeline import build_event_pipeline
from config.db_utils import SCHEMA_PATH
from runtime.registry import get_job_spec
from tools.export_obsidian import export_obsidian
from ui.web_bridge import DashboardDataService


def create_event_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active, is_official, credibility_tier)
            VALUES
                (1, 'Telegram Monitor', 'telegram', 'https://t.me/blocking', 1, 0, 'B'),
                (2, 'Official Registry', 'official_registry', 'https://example.test/docs', 1, 1, 'A');

            INSERT INTO entities(id, entity_type, canonical_name, description)
            VALUES
                (1, 'organization', 'Правительство Российской Федерации', 'Орган власти'),
                (2, 'organization', 'Telegram', 'Мессенджер'),
                (3, 'person', 'Андрей Иванов', 'Комментатор');

            INSERT INTO content_items(
                id, source_id, external_id, content_type, title, body_text, published_at, url, status
            ) VALUES
                (
                    11, 1, 'tg-11', 'post',
                    'Телеграм начали блокировать',
                    'Правительство решило ограничить доступ к Telegram. Пользователи начали жаловаться на сбои и блокировки.',
                    '2026-04-20T09:00:00',
                    'https://t.me/blocking/11',
                    'raw_signal'
                ),
                (
                    12, 2, 'doc-12', 'restriction_record',
                    'Постановление об ограничении Telegram',
                    'Официальный документ об ограничении доступа к Telegram по соображениям безопасности.',
                    '2026-04-20T08:00:00',
                    'https://example.test/docs/12',
                    'official_document'
                ),
                (
                    13, 1, 'tg-13', 'post',
                    'Пошли жалобы на блокировку Telegram',
                    'После начала блокировки Telegram люди стали массово жаловаться на недоступность мессенджера.',
                    '2026-04-21T10:00:00',
                    'https://t.me/blocking/13',
                    'raw_signal'
                );

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence)
            VALUES
                (1, 11, 'issuer', 0.99),
                (2, 11, 'target', 0.99),
                (1, 12, 'issuer', 1.0),
                (2, 12, 'target', 1.0),
                (2, 13, 'target', 0.95),
                (3, 13, 'commentator', 0.90);

            INSERT INTO claims(
                id, content_item_id, claim_text, canonical_text, canonical_hash, claim_type, status, needs_review
            ) VALUES
                (
                    21, 11,
                    'Правительство ограничило доступ к Telegram',
                    'правительство ограничило доступ к telegram',
                    'hash-block-1',
                    'restriction',
                    'verified',
                    0
                ),
                (
                    22, 13,
                    'После начала блокировки Telegram пользователи пожаловались на сбои',
                    'после начала блокировки telegram пользователи пожаловались на сбои',
                    'hash-block-2',
                    'reaction',
                    'verified',
                    0
                );

            INSERT INTO evidence_links(
                id, claim_id, evidence_item_id, evidence_type, evidence_class, strength, notes
            ) VALUES
                (31, 21, 12, 'official_document', 'hard', 'strong', 'Официальное постановление');

            INSERT INTO content_clusters(
                id, cluster_key, cluster_type, canonical_content_id, canonical_title, item_count, similarity_score, representative_score, first_seen_at, last_seen_at, status
            ) VALUES(
                101, 'cluster:telegram-block', 'story', 12, 'Блокировка Telegram', 3, 0.95, 0.96, '2026-04-20T08:00:00', '2026-04-21T10:00:00', 'active'
            );

            INSERT INTO content_cluster_items(cluster_id, content_item_id, similarity_score, reason, is_canonical)
            VALUES
                (101, 11, 0.92, 'story-merge', 0),
                (101, 12, 1.0, 'canonical', 1),
                (101, 13, 0.90, 'story-update', 0);

            INSERT INTO restriction_events(
                id, issuer_entity_id, target_entity_id, target_name, restriction_type, right_category, stated_justification,
                event_date, source_content_id, source_url, evidence_class
            ) VALUES(
                41, 1, 2, 'Telegram', 'internet_block', 'internet', 'по соображениям безопасности',
                '2026-04-20', 12, 'https://example.test/docs/12', 'hard'
            );

            INSERT INTO semantic_neighbors(
                source_kind, source_id, neighbor_kind, neighbor_id, score, method, metadata_json
            ) VALUES
                ('content', 11, 'content', 13, 0.81, 'tfidf', '{"seed":"story"}'),
                ('content', 13, 'content', 11, 0.81, 'tfidf', '{"seed":"story"}');
            """
        )
        conn.commit()
    finally:
        conn.close()


class EventPlatformTests(unittest.TestCase):
    def test_event_pipeline_creates_derivations_events_timeline_and_facts(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "events.db"
            create_event_db(db_path)

            result = build_event_pipeline({"db_path": str(db_path), "ensure_schema_on_connect": True})

            conn = sqlite3.connect(db_path)
            try:
                derivations = conn.execute(
                    """
                    SELECT content_item_id, derivation_type
                    FROM content_derivations
                    ORDER BY content_item_id, derivation_type
                    """
                ).fetchall()
                event_row = conn.execute(
                    """
                    SELECT canonical_title, event_type, event_date_start, event_date_end
                    FROM events
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                event_items = conn.execute(
                    "SELECT content_item_id, item_role FROM event_items ORDER BY content_item_id"
                ).fetchall()
                event_entities = conn.execute(
                    "SELECT entity_id, role FROM event_entities ORDER BY entity_id, role"
                ).fetchall()
                timeline_rows = conn.execute(
                    "SELECT timeline_date, title FROM event_timeline ORDER BY sort_order, id"
                ).fetchall()
                facts = conn.execute(
                    "SELECT canonical_text, fact_type FROM event_facts ORDER BY id"
                ).fetchall()
                fact_evidence = conn.execute(
                    "SELECT fact_id, evidence_class, content_item_id, document_content_id FROM fact_evidence ORDER BY id"
                ).fetchall()
                raw_text = conn.execute("SELECT body_text FROM content_items WHERE id=11").fetchone()[0]
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertEqual(result["events_created"], 1)
            self.assertEqual(len(derivations), 9)
            self.assertEqual(event_row[0], "Блокировка Telegram")
            self.assertEqual(event_row[1], "internet_block")
            self.assertEqual(event_row[2], "2026-04-20T08:00:00")
            self.assertEqual(event_row[3], "2026-04-21T10:00:00")
            self.assertEqual(event_items, [(11, "origin"), (12, "official_doc"), (13, "update")])
            self.assertIn((1, "issuer"), event_entities)
            self.assertIn((2, "target"), event_entities)
            self.assertIn((3, "commentator"), event_entities)
            self.assertEqual(len(timeline_rows), 3)
            self.assertEqual(len(facts), 2)
            self.assertEqual(facts[0][1], "restriction")
            self.assertTrue(any(row[1] == "hard" and row[3] == 12 for row in fact_evidence))
            self.assertIn("Пользователи начали жаловаться", raw_text)

    def test_event_pipeline_promotes_document_votes_to_fact_evidence_without_claims(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "events.db"
            create_event_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    DELETE FROM evidence_links;
                    DELETE FROM claims;
                    DELETE FROM restriction_events;

                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES
                        (10, 'organization', 'Роскомнадзор', 'Регулятор'),
                        (11, 'organization', 'Оператор сайта', 'Проверяемая организация');

                    INSERT INTO content_items(
                        id, source_id, external_id, content_type, title, body_text, published_at, url, status
                    ) VALUES(
                        30, 1, 'tg-doc-30', 'post',
                        'Штрафы до 15 млн рублей начал выписывать РКН владельцам сайтов',
                        'В посте опубликован скриншот требования Роскомнадзора о нарушениях обработки персональных данных и штрафах.',
                        '2026-04-30T12:00:00',
                        'https://t.me/yep_news/30',
                        'raw_signal'
                    );

                    INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence)
                    VALUES
                        (10, 30, 'mentioned', 0.98),
                        (11, 30, 'target', 0.91);

                    INSERT INTO content_tag_votes(
                        content_item_id, voter_name, tag_name, namespace, normalized_tag, vote_value,
                        confidence_raw, evidence_text, signal_layer
                    ) VALUES
                        (30, 'telegram_relevance_filter', 'document/screenshot', 'document', 'document/screenshot', 'support', 0.90, 'скриншот требования', 'raw'),
                        (30, 'telegram_relevance_filter', 'document/authenticity_review', 'document', 'document/authenticity_review', 'support', 0.90, 'проверить документ', 'raw'),
                        (30, 'telegram_relevance_filter', 'restriction/fines', 'restriction', 'restriction/fines', 'support', 0.88, 'штрафы до 15 млн рублей', 'raw'),
                        (30, 'telegram_relevance_filter', 'restriction/privacy', 'restriction', 'restriction/privacy', 'support', 0.82, 'персональные данные', 'raw');
                    """
                )
                conn.commit()
            finally:
                conn.close()

            result = build_event_pipeline({"db_path": str(db_path), "ensure_schema_on_connect": True})

            conn = sqlite3.connect(db_path)
            try:
                fact = conn.execute(
                    """
                    SELECT ef.fact_type, ef.canonical_text, fe.evidence_class, fe.document_content_id
                    FROM event_facts ef
                    JOIN fact_evidence fe ON fe.fact_id=ef.id
                    WHERE fe.content_item_id=30
                    ORDER BY ef.id DESC
                    LIMIT 1
                    """
                ).fetchone()
                roles = conn.execute(
                    """
                    SELECT e.canonical_name, ee.role
                    FROM event_entities ee
                    JOIN entities e ON e.id=ee.entity_id
                    WHERE ee.event_id=(SELECT event_id FROM fact_evidence fe JOIN event_facts ef ON ef.id=fe.fact_id WHERE fe.content_item_id=30 LIMIT 1)
                    ORDER BY e.id, ee.role
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertIsNotNone(fact)
            self.assertEqual(fact[0], "fine")
            self.assertIn("Роскомнадзор", fact[1])
            self.assertEqual(fact[2], "hard")
            self.assertEqual(fact[3], 30)
            self.assertIn(("Роскомнадзор", "regulator"), roles)
            self.assertIn(("Оператор сайта", "target"), roles)

    def test_event_pipeline_overrides_bad_ner_types_for_authorities_and_media(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "events.db"
            create_event_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO entities(id, entity_type, canonical_name, description)
                    VALUES
                        (40, 'location', 'Кремль', 'Неверно распознан как локация'),
                        (41, 'person', 'Минцифры', 'Неверно распознано как персона'),
                        (42, 'person', 'Коммерсантъ', 'Неверно распознан как персона'),
                        (43, 'organization', 'VPN-сервис', 'Цель ограничения');

                    INSERT INTO content_items(
                        id, source_id, external_id, content_type, title, body_text, published_at, url, status
                    ) VALUES(
                        40, 1, 'tg-doc-40', 'post',
                        'Кремль и Минцифры объяснили ограничения VPN',
                        'Коммерсантъ сообщил, что Кремль и Минцифры объяснили ограничения VPN-сервисов нехваткой ресурсов.',
                        '2026-04-30T13:00:00',
                        'https://t.me/yep_news/40',
                        'raw_signal'
                    );

                    INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence)
                    VALUES
                        (40, 40, 'location', 0.98),
                        (41, 40, 'mentioned', 0.98),
                        (42, 40, 'mentioned', 0.90),
                        (43, 40, 'target', 0.91);

                    INSERT INTO content_tag_votes(
                        content_item_id, voter_name, tag_name, namespace, normalized_tag, vote_value,
                        confidence_raw, evidence_text, signal_layer
                    ) VALUES
                        (40, 'telegram_relevance_filter', 'restriction/internet', 'restriction', 'restriction/internet', 'support', 0.86, 'ограничения VPN', 'raw');
                    """
                )
                conn.commit()
            finally:
                conn.close()

            result = build_event_pipeline({"db_path": str(db_path), "ensure_schema_on_connect": True})

            conn = sqlite3.connect(db_path)
            try:
                roles = conn.execute(
                    """
                    SELECT e.canonical_name, e.entity_type, ee.role
                    FROM event_entities ee
                    JOIN entities e ON e.id=ee.entity_id
                    JOIN event_items ei ON ei.event_id=ee.event_id
                    WHERE ei.content_item_id=40
                    ORDER BY e.id, ee.role
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertTrue(result["ok"])
            self.assertIn(("Кремль", "location", "regulator"), roles)
            self.assertIn(("Минцифры", "person", "regulator"), roles)
            self.assertIn(("Коммерсантъ", "person", "commentator"), roles)
            self.assertIn(("VPN-сервис", "organization", "target"), roles)

    def test_runtime_registry_exposes_event_pipeline_job(self):
        spec = get_job_spec("event_pipeline")
        self.assertIsNotNone(spec)
        self.assertEqual(spec.stage, "analysis")

    def test_events_screen_returns_event_detail_timeline_roles_and_facts(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "events.db"
            create_event_db(db_path)
            build_event_pipeline({"db_path": str(db_path), "ensure_schema_on_connect": True})

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                service = DashboardDataService(conn, {})
                payload = service.screen_payload("events", {"selected_id": 1})
            finally:
                conn.close()

            self.assertEqual(len(payload["items"]), 1)
            self.assertEqual(payload["items"][0]["canonical_title"], "Блокировка Telegram")
            self.assertEqual(payload["detail"]["id"], 1)
            self.assertEqual(payload["detail"]["event_type"], "internet_block")
            self.assertEqual(len(payload["detail"]["timeline"]), 3)
            self.assertTrue(any(item["role"] == "issuer" for item in payload["detail"]["entities"]))
            self.assertTrue(any(item["role"] == "target" for item in payload["detail"]["entities"]))
            self.assertEqual(len(payload["detail"]["facts"]), 2)
            self.assertTrue(any(item["item_role"] == "official_doc" for item in payload["detail"]["items"]))

    def test_graph_export_creates_event_and_fact_notes(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "events.db"
            vault = tmp_path / "vault"
            create_event_db(db_path)
            build_event_pipeline({"db_path": str(db_path), "ensure_schema_on_connect": True})

            export_obsidian(db_path=db_path, vault=vault, mode="graph", copy_media=False)

            event_note = vault / "Events" / "1-Блокировка-Telegram.md"
            fact_note = vault / "Facts" / "1-правительство-ограничило-доступ-к-telegram.md"
            index_note = vault / "Events" / "index.md"

            self.assertTrue(event_note.exists(), event_note)
            self.assertTrue(fact_note.exists(), fact_note)
            self.assertTrue(index_note.exists(), index_note)

            event_text = event_note.read_text(encoding="utf-8")
            fact_text = fact_note.read_text(encoding="utf-8")
            self.assertIn("[[Entities/organization/1-Правительство-Российской-Федерации|Правительство Российской Федерации]]", event_text)
            self.assertIn("[[Entities/organization/2-Telegram|Telegram]]", event_text)
            self.assertIn("[[Facts/1-правительство-ограничило-доступ-к-telegram|restriction #1]]", event_text)
            self.assertIn("[[Content/2026-04/12-Постановление-об-ограничении-Telegram|Постановление об ограничении Telegram]]", event_text)
            self.assertIn("[[Events/1-Блокировка-Telegram|Блокировка Telegram]]", fact_text)


if __name__ == "__main__":
    unittest.main()
