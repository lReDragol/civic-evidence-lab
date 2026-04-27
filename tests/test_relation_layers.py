import sqlite3
import tempfile
import unittest
from pathlib import Path

from analysis.entity_relation_builder import build_co_occurrence_from_mentions
from graph.relation_candidates import rebuild_relation_candidates
from ner.relation_extractor import extract_co_occurrence_relations


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


def create_relation_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Source A', 'media', 'https://a.example.test/a', 1),
                (2, 'Source B', 'media', 'https://b.example.test/b', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (1, 'person', 'Иван Иванов'),
                (2, 'organization', 'Министерство тестирования'),
                (3, 'location', 'Москва');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (101, 1, 'article', 'A1', 'A1', 'raw_signal'),
                (102, 1, 'article', 'A2', 'A2', 'raw_signal'),
                (103, 2, 'article', 'B1', 'B1', 'raw_signal');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (1, 101, 'subject', 1.0),
                (2, 101, 'organization', 1.0),
                (3, 101, 'location', 1.0),
                (1, 102, 'subject', 1.0),
                (2, 102, 'organization', 1.0),
                (3, 102, 'location', 1.0),
                (1, 103, 'subject', 1.0),
                (2, 103, 'organization', 1.0);

            INSERT INTO entity_relations(id, from_entity_id, to_entity_id, relation_type, strength, detected_by)
            VALUES (900, 1, 3, 'mentioned_together', 'weak', 'co_occurrence:2');
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_structural_relation_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (10, 'organization', 'Заказчик'),
                (11, 'organization', 'Поставщик');

            INSERT INTO contracts(id, title, contract_number) VALUES
                (501, 'Контракт 501', '501');

            INSERT INTO contract_parties(contract_id, entity_id, party_name, party_role, inn) VALUES
                (501, 10, 'Заказчик', 'customer', '1000000000'),
                (501, 11, 'Поставщик', 'supplier', '2000000000');
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_vote_pattern_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (20, 'person', 'Депутат А'),
                (21, 'person', 'Депутат Б');
            """
        )
        for session_id in range(1, 21):
            conn.execute(
                """
                INSERT INTO bill_vote_sessions(id, vote_date, vote_stage, result)
                VALUES(?, '2026-01-01', ?, 'accepted')
                """,
                (session_id, f'Vote {session_id}'),
            )
            conn.execute(
                """
                INSERT INTO bill_votes(vote_session_id, entity_id, deputy_name, vote_result)
                VALUES(?, 20, 'Депутат А', 'за')
                """,
                (session_id,),
            )
            conn.execute(
                """
                INSERT INTO bill_votes(vote_session_id, entity_id, deputy_name, vote_result)
                VALUES(?, 21, 'Депутат Б', 'за')
                """,
                (session_id,),
            )
        conn.commit()
    finally:
        conn.close()


def create_bill_cluster_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (30, 'person', 'Депутат В'),
                (31, 'person', 'Депутат Г');

            INSERT INTO bills(id, number, title) VALUES
                (601, '601', 'Bill 601'),
                (602, '602', 'Bill 602'),
                (603, '603', 'Bill 603');

            INSERT INTO bill_sponsors(bill_id, entity_id, sponsor_name, sponsor_role) VALUES
                (601, 30, 'Депутат В', 'sponsor'),
                (601, 31, 'Депутат Г', 'sponsor'),
                (602, 30, 'Депутат В', 'sponsor'),
                (602, 31, 'Депутат Г', 'sponsor'),
                (603, 30, 'Депутат В', 'sponsor'),
                (603, 31, 'Депутат Г', 'sponsor');
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_case_cluster_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Case Source A', 'media', 'https://case-a.example.test', 1),
                (2, 'Case Source B', 'media', 'https://case-b.example.test', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (40, 'person', 'Фигурант А'),
                (41, 'organization', 'Компания Б');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (301, 1, 'article', 'Case A', 'Case A body', 'raw_signal'),
                (302, 2, 'article', 'Case B', 'Case B body', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, status) VALUES
                (701, 301, 'Claim A', 'pending'),
                (702, 302, 'Claim B', 'pending');

            INSERT INTO cases(id, title, description, case_type, status) VALUES
                (801, 'Кейс 801', 'Расследование', 'investigation', 'open');

            INSERT INTO case_claims(case_id, claim_id, role) VALUES
                (801, 701, 'central'),
                (801, 702, 'supporting');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (40, 301, 'subject', 1.0),
                (41, 302, 'organization', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_case_claim_only_seed_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Seed Source', 'media', 'https://seed.example.test', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (50, 'person', 'Фигурант В'),
                (51, 'organization', 'Компания Г');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status)
            VALUES
                (401, 1, 'article', 'Claim A', 'Claim body A', 'raw_signal'),
                (402, 1, 'article', 'Claim B', 'Claim body B', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, canonical_text, canonical_hash, claim_cluster_id, status)
            VALUES
                (901, 401, 'Совместное утверждение', 'fact', 'совместное утверждение', 'hash-1', 77, 'unverified'),
                (902, 402, 'Совместное утверждение', 'fact', 'совместное утверждение', 'hash-1', 77, 'unverified');

            INSERT INTO cases(id, title, description, case_type, status) VALUES
                (950, 'Кейс 950', 'Case cluster only', 'investigation', 'open');

            INSERT INTO case_claims(case_id, claim_id, role) VALUES
                (950, 901, 'central'),
                (950, 902, 'supporting');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (50, 401, 'subject', 1.0),
                (51, 402, 'organization', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_case_telegram_only_support_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Telegram A', 'telegram', NULL, 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (55, 'person', 'Фигурант Д'),
                (56, 'organization', 'Компания Е');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status)
            VALUES
                (451, 1, 'post', 'Claim A', 'Claim body A', 'raw_signal'),
                (452, 1, 'post', 'Claim B', 'Claim body B', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, canonical_text, canonical_hash, claim_cluster_id, status)
            VALUES
                (951, 451, 'Телеграм-кейс', 'fact', 'телеграм-кейс', 'hash-tg-1', 88, 'unverified'),
                (952, 452, 'Телеграм-кейс', 'fact', 'телеграм-кейс', 'hash-tg-1', 88, 'unverified');

            INSERT INTO cases(id, title, description, case_type, status) VALUES
                (955, 'Кейс 955', 'Telegram only support', 'investigation', 'open');

            INSERT INTO case_claims(case_id, claim_id, role) VALUES
                (955, 951, 'central'),
                (955, 952, 'supporting');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (55, 451, 'subject', 1.0),
                (56, 451, 'organization', 1.0),
                (55, 452, 'subject', 1.0),
                (56, 452, 'organization', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_case_telegram_votepattern_support_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Telegram A', 'telegram', NULL, 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (57, 'person', 'Фигурант Ж'),
                (58, 'person', 'Фигурант З');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status)
            VALUES
                (461, 1, 'post', 'Claim A', 'Claim body A', 'raw_signal'),
                (462, 1, 'post', 'Claim B', 'Claim body B', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, canonical_text, canonical_hash, claim_cluster_id, status)
            VALUES
                (961, 461, 'Телеграм-кейс с голосованием', 'fact', 'телеграм-кейс с голосованием', 'hash-tg-vote-1', 89, 'unverified'),
                (962, 462, 'Телеграм-кейс с голосованием', 'fact', 'телеграм-кейс с голосованием', 'hash-tg-vote-1', 89, 'unverified');

            INSERT INTO cases(id, title, description, case_type, status) VALUES
                (956, 'Кейс 956', 'Telegram support and vote pattern', 'investigation', 'open');

            INSERT INTO case_claims(case_id, claim_id, role) VALUES
                (956, 961, 'central'),
                (956, 962, 'supporting');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (57, 461, 'subject', 1.0),
                (58, 461, 'subject', 1.0),
                (57, 462, 'subject', 1.0),
                (58, 462, 'subject', 1.0);
            """
        )
        for session_id in range(1, 9):
            conn.execute(
                """
                INSERT INTO bill_vote_sessions(id, vote_date, vote_stage, result)
                VALUES(?, '2026-01-01', ?, 'accepted')
                """,
                (3000 + session_id, f'Vote {session_id}'),
            )
            conn.execute(
                """
                INSERT INTO bill_votes(vote_session_id, entity_id, deputy_name, vote_result)
                VALUES(?, 57, 'Фигурант Ж', 'за')
                """,
                (3000 + session_id,),
            )
            conn.execute(
                """
                INSERT INTO bill_votes(vote_session_id, entity_id, deputy_name, vote_result)
                VALUES(?, 58, 'Фигурант З', 'за')
                """,
                (3000 + session_id,),
            )
        conn.commit()
    finally:
        conn.close()


def create_missing_domain_support_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Blank Source A', 'media', NULL, 1),
                (2, 'Blank Source B', 'media', NULL, 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (100, 'person', 'Иванов Иван'),
                (101, 'organization', 'Тестовая организация');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (1001, 1, 'post', 'A1', 'Body A1', 'raw_signal'),
                (1002, 1, 'post', 'A2', 'Body A2', 'raw_signal'),
                (1003, 2, 'post', 'B1', 'Body B1', 'raw_signal');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (100, 1001, 'subject', 1.0),
                (101, 1001, 'organization', 1.0),
                (100, 1002, 'subject', 1.0),
                (101, 1002, 'organization', 1.0),
                (100, 1003, 'subject', 1.0),
                (101, 1003, 'organization', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_low_specificity_case_promotion_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Registry A', 'official_registry', 'https://registry.example.test/a', 1),
                (2, 'Docs B', 'official_site', 'https://docs.example.test/b', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (60, 'location', 'России'),
                (61, 'person', 'Василий Пискарёв');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (501, 1, 'registry_record', 'R1', 'Официальная запись 1', 'raw_signal'),
                (502, 2, 'restriction_record', 'R2', 'Официальная запись 2', 'raw_signal'),
                (503, 2, 'transcript', 'R3', 'Официальная запись 3', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, canonical_text, canonical_hash, claim_cluster_id, status) VALUES
                (1001, 501, 'Совместный кейс 1', 'fact', 'совместный кейс', 'case-hash-1', 77, 'unverified'),
                (1002, 502, 'Совместный кейс 2', 'fact', 'совместный кейс', 'case-hash-1', 77, 'unverified'),
                (1003, 503, 'Совместный кейс 3', 'fact', 'совместный кейс', 'case-hash-1', 77, 'unverified');

            INSERT INTO cases(id, title, description, case_type, status) VALUES
                (801, 'Кейс 801', 'Case with low-specificity location', 'investigation', 'open');

            INSERT INTO case_claims(case_id, claim_id, role) VALUES
                (801, 1001, 'central'),
                (801, 1002, 'supporting'),
                (801, 1003, 'supporting');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (60, 501, 'location', 1.0),
                (61, 501, 'subject', 1.0),
                (60, 502, 'location', 1.0),
                (61, 502, 'subject', 1.0),
                (60, 503, 'location', 1.0),
                (61, 503, 'subject', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_content_cluster_dedupe_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Source A', 'media', 'https://alpha.example.test/feed', 1),
                (2, 'Source B', 'media', 'https://beta.example.test/feed', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (90, 'person', 'Персона А'),
                (91, 'organization', 'Организация Б');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (901, 1, 'article', 'Story 1', 'Duplicate story one', 'raw_signal'),
                (902, 1, 'article', 'Story 1 copy', 'Duplicate story one copy', 'raw_signal'),
                (903, 2, 'article', 'Story 1 mirror', 'Duplicate story one mirror', 'raw_signal'),
                (904, 2, 'article', 'Story 2', 'Second independent story', 'raw_signal');

            INSERT INTO content_clusters(id, cluster_key, cluster_type, canonical_content_id, canonical_title, method, similarity_score, representative_score, item_count, status)
            VALUES
                (1, 'story-cluster-1', 'story', 901, 'Story 1', 'title_signature', 0.95, 0.95, 3, 'active'),
                (2, 'story-cluster-2', 'story', 904, 'Story 2', 'title_signature', 0.91, 0.91, 1, 'active');

            INSERT INTO content_cluster_items(cluster_id, content_item_id, similarity_score, reason, is_canonical) VALUES
                (1, 901, 1.0, 'canonical', 1),
                (1, 902, 0.98, 'duplicate', 0),
                (1, 903, 0.94, 'mirror', 0),
                (2, 904, 1.0, 'canonical', 1);

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (90, 901, 'subject', 1.0),
                (91, 901, 'organization', 1.0),
                (90, 902, 'subject', 1.0),
                (91, 902, 'organization', 1.0),
                (90, 903, 'subject', 1.0),
                (91, 903, 'organization', 1.0),
                (90, 904, 'subject', 1.0),
                (91, 904, 'organization', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_contract_promotion_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Registry A', 'official_registry', 'https://registry.example.test/contracts', 1),
                (2, 'Docs B', 'official_site', 'https://docs.example.test/contracts', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (70, 'organization', 'Заказчик А'),
                (71, 'organization', 'Поставщик Б');

            INSERT INTO contracts(id, title, contract_number) VALUES
                (901, 'Контракт 901', '901');

            INSERT INTO contract_parties(contract_id, entity_id, party_name, party_role, inn) VALUES
                (901, 70, 'Заказчик А', 'customer', '1000000001'),
                (901, 71, 'Поставщик Б', 'supplier', '2000000002');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (701, 1, 'registry_record', 'Contract A', 'Контракт А', 'raw_signal'),
                (702, 2, 'procurement', 'Contract B', 'Контракт Б', 'raw_signal'),
                (703, 2, 'transcript', 'Contract C', 'Контракт В', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, canonical_text, canonical_hash, claim_cluster_id, status) VALUES
                (1701, 701, 'Контрактная связь', 'fact', 'контрактная связь', 'contract-hash', 170, 'unverified'),
                (1702, 702, 'Контрактная связь', 'fact', 'контрактная связь', 'contract-hash', 170, 'unverified'),
                (1703, 703, 'Контрактная связь', 'fact', 'контрактная связь', 'contract-hash', 170, 'unverified');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (70, 701, 'organization', 1.0),
                (71, 701, 'organization', 1.0),
                (70, 702, 'organization', 1.0),
                (71, 702, 'organization', 1.0),
                (70, 703, 'organization', 1.0),
                (71, 703, 'organization', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_restriction_official_promotion_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'RKN', 'official_site', 'https://rkn.gov.ru/docs/restrictions', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (75, 'organization', 'Роскомнадзор'),
                (76, 'person', 'Андрей Юрьевич Липов');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (751, 1, 'restriction_record', 'Restriction A', 'Официальный документ об ограничении', 'raw_signal');

            INSERT INTO restriction_events(
                id, source_content_id, issuer_entity_id, target_entity_id, target_name,
                restriction_type, right_category, evidence_class
            ) VALUES
                (752, 751, 75, 76, 'Андрей Юрьевич Липов', 'restriction_notice', 'information', 'hard');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (75, 751, 'organization', 1.0),
                (76, 751, 'subject', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_disclosure_official_promotion_db(db_path: Path, *, with_structural_relation: bool = False):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Duma archive', 'official_site', 'https://web.archive.org/web/20220415143334/http://duma.gov.ru/duma/persons/properties/2021/', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (85, 'person', 'Тестовый Депутат'),
                (86, 'organization', 'Государственная Дума РФ');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (851, 1, 'declaration', 'Disclosure A', 'Официальная декларация о доходах', 'raw_signal');

            INSERT INTO person_disclosures(
                id, entity_id, disclosure_year, source_content_id, source_url, source_type,
                income_amount, raw_income_text, evidence_class, metadata_json
            ) VALUES
                (
                    852,
                    85,
                    2021,
                    851,
                    'https://web.archive.org/web/20220415143334/http://duma.gov.ru/duma/persons/properties/2021/',
                    'official_archive',
                    1234567.89,
                    '1234567,89',
                    'hard',
                    '{"position":"член комитета Государственной Думы"}'
                );

            INSERT INTO official_positions(
                entity_id, position_title, organization, source_url, source_type, is_active
            ) VALUES
                (85, 'Депутат Государственной Думы', 'Государственная Дума РФ', 'https://duma.gov.ru/deputies/85', 'deputy_profile', 1);
            """
        )
        if with_structural_relation:
            conn.execute(
                """
                INSERT INTO entity_relations(
                    from_entity_id, to_entity_id, relation_type, strength, detected_by
                ) VALUES(?,?,?,?,?)
                """,
                (85, 86, "works_at", "strong", "official_positions"),
            )
        conn.commit()
    finally:
        conn.close()


def create_bill_promotion_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.executescript(
            """
            INSERT INTO sources(id, name, category, url, is_active) VALUES
                (1, 'Registry A', 'official_registry', 'https://registry.example.test/bills', 1),
                (2, 'Docs B', 'official_site', 'https://docs.example.test/bills', 1);

            INSERT INTO entities(id, entity_type, canonical_name) VALUES
                (80, 'person', 'Депутат А'),
                (81, 'person', 'Депутат Б');

            INSERT INTO bills(id, number, title) VALUES
                (611, '611', 'Bill 611'),
                (612, '612', 'Bill 612'),
                (613, '613', 'Bill 613');

            INSERT INTO bill_sponsors(bill_id, entity_id, sponsor_name, sponsor_role) VALUES
                (611, 80, 'Депутат А', 'sponsor'),
                (611, 81, 'Депутат Б', 'sponsor'),
                (612, 80, 'Депутат А', 'sponsor'),
                (612, 81, 'Депутат Б', 'sponsor'),
                (613, 80, 'Депутат А', 'sponsor'),
                (613, 81, 'Депутат Б', 'sponsor');

            INSERT INTO content_items(id, source_id, content_type, title, body_text, status) VALUES
                (801, 1, 'bill', 'Bill A', 'Законопроект А', 'raw_signal'),
                (802, 2, 'bill', 'Bill B', 'Законопроект Б', 'raw_signal'),
                (803, 2, 'transcript', 'Bill C', 'Обсуждение законопроекта', 'raw_signal');

            INSERT INTO claims(id, content_item_id, claim_text, claim_type, canonical_text, canonical_hash, claim_cluster_id, status) VALUES
                (1801, 801, 'Связь по биллам', 'fact', 'связь по биллам', 'bill-hash', 180, 'unverified'),
                (1802, 802, 'Связь по биллам', 'fact', 'связь по биллам', 'bill-hash', 180, 'unverified'),
                (1803, 803, 'Связь по биллам', 'fact', 'связь по биллам', 'bill-hash', 180, 'unverified');

            INSERT INTO entity_mentions(entity_id, content_item_id, mention_type, confidence) VALUES
                (80, 801, 'subject', 1.0),
                (81, 801, 'subject', 1.0),
                (80, 802, 'subject', 1.0),
                (81, 802, 'subject', 1.0),
                (80, 803, 'subject', 1.0),
                (81, 803, 'subject', 1.0);
            """
        )
        conn.commit()
    finally:
        conn.close()


class RelationLayerTests(unittest.TestCase):
    def test_relation_extractor_requires_independent_sources_and_cleans_old_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_relation_db(db_path)

            result = extract_co_occurrence_relations({"db_path": str(db_path)})
            self.assertEqual(result["relations_inserted"], 0)
            self.assertEqual(result["relation_candidates_created"], 1)

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, support_items, support_sources, support_domains, promotion_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                promoted = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0:3], (1, 2, "likely_association"))
            self.assertEqual(rows[0][3:6], (3, 2, 2))
            self.assertEqual(rows[0][6], "review")
            self.assertEqual(promoted, 0)

    def test_entity_relation_builder_skips_same_source_only_pair(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_relation_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                created = build_co_occurrence_from_mentions(conn)
                conn.commit()
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, support_items, support_sources, support_domains, promotion_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                weak_edges = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE relation_type='mentioned_together'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(created, 1)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0:3], (1, 2, "likely_association"))
            self.assertEqual(rows[0][3:6], (3, 2, 2))
            self.assertEqual(rows[0][6], "review")
            self.assertEqual(weak_edges, 0)

    def test_relation_candidate_builder_keeps_structural_only_contract_pairs_as_seed_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_structural_relation_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state, support_items, support_sources, support_domains
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                promoted = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(result["promoted_relations"], 0)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0:3], (10, 11, "same_contract_cluster"))
            self.assertEqual(rows[0][3:5], ("seed_only", "seed_only"))
            self.assertEqual(rows[0][5:8], (0, 0, 0))
            self.assertEqual(promoted, 0)

    def test_relation_candidate_builder_keeps_structural_seed_only_when_only_semantic_support_exists(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_structural_relation_db(db_path)

            conn = sqlite3.connect(db_path)
            try:
                conn.executescript(
                    """
                    INSERT INTO semantic_neighbors(source_kind, source_id, neighbor_kind, neighbor_id, score, method)
                    VALUES
                        ('entity', 10, 'entity', 11, 0.71, 'tfidf'),
                        ('entity', 11, 'entity', 10, 0.71, 'tfidf');
                    """
                )
                conn.commit()
            finally:
                conn.close()

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT candidate_type, promotion_state, candidate_state, semantic_score
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                support = conn.execute(
                    """
                    SELECT support_kind, metric_value
                    FROM relation_support
                    WHERE support_kind='semantic_neighbor'
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(row[0], "same_contract_cluster")
            self.assertEqual(row[1:3], ("seed_only", "seed_only"))
            self.assertGreaterEqual(float(row[3] or 0.0), 0.71)
            self.assertTrue(support)

    def test_relation_candidate_builder_keeps_structural_vote_pattern_as_seed_only_without_support(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_vote_pattern_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                promoted = conn.execute(
                    """
                    SELECT from_entity_id, to_entity_id, relation_type
                    FROM entity_relations
                    WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(result["promoted_relations"], 0)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0], (20, 21, "same_vote_pattern", "seed_only", "seed_only"))
            self.assertEqual(promoted, [])

    def test_relation_candidate_builder_marks_structural_bill_cluster_as_seed_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_bill_cluster_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                promoted = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(result["promoted_relations"], 0)
            self.assertEqual(rows, [(30, 31, "same_bill_cluster", "seed_only", "seed_only")])
            self.assertEqual(promoted, 0)

    def test_relation_candidate_builder_marks_structural_case_cluster_as_seed_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_case_cluster_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                rows = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state
                    FROM relation_candidates
                    ORDER BY id
                    """
                ).fetchall()
                support = conn.execute(
                    """
                    SELECT support_kind, metadata_json
                    FROM relation_support
                    ORDER BY id
                    """
                ).fetchall()
                promoted = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(result["promoted_relations"], 0)
            self.assertEqual(rows, [(40, 41, "same_case_cluster", "seed_only", "seed_only")])
            self.assertTrue(any(kind == "case" and '"case_id": 801' in metadata for kind, metadata in support))
            self.assertEqual(promoted, 0)

    def test_relation_candidate_builder_does_not_move_case_seed_to_review_without_evidence_items(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_case_claim_only_seed_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state,
                           support_items, support_sources, support_domains, support_claim_cluster_count
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                support_rows = conn.execute(
                    """
                    SELECT support_kind, metadata_json
                    FROM relation_support
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(row[0:5], (50, 51, "same_case_cluster", "seed_only", "seed_only"))
            self.assertEqual(row[5:8], (0, 0, 0))
            self.assertEqual(row[8], 1)
            self.assertTrue(any(kind == "claim_cluster" for kind, _ in support_rows))

    def test_relation_candidate_builder_keeps_same_case_telegram_only_support_as_seed_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_case_telegram_only_support_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state,
                           support_items, support_sources, support_domains, support_claim_cluster_count,
                           promotion_block_reason
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(row[0:5], (55, 56, "same_case_cluster", "seed_only", "seed_only"))
            self.assertEqual(row[5:9], (2, 1, 0, 1))
            self.assertEqual(row[9], "same_case_requires_nonseed_bridge")

    def test_relation_candidate_builder_keeps_same_case_votepattern_without_real_evidence_as_seed_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_case_telegram_votepattern_support_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT entity_a_id, entity_b_id, candidate_type, promotion_state, candidate_state,
                           support_items, support_sources, support_domains, support_claim_cluster_count,
                           promotion_block_reason
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 1)
            self.assertEqual(row[0:5], (57, 58, "same_case_cluster", "seed_only", "seed_only"))
            self.assertEqual(row[5:9], (2, 1, 0, 1))
            self.assertEqual(row[9], "same_case_requires_evidence_bridge")

    def test_relation_candidate_builder_skips_pair_when_domain_diversity_is_only_source_surrogate(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_missing_domain_support_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                count = conn.execute("SELECT COUNT(*) FROM relation_candidates").fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(result["relation_candidates_created"], 0)
            self.assertEqual(count, 0)

    def test_relation_candidate_builder_blocks_low_specificity_location_from_promotion(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_low_specificity_case_promotion_db(db_path)

            rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT candidate_type, candidate_state, promotion_state, support_items, support_sources, support_domains,
                           support_claim_cluster_count, support_hard_evidence_count, promotion_block_reason
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                promoted = conn.execute(
                    "SELECT COUNT(*) FROM entity_relations WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'"
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(row[0], "same_case_cluster")
            self.assertNotEqual(row[1], "promoted")
            self.assertNotEqual(row[2], "promoted")
            self.assertEqual(row[3:6], (3, 2, 2))
            self.assertEqual(row[6], 1)
            self.assertGreaterEqual(row[7], 3)
            self.assertEqual(row[8], "low_entity_specificity")
            self.assertEqual(promoted, 0)

    def test_relation_candidate_builder_collapses_duplicate_cluster_support(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_content_cluster_dedupe_db(db_path)

            rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT candidate_type, candidate_state, support_items, support_sources, support_domains, evidence_mix_json
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                support_rows = conn.execute(
                    """
                    SELECT COUNT(*) FROM relation_support
                    WHERE support_class='evidence'
                    """
                ).fetchone()[0]
            finally:
                conn.close()

            self.assertEqual(row[0], "likely_association")
            self.assertEqual(row[1], "review")
            self.assertEqual(row[2:5], (3, 2, 2))
            self.assertEqual(support_rows, 3)
            self.assertIn('"content_clusters"', row[5])

    def test_relation_candidate_builder_promotes_contract_cluster_with_real_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_contract_promotion_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT candidate_type, candidate_state, promotion_state, support_items, support_sources, support_domains,
                           support_claim_cluster_count, support_hard_evidence_count, explain_path_json
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                promoted = conn.execute(
                    """
                    SELECT relation_type
                    FROM entity_relations
                    WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["promoted_relations"], 1)
            self.assertEqual(row[0], "same_contract_cluster")
            self.assertEqual(row[1:3], ("promoted", "promoted"))
            self.assertEqual(row[3:6], (3, 2, 2))
            self.assertEqual(row[6], 1)
            self.assertGreaterEqual(row[7], 3)
            self.assertIn("ClaimCluster", row[8])
            self.assertIn("Contract", row[8])
            self.assertEqual(promoted, [("same_contract_cluster",)])

    def test_relation_candidate_builder_promotes_single_source_official_restriction_bridge(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_restriction_official_promotion_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT candidate_type, candidate_state, promotion_state, support_items, support_sources, support_domains,
                           support_hard_evidence_count, promotion_block_reason, evidence_mix_json, explain_path_json
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                promoted = conn.execute(
                    """
                    SELECT relation_type
                    FROM entity_relations
                    WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["promoted_relations"], 1)
            self.assertEqual(row[0], "likely_association")
            self.assertEqual(row[1:3], ("promoted", "promoted"))
            self.assertEqual(row[3:6], (1, 1, 1))
            self.assertGreaterEqual(row[6], 1)
            self.assertIsNone(row[7])
            self.assertIn('"official_bridge_count"', row[8])
            self.assertIn('"official_content_types"', row[8])
            self.assertIn("RestrictionEvent", row[9])
            self.assertIn("OfficialDocument", row[9])
            self.assertEqual(promoted, [("likely_association",)])

    def test_relation_candidate_builder_promotes_single_source_official_disclosure_bridge(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_disclosure_official_promotion_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT candidate_type, candidate_state, promotion_state, support_items, support_sources, support_domains,
                           support_hard_evidence_count, promotion_block_reason, evidence_mix_json, explain_path_json
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                promoted = conn.execute(
                    """
                    SELECT relation_type
                    FROM entity_relations
                    WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["promoted_candidates"], 1)
            self.assertEqual(result["promoted_relations"], 1)
            self.assertEqual(row[0], "likely_association")
            self.assertEqual(row[1:3], ("promoted", "promoted"))
            self.assertEqual(row[3:6], (1, 1, 1))
            self.assertGreaterEqual(row[6], 1)
            self.assertIsNone(row[7])
            self.assertIn('"official_bridge_count"', row[8])
            self.assertIn('"official_content_types"', row[8])
            self.assertIn("Disclosure", row[9])
            self.assertIn("OfficialDocument", row[9])
            self.assertEqual(promoted, [("likely_association",)])

    def test_relation_candidate_builder_skips_materializing_duplicate_over_existing_structural_edge(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_disclosure_official_promotion_db(db_path, with_structural_relation=True)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                counts = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN COALESCE(detected_by, '') LIKE 'relation_candidate:%' THEN 1 ELSE 0 END) AS candidate_edges,
                        SUM(CASE WHEN relation_type='works_at' AND COALESCE(detected_by, '')='official_positions' THEN 1 ELSE 0 END) AS structural_edges
                    FROM entity_relations
                    """
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(result["promoted_candidates"], 1)
            self.assertEqual(result["promoted_relations"], 0)
            self.assertEqual(counts, (0, 1))

    def test_relation_candidate_builder_promotes_bill_cluster_with_real_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "relations.db"
            create_bill_promotion_db(db_path)

            result = rebuild_relation_candidates({"db_path": str(db_path)})

            conn = sqlite3.connect(db_path)
            try:
                row = conn.execute(
                    """
                    SELECT candidate_type, candidate_state, promotion_state, support_items, support_sources, support_domains,
                           support_claim_cluster_count, support_hard_evidence_count, explain_path_json
                    FROM relation_candidates
                    ORDER BY id
                    LIMIT 1
                    """
                ).fetchone()
                promoted = conn.execute(
                    """
                    SELECT relation_type
                    FROM entity_relations
                    WHERE COALESCE(detected_by, '') LIKE 'relation_candidate:%'
                    ORDER BY id
                    """
                ).fetchall()
            finally:
                conn.close()

            self.assertEqual(result["promoted_relations"], 1)
            self.assertEqual(row[0], "same_bill_cluster")
            self.assertEqual(row[1:3], ("promoted", "promoted"))
            self.assertEqual(row[3:6], (3, 2, 2))
            self.assertEqual(row[6], 1)
            self.assertGreaterEqual(row[7], 2)
            self.assertIn("ClaimCluster", row[8])
            self.assertIn("Bill", row[8])
            self.assertEqual(promoted, [("same_bill_cluster",)])


if __name__ == "__main__":
    unittest.main()
