from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.entity_relation_builder import run_all as run_entity_relation_builder
from cases.risk_detector import detect_all_patterns
from cases.structural_links import run_all_structural_links
from config.db_utils import load_settings
from ner.relation_extractor import extract_co_occurrence_relations, extract_head_role_relations
from verification.contradiction_detector import run_contradiction_detection
from verification.evidence_linker import auto_link_by_content_type, auto_link_evidence
from runtime.state import get_runtime_metadata, set_runtime_metadata


STRUCTURAL_RELATION_TYPES = {
    "works_at",
    "head_of",
    "party_member",
    "member_of",
    "member_of_committee",
    "represents_region",
    "sponsored_bill",
    "voted_for",
    "voted_against",
    "voted_abstained",
    "voted_absent",
}
WEAK_RELATION_TYPES = {"mentioned_together"}

ANALYSIS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS contracts (
    id              INTEGER PRIMARY KEY,
    material_id     INTEGER,
    content_item_id INTEGER,
    contract_number TEXT,
    title           TEXT NOT NULL,
    summary         TEXT,
    publication_date TEXT,
    source_org      TEXT,
    customer_inn    TEXT,
    supplier_inn    TEXT,
    raw_data        TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_contracts_number ON contracts(contract_number);
CREATE INDEX IF NOT EXISTS idx_contracts_material ON contracts(material_id);
CREATE INDEX IF NOT EXISTS idx_contracts_customer_inn ON contracts(customer_inn);
CREATE INDEX IF NOT EXISTS idx_contracts_supplier_inn ON contracts(supplier_inn);

CREATE TABLE IF NOT EXISTS contract_parties (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    contract_id     INTEGER NOT NULL,
    entity_id       INTEGER,
    party_name      TEXT,
    party_role      TEXT NOT NULL,
    inn             TEXT,
    metadata_json   TEXT
);
CREATE INDEX IF NOT EXISTS idx_contract_parties_contract ON contract_parties(contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_parties_entity ON contract_parties(entity_id);
CREATE INDEX IF NOT EXISTS idx_contract_parties_inn ON contract_parties(inn);
"""


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def copy_database(source_db: Path, target_db: Path):
    ensure_dir(target_db.parent)
    source_conn = sqlite3.connect(str(source_db), timeout=60)
    target_conn = sqlite3.connect(str(target_db), timeout=60)
    try:
        source_conn.backup(target_conn)
    finally:
        target_conn.close()
        source_conn.close()


def semantic_relation_layer(
    relation_type: str,
    detected_by: str | None,
    evidence_item_id: Any | None = None,
) -> str:
    if relation_type in WEAK_RELATION_TYPES or (detected_by or "").startswith("co_occurrence:"):
        return "weak_similarity"
    if evidence_item_id is not None or relation_type == "contradicts":
        return "evidence"
    if relation_type in STRUCTURAL_RELATION_TYPES:
        return "structural"
    return "evidence"


def parse_json(raw_text: str | None, default):
    if not raw_text:
        return default
    try:
        return json.loads(raw_text)
    except (json.JSONDecodeError, TypeError):
        return default


def coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            try:
                return int(stripped)
            except ValueError:
                return None
    return None


def normalize_inn(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_name(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split()).casefold()


def normalize_party_role(value: Any) -> str:
    role = normalize_name(value)
    if role in {"заказчик", "customer", "client"}:
        return "customer"
    if role in {"поставщик", "supplier", "vendor", "исполнитель", "подрядчик"}:
        return "supplier"
    if role in {"", "party"}:
        return ""
    return role


def resolve_party_entity_id(
    conn: sqlite3.Connection,
    entity_by_inn: dict[str, int],
    entity_by_name: dict[str, int],
    party_name: str,
    party_inn: str,
) -> int | None:
    if party_inn and party_inn in entity_by_inn:
        return entity_by_inn[party_inn]

    normalized = normalize_name(party_name)
    if normalized and normalized in entity_by_name:
        return entity_by_name[normalized]

    if normalized:
        row = conn.execute(
            "SELECT id, canonical_name, inn FROM entities WHERE canonical_name = ? LIMIT 1",
            (party_name,),
        ).fetchone()
        if row:
            entity_id = int(row[0])
            entity_by_name[normalized] = entity_id
            if normalize_inn(row[2]):
                entity_by_inn[normalize_inn(row[2])] = entity_id
            return entity_id

        cur = conn.execute(
            """
            INSERT OR IGNORE INTO entities(entity_type, canonical_name, description)
            VALUES('organization', ?, ?)
            """,
            (
                party_name,
                "Нормализовано из government_contract в analysis snapshot",
            ),
        )
        if cur.lastrowid:
            entity_id = int(cur.lastrowid)
        else:
            row = conn.execute(
                "SELECT id FROM entities WHERE entity_type='organization' AND canonical_name=? LIMIT 1",
                (party_name,),
            ).fetchone()
            entity_id = int(row[0]) if row else None

        if entity_id is not None:
            entity_by_name[normalized] = entity_id
            if party_inn:
                entity_by_inn[party_inn] = entity_id
                conn.execute("UPDATE entities SET inn = COALESCE(NULLIF(inn, ''), ?) WHERE id=?", (party_inn, entity_id))
            return entity_id

    return None


def ensure_analysis_tables(conn: sqlite3.Connection):
    conn.executescript(ANALYSIS_SCHEMA_SQL)


def normalize_contracts(conn: sqlite3.Connection) -> dict[str, int]:
    ensure_analysis_tables(conn)
    if not table_exists(conn, "investigative_materials"):
        return {"contracts": 0, "parties": 0}

    conn.execute("DELETE FROM contract_parties")
    conn.execute("DELETE FROM contracts")

    entity_by_inn = {}
    entity_by_name = {}
    if table_exists(conn, "entities"):
        for entity_id, canonical_name, inn in conn.execute(
            "SELECT id, canonical_name, inn FROM entities"
        ).fetchall():
            if normalize_inn(inn):
                entity_by_inn[normalize_inn(inn)] = entity_id
            normalized_name = normalize_name(canonical_name)
            if normalized_name:
                entity_by_name[normalized_name] = entity_id

    contracts_created = 0
    parties_created = 0
    rows = conn.execute(
        """
        SELECT id, content_item_id, title, summary, involved_entities, publication_date, source_org, raw_data
        FROM investigative_materials
        WHERE material_type='government_contract'
        ORDER BY id
        """
    ).fetchall()

    for row in rows:
        raw_data = parse_json(row[7], {})
        if not isinstance(raw_data, dict):
            raw_data = {}
        contract_number = str(raw_data.get("contract_number") or "").strip()
        customer_inn = normalize_inn(raw_data.get("customer_inn"))
        supplier_inn = normalize_inn(raw_data.get("supplier_inn"))
        customer_name = str(raw_data.get("customer") or "").strip()
        supplier_name = str(raw_data.get("supplier") or "").strip()

        conn.execute(
            """
            INSERT INTO contracts(
                id, material_id, content_item_id, contract_number, title, summary,
                publication_date, source_org, customer_inn, supplier_inn, raw_data
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                row[0],
                row[0],
                row[1],
                contract_number,
                row[2],
                row[3],
                row[5],
                row[6],
                customer_inn,
                supplier_inn,
                row[7],
            ),
        )
        contracts_created += 1

        seen_party_keys: set[tuple] = set()
        involved_entities = parse_json(row[4], [])
        if isinstance(involved_entities, list):
            for item in involved_entities:
                if not isinstance(item, dict):
                    continue
                entity_id = coerce_int(item.get("entity_id"))
                party_role = normalize_party_role(item.get("role"))
                party_name = str(item.get("name") or "").strip()
                party_inn = normalize_inn(item.get("inn"))

                if not party_inn:
                    if party_role == "customer":
                        party_inn = customer_inn
                    elif party_role == "supplier":
                        party_inn = supplier_inn

                if not party_name:
                    if party_role == "customer":
                        party_name = customer_name
                    elif party_role == "supplier":
                        party_name = supplier_name

                if entity_id is None:
                    entity_id = resolve_party_entity_id(
                        conn,
                        entity_by_inn,
                        entity_by_name,
                        party_name,
                        party_inn,
                    )

                if not party_role:
                    if party_inn and party_inn == customer_inn:
                        party_role = "customer"
                    elif party_inn and party_inn == supplier_inn:
                        party_role = "supplier"
                    elif party_name and normalize_name(party_name) == normalize_name(customer_name):
                        party_role = "customer"
                    elif party_name and normalize_name(party_name) == normalize_name(supplier_name):
                        party_role = "supplier"
                    else:
                        party_role = "party"

                key = (party_role, normalize_name(party_name), party_inn)
                if key in seen_party_keys:
                    continue
                seen_party_keys.add(key)

                conn.execute(
                    """
                    INSERT INTO contract_parties(
                        contract_id, entity_id, party_name, party_role, inn, metadata_json
                    ) VALUES(?,?,?,?,?,?)
                    """,
                    (
                        row[0],
                        entity_id,
                        party_name or None,
                        party_role,
                        party_inn or None,
                        json.dumps(item, ensure_ascii=False),
                    ),
                )
                parties_created += 1

        for party_role, party_inn, party_name in (
            ("customer", customer_inn, customer_name),
            ("supplier", supplier_inn, supplier_name),
        ):
            if not party_inn and not party_name:
                continue
            entity_id = resolve_party_entity_id(
                conn,
                entity_by_inn,
                entity_by_name,
                party_name,
                party_inn,
            )
            key = (party_role, normalize_name(party_name), party_inn)
            if key in seen_party_keys:
                continue
            seen_party_keys.add(key)
            conn.execute(
                """
                INSERT INTO contract_parties(
                    contract_id, entity_id, party_name, party_role, inn, metadata_json
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    row[0],
                    entity_id,
                    party_name or None,
                    party_role,
                    party_inn,
                    None,
                ),
            )
            parties_created += 1

    conn.commit()
    return {"contracts": contracts_created, "parties": parties_created}


def query_count(conn: sqlite3.Connection, table_name: str, where_sql: str = "", params: tuple = ()) -> int:
    if not table_exists(conn, table_name):
        return 0
    sql = f"SELECT COUNT(*) FROM {table_name}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    return int(conn.execute(sql, params).fetchone()[0])


def collect_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    counts = {
        "sources": query_count(conn, "sources"),
        "content_items": query_count(conn, "content_items"),
        "entities": query_count(conn, "entities"),
        "claims": query_count(conn, "claims"),
        "cases": query_count(conn, "cases"),
        "bills": query_count(conn, "bills"),
        "vote_sessions": query_count(conn, "bill_vote_sessions"),
        "contracts": query_count(conn, "contracts") or query_count(conn, "investigative_materials", "material_type='government_contract'"),
        "risks": query_count(conn, "risk_patterns"),
    }

    relation_layers = {
        "structural": 0,
        "evidence": 0,
        "risk": counts["risks"],
        "weak_similarity": 0,
    }
    evidence_backed_relations = 0

    if table_exists(conn, "entity_relations"):
        for relation_type, evidence_item_id, detected_by, count in conn.execute(
            """
            SELECT relation_type, evidence_item_id, detected_by, COUNT(*)
            FROM entity_relations
            GROUP BY relation_type, evidence_item_id, detected_by
            """
        ).fetchall():
            layer = semantic_relation_layer(relation_type, detected_by, evidence_item_id)
            relation_layers[layer] += int(count)
            if evidence_item_id is not None:
                evidence_backed_relations += int(count)
    if table_exists(conn, "relation_candidates"):
        weak_rows = conn.execute(
            """
            SELECT promotion_state, COUNT(*)
            FROM relation_candidates
            GROUP BY promotion_state
            """
        ).fetchall()
        for state, count in weak_rows:
            if state in {"pending", "review"}:
                relation_layers["weak_similarity"] += int(count)

    claims_total = counts["claims"]
    claims_with_evidence = 0
    if table_exists(conn, "claims") and table_exists(conn, "evidence_links"):
        claims_with_evidence = int(
            conn.execute("SELECT COUNT(DISTINCT claim_id) FROM evidence_links").fetchone()[0]
        )

    isolated_entities = 0
    if table_exists(conn, "entities") and table_exists(conn, "entity_relations"):
        isolated_entities = int(
            conn.execute(
                """
                WITH rel AS (
                    SELECT from_entity_id AS entity_id FROM entity_relations
                    UNION
                    SELECT to_entity_id AS entity_id FROM entity_relations
                )
                SELECT COUNT(*)
                FROM entities e
                LEFT JOIN rel r ON r.entity_id = e.id
                WHERE r.entity_id IS NULL
                """
            ).fetchone()[0]
        )

    evidence_coverage = {
        "claims_total": claims_total,
        "claims_with_evidence": claims_with_evidence,
        "ratio": round((claims_with_evidence / claims_total), 4) if claims_total else 0.0,
    }

    return {
        "counts": counts,
        "relation_layers": relation_layers,
        "evidence_backed_relations": evidence_backed_relations,
        "isolated_entities": isolated_entities,
        "evidence_coverage": evidence_coverage,
    }


def collect_top_hubs(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    if not table_exists(conn, "entities") or not table_exists(conn, "entity_relations"):
        return []

    rows = conn.execute(
        """
        WITH strong_rel AS (
            SELECT id, from_entity_id AS entity_id
            FROM entity_relations
            WHERE relation_type != 'mentioned_together'
              AND COALESCE(detected_by, '') NOT LIKE 'co_occurrence:%'
            UNION ALL
            SELECT id, to_entity_id AS entity_id
            FROM entity_relations
            WHERE relation_type != 'mentioned_together'
              AND COALESCE(detected_by, '') NOT LIKE 'co_occurrence:%'
        )
        SELECT e.id, e.entity_type, e.canonical_name, COUNT(strong_rel.id) AS degree
        FROM entities e
        LEFT JOIN strong_rel ON strong_rel.entity_id = e.id
        GROUP BY e.id
        ORDER BY degree DESC, e.id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    result = []
    for row in rows:
        result.append(
            {
                "entity_id": row[0],
                "entity_type": row[1],
                "canonical_name": row[2],
                "degree": int(row[3]),
            }
        )
    return result


def build_analysis_snapshot(
    source_db: Path,
    target_db: Path,
    report_path: Path,
    pipeline_version: str | None = None,
) -> dict[str, Any]:
    if not source_db.exists():
        raise FileNotFoundError(source_db)

    ensure_dir(report_path.parent)
    copy_database(source_db, target_db)
    prep_conn = sqlite3.connect(str(target_db))
    try:
        normalize_contracts_result = normalize_contracts(prep_conn)
        if pipeline_version is None:
            pipeline_version = get_runtime_metadata(prep_conn, "current_pipeline_version")
    finally:
        prep_conn.close()

    settings = load_settings()
    snapshot_settings = dict(settings)
    snapshot_settings["db_path"] = str(target_db)

    pipeline = []
    pipeline.append(
        {
            "step": "normalize_contracts",
            "status": "ok",
            "started_at": datetime.now().isoformat(timespec="seconds"),
            "finished_at": datetime.now().isoformat(timespec="seconds"),
            "result": normalize_contracts_result,
        }
    )
    steps = [
        ("run_all_structural_links", run_all_structural_links),
        ("run_entity_relation_builder", run_entity_relation_builder),
        ("extract_head_role_relations", extract_head_role_relations),
        ("extract_co_occurrence_relations", extract_co_occurrence_relations),
        ("run_contradiction_detection", run_contradiction_detection),
        ("auto_link_evidence", auto_link_evidence),
        ("auto_link_by_content_type", auto_link_by_content_type),
        ("detect_all_patterns", detect_all_patterns),
    ]

    for step_name, func in steps:
        started_at = datetime.now().isoformat(timespec="seconds")
        try:
            result = func(snapshot_settings)
            pipeline.append(
                {
                    "step": step_name,
                    "status": "ok",
                    "started_at": started_at,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "result": result,
                }
            )
        except Exception as exc:
            pipeline.append(
                {
                    "step": step_name,
                    "status": "error",
                    "started_at": started_at,
                    "finished_at": datetime.now().isoformat(timespec="seconds"),
                    "error": str(exc),
                }
            )

    conn = sqlite3.connect(str(target_db))
    try:
        if pipeline_version:
            set_runtime_metadata(conn, "analysis_built_from_pipeline_version", pipeline_version)
        set_runtime_metadata(conn, "analysis_generated_at", datetime.now().isoformat(timespec="seconds"))
        summary = collect_summary(conn)
        top_hubs = collect_top_hubs(conn)
    finally:
        conn.close()

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "pipeline_version": pipeline_version,
        "db": {
            "source_db": str(source_db),
            "target_db": str(target_db),
        },
        "pipeline": pipeline,
        "summary": summary,
        "top_hubs": top_hubs,
    }

    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return report


def main():
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Build analysis snapshot on a derived SQLite database")
    parser.add_argument(
        "--source-db",
        default=settings.get("db_path", str(PROJECT_ROOT / "db" / "news_unified.db")),
    )
    parser.add_argument(
        "--target-db",
        default=str(PROJECT_ROOT / "db" / "news_analysis.db"),
    )
    parser.add_argument(
        "--report",
        default=str(PROJECT_ROOT / "reports" / "analysis_snapshot_latest.json"),
    )
    parser.add_argument(
        "--pipeline-version",
        default=None,
    )
    args = parser.parse_args()

    report = build_analysis_snapshot(
        source_db=Path(args.source_db),
        target_db=Path(args.target_db),
        report_path=Path(args.report),
        pipeline_version=args.pipeline_version,
    )
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
