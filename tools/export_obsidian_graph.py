from __future__ import annotations

import json
import re
import shutil
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from investigation.models import RELATION_INVERSE_LABELS, RELATION_LABELS
from runtime.state import get_runtime_metadata, set_runtime_metadata


BAD_FILENAME_CHARS = '<>:"/\\|?*\n\r\t'
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


def slugify(value: str, fallback: str = "note", max_len: int = 90) -> str:
    value = (value or "").strip()
    cleaned = []
    last_dash = False
    for ch in value:
        if ch in BAD_FILENAME_CHARS or ch.isspace():
            if not last_dash:
                cleaned.append("-")
                last_dash = True
            continue
        cleaned.append(ch)
        last_dash = False
    slug = "".join(cleaned).strip(" .-")
    slug = slug.replace("_", "-")
    if not slug:
        slug = fallback
    return slug[:max_len].strip(" .-") or fallback


def md_escape(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ").strip()


def yaml_scalar(value: Any) -> str:
    return json.dumps(value if value is not None else "", ensure_ascii=False)


def note_link(rel_path: str, title: Optional[str] = None) -> str:
    target = rel_path.replace("\\", "/").removesuffix(".md")
    if title:
        return f"[[{target}|{title}]]"
    return f"[[{target}]]"


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def write_note(vault: Path, rel_path: str, body: str) -> str:
    out_path = vault / rel_path
    ensure_dir(out_path.parent)
    out_path.write_text(body.rstrip() + "\n", encoding="utf-8")
    return rel_path.replace("\\", "/")


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def rows(conn: sqlite3.Connection, sql: str, params: Iterable = ()):
    return conn.execute(sql, tuple(params)).fetchall()


def one(conn: sqlite3.Connection, sql: str, params: Iterable = ()):
    return conn.execute(sql, tuple(params)).fetchone()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = one(
        conn,
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name=?",
        (table_name,),
    )
    return row is not None


def copy_attachment(
    vault: Path,
    row: sqlite3.Row | dict,
    copy_media: bool = True,
) -> tuple[Optional[str], str]:
    file_path = Path(row["file_path"] or "")
    if not file_path.exists() or not file_path.is_file():
        return None, f"missing: {row['file_path'] or ''}"

    ext = file_path.suffix or ".bin"
    digest = row["hash_sha256"] or f"attachment-{row['id']}"
    safe_name = slugify(file_path.stem, fallback=f"attachment-{row['id']}", max_len=55)
    out_name = f"{digest[:16]}-{safe_name}{ext}"
    dest = vault / "Attachments" / out_name
    if copy_media:
        ensure_dir(dest.parent)
        if not dest.exists() or dest.stat().st_size != file_path.stat().st_size:
            shutil.copy2(file_path, dest)

    if (row.get("mime_type") or row["mime_type"] or "").startswith("image/"):
        wikilink = f"![[Attachments/{out_name}]]"
    else:
        wikilink = f"[[Attachments/{out_name}]]"
    return wikilink, out_name


def frontmatter(data: dict) -> str:
    lines = ["---"]
    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, list):
            lines.append(f"{key}:")
            for item in value:
                lines.append(f"  - {yaml_scalar(item)}")
        else:
            lines.append(f"{key}: {yaml_scalar(value)}")
    lines.append("---")
    return "\n".join(lines)


def parse_json(raw_text: Optional[str], default):
    if not raw_text:
        return default
    try:
        return json.loads(raw_text)
    except (json.JSONDecodeError, TypeError):
        return default


def coerce_int(value: Any) -> Optional[int]:
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


def parse_int_list(raw_text: Optional[str]) -> list[int]:
    data = parse_json(raw_text, [])
    if not isinstance(data, list):
        return []
    result: list[int] = []
    for item in data:
        value = coerce_int(item)
        if value is not None and value not in result:
            result.append(value)
    return result


def normalize_tag_name(tag_name: str) -> str:
    raw = (tag_name or "").strip().lower()
    if not raw:
        return "untagged"
    raw = raw.replace(" / ", "/").replace("\\", "/")
    parts = []
    for part in raw.split("/"):
        clean = re.sub(r"[^\w\s-]+", "", part, flags=re.UNICODE)
        clean = re.sub(r"\s+", "-", clean, flags=re.UNICODE).strip("-_ ")
        if clean:
            parts.append(clean)
    if not parts:
        parts = [slugify(raw, "untagged").lower()]
    if all(p.isdigit() for p in parts):
        parts[0] = f"tag-{parts[0]}"
    return "/".join(parts)


def relation_support_count(detected_by: Optional[str]) -> Optional[int]:
    if not detected_by:
        return None
    match = re.search(r"co_occurrence:(\d+)", detected_by)
    if match:
        return int(match.group(1))
    return None


def relation_layer(
    relation_type: str,
    evidence_item_id: Any,
    detected_by: Optional[str],
) -> str:
    if relation_type in WEAK_RELATION_TYPES or (detected_by or "").startswith("co_occurrence:"):
        return "weak_similarity"
    if relation_type == "contradicts" or evidence_item_id is not None:
        return "evidence"
    if relation_type in STRUCTURAL_RELATION_TYPES:
        return "structural"
    return "structural"


def relation_label(relation_type: str, outgoing: bool) -> str:
    if outgoing:
        return RELATION_LABELS.get(relation_type, relation_type)
    return RELATION_INVERSE_LABELS.get(
        relation_type,
        RELATION_LABELS.get(relation_type, relation_type),
    )


def month_bucket(value: Optional[str]) -> str:
    if value and len(value) >= 7:
        return value[:7]
    return "unknown"


def dedupe_ints(values: Iterable[int]) -> list[int]:
    seen: set[int] = set()
    result: list[int] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def row_title(row: dict, field: str, fallback_prefix: str) -> str:
    return row.get(field) or f"{fallback_prefix} {row['id']}"


def content_title(row: dict) -> str:
    return row.get("title") or row.get("body_text") or f"Content {row['id']}"


def claim_title(row: dict) -> str:
    return f"Claim {row['id']}"


def case_title(row: dict) -> str:
    return row.get("title") or f"Case {row['id']}"


def entity_title(row: dict) -> str:
    return row.get("canonical_name") or f"Entity {row['id']}"


def bill_title(row: dict) -> str:
    return row.get("number") or row.get("title") or f"Bill {row['id']}"


def vote_title(row: dict) -> str:
    bits = [row.get("vote_date") or f"session-{row['id']}"]
    if row.get("bill_number"):
        bits.append(str(row["bill_number"]))
    elif row.get("bill_title"):
        bits.append(str(row["bill_title"]))
    if row.get("vote_stage"):
        bits.append(str(row["vote_stage"]))
    return " ".join(bit for bit in bits if bit).strip() or f"Vote {row['id']}"


def vote_link_title(row: dict) -> str:
    bits = [row.get("vote_date") or ""]
    if row.get("bill_number"):
        bits.append(str(row["bill_number"]))
    elif row.get("bill_title"):
        bits.append(str(row["bill_title"]))
    text = " ".join(bit for bit in bits if bit).strip()
    return text or f"Vote {row['id']}"


def contract_title(row: dict) -> str:
    return row.get("title") or row.get("contract_number") or f"Contract {row['id']}"


def risk_title(row: dict) -> str:
    pattern = row.get("pattern_type") or "risk"
    return f"{pattern} #{row['id']}"


def source_note_path(row: dict) -> str:
    category = row.get("category") or "source"
    source_id = row["id"]
    fallback = "source"
    name = row.get("name") or fallback
    return f"Sources/{category}/{source_id}-{slugify(name, fallback)}.md"


def content_note_path(row: dict) -> str:
    content_id = row["id"]
    month = month_bucket(row.get("published_at") or row.get("collected_at"))
    fallback = f"content-{content_id}"
    title = row.get("title") or row.get("body_text") or fallback
    return f"Content/{month}/{content_id}-{slugify(title, fallback)}.md"


def claim_note_path(row: dict) -> str:
    claim_id = row["id"]
    fallback = f"claim-{claim_id}"
    text = row.get("claim_text") or fallback
    return f"Claims/{claim_id}-{slugify(text, fallback)}.md"


def case_note_path(row: dict) -> str:
    case_id = row["id"]
    fallback = f"case-{case_id}"
    title = row.get("title") or fallback
    return f"Cases/{case_id}-{slugify(title, fallback)}.md"


def entity_note_path(row: dict) -> str:
    entity_id = row["id"]
    entity_type = row.get("entity_type") or "entity"
    fallback = f"entity-{entity_id}"
    title = row.get("canonical_name") or fallback
    return f"Entities/{entity_type}/{entity_id}-{slugify(title, fallback)}.md"


def bill_note_path(row: dict) -> str:
    bill_id = row["id"]
    fallback = f"bill-{bill_id}"
    title = row.get("number") or row.get("title") or fallback
    return f"Bills/{bill_id}-{slugify(title, fallback)}.md"


def vote_note_path(row: dict) -> str:
    vote_id = row["id"]
    fallback = f"vote-{vote_id}"
    title = vote_title(row)
    return f"VoteSessions/{vote_id}-{slugify(title, fallback)}.md"


def contract_note_path(row: dict) -> str:
    contract_id = row["id"]
    number = row.get("contract_number") or f"contract-{contract_id}"
    title = contract_title(row)
    return (
        f"Contracts/{contract_id}-{slugify(number, f'contract-{contract_id}')}"
        f"-{slugify(title, 'contract', max_len=50)}.md"
    )


def risk_note_path(row: dict) -> str:
    risk_id = row["id"]
    fallback = f"risk-{risk_id}"
    stem = f"{row.get('pattern_type') or 'risk'}-{row.get('description') or risk_id}"
    return f"Risks/{risk_id}-{slugify(stem, fallback, max_len=80)}.md"


def weak_note_path(entity_row: dict) -> str:
    entity_id = entity_row["id"]
    entity_type = entity_row.get("entity_type") or "entity"
    fallback = f"entity-{entity_id}"
    title = entity_row.get("canonical_name") or fallback
    return f"WeakLinks/{entity_type}/{entity_id}-{slugify(title, fallback)}.md"


@dataclass
class GraphContext:
    pipeline_version: str | None
    source_rows: list[dict]
    content_rows: list[dict]
    entity_rows: list[dict]
    claim_rows: list[dict]
    case_rows: list[dict]
    bill_rows: list[dict]
    vote_rows: list[dict]
    contract_rows: list[dict]
    risk_rows: list[dict]
    source_paths: dict[int, str]
    content_paths: dict[int, str]
    entity_paths: dict[int, str]
    claim_paths: dict[int, str]
    case_paths: dict[int, str]
    bill_paths: dict[int, str]
    vote_paths: dict[int, str]
    contract_paths: dict[int, str]
    risk_paths: dict[int, str]
    weak_paths: dict[int, str]
    attachments_by_content: dict[int, list[dict]]
    tags_by_content: dict[int, list[dict]]
    tag_index: dict[str, set[int]]
    entity_mentions_by_content: dict[int, list[dict]]
    content_ids_by_entity: dict[int, list[int]]
    claims_by_content: dict[int, list[dict]]
    claims_by_entity: dict[int, list[int]]
    evidence_by_claim: dict[int, list[dict]]
    case_claims_by_case: dict[int, list[dict]]
    case_ids_by_claim: dict[int, list[int]]
    case_ids_by_entity: dict[int, list[int]]
    case_ids_by_content: dict[int, list[int]]
    case_events_by_case: dict[int, list[dict]]
    risk_ids_by_entity: dict[int, list[int]]
    risk_ids_by_content: dict[int, list[int]]
    risk_ids_by_case: dict[int, list[int]]
    strong_relations_by_entity: dict[int, list[dict]]
    weak_relations_by_entity: dict[int, list[dict]]
    positions_by_entity: dict[int, list[dict]]
    parties_by_entity: dict[int, list[dict]]
    bills_by_entity: dict[int, list[dict]]
    sponsors_by_bill: dict[int, list[dict]]
    vote_ids_by_entity: dict[int, list[int]]
    votes_by_session: dict[int, list[dict]]
    contract_ids_by_entity: dict[int, list[int]]
    contract_ids_by_content: dict[int, list[int]]
    content_to_bill_ids: dict[int, list[int]]
    source_rows_by_id: dict[int, dict]
    content_rows_by_id: dict[int, dict]
    entity_rows_by_id: dict[int, dict]
    claim_rows_by_id: dict[int, dict]
    case_rows_by_id: dict[int, dict]
    bill_rows_by_id: dict[int, dict]
    vote_rows_by_id: dict[int, dict]
    contract_rows_by_id: dict[int, dict]
    risk_rows_by_id: dict[int, dict]


def build_graph_context(conn: sqlite3.Connection) -> GraphContext:
    pipeline_version = get_runtime_metadata(
        conn,
        "analysis_built_from_pipeline_version",
        get_runtime_metadata(conn, "current_pipeline_version"),
    )
    source_rows = []
    if table_exists(conn, "sources"):
        source_rows = [dict(r) for r in rows(conn, "SELECT * FROM sources ORDER BY category, name, id")]

    content_rows = []
    if table_exists(conn, "content_items"):
        if table_exists(conn, "sources"):
            content_rows = [
                dict(r)
                for r in rows(
                    conn,
                    """
                    SELECT c.*, s.name AS source_name, s.category AS source_category
                    FROM content_items c
                    LEFT JOIN sources s ON s.id = c.source_id
                    ORDER BY COALESCE(c.published_at, c.collected_at, c.id), c.id
                    """,
                )
            ]
        else:
            content_rows = [
                dict(r)
                for r in rows(
                    conn,
                    """
                    SELECT c.*, NULL AS source_name, NULL AS source_category
                    FROM content_items c
                    ORDER BY COALESCE(c.published_at, c.collected_at, c.id), c.id
                    """,
                )
            ]

    entity_rows = []
    if table_exists(conn, "entities"):
        entity_rows = [dict(r) for r in rows(conn, "SELECT * FROM entities ORDER BY entity_type, canonical_name, id")]

    claim_rows = []
    if table_exists(conn, "claims"):
        claim_rows = [dict(r) for r in rows(conn, "SELECT * FROM claims ORDER BY id")]

    case_rows = []
    if table_exists(conn, "cases"):
        case_rows = [dict(r) for r in rows(conn, "SELECT * FROM cases ORDER BY id")]

    bill_rows = []
    if table_exists(conn, "bills"):
        bill_rows = [
            dict(r)
            for r in rows(
                conn,
                "SELECT * FROM bills ORDER BY COALESCE(registration_date, id), id",
            )
        ]

    vote_rows = []
    if table_exists(conn, "bill_vote_sessions"):
        if table_exists(conn, "bills"):
            vote_rows = [
                dict(r)
                for r in rows(
                    conn,
                    """
                    SELECT bvs.*, b.number AS bill_number, b.title AS bill_title
                    FROM bill_vote_sessions bvs
                    LEFT JOIN bills b ON b.id = bvs.bill_id
                    ORDER BY COALESCE(bvs.vote_date, bvs.id), bvs.id
                    """,
                )
            ]
        else:
            vote_rows = [
                dict(r)
                for r in rows(
                    conn,
                    """
                    SELECT bvs.*, NULL AS bill_number, NULL AS bill_title
                    FROM bill_vote_sessions bvs
                    ORDER BY COALESCE(bvs.vote_date, bvs.id), bvs.id
                    """,
                )
            ]

    contract_rows: list[dict] = []
    if table_exists(conn, "contracts"):
        contract_rows = [
            dict(r)
            for r in rows(
                conn,
                """
                SELECT id, material_id, content_item_id, contract_number, title, summary,
                       publication_date, source_org, customer_inn, supplier_inn, raw_data
                FROM contracts
                ORDER BY COALESCE(publication_date, id), id
                """,
            )
        ]
        party_rows_by_contract: dict[int, list[dict]] = defaultdict(list)
        if table_exists(conn, "contract_parties"):
            for row in rows(conn, "SELECT * FROM contract_parties ORDER BY contract_id, id"):
                party_rows_by_contract[row["contract_id"]].append(dict(row))
        for record in contract_rows:
            raw_data = parse_json(record.get("raw_data"), {})
            if not isinstance(raw_data, dict):
                raw_data = {}
            record["raw_data_dict"] = raw_data
            if not record.get("contract_number"):
                record["contract_number"] = str(raw_data.get("contract_number") or "").strip()
            entity_ids: list[int] = []
            for party in party_rows_by_contract.get(record["id"], []):
                entity_id = coerce_int(party.get("entity_id"))
                if entity_id is not None and entity_id not in entity_ids:
                    entity_ids.append(entity_id)
            record["entity_ids"] = entity_ids
    elif table_exists(conn, "investigative_materials"):
        for row in rows(
            conn,
            """
            SELECT id, content_item_id, title, summary, involved_entities, publication_date, source_org, raw_data
            FROM investigative_materials
            WHERE material_type='government_contract'
            ORDER BY COALESCE(publication_date, id), id
            """,
        ):
            record = dict(row)
            raw_data = parse_json(record.get("raw_data"), {})
            if not isinstance(raw_data, dict):
                raw_data = {}
            record["raw_data_dict"] = raw_data
            record["contract_number"] = str(raw_data.get("contract_number") or "").strip()
            entity_ids: list[int] = []
            involved = parse_json(record.get("involved_entities"), [])
            if isinstance(involved, list):
                for item in involved:
                    if isinstance(item, dict):
                        value = coerce_int(item.get("entity_id"))
                    else:
                        value = coerce_int(item)
                    if value is not None and value not in entity_ids:
                        entity_ids.append(value)
            record["entity_ids"] = entity_ids
            contract_rows.append(record)

    risk_rows: list[dict] = []
    if table_exists(conn, "risk_patterns"):
        for row in rows(conn, "SELECT * FROM risk_patterns ORDER BY id"):
            record = dict(row)
            record["entity_ids_list"] = parse_int_list(record.get("entity_ids"))
            record["evidence_ids_list"] = parse_int_list(record.get("evidence_ids"))
            risk_rows.append(record)

    source_paths = {row["id"]: source_note_path(row) for row in source_rows}
    content_paths = {row["id"]: content_note_path(row) for row in content_rows}
    entity_paths = {row["id"]: entity_note_path(row) for row in entity_rows}
    claim_paths = {row["id"]: claim_note_path(row) for row in claim_rows}
    case_paths = {row["id"]: case_note_path(row) for row in case_rows}
    bill_paths = {row["id"]: bill_note_path(row) for row in bill_rows}
    vote_paths = {row["id"]: vote_note_path(row) for row in vote_rows}
    contract_paths = {row["id"]: contract_note_path(row) for row in contract_rows}
    risk_paths = {row["id"]: risk_note_path(row) for row in risk_rows}

    attachments_by_content: dict[int, list[dict]] = defaultdict(list)
    if table_exists(conn, "attachments"):
        for row in rows(conn, "SELECT * FROM attachments ORDER BY content_item_id, id"):
            attachments_by_content[row["content_item_id"]].append(dict(row))

    tags_by_content: dict[int, list[dict]] = defaultdict(list)
    tag_index: dict[str, set[int]] = defaultdict(set)
    if table_exists(conn, "content_tags"):
        for row in rows(conn, "SELECT * FROM content_tags ORDER BY content_item_id, tag_level, tag_name, id"):
            record = dict(row)
            record["obsidian_tag"] = normalize_tag_name(record.get("tag_name") or "")
            tags_by_content[record["content_item_id"]].append(record)
            tag_index[record["obsidian_tag"]].add(record["content_item_id"])

    entity_mentions_by_content: dict[int, list[dict]] = defaultdict(list)
    content_ids_by_entity: dict[int, list[int]] = defaultdict(list)
    if table_exists(conn, "entity_mentions") and table_exists(conn, "entities"):
        for row in rows(
            conn,
            """
            SELECT em.content_item_id, em.entity_id, em.mention_type, em.confidence,
                   e.entity_type, e.canonical_name
            FROM entity_mentions em
            JOIN entities e ON e.id = em.entity_id
            ORDER BY em.content_item_id, em.confidence DESC, e.canonical_name
            """,
        ):
            record = dict(row)
            entity_mentions_by_content[record["content_item_id"]].append(record)
            content_ids_by_entity[record["entity_id"]].append(record["content_item_id"])

    claims_by_content: dict[int, list[dict]] = defaultdict(list)
    claims_by_entity: dict[int, list[int]] = defaultdict(list)
    for claim in claim_rows:
        content_id = claim.get("content_item_id")
        if content_id is None:
            continue
        claims_by_content[content_id].append(claim)
        for mention in entity_mentions_by_content.get(content_id, []):
            if claim["id"] not in claims_by_entity[mention["entity_id"]]:
                claims_by_entity[mention["entity_id"]].append(claim["id"])

    evidence_by_claim: dict[int, list[dict]] = defaultdict(list)
    if table_exists(conn, "evidence_links"):
        for row in rows(
            conn,
            """
            SELECT el.*, ci.title AS evidence_title, ci.content_type AS evidence_content_type
            FROM evidence_links el
            LEFT JOIN content_items ci ON ci.id = el.evidence_item_id
            ORDER BY el.claim_id, el.id
            """,
        ):
            evidence_by_claim[row["claim_id"]].append(dict(row))

    case_claims_by_case: dict[int, list[dict]] = defaultdict(list)
    case_ids_by_claim: dict[int, list[int]] = defaultdict(list)
    case_ids_by_entity: dict[int, list[int]] = defaultdict(list)
    case_ids_by_content: dict[int, list[int]] = defaultdict(list)
    if table_exists(conn, "case_claims") and table_exists(conn, "claims"):
        for row in rows(
            conn,
            """
            SELECT cc.case_id, cc.claim_id, cc.role, cl.content_item_id, cl.claim_text
            FROM case_claims cc
            JOIN claims cl ON cl.id = cc.claim_id
            ORDER BY cc.case_id, cc.claim_id
            """,
        ):
            record = dict(row)
            case_claims_by_case[record["case_id"]].append(record)
            case_ids_by_claim[record["claim_id"]].append(record["case_id"])
            if record.get("content_item_id") is not None:
                case_ids_by_content[record["content_item_id"]].append(record["case_id"])
                for mention in entity_mentions_by_content.get(record["content_item_id"], []):
                    if record["case_id"] not in case_ids_by_entity[mention["entity_id"]]:
                        case_ids_by_entity[mention["entity_id"]].append(record["case_id"])

    case_events_by_case: dict[int, list[dict]] = defaultdict(list)
    if table_exists(conn, "case_events"):
        for row in rows(conn, "SELECT * FROM case_events ORDER BY case_id, event_date, event_order, id"):
            record = dict(row)
            case_events_by_case[record["case_id"]].append(record)
            if record.get("content_item_id"):
                case_ids_by_content[record["content_item_id"]].append(record["case_id"])

    risk_ids_by_entity: dict[int, list[int]] = defaultdict(list)
    risk_ids_by_content: dict[int, list[int]] = defaultdict(list)
    risk_ids_by_case: dict[int, list[int]] = defaultdict(list)
    for risk in risk_rows:
        for entity_id in risk["entity_ids_list"]:
            risk_ids_by_entity[entity_id].append(risk["id"])
        for content_id in risk["evidence_ids_list"]:
            risk_ids_by_content[content_id].append(risk["id"])
        if risk.get("case_id"):
            risk_ids_by_case[risk["case_id"]].append(risk["id"])

    strong_relations_by_entity: dict[int, list[dict]] = defaultdict(list)
    weak_relations_by_entity: dict[int, list[dict]] = defaultdict(list)
    weak_paths: dict[int, str] = {}
    if table_exists(conn, "entity_relations") and table_exists(conn, "entities"):
        entity_rows_by_id = {row["id"]: row for row in entity_rows}
        for row in rows(
            conn,
            """
            SELECT er.*, e1.canonical_name AS from_name, e1.entity_type AS from_type,
                   e2.canonical_name AS to_name, e2.entity_type AS to_type
            FROM entity_relations er
            JOIN entities e1 ON e1.id = er.from_entity_id
            JOIN entities e2 ON e2.id = er.to_entity_id
            ORDER BY er.id
            """,
        ):
            record = dict(row)
            layer = relation_layer(
                record["relation_type"],
                record["evidence_item_id"],
                record.get("detected_by"),
            )
            support_count = relation_support_count(record.get("detected_by"))
            for entity_id, other_id, other_name, other_type, outgoing in (
                (record["from_entity_id"], record["to_entity_id"], record["to_name"], record["to_type"], True),
                (record["to_entity_id"], record["from_entity_id"], record["from_name"], record["from_type"], False),
            ):
                item = {
                    "relation_type": record["relation_type"],
                    "label": relation_label(record["relation_type"], outgoing),
                    "other_entity_id": other_id,
                    "other_name": other_name,
                    "other_type": other_type,
                    "strength": record.get("strength") or "",
                    "detected_by": record.get("detected_by") or "",
                    "evidence_item_id": record.get("evidence_item_id"),
                    "support_count": support_count,
                    "layer": layer,
                }
                if layer == "weak_similarity":
                    weak_relations_by_entity[entity_id].append(item)
                    entity_row = entity_rows_by_id.get(entity_id)
                    if entity_row is not None:
                        weak_paths[entity_id] = weak_note_path(entity_row)
                else:
                    strong_relations_by_entity[entity_id].append(item)

    if table_exists(conn, "relation_candidates") and table_exists(conn, "entities"):
        entity_rows_by_id = {row["id"]: row for row in entity_rows}
        for row in rows(
            conn,
            """
            SELECT rc.*, e1.canonical_name AS entity_a_name, e1.entity_type AS entity_a_type,
                   e2.canonical_name AS entity_b_name, e2.entity_type AS entity_b_type
            FROM relation_candidates rc
            JOIN entities e1 ON e1.id = rc.entity_a_id
            JOIN entities e2 ON e2.id = rc.entity_b_id
            WHERE rc.promotion_state IN ('pending', 'review')
            ORDER BY rc.score DESC, rc.id DESC
            """,
        ):
            record = dict(row)
            for entity_id, other_id, other_name, other_type in (
                (record["entity_a_id"], record["entity_b_id"], record["entity_b_name"], record["entity_b_type"]),
                (record["entity_b_id"], record["entity_a_id"], record["entity_a_name"], record["entity_a_type"]),
            ):
                weak_relations_by_entity[entity_id].append(
                    {
                        "relation_type": record["candidate_type"],
                        "label": record["candidate_type"],
                        "other_entity_id": other_id,
                        "other_name": other_name,
                        "other_type": other_type,
                        "strength": "candidate",
                        "detected_by": record.get("origin") or "",
                        "evidence_item_id": None,
                        "support_count": record.get("support_items") or 0,
                        "layer": "weak_similarity",
                        "score": record.get("score") or 0,
                    }
                )
                entity_row = entity_rows_by_id.get(entity_id)
                if entity_row is not None:
                    weak_paths[entity_id] = weak_note_path(entity_row)

    positions_by_entity: dict[int, list[dict]] = defaultdict(list)
    if table_exists(conn, "official_positions"):
        for row in rows(
            conn,
            "SELECT * FROM official_positions ORDER BY entity_id, is_active DESC, started_at DESC, id DESC",
        ):
            positions_by_entity[row["entity_id"]].append(dict(row))

    parties_by_entity: dict[int, list[dict]] = defaultdict(list)
    if table_exists(conn, "party_memberships"):
        for row in rows(
            conn,
            "SELECT * FROM party_memberships ORDER BY entity_id, is_current DESC, started_at DESC, id DESC",
        ):
            parties_by_entity[row["entity_id"]].append(dict(row))

    bills_by_entity: dict[int, list[dict]] = defaultdict(list)
    sponsors_by_bill: dict[int, list[dict]] = defaultdict(list)
    if table_exists(conn, "bill_sponsors") and table_exists(conn, "bills"):
        for row in rows(
            conn,
            """
            SELECT bs.bill_id, bs.entity_id, bs.sponsor_name, bs.sponsor_role, bs.faction,
                   b.number, b.title, b.status, b.registration_date
            FROM bill_sponsors bs
            JOIN bills b ON b.id = bs.bill_id
            ORDER BY bs.bill_id, bs.id
            """,
        ):
            record = dict(row)
            if record.get("entity_id") is not None:
                bills_by_entity[record["entity_id"]].append(record)
            sponsors_by_bill[record["bill_id"]].append(record)

    vote_ids_by_entity: dict[int, list[int]] = defaultdict(list)
    votes_by_session: dict[int, list[dict]] = defaultdict(list)
    if table_exists(conn, "bill_votes") and table_exists(conn, "bill_vote_sessions"):
        if table_exists(conn, "bills"):
            vote_sql = """
                SELECT bv.vote_session_id, bv.entity_id, bv.deputy_name, bv.faction, bv.vote_result,
                       bvs.vote_date, bvs.vote_stage, b.number AS bill_number, b.title AS bill_title
                FROM bill_votes bv
                JOIN bill_vote_sessions bvs ON bvs.id = bv.vote_session_id
                LEFT JOIN bills b ON b.id = bvs.bill_id
                ORDER BY bv.vote_session_id, bv.id
            """
        else:
            vote_sql = """
                SELECT bv.vote_session_id, bv.entity_id, bv.deputy_name, bv.faction, bv.vote_result,
                       bvs.vote_date, bvs.vote_stage, NULL AS bill_number, NULL AS bill_title
                FROM bill_votes bv
                JOIN bill_vote_sessions bvs ON bvs.id = bv.vote_session_id
                ORDER BY bv.vote_session_id, bv.id
            """
        for row in rows(conn, vote_sql):
            record = dict(row)
            votes_by_session[record["vote_session_id"]].append(record)
            if record.get("entity_id") is not None:
                if record["vote_session_id"] not in vote_ids_by_entity[record["entity_id"]]:
                    vote_ids_by_entity[record["entity_id"]].append(record["vote_session_id"])

    contract_ids_by_entity: dict[int, list[int]] = defaultdict(list)
    contract_ids_by_content: dict[int, list[int]] = defaultdict(list)
    entity_by_inn = {str(row.get("inn")).strip(): row["id"] for row in entity_rows if row.get("inn")}
    for contract in contract_rows:
        raw_data = contract["raw_data_dict"]
        for key in ("customer_inn", "supplier_inn"):
            inn = str(raw_data.get(key) or "").strip()
            if inn and inn in entity_by_inn and entity_by_inn[inn] not in contract["entity_ids"]:
                contract["entity_ids"].append(entity_by_inn[inn])
        for entity_id in contract["entity_ids"]:
            contract_ids_by_entity[entity_id].append(contract["id"])
        if contract.get("content_item_id"):
            contract_ids_by_content[contract["content_item_id"]].append(contract["id"])

    content_to_bill_ids: dict[int, list[int]] = defaultdict(list)
    bills_by_number = {
        str(row.get("number") or "").strip(): row["id"]
        for row in bill_rows
        if row.get("number")
    }
    for content in content_rows:
        if content.get("content_type") != "bill":
            continue
        content_text = " ".join(str(content.get(field) or "") for field in ("title", "body_text"))
        for bill_number, bill_id in bills_by_number.items():
            if bill_number and bill_number in content_text:
                content_to_bill_ids[content["id"]].append(bill_id)

    return GraphContext(
        pipeline_version=pipeline_version,
        source_rows=source_rows,
        content_rows=content_rows,
        entity_rows=entity_rows,
        claim_rows=claim_rows,
        case_rows=case_rows,
        bill_rows=bill_rows,
        vote_rows=vote_rows,
        contract_rows=contract_rows,
        risk_rows=risk_rows,
        source_paths=source_paths,
        content_paths=content_paths,
        entity_paths=entity_paths,
        claim_paths=claim_paths,
        case_paths=case_paths,
        bill_paths=bill_paths,
        vote_paths=vote_paths,
        contract_paths=contract_paths,
        risk_paths=risk_paths,
        weak_paths=weak_paths,
        attachments_by_content=attachments_by_content,
        tags_by_content=tags_by_content,
        tag_index=tag_index,
        entity_mentions_by_content=entity_mentions_by_content,
        content_ids_by_entity=content_ids_by_entity,
        claims_by_content=claims_by_content,
        claims_by_entity=claims_by_entity,
        evidence_by_claim=evidence_by_claim,
        case_claims_by_case=case_claims_by_case,
        case_ids_by_claim=case_ids_by_claim,
        case_ids_by_entity=case_ids_by_entity,
        case_ids_by_content=case_ids_by_content,
        case_events_by_case=case_events_by_case,
        risk_ids_by_entity=risk_ids_by_entity,
        risk_ids_by_content=risk_ids_by_content,
        risk_ids_by_case=risk_ids_by_case,
        strong_relations_by_entity=strong_relations_by_entity,
        weak_relations_by_entity=weak_relations_by_entity,
        positions_by_entity=positions_by_entity,
        parties_by_entity=parties_by_entity,
        bills_by_entity=bills_by_entity,
        sponsors_by_bill=sponsors_by_bill,
        vote_ids_by_entity=vote_ids_by_entity,
        votes_by_session=votes_by_session,
        contract_ids_by_entity=contract_ids_by_entity,
        contract_ids_by_content=contract_ids_by_content,
        content_to_bill_ids=content_to_bill_ids,
        source_rows_by_id={row["id"]: row for row in source_rows},
        content_rows_by_id={row["id"]: row for row in content_rows},
        entity_rows_by_id={row["id"]: row for row in entity_rows},
        claim_rows_by_id={row["id"]: row for row in claim_rows},
        case_rows_by_id={row["id"]: row for row in case_rows},
        bill_rows_by_id={row["id"]: row for row in bill_rows},
        vote_rows_by_id={row["id"]: row for row in vote_rows},
        contract_rows_by_id={row["id"]: row for row in contract_rows},
        risk_rows_by_id={row["id"]: row for row in risk_rows},
    )


def export_graph_index(vault: Path, ctx: GraphContext, generated_at: str):
    lines = [
        frontmatter(
            {
                "type": "graph_index",
                "generated_at": generated_at,
                "mode": "graph",
                "built_from_pipeline_version": ctx.pipeline_version,
            }
        ),
        "# Investigation Graph",
        "",
        f"Generated at: `{generated_at}`",
        f"Pipeline version: `{ctx.pipeline_version or ''}`",
        "",
        "## Sections",
        "",
        "- [[Sources/index|Sources]]",
        "- [[Content/index|Content]]",
        "- [[Claims/index|Claims]]",
        "- [[Cases/index|Cases]]",
        "- [[Entities/index|Entities]]",
        "- [[Bills/index|Bills]]",
        "- [[VoteSessions/index|Vote Sessions]]",
        "- [[Contracts/index|Contracts]]",
        "- [[Risks/index|Risks]]",
        "- [[WeakLinks/index|Weak Similarity]]",
        "- [[Tags/index|Tags]]",
        "- [[Files/index|Files]]",
        "",
        "## Counts",
        "",
        f"- Sources: `{len(ctx.source_rows)}`",
        f"- Content: `{len(ctx.content_rows)}`",
        f"- Claims: `{len(ctx.claim_rows)}`",
        f"- Cases: `{len(ctx.case_rows)}`",
        f"- Entities: `{len(ctx.entity_rows)}`",
        f"- Bills: `{len(ctx.bill_rows)}`",
        f"- Vote sessions: `{len(ctx.vote_rows)}`",
        f"- Contracts: `{len(ctx.contract_rows)}`",
        f"- Risks: `{len(ctx.risk_rows)}`",
    ]
    write_note(vault, "index.md", "\n".join(lines))


def export_graph_sources(vault: Path, ctx: GraphContext):
    index_lines = ["# Sources", "", "| ID | Category | Name | Tier |", "|---:|---|---|---|"]
    content_count_by_source = defaultdict(int)
    for row in ctx.content_rows:
        if row.get("source_id") is not None:
            content_count_by_source[row["source_id"]] += 1

    for src in ctx.source_rows:
        rel = ctx.source_paths[src["id"]]
        title = src.get("name") or "source"
        index_lines.append(
            f"| {src['id']} | {md_escape(src.get('category'))} | {note_link(rel, md_escape(title))} | {md_escape(src.get('credibility_tier'))} |"
        )
        body = [
            frontmatter(
                {
                    "type": "source",
                    "source_id": src["id"],
                    "category": src.get("category"),
                    "credibility_tier": src.get("credibility_tier"),
                    "content_count": content_count_by_source.get(src["id"], 0),
                }
            ),
            f"# {title}",
            "",
            f"- ID: `{src['id']}`",
            f"- Category: `{src.get('category') or ''}`",
            f"- URL: {src.get('url') or ''}",
            f"- Access: `{src.get('access_method') or ''}`",
            f"- Tier: `{src.get('credibility_tier') or ''}`",
            f"- Official: `{src.get('is_official') or 0}`",
            f"- Content items: `{content_count_by_source.get(src['id'], 0)}`",
        ]
        notes = (src.get("notes") or "").strip()
        if notes:
            body.extend(["", "## Notes", "", notes])
        write_note(vault, rel, "\n".join(body))

    write_note(vault, "Sources/index.md", "\n".join(index_lines))


def export_graph_content(vault: Path, ctx: GraphContext, copy_media: bool):
    index_lines = ["# Content", "", "| ID | Date | Type | Title |", "|---:|---|---|---|"]

    for item in ctx.content_rows:
        rel = ctx.content_paths[item["id"]]
        title = content_title(item)
        index_lines.append(
            f"| {item['id']} | {md_escape((item.get('published_at') or '')[:10])} | {md_escape(item.get('content_type'))} | {note_link(rel, md_escape(title))} |"
        )

        tags = ctx.tags_by_content.get(item["id"], [])
        mentions = ctx.entity_mentions_by_content.get(item["id"], [])
        claims = ctx.claims_by_content.get(item["id"], [])
        content_case_ids = dedupe_ints(ctx.case_ids_by_content.get(item["id"], []))
        content_risk_ids = dedupe_ints(ctx.risk_ids_by_content.get(item["id"], []))
        content_bill_ids = dedupe_ints(ctx.content_to_bill_ids.get(item["id"], []))
        content_contract_ids = dedupe_ints(ctx.contract_ids_by_content.get(item["id"], []))
        attachments = ctx.attachments_by_content.get(item["id"], [])

        body = [
            frontmatter(
                {
                    "type": "content_item",
                    "content_id": item["id"],
                    "source_id": item.get("source_id"),
                    "source_name": item.get("source_name"),
                    "content_type": item.get("content_type"),
                    "status": item.get("status"),
                    "published_at": item.get("published_at"),
                    "url": item.get("url"),
                    "tags": [tag["obsidian_tag"] for tag in tags],
                }
            ),
            f"# {title}",
            "",
            f"- ID: `{item['id']}`",
            f"- Source: {item.get('source_name') or ''} (`{item.get('source_category') or ''}`)",
            f"- Type: `{item.get('content_type') or ''}`",
            f"- Status: `{item.get('status') or ''}`",
            f"- Published: `{item.get('published_at') or ''}`",
            f"- URL: {item.get('url') or ''}",
            "",
            "## Text",
            "",
            item.get("body_text") or "",
        ]

        if attachments:
            body.extend(["", "## Attachments", ""])
            for att in attachments:
                link, status = copy_attachment(vault, att, copy_media=copy_media)
                label = link or f"`{status}`"
                body.append(
                    f"- {label} type=`{att.get('attachment_type') or ''}` size=`{att.get('file_size') or 0}` hash=`{att.get('hash_sha256') or ''}`"
                )
                if att.get("ocr_text"):
                    body.extend(["", f"### OCR attachment {att['id']}", "", att["ocr_text"]])

        if mentions:
            body.extend(["", "## Entities", ""])
            for ent in mentions[:50]:
                path = ctx.entity_paths.get(ent["entity_id"])
                if not path:
                    continue
                body.append(
                    f"- {note_link(path, ent['canonical_name'])} `{ent['entity_type']}` ({ent.get('mention_type') or ''}, {ent.get('confidence') or 0:.2f})"
                )

        if claims:
            body.extend(["", "## Claims", ""])
            for claim in claims:
                claim_path = ctx.claim_paths.get(claim["id"])
                if claim_path:
                    body.append(
                        f"- {note_link(claim_path, claim_title(claim))} `{claim.get('status') or ''}`"
                    )

        if content_case_ids:
            body.extend(["", "## Cases", ""])
            for case_id in content_case_ids:
                case = ctx.case_rows_by_id.get(case_id)
                case_path = ctx.case_paths.get(case_id)
                if case and case_path:
                    body.append(f"- {note_link(case_path, case_title(case))}")

        if content_risk_ids:
            body.extend(["", "## Risks", ""])
            for risk_id in content_risk_ids:
                risk = ctx.risk_rows_by_id.get(risk_id)
                risk_path = ctx.risk_paths.get(risk_id)
                if risk and risk_path:
                    body.append(f"- {note_link(risk_path, risk_title(risk))}")

        if content_bill_ids:
            body.extend(["", "## Bills", ""])
            for bill_id in content_bill_ids:
                bill = ctx.bill_rows_by_id.get(bill_id)
                bill_path = ctx.bill_paths.get(bill_id)
                if bill and bill_path:
                    body.append(f"- {note_link(bill_path, bill_title(bill))}")

        if content_contract_ids:
            body.extend(["", "## Contracts", ""])
            for contract_id in content_contract_ids:
                contract = ctx.contract_rows_by_id.get(contract_id)
                contract_path = ctx.contract_paths.get(contract_id)
                if contract and contract_path:
                    body.append(f"- {note_link(contract_path, contract_title(contract))}")

        if tags:
            body.extend(["", "## Hashtags", "", " ".join(f"#{tag['obsidian_tag']}" for tag in tags)])

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "Content/index.md", "\n".join(index_lines))


def export_graph_claims(vault: Path, ctx: GraphContext):
    index_lines = ["# Claims", "", "| ID | Status | Type | Text |", "|---:|---|---|---|"]

    for claim in ctx.claim_rows:
        rel = ctx.claim_paths[claim["id"]]
        text_preview = md_escape((claim.get("claim_text") or "")[:80])
        index_lines.append(
            f"| {claim['id']} | {md_escape(claim.get('status'))} | {md_escape(claim.get('claim_type'))} | {note_link(rel, text_preview or claim_title(claim))} |"
        )

        content = ctx.content_rows_by_id.get(claim["content_item_id"])
        entities = ctx.entity_mentions_by_content.get(claim["content_item_id"], [])
        linked_cases = dedupe_ints(ctx.case_ids_by_claim.get(claim["id"], []))
        evidence = ctx.evidence_by_claim.get(claim["id"], [])

        body = [
            frontmatter(
                {
                    "type": "claim",
                    "claim_id": claim["id"],
                    "status": claim.get("status"),
                    "claim_type": claim.get("claim_type"),
                }
            ),
            f"# {claim_title(claim)}",
            "",
            claim.get("claim_text") or "",
            "",
            f"- Content: {note_link(ctx.content_paths[claim['content_item_id']], content_title(content)) if content and claim['content_item_id'] in ctx.content_paths else claim['content_item_id']}",
            f"- Type: `{claim.get('claim_type') or ''}`",
            f"- Status: `{claim.get('status') or ''}`",
            f"- Final confidence: `{claim.get('confidence_final') or ''}`",
        ]

        if entities:
            body.extend(["", "## Entities", ""])
            for ent in entities:
                path = ctx.entity_paths.get(ent["entity_id"])
                if path:
                    body.append(f"- {note_link(path, ent['canonical_name'])}")

        if linked_cases:
            body.extend(["", "## Cases", ""])
            for case_id in linked_cases:
                case = ctx.case_rows_by_id.get(case_id)
                case_path = ctx.case_paths.get(case_id)
                if case and case_path:
                    body.append(f"- {note_link(case_path, case_title(case))}")

        if evidence:
            body.extend(["", "## Evidence", ""])
            for ev in evidence:
                evidence_target = str(ev.get("evidence_item_id") or "")
                evidence_id = ev.get("evidence_item_id")
                if evidence_id in ctx.content_paths:
                    evidence_content = ctx.content_rows_by_id.get(evidence_id)
                    evidence_target = note_link(
                        ctx.content_paths[evidence_id],
                        content_title(evidence_content),
                    )
                body.append(
                    f"- `{ev.get('evidence_type') or ''}` `{ev.get('strength') or ''}` -> {evidence_target} {ev.get('notes') or ''}".strip()
                )

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "Claims/index.md", "\n".join(index_lines))


def export_graph_cases(vault: Path, ctx: GraphContext):
    index_lines = ["# Cases", "", "| ID | Status | Type | Title |", "|---:|---|---|---|"]

    for case in ctx.case_rows:
        rel = ctx.case_paths[case["id"]]
        title = case_title(case)
        index_lines.append(
            f"| {case['id']} | {md_escape(case.get('status'))} | {md_escape(case.get('case_type'))} | {note_link(rel, md_escape(title))} |"
        )

        linked_claims = ctx.case_claims_by_case.get(case["id"], [])
        entity_ids: list[int] = []
        content_ids: list[int] = []

        for claim in linked_claims:
            if claim["content_item_id"] not in content_ids:
                content_ids.append(claim["content_item_id"])
            for mention in ctx.entity_mentions_by_content.get(claim["content_item_id"], []):
                if mention["entity_id"] not in entity_ids:
                    entity_ids.append(mention["entity_id"])

        for event in ctx.case_events_by_case.get(case["id"], []):
            content_id = event.get("content_item_id")
            if content_id and content_id not in content_ids:
                content_ids.append(content_id)

        body = [
            frontmatter(
                {
                    "type": "case",
                    "case_id": case["id"],
                    "status": case.get("status"),
                    "case_type": case.get("case_type"),
                }
            ),
            f"# {title}",
            "",
            case.get("description") or "",
            "",
            f"- Status: `{case.get('status') or ''}`",
            f"- Type: `{case.get('case_type') or ''}`",
            f"- Region: `{case.get('region') or ''}`",
        ]

        if linked_claims:
            body.extend(["", "## Claims", ""])
            for claim in linked_claims:
                claim_row = ctx.claim_rows_by_id.get(claim["claim_id"])
                claim_path = ctx.claim_paths.get(claim["claim_id"])
                if claim_row and claim_path:
                    body.append(
                        f"- {note_link(claim_path, claim_title(claim_row))} `{claim.get('role') or ''}`"
                    )

        if entity_ids:
            body.extend(["", "## Entities", ""])
            for entity_id in entity_ids:
                entity = ctx.entity_rows_by_id.get(entity_id)
                entity_path = ctx.entity_paths.get(entity_id)
                if entity and entity_path:
                    body.append(f"- {note_link(entity_path, entity_title(entity))}")

        if content_ids:
            body.extend(["", "## Related content timeline", ""])
            ordered = []
            for content_id in content_ids:
                content = ctx.content_rows_by_id.get(content_id)
                if content:
                    ordered.append(content)
            ordered.sort(key=lambda item: (item.get("published_at") or "", item["id"]))
            for content in ordered[:50]:
                body.append(
                    f"- `{(content.get('published_at') or '')[:10]}` {note_link(ctx.content_paths[content['id']], content_title(content))}"
                )

        risk_ids = dedupe_ints(ctx.risk_ids_by_case.get(case["id"], []))
        if risk_ids:
            body.extend(["", "## Risks", ""])
            for risk_id in risk_ids:
                risk = ctx.risk_rows_by_id.get(risk_id)
                risk_path = ctx.risk_paths.get(risk_id)
                if risk and risk_path:
                    body.append(f"- {note_link(risk_path, risk_title(risk))}")

        events = ctx.case_events_by_case.get(case["id"], [])
        if events:
            body.extend(["", "## Events", ""])
            for event in events:
                target = ""
                content_id = event.get("content_item_id")
                if content_id in ctx.content_paths:
                    content = ctx.content_rows_by_id.get(content_id)
                    target = f" -> {note_link(ctx.content_paths[content_id], content_title(content))}"
                body.append(
                    f"- `{event.get('event_date') or ''}` {event.get('event_title') or ''}{target}"
                )

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "Cases/index.md", "\n".join(index_lines))


def export_graph_entities(vault: Path, ctx: GraphContext):
    index_lines = ["# Entities", "", "| ID | Type | Name | Content |", "|---:|---|---|---:|"]

    for ent in ctx.entity_rows:
        rel = ctx.entity_paths[ent["id"]]
        related_content_ids = dedupe_ints(ctx.content_ids_by_entity.get(ent["id"], []))
        title = entity_title(ent)
        index_lines.append(
            f"| {ent['id']} | {md_escape(ent.get('entity_type'))} | {note_link(rel, md_escape(title))} | {len(related_content_ids)} |"
        )

        claim_ids = dedupe_ints(ctx.claims_by_entity.get(ent["id"], []))
        case_ids = dedupe_ints(ctx.case_ids_by_entity.get(ent["id"], []))
        risk_ids = dedupe_ints(ctx.risk_ids_by_entity.get(ent["id"], []))
        bill_ids = dedupe_ints(item["bill_id"] for item in ctx.bills_by_entity.get(ent["id"], []))
        vote_ids = dedupe_ints(ctx.vote_ids_by_entity.get(ent["id"], []))
        contract_ids = dedupe_ints(ctx.contract_ids_by_entity.get(ent["id"], []))
        strong_links = sorted(
            ctx.strong_relations_by_entity.get(ent["id"], []),
            key=lambda item: (item.get("label") or "", item.get("other_name") or ""),
        )

        for vote_id in vote_ids:
            vote_row = ctx.vote_rows_by_id.get(vote_id)
            if vote_row and vote_row.get("bill_id") is not None:
                bill_ids.append(vote_row["bill_id"])

        for content_id in related_content_ids:
            for bill_id in ctx.content_to_bill_ids.get(content_id, []):
                bill_ids.append(bill_id)

        bill_ids = dedupe_ints(bill_ids)

        body = [
            frontmatter(
                {
                    "type": "entity",
                    "entity_id": ent["id"],
                    "entity_type": ent.get("entity_type"),
                }
            ),
            f"# {title}",
            "",
            f"- ID: `{ent['id']}`",
            f"- Type: `{ent.get('entity_type') or ''}`",
            f"- INN: `{ent.get('inn') or ''}`",
            f"- OGRN: `{ent.get('ogrn') or ''}`",
            f"- Related content: `{len(related_content_ids)}`",
        ]

        description = (ent.get("description") or "").strip()
        if description:
            body.extend(["", description])

        positions = ctx.positions_by_entity.get(ent["id"], [])
        if positions:
            body.extend(["", "## Positions", ""])
            seen_positions: set[tuple[str, str]] = set()
            for position in positions[:10]:
                active = " [active]" if position.get("is_active") else ""
                organization = position.get("organization") or ""
                title_text = position.get("position_title") or ""
                position_key = (title_text, organization)
                if position_key in seen_positions:
                    continue
                seen_positions.add(position_key)
                body.append(f"- {title_text} @ {organization}{active}".strip())

        parties = ctx.parties_by_entity.get(ent["id"], [])
        if parties:
            body.extend(["", "## Party memberships", ""])
            for party in parties[:10]:
                current = " [current]" if party.get("is_current") else ""
                role = f" ({party.get('role')})" if party.get("role") else ""
                body.append(f"- {party.get('party_name') or ''}{role}{current}")

        if strong_links:
            body.extend(["", "## Strong links", ""])
            for item in strong_links[:50]:
                target_path = ctx.entity_paths.get(item["other_entity_id"])
                if not target_path:
                    continue
                target = note_link(target_path, item["other_name"])
                meta = []
                if item.get("strength"):
                    meta.append(item["strength"])
                if item.get("detected_by"):
                    meta.append(item["detected_by"])
                evidence_id = item.get("evidence_item_id")
                if evidence_id in ctx.content_paths:
                    evidence_content = ctx.content_rows_by_id.get(evidence_id)
                    evidence_link = note_link(
                        ctx.content_paths[evidence_id],
                        content_title(evidence_content),
                    )
                    meta.append(f"evidence {evidence_link}")
                suffix = f" ({'; '.join(meta)})" if meta else ""
                body.append(f"- `{item['label']}` {target}{suffix}")

        if claim_ids:
            body.extend(["", "## Claims", ""])
            for claim_id in claim_ids[:30]:
                claim = ctx.claim_rows_by_id.get(claim_id)
                claim_path = ctx.claim_paths.get(claim_id)
                if claim and claim_path:
                    body.append(
                        f"- {note_link(claim_path, claim_title(claim))} `{claim.get('status') or ''}`"
                    )

        if case_ids:
            body.extend(["", "## Cases", ""])
            for case_id in case_ids[:20]:
                case = ctx.case_rows_by_id.get(case_id)
                case_path = ctx.case_paths.get(case_id)
                if case and case_path:
                    body.append(f"- {note_link(case_path, case_title(case))}")

        if risk_ids:
            body.extend(["", "## Risks", ""])
            for risk_id in risk_ids[:20]:
                risk = ctx.risk_rows_by_id.get(risk_id)
                risk_path = ctx.risk_paths.get(risk_id)
                if risk and risk_path:
                    body.append(
                        f"- {note_link(risk_path, risk_title(risk))} `{risk.get('risk_level') or ''}`"
                    )

        if related_content_ids:
            body.extend(["", "## Related content", ""])
            ordered_content = [
                ctx.content_rows_by_id[cid]
                for cid in related_content_ids
                if cid in ctx.content_rows_by_id
            ]
            ordered_content.sort(
                key=lambda item: (item.get("published_at") or "", item["id"]),
                reverse=True,
            )
            for content in ordered_content[:30]:
                body.append(
                    f"- `{(content.get('published_at') or '')[:10]}` {note_link(ctx.content_paths[content['id']], content_title(content))}"
                )

        if bill_ids:
            body.extend(["", "## Bills", ""])
            for bill_id in bill_ids[:20]:
                bill = ctx.bill_rows_by_id.get(bill_id)
                bill_path = ctx.bill_paths.get(bill_id)
                if bill and bill_path:
                    body.append(f"- {note_link(bill_path, bill_title(bill))}")

        if vote_ids:
            body.extend(["", "## Vote sessions", ""])
            for vote_id in vote_ids[:20]:
                vote = ctx.vote_rows_by_id.get(vote_id)
                vote_path = ctx.vote_paths.get(vote_id)
                if vote and vote_path:
                    body.append(f"- {note_link(vote_path, vote_link_title(vote))}")

        if contract_ids:
            body.extend(["", "## Contracts", ""])
            for contract_id in contract_ids[:20]:
                contract = ctx.contract_rows_by_id.get(contract_id)
                contract_path = ctx.contract_paths.get(contract_id)
                if contract and contract_path:
                    body.append(f"- {note_link(contract_path, contract_title(contract))}")

        weak_links = ctx.weak_relations_by_entity.get(ent["id"], [])
        if weak_links and ent["id"] in ctx.weak_paths:
            body.extend(
                [
                    "",
                    "## Weak similarity",
                    "",
                    f"- {note_link(ctx.weak_paths[ent['id']], 'Weak similarity layer')} ({len(weak_links)} links)",
                ]
            )

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "Entities/index.md", "\n".join(index_lines))


def export_graph_bills(vault: Path, ctx: GraphContext):
    index_lines = ["# Bills", "", "| ID | Number | Status | Title |", "|---:|---|---|---|"]
    vote_rows_by_bill: dict[int, list[dict]] = defaultdict(list)
    for vote in ctx.vote_rows:
        if vote.get("bill_id") is not None:
            vote_rows_by_bill[vote["bill_id"]].append(vote)

    for bill in ctx.bill_rows:
        rel = ctx.bill_paths[bill["id"]]
        title = row_title(bill, "title", "Bill")
        index_lines.append(
            f"| {bill['id']} | {md_escape(bill.get('number'))} | {md_escape(bill.get('status'))} | {note_link(rel, md_escape(title))} |"
        )

        body = [
            frontmatter(
                {
                    "type": "bill",
                    "bill_id": bill["id"],
                    "number": bill.get("number"),
                    "status": bill.get("status"),
                }
            ),
            f"# {bill_title(bill)}",
            "",
            title,
            "",
            f"- Status: `{bill.get('status') or ''}`",
            f"- Registration: `{bill.get('registration_date') or ''}`",
            f"- URL: {bill.get('duma_url') or ''}",
        ]

        sponsors = ctx.sponsors_by_bill.get(bill["id"], [])
        if sponsors:
            body.extend(["", "## Sponsors", ""])
            for sponsor in sponsors[:20]:
                target = sponsor.get("sponsor_name") or ""
                entity_id = sponsor.get("entity_id")
                if entity_id in ctx.entity_paths:
                    target = note_link(ctx.entity_paths[entity_id], sponsor.get("sponsor_name") or f"Entity {entity_id}")
                role = f" `{sponsor.get('sponsor_role') or ''}`" if sponsor.get("sponsor_role") else ""
                body.append(f"- {target}{role}")

        votes = vote_rows_by_bill.get(bill["id"], [])
        if votes:
            body.extend(["", "## Vote sessions", ""])
            for vote in votes[:20]:
                vote_path = ctx.vote_paths.get(vote["id"])
                if vote_path:
                    label = vote_link_title(vote)
                    body.append(f"- {note_link(vote_path, label)}")

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "Bills/index.md", "\n".join(index_lines))


def export_graph_vote_sessions(vault: Path, ctx: GraphContext):
    index_lines = ["# Vote Sessions", "", "| ID | Date | Bill | Stage |", "|---:|---|---|---|"]

    for vote in ctx.vote_rows:
        rel = ctx.vote_paths[vote["id"]]
        index_lines.append(
            f"| {vote['id']} | {md_escape(vote.get('vote_date'))} | {md_escape(vote.get('bill_number'))} | {md_escape(vote.get('vote_stage'))} |"
        )

        body = [
            frontmatter(
                {
                    "type": "vote_session",
                    "vote_session_id": vote["id"],
                    "vote_date": vote.get("vote_date"),
                    "vote_stage": vote.get("vote_stage"),
                }
            ),
            f"# {vote_title(vote)}",
            "",
            f"- Date: `{vote.get('vote_date') or ''}`",
            f"- Stage: `{vote.get('vote_stage') or ''}`",
            f"- Result: `{vote.get('result') or ''}`",
        ]

        bill_id = vote.get("bill_id")
        if bill_id in ctx.bill_paths:
            bill = ctx.bill_rows_by_id.get(bill_id)
            if bill:
                body.append(f"- Bill: {note_link(ctx.bill_paths[bill_id], bill_title(bill))}")

        session_votes = ctx.votes_by_session.get(vote["id"], [])
        if session_votes:
            body.extend(["", "## Votes", ""])
            for item in session_votes[:50]:
                target = item.get("deputy_name") or ""
                entity_id = item.get("entity_id")
                if entity_id in ctx.entity_paths:
                    target = note_link(ctx.entity_paths[entity_id], item.get("deputy_name") or f"Entity {entity_id}")
                body.append(f"- {target} `{item.get('vote_result') or ''}`")

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "VoteSessions/index.md", "\n".join(index_lines))


def export_graph_contracts(vault: Path, ctx: GraphContext):
    index_lines = ["# Contracts", "", "| ID | Number | Date | Title |", "|---:|---|---|---|"]

    for contract in ctx.contract_rows:
        rel = ctx.contract_paths[contract["id"]]
        title = contract_title(contract)
        index_lines.append(
            f"| {contract['id']} | {md_escape(contract.get('contract_number'))} | {md_escape(contract.get('publication_date'))} | {note_link(rel, md_escape(title))} |"
        )

        body = [
            frontmatter(
                {
                    "type": "contract",
                    "contract_id": contract["id"],
                    "contract_number": contract.get("contract_number"),
                }
            ),
            f"# {title}",
            "",
            contract.get("summary") or "",
            "",
            f"- Number: `{contract.get('contract_number') or ''}`",
            f"- Date: `{contract.get('publication_date') or ''}`",
            f"- Source org: `{contract.get('source_org') or ''}`",
        ]

        content_id = contract.get("content_item_id")
        if content_id in ctx.content_paths:
            content = ctx.content_rows_by_id.get(content_id)
            if content:
                body.append(f"- Source content: {note_link(ctx.content_paths[content_id], content_title(content))}")

        raw_data = contract.get("raw_data_dict") or {}
        if raw_data:
            body.extend(["", "## Raw context", ""])
            for key in ("customer_inn", "supplier_inn", "contract_number"):
                if raw_data.get(key):
                    body.append(f"- {key}: `{raw_data[key]}`")

        if contract.get("entity_ids"):
            body.extend(["", "## Related entities", ""])
            for entity_id in contract["entity_ids"]:
                entity = ctx.entity_rows_by_id.get(entity_id)
                entity_path = ctx.entity_paths.get(entity_id)
                if entity and entity_path:
                    body.append(f"- {note_link(entity_path, entity_title(entity))}")

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "Contracts/index.md", "\n".join(index_lines))


def export_graph_risks(vault: Path, ctx: GraphContext):
    index_lines = ["# Risks", "", "| ID | Level | Type | Description |", "|---:|---|---|---|"]

    for risk in ctx.risk_rows:
        rel = ctx.risk_paths[risk["id"]]
        desc = md_escape((risk.get("description") or "")[:80])
        index_lines.append(
            f"| {risk['id']} | {md_escape(risk.get('risk_level'))} | {md_escape(risk.get('pattern_type'))} | {note_link(rel, desc or risk_title(risk))} |"
        )

        body = [
            frontmatter(
                {
                    "type": "risk",
                    "risk_id": risk["id"],
                    "pattern_type": risk.get("pattern_type"),
                    "risk_level": risk.get("risk_level"),
                }
            ),
            f"# {risk_title(risk)}",
            "",
            risk.get("description") or "",
            "",
            f"- Level: `{risk.get('risk_level') or ''}`",
            f"- Detected at: `{risk.get('detected_at') or ''}`",
        ]

        if risk["entity_ids_list"]:
            body.extend(["", "## Entities", ""])
            for entity_id in risk["entity_ids_list"]:
                entity = ctx.entity_rows_by_id.get(entity_id)
                entity_path = ctx.entity_paths.get(entity_id)
                if entity and entity_path:
                    body.append(f"- {note_link(entity_path, entity_title(entity))}")

        if risk["evidence_ids_list"]:
            body.extend(["", "## Evidence items", ""])
            for content_id in risk["evidence_ids_list"][:50]:
                content = ctx.content_rows_by_id.get(content_id)
                if content and content_id in ctx.content_paths:
                    body.append(f"- {note_link(ctx.content_paths[content_id], content_title(content))}")

        case_id = risk.get("case_id")
        if case_id in ctx.case_paths:
            case = ctx.case_rows_by_id.get(case_id)
            if case:
                body.extend(["", "## Case", "", f"- {note_link(ctx.case_paths[case_id], case_title(case))}"])

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "Risks/index.md", "\n".join(index_lines))


def export_graph_weak_links(vault: Path, ctx: GraphContext):
    index_lines = ["# Weak Similarity", "", "| Entity | Weak links |", "|---|---:|"]

    for ent in ctx.entity_rows:
        weak_links = ctx.weak_relations_by_entity.get(ent["id"], [])
        if not weak_links:
            continue
        rel = ctx.weak_paths[ent["id"]]
        title = entity_title(ent)
        index_lines.append(f"| {note_link(rel, md_escape(title))} | {len(weak_links)} |")

        body = [
            frontmatter(
                {
                    "type": "weak_similarity",
                    "entity_id": ent["id"],
                    "entity_type": ent.get("entity_type"),
                }
            ),
            f"# Weak links for {title}",
            "",
            "Secondary layer for co-occurrence and weak similarity. These links are excluded from the main entity graph notes.",
            "",
            "## Links",
            "",
        ]

        sorted_links = sorted(
            weak_links,
            key=lambda item: (item.get("support_count") or 0, item.get("other_name") or ""),
            reverse=True,
        )
        for item in sorted_links[:100]:
            target_path = ctx.entity_paths.get(item["other_entity_id"])
            if not target_path:
                continue
            target = note_link(target_path, item["other_name"])
            body.append(
                f"- support_count=`{item.get('support_count') or 0}` score=`{item.get('score') or 0}` {target} `{item.get('label')}` `{item.get('strength') or ''}`"
            )

        write_note(vault, rel, "\n".join(body))

    write_note(vault, "WeakLinks/index.md", "\n".join(index_lines))


def export_graph_tags(vault: Path, ctx: GraphContext):
    namespace_index: dict[str, set[str]] = defaultdict(set)
    lines = ["# Tags", "", "| Namespace | Tags | Notes |", "|---|---:|---:|"]

    for tag, content_ids in sorted(ctx.tag_index.items()):
        namespace = tag.split("/")[0]
        namespace_index[namespace].add(tag)
        note_count = len(content_ids)
        lines.append(
            f"| {note_link(f'Tags/{slugify(namespace, 'tags')}.md', namespace)} | {len(namespace_index[namespace])} | {note_count} |"
        )

    written_namespaces: set[str] = set()
    for namespace, tags in sorted(namespace_index.items()):
        if namespace in written_namespaces:
            continue
        written_namespaces.add(namespace)
        note_path = f"Tags/{slugify(namespace, 'tags')}.md"
        body = [
            frontmatter({"type": "tag_namespace", "namespace": namespace}),
            f"# {namespace}",
            "",
            "| Tag | Notes |",
            "|---|---:|",
        ]
        for tag in sorted(tags):
            body.append(f"| `#{tag}` | {len(ctx.tag_index[tag])} |")
        write_note(vault, note_path, "\n".join(body))

    write_note(vault, "Tags/index.md", "\n".join(lines))


def export_graph_files(vault: Path, conn: sqlite3.Connection, copy_media: bool):
    if not table_exists(conn, "raw_blobs"):
        write_note(vault, "Files/index.md", "# Files\n")
        return

    blob_rows = rows(conn, "SELECT * FROM raw_blobs ORDER BY id")
    lines = ["# Files", "", "| ID | Type | File | Size | SHA-256 |", "|---:|---|---|---:|---|"]
    for blob in blob_rows:
        fake_att = {
            "id": blob["id"],
            "file_path": blob["file_path"],
            "hash_sha256": blob["hash_sha256"],
            "mime_type": blob["mime_type"],
        }
        link, status = copy_attachment(vault, fake_att, copy_media=copy_media)
        lines.append(
            f"| {blob['id']} | {md_escape(blob['blob_type'])} | {link or md_escape(status)} | {blob['file_size'] or 0} | `{blob['hash_sha256'] or ''}` |"
        )
    write_note(vault, "Files/index.md", "\n".join(lines))


def export_graph_obsidian(
    db_path: Path,
    vault: Path,
    limit: Optional[int] = None,
    copy_media: bool = True,
):
    if limit:
        raise ValueError("graph export does not support --limit; use archive mode for smoke subsets")
    ensure_dir(vault)
    conn = connect(db_path)
    try:
        ctx = build_graph_context(conn)
        generated_at = datetime.now().isoformat(timespec="seconds")
        export_graph_index(vault, ctx, generated_at)
        export_graph_sources(vault, ctx)
        export_graph_content(vault, ctx, copy_media=copy_media)
        export_graph_claims(vault, ctx)
        export_graph_cases(vault, ctx)
        export_graph_entities(vault, ctx)
        export_graph_bills(vault, ctx)
        export_graph_vote_sessions(vault, ctx)
        export_graph_contracts(vault, ctx)
        export_graph_risks(vault, ctx)
        export_graph_weak_links(vault, ctx)
        export_graph_tags(vault, ctx)
        export_graph_files(vault, conn, copy_media=copy_media)
        set_runtime_metadata(conn, "obsidian_built_from_pipeline_version", ctx.pipeline_version)
        set_runtime_metadata(conn, "obsidian_export_generated_at", generated_at)
    finally:
        conn.close()
