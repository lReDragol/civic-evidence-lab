"""Claim-centered investigations. Workflow completion never establishes truth."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone


def _now():
    return datetime.now(timezone.utc).isoformat()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def validate_locator(conn, revision_id, locator):
    row = conn.execute("SELECT payload_json FROM source_revisions WHERE id=?", (revision_id,)).fetchone()
    if not row:
        raise ValueError("Unknown source revision")
    if not isinstance(locator, dict):
        raise ValueError("A structured exact locator is required")
    kind = locator.get("type")
    if kind == "quote":
        quote = locator.get("quote")
        def strings(value):
            if isinstance(value, str):
                yield value
            elif isinstance(value, dict):
                for child in value.values():
                    yield from strings(child)
            elif isinstance(value, list):
                for child in value:
                    yield from strings(child)
        try:
            payload = json.loads(row[0])
        except (ValueError, TypeError):
            payload = row[0]
        if not isinstance(quote,str) or not quote.strip() or not any(quote in text for text in strings(payload)):
            raise ValueError("Quote must occur in the referenced immutable source")
    elif kind in {"page_region", "video_segment"}:
        from pathlib import Path
        blob = conn.execute("SELECT b.storage_path FROM blobs b JOIN revision_blobs r ON r.blob_id=b.id WHERE r.revision_id=? AND b.id=?", (revision_id,locator.get("blob_id"))).fetchone()
        if not blob or not Path(blob[0]).is_file():
            raise ValueError("Locator requires an archived original file")
        if kind == "video_segment":
            start,end=locator.get("start_ms"),locator.get("end_ms")
            if not isinstance(start,int) or not isinstance(end,int) or start<0 or end<=start:
                raise ValueError("Invalid video time range")
        else:
            bbox=locator.get("bbox")
            if not isinstance(locator.get("page"),int) or locator["page"]<1 or not isinstance(bbox,list) or len(bbox)!=4 or not all(isinstance(n,(int,float)) and 0<=n<=1 for n in bbox) or bbox[2]<=bbox[0] or bbox[3]<=bbox[1]:
                raise ValueError("Page locator needs a normalized bounding box")
    else:
        raise ValueError("Unsupported evidence locator")
    return _json(locator)


def add_claim(conn, *, source_revision_id, claim_text, polarity, modality, locator, attribution=None):
    location=validate_locator(conn,source_revision_id,locator)
    if not claim_text.strip():
        raise ValueError("Claim text is required")
    key=hashlib.sha256(_json([source_revision_id,claim_text,polarity,modality,attribution,locator]).encode()).hexdigest()
    conn.execute("""INSERT OR IGNORE INTO civic_claims(source_revision_id,claim_key,claim_text,polarity,modality,attribution,locator_json,created_at)
        VALUES(?,?,?,?,?,?,?,?)""",(source_revision_id,key,claim_text,polarity,modality,attribution,location,_now()))
    return conn.execute("SELECT id FROM civic_claims WHERE claim_key=?",(key,)).fetchone()[0]


def link_evidence(conn, *, claim_id, source_revision_id, stance, locator, origin_key=None):
    location=validate_locator(conn,source_revision_id,locator)
    conn.execute("""INSERT OR IGNORE INTO civic_evidence_links(claim_id,source_revision_id,stance,locator_json,origin_key,created_at)
        VALUES(?,?,?,?,?,?)""",(claim_id,source_revision_id,stance,location,origin_key,_now()))
    return conn.execute("SELECT id FROM civic_evidence_links WHERE claim_id=? AND source_revision_id=? AND stance=? AND locator_json=?",(claim_id,source_revision_id,stance,location)).fetchone()[0]


def claim_evidence_summary(conn, claim_id):
    rows=conn.execute("SELECT stance,origin_key,verification_state,authenticity_state,reviewed_by FROM civic_evidence_links WHERE claim_id=?",(claim_id,)).fetchall()
    origins={"supports":set(),"refutes":set()}
    for stance,origin,verified,authenticity,reviewer in rows:
        if stance in origins and origin and reviewer and verified=="verified" and authenticity=="authentic":
            origins[stance].add(origin)
    return {"links":len(rows),"reviewed_support_origins":len(origins["supports"]),
            "reviewed_refutation_origins":len(origins["refutes"]),
            "contradiction":bool(origins["supports"] and origins["refutes"]),
            "publication_allowed":False}


def revise_thread(conn, *, thread_key, question, members, reason, actor, expected_revision_id=None, operation="edit"):
    if not reason.strip() or not actor.strip() or not question.strip():
        raise ValueError("Question, reason and actor are required")
    allowed={"claim":"civic_claims","event":"events","evidence":"civic_evidence_links"}
    canonical=[]
    for member in members:
        table=allowed.get(member.get("type"))
        if not table or not conn.execute(f"SELECT id FROM {table} WHERE id=?",(member.get("id"),)).fetchone():
            raise ValueError("Unresolved thread member")
        canonical.append({"type":member["type"],"id":int(member["id"])})
    canonical=sorted({(m["type"],m["id"]) for m in canonical})
    conn.execute("SAVEPOINT thread_revision")
    try:
        conn.execute("INSERT OR IGNORE INTO investigation_threads(thread_key,question,created_at) VALUES(?,?,?)",(thread_key,question,_now()))
        thread_id,current=conn.execute("SELECT id,current_revision_id FROM investigation_threads WHERE thread_key=?",(thread_key,)).fetchone()
        if current!=expected_revision_id:
            raise ValueError("Thread revision conflict")
        number=conn.execute("SELECT COALESCE(MAX(revision_no),0)+1 FROM thread_revisions WHERE thread_id=?",(thread_id,)).fetchone()[0]
        revision=conn.execute("""INSERT INTO thread_revisions(thread_id,revision_no,previous_revision_id,operation,reason,actor,membership_json,created_at)
            VALUES(?,?,?,?,?,?,?,?)""",(thread_id,number,current,operation,reason,actor,_json([{"type":t,"id":i} for t,i in canonical]),_now())).lastrowid
        conn.execute("UPDATE investigation_threads SET current_revision_id=?,question=? WHERE id=?",(revision,question,thread_id))
        conn.execute("RELEASE thread_revision")
        return {"thread_id":thread_id,"revision_id":revision,"revision_no":number}
    except Exception:
        conn.execute("ROLLBACK TO thread_revision")
        conn.execute("RELEASE thread_revision")
        raise
