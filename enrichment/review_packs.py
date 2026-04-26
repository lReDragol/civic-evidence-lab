from __future__ import annotations

import csv
from pathlib import Path
from typing import Any
from uuid import uuid4

from enrichment.common import now_iso, open_db

CSV_FIELDS = (
    "task_key",
    "queue_key",
    "subject_type",
    "subject_id",
    "related_id",
    "suggested_action",
    "confidence",
    "status",
    "reviewer",
    "review_pack_id",
    "machine_reason",
    "candidate_payload",
    "source_links_json",
    "resolution_notes",
)


def export_review_pack(settings: dict[str, Any] | None = None, *, queue_key: str, csv_path: Path | str) -> dict[str, Any]:
    settings = settings or {}
    conn = open_db(settings)
    csv_target = Path(csv_path)
    csv_target.parent.mkdir(parents=True, exist_ok=True)
    pack_id = f"{queue_key}-{uuid4().hex[:12]}"
    try:
        rows = conn.execute(
            """
            SELECT task_key, queue_key, subject_type, subject_id, related_id, suggested_action, confidence,
                   status, reviewer, machine_reason, candidate_payload, source_links_json, resolution_notes
            FROM review_tasks
            WHERE queue_key=?
            ORDER BY id
            """,
            (queue_key,),
        ).fetchall()
        with csv_target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for row in rows:
                payload = dict(row)
                payload["review_pack_id"] = pack_id
                writer.writerow(payload)
                conn.execute(
                    "UPDATE review_tasks SET review_pack_id=?, updated_at=? WHERE task_key=?",
                    (pack_id, now_iso(), row["task_key"]),
                )
        conn.commit()
        return {"ok": True, "items_seen": len(rows), "items_new": len(rows), "review_pack_id": pack_id, "csv_path": str(csv_target)}
    finally:
        conn.close()


def import_review_pack(settings: dict[str, Any] | None = None, *, csv_path: Path | str) -> dict[str, Any]:
    settings = settings or {}
    conn = open_db(settings)
    csv_target = Path(csv_path)
    updated = 0
    try:
        with csv_target.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                conn.execute(
                    """
                    UPDATE review_tasks
                    SET status=?, reviewer=?, resolution_notes=?, review_pack_id=COALESCE(?, review_pack_id), updated_at=?
                    WHERE task_key=?
                    """,
                    (
                        row.get("status") or "open",
                        row.get("reviewer") or None,
                        row.get("resolution_notes") or None,
                        row.get("review_pack_id") or None,
                        now_iso(),
                        row.get("task_key"),
                    ),
                )
                updated += 1
        conn.commit()
        return {"ok": True, "items_seen": updated, "items_updated": updated}
    finally:
        conn.close()
