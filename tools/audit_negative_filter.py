import argparse
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from classifier.negative_filter import classify_negative_profile
from classifier.tagger_v2 import infer_tags_v2

DEFAULT_DB = PROJECT_ROOT / "db" / "news_telegram_test.db"
DEFAULT_REPORT = PROJECT_ROOT / "reports" / "negative_filter_audit_latest.json"


def open_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def flatten_tags(tag_map: Dict[int, List]) -> List[str]:
    return [tag for tags in tag_map.values() for tag, _score in tags]


def audit(db_path: Path, limit: int, examples: int) -> Dict[str, object]:
    conn = open_db(db_path)
    try:
        rows = conn.execute(
            """
            SELECT c.id, c.title, c.body_text, c.url, c.published_at,
                   s.name AS source_name, s.url AS source_url, s.subcategory,
                   s.political_alignment, s.owner, s.bias_notes, s.notes
            FROM content_items c
            JOIN sources s ON s.id = c.source_id
            WHERE c.content_type='post'
            ORDER BY c.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    kept = []
    rejected = []
    categories = Counter()
    risk_tags = Counter()
    strict_context = Counter()
    thresholds = Counter()

    for row in rows:
        text = f"{row['title'] or ''}\n{row['body_text'] or ''}"
        tag_map = infer_tags_v2(text)
        source = {
            "name": row["source_name"],
            "url": row["source_url"],
            "subcategory": row["subcategory"],
            "political_alignment": row["political_alignment"],
            "owner": row["owner"],
            "bias_notes": row["bias_notes"],
            "notes": row["notes"],
        }
        profile = classify_negative_profile(text, source=source, tag_names=flatten_tags(tag_map))
        item = {
            "content_id": row["id"],
            "source": row["source_name"],
            "url": row["url"],
            "title": row["title"],
            "negative_score": profile["negative_score"],
            "threshold": profile["threshold"],
            "categories": profile["negative_categories"],
            "risk_tags": profile["risk_tags"],
            "source_context": profile["source_context"],
            "reasons": profile["negative_reasons"][:10],
        }
        if profile["is_negative_public_interest"]:
            kept.append(item)
            categories.update(profile["negative_categories"])
        else:
            rejected.append(item)
        risk_tags.update(profile["risk_tags"])
        thresholds[str(profile["threshold"])] += 1
        for key, value in profile["source_context"].items():
            if key.startswith("is_") and value:
                strict_context[key] += 1

    return {
        "db": str(db_path),
        "checked": len(rows),
        "would_keep_negative_only": len(kept),
        "would_reject_negative_only": len(rejected),
        "negative_categories": dict(categories),
        "risk_tags": dict(risk_tags),
        "source_context_counts": dict(strict_context),
        "threshold_distribution": dict(thresholds),
        "kept_examples": kept[:examples],
        "rejected_examples": rejected[:examples],
    }


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    parser = argparse.ArgumentParser(description="Audit strict negative/public-interest filtering on an existing DB")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--examples", type=int, default=20)
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    result = audit(Path(args.db), limit=args.limit, examples=args.examples)
    output = json.dumps(result, ensure_ascii=False, indent=2)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
