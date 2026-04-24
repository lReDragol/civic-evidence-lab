import json
import logging
import sqlite3
import sys
from pathlib import Path

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def backfill_vote_entities(conn):
    unmatched = conn.execute(
        "SELECT DISTINCT bv.deputy_name, bv.faction "
        "FROM bill_votes bv "
        "WHERE bv.entity_id IS NULL AND bv.deputy_name NOT LIKE 'Фракция:%'"
    ).fetchall()
    log.info("Unmatched deputy names in bill_votes: %d", len(unmatched))

    matched = 0
    created = 0

    for name, faction in unmatched:
        faction_norm = {
            "ЕР": "Единая Россия", "КПРФ": "КПРФ", "ЛДПР": "ЛДПР",
            "СР": "Справедливая Россия", "НЛ": "Новые люди",
            "Родина": "Родина", "Вне фракций": "независимый",
        }.get(faction, faction)

        parts = name.split()
        last_name = parts[0] if parts else ""

        entity_id = None

        # Try 1: match by full_name LIKE 'Lastname%' AND faction
        row = conn.execute(
            "SELECT dp.entity_id FROM deputy_profiles dp "
            "WHERE dp.full_name LIKE ? AND dp.faction = ? LIMIT 1",
            (f"{last_name}%", faction_norm),
        ).fetchone()
        if row:
            entity_id = row[0]
            matched += 1

        # Try 2: match by entity_aliases
        if not entity_id:
            row = conn.execute(
                "SELECT ea.entity_id FROM entity_aliases ea "
                "JOIN deputy_profiles dp ON dp.entity_id = ea.entity_id "
                "WHERE ea.alias LIKE ? AND dp.faction = ? LIMIT 1",
                (f"{last_name}%", faction_norm),
            ).fetchone()
            if row:
                entity_id = row[0]
                matched += 1

        # Try 3: match by entities canonical_name + person type
        if not entity_id:
            row = conn.execute(
                "SELECT id FROM entities WHERE entity_type='person' AND canonical_name LIKE ? LIMIT 1",
                (f"{last_name}%",),
            ).fetchone()
            if row:
                # Verify it's a deputy via official_positions
                pos = conn.execute(
                    "SELECT id FROM official_positions WHERE entity_id=? AND organization LIKE '%Госдума%' AND is_active=1",
                    (row[0],),
                ).fetchone()
                if pos:
                    entity_id = row[0]
                    matched += 1

        # Try 4: create new entity + deputy_profile
        if not entity_id:
            cur = conn.execute(
                "INSERT INTO entities(entity_type, canonical_name, description) VALUES('person', ?, ?)",
                (name, f"Депутат Госдумы РФ, фракция {faction_norm}"),
            )
            entity_id = cur.lastrowid

            conn.execute(
                "INSERT INTO deputy_profiles(entity_id, full_name, faction, is_active) VALUES(?, ?, ?, 1)",
                (entity_id, name, faction_norm),
            )
            conn.execute(
                "INSERT INTO entity_aliases(entity_id, alias, alias_type) VALUES(?, ?, 'spelling')",
                (entity_id, last_name),
            )
            conn.execute(
                "INSERT INTO official_positions(entity_id, position_title, organization, faction, is_active, source_type) "
                "VALUES(?, 'Депутат ГД', 'Госдума РФ', ?, 1, 'vote_record')",
                (entity_id, faction_norm),
            )
            created += 1

        if entity_id:
            rows_to_update = conn.execute(
                "SELECT id FROM bill_votes WHERE deputy_name=? AND entity_id IS NULL",
                (name,),
            ).fetchall()
            for (row_id,) in rows_to_update:
                try:
                    conn.execute("UPDATE bill_votes SET entity_id=? WHERE id=?", (entity_id, row_id))
                except Exception:
                    pass

    conn.commit()
    log.info("Matched: %d, Created new: %d", matched, created)
    return matched, created


if __name__ == "__main__":
    settings = load_settings()
    conn = get_db(settings)
    matched, created = backfill_vote_entities(conn)
    total = conn.execute("SELECT COUNT(*) FROM bill_votes WHERE entity_id IS NOT NULL AND deputy_name NOT LIKE 'Фракция:%'").fetchone()[0]
    log.info("Total individual votes with entity_id: %d", total)
    conn.close()
