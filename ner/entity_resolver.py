import json
import logging
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

sys_path = str(Path(__file__).resolve().parent.parent)
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from config.db_utils import get_db, load_settings

log = logging.getLogger(__name__)

_YO_MAP = str.maketrans("ёЁ", "еЕ")


def _normalize_yo(text: str) -> str:
    return text.translate(_YO_MAP)

MASC_SURNAMES_ENDINGS = {
    "ов": ["ова", "ову", "овым", "ове"],
    "ев": ["ева", "еву", "евым", "еве"],
    "ёв": ["ёва", "ёву", "ёвым", "ёве"],
    "ин": ["ина", "ину", "иным", "ине"],
    "ын": ["ына", "ыну", "ыным", "ыне"],
    "ский": ["ского", "скому", "ским", "ском"],
    "цкий": ["цкого", "цкому", "цким", "цком"],
    "ий": ["его", "ему", "им", "ем"],
    "ой": ["ого", "ому", "ым", "ом"],
}

FEM_SURNAMES_ENDINGS = {
    "ова": ["овой", "ову"],
    "ева": ["евой", "еву"],
    "ёва": ["ёвой", "ёву"],
    "ина": ["иной", "ину"],
    "ына": ["ыной", "ыну"],
    "ская": ["ской"],
    "цкая": ["цкой"],
    "ая": ["ой", "ую"],
}

FIRST_NAME_MASC_DECL = {
    "ий": ["ия", "ию", "ием", "ие"],
    "ей": ["ея", "ею", "еем", "ее"],
    "ай": ["ая", "аю", "аем", "ае"],
    "ей": ["ея", "ею", "еем"],
}

FIRST_NAME_FEM_DECL = {
    "а": ["ы", "е", "у", "ой", "ою"],
    "я": ["и", "е", "ю", "ей", "её"],
    "ия": ["ии", "ию", "ией"],
    "ья": ["ьи", "ье", "ью", "ьей"],
}


def _generate_surname_forms(surname: str) -> List[str]:
    forms = [surname]
    for ending, declined in MASC_SURNAMES_ENDINGS.items():
        if surname.endswith(ending):
            stem = surname[:-len(ending)]
            for d in declined:
                forms.append(stem + d)
            return forms
    for ending, declined in FEM_SURNAMES_ENDINGS.items():
        if surname.endswith(ending):
            stem = surname[:-len(ending)]
            for d in declined:
                forms.append(stem + d)
            return forms
    consonant_endings = ["н", "р", "к", "л", "м", "п", "т", "с", "д", "з", "б", "в", "г", "ж", "ш", "ч", "х"]
    if surname[-1].lower() in consonant_endings or surname[-1] in consonant_endings:
        for suffix in ["а", "у", "ом", "е", "ым"]:
            forms.append(surname + suffix)
        return forms
    if surname.endswith("ь"):
        stem = surname[:-1]
        for suffix in ["я", "ю", "ём", "е", "ём"]:
            forms.append(stem + suffix)
        return forms
    return forms


def _generate_firstname_forms(firstname: str) -> List[str]:
    forms = [firstname]
    for ending, declined in FIRST_NAME_MASC_DECL.items():
        if firstname.endswith(ending):
            stem = firstname[:-len(ending)]
            for d in declined:
                forms.append(stem + d)
            return forms
    for ending, declined in FIRST_NAME_FEM_DECL.items():
        if firstname.endswith(ending):
            stem = firstname[:-len(ending)]
            for d in declined:
                forms.append(stem + d)
            return forms
    return forms


def _generate_all_name_forms(full_name: str) -> List[str]:
    parts = full_name.split()
    forms = set()
    if len(parts) == 1:
        forms.update(_generate_surname_forms(parts[0]))
        return list(forms)
    surname = parts[-1]
    first_name = parts[0]
    patronymic = parts[1] if len(parts) > 2 else None
    surname_forms = _generate_surname_forms(surname)
    first_forms = _generate_firstname_forms(first_name)
    forms.update(surname_forms)
    for sf in surname_forms:
        for ff in first_forms:
            forms.add(f"{ff} {sf}")
            if patronymic:
                forms.add(f"{ff} {patronymic} {sf}")
                if patronymic.endswith("ич"):
                    pat_forms = [patronymic, patronymic[:-2] + "ича", patronymic[:-2] + "ичу",
                                 patronymic[:-2] + "ичем", patronymic[:-2] + "иче"]
                    for pf in pat_forms:
                        forms.add(f"{ff} {pf} {sf}")
                elif patronymic.endswith("на"):
                    pat_forms = [patronymic, patronymic[:-1] + "ы", patronymic[:-1] + "не",
                                 patronymic[:-1] + "ну", patronymic[:-1] + "ной"]
                    for pf in pat_forms:
                        forms.add(f"{ff} {pf} {sf}")
    return list(forms)


def resolve_deputies(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    deputies = conn.execute(
        """
        SELECT dp.id, dp.entity_id, dp.full_name
        FROM deputy_profiles dp
        WHERE dp.is_active = 1
        """
    ).fetchall()

    if not deputies:
        log.info("No active deputy profiles found")
        conn.close()
        return {"merged": 0, "mentions_redirected": 0}

    total_merged = 0
    total_mentions_redirected = 0
    total_aliases_added = 0

    for dep in deputies:
        deputy_id = dep["id"]
        target_entity_id = dep["entity_id"]
        full_name = dep["full_name"]

        name_forms = _generate_all_name_forms(full_name)
        surname = full_name.split()[-1]
        surname_forms = _generate_surname_forms(surname)

        matched_entity_ids = set()

        all_forms = list(set(name_forms + surname_forms))
        all_forms_normalized = list(set(_normalize_yo(f) for f in all_forms))
        extra_forms = [f for f in all_forms_normalized if f not in all_forms]
        all_search_forms = all_forms + extra_forms

        for form in all_search_forms:
            form_n = _normalize_yo(form)
            rows = conn.execute(
                "SELECT id FROM entities WHERE entity_type = 'person' AND (canonical_name = ? OR REPLACE(REPLACE(canonical_name,'ё','е'),'Ё','Е') = ?) AND id != ?",
                (form, form_n, target_entity_id),
            ).fetchall()
            for r in rows:
                matched_entity_ids.add(r[0])

        for sf in surname_forms + [_normalize_yo(sf) for sf in surname_forms]:
            sf_n = _normalize_yo(sf)
            rows2 = conn.execute(
                "SELECT id, canonical_name FROM entities WHERE entity_type = 'person' AND (canonical_name LIKE ? OR REPLACE(REPLACE(canonical_name,'ё','е'),'Ё','Е') LIKE ?) AND id != ?",
                (f"% {sf}", f"% {sf_n}", target_entity_id),
            ).fetchall()
            for r in rows2:
                name = r[1]
                name_parts = name.split()
                if len(name_parts) >= 2:
                    first_part = name_parts[0]
                    dep_first = full_name.split()[0]
                    first_forms = _generate_firstname_forms(dep_first)
                    first_forms_n = [_normalize_yo(f) for f in first_forms]
                    if first_part in first_forms or _normalize_yo(first_part) in first_forms_n or dep_first[:3].lower() == first_part[:3].lower():
                        matched_entity_ids.add(r[0])

        alias_forms = list(set(name_forms + [_normalize_yo(f) for f in name_forms]))
        alias_rows = conn.execute(
            "SELECT entity_id, alias FROM entity_aliases WHERE alias IN ({}) AND entity_id != ?".format(
                ",".join("?" * len(alias_forms))
            ),
            alias_forms + [target_entity_id],
        ).fetchall()
        for ar in alias_rows:
            if ar[0] != target_entity_id:
                matched_entity_ids.add(ar[0])

        if not matched_entity_ids:
            continue

        log.info("Deputy '%s' (entity=%d): found %d matching NER entities: %s",
                 full_name, target_entity_id, len(matched_entity_ids), matched_entity_ids)

        for src_entity_id in matched_entity_ids:
            src_name = conn.execute(
                "SELECT canonical_name FROM entities WHERE id = ?", (src_entity_id,)
            ).fetchone()

            mentions_moved = conn.execute(
                """
                UPDATE entity_mentions
                SET entity_id = ?, confidence = MIN(confidence, 0.9)
                WHERE entity_id = ? AND content_item_id NOT IN (
                    SELECT content_item_id FROM entity_mentions WHERE entity_id = ?
                )
                """,
                (target_entity_id, src_entity_id, target_entity_id),
            ).rowcount

            duplicate_mentions = conn.execute(
                """
                SELECT COUNT(*) FROM entity_mentions
                WHERE entity_id = ? AND content_item_id IN (
                    SELECT content_item_id FROM entity_mentions WHERE entity_id = ?
                )
                """,
                (src_entity_id, target_entity_id),
            ).fetchone()[0]

            if duplicate_mentions > 0:
                conn.execute(
                    "DELETE FROM entity_mentions WHERE entity_id = ? AND content_item_id IN (SELECT content_item_id FROM entity_mentions WHERE entity_id = ?)",
                    (src_entity_id, target_entity_id),
                )

            if src_name:
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO entity_aliases(entity_id, alias, alias_type) VALUES(?,?,?)",
                        (target_entity_id, src_name[0], "ner_variant"),
                    )
                    total_aliases_added += 1
                except Exception:
                    pass

            total_mentions_redirected += mentions_moved
            total_merged += 1

    conn.commit()

    log.info(
        "Entity resolution done: %d entities merged, %d mentions redirected, %d aliases added",
        total_merged, total_mentions_redirected, total_aliases_added,
    )

    stats = {
        "merged": total_merged,
        "mentions_redirected": total_mentions_redirected,
        "aliases_added": total_aliases_added,
    }

    conn.close()
    return stats


def _can_merge_names(name1: str, name2: str) -> bool:
    if _normalize_yo(name1) == _normalize_yo(name2):
        return True

    parts1 = name1.split()
    parts2 = name2.split()

    surnames1 = [parts1[0]] if len(parts1) >= 1 else []
    surnames2 = [parts2[0]] if len(parts2) >= 1 else []

    if not surnames1 or not surnames2:
        return False

    s1 = surnames1[0]
    s2 = surnames2[0]

    s1_forms = _generate_surname_forms(s1) + [_normalize_yo(f) for f in _generate_surname_forms(_normalize_yo(s1))]
    s2_forms = _generate_surname_forms(s2) + [_normalize_yo(f) for f in _generate_surname_forms(_normalize_yo(s2))]

    surname_match = s2 in s1_forms or s1 in s2_forms or _normalize_yo(s2) in s1_forms or _normalize_yo(s1) in s2_forms

    if not surname_match:
        if s1[:4].lower() == s2[:4].lower() and len(s1) >= 4 and len(s2) >= 4:
            surname_match = True
        else:
            return False

    if len(parts1) >= 2 and len(parts2) >= 2:
        first1 = parts1[0] if len(parts1) > 2 else parts1[0]
        first2 = parts2[0] if len(parts2) > 2 else parts2[0]

        if len(parts1) >= 3 and len(parts2) >= 3:
            fn1 = parts1[1] if parts1[0] == surnames1[0] else parts1[0]
            fn2 = parts2[1] if parts2[0] == surnames2[0] else parts2[0]
        elif len(parts1) >= 2 and len(parts2) >= 2:
            fn1 = parts1[0]
            fn2 = parts2[0]
        else:
            fn1 = parts1[0]
            fn2 = parts2[0]

        if fn1[:3].lower() != fn2[:3].lower():
            return False

    return True


def _merge_entity(conn: sqlite3.Connection, target_id: int, source_id: int) -> int:
    mentions_moved = conn.execute(
        """
        UPDATE entity_mentions
        SET entity_id = ?, confidence = MIN(confidence, 0.85)
        WHERE entity_id = ? AND content_item_id NOT IN (
            SELECT content_item_id FROM entity_mentions WHERE entity_id = ?
        )
        """,
        (target_id, source_id, target_id),
    ).rowcount

    dup = conn.execute(
        """
        SELECT COUNT(*) FROM entity_mentions
        WHERE entity_id = ? AND content_item_id IN (
            SELECT content_item_id FROM entity_mentions WHERE entity_id = ?
        )
        """,
        (source_id, target_id),
    ).fetchone()[0]

    if dup > 0:
        conn.execute(
            "DELETE FROM entity_mentions WHERE entity_id = ? AND content_item_id IN (SELECT content_item_id FROM entity_mentions WHERE entity_id = ?)",
            (source_id, target_id),
        )

    src_name = conn.execute("SELECT canonical_name FROM entities WHERE id = ?", (source_id,)).fetchone()
    if src_name:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO entity_aliases(entity_id, alias, alias_type) VALUES(?,?,?)",
                (target_id, src_name[0], "declension_variant"),
            )
        except Exception:
            pass

    return mentions_moved


def resolve_all_persons(settings: dict = None) -> Dict:
    if settings is None:
        settings = load_settings()

    conn = get_db(settings)

    deputy_entity_ids = set(
        r[0] for r in conn.execute("SELECT entity_id FROM deputy_profiles").fetchall()
    )

    persons = conn.execute(
        """
        SELECT e.id, e.canonical_name,
               (SELECT COUNT(*) FROM entity_mentions WHERE entity_id = e.id) as mention_count
        FROM entities e
        WHERE e.entity_type = 'person'
        ORDER BY mention_count DESC
        """
    ).fetchall()

    surname_groups = defaultdict(list)
    for pid, name, mcount in persons:
        parts = name.split()
        if not parts:
            continue
        surname = parts[0]
        root = _normalize_yo(surname[:4].lower())
        surname_groups[root].append((pid, name, mcount))

    total_merged = 0
    total_mentions_redirected = 0
    already_merged = set()

    for root, group in surname_groups.items():
        if len(group) < 2:
            continue

        sorted_group = sorted(group, key=lambda x: (-len(x[1].split()), -x[2]))

        clusters = []
        used = set()

        for i, (pid, pname, pmcount) in enumerate(sorted_group):
            if i in used:
                continue
            cluster = [(pid, pname, pmcount)]
            used.add(i)
            for j, (qid, qname, qmcount) in enumerate(sorted_group):
                if j in used:
                    continue
                if _can_merge_names(pname, qname):
                    cluster.append((qid, qname, qmcount))
                    used.add(j)
            clusters.append(cluster)

        for cluster in clusters:
            if len(cluster) < 2:
                continue

            cluster_sorted = sorted(cluster, key=lambda x: (-x[2], -len(x[1].split())))
            primary = cluster_sorted[0]
            primary_id = primary[0]

            if primary_id in deputy_entity_ids or primary_id in already_merged:
                target_id = primary_id
            elif any(cid in deputy_entity_ids for cid, _, _ in cluster):
                dep_entry = [(cid, cn, mc) for cid, cn, mc in cluster if cid in deputy_entity_ids][0]
                target_id = dep_entry[0]
            else:
                target_id = primary_id

            for cid, cname, cmcount in cluster_sorted[1:]:
                if cid in deputy_entity_ids or cid in already_merged:
                    continue
                mentions_moved = _merge_entity(conn, target_id, cid)
                total_mentions_redirected += mentions_moved
                total_merged += 1
                already_merged.add(cid)

    conn.commit()

    log.info(
        "All-persons resolution: %d entities merged, %d mentions redirected",
        total_merged, total_mentions_redirected,
    )

    stats = {
        "merged": total_merged,
        "mentions_redirected": total_mentions_redirected,
    }

    conn.close()
    return stats


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--deputies-only", action="store_true", help="Only resolve deputy entities")
    parser.add_argument("--all-persons", action="store_true", help="Also merge non-deputy person entities by declension")
    args = parser.parse_args()

    result = resolve_deputies()
    if args.all_persons:
        result2 = resolve_all_persons()
        result["all_persons_merged"] = result2["merged"]
        result["all_persons_mentions_redirected"] = result2["mentions_redirected"]

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
