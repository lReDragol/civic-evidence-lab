import sqlite3
import tempfile
import unittest
from pathlib import Path

from collectors.executive_directory_scraper import (
    collect_source,
    ensure_source,
    parse_profile_page,
    parse_profile_links_directory,
    parse_text_directory,
    store_person_record,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "db" / "schema.sql"


PROFILE_INDEX_HTML = """
<html>
  <body>
    <section class="leaders">
      <article class="leader-card">
        <a href="/ru/ministry/structure/management/?id_4=5-ivanov_ivan_ivanovich">
          Иванов Иван Иванович
        </a>
        <p>Министр тестирования Российской Федерации</p>
      </article>
      <article class="leader-card">
        <a href="/ru/ministry/structure/management/?id_4=6-petrov_petr_petrovich">
          Петров Пётр Петрович
        </a>
        <p>Первый заместитель министра тестирования Российской Федерации</p>
      </article>
    </section>
  </body>
</html>
"""


TEXT_DIRECTORY_HTML = """
<html>
  <body>
    <main>
      Андрей Юрьевич Липов Руководитель подробнее Родился 23 ноября 1969 года в Москве.
      Владимир Викторович Логунов Заместитель руководителя подробнее Родился 1 ноября 1982 года.
    </main>
  </body>
</html>
"""


RELATIVE_HREF_HTML = """
<html>
  <body>
    <a href="4312378/">Егоров Даниил Вячеславович</a>
  </body>
</html>
"""


FILTERED_INDEX_HTML = """
<html>
  <body>
    <article>
      <a href="1/">Иванов Иван Иванович</a>
      <p>Руководитель службы</p>
    </article>
    <article>
      <a href="2/">Петров Пётр Петрович</a>
      <p>Руководители службы новейшего времени</p>
    </article>
  </body>
</html>
"""


GOVERNMENT_INLINE_HTML = """
<html>
  <body>
    <section>
      <a href="/gov/persons/621/">
        Заместитель Председателя Правительства – Руководитель Аппарата Правительства Дмитрий Юрьевич Григоренко
      </a>
    </section>
  </body>
</html>
"""


PROFILE_PAGE_HTML = """
<html>
  <body>
    <main>
      <h3>Артюхин Роман Евгеньевич</h3>
      <p class="sm-hide">Руководитель Федерального казначейства</p>
      <div class="managerbio">
        <div class="manager-buttons">Биография Публикации</div>
        <p>Окончил профильный вуз.</p>
        <p>До назначения на должность руководителя Казначейства России работал заместителем руководителя.</p>
        <p>Член Коллегии Минфина России.</p>
      </div>
      <div>Информация размещена с согласия субъекта персональных данных</div>
      <footer>
        Телефон единого контактного центра
      </footer>
    </main>
  </body>
</html>
"""


FAS_PROFILE_PAGE_HTML = """
<html>
  <body>
    <h1 class="page_title">Фесюк Даниил Валерьевич</h1>
    <div class="page_description">
      Родился 8 февраля 1972 года в г. Тула.
      С 2015 года – заместитель руководителя Федеральной антимонопольной службы.
      Курирует отдельные направления.
    </div>
    <footer>Обратная связь по сайту</footer>
  </body>
</html>
"""


MINFIN_PROFILE_PAGE_HTML = """
<html>
  <body>
    <div class="manager_block first_manager">
      <div class="manager_block_content">
        <div class="manager_block_title">
          <h2 class="t_bb2">Силуанов Антон Германович</h2>
        </div>
        Министр финансов Российской Федерации
        <img src="/common/upload/persons/2022/siluanov_2022.jpg" alt="Силуанов Антон Германович">
      </div>
      <div class="manager_block_content">
        <p>С 2020 Министр финансов Российской Федерации</p>
        <p>Краткая биография.</p>
      </div>
    </div>
  </body>
</html>
"""


def create_db(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.commit()
    finally:
        conn.close()


class ExecutiveDirectoryScraperTests(unittest.TestCase):
    def test_parse_profile_links_directory_extracts_people(self):
        people = parse_profile_links_directory(
            PROFILE_INDEX_HTML,
            "https://example.test/ru/ministry/structure/",
            href_patterns=["/management/"],
        )

        self.assertEqual(len(people), 2)
        self.assertEqual(people[0]["full_name"], "Иванов Иван Иванович")
        self.assertEqual(
            people[0]["position_title"],
            "Министр тестирования Российской Федерации",
        )
        self.assertEqual(
            people[0]["profile_url"],
            "https://example.test/ru/ministry/structure/management/?id_4=5-ivanov_ivan_ivanovich",
        )
        self.assertEqual(people[1]["full_name"], "Петров Пётр Петрович")

    def test_parse_text_directory_extracts_head_and_deputy(self):
        people = parse_text_directory(TEXT_DIRECTORY_HTML)

        self.assertEqual(len(people), 2)
        self.assertEqual(people[0]["full_name"], "Андрей Юрьевич Липов")
        self.assertEqual(people[0]["position_title"], "Руководитель")
        self.assertEqual(people[1]["full_name"], "Владимир Викторович Логунов")
        self.assertEqual(people[1]["position_title"], "Заместитель руководителя")

    def test_parse_profile_links_directory_matches_full_url_patterns(self):
        people = parse_profile_links_directory(
            RELATIVE_HREF_HTML,
            "https://example.test/rn77/about_fts/fts/structure_fts/ca_fns/",
            href_patterns=["/ca_fns/"],
        )

        self.assertEqual(len(people), 1)
        self.assertEqual(people[0]["full_name"], "Егоров Даниил Вячеславович")
        self.assertEqual(
            people[0]["profile_url"],
            "https://example.test/rn77/about_fts/fts/structure_fts/ca_fns/4312378/",
        )

    def test_parse_profile_links_directory_extracts_government_style_position_and_name(self):
        people = parse_profile_links_directory(
            GOVERNMENT_INLINE_HTML,
            "https://example.test/office/persons/",
            href_patterns=["/gov/persons/"],
        )

        self.assertEqual(len(people), 1)
        self.assertEqual(people[0]["full_name"], "Дмитрий Юрьевич Григоренко")
        self.assertEqual(
            people[0]["position_title"],
            "Заместитель Председателя Правительства – Руководитель Аппарата Правительства",
        )
        self.assertEqual(
            people[0]["profile_url"],
            "https://example.test/gov/persons/621/",
        )

    def test_parse_profile_page_prefers_local_bio_block_and_trims_footer_noise(self):
        parsed = parse_profile_page(PROFILE_PAGE_HTML, "https://example.test/roskazna/person/22")

        self.assertEqual(parsed["full_name"], "Артюхин Роман Евгеньевич")
        self.assertEqual(parsed["position_title"], "Руководитель Федерального казначейства")
        self.assertIn("Окончил профильный вуз.", parsed["bio_text"])
        self.assertIn("Член Коллегии Минфина России.", parsed["bio_text"])
        self.assertNotIn("Телефон единого контактного центра", parsed["bio_text"])
        self.assertNotIn("Информация размещена с согласия", parsed["bio_text"])

    def test_parse_profile_page_infers_position_from_timeline_sentence(self):
        parsed = parse_profile_page(FAS_PROFILE_PAGE_HTML, "https://example.test/fas/people/381")

        self.assertEqual(parsed["full_name"], "Фесюк Даниил Валерьевич")
        self.assertEqual(
            parsed["position_title"],
            "Заместитель руководителя Федеральной антимонопольной службы",
        )
        self.assertNotIn("Обратная связь по сайту", parsed["bio_text"])

    def test_parse_profile_page_extracts_manager_block_position_and_photo(self):
        parsed = parse_profile_page(MINFIN_PROFILE_PAGE_HTML, "https://example.test/minfin/person/5")

        self.assertEqual(parsed["full_name"], "Силуанов Антон Германович")
        self.assertEqual(parsed["position_title"], "Министр финансов Российской Федерации")
        self.assertEqual(
            parsed["photo_url"],
            "https://example.test/common/upload/persons/2022/siluanov_2022.jpg",
        )
        self.assertIn("Краткая биография.", parsed["bio_text"])

    def test_store_person_record_upserts_entities_positions_and_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "executive.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                source_cfg = {
                    "key": "minfin",
                    "name": "Минфин РФ — руководство",
                    "organization": "Министерство финансов Российской Федерации",
                    "url": "https://example.test/minfin/structure/",
                    "category": "official_site",
                    "subcategory": "executive_directory",
                }
                source_id = ensure_source(conn, source_cfg)

                person = {
                    "full_name": "Иванов Иван Иванович",
                    "position_title": "Министр тестирования Российской Федерации",
                    "organization": "Министерство финансов Российской Федерации",
                    "profile_url": "https://example.test/minfin/person/1",
                    "photo_url": "https://example.test/minfin/person/1/photo.jpg",
                    "bio_text": "Краткая биография руководителя.",
                    "aliases": ["Иванов И. И."],
                }

                store_person_record(conn, source_id, source_cfg, person)
                store_person_record(conn, source_id, source_cfg, person)
                conn.commit()

                entities = [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT entity_type, canonical_name FROM entities ORDER BY entity_type, canonical_name"
                    ).fetchall()
                ]
                aliases = [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT alias FROM entity_aliases ORDER BY alias"
                    ).fetchall()
                ]
                positions = [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT position_title, organization, source_type, is_active FROM official_positions"
                    ).fetchall()
                ]
                raw_count = conn.execute(
                    "SELECT COUNT(*) FROM raw_source_items"
                ).fetchone()[0]
                content_rows = [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT external_id, title, body_text FROM content_items"
                    ).fetchall()
                ]
                mention_rows = [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT entity_id, mention_type FROM entity_mentions ORDER BY entity_id"
                    ).fetchall()
                ]
            finally:
                conn.close()

            self.assertEqual(
                entities,
                [
                    ("organization", "Министерство финансов Российской Федерации"),
                    ("person", "Иванов Иван Иванович"),
                ],
            )
            self.assertEqual([row[0] for row in aliases], ["Иванов И. И."])
            self.assertEqual(
                positions,
                [
                    (
                        "Министр тестирования Российской Федерации",
                        "Министерство финансов Российской Федерации",
                        "executive_directory:minfin",
                        1,
                    )
                ],
            )
            self.assertEqual(raw_count, 1)
            self.assertEqual(len(content_rows), 1)
            self.assertIn("Иванов Иван Иванович", content_rows[0][1])
            self.assertIn("Краткая биография", content_rows[0][2])
            self.assertEqual(
                mention_rows,
                [
                    (1, "subject"),
                    (2, "organization"),
                ],
            )

    def test_ensure_source_updates_existing_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "executive.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                source_cfg = {
                    "key": "minfin",
                    "name": "Минфин России — руководство",
                    "organization": "Министерство финансов Российской Федерации",
                    "category": "official_site",
                    "subcategory": "executive_directory",
                    "url": "https://example.test/minfin/structure/",
                    "notes": "v1",
                }
                source_id = ensure_source(conn, source_cfg)
                source_cfg["notes"] = "v2"
                source_cfg["name"] = "Минфин России — обновлено"
                source_id_again = ensure_source(conn, source_cfg)
                conn.commit()

                row = conn.execute(
                    "SELECT name, notes, is_active FROM sources WHERE id=?",
                    (source_id,),
                ).fetchone()
            finally:
                conn.close()

            self.assertEqual(source_id_again, source_id)
            self.assertEqual(tuple(row), ("Минфин России — обновлено", "v2", 1))

    def test_store_person_record_deactivates_old_position_for_same_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "executive.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                source_cfg = {
                    "key": "rkn",
                    "name": "РКН — руководство",
                    "organization": "Роскомнадзор",
                    "url": "https://example.test/rkn/leadership/",
                    "category": "official_site",
                    "subcategory": "executive_directory",
                }
                source_id = ensure_source(conn, source_cfg)

                store_person_record(
                    conn,
                    source_id,
                    source_cfg,
                    {
                        "full_name": "Петров Пётр Петрович",
                        "position_title": "Заместитель руководителя",
                        "organization": "Роскомнадзор",
                        "profile_url": "https://example.test/rkn/person/7",
                    },
                )
                store_person_record(
                    conn,
                    source_id,
                    source_cfg,
                    {
                        "full_name": "Петров Пётр Петрович",
                        "position_title": "Первый заместитель руководителя",
                        "organization": "Роскомнадзор",
                        "profile_url": "https://example.test/rkn/person/7",
                    },
                )
                conn.commit()

                positions = [
                    tuple(row)
                    for row in conn.execute(
                        """
                        SELECT position_title, is_active
                        FROM official_positions
                        ORDER BY id
                        """
                    ).fetchall()
                ]
            finally:
                conn.close()

            self.assertEqual(
                positions,
                [
                    ("Заместитель руководителя", 0),
                    ("Первый заместитель руководителя", 1),
                ],
            )

    def test_collect_source_applies_position_and_exclude_filters(self):
        class FakeResponse:
            def __init__(self, text: str, url: str):
                self.text = text
                self.url = url

            def raise_for_status(self):
                return None

        class FakeSession:
            def get(self, url, timeout=25, allow_redirects=True, verify=True):
                return FakeResponse(FILTERED_INDEX_HTML, url)

        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "executive.db"
            create_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                source_cfg = {
                    "key": "roskazna",
                    "name": "Федеральное казначейство — руководство",
                    "organization": "Федеральное казначейство",
                    "category": "official_site",
                    "subcategory": "executive_directory",
                    "url": "https://example.test/roskazna/rukovodstvo/",
                    "mode": "profile_links",
                    "href_patterns": ["/rukovodstvo/"],
                    "allowed_position_patterns": ["руководитель службы"],
                    "exclude_text_patterns": ["новейшего времени"],
                    "fetch_profiles": False,
                }

                result = collect_source(conn, source_cfg, session=FakeSession())
                positions = [
                    tuple(row)
                    for row in conn.execute(
                        "SELECT position_title, organization FROM official_positions ORDER BY id"
                    ).fetchall()
                ]
            finally:
                conn.close()

            self.assertEqual(result["people_found"], 1)
            self.assertEqual(
                positions,
                [("Руководитель службы", "Федеральное казначейство")],
            )


if __name__ == "__main__":
    unittest.main()
