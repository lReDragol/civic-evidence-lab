import json
import sqlite3

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class InvestigativeTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(8)

        self._type_filter = QComboBox()
        self._type_filter.addItem("Все типы", "")
        type_labels = {
            "ach_news": "Новости Счётной палаты",
            "audit_report": "Аудиторский отчёт",
            "fas_decision": "Решение ФАС",
            "foreign_agent": "Иноагент",
            "government_contract": "Госзакупка",
            "government_decision": "Постановление Правительства",
            "investigation_report": "Следственное сообщение",
            "legal_act_publication": "Публикация правового акта",
            "presidential_act": "Указ Президента",
            "undesirable_org": "Нежелательная организация",
        }
        for r in self.db.execute("SELECT DISTINCT material_type FROM investigative_materials WHERE material_type IS NOT NULL ORDER BY material_type"):
            label = type_labels.get(r[0], r[0])
            self._type_filter.addItem(label, r[0])
        self._type_filter.currentIndexChanged.connect(self._load_materials)
        toolbar.addWidget(QLabel("Тип:"))
        toolbar.addWidget(self._type_filter)

        self._status_filter = QComboBox()
        self._status_filter.addItem("Все статусы", "")
        status_labels = {"verified": "Подтверждено", "partially": "Частично", "unverified": "Не проверено", "disputed": "Оспаривается", "confirmed": "Подтверждено", "archived": "В архиве"}
        for r in self.db.execute("SELECT DISTINCT verification_status FROM investigative_materials WHERE verification_status IS NOT NULL ORDER BY verification_status"):
            label = status_labels.get(r[0], r[0])
            self._status_filter.addItem(label, r[0])
        self._status_filter.currentIndexChanged.connect(self._load_materials)
        toolbar.addWidget(QLabel("Верификация:"))
        toolbar.addWidget(self._status_filter)

        self._org_filter = QComboBox()
        self._org_filter.addItem("Все органы", "")
        for o in self.db.execute(
            "SELECT DISTINCT source_org FROM investigative_materials WHERE source_org IS NOT NULL ORDER BY source_org"
        ):
            self._org_filter.addItem(o[0], o[0])
        self._org_filter.currentIndexChanged.connect(self._load_materials)
        toolbar.addWidget(QLabel("Орган:"))
        toolbar.addWidget(self._org_filter)

        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Поиск по названию...")
        self._search_input.returnPressed.connect(self._load_materials)
        toolbar.addWidget(self._search_input, stretch=2)

        btn_refresh = QPushButton("Обновить")
        btn_refresh.clicked.connect(self._load_materials)
        toolbar.addWidget(btn_refresh)
        layout.addLayout(toolbar)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 7)
        self._table.setHorizontalHeaderLabels(["ID", "Название", "Тип", "Орган", "Дата", "Верификация", "Связанные лица"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeToContents)
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableWidget.SelectRows)
        self._table.setEditTriggers(QTableWidget.NoEditTriggers)
        self._table.setSortingEnabled(True)
        self._table.setShowGrid(False)
        self._table.setWordWrap(False)
        self._table.verticalHeader().setVisible(False)
        self._table.verticalHeader().setDefaultSectionSize(34)
        self._table.currentCellChanged.connect(self._on_row_selected)
        splitter.addWidget(self._table)

        detail = QWidget()
        detail_layout = QVBoxLayout(detail)
        detail_layout.setContentsMargins(8, 0, 0, 0)
        detail_layout.setSpacing(8)

        self._detail_title = QLabel("Материал не выбран")
        self._detail_title.setStyleSheet("font-size: 16px; font-weight: 700; color: #eef5ff;")
        self._detail_title.setWordWrap(True)
        detail_layout.addWidget(self._detail_title)

        self._detail_info = QTextEdit()
        self._detail_info.setReadOnly(True)
        self._detail_info.setMaximumHeight(200)
        self._detail_info.setPlaceholderText("Описание и метаинформация.")
        detail_layout.addWidget(self._detail_info)

        entities_label = QLabel("Связанные лица / организации")
        entities_label.setObjectName("sectionLabel")
        detail_layout.addWidget(entities_label)

        self._entities_text = QTextEdit()
        self._entities_text.setReadOnly(True)
        self._entities_text.setMaximumHeight(120)
        detail_layout.addWidget(self._entities_text)

        laws_label = QLabel("Упомянутые законы")
        laws_label.setObjectName("sectionLabel")
        detail_layout.addWidget(laws_label)

        self._laws_text = QTextEdit()
        self._laws_text.setReadOnly(True)
        self._laws_text.setMaximumHeight(100)
        detail_layout.addWidget(self._laws_text)

        cases_label = QLabel("Связанные кейсы")
        cases_label.setObjectName("sectionLabel")
        detail_layout.addWidget(cases_label)

        self._cases_text = QTextEdit()
        self._cases_text.setReadOnly(True)
        self._cases_text.setMaximumHeight(100)
        detail_layout.addWidget(self._cases_text)

        verify_btn = QPushButton("Верифицировать против официальных данных")
        verify_btn.setObjectName("runBtn")
        verify_btn.clicked.connect(self._verify_material)
        detail_layout.addWidget(verify_btn)

        self._verify_result = QTextEdit()
        self._verify_result.setReadOnly(True)
        self._verify_result.setMaximumHeight(80)
        self._verify_result.setPlaceholderText("Результат верификации.")
        detail_layout.addWidget(self._verify_result, stretch=1)

        splitter.addWidget(detail)
        splitter.setSizes([700, 560])
        layout.addWidget(splitter, stretch=1)

        self._current_material_id = None
        self._load_materials()

    def _load_materials(self):
        mtype = self._type_filter.currentData() or ""
        vstatus = self._status_filter.currentData() or ""
        org = self._org_filter.currentData() or ""
        search = self._search_input.text().strip()

        where_parts = ["1=1"]
        params = []
        if mtype:
            where_parts.append("im.material_type = ?")
            params.append(mtype)
        if vstatus:
            where_parts.append("im.verification_status = ?")
            params.append(vstatus)
        if org:
            where_parts.append("im.source_org = ?")
            params.append(org)
        if search:
            where_parts.append("im.title LIKE ?")
            params.append(f"%{search}%")

        where = " AND ".join(where_parts)
        rows = self.db.execute(
            f"""
            SELECT im.id, im.title, im.material_type, im.source_org,
                   im.publication_date, im.verification_status,
                   CASE WHEN im.involved_entities IS NOT NULL AND im.involved_entities != ''
                        THEN json_array_length(im.involved_entities) ELSE 0 END
            FROM investigative_materials im
            WHERE {where}
            ORDER BY im.publication_date DESC
            LIMIT 300
            """,
            params,
        ).fetchall()

        self._table.setRowCount(len(rows))
        for i, r in enumerate(rows):
            for c, v in enumerate(r):
                self._table.setItem(i, c, QTableWidgetItem("" if v is None else str(v)))

    def _on_row_selected(self, row, col, prev_row, prev_col):
        if row < 0:
            return
        item = self._table.item(row, 0)
        if not item:
            return
        material_id = int(item.text())
        self._current_material_id = material_id
        self._show_material_detail(material_id)

    def _show_material_detail(self, material_id):
        mat = self.db.execute("SELECT * FROM investigative_materials WHERE id = ?", (material_id,)).fetchone()
        if not mat:
            return

        self._detail_title.setText(mat["title"] or "Без названия")

        info_lines = [
            f"Тип: {mat['material_type'] or '—'}",
            f"Орган: {mat['source_org'] or '—'}",
            f"Дата публикации: {mat['publication_date'] or '—'}",
            f"Верификация: {mat['verification_status'] or '—'}",
            f"Достоверность источника: {mat['source_credibility'] or '—'}",
        ]
        if mat["url"]:
            info_lines.append(f"URL: {mat['url']}")
        if mat["summary"]:
            info_lines.append(f"\nСводка:\n{mat['summary'][:600]}")
        self._detail_info.setPlainText("\n".join(info_lines))

        if mat["involved_entities"]:
            try:
                eids = json.loads(mat["involved_entities"])
                names = []
                for eid in eids[:20]:
                    row = self.db.execute("SELECT canonical_name FROM entities WHERE id = ?", (eid,)).fetchone()
                    if row:
                        names.append(f"  [{eid}] {row[0]}")
                self._entities_text.setPlainText("\n".join(names) if names else "Сущности не найдены.")
            except Exception:
                self._entities_text.setPlainText(f"Сырые данные: {mat['involved_entities'][:300]}")
        else:
            self._entities_text.setPlainText("Связанные лица не указаны.")

        if mat["referenced_laws"]:
            try:
                laws = json.loads(mat["referenced_laws"])
                self._laws_text.setPlainText("\n".join(f"  {l}" for l in laws[:20]))
            except Exception:
                self._laws_text.setPlainText(f"Сырые данные: {mat['referenced_laws'][:300]}")
        else:
            self._laws_text.setPlainText("Законы не указаны.")

        if mat["referenced_cases"]:
            try:
                case_ids = json.loads(mat["referenced_cases"])
                rows = self.db.execute(
                    f"SELECT id, title, case_type, status FROM cases WHERE id IN ({','.join('?' * len(case_ids))})",
                    case_ids[:20],
                ).fetchall()
                if rows:
                    self._cases_text.setPlainText("\n".join(f"  [{r[0]}] {r[1]} ({r[2]}/{r[3]})" for r in rows))
                else:
                    self._cases_text.setPlainText("Кейсы не найдены.")
            except Exception:
                self._cases_text.setPlainText(f"Сырые данные: {mat['referenced_cases'][:300]}")
        else:
            linked = self.db.execute(
                """
                SELECT DISTINCT c.id, c.title, c.case_type, c.status
                FROM cases c
                JOIN case_claims cc ON cc.case_id = c.id
                JOIN claims cl ON cl.id = cc.claim_id
                WHERE cl.content_item_id = ?
                LIMIT 10
                """,
                (mat["content_item_id"],),
            ).fetchall() if mat["content_item_id"] else []
            if linked:
                self._cases_text.setPlainText("\n".join(f"  [{r[0]}] {r[1]} ({r[2]}/{r[3]})" for r in linked))
            else:
                self._cases_text.setPlainText("Связанных кейсов нет.")

        self._verify_result.setPlainText("")

    def _verify_material(self):
        if self._current_material_id is None:
            return
        mat = self.db.execute("SELECT * FROM investigative_materials WHERE id = ?", (self._current_material_id,)).fetchone()
        if not mat:
            return

        evidence_count = 0
        verification_notes = []

        if mat["content_item_id"]:
            elinks = self.db.execute(
                """
                SELECT COUNT(*), GROUP_CONCAT(DISTINCT el.evidence_type)
                FROM evidence_links el
                WHERE el.claim_id IN (
                    SELECT id FROM claims WHERE content_item_id = ?
                )
                """,
                (mat["content_item_id"],),
            ).fetchone()
            if elinks and elinks[0] > 0:
                evidence_count = elinks[0]
                verification_notes.append(f"Свидетельств привязано: {evidence_count} ({elinks[1] or '?'})")

        if mat["involved_entities"]:
            try:
                eids = json.loads(mat["involved_entities"])
                verified_entities = 0
                for eid in eids[:10]:
                    positions = self.db.execute(
                        "SELECT COUNT(*) FROM official_positions WHERE entity_id = ? AND is_active = 1",
                        (eid,),
                    ).fetchone()[0]
                    if positions > 0:
                        verified_entities += 1
                verification_notes.append(f"Сущностей с должностями: {verified_entities}/{len(eids)}")
            except Exception:
                pass

        source_cred = mat["source_credibility"] or "—"
        verification_notes.append(f"Достоверность источника: {source_cred}")

        if mat["referenced_laws"]:
            try:
                laws = json.loads(mat["referenced_laws"])
                verification_notes.append(f"Ссылок на законы: {len(laws)}")
            except Exception:
                pass

        vstatus = mat["verification_status"] or "unverified"
        if evidence_count >= 3:
            new_status = "verified"
        elif evidence_count >= 1:
            new_status = "partially"
        else:
            new_status = vstatus

        if new_status != vstatus:
            self.db.execute(
                "UPDATE investigative_materials SET verification_status = ? WHERE id = ?",
                (new_status, self._current_material_id),
            )
            self.db.commit()
            verification_notes.append(f"Статус обновлён: {vstatus} → {new_status}")
        else:
            verification_notes.append(f"Статус без изменений: {vstatus}")

        self._verify_result.setPlainText("\n".join(verification_notes))
