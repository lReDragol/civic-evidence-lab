import json
import sqlite3

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
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


class InvolvementTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        toolbar = QHBoxLayout()
        toolbar.setSpacing(8)

        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Поиск по ФИО / названию сущности...")
        self._search_input.returnPressed.connect(self._load_entities)
        toolbar.addWidget(self._search_input, stretch=2)

        btn_refresh = QPushButton("Обновить")
        btn_refresh.clicked.connect(self._load_entities)
        toolbar.addWidget(btn_refresh)

        btn_investigate = QPushButton("Найти зацепки")
        btn_investigate.setObjectName("accentButton")
        btn_investigate.setToolTip("Запустить расследование: раскрыть связи, найти наводки, построить досье")
        btn_investigate.clicked.connect(self._start_investigation)
        toolbar.addWidget(btn_investigate)
        layout.addLayout(toolbar)

        intro = QLabel("Полная карта причастности: законы, голосования, заявления, кейсы, риски, партии, должности.")
        intro.setObjectName("mutedLabel")
        intro.setWordWrap(True)
        layout.addWidget(intro)

        splitter = QSplitter(Qt.Horizontal)

        self._table = QTableWidget(0, 5)
        self._table.setHorizontalHeaderLabels(["ID", "Имя", "Тип", "Позиций", "Связей"])
        self._table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
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
        detail_layout.setSpacing(6)

        self._detail_name = QLabel("Сущность не выбрана")
        self._detail_name.setStyleSheet("font-size: 16px; font-weight: 700; color: #eef5ff;")
        self._detail_name.setWordWrap(True)
        detail_layout.addWidget(self._detail_name)

        self._detail_info = QTextEdit()
        self._detail_info.setReadOnly(True)
        self._detail_info.setMaximumHeight(120)
        self._detail_info.setPlaceholderText("Основная информация и должности.")
        detail_layout.addWidget(self._detail_info)

        bills_label = QLabel("Законопроекты (спонсор)")
        bills_label.setObjectName("sectionLabel")
        detail_layout.addWidget(bills_label)

        self._bills_text = QTextEdit()
        self._bills_text.setReadOnly(True)
        self._bills_text.setMaximumHeight(100)
        detail_layout.addWidget(self._bills_text)

        votes_label = QLabel("Голосования")
        votes_label.setObjectName("sectionLabel")
        detail_layout.addWidget(votes_label)

        self._votes_text = QTextEdit()
        self._votes_text.setReadOnly(True)
        self._votes_text.setMaximumHeight(120)
        detail_layout.addWidget(self._votes_text)

        claims_label = QLabel("Заявления")
        claims_label.setObjectName("sectionLabel")
        detail_layout.addWidget(claims_label)

        self._claims_text = QTextEdit()
        self._claims_text.setReadOnly(True)
        self._claims_text.setMaximumHeight(100)
        detail_layout.addWidget(self._claims_text)

        cases_label = QLabel("Кейсы")
        cases_label.setObjectName("sectionLabel")
        detail_layout.addWidget(cases_label)

        self._cases_text = QTextEdit()
        self._cases_text.setReadOnly(True)
        self._cases_text.setMaximumHeight(80)
        detail_layout.addWidget(self._cases_text)

        risks_label = QLabel("Рисковые паттерны")
        risks_label.setObjectName("sectionLabel")
        detail_layout.addWidget(risks_label)

        self._risks_text = QTextEdit()
        self._risks_text.setReadOnly(True)
        self._risks_text.setMaximumHeight(80)
        detail_layout.addWidget(self._risks_text)

        relations_label = QLabel("Сеть связей")
        relations_label.setObjectName("sectionLabel")
        detail_layout.addWidget(relations_label)

        self._relations_text = QTextEdit()
        self._relations_text.setReadOnly(True)
        self._relations_text.setMaximumHeight(120)
        detail_layout.addWidget(self._relations_text)

        score_label = QLabel("Accountability Index")
        score_label.setObjectName("sectionLabel")
        detail_layout.addWidget(score_label)

        self._score_text = QTextEdit()
        self._score_text.setReadOnly(True)
        self._score_text.setMaximumHeight(60)
        detail_layout.addWidget(self._score_text, stretch=1)

        splitter.addWidget(detail)
        splitter.setSizes([500, 760])
        layout.addWidget(splitter, stretch=1)

        self._current_entity_id = None
        self._load_entities()

    @staticmethod
    def _parse_json(raw_text, default):
        if not raw_text:
            return default
        try:
            return json.loads(raw_text)
        except (json.JSONDecodeError, TypeError):
            return default

    @classmethod
    def _entity_ids_from_json(cls, raw_text):
        data = cls._parse_json(raw_text, [])
        if not isinstance(data, list):
            return []
        entity_ids = []
        for item in data:
            if isinstance(item, int):
                entity_id = item
            elif isinstance(item, str) and item.strip().isdigit():
                entity_id = int(item.strip())
            else:
                continue
            if entity_id not in entity_ids:
                entity_ids.append(entity_id)
        return entity_ids

    def _load_entities(self):
        search = self._search_input.text().strip()
        where = "1=1"
        params = []
        if search:
            where = "e.canonical_name LIKE ?"
            params.append(f"%{search}%")

        rows = self.db.execute(
            f"""
            SELECT e.id, e.canonical_name, e.entity_type,
                   (SELECT COUNT(*) FROM official_positions op WHERE op.entity_id = e.id AND op.is_active = 1),
                   (SELECT COUNT(*) FROM entity_relations er WHERE er.from_entity_id = e.id OR er.to_entity_id = e.id)
            FROM entities e
            WHERE {where}
            ORDER BY e.id DESC
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
        entity_id = int(item.text())
        self._current_entity_id = entity_id
        self._show_involvement(entity_id)

    def _show_involvement(self, entity_id):
        entity = self.db.execute("SELECT * FROM entities WHERE id = ?", (entity_id,)).fetchone()
        if not entity:
            return

        self._detail_name.setText(entity["canonical_name"] or "Без имени")

        positions = self.db.execute(
            """
            SELECT position_title, organization, region, faction, started_at, ended_at, is_active
            FROM official_positions WHERE entity_id = ?
            ORDER BY is_active DESC, started_at DESC
            LIMIT 10
            """,
            (entity_id,),
        ).fetchall()
        parties = self.db.execute(
            """
            SELECT party_name, role, is_current
            FROM party_memberships WHERE entity_id = ?
            ORDER BY is_current DESC
            LIMIT 10
            """,
            (entity_id,),
        ).fetchall()

        info_lines = [f"Тип: {entity['entity_type']}"]
        if entity["inn"]:
            info_lines.append(f"ИНН: {entity['inn']}")
        if positions:
            info_lines.append("\nДолжности:")
            for p in positions:
                active = "●" if p[6] else "○"
                end = f"–{p[5]}" if p[5] else "–н.в."
                info_lines.append(f"  {active} {p[0]}, {p[1]} ({p[4] or '?'}{end})")
        if parties:
            info_lines.append("\nПартии:")
            for pm in parties:
                cur = "●" if pm[2] else "○"
                role = f" ({pm[1]})" if pm[1] else ""
                info_lines.append(f"  {cur} {pm[0]}{role}")
        self._detail_info.setPlainText("\n".join(info_lines))

        sponsored = self.db.execute(
            """
            SELECT b.number, b.title, b.status, bs.faction, bs.sponsor_role
            FROM bill_sponsors bs
            JOIN bills b ON b.id = bs.bill_id
            WHERE bs.entity_id = ?
            ORDER BY b.registration_date DESC
            LIMIT 20
            """,
            (entity_id,),
        ).fetchall()
        if sponsored:
            lines = [f"  {s[0]} [{s[2] or '?'}] {s[3] or ''} — {s[1][:80] if s[1] else '?'}" for s in sponsored]
            self._bills_text.setPlainText("\n".join(lines))
        else:
            self._bills_text.setPlainText("Не спонсировал законопроекты.")

        votes = self.db.execute(
            """
            SELECT bvs.vote_date, bvs.vote_stage, bv.vote_result, b.number, b.title
            FROM bill_votes bv
            JOIN bill_vote_sessions bvs ON bvs.id = bv.vote_session_id
            JOIN bills b ON b.id = bvs.bill_id
            WHERE bv.entity_id = ?
            ORDER BY bvs.vote_date DESC
            LIMIT 30
            """,
            (entity_id,),
        ).fetchall()
        if votes:
            result_colors = {"за": "+", "против": "−", "воздержался": "~", "не голосовал": "○", "отсутствовал": "○"}
            lines = []
            for v in votes:
                sym = result_colors.get(v[2], v[2])
                lines.append(f"  {v[0] or '?'} [{sym}] {v[3]} ({v[1] or '?'})")
            self._votes_text.setPlainText("\n".join(lines))
        else:
            self._votes_text.setPlainText("Данных о голосованиях нет.")

        claims = self.db.execute(
            """
            SELECT DISTINCT cl.claim_text, cl.claim_type, cl.status
            FROM claims cl
            JOIN entity_mentions em ON em.content_item_id = cl.content_item_id
            WHERE em.entity_id = ?
            ORDER BY cl.id DESC
            LIMIT 15
            """,
            (entity_id,),
        ).fetchall()
        if claims:
            lines = [f"  [{c[1] or '?'}/{c[2] or '?'}] {c[0][:120]}" for c in claims]
            self._claims_text.setPlainText("\n".join(lines))
        else:
            self._claims_text.setPlainText("Заявления не найдены.")

        cases = self.db.execute(
            """
            SELECT DISTINCT c.id, c.title, c.case_type, c.status
            FROM cases c
            JOIN case_claims cc ON cc.case_id = c.id
            JOIN claims cl ON cl.id = cc.claim_id
            JOIN entity_mentions em ON em.content_item_id = cl.content_item_id
            WHERE em.entity_id = ?
            ORDER BY c.updated_at DESC
            LIMIT 15
            """,
            (entity_id,),
        ).fetchall()
        if cases:
            lines = [f"  [{c[0]}] {c[1]} ({c[2]}/{c[3]})" for c in cases]
            self._cases_text.setPlainText("\n".join(lines))
        else:
            self._cases_text.setPlainText("Связанных кейсов нет.")

        risk_rows = self.db.execute(
            """
            SELECT rp.pattern_type, rp.description, rp.risk_level, rp.entity_ids
            FROM risk_patterns rp
            ORDER BY rp.risk_level DESC, rp.detected_at DESC
            LIMIT 500
            """
        ).fetchall()
        risks = []
        for risk in risk_rows:
            if entity_id in self._entity_ids_from_json(risk[3]):
                risks.append(risk)
            if len(risks) >= 10:
                break
        if risks:
            lines = [f"  [{r[2] or '?'}] {r[0]}: {r[1][:100]}" for r in risks]
            self._risks_text.setPlainText("\n".join(lines))
        else:
            self._risks_text.setPlainText("Рисковых паттернов не найдено.")

        relations = self.db.execute(
            """
            SELECT
                CASE WHEN er.from_entity_id = ? THEN e2.canonical_name ELSE e1.canonical_name END AS other_name,
                CASE WHEN er.from_entity_id = ? THEN '→' ELSE '←' END AS direction,
                er.relation_type,
                er.strength
            FROM entity_relations er
            JOIN entities e1 ON e1.id = er.from_entity_id
            JOIN entities e2 ON e2.id = er.to_entity_id
            WHERE er.from_entity_id = ? OR er.to_entity_id = ?
            ORDER BY er.detected_at DESC
            LIMIT 30
            """,
            (entity_id, entity_id, entity_id, entity_id),
        ).fetchall()
        if relations:
            lines = [f"  {r[1]} {r[0]} [{r[2]}] ({r[3] or '?'})" for r in relations]
            self._relations_text.setPlainText("\n".join(lines))
        else:
            self._relations_text.setPlainText("Связей не найдено.")

        dp = self.db.execute(
            """
            SELECT dp.id FROM deputy_profiles dp WHERE dp.entity_id = ?
            """,
            (entity_id,),
        ).fetchone()
        if dp:
            ai = self.db.execute(
                """
                SELECT calculated_score, public_speeches_count, flagged_statements_count,
                       votes_tracked_count, linked_cases_count, period
                FROM accountability_index
                WHERE deputy_id = ?
                ORDER BY period DESC LIMIT 1
                """,
                (dp[0],),
            ).fetchone()
            if ai:
                self._score_text.setPlainText(
                    f"Балл: {ai[0]:.2f} | Период: {ai[5]}\n"
                    f"Выступлений: {ai[1]} | Флаги: {ai[2]} | Голосований: {ai[3]} | Дел: {ai[4]}"
                )
            else:
                self._score_text.setPlainText("Индекс не рассчитан.")
        else:
            self._score_text.setPlainText("Не депутат — индекс не применим.")

    def _start_investigation(self):
        row = self._table.currentRow()
        if row < 0:
            return
        entity_id_item = self._table.item(row, 0)
        if not entity_id_item:
            return
        entity_id = int(entity_id_item.text())

        from PySide6.QtWidgets import QProgressDialog
        from threading import Thread

        progress = QProgressDialog("Раскручивание связей...", None, 0, 0, self)
        progress.setWindowTitle("Расследование")
        progress.setCancelButton(None)
        progress.setModal(True)
        progress.show()

        self._investigation_result = None
        self._investigation_dossier = ""
        self._investigation_error = ""

        def run():
            try:
                from investigation.engine import InvestigationEngine
                from investigation.dossier import DossierGenerator

                engine = InvestigationEngine(self.settings.get("db_path", "db/news_unified.db"))
                result = engine.investigate(
                    entity_id,
                    max_hops=3,
                    min_confidence=__import__("investigation.models", fromlist=["Confidence"]).Confidence.LIKELY,
                )
                dossier = DossierGenerator(result).generate()
                engine.close()
                self._investigation_result = result
                self._investigation_dossier = dossier
            except Exception as e:
                self._investigation_error = str(e)

        t = Thread(target=run, daemon=True)
        t.start()

        from PySide6.QtCore import QTimer
        def check_done():
            if t.is_alive():
                return
            timer.stop()
            progress.close()
            if self._investigation_result:
                self._show_investigation_results()
            elif self._investigation_error:
                from PySide6.QtWidgets import QMessageBox
                QMessageBox.critical(self, "Расследование", self._investigation_error)

        timer = QTimer(self)
        timer.timeout.connect(check_done)
        timer.start(500)

    def _show_investigation_results(self):
        result = self._investigation_result
        dossier = self._investigation_dossier
        if not result:
            return

        from PySide6.QtWidgets import QDialog, QTabWidget, QFileDialog

        dlg = QDialog(self)
        dlg.setWindowTitle(f"Расследование: {result.seed_name}")
        dlg.setMinimumSize(900, 700)

        layout = QVBoxLayout(dlg)
        tabs = QTabWidget()
        layout.addWidget(tabs)

        dossier_tab = QWidget()
        dossier_layout = QVBoxLayout(dossier_tab)
        from PySide6.QtWidgets import QPlainTextEdit
        dossier_edit = QPlainTextEdit()
        dossier_edit.setReadOnly(True)
        dossier_edit.setPlainText(dossier)
        dossier_layout.addWidget(dossier_edit)
        tabs.addTab(dossier_tab, "Досье")

        stats_tab = QWidget()
        stats_layout = QVBoxLayout(stats_tab)
        stats_text = QPlainTextEdit()
        stats_text.setReadOnly(True)
        s = result.stats
        stats_text.setPlainText(
            f"Узлов: {s.get('total_nodes',0)}\n"
            f"Рёбер: {s.get('total_edges',0)}\n"
            f"Подтверждено: {s.get('confirmed_edges',0)}\n"
            f"Вероятно: {s.get('likely_edges',0)}\n"
            f"Требует проверки: {s.get('unconfirmed_edges',0)}\n"
            f"Противоречия: {s.get('contradictions',0)}\n"
            f"Риски: {s.get('risk_patterns',0)}\n"
            f"Цепи доказательств: {s.get('evidence_chains',0)}\n"
            f"Наводки: {s.get('leads',0)}\n"
        )
        stats_layout.addWidget(stats_text)
        tabs.addTab(stats_tab, "Статистика")

        btn_bar = QHBoxLayout()
        btn_save = QPushButton("Сохранить в БД")
        btn_save.clicked.connect(lambda: self._save_investigation(result, dossier))
        btn_bar.addWidget(btn_save)

        btn_graph = QPushButton("Открыть граф (DearPyGui)")
        btn_graph.clicked.connect(lambda: self._open_node_viewer(result, dossier))
        btn_bar.addWidget(btn_graph)

        btn_export = QPushButton("Экспорт JSON")
        btn_export.clicked.connect(lambda: self._export_investigation_json(result))
        btn_bar.addWidget(btn_export)

        btn_close = QPushButton("Закрыть")
        btn_close.clicked.connect(dlg.close)
        btn_bar.addWidget(btn_close)

        layout.addLayout(btn_bar)
        dlg.exec()

    def _save_investigation(self, result, dossier):
        from db.migrate_v3 import migrate, save_investigation
        migrate(self.db)
        inv_id = save_investigation(self.db, result.seed_entity_id, result, dossier,
                                     params={"hops": 3, "source": "involvement_tab"})
        from PySide6.QtWidgets import QMessageBox
        QMessageBox.information(self, "Сохранено", f"Расследование сохранено (ID={inv_id})")

    def _open_node_viewer(self, result, dossier):
        from investigation.node_viewer import launch_viewer
        p = launch_viewer(result, dossier)

    def _export_investigation_json(self, result):
        from PySide6.QtWidgets import QFileDialog
        path, _ = QFileDialog.getSaveFileName(self, "Экспорт JSON", f"investigation_{result.seed_entity_id}.json", "JSON (*.json)")
        if path:
            with open(path, "w", encoding="utf-8") as f:
                f.write(result.to_json())
