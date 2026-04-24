import sqlite3

from PySide6.QtCore import Qt
from PySide6.QtWidgets import QGridLayout, QLabel, QVBoxLayout, QWidget


class StatCard(QWidget):
    def __init__(self, title: str, value: str = "0", subtitle: str = "", parent=None):
        super().__init__(parent)
        self.setObjectName("OverviewCard")
        layout = QVBoxLayout(self)
        layout.setContentsMargins(14, 12, 14, 12)
        layout.setSpacing(4)

        title_label = QLabel(title)
        title_label.setObjectName("mutedLabel")
        layout.addWidget(title_label)

        self._value_label = QLabel(value)
        self._value_label.setObjectName("statValue")
        layout.addWidget(self._value_label)

        self._subtitle_label = QLabel(subtitle)
        self._subtitle_label.setObjectName("statSubtitle")
        self._subtitle_label.setWordWrap(True)
        layout.addWidget(self._subtitle_label)

    def set_value(self, value: str, subtitle: str = ""):
        self._value_label.setText(value)
        if subtitle:
            self._subtitle_label.setText(subtitle)


class OverviewTab(QWidget):
    def __init__(self, db: sqlite3.Connection, settings: dict, parent=None):
        super().__init__(parent)
        self.db = db
        self.settings = settings
        self._cards = {}

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        intro = QLabel(
            "Короткий срез по базе: объём контента, проверок, сущностей, дел и медиа-вложений."
        )
        intro.setObjectName("mutedLabel")
        intro.setWordWrap(True)
        layout.addWidget(intro)

        grid = QGridLayout()
        grid.setContentsMargins(0, 0, 0, 0)
        grid.setHorizontalSpacing(12)
        grid.setVerticalSpacing(12)

        cards = [
            ("content", "Контент", "0", "Посты, статьи, видео"),
            ("claims", "Заявления", "0", "Извлечённые claims"),
            ("entities", "Сущности", "0", "Персоны, организации, места"),
            ("cases", "Дела", "0", "Связанные кейсы"),
            ("persons", "Персоны", "0", "NER и профили"),
            ("quotes", "Цитаты", "0", "Прямые и косвенные"),
            ("flagged", "Флаговые цитаты", "0", "Риторические маркеры"),
            ("tags", "Теги", "0", "Уникальные метки"),
            ("sources", "Источники", "0", "Активные каналы"),
            ("deputies", "Депутаты", "0", "Активные профили"),
            ("evidence", "Свидетельства", "0", "Связи claim/evidence"),
            ("attachments", "Вложения", "0", "Файлы и изображения"),
            ("bills", "Законопроекты", "0", "Собранные bills"),
            ("vote_sessions", "Голосования", "0", "Сессии голосований"),
            ("investigative", "Следственные мат.", "0", "Расследования, суды"),
            ("relations", "Связи", "0", "entity_relations"),
        ]

        for index, (key, title, value, subtitle) in enumerate(cards):
            card = StatCard(title, value, subtitle)
            self._cards[key] = card
            grid.addWidget(card, index // 6, index % 6)

        layout.addLayout(grid)

        risk_title = QLabel("Слабая подотчётность")
        risk_title.setObjectName("sectionLabel")
        layout.addWidget(risk_title)

        self._top_deputies_label = QLabel("")
        self._top_deputies_label.setObjectName("mutedLabel")
        self._top_deputies_label.setWordWrap(True)
        self._top_deputies_label.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        self._top_deputies_label.setStyleSheet(
            "background-color: #11161f; border: 1px solid #263041; border-radius: 12px; padding: 12px;"
        )
        layout.addWidget(self._top_deputies_label)
        layout.addStretch()

        self.refresh_stats()

    def refresh_stats(self):
        try:
            self._set_card("content", self.db.execute("SELECT COUNT(*) FROM content_items").fetchone()[0])
            self._set_card("claims", self.db.execute("SELECT COUNT(*) FROM claims").fetchone()[0])
            self._set_card("entities", self.db.execute("SELECT COUNT(*) FROM entities").fetchone()[0])
            self._set_card("cases", self.db.execute("SELECT COUNT(*) FROM cases").fetchone()[0])
            self._set_card("persons", self.db.execute("SELECT COUNT(*) FROM entities WHERE entity_type='person'").fetchone()[0])
            self._set_card("quotes", self.db.execute("SELECT COUNT(*) FROM quotes").fetchone()[0])
            self._set_card("flagged", self.db.execute("SELECT COUNT(*) FROM quotes WHERE is_flagged=1").fetchone()[0])
            self._set_card("tags", self.db.execute("SELECT COUNT(DISTINCT tag_name) FROM content_tags").fetchone()[0])
            self._set_card("sources", self.db.execute("SELECT COUNT(*) FROM sources WHERE is_active=1").fetchone()[0])
            self._set_card("deputies", self.db.execute("SELECT COUNT(*) FROM deputy_profiles WHERE is_active=1").fetchone()[0])
            self._set_card("evidence", self.db.execute("SELECT COUNT(*) FROM evidence_links").fetchone()[0])
            self._set_card("attachments", self.db.execute("SELECT COUNT(*) FROM attachments").fetchone()[0])
            self._set_card("bills", self.db.execute("SELECT COUNT(*) FROM bills").fetchone()[0])
            self._set_card("vote_sessions", self.db.execute("SELECT COUNT(*) FROM bill_vote_sessions").fetchone()[0])
            self._set_card("investigative", self.db.execute("SELECT COUNT(*) FROM investigative_materials").fetchone()[0])
            self._set_card("relations", self.db.execute("SELECT COUNT(*) FROM entity_relations").fetchone()[0])

            top_deputies = self.db.execute(
                """
                SELECT dp.full_name, dp.faction, ai.calculated_score,
                       ai.public_speeches_count, ai.flagged_statements_count, ai.linked_cases_count
                FROM accountability_index ai
                JOIN deputy_profiles dp ON dp.id = ai.deputy_id
                WHERE ai.period = (SELECT MAX(period) FROM accountability_index)
                ORDER BY ai.calculated_score ASC
                LIMIT 10
                """
            ).fetchall()
            if top_deputies:
                lines = ["Нижние 10 по последнему периоду:"]
                for deputy in top_deputies:
                    lines.append(
                        f"• {deputy[0]} ({deputy[1]}): {deputy[2]:.1f} "
                        f"| выступления {deputy[3]} | флаги {deputy[4]} | дела {deputy[5]}"
                    )
                self._top_deputies_label.setText("\n".join(lines))
            else:
                self._top_deputies_label.setText("Данные по индексу подотчётности пока не рассчитаны.")
        except Exception as error:
            self._top_deputies_label.setText(f"Ошибка загрузки обзора: {error}")

    def _set_card(self, key: str, value):
        if key in self._cards:
            self._cards[key].set_value(str(value))
