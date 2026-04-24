import argparse
import csv
import os
import re
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from news_tagging import infer_tags, split_tags
from PySide6.QtCore import QDate, Qt, Signal
from PySide6.QtGui import QColor, QFont, QPixmap, QTextCharFormat
from PySide6.QtWidgets import (
    QApplication,
    QCalendarWidget,
    QDialog,
    QGraphicsPixmapItem,
    QGraphicsScene,
    QGraphicsView,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QPushButton,
    QSplitter,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


@dataclass
class Event:
    date: QDate
    date_text: str
    time_text: str
    title: str
    text: str
    photos: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    message_ids: List[str] = field(default_factory=list)
    message_db_ids: List[int] = field(default_factory=list)


@dataclass
class DayBundle:
    date: QDate
    date_text: str
    events: List[Event]
    summaries: List[str]
    tags: List[str]


@dataclass
class PhotoAsset:
    rel_path: str
    path: str = ""
    blob: bytes = b""


def parse_time(value: str):
    try:
        return datetime.strptime(value, "%H:%M:%S")
    except Exception:
        return None


def short_text(text: str, limit: int = 180) -> str:
    if not text:
        return ""
    txt = re.sub(r"\s+", " ", text).strip()
    txt = re.sub(r"^(бля+|хаха+|опа+|кароч|кста|там кста|во первых|ребята[, ]+)", "", txt, flags=re.IGNORECASE).strip(" .-")
    if len(txt) <= limit:
        return txt
    return txt[: limit - 3].rstrip() + "..."


def first_sentence(text: str) -> str:
    txt = re.sub(r"\s+", " ", text or "").strip()
    if not txt:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", txt)
    return short_text(parts[0] if parts else txt)


def parse_calendar_summaries(path: str) -> Dict[str, List[str]]:
    if not os.path.exists(path):
        return {}
    data: Dict[str, List[str]] = defaultdict(list)
    current_date = None
    date_re = re.compile(r"^##\s+(\d{4}-\d{2}-\d{2})\s*$")
    item_re = re.compile(r"^\d+\.\s+(.*)$")
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            m_date = date_re.match(line)
            if m_date:
                current_date = m_date.group(1)
                continue
            if current_date:
                m_item = item_re.match(line)
                if m_item:
                    data[current_date].append(m_item.group(1).strip())
    return dict(data)


def detect_tags(text: str) -> List[str]:
    return infer_tags(text)


def merge_photo_only_rows(rows: List[dict], day_summaries: List[str], qdate: QDate) -> List[Event]:
    rows_sorted = sorted(rows, key=lambda r: (r.get("time", ""), r.get("message_id", ""), str(r.get("message_db_id", ""))))
    events: List[Event] = []

    for row in rows_sorted:
        text = (row.get("text") or "").strip()
        headline = (row.get("headline") or "").strip()
        photos = [x.strip() for x in (row.get("kept_photos") or "").split(";") if x.strip()]
        time_text = (row.get("time") or "").strip()
        msg_id = str(row.get("message_id", "") or "")
        db_id_raw = row.get("message_db_id", 0)
        try:
            msg_db_id = int(db_id_raw or 0)
        except Exception:
            msg_db_id = 0

        if text:
            title = headline
            if not title or title.startswith("Новость в изображении"):
                title = first_sentence(text)
            if not title:
                title = "Новость без заголовка"

            row_tags = split_tags(row.get("tags", ""))
            if not row_tags:
                row_tags = detect_tags(f"{title}\n{text}")

            event = Event(
                date=qdate,
                date_text=row["date"],
                time_text=time_text,
                title=title,
                text=text,
                photos=photos,
                tags=row_tags,
                message_ids=[msg_id] if msg_id else [],
                message_db_ids=[msg_db_id] if msg_db_id else [],
            )
            events.append(event)
            continue

        if events:
            current_t = parse_time(time_text)
            prev_t = parse_time(events[-1].time_text)
            if current_t and prev_t and abs((current_t - prev_t).total_seconds()) <= 900:
                events[-1].photos.extend(photos)
                if msg_id:
                    events[-1].message_ids.append(msg_id)
                if msg_db_id:
                    events[-1].message_db_ids.append(msg_db_id)
                continue

        fallback_title = day_summaries[0] if day_summaries else "Серия изображений по новости"
        event = Event(
            date=qdate,
            date_text=row["date"],
            time_text=time_text,
            title=short_text(fallback_title, 110) or "Серия изображений по новости",
            text="Текст в выгрузке отсутствует. Смысл новости сохранен по изображениям.",
            photos=photos,
            tags=detect_tags(fallback_title),
            message_ids=[msg_id] if msg_id else [],
            message_db_ids=[msg_db_id] if msg_db_id else [],
        )
        events.append(event)

    generic_events = [e for e in events if e.title.startswith("Серия изображений") or e.title.startswith("Новость в")]
    if day_summaries and generic_events:
        for idx, event in enumerate(generic_events):
            if idx < len(day_summaries):
                event.title = short_text(day_summaries[idx], 120)
                if event.text.startswith("Текст в выгрузке отсутствует"):
                    event.text = day_summaries[idx]
                event.tags = detect_tags(day_summaries[idx])

    return events


def load_pixmap_from_asset(asset: PhotoAsset) -> QPixmap:
    pixmap = QPixmap()
    if asset.blob:
        pixmap.loadFromData(asset.blob)
    if pixmap.isNull() and asset.path:
        pixmap = QPixmap(asset.path)
    return pixmap


class DataBackend:
    def __init__(self, data_dir: str = "", db_path: str = ""):
        self.data_dir = os.path.abspath(data_dir) if data_dir else ""
        self.db_path = os.path.abspath(db_path) if db_path else ""
        self.conn: Optional[sqlite3.Connection] = None
        if self.db_path:
            if not os.path.exists(self.db_path):
                raise FileNotFoundError(f"Не найден файл БД: {self.db_path}")
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def source_label(self) -> str:
        if self.conn:
            return f"БД: {self.db_path}"
        return f"CSV: {self.data_dir}"

    def resolve_photo(self, rel_path: str, export_dir: str = "") -> str:
        candidates = []
        if export_dir:
            candidates.append(os.path.join(export_dir, rel_path))
            candidates.append(os.path.join(export_dir, "news_output", "photos", os.path.basename(rel_path)))
        if self.data_dir:
            candidates.append(os.path.join(self.data_dir, rel_path))
            candidates.append(os.path.join(self.data_dir, "news_output", "photos", os.path.basename(rel_path)))
        for path in candidates:
            if path and os.path.exists(path):
                return path
        return ""

    def load_bundles(self) -> Dict[QDate, DayBundle]:
        if self.conn:
            return self._load_from_db()
        return self._load_from_csv()

    def _load_from_csv(self) -> Dict[QDate, DayBundle]:
        csv_path = os.path.join(self.data_dir, "news_output", "news_messages.csv")
        md_path = os.path.join(self.data_dir, "news_output", "news_calendar.md")
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Не найден файл: {csv_path}")

        summaries_by_date = parse_calendar_summaries(md_path)
        rows_by_date: Dict[str, List[dict]] = defaultdict(list)
        with open(csv_path, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                date_text = (row.get("date") or "").strip()
                if not date_text:
                    continue
                row["message_db_id"] = 0
                rows_by_date[date_text].append(row)

        bundles: Dict[QDate, DayBundle] = {}
        for date_text, rows in rows_by_date.items():
            qdate = QDate.fromString(date_text, "yyyy-MM-dd")
            if not qdate.isValid():
                continue

            day_summaries = summaries_by_date.get(date_text, [])
            events = merge_photo_only_rows(rows, day_summaries, qdate)

            tag_counter = defaultdict(int)
            for ev in events:
                for tag in ev.tags:
                    tag_counter[tag] += 1
            for sm in day_summaries:
                for tag in detect_tags(sm):
                    tag_counter[tag] += 1
            day_tags = [k for k, _ in sorted(tag_counter.items(), key=lambda x: (-x[1], x[0]))[:3]]

            bundles[qdate] = DayBundle(
                date=qdate,
                date_text=date_text,
                events=events,
                summaries=day_summaries,
                tags=day_tags or ["Прочее"],
            )
        return bundles

    def _load_from_db(self) -> Dict[QDate, DayBundle]:
        rows_by_date: Dict[str, List[dict]] = defaultdict(list)
        sql = """
            SELECT id AS message_db_id, date, time, message_id, headline, text, tags, kept_photos
            FROM messages
            WHERE COALESCE(decision, 'keep') = 'keep'
            ORDER BY date, time, id
        """
        for row in self.conn.execute(sql):
            date_text = (row["date"] or "").strip()
            if not date_text:
                continue
            rows_by_date[date_text].append(
                {
                    "message_db_id": row["message_db_id"],
                    "date": date_text,
                    "time": (row["time"] or "").strip(),
                    "message_id": str(row["message_id"] or ""),
                    "headline": (row["headline"] or "").strip(),
                    "text": (row["text"] or "").strip(),
                    "tags": (row["tags"] or "").strip(),
                    "kept_photos": (row["kept_photos"] or "").strip(),
                }
            )

        bundles: Dict[QDate, DayBundle] = {}
        for date_text, rows in rows_by_date.items():
            qdate = QDate.fromString(date_text, "yyyy-MM-dd")
            if not qdate.isValid():
                continue

            events = merge_photo_only_rows(rows, [], qdate)
            tag_counter = defaultdict(int)
            for ev in events:
                for tag in ev.tags:
                    tag_counter[tag] += 1
            day_tags = [k for k, _ in sorted(tag_counter.items(), key=lambda x: (-x[1], x[0]))[:3]]

            bundles[qdate] = DayBundle(
                date=qdate,
                date_text=date_text,
                events=events,
                summaries=[],
                tags=day_tags or ["Прочее"],
            )
        return bundles

    def get_photos_for_event(self, event: Event) -> List[PhotoAsset]:
        if self.conn and event.message_db_ids:
            assets = self._load_photos_from_db(event.message_db_ids)
            if assets:
                return assets

        assets = []
        for rel in event.photos:
            assets.append(PhotoAsset(rel_path=rel, path=self.resolve_photo(rel), blob=b""))
        return assets

    def _load_photos_from_db(self, message_db_ids: List[int]) -> List[PhotoAsset]:
        ids = []
        seen = set()
        for value in message_db_ids:
            try:
                v = int(value)
            except Exception:
                continue
            if v > 0 and v not in seen:
                seen.add(v)
                ids.append(v)
        if not ids:
            return []

        placeholders = ",".join("?" for _ in ids)
        sql = f"""
            SELECT p.photo_rel_path, p.image_blob, p.exists_on_disk, e.export_dir
            FROM photos p
            JOIN messages m ON m.id = p.message_db_id
            JOIN exports e ON e.id = m.export_id
            WHERE p.message_db_id IN ({placeholders})
            ORDER BY p.id
        """

        assets: List[PhotoAsset] = []
        for row in self.conn.execute(sql, ids):
            rel_path = (row["photo_rel_path"] or "").strip()
            blob = bytes(row["image_blob"]) if row["image_blob"] is not None else b""
            export_dir = (row["export_dir"] or "").strip()
            abs_path = ""
            if int(row["exists_on_disk"] or 0):
                abs_path = self.resolve_photo(rel_path, export_dir)
            assets.append(PhotoAsset(rel_path=rel_path, path=abs_path, blob=blob))
        return assets


class ClickableLabel(QLabel):
    clicked = Signal()

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)


class ZoomGraphicsView(QGraphicsView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.scene_obj = QGraphicsScene(self)
        self.setScene(self.scene_obj)
        self.pix_item = QGraphicsPixmapItem()
        self.scene_obj.addItem(self.pix_item)
        self.setBackgroundBrush(QColor("#111111"))
        self.setDragMode(QGraphicsView.DragMode.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)
        self.setResizeAnchor(QGraphicsView.ViewportAnchor.AnchorUnderMouse)

    def has_image(self) -> bool:
        return not self.pix_item.pixmap().isNull()

    def set_pixmap(self, pixmap: QPixmap):
        self.pix_item.setPixmap(pixmap)
        self.scene_obj.setSceneRect(self.pix_item.boundingRect())
        self.fit_to_window()

    def fit_to_window(self):
        if not self.has_image():
            return
        self.resetTransform()
        self.fitInView(self.pix_item, Qt.AspectRatioMode.KeepAspectRatio)

    def reset_zoom(self):
        self.resetTransform()

    def zoom_in(self):
        if self.has_image():
            self.scale(1.25, 1.25)

    def zoom_out(self):
        if self.has_image():
            self.scale(0.8, 0.8)

    def wheelEvent(self, event):
        if not self.has_image():
            super().wheelEvent(event)
            return
        if event.angleDelta().y() > 0:
            self.zoom_in()
        else:
            self.zoom_out()


class ImageViewerDialog(QDialog):
    def __init__(self, assets: List[PhotoAsset], start_index: int = 0, parent=None):
        super().__init__(parent)
        self.assets = assets or []
        self.index = max(0, min(start_index, len(self.assets) - 1)) if self.assets else 0

        self.setWindowTitle("Просмотр документа")
        self.resize(1100, 820)

        layout = QVBoxLayout(self)

        nav = QHBoxLayout()
        self.prev_btn = QPushButton("Предыдущее")
        self.next_btn = QPushButton("Следующее")
        self.counter = QLabel("")
        self.counter.setAlignment(Qt.AlignmentFlag.AlignCenter)
        nav.addWidget(self.prev_btn)
        nav.addWidget(self.counter, 1)
        nav.addWidget(self.next_btn)
        layout.addLayout(nav)

        zoom = QHBoxLayout()
        self.zoom_out_btn = QPushButton("-")
        self.zoom_in_btn = QPushButton("+")
        self.zoom_100_btn = QPushButton("100%")
        self.fit_btn = QPushButton("Вписать")
        zoom.addWidget(self.zoom_out_btn)
        zoom.addWidget(self.zoom_in_btn)
        zoom.addWidget(self.zoom_100_btn)
        zoom.addWidget(self.fit_btn)
        zoom.addStretch(1)
        layout.addLayout(zoom)

        self.view = ZoomGraphicsView()
        layout.addWidget(self.view, 1)

        self.prev_btn.clicked.connect(self.prev_photo)
        self.next_btn.clicked.connect(self.next_photo)
        self.zoom_in_btn.clicked.connect(self.view.zoom_in)
        self.zoom_out_btn.clicked.connect(self.view.zoom_out)
        self.zoom_100_btn.clicked.connect(self.view.reset_zoom)
        self.fit_btn.clicked.connect(self.view.fit_to_window)

        self.render_current()

    def render_current(self):
        if not self.assets:
            self.counter.setText("Нет изображений")
            self.prev_btn.setEnabled(False)
            self.next_btn.setEnabled(False)
            self.view.set_pixmap(QPixmap())
            return

        self.index = max(0, min(self.index, len(self.assets) - 1))
        asset = self.assets[self.index]
        pixmap = load_pixmap_from_asset(asset)
        self.view.set_pixmap(pixmap)

        name = os.path.basename(asset.rel_path) if asset.rel_path else f"Фото {self.index + 1}"
        self.setWindowTitle(f"Просмотр документа: {name}")
        self.counter.setText(f"{self.index + 1} / {len(self.assets)}")
        self.prev_btn.setEnabled(self.index > 0)
        self.next_btn.setEnabled(self.index < len(self.assets) - 1)

    def prev_photo(self):
        if self.index > 0:
            self.index -= 1
            self.render_current()

    def next_photo(self):
        if self.index < len(self.assets) - 1:
            self.index += 1
            self.render_current()


class EventDetailWidget(QWidget):
    def __init__(self, backend: DataBackend, parent=None):
        super().__init__(parent)
        self.backend = backend
        self.photo_assets: List[PhotoAsset] = []
        self.photo_index = 0

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.title_label = QLabel("Выберите новость")
        self.title_label.setWordWrap(True)
        title_font = QFont()
        title_font.setPointSize(13)
        title_font.setBold(True)
        self.title_label.setFont(title_font)
        layout.addWidget(self.title_label)

        self.meta_label = QLabel("")
        self.meta_label.setStyleSheet("color: #666;")
        layout.addWidget(self.meta_label)

        self.tags_label = QLabel("")
        self.tags_label.setStyleSheet("color: #0f5132; font-weight: bold;")
        layout.addWidget(self.tags_label)

        self.photo_label = ClickableLabel("Нет изображения")
        self.photo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.photo_label.setMinimumHeight(300)
        self.photo_label.setStyleSheet("border: 1px solid #ccc; background: #fafafa;")
        self.photo_label.setCursor(Qt.CursorShape.PointingHandCursor)
        layout.addWidget(self.photo_label)

        nav = QHBoxLayout()
        self.prev_btn = QPushButton("Предыдущее")
        self.next_btn = QPushButton("Следующее")
        self.counter_label = QLabel("")
        self.counter_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.open_btn = QPushButton("Открыть / Увеличить")
        nav.addWidget(self.prev_btn)
        nav.addWidget(self.counter_label, 1)
        nav.addWidget(self.next_btn)
        nav.addWidget(self.open_btn)
        layout.addLayout(nav)

        self.text_edit = QTextEdit()
        self.text_edit.setReadOnly(True)
        layout.addWidget(self.text_edit, 1)

        self.prev_btn.clicked.connect(self.prev_photo)
        self.next_btn.clicked.connect(self.next_photo)
        self.open_btn.clicked.connect(self.open_viewer)
        self.photo_label.clicked.connect(self.open_viewer)
        self.clear()

    def set_event(self, event: Event):
        self.title_label.setText(event.title)
        msg_count = max(len(event.message_ids), len(event.message_db_ids), 1)
        self.meta_label.setText(
            f"Дата: {event.date_text}   Время: {event.time_text or '--:--:--'}   Сообщений в событии: {msg_count}"
        )
        self.tags_label.setText("Темы: " + ", ".join(event.tags))
        self.text_edit.setPlainText(event.text or "(Текст отсутствует)")

        self.photo_assets = self.backend.get_photos_for_event(event)
        self.photo_index = 0
        self.render_photo()

    def clear(self):
        self.title_label.setText("Выберите новость")
        self.meta_label.setText("")
        self.tags_label.setText("")
        self.text_edit.setPlainText("")
        self.photo_assets = []
        self.photo_index = 0
        self.render_photo()

    def render_photo(self):
        if not self.photo_assets:
            self.photo_label.setPixmap(QPixmap())
            self.photo_label.setText("Нет изображения")
            self.prev_btn.setEnabled(False)
            self.next_btn.setEnabled(False)
            self.open_btn.setEnabled(False)
            self.counter_label.setText("")
            return

        self.photo_index = max(0, min(self.photo_index, len(self.photo_assets) - 1))
        asset = self.photo_assets[self.photo_index]
        pixmap = load_pixmap_from_asset(asset)
        if pixmap.isNull():
            self.photo_label.setPixmap(QPixmap())
            self.photo_label.setText("Не удалось загрузить изображение")
        else:
            scaled = pixmap.scaled(
                self.photo_label.size(),
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
            self.photo_label.setPixmap(scaled)
            self.photo_label.setText("")

        self.prev_btn.setEnabled(self.photo_index > 0)
        self.next_btn.setEnabled(self.photo_index < len(self.photo_assets) - 1)
        self.open_btn.setEnabled(not pixmap.isNull())
        self.counter_label.setText(f"{self.photo_index + 1} / {len(self.photo_assets)}")

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.render_photo()

    def prev_photo(self):
        if self.photo_index > 0:
            self.photo_index -= 1
            self.render_photo()

    def next_photo(self):
        if self.photo_index < len(self.photo_assets) - 1:
            self.photo_index += 1
            self.render_photo()

    def open_viewer(self):
        if not self.photo_assets:
            return
        dlg = ImageViewerDialog(self.photo_assets, self.photo_index, self)
        dlg.exec()
        self.photo_index = dlg.index
        self.render_photo()


class NewsCalendarWindow(QMainWindow):
    def __init__(self, bundles: Dict[QDate, DayBundle], backend: DataBackend):
        super().__init__()
        self.bundles = bundles
        self.backend = backend
        self.sorted_dates = sorted(self.bundles.keys(), reverse=True)
        self.current_date = None
        self._syncing = False

        self.setWindowTitle("Новости: календарь и лента по дням")
        self.resize(1280, 800)

        root = QWidget()
        self.setCentralWidget(root)
        root_layout = QVBoxLayout(root)

        header = QLabel(
            f"Дней с новостями: {len(self.bundles)} | Событий: {sum(len(v.events) for v in self.bundles.values())}\n"
            f"Источник: {self.backend.source_label()}"
        )
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        root_layout.addWidget(header)

        split = QSplitter(Qt.Orientation.Horizontal)
        root_layout.addWidget(split, 1)

        # Left panel: highlighted calendar + day list with counts/tags.
        left = QWidget()
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(6, 6, 6, 6)

        self.calendar = QCalendarWidget()
        self.calendar.setGridVisible(True)
        self.calendar.clicked.connect(self.on_calendar_clicked)
        left_layout.addWidget(self.calendar)

        day_title = QLabel("Дни с новостями")
        day_title.setStyleSheet("font-weight: bold;")
        left_layout.addWidget(day_title)

        self.day_list = QListWidget()
        self.day_list.currentRowChanged.connect(self.on_day_row_changed)
        left_layout.addWidget(self.day_list, 1)

        split.addWidget(left)

        # Right panel: list of events + detail card (no popups).
        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(6, 6, 6, 6)

        self.day_header = QLabel("Выберите день")
        self.day_header.setStyleSheet("font-size: 16px; font-weight: bold;")
        right_layout.addWidget(self.day_header)

        self.day_summary = QLabel("")
        self.day_summary.setWordWrap(True)
        self.day_summary.setStyleSheet("color: #555;")
        right_layout.addWidget(self.day_summary)

        right_split = QSplitter(Qt.Orientation.Vertical)
        right_layout.addWidget(right_split, 1)

        top_events = QWidget()
        top_events_layout = QVBoxLayout(top_events)
        top_events_layout.setContentsMargins(0, 0, 0, 0)

        events_label = QLabel("События за выбранный день")
        events_label.setStyleSheet("font-weight: bold;")
        top_events_layout.addWidget(events_label)

        self.events_list = QListWidget()
        self.events_list.currentRowChanged.connect(self.on_event_row_changed)
        top_events_layout.addWidget(self.events_list, 1)
        right_split.addWidget(top_events)

        self.detail = EventDetailWidget(backend)
        right_split.addWidget(self.detail)
        right_split.setSizes([260, 440])

        split.addWidget(right)
        split.setSizes([380, 860])

        self.apply_calendar_highlights()
        self.fill_day_list()
        if self.day_list.count() > 0:
            self.day_list.setCurrentRow(0)

    def apply_calendar_highlights(self):
        fmt = QTextCharFormat()
        fmt.setBackground(QColor("#2f9e44"))
        fmt.setForeground(QColor("#ffffff"))
        fmt.setFontWeight(QFont.Weight.Bold)
        for qdate in self.bundles:
            self.calendar.setDateTextFormat(qdate, fmt)

    def fill_day_list(self):
        self.day_list.clear()
        for qdate in self.sorted_dates:
            bundle = self.bundles[qdate]
            date_label = qdate.toString("dd.MM.yyyy")
            topics = ", ".join(bundle.tags[:2])
            txt = f"{date_label}  |  {len(bundle.events)} новостей  |  {topics}"
            item = QListWidgetItem(txt)
            item.setData(Qt.ItemDataRole.UserRole, qdate)
            tooltip = "\n".join(bundle.summaries[:2]) if bundle.summaries else "\n".join(ev.title for ev in bundle.events[:2])
            item.setToolTip(tooltip)
            self.day_list.addItem(item)

    def on_calendar_clicked(self, qdate: QDate):
        idx = self.sorted_dates.index(qdate) if qdate in self.sorted_dates else -1
        if idx >= 0:
            self.day_list.setCurrentRow(idx)

    def on_day_row_changed(self, row: int):
        if row < 0:
            return
        item = self.day_list.item(row)
        if not item:
            return
        qdate = item.data(Qt.ItemDataRole.UserRole)
        if not isinstance(qdate, QDate):
            return
        self.current_date = qdate
        bundle = self.bundles[qdate]

        # sync calendar selection
        if not self._syncing:
            self._syncing = True
            self.calendar.setSelectedDate(qdate)
            self._syncing = False

        self.day_header.setText(f"{qdate.toString('dd.MM.yyyy')} — {len(bundle.events)} событий")
        if bundle.summaries:
            self.day_summary.setText("Кратко: " + " | ".join(bundle.summaries[:2]))
        else:
            titles = [ev.title for ev in bundle.events[:3]]
            self.day_summary.setText("Кратко: " + (" | ".join(titles) if titles else "нет дополнительного описания."))

        self.events_list.clear()
        for ev in bundle.events:
            line = f"{ev.time_text or '--:--:--'}  |  {ev.title}  ({len(ev.photos)} фото)"
            lw = QListWidgetItem(line)
            lw.setData(Qt.ItemDataRole.UserRole, ev)
            self.events_list.addItem(lw)

        if self.events_list.count() > 0:
            self.events_list.setCurrentRow(0)
        else:
            self.detail.clear()

    def on_event_row_changed(self, row: int):
        if row < 0:
            self.detail.clear()
            return
        item = self.events_list.item(row)
        if not item:
            self.detail.clear()
            return
        ev = item.data(Qt.ItemDataRole.UserRole)
        if isinstance(ev, Event):
            self.detail.set_event(ev)


def count_csv_rows(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0
    with open(csv_path, encoding="utf-8-sig") as f:
        return sum(1 for _ in csv.DictReader(f))


def discover_best_data_dir(script_dir: str, explicit: Optional[str] = None) -> str:
    if explicit:
        return os.path.abspath(explicit)

    candidates = set()
    cwd = os.getcwd()
    for root in {cwd, script_dir}:
        candidates.add(root)
        try:
            for name in os.listdir(root):
                full = os.path.join(root, name)
                if os.path.isdir(full) and name.lower().startswith("chatexport"):
                    candidates.add(full)
        except Exception:
            pass

    best_dir = script_dir
    best_count = -1
    for d in candidates:
        csv_path = os.path.join(d, "news_output", "news_messages.csv")
        n = count_csv_rows(csv_path)
        if n > best_count:
            best_count = n
            best_dir = d
    return os.path.abspath(best_dir)


def discover_default_db(script_dir: str, explicit: Optional[str] = None) -> str:
    if explicit:
        return os.path.abspath(explicit)

    cwd = os.getcwd()
    candidates = []
    for path in [os.path.join(cwd, "news_unified.db"), os.path.join(script_dir, "news_unified.db")]:
        if path not in candidates:
            candidates.append(path)

    existing = [path for path in candidates if os.path.exists(path)]
    if not existing:
        return ""
    return max(existing, key=lambda p: os.path.getsize(p))


def main():
    parser = argparse.ArgumentParser(description="UI-календарь новостей из Telegram-выгрузки")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Папка, в которой лежит news_output/news_messages.csv",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Путь к unified SQLite БД (если не задан, ищется news_unified.db)",
    )
    args = parser.parse_args()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = discover_default_db(script_dir, args.db)
    backend: Optional[DataBackend] = None

    try:
        if db_path:
            backend = DataBackend(db_path=db_path)
        else:
            data_dir = discover_best_data_dir(script_dir, args.data_dir)
            backend = DataBackend(data_dir=data_dir)

        bundles = backend.load_bundles()
        if not bundles:
            raise RuntimeError("Новости не найдены: проверьте source CSV/DB.")

        app = QApplication(sys.argv)
        window = NewsCalendarWindow(bundles, backend)
        window.show()
        exit_code = app.exec()
        backend.close()
        sys.exit(exit_code)
    except Exception as exc:
        if backend:
            backend.close()
        print(f"[error] {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
