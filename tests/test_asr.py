"""Tests for media_pipeline/asr.py.

Mocks faster_whisper / torch so tests run without GPU model loading.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from tests.conftest import create_db


class FakeSegment:
    def __init__(self, start, end, text):
        self.start = start
        self.end = end
        self.text = text


class FakeInfo:
    language = "ru"


class FakeWhisperPipeline:
    def transcribe(self, audio_path, *, beam_size=5, batch_size=8, language="ru", vad_filter=True):
        segments = [
            FakeSegment(0.0, 2.0, "привет"),
            FakeSegment(2.5, 5.0, "мир"),
        ]
        return iter(segments), FakeInfo()


class TestASR(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "test.db"
        create_db(self.db_path)

    def test_get_pipeline_returns_none_when_import_fails(self):
        from media_pipeline.asr import get_pipeline
        with patch("builtins.__import__", side_effect=ImportError("no module")):
            result = get_pipeline({})
        self.assertIsNone(result)

    def test_transcribe_file_success(self):
        from media_pipeline.asr import transcribe_file
        pipeline = FakeWhisperPipeline()
        result = transcribe_file("fake.wav", settings={"whisper_model": "tiny"})
        # Without mocking get_pipeline it will try real import; so mock it
        # This test verifies the output shape when pipeline works
        # We test via direct mock injection

    def test_transcribe_file_with_mocked_pipeline(self):
        from media_pipeline.asr import transcribe_file
        fake_pipeline = FakeWhisperPipeline()
        with patch("media_pipeline.asr.get_pipeline", return_value=fake_pipeline):
            result = transcribe_file("fake.wav", settings={"whisper_model": "tiny"})
        self.assertIn("привет мир", result["text"])
        self.assertEqual(len(result["segments"]), 2)
        self.assertEqual(result["language"], "ru")

    def test_transcribe_file_returns_empty_when_pipeline_none(self):
        from media_pipeline.asr import transcribe_file
        with patch("media_pipeline.asr.get_pipeline", return_value=None):
            result = transcribe_file("fake.wav")
        self.assertEqual(result["text"], "")
        self.assertEqual(result["segments"], [])

    def test_process_untranscribed_videos_empty_db(self):
        from media_pipeline.asr import process_untranscribed_videos
        def _fake_get_db(_s=None):
            conn = sqlite3.connect(str(self.db_path))
            conn.row_factory = sqlite3.Row
            return conn
        with patch("media_pipeline.asr.get_db", _fake_get_db):
            result = process_untranscribed_videos()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
