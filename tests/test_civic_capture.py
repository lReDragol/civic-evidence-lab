import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlsplit
from collectors.civic_capture import _fetch_once
from collectors.civic_capture import CaptureBlocked, resolve_public_url, capture_http
from collectors.evidence_archive import EvidenceArchive


class CivicCaptureTests(unittest.TestCase):
    def test_connection_close_body_is_read_before_socket_close(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Connection","close")
                self.send_header("Content-Length","5")
                self.end_headers()
                self.wfile.write(b"hello")
            def log_message(self,*args):
                pass
        server=HTTPServer(("127.0.0.1",0),Handler)
        thread=threading.Thread(target=server.handle_request)
        thread.start()
        try:
            with patch("collectors.civic_capture.resolve_public_url",return_value=(urlsplit("http://example.org/"),"example.org",server.server_port,"127.0.0.1")):
                result=_fetch_once("http://example.org/",{"example.org"},time.monotonic()+5,100)
            self.assertEqual(result[3],b"hello")
        finally:
            thread.join(5)
            server.server_close()

    def test_private_and_credentials_and_unapproved_hosts_blocked(self):
        for url in ("file:///C:/secret", "http://user:secret@example.org/", "https://evil.example/", "https://example.org:9000/"):
            with self.assertRaises(CaptureBlocked):
                resolve_public_url(url,{"example.org"})
        with patch("socket.getaddrinfo",return_value=[(2,1,6,"",("127.0.0.1",443))]):
            with self.assertRaises(CaptureBlocked):
                resolve_public_url("https://example.org/",{"example.org"})

    def test_capture_creates_warc_and_does_not_bypass_access(self):
        from warcio.archiveiterator import ArchiveIterator
        with tempfile.TemporaryDirectory() as tmp:
            archive=EvidenceArchive({"evidence_archive_root":tmp,"evidence_archive_reserve_bytes":0})
            with patch("collectors.civic_capture._fetch_once",return_value=(403,"Forbidden",[("Content-Type","text/html")],b"captcha")):
                result=capture_http("https://example.org/",allowed_hosts={"example.org"},archive=archive)
            self.assertEqual(result["status"],"needs_user_access")
            self.assertFalse(result["publication_allowed"])
            with Path(result["warc_path"]).open("rb") as stream:
                records=list(ArchiveIterator(stream))
                self.assertEqual(len(records),1)
