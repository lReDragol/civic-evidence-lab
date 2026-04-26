import argparse
import html
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from collectors.official_scraper import HEADERS
from config.source_health import load_source_health_manifest, smoke_fixture

DEFAULT_REPORT = PROJECT_ROOT / "reports" / "source_health_latest.json"

SOURCE_PROBES = [
    {"source": "minjust_reestrs_api", "url": "https://reestrs.minjust.gov.ru/rest/registry/39b95df9-9a68-6b6d-e1e3-e6388507067e/info", "kind": "json", "verify": False},
    {"source": "minjust_inoagents_page", "url": "https://minjust.gov.ru/ru/pages/reestr-inostryannykh-agentov/", "kind": "html"},
    {"source": "duma_bills", "url": "https://sozd.duma.gov.ru/oz/b", "kind": "html"},
    {"source": "zakupki_contracts", "url": "https://zakupki.gov.ru/epz/contract/search/results.html", "kind": "html"},
    {"source": "gis_gkh", "url": "https://dom.gosuslugi.ru/", "kind": "html"},
    {"source": "government_news", "url": "https://government.ru/news/", "kind": "html"},
    {"source": "government_docs", "url": "https://government.ru/docs/", "kind": "html"},
    {"source": "publication_pravo_https", "url": "https://publication.pravo.gov.ru/", "kind": "html", "verify": False},
    {"source": "publication_pravo_http", "url": "http://publication.pravo.gov.ru/", "kind": "html"},
    {"source": "pravo_gov", "url": "https://pravo.gov.ru/", "kind": "html", "verify": False},
    {"source": "kremlin_transcripts", "url": "https://kremlin.ru/events/president/transcripts/", "kind": "html"},
    {"source": "kremlin_transcripts_special", "url": "https://special.kremlin.ru/events/president/transcripts", "kind": "html"},
    {"source": "rosreestr_press_archive", "url": "https://rosreestr.gov.ru/press/archive/", "kind": "html", "verify": False},
    {"source": "rosreestr_press_news", "url": "https://rosreestr.gov.ru/site/press/news/", "kind": "html", "verify": False},
    {"source": "rosreestr_lk_property", "url": "https://lk.rosreestr.ru/eservices/real-estate-objects-online", "kind": "html", "verify": False},
]


def _extract_title(text: str) -> str:
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(text, "lxml")
        if soup.title:
            return " ".join(soup.title.get_text(" ", strip=True).split())
    except Exception:
        pass
    match = re.search(r"<title[^>]*>(.*?)</title>", text, re.I | re.S)
    if not match:
        return ""
    return " ".join(html.unescape(re.sub(r"\s+", " ", match.group(1))).split())


def _count_links(text: str) -> int:
    try:
        from bs4 import BeautifulSoup

        return len(BeautifulSoup(text, "lxml").find_all("a"))
    except Exception:
        return len(re.findall(r"<a\b", text, re.I))


def probe_source(probe: Dict[str, object], timeout: int = 8) -> Dict[str, object]:
    started = time.perf_counter()
    url = str(probe["url"])
    verify = bool(probe.get("verify", True))
    if not verify:
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    result = {
        "source": probe["source"],
        "url": url,
        "ok": False,
        "status": None,
        "elapsed_sec": None,
        "final_url": "",
        "content_type": "",
        "length": 0,
        "title": "",
        "link_count": 0,
        "error": "",
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }
    try:
        resp = requests.get(
            url,
            headers=HEADERS,
            timeout=(min(4, timeout), timeout),
            verify=verify,
            allow_redirects=True,
        )
        result["status"] = resp.status_code
        result["elapsed_sec"] = round(time.perf_counter() - started, 3)
        result["final_url"] = resp.url
        result["content_type"] = resp.headers.get("content-type", "")
        result["length"] = len(resp.content or b"")
        result["ok"] = 200 <= resp.status_code < 400
        if "html" in result["content_type"].lower() or str(probe.get("kind")) == "html":
            result["title"] = _extract_title(resp.text[:500000])
            result["link_count"] = _count_links(resp.text[:500000])
    except Exception as exc:
        result["elapsed_sec"] = round(time.perf_counter() - started, 3)
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def check_sources(
    timeout: int = 8,
    probes: Optional[Iterable[Dict[str, object]]] = None,
    settings: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    settings_dict = dict(settings or {})
    manifest = load_source_health_manifest(settings_dict)
    items: List[Dict[str, object]] = []
    for probe in (probes or SOURCE_PROBES):
        item = probe_source(probe, timeout=timeout)
        if not item.get("ok"):
            item["fixture_smoke"] = smoke_fixture(
                str(probe.get("source") or item.get("source") or ""),
                settings=settings_dict,
                manifest=manifest,
            )
        items.append(item)
    ok_count = sum(1 for item in items if item["ok"])
    return {
        "checked_at": datetime.now().isoformat(timespec="seconds"),
        "total": len(items),
        "ok": ok_count,
        "failed": len(items) - ok_count,
        "items": items,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Check availability of official evidence sources")
    parser.add_argument("--timeout", type=int, default=8)
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    result = check_sources(timeout=args.timeout)
    output = json.dumps(result, ensure_ascii=False, indent=2)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
