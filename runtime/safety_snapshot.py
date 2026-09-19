"""Non-destructive, consistent SQLite backups before Civic migrations."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def snapshot(root: Path, destination: Path) -> dict:
    destination.mkdir(parents=True, exist_ok=False)
    manifest = {"started_at": datetime.now(timezone.utc).isoformat(), "files": []}
    for relative in ("db/news_unified.db", "config/settings.json", "config/secrets.json", "app.log", "log.log"):
        source = root / relative
        if not source.is_file():
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if source.suffix == ".db":
            with sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True) as src:
                with sqlite3.connect(target) as dst:
                    src.backup(dst, pages=4096)
                    check = dst.execute("PRAGMA quick_check").fetchone()[0]
                    if check != "ok":
                        raise RuntimeError("Backup integrity check failed")
        else:
            shutil.copy2(source, target)
        manifest["files"].append({"path": relative, "bytes": target.stat().st_size})
    sessions = root / "config/telegram_test_sessions"
    if sessions.exists():
        for source in sessions.glob("*.session"):
            target = destination / "sessions" / source.name
            target.parent.mkdir(exist_ok=True)
            with sqlite3.connect(source.resolve().as_uri() + "?mode=ro", uri=True) as src:
                with sqlite3.connect(target) as dst:
                    src.backup(dst)
            manifest["files"].append({"path": "sessions/" + source.name, "bytes": target.stat().st_size})
    for name, args in (("git-status.txt", ["status", "--short", "--branch"]), ("worktree.patch", ["diff", "HEAD", "--binary"])):
        output = subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True).stdout
        (destination / name).write_bytes(output)
    # Include untracked source files: git diff alone does not preserve them.
    listing = subprocess.run(["git", "-C", str(root), "ls-files", "--others", "--exclude-standard", "-z"], check=True, capture_output=True).stdout
    for raw in listing.split(b"\0"):
        if not raw:
            continue
        relative = raw.decode("utf-8")
        source = root / relative
        if source.suffix.lower() not in {".py", ".sql", ".js", ".css", ".html", ".md"}:
            continue
        target = destination / "untracked" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return {"destination": str(destination), "files": len(manifest["files"]), "ok": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(snapshot(Path(__file__).resolve().parents[1], args.destination)))
