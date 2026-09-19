"""Profile-driven single-instance collection with durable bounds and live reports.

Never reads legacy settings/secrets, discovers credentials or publishes claims.
HTTP sources are explicit: remote content cannot extend the collection frontier.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid
from urllib.parse import urlsplit

from db.reactor import bootstrap_reactor_databases, open_reactor_db
from runtime.civic import register_capture, dispatch_observations, index_observations, accept_extractions, worker_once

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROFILE = ROOT / "config/civic_collection.json"
LOG = logging.getLogger(__name__)


def load_profile(path):
    path = Path(path).resolve()
    if path.stat().st_size > 65536:
        raise ValueError("profile_size_limit")
    profile = json.loads(path.read_text(encoding="utf-8"))
    if profile.get("version") != 1 or not isinstance(profile.get("profile_id"), str) or not profile["profile_id"]:
        raise ValueError("invalid_profile")
    for name in ("db_dir", "archive_root", "report_dir"):
        value = Path(profile[name])
        if not value.is_absolute():
            value = path.parent / value
        profile[name] = str(value.resolve())
    bounds = {"max_http_requests_24h": (1, 10000), "max_model_requests_24h": (0, 10000),
              "max_pending_tasks": (1, 100000), "report_interval_seconds": (10, 3600),
              "archive_max_bytes": (1, 10**15), "archive_reserve_bytes": (0, 10**15)}
    for key, (minimum, maximum) in bounds.items():
        value = profile[key]
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError("invalid_limit:" + key)
    keywords = profile.get("keywords", [])
    if not isinstance(keywords, list) or len(keywords) > 200 or any(not isinstance(k, str) or not 1 <= len(k) <= 100 for k in keywords):
        raise ValueError("invalid_keywords")
    modules = profile.get("domain_modules", [])
    if not isinstance(modules, list) or any(m not in {"elections"} for m in modules):
        raise ValueError("unknown_domain_module")
    sources = profile.get("sources")
    if not isinstance(sources, list) or not 1 <= len(sources) <= 100:
        raise ValueError("invalid_sources")
    seen = set()
    for source in sources:
        parsed = urlsplit(source["url"])
        hosts = source["allowed_hosts"]
        if parsed.scheme not in {"http", "https"} or parsed.username or parsed.password or parsed.hostname not in hosts:
            raise ValueError("unapproved_source")
        if not isinstance(hosts, list) or not 1 <= len(hosts) <= 10 or any(not isinstance(h, str) for h in hosts):
            raise ValueError("invalid_hosts")
        interval = source["interval_seconds"]
        if type(interval) is not int or not 30 <= interval <= 86400:
            raise ValueError("invalid_source_interval")
        if source["url"] in seen:
            raise ValueError("duplicate_source")
        seen.add(source["url"])
    gateway = profile.get("gateway_config")
    if gateway:
        profile["gateway_config"] = str((path.parent / gateway).resolve()) if not Path(gateway).is_absolute() else gateway
    profile["_path"] = str(path)
    return profile


@contextmanager
def writer_lock(db_dir):
    directory = Path(db_dir)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".civic-writer.lock").open("a+b") as stream:
        acquired = False
        try:
            if os.name == "nt":
                import msvcrt
                stream.seek(0)
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except OSError:
            raise RuntimeError("collection_already_running") from None
        try:
            yield
        finally:
            if acquired:
                if os.name == "nt":
                    stream.seek(0)
                    msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _ops(profile, readonly=False):
    return open_reactor_db("ops", settings={"reactor_db_dir": profile["db_dir"]}, readonly=readonly)


def _counts(knowledge):
    return {table: knowledge.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
            for table in ("source_objects", "source_revisions", "source_observations", "civic_claims", "blobs")}


def _event(ops, run_id, kind, status, payload=None, source=None):
    with ops:
        ops.execute("INSERT INTO collection_events(run_id,created_at,kind,source_key,status,payload_json) VALUES(?,?,?,?,?,?)",
                    (run_id, time.time(), kind, source, status, json.dumps(payload or {}, ensure_ascii=False)))
    LOG.info("collection run=%s kind=%s source=%s status=%s", run_id, kind, source or "", status)


def _allow_request(ops, run_id, kind, cap, source=None):
    # Reserve BEFORE dispatch; process death and network uncertainty spend a slot.
    with ops:
        ops.execute("BEGIN IMMEDIATE")
        count = ops.execute("SELECT COUNT(*) FROM collection_events WHERE kind=? AND created_at>=?", (kind, time.time()-86400)).fetchone()[0]
        if count >= cap:
            return False
        ops.execute("INSERT INTO collection_events(run_id,created_at,kind,source_key,status,payload_json) VALUES(?,?,?,?, 'reserved','{}')",
                    (run_id, time.time(), kind, source))
    return True


def collection_status(profile_path=DEFAULT_PROFILE):
    profile = load_profile(profile_path)
    result = {"profile_id": profile["profile_id"], "title": profile.get("title", profile["profile_id"]),
              "domain_modules": profile.get("domain_modules", []),
              "status": "stopped", "running": False, "model_state": "not_configured", "publication_allowed": False}
    try:
        conn = _ops(profile, readonly=True)
        try:
            row = conn.execute("SELECT * FROM collection_runs ORDER BY started_at DESC LIMIT 1").fetchone()
            if row:
                result.update(dict(row))
                age = max(0, time.time()-row["heartbeat_at"])
                result["heartbeat_age_seconds"] = round(age, 1)
                result["running"] = row["status"] == "running" and age < 180
                if row["status"] == "running" and not result["running"]:
                    result["status"] = "stale"
                result.pop("baseline_json", None)
        finally:
            conn.close()
    except (sqlite3.Error, OSError) as exc:
        result.update(status="unavailable", error_type=type(exc).__name__)
    return result


def _atomic_text(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".civic-report-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def export_collection_report(profile_path=DEFAULT_PROFILE, *, _profile=None):
    profile = _profile or load_profile(profile_path)
    ops = _ops(profile, readonly=True)
    knowledge = open_reactor_db("knowledge", settings={"reactor_db_dir": profile["db_dir"]}, readonly=True)
    try:
        # Read snapshots do not request the collector's writer lock.
        ops.execute("BEGIN")
        row = ops.execute("SELECT * FROM collection_runs ORDER BY started_at DESC LIMIT 1").fetchone()
        if not row:
            return {"status": "no_runs", "profile_id": profile["profile_id"]}
        run = dict(row)
        aggregate = [dict(r) for r in ops.execute("SELECT kind,status,COUNT(*) AS count FROM collection_events WHERE run_id=? GROUP BY kind,status", (run["run_id"],))]
        errors = [dict(r) for r in ops.execute("SELECT created_at,kind,source_key,status,payload_json FROM collection_events WHERE run_id=? AND status NOT IN ('captured','completed','reserved','unchanged','idle','started','ok') ORDER BY id DESC LIMIT 100", (run["run_id"],))]
        sources = [dict(r) for r in ops.execute("SELECT * FROM collection_sources WHERE profile_id=? ORDER BY source_key LIMIT 100", (profile["profile_id"],))]
        tasks = dict(ops.execute("SELECT status,COUNT(*) FROM agent_tasks GROUP BY status").fetchall())
        now_counts = _counts(knowledge)
        baseline = json.loads(run.pop("baseline_json"))
        finished = run["finished_at"] or time.time()
        report = {"generated_at": datetime.now(timezone.utc).isoformat(), "profile_id": run["profile_id"], "run": run,
                  "duration_seconds": round(finished-run["started_at"], 1), "is_final": run["status"] != "running",
                  "run_delta": {k: now_counts[k]-baseline.get(k, 0) for k in now_counts}, "stored_totals": now_counts,
                  "activity": aggregate, "sources": sources, "task_states_all_runs": tasks, "recent_errors": errors,
                  "publication_allowed": False,
                  "limitations": ["Sequential ops/knowledge snapshots, not one cross-database transaction.",
                      "Captures are source observations, not counted incidents, precincts or votes.",
                      "Model work requires an approved live gateway; queue growth is not analysis success.",
                      "Only explicitly configured sources are visited; no recursive discovery."]}
    finally:
        ops.close()
        knowledge.close()
    destination = Path(profile["report_dir"])
    # Concurrent readers get immutable snapshots; an older export can never
    # overwrite a newer/final report after the collector finishes.
    stem = "collection-" + run["run_id"] + "-" + str(time.time_ns())
    json_path, markdown_path = destination/(stem+".json"), destination/(stem+".md")
    _atomic_text(json_path, json.dumps(report, ensure_ascii=False, indent=2))
    lines = ["# Civic collection report", "", f"Profile: {run['profile_id']}", f"Run: {run['run_id']}",
             f"Status: {run['status']}; duration: {report['duration_seconds']} seconds", f"Models: {run['model_state']}",
             f"Started UTC: {datetime.fromtimestamp(run['started_at'], timezone.utc).isoformat()}",
             f"Finished UTC: {datetime.fromtimestamp(run['finished_at'], timezone.utc).isoformat() if run['finished_at'] else 'running'}",
             "", "## Stored during run", *[f"- {key}: {value}" for key,value in report['run_delta'].items()],
             "", "## Activity", *[f"- {r['kind']} / {r['status']}: {r['count']}" for r in aggregate],
             "", "## Sources", *[f"- {r['source_key']}: {r['status']}; failures={r['failures']}; error={r['last_error']}" for r in sources],
             "", "## Limits", *["- "+s for s in report['limitations']], "", "Publication: blocked.", ""]
    _atomic_text(markdown_path, "\n".join(lines))
    return {"status": "exported", "run_id": run["run_id"], "is_final": report["is_final"],
            "json_path": str(json_path), "markdown_path": str(markdown_path), "report": report}


def request_stop(profile_path=DEFAULT_PROFILE):
    profile = load_profile(profile_path)
    status = collection_status(profile_path)
    if status.get("run_id") and status.get("running"):
        _atomic_text(Path(profile["db_dir"])/"civic-stop.json", json.dumps({"run_id": status["run_id"]}))
        return {"status": "stop_requested", "run_id": status["run_id"]}
    return status


def launch_collection(profile_path=DEFAULT_PROFILE):
    profile_path = Path(profile_path).resolve()
    profile = load_profile(profile_path)
    status = collection_status(profile_path)
    if status.get("running"):
        return {**status, "already_running": True}
    Path(profile["report_dir"]).mkdir(parents=True, exist_ok=True)
    log = Path(profile["report_dir"])/"launcher.log"
    with log.open("ab") as stream:
        process = subprocess.Popen([sys.executable, "-m", "runtime.civic_service", "--profile", str(profile_path), "--run"],
            cwd=ROOT, stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0, start_new_session=os.name != "nt")
    return {"status": "starting", "pid": process.pid, "profile_id": profile["profile_id"]}


def _capture_source(source, profile):
    from collectors.civic_capture import capture_http
    from collectors.evidence_archive import ArchivedFile, EvidenceArchive
    archive = EvidenceArchive({"evidence_archive_root":profile["archive_root"],
        "evidence_archive_max_bytes":profile["archive_max_bytes"], "evidence_archive_reserve_bytes":profile["archive_reserve_bytes"]})
    payload = capture_http(source["url"], allowed_hosts=source["allowed_hosts"], archive=archive, timeout=30)
    original = ArchivedFile(Path(payload["storage_path"]),payload["sha256"],payload["byte_size"]) if payload["storage_path"] else None
    if original and payload["status"] == "captured":
        media = payload.get("media_type", "")
        if "html" in media:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(original.path.read_bytes(), "html.parser")
            for element in soup(["script", "style", "noscript"]):
                element.decompose()
            payload["text"] = soup.get_text("\n", strip=True)[:262144]
        elif media.startswith("text/"):
            payload["text"] = original.path.read_text(encoding="utf-8", errors="replace")[:262144]
    text = payload.get("text", "").casefold()
    matched = [keyword for keyword in profile.get("keywords", []) if keyword.casefold() in text]
    payload["analysis_eligible"] = bool(text) and (not profile.get("keywords") or bool(matched))
    payload["selection"] = {"profile_id":profile["profile_id"], "matched_keywords":matched,
        "reason":"matched_profile" if payload["analysis_eligible"] else "no_profile_match_or_text"}
    return payload, original


def _restore_route_holds(ops, routes):
    now = time.time()
    holds = ops.execute("SELECT * FROM civic_model_holds WHERE blocked=1 OR until_at>?",(now,)).fetchall()
    for hold in holds:
        for route in routes.snapshot.routes:
            if route.provider == hold['provider'] and (not hold['account_id'] or route.account_id == hold['account_id']):
                routes.defer(route.route_id,scope='account' if hold['account_id'] else 'provider',
                    retry_after_seconds=max(0,hold['until_at']-now),blocked=bool(hold['blocked']))


def _save_route_failure(ops, route, result):
    error = result.get('error')
    if not error:
        return
    code = error.get('code') if isinstance(error,dict) else str(error)
    delay = error.get('retry_after_seconds',60) if isinstance(error,dict) else 60
    if type(delay) not in (int,float) or not math.isfinite(delay) or delay < 0:
        delay = 60
    provider_wide = code in {'rate_limited','account_backoff','provider_quota','provider_auth',
                             'provider_unavailable','gateway_unavailable','gateway_busy'}
    blocked = code in {'quota_exceeded','provider_quota','provider_auth','secret_unavailable'}
    with ops:
        ops.execute("""INSERT INTO civic_model_holds(provider,account_id,until_at,blocked,reason,updated_at) VALUES(?,?,?,?,?,?)
            ON CONFLICT(provider,account_id) DO UPDATE SET until_at=MAX(until_at,excluded.until_at),
            blocked=MAX(blocked,excluded.blocked),reason=excluded.reason,updated_at=excluded.updated_at""",
            (route.provider,'' if provider_wide else route.account_id,time.time()+max(60,delay),int(blocked),code,time.time()))


def _model_once(ops, knowledge, profile, run_id, routes):
    path = profile.get("gateway_config")
    if not path:
        return "not_configured"
    try:
        from integrations.fcm_gateway import GatewayError
        routes.reload(path)
        _restore_route_holds(ops,routes)
        route = routes.select('extract',max_input_tokens=8192,max_output_tokens=256)
        if not ops.execute("SELECT 1 FROM agent_tasks WHERE target_group='collection_structure' AND status IN ('pending','needs_retry') LIMIT 1").fetchone():
            return "idle"
        if not _allow_request(ops,run_id,"model_request",profile["max_model_requests_24h"]):
            return "daily_budget_exhausted"
        result = worker_once(ops,knowledge,client=routes,snapshot=routes.snapshot,route_id=route.route_id,max_cost_microusd=0)
        _save_route_failure(ops,route,result)
        _event(ops,run_id,"model_result",result["status"],{**result,"provider":route.provider,"model":route.model,"account_id":route.account_id})
        accept_extractions(ops,knowledge)
        return result["status"]
    except Exception as exc:
        from integrations.fcm_gateway import GatewayError
        return exc.code if isinstance(exc,GatewayError) else "gateway_unavailable:" + type(exc).__name__


def run_collection(profile_path=DEFAULT_PROFILE, *, max_seconds=None, capture=None):
    profile = load_profile(profile_path)
    if max_seconds is not None and (not math.isfinite(max_seconds) or max_seconds <= 0):
        raise ValueError("invalid_duration")
    from config.db_utils import setup_logging
    setup_logging({"log_level":"INFO"})
    run_id = uuid.uuid4().hex
    stop = False
    def stop_signal(*_):
        nonlocal stop
        stop = True
    old_signals = {}
    if __import__("threading").current_thread() is __import__("threading").main_thread():
        for signum in (signal.SIGINT, signal.SIGTERM):
            old_signals[signum] = signal.signal(signum,stop_signal)
    try:
        with writer_lock(profile["db_dir"]):
            bootstrap_reactor_databases({"reactor_db_dir":profile["db_dir"]})
            ops = _ops(profile)
            knowledge = open_reactor_db("knowledge", settings={"reactor_db_dir":profile["db_dir"]})
            search = open_reactor_db("search", settings={"reactor_db_dir":profile["db_dir"]})
            try:
                started = time.time()
                deadline = time.monotonic()+max_seconds if max_seconds else math.inf
                with ops:
                    ops.execute("UPDATE collection_runs SET status='abandoned',finished_at=? WHERE status='running'",(started,))
                    ops.execute("INSERT INTO collection_runs(run_id,profile_id,profile_hash,status,pid,started_at,heartbeat_at,baseline_json) VALUES(?,?,?,'running',?,?,?,?)",
                        (run_id,profile["profile_id"],hashlib.sha256(json.dumps(profile,sort_keys=True).encode()).hexdigest(),os.getpid(),started,started,json.dumps(_counts(knowledge))))
                    for source in profile["sources"]:
                        ops.execute("INSERT OR IGNORE INTO collection_sources(profile_id,source_key) VALUES(?,?)",(profile["profile_id"],source["url"]))
                _event(ops,run_id,"lifecycle","started")
                last_report = 0
                next_model = 0
                from integrations.civic_routes import CivicRoutes
                routes = CivicRoutes(max_attempts=10000)
                while not stop and time.monotonic() < deadline:
                    now = time.time()
                    with ops:
                        ops.execute("UPDATE collection_runs SET heartbeat_at=? WHERE run_id=?",(now,run_id))
                    stop_path = Path(profile["db_dir"])/"civic-stop.json"
                    if stop_path.exists():
                        try:
                            if json.loads(stop_path.read_text(encoding="utf-8")).get("run_id") == run_id:
                                break
                        except (ValueError,OSError):
                            pass
                    pending = ops.execute("SELECT COUNT(*) FROM agent_tasks WHERE status IN ('pending','needs_retry','running')").fetchone()[0]
                    for source in profile["sources"]:
                        if stop or time.monotonic() >= deadline:
                            break
                        state = ops.execute("SELECT * FROM collection_sources WHERE profile_id=? AND source_key=?",(profile["profile_id"],source["url"])).fetchone()
                        if state["next_due"] > now:
                            continue
                        if pending >= profile["max_pending_tasks"]:
                            with ops:
                                ops.execute("UPDATE collection_sources SET status='backpressure',next_due=? WHERE profile_id=? AND source_key=?",(now+60,profile["profile_id"],source["url"]))
                            continue
                        if not _allow_request(ops,run_id,"http_request",profile["max_http_requests_24h"],source["url"]):
                            with ops:
                                ops.execute("UPDATE collection_sources SET status='daily_budget_exhausted',next_due=? WHERE profile_id=? AND source_key=?",(now+300,profile["profile_id"],source["url"]))
                            continue
                        begin = time.monotonic()
                        try:
                            payload,original = (capture or _capture_source)(source,profile)
                        except Exception as exc:
                            payload,original = {"status":"capture_failed","error_type":type(exc).__name__,"source_url":source["url"]},None
                        revision = register_capture(knowledge,source_url=source["url"],payload=payload,original=original,media_type=payload.get("media_type"))
                        success = payload["status"] == "captured"
                        failures = 0 if success else state["failures"]+1
                        delay = source["interval_seconds"] if success else min(86400,max(source["interval_seconds"],60*2**min(failures,10)))
                        if payload["status"] == "needs_user_access":
                            delay = max(delay,86400)
                        status = payload["status"] if revision["observation_created"] or not success else "unchanged"
                        _event(ops,run_id,"capture",status,{"revision_id":revision["revision_id"],"new_observation":revision["observation_created"],
                            "analysis_eligible":payload.get("analysis_eligible",False),"error_type":payload.get("error_type"),
                            "latency_ms":round((time.monotonic()-begin)*1000)},source["url"])
                        with ops:
                            ops.execute("UPDATE collection_sources SET next_due=?,failures=?,status=?,last_success=CASE WHEN ? THEN ? ELSE last_success END,last_error=?,last_run_id=? WHERE profile_id=? AND source_key=?",
                                (time.time()+delay,failures,status,success,time.time(),payload.get("error_type"),run_id,profile["profile_id"],source["url"]))
                        dispatch_observations(knowledge,ops)
                        index_observations(knowledge,search)
                        pending += int(revision["observation_created"])
                    if time.time() >= next_model:
                        model_state = _model_once(ops,knowledge,profile,run_id,routes)
                        with ops:
                            ops.execute("UPDATE collection_runs SET model_state=?,heartbeat_at=? WHERE run_id=?",(model_state,time.time(),run_id))
                        next_model = time.time()+(5 if model_state == "completed" else 60)
                    if time.time()-last_report >= profile["report_interval_seconds"]:
                        try:
                            export_collection_report(profile_path, _profile=profile)
                        except (OSError, sqlite3.Error) as exc:
                            _event(ops,run_id,"report","failed",{"error_type":type(exc).__name__})
                        last_report = time.time()
                    time.sleep(min(1,max(0,deadline-time.monotonic())))
                with ops:
                    ops.execute("UPDATE collection_runs SET status='stopped',finished_at=?,heartbeat_at=? WHERE run_id=?",(time.time(),time.time(),run_id))
                _event(ops,run_id,"lifecycle","stopped")
            except BaseException as exc:
                with ops:
                    ops.execute("UPDATE collection_runs SET status='failed',finished_at=?,error_type=? WHERE run_id=?",(time.time(),type(exc).__name__,run_id))
                raise
            finally:
                ops.close()
                knowledge.close()
                search.close()
    finally:
        for signum,handler in old_signals.items():
            signal.signal(signum,handler)
    return export_collection_report(profile_path, _profile=profile)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile",type=Path,default=DEFAULT_PROFILE)
    actions = parser.add_mutually_exclusive_group(required=True)
    for flag in ("run","start","status","stop","report"):
        actions.add_argument("--"+flag,action="store_true")
    parser.add_argument("--duration-seconds",type=float)
    args = parser.parse_args()
    if args.run:
        result = run_collection(args.profile,max_seconds=args.duration_seconds)
    elif args.start:
        result = launch_collection(args.profile)
    elif args.stop:
        result = request_stop(args.profile)
    elif args.report:
        result = export_collection_report(args.profile)
    else:
        result = collection_status(args.profile)
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
