from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.db_utils import PROJECT_ROOT, get_db, load_settings

log = logging.getLogger(__name__)


def _json_loads(value: str | None, default: Any = None) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _tsort(d: dict, reverse: bool = True) -> dict:
    return dict(sorted(d.items(), key=lambda x: x[1], reverse=reverse))


def generate_report(settings: dict[str, Any] | None = None, output_dir: Path | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    output_dir = output_dir or Path(settings.get("report_output_dir", str(PROJECT_ROOT / "reports")))
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = get_db(settings)
    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sections": {},
    }

    # === 1. TEST DURATION ===
    try:
        first_metric = conn.execute("SELECT MIN(collected_at) FROM autonomy_test_metrics").fetchone()[0]
        last_metric = conn.execute("SELECT MAX(collected_at) FROM autonomy_test_metrics").fetchone()[0]
        metrics_count = conn.execute("SELECT COUNT(*) FROM autonomy_test_metrics").fetchone()[0]
        snapshots = conn.execute("SELECT COUNT(DISTINCT collected_at) FROM autonomy_test_metrics").fetchone()[0]
        report["test_duration"] = {
            "first_metric_at": first_metric,
            "last_metric_at": last_metric,
            "total_metric_rows": metrics_count,
            "snapshot_count": snapshots,
        }
    except Exception:
        report["test_duration"] = {"error": "no metrics data"}

    # === 2. SOURCES ===
    src_summary = {}
    try:
        for row in conn.execute(
            "SELECT source_key, state, quality_state, consecutive_failures, last_error, last_success_at, last_attempt_at "
            "FROM source_sync_state ORDER BY source_key"
        ).fetchall():
            src_summary[row[0]] = {
                "state": row[1],
                "quality_state": row[2],
                "consecutive_failures": row[3],
                "last_error": row[4],
                "last_success_at": row[5],
                "last_attempt_at": row[6],
            }
    except Exception as e:
        src_summary = {"error": str(e)}
    report["sections"]["sources"] = src_summary

    # === 3. CONTENT ===
    try:
        total = conn.execute("SELECT COUNT(*) FROM content_items").fetchone()[0]
        by_status = dict(conn.execute("SELECT COALESCE(status,'active'), COUNT(*) FROM content_items GROUP BY 1").fetchall())
        by_type = dict(conn.execute("SELECT COALESCE(content_type,'unknown'), COUNT(*) FROM content_items GROUP BY 1").fetchall())
        suppressed = sum(v for k, v in by_status.items() if str(k).startswith("suppressed"))
        active = total - suppressed
        llm_done = conn.execute("SELECT COUNT(*) FROM content_items WHERE llm_processed=1").fetchone()[0]
        ner_done = conn.execute("SELECT COUNT(*) FROM content_items WHERE ner_processed=1").fetchone()[0]
        claims_done = conn.execute("SELECT COUNT(*) FROM content_items WHERE claims_processed=1").fetchone()[0]
        garbage_checked = conn.execute("SELECT COUNT(*) FROM content_items WHERE garbage_checked=1").fetchone()[0]

        content_timeline = []
        try:
            rows = conn.execute(
                "SELECT collected_at, metric_value_json FROM autonomy_test_metrics "
                "WHERE metric_group='content' AND metric_key='summary' ORDER BY collected_at"
            ).fetchall()
            for r in rows:
                val = _json_loads(r[1], {})
                content_timeline.append({"at": r[0], "total": val.get("total", 0), "llm_unprocessed": val.get("llm_unprocessed", 0), "ner_unprocessed": val.get("ner_unprocessed", 0)})
        except Exception:
            pass

        report["sections"]["content"] = {
            "total": total, "active": active, "suppressed": suppressed,
            "by_status": _tsort(by_status),
            "by_type": _tsort(by_type),
            "pipeline_progress": {
                "llm_processed": llm_done, "llm_pct": round(100 * llm_done / max(total, 1), 1),
                "ner_processed": ner_done, "ner_pct": round(100 * ner_done / max(total, 1), 1),
                "claims_processed": claims_done, "claims_pct": round(100 * claims_done / max(total, 1), 1),
                "garbage_checked": garbage_checked, "garbage_pct": round(100 * garbage_checked / max(total, 1), 1),
            },
            "timeline": content_timeline,
        }
    except Exception as e:
        report["sections"]["content"] = {"error": str(e)}

    # === 4. ENTITIES & RELATIONS ===
    try:
        entity_total = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        entity_by_type = dict(conn.execute("SELECT entity_type, COUNT(*) FROM entities GROUP BY 1 ORDER BY 2 DESC").fetchall())
        relations_total = conn.execute("SELECT COUNT(*) FROM entity_relations").fetchone()[0]
        relations_by_type = dict(conn.execute("SELECT relation_type, COUNT(*) FROM entity_relations GROUP BY 1 ORDER BY 2 DESC LIMIT 25").fetchall())
        mentions_total = conn.execute("SELECT COUNT(*) FROM entity_mentions").fetchone()[0]
        new_from_ner = 0
        try:
            new_from_ner = conn.execute("SELECT COUNT(*) FROM ner_new_entities").fetchone()[0]
            ner_searched = conn.execute("SELECT COUNT(*) FROM ner_new_entities WHERE search_triggered=1").fetchone()[0]
        except Exception:
            ner_searched = 0

        entity_timeline = []
        try:
            rows = conn.execute(
                "SELECT collected_at, metric_value_json FROM autonomy_test_metrics "
                "WHERE metric_group='entities' AND metric_key='summary' ORDER BY collected_at"
            ).fetchall()
            for r in rows:
                val = _json_loads(r[1], {})
                entity_timeline.append({"at": r[0], "total": val.get("total", 0), "relations_total": val.get("relations_total", 0)})
        except Exception:
            pass

        report["sections"]["entities"] = {
            "total": entity_total, "by_type": _tsort(entity_by_type),
            "relations_total": relations_total, "relations_by_type": _tsort(relations_by_type),
            "mentions_total": mentions_total,
            "ner_new_entities": new_from_ner, "ner_searched": ner_searched,
            "timeline": entity_timeline,
        }
    except Exception as e:
        report["sections"]["entities"] = {"error": str(e)}

    # === 5. CLAIMS & VERIFICATION ===
    try:
        claims_total = conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        claims_by_status = dict(conn.execute("SELECT status, COUNT(*) FROM claims GROUP BY 1").fetchall())
        claims_by_type = dict(conn.execute("SELECT claim_type, COUNT(*) FROM claims GROUP BY 1 ORDER BY 2 DESC LIMIT 15").fetchall())
        evidence_total = conn.execute("SELECT COUNT(*) FROM claim_evidence").fetchone()[0] if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='claim_evidence'").fetchone() else 0
        contradictions = 0
        try:
            contradictions = conn.execute("SELECT COUNT(*) FROM claim_contradictions").fetchone()[0]
        except Exception:
            pass
        confirmed = claims_by_status.get("confirmed", 0)
        partially = claims_by_status.get("partially_confirmed", 0)
        unverified = claims_by_status.get("unverified", 0)
        report["sections"]["claims"] = {
            "total": claims_total,
            "by_status": claims_by_status, "by_type": _tsort(claims_by_type),
            "evidence_links": evidence_total,
            "confirmed": confirmed, "partially_confirmed": partially, "unverified": unverified,
            "confirmed_pct": round(100 * (confirmed + partially) / max(claims_total, 1), 1),
            "contradictions": contradictions,
        }
    except Exception as e:
        report["sections"]["claims"] = {"error": str(e)}

    # === 6. LLM PROVIDERS & MODELS ===
    try:
        from llm.key_pool import list_active_keys
        active_keys = list_active_keys(conn)
        by_provider: dict[str, int] = {}
        by_provider_model: dict[str, dict] = {}
        for k in active_keys:
            p = k["provider"]
            by_provider[p] = by_provider.get(p, 0) + 1
            pm = f"{p}/{k.get('model_name', '?')}"
            if pm not in by_provider_model:
                by_provider_model[pm] = {"count": 0, "tier": k.get("capability_tier", 0), "stage_roles": k.get("stage_roles", [])}
            by_provider_model[pm]["count"] += 1

        key_failures = {}
        try:
            for r in conn.execute("SELECT provider, failure_kind, COUNT(*) FROM llm_key_failures GROUP BY provider, failure_kind ORDER BY COUNT(*) DESC"):
                pk = f"{r[0]}:{r[1]}"
                key_failures[pk] = r[2]
        except Exception:
            pass

        llm_attempts = {}
        try:
            for r in conn.execute(
                "SELECT provider, model_name, status, COUNT(*) FROM ai_task_attempts GROUP BY provider, model_name, status ORDER BY COUNT(*) DESC"
            ).fetchall():
                k = f"{r[0]}/{r[1]}"
                if k not in llm_attempts:
                    llm_attempts[k] = {"ok": 0, "failed": 0}
                if r[2] == "ok":
                    llm_attempts[k]["ok"] = r[3]
                else:
                    llm_attempts[k]["failed"] = r[3]
        except Exception:
            pass

        key_timeline = []
        try:
            rows = conn.execute(
                "SELECT collected_at, metric_value_json FROM autonomy_test_metrics "
                "WHERE metric_group='llm' AND metric_key='keys' ORDER BY collected_at"
            ).fetchall()
            for r in rows:
                val = _json_loads(r[1], {})
                key_timeline.append({"at": r[0], "active_total": val.get("active_total", 0), "by_provider": val.get("by_provider", {})})
        except Exception:
            pass

        report["sections"]["llm"] = {
            "active_key_count": len(active_keys),
            "by_provider": _tsort(by_provider),
            "by_provider_model": {k: v for k, v in sorted(by_provider_model.items(), key=lambda x: x[1]["count"], reverse=True)},
            "key_failures": key_failures,
            "task_attempts_by_model": llm_attempts,
            "timeline": key_timeline,
        }
    except Exception as e:
        report["sections"]["llm"] = {"error": str(e)}

    # === 7. JOB RUNS ===
    try:
        all_runs = {}
        for r in conn.execute(
            "SELECT job_id, status, COUNT(*) as cnt, "
            "SUM(items_new) as new, SUM(items_updated) as upd, SUM(items_seen) as seen, "
            "MIN(started_at) as first_at, MAX(started_at) as last_at "
            "FROM job_runs GROUP BY job_id, status ORDER BY job_id"
        ).fetchall():
            jid = r[0]
            if jid not in all_runs:
                all_runs[jid] = {"ok": 0, "failed": 0, "items_new": 0, "items_updated": 0, "items_seen": 0, "first_at": None, "last_at": None}
            status = r[1]
            cnt = r[2]
            if status == "ok":
                all_runs[jid]["ok"] = cnt
            else:
                all_runs[jid]["failed"] = cnt
            all_runs[jid]["items_new"] += int(r[3] or 0)
            all_runs[jid]["items_updated"] += int(r[4] or 0)
            all_runs[jid]["items_seen"] += int(r[5] or 0)
            all_runs[jid]["first_at"] = all_runs[jid]["first_at"] or r[6]
            all_runs[jid]["last_at"] = r[7]

        pipeline_runs = {}
        for r in conn.execute(
            "SELECT mode, status, COUNT(*) FROM pipeline_runs GROUP BY mode, status"
        ).fetchall():
            if r[0] not in pipeline_runs:
                pipeline_runs[r[0]] = {"ok": 0, "failed": 0}
            pipeline_runs[r[0]][r[1]] = r[2]

        job_timeline = []
        try:
            rows = conn.execute(
                "SELECT collected_at, metric_value_json FROM autonomy_test_metrics "
                "WHERE metric_group='jobs' AND metric_key='last_hour' ORDER BY collected_at"
            ).fetchall()
            for r in rows:
                val = _json_loads(r[1], {})
                job_timeline.append({"at": r[0], "jobs": val})
        except Exception:
            pass

        report["sections"]["jobs"] = {
            "all_runs": all_runs,
            "pipeline_runs": pipeline_runs,
            "timeline": job_timeline,
        }
    except Exception as e:
        report["sections"]["jobs"] = {"error": str(e)}

    # === 8. ERRORS & ALERTS ===
    try:
        dead_letters = 0
        try:
            dead_letters = conn.execute("SELECT COUNT(*) FROM dead_letter_items WHERE resolved_at IS NULL").fetchone()[0]
        except Exception:
            pass
        degraded_sources = conn.execute("SELECT COUNT(*) FROM source_sync_state WHERE COALESCE(quality_state, state)='degraded'").fetchone()[0]
        degraded_list = [r[0] for r in conn.execute("SELECT source_key FROM source_sync_state WHERE COALESCE(quality_state, state)='degraded'").fetchall()]
        alerts = []
        try:
            for r in conn.execute("SELECT alert_type, severity, message, created_at FROM daemon_alerts ORDER BY id DESC LIMIT 50").fetchall():
                alerts.append({"type": r[0], "severity": r[1], "message": r[2], "at": r[3]})
        except Exception:
            pass
        report["sections"]["errors"] = {
            "dead_letters": dead_letters,
            "degraded_sources": degraded_sources,
            "degraded_list": degraded_list,
            "alerts": alerts,
        }
    except Exception as e:
        report["sections"]["errors"] = {"error": str(e)}

    conn.close()

    # === SAVE JSON ===
    json_path = output_dir / "autonomy_report.json"
    with open(str(json_path), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)

    # === GENERATE MARKDOWN ===
    md = _generate_markdown(report)
    md_path = output_dir / "autonomy_report.md"
    with open(str(md_path), "w", encoding="utf-8") as f:
        f.write(md)

    report["_output_files"] = {"json": str(json_path), "markdown": str(md_path)}
    return report


def _generate_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Отчёт автономного тестирования")
    lines.append("")

    dur = report.get("test_duration", {})
    if "error" not in dur:
        lines.append(f"**Период:** {dur.get('first_metric_at', '?')} — {dur.get('last_metric_at', '?')}")
        lines.append(f"**Снапшотов метрик:** {dur.get('snapshot_count', 0)}")
    lines.append(f"**Сгенерирован:** {report.get('generated_at', '?')}")
    lines.append("")

    # SOURCES
    sources = report.get("sections", {}).get("sources", {})
    if sources and "error" not in sources:
        lines.append("## 1. Источники")
        lines.append("")
        ok_count = sum(1 for v in sources.values() if v.get("state") == "ok")
        degraded = [k for k, v in sources.items() if v.get("quality_state") == "degraded"]
        lines.append(f"Всего: **{len(sources)}**, OK: **{ok_count}**, Деградировано: **{len(degraded)}**")
        lines.append("")
        if degraded:
            lines.append("### Деградированные источники")
            for s in degraded:
                info = sources[s]
                lines.append(f"- **{s}**: {info.get('last_error', '?')} (failures: {info.get('consecutive_failures', 0)})")
            lines.append("")
        lines.append("| Источник | Статус | Качество | Ошибок подряд | Последняя ошибка |")
        lines.append("|----------|--------|----------|---------------|------------------|")
        for k in sorted(sources.keys()):
            v = sources[k]
            lines.append(f"| {k} | {v.get('state', '?')} | {v.get('quality_state', '?')} | {v.get('consecutive_failures', 0)} | {str(v.get('last_error', ''))[:60]} |")
        lines.append("")

    # CONTENT
    content = report.get("sections", {}).get("content", {})
    if content and "error" not in content:
        lines.append("## 2. Контент")
        lines.append("")
        lines.append(f"Всего: **{content.get('total', 0)}**, Активно: **{content.get('active', 0)}**, Подавлено: **{content.get('suppressed', 0)}**")
        pp = content.get("pipeline_progress", {})
        lines.append("")
        lines.append(f"- LLM обработано: {pp.get('llm_processed', 0)} ({pp.get('llm_pct', 0)}%)")
        lines.append(f"- NER обработано: {pp.get('ner_processed', 0)} ({pp.get('ner_pct', 0)}%)")
        lines.append(f"- Claims обработано: {pp.get('claims_processed', 0)} ({pp.get('claims_pct', 0)}%)")
        lines.append(f"- Мусор проверен: {pp.get('garbage_checked', 0)} ({pp.get('garbage_pct', 0)}%)")
        lines.append("")
        by_type = content.get("by_type", {})
        if by_type:
            lines.append("### По типу контента")
            lines.append("")
            for k, v in sorted(by_type.items(), key=lambda x: x[1], reverse=True)[:15]:
                lines.append(f"- {k}: {v}")
            lines.append("")
        by_status = content.get("by_status", {})
        if by_status:
            lines.append("### По статусу")
            lines.append("")
            for k, v in sorted(by_status.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- {k}: {v}")
            lines.append("")

    # ENTITIES
    entities = report.get("sections", {}).get("entities", {})
    if entities and "error" not in entities:
        lines.append("## 3. Сущности и связи")
        lines.append("")
        lines.append(f"Сущностей: **{entities.get('total', 0)}**, Связей: **{entities.get('relations_total', 0)}**, Упоминаний: **{entities.get('mentions_total', 0)}**")
        lines.append(f"NER новых (неисследованных): **{entities.get('ner_new_entities', 0)}**, Поиск проведён: **{entities.get('ner_searched', 0)}**")
        by_type = entities.get("by_type", {})
        if by_type:
            lines.append("")
            lines.append("### По типу сущности")
            for k, v in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- {k}: {v}")
        rel_by_type = entities.get("relations_by_type", {})
        if rel_by_type:
            lines.append("")
            lines.append("### По типу связи (топ-15)")
            for k, v in sorted(rel_by_type.items(), key=lambda x: x[1], reverse=True)[:15]:
                lines.append(f"- {k}: {v}")
        lines.append("")

    # CLAIMS
    claims = report.get("sections", {}).get("claims", {})
    if claims and "error" not in claims:
        lines.append("## 4. Заявления и верификация")
        lines.append("")
        lines.append(f"Всего заявлений: **{claims.get('total', 0)}**")
        lines.append(f"Подтверждено: **{claims.get('confirmed', 0)}**, Частично: **{claims.get('partially_confirmed', 0)}**, Не проверено: **{claims.get('unverified', 0)}**")
        lines.append(f"Уровень верификации: **{claims.get('confirmed_pct', 0)}%**")
        lines.append(f"Связей с доказательствами: **{claims.get('evidence_links', 0)}**")
        lines.append(f"Противоречий: **{claims.get('contradictions', 0)}**")
        by_type = claims.get("by_type", {})
        if by_type:
            lines.append("")
            lines.append("### По типу заявления")
            for k, v in sorted(by_type.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- {k}: {v}")
        lines.append("")

    # LLM
    llm = report.get("sections", {}).get("llm", {})
    if llm and "error" not in llm:
        lines.append("## 5. LLM-провайдеры и модели")
        lines.append("")
        lines.append(f"Активных ключей: **{llm.get('active_key_count', 0)}**")
        by_prov = llm.get("by_provider", {})
        if by_prov:
            lines.append("")
            lines.append("### По провайдеру")
            for k, v in sorted(by_prov.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- {k}: {v} ключей")
        by_pm = llm.get("by_provider_model", {})
        if by_pm:
            lines.append("")
            lines.append("### По модели (ключи)")
            lines.append("")
            lines.append("| Модель | Ключей | Tier | Роли |")
            lines.append("|--------|--------|------|------|")
            for k, v in sorted(by_pm.items(), key=lambda x: x[1]["count"], reverse=True):
                roles = ", ".join(v.get("stage_roles", [])[:3])
                lines.append(f"| {k} | {v['count']} | {v.get('tier', '?')} | {roles} |")
        attempts = llm.get("task_attempts_by_model", {})
        if attempts:
            lines.append("")
            lines.append("### Вызовы по модели (за всё время)")
            lines.append("")
            lines.append("| Модель | OK | Ошибки | % успеха |")
            lines.append("|--------|-----|--------|----------|")
            for k, v in sorted(attempts.items(), key=lambda x: x[1].get("ok", 0), reverse=True):
                ok = v.get("ok", 0)
                fail = v.get("failed", 0)
                total = ok + fail
                pct = round(100 * ok / max(total, 1), 1)
                lines.append(f"| {k} | {ok} | {fail} | {pct}% |")
        failures = llm.get("key_failures", {})
        if failures:
            lines.append("")
            lines.append("### Ошибки ключей")
            for k, v in sorted(failures.items(), key=lambda x: x[1], reverse=True)[:10]:
                lines.append(f"- {k}: {v}")
        lines.append("")

    # JOBS
    jobs = report.get("sections", {}).get("jobs", {})
    if jobs and "error" not in jobs:
        lines.append("## 6. Выполнение задач")
        lines.append("")
        all_runs = jobs.get("all_runs", {})
        if all_runs:
            lines.append("| Задача | OK | Ошибки | Новых | Обновлено | Просмотрено | Первое | Последнее |")
            lines.append("|--------|-----|--------|-------|-----------|-------------|--------|-----------|")
            for jid in sorted(all_runs.keys()):
                r = all_runs[jid]
                lines.append(f"| {jid} | {r.get('ok', 0)} | {r.get('failed', 0)} | {r.get('items_new', 0)} | {r.get('items_updated', 0)} | {r.get('items_seen', 0)} | {str(r.get('first_at', ''))[:16]} | {str(r.get('last_at', ''))[:16]} |")
        pipelines = jobs.get("pipeline_runs", {})
        if pipelines:
            lines.append("")
            lines.append("### Пайплайны")
            for mode, counts in pipelines.items():
                lines.append(f"- {mode}: OK={counts.get('ok', 0)}, Failed={counts.get('failed', 0)}")
        lines.append("")

    # ERRORS
    errors = report.get("sections", {}).get("errors", {})
    if errors and "error" not in errors:
        lines.append("## 7. Ошибки и алерты")
        lines.append("")
        lines.append(f"Dead letters: **{errors.get('dead_letters', 0)}**")
        lines.append(f"Деградированных источников: **{errors.get('degraded_sources', 0)}**")
        dl = errors.get("degraded_list", [])
        if dl:
            for s in dl:
                lines.append(f"- {s}")
        alerts = errors.get("alerts", [])
        if alerts:
            lines.append("")
            lines.append("### Алерты")
            for a in alerts:
                lines.append(f"- [{a.get('severity', '?')}] {a.get('type', '?')}: {a.get('message', '?')}")
        lines.append("")

    lines.append("---")
    lines.append(f"_Отчёт сгенерирован автоматически {report.get('generated_at', '?')}_")
    return "\n".join(lines)


def main():
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    report = generate_report(output_dir=output_dir)
    print(f"JSON: {report.get('_output_files', {}).get('json', '?')}")
    print(f"Markdown: {report.get('_output_files', {}).get('markdown', '?')}")


if __name__ == "__main__":
    main()
