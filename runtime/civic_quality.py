"""Publication readiness report. Human gold labels are never generated here."""
import argparse
import json
import math
from pathlib import Path
from db.reactor import open_reactor_db


def baseline_report(entries):
    required={"claim_incident":300,"protocol":100,"link_precinct_match":200}
    counts={kind:0 for kind in required}
    accepted=correct=0
    seen=set()
    errors=[]
    for item in entries:
        kind=item.get("kind")
        identity=(kind,item.get("id"))
        if kind not in required or not item.get("id") or identity in seen or not item.get("reviewed_by") or not item.get("reviewed_at"):
            errors.append("invalid_or_duplicate_review")
            continue
        if type(item.get("accepted")) is not bool or type(item.get("correct")) is not bool:
            errors.append("missing_human_verdict")
            continue
        seen.add(identity)
        counts[kind]+=1
        if kind=="link_precinct_match" and item["accepted"]:
            accepted+=1
            correct+=int(item["correct"])
    precision=correct/accepted if accepted else None
    interval=None
    if accepted:
        z=1.96
        center=(precision+z*z/(2*accepted))/(1+z*z/accepted)
        radius=z*math.sqrt(precision*(1-precision)/accepted+z*z/(4*accepted*accepted))/(1+z*z/accepted)
        interval=[max(0,center-radius),min(1,center+radius)]
    missing={kind:max(0,required[kind]-counts[kind]) for kind in required}
    return {"reviewed_counts":counts,"missing":missing,"accepted_sample":accepted,
            "precision":precision,"wilson_95_interval":interval,"errors":errors[:20],
            "ok":not errors and not any(missing.values()) and precision is not None and precision>=0.95}


def quality_report(conn, baseline=None):
    from knowledge.investigations import validate_locator
    violations=[]
    for claim in conn.execute("SELECT id,source_revision_id,locator_json FROM civic_claims"):
        try:
            validate_locator(conn,claim[1],json.loads(claim[2]))
        except (ValueError,TypeError,OSError):
            violations.append(claim[0])
    review=baseline_report(baseline or [])
    types={"claim":("claim_incident","civic_claims"),"incident":("claim_incident","election_incidents"),
           "protocol":("protocol","election_protocol_versions"),"evidence_link":("link_precinct_match","civic_evidence_links"),
           "precinct":("link_precinct_match","election_precincts")}
    unbound=0
    for item in baseline or []:
        prefix,_,identifier=str(item.get("id","")).partition(":")
        binding=types.get(prefix)
        if not binding or binding[0]!=item.get("kind") or not identifier.isdigit() or not conn.execute(f"SELECT id FROM {binding[1]} WHERE id=?",(int(identifier),)).fetchone():
            unbound+=1
    foreign=len(conn.execute("PRAGMA foreign_key_check").fetchall())
    pending=conn.execute("SELECT COUNT(*) FROM civic_claims WHERE status='unreviewed'").fetchone()[0]
    # This command reports readiness, never grants publication or initiates it.
    return {"gold":review,"invalid_provenance_claim_ids":violations[:100],"invalid_provenance_count":len(violations),
            "foreign_key_violations":foreign,"unreviewed_claims":pending,
            "unbound_gold_artifacts":unbound,
            "technical_ready":review["ok"] and not violations and not foreign and not unbound,
            "publication_allowed":False,"requires":"reviewed artifact identities, numeric protocol checks and explicit operator release"}


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-dir",type=Path,default=Path("db/civic-shadow"))
    parser.add_argument("--baseline",type=Path)
    parser.add_argument("--report",type=Path)
    args=parser.parse_args()
    conn=open_reactor_db("knowledge",settings={"reactor_db_dir":str(args.db_dir)},readonly=True)
    try:
        report=quality_report(conn,json.loads(args.baseline.read_text(encoding="utf-8")) if args.baseline else None)
    finally:
        conn.close()
    output=json.dumps(report,ensure_ascii=False,indent=2)
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(output,encoding="utf-8")
    print(output)
    raise SystemExit(0 if report["technical_ready"] else 2)
