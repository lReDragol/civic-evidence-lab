"""Import an explicit user-supplied screenshot manifest as unverified leads."""
import argparse
import json
from pathlib import Path

from collectors.evidence_archive import EvidenceArchive
from db.reactor import open_reactor_db
from runtime.civic import register_capture, dispatch_observations


def import_manifest(manifest, *, input_dir, settings, archive_settings):
    archive=EvidenceArchive(archive_settings)
    knowledge=open_reactor_db("knowledge",settings=settings)
    ops=open_reactor_db("ops",settings=settings)
    result={"imported":[],"missing":[],"publication_allowed":False}
    try:
        for item in manifest:
            name=item["file"]
            if Path(name).name!=name:
                raise ValueError("Manifest filenames cannot escape the input directory")
            path=input_dir/name
            if not path.is_file():
                result["missing"].append(name)
                continue
            if path.stat().st_size>20*1024**2:
                raise ValueError("Screenshot exceeds 20MiB")
            original=archive.store_bytes(path.read_bytes())
            payload={"file":name,"sha256":original.sha256,"evidence_type":"user_screenshot",
                "verification_state":"unverified","origin_url":item.get("origin_url"),
                "text":item["visible_text"],"text_method":"manual_frame_transcription_unverified",
                "limitations":item["limitations"],"lead_only":True}
            rev=register_capture(knowledge,source_url="user-screenshot:"+original.sha256,payload=payload,
                original=original,media_type="image/png")
            result["imported"].append({"file":name,"sha256":original.sha256,**rev})
        result["dispatch"]=dispatch_observations(knowledge,ops)
        return result
    finally:
        knowledge.close()
        ops.close()


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest",type=Path,required=True)
    parser.add_argument("--input-dir",type=Path,required=True)
    parser.add_argument("--db-dir",type=Path,required=True)
    parser.add_argument("--archive-root",type=Path,default=Path(r"E:\CivicEvidence"))
    parser.add_argument("--report",type=Path,required=True)
    args=parser.parse_args()
    report=import_manifest(json.loads(args.manifest.read_text(encoding="utf-8")),input_dir=args.input_dir,
        settings={"reactor_db_dir":str(args.db_dir)},archive_settings={"evidence_archive_root":str(args.archive_root)})
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps({"imported":len(report["imported"]),"missing":report["missing"],"report":str(args.report)},ensure_ascii=False))
