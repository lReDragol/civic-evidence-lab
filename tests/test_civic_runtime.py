import json
import tempfile
import time
import unittest
from dataclasses import dataclass
from unittest.mock import Mock

from db.reactor import bootstrap_reactor_databases, open_reactor_db
from runtime.civic import register_capture,dispatch_observations,worker_once,accept_extractions,index_observations
from knowledge.investigations import add_claim,link_evidence,claim_evidence_summary,revise_thread


class CivicRuntimeTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        settings={"reactor_db_dir":tmp.name}
        bootstrap_reactor_databases(settings)
        self.k=open_reactor_db("knowledge",settings=settings)
        self.ops=open_reactor_db("ops",settings=settings)
        self.search=open_reactor_db("search",settings=settings)
        self.addCleanup(self.search.close)
        self.addCleanup(self.k.close)
        self.addCleanup(self.ops.close)

    def test_source_task_gateway_artifacts_claims_and_replay(self):
        rev=register_capture(self.k,source_url="https://example.org/one",payload={"text":"The commission denies ballot stuffing."})
        dispatch_observations(self.k,self.ops)
        dispatch_observations(self.k,self.ops)
        self.assertEqual(self.ops.execute("SELECT COUNT(*) FROM agent_tasks").fetchone()[0],1)
        client=Mock()
        client.infer.return_value={"ok":True,"output":{"claims":[{"text":"Commission denies stuffing","quote":"denies ballot stuffing","polarity":"denied","modality":"alleged","attribution":"commission"}]}}
        snapshot=Mock(expires_at=time.time()+600,routes=[Mock(route_id="one",max_input_tokens=32768,max_output_tokens=4096)])
        result=worker_once(self.ops,self.k,client=client,snapshot=snapshot,route_id="one")
        self.assertEqual(result["status"],"completed")
        self.assertEqual(self.k.execute("SELECT COUNT(*) FROM civic_claims").fetchone()[0],0)
        accept_extractions(self.ops,self.k)
        accept_extractions(self.ops,self.k)
        row=self.k.execute("SELECT polarity,status FROM civic_claims").fetchone()
        self.assertEqual(tuple(row),("denied","unreviewed"))
        self.assertEqual(self.k.execute("SELECT COUNT(*) FROM civic_claims").fetchone()[0],1)

    def test_search_head_replays_without_duplicate_current_results(self):
        for text in ("first","second","first"):
            register_capture(self.k,source_url="https://example.org/one",payload={"text":text})
        index_observations(self.k,self.search)
        index_observations(self.k,self.search)
        self.assertEqual(self.search.execute("SELECT COUNT(*) FROM search_documents").fetchone()[0],3)
        self.assertEqual(self.search.execute("SELECT body FROM current_search_documents_v").fetchone()[0],"first")
        self.assertEqual(self.search.execute("SELECT COUNT(*) FROM current_search_documents_v").fetchone()[0],1)

    def test_capture_timestamps_do_not_create_content_revisions(self):
        for stamp in ("2026-09-20T00:00:00Z","2026-09-20T00:01:00Z"):
            register_capture(self.k,source_url="https://example.org/one",payload={"text":"same","fetched_at":stamp,"warc_sha256":stamp})
        self.assertEqual(self.k.execute("SELECT COUNT(*) FROM source_revisions").fetchone()[0],1)
        self.assertEqual(self.k.execute("SELECT COUNT(*) FROM source_captures").fetchone()[0],2)

    def test_bad_quote_never_leaves_extraction_artifact(self):
        register_capture(self.k,source_url="https://example.org/one",payload={"text":"No evidence available."})
        dispatch_observations(self.k,self.ops)
        client=Mock()
        client.infer.return_value={"ok":True,"output":{"claims":[{"quote":"fabricated quote"}]}}
        snapshot=Mock(expires_at=time.time()+600,routes=[Mock(route_id="one",max_input_tokens=32768,max_output_tokens=4096)])
        self.assertEqual(worker_once(self.ops,self.k,client=client,snapshot=snapshot,route_id="one")["status"],"failed")
        self.assertEqual(self.ops.execute("SELECT COUNT(*) FROM agent_artifacts").fetchone()[0],0)

    def test_provider_quota_defers_input_instead_of_discarding_it(self):
        register_capture(self.k,source_url="https://example.org/one",payload={"text":"Source text."})
        dispatch_observations(self.k,self.ops)
        client=Mock()
        client.infer.return_value={"ok":False,"error":{"code":"provider_quota","retryable":False,"retry_after_seconds":300}}
        snapshot=Mock(expires_at=time.time()+600,routes=[Mock(route_id="one",max_input_tokens=32768,max_output_tokens=4096)])
        self.assertEqual(worker_once(self.ops,self.k,client=client,snapshot=snapshot,route_id="one")["status"],"gateway_error")
        self.assertEqual(self.ops.execute("SELECT status FROM agent_tasks").fetchone()[0],"needs_retry")
        self.assertEqual(self.ops.execute("SELECT COUNT(*) FROM agent_artifacts").fetchone()[0],0)

    def test_stance_and_versioned_thread_do_not_infer_truth(self):
        rev=register_capture(self.k,source_url="https://example.org/one",payload={"text":"Someone alleges an incident."})
        locator={"type":"quote","quote":"alleges an incident"}
        claim=add_claim(self.k,source_revision_id=rev["revision_id"],claim_text="Incident alleged",polarity="affirmed",modality="alleged",locator=locator)
        link_evidence(self.k,claim_id=claim,source_revision_id=rev["revision_id"],stance="refutes",locator=locator,origin_key="same-video")
        report=claim_evidence_summary(self.k,claim)
        self.assertFalse(report["publication_allowed"])
        self.assertEqual(report["reviewed_support_origins"],0)
        first=revise_thread(self.k,thread_key="one",question="What happened?",members=[{"type":"claim","id":claim}],reason="new lead",actor="researcher")
        with self.assertRaisesRegex(ValueError,"revision conflict"):
            revise_thread(self.k,thread_key="one",question="Other question",members=[],reason="edit",actor="researcher")
        second=revise_thread(self.k,thread_key="one",question="Updated question",members=[],reason="edit",actor="researcher",expected_revision_id=first["revision_id"])
        self.assertEqual(second["revision_no"],2)
        self.assertEqual(self.k.execute("SELECT COUNT(*) FROM thread_revisions").fetchone()[0],2)
