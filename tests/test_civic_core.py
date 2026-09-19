import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from agents.bus import enqueue_agent_task, lease_agent_task, complete_agent_task, heartbeat_agent_task
from agents.search import persist_search_result
from db.reactor import bootstrap_reactor_databases, open_reactor_db
from knowledge.revisions import record_source_revision
from knowledge.projection import start_generation, mark_generation_validated, activate_generation
from runtime.transfers import enqueue_transfer, receive_transfer, relay_pending


class CivicCoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings = {"reactor_db_dir": self.tmp.name}
        bootstrap_reactor_databases(self.settings)
        self.ops = open_reactor_db("ops", settings=self.settings)
        self.k = open_reactor_db("knowledge", settings=self.settings)
        self.addCleanup(self.ops.close)
        self.addCleanup(self.k.close)

    def task(self):
        return enqueue_agent_task(self.ops, task_type="search",requester_group="resolver",
            target_group="search",subject_type="claim",subject_key="claim:region:1",payload={"revision":1})

    def test_same_owner_reclaim_rejects_stale_artifacts(self):
        task = self.task()
        first = lease_agent_task(self.ops, lease_owner="shared")
        self.ops.execute("UPDATE agent_tasks SET lease_expires_at='2000-01-01'")
        self.ops.commit()
        second = lease_agent_task(self.ops, lease_owner="shared")
        self.assertNotEqual(first["lease_token"], second["lease_token"])
        self.assertFalse(heartbeat_agent_task(self.ops,task["task_id"],lease_owner="shared",lease_token=first["lease_token"]))
        with self.assertRaisesRegex(RuntimeError,"Stale"):
            persist_search_result(self.ops,task["task_id"],{"citations":["https://example.org/source"]},lease_owner="shared",lease_token=first["lease_token"])
        self.assertEqual(self.ops.execute("SELECT COUNT(*) FROM agent_artifacts").fetchone()[0],0)
        persist_search_result(self.ops,task["task_id"],{"citations":["https://example.org/source"]},lease_owner="shared",lease_token=second["lease_token"])
        self.assertEqual(self.ops.execute("SELECT COUNT(*) FROM agent_artifacts").fetchone()[0],1)

    def test_artifact_failure_rolls_back_completion(self):
        task = self.task()
        leased = lease_agent_task(self.ops,lease_owner="worker")
        self.ops.execute("CREATE TRIGGER fail_artifact BEFORE INSERT ON agent_artifacts BEGIN SELECT RAISE(ABORT,'crash'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            persist_search_result(self.ops,task["task_id"],{"citations":["https://example.org"]},lease_owner="worker",lease_token=leased["lease_token"])
        self.assertEqual(self.ops.execute("SELECT status FROM agent_tasks").fetchone()[0],"running")
        self.assertEqual(self.ops.execute("SELECT COUNT(*) FROM search_evidence").fetchone()[0],0)

    def test_retry_has_backoff_and_three_round_limit(self):
        task = self.task()
        for _ in range(3):
            leased = lease_agent_task(self.ops,lease_owner="w")
            self.assertIsNotNone(leased)
            complete_agent_task(self.ops,task["task_id"],status="needs_retry",lease_owner="w",lease_token=leased["lease_token"])
            self.assertIsNone(lease_agent_task(self.ops,lease_owner="w"))
            self.ops.execute("UPDATE agent_tasks SET available_at='2000-01-01'")
            self.ops.commit()
        self.assertEqual(self.ops.execute("SELECT status FROM agent_tasks").fetchone()[0],"insufficient_evidence")

    def test_a_b_a_retains_observation_history(self):
        self.k.execute("INSERT INTO source_systems(source_key,source_type,title) VALUES('one','official','One')")
        self.k.commit()
        ids=[]
        for text in ("A","B","A","A"):
            result=record_source_revision(self.k,source_id=1,external_id="post",raw_payload={"text":text})
            ids.append(result["revision_id"])
        self.assertEqual(ids[0],ids[2])
        self.assertEqual(self.k.execute("SELECT COUNT(*) FROM source_observations").fetchone()[0],3)
        self.assertEqual(self.k.execute("SELECT id FROM source_revisions WHERE is_current=1").fetchone()[0],ids[0])
        self.assertEqual(self.k.execute("SELECT COUNT(*) FROM source_revisions").fetchone()[0],2)

    def test_outbox_inbox_replay_after_receiver_commit(self):
        self.k.execute("BEGIN IMMEDIATE")
        message_id=enqueue_transfer(self.k,destination="ops",event_type="revision",subject_key="x",input_revision="1",payload={"source":1})
        self.k.commit()
        self.ops.execute("CREATE TABLE processed(id INTEGER)")
        handler=lambda conn,event,payload: conn.execute("INSERT INTO processed VALUES(?)",(payload["source"],))
        envelope=dict(self.k.execute("SELECT * FROM transfer_outbox").fetchone())
        receive_transfer(self.ops,envelope,handler)
        relay_pending(self.k,self.ops,destination_name="ops",handler=handler)
        self.assertEqual(self.ops.execute("SELECT COUNT(*) FROM processed").fetchone()[0],1)
        self.assertIsNotNone(self.k.execute("SELECT delivered_at FROM transfer_outbox").fetchone()[0])

    def test_older_generation_does_not_replace_newer(self):
        first=start_generation(self.k,projection_type="relations",generation_key="old",source_watermark="1")
        second=start_generation(self.k,projection_type="relations",generation_key="new",source_watermark="2")
        mark_generation_validated(self.k,first)
        mark_generation_validated(self.k,second)
        activate_generation(self.k,second)
        with self.assertRaisesRegex(RuntimeError,"Stale"):
            activate_generation(self.k,first)
        self.assertEqual(self.k.execute("SELECT id FROM projection_generations WHERE status='active'").fetchone()[0],second)
