import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch

from db.reactor import bootstrap_reactor_databases, open_reactor_db
from runtime.civic_service import (load_profile, writer_lock, run_collection,
    collection_status, export_collection_report, request_stop, _allow_request,
    _save_route_failure, _restore_route_holds)


class CivicServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root/'profile.json'
        self.profile = {"version":1,"profile_id":"universal-science","title":"Science",
            "db_dir":str(self.root/'db'),"archive_root":str(self.root/'archive'),"report_dir":str(self.root/'reports'),
            "archive_max_bytes":1000000,"archive_reserve_bytes":0,"max_http_requests_24h":2,
            "max_model_requests_24h":0,"max_pending_tasks":10,"report_interval_seconds":10,
            "gateway_config":None,"keywords":["research"],
            "sources":[{"url":"https://example.org/science","allowed_hosts":["example.org"],"interval_seconds":30}]}
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.profile),encoding='utf-8')

    @staticmethod
    def capture(source,profile):
        return {"source_url":source['url'],"status":"captured","text":"research result",
                "analysis_eligible":True},None

    def test_universal_profile_validated_and_invalid_limits_rejected(self):
        self.assertEqual(load_profile(self.path)['profile_id'],'universal-science')
        self.profile['max_http_requests_24h']=-1
        self.save()
        with self.assertRaises(ValueError):
            load_profile(self.path)

    def test_duplicate_sources_rejected(self):
        self.profile['sources']*=2
        self.save()
        with self.assertRaises(ValueError):
            load_profile(self.path)

    def test_single_writer_lock(self):
        with writer_lock(self.root/'db'):
            with self.assertRaises(RuntimeError):
                with writer_lock(self.root/'db'):
                    pass
        with writer_lock(self.root/'db'):
            pass

    def test_autonomous_capture_dispatch_index_report_and_no_model(self):
        result=run_collection(self.path,max_seconds=.2,capture=self.capture)
        self.assertEqual(result['status'],'exported')
        self.assertTrue(result['is_final'])
        report=result['report']
        self.assertEqual(report['run_delta']['source_revisions'],1)
        self.assertEqual(report['task_states_all_runs'],{'pending':1})
        self.assertEqual(report['run']['model_state'],'not_configured')
        self.assertFalse(report['publication_allowed'])
        self.assertTrue(Path(result['markdown_path']).exists())
        self.assertFalse(collection_status(self.path)['running'])

    def test_irrelevant_capture_is_saved_but_not_sent_to_model(self):
        def capture(source,profile):
            return {"status":"captured","text":"advertisement","analysis_eligible":False},None
        result=run_collection(self.path,max_seconds=.2,capture=capture)
        self.assertEqual(result['report']['run_delta']['source_revisions'],1)
        self.assertEqual(result['report']['task_states_all_runs'],{})

    def test_failure_backoff_survives_restart(self):
        calls=[]
        def capture(source,profile):
            calls.append(source['url'])
            raise TimeoutError()
        first=run_collection(self.path,max_seconds=.2,capture=capture)
        second=run_collection(self.path,max_seconds=.2,capture=capture)
        self.assertEqual(len(calls),1)
        self.assertEqual(first['report']['sources'][0]['failures'],1)
        self.assertEqual(second['report']['sources'][0]['status'],'capture_failed')

    def test_report_during_collection_and_graceful_stop(self):
        failures=[]
        def worker():
            try:
                run_collection(self.path,max_seconds=10,capture=self.capture)
            except BaseException as exc:
                failures.append(exc)
        thread=threading.Thread(target=worker)
        thread.start()
        try:
            deadline=time.monotonic()+5
            while not collection_status(self.path)['running'] and time.monotonic()<deadline:
                time.sleep(.05)
            report=export_collection_report(self.path)
            self.assertFalse(report['is_final'])
            self.assertTrue(collection_status(self.path)['running'])
            self.assertEqual(request_stop(self.path)['status'],'stop_requested')
        finally:
            thread.join(12)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures,[])
        self.assertFalse(collection_status(self.path)['running'])

    def test_budget_reserved_across_runs_and_reports_readonly(self):
        result=run_collection(self.path,max_seconds=.2,capture=self.capture)
        conn=open_reactor_db('ops',settings={'reactor_db_dir':self.profile['db_dir']})
        try:
            self.assertFalse(_allow_request(conn,result['run_id'],'http_request',1))
            before=conn.total_changes
            export_collection_report(self.path)
            self.assertEqual(conn.total_changes,before)
        finally:
            conn.close()

    def test_provider_quota_and_retry_after_survive_client_recreation(self):
        run_collection(self.path,max_seconds=.1,capture=self.capture)
        conn=open_reactor_db('ops',settings={'reactor_db_dir':self.profile['db_dir']})
        try:
            first=Mock(provider='provider',account_id='one',route_id='first')
            alias=Mock(provider='provider',account_id='two',route_id='alias')
            _save_route_failure(conn,first,{'error':{'code':'provider_quota','retry_after_seconds':3600}})
            recreated=Mock(snapshot=Mock(routes=[first,alias]))
            _restore_route_holds(conn,recreated)
            self.assertEqual(recreated.defer.call_count,2)
            self.assertTrue(all(call.kwargs['blocked'] for call in recreated.defer.call_args_list))
            self.assertTrue(all(call.kwargs['scope']=='provider' for call in recreated.defer.call_args_list))
            _save_route_failure(conn,first,{'error':{'code':'rate_limited','retry_after_seconds':1}})
            row=conn.execute('SELECT blocked,until_at FROM civic_model_holds').fetchone()
            self.assertEqual(row[0],1)
            self.assertGreater(row[1],time.time()+3500)
        finally:
            conn.close()

    def test_report_snapshots_do_not_overwrite_each_other(self):
        run_collection(self.path,max_seconds=.1,capture=self.capture)
        one=export_collection_report(self.path)
        two=export_collection_report(self.path)
        self.assertNotEqual(one['json_path'],two['json_path'])
        self.assertTrue(Path(one['json_path']).exists())


if __name__=='__main__':
    unittest.main()
