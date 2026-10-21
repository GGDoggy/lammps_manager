"""Durable monitoring counters, pagination, unknowns and alert recovery."""
from dataclasses import asdict, replace
import io
import json
import unittest
from unittest.mock import patch

import test_scheduler
from lammps_manager.config import Monitoring, ConfigurationError, _parse
from lammps_manager.monitoring import (status, transfer, file_progress, transfers,
                                       refresh_alerts, ProgressReader)
from lammps_manager.store import Store
from lammps_manager.models import JobSpecification, Resources


class MonitoringTests(unittest.TestCase):
    setUp = test_scheduler.SchedulerTests.setUp
    submit = test_scheduler.SchedulerTests.submit

    def test_exhausted_alert_refresh_scales_on_fresh_and_reopened_databases(self):
        for reopen in (False, True):
            with self.subTest(reopen=reopen):
                path = str(self.path) + ('-reopened' if reopen else '-fresh')

                def seed(store, start, end):
                    with store._transaction():
                        for index in range(start, end):
                            job_id, attempt_id = f'job-{index}', f'attempt-{index}'
                            store.submit(JobSpecification(job_id, '/work/' + job_id, 'in',
                                                          'a' * 64, Resources()))
                            store.assign(job_id, attempt_id, 'w', '/work/' + attempt_id)
                            store.transition(attempt_id, 'failed', reason='interrupted',
                                             termination_evidence='confirmed terminated')
                            store._db.execute('INSERT INTO interruptions VALUES(?,?,?,?,?)',
                                (attempt_id, '{}', 'blocked', None,
                                 'Infrastructure recovery budget exhausted'))

                def snapshots(store):
                    return (store._db.execute('SELECT * FROM jobs ORDER BY rowid').fetchall(),
                            store._db.execute('SELECT * FROM attempts ORDER BY rowid').fetchall())

                def work(store, count):
                    # Measure steady-state refresh, excluding initial alert insertion.
                    refresh_alerts(store, self.config, 100)
                    ticks = 0

                    def tick():
                        nonlocal ticks
                        ticks += 1
                        return 0

                    store._db.set_progress_handler(tick, 100)
                    try:
                        refresh_alerts(store, self.config, 101)
                    finally:
                        store._db.set_progress_handler(None, 0)
                    alerts = store._db.execute('SELECT * FROM monitoring_alerts').fetchall()
                    self.assertEqual({row['subject'] for row in alerts},
                                     {f'job-{index}' for index in range(count)})
                    self.assertTrue(all(row['active'] and row['kind'] == 'exhausted_recovery'
                                        and row['last_changed_at'] == 100 for row in alerts))
                    return ticks

                with Store(path) as store:
                    if reopen:
                        store._db.execute('DROP INDEX IF EXISTS attempts_job')
                        seed(store, 0, 256)
                        before = snapshots(store)
                    else:
                        seed(store, 0, 256)
                        small = work(store, 256)
                        seed(store, 256, 512)
                        self.assertLess(work(store, 512), small * 3)
                if reopen:
                    with Store(path) as store:
                        self.assertEqual(snapshots(store), before)
                        small = work(store, 256)
                        seed(store, 256, 512)
                        self.assertLess(work(store, 512), small * 3)

    def test_page_filters_counts_and_unknowns(self):
        for name in ('a','b','c'):
            self.submit(name, cpu=1)
        self.store.assign('b', 'b1', 'w', '/work/b/b1')
        first = status(self.store, self.config, limit=1, now=100)
        self.assertEqual(first['counts'], {'queued': 2, 'assigned': 1})
        self.assertEqual(first['jobs'][0]['job_id'], 'a')
        self.assertIsNone(first['workers'][0]['online'])
        self.assertIsNone(first['workers'][0]['available_memory_gib'])
        self.assertIsNone(first['jobs'][0]['transfers'])
        second = status(self.store, self.config, after=first['next_cursor'], limit=1, now=100)
        self.assertEqual(second['jobs'][0]['job_id'], 'b')
        self.assertEqual(second['jobs'][0]['reservations'][0]['cpu_slots'], 1)
        self.assertEqual(status(self.store,self.config,state='queued',now=100)['counts'], first['counts'])
        self.assertEqual([j['job_id'] for j in status(self.store,self.config,worker_id='w',now=100)['jobs']], ['b'])
        for args in ({'limit': 0}, {'limit': 501}, {'after': -1}, {'state': 'secret'}):
            with self.assertRaises(ValueError):
                status(self.store,self.config,**args)

    def test_partial_replay_restart_and_verified_counters(self):
        self.submit()
        self.store.assign('j','a','w','/work/j/a')
        transfer(self.store,'a','input','transferring',total_bytes=10,total_files=2,now=100)
        file_progress(self.store,'a','input',0,transferred=3,now=101)
        transfer(self.store,'a','input','failed',error='interrupted',now=102)
        transfer(self.store,'a','input','failed',error='interrupted',now=103)
        with Store(self.path) as reopened:
            progress = transfers(reopened,'a')['input']
            self.assertEqual(progress['transferred_bytes'],3)
            self.assertIsNone(progress['verified_bytes'])
            self.assertEqual(progress['retries'],1)
            file_progress(reopened,'a','input',0,transferred=5,verified_bytes=5,now=104)
            file_progress(reopened,'a','input',0,verified_bytes=5,now=105)
            progress = transfers(reopened,'a')['input']
            self.assertEqual(progress['transferred_bytes'],8)
            self.assertEqual(progress['verified_bytes'],5)
            self.assertEqual(progress['verified_files'],1)
            self.assertFalse(progress['network_byte_resume'])
        self.assertEqual(len(self.store.events('j')),2)

    def test_alert_edges_stall_recovery_and_restart(self):
        self.submit()
        self.store.assign('j','a','w','/work/j/a')
        self.store.transition('a','unknown')
        self.store.set_worker('w',snapshot={**asdict(self.snapshot),'staged_free_disk_gib':1})
        transfer(self.store,'a','result','transferring',now=100)
        view = status(self.store,self.config,now=500)
        kinds = {a['kind'] for a in view['alerts'] if a['active']}
        self.assertEqual(kinds, {'unknown_execution','offline_worker','stale_metrics','low_disk','stalled_result'})
        original = view['alerts']
        self.assertEqual(status(self.store,self.config,now=501)['alerts'], original)
        self.store.transition('a','assigned')
        sample = replace(self.snapshot,observed_at=502,heartbeat_at=502)
        self.store.set_worker('w',snapshot={**asdict(sample),'staged_free_disk_gib':10})
        transfer(self.store,'a','result','acknowledged',now=502)
        with Store(self.path) as reopened:
            recovered = status(reopened,self.config,now=502)['alerts']
            self.assertTrue(all(not a['active'] and a['resolved_at']==502 for a in recovered))
        self.assertEqual(len(recovered),5)

    def test_unknown_totals_and_bounded_reader(self):
        self.submit()
        self.store.assign('j','a','w','/work/j/a')
        transfer(self.store,'a','result','pending',now=100)
        self.assertIsNone(transfers(self.store,'a')['result']['total_bytes'])
        reader = ProgressReader(io.BytesIO(b'x'*100),self.store,'a','result',0)
        with patch('lammps_manager.monitoring.time.monotonic',return_value=reader.updated):
            for _ in range(100):
                reader.read(1)
        self.assertIsNone(transfers(self.store,'a')['result']['transferred_bytes'])
        reader.flush()
        self.assertEqual(transfers(self.store,'a')['result']['transferred_bytes'],100)
        self.assertEqual(len(self.store.events('j')),2)
        for settings in ({'transfer_stall_seconds':0},{'low_disk_gib':float('nan')}):
            with self.assertRaises(ConfigurationError):
                _parse(Monitoring,settings,'monitoring')

    def test_monitoring_omits_submission_and_worker_secrets(self):
        self.submit()
        self.store.set_worker('w',snapshot={**asdict(self.snapshot),'token':'do-not-show','environment':{'PASSWORD':'do-not-show'}})
        rendered = json.dumps(status(self.store,self.config,now=100))
        self.assertNotIn('do-not-show',rendered)
        self.assertNotIn('specification',rendered)
        self.assertNotIn('input_sha256',rendered)

    def test_exhausted_recovery_is_distinct_from_unknown_budget(self):
        self.submit()
        self.store.assign('j','a','w','/work/j/a')
        self.store.transition('a','failed',reason='interrupted',termination_evidence='confirmed never started')
        self.store._db.execute('INSERT INTO interruptions VALUES(?,?,?,?,?)',
            ('a','{}','blocked',None,'Infrastructure recovery budget exhausted'))
        view = status(self.store,self.config,now=100)
        self.assertIn('exhausted_recovery', {a['kind'] for a in view['alerts'] if a['active']})

    def test_inspect_history_pages_and_alert_pages(self):
        from lammps_manager.local import inspect_job
        self.submit()
        self.store.assign('j','a','w','/work/j/a')
        self.store.reject_launch('a','not_ready','closed',0)
        self.store.return_rejected('a')
        self.store.assign('j','b','w','/work/j/b')
        first = inspect_job(self.store,'j',history_limit=1)
        self.assertEqual(first['attempts'][0]['attempt_id'],'b')
        older = inspect_job(self.store,'j',history_limit=1,
                            attempt_before=first['history_cursors']['attempt_before'],
                            event_before=first['history_cursors']['event_before'])
        self.assertEqual(older['attempts'][0]['attempt_id'],'a')
        self.assertLess(older['events'][0]['event_id'],first['events'][0]['event_id'])
        self.store.transition('b','unknown')
        self.store.set_worker('w',snapshot={**asdict(self.snapshot),'staged_free_disk_gib':1})
        page = status(self.store,self.config,limit=1,now=500)
        self.assertEqual(len(page['alerts']),1)
        self.assertIsNotNone(page['next_alert_cursor'])
        second = status(self.store,self.config,limit=1,alert_after=page['next_alert_cursor'],now=500)
        self.assertNotEqual(page['alerts'][0]['alert_key'],second['alerts'][0]['alert_key'])

    def test_retry_without_bytes_does_not_hide_stall(self):
        self.submit()
        self.store.assign('j','a','w','/work/j/a')
        transfer(self.store,'a','result','transferring',now=100)
        transfer(self.store,'a','result','failed',now=200)
        transfer(self.store,'a','result','transferring',now=350)
        self.assertEqual(transfers(self.store,'a')['result']['last_progress_at'],100)
        alerts = status(self.store,self.config,now=401)['alerts']
        self.assertIn('stalled_result',{a['kind'] for a in alerts if a['active']})

    def test_verification_only_keeps_network_bytes_unknown(self):
        self.submit()
        self.store.assign('j','a','w','/work/j/a')
        transfer(self.store,'a','input','transferring',total_bytes=5,total_files=1,now=100)
        file_progress(self.store,'a','input',0,verified_bytes=5,now=101)
        progress = transfers(self.store,'a')['input']
        self.assertIsNone(progress['transferred_bytes'])
        self.assertEqual(progress['verified_bytes'],5)

    def test_priority_audit_does_not_replace_wait_reason(self):
        self.submit()
        self.store.record_waiting('j','missing_metrics')
        self.store.set_priority('j',3)
        job = status(self.store,self.config,now=100)['jobs'][0]
        self.assertEqual(job['wait_reason'],'missing_metrics')

    def test_late_transfer_reports_do_not_regress_publication(self):
        self.submit()
        self.store.assign('j','a','w','/work/j/a')
        for direction, phase in [('input','ready'),('result','acknowledged')]:
            transfer(self.store,'a',direction,phase,total_bytes=5,total_files=1,now=100)
            transfer(self.store,'a',direction,'failed',now=101)
            self.assertEqual(transfers(self.store,'a')[direction]['phase'],phase)


class MonitoringHTTPTests(unittest.TestCase):
    import test_results as _results
    setUp = _results.ResultTests.setUp
    worker = _results.ResultTests.worker
    configure_process = _results.ResultTests.configure_process
    finish_process = _results.ResultTests.finish_process
    tick = _results.ResultTests.tick

    def test_poll_preserves_active_result_transfer_phase(self):
        import threading

        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        original = worker._result_request
        entered, release = threading.Event(), threading.Event()
        uploads = 0

        def blocked(attempt, operation, *args):
            nonlocal uploads
            if operation.isdigit():
                uploads += 1
                if uploads == 2:
                    entered.set()
                    if not release.wait(10):
                        raise TimeoutError('Blocked upload was not released')
            return original(attempt, operation, *args)

        def phase():
            with Store(self.config.manager.database_path) as store:
                return transfers(store, attempt_id)['result']['phase']

        try:
            with patch.object(worker, '_result_request', side_effect=blocked):
                worker.tick()
                self.assertTrue(entered.wait(5))
                self.assertEqual(phase(), 'transferring')
                worker.tick()  # The worker now reports uploading during background I/O.
                self.assertEqual(phase(), 'transferring')
                for current, report, expected in (
                        ('transferring', 'pending', 'transferring'),
                        ('verifying', 'uploading', 'verifying'),
                        ('verifying', 'pending', 'verifying'),
                        ('verifying', 'failed', 'failed'),
                        ('failed', 'pending', 'failed'),
                        ('failed', 'uploading', 'transferring')):
                    with self.subTest(current=current, report=report):
                        with Store(self.config.manager.database_path) as store:
                            transfer(store, attempt_id, 'result', current)
                        with Store(worker.database) as store:
                            store.set_result_collection(attempt_id, report)
                        worker.acknowledged.pop(attempt_id, None)
                        worker.tick()
                        self.assertEqual(phase(), expected)
                release.set()
                worker.collection_future.result(timeout=10)
        finally:
            release.set()
            if worker.collection_future:
                worker.collection_future.result(timeout=10)
        self.assertEqual(phase(), 'acknowledged')

    def test_real_loopback_transfer_health_auth_and_cleanup_history(self):
        from urllib.request import Request, urlopen
        from urllib.error import HTTPError
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        for path in ('/v1/health', f'/v2/input-verified/{attempt_id}'):
            with self.assertRaises(HTTPError) as caught:
                urlopen(Request(self.endpoint+path,data=b'{"index":0}' if 'verified' in path else None),timeout=3)
            self.assertEqual(caught.exception.code,401)
        request = Request(self.endpoint+'/v1/health',headers={'X-Worker-ID':'a','Authorization':'Bearer '+'a'*40})
        with urlopen(request,timeout=3) as response:
            health = json.load(response)
        self.assertEqual(health['worker_id'],'a')
        self.assertIsNotNone(health['staged_free_disk_gib'])
        self.assertNotIn('token',json.dumps(health))
        with self.assertRaises(ValueError):
            self.service.input_verified('b',attempt_id,0)
        with self.assertRaises(ValueError):
            self.service.input_verified('a',attempt_id,10**9)
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            progress = transfers(store,attempt_id)
            self.assertEqual(progress['input']['phase'],'ready')
            self.assertEqual(progress['input']['verified_files'],len(self.spec.input_manifest))
            self.assertTrue(progress['result']['acknowledged'])
            self.assertEqual(progress['result']['verified_files'],progress['result']['total_files'])
            self.assertGreater(progress['result']['transferred_bytes'],0)
            before = progress
        self.tick(worker)
        if worker.cleanup_future:
            worker.cleanup_future.result(timeout=10)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(transfers(store,attempt_id),before)
