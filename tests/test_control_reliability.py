"""Contention and independent maintenance/staging regressions without real engines."""
from dataclasses import asdict, replace
from io import BytesIO
from pathlib import Path
import sqlite3
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import json

import test_workers as fixtures
from lammps_manager.store import Store, is_busy
from lammps_manager.worker import LinuxMonitor
from lammps_manager.monitoring import ProgressReader


class ControlReliabilityTests(unittest.TestCase):
    setUp = fixtures.WorkerTests.setUp
    worker_config = fixtures.WorkerTests.worker_config

    def payload(self):
        return dict(proof=self.service.proof, reports=[], heartbeat_only=True,
                    snapshot=LinuxMonitor().sample(self.worker_config()))

    def request(self, payload):
        return Request(self.endpoint + '/v1/poll', data=json.dumps(payload).encode(),
                       headers={'X-Worker-ID': 'a', 'Authorization': 'Bearer ' + 'a' * 40})

    def test_dispatch_accepts_heartbeat_arriving_during_snapshot_read(self):
        self.service.close()  # Drive maintenance deterministically on this thread.
        job, = fixtures.WorkerTests.submit(self, (0,))
        payload = self.payload()
        payload['snapshot'].update(observed_at=1001, cpu_sample_started_at=1000)
        clock = [1000]
        injected = []
        refreshed = []
        records = Store.worker_records

        def read_records(store):
            if refreshed and not injected:
                injected.append(True)
                clock[0] = 1001
                self.service._poll('a', payload)
            return records(store)

        with patch('lammps_manager.manager.time.time', side_effect=lambda: clock[0]), \
                patch('lammps_manager.segments.refresh', side_effect=lambda *args, **kwargs: refreshed.append(True)), \
                patch.object(Store, 'worker_records', read_records):
            self.service.expire_workers()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(job), 'assigned')
            self.assertEqual(len(store.reservations()), 1)
        self.assertEqual(self.service.last_schedule, 1001)

    def test_bounded_dispatch_keeps_capacity_atomic_across_rounds(self):
        from lammps_manager.scheduler import schedule, WorkerSnapshot
        from lammps_manager.config import WorkerCapacity
        ids = fixtures.WorkerTests.submit(self, (0,) * 6)
        capacity = replace(self.capacities[0], cpu_slots=4, memory_budget_gib=4, max_concurrent_jobs=4)
        config = replace(self.config, workers=(capacity, self.capacities[1]))
        sample = LinuxMonitor().sample(replace(self.worker_config(), worker=capacity))
        snapshot = WorkerSnapshot(**{**sample, 'capacity': capacity})
        with Store(config.manager.database_path) as store:
            first = schedule(store, config, (snapshot,), now=time.time(), max_assignments=4)
            second = schedule(store, config, (snapshot,), now=time.time(), max_assignments=4)
            self.assertEqual(len(first.assignments), 4)
            self.assertFalse(second.assignments)
            self.assertEqual(sum(r.cpu_slots for r in store.reservations()), 4)
            self.assertEqual(sum(store.job_state(job) == 'queued' for job in ids), 2)

    def test_worker_busy_backoff_is_bounded_and_resets_after_success(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        from lammps_manager.worker import Worker
        worker = SimpleNamespace(config=SimpleNamespace(heartbeat_interval_seconds=.01),
            tick=Mock(side_effect=[*({'retry_after': 1} for _ in range(7)),
                                   {'assignments': []}, {'retry_after': 1}, KeyboardInterrupt]))
        delays = []
        with patch('lammps_manager.worker.time.sleep', side_effect=delays.append), self.assertRaises(KeyboardInterrupt):
            Worker.run(worker)
        self.assertEqual(delays, [1, 2, 4, 8, 16, 30, 30, .01, 1])

    def test_startup_under_writer_still_serves_and_recovers(self):
        from lammps_manager.manager import ManagerService, make_server
        with sqlite3.connect(self.config.manager.database_path, isolation_level=None) as writer:
            writer.execute('BEGIN IMMEDIATE')
            service = ManagerService(self.config, self.tokens)
            self.addCleanup(service.close)
            server = make_server(service, '127.0.0.1', 0)
            thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
            thread.start()
            def stop():
                server.shutdown()
                thread.join(timeout=3)
                server.server_close()
            self.addCleanup(stop)
            request = Request(f'http://127.0.0.1:{server.server_port}/v1/health',
                              headers={'X-Worker-ID': 'a', 'Authorization': 'Bearer ' + 'a' * 40})
            with urlopen(request, timeout=2) as reply:
                self.assertEqual(reply.status, 200)
            writer.execute('ROLLBACK')
        payload = self.payload()
        payload['proof'] = service.proof
        poll = Request(f'http://127.0.0.1:{server.server_port}/v1/poll', data=json.dumps(payload).encode(),
                       headers={'X-Worker-ID': 'a', 'Authorization': 'Bearer ' + 'a' * 40})
        with urlopen(poll, timeout=2) as reply:
            self.assertEqual(reply.status, 200)

    def test_acceptance_waits_for_delivery_after_resources_are_released(self):
        from tools.verification.control_real import pending_delivery
        job, = fixtures.WorkerTests.submit(self, (0,))
        with Store(self.config.manager.database_path) as store:
            spec = store.specification(job)
            store.assign(job, 'delivery-probe', 'a', str(Path(spec.work_directory) / 'delivery-probe'))
            store.transition('delivery-probe', 'failed', reason='Test failure before launch',
                             termination_evidence='Never started test process')
            pending = pending_delivery(store)
            self.assertEqual(pending['reservations'], 0)
            self.assertEqual(pending['results'], 1)
            store.set_result_collection('delivery-probe', 'collected', dict(files=[], omitted=[]))
            self.assertFalse(any(pending_delivery(store).values()))

    def test_open_existing_database_under_writer_does_not_write(self):
        with sqlite3.connect(self.config.manager.database_path, isolation_level=None) as writer:
            writer.execute('BEGIN IMMEDIATE')
            started = time.monotonic()
            with Store(self.config.manager.database_path, timeout=0) as reader:
                self.assertEqual(reader.worker_records(), {})
                self.assertEqual(reader._db.execute('PRAGMA foreign_keys').fetchone()[0], 1)
            self.assertLess(time.monotonic() - started, .5)
            writer.execute('ROLLBACK')

    def test_busy_http_is_retryable_and_accept_loop_survives(self):
        with sqlite3.connect(self.config.manager.database_path, isolation_level=None) as writer:
            writer.execute('BEGIN IMMEDIATE')
            with self.assertRaises(HTTPError) as caught:
                urlopen(self.request(self.payload()), timeout=2)
            self.assertEqual(caught.exception.code, 503)
            self.assertEqual(caught.exception.headers['Retry-After'], '1')
            caught.exception.close()
            writer.execute('ROLLBACK')
        with urlopen(self.request(self.payload()), timeout=2) as reply:
            self.assertEqual(reply.status, 200)
        self.assertFalse(is_busy(sqlite3.OperationalError('disk I/O error')))

    def test_lock_classification_on_python310_and_extended_codes(self):
        for message in ('database is locked', 'database table is locked: jobs',
                        'database schema is locked: main'):
            self.assertTrue(is_busy(sqlite3.OperationalError(message)))
        error = sqlite3.OperationalError('extended busy')
        error.sqlite_errorcode = 5 | (2 << 8)
        self.assertTrue(is_busy(error))
        error.sqlite_errorcode = 10
        self.assertFalse(is_busy(error))

    def test_nonbusy_database_failure_is_not_retryable(self):
        request = Request(self.endpoint + '/v1/health',
                          headers={'X-Worker-ID': 'a', 'Authorization': 'Bearer ' + 'a' * 40})
        with patch.object(self.service, 'health', side_effect=sqlite3.OperationalError('disk I/O error')), \
                self.assertLogs('lammps_manager.manager', level='ERROR'), self.assertRaises(HTTPError) as caught:
            urlopen(request, timeout=2)
        self.assertEqual(caught.exception.code, 500)
        self.assertIsNone(caught.exception.headers.get('Retry-After'))
        caught.exception.close()

    def test_slow_maintenance_does_not_block_heartbeat(self):
        entered, release = threading.Event(), threading.Event()
        original = self.service.expire_workers
        def slow():
            entered.set()
            release.wait(3)
            return original()
        with patch.object(self.service, 'expire_workers', side_effect=slow):
            self.service._maintenance_wake.set()
            self.assertTrue(entered.wait(2))
            try:
                started = time.monotonic()
                with urlopen(self.request(self.payload()), timeout=1) as response:
                    self.assertEqual(json.load(response)['assignments'], [])
                self.assertLess(time.monotonic() - started, 1)
            finally:
                release.set()

    def test_busy_progress_does_not_abort_stream(self):
        with Store(self.config.manager.database_path, timeout=0) as store:
            reader = ProgressReader(BytesIO(b'input'), store, 'unused', 'input', 0)
            with patch('lammps_manager.monitoring.file_progress', side_effect=sqlite3.OperationalError('database is locked')):
                self.assertEqual(reader.read(), b'input')
                reader.flush()
                self.assertEqual(reader.pending, 5)
            with patch('lammps_manager.monitoring.file_progress') as progress:
                reader.flush()
                self.assertEqual(reader.pending, 0)
                self.assertEqual(progress.call_args.kwargs['transferred'], 5)


class StagingIndependenceTests(unittest.TestCase):
    import test_staging as staging
    setUp = staging.StagedWorkerTests.setUp

    def test_failed_input_does_not_starve_other_reserved_attempts(self):
        import shutil
        worker = self.staging.StagedWorkerTests.worker(self)
        healthy_root = self.manager_root / 'healthy'
        shutil.copytree(self.spec.work_directory, healthy_root)
        healthy = replace(self.spec, job_id='healthy', work_directory=str(healthy_root))
        (Path(self.spec.work_directory) / 'inputs/potential').unlink()
        capacity = replace(self.capacity, max_concurrent_jobs=2)
        self.service.config = replace(self.config, workers=(capacity,))
        worker.config = replace(worker.config, worker=capacity)
        for database in (self.config.manager.database_path, worker.database):
            with Store(database) as store:
                if database == worker.database:
                    store.submit(self.spec)
                store.submit(healthy)
                for spec, attempt_id in ((self.spec, 'a-failed'), (healthy, 'b-healthy')):
                    root = self.worker_root if database == worker.database else self.manager_root
                    store.assign(spec.job_id, attempt_id, 'a', str(root / spec.job_id / attempt_id))
        for _ in range(4):
            worker.tick()
            if worker.input_future is not None:
                worker.input_future.result(timeout=3)
        with Store(worker.database) as store:
            self.assertEqual(store.input_transfer('a-failed')['status'], 'failed')
            transfer = store.input_transfer('b-healthy')
            self.assertIsNotNone(transfer)
            self.assertEqual(transfer['status'], 'ready')
            self.assertFalse(store.launch_claimed('b-healthy'))
            self.assertEqual(len(store.reservations()), 2)
        (Path(self.spec.work_directory) / 'inputs/potential').write_text('potential')
        worker.tick()
        worker.input_future.result(timeout=3)
        with Store(worker.database) as store:
            self.assertEqual(store.input_transfer('a-failed')['status'], 'ready')
            self.assertEqual(len(store.job_attempts(self.job_id)), 1)

    def test_preparation_skips_uncertain_cancelled_and_claimed_attempts(self):
        worker = self.staging.StagedWorkerTests.worker(self)
        with Store(self.config.manager.database_path) as store:
            store.assign(self.job_id, 'z-healthy', 'a',
                         str(self.manager_root / self.job_id / 'z-healthy'))
        with Store(worker.database) as store:
            for attempt_id, state in (('a-unknown', 'unknown'), ('b-cancelling', 'cancelling'),
                                      ('c-claimed', 'assigned'), ('d-cancelled', 'cancelled'),
                                      ('z-healthy', 'assigned')):
                spec = self.spec if attempt_id == 'z-healthy' else replace(self.spec, job_id=attempt_id)
                store.submit(spec)
                store.assign(spec.job_id, attempt_id, 'a', str(self.worker_root / spec.job_id / attempt_id))
                if state == 'unknown':
                    store.transition(attempt_id, state, reason='Uncertain launch identity')
                elif state in {'cancelling', 'cancelled'}:
                    store.request_cancel(attempt_id)
                    if state == 'cancelled':
                        store.transition(attempt_id, state, termination_evidence='Never started')
                elif attempt_id == 'c-claimed':
                    store.claim_launch(attempt_id)
            worker._queue_input(store)
        worker.input_future.result(timeout=3)
        with Store(worker.database) as store:
            self.assertEqual(store.input_transfer('z-healthy')['status'], 'ready')
            for attempt_id in ('a-unknown', 'b-cancelling', 'c-claimed', 'd-cancelled'):
                self.assertIsNone(store.input_transfer(attempt_id))
            self.assertEqual(len(store.reservations()), 4)
            self.assertTrue(store.launch_claimed('c-claimed'))
        self.assertFalse(worker.futures)

    def test_busy_optional_transfer_connection_preserves_bytes(self):
        import inspect
        worker = self.staging.StagedWorkerTests.worker(self)
        def connection(path, **kwargs):
            if inspect.currentframe().f_back.f_code.co_name == 'do_GET' and kwargs.get('timeout') == 0:
                raise sqlite3.OperationalError('database is locked')
            return Store(path, **kwargs)
        with patch('lammps_manager.manager.Store', side_effect=connection):
            self.staging.StagedWorkerTests.tick(self, worker)
        with Store(worker.database) as journal:
            attempt, = journal.job_attempts(self.job_id)
            self.assertEqual(journal.input_transfer(attempt.attempt_id)['status'], 'ready')

    def test_slow_preparation_keeps_normal_poll_and_cancellation_live(self):
        worker = self.staging.StagedWorkerTests.worker(self)
        entered, release = threading.Event(), threading.Event()
        original = worker._stage
        def slow(store, attempt_id):
            # A connection created on the preparation thread is usable here.
            store._db.execute('SELECT 1').fetchone()
            entered.set()
            release.wait(3)
            return original(store, attempt_id)
        with patch.object(worker, '_stage', side_effect=slow):
            try:
                started = time.monotonic()
                worker.tick()
                self.assertLess(time.monotonic() - started, 1)
                self.assertTrue(entered.wait(2))
                with Store(self.config.manager.database_path) as store:
                    attempt, = store.job_attempts(self.job_id)
                    store.request_cancel(attempt.attempt_id, 'Cancel blocked preparation')
                worker.tick()
                with Store(worker.database) as store:
                    self.assertEqual(store.attempt(attempt.attempt_id).state, 'cancelled')
                    self.assertFalse(store.launch_claimed(attempt.attempt_id))
                    self.assertFalse(store.reservations())
                self.assertFalse(worker.futures)
            finally:
                release.set()
            worker.input_future.result(timeout=3)


import test_segments as segment_fixtures
from lammps_manager import segments


class ContinuationPreparationTests(unittest.TestCase):
    setUp = segment_fixtures.SegmentTests.setUp
    submit = segment_fixtures.SegmentTests.submit
    current = segment_fixtures.SegmentTests.current
    assign = segment_fixtures.SegmentTests.assign
    finish = segment_fixtures.SegmentTests.finish
    checkpoint = segment_fixtures.SegmentTests.checkpoint

    def test_cancel_during_file_preparation_cannot_enqueue(self):
        self.submit()
        self.finish()
        original = segments.publish_inputs
        cancelled = []
        def prepare(*args, **kwargs):
            self.assertFalse(self.store._db.in_transaction)
            if not cancelled:
                with Store(self.db, timeout=0) as other:
                    segments.cancel_run(other, 'run')
                cancelled.append(True)
            return original(*args, **kwargs)
        with patch.object(segments, 'publish_inputs', side_effect=prepare):
            segments.advance(self.store, 'run')
        self.assertEqual(len(self.store.jobs()), 1)
        self.assertEqual(self.store.job_state(self.current()['job_id']), 'succeeded')
        self.assertEqual(segments.inspect_run(self.store, 'run')['status'], 'cancelled')

    def test_late_preparation_failure_does_not_overwrite_cancellation(self):
        self.submit()
        self.finish()
        def fail(*args, **kwargs):
            with Store(self.db, timeout=0) as other:
                segments.cancel_run(other, 'run')
            raise OSError('Injected late preparation failure')
        with patch.object(segments, 'publish_inputs', side_effect=fail):
            segments.refresh(self.store)
        self.assertEqual(segments.inspect_run(self.store, 'run')['status'], 'cancelled')
        self.assertEqual(len(self.store.jobs()), 1)

    def test_generation_change_during_preparation_cannot_enqueue(self):
        self.submit()
        self.finish()
        original = segments.publish_inputs
        changed = []
        def prepare(*args, **kwargs):
            self.assertFalse(self.store._db.in_transaction)
            if not changed:
                with Store(self.db, timeout=0) as other:
                    other._db.execute("UPDATE run_environments SET generation=generation+1 WHERE run_id='run'")
                changed.append(True)
            return original(*args, **kwargs)
        with patch.object(segments, 'publish_inputs', side_effect=prepare):
            segments.advance(self.store, 'run')
        self.assertEqual(len(self.store.jobs()), 1)
        self.assertFalse(self.current()['completed'])

    def test_control_reads_do_not_decode_large_initial_arrays(self):
        self.submit()
        with patch.object(segments.json, 'loads', side_effect=AssertionError('Unexpected full snapshot decode')):
            self.assertFalse(segments._run(self.store, 'run', snapshot=False)['cancelled'])


class RealControlLatencyTests(unittest.TestCase):
    def test_generated_bootstrap_records_http_and_transport_failures(self):
        import runpy
        import tempfile
        import urllib.request
        from unittest.mock import Mock
        from tools.verification.control_real import WORKER_BOOTSTRAP

        busy = HTTPError('http://localhost/v1/poll', 503, 'busy', {}, None)
        timeout = TimeoutError('slow poll')
        response = Mock(status=200)
        interrupted = Mock(status=200)
        artifact = Mock(status=200)
        original = Mock(side_effect=[busy, timeout, response, interrupted, artifact])
        opener = urllib.request.OpenerDirector.open
        with tempfile.TemporaryDirectory() as folder:
            namespace = {'__file__': str(Path(folder) / 'bootstrap.py')}
            with patch.object(urllib.request.OpenerDirector, 'open', original), \
                    patch.object(runpy, 'run_module'), patch('logging.basicConfig'):
                exec(compile(WORKER_BOOTSTRAP, 'bootstrap.py', 'exec'), namespace)
                timed = urllib.request.OpenerDirector.open
            self.assertIs(urllib.request.OpenerDirector.open, opener)
            request = Request('http://localhost/v1/poll')
            for error, clock in ((busy, [10, 16]), (timeout, [20, 27])):
                with patch('time.monotonic', side_effect=clock), self.assertRaises(type(error)) as caught:
                    timed(None, request)
                self.assertIs(caught.exception, error)
            with patch('time.monotonic', side_effect=[30, 30.1, 30.2]):
                with timed(None, request) as reply:
                    self.assertEqual(reply.status, 200)
            from http.client import IncompleteRead
            with patch('time.monotonic', side_effect=[40, 40.1, 49]), self.assertRaises(IncompleteRead):
                with timed(None, request):
                    raise IncompleteRead(b'partial', 10)
            self.assertIs(timed(None, Request('http://localhost/v2/inputs/test/files/0')), artifact)
            samples = [json.loads(line) for line in (Path(folder) / 'control-latency.jsonl').read_text().splitlines()]
            self.assertEqual(len(samples), 4)
            self.assertEqual([sample['outcome'] for sample in samples], ['http_error', 'transport_error', 'success', 'transport_error'])
            self.assertEqual(samples[0]['status'], 503)
            self.assertEqual(samples[1]['error'], 'TimeoutError')
            values = sorted(sample['seconds'] for sample in samples)
            self.assertEqual(samples[3]['error'], 'IncompleteRead')
            self.assertEqual(values[int(len(values) * .95)], 9)
            self.assertEqual(max(values), 9)
            response.close.assert_called_once()
            interrupted.close.assert_called_once()
            artifact.close.assert_not_called()


class RealManagerAcceptanceTests(unittest.TestCase):
    def acceptance(self, manager):
        # Execute the actual shipped gate without starting SSH/systemd or jobs.
        import ast
        from types import SimpleNamespace
        from tools.verification import control_real
        module = ast.parse(Path(control_real.__file__).read_text())
        assignment = next(node for node in ast.walk(module)
            if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Subscript) and isinstance(target.slice, ast.Constant)
                and target.slice.value == 'bounded_acceptance' for target in node.targets))
        report = dict(remaining_reservations=0, elapsed_seconds=3600, completed_segments=2,
            pending_delivery=dict(reservations=0, results=0, checkpoints=0),
            heartbeat_expiries=0, manager_service_before_stop=manager)
        context = dict(failure=None, report=report, args=SimpleNamespace(seconds=3600),
            cleanup_errors=[], integrity=dict(errors=[]), audits={host:dict(p95=.1,
                maximum=.2, service='NRestarts=0\nActiveState=active\n')
                for host in ('poincare', 'mozzarella', 'allegro')})
        return eval(compile(ast.Expression(assignment.value), '<acceptance-gate>', 'eval'), context)

    def test_manager_restart_or_missing_evidence_cannot_pass_actual_gate(self):
        self.assertTrue(self.acceptance(dict(healthy=True, restarts=0, active_state='active')))
        for manager in (dict(healthy=False, restarts=1, active_state='active'),
                        dict(healthy=False, error='TimeoutExpired'), {}):
            with self.subTest(manager=manager):
                self.assertFalse(self.acceptance(manager))

    def test_manager_query_requires_successful_complete_zero_restart_evidence(self):
        import subprocess
        from tools.verification.control_real import manager_service_audit
        cases = (
            (0, 'NRestarts=0\nActiveState=active\n', True),
            (0, 'ActiveState=active\nNRestarts=0\n', True),
            (0, 'NRestarts=1\nActiveState=active\n', False),
            (0, 'NRestarts=10\nActiveState=active\n', False),
            (1, 'NRestarts=0\nActiveState=active\n', False),
            (0, '', False),
            (0, 'ActiveState=active\n', False),
            (0, 'NRestarts=0\n', False),
            (0, 'NRestarts=unknown\nActiveState=active\n', False),
            (0, 'NRestarts=-1\nActiveState=active\n', False),
            (0, 'NRestarts=0\nNRestarts=1\nActiveState=active\n', False),
            (0, 'NRestarts=0\nActiveState=failed\n', False),
            (0, 'NRestarts=0\nActiveState=inactive\n', False),
            (0, 'NRestarts=0\nActiveState=activating\n', False),
        )
        for returncode, stdout, expected in cases:
            with self.subTest(returncode=returncode, stdout=stdout):
                result = subprocess.CompletedProcess([], returncode, stdout, '')
                with patch('tools.verification.control_real.subprocess.run', return_value=result) as query:
                    audit = manager_service_audit('test-manager')
                self.assertEqual(audit['healthy'], expected)
                self.assertEqual(audit['properties'], stdout)
                self.assertEqual(query.call_args.args[0], ['systemctl', '--user', 'show',
                    'test-manager', '-p', 'NRestarts', '-p', 'ActiveState'])
                self.assertEqual(query.call_args.kwargs['timeout'], 10)
                self.assertEqual(self.acceptance(audit), expected)

    def test_manager_query_errors_are_reported_without_interrupting_cleanup(self):
        import subprocess
        from tools.verification.control_real import manager_service_audit
        for error in (subprocess.TimeoutExpired('systemctl', 10), OSError('user bus unavailable')):
            with self.subTest(error=type(error).__name__):
                with patch('tools.verification.control_real.subprocess.run', side_effect=error):
                    audit = manager_service_audit('test-manager')
                self.assertFalse(audit['healthy'])
                self.assertEqual(audit['error'], type(error).__name__)
                self.assertFalse(self.acceptance(audit))

    def test_actual_finalizer_captures_manager_before_cleanup_even_when_query_fails(self):
        import ast
        import subprocess
        from tools.verification import control_real
        module = ast.parse(Path(control_real.__file__).read_text())
        main = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == 'main')
        finalizer = next(node for node in main.body if isinstance(node, ast.Try) and node.finalbody).finalbody
        def writes_report(node, key):
            return isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Subscript) and isinstance(target.slice, ast.Constant)
                and target.slice.value == key for target in node.targets)
        start = next(i for i, node in enumerate(finalizer) if writes_report(node, 'manager_service_before_stop'))
        stop = next(i for i, node in enumerate(finalizer) if writes_report(node, 'cleanup_errors'))
        code = compile(ast.Module(body=finalizer[start:stop + 1], type_ignores=[]), '<actual-finalizer>', 'exec')
        for fail_query in (False, True):
            with self.subTest(fail_query=fail_query):
                events = []
                def query(*args, **kwargs):
                    events.append('manager-query')
                    if fail_query:
                        raise subprocess.TimeoutExpired('systemctl', 10)
                    return subprocess.CompletedProcess([], 0, 'NRestarts=0\nActiveState=active\n', '')
                def stop_unit(index, command):
                    events.append(('stop', index))
                report = {}
                context = dict(report=report, manager_unit='test-manager',
                    manager_service_audit=control_real.manager_service_audit,
                    started_units=[(None, 'test-manager'), (0, 'test-worker')], run=stop_unit)
                with patch.object(control_real.subprocess, 'run', side_effect=query):
                    exec(code, context)
                self.assertEqual(events, ['manager-query', ('stop', 0), ('stop', None)])
                self.assertEqual(report['cleanup_errors'], [])
                self.assertEqual(report['manager_service_before_stop']['healthy'], not fail_query)
                self.assertEqual(self.acceptance(report['manager_service_before_stop']), not fail_query)
