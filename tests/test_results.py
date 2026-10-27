"""Staged execution and result delivery with distinct local roots and fake processes."""

from dataclasses import replace
from io import BytesIO
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import test_staging as staging_tests
from test_staging import file_entry
from lammps_manager.config import Execution
from lammps_manager.local import inspect_job
from lammps_manager.results import enumerate_results, output_limits, validate_results
from lammps_manager.store import Store
from lammps_manager.worker import Worker


class ResultTests(unittest.TestCase):
    setUp = staging_tests.StagedWorkerTests.setUp

    def test_large_compression_manifest_publishes_over_http_and_replays(self) -> None:
        import gzip
        import hashlib
        worker = self.worker()
        attempt_id = 'large-result-manifest'
        with Store(self.config.manager.database_path) as store:
            directory = Path(self.spec.work_directory) / attempt_id
            store.assign(self.job_id, attempt_id, 'a', str(directory))
            store.transition(attempt_id, 'running')
            store.transition(attempt_id, 'succeeded', exit_code=0,
                termination_evidence='Temporary byte fixture; no process launched')
        original = b'1 0.12345678901234567 0 0 0 1 0 0\n' * 40
        archive = gzip.compress(original, mtime=0)
        files, compression, raw_files = [], [], []
        for index in range(3000):
            raw = dict(path=f'work/frame-{index:06d}.dump', size=len(original),
                sha256=hashlib.sha256(original).hexdigest())
            item = dict(path=raw['path'] + '.gz', size=len(archive),
                sha256=hashlib.sha256(archive).hexdigest())
            raw_files.append(raw)
            files.append(item)
            compression.append(dict(**item, compression='gzip', original=raw))
        manifest = dict(files=files, omitted=[], compression=compression)
        raw_manifest = dict(files=raw_files, omitted=[])
        self.assertGreater(len(json.dumps(manifest).encode()), 1048576)
        self.assertLess(sum(item['size'] for item in files) + len(json.dumps(manifest).encode()),
            sum(item['size'] for item in raw_files) + len(json.dumps(raw_manifest).encode()))
        self.assertEqual(worker._result_request(attempt_id, 'manifest', manifest)['status'], 'uploading')
        # Use the actual upload verifier/spool without 3000 redundant HTTP
        # connections; manifest and commit exercise the authenticated transport.
        for index in range(len(files)):
            self.service.result_operation('a', attempt_id, str(index), stream=BytesIO(archive), length=len(archive))
        self.assertEqual(worker._result_request(attempt_id, 'commit', {})['status'], 'collected')
        self.assertEqual(worker._result_request(attempt_id, 'manifest', manifest)['status'], 'collected')
        self.assertEqual(worker._result_request(attempt_id, 'commit', {})['status'], 'collected')
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.result_collection(attempt_id)['manifest'], manifest)
            self.assertEqual(store.reservations(), ())
        for item in files:
            collected = directory / 'results' / item['path']
            self.assertEqual(hashlib.sha256(collected.read_bytes()).hexdigest(), item['sha256'])
            self.assertEqual(gzip.decompress(collected.read_bytes()), original)

    def worker(self) -> Worker:
        worker = Worker(self.worker_config, self.root / 'journal')
        self.addCleanup(worker.close)
        return worker

    def configure_process(self, exit_code: int = 0) -> None:
        launcher = self.root / 'launcher'
        launcher.write_text('#!/usr/bin/env python3\n'
                            'from pathlib import Path\nimport sys\n'
                            'p=Path("count"); p.write_text("one")\n'
                            'Path("generated").write_text("scientific output")\n'
                            'print("stdout evidence")\nprint("stderr evidence", file=sys.stderr)\n'
                            f'sys.exit({exit_code})\n')
        launcher.chmod(0o700)
        self.worker_config = replace(self.worker_config, execution=Execution('/bin/false', str(launcher)))

    def finish_process(self, worker: Worker) -> str:
        self.tick(worker)  # Prepare inputs.
        self.tick(worker)  # Consume durable launch claim.
        self.assertTrue(worker.futures)
        next(iter(worker.futures.values())).result(timeout=10)
        with Store(worker.database) as store:
            attempt, = store.job_attempts(self.job_id)
            self.assertIsNotNone(attempt.termination_evidence)
            return attempt.attempt_id

    def tick(self, worker: Worker) -> dict:
        """Await delivery explicitly in tests that inspect collection outcomes."""
        result = staging_tests.StagedWorkerTests.tick(self, worker)
        if worker.collection_future is not None:
            worker.collection_future.result(timeout=10)
        return result

    def test_trajectory_compression_failure_retains_data_then_delivers_verified_archive(self) -> None:
        import gzip
        import hashlib
        from lammps_manager.cleanup import cleanup_attempt
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        with Store(worker.database) as journal:
            runtime = Path(journal.attempt(attempt_id).directory)
        original = b'1 0.12345678901234567 -0 0 0 1e-300 0 0\n' * 100
        (runtime / 'work/trajectory.dump').write_bytes(original)
        (runtime / 'work/small.dump').write_bytes(b'x' * 200)
        (runtime / 'work/empty.dump').write_bytes(b'')
        with patch('lammps_manager.compression.gzip.open', return_value=BytesIO(b'corrupt decompression')):
            self.tick(worker)
        with Store(worker.database) as journal:
            failed = journal.result_collection(attempt_id)
            self.assertEqual(failed['status'], 'failed')
            self.assertIsNone(failed['manifest'])
            self.assertTrue((runtime / 'work/trajectory.dump').exists())
            self.assertFalse((runtime / 'work/trajectory.dump.gz').exists())
            with self.assertRaises(ValueError):
                cleanup_attempt(journal, attempt_id, self.worker_root, ())
        self.tick(worker)
        with Store(self.config.manager.database_path) as manager, Store(worker.database) as journal:
            result = manager.result_collection(attempt_id)
            self.assertEqual(result['status'], 'collected')
            metadata, = result['manifest']['compression']
            self.assertEqual(metadata['original']['sha256'], hashlib.sha256(original).hexdigest())
            publication = Path(manager.attempt(attempt_id).directory) / 'results'
            self.assertEqual(gzip.decompress((publication / metadata['path']).read_bytes()), original)
            self.assertNotIn('work/trajectory.dump', {entry['path'] for entry in result['manifest']['files']})
            self.assertEqual((publication / 'work/small.dump').read_bytes(), b'x' * 200)
            self.assertEqual((publication / 'work/empty.dump').read_bytes(), b'')
            self.assertFalse((runtime / 'work/small.dump.gz').exists())
            self.assertFalse((runtime / 'work/empty.dump.gz').exists())
            self.assertEqual(len(manager.job_attempts(self.job_id)), 1)
            cleanup_attempt(journal, attempt_id, self.worker_root, ())
            self.assertFalse(runtime.exists())
            self.assertTrue((publication / metadata['path']).exists())

    def test_blocked_collection_keeps_polling_and_recovered_process_control(self) -> None:
        from concurrent.futures import ThreadPoolExecutor
        from itertools import product
        import subprocess
        import sys
        import threading
        from lammps_manager.recovery import process_identity, session_members

        for index, (control, phase) in enumerate(product(('cancel', 'timeout'), ('enumerate', 'upload', 'commit'))):
            with self.subTest(control=control, phase=phase):
                if index:
                    self.doCleanups()
                    self.setUp()
                self.configure_process()
                worker = self.worker()
                completed_id = self.finish_process(worker)
                entered = threading.Event()
                release = threading.Event()
                original = worker._result_request

                def wait_for_release():
                    entered.set()
                    if not release.wait(10):
                        raise TimeoutError('Test collection was not released')

                def blocked(attempt_id, operation, *args):
                    if ((phase == 'upload' and operation.isdigit())
                            or (phase == 'commit' and operation == 'commit')) and not entered.is_set():
                        wait_for_release()
                    return original(attempt_id, operation, *args)

                def blocked_enumeration(*args):
                    if phase == 'enumerate':
                        wait_for_release()
                    return enumerate_results(*args)

                process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                           start_new_session=True)
                def cleanup_process():
                    if process.poll() is None:
                        process.kill()
                    process.wait(timeout=5)
                self.addCleanup(cleanup_process)
                runner = ThreadPoolExecutor(max_workers=1)
                try:
                    with patch.object(worker, '_result_request', side_effect=blocked), \
                            patch('lammps_manager.results.enumerate_results', side_effect=blocked_enumeration), \
                            patch.object(worker, '_collect_results', wraps=worker._collect_results) as collect:
                        first_tick = runner.submit(worker.tick)
                        self.assertTrue(entered.wait(5))
                        first_tick.result(timeout=2)
                        # Represent a process recovered after worker restart. Its
                        # execution is monitored by tick, not an executor thread.
                        recovered_spec = replace(self.spec, job_id='recovered',
                            work_directory=str(self.manager_root / 'recovered'),
                            resources=replace(self.spec.resources,
                                              time_limit_seconds=1 if control == 'timeout' else 3600))
                        Path(recovered_spec.work_directory).mkdir()
                        runtime = self.worker_root / 'recovered/attempt-recovered'
                        runtime.mkdir(parents=True)
                        identity = process_identity(process.pid)
                        (runtime / 'process.json').write_text(json.dumps(identity))
                        for database, directory in ((self.config.manager.database_path,
                                Path(recovered_spec.work_directory) / 'attempt-recovered'),
                                (worker.database, runtime)):
                            with Store(database) as store:
                                store.submit(recovered_spec)
                                store.assign('recovered', 'attempt-recovered', 'a', str(directory))
                                store.claim_launch('attempt-recovered')
                                store.transition('attempt-recovered', 'running')
                        if control == 'cancel':
                            with Store(self.config.manager.database_path) as store:
                                store.request_cancel('attempt-recovered', 'Cancel during upload')
                        else:
                            # An elapsed persisted launch timestamp makes timeout
                            # enforcement deterministic without sleeping.
                            identity['launched_at'] = '2000-01-01T00:00:00+00:00'
                            (runtime / 'process.json').write_text(json.dumps(identity))
                        with Store(self.config.manager.database_path) as store:
                            before = store.worker_records()['a']['snapshot']['heartbeat_at']
                        runner.submit(worker.tick).result(timeout=2)
                        runner.submit(worker.tick).result(timeout=2)
                        with Store(self.config.manager.database_path) as store:
                            self.assertGreater(store.worker_records()['a']['snapshot']['heartbeat_at'], before)
                            self.assertEqual(store.attempt('attempt-recovered').state, 'cancelled')
                            self.assertEqual(store.reservations(), ())
                            self.assertEqual(len(store.job_attempts(self.job_id)), 1)
                            self.assertNotEqual(store.result_collection(completed_id)['status'], 'collected')
                        self.assertFalse(session_members(process.pid))
                        self.assertEqual(collect.call_count, 1)
                        release.set()
                        worker.collection_future.result(timeout=5)
                finally:
                    release.set()
                    runner.shutdown(wait=True)
                with Store(self.config.manager.database_path) as store:
                    self.assertEqual(store.result_collection(completed_id)['status'], 'collected')

    def test_blocked_collection_allows_next_simulation_to_execute(self) -> None:
        import shutil
        import threading
        self.configure_process()
        worker = self.worker()
        first_id = self.finish_process(worker)
        entered = threading.Event()
        release = threading.Event()
        original = worker._result_request

        def blocked(attempt_id, operation, *args):
            if operation == 'commit':
                entered.set()
                if not release.wait(5):
                    raise TimeoutError('Test commit was not released')
            return original(attempt_id, operation, *args)

        try:
            with patch.object(worker, '_result_request', side_effect=blocked):
                worker.tick()
                self.assertTrue(entered.wait(5))
                second_spec = replace(self.spec, job_id='next-job',
                                      work_directory=str(self.manager_root / 'next-job'))
                shutil.copytree(Path(self.spec.work_directory) / 'inputs',
                                Path(second_spec.work_directory) / 'inputs')
                with Store(self.config.manager.database_path) as store:
                    store.submit(second_spec)
                staging_tests.StagedWorkerTests.tick(self, worker)  # Await input preparation independently of collection.
                worker.tick()  # Execute using the independent process executor.
                self.assertTrue(worker.futures)
                next(iter(worker.futures.values())).result(timeout=5)
                worker.tick()
                with Store(self.config.manager.database_path) as store:
                    self.assertEqual(store.job_state('next-job'), 'succeeded')
                    self.assertEqual(len(store.job_attempts('next-job')), 1)
                    self.assertNotEqual(store.result_collection(first_id)['status'], 'collected')
                release.set()
                worker.collection_future.result(timeout=5)
        finally:
            release.set()
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            second_attempt, = store.job_attempts('next-job')
            self.assertEqual(store.result_collection(second_attempt.attempt_id)['status'], 'collected')

    def test_background_collection_failure_surfaces_on_next_tick(self) -> None:
        self.configure_process()
        worker = self.worker()
        self.finish_process(worker)
        with patch.object(worker, '_collect_results', side_effect=RuntimeError('Unexpected collector failure')):
            worker.tick()
            with self.assertRaisesRegex(RuntimeError, 'Unexpected collector failure'):
                worker.collection_future.result(timeout=5)
        with self.assertRaisesRegex(RuntimeError, 'Unexpected collector failure'):
            worker.tick()

    def test_collection_only_visits_acknowledged_terminal_attempts(self) -> None:
        import threading
        self.configure_process()
        worker = self.worker()
        first_id = self.finish_process(worker)
        entered = threading.Event()
        release = threading.Event()
        original = worker._result_request

        def blocked(attempt_id, operation, *args):
            if operation == 'manifest':
                entered.set()
                if not release.wait(5):
                    raise TimeoutError('Test manifest was not released')
            return original(attempt_id, operation, *args)

        try:
            with patch.object(worker, '_result_request', side_effect=blocked):
                worker.tick()
                self.assertTrue(entered.wait(5))
                second_spec = replace(self.spec, job_id='not-reported',
                                      work_directory=str(self.manager_root / 'not-reported'))
                Path(second_spec.work_directory).mkdir()
                with Store(self.config.manager.database_path) as store:
                    store.submit(second_spec)
                    store.assign(second_spec.job_id, 'attempt-not-reported', 'a',
                                 str(Path(second_spec.work_directory) / 'attempt-not-reported'))
                with Store(worker.database) as store:
                    store.submit(second_spec)
                    store.assign(second_spec.job_id, 'attempt-not-reported', 'a',
                                 str(self.worker_root / 'not-reported/attempt-not-reported'))
                    store.transition('attempt-not-reported', 'failed', reason='Test prelaunch failure',
                                     termination_evidence='Launch did not start a process')
                release.set()
                worker.collection_future.result(timeout=5)
        finally:
            release.set()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.result_collection(first_id)['status'], 'collected')
            self.assertIsNone(store.result_collection('attempt-not-reported'))
        with Store(worker.database) as store:
            self.assertIsNone(store.result_collection('attempt-not-reported'))
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.result_collection('attempt-not-reported')['status'], 'collected')

    def test_close_joins_collection_before_unlock_and_restart_replays(self) -> None:
        from concurrent.futures import ThreadPoolExecutor
        import threading
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        entered = threading.Event()
        release = threading.Event()
        original = worker._result_request

        def blocked(identifier, operation, *args):
            if operation.isdigit():
                entered.set()
                if not release.wait(5):
                    raise TimeoutError('Test file was not released')
            return original(identifier, operation, *args)

        runner = ThreadPoolExecutor(max_workers=1)
        try:
            with patch.object(worker, '_result_request', side_effect=blocked):
                worker.tick()
                self.assertTrue(entered.wait(5))
                closing = runner.submit(worker.close)
                self.assertTrue(worker.collection_stop.wait(2))
                self.assertFalse(closing.done())
                with self.assertRaisesRegex(ValueError, 'already locked'):
                    Worker(self.worker_config, self.root / 'journal')
                release.set()
                closing.result(timeout=5)
        finally:
            release.set()
            runner.shutdown(wait=True)
        restarted = self.worker()
        self.tick(restarted)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')
            self.assertEqual(len(store.job_attempts(self.job_id)), 1)

    def test_separate_roots_success_and_nonzero(self) -> None:
        for code in (0, 7):
            with self.subTest(code=code):
                # Reuse one independent fixture per exit status.
                if code:
                    self.doCleanups()
                    self.setUp()
                self.configure_process(code)
                worker = self.worker()
                attempt_id = self.finish_process(worker)
                self.tick(worker)
                with Store(self.config.manager.database_path) as store:
                    attempt = store.attempt(attempt_id)
                    self.assertEqual(attempt.state, 'succeeded' if code == 0 else 'failed')
                    self.assertEqual(attempt.exit_code, code)
                    self.assertEqual(store.reservations(), ())
                    report = inspect_job(store, self.job_id)['attempts'][0]
                    self.assertEqual(report['result_collection']['status'], 'collected')
                    self.assertIn('stdout evidence', Path(report['stdout']).read_text())
                    self.assertIn('stderr evidence', Path(report['stderr']).read_text())
                    result = Path(report['results_directory'])
                    self.assertEqual((result / 'work/generated').read_text(), 'scientific output')
                    self.assertTrue(result.is_relative_to(self.manager_root))
                    self.assertEqual(store.specification(self.job_id), self.spec)
                for _ in range(3):
                    self.tick(worker)
                with Store(worker.database) as store:
                    self.assertEqual(len(store.job_attempts(self.job_id)), 1)
                if worker.cleanup_future:
                    worker.cleanup_future.result(timeout=5)
                worker._cleanup_results()
                with Store(worker.database) as store:
                    self.assertEqual(store.cleanup_record(attempt_id)['status'], 'cleaned')
                    self.assertFalse(Path(store.attempt(attempt_id).directory).exists())

    def test_rejected_commit_refreshes_inventory_and_repairs_spool(self) -> None:
        import hashlib
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        original = worker._result_request
        damaged = []

        def corrupt_before_commit(identity, operation, *args, **kwargs):
            if operation == 'commit' and not damaged:
                with Store(self.config.manager.database_path) as manager:
                    directory = Path(manager.attempt(identity).directory)
                    manifest = manager.result_collection(identity)['manifest']
                item = manifest['files'][0]
                spool = directory / '.results-upload' / hashlib.sha256(item['path'].encode()).hexdigest()
                spool.write_bytes(b'corrupt spool')
                damaged.append(item)
            return original(identity, operation, *args, **kwargs)

        with patch.object(worker, '_result_request', side_effect=corrupt_before_commit):
            self.tick(worker)
        with Store(worker.database) as journal:
            self.assertEqual(journal.result_collection(attempt_id)['status'], 'failed')
            self.assertIn('HTTP 400', journal.result_collection(attempt_id)['reason'])
            self.assertIsNone(journal._db.execute('SELECT 1 FROM result_commit_intents WHERE attempt_id=?',
                                                (attempt_id,)).fetchone())
        with patch.object(worker, '_result_request', wraps=original) as requests:
            self.tick(worker)
        self.assertEqual([call.args[1] for call in requests.call_args_list], ['manifest', '0', 'commit'])
        with Store(worker.database) as journal, Store(self.config.manager.database_path) as manager:
            self.assertEqual(journal.result_collection(attempt_id)['status'], 'collected')
            self.assertEqual(manager.result_collection(attempt_id)['status'], 'collected')
            self.assertEqual(len(manager.job_attempts(self.job_id)), 1)
            published = Path(manager.attempt(attempt_id).directory) / 'results' / damaged[0]['path']
            self.assertEqual(hashlib.sha256(published.read_bytes()).hexdigest(), damaged[0]['sha256'])

    def test_busy_commit_retains_intent_and_replays_commit_only(self) -> None:
        import sqlite3
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        original = self.service.result_operation

        def busy_commit(worker_id, identity, operation, *args, **kwargs):
            if operation == 'commit':
                raise sqlite3.OperationalError('database is locked')
            return original(worker_id, identity, operation, *args, **kwargs)

        with patch.object(self.service, 'result_operation', side_effect=busy_commit):
            self.tick(worker)
        with Store(worker.database) as journal:
            collection = journal.result_collection(attempt_id)
            self.assertEqual(collection['status'], 'failed')
            self.assertIn('Manager database busy', collection['reason'])
            self.assertIsNotNone(journal._db.execute('SELECT 1 FROM result_commit_intents WHERE attempt_id=?',
                                                   (attempt_id,)).fetchone())
        with patch.object(worker, '_result_request', wraps=worker._result_request) as requests:
            self.tick(worker)
        self.assertEqual([call.args[1] for call in requests.call_args_list], ['commit'])
        with Store(worker.database) as journal:
            self.assertEqual(journal.result_collection(attempt_id)['status'], 'collected')
            self.assertIsNone(journal._db.execute('SELECT 1 FROM result_commit_intents WHERE attempt_id=?',
                                                (attempt_id,)).fetchone())

    def test_lost_last_file_reply_resumes_only_verified_missing_files(self) -> None:
        self._check_lost_file_reply(at_end=True)

    def test_lost_first_file_reply_uploads_only_remaining_files(self) -> None:
        self._check_lost_file_reply(at_end=False)

    def _check_lost_file_reply(self, *, at_end: bool) -> None:
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        original = worker._result_request
        uploaded = []
        last = []
        def lose_file_reply(identity, operation, *args, **kwargs):
            reply = original(identity, operation, *args, **kwargs)
            if operation == 'manifest':
                last.append(str(len(args[0]['files']) - 1))
            if operation.isdigit():
                uploaded.append(operation)
                if operation == (last[0] if at_end else '0'):
                    raise ConnectionError('Lost verified file response')
            return reply
        with patch.object(worker, '_result_request', side_effect=lose_file_reply):
            self.tick(worker)
        self.assertTrue(uploaded)
        received = set(uploaded)
        def track(identity, operation, *args, **kwargs):
            if operation.isdigit():
                self.assertNotIn(operation, received)
                uploaded.append(operation)
            return original(identity, operation, *args, **kwargs)
        with patch.object(worker, '_result_request', side_effect=track) as replay:
            self.tick(worker)
        self.assertEqual(len(uploaded), len(set(uploaded)))
        if at_end:
            self.assertEqual([call.args[1] for call in replay.call_args_list], ['manifest', 'commit'])
        with Store(worker.database) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')

    def test_resume_inventory_survives_restart_and_rejects_changed_spool_bytes(self) -> None:
        import hashlib
        from lammps_manager.manager import ManagerService
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        with patch.object(worker, '_collect_results'):
            self.tick(worker)
        item = file_entry('stdout', b'abc')
        manifest = dict(files=[dict(path=item.path, size=item.size, sha256=item.sha256)], omitted=[])
        reply = self.service.result_operation('a', attempt_id, 'manifest', manifest)
        self.assertEqual(reply['verified_files'], [])
        self.service.result_operation('a', attempt_id, '0', stream=BytesIO(b'abc'), length=3)
        recovered = ManagerService(self.config, self.tokens)
        self.addCleanup(recovered.close)
        self.assertEqual(recovered.result_operation('a', attempt_id, 'manifest', manifest)['verified_files'], [0])
        with Store(self.config.manager.database_path) as store:
            directory = Path(store.attempt(attempt_id).directory)
        spool = directory / '.results-upload' / hashlib.sha256(b'stdout').hexdigest()
        spool.write_bytes(b'xxx')
        self.assertEqual(recovered.result_operation('a', attempt_id, 'manifest', manifest)['verified_files'], [])
        with self.assertRaises(ValueError):
            recovered.result_operation('b', attempt_id, 'manifest', manifest)

    def test_paused_maintenance_keeps_reconciliation_without_dispatch(self) -> None:
        from dataclasses import asdict
        import time
        from lammps_manager.scheduler import WorkerSnapshot
        from lammps_manager.manager import ManagerService
        service = object.__new__(ManagerService)
        service.config, service.tokens = self.config, self.tokens
        service.last_schedule, service._run_cursor = 0., ''
        capacity = self.config.workers[0]
        now = time.time()
        snapshot = WorkerSnapshot(capacity, capacity.cpu_slots, 32, now, now, 0, 32, paused=True)
        with Store(self.config.manager.database_path) as store:
            store.set_worker(capacity.id, paused=True, snapshot=asdict(snapshot))
        with patch('lammps_manager.manager.schedule', side_effect=AssertionError('Paused scan')), \
             patch('lammps_manager.segments.refresh', return_value='') as refresh, \
             patch('lammps_manager.monitoring.refresh_alerts') as alerts:
            service.expire_workers()
        refresh.assert_called_once()
        self.assertEqual(refresh.call_args.kwargs['limit'], 16)
        alerts.assert_called_once()

    def test_artifact_locks_isolate_attempts_and_retire_waiters(self) -> None:
        from concurrent.futures import ThreadPoolExecutor
        import threading
        entered = threading.Event()
        def unrelated():
            with self.service._attempt_lock('other'):
                entered.set()
        with self.service._attempt_lock('blocked'):
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(unrelated)
                self.assertTrue(entered.wait(2))
                future.result(timeout=2)
        self.assertEqual(self.service._artifact_locks, {})

    def test_lost_ack_restart_replays_without_execution(self) -> None:
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        original = worker._result_request

        def lose_commit(*args, **kwargs):
            result = original(*args, **kwargs)
            if args[1] == 'commit':
                raise ConnectionError('Lost acknowledgement')
            return result

        with patch.object(worker, '_result_request', side_effect=lose_commit):
            self.tick(worker)
        with Store(worker.database) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'failed')
        worker.close()
        # Manager service recreation retains the same SQLite and publication.
        from lammps_manager.manager import ManagerService
        recovered = ManagerService(self.config, self.tokens)
        self.addCleanup(recovered.close)
        self.service.result_operation = recovered.result_operation
        restarted = self.worker()
        with patch.object(restarted, '_execute', side_effect=AssertionError('Duplicate launch')), \
             patch.object(restarted, '_result_request', wraps=restarted._result_request) as requests:
            self.tick(restarted)
            self.assertEqual([call.args[1] for call in requests.call_args_list], ['commit'])
        with Store(restarted.database) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')
            self.assertEqual(len(store.job_attempts(self.job_id)), 1)

    def test_slow_commit_and_lost_ack_replay_use_configured_timeout(self) -> None:
        import time
        from lammps_manager.results import commit_results

        self.configure_process()
        self.worker_config = replace(self.worker_config, result_request_timeout_seconds=20)
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        original = worker._result_request

        def slow_commit(*args):
            time.sleep(10.2)  # Exceed the former hard-coded socket timeout.
            return commit_results(*args)

        def lose_ack(*args, **kwargs):
            result = original(*args, **kwargs)
            if args[1] == 'commit':
                raise ConnectionError('Lost acknowledgement after slow commit')
            return result

        with patch('lammps_manager.results.commit_results', side_effect=slow_commit) as commit:
            with patch.object(worker, '_result_request', side_effect=lose_ack):
                worker.tick()
                worker.collection_future.result(timeout=25)
            with Store(worker.database) as store:
                self.assertIn('Lost acknowledgement', store.result_collection(attempt_id)['reason'])
            with Store(self.config.manager.database_path) as store:
                self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')
            with patch.object(worker, '_execute', side_effect=AssertionError('Duplicate launch')):
                worker.tick()
                worker.collection_future.result(timeout=25)
            self.assertEqual(commit.call_count, 2)
        for database in (worker.database, self.config.manager.database_path):
            with Store(database) as store:
                self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')
                self.assertEqual(len(store.job_attempts(self.job_id)), 1)

    def test_configured_result_timeout_remains_retryable(self) -> None:
        import threading
        from lammps_manager.results import commit_results

        self.configure_process()
        self.worker_config = replace(self.worker_config, result_request_timeout_seconds=.1)
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        release = threading.Event()
        finished = threading.Event()

        def blocked_commit(*args):
            try:
                if not release.wait(5):
                    raise AssertionError('Test did not release commit')
                return commit_results(*args)
            finally:
                finished.set()

        try:
            with patch('lammps_manager.results.commit_results', side_effect=blocked_commit):
                worker.tick()
                worker.collection_future.result(timeout=5)
                with Store(worker.database) as store:
                    collection = store.result_collection(attempt_id)
                    self.assertEqual(collection['status'], 'failed')
                    self.assertIn('TimeoutError', collection['reason'])
                release.set()
                self.assertTrue(finished.wait(5))
        finally:
            release.set()
        worker.config = replace(worker.config, result_request_timeout_seconds=20)
        self.tick(worker)
        with Store(worker.database) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')
            self.assertEqual(len(store.job_attempts(self.job_id)), 1)

    def test_interrupted_upload_and_disk_failure_retry(self) -> None:
        self.configure_process(9)
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        from lammps_manager.results import upload_file
        with patch('lammps_manager.results.upload_file', side_effect=OSError('disk full')):
            self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            attempt = store.attempt(attempt_id)
            self.assertEqual(attempt.state, 'failed')
            self.assertEqual(store.reservations(), ())
            self.assertNotEqual(store.result_collection(attempt_id)['status'], 'collected')
            directory = Path(attempt.directory)
            self.assertFalse((directory / 'results').exists())
            manifest = store.result_collection(attempt_id)['manifest']
        with self.assertRaises(ValueError):
            upload_file(directory, file_entry('stdout', b'abcd'), BytesIO(b'ab'))
        self.assertFalse((directory / 'results').exists())
        worker.close()
        restarted = self.worker()
        with patch.object(restarted, '_execute', side_effect=AssertionError('Duplicate launch')):
            self.tick(restarted)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')
            self.assertEqual(store.result_collection(attempt_id)['manifest'], manifest)

    def test_authorization_conflict_limits_and_missing_files(self) -> None:
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        with Store(worker.database) as store:
            directory = Path(store.attempt(attempt_id).directory)
        (directory / 'stderr').unlink()
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            collection = store.result_collection(attempt_id)
            self.assertEqual(collection['status'], 'collected')
            self.assertIsNone(inspect_job(store, self.job_id)['attempts'][0]['stderr'])
            self.assertTrue(any(item['path'] == 'stderr' for item in collection['manifest']['omitted']))
        with self.assertRaises(ValueError):
            self.service.result_operation('b', attempt_id, 'commit', {})
        conflicting = {**collection['manifest'], 'omitted': []}
        with self.assertRaises(ValueError):
            self.service.result_operation('a', attempt_id, 'manifest', conflicting)
        for update in (dict(max_files=1), dict(max_file_bytes=1), dict(max_total_bytes=1)):
            with self.subTest(update=update), self.assertRaises(ValueError):
                validate_results(collection['manifest'], {**output_limits(self.config.storage), **update})
        (directory / 'work/escape').symlink_to(self.root / 'a.token')
        with self.assertRaises(ValueError):
            enumerate_results(directory, output_limits(self.config.storage))

    def test_running_attempt_rejects_results_and_retains_reservation(self) -> None:
        worker = self.worker()
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            attempt, = store.job_attempts(self.job_id)
        with self.assertRaises(ValueError):
            self.service.result_operation('a', attempt.attempt_id, 'manifest', {})
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(len(store.reservations()), 1)
            self.assertIsNone(store.result_collection(attempt.attempt_id))

    def test_publication_before_ack_and_repeated_sync_failure(self) -> None:
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        from lammps_manager.staging import sync_directory
        with Store(self.config.manager.database_path) as store:
            directory = Path(store.attempt(attempt_id).directory)

        def fail_sync(path):
            if path == directory and (directory / 'results').exists():
                raise OSError('Injected publication ancestor sync failure')
            sync_directory(path)

        with patch('lammps_manager.staging.sync_directory', side_effect=fail_sync):
            for _ in range(2):
                self.tick(worker)
                self.assertTrue((directory / 'results').exists())
                with Store(self.config.manager.database_path) as store:
                    self.assertNotEqual(store.result_collection(attempt_id)['status'], 'collected')
                    self.assertIsNone(inspect_job(store, self.job_id)['attempts'][0]['stdout'])
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')

    def test_restart_mid_transfer_and_publication_database_failure(self) -> None:
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        original = worker._result_request
        sent = []

        def interrupt(attempt, operation, *args):
            if operation.isdigit():
                sent.append(operation)
                if len(sent) == 2:
                    raise ConnectionError('Interrupted after first file')
            return original(attempt, operation, *args)

        with patch.object(worker, '_result_request', side_effect=interrupt):
            self.tick(worker)
        self.assertEqual(len(sent), 2)
        with Store(self.config.manager.database_path) as store:
            directory = Path(store.attempt(attempt_id).directory)
            self.assertFalse((directory / 'results').exists())
        worker.close()
        from lammps_manager.manager import ManagerService
        recovered = ManagerService(self.config, self.tokens)
        self.addCleanup(recovered.close)
        self.service.result_operation = recovered.result_operation
        restarted = self.worker()
        original_save = Store.set_result_collection

        def fail_ack(store, identifier, status, *args, **kwargs):
            if str(store._db.execute('PRAGMA database_list').fetchone()[2]) == self.config.manager.database_path and status == 'collected':
                raise OSError('Injected database acknowledgement failure')
            return original_save(store, identifier, status, *args, **kwargs)

        with patch.object(Store, 'set_result_collection', new=fail_ack):
            self.tick(restarted)
        self.assertTrue((directory / 'results').exists())
        with Store(self.config.manager.database_path) as store:
            self.assertNotEqual(store.result_collection(attempt_id)['status'], 'collected')
        self.tick(restarted)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')
            self.assertEqual(len(store.job_attempts(self.job_id)), 1)

    def test_output_limit_failure_and_missing_bound_file_remain_visible(self) -> None:
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        with patch.object(worker, 'config', replace(worker.config, max_output_file_bytes=1)):
            self.tick(worker)
        self.tick(worker)  # Report failure, then retry enumeration with restored limits.
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.result_collection(attempt_id)['status'], 'collected')
        # A second fixture tests loss of a file after the manifest was frozen.
        self.doCleanups()
        self.setUp()
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        with patch.object(worker, '_result_request', side_effect=ConnectionError('offline')):
            self.tick(worker)
        with Store(worker.database) as store:
            directory = Path(store.attempt(attempt_id).directory)
        (directory / 'work/generated').unlink()
        self.tick(worker)
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            collection = store.result_collection(attempt_id)
            self.assertEqual(collection['status'], 'failed')
            self.assertIn('FileNotFoundError', collection['reason'])
            self.assertEqual(store.reservations(), ())
            self.assertIsNone(inspect_job(store, self.job_id)['attempts'][0]['stdout'])

    def test_unsafe_manifest_and_corrupt_upload_rejected(self) -> None:
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        # Report terminal process state while keeping collection offline.
        with patch.object(worker, '_collect_results'):
            self.tick(worker)
        from dataclasses import asdict
        for path in ('../escape', '/escape', 'work/a/../b', 'unexpected'):
            manifest = dict(files=[asdict(file_entry(path, b'x'))], omitted=[])
            with self.subTest(path=path), self.assertRaises(ValueError):
                self.service.result_operation('a', attempt_id, 'manifest', manifest)
        manifest = dict(files=[asdict(file_entry('stdout', b'abc'))], omitted=[])
        self.service.result_operation('a', attempt_id, 'manifest', manifest)
        for data in (b'ab', b'xxx', b'abcd'):
            with self.subTest(data=data), self.assertRaises(ValueError):
                self.service.result_operation('a', attempt_id, '0', stream=BytesIO(data), length=3)
        with Store(self.config.manager.database_path) as store:
            directory = Path(store.attempt(attempt_id).directory)
            self.assertFalse((directory / 'results').exists())
            self.assertNotEqual(store.result_collection(attempt_id)['status'], 'collected')

    def test_launch_claim_crash_and_prelaunch_cancellation(self) -> None:
        self.configure_process()
        worker = self.worker()
        self.tick(worker)
        with Store(worker.database) as store:
            attempt, = store.job_attempts(self.job_id)
            self.assertTrue(store.claim_launch(attempt.attempt_id))
        worker.close()
        restarted = self.worker()
        with patch.object(restarted, '_execute', side_effect=AssertionError('Duplicate launch')):
            self.tick(restarted)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(self.job_id), 'unknown')
            self.assertEqual(len(store.reservations()), 1)
        self.doCleanups()
        self.setUp()
        worker = self.worker()
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            attempt, = store.job_attempts(self.job_id)
            store.request_cancel(attempt.attempt_id)
        self.tick(worker)
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(self.job_id), 'cancelled')
            self.assertEqual(store.result_collection(attempt.attempt_id)['status'], 'collected')
            self.assertEqual(store.reservations(), ())

    def test_recovered_staged_process_cancels_before_result_collection(self) -> None:
        import subprocess
        import sys
        from lammps_manager.recovery import process_identity, session_members
        worker = self.worker()
        self.tick(worker)
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                   start_new_session=True)

        def cleanup():
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)

        self.addCleanup(cleanup)
        with Store(worker.database) as store:
            attempt, = store.job_attempts(self.job_id)
            store.claim_launch(attempt.attempt_id)
            (Path(attempt.directory) / 'process.json').write_text(json.dumps(process_identity(process.pid)))
            store.transition(attempt.attempt_id, 'running')
        worker.close()
        restarted = self.worker()
        self.tick(restarted)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(self.job_id), 'running')
            self.assertEqual(len(store.reservations()), 1)
            self.assertIsNone(store.result_collection(attempt.attempt_id))
            store.request_cancel(attempt.attempt_id)
        self.tick(restarted)  # Receive durable cancellation.
        self.tick(restarted)  # Reconcile and terminate the complete saved session.
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(self.job_id), 'cancelled')
            self.assertEqual(store.reservations(), ())
            self.assertEqual(store.result_collection(attempt.attempt_id)['status'], 'collected')
        self.assertEqual(session_members(process.pid), ())
        process.wait(timeout=5)

    def test_cli_distinguishes_process_success_and_pending_delivery(self) -> None:
        from contextlib import redirect_stdout
        from io import StringIO
        from lammps_manager.cli import main
        self.configure_process()
        worker = self.worker()
        self.finish_process(worker)
        with patch.object(worker, '_collect_results'):
            self.tick(worker)
        for command in ('status', 'inspect'):
            output = StringIO()
            args = [command, '--config', 'unused'] + ([self.job_id] if command == 'inspect' else [])
            with patch('lammps_manager.cli.load_config', return_value=self.config), redirect_stdout(output):
                self.assertEqual(main(args), 0)
            payload = json.loads(output.getvalue())
            entry = payload['jobs'][0] if command == 'status' else payload['attempts'][0]
            self.assertEqual(entry['state'], 'succeeded')
            self.assertEqual(entry['result_collection']['status'], 'pending')
            if command == 'inspect':
                self.assertIsNone(entry['stdout'])
                self.assertIsNone(entry['results_directory'])
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(inspect_job(store, self.job_id)['attempts'][0]['result_collection']['status'], 'collected')

    def test_result_file_cache_keeps_live_authorization_and_limit_checks(self) -> None:
        from dataclasses import asdict
        from lammps_manager.results import validate_results
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        with patch.object(worker, '_collect_results'):
            self.tick(worker)
        manifest = dict(files=[asdict(file_entry('stdout', b'abc'))], omitted=[])
        self.service.result_operation('a', attempt_id, 'manifest', manifest)
        with patch('lammps_manager.results.validate_results', wraps=validate_results) as validate:
            for _ in range(3):
                self.service.result_operation('a', attempt_id, '0', stream=BytesIO(b'abc'), length=3)
            self.assertEqual(validate.call_count, 0)
            with self.assertRaises(ValueError):
                self.service.result_operation('b', attempt_id, '0', stream=BytesIO(b'abc'), length=3)
            with patch.object(self.service, 'config', replace(self.config, storage=replace(
                    self.config.storage, max_output_file_bytes=1))):
                with self.assertRaises(ValueError):
                    self.service.result_operation('a', attempt_id, '0', stream=BytesIO(b'abc'), length=3)
            self.assertEqual(validate.call_count, 1)

    def test_launch_revalidates_auxiliary_inputs(self) -> None:
        self.configure_process()
        worker = self.worker()
        self.tick(worker)
        with Store(worker.database) as store:
            attempt, = store.job_attempts(self.job_id)
            (Path(attempt.directory) / 'inputs/potential').write_text('tampered')
        self.tick(worker)
        next(iter(worker.futures.values())).result(timeout=5)
        self.tick(worker)
        with Store(self.config.manager.database_path) as store:
            final = store.attempt(attempt.attempt_id)
            self.assertEqual(final.state, 'failed')
            self.assertEqual(final.termination_evidence, 'Launch did not start a process')
            self.assertEqual(store.result_collection(attempt.attempt_id)['status'], 'collected')
        self.assertFalse((Path(attempt.directory) / 'process.json').exists())

    def test_unreadable_output_tree_is_not_silently_omitted(self) -> None:
        self.configure_process()
        worker = self.worker()
        attempt_id = self.finish_process(worker)
        with Store(worker.database) as store:
            directory = Path(store.attempt(attempt_id).directory)

        def unreadable(*args, **kwargs):
            kwargs['onerror'](PermissionError('Cannot enumerate output subtree'))
            return iter(())

        with patch('lammps_manager.results.os.walk', side_effect=unreadable):
            with self.assertRaises(PermissionError):
                enumerate_results(directory, output_limits(self.config.storage))
        import os
        os.mkfifo(directory / 'work/unsafe')
        with self.assertRaises(ValueError):
            enumerate_results(directory, output_limits(self.config.storage))

    def test_manager_result_capability_is_required_for_launch(self) -> None:
        self.configure_process()
        worker = self.worker()
        self.tick(worker)
        poll = self.service._reply
        for field in ('staged_launch_enabled', 'results_protocol'):
            def old_manager(*args):
                response = poll(*args)
                response.pop(field)
                return response

            with patch.object(self.service, '_reply', side_effect=old_manager):
                self.tick(worker)
            self.assertFalse(worker.futures)
            with Store(worker.database) as store:
                attempt, = store.job_attempts(self.job_id)
                self.assertEqual(attempt.state, 'assigned')
                self.assertFalse(store.launch_claimed(attempt.attempt_id))
                self.assertEqual(len(store.reservations()), 1)


if __name__ == '__main__':
    unittest.main()
