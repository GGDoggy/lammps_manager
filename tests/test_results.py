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
        self.assertIsNotNone(worker.future)
        worker.future.result(timeout=10)
        with Store(worker.database) as store:
            attempt, = store.job_attempts(self.job_id)
            self.assertIsNotNone(attempt.termination_evidence)
            return attempt.attempt_id

    def tick(self, worker: Worker) -> dict:
        """Await delivery explicitly in tests that inspect collection outcomes."""
        result = worker.tick()
        if worker.collection_future is not None:
            worker.collection_future.result(timeout=10)
        return result

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
                worker.tick()  # Accept and stage while earlier commit is blocked.
                worker.tick()  # Execute using the independent process executor.
                self.assertIsNotNone(worker.future)
                worker.future.result(timeout=5)
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
                    self.assertTrue((Path(store.attempt(attempt_id).directory) / 'work/generated').exists())

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
        with patch.object(restarted, '_execute', side_effect=AssertionError('Duplicate launch')):
            self.tick(restarted)
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
            entry = payload[0] if command == 'status' else payload['attempts'][0]
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
        worker.future.result(timeout=5)
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
        poll = self.service._poll
        for field in ('staged_launch_enabled', 'results_protocol'):
            def old_manager(*args):
                response = poll(*args)
                response.pop(field)
                return response

            with patch.object(self.service, '_poll', side_effect=old_manager):
                self.tick(worker)
            self.assertIsNone(worker.future)
            with Store(worker.database) as store:
                attempt, = store.job_attempts(self.job_id)
                self.assertEqual(attempt.state, 'assigned')
                self.assertFalse(store.launch_claimed(attempt.attempt_id))
                self.assertEqual(len(store.reservations()), 1)


if __name__ == '__main__':
    unittest.main()
