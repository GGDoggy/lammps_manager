"""Temporary-file cleanup safety, durable eligibility and background integration."""

from dataclasses import replace
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from lammps_manager.cleanup import cleanup_attempt, prepare_directory
from lammps_manager.models import InputFile, JobSpecification, Resources
from lammps_manager.store import Store, StoreError
import test_results as results_tests


class CleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / 'worker'
        self.root.mkdir()
        self.database = self.base / 'journal.sqlite'
        self.store = Store(self.database)
        self.addCleanup(self.store.close)
        spec = JobSpecification('job', str(self.base / 'manager/job'), 'input', '0' * 64,
                                Resources(), 'staged', (InputFile('input', 0, '0' * 64),))
        self.store.submit(spec)
        self.target = self.root / 'job/attempt'
        self.store.assign('job', 'attempt', 'worker', str(self.target))
        prepare_directory(self.store, 'attempt', self.root)
        (self.target / 'output').write_text('preserve until acknowledgement')
        self.store.claim_launch('attempt')

    def acknowledge(self) -> None:
        self.store.transition('attempt', 'failed', reason='Test process ended', termination_evidence='No live launch session')
        self.store.set_result_collection('attempt', 'collected', {'files': [], 'omitted': []})

    def clean(self, budget: int = 256, protected: tuple = ()) -> None:
        cleanup_attempt(self.store, 'attempt', self.root, protected, budget)

    def test_termination_and_acknowledgement_gate(self) -> None:
        for state in ('assigned', 'unknown'):
            with self.subTest(state=state):
                if state == 'unknown':
                    self.store.transition('attempt', 'unknown', reason='Disconnected')
                with self.assertRaises(StoreError):
                    self.clean()
                self.assertTrue((self.target / 'output').exists())
                self.assertTrue(self.store.reservations())
        self.store.transition('attempt', 'failed', reason='Test process ended', termination_evidence='Confirmed termination')
        for status in ('pending', 'uploading', 'failed'):
            self.store.set_result_collection('attempt', status, {'files': [], 'omitted': []})
            with self.assertRaises(StoreError):
                self.clean()
            self.assertTrue(self.target.exists())
        self.store.set_result_collection('attempt', 'collected')
        self.clean()
        self.assertFalse(self.target.exists())
        self.assertTrue(self.store.launch_claimed('attempt'))
        self.assertEqual(self.store.attempt('attempt').state, 'failed')
        self.assertEqual(self.store.cleanup_record('attempt')['status'], 'cleaned')
        self.clean()

    def test_bounded_partial_cleanup_and_restart(self) -> None:
        self.acknowledge()
        for i in range(8):
            (self.target / f'file-{i}').write_text('output')
        self.clean(budget=2)
        self.assertEqual(len(list(self.target.iterdir())), 7)
        self.assertEqual(self.store.cleanup_record('attempt')['status'], 'deleting')
        with Store(self.database) as restarted:
            cleanup_attempt(restarted, 'attempt', self.root, ())
            self.assertEqual(restarted.cleanup_record('attempt')['status'], 'cleaned')
            self.assertEqual(restarted.result_collection('attempt')['status'], 'collected')
            self.assertTrue(restarted.launch_claimed('attempt'))

    def test_nested_deletion_budget_includes_parent_directories(self) -> None:
        self.acknowledge()
        (self.target / 'output').unlink()
        (self.target / 'nested').mkdir()
        (self.target / 'nested/file').write_text('one')
        self.clean(budget=1)
        self.assertTrue((self.target / 'nested').is_dir())
        self.assertFalse((self.target / 'nested/file').exists())
        self.clean(budget=1)
        self.assertFalse(self.target.exists())

    def test_bounded_candidate_rotation_skips_cleaned_and_unacknowledged(self) -> None:
        self.acknowledge()
        for number in range(35):
            job = f'job-{number}'
            identifier = f'attempt-{number}'
            spec = replace(self.store.specification('job'), job_id=job)
            self.store.submit(spec)
            self.store.assign(job, identifier, 'worker', str(self.root / job / identifier))
            prepare_directory(self.store, identifier, self.root)
            if number == 34:
                continue  # Unacknowledged work must never be selected.
            self.store.transition(identifier, 'failed', reason='Ended', termination_evidence='Confirmed')
            self.store.set_result_collection(identifier, 'collected', {'files': [], 'omitted': []})
        first = self.store.cleanup_candidates()
        self.assertEqual(len(first), 32)
        second = self.store.cleanup_candidates(first[-1][0])
        self.assertEqual(len(second), 3)
        self.assertFalse(set(first) & set(second))
        self.clean()
        self.assertNotIn('attempt', [identifier for _, identifier in self.store.cleanup_candidates()])

    def test_replacement_with_symlink_between_stat_and_open_is_rejected(self) -> None:
        self.acknowledge()
        nested = self.target / 'nested'
        nested.mkdir()
        (nested / 'own').write_text('own')
        outside = self.base / 'other-attempt'
        outside.mkdir()
        (outside / 'sentinel').write_text('unrelated')
        original = os.open
        def replace_before_open(path, flags, *args, **kwargs):
            if path == 'nested':
                nested.rename(self.target / 'saved')
                nested.symlink_to(outside, target_is_directory=True)
            return original(path, flags, *args, **kwargs)
        with patch('lammps_manager.cleanup.os.open', side_effect=replace_before_open):
            self.clean()
        self.assertEqual((outside / 'sentinel').read_text(), 'unrelated')
        self.assertEqual(self.store.cleanup_record('attempt')['status'], 'failed')

    def test_permission_failure_and_retry(self) -> None:
        self.acknowledge()
        with patch('lammps_manager.cleanup.os.unlink', side_effect=PermissionError('Injected denial')):
            self.clean()
        self.assertIn('Injected denial', self.store.cleanup_record('attempt')['reason'])
        self.assertEqual(self.store.result_collection('attempt')['status'], 'collected')
        self.assertEqual(self.store.attempt('attempt').state, 'failed')
        self.clean()
        self.assertFalse(self.target.exists())

    def test_crash_after_rmdir_before_tombstone(self) -> None:
        self.acknowledge()
        original = self.store.set_cleanup
        def fail(identifier, status, reason=None):
            if status == 'cleaned':
                raise RuntimeError('Injected crash')
            original(identifier, status, reason)
        with patch.object(self.store, 'set_cleanup', side_effect=fail), self.assertRaises(RuntimeError):
            self.clean()
        self.assertFalse(self.target.exists())
        self.assertEqual(self.store.cleanup_record('attempt')['status'], 'deleting')
        self.clean()
        self.assertEqual(self.store.cleanup_record('attempt')['status'], 'cleaned')

    def test_replaced_attempt_job_and_root_retain_other_files(self) -> None:
        self.acknowledge()
        for path in (self.target, self.target.parent, self.root):
            with self.subTest(path=path):
                saved = path.with_name(path.name + '-saved')
                path.rename(saved)
                path.mkdir()
                sentinel = path / 'other-attempt-output'
                sentinel.write_text('unrelated')
                self.clean()
                self.assertEqual(sentinel.read_text(), 'unrelated')
                self.assertEqual(self.store.cleanup_record('attempt')['status'], 'failed')
                sentinel.unlink()
                path.rmdir()
                saved.rename(path)
        self.clean()
        self.assertFalse(self.target.exists())

    def test_symlinks_hardlinks_special_files_and_protected_paths(self) -> None:
        self.acknowledge()
        external = self.base / 'credentials'
        external.write_text('secret')
        unsafe = self.target / 'unsafe'
        for kind in ('symlink', 'hardlink', 'fifo', 'directory-link'):
            with self.subTest(kind=kind):
                if kind == 'symlink':
                    unsafe.symlink_to(external)
                elif kind == 'directory-link':
                    unsafe.symlink_to(self.base, target_is_directory=True)
                elif kind == 'hardlink':
                    os.link(external, unsafe)
                else:
                    os.mkfifo(unsafe)
                self.clean()
                self.assertEqual(external.read_text(), 'secret')
                self.assertEqual(self.store.cleanup_record('attempt')['status'], 'failed')
                unsafe.unlink()
        protected = self.target / 'journal'
        protected.write_text('journal')
        self.clean(protected=(protected,))
        self.assertEqual(protected.read_text(), 'journal')
        self.assertIn('protected', self.store.cleanup_record('attempt')['reason'])
        protected.unlink()
        self.clean()
        self.assertEqual(external.read_text(), 'secret')

    def test_no_adoption_of_legacy_directory_or_shared_output(self) -> None:
        legacy = replace(self.store.specification('job'), job_id='legacy')
        self.store.submit(legacy)
        directory = self.root / 'legacy/old'
        directory.mkdir(parents=True)
        (directory / 'output').write_text('legacy')
        self.store.assign('legacy', 'old', 'worker', str(directory))
        prepare_directory(self.store, 'old', self.root)
        self.assertIsNone(self.store.cleanup_record('old'))
        cleanup_attempt(self.store, 'old', self.root, ())
        self.assertEqual((directory / 'output').read_text(), 'legacy')
        shared = replace(legacy, job_id='shared', storage_mode='shared', input_manifest=())
        self.store.submit(shared)
        self.store.assign('shared', 'shared-attempt', 'worker', str(self.base / 'shared-attempt'))
        with self.assertRaises(StoreError):
            self.store.own_staged_directory('shared-attempt', [[1, 1], [1, 2], [1, 3]])

    def test_manager_job_overlap_is_never_deleted(self) -> None:
        self.acknowledge()
        for manager_directory in (self.target.parent, self.target / 'results'):
            with self.subTest(manager_directory=manager_directory):
                cleanup_attempt(self.store, 'attempt', self.root, (),
                                manager_directory=manager_directory)
                self.assertTrue((self.target / 'output').exists())
                self.assertIn('manager', self.store.cleanup_record('attempt')['reason'])

    def test_unsafe_runtime_path_and_ancestor_symlink(self) -> None:
        self.acknowledge()
        self.store._db.execute("UPDATE attempts SET directory=? WHERE attempt_id='attempt'",
                               (str(self.base),))
        self.clean()
        self.assertEqual(self.store.cleanup_record('attempt')['status'], 'failed')
        self.assertTrue((self.target / 'output').exists())
        self.store._db.execute("UPDATE attempts SET directory=? WHERE attempt_id='attempt'",
                               (str(self.target),))
        saved = self.target.parent.with_name('saved-job')
        self.target.parent.rename(saved)
        self.target.parent.symlink_to(saved, target_is_directory=True)
        self.clean()
        self.assertTrue((saved / 'attempt/output').exists())
        self.assertEqual(self.store.cleanup_record('attempt')['status'], 'failed')


class CleanupIntegrationTests(unittest.TestCase):
    setUp = results_tests.ResultTests.setUp
    worker = results_tests.ResultTests.worker
    configure_process = results_tests.ResultTests.configure_process
    finish_process = results_tests.ResultTests.finish_process
    tick = results_tests.ResultTests.tick

    def test_directory_preparation_failure_retries_after_poll_and_restart(self) -> None:
        import errno
        from lammps_manager import cleanup

        cases = ((operation, restart) for operation in ('mkdir', 'open', 'fsync')
                 for restart in (False, True))
        for index, (operation, restart) in enumerate(cases):
            with self.subTest(operation=operation, restart=restart):
                if index:
                    self.doCleanups()
                    self.setUp()
                self.configure_process()
                worker = self.worker()
                original = cleanup.prepare_directory

                def fail_preparation(*args):
                    filesystem_call = getattr(cleanup.os, operation)

                    def fail_at_attempt(*call_args, **kwargs):
                        if operation != 'fsync' and call_args[0] != args[1]:
                            return filesystem_call(*call_args, **kwargs)
                        raise OSError(errno.ENOSPC, 'Injected disk full')

                    with patch.object(cleanup.os, operation,
                                      side_effect=fail_at_attempt):
                        original(*args)

                with patch.object(cleanup, 'prepare_directory', side_effect=fail_preparation):
                    worker.tick()
                with Store(worker.database) as store:
                    attempt, = store.job_attempts(self.job_id)
                    identifier = attempt.attempt_id
                    self.assertEqual(attempt.state, 'assigned')
                    self.assertTrue(store.reservations())
                    self.assertFalse(store.launch_claimed(identifier))
                    transfer = store.input_transfer(identifier)
                    self.assertEqual(transfer['status'], 'failed')
                    self.assertIn('Injected disk full', transfer['reason'])
                self.assertFalse(worker.futures)
                with patch.object(cleanup, 'prepare_directory',
                                  side_effect=PermissionError('Still unavailable')):
                    worker.tick()  # Report the first failure and retry without exiting.
                with Store(self.config.manager.database_path) as store:
                    self.assertEqual(store.input_transfer(identifier)['status'], 'failed')
                if restart:
                    worker.close()
                    worker = self.worker()
                self.tick(worker)  # Retry preparation and launch the accepted attempt.
                self.assertTrue(worker.futures)
                next(iter(worker.futures.values())).result(timeout=5)
                with Store(worker.database) as store:
                    self.assertEqual(store.job_state(self.job_id), 'succeeded')
                    self.assertEqual(len(store.job_attempts(self.job_id)), 1)
                    self.assertTrue(store.launch_claimed(identifier))
                    if operation in ('open', 'fsync'):
                        # Creation preceded the sync failure. Retrying must not
                        # adopt that existing unowned tree for later deletion.
                        self.assertIsNone(store.cleanup_record(identifier))
                    else:
                        self.assertIsNotNone(store.cleanup_record(identifier))

    def test_cancellation_survives_directory_preparation_failure(self) -> None:
        worker = self.worker()
        with patch('lammps_manager.cleanup.prepare_directory',
                   side_effect=PermissionError('Injected directory denial')):
            worker.tick()
            with Store(worker.database) as store:
                identifier = store.job_attempts(self.job_id)[0].attempt_id
            with Store(self.config.manager.database_path) as store:
                store.request_cancel(identifier, 'Cancel unavailable directory')
            self.tick(worker)
        with Store(worker.database) as store:
            self.assertEqual(store.attempt(identifier).state, 'cancelled')
            self.assertFalse(store.launch_claimed(identifier))
            self.assertFalse(store.reservations())
        self.assertFalse(worker.futures)

    def test_lost_ack_retains_files_then_cleanup_restart_replays_without_files(self) -> None:
        self.configure_process()
        worker = self.worker()
        identifier = self.finish_process(worker)
        original = worker._result_request
        def lost(*args, **kwargs):
            result = original(*args, **kwargs)
            if args[1] == 'commit':
                raise ConnectionError('Lost durable acknowledgement')
            return result
        with patch.object(worker, '_result_request', side_effect=lost):
            self.tick(worker)
        with Store(worker.database) as store:
            target = Path(store.attempt(identifier).directory)
            self.assertTrue((target / 'work/generated').exists())
            self.assertEqual(store.cleanup_record(identifier)['status'], 'retained')
        self.tick(worker)
        if worker.cleanup_future:
            worker.cleanup_future.result(timeout=5)
        worker._cleanup_results()
        self.assertFalse(target.exists())
        worker.close()
        restarted = self.worker()
        with patch.object(restarted, '_execute', side_effect=AssertionError('Duplicate launch')), \
                patch.object(restarted, '_result_request', side_effect=AssertionError('Deleted files requested')):
            self.tick(restarted)
        with Store(restarted.database) as store:
            self.assertEqual(store.cleanup_record(identifier)['status'], 'cleaned')
            self.assertEqual(len(store.job_attempts(self.job_id)), 1)
            self.assertTrue(store.launch_claimed(identifier))
        with Store(self.config.manager.database_path) as store:
            from lammps_manager.local import inspect_job
            result = Path(inspect_job(store, self.job_id)['attempts'][0]['results_directory'])
            import hashlib
            for item in store.result_collection(identifier)['manifest']['files']:
                self.assertEqual(hashlib.sha256((result / item['path']).read_bytes()).hexdigest(), item['sha256'])

    def test_noncollected_commit_response_never_authorizes_cleanup(self) -> None:
        self.configure_process()
        worker = self.worker()
        identifier = self.finish_process(worker)
        original = worker._result_request
        def not_acknowledged(*args, **kwargs):
            reply = original(*args, **kwargs)
            return {'status': 'pending'} if args[1] == 'commit' else reply
        with patch.object(worker, '_result_request', side_effect=not_acknowledged):
            self.tick(worker)
        with Store(worker.database) as store:
            self.assertEqual(store.result_collection(identifier)['status'], 'failed')
            self.assertEqual(store.cleanup_record(identifier)['status'], 'retained')
            self.assertTrue(Path(store.attempt(identifier).directory).exists())

    def test_disabled_cleanup_preserves_acknowledged_worker_files(self) -> None:
        self.configure_process()
        self.worker_config = replace(self.worker_config, cleanup_staged_files=False)
        worker = self.worker()
        identifier = self.finish_process(worker)
        for _ in range(3):
            self.tick(worker)
        self.assertIsNone(worker.cleanup_future)
        with Store(worker.database) as store:
            self.assertEqual(store.result_collection(identifier)['status'], 'collected')
            self.assertTrue(Path(store.attempt(identifier).directory).exists())
            self.assertEqual(store.cleanup_record(identifier)['status'], 'retained')

    def test_blocked_cleanup_keeps_polling_and_next_execution_active(self) -> None:
        import shutil
        self.configure_process()
        worker = self.worker()
        identifier = self.finish_process(worker)
        self.tick(worker)
        if worker.cleanup_future:
            worker.cleanup_future.result(timeout=5)
        entered, release = threading.Event(), threading.Event()
        from lammps_manager.cleanup import cleanup_attempt as original
        def blocked(*args, **kwargs):
            entered.set()
            if not release.wait(10):
                raise TimeoutError('Cleanup test not released')
            return original(*args, **kwargs)
        try:
            with patch('lammps_manager.cleanup.cleanup_attempt', side_effect=blocked):
                worker.tick()
                self.assertTrue(entered.wait(5))
                future = worker.cleanup_future
                next_spec = replace(self.spec, job_id='next', work_directory=str(self.manager_root / 'next'))
                shutil.copytree(Path(self.spec.work_directory) / 'inputs', Path(next_spec.work_directory) / 'inputs')
                with Store(self.config.manager.database_path) as store:
                    store.submit(next_spec)
                    before = store.worker_records()['a']['snapshot']['heartbeat_at']
                worker.tick()
                worker.tick()
                self.assertIs(worker.cleanup_future, future)
                self.assertTrue(worker.futures)
                for execution in worker.futures.values():
                    execution.result(timeout=5)
                with Store(worker.database) as store:
                    self.assertEqual(store.job_state('next'), 'succeeded')
                    self.assertEqual(store.cleanup_record(identifier)['status'], 'retained')
                with Store(self.config.manager.database_path) as store:
                    self.assertGreater(store.worker_records()['a']['snapshot']['heartbeat_at'], before)
        finally:
            release.set()
            if worker.cleanup_future:
                worker.cleanup_future.result(timeout=5)

    def test_shutdown_joins_cleanup_before_releasing_worker_lock(self) -> None:
        from concurrent.futures import ThreadPoolExecutor
        from lammps_manager.worker import Worker
        self.configure_process()
        worker = self.worker()
        self.finish_process(worker)
        self.tick(worker)
        if worker.cleanup_future:
            worker.cleanup_future.result(timeout=5)
        entered, release, stopping = threading.Event(), threading.Event(), threading.Event()
        from lammps_manager.cleanup import cleanup_attempt as original
        def blocked(*args, **kwargs):
            entered.set()
            if not release.wait(10):
                raise TimeoutError('Shutdown test not released')
            return original(*args, **kwargs)
        def close():
            stopping.set()
            worker.close()
        runner = ThreadPoolExecutor(max_workers=1)
        try:
            with patch('lammps_manager.cleanup.cleanup_attempt', side_effect=blocked):
                worker.tick()
                self.assertTrue(entered.wait(5))
                closed = runner.submit(close)
                self.assertTrue(stopping.wait(5))
                self.assertFalse(closed.done())
                with self.assertRaisesRegex(ValueError, 'locked'):
                    Worker(self.worker_config, self.root / 'journal')
                release.set()
                closed.result(timeout=5)
        finally:
            release.set()
            runner.shutdown(wait=True)
        restarted = self.worker()
        self.tick(restarted)

    def test_prelaunch_cancellation_is_collected_then_cleaned(self) -> None:
        worker = self.worker()
        self.tick(worker)
        with Store(worker.database) as store:
            identifier = store.job_attempts(self.job_id)[0].attempt_id
        with Store(self.config.manager.database_path) as store:
            store.request_cancel(identifier, 'Cancel before launch')
        for _ in range(3):
            self.tick(worker)
        if worker.cleanup_future:
            worker.cleanup_future.result(timeout=5)
        worker._cleanup_results()
        with Store(worker.database) as store:
            self.assertEqual(store.attempt(identifier).state, 'cancelled')
            self.assertEqual(store.result_collection(identifier)['status'], 'collected')
            self.assertEqual(store.cleanup_record(identifier)['status'], 'cleaned')
            self.assertFalse(Path(store.attempt(identifier).directory).exists())
