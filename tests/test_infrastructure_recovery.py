"""Bounded infrastructure recovery using local fakes and isolated loopback workers."""
from dataclasses import asdict, replace
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import test_workers
import test_local
import test_staging
import test_results
from lammps_manager.config import ManagerConfig, ManagerSettings, Storage, WorkerCapacity, Scheduler
from lammps_manager.local import inspect_job
from lammps_manager.manager import ManagerService
from lammps_manager.models import InfrastructureRecovery, JobSpecification, Resources, specification_from_dict
from lammps_manager.recovery import reconcile
from lammps_manager.scheduler import WorkerSnapshot, schedule
from lammps_manager.store import Store, StoreError

OLD_BOOT = '00000000-0000-0000-0000-000000000001'
NEW_BOOT = '00000000-0000-0000-0000-000000000002'
IDENTITY = dict(pid=12345678, process_group=12345678, session=12345678,
                start_ticks='9', boot_id=OLD_BOOT)
EVIDENCE = dict(kind='boot_changed', process=IDENTITY, observed_boot_id=NEW_BOOT)


class InfrastructureStoreTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'state.sqlite'
        self.store = Store(self.path)
        self.addCleanup(self.store.close)
        self.spec = JobSpecification('j', '/work/j', 'in', 'a' * 64, Resources(1, 1, .01),
                                    infrastructure_recovery=InfrastructureRecovery(1, 30))
        self.store.submit(self.spec)
        self.store.assign('j', 'a', 'w', '/work/j/a')

    def stopped(self, attempt='a'):
        self.store.transition(attempt, 'failed', reason='Interrupted worker',
                              termination_evidence='Linux boot changed')
        self.store.record_interruption(attempt, EVIDENCE)

    def test_replay_restart_delay_and_exhaustion(self):
        self.stopped()
        self.store.recover_interrupted('a', 10)
        first = self.store.interruption('a')
        with Store(self.path) as reopened:
            reopened.record_interruption('a', EVIDENCE)
            reopened.recover_interrupted('a', 1000)
            self.assertEqual(reopened.interruption('a'), first)
            self.assertEqual(reopened.recovery_blocker('j', 39), 'infrastructure_recovery_delay')
            self.assertIsNone(reopened.recovery_blocker('j', 40))
            reopened.assign('j', 'b', 'w', '/work/j/b')
        self.stopped('b')
        self.store.recover_interrupted('b', 100)
        self.assertEqual(self.store.job_state('j'), 'failed')
        self.assertIn('exhausted', self.store.interruption('b')['reason'])
        self.assertEqual(self.store.specification('j'), self.spec)
        self.assertFalse(self.store.reservations())
        with self.assertRaisesRegex(StoreError, 'budget'):
            self.store.retry('j')  # Infrastructure policy does not enable application retries.

    def test_legacy_snapshot_keeps_zero_recovery_without_rewriting_json(self):
        data = asdict(self.spec)
        data.pop('infrastructure_recovery')
        data.update(job_id='legacy', work_directory='/work/legacy')
        self.store.submit(specification_from_dict(data))
        self.store._db.execute('UPDATE jobs SET specification=? WHERE job_id=?',
                               (json.dumps(data), 'legacy'))
        self.store.assign('legacy', 'old', 'w', '/work/legacy/a')
        payload = self.store._db.execute("SELECT specification FROM jobs WHERE job_id='legacy'").fetchone()[0]
        self.stopped('old')
        self.store.recover_interrupted('old', 100)
        self.assertEqual(self.store.job_state('legacy'), 'failed')
        self.assertEqual(self.store.specification('legacy').infrastructure_recovery.max_retries, 0)
        self.assertEqual(self.store._db.execute("SELECT specification FROM jobs WHERE job_id='legacy'").fetchone()[0], payload)

    def test_unknown_generation_and_bad_metadata_never_release(self):
        self.store.worker_generation('w', OLD_BOOT, OLD_BOOT)
        self.store.worker_generation('w', NEW_BOOT, NEW_BOOT)
        self.store.worker_generation('w', NEW_BOOT, NEW_BOOT)
        self.assertEqual(len(self.store.generations('w')), 2)
        self.assertEqual(len(self.store.reservations()), 1)
        with self.assertRaises(StoreError):
            self.store.record_interruption('a', EVIDENCE)
        with tempfile.TemporaryDirectory() as directory:
            self.store._db.execute('UPDATE attempts SET directory=? WHERE attempt_id=?', (directory, 'a'))
            for content in ('bad json', '{}', json.dumps({**IDENTITY, 'boot_id': 'bad'})):
                (Path(directory) / 'process.json').write_text(content)
                reconcile(self.store, self.store.attempt('a'))
                self.assertEqual(self.store.job_state('j'), 'unknown')
                self.assertEqual(len(self.store.reservations()), 1)
                self.assertIsNone(self.store.interruption('a'))

    def test_cancellation_before_and_after_requeue_blocks_successor(self):
        self.store.request_cancel('a')
        self.stopped()
        self.store.recover_interrupted('a', 0)
        self.assertEqual(self.store.job_state('j'), 'failed')
        self.assertIn('cancellation', self.store.interruption('a')['reason'])

    def test_cancel_delayed_recovery_and_stale_replay(self):
        self.stopped()
        self.store.recover_interrupted('a', 0)
        self.store.request_cancel('a')
        self.store.record_interruption('a', EVIDENCE)
        self.store.recover_interrupted('a', 100)
        self.assertEqual(self.store.job_state('j'), 'cancelled')
        self.assertFalse(self.store.reservations())

    def test_recovery_and_terminal_evidence_rollback_together(self):
        with self.assertRaises(RuntimeError), self.store.scheduling_round():
            self.stopped()
            self.store.recover_interrupted('a', 0)
            raise RuntimeError('crash')
        self.assertEqual(self.store.job_state('j'), 'assigned')
        self.assertEqual(len(self.store.reservations()), 1)
        self.assertIsNone(self.store.interruption('a'))

    def test_absent_leader_waits_for_complete_same_boot_session_termination(self):
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        with tempfile.TemporaryDirectory() as directory:
            self.store._db.execute('UPDATE attempts SET directory=? WHERE attempt_id=?', (directory, 'a'))
            (Path(directory) / 'process.json').write_text(json.dumps({**IDENTITY, 'boot_id': boot}))
            with patch('lammps_manager.recovery.process_identity', side_effect=FileNotFoundError), \
                    patch('lammps_manager.recovery.group_alive', return_value=True):
                reconcile(self.store, self.store.attempt('a'))
            self.assertEqual(self.store.job_state('j'), 'unknown')
            self.assertEqual(len(self.store.reservations()), 1)
            self.assertIsNone(self.store.interruption('a'))
            with patch('lammps_manager.recovery.process_identity', side_effect=FileNotFoundError), \
                    patch('lammps_manager.recovery.group_alive', return_value=False):
                reconcile(self.store, self.store.attempt('a'))
            self.assertEqual(self.store.interruption('a')['evidence']['kind'], 'session_ended')
            self.store.recover_interrupted('a', 0)
            self.assertEqual(self.store.job_state('j'), 'queued')

    def test_concurrent_recovery_consumes_budget_exactly_once(self):
        from concurrent.futures import ThreadPoolExecutor
        self.stopped()
        def recover():
            with Store(self.path) as connection:
                connection.recover_interrupted('a', 0)
        with ThreadPoolExecutor(2) as pool:
            first, second = pool.submit(recover), pool.submit(recover)
            first.result()
            second.result()
        self.assertEqual(self.store.recovery_status('j')['used'], 1)
        self.assertEqual(sum(event['reason'] == 'Automatic infrastructure recovery scheduled'
                             for event in self.store.events('j')), 1)

    def test_worker_upgrade_preserves_legacy_policy_and_accepts_authorized_successor(self):
        from lammps_manager.worker import Worker
        worker = Worker.__new__(Worker)
        legacy = replace(self.spec, job_id='legacy', work_directory='/work/legacy',
                         infrastructure_recovery=InfrastructureRecovery(0))
        self.store.submit(legacy)
        self.store.assign('legacy', 'legacy-attempt', 'w', '/work/legacy/old')
        payload = self.store._db.execute("SELECT specification FROM jobs WHERE job_id='legacy'").fetchone()[0]
        self.store.transition('legacy-attempt', 'failed', reason='outage', termination_evidence='Linux boot changed')
        self.store.record_interruption('legacy-attempt', EVIDENCE)
        worker._submit_specification(self.store, replace(legacy, infrastructure_recovery=InfrastructureRecovery(1)))
        worker._accept_retry(self.store, 'legacy')
        self.assertEqual(self.store.job_state('legacy'), 'queued')
        self.assertEqual(self.store._db.execute("SELECT specification FROM jobs WHERE job_id='legacy'").fetchone()[0], payload)
        with self.assertRaises(StoreError):
            worker._submit_specification(self.store, replace(legacy, input_sha256='b' * 64))

    def test_scheduler_honors_durable_delay(self):
        self.stopped()
        self.store.recover_interrupted('a', 10)
        capacity = WorkerCapacity('w', 8, 8, 1)
        config = ManagerConfig(ManagerSettings(str(self.path)), Storage('/work'), (capacity,),
                               scheduler=Scheduler(account_external_load=False, reserve_memory_gib=0))
        snapshot = WorkerSnapshot(capacity, 8, 8, 39, 39, 0, 8)
        self.assertFalse(schedule(self.store, config, (snapshot,), now=39).assignments)
        self.assertEqual(schedule(self.store, config, (snapshot,), now=40).assignments[0].job_id, 'j')


class InfrastructureLocalTests(unittest.TestCase):
    setUp = test_local.LocalTests.setUp

    def interrupted(self):
        from lammps_manager.local import submit_batch
        self.config = replace(self.config, infrastructure_recovery=InfrastructureRecovery(1, 30))
        store = Store(self.config.manager.database_path)
        self.addCleanup(store.close)
        job, = submit_batch(store, self.config, self.manifest)
        directory = Path(store.specification(job).work_directory) / 'interrupted'
        directory.mkdir()
        (directory / 'process.json').write_text(json.dumps(IDENTITY))
        (directory / 'stdout').write_text('prior output')
        store.assign(job, 'interrupted', self.worker.worker.id, str(directory))
        store.transition('interrupted', 'running')
        return store, job, directory

    def test_local_boot_recovery_delay_restart_and_single_successor(self):
        from lammps_manager.local import run_local
        store, job, directory = self.interrupted()
        with patch('lammps_manager.local.time.time', return_value=100):
            self.assertEqual(run_local(store, self.worker), ())
        self.assertEqual(store.job_state(job), 'queued')
        self.assertEqual(store.recovery_status(job)['next_retry_at'], 130)
        with Store(self.config.manager.database_path) as reopened:
            with patch('lammps_manager.local.time.time', return_value=129):
                self.assertEqual(run_local(reopened, self.worker), ())
            self.assertEqual(reopened.recovery_status(job)['next_retry_at'], 130)
            with patch('lammps_manager.local.time.time', return_value=130):
                self.assertEqual(run_local(reopened, self.worker), (job,))
                self.assertEqual(run_local(reopened, self.worker), ())
            self.assertEqual(reopened.job_state(job), 'succeeded')
            self.assertEqual(len(reopened.job_attempts(job)), 2)
            self.assertEqual(reopened.recovery_status(job)['used'], 1)
            self.assertFalse(reopened.reservations())
        self.assertEqual((directory / 'stdout').read_text(), 'prior output')

    def test_local_finishes_previously_detected_recovery_decision(self):
        from lammps_manager.local import run_local
        store, job, _ = self.interrupted()
        reconcile(store, store.attempt('interrupted'))
        self.assertEqual(store.interruption('interrupted')['status'], 'detected')
        with patch('lammps_manager.local.time.time', return_value=100):
            run_local(store, self.worker)
        self.assertEqual(store.job_state(job), 'queued')
        self.assertEqual(store.recovery_status(job)['next_retry_at'], 130)

    def test_local_cancellation_during_delay_does_not_launch(self):
        from lammps_manager.local import run_local
        store, job, _ = self.interrupted()
        with patch('lammps_manager.local.time.time', return_value=100):
            run_local(store, self.worker)
        store.request_cancel('interrupted')
        with patch('lammps_manager.local.time.time', return_value=131):
            self.assertEqual(run_local(store, self.worker), ())
        self.assertEqual(store.job_state(job), 'cancelled')
        self.assertEqual(len(store.job_attempts(job)), 1)

    def test_local_zero_budget_blocks_detected_interruption(self):
        from lammps_manager.local import run_local
        store, job, _ = self.interrupted()
        # Model an older submitted snapshot with no automatic recovery budget.
        data = asdict(store.specification(job))
        data.pop('infrastructure_recovery')
        store._db.execute('UPDATE jobs SET specification=? WHERE job_id=?', (json.dumps(data), job))
        self.assertEqual(run_local(store, self.worker), ())
        self.assertEqual(store.job_state(job), 'failed')
        self.assertEqual(store.interruption('interrupted')['status'], 'blocked')
        self.assertIn('exhausted', store.interruption('interrupted')['reason'])

    def test_local_reconciliation_and_recovery_decision_are_atomic(self):
        from lammps_manager.local import run_local
        store, job, _ = self.interrupted()
        with patch.object(store, 'recover_interrupted', side_effect=RuntimeError('interrupted decision')):
            with self.assertRaisesRegex(RuntimeError, 'interrupted decision'):
                run_local(store, self.worker)
        self.assertEqual(store.job_state(job), 'running')
        self.assertIsNone(store.interruption('interrupted'))
        self.assertEqual(len(store.reservations()), 1)

    def test_local_does_not_authorize_another_workers_detected_recovery(self):
        from lammps_manager.local import run_local
        store, job, _ = self.interrupted()
        reconcile(store, store.attempt('interrupted'))
        other = replace(self.worker, worker=replace(self.worker.worker, id='other'))
        self.assertEqual(run_local(store, other), ())
        self.assertEqual(store.job_state(job), 'failed')
        self.assertEqual(store.interruption('interrupted')['status'], 'detected')


class InfrastructureWorkerTests(unittest.TestCase):
    setUp = test_workers.WorkerTests.setUp
    worker_config = test_workers.WorkerTests.worker_config
    worker = test_workers.WorkerTests.worker
    submit = test_workers.WorkerTests.submit

    def test_boot_evidence_requeues_once_and_preserves_output(self):
        self.recover_with_fake_boot(False)

    def test_staged_boot_recovery_retains_and_collects_both_attempts(self):
        self.recover_with_fake_boot(True)

    def recover_with_fake_boot(self, staged):
        if staged:
            worker_root = self.root / 'private-worker'
            worker_root.mkdir()
            original = self.worker_config
            self.worker_config = lambda index=0: replace(original(index),
                storage_mode='staged', work_root=str(worker_root), cleanup_staged_files=False)
            self.config = replace(self.config, storage=replace(self.config.storage, mode='staged'))
        self.config = replace(self.config, infrastructure_recovery=InfrastructureRecovery(1, .01))
        self.service.config = self.config
        worker = self.worker()
        worker.tick()  # Establish readiness before creating the interrupted job.
        job, = self.submit((0,))
        with Store(self.config.manager.database_path) as manager:
            spec = manager.specification(job)
            directory = Path(spec.work_directory) / 'interrupted'
            manager.assign(job, 'interrupted', 'a', str(directory))
            manager.transition('interrupted', 'running')
        if staged:
            directory = worker_root / job / 'interrupted'
        directory.mkdir(parents=True)
        (directory / 'process.json').write_text(json.dumps(IDENTITY))
        (directory / 'stdout').write_text('prior output')
        with Store(worker.database) as journal:
            journal.submit(spec)
            journal.assign(job, 'interrupted', 'a', str(directory))
            journal.claim_launch('interrupted')
            journal.transition('interrupted', 'running')
        worker.tick()
        with Store(self.config.manager.database_path) as manager:
            first = inspect_job(manager, job)
            self.assertEqual(first['state'], 'queued')
            self.assertEqual(first['attempts'][0]['infrastructure_recovery']['status'], 'queued')
            self.assertFalse(manager.reservations())
        restarted = ManagerService(self.config, self.tokens)
        self.addCleanup(restarted.close)
        self.service.poll = restarted.poll  # Existing loopback listener serves the restarted manager.
        worker.acknowledged.clear()  # Lost acknowledgement / duplicate report.
        time.sleep(.02)
        deadline = time.monotonic() + 3
        while not worker.futures and time.monotonic() < deadline:
            worker.tick()
            time.sleep(.01)
        self.assertTrue(worker.futures)
        next(iter(worker.futures.values())).result(timeout=3)
        worker.tick()
        worker.acknowledged.clear()
        worker.tick()
        with Store(self.config.manager.database_path) as manager:
            self.assertEqual(manager.job_state(job), 'succeeded')
            self.assertEqual(len(manager.job_attempts(job)), 2)
            self.assertEqual((directory / 'stdout').read_text(), 'prior output')
            self.assertEqual(len(manager.generations('a')), 1)
            self.assertFalse(manager.reservations())

        if staged:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                worker.tick()
                if worker.collection_future:
                    worker.collection_future.result(timeout=3)
                with Store(self.config.manager.database_path) as manager:
                    attempts = manager.job_attempts(job)
                    if all(manager.result_collection(a.attempt_id)
                           and manager.result_collection(a.attempt_id)['status'] == 'collected'
                           for a in attempts):
                        self.assertEqual((Path(attempts[0].directory) / 'results/stdout').read_text(), 'prior output')
                        break
                time.sleep(.01)
            else:
                self.fail('Both staged attempts must remain collectable after recovery')


class StagedReadinessTests(unittest.TestCase):
    setUp = test_staging.StagedWorkerTests.setUp
    worker = test_results.ResultTests.worker

    def test_closed_worker_returns_unprepared_staged_acceptance(self):
        worker = self.worker()
        with patch.object(worker, '_stage'):
            worker.tick()
            with Store(worker.database) as journal:
                attempt, = journal.job_attempts(self.job_id)
                self.assertIsNone(journal.input_transfer(attempt.attempt_id))
            worker.draining = True
            worker.tick()
        self.assertFalse(worker.futures)
        worker.tick()
        with Store(self.config.manager.database_path) as manager:
            self.assertEqual(manager.job_state(self.job_id), 'queued')
            self.assertEqual(manager.rejection(attempt.attempt_id)['code'], 'not_ready')
            self.assertFalse(manager.reservations())
