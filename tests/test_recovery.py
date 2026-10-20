"""Recovery checks use isolated local processes; real MPI runs separately."""

from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import pickle
import sqlite3
import subprocess
import sys
import threading
import time
import unittest
from datetime import datetime, timezone

import yaml
from unittest.mock import patch

from lammps_manager.local import run_local, submit_batch
from lammps_manager.recovery import process_identity, reconcile, session_members
from lammps_manager.store import SCHEMA, Attempt, Store, StoreError
from lammps_manager.worker import Worker
from lammps_manager.manager import ManagerService
import test_local
import test_workers


class RecoveryTests(unittest.TestCase):
    setUp = test_local.LocalTests.setUp

    def test_local_recovery_enforces_future_time_limit(self) -> None:
        self._monitor_recovered_local('timeout')

    def test_local_recovery_observes_later_cancellation(self) -> None:
        self._monitor_recovered_local('cancel')

    def test_local_recovery_observes_natural_exit_and_continues_queue(self) -> None:
        self._monitor_recovered_local('exit')

    def test_local_recovery_stops_monitoring_when_identity_becomes_uncertain(self) -> None:
        self._monitor_recovered_local('uncertain')

    def _monitor_recovered_local(self, outcome: str) -> None:
        config = replace(self.config, job_defaults=replace(
            self.config.job_defaults, time_limit_seconds=1 if outcome == 'timeout' else 60))
        with Store(config.manager.database_path) as store:
            job = submit_batch(store, config, self.manifest)[0]
            queued = submit_batch(store, config, self.manifest)[0]
            directory = Path(store.specification(job).work_directory) / 'recovered'
            directory.mkdir()
            store.assign(job, 'recovered', self.worker.worker.id, str(directory))
            process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                       start_new_session=True)
            identity = {**process_identity(process.pid),
                        'launched_at': datetime.now(timezone.utc).isoformat()}
            (directory / 'process.json').write_text(json.dumps(identity))
            (directory / 'stdout').write_text('preserved output')
            store.transition('recovered', 'running')
        errors = []
        results = []
        observed = threading.Event()
        original = reconcile

        def observe(store: Store, attempt: Attempt) -> bool:
            result = original(store, attempt)
            observed.set()
            return result

        def execute() -> None:
            try:
                with Store(config.manager.database_path) as store:
                    results.append(run_local(store, self.worker))
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=execute)
        try:
            with patch('lammps_manager.recovery.reconcile', side_effect=observe):
                thread.start()
                self.assertTrue(observed.wait(3))
                thread.join(.1)
                self.assertTrue(thread.is_alive(), 'Recovered process lost its monitor')
                with Store(config.manager.database_path) as store:
                    self.assertEqual(store.job_state(queued), 'queued')
                    self.assertEqual(len(store.reservations()), 1)
                    if outcome == 'cancel':
                        store.request_cancel('recovered')
                    elif outcome == 'uncertain':
                        (directory / 'process.json').unlink()
                if outcome == 'exit':
                    process.terminate()
                thread.join(6)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            with Store(config.manager.database_path) as store:
                final = store.attempt('recovered')
                self.assertEqual(len(store.job_attempts(job)), 1)
                self.assertEqual((directory / 'stdout').read_text(), 'preserved output')
                if outcome == 'uncertain':
                    self.assertEqual(final.state, 'unknown')
                    self.assertEqual(len(store.reservations()), 1)
                    self.assertEqual(store.job_state(queued), 'queued')
                    self.assertIsNone(process.poll())
                    self.assertEqual(results, [()])
                else:
                    self.assertEqual(final.state, 'failed' if outcome == 'exit' else 'cancelled')
                    self.assertIn({'timeout': 'Time limit', 'cancel': 'Cancellation',
                                   'exit': 'exit status unavailable'}[outcome], final.reason)
                    self.assertTrue(final.termination_evidence)
                    self.assertEqual(session_members(process.pid), ())
                    self.assertEqual(store.reservations(), ())
                    self.assertEqual(results, [(queued,)])
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=3)
            thread.join(3)

    def test_time_limit_kills_child_groups_and_preserves_output(self) -> None:
        self._cancel_process_tree(timeout=True)

    def test_explicit_cancellation_kills_child_groups(self) -> None:
        self._cancel_process_tree(timeout=False)

    def _cancel_process_tree(self, *, timeout: bool) -> None:
        program = Path(self.worker.execution.lammps_path)
        program.write_text('#!' + sys.executable + '\n'
            'import os,signal,time,pathlib\n'
            'signal.signal(signal.SIGTERM, signal.SIG_IGN)\n'
            'pid=os.fork()\n'
            'if pid==0:\n os.setpgid(0,0)\n while True: time.sleep(.1)\n'
            'pathlib.Path("child.pid").write_text(str(pid))\n'
            'print("retained output",flush=True)\n'
            'while True: time.sleep(.1)\n')
        entries = json.loads(self.manifest.read_text())
        entries[0]['resources'] = {'time_limit_seconds': 1 if timeout else 60}
        self.manifest.write_text(json.dumps(entries))
        errors = []
        with Store(self.config.manager.database_path) as store:
            job = submit_batch(store, self.config, self.manifest)[0]
        def execute() -> None:
            try:
                with Store(self.config.manager.database_path) as journal:
                    run_local(journal, self.worker)
            except BaseException as exc:
                errors.append(exc)
        thread = threading.Thread(target=execute)
        thread.start()
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with Store(self.config.manager.database_path) as store:
                    attempts = store.job_attempts(job)
                    if (attempts and attempts[-1].state == 'running'
                            and (Path(attempts[-1].directory) / 'work/child.pid').exists()
                            and (Path(attempts[-1].directory) / 'stdout').read_text()):
                        attempt = attempts[-1]
                        break
                time.sleep(.02)
            else:
                self.fail('Process did not start')
            if not timeout:
                with Store(self.config.manager.database_path) as store:
                    store.request_cancel(attempt.attempt_id)
                    self.assertEqual(len(store.reservations()), 1)
            thread.join(8)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            with Store(self.config.manager.database_path) as store:
                final = store.attempt(attempt.attempt_id)
                self.assertEqual(final.state, 'cancelled')
                self.assertIn('Time limit' if timeout else 'Cancellation', final.reason)
                self.assertEqual(store.reservations(), ())
                self.assertIn('retained output', (Path(final.directory) / 'stdout').read_text())
                identity = json.loads((Path(final.directory) / 'process.json').read_text())
                self.assertEqual(session_members(identity['pid']), ())
        finally:
            if thread.is_alive():
                with Store(self.config.manager.database_path) as store:
                    for r in store.reservations():
                        store.request_cancel(r.attempt_id)
                thread.join(8)

    def test_missing_identity_never_relaunches_or_releases(self) -> None:
        with Store(self.config.manager.database_path) as store:
            job = submit_batch(store, self.config, self.manifest)[0]
            spec = store.specification(job)
            attempt = store.assign(job, 'accepted', self.worker.worker.id,
                                   str(Path(spec.work_directory) / 'accepted'))
            self.assertEqual(run_local(store, self.worker), ())
            self.assertEqual(store.attempt(attempt.attempt_id).state, 'unknown')
            self.assertEqual(len(store.reservations()), 1)
            self.assertFalse(Path(attempt.directory).exists())

    def test_recovered_identity_and_boot_change(self) -> None:
        with Store(self.config.manager.database_path) as store:
            job = submit_batch(store, self.config, self.manifest)[0]
            directory = Path(store.specification(job).work_directory) / 'old'
            directory.mkdir()
            attempt = store.assign(job, 'old', self.worker.worker.id, str(directory))
            store.transition('old', 'running')
            identity = {'pid': 12345678, 'process_group': 12345678,
                        'start_ticks': '9', 'boot_id': '00000000-0000-0000-0000-000000000000'}
            (directory / 'process.json').write_text(json.dumps(identity))
            with patch('lammps_manager.recovery.os.killpg') as kill:
                reconcile(store, attempt)
            kill.assert_not_called()
            self.assertEqual(store.attempt('old').state, 'failed')
            self.assertIn('boot', store.attempt('old').termination_evidence)
            self.assertEqual(store.reservations(), ())

    def test_reused_pid_and_surviving_group_are_never_signalled(self) -> None:
        with Store(self.config.manager.database_path) as store:
            job = submit_batch(store, self.config, self.manifest)[0]
            directory = Path(store.specification(job).work_directory) / 'old'
            directory.mkdir()
            attempt = store.assign(job, 'old', self.worker.worker.id, str(directory))
            identity = process_identity(os.getpid())
            identity.update(pid=12345678, process_group=12345678, session=12345678, start_ticks='1')
            (directory / 'process.json').write_text(json.dumps(identity))
            store.request_cancel('old')
            with patch('lammps_manager.recovery.process_identity', return_value={**identity, 'start_ticks': '2', 'session': 12345678}), patch(
                    'lammps_manager.recovery.group_alive', return_value=True), patch(
                    'lammps_manager.recovery.terminate_group') as terminate:
                reconcile(store, attempt)
            terminate.assert_not_called()
            self.assertEqual(store.attempt('old').state, 'unknown')
            self.assertEqual(len(store.reservations()), 1)

    def test_v1_migration_retry_preserves_history_and_foreign_keys(self) -> None:
        path = self.root / 'v1.sqlite'
        with Store(self.config.manager.database_path) as store:
            job = submit_batch(store, self.config, self.manifest)[0]
            spec = replace(store.specification(job), resources=replace(
                store.specification(job).resources, max_retries=1))
        with sqlite3.connect(path) as db:
            db.executescript(SCHEMA.replace('job_id TEXT NOT NULL REFERENCES jobs(job_id),',
                'job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),', 1).replace(
                    'PRAGMA user_version = 2', 'PRAGMA user_version = 1'))
            payload = json.dumps(asdict(spec))
            db.execute('INSERT INTO jobs(job_id,specification,state) VALUES(?,?,?)', (job, payload, 'failed'))
            db.execute('INSERT INTO attempts(attempt_id,job_id,worker_id,directory,state,reason,termination_evidence) '
                       'VALUES(?,?,?,?,?,?,?)', ('old', job, 'w', '/old', 'failed', 'exit', 'confirmed'))
            db.execute('INSERT INTO events(job_id,attempt_id,state) VALUES(?,?,?)', (job, 'old', 'failed'))
        with Store(path) as store:
            store.retry(job)
            store.assign(job, 'new', 'w', '/new')
            self.assertEqual(len(store.job_attempts(job)), 2)
            self.assertEqual(store.job_attempts(job)[0].termination_evidence, 'confirmed')
            store.transition('new', 'failed', reason='exit 7', exit_code=7, termination_evidence='confirmed')
            with self.assertRaisesRegex(StoreError, 'budget'):
                store.retry(job)
            self.assertEqual(store.specification(job), spec)
            self.assertEqual(store._db.execute('PRAGMA foreign_key_check').fetchall(), [])
            self.assertEqual(store._db.execute('PRAGMA foreign_keys').fetchone()[0], 1)
            self.assertEqual(store._db.execute('SELECT specification FROM jobs').fetchone()[0], payload)
            with self.assertRaises(sqlite3.IntegrityError):
                store._db.execute("DELETE FROM jobs")


class RecoveryWorkerTests(unittest.TestCase):
    setUp = test_workers.WorkerTests.setUp
    worker_config = test_workers.WorkerTests.worker_config
    worker = test_workers.WorkerTests.worker
    submit = test_workers.WorkerTests.submit

    def test_manager_expiry_restart_and_reconnection_keep_reservations(self) -> None:
        job = self.submit((0,))[0]
        worker = self.worker()
        worker.tick()
        with patch.object(worker.pool, 'submit'):
            worker.tick()
        with Store(self.config.manager.database_path) as store:
            attempt = store.job_attempts(job)[0]
            snapshot = store.worker_records()['a']['snapshot']
            store.set_worker('a', snapshot={**snapshot, 'heartbeat_at': 0})
        restarted = ManagerService(self.config, self.tokens)
        self.addCleanup(restarted.close)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.attempt(attempt.attempt_id).state, 'unknown')
            self.assertEqual(len(store.reservations()), 1)
        worker.pool.shutdown()
        worker.lock.close()
        worker = self.worker()
        worker.tick()  # Reconciliation cannot prove the pre-launch crash window safe.
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.attempt(attempt.attempt_id).state, 'unknown')
            self.assertEqual(len(store.reservations()), 1)

    def test_running_report_resynchronizes_after_heartbeat_expiry(self) -> None:
        self.program.write_text('#!' + sys.executable + '\n'
            'import pathlib,time\n'
            'with pathlib.Path("invocations").open("a") as f: f.write("launch\\n")\n'
            'time.sleep(30)\n')
        job = self.submit((0,))[0]
        worker = self.worker()
        worker.tick()
        worker.tick()
        try:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                worker.tick()
                with Store(self.config.manager.database_path) as store:
                    attempt, = store.job_attempts(job)
                    if attempt.state == 'running' and (Path(attempt.directory) / 'work/invocations').exists():
                        break
                time.sleep(.01)
            else:
                self.fail('Running report was not acknowledged')
            with Store(self.config.manager.database_path) as store:
                reservations = store.reservations()
                snapshot = store.worker_records()['a']['snapshot']
                store.set_worker('a', snapshot={**snapshot, 'heartbeat_at': 0})
            self.service.expire_workers()
            with Store(self.config.manager.database_path) as store:
                self.assertEqual(store.job_state(job), 'unknown')
            worker.tick()
            worker.tick()
            worker.tick()
            with Store(self.config.manager.database_path) as store:
                self.assertEqual(store.job_state(job), 'running')
                self.assertEqual(len(store.job_attempts(job)), 1)
                self.assertEqual(store.reservations(), reservations)
            self.assertFalse(worker.future.done())
            self.assertEqual((Path(attempt.directory) / 'work/invocations').read_text(), 'launch\n')
        finally:
            with Store(worker.database) as store:
                for reservation in store.reservations():
                    store.request_cancel(reservation.attempt_id)
            if worker.future is not None:
                worker.future.result(timeout=6)

    def test_timeout_completion_without_running_poll(self) -> None:
        self._timeout_between_polls(report_running=False)

    def test_timeout_completion_without_cancelling_poll(self) -> None:
        self._timeout_between_polls(report_running=True)

    def _timeout_between_polls(self, *, report_running: bool) -> None:
        self.config = replace(self.config, job_defaults=replace(
            self.config.job_defaults, time_limit_seconds=1))
        self.service.config = self.config
        self.program.write_text('#!' + sys.executable + '\n'
            'import time\nprint("timeout output",flush=True)\ntime.sleep(30)\n')
        job = self.submit((0,))[0]
        worker = self.worker()
        worker.tick()
        worker.tick()
        if report_running:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                with Store(worker.database) as store:
                    if store.job_state(job) == 'running':
                        break
                time.sleep(.01)
            else:
                self.fail('Worker did not enter running state')
            worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(job), 'running' if report_running else 'assigned')
            self.assertEqual(len(store.reservations()), 1)
        worker.future.result(timeout=6)
        with Store(worker.database) as store:
            attempt, = store.job_attempts(job)
            self.assertEqual(attempt.state, 'cancelled')
            self.assertEqual(attempt.reason, 'Time limit exceeded')
            self.assertTrue(attempt.termination_evidence)
            self.assertEqual(session_members(json.loads(
                (Path(attempt.directory) / 'process.json').read_text())['pid']), ())
        # No cancelling poll reaches the manager before the terminal report.
        # Reconstructing intermediate states must roll back without exit evidence.
        with self.assertRaisesRegex(StoreError, 'termination evidence'):
            self.service.poll(self.capacities[0].id, {
                'proof': worker.proof,
                'snapshot': worker.monitor.sample(worker.config),
                'reports': [{'attempt_id': attempt.attempt_id, 'state': 'cancelled',
                             'started': True, 'exit_code': attempt.exit_code,
                             'reason': attempt.reason}],
            })
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(job), 'running' if report_running else 'assigned')
            self.assertEqual(len(store.reservations()), 1)
            self.assertNotIn('cancelling', [e['state'] for e in store.events(job)])
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            final = store.attempt(attempt.attempt_id)
            self.assertEqual(final.state, 'cancelled')
            self.assertEqual(final.reason, 'Time limit exceeded')
            self.assertEqual(final.termination_evidence, attempt.termination_evidence)
            self.assertIsNotNone(final.started_at)
            self.assertEqual(store.reservations(), ())
            events = store.events(job)
            self.assertEqual([e['state'] for e in events],
                             ['queued', 'assigned', 'running', 'cancelling', 'cancelled'])
        worker.acknowledged.clear()
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.events(job), events)
            self.assertEqual(len(store.job_attempts(job)), 1)
        self.assertIn('timeout output', (Path(attempt.directory) / 'stdout').read_text())

    def test_cancel_running_poll_and_lost_ack_are_idempotent(self) -> None:
        self.program.write_text('#!' + sys.executable + '\nimport time\nprint("keep",flush=True)\ntime.sleep(30)\n')
        job = self.submit((0,))[0]
        worker = self.worker()
        worker.tick()
        worker.tick()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with Store(worker.database) as store:
                attempt = store.job_attempts(job)[0]
                if attempt.state == 'running':
                    break
            time.sleep(.01)
        with Store(self.config.manager.database_path) as store:
            store.request_cancel(attempt.attempt_id)
            self.assertEqual(len(store.reservations()), 1)
        worker.tick()
        worker.future.result(timeout=6)
        worker.acknowledged.clear()
        worker.tick()
        worker.acknowledged.clear()
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(job), 'cancelled')
            self.assertEqual(store.reservations(), ())
            self.assertEqual(len(store.job_attempts(job)), 1)

    def test_explicit_retry_keeps_both_attempts_and_replay_does_not_reset_job(self) -> None:
        self.config = replace(self.config, job_defaults=replace(self.config.job_defaults, max_retries=1))
        self.service.config = self.config
        job = self.submit((7,))[0]
        worker = self.worker()
        worker.tick()
        worker.tick()
        worker.future.result(timeout=3)
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            first = store.job_attempts(job)[0]
            saved = (Path(first.directory) / 'stdout').read_bytes()
        manager_path = self.root / 'manager.yaml'
        manager_path.write_text(yaml.safe_dump(asdict(self.config)))
        result = subprocess.run([sys.executable, '-m', 'lammps_manager', 'retry',
            '--config', str(manager_path), job], text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['state'], 'queued')
        worker.acknowledged.clear()  # Old failed report can replay after retry was queued.
        worker.tick()
        worker.future.result(timeout=3)
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            attempts = store.job_attempts(job)
            self.assertEqual(len(attempts), 2)
            self.assertEqual(store.job_state(job), 'failed')
            self.assertNotEqual(attempts[0].directory, attempts[1].directory)
            self.assertEqual((Path(first.directory) / 'stdout').read_bytes(), saved)
            self.assertEqual(store.reservations(), ())
            with self.assertRaisesRegex(StoreError, 'budget'):
                store.retry(job)

    def test_cancel_pending_assignment_bypasses_pause_and_low_memory(self) -> None:
        job = self.submit((0,))[0]
        worker = self.worker()
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            spec = store.specification(job)
            store.assign(job, 'pending', 'a', str(Path(spec.work_directory) / 'pending'))
            store.set_worker('a', paused=True)
        manager_path = self.root / 'manager.yaml'
        manager_path.write_text(yaml.safe_dump(asdict(self.config)))
        result = subprocess.run([sys.executable, '-m', 'lammps_manager', 'cancel',
            '--config', str(manager_path), job], text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['state'], 'cancelling')
        sample = worker.monitor.sample(worker.config)
        with patch.object(worker.monitor, 'sample', return_value={**sample, 'available_memory_gib': 0}):
            worker.tick()
        self.assertIsNone(worker.future)
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(job), 'cancelled')
            self.assertEqual(store.reservations(), ())
            self.assertFalse((Path(spec.work_directory) / 'pending').exists())

    def test_periodic_expiry_runs_without_any_further_polls(self) -> None:
        job = self.submit((0,))[0]
        self.service.config = replace(self.config, manager=replace(
            self.config.manager, heartbeat_timeout_seconds=.05))
        worker = self.worker()
        worker.tick()
        with patch.object(worker.pool, 'submit'):
            worker.tick()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with Store(self.config.manager.database_path) as store:
                if store.job_state(job) == 'unknown':
                    self.assertEqual(len(store.reservations()), 1)
                    return
            time.sleep(.02)
        self.fail('Server did not expire disconnected work')

    def test_cancel_uncertain_acceptance_while_paused_never_launches(self) -> None:
        job = self.submit((0,))[0]
        worker = self.worker()
        worker.tick()
        # Leave a durable acceptance without starting its executor.
        with patch.object(worker.pool, 'submit'):
            result = worker.tick()
        attempt_id = result['assignments'][0]['attempt']['attempt_id']
        # Simulate the acceptance-before-executor window, with explicit evidence
        # available only to the original newly accepted execution path.
        with Store(self.config.manager.database_path) as store:
            store.request_cancel(attempt_id)
            store.set_worker('a', paused=True)
        # Existing accepted state without metadata must remain unknown: it must
        # never be mistaken for a confirmed never-started execution after restart.
        worker.pool.shutdown()
        worker.lock.close()
        worker = self.worker()
        worker.tick()
        self.assertIsNone(worker.future)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(len(store.reservations()), 1)
            self.assertIn(store.job_state(job), {'unknown', 'cancelling'})

    def test_worker_crash_recovers_live_process_without_duplicate(self) -> None:
        self.program.write_text('#!' + sys.executable + '\nimport pathlib,time\n'
            'pathlib.Path("invocations").write_text("one")\ntime.sleep(.8)\n')
        job = self.submit((0,))[0]
        config_file = self.root / 'worker.pickle'
        config_file.write_bytes(pickle.dumps(self.worker_config()))
        code = '''import os,pickle,sys,time
from pathlib import Path
from lammps_manager.worker import Worker
w=Worker(pickle.loads(Path(sys.argv[1]).read_bytes()),sys.argv[2])
w.tick(); w.tick()
while not list(Path(w.config.work_root).glob('*/*/process.json')): time.sleep(.01)
os._exit(0)
'''
        result = subprocess.run([sys.executable, '-c', code, str(config_file),
                                 str(self.root / 'journal-0')], timeout=6)
        self.assertEqual(result.returncode, 0)
        worker = self.worker()
        worker.tick()
        self.assertIsNone(worker.future)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            worker.tick()
            with Store(self.config.manager.database_path) as store:
                if store.job_state(job) == 'failed':
                    break
            time.sleep(.02)
        with Store(self.config.manager.database_path) as store:
            attempt = store.job_attempts(job)[0]
            self.assertEqual(attempt.state, 'failed')
            self.assertIn('exit status unavailable', attempt.reason)
            self.assertEqual(store.reservations(), ())
            self.assertEqual(len(store.job_attempts(job)), 1)
            self.assertEqual((Path(attempt.directory) / 'work/invocations').read_text(), 'one')
