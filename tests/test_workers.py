"""Loopback integration uses only temporary fake executables and journals."""

from dataclasses import replace
from http.client import IncompleteRead
import json
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from lammps_manager.config import (Execution, ManagerConfig, ManagerSettings, Scheduler,
                                   Storage, WorkerCapacity, WorkerConfig)
from lammps_manager.local import submit_batch
from lammps_manager.manager import ManagerService, make_server, read_token
from lammps_manager.models import Resources
from lammps_manager.store import Store
from lammps_manager.worker import LinuxMonitor, Worker


class WorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.work = self.root / 'shared'
        self.work.mkdir()
        self.capacities = tuple(WorkerCapacity(name, 1, 1, 1) for name in ('a', 'b'))
        self.config = ManagerConfig(ManagerSettings(str(self.root / 'manager.sqlite'), .001),
            Storage(str(self.work)), self.capacities, Resources(1, 1, .01),
            Scheduler(account_external_load=False, reserve_memory_gib=0))
        self.tokens = {}
        for name in ('a', 'b'):
            path = self.root / (name + '.token')
            path.write_text(name * 40)
            path.chmod(0o600)
            self.tokens[name] = str(path)
        self.launcher = self.root / 'fake mpi'
        self.launcher.write_text('#!' + sys.executable + '\nimport os,sys\nos.execv(sys.argv[3],sys.argv[3:])\n')
        self.launcher.chmod(0o700)
        self.program = self.root / 'fake lammps'
        self.program.write_text('#!' + sys.executable + '\n'
            'import pathlib,time,sys\n'
            'with pathlib.Path("invocations").open("a") as f: f.write("launch\\n")\n'
            'print("output", flush=True)\nprint("error", file=sys.stderr)\n'
            'time.sleep(.08)\nsys.exit(int(pathlib.Path("in.lmp").read_text()))\n')
        self.program.chmod(0o700)
        self.service = ManagerService(self.config, self.tokens)
        self.addCleanup(self.service.close)
        self.server = make_server(self.service, '127.0.0.1', 0)
        self.endpoint = f'http://127.0.0.1:{self.server.server_port}'
        thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01})
        thread.start()
        def stop() -> None:
            self.server.shutdown()
            thread.join(timeout=5)
            self.server.server_close()
        self.addCleanup(stop)

    def worker_config(self, index: int = 0) -> WorkerConfig:
        capacity = self.capacities[index]
        return WorkerConfig(capacity, self.endpoint, str(self.work),
            Execution(str(self.program), str(self.launcher)), self.tokens[capacity.id], .01)

    def worker(self, index: int = 0) -> Worker:
        worker = Worker(self.worker_config(index), self.root / f'journal-{index}')
        self.addCleanup(worker.close)
        return worker

    def assigned_tick(self, worker: Worker) -> dict:
        """Wait for durable asynchronous dispatch before inspecting an assignment."""
        deadline = time.monotonic() + 3
        while True:
            result = worker.tick()
            if result.get('assignments'):
                return result
            if time.monotonic() >= deadline:
                self.fail('No durable assignment arrived')
            time.sleep(.005)

    def submit(self, codes: tuple[int, ...] = (0, 7, 0, 0)) -> tuple[str, ...]:
        manifest = []
        for index, code in enumerate(codes):
            source = self.root / f'source-{index}'
            source.mkdir()
            (source / 'in.lmp').write_text(str(code))
            manifest.append({'source_directory': str(source), 'input_file': 'in.lmp'})
        path = self.root / 'batch.json'
        path.write_text(json.dumps(manifest))
        with Store(self.config.manager.database_path) as store:
            return submit_batch(store, self.config, path)

    def poll(self, worker: str, token: str, payload: dict) -> dict:
        request = Request(self.endpoint + '/v1/poll', data=json.dumps(payload).encode(),
                          headers={'X-Worker-ID': worker, 'Authorization': 'Bearer ' + token})
        with urlopen(request, timeout=3) as response:
            return json.load(response)

    def test_readiness_race_fences_assignment_and_places_on_other_worker(self) -> None:
        ids = self.submit((0,))
        worker = self.worker()
        other = self.worker(1)
        worker.tick()
        self.poll('a', 'a' * 40, {'proof': self.service.proof,
                  'snapshot': worker.monitor.sample(worker.config), 'reports': []})
        self.service.expire_workers()
        original = self.service.poll
        def close_after_dispatch(worker_id: str, payload: dict) -> dict:
            result = original(worker_id, payload)
            worker.draining = True
            return result
        with patch.object(self.service, 'poll', side_effect=close_after_dispatch):
            first = self.assigned_tick(worker)['assignments'][0]['attempt']['attempt_id']
        self.assertFalse(worker.futures)
        original_open = worker.opener.open
        def lose_response(*args, **kwargs):
            response = original_open(*args, **kwargs)
            def interrupted_read():
                raise IncompleteRead(b'{', 10)
            response.read = interrupted_read
            return response
        with patch.object(worker.opener, 'open', side_effect=lose_response), self.assertRaises(IncompleteRead):
            worker.tick()  # Manager committed the fence; response was lost.
        worker.tick()  # Replay must neither double-release nor create another return.
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'queued')
            self.assertIsNotNone(store.rejection(first))
            self.assertFalse(store.reservations())
        other.tick()
        other.tick()
        next(iter(other.futures.values())).result(timeout=3)
        other.tick()
        worker.tick()  # Delayed fence report cannot alter the new outcome.
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'succeeded')
            self.assertEqual(len(store.job_attempts(ids[0])), 2)
            self.assertNotEqual(store.job_attempts(ids[0])[-1].attempt_id, first)

    def test_two_workers_finish_batch_and_duplicate_delivery(self) -> None:
        ids = self.submit()
        workers = [self.worker(0), self.worker(1)]
        for worker in workers:
            worker.tick()  # Shared-path handshake.
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            for worker in workers:
                worker.tick()
            with Store(self.config.manager.database_path) as store:
                if all(store.job_state(job) in ('succeeded', 'failed') for job in ids):
                    break
            time.sleep(.01)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual([store.job_state(job) for job in ids], ['succeeded', 'failed', 'succeeded', 'succeeded'])
            self.assertEqual(store.reservations(), ())
            self.assertEqual({store.job_attempts(job)[0].worker_id for job in ids}, {'a', 'b'})
            for job, code in zip(ids, (0, 7, 0, 0)):
                attempt = store.job_attempts(job)[0]
                directory = Path(attempt.directory)
                self.assertEqual((directory / 'work/invocations').read_text(), 'launch\n')
                self.assertEqual(attempt.exit_code, code)
                self.assertTrue(attempt.termination_evidence)
                identity = json.loads((directory / 'process.json').read_text())
                self.assertGreater(identity['pid'], 0)
                self.assertEqual(identity['pid'], identity['process_group'])
                self.assertTrue(identity['start_ticks'].isdigit())
                self.assertTrue(identity['boot_id'])
                self.assertIn('output', (directory / 'stdout').read_text())
                self.assertIn('error', (directory / 'stderr').read_text())
                # Replay final reports to simulate a lost response.
                payload = {'proof': self.service.proof,
                    'snapshot': LinuxMonitor().sample(self.worker_config(0 if attempt.worker_id == 'a' else 1)),
                    'reports': [{'attempt_id': attempt.attempt_id, 'state': attempt.state,
                                 'started': True, 'exit_code': attempt.exit_code,
                                 'reason': attempt.reason, 'termination_evidence': attempt.termination_evidence}]}
                self.poll(attempt.worker_id, attempt.worker_id * 40, payload)
        for worker in workers:
            worker.tick()

    def test_legacy_shared_protocol_preserves_specification_keys(self) -> None:
        self.submit((0,))
        payload = {'proof': self.service.proof,
                   'snapshot': LinuxMonitor().sample(self.worker_config()), 'reports': []}
        deadline = time.monotonic() + 2
        while True:
            result = self.poll('a', 'a' * 40, payload)
            if result['assignments'] or time.monotonic() >= deadline:
                break
        specification = result['assignments'][0]['specification']
        self.assertEqual(set(specification), {'job_id', 'work_directory', 'input_file',
                                            'input_sha256', 'resources'})
        self.assertEqual(result['storage_mode'], 'shared')

    def test_authentication_shared_path_and_malformed_payload(self) -> None:
        self.submit((0,))
        for identity, token in [('unregistered', 'a' * 40), ('a', 'wrong'), ('a', 'b' * 40)]:
            with self.subTest(identity=identity, token=token), self.assertRaises(HTTPError) as caught:
                self.poll(identity, token, {})
            self.assertEqual(caught.exception.code, 401)
        result = self.poll('a', 'a' * 40, {})
        self.assertEqual(result['assignments'], [])
        with self.assertRaises(HTTPError) as caught:
            self.poll('a', 'a' * 40, {'proof': self.service.proof, 'snapshot': {}})
        self.assertEqual(caught.exception.code, 400)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.jobs()[0]['state'], 'queued')
        other = self.root / 'different-root'
        other.mkdir()
        worker = Worker(replace(self.worker_config(), work_root=str(other)), self.root / 'different-journal')
        self.addCleanup(worker.close)
        with self.assertRaisesRegex(ValueError, 'roots differ'):
            worker.tick()

    def test_pause_resume_persists_and_local_recheck_keeps_reservation(self) -> None:
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            store.set_worker('a', paused=True)
        self.assertTrue(worker.tick()['paused'])
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'queued')
            store.set_worker('a', paused=False)
        real_sample = worker.monitor.sample
        def low_memory(config: WorkerConfig) -> dict:
            return {**real_sample(config), 'available_memory_gib': 0}
        # Manager receives a usable sample; the worker has a stricter local CPU quota.
        worker.config = replace(worker.config, worker=replace(worker.config.worker, cpu_slots=1))
        with patch.object(worker.monitor, 'sample', side_effect=low_memory):
            worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'queued')
        result = worker.tick()
        self.assertTrue(result['assignments'])
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(len(store.reservations('a')), 1)

    def test_durable_acceptance_lock_and_restart_never_relaunch(self) -> None:
        ids = self.submit((0,))
        worker = self.worker()
        with self.assertRaisesRegex(ValueError, 'locked'):
            Worker(self.worker_config(), self.root / 'journal-0')
        worker.tick()
        with patch.object(worker.pool, 'submit'):
            result = self.assigned_tick(worker)
        assignment = result['assignments'][0]
        worker.pool.shutdown()
        worker.lock.close()
        # Reopen the same journal: assigned work is retained and never started.
        restarted = self.worker()
        restarted.tick()
        replay = self.assigned_tick(restarted)
        self.assertEqual(replay['assignments'][0]['attempt']['attempt_id'], assignment['attempt']['attempt_id'])
        self.assertFalse(restarted.futures)
        self.assertFalse(Path(assignment['attempt']['directory']).exists())
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'unknown')
            self.assertEqual(len(store.reservations()), 1)

    def test_foreign_reports_rejected(self) -> None:
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        with patch.object(worker.pool, 'submit'):
            worker.tick()
        with Store(self.config.manager.database_path) as store:
            attempt = store.job_attempts(ids[0])[0]
        payload = {'proof': self.service.proof, 'snapshot': LinuxMonitor().sample(self.worker_config(1)),
                   'reports': [{'attempt_id': attempt.attempt_id, 'state': 'failed',
                                'reason': 'forged', 'termination_evidence': 'forged'}]}
        with self.assertRaises(HTTPError):
            self.poll('b', 'b' * 40, payload)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.attempt(attempt.attempt_id).state, 'assigned')
            self.assertEqual(len(store.reservations()), 1)

    def test_stale_metrics_and_transactional_report_validation(self) -> None:
        ids = self.submit((0,))
        snapshot = LinuxMonitor().sample(self.worker_config())
        snapshot['observed_at'] = 0
        snapshot['cpu_sample_started_at'] = 0
        payload = {'proof': self.service.proof, 'snapshot': snapshot, 'reports': []}
        self.assertEqual(self.poll('a', 'a' * 40, payload)['assignments'], [])
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'queued')
        payload['snapshot']['observed_at'] = time.time()
        time.sleep(.003)
        result = self.poll('a', 'a' * 40, payload)
        attempt_id = result['assignments'][0]['attempt']['attempt_id']
        payload['reports'] = [{'attempt_id': attempt_id, 'state': 'running'},
                              {'attempt_id': attempt_id, 'state': 'invalid'}]
        with self.assertRaises(HTTPError):
            self.poll('a', 'a' * 40, payload)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.attempt(attempt_id).state, 'assigned')
            self.assertEqual([e['state'] for e in store.events(ids[0]) if e['state'] != 'queued'], ['assigned'])
            self.assertEqual(store.job_state(ids[0]), 'assigned')

    def test_local_recheck_returns_assignment_after_capacity_change(self) -> None:
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        self.poll('a', 'a' * 40, {'proof': self.service.proof,
                  'snapshot': worker.monitor.sample(worker.config), 'reports': []})
        self.service.expire_workers()
        original_open = worker.opener.open
        original_config = worker.config
        def lower_limit(*args: object, **kwargs: object) -> object:
            response = original_open(*args, **kwargs)
            worker.config = replace(original_config, worker=replace(original_config.worker,
                                                                    memory_budget_gib=.005))
            return response
        with patch.object(worker.opener, 'open', side_effect=lower_limit):
            result = worker.tick()
        self.assertTrue(result['assignments'])
        self.assertFalse(worker.futures)
        with Store(worker.database) as journal:
            self.assertEqual(journal.job_state(ids[0]), 'failed')
            self.assertIsNotNone(journal.rejection(result['assignments'][0]['attempt']['attempt_id']))
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'assigned')
            self.assertEqual(len(store.reservations()), 1)
        worker.config = original_config
        worker.tick()
        self.assertFalse(worker.futures)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'queued')
            self.assertFalse(store.reservations())

    def test_heartbeats_continue_during_execution_and_pause(self) -> None:
        self.program.write_text(self.program.read_text().replace('time.sleep(.08)', 'time.sleep(.4)'))
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        worker.tick()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with Store(worker.database) as journal:
                if any(j['job_id'] == ids[0] and j['state'] == 'running' for j in journal.jobs()):
                    break
            worker.tick()
            time.sleep(.005)
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'running')
            before = store.worker_records()['a']['snapshot']['heartbeat_at']
            store.set_worker('a', paused=True)
        time.sleep(.005)
        self.assertTrue(worker.tick()['paused'])
        with Store(self.config.manager.database_path) as store:
            self.assertGreater(store.worker_records()['a']['snapshot']['heartbeat_at'], before)
            self.assertEqual(len(store.reservations()), 1)
        next(iter(worker.futures.values())).result(timeout=3)
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'succeeded')
            self.assertEqual(store.reservations(), ())

    def test_duplicate_delivery_and_transport_loss_never_relaunch_or_release_early(self) -> None:
        from urllib.error import URLError
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        assignment = self.assigned_tick(worker)['assignments'][0]
        with patch.object(worker.opener, 'open', side_effect=URLError('temporary disconnection')):
            with self.assertRaises(URLError):
                worker.tick()
        next(iter(worker.futures.values())).result(timeout=3)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'assigned')
            self.assertEqual(len(store.reservations()), 1)
        original_poll = self.service.poll
        def replay(worker_id: str, payload: dict) -> dict:
            result = original_poll(worker_id, payload)
            result['assignments'] = [assignment]
            return result
        with patch.object(self.service, 'poll', side_effect=replay):
            worker.tick()
            worker.tick()
        self.assertFalse(worker.futures)
        directory = Path(assignment['attempt']['directory'])
        self.assertEqual((directory / 'work/invocations').read_text(), 'launch\n')
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'succeeded')
            self.assertEqual(store.reservations(), ())

    def test_existing_attempt_directory_is_unknown_and_preserved(self) -> None:
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        original_poll = self.service.poll
        def collide(worker_id: str, payload: dict) -> dict:
            result = original_poll(worker_id, payload)
            if not result['assignments']:
                return result
            directory = Path(result['assignments'][0]['attempt']['directory'])
            directory.mkdir()
            (directory / 'prior-output').write_text('preserve')
            return result
        with patch.object(self.service, 'poll', side_effect=collide):
            assignment = self.assigned_tick(worker)['assignments'][0]
        self.assertFalse(worker.futures)
        worker.tick()
        directory = Path(assignment['attempt']['directory'])
        self.assertEqual((directory / 'prior-output').read_text(), 'preserve')
        self.assertFalse((directory / 'work').exists())
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'unknown')
            self.assertEqual(len(store.reservations()), 1)

    def test_input_mismatch_is_reported_and_next_job_runs(self) -> None:
        ids = self.submit((0, 0))
        worker = self.worker()
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            spec = store.specification(ids[0])
        (Path(spec.work_directory) / 'inputs/in.lmp').write_text('7')
        self.assigned_tick(worker)
        next(iter(worker.futures.values())).result(timeout=3)
        with Store(worker.database) as journal:
            failed = journal.job_attempts(ids[0])[0]
            self.assertEqual(failed.state, 'failed')
            self.assertIsNone(failed.started_at)
            self.assertIn('SHA-256', failed.reason)
            self.assertEqual(failed.termination_evidence, 'Launch did not start a process')
            self.assertEqual(journal.reservations(), ())
        self.assertFalse((Path(failed.directory) / 'work/invocations').exists())
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'assigned')
            self.assertEqual(len(store.reservations('a')), 1)
        self.assigned_tick(worker)
        next(iter(worker.futures.values())).result(timeout=3)
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual([store.job_state(job) for job in ids], ['failed', 'succeeded'])
            self.assertEqual(store.job_attempts(ids[0])[0].reason, failed.reason)
            self.assertEqual(store.reservations(), ())

    def test_run_retries_incomplete_response_and_reports_completion(self) -> None:
        self.program.write_text(self.program.read_text().replace('time.sleep(.08)', 'time.sleep(.4)'))
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        worker.tick()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with Store(worker.database) as journal:
                if journal.job_state(ids[0]) == 'running':
                    break
            time.sleep(.005)
        original_open = worker.opener.open
        opens = 0
        sleeps = 0

        def interrupted_open(*args: object, **kwargs: object) -> object:
            nonlocal opens
            response = original_open(*args, **kwargs)
            opens += 1
            if opens == 1:
                def interrupted_read() -> bytes:
                    raise IncompleteRead(b'{', 10)
                response.read = interrupted_read
            return response

        def next_poll(interval: float) -> None:
            nonlocal sleeps
            sleeps += 1
            if sleeps == 1:
                with Store(self.config.manager.database_path) as store:
                    self.assertEqual(store.job_state(ids[0]), 'running')
                    self.assertEqual(len(store.reservations()), 1)
                self.assertEqual(worker.acknowledged, {})
                next(iter(worker.futures.values())).result(timeout=3)
            else:
                raise KeyboardInterrupt

        with patch.object(worker.opener, 'open', side_effect=interrupted_open), \
                patch('lammps_manager.worker.time.sleep', side_effect=next_poll), \
                patch('sys.stderr'), self.assertRaises(KeyboardInterrupt):
            worker.run()
        self.assertEqual(opens, 2)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'succeeded')
            self.assertEqual(store.reservations(), ())
            attempt = store.job_attempts(ids[0])[0]
        self.assertEqual((Path(attempt.directory) / 'work/invocations').read_text(), 'launch\n')

    def _shared_check_recovers(self, operation: str, target: str) -> None:
        self.program.write_text(self.program.read_text().replace('time.sleep(.08)', 'time.sleep(.4)'))
        ids = self.submit((0, 0))
        worker = self.worker()
        worker.tick()
        self.assigned_tick(worker)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            with Store(worker.database) as journal:
                if journal.job_state(ids[0]) == 'running':
                    break
            time.sleep(.005)
        else:
            self.fail('First execution did not start')
        original = getattr(Path, operation)
        selected = self.service.challenge if target == 'challenge' else self.work
        failures = 0
        sleeps = 0

        def unavailable(path: Path, *args: object, **kwargs: object) -> object:
            nonlocal failures
            if path == selected and sleeps == 0:
                failures += 1
                if operation == 'read_text':
                    raise PermissionError('Shared challenge temporarily unreadable')
                raise FileNotFoundError('Shared path temporarily unavailable')
            return original(path, *args, **kwargs)

        def next_poll(interval: float) -> None:
            nonlocal sleeps
            sleeps += 1
            if sleeps == 1:
                self.assertEqual(worker.acknowledged, {})
                with Store(self.config.manager.database_path) as store:
                    self.assertEqual(store.job_state(ids[0]), 'running')
                    self.assertEqual(len(store.reservations()), 1)
                with Store(worker.database) as journal:
                    self.assertEqual(len(journal.reservations()), 1)
                next(iter(worker.futures.values())).result(timeout=3)
            elif sleeps == 2:
                # Restored shared storage permits result delivery and the next launch.
                with Store(self.config.manager.database_path) as store:
                    self.assertEqual(store.job_state(ids[0]), 'succeeded')
                    self.assertEqual(store.job_state(ids[1]), 'assigned')
                next(iter(worker.futures.values())).result(timeout=3)
            else:
                raise KeyboardInterrupt

        with patch.object(Path, operation, unavailable), \
                patch('lammps_manager.worker.time.sleep', side_effect=next_poll), \
                patch('sys.stderr') as errors, self.assertRaises(KeyboardInterrupt):
            worker.run()
        self.assertEqual(failures, 1)
        self.assertIn('Shared storage check deferred', str(errors.write.call_args_list))
        with Store(self.config.manager.database_path) as store:
            self.assertEqual([store.job_state(job) for job in ids], ['succeeded', 'succeeded'])
            self.assertEqual(store.reservations(), ())
            for job in ids:
                directory = Path(store.job_attempts(job)[0].directory)
                self.assertEqual((directory / 'work/invocations').read_text(), 'launch\n')

    def test_run_retries_missing_shared_challenge(self) -> None:
        self._shared_check_recovers('resolve', 'challenge')

    def test_run_retries_unavailable_shared_root(self) -> None:
        self._shared_check_recovers('resolve', 'root')

    def test_run_retries_unreadable_shared_challenge(self) -> None:
        self._shared_check_recovers('read_text', 'challenge')

    def test_shared_check_failure_defers_assignment_acceptance(self) -> None:
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        original = Path.read_text

        def unreadable(path: Path, *args: object, **kwargs: object) -> str:
            if path == self.service.challenge:
                raise PermissionError('Shared challenge temporarily unreadable')
            return original(path, *args, **kwargs)

        with patch.object(Path, 'read_text', unreadable), patch('sys.stderr'):
            result = worker.tick()
        self.assertTrue(result['assignments'])
        self.assertFalse(worker.futures)
        with Store(worker.database) as journal:
            self.assertEqual(journal.jobs(), ())
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'assigned')
            self.assertEqual(len(store.reservations()), 1)
        worker.tick()
        next(iter(worker.futures.values())).result(timeout=3)
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'succeeded')
            self.assertEqual(store.reservations(), ())
            directory = Path(store.job_attempts(ids[0])[0].directory)
        self.assertEqual((directory / 'work/invocations').read_text(), 'launch\n')

    def test_shared_challenge_symlink_outside_root_is_rejected(self) -> None:
        worker = self.worker()
        outside = self.root / '.shared-check-outside'
        outside.write_text(self.service.proof)
        self.service.challenge.unlink()
        self.service.challenge.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, 'roots differ'):
            worker.tick()
        self.assertIsNone(worker.proof)

    def test_missing_job_directory_is_deferred_until_restored(self) -> None:
        ids = self.submit((0, 0))
        worker = self.worker()
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            job_root = Path(store.specification(ids[0]).work_directory)
        unavailable = job_root.with_name(job_root.name + '-unavailable')
        job_root.rename(unavailable)
        with patch('sys.stderr') as errors:
            assignment = self.assigned_tick(worker)['assignments'][0]
            worker.tick()
        self.assertIn('FileNotFoundError', str(errors.write.call_args_list))
        self.assertFalse(worker.futures)
        with Store(worker.database) as journal:
            self.assertEqual(journal.jobs(), ())
        with Store(self.config.manager.database_path) as store:
            self.assertEqual([store.job_state(job) for job in ids], ['assigned', 'queued'])
            self.assertEqual(len(store.reservations()), 1)
        unavailable.rename(job_root)
        worker.tick()
        next(iter(worker.futures.values())).result(timeout=3)
        worker.tick()
        next(iter(worker.futures.values())).result(timeout=3)
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual([store.job_state(job) for job in ids], ['succeeded', 'succeeded'])
            self.assertEqual(store.reservations(), ())
        directory = Path(assignment['attempt']['directory'])
        self.assertEqual((directory / 'work/invocations').read_text(), 'launch\n')

    def test_job_directory_permission_error_is_deferred(self) -> None:
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            job_root = Path(store.specification(ids[0]).work_directory)
        self.poll('a', 'a' * 40, {'proof': self.service.proof,
                  'snapshot': worker.monitor.sample(worker.config), 'reports': []})
        self.service.expire_workers()
        original_resolve = Path.resolve

        def resolve(path: Path, *args: object, **kwargs: object) -> Path:
            if path == job_root:
                raise PermissionError('Job directory access denied')
            return original_resolve(path, *args, **kwargs)

        with patch.object(Path, 'resolve', resolve), patch('sys.stderr') as errors:
            worker.tick()
        self.assertIn('PermissionError', str(errors.write.call_args_list))
        self.assertFalse(worker.futures)
        with Store(worker.database) as journal:
            self.assertEqual(journal.jobs(), ())
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'assigned')
            self.assertEqual(len(store.reservations()), 1)
        worker.tick()
        next(iter(worker.futures.values())).result(timeout=3)
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(ids[0]), 'succeeded')
            self.assertEqual(store.reservations(), ())

    def test_assignment_symlink_outside_shared_root_is_rejected(self) -> None:
        ids = self.submit((0,))
        worker = self.worker()
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            job_root = Path(store.specification(ids[0]).work_directory)
        outside = self.root / 'outside'
        job_root.rename(outside)
        job_root.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'outside shared job directory'):
            self.assigned_tick(worker)
        with Store(worker.database) as journal:
            self.assertEqual(journal.jobs(), ())
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(len(store.reservations()), 1)
        self.assertFalse(any(outside.glob('*/work/invocations')))

    def test_removed_inventory_snapshot_does_not_block_other_workers(self) -> None:
        snapshot = LinuxMonitor().sample(self.worker_config())
        snapshot['capacity']['id'] = 'retired'
        with Store(self.config.manager.database_path) as store:
            store.set_worker('retired', snapshot=snapshot, paused=True)
        worker = self.worker()
        worker.tick()
        self.assertEqual(worker.tick()['assignments'], [])

    def test_cli_pause_resume_and_identity_binding(self) -> None:
        import yaml
        from dataclasses import asdict
        config_path = self.root / 'manager.yaml'
        config_path.write_text(yaml.safe_dump(asdict(self.config)))
        for command, expected in [('pause', True), ('resume', False)]:
            result = subprocess.run([sys.executable, '-m', 'lammps_manager', command,
                                     '--config', str(config_path), 'a'],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            with Store(self.config.manager.database_path) as store:
                self.assertIs(store.worker_records()['a']['paused'], expected)
        worker = self.worker()
        worker.close()
        with self.assertRaisesRegex(ValueError, 'identity differs'):
            Worker(self.worker_config(1), self.root / 'journal-0')

    @unittest.skipUnless(shutil.which('openssl'), 'openssl is needed to create a temporary TLS certificate')
    def test_https_verifies_private_ca_and_rejects_untrusted_server(self) -> None:
        certificate = self.root / 'server.crt'
        key = self.root / 'server.key'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(key), '-out', str(certificate), '-days', '1',
                        '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1'],
                       check=True, capture_output=True, timeout=10)
        server = make_server(self.service, '127.0.0.1', 0,
                             certfile=str(certificate), keyfile=str(key))
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
        thread.start()
        def stop() -> None:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()
        self.addCleanup(stop)
        config = replace(self.worker_config(), manager_endpoint=f'https://127.0.0.1:{server.server_port}')
        worker = Worker(config, self.root / 'tls-journal')
        self.addCleanup(worker.close)
        from urllib.error import URLError
        with self.assertRaises(URLError):
            worker.tick()
        with patch.dict(os.environ, {'SSL_CERT_FILE': str(certificate)}):
            import ssl
            from urllib.request import HTTPSHandler, ProxyHandler, build_opener
            worker.opener = build_opener(ProxyHandler({}), HTTPSHandler(context=ssl.create_default_context(cafile=str(certificate))))
            result = worker.tick()
        self.assertEqual(result['assignments'], [])
        self.assertEqual(worker.proof, self.service.proof)

    @unittest.skipUnless(shutil.which('openssl'), 'openssl is needed to create a temporary TLS certificate')
    def test_idle_tls_handshake_does_not_block_authenticated_poll(self) -> None:
        import socket
        import ssl
        certificate = self.root / 'idle-server.crt'
        key = self.root / 'idle-server.key'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(key), '-out', str(certificate), '-days', '1',
                        '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1'],
                       check=True, capture_output=True, timeout=10)
        server = make_server(self.service, '127.0.0.1', 0,
                             certfile=str(certificate), keyfile=str(key))
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
        thread.start()
        def stop() -> None:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()
        self.addCleanup(stop)
        config = replace(self.worker_config(), manager_endpoint=f'https://127.0.0.1:{server.server_port}')
        worker = Worker(config, self.root / 'idle-tls-journal')
        self.addCleanup(worker.close)
        accepted = threading.Event()
        original_wrap = ssl.SSLContext.wrap_socket
        def wrap(context, *args, **kwargs):
            if kwargs.get('server_side'):
                accepted.set()
            return original_wrap(context, *args, **kwargs)
        from urllib.request import HTTPSHandler, ProxyHandler, build_opener
        worker.opener = build_opener(ProxyHandler({}), HTTPSHandler(context=ssl.create_default_context(cafile=str(certificate))))
        original_open = worker.opener.open
        with patch.object(ssl.SSLContext, 'wrap_socket', wrap), \
                patch.object(worker.opener, 'open', side_effect=lambda request, timeout: original_open(request, timeout=.5)), \
                patch.dict(os.environ, {'SSL_CERT_FILE': str(certificate)}):
            idle = socket.create_connection(('127.0.0.1', server.server_port), timeout=1)
            try:
                self.assertTrue(accepted.wait(1), 'Idle peer must reach the server TLS handshake')
                result = worker.tick()
                self.assertEqual(result['assignments'], [])
                self.assertEqual(worker.proof, self.service.proof)
            finally:
                idle.close()

    def test_transport_and_credential_checks(self) -> None:
        path = Path(self.tokens['a'])
        path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, 'private'):
            read_token(path)
        path.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'TLS'):
            make_server(self.service, '0.0.0.0', 0)
        with self.assertRaisesRegex(ValueError, 'loopback'):
            Worker(replace(self.worker_config(), manager_endpoint='http://192.0.2.1:8443'), self.root / 'bad')
        with self.assertRaisesRegex(ValueError, 'match registered'):
            ManagerService(self.config, {'a': self.tokens['a']})
        sample = LinuxMonitor().sample(self.worker_config())
        self.assertGreater(sample['detected_cpu_slots'], 0)
        self.assertGreater(sample['detected_memory_gib'], 0)
        self.assertGreaterEqual(sample['cpu_utilization_percent'], 0)
        self.assertLessEqual(sample['cpu_utilization_percent'], 100)
