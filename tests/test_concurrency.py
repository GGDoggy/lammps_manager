"""Concurrent polling workers verified with isolated local fake processes."""

from dataclasses import replace
from pathlib import Path
import time
import unittest

import test_workers as worker_tests
import test_staging as staging_tests
from lammps_manager.worker import Worker
from lammps_manager.config import WorkerCapacity
from lammps_manager.store import Store


class ConcurrentWorkerTests(unittest.TestCase):
    setUp = worker_tests.WorkerTests.setUp
    worker_config = worker_tests.WorkerTests.worker_config
    worker = worker_tests.WorkerTests.worker
    submit = worker_tests.WorkerTests.submit

    def test_shared_attempts_overlap_and_duplicate_polls_do_not_relaunch(self) -> None:
        self.capacities = (WorkerCapacity('a', 4, 2, 2), self.capacities[1])
        self.config = replace(self.config, workers=self.capacities,
                              job_defaults=replace(self.config.job_defaults, mpi_ranks=2))
        self.service.config = self.config
        self.program.write_text(self.program.read_text().replace('time.sleep(.08)', 'time.sleep(.6)'))
        jobs = self.submit((0, 0, 0))
        worker = self.worker()
        worker.tick()
        deadline = time.monotonic() + 3
        overlap = False
        while time.monotonic() < deadline:
            worker.tick()
            with Store(worker.database) as journal:
                active = [journal.attempt(r.attempt_id) for r in journal.reservations()]
                if len([a for a in active if a.state == 'running']) == 2:
                    overlap = True
                    self.assertEqual(sum(r.cpu_slots for r in journal.reservations()), 4)
                    break
            time.sleep(.01)
        self.assertTrue(overlap, 'Two fake jobs must run concurrently on one worker')
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            worker.tick()
            with Store(self.config.manager.database_path) as store:
                if all(store.job_state(job) == 'succeeded' for job in jobs):
                    for job in jobs:
                        attempt, = store.job_attempts(job)
                        self.assertEqual((Path(attempt.directory) / 'work/invocations').read_text(), 'launch\n')
                    return
            time.sleep(.01)
        self.fail('Concurrent batch did not complete')

    def test_per_cpu_policy_runs_concurrent_fake_jobs_without_aggregate_veto(self) -> None:
        self.config = replace(self.config, scheduler=replace(
            self.config.scheduler, account_external_load=True))
        original = self.worker
        def monitored_worker():
            worker = original()
            sample = worker.monitor.sample
            def deterministic(config):
                metrics = sample(config)
                metrics.update(cpu_utilization_percent=100,
                               per_cpu_utilization_percent={str(i): 0 for i in range(4)},
                               detected_cpu_slots=4)
                return metrics
            worker.monitor.sample = deterministic
            return worker
        self.worker = monitored_worker
        self.test_shared_attempts_overlap_and_duplicate_polls_do_not_relaunch()

    def test_per_cpu_launch_defers_and_excludes_candidate_once(self) -> None:
        from dataclasses import asdict
        self.capacities = (WorkerCapacity('a', 8, 8, 4), self.capacities[1])
        jobs = self.submit((0,))
        worker = self.worker()
        snapshot = worker.monitor.sample(worker.config)
        snapshot.update(detected_cpu_slots=8, detected_memory_gib=8,
                        available_memory_gib=8, observed_at=time.time(),
                        cpu_sample_started_at=time.time() - 1,
                        per_cpu_utilization_percent={'0': 0, '7': 0},
                        cpu_utilization_percent=100)
        result = dict(paused=False, manager_capacity=asdict(self.capacities[0]),
                      policy={**asdict(self.config.scheduler), 'account_external_load': True})
        resources = replace(self.config.job_defaults, mpi_ranks=2, memory_gib=1)
        with Store(self.config.manager.database_path) as manager, Store(worker.database) as journal:
            spec = replace(manager.specification(jobs[0]), resources=resources)
            journal.submit(spec)
            journal.assign(spec.job_id, 'pending', 'a', str(self.work / 'pending'))
            self.assertTrue(worker._can_launch(journal, resources, result, snapshot, 'pending'))
            self.assertFalse(worker._can_launch(journal, resources, result, snapshot))
            self.assertEqual(worker.cpu_admission['pending'], 2)
            self.assertFalse(worker._can_launch(journal, resources, result,
                             {**snapshot, 'per_cpu_utilization_percent': {'0': 0}}, 'pending'))
            self.assertEqual(len(journal.reservations()), 1)
            # Floor can bypass load, but never absent monitoring or staleness.
            floor = {**result, 'policy': {**result['policy'], 'min_slots': 2}}
            self.assertTrue(worker._can_launch(journal, resources, floor,
                            {**snapshot, 'per_cpu_utilization_percent': {'0': 100}}, 'pending'))
            self.assertFalse(worker._can_launch(journal, resources, floor,
                             {**snapshot, 'per_cpu_utilization_percent': None}, 'pending'))
            self.assertFalse(worker._can_launch(journal, resources, floor,
                             {**snapshot, 'observed_at': time.time() - 100}, 'pending'))

    def test_local_admission_counts_unresolved_and_pending_reservations(self) -> None:
        from dataclasses import asdict
        from lammps_manager.worker import LinuxMonitor
        self.capacities = (WorkerCapacity('a', 8, 8, 4), self.capacities[1])
        jobs = self.submit((0,))
        worker = self.worker()
        snapshot = LinuxMonitor().sample(worker.config)
        snapshot.update(detected_cpu_slots=8, detected_memory_gib=8,
                        available_memory_gib=2, observed_at=time.time())
        worker.sample_started = set()
        policy = asdict(self.config.scheduler)
        result = dict(paused=False, manager_capacity=asdict(self.capacities[0]), policy=policy)
        resources = replace(self.config.job_defaults, mpi_ranks=2,
                            omp_threads_per_rank=2, memory_gib=1)
        with Store(self.config.manager.database_path) as manager, Store(worker.database) as journal:
            spec = replace(manager.specification(jobs[0]), resources=resources)
            journal.submit(spec)
            journal.assign(spec.job_id, 'pending', 'a', str(self.work / 'pending'))
            self.assertTrue(worker._can_launch(journal, resources, result, snapshot))
            for change in ({'cpu_slots': 7}, {'memory_budget_gib': 1.5}, {'max_concurrent_jobs': 1}):
                with self.subTest(change=change):
                    limited = {**result, 'manager_capacity': {**result['manager_capacity'], **change}}
                    self.assertFalse(worker._can_launch(journal, resources, limited, snapshot))
            self.assertFalse(worker._can_launch(journal, resources, result,
                                               {**snapshot, 'detected_cpu_slots': 7}))
            self.assertFalse(worker._can_launch(journal, resources, result,
                                               {**snapshot, 'available_memory_gib': 1.9}))
            self.assertFalse(worker._can_launch(journal, resources, result,
                                               {**snapshot, 'observed_at': time.time() - 1000}))
            # The existing candidate is deducted once, permitting exact fits.
            self.assertTrue(worker._can_launch(journal, resources, result,
                                              {**snapshot, 'available_memory_gib': 1}, 'pending'))
            journal.transition('pending', 'running')
            # A new running transition must not reuse the same sampled memory.
            self.assertFalse(worker._can_launch(journal, resources, result,
                                               {**snapshot, 'available_memory_gib': 1.9}))
            snapshot['observed_at'] = time.time() + .001
            time.sleep(.002)
            worker.sample_started = {'pending'}
            self.assertTrue(worker._can_launch(journal, resources, result,
                                              {**snapshot, 'available_memory_gib': 1}))
            journal.transition('pending', 'unknown', reason='Lost identity')
            journal.request_cancel('pending', 'Cancellation awaiting termination')
            self.assertFalse(worker._can_launch(journal, resources, result,
                                               {**snapshot, 'detected_cpu_slots': 7}))
            self.assertEqual(len(journal.reservations()), 1)

    def test_cancellation_and_timeout_leave_other_execution_running(self) -> None:
        from lammps_manager.recovery import session_members
        for control in ('cancel', 'timeout'):
            with self.subTest(control=control):
                if control == 'timeout':
                    self.doCleanups()
                    self.setUp()
                self.capacities = (WorkerCapacity('a', 2, 2, 2), self.capacities[1])
                self.config = replace(self.config, workers=self.capacities)
                self.service.config = self.config
                self.program.write_text(self.program.read_text().replace('time.sleep(.08)', 'time.sleep(2)'))
                if control == 'timeout':
                    from unittest.mock import patch
                    original_submit = worker_tests.submit_batch
                    def short_limit(store, config, path):
                        import json
                        manifest = json.loads(path.read_text())
                        manifest[0]['resources'] = {'time_limit_seconds': 1}
                        manifest[1]['resources'] = {'time_limit_seconds': 10}
                        path.write_text(json.dumps(manifest))
                        return original_submit(store, config, path)
                    with patch.object(worker_tests, 'submit_batch', side_effect=short_limit):
                        jobs = self.submit((0, 0))
                else:
                    jobs = self.submit((0, 0))
                worker = self.worker()
                worker.tick()
                deadline = time.monotonic() + 4
                while time.monotonic() < deadline:
                    worker.tick()
                    with Store(worker.database) as store:
                        active = [store.attempt(r.attempt_id) for r in store.reservations()]
                        if len(active) == 2 and all(a.state == 'running' for a in active):
                            break
                    time.sleep(.01)
                else:
                    self.fail('Both attempts did not start')
                with Store(self.config.manager.database_path) as store:
                    if control == 'cancel':
                        store.request_cancel(store.job_attempts(jobs[0])[0].attempt_id,
                                             'Cancel one concurrent attempt')
                deadline = time.monotonic() + 4
                isolated = False
                while time.monotonic() < deadline:
                    worker.tick()
                    with Store(worker.database) as store:
                        first, = store.job_attempts(jobs[0])
                        second, = store.job_attempts(jobs[1])
                        if first.state == 'cancelled' and second.state == 'running':
                            import json
                            pid = json.loads((Path(second.directory) / 'process.json').read_text())['pid']
                            self.assertTrue(session_members(pid))
                            self.assertTrue(first.termination_evidence)
                            isolated = True
                            break
                    time.sleep(.01)
                self.assertTrue(isolated, 'Cancelling or timing out one attempt must preserve the other')

    def test_recovered_process_is_controlled_while_new_attempt_runs(self) -> None:
        import json
        import subprocess
        import sys
        from lammps_manager.recovery import process_identity, session_members
        self.capacities = (WorkerCapacity('a', 2, 2, 2), self.capacities[1])
        self.config = replace(self.config, workers=self.capacities)
        self.service.config = self.config
        self.program.write_text(self.program.read_text().replace('time.sleep(.08)', 'time.sleep(2)'))
        jobs = self.submit((0, 0))
        worker = self.worker()
        worker.tick()
        # Persist both assignments at the manager without launching locally.
        from unittest.mock import patch
        with patch.object(worker, '_can_launch', return_value=False):
            worker.tick()
        with Store(self.config.manager.database_path) as manager:
            recovered, = manager.job_attempts(jobs[0])
            spec = manager.specification(jobs[0])
        directory = Path(recovered.directory)
        directory.mkdir()
        process = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(15)'],
                                   start_new_session=True)
        def cleanup() -> None:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
        self.addCleanup(cleanup)
        (directory / 'process.json').write_text(json.dumps(process_identity(process.pid)))
        with Store(worker.database) as store:
            store.submit(spec)
            store.assign(spec.job_id, recovered.attempt_id, 'a', str(directory))
            store.transition(recovered.attempt_id, 'running')
        worker.close()
        # Reopen a durable live journal, then accept another concurrent job.
        worker = self.worker()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            worker.tick()
            with Store(worker.database) as store:
                newer = (store.job_attempts(jobs[1]) if any(
                    row['job_id'] == jobs[1] for row in store.jobs()) else ())
                if newer and newer[0].state == 'running':
                    break
            time.sleep(.01)
        else:
            self.fail('New work did not coexist with recovered execution')
        with Store(self.config.manager.database_path) as manager:
            manager.request_cancel(recovered.attempt_id, 'Stop recovered attempt only')
        worker.tick()  # Receive cancellation while the new executor owns its attempt.
        worker.tick()  # Reconcile the unowned recovered attempt independently.
        with Store(worker.database) as store:
            self.assertEqual(store.attempt(recovered.attempt_id).state, 'cancelled')
            self.assertEqual(store.job_attempts(jobs[1])[0].state, 'running')
            self.assertEqual(len(store.reservations()), 1)
        self.assertEqual(session_members(process.pid), ())
        process.wait(timeout=5)


class ConcurrentStagedTests(unittest.TestCase):
    setUp = staging_tests.StagedWorkerTests.setUp

    def test_pending_staged_jobs_progress_when_memory_only_fits_one(self) -> None:
        import sys
        from threading import Event
        from unittest.mock import patch
        from lammps_manager.config import Execution
        from lammps_manager.local import submit_batch

        self.capacity = replace(self.capacity, max_concurrent_jobs=2)
        self.service.config = replace(self.config, workers=(self.capacity, self.config.workers[1]),
            scheduler=replace(self.config.scheduler, account_external_load=True))
        launcher = self.root / 'launcher'
        launcher.write_text('#!' + sys.executable + '\nimport time\ntime.sleep(.08)\n')
        launcher.chmod(0o700)
        config = replace(self.worker_config, worker=self.capacity,
                         execution=Execution('/bin/false', str(launcher)))
        with Store(self.config.manager.database_path) as store:
            second, = submit_batch(store, self.config, self.root / 'batch.json')
        worker = Worker(config, self.root / 'journal')
        self.addCleanup(worker.close)
        sample = worker.monitor.sample(config)
        available = .025
        def sample_memory(_config):
            return {**sample, 'observed_at': time.time(),
                    'cpu_sample_started_at': time.time() - .01,
                    'per_cpu_utilization_percent': {'0': 0, '1': 0},
                    'detected_cpu_slots': 2, 'cpu_utilization_percent': 100,
                    'available_memory_gib': available}

        # Hold the executor before any running transition or memory consumption:
        # the durable launch claim must already consume pending headroom.
        release = Event()
        execute = worker._execute
        def held_execute(attempt_id):
            if not release.wait(5):
                raise TimeoutError('Test executor was not released')
            execute(attempt_id)

        try:
            with patch.object(worker.monitor, 'sample', side_effect=sample_memory), \
                    patch.object(worker, '_execute', side_effect=held_execute):
                worker.tick()  # Accept and stage both while combined memory fits.
                with Store(worker.database) as store:
                    self.assertEqual(len(store.reservations()), 2)
                available = .015  # Still enough for either individual .01 GiB job.
                worker.tick()
                self.assertEqual(len(worker.futures), 1)
                first_future = next(iter(worker.futures.values()))
                for _ in range(2):
                    worker.tick()
                    self.assertEqual(len(worker.futures), 1)
                    with Store(worker.database) as store:
                        self.assertEqual(len(store.reservations()), 2)
                        self.assertEqual(sum(store.launch_claimed(r.attempt_id)
                                             for r in store.reservations()), 1)
                release.set()
                first_future.result(timeout=5)
                worker.tick()
                self.assertEqual(len(worker.futures), 1)
                next(iter(worker.futures.values())).result(timeout=5)
                worker.tick()
                with Store(self.config.manager.database_path) as store:
                    for job in (self.job_id, second):
                        self.assertEqual(store.job_state(job), 'succeeded')
                        self.assertEqual(len(store.job_attempts(job)), 1)
                    self.assertEqual(store.reservations(), ())
        finally:
            release.set()

    def test_unclaimed_staged_reservations_keep_quotas_and_uncertainty_checks(self) -> None:
        from dataclasses import asdict
        self.capacity = replace(self.capacity, max_concurrent_jobs=2)
        worker = Worker(replace(self.worker_config, worker=self.capacity), self.root / 'journal')
        self.addCleanup(worker.close)
        snapshot = worker.monitor.sample(worker.config)
        snapshot.update(available_memory_gib=.015, detected_cpu_slots=2,
                        detected_memory_gib=2)
        result = dict(paused=False, manager_capacity=asdict(self.capacity),
                      policy=asdict(self.config.scheduler))
        with Store(worker.database) as store:
            store.submit(self.spec)
            store.assign(self.spec.job_id, 'pending', 'a', str(self.worker_root / 'pending'))
            resources = self.spec.resources
            self.assertTrue(worker._can_launch(store, resources, result, snapshot))
            for limit in ({'cpu_slots': 1}, {'memory_budget_gib': .015},
                          {'max_concurrent_jobs': 1}):
                with self.subTest(limit=limit):
                    limited = {**result, 'manager_capacity': {**result['manager_capacity'], **limit}}
                    self.assertFalse(worker._can_launch(store, resources, limited, snapshot))
            store.transition('pending', 'unknown', reason='Uncertain process identity')
            self.assertFalse(worker._can_launch(store, resources, result, snapshot))
            store.request_cancel('pending')
            self.assertFalse(worker._can_launch(store, resources, result, snapshot))
            self.assertEqual(len(store.reservations()), 1)

    def test_staged_overlap_collection_and_restart_preserve_isolated_results(self) -> None:
        import threading
        from unittest.mock import patch
        from lammps_manager.config import Execution
        from lammps_manager.local import submit_batch
        from lammps_manager.worker import Worker
        self.capacity = replace(self.capacity, max_concurrent_jobs=2)
        self.config = replace(self.config, workers=(self.capacity, self.config.workers[1]))
        self.service.config = self.config
        launcher = self.root / 'launcher'
        launcher.write_text('#!/usr/bin/env python3\n'
                            'import pathlib,time\n'
                            'p=pathlib.Path("count"); p.write_text("one")\n'
                            'print("isolated output", flush=True)\n'
                            'time.sleep(.5)\n')
        launcher.chmod(0o700)
        self.worker_config = replace(self.worker_config, worker=self.capacity,
                                     execution=Execution('/bin/false', str(launcher)))
        with Store(self.config.manager.database_path) as store:
            second, = submit_batch(store, self.config, self.root / 'batch.json')
        worker = Worker(self.worker_config, self.root / 'journal')
        self.addCleanup(worker.close)
        worker.tick()  # Prepare both inputs.
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            worker.tick()
            with Store(worker.database) as store:
                attempts = [store.attempt(r.attempt_id) for r in store.reservations()]
                if len(attempts) == 2 and all(a.state == 'running' for a in attempts):
                    break
            time.sleep(.01)
        else:
            self.fail('Staged attempts did not overlap')
        for future in worker.futures.values():
            future.result(timeout=5)
        entered, release = threading.Event(), threading.Event()
        original = worker._result_request
        def blocked(*args):
            entered.set()
            if not release.wait(5):
                raise TimeoutError('Test upload not released')
            return original(*args)
        try:
            with patch.object(worker, '_result_request', side_effect=blocked):
                worker.tick()
                self.assertTrue(entered.wait(2))
                with Store(self.config.manager.database_path) as store:
                    third, = submit_batch(store, self.config, self.root / 'batch.json')
                worker.tick()
                worker.tick()
                self.assertIn(third, self._owned_jobs(worker))
                # An in-flight upload cannot prevent heartbeats/new execution.
                self.assertFalse(worker.collection_future.done())
                for future in worker.futures.values():
                    future.result(timeout=5)
                release.set()
                worker.collection_future.result(timeout=5)
        finally:
            release.set()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            worker.tick()
            if worker.collection_future:
                worker.collection_future.result(timeout=5)
            with Store(self.config.manager.database_path) as store:
                attempts = [store.job_attempts(job)[0] for job in (self.job_id, second, third)]
                if all(store.result_collection(a.attempt_id)['status'] == 'collected' for a in attempts):
                    break
            time.sleep(.01)
        else:
            self.fail('Concurrent staged results were not collected')
        worker.close()
        restarted = Worker(self.worker_config, self.root / 'journal')
        self.addCleanup(restarted.close)
        with patch.object(restarted, '_execute', side_effect=AssertionError('Duplicate execution')):
            restarted.tick()
            restarted.tick()
        with Store(self.config.manager.database_path) as store, Store(restarted.database) as journal:
            self.assertEqual(store.reservations(), ())
            for job in (self.job_id, second, third):
                attempt, = store.job_attempts(job)
                local, = journal.job_attempts(job)
                self.assertEqual(attempt.state, 'succeeded')
                self.assertEqual((Path(local.directory) / 'work/count').read_text(), 'one')
                self.assertEqual((Path(attempt.directory) / 'results/work/count').read_text(), 'one')
                self.assertIn('isolated output', (Path(attempt.directory) / 'results/stdout').read_text())

    def _owned_jobs(self, worker: Worker) -> list[str]:
        with Store(worker.database) as store:
            return [store.attempt(attempt_id).job_id for attempt_id in worker.futures]
