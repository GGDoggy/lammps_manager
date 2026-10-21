"""Admission boundaries and transactional quota accounting with fake metrics."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from lammps_manager.config import (ManagerConfig, ManagerSettings, Storage,
                                   WorkerCapacity, Scheduler)
from lammps_manager.local import inspect_job
from lammps_manager.models import JobSpecification, Resources
from lammps_manager.scheduler import WorkerSnapshot, schedule
from lammps_manager.store import Store, StoreError


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'state.sqlite'
        self.store = Store(self.path)
        self.addCleanup(self.store.close)
        self.worker = WorkerCapacity('w', 8, 32, 4)
        self.config = ManagerConfig(ManagerSettings(str(self.path)), Storage('/work'),
                                    (self.worker,))
        self.snapshot = WorkerSnapshot(self.worker, 8, 32, 100, 100, 10, 32,
                                       per_cpu_utilization_percent={str(i): 10 for i in range(8)},
                                       cpu_sample_started_at=99)

    def submit(self, name='j', cpu=4, memory=8, threads=1):
        self.store.submit(JobSpecification(name, '/work/' + name, 'in', 'a' * 64,
                          Resources(mpi_ranks=cpu, omp_threads_per_rank=threads,
                                    memory_gib=memory)))

    def run_round(self, snapshot=None, config=None):
        return schedule(self.store, config or self.config,
                        (snapshot or self.snapshot,), now=100)

    def reason(self, result, job='j'):
        return json.loads(result.waiting[job])['workers']['w']

    def test_worker_exclusions_and_thresholds(self):
        self.submit()
        cases = [({'online': False}, 'offline'), ({'paused': True}, 'paused'),
                 ({'observed_at': 69.99, 'cpu_sample_started_at': 60}, 'stale_metrics'),
                 ({'heartbeat_at': 39.99}, 'heartbeat_timeout'),
                 ({'observed_at': 101}, 'future_timestamp'),
                 ({'heartbeat_at': 101}, 'future_timestamp'),
                 ({'per_cpu_utilization_percent': {str(i): 80 for i in range(8)}}, 'per_cpu_load'),
                 ({'available_memory_gib': 11.99}, 'available_memory'),
                 ({'capacity': replace(self.worker, enabled=False)}, 'disabled')]
        for changes, reason in cases:
            with self.subTest(changes=changes):
                result = self.run_round(replace(self.snapshot, **changes))
                self.assertEqual(self.reason(result), reason)
                self.assertEqual(self.store.reservations(), ())
        missing = schedule(self.store, self.config, (), now=100)
        self.assertEqual(self.reason(missing), 'missing_metrics')
        disabled = replace(self.config, workers=(replace(self.worker, enabled=False),))
        self.assertEqual(self.reason(self.run_round(config=disabled)), 'disabled')
        # Exact age, memory and quota limits pass; CPU threshold is strict.
        result = self.run_round(replace(self.snapshot, observed_at=70, cpu_sample_started_at=60, heartbeat_at=40,
                                       available_memory_gib=12, cpu_utilization_percent=79.99))
        self.assertEqual(len(result.assignments), 1)

    def test_cpu_multiplication_and_reservations_ignore_low_load(self):
        self.submit('a', cpu=2, threads=2)
        self.submit('b', cpu=2, threads=2)
        self.submit('j', cpu=1)
        result = self.run_round(replace(self.snapshot, cpu_utilization_percent=0))
        self.assertEqual([a.job_id for a in result.assignments], ['a', 'b'])
        self.assertEqual(self.reason(result), 'cpu_quota')
        self.assertEqual(sum(r.cpu_slots for r in self.store.reservations()), 8)
        self.assertEqual(self.run_round().assignments, ())

    def test_memory_quota_and_concurrency(self):
        self.submit('a', cpu=1, memory=20)
        self.submit('j', cpu=1, memory=13)
        result = self.run_round(replace(self.snapshot, available_memory_gib=100))
        self.assertEqual(self.reason(result), 'memory_quota')
        config = replace(self.config, workers=(replace(self.worker, max_concurrent_jobs=1),))
        self.assertEqual(self.reason(self.run_round(config=config)), 'memory_quota')
        self.submit('small', cpu=1, memory=1)
        result = self.run_round(config=config)
        self.assertEqual(json.loads(result.waiting['small'])['workers']['w'], 'concurrency_limit')

    def test_available_memory_counts_pending_but_not_running_twice(self):
        self.submit('a', cpu=1, memory=8)
        self.submit('j', cpu=1, memory=8)
        snapshot = replace(self.snapshot, available_memory_gib=12)
        first = self.run_round(snapshot)
        self.assertEqual(len(first.assignments), 1)
        self.assertEqual(self.reason(first), 'available_memory')
        self.store.transition(first.assignments[0].attempt_id, 'running')
        self.store.transition(first.assignments[0].attempt_id, 'unknown', reason='lost contact')
        started = datetime.fromisoformat(
            self.store.attempt(first.assignments[0].attempt_id).started_at.replace('Z', '+00:00')).timestamp()
        fresh = replace(snapshot, observed_at=started + 1, heartbeat_at=started + 1)
        second = schedule(self.store, self.config, (fresh,), now=started + 1)
        self.assertEqual(len(second.assignments), 1)
        self.assertEqual(len(self.store.reservations()), 2)

    def test_pre_start_metrics_keep_memory_reserved_after_start(self):
        self.submit('a', cpu=1, memory=8)
        self.submit('j', cpu=1, memory=8)
        snapshot = replace(self.snapshot, available_memory_gib=12)
        first = self.run_round(snapshot)
        attempt = self.store.transition(first.assignments[0].attempt_id, 'running')
        started = datetime.fromisoformat(attempt.started_at.replace('Z', '+00:00')).timestamp()
        for state in ('running', 'cancelling', 'unknown'):
            if state != 'running':
                self.store.transition(attempt.attempt_id, state)
            for observed in (started - 1, started):
                with self.subTest(state=state, observed=observed):
                    sample = replace(snapshot, observed_at=observed, heartbeat_at=started + 1)
                    result = schedule(self.store, self.config, (sample,), now=started + 1)
                    self.assertEqual(result.assignments, ())
                    self.assertEqual(self.reason(result), 'available_memory')
                    self.assertEqual(len(self.store.reservations()), 1)
        fresh = replace(snapshot, observed_at=started + 1, heartbeat_at=started + 1)
        result = schedule(self.store, self.config, (fresh,), now=started + 1)
        self.assertEqual([a.job_id for a in result.assignments], ['j'])
        self.assertEqual(len(self.store.reservations()), 2)

    def test_not_started_unknown_and_cancelling_remain_pending(self):
        self.submit('a', cpu=1)
        self.submit()
        a = self.store.assign('a', 'a1', 'w', '/work/a/a1')
        for state in ('cancelling', 'unknown'):
            self.store.transition(a.attempt_id, state)
            self.assertEqual(self.reason(self.run_round(
                replace(self.snapshot, available_memory_gib=12))), 'available_memory')
        self.assertEqual(len(self.store.reservations()), 1)

    def test_external_load_disabled_keeps_memory_and_reservations(self):
        self.submit()
        config = replace(self.config, scheduler=Scheduler(account_external_load=False))
        snapshot = replace(self.snapshot, cpu_utilization_percent=100, available_memory_gib=11)
        self.assertEqual(self.reason(self.run_round(snapshot, config)), 'available_memory')
        self.assertEqual(len(self.run_round(replace(snapshot, available_memory_gib=12),
                                            config).assignments), 1)

    def test_effective_limits_and_fifo_skip_with_durable_reasons(self):
        self.submit('j', cpu=5)
        self.submit('small', cpu=2, memory=4)
        snapshot = replace(self.snapshot, detected_cpu_slots=4,
                           per_cpu_utilization_percent={str(i): 10 for i in range(4)})
        result = self.run_round(snapshot)
        self.assertEqual([a.job_id for a in result.assignments], ['small'])
        self.assertEqual(json.loads(result.waiting['j'])['status'], 'unschedulable')
        with Store(self.path) as reopened:
            self.assertEqual(inspect_job(reopened, 'j')['events'][-1]['reason'], result.waiting['j'])
        events = self.store.events('j')
        self.run_round(snapshot)
        self.assertEqual(self.store.events('j'), events)
        # Each of the manager, worker and hardware limits constrains admission.
        for field, value in [('capacity', replace(self.worker, cpu_slots=4)),
                             ('detected_memory_gib', 7),
                             ('capacity', replace(self.worker, memory_budget_gib=7))]:
            with self.subTest(field=field):
                self.assertIn('j', self.run_round(replace(self.snapshot, **{field: value})).waiting)

    def test_deterministic_ranking(self):
        self.submit()
        other = replace(self.worker, id='a')
        config = replace(self.config, workers=(self.worker, other))
        other_snapshot = replace(self.snapshot, capacity=other)
        result = schedule(self.store, config, (self.snapshot, other_snapshot), now=100)
        self.assertEqual(result.assignments[0].worker_id, 'a')
        self.submit('next')
        # Lower utilization wins even with less remaining quota.
        result = schedule(self.store, config,
                          (self.snapshot, replace(other_snapshot, cpu_utilization_percent=1)), now=100)
        self.assertEqual(result.assignments[0].worker_id, 'a')
        self.submit('last')
        result = schedule(self.store, config, (other_snapshot, self.snapshot), now=100)
        self.assertEqual(result.assignments[0].worker_id, 'w')

    def test_each_capacity_source_and_release(self):
        self.submit('a', cpu=1, memory=1)
        self.submit('j', cpu=1, memory=1)
        snapshot = replace(self.snapshot,
                           capacity=replace(self.worker, max_concurrent_jobs=1))
        result = self.run_round(snapshot)
        self.assertEqual(self.reason(result), 'concurrency_limit')
        attempt = result.assignments[0]
        self.store.transition(attempt.attempt_id, 'cancelled',
                              termination_evidence='confirmed never started')
        self.assertEqual(len(self.run_round(snapshot).assignments), 1)
        self.submit('large', cpu=9, memory=1)
        # Manager quota applies even if both worker and hardware are larger.
        large = replace(self.snapshot, capacity=replace(self.worker, cpu_slots=64),
                        detected_cpu_slots=64)
        reason = json.loads(self.run_round(large).waiting['large'])
        self.assertEqual(reason['status'], 'unschedulable')
        self.assertEqual(reason['workers']['w'], 'exceeds_capacity')

    def test_remaining_cpu_then_memory_rank_before_id(self):
        a = replace(self.worker, id='a', cpu_slots=4, memory_budget_gib=16)
        z = replace(self.worker, id='z')
        config = replace(self.config, workers=(a, z))
        snapshots = (replace(self.snapshot, capacity=a), replace(self.snapshot, capacity=z))
        self.submit('cpu', cpu=1, memory=1)
        result = schedule(self.store, config, snapshots, now=100)
        self.assertEqual(result.assignments[0].worker_id, 'z')
        self.store.transition(result.assignments[0].attempt_id, 'cancelled',
                              termination_evidence='never started')
        a = replace(a, cpu_slots=8)
        config = replace(config, workers=(a, z))
        snapshots = (replace(self.snapshot, capacity=a), snapshots[1])
        self.submit('memory', cpu=1, memory=1)
        result = schedule(self.store, config, snapshots, now=100)
        self.assertEqual(result.assignments[0].worker_id, 'z')

    def test_concurrent_rounds_cannot_overreserve(self):
        for i in range(6):
            self.submit(str(i))
        def run(_):
            with Store(self.path) as store:
                return schedule(store, self.config, (self.snapshot,), now=100)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, range(2)))
        self.assertEqual(sum(len(r.assignments) for r in results), 2)
        self.assertEqual(len(self.store.reservations()), 2)
        self.assertEqual(sum(r.cpu_slots for r in self.store.reservations()), 8)

    def test_late_failure_rolls_back_entire_round(self):
        self.submit('a')
        self.submit('b')
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TRIGGER fail BEFORE INSERT ON events "
                       "WHEN NEW.job_id='b' AND NEW.state='assigned' "
                       "BEGIN SELECT RAISE(ABORT, 'injected'); END")
        with self.assertRaisesRegex(StoreError, 'injected'):
            self.run_round()
        self.assertEqual(self.store.reservations(), ())
        for name in ('a', 'b'):
            self.assertEqual(self.store.job_state(name), 'queued')
            self.assertEqual(self.store.job_attempts(name), ())
            self.assertEqual(len(self.store.events(name)), 1)

    def test_invalid_metrics_fail_before_writes(self):
        self.submit()
        for changes in ({'available_memory_gib': float('nan')},
                        {'cpu_utilization_percent': 101}, {'detected_cpu_slots': True},
                        {'detected_memory_gib': 0}, {'paused': 1}, {'observed_at': -1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.snapshot, **changes)
        for snapshots in ((self.snapshot, self.snapshot),
                          (replace(self.snapshot, capacity=replace(self.worker, id='unknown')),)):
            with self.assertRaises(ValueError):
                schedule(self.store, self.config, snapshots, now=100)
        with self.assertRaises(ValueError):
            schedule(self.store, self.config, (self.snapshot,), now=float('nan'))
        self.assertEqual(self.store.reservations(), ())
