"""Per-CPU policy, Linux sampling and durable headroom regressions."""

from dataclasses import replace
from datetime import datetime
import time
import unittest
from unittest.mock import patch

from lammps_manager.cpu import admission, pending_slots
from lammps_manager.worker import LinuxMonitor
from lammps_manager.config import Scheduler
from lammps_manager.scheduler import schedule
import test_scheduler


class PolicyTests(unittest.TestCase):
    def gate(self, values=None, **changes):
        args = dict(threshold=80, pending=0, requested=2, used=0,
                    minimum=0, reserve=1, capacity=8, enabled=True)
        args.update(changes)
        return admission(values, 1, **args)

    def test_distribution_strict_threshold_reserve_and_floor(self):
        # Both distributions average 50%, but only the first has three idle slots.
        self.assertTrue(self.gate(dict(zip('0123', [33, 33, 34, 100])), requested=3,
                                  reserve=0)['allowed'])
        self.assertFalse(self.gate(dict(zip('0123', [0, 0, 100, 100])), requested=3,
                                   reserve=0)['allowed'])
        self.assertTrue(self.gate({'0': 79.99, '1': 0, '2': 0})['allowed'])
        self.assertFalse(self.gate({'0': 80, '1': 0, '2': 0})['allowed'])
        self.assertFalse(self.gate({'0': 81, '1': 0, '2': 0})['allowed'])
        self.assertTrue(self.gate({'0': 100}, minimum=2)['minimum_exception'])
        self.assertFalse(self.gate({'0': 100}, minimum=2, used=1)['allowed'])
        self.assertTrue(self.gate({'0': 0, '1': 0, '2': 0},
                                  minimum=2, used=1)['allowed'])
        self.assertFalse(self.gate(None, minimum=8)['allowed'])
        self.assertFalse(self.gate({}, minimum=8)['allowed'])
        self.assertFalse(self.gate({'0': 0}, minimum=9)['allowed'])
        self.assertFalse(self.gate({'0': 0, '1': 0, '2': 0}, pending=1)['allowed'])

    def test_initial_zero_interval_and_missing_cpu_have_no_headroom(self):
        counters = {'': (0, 0, 0, 100, 0, 0, 0, 0),
                    '3': (0, 0, 0, 100, 0, 0, 0, 0)}
        from types import SimpleNamespace
        with patch.object(LinuxMonitor, '_cpu', return_value=counters), \
                patch('lammps_manager.worker.os.sched_getaffinity', return_value={3}), \
                patch('lammps_manager.worker.asdict', return_value={}):
            monitor = LinuxMonitor()
            sample = monitor.sample(SimpleNamespace(worker=None))
            self.assertEqual(sample['per_cpu_utilization_percent'], {})
            self.assertFalse(self.gate(sample['per_cpu_utilization_percent'], minimum=2)['allowed'])

    def test_affinity_hotplug_reset_and_iowait(self):
        def counters(user=0, idle=0, wait=0):
            return (user, 0, 0, idle, wait, 0, 0, 0)
        first = {'': counters(idle=100), '2': counters(idle=100),
                 '5': counters(idle=100), '9': counters(idle=100)}
        second = {'': counters(idle=200), '2': counters(idle=200),
                  '5': counters(idle=200), '9': counters(wait=100, idle=100),
                  '11': counters(idle=200)}
        third = {'': counters(idle=300), '2': counters(idle=10),
                 '9': second['9'], '11': counters(idle=300)}
        with patch.object(LinuxMonitor, '_cpu', side_effect=[first, second, third]), \
                patch('lammps_manager.worker.os.sched_getaffinity',
                      side_effect=[{2, 5, 9}, {2, 9, 11, 99}, {2, 9, 11}]), \
                patch('lammps_manager.worker.asdict', return_value={}):
            monitor = LinuxMonitor()
            from types import SimpleNamespace
            worker = SimpleNamespace(worker=None)
            sample = monitor.sample(worker)
            self.assertEqual(sample['per_cpu_utilization_percent'], {'2': 0, '9': 100})
            self.assertEqual(sample['detected_cpu_slots'], 3)
            sample = monitor.sample(worker)
            self.assertEqual(sample['per_cpu_utilization_percent'], {'11': 0})


class AdmissionTests(unittest.TestCase):
    setUp = test_scheduler.SchedulerTests.setUp
    submit = test_scheduler.SchedulerTests.submit
    run_round = test_scheduler.SchedulerTests.run_round
    reason = test_scheduler.SchedulerTests.reason

    def test_floor_cannot_bypass_stale_or_missing_and_effective_capacity(self):
        self.submit(cpu=2)
        config = replace(self.config, scheduler=Scheduler(min_slots=2))
        for changes, reason in [
                ({'per_cpu_utilization_percent': None}, 'missing_per_cpu_metrics'),
                ({'cpu_sample_started_at': None}, 'missing_per_cpu_metrics'),
                ({'observed_at': 60, 'cpu_sample_started_at': 59}, 'stale_metrics'),
                ({'detected_cpu_slots': 1, 'per_cpu_utilization_percent': {'0': 100}},
                 'exceeds_capacity')]:
            self.assertEqual(self.reason(self.run_round(replace(self.snapshot, **changes), config)), reason)
        busy = replace(self.snapshot, cpu_utilization_percent=100,
                       per_cpu_utilization_percent={str(i): 100 for i in range(8)})
        self.assertEqual(len(self.run_round(busy, config).assignments), 1)

    def test_minimum_rejects_smaller_effective_capacity(self):
        self.submit(cpu=1)
        config = replace(self.config, scheduler=Scheduler(min_slots=3))
        small = replace(self.snapshot, detected_cpu_slots=2,
                        per_cpu_utilization_percent={'0': 100, '1': 100})
        self.assertEqual(self.reason(self.run_round(small, config)), 'minimum_exceeds_capacity')
        self.assertEqual(self.store.reservations(), ())

    def test_new_policy_ignores_aggregate_veto_and_keeps_budget_after_exit(self):
        self.submit('a', cpu=2, memory=1)
        self.submit('j', cpu=2, memory=1)
        sample = replace(self.snapshot, cpu_utilization_percent=100,
                         per_cpu_utilization_percent={'0': 0, '7': 0})
        first = self.run_round(sample)
        self.assertEqual(len(first.assignments), 1)
        self.assertEqual(self.reason(first), 'per_cpu_load')
        a = first.assignments[0]
        self.store.transition(a.attempt_id, 'running')
        self.store.transition(a.attempt_id, 'succeeded', exit_code=0,
                              termination_evidence='test process exited')
        self.assertEqual(self.run_round(sample).assignments, ())
        now = time.time() + 1
        fresh = replace(sample, observed_at=now, heartbeat_at=now,
                        cpu_sample_started_at=now - .1)
        self.assertEqual(len(schedule(self.store, self.config, (fresh,), now=now).assignments), 1)

    def test_pending_cpu_covers_unknown_cancelling_and_sample_boundary(self):
        from lammps_manager.store import Store
        self.submit('a', cpu=2, memory=1)
        self.submit('j', cpu=2, memory=1)
        a = self.store.assign('a', 'a1', 'w', '/work/a/a1')
        a = self.store.transition(a.attempt_id, 'running')
        started = datetime.fromisoformat(a.started_at.replace('Z', '+00:00')).timestamp()
        # The observation ends after launch, but its window still overlaps startup.
        sample = replace(self.snapshot, observed_at=started + .1, heartbeat_at=started + .1,
                         cpu_sample_started_at=started,
                         per_cpu_utilization_percent={'0': 0, '1': 0})
        for state in ('running', 'unknown', 'cancelling'):
            if state != 'running':
                self.store.transition(a.attempt_id, state)
            with Store(self.path) as reopened:
                result = schedule(reopened, self.config, (sample,), now=started + .1)
            self.assertEqual(self.reason(result), 'per_cpu_load')
            self.assertEqual(result.cpu_diagnostics['j']['w']['pending'], 2)
            self.assertEqual(sum(r.cpu_slots for r in self.store.reservations()), 2)
        # A full later window reflects the start; idle running reservations still
        # consume quotas but must not also subtract the observed headroom.
        fresh = replace(sample, cpu_sample_started_at=started + .01)
        result = schedule(self.store, self.config, (fresh,), now=started + .1)
        self.assertEqual(len(result.assignments), 1)
        self.assertEqual(result.cpu_diagnostics['j']['w']['pending'], 0)
        self.assertEqual(sum(r.cpu_slots for r in self.store.reservations()), 4)

    def test_millisecond_rounding_cannot_reuse_just_started_budget(self):
        self.submit('a', cpu=2, memory=1)
        self.submit('j', cpu=2, memory=1)
        a = self.store.assign('a', 'a1', 'w', '/work/a/a1')
        a = self.store.transition(a.attempt_id, 'running')
        started = datetime.fromisoformat(a.started_at.replace('Z', '+00:00')).timestamp()
        sample = replace(self.snapshot, observed_at=started + 1, heartbeat_at=started + 1,
                         cpu_sample_started_at=started + .0005,
                         per_cpu_utilization_percent={'0': 0, '1': 0})
        result = schedule(self.store, self.config, (sample,), now=started + 1)
        self.assertEqual(self.reason(result), 'per_cpu_load')
        self.assertEqual(result.cpu_diagnostics['j']['w']['pending'], 2)

    def test_concurrent_cpu_headroom_is_not_reused(self):
        from concurrent.futures import ThreadPoolExecutor
        from lammps_manager.store import Store
        for i in range(6):
            self.submit(str(i), cpu=1, memory=1)
        sample = replace(self.snapshot, per_cpu_utilization_percent={'0': 0, '1': 0})
        def run(_):
            with Store(self.path) as store:
                return schedule(store, self.config, (sample,), now=100)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(run, range(2)))
        self.assertEqual(sum(len(r.assignments) for r in results), 2)

    def test_blocked_queue_uses_linear_sql_work(self):
        for i in range(120):
            self.submit(str(i), cpu=1, memory=1)
        busy = replace(self.snapshot,
                       per_cpu_utilization_percent={str(i): 100 for i in range(8)})
        statements = []
        self.store._db.set_trace_callback(statements.append)
        try:
            result = self.run_round(busy)
        finally:
            self.store._db.set_trace_callback(None)
        self.assertEqual(len(result.waiting), 120)
        self.assertEqual(result.assignments, ())
        self.assertLess(len(statements), 120 * 20)
        history_reads = [sql for sql in statements if sql.startswith('SELECT a.*, j.payload')]
        self.assertEqual(len(history_reads), 1)

    def test_pending_history_is_filtered_before_decoding(self):
        from lammps_manager.models import specification_from_dict

        for name in ('old', 'running', 'other', 'pending', 'claimed', 'candidate'):
            self.submit(name, cpu=2, threads=2, memory=1)
            self.store.assign(name, name, 'other-worker' if name == 'other' else 'w',
                              '/work/' + name + '/attempt')
        self.store.transition('old', 'cancelled', termination_evidence='never started')
        self.store.transition('running', 'running')
        self.store.claim_launch('claimed')
        now = time.time() + 1
        # Old terminated history and another worker must not be decoded at all.
        statements = []
        self.store._db.set_trace_callback(statements.append)
        try:
            with patch('lammps_manager.store.specification_from_dict',
                       wraps=specification_from_dict) as decode:
                self.assertEqual(pending_slots(self.store, 'w', now,
                                 candidate_id='candidate', skip_unclaimed_staged=True), 4)
                self.assertEqual(decode.call_count, 0)
        finally:
            self.store._db.set_trace_callback(None)
        self.assertLessEqual(len(statements), 2)
        self.assertEqual(pending_slots(self.store, 'w', now, candidate_id='candidate'), 8)
        self.store.transition('pending', 'unknown')
        self.assertEqual(pending_slots(self.store, 'w', now,
                         candidate_id='candidate', skip_unclaimed_staged=True), 8)

    def test_round_caches_keep_worker_budgets_separate(self):
        other = replace(self.worker, id='other')
        config = replace(self.config, workers=(self.worker, other))
        sample = replace(self.snapshot, per_cpu_utilization_percent={'0': 0, '1': 0})
        snapshots = (sample, replace(sample, capacity=other))
        for i in range(6):
            self.submit(str(i), cpu=1, memory=1)
        result = schedule(self.store, config, snapshots, now=100)
        self.assertEqual(len(result.assignments), 4)
        for worker_id in ('w', 'other'):
            self.assertEqual(sum(a.worker_id == worker_id for a in result.assignments), 2)
            self.assertEqual(result.cpu_diagnostics['5'][worker_id]['pending'], 2)
        # A new round must read the committed charges rather than reuse its cache.
        with test_scheduler.Store(self.path) as reopened:
            self.assertEqual(schedule(reopened, config, snapshots, now=100).assignments, ())

    def test_invalid_per_cpu_metrics(self):
        for changes in ({'per_cpu_utilization_percent': {'-1': 0}},
                        {'per_cpu_utilization_percent': {'0': float('nan')}},
                        {'per_cpu_utilization_percent': {'0': True}},
                        {'per_cpu_utilization_percent': {'0': 101}},
                        {'cpu_sample_started_at': 101}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(self.snapshot, **changes)
