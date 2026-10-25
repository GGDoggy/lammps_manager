"""Dispatch priority, immutable snapshots and serialized updates."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import sqlite3

import unittest
import test_scheduler
from lammps_manager.config import ConfigurationError, Scheduler, _parse
from lammps_manager.store import Store, StoreError


class PriorityTests(unittest.TestCase):
    setUp = test_scheduler.SchedulerTests.setUp
    submit = test_scheduler.SchedulerTests.submit
    run_round = test_scheduler.SchedulerTests.run_round
    def test_priority_demand_fifo_and_snapshot(self):
        for name, cpu in [('z', 1), ('a', 1), ('large', 3), ('urgent', 1)]:
            self.submit(name, cpu=cpu, memory=1)
        self.store.set_priority('urgent', 5, origin='operator')
        result = self.run_round()
        self.assertEqual([a.job_id for a in result.assignments], ['urgent', 'large', 'z', 'a'])
        self.assertEqual(self.store.assignment_priority(result.assignments[0].attempt_id)['priority'], 5)
        with self.assertRaises(StoreError):
            self.store.set_priority('urgent', 10)

    def test_migration_defaults_and_reopen_preserve_json(self):
        self.submit()
        before = self.store._db.execute('SELECT specification FROM jobs').fetchone()[0]
        self.store.set_priority('j', -3)
        with Store(self.path) as reopened:
            self.assertEqual(reopened.scheduling_metadata('j')['priority'], -3)
            self.assertEqual(reopened._db.execute('SELECT specification FROM jobs').fetchone()[0], before)
        self.submit('default', cpu=1)
        config = replace(self.config, scheduler=Scheduler(default_priority=8))
        result = self.run_round(config=config)
        self.assertEqual(result.assignments[0].job_id, 'default')
        self.assertEqual(self.store.assignment_priority(result.assignments[0].attempt_id)['priority'], 8)

    def test_return_gets_fresh_sequence_and_priority_change_keeps_fifo(self):
        self.submit('first', cpu=1)
        self.submit('second', cpu=1)
        original = self.store.scheduling_metadata('first')['queue_sequence']
        self.store.set_priority('first', 0)
        self.assertEqual(self.store.scheduling_metadata('first')['queue_sequence'], original)
        self.store.assign('first', 'a', 'w', '/work/first/a')
        self.store.reject_launch('a', 'not_ready', 'closed', 0)
        self.store.return_rejected('a')
        self.assertEqual([j['job_id'] for j in self.store.queued_jobs()], ['second', 'first'])

    def test_update_racing_dispatch_is_atomic(self):
        self.submit()
        def dispatch():
            with Store(self.path) as store:
                from lammps_manager.scheduler import schedule
                return schedule(store, self.config, (self.snapshot,), now=100)
        def update():
            with Store(self.path) as store:
                try:
                    store.set_priority('j', 7)
                    return True
                except StoreError:
                    return False
        with ThreadPoolExecutor(2) as pool:
            future = pool.submit(dispatch)
            updated = pool.submit(update).result()
            attempt = future.result().assignments[0]
        self.assertEqual(self.store.assignment_priority(attempt.attempt_id)['priority'], 7 if updated else 0)

    def test_queue_lookup_work_scales_after_existing_database_reopen(self):
        # Simulate a database created before the queue lookup index existed.
        self.store._db.execute('DROP INDEX IF EXISTS events_job_event')
        self.store._db.execute('PRAGMA user_version=2')
        for index in range(256):
            self.submit(f'job-{index}', cpu=1)
        before = self.store._db.execute(
            'SELECT job_id,specification FROM jobs ORDER BY rowid').fetchall()

        def work(store):
            ticks = 0

            def tick():
                nonlocal ticks
                ticks += 1
                return 0

            store._db.set_progress_handler(tick, 100)
            try:
                jobs = store.queued_jobs()
            finally:
                store._db.set_progress_handler(None, 0)
            self.assertEqual([job['job_id'] for job in jobs],
                             [f'job-{index}' for index in range(len(jobs))])
            return ticks

        with Store(self.path) as reopened:
            self.assertEqual(reopened._db.execute(
                'SELECT job_id,specification FROM jobs ORDER BY rowid').fetchall(), before)
            small = work(reopened)
            for index in range(256, 512):
                self.submit(f'job-{index}', cpu=1)
            large = work(reopened)
            self.assertLess(large, small * 3)

    def test_invalid_priorities(self):
        self.submit()
        for value in [True, 1.2, 2**63, -(2**63)-1]:
            with self.assertRaises(StoreError):
                self.store.set_priority('j', value)
        for value in [True, 1.2, 2**63]:
            with self.assertRaises(ConfigurationError):
                _parse(Scheduler, {'default_priority': value}, 'scheduler')
