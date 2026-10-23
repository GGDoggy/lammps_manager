"""Atomic multi-host accounting and all-participant termination barriers."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest
from uuid import uuid4

from lammps_manager.config import WorkerCapacity
from lammps_manager.models import InputFile, JobSpecification, MpiParticipant, Resources
from lammps_manager.multihost import (authorize, members, prepare, reserve,
                                     terminate, finish)
from lammps_manager.store import Store, StoreError


class MultiHostTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'manager.sqlite'
        self.store = Store(self.path)
        self.addCleanup(self.store.close)
        self.spec = JobSpecification('j', '/jobs/j', 'in', 'a' * 64,
            Resources(mpi_ranks=3, omp_threads_per_rank=2, memory_gib=.3),
            storage_mode='staged', input_manifest=(InputFile('in', 1, 'a' * 64),),
            mpi_placement=(MpiParticipant('a', 1, .1), MpiParticipant('b', 2, .2)))
        self.store.submit(self.spec)
        self.capacities = {'a': WorkerCapacity('a', 2, 1, 1), 'b': WorkerCapacity('b', 4, 1, 1)}
        self.identities = {worker: (str(uuid4()), str(uuid4())) for worker in self.capacities}
        for worker, identity in self.identities.items():
            self.store.worker_generation(worker, *identity)

    def reserve(self, store=None, attempt='attempt', job='j', capacities=None):
        return reserve(store or self.store, job, attempt, '/jobs/' + job + '/' + attempt,
                       capacities or self.capacities, self.identities)

    def ready(self, worker, **changes):
        options = dict(boot_id=self.identities[worker][0], session_id=self.identities[worker][1],
                       directory='/private/j/attempt/work', executable_sha256='b' * 64)
        options.update(changes)
        prepare(self.store, 'attempt', worker, **options)

    def ended(self, worker, *, kind='never_started', status='cancelled', code=None, **changes):
        evidence = dict(kind=kind, observed_boot_id=self.identities[worker][0], launch_fenced=True)
        if kind == 'session_ended':
            evidence['process'] = dict(pid=100, session=100, process_group=100,
                                      start_ticks='123', boot_id=self.identities[worker][0])
        evidence.update(changes)
        terminate(self.store, 'attempt', worker, evidence=evidence, status=status, exit_code=code)

    def test_atomic_shares_reopen_replay_and_competing_ordinary_job(self):
        first = self.reserve()
        self.assertEqual(first.worker_id, 'a')
        self.assertEqual({r.worker_id: (r.cpu_slots, r.memory_gib) for r in self.store.reservations()},
                         {'a': (2, .1), 'b': (4, .2)})
        self.assertEqual(self.reserve(), first)
        with Store(self.path) as reopened:
            self.assertEqual(len(members(reopened, 'attempt')), 2)
            self.assertEqual(len(reopened.reservations('b')), 1)
            self.assertEqual(reopened.specification('j'), self.spec)
        self.store.submit(replace(self.spec, job_id='other', work_directory='/jobs/other'))
        with self.assertRaisesRegex(StoreError, 'quota'):
            self.reserve(job='other', attempt='other-attempt')
        self.assertEqual(self.store.job_state('other'), 'queued')
        self.assertEqual(self.store.job_attempts('other'), ())
        self.assertEqual(len(self.store.reservations()), 2)
        # Ordinary admission uses the same peer reservation accounting.
        ordinary = replace(self.spec, job_id='ordinary', mpi_placement=(), storage_mode='shared',
                           resources=Resources(mpi_ranks=1, omp_threads_per_rank=1, memory_gib=.1))
        self.store.submit(ordinary)
        from lammps_manager.scheduler import WorkerSnapshot, schedule
        from lammps_manager.config import ManagerConfig, ManagerSettings, Scheduler, Storage
        capacity = self.capacities['b']
        snapshot = WorkerSnapshot(capacity, 4, 8, 100, 100, 0, 8)
        config = ManagerConfig(ManagerSettings(str(self.path)), Storage('/jobs'), (capacity,),
                               scheduler=Scheduler(account_external_load=False, reserve_memory_gib=0))
        result = schedule(self.store, config, (snapshot,), now=100)
        self.assertEqual(result.assignments, ())
        self.assertEqual(self.store.job_state('ordinary'), 'queued')

    def test_late_failure_rolls_back_every_participant(self):
        self.store._db.execute("CREATE TRIGGER fail_member BEFORE INSERT ON mpi_members "
                               "WHEN NEW.worker_id='b' BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaisesRegex((StoreError, sqlite3.IntegrityError), 'injected'):
            self.reserve()
        self.assertEqual(self.store.reservations(), ())
        self.assertEqual(self.store.job_attempts('j'), ())
        self.assertEqual(self.store.job_state('j'), 'queued')
        self.assertEqual(len(self.store.events('j')), 1)

    def test_concurrent_groups_cannot_partially_oversubscribe(self):
        self.store.submit(replace(self.spec, job_id='other', work_directory='/jobs/other'))
        def attempt(job):
            with Store(self.path) as store:
                try:
                    self.reserve(store=store, attempt=job + '-attempt', job=job)
                    return True
                except StoreError:
                    return False
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(sum(pool.map(attempt, ('j', 'other'))), 1)
        self.assertEqual(len(self.store.reservations()), 2)

    def test_preparation_requires_every_pinned_worker_and_matching_runtime(self):
        self.reserve()
        self.assertFalse(authorize(self.store, 'attempt'))
        self.ready('a')
        self.ready('a')
        self.assertFalse(authorize(self.store, 'attempt'))
        for options in (dict(boot_id=str(uuid4())), dict(session_id=str(uuid4())),
                        dict(executable_sha256='c' * 64), dict(directory='/different/work')):
            with self.subTest(options=options), self.assertRaises(StoreError):
                self.ready('b', **options)
        self.ready('b')
        self.assertTrue(authorize(self.store, 'attempt'))
        self.assertTrue(authorize(self.store, 'attempt'))
        self.store.request_cancel('attempt')
        self.assertFalse(authorize(self.store, 'attempt'))

    def test_identity_change_and_disconnection_never_release(self):
        self.reserve()
        self.ready('a'); self.ready('b')
        self.store.worker_generation('b', str(uuid4()), str(uuid4()))
        self.assertFalse(authorize(self.store, 'attempt'))
        self.store.expire_workers(time.time() + 100, 1)
        self.assertEqual(self.store.job_state('j'), 'unknown')
        self.assertEqual(len(self.store.reservations()), 2)

    def test_one_ended_peer_cannot_release_or_complete_entire_group(self):
        self.reserve()
        self.store.request_cancel('attempt')
        self.ended('a')
        self.assertIsNone(finish(self.store, 'attempt'))
        with self.assertRaisesRegex(StoreError, 'participant'):
            self.store.transition('attempt', 'cancelled', termination_evidence='leader alone exited')
        self.assertEqual(len(self.store.reservations()), 2)
        self.ended('b')
        self.assertEqual(finish(self.store, 'attempt').state, 'cancelled')
        self.assertEqual(self.store.reservations(), ())
        self.assertEqual(len(members(self.store, 'attempt')), 2)
        self.assertEqual(finish(self.store, 'attempt').state, 'cancelled')
        self.assertEqual(self.reserve().state, 'cancelled')
        self.assertEqual(self.store.reservations(), ())

    def test_fenced_process_evidence_and_outcome_are_immutable(self):
        self.reserve()
        self.ready('a'); self.ready('b')
        self.assertTrue(authorize(self.store, 'attempt'))
        self.store.transition('attempt', 'running')
        for invalid in (dict(launch_fenced=False), dict(observed_boot_id=str(uuid4())),
                        dict(process=dict(pid=100, session=101, process_group=100,
                                          start_ticks='123', boot_id=self.identities['a'][0]))):
            with self.subTest(invalid=invalid), self.assertRaises(StoreError):
                self.ended('a', kind='session_ended', status='succeeded', code=0, **invalid)
        self.ended('a', kind='session_ended', status='succeeded', code=0)
        self.ended('a', kind='session_ended', status='succeeded', code=0)
        with self.assertRaises(StoreError):
            self.ended('a', status='failed', code=1)
        self.assertIsNone(finish(self.store, 'attempt'))
        self.ended('b', kind='session_ended', status='succeeded', code=0)
        self.assertEqual(finish(self.store, 'attempt').state, 'succeeded')
        self.assertEqual(self.store.reservations(), ())
        with self.assertRaises(StoreError):
            self.ready('b')

    def test_unknown_worker_capacity_and_identity_conflicts_leave_no_attempt(self):
        for capacities, identities in [({'a':self.capacities['a']}, self.identities),
                                      (self.capacities, {'a':self.identities['a']}),
                                      ({**self.capacities, 'b':replace(self.capacities['b'], enabled=False)}, self.identities)]:
            with self.subTest(capacities=capacities), self.assertRaises(StoreError):
                reserve(self.store,'j','attempt','/jobs/j/attempt',capacities,identities)
            self.assertEqual(self.store.job_attempts('j'), ())
        self.reserve()
        for directory in ('jobs/j/attempt', '/jobs/../j/attempt', '/jobs/j/attempt\x00'):
            with self.subTest(directory=directory), self.assertRaises(StoreError):
                reserve(self.store,'j','attempt',directory,self.capacities,self.identities)
        with self.assertRaises(StoreError):
            reserve(self.store,'j','attempt','/jobs/j/attempt',self.capacities,
                    {**self.identities,'b':(str(uuid4()),str(uuid4()))})

    def test_failed_peer_cannot_be_overridden_by_leader_success_and_late_commit_rolls_back(self):
        self.reserve()
        self.ready('a'); self.ready('b')
        authorize(self.store, 'attempt')
        self.store.transition('attempt', 'running')
        self.ended('a', kind='session_ended', status='succeeded', code=0)
        self.ended('b', kind='session_ended', status='failed', code=1)
        with self.assertRaises(StoreError):
            self.store.transition('attempt', 'succeeded', exit_code=0, termination_evidence='leader exit 0')
        self.store._db.execute("CREATE TRIGGER fail_end BEFORE INSERT ON events "
                               "WHEN NEW.state='failed' BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'injected'):
            finish(self.store, 'attempt')
        self.assertEqual(self.store.attempt('attempt').state, 'running')
        self.assertEqual(len(self.store.reservations()), 2)
        self.assertTrue(all(not m['released'] for m in members(self.store, 'attempt')))
        self.store._db.execute('DROP TRIGGER fail_end')
        self.assertEqual(finish(self.store, 'attempt').state, 'failed')
        self.assertEqual(self.store.reservations(), ())

    def test_incomplete_membership_cannot_authorize_or_release(self):
        self.store._db.execute("CREATE TRIGGER ignore_member BEFORE INSERT ON mpi_members "
                               "WHEN NEW.worker_id='b' BEGIN SELECT RAISE(IGNORE); END")
        with self.assertRaisesRegex(StoreError, 'membership'):
            self.reserve()
        self.assertEqual(self.store.reservations(), ())
        self.assertEqual(self.store.job_attempts('j'), ())
        self.store._db.execute('DROP TRIGGER ignore_member')
        self.reserve()
        self.ready('a'); self.ready('b')
        # Simulate lost/corrupt bookkeeping; absence is never remote termination.
        self.store._db.execute("DELETE FROM mpi_members WHERE worker_id='b'")
        with self.assertRaisesRegex(StoreError, 'membership'):
            authorize(self.store, 'attempt')
        self.store.request_cancel('attempt')
        self.ended('a')
        with self.assertRaisesRegex(StoreError, 'membership'):
            self.store.transition('attempt', 'cancelled', termination_evidence='remaining row ended')

    def test_completed_group_keeps_each_hosts_sample_charge_without_extra_queries(self):
        self.reserve()
        self.ready('a'); self.ready('b')
        authorize(self.store, 'attempt')
        self.store.claim_launch('attempt')
        self.store.transition('attempt', 'running')
        started = datetime.fromisoformat(self.store.attempt('attempt').started_at.replace('Z', '+00:00')).timestamp()
        self.ended('a', kind='session_ended', status='succeeded', code=0)
        self.ended('b', kind='session_ended', status='succeeded', code=0)
        finish(self.store, 'attempt')
        from lammps_manager.cpu import pending_slots
        for worker, slots in [('a', 2), ('b', 4)]:
            queries = []
            self.store._db.set_trace_callback(queries.append)
            self.assertEqual(pending_slots(self.store, worker, started), slots)
            self.store._db.set_trace_callback(None)
            self.assertEqual(len(queries), 1)
            self.assertEqual(pending_slots(self.store, worker, started + 2), 0)
        self.assertEqual(pending_slots(self.store, 'unrelated', started), 0)

    def test_memory_and_job_count_limits_are_independent_of_cpu(self):
        self.store.submit(replace(self.spec, job_id='other', work_directory='/jobs/other'))
        large_cpu = {worker: replace(capacity, cpu_slots=100) for worker, capacity in self.capacities.items()}
        self.reserve(capacities=large_cpu)
        # CPU and memory fit, but one concurrent allocation is already held.
        with self.assertRaisesRegex(StoreError, 'quota'):
            self.reserve(job='other', attempt='other-attempt', capacities=large_cpu)
        two_jobs = {worker: replace(capacity, max_concurrent_jobs=2) for worker, capacity in large_cpu.items()}
        two_jobs['a'] = replace(two_jobs['a'], memory_budget_gib=.15)
        with self.assertRaisesRegex(StoreError, 'quota'):
            self.reserve(job='other', attempt='other-attempt', capacities=two_jobs)
        self.assertEqual(self.store.job_attempts('other'), ())
        self.assertEqual(len(self.store.reservations()), 2)

    def test_existing_ordinary_work_blocks_all_or_nothing_group_admission(self):
        ordinary = replace(self.spec, job_id='ordinary', mpi_placement=(), storage_mode='shared',
                           resources=Resources(mpi_ranks=1, omp_threads_per_rank=1, memory_gib=.1))
        self.store.submit(ordinary)
        self.store.assign('ordinary','ordinary-attempt','b','/jobs/ordinary/ordinary-attempt')
        with self.assertRaisesRegex(StoreError, 'quota'):
            self.reserve()
        self.assertEqual(self.store.job_attempts('j'), ())
        self.assertEqual(len(self.store.reservations()), 1)

    def test_boot_change_requires_closed_launch_fence_and_does_not_succeed(self):
        self.reserve()
        self.ended('a', kind='boot_changed', status='failed', observed_boot_id=str(uuid4()))
        self.assertEqual(len(self.store.reservations()), 2)
        self.ended('b', status='failed')
        self.assertEqual(finish(self.store, 'attempt').state, 'failed')
        self.assertEqual(self.store.reservations(), ())


if __name__ == '__main__':
    unittest.main()
