"""Observable persistence, transaction, and state-machine guarantees."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
import sqlite3
import tempfile
import unittest

from lammps_manager.models import JobSpecification, Resources
from lammps_manager.store import Store, StoreError


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'state.sqlite'
        self.store = Store(self.path)
        self.addCleanup(self.store.close)
        self.spec = JobSpecification('j1', '/work/j1', 'in.lmp', 'a' * 64,
                                     Resources(mpi_ranks=3, omp_threads_per_rank=2))
        self.store.submit(self.spec)

    def assign(self):
        return self.store.assign('j1', 'a1', 'w1', '/work/j1/a1')

    def test_restart_snapshots_and_history(self):
        self.assign()
        running = self.store.transition('a1', 'running')
        with Store(self.path) as reopened:
            self.assertEqual(reopened.specification('j1'), self.spec)
            self.assertEqual(reopened.attempt('a1'), running)
            self.assertEqual(reopened.job_state('j1'), 'running')
            self.assertEqual(reopened.reservations('w1')[0].cpu_slots, 6)
            self.assertEqual([e['state'] for e in reopened.events('j1')],
                             ['queued', 'assigned', 'running'])

    def test_replays_and_conflicts(self):
        self.store.submit(self.spec)
        first = self.assign()
        self.assertEqual(self.assign(), first)
        self.assertEqual(len(self.store.events('j1')), 2)
        self.assertEqual(len(self.store.reservations()), 1)
        with self.assertRaises(StoreError):
            self.store.submit(JobSpecification('j1', '/other', 'in', 'b' * 64))
        with self.assertRaises(StoreError):
            self.store.assign('j1', 'a1', 'w2', '/work/j1/a1')
        with self.assertRaises(StoreError):
            self.store.assign('j1', 'a2', 'w1', '/work/j1/a2')
        self.store.transition('a1', 'running')
        self.store.transition('a1', 'running')
        result = dict(exit_code=0, termination_evidence='waitpid exit 0')
        final = self.store.transition('a1', 'succeeded', **result)
        self.assertEqual(self.store.transition('a1', 'succeeded', **result), final)
        self.assertEqual(self.assign(), final)
        self.assertEqual(len(self.store.events('j1')), 4)
        self.assertEqual(self.store.reservations(), ())
        with self.assertRaises(StoreError):
            self.store.transition('a1', 'succeeded', exit_code=0,
                                  termination_evidence='different evidence')

    def test_unknown_and_cancelling_retain_reservations(self):
        self.assign()
        for state in ('running', 'unknown', 'running', 'cancelling', 'unknown'):
            self.store.transition('a1', state)
            self.assertEqual(len(self.store.reservations()), 1)
        for state in ('failed', 'cancelled', 'succeeded'):
            with self.assertRaises(StoreError):
                self.store.transition('a1', state)
        final = self.store.transition('a1', 'cancelled', termination_evidence='confirmed group exit')
        self.assertIsNotNone(final.started_at)
        self.assertIsNotNone(final.ended_at)
        self.assertEqual(self.store.reservations(), ())
        with self.assertRaises(StoreError):
            self.store.transition('a1', 'running')

    def test_invalid_transitions_leave_state_untouched(self):
        self.assign()
        for state, kwargs in [('queued', {}),
                              ('succeeded', {'exit_code': 0, 'termination_evidence': 'exit'}),
                              ('succeeded', {'termination_evidence': 'exit'}),
                              ('failed', {'termination_evidence': 'exit'}),
                              ('running', {'exit_code': 0})]:
            with self.subTest(state=state), self.assertRaises(StoreError):
                self.store.transition('a1', state, **kwargs)
        self.assertEqual(self.store.job_state('j1'), 'assigned')
        self.assertEqual(len(self.store.events('j1')), 2)
        self.assertEqual(len(self.store.reservations()), 1)
        failed = self.store.transition('a1', 'failed', exit_code=2,
                                       termination_evidence='launcher exited', reason='launch failure')
        self.assertEqual(failed.exit_code, 2)
        self.assertEqual(self.store.job_state('j1'), 'failed')

    def test_assignment_rollback_after_reservation_insert(self):
        # Inject a late database failure after the reservation and job update.
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TRIGGER reject_event BEFORE INSERT ON events "
                       "WHEN NEW.state='assigned' BEGIN SELECT RAISE(ABORT, 'injected'); END")
        with self.assertRaisesRegex(StoreError, 'injected'):
            self.assign()
        self.assertEqual(self.store.job_state('j1'), 'queued')
        self.assertEqual(self.store.reservations(), ())
        self.assertEqual(len(self.store.events('j1')), 1)
        with self.assertRaises(StoreError):
            self.store.attempt('a1')
        with sqlite3.connect(self.path) as db:
            db.execute('DROP TRIGGER reject_event')
        self.assign()

    def test_terminal_rollback_restores_reservation(self):
        self.assign()
        self.store.transition('a1', 'running')
        with sqlite3.connect(self.path) as db:
            db.execute("CREATE TRIGGER reject_end BEFORE INSERT ON events "
                       "WHEN NEW.state='succeeded' BEGIN SELECT RAISE(ABORT, 'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.transition('a1', 'succeeded', exit_code=0, termination_evidence='waitpid')
        self.assertEqual(self.store.job_state('j1'), 'running')
        self.assertEqual(self.store.attempt('a1').state, 'running')
        self.assertIsNone(self.store.attempt('a1').ended_at)
        self.assertEqual(len(self.store.reservations()), 1)

    def test_concurrent_assignment(self):
        def assign(identifier):
            with Store(self.path) as store:
                try:
                    return store.assign('j1', identifier, 'w1', '/work/' + identifier).attempt_id
                except StoreError:
                    return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(assign, ('a1', 'a2')))
        self.assertEqual(sum(result is not None for result in results), 1)
        self.assertEqual(len(self.store.reservations()), 1)
        self.assertEqual(len(self.store.events('j1')), 2)

    def test_directory_conflict_and_missing_records(self):
        self.assign()
        self.store.submit(JobSpecification('j2', '/work/j2', 'in', 'b' * 64))
        with self.assertRaises(StoreError):
            self.store.assign('j2', 'a2', 'w1', '/work/j1/a1/.')
        self.assertEqual(self.store.job_state('j2'), 'queued')
        for operation in (lambda: self.store.attempt('missing'),
                          lambda: self.store.specification('missing'),
                          lambda: self.store.events('missing')):
            with self.assertRaises(StoreError):
                operation()

    def test_directory_root_aliases_cannot_bypass_uniqueness(self):
        for root in ('/', '//', '///'):
            with self.subTest(root=root), Store(':memory:') as store:
                store.submit(self.spec)
                store.submit(replace(self.spec, job_id='j2'))
                first = store.assign('j1', 'a1', 'w1', root + 'work/j1/a1')
                self.assertEqual(first.directory, '/work/j1/a1')
                for alias in ('/work/j1/a1', '//work/j1/a1', '///work//j1/./a1/'):
                    self.assertEqual(store.assign('j1', 'a1', 'w1', alias), first)
                    with self.assertRaises(StoreError):
                        store.assign('j2', 'a2', 'w1', alias)
                self.assertEqual(store.job_state('j2'), 'queued')
                self.assertEqual(len(store.events('j2')), 1)
                self.assertEqual(len(store.events('j1')), 2)
                self.assertEqual(len(store.reservations()), 1)
                with self.assertRaises(StoreError):
                    store.attempt('a2')

    def test_schema_version_rejected(self):
        path = Path(self.temp.name) / 'future.sqlite'
        with sqlite3.connect(path) as db:
            db.execute('PRAGMA user_version=99')
        with self.assertRaisesRegex(StoreError, 'version: 99'):
            Store(path)

    def test_invalid_submission_never_creates_records(self):
        invalid = [replace(self.spec, job_id=''), replace(self.spec, input_file='./'),
                   replace(self.spec, input_file='../in'),
                   replace(self.spec, input_sha256='invalid'),
                   replace(self.spec, work_directory='relative')]
        invalid += [replace(self.spec, resources=resource) for resource in (
            Resources(mpi_ranks=True), Resources(omp_threads_per_rank=0),
            Resources(memory_gib=float('nan')), Resources(memory_gib=-1),
            Resources(time_limit_seconds=0), Resources(max_retries=-1))]
        for specification in invalid:
            with self.subTest(specification=specification), self.assertRaises(StoreError):
                self.store.submit(specification)
        self.assertEqual(len(self.store.events('j1')), 1)

    def test_history_preserves_failure_and_disconnect_reasons(self):
        self.assign()
        self.store.transition('a1', 'running')
        self.store.transition('a1', 'unknown', reason='heartbeat expired')
        self.store.transition('a1', 'failed', exit_code=7,
                              termination_evidence='worker confirmed exit', reason='exit 7')
        events = self.store.events('j1')
        self.assertEqual(events[-2]['reason'], 'heartbeat expired')
        self.assertEqual(events[-1]['exit_code'], 7)
        self.assertEqual(events[-1]['termination_evidence'], 'worker confirmed exit')
        self.assertEqual(self.store.reservations(), ())

    def test_concurrent_replay(self):
        def assign(_):
            with Store(self.path) as store:
                return store.assign('j1', 'a1', 'w1', '/work/j1/a1')
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(assign, range(2)))
        self.assertEqual(results[0], results[1])
        self.assertEqual(len(self.store.events('j1')), 2)
        self.assertEqual(len(self.store.reservations()), 1)

    def test_closed_database_reopens_with_terminal_results(self):
        path = Path(self.temp.name) / 'restart.sqlite'
        with Store(path) as store:
            store.submit(self.spec)
            store.assign('j1', 'a1', 'w1', '/work/j1/a1')
            store.transition('a1', 'running')
            final = store.transition('a1', 'succeeded', exit_code=0,
                                     termination_evidence='confirmed group exit')
        with Store(path) as store:
            self.assertEqual(store.attempt('a1'), final)
            self.assertEqual(store.job_state('j1'), 'succeeded')
            self.assertEqual(store.specification('j1'), self.spec)
            self.assertEqual(len(store.events('j1')), 4)
            self.assertEqual(store.reservations(), ())
