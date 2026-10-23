"""Immutable cross-host intent and prevention of accidental single-worker launches."""

from dataclasses import asdict, FrozenInstanceError, replace
import json
from pathlib import Path
import tempfile
import unittest

from lammps_manager.config import ManagerConfig, ManagerSettings, Storage, WorkerCapacity
from lammps_manager.models import (InputFile, JobSpecification, MpiParticipant,
                                   Resources, specification_from_dict)
from lammps_manager.scheduler import schedule
from lammps_manager.store import Store, StoreError


class MpiPlacementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'state.sqlite'
        self.participants = (MpiParticipant('a', 1, .1), MpiParticipant('b', 2, .2))
        self.spec = JobSpecification('mpi', '/work/mpi', 'in.lmp', 'a' * 64,
            Resources(mpi_ranks=3, omp_threads_per_rank=2, memory_gib=.3),
            storage_mode='staged', input_manifest=(InputFile('in.lmp', 1, 'a' * 64),),
            mpi_placement=self.participants)

    def test_snapshot_roundtrip_reopen_and_legacy_bytes(self):
        with Store(self.path) as store:
            store.submit(self.spec)
            legacy = asdict(replace(self.spec, job_id='legacy', mpi_placement=()))
            legacy.pop('mpi_placement')
            payload = json.dumps(legacy)
            store._db.execute('INSERT INTO jobs(job_id,specification,state) VALUES(?,?,?)',
                              ('legacy', payload, 'queued'))
        with Store(self.path) as store:
            self.assertEqual(store.specification('mpi'), self.spec)
            decoded = store.specification('legacy')
            self.assertEqual(decoded.mpi_placement, ())
            store.submit(decoded)
            self.assertEqual(store._db.execute('SELECT specification FROM jobs WHERE job_id=?',
                             ('legacy',)).fetchone()[0], payload)
            store.submit(replace(decoded, job_id='new-single-worker'))
            saved = store._db.execute('SELECT specification FROM jobs WHERE job_id=?',
                                     ('new-single-worker',)).fetchone()[0]
            self.assertNotIn('mpi_placement', json.loads(saved))
        self.assertEqual(specification_from_dict(json.loads(json.dumps(asdict(self.spec)))), self.spec)
        with self.assertRaises(FrozenInstanceError):
            self.participants[0].mpi_ranks = 9

    def test_reject_invalid_participant_and_inconsistent_group(self):
        for worker, ranks, memory in [('', 1, .1), ('a b', 1, .1), ('a\x00', 1, .1),
                ('a', True, .1), ('a', 0, .1), ('a', 1.5, .1), ('a', 1, True),
                ('a', 1, 0), ('a', 1, float('nan')), ('a', 1, float('inf'))]:
            with self.subTest(worker=worker, ranks=ranks, memory=memory), self.assertRaises(ValueError):
                MpiParticipant(worker, ranks, memory)
        changes = [dict(storage_mode='shared'), dict(mpi_placement=list(self.participants)),
                   dict(mpi_placement=self.participants[:1]),
                   dict(mpi_placement=(self.participants[0], self.participants[0])),
                   dict(mpi_placement=(self.participants[0], object())),
                   dict(resources=replace(self.spec.resources, mpi_ranks=4)),
                   dict(resources=replace(self.spec.resources, memory_gib=.4))]
        for values in changes:
            with self.subTest(values=values), self.assertRaises(ValueError):
                replace(self.spec, **values)

    def test_group_cannot_be_assigned_to_one_worker_or_scheduled_silently(self):
        with Store(self.path) as store:
            store.submit(self.spec)
            with self.assertRaisesRegex(StoreError, 'coordinated assignment'):
                store.assign('mpi', 'attempt', 'a', '/work/mpi/attempt')
            self.assertEqual(store.job_attempts('mpi'), ())
            self.assertEqual(store.reservations(), ())
            config = ManagerConfig(ManagerSettings(str(self.path)), Storage('/work', mode='staged'),
                                   (WorkerCapacity('a', 8, 8, 2), WorkerCapacity('b', 8, 8, 2)))
            result = schedule(store, config, (), now=100)
            self.assertEqual(result.assignments, ())
            self.assertIn('multi_host_protocol_unavailable', result.waiting['mpi'])
            self.assertEqual(store.job_state('mpi'), 'queued')
            self.assertEqual(len(store.events('mpi')), 2)
            self.assertEqual(store.reservations(), ())


if __name__ == '__main__':
    unittest.main()
