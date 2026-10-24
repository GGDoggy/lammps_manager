"""Checkpoint HTTP bounds with local byte stand-ins, never real MPI/LAMMPS."""
from dataclasses import asdict, replace
from http.client import HTTPConnection
import json
import time
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

import test_segments as fixtures
from lammps_manager import checkpoints as cp, segments as seg
from lammps_manager.abp_parameters import PARAMETERIZED_VERSION
from lammps_manager.config import Execution, WorkerConfig
from lammps_manager.continuation import ENGINEERING_VERSION, MAX_ENGINE_STEP, Recipe
from lammps_manager.store import Store
from lammps_manager.worker import Worker


class CheckpointTransportTests(unittest.TestCase):
    setUp = fixtures.SegmentWorkerTests.setUp

    def test_large_plans_publish_and_advance_over_shared_and_staged_http(self):
        for mode in ('shared', 'staged'):
            self.service.config = replace(self.config, storage=replace(self.config.storage, mode=mode))
            for version in (ENGINEERING_VERSION, PARAMETERIZED_VERSION):
                with self.subTest(mode=mode, version=version):
                    run_id = mode + '-' + version
                    worker_root = self.work if mode == 'shared' else self.root / run_id
                    worker_root.mkdir(exist_ok=True)
                    worker = Worker(WorkerConfig(self.capacities[0], self.endpoint, str(worker_root),
                        Execution('/bin/false', '/bin/false'), self.tokens['a'], storage_mode=mode),
                        self.root / ('journal-' + run_id))
                    self.addCleanup(worker.close)
                    with Store(self.config.manager.database_path) as store:
                        store.worker_generation('a', worker.boot_id, worker.session_id)
                        seg.observe_environment(store, 'a', worker.boot_id, fixtures.BUILD, time.time())
                        seg.submit_run(store, self.service.config, total_steps=10**12, step_max=10**7,
                            build_fingerprint=fixtures.BUILD, run_id=run_id,
                            recipe=Recipe('wca-abp-2d-v1', 'statistical'), contract_version=version)
                        snapshot = store._db.execute('SELECT snapshot FROM simulation_runs WHERE run_id=?',
                            (run_id,)).fetchone()['snapshot']
                        spec = store.specification(run_id + '.s000000')
                        attempt = store.assign(spec.job_id, run_id + '.attempt', 'a',
                            str(Path(spec.work_directory) / 'attempt'))
                        contract = cp.record(store, attempt.attempt_id)['contract']
                        store.transition(attempt.attempt_id, 'running')
                        store.transition(attempt.attempt_id, 'succeeded', exit_code=0,
                            termination_evidence='Local byte stand-in; no process launched')
                        directory = (Path(attempt.directory) if mode == 'shared' else
                            worker_root / spec.job_id / 'attempt')
                        (directory / 'work').mkdir(parents=True)
                        (directory / 'work/checkpoint.restart').write_bytes(b'opaque checkpoint stand-in')
                        (directory / 'stdout').write_text('1 by 1 by 1 MPI processor grid\n'
                            'using 1 OpenMP thread(s) per MPI task\nStep Time Atoms\n10000000 1000 256\nLoop time\n')
                        with Store(worker.database) as journal:
                            journal.submit(spec)
                            worker._assign(journal, spec, dict(attempt_id=attempt.attempt_id), directory,
                                dict(checkpoint_contract=contract))
                            journal.transition(attempt.attempt_id, 'running')
                            journal.transition(attempt.attempt_id, 'succeeded', exit_code=0,
                                termination_evidence='Local byte stand-in; no process launched')
                            worker._collect_checkpoint(journal, attempt.attempt_id)
                            local = cp.record(journal, attempt.attempt_id)
                            self.assertGreater(len(json.dumps(local['manifest']).encode()), 1048576)
                            self.assertTrue(local['acknowledged'])
                            self.assertEqual(local['identity'], cp.record(store, attempt.attempt_id)['identity'])
                            changed = {**local['manifest'], 'contract': {**contract, 'run_id': 'other-run'}}
                            with self.assertRaisesRegex(ValueError, 'HTTP 400'):
                                worker._result_request(attempt.attempt_id, 'manifest', changed, checkpoint=True)
                            # Replay must preserve the committed identity and contract.
                            worker._collect_checkpoint(journal, attempt.attempt_id)
                            self.assertEqual(cp.record(journal, attempt.attempt_id)['identity'], local['identity'])
                        self.assertTrue(cp.record(store, attempt.attempt_id)['acknowledged'])
                        self.assertEqual(cp.record(store, attempt.attempt_id)['contract'], contract)
                        # Stand in only for the independent ordinary result collection gate.
                        store.set_result_collection(attempt.attempt_id, 'collected', dict(files=[], omitted=[]))
                        seg.advance(store, run_id)
                        view = seg.inspect_run(store, run_id)
                        self.assertEqual(view['committed_step'], 10**7)
                        self.assertEqual(len(view['segments']), 2)
                        successor = store.specification(view['segments'][1]['job_id'])
                        self.assertEqual((Path(successor.work_directory) / 'inputs/checkpoint.restart').read_bytes(),
                            b'opaque checkpoint stand-in')
                        self.assertEqual(store._db.execute('SELECT snapshot FROM simulation_runs WHERE run_id=?',
                            (run_id,)).fetchone()['snapshot'], snapshot)
                        self.assertEqual(store.specification(spec.job_id), spec)
                        self.assertEqual(store.reservations(), ())
                    worker.close()

    def test_oversized_requests_reject_before_reading_or_dispatch(self):
        address = urlsplit(self.endpoint)
        cases = (
            ('/v2/checkpoints/attempt/manifest', 4 * 1024 * 1024 + 1, 'checkpoint_operation'),
            ('/v2/checkpoints/attempt/commit', 1048577, 'checkpoint_operation'),
            ('/v2/results/attempt/manifest', 16 * 1024 * 1024 + 1, 'result_operation'),
            ('/v2/mpi-results/attempt/manifest', 16 * 1024 * 1024 + 1, 'mpi_result_operation'),
            ('/v2/results/attempt/commit', 1048577, 'result_operation'),
            ('/v2/mpi-results/attempt/commit', 1048577, 'mpi_result_operation'),
            ('/v1/poll', 1048577, 'poll'),
        )
        for path, length, handler in cases:
            for authorized in (True, False):
                with self.subTest(path=path, authorized=authorized), patch.object(self.service, handler) as dispatch:
                    connection = HTTPConnection(address.hostname, address.port, timeout=3)
                    try:
                        connection.putrequest('POST', path)
                        connection.putheader('X-Worker-ID', 'a')
                        connection.putheader('Authorization', 'Bearer ' + ('a' * 40 if authorized else 'wrong'))
                        connection.putheader('Content-Length', str(length))
                        connection.endheaders()  # No body: rejection must precede reading it.
                        response = connection.getresponse()
                        self.assertEqual(response.status, 400 if authorized else 401)
                        response.read()
                        dispatch.assert_not_called()
                    finally:
                        connection.close()


class CheckpointPlanSizeTests(unittest.TestCase):
    setUp = fixtures.SegmentTests.setUp

    def test_maximum_global_boundaries_fit_checkpoint_http_budget(self):
        for version in (ENGINEERING_VERSION, PARAMETERIZED_VERSION):
            with self.subTest(version=version):
                seg.submit_run(self.store, self.config, total_steps=1, build_fingerprint=fixtures.BUILD,
                    run_id=version, recipe=Recipe('wca-abp-2d-v1', 'statistical'), contract_version=version)
                job = version + '.s000000'
                attempt = self.store.assign(job, version + '.attempt', 'worker', str(self.root / version))
                contract = cp.decode_contract(cp.record(self.store, attempt.attempt_id)['contract'])
                maximum = (MAX_ENGINE_STEP + 99999) // 100000
                ends = tuple(stop for _, stop in seg.plan_steps(MAX_ENGINE_STEP, maximum))
                self.assertEqual(len(ends), 100000)
                contract = replace(contract, total_steps=MAX_ENGINE_STEP, segment_ends=ends,
                    global_step=ends[0], global_time=ends[0] * .0001)
                cp.validate_contract(contract)
                manifest = dict(contract=asdict(contract), files=[dict(path='checkpoint.restart',
                    size=2**63 - 1, sha256='0' * 64)])
                length = len(json.dumps(manifest).encode())
                self.assertGreater(length, 1024 * 1024)
                self.assertLess(length, 4 * 1024 * 1024)
