"""Local checkpoint reliability tests; byte stand-ins do not validate LAMMPS state."""
from dataclasses import asdict, replace
from io import BytesIO
import hashlib
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from lammps_manager import checkpoints as cp
from lammps_manager.abp_parameters import PARAMETERIZED_VERSION, model_summary, parameterized_digest
from lammps_manager.continuation import (ENGINEERING_VERSION, MAX_RUN_COMMAND, Recipe,
    engineering_parameters, engineering_digest)
from lammps_manager.models import JobSpecification, Resources
from lammps_manager.store import Store, StoreError

BUILD = 'aa8a1af68131f97d13fd4959031c557681b46c9eee23dddef26ca75c7eae0695'
LIMITS = dict(max_files=8, max_file_bytes=1024, max_total_bytes=4096)


def provenance():
    identities = [('setup', 'positions'), ('setup', 'orientations')]
    identities += [('segmented', str(start)) for start in (0, 600, 1400)]
    seeds = [int.from_bytes(hashlib.sha256(json.dumps([
        'local-test', 'confirm', arm, 0, segment]).encode()).digest()[:8], 'big') % 899999999 + 1
        for arm, segment in identities]
    return json.dumps(dict(namespace='local-test', phase='confirm', replica=0, seeds=seeds))


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / 'state.sqlite3'
        self.store = Store(self.db)
        self.addCleanup(lambda: self.store.close())
        from uuid import uuid4
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.store.worker_generation('worker', boot, str(uuid4()))
        self.root.joinpath('job').mkdir()
        self.spec = JobSpecification('job', str(self.root / 'job'), 'in.lmp', '0' * 64,
                                     Resources(1, 1), storage_mode='staged',
                                     input_manifest=(cp.InputFile('in.lmp', 0, '0' * 64),))
        self.store.submit(self.spec)
        self.store.assign('job', 'attempt', 'worker', str(self.root / 'job' / 'attempt'))
        self.contract = cp.CheckpointContract('run', 0, 'attempt', 'job',
            Recipe('ideal-abp-2d-v1', 'statistical'), BUILD, 2000, (600, 1400, 2000),
            600, 3.0, provenance(), execution_boot_id=boot)
        cp.expect(self.store, self.contract)
        self.data = b'opaque restart stand-in'
        self.manifest = json.loads(cp.canonical(dict(contract=asdict(self.contract), files=[dict(
            path='checkpoint.restart', size=len(self.data), sha256=hashlib.sha256(self.data).hexdigest())])))

    def stop(self):
        self.store.transition('attempt', 'running')
        self.store.transition('attempt', 'succeeded', exit_code=0, termination_evidence='test child reaped')

    def op(self, action, **kwargs):
        return cp.operation(self.store, 'worker', 'attempt', action, LIMITS, **kwargs)

    def upload(self):
        self.op('manifest', payload=self.manifest)
        self.op('0', stream=BytesIO(self.data), length=len(self.data))

    def consumer(self, worker='worker'):
        self.store.submit(replace(self.spec, job_id='next', work_directory=str(self.root / 'next')))
        cp.pin(self.store, cp.record(self.store, 'attempt')['identity'], 'next')
        self.store.assign('next', 'next-attempt', worker, str(self.root / 'next-attempt'))

    def test_atomic_publication_replay_restart_and_dependency(self):
        self.stop()
        self.upload()
        with self.assertRaises(StoreError):
            self.consumer()
        reply = self.op('commit')
        self.assertTrue(reply['acknowledged'])
        self.store.close()
        self.store = Store(self.db)
        self.assertEqual(reply, self.op('commit'))
        self.consumer()
        with cp.fetch(self.store, 'worker', 'next-attempt', 0, BUILD)[0] as source:
            self.assertEqual(source.read(), self.data)
        with self.assertRaisesRegex(StoreError, 'still needed'):
            cp.release(self.store, 'next')
        self.store.transition('next-attempt', 'failed', reason='test cancellation before launch',
                              termination_evidence='never launched')
        cp.release(self.store, 'next')
        with self.assertRaises(StoreError):
            cp.fetch(self.store, 'worker', 'next-attempt', 0, BUILD)
        self.assertTrue(Path(cp.record(self.store, 'attempt')['directory']).exists())

    def test_same_worker_fetch_requires_pinned_and_current_boot(self):
        from uuid import uuid4
        boot = str(uuid4())
        self.store.worker_generation('worker', boot, str(uuid4()))
        # Replace the fixture contract before binding any checkpoint content.
        self.contract = replace(self.contract, execution_boot_id=boot)
        self.store._db.execute('UPDATE checkpoints SET contract=? WHERE attempt_id=?',
            (cp.canonical(asdict(self.contract)), 'attempt'))
        self.manifest['contract'] = json.loads(cp.canonical(asdict(self.contract)))
        self.stop()
        self.upload()
        self.op('commit')
        self.consumer()
        self.store.worker_generation('worker', boot, str(uuid4()))
        with cp.fetch(self.store, 'worker', 'next-attempt', 0, BUILD)[0] as source:
            self.assertEqual(source.read(), self.data)
        self.store.worker_generation('worker', str(uuid4()), str(uuid4()))
        with self.assertRaisesRegex(StoreError, 'Unvalidated checkpoint environment'):
            with cp.fetch(self.store, 'worker', 'next-attempt', 0, BUILD)[0]:
                pass
        self.store._db.execute('DELETE FROM worker_generations WHERE worker_id=?', ('worker',))
        self.assertFalse(cp.compatible_worker(self.store, 'attempt', 'worker'))
        self.store.worker_generation('worker', boot, str(uuid4()))
        unpinned = replace(self.contract, execution_boot_id=None)
        self.store._db.execute('UPDATE checkpoints SET contract=? WHERE attempt_id=?',
            (cp.canonical(asdict(unpinned)), 'attempt'))
        self.assertFalse(cp.compatible_worker(self.store, 'attempt', 'worker'))

    def test_missing_corrupt_or_conflicting_state_never_acknowledges(self):
        self.stop()
        for files in ([], [dict(path='../escape', size=1, sha256='0' * 64)],
                      [dict(path='other.restart', size=1, sha256='0' * 64)]):
            with self.subTest(files=files), self.assertRaises(ValueError):
                self.op('manifest', payload={**self.manifest, 'files': files})
        self.op('manifest', payload=self.manifest)
        with self.assertRaises((ValueError, OSError)):
            self.op('commit')
        with self.assertRaises(ValueError):
            self.op('0', stream=BytesIO(b'x' * len(self.data)), length=len(self.data))
        with self.assertRaises(ValueError):
            self.op('0', stream=BytesIO(self.data + b'x'), length=len(self.data))
        self.assertFalse(cp.record(self.store, 'attempt')['acknowledged'])
        self.upload()
        changed = json.loads(json.dumps(self.manifest))
        changed['files'][0]['sha256'] = '1' * 64
        with self.assertRaisesRegex(StoreError, 'Conflicting'):
            self.op('manifest', payload=changed)
        self.op('commit')
        directory = Path(cp.record(self.store, 'attempt')['directory'])
        (directory / 'checkpoint.restart').write_bytes(b'corrupt')
        self.consumer()
        with self.assertRaises(ValueError):
            cp.fetch(self.store, 'worker', 'next-attempt', 0, BUILD)

    def test_ownership_clock_recipe_state_and_stream_contract_fail_closed(self):
        for change in (dict(run_id='../run'), dict(segment=True), dict(global_step=601),
                       dict(global_time=0), dict(build_fingerprint='unknown'),
                       dict(required_state=('checkpoint.restart', 'fix.state')),
                       dict(stream_provenance='{}'), dict(recipe=Recipe())):
            with self.subTest(change=change), self.assertRaises(ValueError):
                cp.validate_contract(replace(self.contract, **change))
        with self.assertRaises(StoreError):
            self.op('manifest', payload=self.manifest)
        self.stop()
        with self.assertRaises(StoreError):
            cp.operation(self.store, 'other', 'attempt', 'manifest', LIMITS, payload=self.manifest)
        changed = {**self.manifest, 'contract': {**self.manifest['contract'], 'run_id': 'different'}}
        with self.assertRaisesRegex(StoreError, 'provenance'):
            self.op('manifest', payload=changed)

    def test_shared_copy_and_upload_recover_from_publication_ack_crash(self):
        self.stop()
        self.op('manifest', payload=self.manifest)
        source = self.root / 'shared'
        source.mkdir()
        (source / 'checkpoint.restart').write_bytes(self.data)
        original = cp.authorize
        calls = []

        def crash_after_publish(*args):
            calls.append(1)
            if len(calls) == 2:
                raise OSError('injected crash before SQLite acknowledgement')
            return original(*args)

        with patch.object(cp, 'authorize', side_effect=crash_after_publish):
            with self.assertRaises(OSError):
                self.op('commit', shared_source=source)
        self.assertFalse(cp.record(self.store, 'attempt')['acknowledged'])
        self.assertTrue((cp.destination(self.store, 'attempt') / 'results').exists())
        # Retry with no source/spool: already published files are reverified.
        self.assertTrue(self.op('commit')['acknowledged'])
        self.assertEqual((source / 'checkpoint.restart').read_bytes(), self.data)

    def test_unsafe_paths_and_cross_worker_fetch_rejected(self):
        self.stop()
        self.upload()
        self.op('commit')
        self.consumer(worker='other')
        with self.assertRaisesRegex(StoreError, 'reassignment'):
            cp.fetch(self.store, 'other', 'next-attempt', 0, BUILD)
        directory = Path(cp.record(self.store, 'attempt')['directory'])
        target = directory / 'checkpoint.restart'
        target.unlink()
        target.symlink_to(self.root / 'anything')
        with self.assertRaises((ValueError, OSError)):
            self.op('commit')

    def test_cleanup_requires_checkpoint_ack_separately_from_result_ack(self):
        self.store.own_staged_directory('attempt', [[1, 1], [1, 2], [1, 3]])
        self.stop()
        self.store.set_result_collection('attempt', 'collected', dict(files=[], omitted=[]))
        self.assertEqual(self.store.cleanup_candidates(), ())
        with self.assertRaisesRegex(StoreError, 'checkpoint'):
            self.store.set_cleanup('attempt', 'deleting')
        self.upload()
        self.op('commit')
        self.assertEqual(len(self.store.cleanup_candidates()), 1)
        self.store.set_cleanup('attempt', 'deleting')

    def test_worker_http_lost_ack_retry_retains_checkpoint_until_ack(self):
        import threading
        from lammps_manager.config import (ManagerConfig, ManagerSettings, Storage,
            WorkerCapacity, WorkerConfig, Execution)
        from lammps_manager.manager import ManagerService, make_server
        from lammps_manager.worker import Worker
        capacity = WorkerCapacity('worker', 1, 8, 1)
        token = self.root / 'token'
        token.write_text('x' * 40)
        token.chmod(0o600)
        config = ManagerConfig(ManagerSettings(str(self.db)), Storage(str(self.root), mode='staged'),
                               (capacity,))
        service = ManagerService(config, {'worker': str(token)})
        self.addCleanup(service.close)
        server = make_server(service, '127.0.0.1', 0)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
        thread.start()
        def stop_server():
            server.shutdown()
            thread.join(5)
            server.server_close()
        self.addCleanup(stop_server)
        worker_root = self.root / 'worker-root'
        worker_root.mkdir()
        worker = Worker(WorkerConfig(capacity, f'http://127.0.0.1:{server.server_port}',
            str(worker_root), Execution('/bin/false', '/bin/false'), str(token), storage_mode='staged'),
            self.root / 'journal')
        self.addCleanup(worker.close)
        with Store(worker.database) as journal:
            journal.submit(self.spec)
            directory = worker_root / 'job' / 'attempt'
            worker._assign(journal, self.spec, dict(attempt_id='attempt'), directory,
                           dict(checkpoint_contract=json.loads(cp.canonical(asdict(self.contract)))))
            (directory / 'work').mkdir(parents=True)
            (directory / 'work/checkpoint.restart').write_bytes(self.data)
            (directory / 'stdout').write_text('Step Time Atoms\n600 3 1024\nLoop time\n')
            journal.transition('attempt', 'running')
            journal.transition('attempt', 'succeeded', exit_code=0, termination_evidence='test child reaped')
            self.stop()
            original = worker._result_request
            lost = []
            def lose_reply(attempt, action, *args, **kwargs):
                reply = original(attempt, action, *args, **kwargs)
                if action == 'commit' and not lost:
                    lost.append(True)
                    raise OSError('injected lost acknowledgement')
                return reply
            with patch.object(worker, '_result_request', side_effect=lose_reply):
                with self.assertRaises(OSError):
                    worker._collect_checkpoint(journal, 'attempt')
            self.assertTrue(cp.record(self.store, 'attempt')['acknowledged'])
            self.assertFalse(cp.record(journal, 'attempt')['acknowledged'])
            self.assertTrue((directory / 'work/checkpoint.restart').exists())
            worker._collect_checkpoint(journal, 'attempt')
            self.assertTrue(cp.record(journal, 'attempt')['acknowledged'])
            self.assertEqual(cp.record(journal, 'attempt')['identity'], cp.record(self.store, 'attempt')['identity'])
            from urllib.request import Request, urlopen
            from urllib.error import HTTPError
            request = Request(worker.config.manager_endpoint + '/v2/checkpoints/attempt/commit',
                              data=b'{}', headers={'X-Worker-ID': 'worker', 'Authorization': 'Bearer wrong'})
            with self.assertRaises(HTTPError) as failure:
                urlopen(request)
            self.assertEqual(failure.exception.code, 401)
            failure.exception.close()
            self.consumer()
            headers = {'X-Worker-ID': 'worker', 'Authorization': 'Bearer ' + 'x' * 40,
                       'X-Build-Fingerprint': BUILD}
            endpoint = worker.config.manager_endpoint + '/v2/checkpoints/next-attempt/files/0'
            with urlopen(Request(endpoint, headers=headers)) as response:
                self.assertEqual(response.read(), self.data)
            with self.assertRaises(HTTPError) as failure:
                urlopen(Request(endpoint, headers={**headers, 'X-Build-Fingerprint': 'unknown'}))
            self.assertEqual(failure.exception.code, 400)
            failure.exception.close()

    def test_fixed_renderer_actual_build_and_safe_boundary_checks(self):
        from lammps_manager.continuation import render_abp_input
        from lammps_manager.config import WorkerCapacity, WorkerConfig, Execution
        work = self.root / 'work'
        work.mkdir()
        seeds = json.loads(provenance())['seeds']
        script = render_abp_input(start=0, stop=600, total=2000,
                                  position_seed=seeds[0], orientation_seed=seeds[1], stream_seed=seeds[2])
        (work / 'in.lmp').write_text(script)
        config = WorkerConfig(WorkerCapacity('worker', 1, 8, 1), 'http://127.0.0.1',
                              str(self.root), Execution('/bin/false', '/bin/false'), '/unused')
        with patch('lammps_manager.continuation.fingerprint_build', return_value=BUILD):
            cp.prepare_launch(self.store, config, 'attempt', work)
            (work / 'in.lmp').write_text(script + 'velocity all create 1 123\n')
            with self.assertRaisesRegex(ValueError, 'renderer'):
                cp.prepare_launch(self.store, config, 'attempt', work)
        with patch('lammps_manager.continuation.fingerprint_build', return_value='unknown'):
            with self.assertRaisesRegex(ValueError, 'environment'):
                cp.prepare_launch(self.store, config, 'attempt', work)
        for thermo in ('Step Time Atoms\n599 2.995 1024\n', 'Step Time Atoms\n600 0 1024\n'):
            (self.root / 'stdout').write_text(thermo)
            with self.assertRaises(ValueError):
                cp.validate_boundary(self.root, self.manifest['contract'])
        (self.root / 'stdout').write_text('Step Time Atoms\n600 3 1024\n')
        cp.validate_boundary(self.root, self.manifest['contract'])


class CheckpointClockTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def contract(self, ends, *, version=ENGINEERING_VERSION, parameters=None, ordinal=None):
        recipe = Recipe('wca-abp-2d-v1', 'statistical')
        model = (model_summary(recipe, parameters or {}) if version == PARAMETERIZED_VERSION
                 else engineering_parameters(recipe))
        digest = (parameterized_digest(recipe, model) if version == PARAMETERIZED_VERSION
                  else engineering_digest(recipe))
        ordinal = len(ends) - 1 if ordinal is None else ordinal
        provenance = cp.canonical(dict(version=version, policy='fresh-random-per-segment-v1',
            allocation_sha256='0' * 64, segment=ordinal, seeds=[1, 2, 3],
            recipe_sha256=digest, model=model, evidence_status='not_assessed'))
        return cp.CheckpointContract('run', ordinal, 'attempt', 'job', recipe, BUILD,
            ends[-1], tuple(ends), ends[ordinal], ends[ordinal] * model['dt'], provenance,
            recipe_sha256=digest)

    def validate(self, contract, elapsed, *, step=None, atoms=None):
        model = json.loads(contract.stream_provenance).get('model', {'particles': 1024})
        (self.root / 'stdout').write_text(
            '1 by 1 by 1 MPI processor grid\nusing 1 OpenMP thread(s) per MPI task\n'
            f'Step Time Atoms\n{contract.global_step if step is None else step} '
            f'{elapsed:.17g} {model["particles"] if atoms is None else atoms}\nLoop time\n')
        cp.validate_boundary(self.root, asdict(contract))

    def test_reported_wca_accumulation_and_wrong_clocks(self):
        ends = tuple(99999 * n for n in range(1, 1002))
        contract = self.contract(ends, ordinal=450)
        elapsed = 0.0
        for _ in range(451):
            elapsed += 99999 * .0001
        self.assertFalse(math.isclose(elapsed, contract.global_time, rel_tol=1e-14, abs_tol=1e-14))
        self.validate(contract, elapsed)
        for bad in (elapsed + .0001, elapsed - .0001, contract.global_step * .005,
                    float('nan'), float('inf'), -float('inf')):
            with self.subTest(elapsed=bad), self.assertRaisesRegex(ValueError, 'clock mismatch'):
                self.validate(contract, bad)
        with self.assertRaises(ValueError):
            self.validate(contract, elapsed, step=contract.global_step - 1)
        with self.assertRaisesRegex(ValueError, 'clock mismatch'):
            self.validate(contract, elapsed, atoms=1024)

    def test_maximum_length_parameterized_chain_accumulates_custom_dt(self):
        ends = tuple(range(10, 1000001, 10))
        contract = self.contract(ends, version=PARAMETERIZED_VERSION, parameters={'dt': .003})
        elapsed = 0.0
        for _ in ends:
            elapsed += 10 * .003
        self.assertFalse(math.isclose(elapsed, contract.global_time, rel_tol=1e-14, abs_tol=1e-14))
        self.validate(contract, elapsed)
        with self.assertRaisesRegex(ValueError, 'clock mismatch'):
            self.validate(contract, elapsed + .003)

    def test_single_segment_counts_engine_command_splits(self):
        commands = 10000
        contract = self.contract((MAX_RUN_COMMAND * commands + 7,))
        elapsed = 0.0
        for _ in range(commands):
            elapsed += MAX_RUN_COMMAND * .0001
        elapsed += 7 * .0001
        self.assertFalse(math.isclose(elapsed, contract.global_time, rel_tol=1e-14, abs_tol=1e-14))
        self.validate(contract, elapsed)

    def test_future_segments_do_not_loosen_first_boundary(self):
        contract = self.contract(tuple(range(99999, 9999900001, 99999)), ordinal=0)
        with self.assertRaisesRegex(ValueError, 'clock mismatch'):
            self.validate(contract, contract.global_time * (1 + 1e-12))

    def test_legacy_clock_tolerance_is_unchanged(self):
        contract = cp.CheckpointContract('run', 2, 'attempt', 'job',
            Recipe('ideal-abp-2d-v1', 'statistical'), BUILD, 2000, (600, 1400, 2000),
            2000, 10.0, provenance())
        self.validate(contract, 10.0)
        self.validate(contract, 10.0 * (1 + 5e-15))
        with self.assertRaisesRegex(ValueError, 'clock mismatch'):
            self.validate(contract, 10.0 * (1 + 2e-14))
        # Exact contract metadata still rejects altered clocks independently of
        # the tolerance used for the engine's rounded stdout evidence.
        with self.assertRaisesRegex(ValueError, 'boundary/clock mismatch'):
            self.validate(replace(contract, global_time=10.0 + 1e-13), 10.0)


if __name__ == '__main__':
    unittest.main()
