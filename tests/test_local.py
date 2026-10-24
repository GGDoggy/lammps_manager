"""Local end-to-end fixtures never invoke real MPI or LAMMPS."""

from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import yaml

from lammps_manager.config import load_config
from lammps_manager.local import inspect_job, run_local, submit_batch
from lammps_manager.store import Store


class LocalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        (self.source / 'in.lmp').write_text('original input')
        (self.source / 'asset').write_text('asset')
        launcher = self.root / 'fake mpi'
        launcher.write_text('#!' + sys.executable + '\nimport os,sys\nos.execv(sys.argv[3],sys.argv[3:])\n')
        launcher.chmod(0o755)
        program = self.root / 'fake lammps'
        program.write_text('#!' + sys.executable + '\n'
            'import os,sys,pathlib\n'
            'print("stdout", os.environ["OMP_NUM_THREADS"], sys.argv[1:])\n'
            'print("stderr", file=sys.stderr)\n'
            'pathlib.Path("result").write_text(pathlib.Path("asset").read_text())\n'
            'sys.exit(int(pathlib.Path("in.lmp").read_text()) if pathlib.Path("in.lmp").read_text().isdigit() else 0)\n')
        program.chmod(0o755)
        manager = yaml.safe_load(Path('examples/manager.yaml').read_text())
        manager['manager']['database_path'] = str(self.root / 'state.sqlite')
        manager['storage']['work_root'] = str(self.root / 'jobs')
        worker = yaml.safe_load(Path('examples/worker.yaml').read_text())
        worker['work_root'] = manager['storage']['work_root']
        worker['execution'].update(lammps_path=str(program), mpi_launcher_path=str(launcher),
                                    mpi_args=[], lammps_args=['', 'literal; $(no shell)'])
        self.manager_path = self.root / 'manager.yaml'
        self.worker_path = self.root / 'worker.yaml'
        self.manager_path.write_text(yaml.safe_dump(manager))
        self.worker_path.write_text(yaml.safe_dump(worker))
        self.config = load_config(self.manager_path, 'manager')
        self.worker = load_config(self.worker_path, 'worker')
        self.manifest = self.root / 'batch.json'
        self.manifest.write_text(json.dumps([{'source_directory': 'source', 'input_file': 'in.lmp',
                                             'resources': {'omp_threads_per_rank': 2}}]))

    def test_sampling_include_is_immutable_and_source_is_preserved(self) -> None:
        (self.source / 'in.lmp').write_text('include sampling.lmp\nrun 10\n')
        request = json.loads(self.manifest.read_text())
        request[0]['sampling'] = dict(unit='step', positions=[2, 7], dt=.1, Dr=1., total_steps=10)
        self.manifest.write_text(json.dumps(request))
        with Store(self.config.manager.database_path) as store:
            jobs = submit_batch(store, self.config, self.manifest)
            spec = store.specification(jobs[0])
            inputs = Path(spec.work_directory) / 'inputs'
            self.assertEqual((inputs / 'snapshot-steps.txt').read_bytes(), b'2\n7\n11\n')
            self.assertEqual(spec.sampling['steps'], [2, 7])
            self.assertEqual({item.path for item in spec.input_manifest},
                {'in.lmp', 'asset', 'sampling.json', 'sampling.lmp', 'snapshot-steps.txt'})
            self.assertFalse((self.source / 'sampling.lmp').exists())
            self.assertEqual(run_local(store, self.worker), jobs)
            self.assertEqual(store.result_collection(store.job_attempts(jobs[0])[0].attempt_id)['status'], 'collected')

    def test_staged_sampling_submits_verified_inputs_and_preserves_mode(self) -> None:
        import hashlib
        from lammps_manager.staging import publish_inputs

        original = b'include sampling.lmp\nrun 10\n'
        (self.source / 'in.lmp').write_bytes(original)
        request = json.loads(self.manifest.read_text())
        request[0]['sampling'] = dict(unit='step', positions=[2, 7], dt=.1, Dr=1., total_steps=10)
        self.manifest.write_text(json.dumps(request))
        config = replace(self.config, storage=replace(self.config.storage, mode='staged'))
        with Store(config.manager.database_path) as store:
            jobs = submit_batch(store, config, self.manifest)
            spec = store.specification(jobs[0])
            self.assertEqual(spec.storage_mode, 'staged')
            self.assertEqual(spec.input_sha256, hashlib.sha256(original).hexdigest())
            self.assertEqual(spec.sampling['steps'], [2, 7])
            inputs = Path(spec.work_directory) / 'inputs'
            self.assertEqual({item.path for item in spec.input_manifest},
                {'in.lmp', 'asset', 'sampling.json', 'sampling.lmp', 'snapshot-steps.txt'})
            publish_inputs(inputs, spec.input_manifest, lambda _: None, {})
            self.assertEqual((inputs / 'snapshot-steps.txt').read_bytes(), b'2\n7\n11\n')
            self.assertEqual(run_local(store, self.worker), ())
            self.assertEqual(store.job_state(jobs[0]), 'queued')
            self.assertEqual(store.job_attempts(jobs[0]), ())
        with Store(config.manager.database_path) as store:
            self.assertEqual(store.specification(jobs[0]), spec)
        self.assertEqual((self.source / 'in.lmp').read_bytes(), original)
        self.assertFalse((self.source / 'sampling.lmp').exists())

    def test_batch_success_failure_snapshots_and_reruns(self) -> None:
        failing = self.root / 'failure'
        failing.mkdir()
        (failing / 'in.lmp').write_text('7')
        (failing / 'asset').write_text('other asset')
        entries = json.loads(self.manifest.read_text())
        entries.append({'source_directory': 'failure', 'input_file': 'in.lmp'})
        self.manifest.write_text(json.dumps(entries))
        with Store(self.config.manager.database_path) as store:
            ids = submit_batch(store, self.config, self.manifest)
            (self.source / 'in.lmp').write_text('changed after submission')
            self.assertEqual(run_local(store, self.worker), ids)
            self.assertEqual([store.job_state(j) for j in ids], ['succeeded', 'failed'])
            directories = []
            for job, code in zip(ids, (0, 7)):
                info = inspect_job(store, job)
                attempt = info['attempts'][0]
                self.assertEqual(attempt['exit_code'], code)
                self.assertIsNotNone(attempt['started_at'])
                self.assertIsNotNone(attempt['ended_at'])
                directory = Path(attempt['directory'])
                directories.append(directory)
                self.assertIn('stdout', (directory / 'stdout').read_text())
                self.assertIn('stderr', (directory / 'stderr').read_text())
                self.assertTrue((directory / 'work/result').is_file())
                metadata = json.loads((directory / 'execution.json').read_text())
                self.assertIn('literal; $(no shell)', metadata['argv'])
                self.assertEqual(metadata['environment']['OMP_NUM_THREADS'], '2' if code == 0 else '1')
            self.assertNotEqual(*directories)
            saved = (directories[0] / 'stdout').read_bytes()
            self.assertEqual(run_local(store, self.worker), ())
            self.assertEqual((directories[0] / 'stdout').read_bytes(), saved)
            self.assertEqual(store.reservations(), ())
            new_ids = submit_batch(store, self.config, self.manifest)
            self.assertTrue(set(ids).isdisjoint(new_ids))
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(inspect_job(store, ids[1])['attempts'][0]['exit_code'], 7)

    def test_validation_and_launch_failure(self) -> None:
        with Store(self.config.manager.database_path) as store:
            entries = json.loads(self.manifest.read_text())
            entries.append({'source_directory': 'source', 'input_file': 'missing'})
            self.manifest.write_text(json.dumps(entries))
            with self.assertRaisesRegex(ValueError, 'Missing'):
                submit_batch(store, self.config, self.manifest)
            self.assertEqual(store.jobs(), ())
            self.manifest.write_text(json.dumps(entries[:1]))
            job = submit_batch(store, self.config, self.manifest)[0]
            broken = replace(self.worker, execution=replace(self.worker.execution,
                             mpi_launcher_path=str(self.root / 'missing')))
            run_local(store, broken)
            attempt = store.job_attempts(job)[0]
            self.assertEqual(attempt.state, 'failed')
            self.assertIsNone(attempt.started_at)
            self.assertIn('Launch did not start', attempt.termination_evidence)
            self.assertEqual(store.reservations(), ())

    def test_tampered_input_and_symlink_rejected(self) -> None:
        with Store(self.config.manager.database_path) as store:
            job = submit_batch(store, self.config, self.manifest)[0]
            spec = store.specification(job)
            (Path(spec.work_directory) / 'inputs/in.lmp').write_text('tampered')
            run_local(store, self.worker)
            self.assertIn('SHA-256', store.job_attempts(job)[0].reason)
            (self.source / 'link').symlink_to(self.source / 'asset')
            with self.assertRaisesRegex(ValueError, 'symlinks'):
                submit_batch(store, self.config, self.manifest)

    def test_uncertain_group_retains_reservation_and_never_relaunches(self) -> None:
        with Store(self.config.manager.database_path) as store:
            job = submit_batch(store, self.config, self.manifest)[0]
            with patch('lammps_manager.local.os.killpg', return_value=None):
                self.assertEqual(run_local(store, self.worker), ())
            self.assertEqual(store.job_state(job), 'unknown')
            self.assertEqual(len(store.reservations()), 1)
            self.assertEqual(run_local(store, self.worker), ())
            self.assertEqual(len(store.job_attempts(job)), 1)

    def test_uncertain_termination_stops_remaining_batch(self) -> None:
        entries = json.loads(self.manifest.read_text())
        self.manifest.write_text(json.dumps(entries * 3))
        for outcome in (None, OSError('group check failed'), ValueError('invalid check'),
                        KeyboardInterrupt()):
            with self.subTest(outcome=outcome), Store(':memory:') as store:
                ids = submit_batch(store, self.config, self.manifest)
                with patch('lammps_manager.local.os.killpg',
                           side_effect=[ProcessLookupError(), outcome, ProcessLookupError()]):
                    if isinstance(outcome, KeyboardInterrupt):
                        with self.assertRaises(KeyboardInterrupt):
                            run_local(store, self.worker)
                    else:
                        self.assertEqual(run_local(store, self.worker), ids[:1])
                self.assertEqual([store.job_state(j) for j in ids],
                                 ['succeeded', 'unknown', 'queued'])
                attempt = store.job_attempts(ids[1])[0]
                self.assertIsNotNone(attempt.started_at)
                self.assertIsNone(attempt.ended_at)
                self.assertEqual(store.job_attempts(ids[2]), ())
                self.assertEqual([r.attempt_id for r in store.reservations()],
                                 [attempt.attempt_id])

    def test_prelaunch_failure_allows_next_job(self) -> None:
        entries = json.loads(self.manifest.read_text())
        self.manifest.write_text(json.dumps(entries * 2))
        with Store(self.config.manager.database_path) as store:
            ids = submit_batch(store, self.config, self.manifest)
            spec = store.specification(ids[0])
            (Path(spec.work_directory) / 'inputs/in.lmp').write_text('tampered')
            self.assertEqual(run_local(store, self.worker), ids[1:])
            self.assertEqual([store.job_state(j) for j in ids], ['failed', 'succeeded'])
            self.assertEqual(store.reservations(), ())

    def test_cli_full_flow(self) -> None:
        def cli(*args: str) -> object:
            result = subprocess.run([sys.executable, '-m', 'lammps_manager', *args,
                         '--config', str(self.manager_path)], text=True, capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            return json.loads(result.stdout)
        ids = cli('submit', str(self.manifest))
        self.assertEqual(cli('status')['jobs'][0]['state'], 'queued')
        self.assertEqual(cli('run-local', '--worker-config', str(self.worker_path)), ids)
        self.assertEqual(cli('inspect', ids[0])['state'], 'succeeded')
        self.assertEqual(cli('status')['jobs'][0]['state'], 'succeeded')
