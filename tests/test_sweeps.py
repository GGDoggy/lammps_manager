"""Cartesian submission regressions; all execution uses temporary fake programs."""

from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from unittest.mock import patch

import test_local
from lammps_manager.local import inspect_job, run_local
from lammps_manager.models import specification_from_dict
from lammps_manager.store import Store
from lammps_manager.sweeps import load_sweep, submit_sweep, SweepSubmissionError


class SweepTests(unittest.TestCase):
    setUp = test_local.LocalTests.setUp

    def request(self, **updates):
        (self.source / 'in.lmp').write_text('{{density}} {{temperature}} {{seed}} {{repetition}}')
        data = dict(version=1, source_directory='source', input_file='in.lmp',
                    template_version='ideal-abp-v1', parameters=dict(temperature=[.5, 1.0], density=[.1, .2, .3]),
                    repetitions=4, seed_base=1000, resources=dict(mpi_ranks=1, omp_threads_per_rank=1))
        data.update(updates)
        self.manifest.write_text(json.dumps(data))
        return data

    def test_example_shared_staged_and_reopen(self):
        for mode in ('shared', 'staged'):
            with self.subTest(mode=mode):
                self.request()
                (self.source / 'asset').write_text('asset')
                config = replace(self.config, storage=replace(self.config.storage, mode=mode))
                plan = load_sweep(config, self.manifest)
                self.assertEqual((plan.combinations, plan.jobs), (6, 24))
                self.assertEqual(len(plan.preview()['preview']), 20)
                before = (self.source / 'in.lmp').read_bytes()
                with Store(config.manager.database_path) as store:
                    result = submit_sweep(store, config, plan)
                    ids = [r['job_id'] for r in result['submitted']]
                    self.assertEqual(len(set(ids)), 24)
                    for ordinal, job in enumerate(ids):
                        spec = store.specification(job)
                        metadata = spec.sweep
                        self.assertEqual(metadata.seed, 1000 + ordinal)
                        self.assertEqual(metadata.combination_index, ordinal // 4 + 1)
                        self.assertEqual(metadata.repetition_index, ordinal % 4 + 1)
                        self.assertEqual(metadata.selected_indices['temperature'], ordinal // 4 % 2)
                        self.assertEqual(metadata.selected_indices['density'], ordinal // 8)
                        self.assertEqual(store.job_state(job), 'queued')
                        root = Path(spec.work_directory) / 'inputs'
                        self.assertEqual((root / 'asset').read_bytes(), b'asset')
                        content = (root / 'in.lmp').read_bytes()
                        self.assertEqual(hashlib.sha256(content).hexdigest(), metadata.rendered_sha256)
                        self.assertEqual(specification_from_dict(asdict(spec)), spec)
                        self.assertEqual(inspect_job(store, job)['specification']['sweep']['sweep_id'], result['sweep_id'])
                    saved = asdict(store.specification(ids[0]))
                (self.source / 'asset').write_text('changed')
                with Store(config.manager.database_path) as store:
                    self.assertEqual(asdict(store.specification(ids[0])), saved)
                self.assertEqual((self.source / 'in.lmp').read_bytes(), before)

    def test_invalid_plans_create_nothing(self):
        cases = [dict(parameters={}), dict(parameters={'density': []}), dict(parameters={'density': [None]}),
                 dict(parameters={'density': [[1]]}), dict(parameters={'density': [float('nan')]}),
                 dict(parameters={'density': ['bad\nline']}), dict(parameters={'seed': [1]}),
                 dict(repetitions=True), dict(repetitions=0), dict(seed_base=True),
                 dict(seed_base=900000000), dict(max_jobs=23), dict(max_jobs=False),
                 dict(resources={'mpi_ranks': 0}), dict(resources={'unknown': 1}), dict(extra=1)]
        for changes in cases:
            with self.subTest(changes=changes):
                self.request(**changes)
                with self.assertRaises(ValueError):
                    load_sweep(self.config, self.manifest)
        for template in ('{{density}} {{temperature}}', '{{seed}} {{oops}}',
                         '{{seed}} {{density }} {{temperature}}', '{{seed}}'):
            self.request()
            (self.source / 'in.lmp').write_text(template)
            with self.assertRaises(ValueError):
                load_sweep(self.config, self.manifest)
        self.manifest.write_text('{"version":1,"version":1}')
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            load_sweep(self.config, self.manifest)
        self.assertFalse(Path(self.config.storage.work_root).exists())
        self.assertFalse(Path(self.config.manager.database_path).exists())

    def test_dry_run_cli_and_duplicate_positions(self):
        self.request(parameters={'temperature': [True, True], 'density': ['literal; $x']}, repetitions=1)
        plan = load_sweep(self.config, self.manifest)
        coordinates = list(plan.coordinates())
        self.assertEqual([c['selected_indices']['temperature'] for c in coordinates], [0, 1])
        self.assertEqual(plan.render(coordinates[0]), b'literal; $x 1 1000 1')
        command = [sys.executable, '-m', 'lammps_manager', 'submit-sweep', '--config',
                   str(self.manager_path), str(self.manifest)]
        preview = subprocess.run(command + ['--dry-run'], check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(preview.stdout)['jobs'], 2)
        self.assertFalse(Path(self.config.storage.work_root).exists())
        self.assertFalse(Path(self.config.manager.database_path).exists())
        submitted = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertEqual(len(json.loads(submitted.stdout)['submitted']), 2)

    def test_source_change_and_partial_failure(self):
        self.request()
        plan = load_sweep(self.config, self.manifest)
        (self.source / 'asset').write_text('tampered')
        with Store(self.config.manager.database_path) as store:
            with self.assertRaisesRegex(SweepSubmissionError, 'Source changed'):
                submit_sweep(store, self.config, plan)
            self.assertEqual(store.jobs(), ())
            self.request()
            plan = load_sweep(self.config, self.manifest)
            original = store.submit
            def fail_second(spec):
                if len(store.jobs()) == 1:
                    raise sqlite3.OperationalError('injected failure')
                original(spec)
            with patch.object(store, 'submit', side_effect=fail_second):
                with self.assertRaises(SweepSubmissionError) as caught:
                    submit_sweep(store, self.config, plan)
            outcome = caught.exception.result
            self.assertEqual(len(outcome['submitted']), 1)
            self.assertEqual(outcome['failed_coordinate']['ordinal'], 1)
            job = outcome['submitted'][0]['job_id']
            self.assertEqual(store.specification(job).sweep.sweep_id, outcome['sweep_id'])
            self.assertEqual(store.job_state(job), 'queued')

    def test_fake_execution_retry_and_legacy(self):
        self.request(parameters={'code': [0, 7]}, repetitions=2, resources={'max_retries': 1})
        (self.source / 'in.lmp').write_text('{{code}}')
        # Seeds remain effective while the fake program reads an explicit first token.
        (self.source / 'in.lmp').write_text('{{code}} {{seed}}')
        program = Path(self.worker.execution.lammps_path)
        program.write_text('#!' + sys.executable + '\nimport pathlib,sys\n'
                           'p=pathlib.Path("in.lmp").read_text()\n'
                           'pathlib.Path("output").write_text(p)\nsys.exit(int(p.split()[0]))\n')
        with Store(self.config.manager.database_path) as store:
            result = submit_sweep(store, self.config, load_sweep(self.config, self.manifest))
            ids = [row['job_id'] for row in result['submitted']]
            run_local(store, self.worker)
            self.assertEqual([store.job_state(j) for j in ids], ['succeeded', 'succeeded', 'failed', 'failed'])
            failed = ids[2]
            saved = store.specification(failed)
            previous = Path(store.job_attempts(failed)[0].directory) / 'work/output'
            contents = previous.read_bytes()
            store.retry(failed)
            run_local(store, self.worker)
            self.assertEqual(store.specification(failed), saved)
            self.assertEqual(previous.read_bytes(), contents)
            self.assertEqual(len(store.job_attempts(failed)), 2)
            self.assertEqual(len(store.jobs()), 4)
            old = asdict(saved)
            old.pop('sweep')
            self.assertIsNone(specification_from_dict(old).sweep)

    def test_source_restrictions_and_staged_limits(self):
        self.request()
        (self.source / 'link').symlink_to(self.source / 'asset')
        with self.assertRaisesRegex(ValueError, 'regular'):
            load_sweep(self.config, self.manifest)
        (self.source / 'link').unlink()
        config = replace(self.config, storage=replace(self.config.storage, work_root=str(self.source / 'jobs')))
        with self.assertRaisesRegex(ValueError, 'work_root'):
            load_sweep(config, self.manifest)
        config = replace(self.config, storage=replace(self.config.storage, mode='staged', max_input_file_bytes=1))
        with self.assertRaises(ValueError):
            load_sweep(config, self.manifest)

    def test_key_order_single_axis_and_publication_verification(self):
        self.request()
        (self.source / 'a').mkdir()
        (self.source / 'a' / 'x').write_bytes(b'nested')
        (self.source / 'a.b').write_bytes(b'sibling')
        original = list(load_sweep(self.config, self.manifest).coordinates())
        data = json.loads(self.manifest.read_text())
        data['parameters'] = dict(reversed(list(data['parameters'].items())))
        self.manifest.write_text(json.dumps(data))
        self.assertEqual(list(load_sweep(self.config, self.manifest).coordinates()), original)
        self.request(parameters={'code': [0]}, repetitions=1)
        (self.source / 'in.lmp').write_text('{{code}} {{seed}}')
        plan = load_sweep(self.config, self.manifest)
        self.assertEqual((plan.combinations, plan.jobs), (1, 1))
        with Store(self.config.manager.database_path) as store:
            outcome = submit_sweep(store, self.config, plan)
            job = outcome['submitted'][0]['job_id']
            root = Path(store.specification(job).work_directory) / 'inputs'
            self.assertEqual((root / 'a/x').read_bytes(), b'nested')
        from lammps_manager import sweeps
        publish = sweeps.publish_inputs
        def tamper(destination, files, fetch, limits):
            def corrupt(index):
                import io
                return io.BytesIO(b'corrupt')
            return publish(destination, files, corrupt, limits)
        with Store(self.config.manager.database_path) as store:
            with patch.object(sweeps, 'publish_inputs', side_effect=tamper):
                with self.assertRaises(SweepSubmissionError) as caught:
                    submit_sweep(store, self.config, plan)
            self.assertEqual(caught.exception.result['submitted'], [])
            self.assertEqual(len(store.jobs()), 1)
