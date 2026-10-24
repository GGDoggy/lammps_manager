"""Per-run ABP parameters, immutable replay and parameter-aware checkpoints."""
from dataclasses import replace
import json
import math
from pathlib import Path
import unittest
from unittest.mock import patch

import test_segments as fixtures
from lammps_manager import checkpoints as cp, segments as seg
from lammps_manager.continuation import Recipe
from lammps_manager.abp_parameters import (PARAMETERIZED_VERSION, normalize_parameters,
    model_summary, render_initialization, render_parameterized_input)
from lammps_manager.store import Store, StoreError

RECIPE = Recipe('wca-abp-2d-v1', 'statistical')
PARAMETERS = dict(particles=9, density=.25, epsilon=2., sigma=.8, kBT=.7,
    gamma_t=4., gamma_r=2., propulsion=3., dt=.002,
    initial_positions={'mode': 'square-lattice'}, initial_orientations={'mode': 'aligned', 'angle': math.pi/2})


class ParameterTests(unittest.TestCase):
    setUp = fixtures.SegmentTests.setUp
    current = fixtures.SegmentTests.current
    assign = fixtures.SegmentTests.assign
    finish = fixtures.SegmentTests.finish
    checkpoint = fixtures.SegmentTests.checkpoint

    def submit(self, **changes):
        request = dict(total_steps=21, step_max=10, build_fingerprint=fixtures.BUILD,
            run_id='run', recipe=RECIPE, model_parameters=PARAMETERS)
        request.update(changes)
        return seg.submit_run(self.store, self.config, **request)

    def test_submit_snapshots_all_parameters_and_rejects_changed_intent(self):
        self.submit()
        view = seg.inspect_run(self.store, 'run')
        self.assertEqual(view['snapshot']['contract_version'], PARAMETERIZED_VERSION)
        self.assertEqual(view['snapshot']['model_parameters'], normalize_parameters(RECIPE, PARAMETERS))
        first = self.store.specification('run.s000000')
        with patch('lammps_manager.segments.secrets.randbelow', side_effect=AssertionError('redrawn')):
            self.submit(model_parameters=json.loads(json.dumps(PARAMETERS)))
            with self.assertRaises(StoreError):
                self.submit(model_parameters={**PARAMETERS, 'dt': .003})
        self.assertEqual(self.store.specification('run.s000000'), first)
        self.submit(run_id='different', model_parameters={**PARAMETERS, 'particles': 10, 'propulsion': 5.})
        self.assertEqual(seg.inspect_run(self.store, 'different')['snapshot']['model']['particles'], 10)

    def test_checkpoint_uses_submitted_particle_count_and_dt(self):
        self.submit()
        for stop in (10, 20, 21):
            attempt = self.assign()
            contract = cp.record(self.store, attempt.attempt_id)['contract']
            self.assertEqual(contract['global_time'], stop * .002)
            cp.validate_contract(cp.decode_contract(contract))
            self.finish(attempt=attempt.attempt_id)
            seg.refresh(self.store)
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'succeeded')
        self.assertEqual(self.store.reservations(), ())

    def test_normalization_and_initialization_choices(self):
        full = normalize_parameters(RECIPE, PARAMETERS)
        summary = model_summary(RECIPE, full)
        self.assertEqual(summary['box_length'], 6.)
        self.assertAlmostEqual(summary['cutoff'], .8 * 2**(1/6))
        initial = render_initialization(RECIPE, full, position_seed=1, orientation_seed=2)
        self.assertEqual(initial.count('create_atoms 1 single'), 9)
        self.assertIn('set group all dipole', initial)
        self.assertNotIn('dipole/random', initial)
        for start, stop in ((0, 10), (10, 20)):
            script = render_parameterized_input(RECIPE, summary, start=start, stop=stop, total=21,
                stream_seed=3, position_seed=1, orientation_seed=2)
            self.assertIn('pair_coeff * * 2 0.80000000000000004', script)
            self.assertIn('gamma_t 4 gamma_r 2', script)
            integrator = next(line.split() for line in script.splitlines() if line.startswith('fix integrate'))
            self.assertEqual(float(integrator[4]), .7)
            self.assertIn('propel/self dipole 3', script)
            self.assertIn('timestep 0.002', script)
            self.assertEqual('include initial-state.lmp' in script, start == 0)
        explicit = normalize_parameters(RECIPE, dict(particles=2, density=.1,
            initial_positions=dict(mode='explicit', coordinates=[[.1, .2], [1., 2.]]),
            initial_orientations=dict(mode='explicit', angles=[0, math.pi])))
        text = render_initialization(RECIPE, explicit, position_seed=1, orientation_seed=2)
        self.assertIn('create_atoms 1 single 1 2 0 units box', text)
        self.assertIn('set atom 1 dipole 1 0 0', text)
        self.assertIn('set atom 2 dipole -1', text)
        random = render_initialization(RECIPE, {'particles': 2, 'initial_positions': {'mode': 'random'}},
            position_seed=1, orientation_seed=2)
        self.assertIn('random 2 1 box overlap 1 maxtry 1000 units box', random)
        self.assertIn('dipole/random 2 1.0', random)

    def test_invalid_values_fail_before_persistence(self):
        bad = [dict(particles=x) for x in (True, 0, -1, 1.5, 1000001)]
        for field in ('density', 'sigma', 'kBT', 'gamma_t', 'gamma_r', 'dt'):
            bad.extend({field: value} for value in (0, -1, True, float('nan'), float('inf'), '1'))
        for field in ('epsilon', 'propulsion'):
            bad.extend({field: value} for value in (-1, True, float('nan'), float('inf'), '1'))
        bad += [dict(unknown=1), dict(initial_positions={'mode': 'shell'}),
            dict(initial_orientations={'mode': 'aligned', 'angle': float('nan')}),
            dict(initial_positions={'mode': 'explicit', 'coordinates': [[0,0]]}),
            dict(initial_orientations={'mode': 'explicit', 'angles': [0]})]
        for changes in bad:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.submit(model_parameters={**PARAMETERS, **changes})
        self.assertEqual(self.store.jobs(), ())
        self.assertEqual(self.store._db.execute('SELECT count(*) FROM simulation_runs').fetchone()[0], 0)

    def test_ideal_parameters_and_incompatible_interaction(self):
        ideal = Recipe('ideal-abp-2d-v1', 'statistical')
        parameters = {**PARAMETERS, 'epsilon': 0, 'initial_positions': {'mode': 'random'},
                      'initial_orientations': {'mode': 'random'}}
        self.submit(recipe=ideal, model_parameters=parameters)
        snapshot = seg.inspect_run(self.store, 'run')['snapshot']
        script = (self.root / 'run.s000000/inputs/in.lmp').read_text()
        initializer = (self.root / 'run.s000000/inputs/initial-state.lmp').read_text()
        self.assertIn('pair_style zero 0.80000000000000004', script)
        self.assertNotIn('pair_style lj/cut', script)
        self.assertIn('create_atoms 1 random 9', initializer)
        self.assertNotIn('overlap', initializer)
        self.assertEqual(snapshot['model']['particles'], 9)
        with self.assertRaisesRegex(ValueError, 'epsilon=0'):
            self.submit(run_id='invalid-ideal', recipe=ideal, model_parameters=PARAMETERS)

    def test_extreme_geometry_time_and_explicit_input_validation(self):
        bad = [dict(density=5e-324), dict(dt=1e308),
            dict(initial_positions=dict(mode='explicit', coordinates=[[0, 0]] * 9)),
            dict(initial_positions=dict(mode='explicit', coordinates=[[0, 0]] * 8 + [[99, 0]])),
            dict(initial_positions=dict(mode='explicit', coordinates=[[0, 0]] * 8 + [[-1, 0]])),
            dict(initial_orientations=dict(mode='explicit', angles=[0] * 8 + [True])),
            dict(initial_orientations=dict(mode='random', script='untracked'))]
        for changes in bad:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.submit(model_parameters={**PARAMETERS, **changes})
        self.assertEqual(self.store._db.execute('SELECT count(*) FROM simulation_runs').fetchone()[0], 0)

    def test_reopen_retry_and_successor_keep_parameters_and_seed(self):
        self.submit()
        original = seg.inspect_run(self.store, 'run')['snapshot']
        self.finish()
        seg.refresh(self.store)
        job = self.current()['job_id']
        before = self.store.specification(job)
        script = (Path(before.work_directory) / 'inputs/in.lmp').read_text()
        self.assertNotIn('include initial-state', script)
        self.assertNotIn('create_atoms', script)
        self.assertNotIn('dipole/random', script)
        failed = self.assign(job)
        self.store.transition(failed.attempt_id, 'running')
        self.store.transition(failed.attempt_id, 'failed', reason='injected', termination_evidence='reaped')
        provenance = cp.record(self.store, failed.attempt_id)['contract']['stream_provenance']
        self.store.close()
        self.store = Store(self.db)
        with patch('lammps_manager.segments.secrets.randbelow', side_effect=AssertionError('redrawn')):
            self.submit()
            self.store.retry(job)
            retry = self.assign(job, attempt='retry')
        self.assertEqual(self.store.specification(job), before)
        self.assertEqual(cp.record(self.store, retry.attempt_id)['contract']['stream_provenance'], provenance)
        self.assertEqual(seg.inspect_run(self.store, 'run')['snapshot'], original)

    def test_parameterized_boundary_and_launch_reject_corruption(self):
        from types import SimpleNamespace
        from lammps_manager.config import Execution
        self.submit()
        attempt = self.assign()
        contract = cp.record(self.store, attempt.attempt_id)['contract']
        directory = self.root / 'boundary'
        directory.mkdir()
        valid = '1 by 1 by 1 MPI processor grid\nusing 1 OpenMP thread(s) per MPI task\nStep Time Atoms\n10 0.02 9\nLoop time\n'
        (directory / 'stdout').write_text(valid)
        cp.validate_boundary(directory, contract)
        for bad in (valid.replace('0.02', '0.001'), valid.replace('0.02 9', '0.02 256')):
            (directory / 'stdout').write_text(bad)
            with self.assertRaises(ValueError):
                cp.validate_boundary(directory, contract)
        worker = SimpleNamespace(execution=Execution('/fake/lmp', '/fake/mpirun'))
        root = Path(self.store.specification(attempt.job_id).work_directory) / 'inputs'
        with patch('lammps_manager.continuation.fingerprint_build', return_value=fixtures.BUILD):
            cp.prepare_launch(self.store, worker, attempt.attempt_id, root)
            (root / 'model-parameters.json').write_text('{}')
            with self.assertRaises(ValueError):
                cp.prepare_launch(self.store, worker, attempt.attempt_id, root)
        model = json.loads(contract['stream_provenance'])
        model['model']['dt'] = .003
        with self.assertRaises(ValueError):
            cp.validate_contract(replace(cp.decode_contract(contract), stream_provenance=cp.canonical(model)))

    def test_explicit_initial_arrays_keep_checkpoint_bounded(self):
        n = 2000
        parameters = dict(particles=n, density=.01,
            initial_positions=dict(mode='explicit', coordinates=[[float(i), 0.] for i in range(n)]),
            initial_orientations=dict(mode='explicit', angles=[0.] * n))
        # Fit the explicit coordinates inside a square of side sqrt(n/density).
        parameters['initial_positions']['coordinates'] = [[(i % 100) * 2., (i // 100) * 2.] for i in range(n)]
        self.submit(model_parameters=parameters)
        snapshot = seg.inspect_run(self.store, 'run')['snapshot']
        self.assertGreater(len(cp.canonical(snapshot['model_parameters'])), 16384)
        self.assertLess(len(seg.checkpoint_provenance(snapshot, 0)), 16384)
        attempt = self.assign()
        cp.validate_contract(cp.decode_contract(cp.record(self.store, attempt.attempt_id)['contract']))

    def test_cli_parameters_and_v1_parameter_rejection(self):
        from dataclasses import asdict
        from contextlib import redirect_stdout
        from io import StringIO
        import yaml
        from lammps_manager.cli import main
        config = self.root / 'manager.yaml'
        config.write_text(yaml.safe_dump(json.loads(json.dumps(asdict(self.config)))))
        request = self.root / 'run.json'
        request.write_text(json.dumps(dict(total_steps=21, build_fingerprint=fixtures.BUILD,
            run_id='cli', recipe=dict(name=RECIPE.name, mode=RECIPE.mode), model_parameters=PARAMETERS)))
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(['submit-run', '--config', str(config), str(request)]), 0)
        self.assertEqual(json.loads(output.getvalue())['snapshot']['model_parameters']['particles'], 9)
        for version in ('engineering-abp-v1', '', 'scientific'):
            with self.assertRaises(ValueError):
                self.submit(contract_version=version)


class ParameterWorkerTests(unittest.TestCase):
    setUp = fixtures.SegmentWorkerTests.setUp

    def test_parameterized_shared_and_staged_pipelines(self):
        import time
        from lammps_manager.config import Execution, WorkerConfig
        from lammps_manager.worker import Worker
        self.program.write_text(
            '#!/usr/bin/env python3\n'
            'from pathlib import Path\nimport re,json\n'
            'text=Path("in.lmp").read_text(); model=json.loads(Path("model-parameters.json").read_text())\n'
            'steps,total=map(int,re.findall(r"run (\\d+) start 0 stop (\\d+)",text)[-1])\n'
            'initial="include initial-state.lmp" in text\n'
            'start=0 if initial else int(Path("checkpoint.restart").read_text())\n'
            'stop=start+steps; dt=model["dt"]; n=model["particles"]\n'
            'print("1 by 1 by 1 MPI processor grid")\n'
            'print("using 1 OpenMP thread(s) per MPI task")\n'
            'print("Step Time Atoms");print(start,start*dt,n);print(stop,stop*dt,n);print("Loop time")\n'
            'Path("checkpoint.restart").write_text(str(stop))\n')
        self.program.chmod(0o700)
        for mode in ('shared', 'staged'):
            self.config = replace(self.config, storage=replace(self.config.storage, mode=mode))
            self.service.config = self.config
            root = self.work if mode == 'shared' else self.root / 'parameterized-worker'
            root.mkdir(exist_ok=True)
            worker = Worker(WorkerConfig(self.capacities[0], self.endpoint, str(root),
                Execution(str(self.program), str(self.launcher)), self.tokens['a'], storage_mode=mode),
                self.root / ('parameterized-journal-' + mode))
            self.addCleanup(worker.close)
            with Store(self.config.manager.database_path) as store:
                seg.submit_run(store, self.config, total_steps=21, step_max=10,
                    build_fingerprint=fixtures.BUILD, run_id=mode, recipe=RECIPE, model_parameters=PARAMETERS)
            deadline = time.monotonic() + 15
            with patch('lammps_manager.continuation.fingerprint_build', return_value=fixtures.BUILD):
                while True:
                    worker.tick()
                    for future in tuple(worker.futures.values()):
                        future.result(timeout=5)
                    if worker.collection_future:
                        worker.collection_future.result(timeout=5)
                    with Store(self.config.manager.database_path) as store:
                        view = seg.inspect_run(store, mode)
                        if view['status'] == 'succeeded':
                            self.assertEqual(view['committed_steps'], 21)
                            self.assertEqual(len(view['segments']), 3)
                            self.assertEqual(store.reservations(), ())
                            for item in view['segments']:
                                attempts = store.job_attempts(item['job_id'])
                                self.assertEqual(len(attempts), 1)
                                contract = cp.record(store, attempts[0].attempt_id)
                                self.assertTrue(contract['acknowledged'])
                                self.assertEqual(contract['contract']['global_time'], item['stop_step'] * .002)
                                self.assertEqual(store.result_collection(attempts[0].attempt_id)['status'], 'collected')
                            break
                        if time.monotonic() > deadline:
                            self.fail(str(view))
                    time.sleep(.01)
            worker.close()
