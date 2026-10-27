"""T021 engineering contracts: entropy, immutable replay and physical clocks."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

import test_segments as fixtures
BUILD = fixtures.BUILD
from lammps_manager import segments as seg, checkpoints as cp
from lammps_manager.continuation import (Recipe, ENGINEERING_VERSION, MAX_RUN_COMMAND,
    render_engineering_input, engineering_parameters, require_contract, require_validated)
from lammps_manager.store import Store, StoreError


class EngineeringTests(unittest.TestCase):
    setUp = fixtures.SegmentTests.setUp
    current = fixtures.SegmentTests.current
    assign = fixtures.SegmentTests.assign
    finish = fixtures.SegmentTests.finish
    checkpoint = fixtures.SegmentTests.checkpoint

    def submit(self, **changes):
        request = dict(total_steps=250001, step_max=100000, build_fingerprint=BUILD,
            run_id='run', recipe=Recipe('wca-abp-2d-v1', 'statistical'))
        request.update(changes)
        return seg.submit_run(self.store, self.config, **request)

    def test_random_replay_reopen_and_collision_redraw(self):
        with patch('lammps_manager.segments.secrets.randbelow', side_effect=[0, 0, 1, 2, 3, 4]) as entropy:
            self.submit()
            self.assertEqual(entropy.call_count, 6)
        before = seg.inspect_run(self.store, 'run')['snapshot']
        self.assertEqual(json.loads(before['stream_provenance'])['seeds'], [1, 2, 3, 4, 5])
        spec = self.store.specification('run.s000000')
        self.store.close()
        self.store = Store(self.db)
        with patch('lammps_manager.segments.secrets.randbelow', side_effect=AssertionError('redrawn')):
            self.submit()
            with self.assertRaises(StoreError):
                self.submit(total_steps=250002)
        self.assertEqual(seg.inspect_run(self.store, 'run')['snapshot'], before)
        self.assertEqual(self.store.specification('run.s000000'), spec)

    def test_failed_publication_preserves_allocation_for_replay(self):
        with patch('lammps_manager.segments.publish_inputs', side_effect=OSError('disk unavailable')):
            with self.assertRaises(OSError):
                self.submit()
        before = seg.inspect_run(self.store, 'run')['snapshot']
        self.assertEqual(self.store.jobs(), ())
        with patch('lammps_manager.segments.secrets.randbelow', side_effect=AssertionError('redrawn')):
            self.submit()
        self.assertEqual(seg.inspect_run(self.store, 'run')['snapshot'], before)
        self.assertEqual(len(self.store.jobs()), 1)

    def test_database_rollback_after_publication_replays_same_tree(self):
        with patch.object(self.store, '_submit_prepared', side_effect=StoreError('injected DB rollback')):
            with self.assertRaises(StoreError):
                self.submit()
        path = self.root / 'run.s000000/inputs/in.lmp'
        before = path.read_bytes()
        snapshot = seg.inspect_run(self.store, 'run')['snapshot']
        self.assertEqual(self.store.jobs(), ())
        with patch('lammps_manager.segments.secrets.randbelow', side_effect=AssertionError('redrawn')):
            self.submit()
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(seg.inspect_run(self.store, 'run')['snapshot'], snapshot)
        self.assertEqual(self.store.specification('run.s000000').input_sha256, hashlib.sha256(before).hexdigest())

    def test_concurrent_submission_allocates_once(self):
        barrier = threading.Barrier(2)
        results = []
        errors = []
        def submit():
            try:
                with Store(self.db) as store:
                    barrier.wait(timeout=5)
                    results.append(seg.submit_run(store, self.config, total_steps=250001,
                        build_fingerprint=BUILD, run_id='run', recipe=Recipe('wca-abp-2d-v1', 'statistical')))
            except Exception as exc:
                errors.append(exc)
        with patch('lammps_manager.segments.secrets.randbelow', side_effect=range(5)) as entropy:
            threads = [threading.Thread(target=submit) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(10)
            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual(errors, [])
            self.assertEqual(results, ['run', 'run'])
            self.assertEqual(entropy.call_count, 5)
        self.assertEqual(len(self.store.jobs()), 1)

    def test_recipe_clock_digest_retry_and_completed_chain(self):
        self.submit()
        snapshot = seg.inspect_run(self.store, 'run')['snapshot']
        for ordinal, stop in enumerate((100000, 200000, 250001)):
            job = self.current()['job_id']
            attempt = self.assign(job)
            contract = cp.record(self.store, attempt.attempt_id)['contract']
            self.assertEqual(contract['global_time'], stop * .0001)
            self.assertEqual(contract['recipe_sha256'], snapshot['recipe_sha256'])
            self.assertLess(len(contract['stream_provenance']), 16384)
            cp.validate_contract(cp.decode_contract(contract))
            for field, value in (('global_time', stop * .005), ('recipe_sha256', '0' * 64)):
                with self.assertRaises(ValueError):
                    cp.validate_contract(cp.decode_contract({**contract, field: value}))
            if ordinal == 1:
                before = self.store.specification(job)
                self.store.transition(attempt.attempt_id, 'running')
                self.store.transition(attempt.attempt_id, 'failed', reason='injected', termination_evidence='reaped')
                self.store.retry(job)
                attempt = self.assign(job, attempt='retry')
                self.assertEqual(self.store.specification(job), before)
                self.assertEqual(cp.record(self.store, attempt.attempt_id)['contract']['stream_provenance'], contract['stream_provenance'])
            self.finish(job, attempt=attempt.attempt_id)
            seg.refresh(self.store)
        view = seg.inspect_run(self.store, 'run')
        self.assertEqual(view['status'], 'succeeded')
        self.assertEqual(view['committed_steps'], 250001)
        self.assertEqual(self.store.reservations(), ())

    def test_render_state_layout_bounds_and_legacy_gate(self):
        recipe = Recipe('wca-abp-2d-v1', 'statistical', 3, 1)
        initial = render_engineering_input(recipe, start=0, stop=10, total=21,
            stream_seed=1, position_seed=2, orientation_seed=3)
        resumed = render_engineering_input(recipe, start=10, stop=21, total=21,
            stream_seed=4, position_seed=2, orientation_seed=3)
        for text in (initial, resumed):
            self.assertIn('pair_modify shift yes', text)
            self.assertIn('timestep 0.0001', text)
            self.assertNotIn('reset_timestep', text)
            self.assertNotIn('20000', text)
        self.assertIn('lattice sq 0.5', initial)
        self.assertEqual(initial.count('create_atoms'), 1)
        self.assertIn('read_restart engineering-smoke.restart', initial)
        self.assertIn('read_restart engineering-initial.restart', initial)
        self.assertNotIn('engineering-smoke', resumed)
        self.assertIn('read_restart checkpoint.restart', resumed)
        self.assertNotIn('create_atoms', resumed)
        self.assertNotIn('dipole/random', resumed)
        long = render_engineering_input(recipe, start=0, stop=MAX_RUN_COMMAND + 1,
            total=MAX_RUN_COMMAND + 1, stream_seed=1, position_seed=2, orientation_seed=3)
        self.assertIn(f'run {MAX_RUN_COMMAND} start', long)
        self.assertIn(f'run 1 start 0 stop {MAX_RUN_COMMAND + 1}\nwrite_restart checkpoint.restart', long)
        with self.assertRaises(ValueError):
            require_validated(Recipe('ideal-abp-2d-v1', 'statistical'),
                build_fingerprint=BUILD, total_steps=250001, segment_ends=(100000, 200000, 250001))
        for changes in (dict(build_fingerprint='invalid'), dict(recipe=Recipe('wca-abp-2d-v1', 'exact')),
                        dict(recipe=Recipe('wca-abp-2d-v1', 'statistical', 1, 2)),
                        dict(recipe=Recipe('unsupported', 'statistical')),
                        dict(recipe=Recipe('wca-abp-2d-v1', 'statistical', 0, 1)),
                        dict(recipe=Recipe('wca-abp-2d-v1', 'statistical', True, 1)),
                        dict(total_steps=2**63), dict(version='unknown')):
            request = dict(recipe=recipe, build_fingerprint=BUILD, total_steps=21,
                segment_ends=(10,21), version=ENGINEERING_VERSION)
            request.update(changes)
            with self.assertRaises(ValueError):
                require_contract(**request)
        self.submit(recipe=Recipe('ideal-abp-2d-v1', 'statistical'), contract_version=ENGINEERING_VERSION)
        self.assertEqual(seg.inspect_run(self.store, 'run')['snapshot']['model']['dt'], .005)

    def test_cli_versioned_wca_submission_and_inspection(self):
        from dataclasses import asdict
        from contextlib import redirect_stdout
        from io import StringIO
        import yaml
        from lammps_manager.cli import main
        config = self.root / 'manager.yaml'
        config.write_text(yaml.safe_dump(json.loads(json.dumps(asdict(self.config)))))
        request = self.root / 'run.json'
        request.write_text(json.dumps(dict(total_steps=250001, build_fingerprint=BUILD,
            run_id='cli-wca', recipe=dict(name='wca-abp-2d-v1', mode='statistical'),
            contract_version=ENGINEERING_VERSION)))
        for command, target in (('submit-run', str(request)), ('inspect-run', 'cli-wca')):
            output = StringIO()
            with redirect_stdout(output):
                self.assertEqual(main([command, '--config', str(config), target]), 0)
            view = json.loads(output.getvalue())
            self.assertEqual(view['snapshot']['contract_version'], ENGINEERING_VERSION)
            self.assertEqual(view['snapshot']['evidence_status'], 'not_assessed')
            self.assertEqual(view['snapshot']['model']['dt'], .0001)
        with self.assertRaises(ValueError):
            self.submit(contract_version='')

    def test_maximum_seed_allocation_has_bounded_checkpoint_reference(self):
        with patch('lammps_manager.segments.secrets.randbelow', side_effect=iter(range(100002))):
            self.submit(total_steps=100000, step_max=1)
        snapshot = seg.inspect_run(self.store, 'run')['snapshot']
        self.assertEqual(len(json.loads(snapshot['stream_provenance'])['seeds']), 100002)
        self.assertLess(len(seg.checkpoint_provenance(snapshot, 99999)), 16384)
        attempt = self.assign()
        cp.validate_contract(cp.decode_contract(cp.record(self.store, attempt.attempt_id)['contract']))

    def test_preflight_actual_build_input_and_fixed_profile(self):
        from types import SimpleNamespace
        from lammps_manager.config import Execution
        self.submit()
        attempt = self.assign()
        work = Path(self.store.specification(attempt.job_id).work_directory) / 'inputs'
        worker = SimpleNamespace(execution=Execution('/fake/lmp', '/fake/mpirun'))
        with patch('lammps_manager.continuation.fingerprint_build', return_value=BUILD):
            cp.prepare_launch(self.store, worker, attempt.attempt_id, work)
            worker.execution = replace(worker.execution, mpi_args=('--bind-to', 'none'))
            cp.prepare_launch(self.store, worker, attempt.attempt_id, work)
            worker.execution = replace(worker.execution, mpi_args=('--host', 'elsewhere'))
            with self.assertRaisesRegex(ValueError, 'fixed local MPI'):
                cp.prepare_launch(self.store, worker, attempt.attempt_id, work)
            worker.execution = replace(worker.execution, mpi_args=())
            (work / 'in.lmp').write_text('edited input')
            with self.assertRaisesRegex(ValueError, 'renderer'):
                cp.prepare_launch(self.store, worker, attempt.attempt_id, work)
        with patch('lammps_manager.continuation.fingerprint_build', return_value='0'*64):
            with self.assertRaisesRegex(ValueError, 'environment'):
                cp.prepare_launch(self.store, worker, attempt.attempt_id, work)

    def test_boundary_checks_recipe_clock_particles_and_actual_layout(self):
        self.submit()
        attempt = self.assign()
        data = cp.record(self.store, attempt.attempt_id)['contract']
        root = self.root / 'boundary'
        root.mkdir()
        valid = '1 by 1 by 1 MPI processor grid\nusing 1 OpenMP thread(s) per MPI task\nStep Time Atoms\n100000 10 256\nLoop time\n'
        (root / 'stdout').write_text(valid)
        cp.validate_boundary(root, data)
        for broken in (valid.replace('1 by 1 by 1', '2 by 1 by 1'),
                       valid.replace('using 1 OpenMP', 'using 2 OpenMP'),
                       valid.replace('100000 10 256', '100000 500 256'),
                       valid.replace('100000 10 256', '100000 10 1024'),
                       valid.replace('1 by 1 by 1 MPI processor grid\n', '')):
            (root / 'stdout').write_text(broken)
            with self.assertRaises(ValueError):
                cp.validate_boundary(root, data)

    def test_corrupt_seed_reference_cannot_register(self):
        self.submit()
        attempt = self.assign()
        contract = cp.decode_contract(cp.record(self.store, attempt.attempt_id)['contract'])
        provenance = json.loads(contract.stream_provenance)
        provenance['seeds'][2] += 100
        with self.assertRaisesRegex(StoreError, 'immutable random allocation'):
            cp.expect(self.store, replace(contract, stream_provenance=cp.canonical(provenance)))

    def test_unknown_execution_and_active_cancellation_retain_reservations(self):
        fixtures.SegmentTests.test_unknown_attempt_retains_resources_and_no_successor(self)
        seg.cancel_run(self.store, 'run')
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'cancelling')
        self.assertEqual(len(self.store.reservations()), 1)
        self.assertEqual(len(self.store.jobs()), 1)

    def test_corrupt_checkpoint_never_creates_successor(self):
        fixtures.SegmentTests.test_corrupt_checkpoint_blocks_successor_and_preserves_progress(self)

    def test_reused_worker_boot_cannot_restore_new_contract(self):
        fixtures.SegmentTests.test_same_worker_id_cannot_replace_pinned_boot(self)

    def test_explicit_short_unequal_plan_and_allocation_priority_intent(self):
        self.submit(total_steps=21, step_max=100, segment_ends=(4, 11, 21))
        with patch('lammps_manager.segments.secrets.randbelow', side_effect=AssertionError('redrawn')):
            with self.assertRaises(StoreError):
                self.submit(total_steps=21, step_max=100, segment_ends=(4, 11, 21), initial_priority=10)


class EngineeringWorkerTests(unittest.TestCase):
    setUp = fixtures.SegmentWorkerTests.setUp

    def test_shared_and_staged_long_engineering_chains(self):
        for mode in ('shared', 'staged'):
            with self.subTest(mode=mode):
                self._execute(mode)

    def _execute(self, mode):
        import time
        from lammps_manager.worker import Worker
        from lammps_manager.config import WorkerConfig, Execution
        # Each complete fake chain runs through actual polling, input verification,
        # checkpoint/result uploads and scheduler reservation release.
        self.program.write_text(
            '#!/usr/bin/env python3\n'
            'from pathlib import Path\nimport re\n'
            'text=Path("in.lmp").read_text()\n'
            'steps,total=map(int,re.findall(r"run (\\d+) start 0 stop (\\d+)",text)[-1])\n'
            'start=int(Path("checkpoint.restart").read_text()) if "create_atoms" not in text else 0\n'
            'stop=start+steps\n'
            'print("1 by 1 by 1 MPI processor grid")\n'
            'print("using 1 OpenMP thread(s) per MPI task")\n'
            'print("Step Time Atoms")\nprint(start,start*.0001,256)\n'
            'print(stop,stop*.0001,256)\nprint("Loop time")\n'
            'Path("checkpoint.restart").write_text(str(stop))\n')
        self.program.chmod(0o700)
        self.config = replace(self.config, storage=replace(self.config.storage, mode=mode))
        self.service.config = self.config
        work_root = self.work if mode == 'shared' else self.root / 'engineering-worker'
        work_root.mkdir(exist_ok=True)
        worker = Worker(WorkerConfig(self.capacities[0], self.endpoint, str(work_root),
            Execution(str(self.program), str(self.launcher)), self.tokens['a'], storage_mode=mode),
            self.root / ('engineering-journal-' + mode))
        self.addCleanup(worker.close)
        run_id = 'engineering-' + mode
        with Store(self.config.manager.database_path) as store:
            seg.submit_run(store, self.config, total_steps=250001, build_fingerprint=BUILD,
                run_id=run_id, recipe=Recipe('wca-abp-2d-v1', 'statistical'))
        deadline = time.monotonic() + 15
        with patch('lammps_manager.continuation.fingerprint_build', return_value=BUILD):
            while True:
                worker.tick()
                for future in tuple(worker.futures.values()):
                    future.result(timeout=5)
                if worker.collection_future:
                    worker.collection_future.result(timeout=5)
                with Store(self.config.manager.database_path) as store:
                    view = seg.inspect_run(store, run_id)
                    if view['status'] == 'succeeded':
                        self.assertEqual(view['committed_step'], 250001)
                        self.assertEqual(len(view['segments']), 3)
                        self.assertEqual(store.reservations(), ())
                        for item in view['segments']:
                            attempts = store.job_attempts(item['job_id'])
                            self.assertEqual(len(attempts), 1)
                            self.assertTrue(cp.record(store, attempts[0].attempt_id)['acknowledged'])
                            self.assertEqual(store.result_collection(attempts[0].attempt_id)['status'], 'collected')
                        break
                    if time.monotonic() > deadline:
                        self.fail(str(view))
                time.sleep(.01)
        worker.close()


class EngineeringDriverTests(unittest.TestCase):
    setUp = fixtures.SegmentWorkerTests.setUp

    def test_separate_acceptance_driver_with_fake_engine(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location('engineering_acceptance',
            Path('tools/verification/engineering_continuation_acceptance.py'))
        driver = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(driver)
        self.program.write_text(
            '#!/usr/bin/env python3\n'
            'from pathlib import Path\nimport re,sys\n'
            'if "-help" in sys.argv:\n'
            ' print("hybrid sphere dipole brownian/sphere propel/self lj/cut zero");sys.exit(0)\n'
            'text=Path("in.lmp").read_text()\n'
            'steps,total=map(int,re.findall(r"run (\d+) start 0 stop (\d+)",text)[-1])\n'
            'start=int(Path("checkpoint.restart").read_text()) if "create_atoms" not in text else 0\n'
            'stop=start+steps\n'
            'print("1 by 1 by 1 MPI processor grid")\n'
            'print("using 1 OpenMP thread(s) per MPI task")\n'
            'print("Step Time Atoms")\nprint(start,start*.0001,256)\n'
            'print(stop,stop*.0001,256)\nprint("Loop time")\n'
            'Path("checkpoint.restart").write_text(str(stop))\n'
            'def frames(path,steps):\n'
            ' with Path(path).open("w") as output:\n'
            '  for step in steps:\n'
            '   output.write(f"ITEM: TIMESTEP\\n{step}\\nITEM: NUMBER OF ATOMS\\n256\\n")\n'
            '   output.write("ITEM: BOX BOUNDS pp pp pp\\n0 1\\n0 1\\n-.5 .5\\nITEM: ATOMS id x y ix iy mux muy muz\\n")\n'
            '   for index in range(1,257): output.write(f"{index} 0 0 0 0 1 0 0\\n")\n'
            'points=[int(v) for v in Path("snapshot-steps.txt").read_text().split() if start<int(v)<=stop]\n'
            'frames("trajectory.dump",points)\n'
            'if start==0:\n'
            ' frames("initial.dump",[0]);frames("engineering-smoke-first.dump",[0])\n')
        self.program.chmod(0o700)
        report = dict(scenarios=[])
        with patch('lammps_manager.continuation.fingerprint_build', return_value=BUILD), \
                patch.object(driver, 'fingerprint_build', return_value=BUILD):
            for mode in ('shared', 'staged'):
                driver.run_scenario(self.root / ('acceptance-' + mode), self.program, self.launcher, 1, mode, report)
        self.assertEqual(len(report['scenarios']), 2)
        for scenario in report['scenarios']:
            self.assertTrue(scenario['manager_restarted'])
            self.assertTrue(scenario['retried'])
            self.assertEqual(len(scenario['stages']), 3)
            self.assertEqual(scenario['parent']['status'], 'succeeded')
