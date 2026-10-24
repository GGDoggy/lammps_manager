"""Environment-bound continuation generations, using only local fake state."""
from dataclasses import replace
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4
from unittest.mock import patch
import json
import unittest

import test_segments as fixtures
from lammps_manager import checkpoints as cp, segments as seg
from lammps_manager.config import Segmentation, load_config
from lammps_manager.continuation import Recipe, require_validated
from lammps_manager.store import Store, StoreError


class EnvironmentTests(unittest.TestCase):
    setUp = fixtures.SegmentTests.setUp
    current = fixtures.SegmentTests.current
    assign = fixtures.SegmentTests.assign
    finish = fixtures.SegmentTests.finish
    checkpoint = fixtures.SegmentTests.checkpoint

    def setup_run(self, *, engineering=True):
        self.config = replace(self.config,
            manager=replace(self.config.manager, heartbeat_timeout_seconds=10),
            segmentation=Segmentation(environment_reset_seconds=60))
        self.boot = self.store.generations('worker')[-1]['boot_id']
        seg.observe_environment(self.store, 'worker', self.boot, 'f' * 64, 100)
        request = dict(total_steps=2000, step_max=800, segment_ends=(600, 1400, 2000), run_id='run')
        if engineering:
            request.update(recipe=Recipe('wca-abp-2d-v1', 'statistical'))
        seg.submit_run(self.store, self.config, **request)

    def refresh(self, now):
        seg.refresh(self.store, self.config, now=now)
        return seg.inspect_run(self.store, 'run')

    def test_reconciliation_rechecks_candidates_after_another_writer(self):
        self.setup_run()
        for action in ('reset', 'cancel', 'complete', 'return'):
            with self.subTest(action=action):
                run_id = 'interleave-' + action
                seg.submit_run(self.store, self.config, total_steps=2000,
                    segment_ends=(600, 1400, 2000), step_max=800, run_id=run_id)
                attempt = self.assign(run_id + '.s000000', attempt=action)
                self.store.transition(attempt.attempt_id, 'failed', reason='test failure',
                                      termination_evidence='never launched')
                # Persist an expired timer before candidate discovery.
                self.store._db.execute('UPDATE run_environments SET missing_since=110 WHERE run_id=?', (run_id,))
                original = self.store.scheduling_round
                injected = False

                @contextmanager
                def interleave():
                    nonlocal injected
                    if not injected:
                        injected = True
                        with Store(self.db) as other:
                            if action == 'reset':
                                seg.refresh(other, self.config, now=170)
                            elif action == 'cancel':
                                seg.cancel_run(other, run_id)
                            elif action == 'return':
                                seg.observe_environment(other, 'worker', self.boot, 'f' * 64, 169)
                                seg.refresh(other, self.config, now=169)
                            else:
                                # Model a completion committed by another reconciliation.
                                with other.scheduling_round():
                                    other._db.execute("UPDATE simulation_runs SET status='succeeded' WHERE run_id=?", (run_id,))
                    with original():
                        yield

                with patch.object(self.store, 'scheduling_round', interleave):
                    seg.reconcile_environments(self.store, self.config, now=170)
                view = seg.inspect_run(self.store, run_id)
                self.assertTrue(injected)
                self.assertEqual(view['environment']['generation'], 1 if action == 'reset' else 0)
                self.assertEqual(view['status'], {'reset': 'queued', 'cancel': 'cancelled',
                    'complete': 'succeeded', 'return': 'failed'}[action])
                self.assertEqual(len(view['history']), 1 if action == 'reset' else 0)
                self.assertEqual(len(view['segments']), 1)

    def test_reset_error_does_not_overwrite_concurrent_parent_cancellation(self):
        self.setup_run()
        attempt = self.assign()
        self.store.transition(attempt.attempt_id, 'failed', reason='test failure',
                              termination_evidence='never launched')
        original = self.store.scheduling_round
        calls = 0

        @contextmanager
        def interleave():
            nonlocal calls
            calls += 1
            if calls == 2:
                with Store(self.db) as other:
                    seg.cancel_run(other, 'run', reason='user cancellation')
            with original():
                yield

        with patch.object(self.store, 'scheduling_round', interleave), patch.object(
                seg, '_reconcile_environment', side_effect=OSError('disk full')):
            seg.reconcile_environments(self.store, self.config, now=170)
        view = seg.inspect_run(self.store, 'run')
        self.assertEqual(calls, 2)
        self.assertEqual(view['status'], 'cancelled')
        self.assertEqual(view['reason'], 'user cancellation')
        self.assertEqual(view['environment']['generation'], 0)

    def test_no_build_required_first_assignment_binds_actual_environment(self):
        self.setup_run()
        self.assertIsNone(seg.inspect_run(self.store, 'run')['environment']['fingerprint'])
        before = seg.inspect_run(self.store, 'run')['snapshot']
        attempt = self.assign()
        view = seg.inspect_run(self.store, 'run')
        self.assertEqual(view['environment']['fingerprint'], 'f' * 64)
        self.assertEqual(view['environment']['worker_id'], 'worker')
        self.assertEqual(view['environment']['boot_id'], self.boot)
        self.assertEqual(cp.record(self.store, attempt.attempt_id)['contract']['build_fingerprint'], 'f' * 64)
        self.assertEqual(view['snapshot'], before)
        self.assertIsNone(view['snapshot']['build_fingerprint'])

    def test_measured_recipe_accepts_new_build_without_claiming_measured_evidence(self):
        self.setup_run(engineering=False)
        self.assign()
        scope = require_validated(Recipe('ideal-abp-2d-v1', 'statistical'),
            build_fingerprint='f' * 64, total_steps=2000, segment_ends=(600, 1400, 2000))
        self.assertEqual(scope.evidence, 'not_assessed')

    def test_submission_build_is_provenance_not_an_assignment_constraint(self):
        self.setup_run()
        request = seg.inspect_run(self.store, 'run')['snapshot']
        # A separate run can retain an old caller hint and still bind actual inventory.
        seg.submit_run(self.store, self.config, total_steps=10, build_fingerprint='a' * 64,
            recipe=Recipe('wca-abp-2d-v1', 'statistical'), run_id='hint')
        attempt = self.assign('hint.s000000', attempt='hint-attempt')
        self.assertEqual(cp.record(self.store, attempt.attempt_id)['contract']['build_fingerprint'], 'f' * 64)
        self.assertIsNone(request['build_fingerprint'])

    def test_unavailable_inventory_and_conflicting_assignment_roll_back(self):
        self.setup_run()
        seg.observe_environment(self.store, 'worker', self.boot, None, 101)
        with self.assertRaises(StoreError):
            self.assign()
        self.assertEqual(self.store.reservations(), ())
        self.assertIsNone(seg.environment(self.store, 'run')['fingerprint'])
        seg.observe_environment(self.store, 'worker', self.boot, 'f' * 64, 102)
        with patch.object(cp, 'expect', side_effect=StoreError('rollback')):
            with self.assertRaises(StoreError):
                self.assign()
        self.assertIsNone(seg.environment(self.store, 'run')['fingerprint'])
        self.assertEqual(self.store.reservations(), ())

    def test_retries_and_initial_segment_are_pinned(self):
        self.setup_run()
        attempt = self.assign()
        self.store.transition(attempt.attempt_id, 'failed', reason='test failure', termination_evidence='never launched')
        self.store.retry(attempt.job_id)
        seg.observe_environment(self.store, 'worker', self.boot, 'e' * 64, 101)
        self.assertEqual(seg.worker_blocker(self.store, attempt.job_id, 'worker'), 'bound_continuation_environment')
        with self.assertRaises(StoreError):
            self.assign(attempt='retry')
        self.assertEqual(self.store.reservations(), ())
        seg.observe_environment(self.store, 'worker', self.boot, 'f' * 64, 102)
        self.assertEqual(self.assign(attempt='retry').worker_id, 'worker')

    def test_continuous_disappearance_timer_survives_reopen_and_return_clears_it(self):
        self.setup_run()
        self.finish()
        view = self.refresh(111)
        self.assertEqual(view['environment']['missing_since'], 110)
        self.store.close()
        self.store = Store(self.db)
        self.assertEqual(self.refresh(169)['environment']['generation'], 0)
        seg.observe_environment(self.store, 'worker', self.boot, 'f' * 64, 169)
        self.assertIsNone(self.refresh(169)['environment']['missing_since'])
        self.assertEqual(self.refresh(180)['environment']['missing_since'], 179)
        self.assertEqual(self.refresh(238)['environment']['generation'], 0)
        self.assertEqual(self.refresh(239)['environment']['generation'], 1)

    def test_full_reset_preserves_outputs_snapshots_and_rebinds_another_host(self):
        self.setup_run()
        first = self.finish()
        view = self.refresh(100)
        self.assertEqual(view['committed_steps'], 600)
        snapshot = view['snapshot']
        spec = self.store.specification(first.job_id)
        checkpoint = cp.record(self.store, first.attempt_id)
        old_bytes = (Path(checkpoint['directory']) / 'checkpoint.restart').read_bytes()
        view = self.refresh(170)
        self.assertEqual(view['environment']['generation'], 1)
        self.assertIsNone(view['environment']['fingerprint'])
        self.assertEqual(view['committed_steps'], 0)
        self.assertEqual(view['snapshot'], snapshot)
        self.assertEqual(len(view['history']), 2)
        self.assertEqual(view['segments'][0]['job_id'], 'run.g000001.s000000')
        self.assertEqual(view['segments'][0]['start_step'], 0)
        self.assertIsNone(view['segments'][0]['checkpoint_id'])
        self.assertEqual(self.store.specification(view['segments'][0]['job_id']).input_sha256, spec.input_sha256)
        self.assertEqual(self.store.specification(first.job_id), spec)
        self.assertEqual((Path(checkpoint['directory']) / 'checkpoint.restart').read_bytes(), old_bytes)
        new_boot = str(uuid4())
        self.store.worker_generation('other', new_boot, str(uuid4()))
        seg.observe_environment(self.store, 'other', new_boot, 'd' * 64, 171)
        attempt = self.assign(worker='other')
        self.assertEqual(cp.record(self.store, attempt.attempt_id)['contract']['build_fingerprint'], 'd' * 64)
        self.assertEqual(seg.environment(self.store, 'run')['boot_id'], new_boot)
        self.assertIsNone(seg.worker_blocker(self.store, attempt.job_id, 'other'))
        self.assertEqual(seg.worker_blocker(self.store, first.job_id, 'worker'), 'retired_continuation_generation')
        with self.assertRaisesRegex(StoreError, 'Retired'):
            self.store.retry(first.job_id)
        # Late successful reports/publication cannot advance this new generation.
        seg.advance(self.store, 'run')
        self.assertEqual(seg.inspect_run(self.store, 'run')['committed_steps'], 0)

    def test_expiry_cancels_unresolved_work_and_waits_for_positive_termination(self):
        self.setup_run()
        attempt = self.assign()
        self.store.transition(attempt.attempt_id, 'running')
        view = self.refresh(170)
        self.assertEqual(view['status'], 'resetting')
        self.assertEqual(view['environment']['generation'], 0)
        self.assertTrue(self.store.cancellation(attempt.attempt_id))
        self.assertEqual(len(self.store.reservations()), 1)
        self.store.close()
        self.store = Store(self.db)
        # A return after expiry cannot revoke cancellation or skip the reset barrier.
        seg.observe_environment(self.store, 'worker', self.boot, 'f' * 64, 171)
        self.assertEqual(self.refresh(171)['status'], 'resetting')
        self.store.transition(attempt.attempt_id, 'cancelled', termination_evidence='whole session reaped')
        self.assertEqual(self.refresh(172)['environment']['generation'], 1)
        self.assertEqual(self.store.reservations(), ())

    def test_cancellation_during_reset_prevents_new_generation(self):
        self.setup_run()
        attempt = self.assign()
        self.refresh(170)
        seg.cancel_run(self.store, 'run')
        self.store.transition(attempt.attempt_id, 'cancelled', termination_evidence='never started')
        view = self.refresh(200)
        self.assertEqual(view['status'], 'cancelled')
        self.assertEqual(view['environment']['generation'], 0)
        self.assertEqual(len(view['segments']), 1)

    def test_reset_publication_failure_is_atomic_and_replayable(self):
        self.setup_run()
        self.finish()
        self.refresh(100)
        with patch('lammps_manager.segments._enqueue', side_effect=OSError('disk full')):
            self.assertEqual(self.refresh(170)['status'], 'blocked')
        view = seg.inspect_run(self.store, 'run')
        self.assertEqual(view['environment']['generation'], 0)
        self.assertEqual(view['committed_steps'], 600)
        self.assertEqual(view['history'], [])
        self.assertEqual(self.refresh(170)['environment']['generation'], 1)
        self.assertEqual(self.refresh(171)['environment']['generation'], 1)

    def test_changed_build_and_missing_inventory_start_detection_timer(self):
        for inventory in (None, 'e' * 64):
            with self.subTest(inventory=inventory):
                if self.store.jobs():
                    # Keep each case isolated using a new fixture database.
                    self.store.close()
                    self.root = self.root / 'second'
                    self.root.mkdir()
                    self.config = replace(self.config, storage=replace(self.config.storage, work_root=str(self.root)))
                    self.db = self.root / 'second.sqlite3'
                    self.store = Store(self.db)
                    self.store.worker_generation('worker', self.boot, str(uuid4()))
                self.setup_run()
                self.finish()
                seg.observe_environment(self.store, 'worker', self.boot, inventory, 120)
                view = self.refresh(120)
                self.assertEqual(view['environment']['missing_since'], 120)
                self.assertEqual(self.refresh(179)['environment']['generation'], 0)
                self.assertEqual(self.refresh(180)['environment']['generation'], 1)

    def test_default_timeout_and_strict_yaml_validation(self):
        from dataclasses import asdict
        import yaml
        self.assertEqual(Segmentation().environment_reset_seconds, 3600)
        path = self.root / 'config.yaml'
        data = json.loads(json.dumps(asdict(self.config)))
        for value in (0, -1, True, '3600', float('inf')):
            data['segmentation']['environment_reset_seconds'] = value
            path.write_text(yaml.safe_dump(data))
            with self.assertRaises(ValueError):
                load_config(path, 'manager')
        data['segmentation']['environment_reset_seconds'] = 12.5
        path.write_text(yaml.safe_dump(data))
        self.assertEqual(load_config(path, 'manager').segmentation.environment_reset_seconds, 12.5)

    def test_late_return_does_not_erase_an_already_expired_timer(self):
        self.setup_run()
        self.finish()
        self.refresh(111)
        seg.observe_environment(self.store, 'worker', self.boot, 'f' * 64, 180)
        self.assertEqual(self.refresh(180)['environment']['generation'], 1)

    def test_legacy_chain_migrates_from_first_contract_without_rewriting_snapshot(self):
        self.setup_run()
        attempt = self.assign()
        before = seg.inspect_run(self.store, 'run')['snapshot']
        contract = cp.record(self.store, attempt.attempt_id)['contract']
        self.store._db.execute('DROP TABLE run_environments')
        self.store.close()
        self.store = Store(self.db)
        binding = seg.environment(self.store, 'run')
        self.assertEqual(binding['fingerprint'], contract['build_fingerprint'])
        self.assertEqual(binding['boot_id'], contract['execution_boot_id'])
        self.assertEqual(seg.inspect_run(self.store, 'run')['snapshot'], before)

    def test_rollback_after_generation_inputs_publish_reuses_identical_tree(self):
        self.setup_run()
        self.finish()
        self.refresh(100)
        with patch.object(self.store, 'submit', side_effect=StoreError('database failure')):
            self.assertEqual(self.refresh(170)['status'], 'blocked')
        path = self.root / 'run.g000001.s000000/inputs/in.lmp'
        data = path.read_bytes()
        self.assertEqual(seg.environment(self.store, 'run')['generation'], 0)
        view = self.refresh(171)
        self.assertEqual(view['environment']['generation'], 1)
        self.assertEqual(path.read_bytes(), data)
        self.assertEqual(view['committed_steps'], 0)

    def test_inspection_keeps_diagnostics_when_current_recipe_validation_fails(self):
        self.setup_run()
        self.assign()
        with patch('lammps_manager.segments.require_contract', side_effect=ValueError('recipe changed')):
            view = seg.inspect_run(self.store, 'run')
        self.assertEqual(view['execution_evidence'], 'not_assessed')
        self.assertEqual(view['execution_evidence_error'], 'recipe changed')
        self.assertEqual(view['environment']['fingerprint'], 'f' * 64)

    def test_missing_inventory_reports_reason_without_blocking_ordinary_jobs(self):
        from contextlib import redirect_stderr
        from io import StringIO
        from types import SimpleNamespace
        from lammps_manager.continuation import ContinuationError
        from lammps_manager.worker import Worker
        worker = SimpleNamespace(config=SimpleNamespace(execution=SimpleNamespace(lammps_path='/fake/lmp')))
        errors = StringIO()
        with redirect_stderr(errors), patch('lammps_manager.continuation.fingerprint_build',
                                           side_effect=ContinuationError('missing linked library')):
            self.assertIsNone(Worker._continuation_inventory(worker))
            self.assertIsNone(Worker._continuation_inventory(worker))
        self.assertEqual(errors.getvalue().count('missing linked library'), 1)
        self.assertEqual(worker.continuation_inventory_error, 'missing linked library')

    def test_unassigned_runs_do_not_expire_and_repeated_resets_use_new_paths(self):
        self.setup_run()
        view = self.refresh(10000)
        self.assertEqual(view['environment']['generation'], 0)
        self.assertIsNone(view['environment']['missing_since'])
        attempt = self.assign()
        self.store.transition(attempt.attempt_id, 'failed', reason='test failure', termination_evidence='no process')
        self.assertEqual(self.refresh(170)['environment']['generation'], 1)
        seg.observe_environment(self.store, 'worker', self.boot, 'f' * 64, 171)
        attempt = self.assign()
        self.store.transition(attempt.attempt_id, 'failed', reason='test failure', termination_evidence='no process')
        seg.observe_environment(self.store, 'worker', self.boot, None, 180)
        self.refresh(180)
        view = self.refresh(240)
        self.assertEqual(view['environment']['generation'], 2)
        self.assertEqual(view['segments'][0]['job_id'], 'run.g000002.s000000')
        self.assertEqual([item['generation'] for item in view['history']], [1, 0])
        self.assertEqual(len(self.store.jobs()), 3)


class EnvironmentWorkerTests(unittest.TestCase):
    """Real local polling and subprocess paths, with fake simulation executables."""
    setUp = fixtures.SegmentWorkerTests.setUp

    def execute_reset(self, mode):
        import time
        from lammps_manager.worker import Worker
        from lammps_manager.config import WorkerConfig, Execution
        self.config = replace(self.config, storage=replace(self.config.storage, mode=mode),
            segmentation=Segmentation(environment_reset_seconds=1))
        self.service.config = self.config
        workers = []
        for index, capacity in enumerate(self.capacities):
            root = self.work if mode == 'shared' else self.root / f'worker-{index}'
            root.mkdir(exist_ok=True)
            worker = Worker(WorkerConfig(capacity, self.endpoint, str(root),
                Execution(str(self.program), str(self.launcher)), self.tokens[capacity.id], storage_mode=mode),
                self.root / f'journal-reset-{index}')
            workers.append(worker)
            self.addCleanup(worker.close)
        with Store(self.config.manager.database_path) as store:
            seg.submit_run(store, self.config, total_steps=2000, step_max=800,
                segment_ends=(600, 1400, 2000), run_id='pipeline')
        deadline = time.monotonic() + 15
        first = workers[0]
        with patch('lammps_manager.continuation.fingerprint_build', return_value='a' * 64):
            while not first.futures:
                first.tick()
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.01)
            # Pause before reporting completion so the successor remains queued.
            with Store(self.config.manager.database_path) as store:
                store.set_worker('a', paused=True)
            while True:
                for future in first.futures.values():
                    future.result(timeout=5)
                first.tick()
                if first.collection_future:
                    first.collection_future.result(timeout=5)
                with Store(self.config.manager.database_path) as store:
                    view = seg.inspect_run(store, 'pipeline')
                if view['committed_steps'] == 600:
                    break
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.01)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.reservations(), ())
            previous = cp.record(store, store.job_attempts(view['segments'][0]['job_id'])[0].attempt_id)
            now = time.time()
            seg.observe_environment(store, 'a', first.boot_id, None, now - 2)
            seg.refresh(store, self.config, now=now - 2)
            seg.refresh(store, self.config, now=now)
            view = seg.inspect_run(store, 'pipeline')
            self.assertEqual(view['committed_steps'], 0)
            self.assertEqual(view['environment']['generation'], 1)
            self.assertIsNone(view['environment']['fingerprint'])
        second = workers[1]
        with patch('lammps_manager.continuation.fingerprint_build', return_value='b' * 64):
            while True:
                second.tick()
                for future in second.futures.values():
                    future.result(timeout=5)
                if second.collection_future:
                    second.collection_future.result(timeout=5)
                with Store(self.config.manager.database_path) as store:
                    view = seg.inspect_run(store, 'pipeline')
                if view['status'] == 'succeeded':
                    break
                self.assertLess(time.monotonic(), deadline, str(view))
                time.sleep(.01)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(view['committed_steps'], 2000)
            self.assertEqual(view['environment']['worker_id'], 'b')
            self.assertEqual(view['environment']['fingerprint'], 'b' * 64)
            self.assertEqual(view['execution_evidence'], 'not_assessed')
            self.assertEqual(store.reservations(), ())
            for item in view['segments']:
                attempts = store.job_attempts(item['job_id'])
                self.assertEqual(len(attempts), 1)
                self.assertEqual(attempts[0].worker_id, 'b')
                self.assertEqual(store.result_collection(attempts[0].attempt_id)['status'], 'collected')
            self.assertEqual((Path(previous['directory']) / 'checkpoint.restart').read_text(), '600')
        # Old-worker result replay cannot count old progress in the current generation.
        with patch('lammps_manager.continuation.fingerprint_build', return_value='a' * 64):
            first.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(seg.inspect_run(store, 'pipeline')['status'], 'succeeded')
            self.assertEqual(seg.inspect_run(store, 'pipeline')['environment']['generation'], 1)

    def test_shared_generation_restart_and_reassignment(self):
        self.execute_reset('shared')

    def test_staged_generation_restart_and_reassignment(self):
        self.execute_reset('staged')
