"""Sequential run state/queue tests with local byte stand-ins, never LAMMPS."""
from dataclasses import asdict, replace
from io import BytesIO
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from lammps_manager import checkpoints as cp, segments as seg
from lammps_manager.config import (ManagerConfig, ManagerSettings, Storage, WorkerCapacity,
    Scheduler, Segmentation, load_config)
from lammps_manager.continuation import ContinuationError
from lammps_manager.models import Resources, JobSpecification
from lammps_manager.store import Store, StoreError

BUILD = 'aa8a1af68131f97d13fd4959031c557681b46c9eee23dddef26ca75c7eae0695'
LIMITS = dict(max_files=10, max_file_bytes=1024, max_total_bytes=4096)


class SegmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / 'manager.sqlite3'
        self.store = Store(self.db)
        self.addCleanup(lambda: self.store.close())
        from uuid import uuid4
        self.store.worker_generation('worker', Path('/proc/sys/kernel/random/boot_id').read_text().strip(), str(uuid4()))
        import time
        seg.observe_environment(self.store, 'worker', Path('/proc/sys/kernel/random/boot_id').read_text().strip(), BUILD, time.time())
        self.config = ManagerConfig(ManagerSettings(str(self.db)), Storage(str(self.root)),
            (WorkerCapacity('worker', 4, 32, 4), WorkerCapacity('other', 4, 32, 4)),
            Resources(1, 1, max_retries=1), Scheduler(default_priority=7))

    def submit(self, **changes):
        return seg.submit_run(self.store, self.config, **dict(
            total_steps=2000, step_max=800, segment_ends=(600, 1400, 2000),
            build_fingerprint=BUILD, run_id='run', **changes))

    def current(self):
        return seg.inspect_run(self.store, 'run')['segments'][-1]

    def assign(self, job=None, worker='worker', attempt=None):
        job = job or self.current()['job_id']
        attempt = attempt or job + '.attempt'
        return self.store.assign(job, attempt, worker,
            str(Path(self.store.specification(job).work_directory) / attempt), default_priority=7)

    def finish(self, job=None, *, checkpoint=True, results=True, attempt=None):
        job = job or self.current()['job_id']
        previous = self.store.job_attempts(job)
        execution = self.store.attempt(attempt) if attempt else (
            previous[-1] if previous else self.assign(job))
        if execution.state == 'assigned':
            self.store.transition(execution.attempt_id, 'running')
        self.store.transition(execution.attempt_id, 'succeeded', exit_code=0, termination_evidence='test process reaped')
        if checkpoint:
            self.checkpoint(execution.attempt_id)
        if results:
            self.store.set_result_collection(execution.attempt_id, 'collected', dict(files=[], omitted=[]))
        return execution

    def checkpoint(self, attempt):
        item = cp.record(self.store, attempt)
        data = b'state at ' + str(item['contract']['global_step']).encode()
        manifest = dict(contract=item['contract'], files=[dict(path='checkpoint.restart', size=len(data),
            sha256=hashlib.sha256(data).hexdigest())])
        cp.operation(self.store, 'worker', attempt, 'manifest', LIMITS, payload=manifest)
        cp.operation(self.store, 'worker', attempt, '0', LIMITS, stream=BytesIO(data), length=len(data))
        return cp.operation(self.store, 'worker', attempt, 'commit', LIMITS)

    def test_interval_arithmetic_and_default(self):
        for count in (1, 99999, 100000, 100001, 200000, 250001):
            with self.subTest(count=count):
                ranges = seg.plan_steps(count)
                self.assertEqual(ranges[0][0], 0)
                self.assertEqual(ranges[-1][1], count)
                self.assertEqual(sum(b - a for a, b in ranges), count)
                self.assertTrue(all(0 < b - a <= 100000 for a, b in ranges))
                self.assertTrue(all(a[1] == b[0] for a, b in zip(ranges, ranges[1:])))
        self.assertEqual(seg.plan_steps(250001), ((0, 100000), (100000, 200000), (200000, 250001)))
        self.assertEqual(seg.plan_steps(7, 3, 10), ((10, 13), (13, 16), (16, 17)))
        for args in ((True,), (0,), (-1,), (2, 0), (2, True), (2, 1, -1), (2**63,), (100001, 1)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                seg.plan_steps(*args)

    def test_scientific_gate_rejects_unmeasured_default_plan_and_layout(self):
        for changes in (dict(total_steps=250001, segment_ends=None, step_max=100000),
                        dict(segment_ends=None), dict(build_fingerprint='unvalidated'),
                        dict(start_step=1), dict(resource_overrides={'mpi_ranks': 2})):
            values = dict(total_steps=2000, step_max=800, segment_ends=(600, 1400, 2000), build_fingerprint=BUILD)
            values.update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                seg.submit_run(self.store, self.config, **values)
        self.assertEqual(self.store.jobs(), ())
        self.assertEqual([p for p in self.root.glob('*.s*') if p.is_dir()], [])

    def test_default_maximum_runs_measured_reference_as_one_segment(self):
        run = seg.submit_run(self.store, self.config, total_steps=2000,
                             build_fingerprint=BUILD, run_id='run')
        view = seg.inspect_run(self.store, run)
        self.assertEqual(len(view['segments']), 1)
        self.assertEqual(view['snapshot']['step_max'], 100000)
        self.finish()
        seg.advance(self.store, run)
        view = seg.inspect_run(self.store, run)
        self.assertEqual(view['status'], 'succeeded')
        self.assertEqual(view['committed_steps'], 2000)
        self.assertEqual(len(view['segments']), 1)

    def test_three_segments_commit_once_and_inherit_first_effective_priority(self):
        self.submit()
        snapshot = seg.inspect_run(self.store, 'run')['snapshot']
        first = self.current()['job_id']
        self.store.set_priority(first, 12)
        for index, end in enumerate((600, 1400, 2000)):
            job = self.current()['job_id']
            attempt = self.assign(job)
            self.assertEqual(self.store.assignment_priority(attempt.attempt_id)['priority'], 12)
            self.finish(job)
            seg.advance(self.store, 'run')
            seg.advance(self.store, 'run')
            self.store.close()
            self.store = Store(self.db)
            view = seg.inspect_run(self.store, 'run')
            self.assertEqual(view['committed_steps'], end)
            self.assertEqual(view['first_priority'], 12)
            self.assertEqual(view['snapshot'], snapshot)
            self.assertEqual(len(view['segments']), min(index + 2, 3))
        self.assertEqual(view['status'], 'succeeded')
        self.assertEqual(len(self.store.jobs()), 3)
        self.assertEqual(self.store.reservations(), ())
        self.assertTrue(all(item['completed'] for item in view['segments']))
        self.assertEqual(len([e for e in view['events'] if 'Committed' in e['description']]), 3)

    def test_wait_for_checkpoint_and_staged_results_without_reexecution(self):
        self.config = replace(self.config, storage=replace(self.config.storage, mode='staged'))
        self.submit()
        attempt = self.finish(checkpoint=False, results=False)
        seg.advance(self.store, 'run')
        self.assertIn('checkpoint', seg.inspect_run(self.store, 'run')['reason'])
        self.checkpoint(attempt.attempt_id)
        seg.advance(self.store, 'run')
        view = seg.inspect_run(self.store, 'run')
        self.assertIn('results', view['reason'])
        self.assertEqual(view['committed_steps'], 0)
        self.assertEqual(len(self.store.jobs()), 1)
        self.store.set_result_collection(attempt.attempt_id, 'failed', reason='upload interrupted')
        seg.refresh(self.store)
        self.assertEqual(len(self.store.job_attempts(attempt.job_id)), 1)
        self.store.set_result_collection(attempt.attempt_id, 'collected', dict(files=[], omitted=[]))
        seg.refresh(self.store)
        self.assertEqual(len(self.store.jobs()), 2)
        self.assertEqual(seg.inspect_run(self.store, 'run')['committed_steps'], 600)

    def test_shared_parent_waits_for_durable_result_publication(self):
        self.submit()
        attempt = self.finish(results=False)
        seg.advance(self.store, 'run')
        view = seg.inspect_run(self.store, 'run')
        self.assertEqual(view['committed_steps'], 0)
        self.assertIn('durable segment results', view['reason'])
        self.assertEqual(len(self.store.jobs()), 1)
        self.store.set_result_collection(attempt.attempt_id, 'collected', dict(files=[], omitted=[]))
        seg.advance(self.store, 'run')
        self.assertEqual(seg.inspect_run(self.store, 'run')['committed_steps'], 600)

    def test_enqueue_crash_rolls_back_progress_and_successor_then_replays_files(self):
        self.submit()
        self.finish()
        with patch.object(cp, 'pin', side_effect=OSError('injected after next job insert')):
            with self.assertRaises(OSError):
                seg.advance(self.store, 'run')
        self.assertEqual(seg.inspect_run(self.store, 'run')['committed_steps'], 0)
        self.assertEqual(len(self.store.jobs()), 1)
        self.assertTrue((self.root / 'run.s000001/inputs/checkpoint.restart').exists())
        self.store.close()
        self.store = Store(self.db)
        seg.refresh(self.store)
        self.assertEqual(len(self.store.jobs()), 2)
        self.assertEqual(seg.inspect_run(self.store, 'run')['committed_steps'], 600)
        seg.refresh(self.store)
        self.assertEqual(len(self.store.jobs()), 2)

    def test_corrupt_checkpoint_blocks_successor_and_preserves_progress(self):
        self.submit()
        attempt = self.finish()
        checkpoint = cp.record(self.store, attempt.attempt_id)
        path = Path(checkpoint['directory']) / 'checkpoint.restart'
        original = path.read_bytes()
        path.write_bytes(b'corrupt')
        seg.refresh(self.store)
        view = seg.inspect_run(self.store, 'run')
        self.assertEqual(view['status'], 'blocked')
        self.assertIn('Checkpoint/enqueue', view['reason'])
        self.assertEqual(view['committed_steps'], 0)
        self.assertEqual(len(self.store.jobs()), 1)
        path.write_bytes(original)
        seg.refresh(self.store)
        self.assertEqual(len(self.store.jobs()), 2)

    def test_fresh_fifo_and_priority_override_do_not_mutate_running_segment(self):
        self.submit()
        self.assign()
        seg.set_priorities(self.store, 'run', continuation=-4)
        first = self.current()['job_id']
        first_spec = self.store.specification(first)
        competitor = JobSpecification('competitor', str(self.root / 'competitor'), 'in.lmp', '0' * 64, Resources(1, 1))
        self.store.submit(competitor)
        self.finish()
        seg.advance(self.store, 'run')
        second = self.current()['job_id']
        self.assertEqual(self.store.scheduling_metadata(second)['priority'], -4)
        self.assertEqual(self.store.queued_jobs(7)[0]['job_id'], 'competitor')
        seg.set_priorities(self.store, 'run', continuation=7)
        queued = self.store.queued_jobs(7)
        self.assertEqual([item['job_id'] for item in queued], ['competitor', second])
        self.assign(second)
        seg.set_priorities(self.store, 'run', continuation=20)
        self.assertEqual(self.store.assignment_priority(self.store.job_attempts(second)[-1].attempt_id)['priority'], 7)
        self.assertEqual(self.store.specification(first), first_spec)
        self.finish(second)
        seg.advance(self.store, 'run')
        self.assertEqual(self.store.scheduling_metadata(self.current()['job_id'])['priority'], 20)
        with self.assertRaises(StoreError):
            seg.set_priorities(self.store, 'run', initial=99)

    def test_cancellation_before_assignment_and_after_checkpoint_blocks_future_segments(self):
        self.submit()
        seg.cancel_run(self.store, 'run')
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'cancelled')
        with self.assertRaises(StoreError):
            self.assign()
        self.assertEqual(len(self.store.jobs()), 1)
        # Use a new run; do not resurrect cancelled IDs.
        seg.submit_run(self.store, self.config, total_steps=2000, step_max=800,
            segment_ends=(600, 1400, 2000), build_fingerprint=BUILD, run_id='run2')
        job = seg.inspect_run(self.store, 'run2')['segments'][0]['job_id']
        self.finish(job)
        seg.cancel_run(self.store, 'run2')
        seg.refresh(self.store)
        self.assertEqual(len(seg.inspect_run(self.store, 'run2')['segments']), 1)
        self.assertEqual(seg.inspect_run(self.store, 'run2')['status'], 'cancelled')

    def test_active_cancel_retains_reservation_until_group_termination(self):
        self.submit()
        attempt = self.assign()
        self.store.transition(attempt.attempt_id, 'running')
        seg.cancel_run(self.store, 'run')
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'cancelling')
        self.assertTrue(self.store.cancellation(attempt.attempt_id))
        self.assertEqual(len(self.store.reservations()), 1)
        self.store.transition(attempt.attempt_id, 'cancelled', reason='Parent run cancellation requested',
                              exit_code=-15, termination_evidence='test group gone')
        seg.refresh(self.store)
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'cancelled')
        self.assertEqual(len(self.store.jobs()), 1)

    def test_retry_resumes_same_segment_input_and_obsolete_upload_is_rejected(self):
        self.submit()
        self.finish()
        seg.advance(self.store, 'run')
        job = self.current()['job_id']
        before = self.store.specification(job)
        attempt = self.assign(job)
        self.store.transition(attempt.attempt_id, 'running')
        self.store.transition(attempt.attempt_id, 'failed', reason='test infrastructure failure',
                              termination_evidence='test group gone')
        seg.refresh(self.store)
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'failed')
        self.store.retry(job)
        seg.refresh(self.store)
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'queued')
        replacement = self.assign(job, attempt='replacement')
        self.assertEqual(self.store.specification(job), before)
        self.finish(job, attempt=replacement.attempt_id)
        seg.refresh(self.store)
        self.assertEqual(seg.inspect_run(self.store, 'run')['committed_steps'], 1400)
        self.assertEqual(len(seg.inspect_run(self.store, 'run')['segments']), 3)
        self.assertEqual(len(self.store.job_attempts(job)), 2)
        with self.assertRaises(StoreError):
            self.store.retry(job)

    def test_cross_worker_gate_and_unique_run_replay(self):
        self.submit()
        self.submit()
        self.assertEqual(len(self.store.jobs()), 1)
        with self.assertRaises(StoreError):
            self.submit(resource_overrides={'memory_gib': 9})
        self.finish()
        seg.advance(self.store, 'run')
        job = self.current()['job_id']
        self.assertEqual(seg.worker_blocker(self.store, job, 'other'), 'continuation_environment_unavailable')
        with self.assertRaisesRegex(StoreError, 'environment'):
            self.assign(job, worker='other')
        self.assertEqual(self.store.reservations(), ())
        self.assertEqual(self.store.job_attempts(job), ())

    def test_config_default_strict_positive_and_snapshot_frozen(self):
        import yaml
        self.assertEqual(self.config.segmentation.step_max, 100000)
        data = yaml.safe_load(Path('examples/manager.yaml').read_text())
        path = self.root / 'manager.yaml'
        for value in (0, -1, True, 1.5):
            data['segmentation'] = dict(step_max=value)
            path.write_text(yaml.safe_dump(data))
            with self.subTest(value=value), self.assertRaises(ValueError):
                load_config(path, 'manager')
        data['segmentation'] = dict(step_max=800)
        path.write_text(yaml.safe_dump(data))
        self.assertEqual(load_config(path, 'manager').segmentation.step_max, 800)
        self.submit()
        self.config = replace(self.config, segmentation=Segmentation(100000))
        self.assertEqual(seg.inspect_run(self.store, 'run')['snapshot']['step_max'], 800)

    def test_unknown_attempt_retains_resources_and_no_successor(self):
        self.submit()
        attempt = self.assign()
        self.store.transition(attempt.attempt_id, 'running')
        self.store.transition(attempt.attempt_id, 'unknown', reason='heartbeat lost')
        seg.refresh(self.store)
        self.assertEqual(len(self.store.reservations()), 1)
        self.assertEqual(len(self.store.jobs()), 1)
        self.assertEqual(seg.inspect_run(self.store, 'run')['committed_steps'], 0)
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'unknown')
        with self.assertRaises(StoreError):
            self.assign(attempt='duplicate')

    def test_failed_checkpoint_diagnostic_and_final_results_gate(self):
        self.config = replace(self.config, storage=replace(self.config.storage, mode='staged'))
        seg.submit_run(self.store, self.config, total_steps=2000, build_fingerprint=BUILD, run_id='run')
        attempt = self.finish(checkpoint=False, results=False)
        cp.set_error(self.store, attempt.attempt_id, 'Required restart is missing')
        seg.refresh(self.store)
        self.assertIn('restart is missing', seg.inspect_run(self.store, 'run')['reason'])
        from lammps_manager.monitoring import job_view
        view = job_view(self.store, attempt.job_id, self.config, 0)['continuation']
        self.assertEqual(view['committed_steps'], 0)
        self.assertEqual(view['segment_stop'], 2000)
        self.assertIn('restart is missing', view['checkpoint_error'])
        self.assertFalse(view['next_segment_eligible'])
        self.checkpoint(attempt.attempt_id)
        seg.refresh(self.store)
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'blocked')
        self.assertEqual(seg.inspect_run(self.store, 'run')['committed_steps'], 0)
        self.store.set_result_collection(attempt.attempt_id, 'collected', dict(files=[], omitted=[]))
        seg.refresh(self.store)
        self.assertEqual(seg.inspect_run(self.store, 'run')['status'], 'succeeded')

    def test_same_worker_id_cannot_replace_pinned_boot(self):
        from uuid import uuid4
        boot = str(uuid4())
        self.store.worker_generation('worker', boot, str(uuid4()))
        seg.observe_environment(self.store, 'worker', boot, BUILD, 100)
        self.submit()
        first = self.finish()
        seg.refresh(self.store)
        job = self.current()['job_id']
        contract = cp.record(self.store, first.attempt_id)['contract']
        self.store.worker_generation('worker', boot, str(uuid4()))
        self.assertIsNone(seg.worker_blocker(self.store, job, 'worker'))
        # A new session is safe on the pinned boot, but a reused ID on another
        # boot must not obtain a newly pinned successor contract.
        self.store.worker_generation('worker', str(uuid4()), str(uuid4()))
        self.assertEqual(seg.worker_blocker(self.store, job, 'worker'),
                         'continuation_environment_unavailable')
        with self.assertRaisesRegex(StoreError, 'environment'):
            self.assign(job)
        self.assertEqual(self.store.job_attempts(job), ())
        self.assertEqual(self.store.reservations(), ())
        self.assertEqual(cp.record(self.store, first.attempt_id)['contract'], contract)

    def test_bound_worker_identity_cannot_be_reassigned_even_on_same_boot(self):
        from uuid import uuid4
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.store.worker_generation('other', boot, str(uuid4()))
        seg.observe_environment(self.store, 'other', boot, BUILD, 100)
        self.submit()
        self.finish()
        seg.refresh(self.store)
        job = self.current()['job_id']
        self.assertEqual(seg.worker_blocker(self.store, job, 'other'), 'bound_continuation_environment')
        with self.assertRaisesRegex(StoreError, 'bound continuation environment'):
            self.assign(job, worker='other')
        self.assertEqual(self.store.reservations(), ())
        seg.cancel_run(self.store, 'run')
        with self.assertRaisesRegex(StoreError, 'Parent run cancelled'):
            self.store.retry(job)


    def test_cli_submission_inspection_and_priority_controls(self):
        from contextlib import redirect_stdout
        from io import StringIO
        import yaml
        from lammps_manager.cli import main
        path = self.root / 'manager.yaml'
        path.write_text(yaml.safe_dump(json.loads(json.dumps(asdict(self.config)))))
        request = self.root / 'run.json'
        request.write_text(json.dumps(dict(total_steps=2000, build_fingerprint=BUILD, run_id='cli')))
        stream = StringIO()
        with redirect_stdout(stream):
            self.assertEqual(main(['submit-run', '--config', str(path), str(request)]), 0)
        self.assertEqual(json.loads(stream.getvalue())['snapshot']['step_max'], 100000)
        stream = StringIO()
        with redirect_stdout(stream):
            self.assertEqual(main(['run-priority', '--config', str(path), 'cli', '--initial', '20', '--continuation', '-2']), 0)
        self.assertEqual(json.loads(stream.getvalue())['initial_override'], 20)
        stream = StringIO()
        with redirect_stdout(stream):
            self.assertEqual(main(['cancel-run', '--config', str(path), 'cli']), 0)
        self.assertEqual(json.loads(stream.getvalue())['status'], 'cancelled')


class SegmentWorkerTests(unittest.TestCase):
    """Actual polling/executor/transfer path with fake MPI/LAMMPS processes."""
    def setUp(self):
        from test_workers import WorkerTests
        WorkerTests.setUp(self)
        self.program.write_text(
            '#!/usr/bin/env python3\n'
            'from pathlib import Path\nimport re\n'
            'text=Path("in.lmp").read_text()\n'
            'steps,total=map(int,re.search(r"run (\\d+) start 0 stop (\\d+)",text).groups())\n'
            'start=int(Path("checkpoint.restart").read_text()) if "read_restart" in text else 0\n'
            'stop=start+steps\n'
            'print("Step Time Atoms")\n'
            'print(start,start*.005,1024)\n'
            'print(stop,stop*.005,1024)\n'
            'print("Loop time")\n'
            'Path("checkpoint.restart").write_text(str(stop))\n'
            'with Path("invocations").open("a") as f: f.write("one launch\\n")\n')
        self.program.chmod(0o700)

    def execute_chain(self, mode, *, lose_ack=False):
        import time
        from lammps_manager.worker import Worker
        from lammps_manager.config import WorkerConfig, Execution
        self.config = replace(self.config, storage=replace(self.config.storage, mode=mode))
        self.service.config = self.config
        work_root = self.work if mode == 'shared' else self.root / 'worker-files'
        work_root.mkdir(exist_ok=True)
        worker = Worker(WorkerConfig(self.capacities[0], self.endpoint, str(work_root),
            Execution(str(self.program), str(self.launcher)), self.tokens['a'], storage_mode=mode),
            self.root / 'journal')
        self.addCleanup(worker.close)
        with Store(self.config.manager.database_path) as store:
            seg.submit_run(store, self.config, total_steps=2000, step_max=800,
                segment_ends=(600, 1400, 2000), build_fingerprint=BUILD, run_id='pipeline')
        original = worker._result_request
        lost = []
        def lose_reply(attempt, action, *args, **kwargs):
            reply = original(attempt, action, *args, **kwargs)
            if kwargs.get('checkpoint') and action == 'commit' and not lost:
                lost.append(attempt)
                raise OSError('injected checkpoint acknowledgement loss')
            return reply
        deadline = time.monotonic() + 15
        with patch('lammps_manager.continuation.fingerprint_build', return_value=BUILD), \
                patch.object(worker, '_result_request', side_effect=lose_reply if lose_ack else original):
            while True:
                worker.tick()
                for future in tuple(worker.futures.values()):
                    future.result(timeout=5)
                if worker.collection_future is not None:
                    worker.collection_future.result(timeout=5)
                with Store(self.config.manager.database_path) as store:
                    view = seg.inspect_run(store, 'pipeline')
                    if view['status'] == 'succeeded':
                        break
                    if time.monotonic() > deadline:
                        self.fail(str(view))
                time.sleep(.01)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(view['committed_steps'], 2000)
            self.assertEqual(len(view['segments']), 3)
            for item in view['segments']:
                attempts = store.job_attempts(item['job_id'])
                self.assertEqual(len(attempts), 1)
                self.assertEqual(attempts[0].state, 'succeeded')
                self.assertTrue(cp.record(store, attempts[0].attempt_id)['acknowledged'])
            self.assertEqual(store.reservations(), ())
        with Store(self.config.manager.database_path) as store:
            for item in view['segments']:
                attempt = store.job_attempts(item['job_id'])[0]
                self.assertEqual((Path(attempt.directory) / 'results/work/invocations').read_text(), 'one launch\n')
        if lose_ack:
            self.assertEqual(len(lost), 1)
        return worker, view

    def test_shared_pipeline_and_lost_checkpoint_ack(self):
        self.execute_chain('shared', lose_ack=True)

    def test_staged_pipeline_collects_all_results_before_parent_success(self):
        self.execute_chain('staged', lose_ack=True)
        with Store(self.config.manager.database_path) as store:
            for item in seg.inspect_run(store, 'pipeline')['segments']:
                attempt = store.job_attempts(item['job_id'])[0]
                self.assertEqual(store.result_collection(attempt.attempt_id)['status'], 'collected')
                self.assertEqual((Path(attempt.directory) / 'results/work/invocations').read_text(), 'one launch\n')

    def test_parent_cancel_reaches_actual_owned_process_group(self):
        import time
        from test_workers import WorkerTests
        from lammps_manager.worker import Worker
        from lammps_manager.recovery import session_members
        self.program.write_text('#!/usr/bin/env python3\n'
            'from pathlib import Path\nimport subprocess,sys,time\n'
            'subprocess.Popen([sys.executable,"-c","import time;time.sleep(60)"])\n'
            'Path("started").write_text("owned descendant")\n'
            'time.sleep(60)\n')
        self.program.chmod(0o700)
        worker = Worker(WorkerTests.worker_config(self), self.root / 'cancel-journal')
        self.addCleanup(worker.close)
        with Store(self.config.manager.database_path) as store:
            seg.submit_run(store, self.config, total_steps=2000, step_max=800,
                segment_ends=(600, 1400, 2000), build_fingerprint=BUILD, run_id='cancel-pipeline')
        with patch('lammps_manager.continuation.fingerprint_build', return_value=BUILD):
            deadline = time.monotonic() + 10
            while not list(self.work.rglob('started')):
                worker.tick()
                if time.monotonic() > deadline:
                    self.fail('owned process did not start')
                time.sleep(.01)
            with Store(self.config.manager.database_path) as store:
                seg.cancel_run(store, 'cancel-pipeline')
                self.assertEqual(len(store.reservations()), 1)
            while True:
                worker.tick()
                with Store(self.config.manager.database_path) as store:
                    view = seg.inspect_run(store, 'cancel-pipeline')
                    if view['status'] == 'cancelled':
                        attempt = store.job_attempts(view['segments'][0]['job_id'])[0]
                        self.assertEqual(store.reservations(), ())
                        break
                if time.monotonic() > deadline:
                    self.fail('parent did not confirm cancellation')
                time.sleep(.01)
        metadata = json.loads((Path(attempt.directory) / 'process.json').read_text())
        self.assertEqual(session_members(metadata['pid']), ())
        self.assertEqual(len(view['segments']), 1)
        self.assertEqual(view['committed_steps'], 0)

    def test_worker_and_manager_restart_after_lost_checkpoint_ack(self):
        import threading
        import time
        from test_workers import WorkerTests
        from lammps_manager.worker import Worker
        from lammps_manager.manager import ManagerService, make_server
        worker_config = WorkerTests.worker_config(self)
        journal_path = self.root / 'restart-journal'
        worker = Worker(worker_config, journal_path)
        self.addCleanup(worker.close)
        with Store(self.config.manager.database_path) as store:
            seg.submit_run(store, self.config, total_steps=2000, step_max=800,
                segment_ends=(600, 1400, 2000), build_fingerprint=BUILD, run_id='restart-pipeline')
        original = worker._result_request
        lost = []
        def lose_reply(attempt, action, *args, **kwargs):
            reply = original(attempt, action, *args, **kwargs)
            if kwargs.get('checkpoint') and action == 'commit' and not lost:
                lost.append(attempt)
                raise OSError('injected acknowledgement loss before worker restart')
            return reply
        deadline = time.monotonic() + 15
        with patch('lammps_manager.continuation.fingerprint_build', return_value=BUILD):
            with patch.object(worker, '_result_request', side_effect=lose_reply):
                while not lost:
                    worker.tick()
                    for future in tuple(worker.futures.values()):
                        future.result(timeout=5)
                    if worker.collection_future:
                        worker.collection_future.result(timeout=5)
                    if time.monotonic() > deadline:
                        self.fail('checkpoint loss was not injected')
                    time.sleep(.01)
            with Store(worker.database) as journal:
                self.assertFalse(cp.record(journal, lost[0])['acknowledged'])
            worker.close()
            # Keep the endpoint and durable database, replace the real HTTP
            # service and all its volatile caches/challenge/scheduling state.
            port = self.server.server_port
            self.server.shutdown()
            self.server.server_close()
            self.service.close()
            self.service = ManagerService(self.config, self.tokens)
            self.addCleanup(self.service.close)
            self.server = make_server(self.service, '127.0.0.1', port)
            thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01})
            thread.start()
            def stop_restart_server():
                self.server.shutdown()
                thread.join(5)
                self.server.server_close()
            self.addCleanup(stop_restart_server)
            worker = Worker(worker_config, journal_path)
            self.addCleanup(worker.close)
            while True:
                worker.tick()
                for future in tuple(worker.futures.values()):
                    future.result(timeout=5)
                if worker.collection_future:
                    worker.collection_future.result(timeout=5)
                with Store(self.config.manager.database_path) as store:
                    view = seg.inspect_run(store, 'restart-pipeline')
                    if view['status'] == 'succeeded':
                        self.assertEqual(view['committed_steps'], 2000)
                        self.assertEqual(len(view['segments']), 3)
                        self.assertTrue(all(len(store.job_attempts(row['job_id'])) == 1 for row in view['segments']))
                        self.assertEqual(store.reservations(), ())
                        break
                if time.monotonic() > deadline:
                    self.fail('restarted chain did not finish: ' + str(view))
                time.sleep(.01)


if __name__ == '__main__':
    unittest.main()
