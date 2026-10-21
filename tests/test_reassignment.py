"""Prelaunch fences, replay, cancellation and transactional admission."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest

from lammps_manager.models import JobSpecification, Resources
from lammps_manager.store import Store


class ReassignmentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'state.sqlite'
        self.store = Store(self.path)
        self.addCleanup(self.store.close)
        self.store.submit(JobSpecification('j', '/work/j', 'in', 'a' * 64, Resources()))
        self.store.assign('j', 'a', 'w', '/work/j/a')

    def test_replay_restart_stale_delivery_and_new_identity(self):
        self.assertTrue(self.store.reject_launch('a', 'headroom', 'busy', 30))
        self.store.return_rejected('a')
        with Store(self.path) as restarted:
            self.assertFalse(restarted.claim_launch('a'))
            restarted.assign('j', 'b', 'other', '/work/j/b')
            self.assertTrue(restarted.reject_launch('a', 'headroom', 'busy', 30))
            restarted.return_rejected('a')
            self.assertEqual(restarted.job_state('j'), 'assigned')
            self.assertEqual([r.attempt_id for r in restarted.reservations()], ['b'])
            self.assertEqual(restarted.specification('j').resources.max_retries, 0)
            self.assertEqual(restarted.rejection_blocker('j', 'w', 29), 'prelaunch_rejection_cooldown')
            self.assertIsNone(restarted.rejection_blocker('j', 'w', 30))

    def test_claim_rejection_race_has_exactly_one_winner(self):
        def claim():
            with Store(self.path) as store:
                return store.claim_launch('a')
        def reject():
            with Store(self.path) as store:
                return store.reject_launch('a', 'not_ready', 'closed', 30)
        with ThreadPoolExecutor(2) as pool:
            a, b = pool.submit(claim), pool.submit(reject)
            self.assertEqual(sum((a.result(), b.result())), 1)
        self.assertEqual(len(self.store.reservations()), int(self.store.launch_claimed('a')))

    def test_cancel_and_invalid_input_never_requeue(self):
        self.store.request_cancel('a')
        self.store.reject_launch('a', 'not_ready', 'closed', 30)
        self.store.return_rejected('a')
        self.assertEqual(self.store.job_state('j'), 'cancelled')

    def test_rollback_restores_fence_and_reservation(self):
        with self.assertRaises(RuntimeError), self.store.scheduling_round():
            self.store.reject_launch('a', 'headroom', 'busy', 30)
            self.store.return_rejected('a')
            raise RuntimeError('rollback')
        self.assertIsNone(self.store.rejection('a'))
        self.assertEqual(len(self.store.reservations()), 1)

    def test_restart_before_claim_uses_durable_acceptance_fence(self):
        from lammps_manager.recovery import reconcile
        self.store.accept_prelaunch('a')
        with Store(self.path) as restarted:
            reconcile(restarted, restarted.attempt('a'))
            self.assertIsNotNone(restarted.rejection('a'))
            self.assertFalse(restarted.claim_launch('a'))
            restarted.return_rejected('a')
            self.assertEqual(restarted.job_state('j'), 'queued')

    def test_temporary_headroom_grace_is_durable_and_bounded(self):
        self.assertTrue(self.store.launch_deferred('a', 10))
        with Store(self.path) as restarted:
            self.assertTrue(restarted.launch_deferred('a', 39))
            self.assertFalse(restarted.launch_deferred('a', 40))

    def test_running_unknown_cannot_be_fenced_without_launch_claim(self):
        self.store.transition('a', 'running')
        self.store.transition('a', 'unknown')
        self.assertFalse(self.store.reject_launch('a', 'headroom', 'busy', 30))
        self.assertEqual(len(self.store.reservations()), 1)

    def test_returns_do_not_consume_future_application_retry_budget(self):
        from dataclasses import replace
        spec = self.store.specification('j')
        spec = replace(spec, job_id='retry', work_directory='/work/retry',
                       resources=replace(spec.resources, max_retries=1))
        self.store.submit(spec)
        self.store.assign('retry', 'r1', 'w', '/work/retry/r1')
        self.store.reject_launch('r1', 'headroom', 'busy', 0)
        self.store.return_rejected('r1')
        self.store.assign('retry', 'r2', 'w', '/work/retry/r2')
        self.store.transition('r2', 'failed', reason='application failure', termination_evidence='exit')
        self.store.retry('retry')
        self.assertEqual(self.store.job_state('retry'), 'queued')

    def test_repeated_rejection_is_bounded(self):
        for index in range(3):
            attempt = 'a' if index == 0 else str(index)
            if index:
                self.store.assign('j', attempt, 'w', '/work/j/' + attempt)
            self.store.reject_launch(attempt, 'incompatible', 'not supported', 0)
            self.store.return_rejected(attempt)
        self.assertEqual(self.store.rejection_blocker('j', 'w', 100), 'prelaunch_rejection_exhausted')
