"""Offline regressions for retained failure evidence and executable affinity."""
import importlib.util
import contextlib
import io
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from lammps_manager.models import JobSpecification, Resources
from lammps_manager.store import Store

DRIVERS = Path(__file__).resolve().parents[1] / 'tools' / 'verification'


def load_driver(name: str):
    spec = importlib.util.spec_from_file_location(name, DRIVERS / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


crosshost = load_driver('crosshost_acceptance')
guard = load_driver('wca_affinity_guard')


class CrosshostReportTests(unittest.TestCase):
    def test_unresolved_reservation_preserves_original_failure_and_report(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with Store(root / 'manager.sqlite') as store:
                store.submit(JobSpecification('job', '/work/job', 'in.lmp', 'a' * 64,
                                             Resources(mpi_ranks=2, memory_gib=.5)))
                store.assign('job', 'attempt', 'worker', '/work/job/attempt')
                store.transition('attempt', 'unknown', reason='Worker disconnected')
            report = dict(passed=False, error='TimeoutError: original deadline')
            with self.assertRaisesRegex(TimeoutError, 'original deadline'):
                try:
                    raise TimeoutError('original deadline')
                finally:
                    crosshost.write_final_report(root, 'job', report)
            saved = json.loads((root / 'report.json').read_text())
            self.assertEqual(saved['error'], report['error'])
            self.assertFalse(saved['passed'])
            self.assertEqual(saved['final_state'], 'unknown')
            self.assertEqual(saved['final_reservations'], [dict(
                attempt_id='attempt', worker_id='worker', cpu_slots=2, memory_gib=.5)])
            with Store(root / 'manager.sqlite') as store:
                self.assertEqual(len(store.reservations()), 1)

    def test_empty_reservations_remain_json_array(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with Store(root / 'manager.sqlite') as store:
                store.submit(JobSpecification('job', '/work/job', 'in.lmp', 'a' * 64))
            crosshost.write_final_report(root, 'job', dict(passed=False))
            self.assertEqual(json.loads((root / 'report.json').read_text())['final_reservations'], [])


    def cleanup_fixture(self, root: Path) -> Mock:
        with Store(root / 'manager.sqlite') as store:
            store.submit(JobSpecification('job', '/work/job', 'in.lmp', 'a' * 64))
        manager = Mock()
        manager.poll.return_value = None
        return manager

    def remote_cleanup(self, results: list[subprocess.CompletedProcess],
                       cgroup: Path) -> subprocess.CompletedProcess:
        """Execute the real SSH program with only systemctl and cgroup I/O mocked."""
        def systemctl(argv, **kwargs):
            result = results.pop(0)
            if kwargs.get('check'):
                result.check_returncode()
            return result
        output = io.StringIO()
        with patch('subprocess.run', side_effect=systemctl), \
                patch('pathlib.Path', return_value=cgroup), contextlib.redirect_stdout(output):
            exec(compile(crosshost.worker_stop_script('private-worker.service'),
                         '<remote-worker-cleanup>', 'exec'), {})
        return subprocess.CompletedProcess('ssh', 0, output.getvalue())

    def test_remote_systemctl_failures_cannot_publish_success(self) -> None:
        terminal = 'ActiveState=inactive\nMainPID=0\nControlGroup=\n'
        cases = [
            (1, 1, '', 'Failed to connect to bus'),
            (1, 0, terminal, 'Stop failed'),
            (0, 1, '', 'Query failed'),
            (0, 0, '', ''),
            (0, 0, 'ActiveState=inactive\nControlGroup=\n', ''),
            (0, 0, 'ActiveState=inactive\nMainPID=0\n', ''),
            (0, 0, terminal.replace('inactive', 'unknown'), ''),
            (0, 0, terminal.replace('inactive', 'active'), ''),
            (0, 0, terminal.replace('MainPID=0', 'MainPID=123'), ''),
            (0, 0, terminal.replace('MainPID=0', 'MainPID=invalid'), ''),
        ]
        for stop_code, query_code, properties, error in cases:
            with self.subTest(case=(stop_code, query_code, properties)), \
                    tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                manager = self.cleanup_fixture(root)
                results = [subprocess.CompletedProcess(['systemctl', 'stop'], stop_code, '', error),
                           subprocess.CompletedProcess(['systemctl', 'show'], query_code, properties, error)]
                with self.assertRaisesRegex(RuntimeError, 'Worker cleanup failed'), \
                        patch.object(crosshost, 'print', create=True):
                    crosshost.finish_run(root, 'job', dict(passed=True), manager,
                                         lambda: self.remote_cleanup(results, root / 'cgroup'))
                saved = json.loads((root / 'report.json').read_text())
                self.assertFalse(saved['passed'])
                self.assertIn('worker_cleanup_error', saved)
                manager.wait.assert_called_once_with(timeout=45)

    def test_remote_cleanup_requires_terminal_unit_and_empty_cgroup(self) -> None:
        for state, members in [('inactive', ''), ('failed', ''), ('inactive', '123\n')]:
            with self.subTest(state=state, members=members), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                group = root / 'private.service'; group.mkdir()
                (group / 'cgroup.procs').write_text(members)
                results = [subprocess.CompletedProcess(['systemctl', 'stop'], 0, '', ''),
                           subprocess.CompletedProcess(['systemctl', 'show'], 0,
                               f'ActiveState={state}\nMainPID=0\nControlGroup=/private.service\n', '')]
                if members:
                    with self.assertRaisesRegex(RuntimeError, 'not fully stopped'):
                        self.remote_cleanup(results, root)
                else:
                    stopped = self.remote_cleanup(results, root)
                    evidence = json.loads(stopped.stdout)
                    self.assertEqual(evidence['properties']['ActiveState'], state)
                    self.assertEqual(evidence['remaining_cgroup_pids'], [])

    def test_cleanup_failure_marks_report_failed_after_stopping_manager(self) -> None:
        errors = [OSError('SSH disconnected'), subprocess.TimeoutExpired('ssh', 45),
                  subprocess.CalledProcessError(1, 'ssh', stderr='Worker service not fully stopped')]
        for error in errors:
            with self.subTest(error=type(error).__name__), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                manager = self.cleanup_fixture(root)
                stop_worker = Mock(side_effect=error)
                with self.assertRaisesRegex(RuntimeError, 'Worker cleanup failed') as raised, patch('builtins.print'):
                    crosshost.finish_run(root, 'job', dict(passed=True), manager, stop_worker)
                self.assertIs(raised.exception.__cause__, error)
                manager.send_signal.assert_called_once_with(crosshost.signal.SIGINT)
                manager.wait.assert_called_once_with(timeout=45)
                saved = json.loads((root / 'report.json').read_text())
                self.assertFalse(saved['passed'])
                self.assertIn('Worker cleanup failed', saved['error'])
                self.assertEqual(saved['worker_cleanup_error'], str(error))

    def test_cleanup_failure_preserves_original_execution_exception(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manager = self.cleanup_fixture(root)
            original = TimeoutError('original deadline')
            report = dict(passed=False, error='TimeoutError: original deadline')
            with self.assertRaises(TimeoutError) as raised, patch('builtins.print'):
                try:
                    raise original
                finally:
                    crosshost.finish_run(root, 'job', report, manager,
                                         Mock(side_effect=OSError('SSH disconnected')))
            self.assertIs(raised.exception, original)
            saved = json.loads((root / 'report.json').read_text())
            self.assertEqual(saved['error'], 'TimeoutError: original deadline')
            self.assertEqual(saved['worker_cleanup_error'], 'SSH disconnected')
            manager.wait.assert_called_once_with(timeout=45)

    def test_successful_cleanup_retains_success_and_manager_escalation(self) -> None:
        for escalate in (False, True):
            with self.subTest(escalate=escalate), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                manager = self.cleanup_fixture(root)
                if escalate:
                    manager.wait.side_effect = [subprocess.TimeoutExpired('manager', 45), 0]
                stopped = dict(unit='private-worker.service', remaining_cgroup_pids=[])
                stop_worker = Mock(return_value=subprocess.CompletedProcess('ssh', 0, json.dumps(stopped)))
                with patch('builtins.print'):
                    crosshost.finish_run(root, 'job', dict(passed=True), manager, stop_worker)
                saved = json.loads((root / 'report.json').read_text())
                self.assertTrue(saved['passed'])
                self.assertEqual(saved['worker_stop_requested'], stopped)
                self.assertNotIn('worker_cleanup_error', saved)
                if escalate:
                    manager.kill.assert_called_once_with()
                    self.assertTrue(saved['manager_escalation'])


class AffinityGuardTests(unittest.TestCase):
    def fixture(self, base: Path):
        root = base / 'pool'
        stage = root / 'chunks/s0-0-64/prepare'; stage.mkdir(parents=True)
        executable = base / 'custom-install/lmp'; executable.parent.mkdir()
        executable.write_bytes(b'selected binary')
        link = base / 'lmp-link'; link.symlink_to(executable)
        proc = base / 'proc'; entry = proc / '123'; entry.mkdir(parents=True)
        (entry / 'exe').symlink_to(executable)
        (entry / 'cwd').symlink_to(stage)
        pool = dict(allowed_cpus=[0, 1, 2, 3], active=[dict(name='s0-0-64', cpus=[2, 3])])
        return root, executable, link, proc, entry, pool

    def test_custom_path_and_symlink_correct_only_selected_ranks(self) -> None:
        for alias in (False, True):
            with self.subTest(alias=alias), tempfile.TemporaryDirectory() as temporary:
                root, executable, link, proc, entry, pool = self.fixture(Path(temporary))
                with patch.object(guard, 'process_identity', return_value={'pid': 123, 'start_ticks': 7}), \
                        patch.object(guard.os, 'sched_getaffinity', side_effect=[{0, 1, 2, 3}, {2, 3}]), \
                        patch.object(guard.os, 'sched_setaffinity') as set_affinity:
                    changes = guard.enforce_allocations(root, pool, link if alias else executable,
                                                        proc_root=proc)
                set_affinity.assert_called_once_with(123, {2, 3})
                self.assertEqual(changes[0]['after'], [2, 3])
                self.assertEqual(changes[0]['chunk'], 's0-0-64')

    def test_unrelated_processes_and_reused_pids_are_excluded(self) -> None:
        for scenario in ('other-executable', 'outside-root', 'inactive-chunk', 'reused-pid'):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temporary:
                base = Path(temporary)
                root, executable, link, proc, entry, pool = self.fixture(base)
                if scenario == 'other-executable':
                    other = base / 'other'; other.write_bytes(b'other binary')
                    (entry / 'exe').unlink(); (entry / 'exe').symlink_to(other)
                elif scenario == 'outside-root':
                    (entry / 'cwd').unlink(); (entry / 'cwd').symlink_to(base)
                elif scenario == 'inactive-chunk':
                    pool['active'] = []
                identities = [{'pid': 123, 'start_ticks': 7}, {'pid': 123, 'start_ticks': 8}]
                with patch.object(guard, 'process_identity', side_effect=identities), \
                        patch.object(guard.os, 'sched_getaffinity', return_value={0, 1, 2, 3}), \
                        patch.object(guard.os, 'sched_setaffinity') as set_affinity:
                    self.assertEqual(guard.enforce_allocations(root, pool, executable, proc_root=proc), [])
                set_affinity.assert_not_called()

    def test_allocation_outside_pool_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, executable, link, proc, entry, pool = self.fixture(Path(temporary))
            pool['active'][0]['cpus'] = [99]
            with patch.object(guard, 'process_identity', return_value={'pid': 123}), \
                    patch.object(guard.os, 'sched_setaffinity') as set_affinity:
                with self.assertRaisesRegex(ValueError, 'exceeds pool'):
                    guard.enforce_allocations(root, pool, executable, proc_root=proc)
            set_affinity.assert_not_called()


if __name__ == '__main__':
    unittest.main()
