"""Local startup-cleanup regressions; external services are always mocked."""
import importlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, call, patch


# These standalone drivers use sibling imports and run only under __main__.
# Restore the import environment after loading them for local regression tests.
helpers = str(Path(__file__).resolve().parents[1] / 'tools/verification')
with patch.object(sys, 'path', [helpers, *sys.path]), patch.dict(sys.modules):
    artifact_acceptance = importlib.import_module('artifact_acceptance')
    distributed_acceptance = importlib.import_module('distributed_acceptance')


class ArtifactStartupCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / 'manager.yaml').write_text('storage: {}\n')
        self.manager = Mock(name='manager-process')
        self.proxy = Mock(name='proxy-process')
        self.worker = Mock(name='worker-process')
        self.observer = Mock(name='observer', ident=None)
        self.store = Mock()
        self.store.__enter__ = Mock(return_value=self.store)
        self.store.__exit__ = Mock(return_value=False)
        self.store.jobs.return_value = []
        self.store.reservations.return_value = []
        replacements = dict(ROOT=root, setup=Mock(return_value=root), prepare=Mock(),
                            cli=Mock(return_value=['job']), Store=Mock(return_value=self.store),
                            manager=Mock(return_value=self.manager),
                            start=Mock(return_value=self.proxy), worker=Mock(return_value=self.worker),
                            stop=Mock(), gate=Mock(), wait_for=Mock(),
                            BUDGET={**artifact_acceptance.BUDGET, 'large_bytes': 65536,
                                    'small_files': 1, 'disk_bytes': 1})
        self.mocks = replacements
        for patcher in (
            patch.multiple(artifact_acceptance, **replacements),
            patch.object(artifact_acceptance.threading, 'Thread', return_value=self.observer),
            patch.object(subprocess, 'run', return_value=subprocess.CompletedProcess(
                [], 1, stdout='', stderr='Input limit exceeded')),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_manager_readiness_timeout_cleans_up_unreturned_process(self) -> None:
        self.manager.poll.return_value = None
        with patch.object(artifact_acceptance, 'manager', distributed_acceptance.manager), \
                patch.object(distributed_acceptance, 'start', return_value=self.manager), \
                patch.object(distributed_acceptance, 'stop') as stop, \
                patch.object(distributed_acceptance.socket, 'create_connection',
                             side_effect=ConnectionRefusedError), \
                patch.object(distributed_acceptance.time, 'sleep'):
            with self.assertRaisesRegex(AssertionError, 'Manager did not listen'):
                artifact_acceptance.run()
            stop.assert_called_once_with(self.manager)
        self.mocks['start'].assert_not_called()
        self.mocks['worker'].assert_not_called()

    def test_proxy_readiness_timeout_stops_started_services(self) -> None:
        failure = AssertionError('Timed out: proxy listening')
        self.mocks['wait_for'].side_effect = failure
        with self.assertRaises(AssertionError) as caught:
            artifact_acceptance.run()
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.mocks['stop'].call_args_list,
                         [call(self.proxy), call(self.manager)])
        self.mocks['worker'].assert_not_called()
        self.observer.start.assert_not_called()
        self.observer.join.assert_not_called()

    def test_proxy_spawn_failure_stops_manager(self) -> None:
        self.mocks['start'].side_effect = OSError('Cannot spawn proxy')
        with self.assertRaisesRegex(OSError, 'Cannot spawn proxy'):
            artifact_acceptance.run()
        self.mocks['stop'].assert_called_once_with(self.manager)
        self.mocks['worker'].assert_not_called()
        self.observer.join.assert_not_called()

    def test_worker_spawn_failure_stops_proxy_and_manager(self) -> None:
        self.mocks['worker'].side_effect = OSError('Cannot spawn worker')
        with self.assertRaisesRegex(OSError, 'Cannot spawn worker'):
            artifact_acceptance.run()
        self.assertEqual(self.mocks['stop'].call_args_list,
                         [call(self.proxy), call(self.manager)])
        self.observer.join.assert_not_called()

    def test_observer_start_failure_stops_all_services(self) -> None:
        self.observer.start.side_effect = RuntimeError('Cannot start observer')
        with self.assertRaisesRegex(RuntimeError, 'Cannot start observer'):
            artifact_acceptance.run()
        self.assertEqual(self.mocks['stop'].call_args_list,
                         [call(self.worker), call(self.proxy), call(self.manager)])
        self.observer.join.assert_not_called()

    def test_execution_failure_stops_services_and_joins_observer(self) -> None:
        self.observer.start.side_effect = lambda: setattr(self.observer, 'ident', 1)
        self.mocks['wait_for'].side_effect = [None, AssertionError('No partial input')]
        with self.assertRaisesRegex(AssertionError, 'No partial input'):
            artifact_acceptance.run()
        self.assertEqual(self.mocks['stop'].call_args_list,
                         [call(self.worker), call(self.proxy), call(self.manager)])
        self.observer.join.assert_called_once_with(timeout=10)

    def test_fault_reset_failure_does_not_skip_service_cleanup(self) -> None:
        self.mocks['wait_for'].side_effect = AssertionError('Proxy timeout')
        self.mocks['gate'].side_effect = [None, OSError('Cannot reset fault')]
        with self.assertRaisesRegex(OSError, 'Cannot reset fault'):
            artifact_acceptance.run()
        self.assertEqual(self.mocks['stop'].call_args_list,
                         [call(self.proxy), call(self.manager)])

    def test_stop_failure_does_not_skip_other_services_or_observer(self) -> None:
        self.observer.start.side_effect = lambda: setattr(self.observer, 'ident', 1)
        self.mocks['wait_for'].side_effect = [None, AssertionError('No partial input')]
        self.mocks['stop'].side_effect = [OSError('Cannot stop worker'), None, None]
        with self.assertRaisesRegex(OSError, 'Cannot stop worker'):
            artifact_acceptance.run()
        self.assertEqual(self.mocks['stop'].call_args_list,
                         [call(self.worker), call(self.proxy), call(self.manager)])
        self.observer.join.assert_called_once_with(timeout=10)


class ManagerReadinessTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        (self.root / 'manager.log').write_text('manager startup failed')
        self.process = Mock()
        self.process.poll.return_value = None

    def test_readiness_failure_stops_and_reaps_process(self) -> None:
        for scenario in ('early-exit', 'interruption'):
            with self.subTest(scenario=scenario):
                self.process.reset_mock()
                self.process.poll.return_value = 1 if scenario == 'early-exit' else None
                failure = KeyboardInterrupt('interrupted')
                with patch.object(distributed_acceptance, 'start', return_value=self.process), \
                        patch.object(distributed_acceptance, 'stop',
                                     wraps=distributed_acceptance.stop) as stop, \
                        patch.object(distributed_acceptance.socket, 'create_connection',
                                     side_effect=failure), \
                        patch.object(distributed_acceptance.time, 'sleep'):
                    expected = AssertionError if scenario == 'early-exit' else KeyboardInterrupt
                    with self.assertRaises(expected) as caught:
                        distributed_acceptance.manager(self.root)
                    stop.assert_called_once_with(self.process)
                if scenario == 'interruption':
                    self.assertIs(caught.exception, failure)
                    self.process.send_signal.assert_called_once_with(
                        distributed_acceptance.signal.SIGINT)
                    self.process.wait.assert_called_once_with(timeout=45)
                else:
                    self.process.send_signal.assert_not_called()

    def test_success_returns_process_without_stopping_it(self) -> None:
        with patch.object(distributed_acceptance, 'start', return_value=self.process), \
                patch.object(distributed_acceptance.socket, 'create_connection',
                             return_value=MagicMock()), \
                patch.object(distributed_acceptance, 'stop') as stop:
            self.assertIs(distributed_acceptance.manager(self.root), self.process)
            stop.assert_not_called()


if __name__ == '__main__':
    unittest.main()
