"""Local V007 startup regressions; all external commands are mocked."""
import importlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


helpers = str(Path(__file__).resolve().parents[1] / 'tools/verification')
with patch.object(sys, 'path', [helpers, *sys.path]), patch.dict(sys.modules):
    service_acceptance = importlib.import_module('service_acceptance')


class ServiceStartupCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def check_failure(self, failure: BaseException, phase: str) -> None:
        commands = []

        def command(argv, **kwargs):
            commands.append(argv)
            if phase == 'startup' and argv[0] == 'systemd-run':
                raise failure
            if phase == 'validation':
                raise failure
            return subprocess.CompletedProcess(argv, 0, '', '')

        with patch.multiple(service_acceptance, ROOT=self.root), \
                patch.object(service_acceptance, 'setup', return_value=self.root), \
                patch.object(service_acceptance, 'prepare'), \
                patch.object(service_acceptance, 'port_open', return_value=False), \
                patch.object(service_acceptance, 'wait_for', side_effect=failure), \
                patch.object(service_acceptance.subprocess, 'run', side_effect=command):
            with self.assertRaises(type(failure)) as caught:
                service_acceptance.run()
        self.assertIs(caught.exception, failure)
        stops = [argv for argv in commands if argv[:3] == ['systemctl', '--user', 'stop']]
        if phase == 'validation':
            self.assertFalse(stops)
            self.assertFalse(any(argv[0] == 'systemd-run' for argv in commands))
        else:
            prefix = self.root.name + '-v007'
            self.assertEqual(stops, [['systemctl', '--user', 'stop',
                                     prefix + '-worker.service', prefix + '-manager.service']])
            starts = [argv for argv in commands if argv[0] == 'systemd-run']
            self.assertEqual(len(starts), 1)
            self.assertEqual(starts[0][3], prefix + '-manager')

    def test_manager_startup_timeout_stops_named_units(self) -> None:
        self.check_failure(subprocess.TimeoutExpired(['systemd-run'], 60), 'startup')

    def test_manager_startup_interruption_stops_named_units(self) -> None:
        self.check_failure(KeyboardInterrupt('interrupted startup'), 'startup')

    def test_manager_readiness_failure_stops_named_units(self) -> None:
        self.check_failure(AssertionError('Manager did not listen'), 'readiness')

    def test_validation_failure_does_not_touch_services(self) -> None:
        self.check_failure(OSError('Validation failed'), 'validation')


if __name__ == '__main__':
    unittest.main()
