"""Exercise the public module entry point without a LAMMPS installation."""

import subprocess
import sys
import unittest


class CliTests(unittest.TestCase):
    def run_cli(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "lammps_manager", *args],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def test_help_and_default_invocation(self) -> None:
        for args in [(), ("--help",)]:
            with self.subTest(args=args):
                result = self.run_cli(*args)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("usage: lammps-manager", result.stdout)
                self.assertIn("not implemented yet", " ".join(result.stdout.split()))

    def test_validate_config(self) -> None:
        for kind in ('manager', 'worker'):
            result = self.run_cli('validate-config', kind, f'examples/{kind}.yaml')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('deployment paths not checked', result.stdout)
        result = self.run_cli('validate-config', 'manager', '/missing/config.yaml')
        self.assertEqual(result.returncode, 2)
        self.assertIn('/missing/config.yaml', result.stderr)

    def test_version(self) -> None:
        result = self.run_cli("--version")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(result.stdout.startswith("lammps-manager "))

    def test_unsupported_command_is_rejected(self) -> None:
        result = self.run_cli("submit")
        self.assertEqual(result.returncode, 2)
        self.assertIn("invalid choice", result.stderr)


if __name__ == "__main__":
    unittest.main()
