"""Finite plan execution and retained failure status, without external hosts."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

DRIVER = Path(__file__).resolve().parents[1] / 'tools/verification/unattended_run.py'
spec = importlib.util.spec_from_file_location('unattended_acceptance', DRIVER)
runner = importlib.util.module_from_spec(spec); spec.loader.exec_module(runner)


class UnattendedTests(unittest.TestCase):
    def test_invalid_plan_rejected_before_any_execution(self) -> None:
        for steps in [[], [{'argv': ['true'], 'timeout_seconds': 0}],
                      [{'argv': 'true', 'timeout_seconds': 1}],
                      [{'argv': ['true'], 'timeout_seconds': True}]]:
            with self.assertRaises(ValueError):
                runner.validate(dict(steps=steps))

    def test_success_and_failure_preserve_progress_and_do_not_run_later_steps(self) -> None:
        for returncode in [0, 7]:
            with self.subTest(returncode=returncode), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                plan = dict(cwd=temporary, steps=[
                    dict(argv=[sys.executable, '-c', f'raise SystemExit({returncode})'], timeout_seconds=5),
                    dict(argv=[sys.executable, '-c', "open('later','w').close()"], timeout_seconds=5)])
                (root / 'plan.json').write_text(json.dumps(plan))
                result = subprocess.run([sys.executable, str(DRIVER), temporary], timeout=10)
                progress = json.loads((root / 'progress.json').read_text())
                self.assertEqual(result.returncode, 0 if returncode == 0 else 1)
                self.assertEqual(progress['completed'], returncode == 0)
                self.assertEqual((root / 'later').exists(), returncode == 0)
                self.assertEqual(progress['steps'][0]['returncode'], returncode)
                self.assertTrue((root / 'step-0.log').exists())

    def test_timeout_ends_owned_child_and_marks_incomplete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'plan.json').write_text(json.dumps(dict(cwd=temporary, steps=[dict(
                argv=[sys.executable, '-c', 'import time;time.sleep(60)'], timeout_seconds=1)])))
            result = subprocess.run([sys.executable, str(DRIVER), temporary], timeout=10)
            progress = json.loads((root / 'progress.json').read_text())
            self.assertEqual(result.returncode, 1)
            self.assertIn('TimeoutExpired', progress['error'])
            self.assertFalse(Path(f'/proc/{progress["steps"][0]["pid"]}').exists())


if __name__ == '__main__':
    unittest.main()
