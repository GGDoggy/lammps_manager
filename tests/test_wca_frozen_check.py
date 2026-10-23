"""Reject design drift when rebuilding missing pilot raw evidence."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('frozen_pilot_check', Path(__file__).resolve().parents[1] /
                                              'tools/verification/wca_frozen_check.py')
checker = importlib.util.module_from_spec(spec); spec.loader.exec_module(checker)


class FrozenPilotTests(unittest.TestCase):
    def test_frozen_design_matches_but_changed_build_counts_or_missing_replicas_stop(self) -> None:
        original = dict(build='b', protocol=dict(pilot_pairs=2, strata=[1]), driver_sha256='d',
                        replicas=[{}, {}], strata=[dict(duration=100, layout=[1, 2, 1], pairs=64,
                                                       required={'energy': 64}, feasible=True)])
        rebuilt = copy.deepcopy(original); rebuilt['driver_sha256'] = 'new-wrapper'
        self.assertEqual(checker.check(rebuilt, original)['total_pairs'], 64)
        for field in ['build', 'protocol', 'replicas', 'pairs']:
            changed = copy.deepcopy(rebuilt)
            if field == 'pairs':
                changed['strata'][0]['pairs'] += 1
            elif field == 'replicas':
                changed[field] = [{}]
            else:
                changed[field] = 'changed'
            with self.subTest(field=field), self.assertRaises(ValueError):
                checker.check(changed, original)


if __name__ == '__main__':
    unittest.main()
