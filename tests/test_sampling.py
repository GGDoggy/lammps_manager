"""Observable global schedule conversion and segment selection."""

import hashlib
import unittest

from lammps_manager.sampling import (normalize_sampling, segment_steps, steps_bytes,
    sampling_files, apply_sampling, validate_sampling)


class SamplingTests(unittest.TestCase):
    def test_split_run_commands_reopen_schedule_without_skipping_points(self):
        from lammps_manager.continuation import MAX_RUN_COMMAND, Recipe, render_engineering_input
        end = MAX_RUN_COMMAND + 10
        schedule = normalize_sampling(dict(unit='step', positions=[1, MAX_RUN_COMMAND + 1, end]), dt=.1, dr=1.)
        files = sampling_files(schedule, start=0, stop=end)
        self.assertEqual(files['snapshot-steps.txt'], f'1\n{MAX_RUN_COMMAND+1}\n'.encode())
        self.assertEqual(files['snapshot-steps-000001.txt'], f'{MAX_RUN_COMMAND+1}\n{end}\n{end+1}\n'.encode())
        self.assertIn(b'undump trajectory', files['sampling-000001.lmp'])
        self.assertIn(b'append yes', files['sampling-000001.lmp'])
        text = render_engineering_input(Recipe('wca-abp-2d-v1', 'statistical'),
            start=0, stop=end, total=end, stream_seed=1, position_seed=2, orientation_seed=3)
        text = apply_sampling(text)
        self.assertIn(f'include sampling-000001.lmp\nrun 10 start 0 stop {end}\n', text)
        self.assertEqual(text.count('include sampling.lmp'), 1)

    def test_generated_inputs_and_tamper_detection(self):
        schedule = normalize_sampling(dict(unit='step', positions=[2, 7]), dt=0.1, dr=1.0)
        first = sampling_files(schedule, start=0, stop=2)
        self.assertEqual(first['snapshot-steps.txt'], b'2\n3\n')
        self.assertIn(b'initial.dump', first['sampling.lmp'])
        self.assertIn(b'first no', first['sampling.lmp'])
        empty = sampling_files(schedule, start=2, stop=5)
        self.assertEqual(empty['sampling.lmp'], b'')
        self.assertEqual(empty['snapshot-steps.txt'], b'6\n')
        final = sampling_files(schedule, start=5, stop=7)
        self.assertNotIn(b'initial.dump', final['sampling.lmp'])
        self.assertEqual(final['snapshot-steps.txt'], b'7\n8\n')
        validate_sampling(schedule)
        schedule['steps'][0] = 1
        with self.assertRaises(ValueError):
            validate_sampling(schedule)

    def test_default_and_extended_sampling(self):
        schedule = normalize_sampling(None, dt=0.0001, dr=1.0)
        self.assertEqual(len(schedule['steps']), 30000)
        self.assertEqual(schedule['steps'][:2], [100, 200])
        self.assertEqual(schedule['steps'][-1], 3000000)
        extended = normalize_sampling(dict(unit='Dr', interval=0.01, count=30001), dt=0.0001, dr=1.0)
        self.assertEqual(extended['steps'][-1], 3000100)
        self.assertEqual(schedule['steps_sha256'], hashlib.sha256(steps_bytes(schedule['steps'])).hexdigest())

    def test_half_up_and_global_segment_boundaries(self):
        schedule = normalize_sampling(dict(unit='Dr', positions=[0.015, 0.025, 0.045]), dt=0.01, dr=1.0)
        self.assertEqual(schedule['steps'], [2, 3, 5])
        self.assertEqual(segment_steps(schedule, start=0, stop=3), b'2\n3\n')
        self.assertEqual(segment_steps(schedule, start=3, stop=5), b'5\n')
        self.assertEqual(segment_steps(schedule, start=5, stop=6), b'')

    def test_explicit_steps_and_interval(self):
        explicit = normalize_sampling(dict(unit='step', positions=[1, 7, 999]), dt=0.1, dr=2.0)
        self.assertEqual(explicit['steps'], [1, 7, 999])
        regular = normalize_sampling(dict(unit='step', interval=7, count=3), dt=0.1, dr=2.0)
        self.assertEqual(regular['steps'], [7, 14, 21])

    def test_invalid_schedules_fail_instead_of_dropping_points(self):
        invalid = [dict(unit='Dr', positions=[0.001]),
                   dict(unit='Dr', positions=[0.01, 0.011]),
                   dict(unit='Dr', positions=[0.02, 0.01]),
                   dict(unit='Dr', positions=[float('nan')]),
                   dict(unit='step', positions=[True]),
                   dict(unit='step', positions=[1.0]),
                   dict(unit='step', positions=[0]),
                   dict(unit='step', positions=[2**63]),
                   dict(unit='step', interval=1.5, count=2),
                   dict(unit='step', interval=1, count=True),
                   dict(unit='step', positions=[]),
                   dict(unit='Dr', interval=0.01, count=2, positions=[1])]
        for settings in invalid:
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                normalize_sampling(settings, dt=0.01, dr=1.0)

    def test_invalid_physics(self):
        for value in (0, -1, float('inf'), True, '1'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_sampling(None, dt=value, dr=1.0)


if __name__ == '__main__':
    unittest.main()
