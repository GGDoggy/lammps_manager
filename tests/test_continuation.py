"""Local contract tests; never execute LAMMPS or establish scientific equivalence."""
import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from lammps_manager.continuation import (
    ContinuationError, Recipe, recipe_digest, render_input, require_validated,
    validate_candidate,
)

spec = importlib.util.spec_from_file_location(
    'continuation_acceptance', Path(__file__).resolve().parents[1]
    / 'tools/verification/continuation_acceptance.py')
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


class ContinuationTests(unittest.TestCase):
    def test_interruption_kills_group_reaps_child_and_preserves_output(self):
        # Use local Python stand-ins, never MPI/LAMMPS. The descendant shares
        # the new session's process group and must also stop on interruption.
        launched = []
        commands = []
        real_popen = subprocess.Popen
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            script = (
                "import subprocess, sys, time\n"
                "from pathlib import Path\n"
                "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
                "print('retained stdout', flush=True)\n"
                "print('retained stderr', file=sys.stderr, flush=True)\n"
                "Path('ready.tmp').write_text(str(child.pid))\n"
                "Path('ready.tmp').rename('ready')\n"
                "time.sleep(60)\n"
            )

            def launch(*args, **kwargs):
                process = real_popen(*args, **kwargs)
                launched.append(process)
                real_wait = process.wait

                def interrupted_wait(timeout=None):
                    if timeout == 30:
                        deadline = time.monotonic() + 5
                        while not (directory / 'ready').exists():
                            if time.monotonic() > deadline:
                                self.fail('stand-in process did not become ready')
                            time.sleep(.01)
                        raise KeyboardInterrupt
                    return real_wait(timeout=timeout)

                process.wait = interrupted_wait
                return process

            try:
                with patch.object(harness.subprocess, 'Popen', side_effect=launch):
                    with self.assertRaises(KeyboardInterrupt):
                        harness.execute([sys.executable, '-c', script], directory, commands)
                self.assertIsNotNone(launched[0].returncode, 'direct child was not reaped')
                self.assertTrue(commands[0]['interrupted'])
                self.assertEqual(commands[0]['returncode'], -signal.SIGKILL)
                descendant = int((directory / 'ready').read_text())
                deadline = time.monotonic() + 5
                while True:
                    try:
                        state = Path(f'/proc/{descendant}/stat').read_text().split(') ', 1)[1][0]
                    except (FileNotFoundError, ProcessLookupError):
                        break
                    if state == 'Z':  # Dead descendant awaits its system reaper.
                        break
                    if time.monotonic() > deadline:
                        self.fail('descendant survived interruption')
                    time.sleep(.01)
                self.assertIn('retained stdout', (directory / 'stdout').read_text())
                self.assertIn('retained stderr', (directory / 'stderr').read_text())
            finally:
                for process in launched:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()

    def test_timeout_reaps_even_when_group_has_already_exited(self):
        process = Mock(pid=12345)
        process.wait.side_effect = [subprocess.TimeoutExpired(['stand-in'], 30), -9]
        commands = []
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(harness.subprocess, 'Popen', return_value=process), \
                patch.object(harness.os, 'killpg', side_effect=ProcessLookupError):
            with self.assertRaisesRegex(RuntimeError, 'exceeded 30 seconds'):
                harness.execute(['stand-in'], Path(tmp), commands)
        self.assertEqual(process.wait.call_count, 2)
        self.assertTrue(commands[0]['timed_out'])
        self.assertEqual(commands[0]['returncode'], -9)

    def test_gate_rejects_unvalidated_builds_and_stochastic_layouts(self):
        validate_candidate(Recipe())
        for recipe in (Recipe(), Recipe(mode='statistical'), Recipe(name='langevin'),
                       Recipe(mpi_ranks=2), Recipe(omp_threads=2), Recipe(mpi_ranks=True)):
            with self.subTest(recipe=recipe), self.assertRaises(ContinuationError):
                require_validated(recipe, build_fingerprint='claimed-passing-build')

    def test_resume_preserves_clock_and_does_not_reinitialize(self):
        initial = render_input(start=0, stop=37, total=100)
        resumed = render_input(start=37, stop=99, total=100)
        self.assertIn('velocity all create', initial)
        self.assertIn('read_restart checkpoint.restart', resumed)
        self.assertNotIn('velocity ', resumed)
        self.assertNotIn('create_atoms', resumed)
        self.assertNotIn('reset_timestep', resumed)
        self.assertIn('run 62 start 0 stop 100', resumed)
        self.assertEqual(len(recipe_digest()), 64)
        for bounds in ((True, 1, 2), (0, 0, 1), (2, 1, 3), (0, 1, 1001), (0, 1.0, 2)):
            with self.assertRaises(ContinuationError):
                render_input(start=bounds[0], stop=bounds[1], total=bounds[2])

    def test_all_boundary_states_compared_before_deduplication(self):
        reference = dict(enumerate(('a', 'b', 'c', 'd')))
        result = harness.compare(reference, [{0: 'a', 1: 'b'}, {1: 'b', 2: 'c', 3: 'd'}], 3)
        self.assertTrue(result['passed'])
        self.assertEqual(result['boundaries'], [1])
        self.assertFalse(harness.compare(reference, [{0: 'a', 1: 'wrong'},
                                                    {1: 'b', 2: 'c', 3: 'd'}], 3)['passed'])
        for parts in ([{0: 'a', 1: 'b'}, {2: 'c', 3: 'd'}], [{0: 'a', 1: 'b'}]):
            with self.assertRaises(ValueError):
                harness.compare(reference, parts, 3)

    def test_dump_validation_checks_atom_count_order_finiteness(self):
        header = ('ITEM: TIMESTEP\n0\nITEM: NUMBER OF ATOMS\n108\n'
                  'ITEM: BOX BOUNDS pp pp pp\n0 5\n0 5\n0 5\n'
                  'ITEM: ATOMS id type x y z vx vy vz ix iy iz fx fy fz\n')
        rows = ''.join(f'{i} 1 0 0 0 0 0 0 0 0 0 0 0 0\n' for i in range(1, 109))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'dump'
            for text, valid in ((header + rows, True), ('', False),
                                (header + rows.replace('1 1 0', '1 1 nan', 1), False),
                                ((header + rows) * 2, False),
                                (header + rows.replace('108 1', '107 1'), False)):
                path.write_text(text)
                if valid:
                    self.assertEqual(set(harness.read_frames(path)), {0})
                else:
                    with self.assertRaises(ValueError):
                        harness.read_frames(path)

    def test_thermodynamic_clock_and_finite_values(self):
        header = 'Step Time Atoms Temp PotEng KinEng TotEng Press\n'
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'log'
            valid = header + '0 0 108 1 -5 1 -4 0\n1 0.005 108 1 -5 1 -4 0\nLoop time of 1\n'
            path.write_text(valid)
            self.assertEqual(set(harness.read_thermo(path)), {0, 1})
            for text in (valid.replace('1 0.005', '1 0'),
                         valid.replace('1 -5', 'nan -5'),
                         valid.replace('108', '107'), '',
                         header + '0 0 108 1 -5 1 -4 0\n' * 2):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    harness.read_thermo(path)


abp_spec = importlib.util.spec_from_file_location(
    'abp_acceptance', Path(__file__).resolve().parents[1]
    / 'tools/verification/abp_acceptance.py')
sys.modules['continuation_acceptance'] = harness
abp = importlib.util.module_from_spec(abp_spec)
abp_spec.loader.exec_module(abp)


class ABPContractTests(unittest.TestCase):
    def test_stream_provenance_collision_and_reproducibility(self):
        used = set()
        first = abp.seed('pilot', 'reference', 0, '0', used)
        self.assertEqual(first, abp.seed('pilot', 'reference', 0, '0', set()))
        self.assertNotEqual(first, abp.seed('confirm', 'reference', 0, '0', used))
        self.assertNotEqual(first, abp.seed('pilot', 'segmented', 0, '0', used))
        with self.assertRaises(ValueError):
            abp.seed('pilot', 'reference', 0, '0', used)

    def test_abp_resume_does_not_reseed_orientation_or_reinitialize(self):
        from lammps_manager.continuation import render_abp_input
        text = render_abp_input(start=600, stop=1400, total=2000,
                                stream_seed=123, position_seed=456, orientation_seed=789)
        self.assertIn('brownian/sphere 1.0 123', text)
        self.assertIn('propel/self dipole 10.0', text)
        self.assertIn('run 800 start 0 stop 2000', text)
        self.assertNotIn('dipole/random', text)
        self.assertNotIn('timestep ', text)
        self.assertNotIn('velocity ', text)
        self.assertNotIn('create_atoms', text)
        with self.assertRaises(ContinuationError):
            render_abp_input(start=0, stop=1, total=1, stream_seed=0,
                             position_seed=1, orientation_seed=2)

    def test_student_quantile_and_equivalence_require_contained_interval(self):
        self.assertAlmostEqual(abp.student_quantile(.975, 9), 2.2621571627, places=7)
        # No detected mean difference is insufficient when uncertainty is too wide.
        wide = [{'msd-1.0': (-1)**i} for i in range(64)]
        self.assertFalse(abp.analyze(wide)['passed'])
        narrow = [{'msd-1.0': .001*(-1)**i} for i in range(64)]
        self.assertTrue(abp.analyze(narrow)['passed'])
        biased = [{'msd-1.0': .06+.001*(-1)**i} for i in range(64)]
        self.assertFalse(abp.analyze(biased)['passed'])
        too_noisy_pilot = [{'msd-1.0': (-1)**i} for i in range(8)]
        self.assertFalse(abp.choose_size(too_noisy_pilot)['feasible'])

    def test_abp_dump_rejects_box_changes_and_nonplanar_orientations(self):
        header = ('ITEM: TIMESTEP\n0\nITEM: NUMBER OF ATOMS\n1024\n'
                  'ITEM: BOX BOUNDS pp pp pp\n0 32\n0 32\n-0.5 0.5\n'
                  'ITEM: ATOMS id xu yu mux muy muz\n')
        rows = ''.join(f'{i} 0 0 1 0 0\n' for i in range(1, 1025))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'dump'
            path.write_text(header + rows)
            self.assertEqual(set(abp.frames(path)), {0})
            for text in ((header + rows).replace('0 32', '0 31', 1),
                         header + rows.replace('1 0 0 1 0 0', '1 0 0 1 0 0.1', 1)):
                path.write_text(text)
                with self.assertRaises(ValueError):
                    abp.frames(path)

    def test_build_identity_changes_with_executable_or_linked_library(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        from lammps_manager.continuation import fingerprint_build
        with tempfile.TemporaryDirectory() as tmp:
            executable = Path(tmp) / 'lmp'
            library = Path(tmp) / 'libmodel.so'
            executable.write_bytes(b'executable-v1')
            library.write_bytes(b'library-v1')
            with patch('lammps_manager.continuation.subprocess.run',
                       return_value=SimpleNamespace(stdout=str(library))):
                initial = fingerprint_build(executable)
                self.assertEqual(initial, fingerprint_build(executable))
                library.write_bytes(b'library-v2')
                self.assertNotEqual(initial, fingerprint_build(executable))
                library.write_bytes(b'library-v1')
                executable.write_bytes(b'executable-v2')
                self.assertNotEqual(initial, fingerprint_build(executable))
            with patch('lammps_manager.continuation.subprocess.run',
                       return_value=SimpleNamespace(stdout='')):
                with self.assertRaises(ContinuationError):
                    fingerprint_build(executable)

    def test_inventory_failure_retains_report_without_launching(self):
        import json
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            executable = Path(tmp) / 'lmp'
            executable.write_bytes(b'unvalidated-build')
            root = Path(tmp) / 'comparison'
            argv = ['abp_acceptance.py', '--lammps', str(executable), '--root', str(root)]
            with patch.object(sys, 'argv', argv), \
                    patch.object(abp, 'fingerprint_build', side_effect=ContinuationError('inventory failed')), \
                    patch.object(abp, 'execute') as launch:
                with self.assertRaises(ContinuationError):
                    abp.main()
                launch.assert_not_called()
            report = json.loads((root / 'report.json').read_text())
            self.assertFalse(report['passed'])
            self.assertIn('inventory failed', report['error'])

    def test_release_gate_accepts_only_measured_statistical_scope(self):
        from unittest.mock import patch
        recipe = Recipe(name='ideal-abp-2d-v1', mode='statistical')
        build = 'aa8a1af68131f97d13fd4959031c557681b46c9eee23dddef26ca75c7eae0695'
        scope = dict(build_fingerprint=build, total_steps=2000,
                     segment_ends=(600, 1400, 2000))
        accepted = require_validated(recipe, **scope)
        self.assertEqual(accepted.recipe.mode, 'statistical')
        self.assertEqual(accepted.segment_ends, (600, 1400, 2000))
        self.assertIn('t017-continuation.json', accepted.evidence)
        for changed in (dict(build_fingerprint='other-build'), dict(total_steps=2001),
                        dict(segment_ends=(500, 1400, 2000)), dict(segment_ends=[600,1400,2000]),
                        dict(total_steps=True)):
            with self.assertRaises(ContinuationError):
                require_validated(recipe, **{**scope, **changed})
        with self.assertRaises(ContinuationError):
            require_validated(Recipe(), **scope)
        with self.assertRaises(ContinuationError):
            require_validated(Recipe(name='ideal-abp-2d-v1',mode='exact'), **scope)
        with patch('lammps_manager.continuation.abp_recipe_digest', return_value='edited-recipe'):
            with self.assertRaises(ContinuationError):
                require_validated(recipe, **scope)
