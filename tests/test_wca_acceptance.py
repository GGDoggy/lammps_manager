"""Local scientific-driver checks; no MPI, workers or LAMMPS are launched."""
import importlib.util
import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

DRIVERS = Path(__file__).resolve().parents[1] / 'tools' / 'verification'
with patch.object(sys, 'path', [str(DRIVERS), *sys.path]):
    spec = importlib.util.spec_from_file_location('wca_external_checks', DRIVERS / 'wca_acceptance.py')
    wca = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wca)


class WcaDriverTests(unittest.TestCase):
    def test_large_global_bounds_keep_state_and_clock(self) -> None:
        text = wca.render(start=10, stop=5_000_000_010, total=6_000_000_000,
                          stream=11, orientation=12, interval=100)
        self.assertIn('read_restart checkpoint.restart', text)
        self.assertNotIn('create_atoms', text)
        self.assertNotIn('reset_timestep', text)
        commands = [line.split() for line in text.splitlines() if line.startswith('run ')]
        self.assertEqual(sum(int(parts[1]) for parts in commands), 5_000_000_000)
        self.assertTrue(all(parts[2:] == ['start', '0', 'stop', '6000000000'] for parts in commands))
        with self.assertRaises(ValueError):
            wca.render(start=1, stop=1, total=2, stream=11, orientation=12, interval=100)

    def test_dump_parser_rejects_missing_atoms_clock_and_nonfinite_state(self) -> None:
        header = f'ITEM: TIMESTEP\n0\nITEM: NUMBER OF ATOMS\n{wca.N}\nITEM: BOX BOUNDS pp pp pp\n0 {wca.L}\n0 {wca.L}\n-0.5 0.5\nITEM: ATOMS id xu yu mux muy muz\n'
        atoms = ''.join(f'{i} 0 0 1 0 0\n' for i in range(1, wca.N + 1))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'trajectory.dump'
            path.write_text(header + atoms)
            self.assertEqual(len(wca.frames(path, 1, 0, 0)[0]), wca.N)
            with self.assertRaises(ValueError):
                wca.frames(path, 1, 0, 1)
            path.write_text(header + atoms.replace('1 0 0 1 0 0', '1 nan 0 1 0 0', 1))
            with self.assertRaises(ValueError):
                wca.frames(path, 1, 0, 0)
            path.write_text(header + atoms.rsplit('\n', 2)[0] + '\n')
            with self.assertRaises(ValueError):
                wca.frames(path, 1, 0, 0)

    def test_observables_detect_drift_energy_and_boundary_discontinuity(self) -> None:
        duration = 20000
        interval = duration // 100
        initial = [[(i % 16) * wca.L / 16, (i // 16) * wca.L / 16, 1., 0., 0.]
                   for i in range(wca.N)]
        data = {wca.WARM + k * interval:
                [[a[0] + k * .01, *a[1:]] for a in initial] for k in range(101)}
        energy = {step: .25 for step in data}
        result = wca.observables(data, energy, duration)
        self.assertEqual(len(result), wca.PROTOCOL['endpoints'])
        self.assertEqual(result['energy'], .25)
        self.assertAlmostEqual(result['lag-1000-msd'], .05 ** 2 / wca.msd_theory(.1))
        self.assertEqual(result['lag-1000-acf'], 1.)
        changed = {step: [row[:] for row in atoms] for step, atoms in data.items()}
        for row in changed[wca.WARM + 6200]:
            row[0] += 1.
        perturbed = wca.observables(changed, energy, duration)
        self.assertGreater(perturbed['boundary-0.3-cross-msd'], result['boundary-0.3-cross-msd'])
        self.assertAlmostEqual(result['shell-0'], 0.)
        self.assertTrue(all(math.isfinite(value) for value in result.values()))


if __name__ == '__main__':
    unittest.main()
