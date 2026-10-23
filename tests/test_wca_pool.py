"""Frozen scientific chunk coverage and exclusive CPU allocations, without jobs."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

DRIVERS = Path(__file__).resolve().parents[1] / 'tools' / 'verification'
with patch.object(sys, 'path', [str(DRIVERS), *sys.path]):
    spec = importlib.util.spec_from_file_location('wca_external_pool', DRIVERS / 'wca_pool.py')
    pool = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = pool
    spec.loader.exec_module(pool)


class WcaPoolTests(unittest.TestCase):
    def pilot(self):
        return dict(protocol=json.loads(json.dumps(pool.PROTOCOL)), strata=[
            dict(duration=duration, layout=list(layout), feasible=True, pairs=64 + index * 13)
            for index, (duration, layout) in enumerate(pool.STRATA)])

    def test_all_frozen_replicas_are_covered_exactly_once(self) -> None:
        pilot = self.pilot()
        chunks = pool.plan_chunks(pilot)
        coordinates = set()
        for chunk in chunks:
            self.assertLessEqual(chunk.stop - chunk.start, 64)
            self.assertEqual(chunk.ranks, max(pool.STRATA[chunk.stratum][1]))
            for replica in range(chunk.start, chunk.stop):
                coordinate = (chunk.stratum, replica)
                self.assertNotIn(coordinate, coordinates)
                coordinates.add(coordinate)
        self.assertEqual(coordinates, {(index, replica) for index, row in enumerate(pilot['strata'])
                                       for replica in range(row['pairs'])})
        pilot['strata'][1] = pilot['strata'][0]
        with self.assertRaises(ValueError):
            pool.plan_chunks(pilot)

    def test_allocations_never_overlap_or_spend_partial_capacity(self) -> None:
        free = set(range(10))
        first = pool.allocate(free, 4)
        second = pool.allocate(free, 4)
        before = free.copy()
        self.assertIsNone(pool.allocate(free, 4))
        self.assertEqual(free, before)
        third = pool.allocate(free, 2)
        self.assertEqual(len(set(first) | set(second) | set(third)), 10)
        self.assertFalse(free)
        free.update(first)
        self.assertEqual(pool.allocate(free, 4), first)

    def test_partial_family_cannot_be_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'missing frozen'):
                pool.analyze(Path(directory), self.pilot(), [])
        pilot = self.pilot()
        pilot['error'] = 'pilot failed'
        with self.assertRaises(ValueError):
            pool.plan_chunks(pilot)

    def test_pool_passes_resolved_selected_executable_to_required_guard(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            executable = base / 'custom-lmp'; executable.write_bytes(b'binary')
            alias = base / 'lmp-link'; alias.symlink_to(executable)
            pilot = base / 'pilot.json'; pilot.write_text(json.dumps(self.pilot()))
            root = base / 'confirmation'
            argv = ['wca_pool.py', '--pilot-report', str(pilot), '--pilot-driver', str(executable),
                    '--lammps', str(alias), '--root', str(root), '--enforce-rank-affinity']
            with patch.object(sys, 'argv', argv), \
                    patch.object(pool.os, 'open', return_value=123), \
                    patch.object(pool.fcntl, 'flock'), \
                    patch.object(pool.os, 'sched_getaffinity', return_value=set(range(8))), \
                    patch.object(pool.os, 'cpu_count', return_value=16), \
                    patch.object(pool.os, 'sched_setaffinity'), \
                    patch.object(pool, 'process_identity', return_value={'pid': 123}), \
                    patch.object(pool.subprocess, 'Popen') as popen:
                # Stop before starting any measurement; a required guard failure
                # must abort the pool, and the launch must identify our binary.
                popen.return_value.poll.return_value = 1
                with self.assertRaisesRegex(ValueError, 'affinity guard exited'):
                    pool.main()
            popen.assert_called_once()
            self.assertEqual(popen.call_args.args[0][-3:],
                             [str(root), '--lammps', str(executable.resolve())])

    def family(self, root: Path):
        pilot = self.pilot()
        for row in pilot['strata']:
            row['pairs'] = 64
        # These are the declared scientific endpoints, independent of analyzer iteration.
        names = ['energy'] + [f'shell-{i}' for i in range(4)] + [f'structure-{i}' for i in range(2)]
        names += [f'lag-{lag}-{metric}' for lag in (1000, 2000) for metric in ('msd', 'acf')]
        names += [f'boundary-{fraction}-{window}-{metric}' for fraction in (.3, .7)
                  for window in ('before', 'cross', 'after') for metric in ('msd', 'acf')]
        completed = []
        for index, (duration, layout) in enumerate(pool.STRATA):
            name = f's{index}-0-64'; directory = root / 'chunks' / name; directory.mkdir(parents=True)
            prefix = f'{duration}-' + ''.join(map(str, layout)) + '-confirm-'
            rows = [dict(key=prefix + str(replica), observables={
                arm: dict.fromkeys(names, 0.0) for arm in ('reference', 'segmented')}) for replica in range(64)]
            (directory / 'report.json').write_text(json.dumps(dict(measurements_complete=True,
                selected_stratum=index, protocol=pilot['protocol'], replicas=rows)))
            completed.append(dict(name=name, stratum=index, start=0, stop=64))
        return pilot, completed

    def test_full_family_requires_all_declared_endpoints_in_every_arm(self) -> None:
        for mutation in ('empty', 'missing', 'extra', 'boolean', 'infinite', 'extra-arm'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); pilot, completed = self.family(root)
                path = root / 'chunks' / completed[0]['name'] / 'report.json'
                report = json.loads(path.read_text())
                for row in report['replicas']:
                    if mutation == 'empty':
                        row['observables'] = dict(reference={}, segmented={})
                    elif mutation == 'missing':
                        for arm in row['observables'].values():
                            del arm['energy']
                    elif mutation == 'extra':
                        for arm in row['observables'].values():
                            arm['unplanned'] = 0.0
                    elif mutation == 'boolean':
                        row['observables']['reference']['energy'] = False
                    elif mutation == 'infinite':
                        row['observables']['reference']['energy'] = float('inf')
                    else:
                        row['observables']['extra'] = {}
                path.write_text(json.dumps(report))
                with self.assertRaises(ValueError):
                    pool.analyze(root, pilot, completed)

    def test_valid_full_family_passes_and_detects_a_failed_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); pilot, completed = self.family(root)
            result = pool.analyze(root, pilot, completed)
            self.assertTrue(result['passed'])
            self.assertEqual(len(result['strata']), 10)
            self.assertTrue(all(len(row['endpoints']) == 23 for row in result['strata']))
            path = root / 'chunks' / completed[-1]['name'] / 'report.json'
            report = json.loads(path.read_text())
            for row in report['replicas']:
                row['observables']['segmented']['energy'] = .06
            path.write_text(json.dumps(report))
            result = pool.analyze(root, pilot, completed)
            self.assertFalse(result['passed'])
            self.assertFalse(result['strata'][-1]['endpoints']['energy']['equivalent'])

    def test_reuse_requires_compatible_complete_audited_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); old = base / 'old'; new = base / 'new'
            (old / 'chunks').mkdir(parents=True); (new / 'chunks').mkdir(parents=True)
            pilot = self.pilot(); pending = pool.plan_chunks(pilot)
            chunk = pending[0]; (old / 'chunks' / chunk.name).mkdir()
            record = dict(name=chunk.name, stratum=chunk.stratum, start=chunk.start, stop=chunk.stop)
            prior = dict(pilot_report_sha256='pilot', driver_sha256='driver',
                         boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                         completed=[record], active=[{'name': 'interrupted'}])
            (old / 'pool.json').write_text(json.dumps(prior))
            with patch.object(sys, 'path', [str(DRIVERS), *sys.path]), patch('wca_audit.verify_chunk', return_value={'replicas': 64}) as verify:
                reused = pool.reuse_completed(old, new, pilot, 'pilot', 'driver', pending)
            verify.assert_called_once_with(old, record, prior, pilot)
            self.assertEqual(len(reused), 1)
            self.assertNotIn(chunk, pending)
            self.assertEqual((new / 'chunks' / chunk.name).resolve(), (old / 'chunks' / chunk.name).resolve())
            self.assertFalse((new / 'chunks/interrupted').exists())
            with self.assertRaisesRegex(ValueError, 'differs'):
                pool.reuse_completed(old, new, pilot, 'different', 'driver', pending)

    def test_corrupt_reused_chunk_never_changes_pending_or_creates_link(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory); old = base / 'old'; new = base / 'new'
            old.mkdir(); (new / 'chunks').mkdir(parents=True)
            pilot = self.pilot(); pending = pool.plan_chunks(pilot); before = pending.copy()
            chunk = pending[0]
            prior = dict(pilot_report_sha256='pilot', driver_sha256='driver',
                         boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                         completed=[dict(name=chunk.name, stratum=chunk.stratum, start=chunk.start, stop=chunk.stop)])
            (old / 'pool.json').write_text(json.dumps(prior))
            with patch.object(sys, 'path', [str(DRIVERS), *sys.path]), patch('wca_audit.verify_chunk', side_effect=ValueError('corrupt inventory')):
                with self.assertRaisesRegex(ValueError, 'corrupt'):
                    pool.reuse_completed(old, new, pilot, 'pilot', 'driver', pending)
            self.assertEqual(pending, before)
            self.assertEqual(list((new / 'chunks').iterdir()), [])


if __name__ == '__main__':
    unittest.main()
