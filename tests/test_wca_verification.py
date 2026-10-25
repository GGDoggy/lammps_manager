"""Offline WCA verification-tool regressions; no real simulations or hosts."""
import importlib.util
import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import json
import gzip
import hashlib
import copy

DRIVERS = Path(__file__).resolve().parents[1] / 'tools' / 'verification'

with patch.object(sys, 'path', [str(DRIVERS), *sys.path]):
    spec = importlib.util.spec_from_file_location('wca_external_checks', DRIVERS / 'wca_acceptance.py')
    wca = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wca)

with patch.object(sys, 'path', [str(DRIVERS), *sys.path]):
    spec = importlib.util.spec_from_file_location('wca_external_pool', DRIVERS / 'wca_pool.py')
    pool = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = pool
    spec.loader.exec_module(pool)

with patch.object(sys, 'path', [str(DRIVERS), *sys.path]):
    spec = importlib.util.spec_from_file_location('wca_external_auditor', DRIVERS / 'wca_audit.py')
    auditor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(auditor)

spec = importlib.util.spec_from_file_location('frozen_pilot_check', Path(__file__).resolve().parents[1] /
                                              'tools/verification/wca_frozen_check.py')

checker = importlib.util.module_from_spec(spec)

spec.loader.exec_module(checker)


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


class WcaAuditTests(unittest.TestCase):
    def test_full_audit_reproduces_saved_analysis_and_rejects_changed_values(self) -> None:
        for mutation in (None, 'endpoint', 'protocol'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                base = Path(temporary)
                root = base / 'confirmation'; root.mkdir()
                pilot, completed = WcaPoolTests().family(root)
                analysis = auditor.analyze(root, pilot, completed)
                self.assertIsInstance(analysis['strata'][0]['layout'], tuple)
                saved = json.loads(json.dumps(analysis))
                if mutation == 'endpoint':
                    saved['strata'][0]['endpoints']['energy']['lower'] += .001
                elif mutation == 'protocol':
                    saved['protocol']['strata'][0][1][0] = 2
                recorded = root / 'analysis.json'
                recorded.write_text(json.dumps(saved))
                original = recorded.read_bytes()
                pilot_path = base / 'pilot.json'; pilot_path.write_text(json.dumps(pilot))
                (base / 'source-sha256.json').write_text('{}')
                (root / 'pool.json').write_text(json.dumps(dict(
                    driver_sha256='hash', pool_sha256='hash', pilot_report_sha256='hash',
                    pending=0, active=[], completed=completed, updated_at=1)))
                output = base / 'audit.json'
                argv = ['wca_audit.py', str(root), '--pilot-report', str(pilot_path),
                        '--output', str(output)]
                with patch.object(sys, 'argv', argv), \
                        patch.object(auditor, 'digest', return_value='hash'), \
                        patch.object(auditor, 'verify_chunk', return_value=dict(
                            replicas=64, files=1, bytes=1)) as verify, \
                        patch.object(auditor, 'root_processes', return_value=[]), \
                        patch('builtins.print'):
                    if mutation is None:
                        auditor.main()
                        result = json.loads(output.read_text())
                        self.assertTrue(result['complete_family'])
                        self.assertTrue(result['scientific_passed'])
                        self.assertTrue(result['raw_endpoints_recomputed'])
                        self.assertEqual(result['analysis'], saved)
                        self.assertTrue(all(call.kwargs['recompute'] for call in verify.call_args_list))
                    else:
                        with self.assertRaisesRegex(ValueError, 'Full-family analysis does not reproduce'):
                            auditor.main()
                        self.assertFalse(output.exists())
                self.assertEqual(recorded.read_bytes(), original)

    def test_original_inventory_matches_and_corruption_never_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / 'chunks/s0-0-1'; directory.mkdir(parents=True)
            raw = directory / 'trajectory.dump.gz'; raw.write_bytes(b'retained raw bytes')
            record = dict(name='s0-0-1', stratum=0, start=0, stop=1)
            pool = dict(driver_sha256='d', pilot_report_sha256='p', boot_id='boot')
            pilot = dict(build='b')
            report = dict(measurements_complete=True, driver_sha256='d', pilot_report_sha256='p',
                          build='b', boot_id='boot', selected_stratum=0,
                          replicas=[dict(key='20000-111-confirm-0',observables={
                              arm:dict.fromkeys(auditor.ENDPOINTS,0.0) for arm in ('reference','segmented')})],
                          commands=[dict(returncode=0, remaining_session_members=[],
                                         identity=dict(boot_id='boot')) for _ in range(5)],
                          retained_files={'trajectory.dump.gz': dict(bytes=raw.stat().st_size,
                                                                    sha256=auditor.digest(raw))})
            path = directory / 'report.json'; path.write_text(json.dumps(report))
            self.assertEqual(auditor.verify_chunk(root, record, pool, pilot)['replicas'], 1)
            raw.write_bytes(b'corrupted raw bytes')
            with self.assertRaisesRegex(ValueError, 'inventory'):
                auditor.verify_chunk(root, record, pool, pilot)
            raw.unlink()
            with self.assertRaises(ValueError):
                auditor.verify_chunk(root, record, pool, pilot)
            raw.write_bytes(b'retained raw bytes')
            extra = directory / 'unexpected'; extra.write_bytes(b'extra')
            with self.assertRaises(ValueError):
                auditor.verify_chunk(root, record, pool, pilot)
            extra.unlink()
            report['commands'][0]['remaining_session_members'] = [123]
            path.write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, 'session'):
                auditor.verify_chunk(root, record, pool, pilot)

    def test_process_audit_excludes_observer_ancestors_but_detects_scientific_children(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base=Path(temporary);root=base/'science';root.mkdir();proc=base/'proc';proc.mkdir()
            for pid,parent,cwd,argv in [
                    (100,90,base,['python3','audit.py',str(root)]),
                    (90,1,base,['bash','-c','python3 audit.py '+str(root)]),
                    (1,0,base,['systemd']),
                    (101,100,root,['lmp','-in','in.lmp']),
                    (102,90,base,['python3','driver.py',str(root)]),
                    (103,90,base,['unrelated'])]:
                entry=proc/str(pid);entry.mkdir()
                (entry/'stat').write_text(f'{pid} (process name) S {parent} 0 0')
                (entry/'cmdline').write_bytes(bytes([0]).join(arg.encode() for arg in argv))
                (entry/'cwd').symlink_to(cwd,target_is_directory=True)
            self.assertEqual(auditor.root_processes(root,proc_root=proc,current_pid=100),[101,102])
            (proc/'101/cwd').unlink();(proc/'101/cmdline').write_bytes(b'')
            self.assertEqual(auditor.root_processes(root,proc_root=proc,current_pid=100),[102])

    def raw_replica(self, root: Path):
        duration = 20000; interval = duration // 100; warm = auditor.WARM
        key = '20000-111-confirm-0'
        atoms = [(index % 16 * auditor.L/16, index // 16 * auditor.L/16, 1.0, 0.0, 0.0)
                 for index in range(auditor.N)]
        atom_rows = '\n'.join(f'{index} ' + ' '.join(format(x, '.17g') for x in atom)
                              for index, atom in enumerate(atoms, 1)) + '\n'
        orientation = auditor.stream_seed(0, 0, 'orientation')
        stages = [('prepare', 0, warm, True), ('reference-0', warm, warm+duration, False),
                  ('segmented-0', warm, warm+6000, False),
                  ('segmented-1', warm+6000, warm+14000, False),
                  ('segmented-2', warm+14000, warm+duration, False)]
        for label, start, stop, initial in stages:
            stage = root / f'{key}-{label}'; stage.mkdir()
            (stage/'in.lmp').write_text(auditor.render(start=start,stop=stop,total=warm+duration,
                stream=auditor.stream_seed(0,0,label),orientation=orientation,interval=interval,initial=initial))
            with gzip.open(stage/'trajectory.dump.gz','wt') as stream:
                for step in range(start,stop+1,interval):
                    stream.write(f'ITEM: TIMESTEP\n{step}\nITEM: NUMBER OF ATOMS\n{auditor.N}\n'
                                 'ITEM: BOX BOUNDS pp pp pp\n'
                                 f'0 {auditor.L:.17g}\n0 {auditor.L:.17g}\n-0.5 0.5\n'
                                 'ITEM: ATOMS id xu yu mux muy muz\n'+atom_rows)
            (stage/'log.lammps').write_text('Step Time Atoms PotEng\n'+''.join(
                f'{step} {step*auditor.DT:.17g} {auditor.N} 0.02\n'
                for step in range(start,stop+1,interval))+'Loop time\n')
        data={step:atoms for step in range(warm,warm+duration+1,interval)}
        energy=dict.fromkeys(data,.02)
        measurements=auditor.observables(data,energy,duration)
        row=dict(key=key,observables={arm:measurements.copy() for arm in ('reference','segmented')})
        return row,dict(name='s0-0-1',stratum=0,start=0,stop=1)

    def test_raw_endpoints_reproduce_and_report_drift_or_changed_input_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);row,record=self.raw_replica(root)
            raw=root/(row['key']+'-reference-0')/'trajectory.dump.gz'
            original=hashlib.sha256(raw.read_bytes()).hexdigest()
            result=auditor.recompute_replica(root,row,record)
            self.assertTrue(result['matched']);self.assertEqual(result['endpoints'],23)
            row['observables']['reference']['energy']+=.001
            with self.assertRaisesRegex(ValueError,'endpoints differ'):
                auditor.recompute_replica(root,row,record)
            row['observables']['reference']['energy']-=.001
            input_path=root/(row['key']+'-segmented-0')/'in.lmp'
            input_path.write_text(input_path.read_text().replace('run 6000 ','run 6001 '))
            with self.assertRaisesRegex(ValueError,'input differs'):
                auditor.recompute_replica(root,row,record)
            self.assertEqual(hashlib.sha256(raw.read_bytes()).hexdigest(),original)

    def test_missing_or_damaged_raw_trajectory_never_reproduces(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);row,record=self.raw_replica(root)
            raw=root/(row['key']+'-prepare')/'trajectory.dump.gz'
            raw.write_bytes(b'not gzip')
            with self.assertRaisesRegex(ValueError,'invalid retained'):
                auditor.recompute_replica(root,row,record)
            raw.unlink()
            with self.assertRaisesRegex(ValueError,'retained trajectory representation'):
                auditor.recompute_replica(root,row,record)


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
