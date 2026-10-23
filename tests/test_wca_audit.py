"""Scientific raw inventory corruption checks without simulation or credentials."""
import importlib.util
import json
import gzip
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

DRIVERS = Path(__file__).resolve().parents[1] / 'tools' / 'verification'
with patch.object(sys, 'path', [str(DRIVERS), *sys.path]):
    spec = importlib.util.spec_from_file_location('wca_external_auditor', DRIVERS / 'wca_audit.py')
    auditor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(auditor)


class WcaAuditTests(unittest.TestCase):
    def test_full_audit_reproduces_saved_analysis_and_rejects_changed_values(self) -> None:
        from test_wca_pool import WcaPoolTests

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


if __name__ == '__main__':
    unittest.main()
