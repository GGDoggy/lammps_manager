"""Read-only verification of original V009 sweep attempts, without resubmission."""
import hashlib, json
from pathlib import Path
from dataclasses import asdict
from real_acceptance import ROOT, audit
from lammps_manager.local import inspect_job
from lammps_manager.store import Store
from lammps_manager.recovery import session_members

def digest(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()
results = []
for mode in ('shared', 'staged'):
    root = ROOT / ('sweep-' + mode)
    with Store(root / 'manager.sqlite') as store:
        specs = sorted((store.specification(j['job_id']) for j in store.jobs()), key=lambda s: s.sweep.seed)
        assert len(specs) == 24 and len({s.job_id for s in specs}) == 24
        records = []
        for (ordinal, s) in enumerate(specs):
            p = s.sweep
            assert p.seed == 1000 + ordinal and p.combination_index == ordinal // 4 + 1 and (p.repetition_index == ordinal % 4 + 1)
            assert p.selected_values == dict(density=[0.7, 0.8, 0.8442][ordinal // 8], temperature=[1.0, 1.44][ordinal // 4 % 2])
            r = inspect_job(store, s.job_id)
            a = r['attempts'][0]
            assert r['state'] == 'succeeded' and len(r['attempts']) == 1 and a['termination_evidence']
            base = Path(a['results_directory']) if mode == 'staged' else Path(a['directory'])
            work = base / 'work'
            assert (work / 'output.data').is_file()
            assert digest(work / 'in.lmp') == p.rendered_sha256
            assert (work / 'aux.txt').read_text() == 'retained auxiliary bytes\n'
            stdout = (base / 'stdout').read_text()
            assert stdout.count('Loop time of') == 1 and f'repetition={p.repetition_index}' in stdout
            identity = json.loads((base / 'process.json').read_text())
            assert not session_members(identity['pid'])
            hashes = {str(f.relative_to(base)): digest(f) for f in base.rglob('*') if f.is_file()}
            if mode == 'staged':
                manifest = a['result_collection']['manifest']
                assert a['result_collection']['status'] == 'collected'
                assert set(hashes) == {f['path'] for f in manifest['files']}
                for f in manifest['files']:
                    assert (base / f['path']).stat().st_size == f['size'] and hashes[f['path']] == f['sha256']
            records.append(dict(specification=asdict(s), attempt=a, hashes=hashes))
        assert not store.reservations()
    results.append(dict(mode=mode, records=records, audit=audit(root)))
boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
assert boot == 'c5831636-0ddc-4c03-9c62-de8bd57e8277'
(ROOT / 'v009-sweep-audit.json').write_text(json.dumps(dict(boot_id=boot, results=results), indent=2))
print('48 original attempts verified; no resubmission')
