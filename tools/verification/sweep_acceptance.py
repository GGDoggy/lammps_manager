"""Real bounded Cartesian sweep acceptance in shared and staged modes."""
import hashlib, json, os, time
from pathlib import Path
from real_acceptance import ROOT, INPUT, setup, cli, inspect, audit, COMMANDS
from distributed_acceptance import prepare, manager, worker, stop, wait, terminal, TIMELINE
from lammps_manager.store import Store

def run() -> None:
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    results = []
    os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:4])
    for mode in ('shared', 'staged'):
        root = setup('sweep-' + mode, mode=mode)
        source = root / 'template'
        source.mkdir()
        text = INPUT.replace('0.8442', '{{density}}').replace('1.44 87287', '{{temperature}} {{seed}}').replace('STEPS', '1000') + 'print "repetition={{repetition}}"\n'
        (source / 'in.lmp').write_text(text)
        (source / 'aux.txt').write_text('retained auxiliary bytes\n')
        manifest = root / 'sweep.json'
        manifest.write_text(json.dumps(dict(version=1, source_directory='template', input_file='in.lmp', template_version='v009-lj', parameters=dict(density=[0.7, 0.8, 0.8442], temperature=[1.0, 1.44]), repetitions=4, seed_base=1000, resources=dict(mpi_ranks=2, omp_threads_per_rank=1))))
        preview = cli(root / 'manager.yaml', 'submit-sweep', manifest, '--dry-run')
        assert preview['jobs'] == 24 and (not (root / 'manager.sqlite').exists())
        submitted = cli(root / 'manager.yaml', 'submit-sweep', manifest)
        jobs = [r['job_id'] for r in submitted['submitted']]
        assert len(set(jobs)) == 24
        with Store(root / 'manager.sqlite') as store:
            specs = {j: store.specification(j) for j in jobs}
        prepare(root)
        m = manager(root)
        w = None
        try:
            w = worker(root)
            wait(root, lambda v: terminal(v, jobs), 'sweep-complete', timeout=180)
            if mode == 'staged':

                def delivered(v):
                    with Store(root / 'manager.sqlite') as store:
                        return all(((store.result_collection(store.job_attempts(j)[0].attempt_id) or {}).get('status') == 'collected' for j in jobs))
                wait(root, delivered, 'all-results-collected', timeout=90)
            records = inspect(root, jobs)
            hashes = {}
            for (ordinal, r) in enumerate(records):
                assert r['state'] == 'succeeded' and len(r['attempts']) == 1, r
                spec = specs[jobs[ordinal]]
                metadata = spec.sweep
                assert metadata.seed == 1000 + ordinal and metadata.repetition_index == ordinal % 4 + 1
                assert metadata.combination_index == ordinal // 4 + 1
                a = r['attempts'][0]
                assert a['termination_evidence']
                out = Path(a['results_directory']) if mode == 'staged' else Path(a['directory']) / 'work'
                work = out / 'work' if mode == 'staged' else out
                assert (work / 'output.data').is_file()
                if mode == 'staged':
                    for item in a['result_collection']['manifest']['files']:
                        p = out / item['path']
                        assert p.stat().st_size == item['size'] and hashlib.sha256(p.read_bytes()).hexdigest() == item['sha256']
                hashes[jobs[ordinal]] = hashlib.sha256((work / 'output.data').read_bytes()).hexdigest()
            with Store(root / 'manager.sqlite') as store:
                assert all((store.specification(j) == specs[j] for j in jobs))
            assert (source / 'in.lmp').read_text() == text
            results.append(dict(mode=mode, preview=preview, submission=submitted, records=records, outputs=hashes, audit=audit(root)))
        finally:
            if w is not None:
                stop(w)
            stop(m)
    assert Path('/proc/sys/kernel/random/boot_id').read_text().strip() == boot
    (ROOT / 'v009-sweeps.json').write_text(json.dumps(dict(boot_id=boot, results=results, commands=COMMANDS), indent=2))
    print('48 real sweep jobs passed', flush=True)
if __name__ == '__main__':
    run()
