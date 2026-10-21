"""Explicit real MPI/LAMMPS acceptance; never imported by default tests.

Run only in a fresh private directory on an authorized host.
"""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
from dataclasses import asdict

import yaml

from lammps_manager.store import Store
from lammps_manager.recovery import session_members

ROOT = Path.cwd()
PYTHON = sys.executable
ENV = {**os.environ, 'PYTHONPATH': str(ROOT / 'src')}
COMMANDS = []
INPUT = '''units lj
atom_style atomic
lattice fcc 0.8442
region box block 0 4 0 4 0 4
create_box 1 box
create_atoms 1 box
mass 1 1.0
pair_style lj/cut 2.5
pair_coeff 1 1 1.0 1.0 2.5
velocity all create 1.44 87287
neighbor 0.3 bin
neigh_modify delay 0 every 1 check yes
fix integrate all nve
thermo 100000
run STEPS
write_data output.data
'''


def cli(config, command, *args):
    argv = [PYTHON, '-m', 'lammps_manager', command, '--config', str(config), *map(str, args)]
    result = subprocess.run(argv, env=ENV, capture_output=True, text=True, timeout=75)
    COMMANDS.append(dict(argv=argv, time=time.time(), returncode=result.returncode,
                         stdout=result.stdout, stderr=result.stderr))
    assert result.returncode == 0, COMMANDS[-1]
    return json.loads(result.stdout)


def setup(name, *, mode='shared', slots=4, count=2, external=False):
    root = ROOT / name
    root.mkdir()
    (root / 'jobs').mkdir()
    manager = dict(manager=dict(database_path=str(root / 'manager.sqlite'),
                               schedule_interval_seconds=.05, heartbeat_timeout_seconds=2),
                   storage=dict(mode=mode, work_root=str(root / 'jobs')),
                   job_defaults=dict(mpi_ranks=2, omp_threads_per_rank=1, memory_gib=.1,
                                     time_limit_seconds=30),
                   scheduler=dict(account_external_load=external, reserve_memory_gib=.1),
                   workers=[dict(id=name, cpu_slots=slots, memory_budget_gib=1,
                                 max_concurrent_jobs=count)])
    worker = dict(worker=manager['workers'][0], manager_endpoint='https://127.0.0.1:18448',
                  work_root=str(root / ('worker' if mode == 'staged' else 'jobs')),
                  token_file=str(root / 'token'), heartbeat_interval_seconds=.1,
                  cleanup_staged_files=False,
                  execution=dict(lammps_path='/usr/bin/lmp',
                                 mpi_launcher_path='/usr/bin/mpirun.openmpi',
                                 mpi_args=['--bind-to', 'none']))
    if mode == 'staged':
        worker['storage_mode'] = mode
        (root / 'worker').mkdir()
    for filename, data in [('manager.yaml', manager), ('worker.yaml', worker)]:
        (root / filename).write_text(yaml.safe_dump(data))
    return root


def submit(root, cases):
    entries = []
    for i, (steps, limit, invalid) in enumerate(cases):
        source = root / ('source-' + str(len(list(root.glob('source-*')))))
        source.mkdir()
        (source / 'in.lmp').write_text('invalid_acceptance_command\n' if invalid else INPUT.replace('STEPS', str(steps)))
        entries.append(dict(source_directory=str(source), input_file='in.lmp',
                            resources=dict(time_limit_seconds=limit)))
    manifest = root / ('batch-' + str(time.time_ns()) + '.json')
    manifest.write_text(json.dumps(entries))
    return cli(root / 'manager.yaml', 'submit', manifest)


def inspect(root, jobs):
    return [cli(root / 'manager.yaml', 'inspect', j) for j in jobs]


def audit(root):
    with Store(root / 'manager.sqlite') as store:
        attempts = [a for j in store.jobs() for a in store.job_attempts(j['job_id'])]
        assert not store.reservations()
    identities = []
    for a in attempts:
        path = Path(a.directory) / 'process.json'
        if path.exists():
            identity = json.loads(path.read_text())
            members = session_members(identity['pid'])
            assert not members, members
            identities.append(dict(identity=identity, members=members))
    return dict(sessions=identities, reservations=[])


def local_acceptance():
    root = setup('v008')
    jobs = submit(root, [(1000, 30, False), (1, 30, True)])
    cli(root / 'manager.yaml', 'run-local', '--worker-config', root / 'worker.yaml')
    records = inspect(root, jobs)
    assert [r['state'] for r in records] == ['succeeded', 'failed'], records
    for record in records:
        assert len(record['attempts']) == 1
    good = Path(records[0]['attempts'][0]['directory'])
    assert 'LAMMPS (' in (good / 'stdout').read_text()
    assert (good / 'work/output.data').is_file()
    # Cancellation is issued through the documented CLI while real MPI is alive.
    later = submit(root, [(100000000, 30, False), (1000, 30, False)])
    argv = [PYTHON, '-m', 'lammps_manager', 'run-local', '--config', str(root / 'manager.yaml'),
            '--worker-config', str(root / 'worker.yaml')]
    with (root / 'run-local.stdout').open('w') as out, (root / 'run-local.stderr').open('w') as err:
        process = subprocess.Popen(argv, env=ENV, stdout=out, stderr=err)
        COMMANDS.append(dict(argv=argv, pid=process.pid, time=time.time()))
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            with Store(root / 'manager.sqlite') as store:
                attempts = store.job_attempts(later[0])
                if attempts and attempts[0].state == 'running':
                    break
            time.sleep(.05)
        else:
            raise AssertionError('Cancellation target did not start')
        cli(root / 'manager.yaml', 'cancel', later[0])
        assert process.wait(timeout=40) == 0
    records += inspect(root, later)
    assert [r['state'] for r in records[2:]] == ['cancelled', 'succeeded']
    cancelled, successor = [r['attempts'][0] for r in records[2:]]
    assert cancelled['ended_at'] <= successor['started_at']
    before = json.dumps(records, sort_keys=True)
    cli(root / 'manager.yaml', 'run-local', '--worker-config', root / 'worker.yaml')
    assert json.dumps(inspect(root, jobs + later), sort_keys=True) == before
    result = dict(records=records, audit=audit(root), commands=COMMANDS,
                  hashes={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in (root / 'jobs').rglob('*') if p.is_file()})
    (ROOT / 'v008.json').write_text(json.dumps(result, indent=2))
    print('V008 passed', flush=True)


if __name__ == '__main__':
    local_acceptance()
