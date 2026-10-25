"""Explicit lightweight four-job Open MPI affinity acceptance on one Linux host.

Uses fresh isolated state, real scheduler reservations and the common executor.
Cancels only its own jobs once three short overlapping samples are captured.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import socket
import time

from lammps_manager.config import Execution, ManagerConfig, ManagerSettings, Scheduler, Storage, WorkerCapacity, WorkerConfig
from lammps_manager.local import execute_attempt, submit_batch
from lammps_manager.models import Resources
from lammps_manager.scheduler import WorkerSnapshot, schedule
from lammps_manager.store import Store
from lammps_manager.worker import LinuxMonitor

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
fix integrate all nve
thermo 1000000
run 100000000
'''


def sample(root: Path, executable: Path) -> dict:
    """Read only ranks inside this acceptance root, including all task counters."""
    processes = []
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if not (entry / 'exe').samefile(executable) or not (entry / 'cwd').resolve().is_relative_to(root):
                continue
            tasks = []
            for task in (entry / 'task').iterdir():
                fields = (task / 'stat').read_text().rsplit(')', 1)[1].split()
                tasks.append(dict(tid=int(task.name), ticks=int(fields[11]) + int(fields[12]), cpu=int(fields[36])))
            processes.append(dict(pid=int(entry.name), affinity=sorted(os.sched_getaffinity(int(entry.name))), tasks=tasks))
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    return dict(time=time.monotonic(), processes=processes, cpu=LinuxMonitor._cpu(), load=os.getloadavg())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--lammps', type=Path, required=True)
    parser.add_argument('--mpi', default='/usr/bin/mpirun.openmpi')
    args = parser.parse_args()
    root, executable = args.root.absolute(), args.lammps.resolve(strict=True)
    root.mkdir(mode=0o700, exist_ok=False)
    (root / 'jobs').mkdir()
    source = root / 'source'
    source.mkdir()
    (source / 'in.lmp').write_text(INPUT)
    manifest = root / 'batch.json'
    manifest.write_text(json.dumps([dict(source_directory=str(source), input_file='in.lmp')] * 4))
    capacity = WorkerCapacity('affinity', 4, 1, 4)
    config = ManagerConfig(ManagerSettings(str(root / 'manager.sqlite3')), Storage(str(root / 'jobs')),
        (capacity,), Resources(1, 1, .1, 20, 0), Scheduler(account_external_load=False, reserve_memory_gib=0))
    worker = WorkerConfig(capacity, 'http://127.0.0.1:1', str(root / 'jobs'),
        Execution(str(executable), args.mpi), str(root / 'unused.token'))
    monitor = LinuxMonitor()
    time.sleep(.1)
    snapshot = WorkerSnapshot(**{**monitor.sample(worker), 'capacity': capacity})
    with Store(config.manager.database_path) as store:
        jobs = submit_batch(store, config, manifest)
        original = {j: asdict(store.specification(j)) for j in jobs}
        attempts = schedule(store, config, (snapshot,), now=time.time()).assignments
        if len(attempts) != 4:
            raise ValueError('Four jobs did not fit configured admission')
    report = dict(passed=False, host=socket.gethostname(), samples=[], intervals=[],
                  allowed_cpus=sorted(os.sched_getaffinity(0)), scientific_evidence='not_assessed')
    report['topology'] = {str(cpu): [
        (Path('/sys/devices/system/cpu') / f'cpu{cpu}/topology' / name).read_text().strip()
        for name in ('physical_package_id', 'core_id')] for cpu in report['allowed_cpus']}
    def execute(attempt):
        with Store(config.manager.database_path) as store:
            execute_attempt(store, worker, store.attempt(attempt.attempt_id))
    try:
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(execute, attempt) for attempt in attempts]
            try:
                deadline = time.monotonic() + 15
                while time.monotonic() < deadline:
                    current = sample(root, executable)
                    if len(current['processes']) == 4:
                        report['samples'].append(current)
                        if len(report['samples']) == 4:
                            break
                        time.sleep(.5)
                    else:
                        time.sleep(.05)
                if len(report['samples']) != 4:
                    raise ValueError('No four-rank overlapping sample window')
            finally:
                with Store(config.manager.database_path) as store:
                    for attempt in attempts:
                        if store.attempt(attempt.attempt_id).state in ('assigned', 'running'):
                            store.request_cancel(attempt.attempt_id, 'Affinity evidence collected; stop bounded acceptance')
                for future in futures:
                    future.result(timeout=25)
        hz = os.sysconf('SC_CLK_TCK')
        for previous, current in zip(report['samples'], report['samples'][1:]):
            old = {t['tid']: t['ticks'] for p in previous['processes'] for t in p['tasks']}
            usage, cores = {}, set()
            for process in current['processes']:
                if set(process['affinity']) != set(report['allowed_cpus']):
                    raise ValueError('Rank affinity differs from inherited allowed CPU set')
                ticks = 0
                for task in process['tasks']:
                    delta = task['ticks'] - old.get(task['tid'], task['ticks'])
                    ticks += delta
                    if delta > 0:
                        cores.add(tuple(report['topology'][str(task['cpu'])]))
                usage[str(process['pid'])] = 100 * ticks / hz / (current['time'] - previous['time'])
            report['intervals'].append(dict(cpu_percent=usage, active_physical_cores=sorted(cores)))
        if not any(len(i['active_physical_cores']) >= 3 and all(v > 5 for v in i['cpu_percent'].values())
                   and sum(i['cpu_percent'].values()) > 180 for i in report['intervals']):
            raise ValueError('Insufficient simultaneous multi-core CPU execution evidence')
        with Store(config.manager.database_path) as store:
            if store.reservations():
                raise ValueError('Reservations retained after bounded cancellation')
            report['attempts'] = [asdict(store.attempt(a.attempt_id)) for a in attempts]
            for attempt in attempts:
                row = store.attempt(attempt.attempt_id)
                if not row.termination_evidence or asdict(store.specification(row.job_id)) != original[row.job_id]:
                    raise ValueError('Missing termination or changed snapshot')
                metadata = json.loads((Path(row.directory) / 'execution.json').read_text())
                if metadata['argv'][1:5] != ['--bind-to', 'none', '-np', '1']:
                    raise ValueError('Unexpected effective launch arguments')
        report['passed'] = True
    except BaseException as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        report['source_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in Path(__file__).resolve().parents[2].joinpath('src/lammps_manager').glob('*.py')}
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
