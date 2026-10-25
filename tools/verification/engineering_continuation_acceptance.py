"""Explicit isolated T021 engineering restart smoke; launches real MPI/LAMMPS.

No ensemble/equivalence claims. Use a new root and an authorized resource budget.
Retains failures, checkpoints, output, manager restart and retry evidence.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import sys
import threading
import time

from lammps_manager import checkpoints as cp, segments as seg
from lammps_manager.config import (Execution, ManagerConfig, ManagerSettings,
    Scheduler, Storage, WorkerCapacity, WorkerConfig)
from lammps_manager.continuation import (Recipe, fingerprint_build,
    require_contract, ENGINEERING_VERSION)
from lammps_manager.manager import ManagerService, make_server
from lammps_manager.models import Resources, InfrastructureRecovery
from lammps_manager.recovery import session_members
from lammps_manager.store import Store
from lammps_manager.worker import Worker


def digest(path: Path) -> str:
    """Stream retained artifacts without loading simulation files into memory."""
    value = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def boundary_frames(path: Path) -> dict[int, tuple[str, ...]]:
    """Read sorted full-precision physical frames for the fixed WCA smoke."""
    result = {}
    import gzip
    archive = Path(str(path) + '.gz')
    with (gzip.open(archive, 'rt') if archive.exists() else path.open()) as source:
        while line := source.readline():
            if line != 'ITEM: TIMESTEP\n':
                raise ValueError('Invalid trajectory header')
            step = int(source.readline())
            if source.readline() != 'ITEM: NUMBER OF ATOMS\n' or int(source.readline()) != 256:
                raise ValueError('Invalid WCA particle count')
            header = tuple(source.readline().rstrip() for _ in range(5))
            columns = header[-1].removeprefix('ITEM: ATOMS ').split()
            if columns not in (['id', 'xu', 'yu', 'mux', 'muy', 'muz'],
                               ['id', 'x', 'y', 'ix', 'iy', 'mux', 'muy', 'muz']):
                raise ValueError('Invalid physical frame schema')
            atoms = tuple(source.readline().rstrip() for _ in range(256))
            if step in result or any(len(row.split()) != len(columns) for row in atoms):
                raise ValueError('Duplicate/incomplete physical frame')
            result[step] = header + atoms
    return result


def run_scenario(root: Path, lammps: Path, mpi: Path, ranks: int, mode: str,
                 report: dict) -> None:
    """Run three unequal segments, one forced launcher failure and manager restart."""
    root.mkdir()
    files = root / 'manager-files'
    files.mkdir()
    database = root / 'manager.sqlite3'
    token = root / 'worker.token'
    token.write_text(os.urandom(32).hex())
    token.chmod(0o600)
    # Fail exactly the second launcher invocation, before real MPI starts. The
    # normal attempt records the failure; an explicit retry reuses immutable input.
    launcher = root / 'launcher'
    launcher.write_text('#!' + sys.executable + '\n'
        'from pathlib import Path\nimport os,sys\n'
        f'counter=Path({str(root / "launch-count")!r})\n'
        'count=int(counter.read_text())+1 if counter.exists() else 1\n'
        'counter.write_text(str(count))\n'
        'if count==2: sys.exit(17)\n'
        f'os.execv({str(mpi)!r}, [{str(mpi)!r}, *sys.argv[1:]])\n')
    launcher.chmod(0o700)
    capacity = WorkerCapacity('smoke', ranks, 1, 1)
    config = ManagerConfig(ManagerSettings(str(database), .001), Storage(str(files), mode=mode),
        (capacity,), Resources(ranks, 1, .1, 30, 1),
        Scheduler(account_external_load=False, reserve_memory_gib=0),
        infrastructure_recovery=InfrastructureRecovery(max_retries=0))
    recipe = Recipe('wca-abp-2d-v1', 'statistical', ranks, 1)
    build = fingerprint_build(lammps)
    require_contract(recipe, build_fingerprint=build, total_steps=180,
                     segment_ends=(40, 100, 180), version=ENGINEERING_VERSION)
    service = ManagerService(config, {'smoke': str(token)})
    server = make_server(service, '127.0.0.1', 0)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
    thread.start()
    work_root = files if mode == 'shared' else root / 'worker-files'
    work_root.mkdir(exist_ok=True)
    worker = None
    evidence = dict(mode=mode, stages=[], manager_restarted=False, retried=False)
    report['scenarios'].append(evidence)
    run_id = None
    last = None
    retry_inputs = None
    deadline = time.monotonic() + 180
    try:
        worker = Worker(WorkerConfig(capacity, f'http://127.0.0.1:{port}', str(work_root),
            Execution(str(lammps), str(launcher)), str(token), storage_mode=mode,
            cleanup_staged_files=False), root / 'journal')
        with Store(database) as store:
            run_id = seg.submit_run(store, config, total_steps=180, step_max=80,
                segment_ends=(40, 100, 180), build_fingerprint=build, recipe=recipe, run_id='engineering-smoke',
                sampling=dict(unit='step', interval=20, count=9))
            original = seg.inspect_run(store, run_id)['snapshot']
        while True:
            worker.tick()
            for future in tuple(worker.futures.values()):
                future.result(timeout=35)
                with Store(database) as store:
                    store.set_worker('smoke', paused=True)
            if worker.collection_future:
                worker.collection_future.result(timeout=15)
            with Store(database) as store:
                view = seg.inspect_run(store, run_id)
                if view['snapshot'] != original:
                    raise ValueError('Immutable allocation changed')
                if view['status'] == 'failed' and not evidence['retried']:
                    job = view['segments'][-1]['job_id']
                    attempt = store.job_attempts(job)[-1]
                    collection = store.result_collection(attempt.attempt_id)
                    if not collection or collection['status'] != 'collected':
                        if time.monotonic() > deadline:
                            raise RuntimeError('Failed output collection exceeded deadline')
                        time.sleep(.01)
                        continue
                    if attempt.exit_code != 17 or not attempt.termination_evidence or store.reservations():
                        raise ValueError('Injected failure not conclusively terminated')
                    retry_inputs = asdict(store.specification(job))
                    evidence['failed_attempt'] = asdict(attempt)
                    store.retry(job)
                    evidence['retried'] = True
                    store.set_worker('smoke', paused=False)
                for item in view['segments']:
                    if not item['completed'] or item['ordinal'] < len(evidence['stages']):
                        continue
                    attempt = store.job_attempts(item['job_id'])[-1]
                    contract = cp.record(store, attempt.attempt_id)
                    if not contract['acknowledged'] or store.result_collection(attempt.attempt_id)['status'] != 'collected':
                        raise ValueError('Boundary lacks durable checkpoint/results')
                    collected = Path(attempt.directory) / 'results'
                    manifest = store.result_collection(attempt.attempt_id)['manifest']
                    for entry in manifest['files']:
                        path = collected / entry['path']
                        if path.stat().st_size != entry['size'] or digest(path) != entry['sha256']:
                            raise ValueError('Collected result differs from committed manifest')
                    cp.validate_boundary(collected, contract['contract'])
                    physical = boundary_frames(collected / 'work/trajectory.dump')
                    expected_steps = set(range(item['start_step'] + 20, item['stop_step'] + 1, 20))
                    if set(physical) != expected_steps:
                        raise ValueError('Trajectory differs from immutable global schedule')
                    if item['ordinal'] == 0:
                        preflight = boundary_frames(collected / 'work/engineering-smoke-first.dump')
                        initial = boundary_frames(collected / 'work/initial.dump')
                        def physical_atoms(frame):
                            columns = frame[4].removeprefix('ITEM: ATOMS ').split()
                            # Images start at zero in the saved initial state.
                            names = ('id', 'x' if 'x' in columns else 'xu',
                                     'y' if 'y' in columns else 'yu', 'mux', 'muy', 'muz')
                            return [tuple(row.split()[columns.index(name)] for name in names) for row in frame[5:]]
                        if set(initial) != {0} or physical_atoms(preflight[0]) != physical_atoms(initial[0]):
                            raise ValueError('Production initial state differs from saved preflight boundary')
                    last = physical
                    process = json.loads((collected / 'process.json').read_text())
                    if session_members(process['pid']) or not attempt.termination_evidence or store.reservations():
                        raise ValueError('Unresolved process/reservation at boundary')
                    if item['ordinal'] == 1 and asdict(store.specification(item['job_id'])) != retry_inputs:
                        raise ValueError('Retry changed generated input')
                    evidence['stages'].append(dict(segment=dict(item), attempt=asdict(attempt), checkpoint=contract))
                    store.set_worker('smoke', paused=False)
                if view['status'] == 'succeeded':
                    if not evidence['retried'] or len(evidence['stages']) != 3 or view['committed_step'] != 180:
                        raise ValueError('Incomplete engineering smoke')
                    evidence['parent'] = view
                    break
            if view['committed_step'] == 40 and not evidence['manager_restarted']:
                server.shutdown()
                thread.join(5)
                server.server_close()
                service.close()
                service = ManagerService(config, {'smoke': str(token)})
                server = make_server(service, '127.0.0.1', port)
                thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
                thread.start()
                evidence['manager_restarted'] = True
            if time.monotonic() > deadline:
                raise RuntimeError('Engineering smoke exceeded its observation deadline')
            time.sleep(.01)
        launches = []
        for path in sorted(work_root.glob('*/*/execution.json')):
            metadata = json.loads(path.read_text())
            if metadata['argv'][1:5] != ['--bind-to', 'none', '-np', str(ranks)]:
                raise ValueError('Initial/successor/retry MPI launch policy differs')
            launches.append(metadata['argv'])
        if len(launches) != 4:
            raise ValueError('Missing initial/successor/retry launch metadata')
        evidence['effective_launches'] = launches
        if not evidence['manager_restarted']:
            raise ValueError('Manager restart was not exercised')
    finally:
        # Persist cancellation before cleanup; never infer termination from a
        # polling timeout. Journals and reservations remain available on failure.
        with Store(database) as store:
            if run_id is not None and seg.inspect_run(store, run_id)['status'] != 'succeeded':
                seg.cancel_run(store, run_id)
        if worker is not None:
            worker.close()
        server.shutdown()
        thread.join(5)
        server.server_close()
        service.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--lammps', required=True, type=Path)
    parser.add_argument('--mpi', default=Path('/usr/bin/mpirun.openmpi'), type=Path)
    parser.add_argument('--ranks', default=1, type=int)
    args = parser.parse_args()
    root = args.root.absolute()
    root.mkdir(mode=0o700, exist_ok=False)
    report = dict(passed=False, scientific_evidence='not_assessed', scenarios=[],
        limitations=['No scientific equivalence, stream independence, power-loss or cross-host claim'])
    source = list(Path('src/lammps_manager').glob('*.py')) + [Path(__file__)]
    report['source_sha256'] = {str(path): digest(path) for path in source}
    try:
        for mode in ('shared', 'staged'):
            run_scenario(root / mode, args.lammps.resolve(), args.mpi.resolve(), args.ranks, mode, report)
        if report['source_sha256'] != {str(path): digest(path) for path in source}:
            raise ValueError('Source changed during acceptance')
        report['passed'] = True
    except BaseException as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        report['files'] = {str(path.relative_to(root)): dict(size=path.stat().st_size, sha256=digest(path))
            for path in root.rglob('*') if path.is_file() and path != root / 'report.json' and not path.name.endswith('.token')}
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
