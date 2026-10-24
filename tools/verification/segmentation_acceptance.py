"""Explicit T019 polling/queue acceptance on the released singleton ABP build.

Shared and staged runs retain one bound worker throughout each generation.
Cross-host checkpoint reassignment remains outside this verification.
Historical T017 confirmatory evidence retains its original build scope; the new
reference pairs are functional checks, not a replacement equivalence study.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import threading
import time

from lammps_manager import checkpoints as cp, segments as seg
from lammps_manager.config import (Execution, ManagerConfig, ManagerSettings,
    Scheduler, Storage, WorkerCapacity, WorkerConfig)
from lammps_manager.continuation import fingerprint_build, render_abp_input
from lammps_manager.manager import ManagerService, make_server
from lammps_manager.models import Resources
from lammps_manager.store import Store
from lammps_manager.worker import Worker
from lammps_manager.recovery import session_members
from abp_acceptance import frames, observables
from continuation_acceptance import digest, execute


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--lammps', type=Path, required=True)
    parser.add_argument('--mpi', type=Path, default=Path('/usr/bin/mpirun.openmpi'))
    args = parser.parse_args()
    root = args.root.absolute()
    root.mkdir(mode=0o700, exist_ok=False)
    os.environ.pop('DISPLAY', None)
    report = dict(passed=False, root=str(root), modes=[], reference_commands=[],
        scientific_gate='Historical T017 evidence covers its original build only; new pairs are functional checks',
        limitations=['No new model/build/range acceptance; no cross-host compatibility or physical power-loss test'])
    source_paths = sorted(Path('src/lammps_manager').glob('*.py'))
    report['initial_source_sha256'] = {str(path): digest(path) for path in source_paths}
    report['driver_sha256'] = {str(path): digest(path) for path in (Path(__file__),
        Path(__file__).with_name('abp_acceptance.py'), Path(__file__).with_name('continuation_acceptance.py'))}
    try:
        build = fingerprint_build(args.lammps)
        report['build_fingerprint'] = build
        for scenario in ('shared', 'staged', 'single'):
            mode = 'shared' if scenario == 'single' else scenario
            ends = (2000,) if scenario == 'single' else (600, 1400, 2000)
            starts = (0, *ends[:-1])
            mode_root = root / scenario
            mode_root.mkdir()
            manager_root = mode_root / 'manager-files'
            manager_root.mkdir()
            database = mode_root / 'manager.sqlite3'
            capacities = tuple(WorkerCapacity(name, 1, 1, 1) for name in ('a', 'b'))
            config = ManagerConfig(ManagerSettings(str(database), .001), Storage(str(manager_root), mode=mode),
                capacities, Resources(1, 1, .1, 30), Scheduler(account_external_load=False, reserve_memory_gib=0))
            tokens = {}
            for name in ('a', 'b'):
                path = mode_root / (name + '.token')
                path.write_text(os.urandom(32).hex())
                path.chmod(0o600)
                tokens[name] = str(path)
            service = ManagerService(config, tokens)
            server = make_server(service, '127.0.0.1', 0)
            thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
            thread.start()
            workers = {}
            mode_report = dict(mode=mode, scenario=scenario, stages=[], initial_priority=11, continuation_priority=-3)
            report['modes'].append(mode_report)
            try:
                for capacity in capacities:
                    work_root = manager_root if mode == 'shared' else mode_root / ('worker-' + capacity.id)
                    work_root.mkdir(exist_ok=True)
                    workers[capacity.id] = Worker(WorkerConfig(capacity,
                        f'http://127.0.0.1:{server.server_port}', str(work_root),
                        Execution(str(args.lammps), str(args.mpi)), tokens[capacity.id],
                        storage_mode=mode, cleanup_staged_files=False), mode_root / ('journal-' + capacity.id))
                with Store(database) as store:
                    run_id = seg.submit_run(store, config, total_steps=2000,
                        **({} if scenario == 'single' else dict(step_max=800, segment_ends=ends)), build_fingerprint=build,
                        initial_priority=11, continuation_priority=-3, run_id='acceptance-' + scenario)
                all_frames = {}
                last_frames = None
                for index, stop in enumerate(ends):
                    selected = 'a'  # T023 pins the entire run to its first worker environment.
                    worker = workers[selected]
                    with Store(database) as store:
                        for name in workers:
                            store.set_worker(name, paused=name != selected)
                    deadline = time.monotonic() + 45
                    while not worker.futures:
                        worker.tick()
                        if time.monotonic() > deadline:
                            raise RuntimeError('No bounded segment execution started')
                        time.sleep(.01)
                    attempt_id, future = next(iter(worker.futures.items()))
                    future.result(timeout=35)
                    # Stop dispatch between boundaries so the next worker selection
                    # is made by the manager queue, never by a local chaining loop.
                    with Store(database) as store:
                        for name in workers:
                            store.set_worker(name, paused=True)
                    while True:
                        worker.tick()
                        if worker.collection_future:
                            worker.collection_future.result(timeout=10)
                        with Store(database) as store:
                            seg.refresh(store)
                            view = seg.inspect_run(store, run_id)
                            if view['committed_step'] == stop:
                                attempt = store.attempt(attempt_id)
                                checkpoint = cp.record(store, attempt_id)
                                priority = store.assignment_priority(attempt_id)
                                if store.reservations():
                                    raise ValueError('Boundary retains unresolved reservations')
                                break
                        if time.monotonic() > deadline:
                            raise RuntimeError('Checkpoint/results boundary did not commit: ' + str(view))
                        time.sleep(.01)
                    with Store(worker.database) as journal:
                        local_attempt = journal.attempt(attempt_id)
                        work = Path(local_attempt.directory) / 'work'
                        metadata = json.loads((Path(local_attempt.directory) / 'process.json').read_text())
                        if session_members(metadata['pid']):
                            raise ValueError('Segment session is still alive')
                    physical = frames(work / 'trajectory.dump')
                    start = starts[index]
                    if last_frames is not None and last_frames[start] != physical[start]:
                        raise ValueError('Restored physical boundary frame differs')
                    all_frames.update(physical)
                    last_frames = physical
                    mode_report['stages'].append(dict(worker=selected, attempt=asdict(attempt),
                        checkpoint=checkpoint, dispatch=priority, process=metadata,
                        runtime_directory=str(work), parent_progress=stop, parent_status=view['status']))
                if view['status'] != 'succeeded' or set(all_frames) != set(range(0, 2001, 20)):
                    raise ValueError('Parent completion or global output numbering mismatch')
                snapshot = view['snapshot']
                seeds = json.loads(snapshot['stream_provenance'])['seeds']
                namespace = json.loads(snapshot['stream_provenance'])['namespace']
                reference_seed = int.from_bytes(hashlib.sha256(json.dumps([
                    namespace, 'run', 'reference', 0, '0']).encode()).digest()[:8], 'big') % 899999999 + 1
                if reference_seed in seeds:
                    raise ValueError('Reference stream seed collision')
                reference = mode_root / 'uninterrupted'
                reference.mkdir()
                (reference / 'in.lmp').write_text(render_abp_input(start=0, stop=2000, total=2000,
                    stream_seed=reference_seed, position_seed=seeds[0], orientation_seed=seeds[1]))
                execute([str(args.mpi), '-np', '1', str(args.lammps), '-in', 'in.lmp'], reference, report['reference_commands'])
                if session_members(report['reference_commands'][-1]['pid']):
                    raise ValueError('Reference session is still alive')
                reference_frames = frames(reference / 'trajectory.dump')
                if reference_frames[0] != all_frames[0]:
                    raise ValueError('Reference/segmented initial ensemble differs')
                mode_report.update(parent=view, segmented_observables=observables(all_frames),
                    reference_observables=observables(reference_frames), reference_seed=reference_seed,
                    frame_count=len(all_frames), full_boundary_copies_equal=True,
                    worker_reassignment_verified=False, environment_binding=view['environment'])
            finally:
                for worker in workers.values():
                    worker.close()
                server.shutdown()
                thread.join(5)
                server.server_close()
                service.close()
        if fingerprint_build(args.lammps) != build:
            raise ValueError('Build changed during acceptance')
        if {str(path): digest(path) for path in source_paths} != report['initial_source_sha256']:
            raise ValueError('Source changed during acceptance')
        report['passed'] = True
    finally:
        report['source_sha256'] = {str(path): digest(path) for path in sorted(Path('src/lammps_manager').glob('*.py'))}
        report['files'] = {str(path.relative_to(root)): dict(size=path.stat().st_size, sha256=digest(path))
            for path in sorted(root.rglob('*')) if path.is_file() and not path.name.endswith('.token')}
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(passed=report['passed'], root=str(root), modes=len(report['modes']))))


if __name__ == '__main__':
    main()
