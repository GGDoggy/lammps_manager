"""Explicit T018 real restart restoration through shared and staged publications.

Run only on an authorized compatible host, in a new private root. Six bounded
singleton simulations test storage/restore, not a new scientific equivalence gate.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import shutil
from uuid import uuid4

from lammps_manager import checkpoints as cp
from lammps_manager.continuation import Recipe, fingerprint_build, render_abp_input
from lammps_manager.models import InputFile, JobSpecification, Resources
from lammps_manager.store import Store
from abp_acceptance import frames
from continuation_acceptance import digest, execute


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--lammps', type=Path, required=True)
    parser.add_argument('--mpi', type=Path, default=Path('/usr/bin/mpirun.openmpi'))
    args = parser.parse_args()
    root = args.root.absolute()
    root.mkdir(mode=0o700, exist_ok=False)
    report = dict(passed=False, root=str(root), commands=[], checkpoints=[],
                  scope='T018 storage/restore only; no new scientific or cross-worker acceptance')
    try:
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        session = str(uuid4())
        build = fingerprint_build(args.lammps)
        report['build_fingerprint'] = build
        limits = dict(max_files=10, max_file_bytes=64 * 1024 * 1024, max_total_bytes=128 * 1024 * 1024)
        namespace = 't018-storage-restore-v1'
        identities = [('setup', 'positions'), ('setup', 'orientations')]
        identities += [('segmented', str(start)) for start in (0, 600, 1400)]
        seeds = [int.from_bytes(hashlib.sha256(json.dumps([namespace, 'restore', arm, 0, segment]).encode())
                 .digest()[:8], 'big') % 899999999 + 1 for arm, segment in identities]
        provenance = cp.canonical(dict(namespace=namespace, phase='restore', replica=0, seeds=seeds))
        os.environ.pop('DISPLAY', None)
        for mode in ('shared', 'staged'):
            database = root / (mode + '.sqlite3')
            previous = None
            previous_frames = None
            for index, (start, stop) in enumerate(((0, 600), (600, 1400), (1400, 2000))):
                job = f'{mode}-{index}'
                job_root = root / job
                work = job_root / 'attempt' / 'work'
                work.mkdir(parents=True)
                script = render_abp_input(start=start, stop=stop, total=2000, stream_seed=seeds[index + 2],
                                          position_seed=seeds[0], orientation_seed=seeds[1])
                (work / 'in.lmp').write_text(script)
                if previous:
                    with Store(database) as store:
                        spec = JobSpecification(job, str(job_root), 'in.lmp', digest(work / 'in.lmp'),
                            Resources(1, 1), mode, (InputFile('in.lmp', len(script.encode()), digest(work / 'in.lmp')),))
                        store.submit(spec)
                        cp.pin(store, previous, job)
                        store.assign(job, job, 'validated-host', str(work.parent))
                        with cp.fetch(store, 'validated-host', job, 0, build)[0] as incoming:
                            with (work / 'checkpoint.restart').open('xb') as outgoing:
                                shutil.copyfileobj(incoming, outgoing)
                with Store(database) as store:
                    store.worker_generation('validated-host', boot, session)
                    if not previous:
                        store.submit(JobSpecification(job, str(job_root), 'in.lmp', digest(work / 'in.lmp'),
                            Resources(1, 1), mode, (InputFile('in.lmp', len(script.encode()), digest(work / 'in.lmp')),)))
                        store.assign(job, job, 'validated-host', str(work.parent))
                    contract = cp.CheckpointContract(mode, index, job, job,
                        Recipe('ideal-abp-2d-v1', 'statistical'), build, 2000, (600, 1400, 2000),
                        stop, stop * .005, provenance, parent_checkpoint_id=previous, execution_boot_id=boot)
                    cp.expect(store, contract)
                    store.transition(job, 'running')
                execute([str(args.mpi), '-np', '1', str(args.lammps), '-in', 'in.lmp'], work, report['commands'])
                # Do not infer MPI termination from the launcher alone.
                from lammps_manager.recovery import session_members
                if session_members(report['commands'][-1]['pid']):
                    raise ValueError('MPI launch session remains alive')
                shutil.copyfile(work / 'stdout', work.parent / 'stdout')
                cp.validate_boundary(work.parent, json.loads(cp.canonical(asdict(contract))))
                data = frames(work / 'trajectory.dump')
                if previous_frames is not None and previous_frames[start] != data[start]:
                    raise ValueError('Checkpoint physical boundary changed on restore')
                previous_frames = data
                restart = work / 'checkpoint.restart'
                manifest = json.loads(cp.canonical(dict(contract=asdict(contract), files=[asdict(
                    InputFile('checkpoint.restart', restart.stat().st_size, digest(restart)))])))
                with Store(database) as store:
                    store.transition(job, 'succeeded', exit_code=0,
                                     termination_evidence='wait reaped launcher; launch session empty')
                    cp.operation(store, 'validated-host', job, 'manifest', limits, payload=manifest)
                    if mode == 'staged':
                        with restart.open('rb') as incoming:
                            cp.operation(store, 'validated-host', job, '0', limits,
                                         stream=incoming, length=restart.stat().st_size)
                    acknowledgement = cp.operation(store, 'validated-host', job, 'commit', limits,
                                                   shared_source=work if mode == 'shared' else None)
                    previous = acknowledgement['identity']
                # Reopen for a lost-response/restart replay; no new simulation.
                with Store(database) as store:
                    replay = cp.operation(store, 'validated-host', job, 'commit', limits)
                    if replay != acknowledgement:
                        raise ValueError('Checkpoint replay identity changed')
                report['checkpoints'].append(dict(mode=mode, step=stop, identity=previous,
                    manifest=manifest, directory=str(work), boundary_restored=start > 0))
        report['passed'] = True
    finally:
        report['source_sha256'] = {str(path): digest(path) for path in
            sorted(Path('src/lammps_manager').glob('*.py'))}
        report['files'] = {str(path.relative_to(root)): dict(size=path.stat().st_size, sha256=digest(path))
            for path in sorted(root.rglob('*')) if path.is_file()}
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(passed=report['passed'], root=str(root), checkpoints=len(report['checkpoints']))))


if __name__ == '__main__':
    main()
