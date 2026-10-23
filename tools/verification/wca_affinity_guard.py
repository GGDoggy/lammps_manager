"""Keep owned WCA ranks inside their existing half-host pool allocations."""
import argparse
import json
import os
from pathlib import Path
import time

from lammps_manager.recovery import process_identity


def enforce_allocations(root: Path, pool: dict, executable: Path, *,
                        proc_root: Path = Path('/proc')) -> list[dict]:
    """Correct only selected-executable ranks inside active chunk directories."""
    cpus = set(pool['allowed_cpus'])
    allocation = {record['name']: set(record['cpus']) for record in pool['active']}
    changes = []
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if entry.stat().st_uid != os.getuid():
                continue
            pid = int(entry.name)
            saved = process_identity(pid)
            if not (entry / 'exe').samefile(executable):
                continue
            cwd = (entry / 'cwd').resolve()
            relative = cwd.relative_to(root / 'chunks')
            desired = allocation.get(relative.parts[0])
            if not desired:
                continue
            if not desired.issubset(cpus):
                raise ValueError('Chunk allocation exceeds pool CPU budget')
            observed = set(os.sched_getaffinity(pid))
            if observed != desired and process_identity(pid) == saved:
                os.sched_setaffinity(pid, desired)
                changes.append(dict(time=time.time(), identity=saved,
                                    chunk=relative.parts[0], before=sorted(observed),
                                    after=sorted(os.sched_getaffinity(pid))))
        except (FileNotFoundError, ProcessLookupError, PermissionError, ValueError) as exc:
            if isinstance(exc, ValueError) and str(exc) == 'Chunk allocation exceeds pool CPU budget':
                raise
            continue  # A process outside this root or one that exited is not ours to change.
    return changes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--lammps', type=Path, required=True,
                        help='Selected LAMMPS executable; symlinks are resolved')
    args = parser.parse_args()
    executable = args.lammps.resolve(strict=True)
    root = args.root.resolve(strict=True)
    initial = json.loads((root / 'pool.json').read_text())
    cpus = set(initial['allowed_cpus'])
    if len(cpus) > max(1, (os.cpu_count() or 1) // 2):
        raise ValueError('Pool exceeds half-host CPU budget')
    os.sched_setaffinity(0, cpus)
    with (root.parent / 'affinity-guard.jsonl').open('a') as log:
        while True:
            try:
                current = process_identity(initial['identity']['pid'])
            except FileNotFoundError:
                return
            if current != initial['identity']:
                return  # Original pool ended; never follow a reused PID.
            pool = json.loads((root / 'pool.json').read_text())
            for change in enforce_allocations(root, pool, executable):
                log.write(json.dumps(change) + '\n')
                log.flush()
            time.sleep(.1)


if __name__ == '__main__':
    main()
