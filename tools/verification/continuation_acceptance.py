"""Explicit bounded real-LAMMPS comparison, outside default test discovery.

Run with PYTHONPATH=src python3 tools/verification/continuation_acceptance.py
--lammps /absolute/path/lmp --root /new/private/path. All files are retained.
A passing comparison is evidence for review, not a production release approval.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess

from lammps_manager.continuation import Recipe, recipe_digest, render_input


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_frames(path: Path) -> dict[int, str]:
    """Reject malformed/nonfinite dumps; retain every printed state bit for equality."""
    frames = {}
    text = path.read_text()
    if not text.startswith('ITEM: TIMESTEP\n'):
        raise ValueError('missing trajectory header')
    for block in text.split('ITEM: TIMESTEP\n')[1:]:
        lines = block.splitlines()
        step = int(lines[0])
        if step in frames or lines[1] != 'ITEM: NUMBER OF ATOMS' or int(lines[2]) != 108:
            raise ValueError('duplicate step or unexpected atom count/dump format')
        header = 'ITEM: ATOMS id type x y z vx vy vz ix iy iz fx fy fz'
        if (len(lines) != 116 or lines[7] != header
                or lines[3] != 'ITEM: BOX BOUNDS pp pp pp'):
            raise ValueError('unexpected dump fields or length')
        for row in lines[4:7]:
            if len(row.split()) != 2 or not all(math.isfinite(float(x)) for x in row.split()):
                raise ValueError('invalid box bounds')
        for expected, row in enumerate(lines[8:], 1):
            values = row.split()
            if len(values) != 14 or int(values[0]) != expected:
                raise ValueError('missing atom or unexpected fields')
            if not all(math.isfinite(float(value)) for value in values):
                raise ValueError('nonfinite state')
        frames[step] = '\n'.join(lines[1:])
    if not frames:
        raise ValueError('empty trajectory')
    return frames


def read_thermo(path: Path) -> dict[int, str]:
    """Read full-precision global clock, thermodynamics and atom count."""
    rows = {}
    active = False
    for line in path.read_text().splitlines():
        if line.split() == ['Step', 'Time', 'Atoms', 'Temp', 'PotEng', 'KinEng', 'TotEng', 'Press']:
            active = True
            continue
        if active and line.startswith('Loop time'):
            active = False
        if not active:
            continue
        fields = line.split()
        if len(fields) != 8 or not all(math.isfinite(float(x)) for x in fields):
            raise ValueError('unexpected or nonfinite thermo output')
        step = int(fields[0])
        if step in rows or int(fields[2]) != 108:
            raise ValueError('duplicate thermo step or wrong atom count')
        if float(fields[1]) != float(format(step * 0.005, '.17g')):
            # Multiplication may differ by a rounding bit from LAMMPS elapsed time;
            # exact reference/segment string equality is also checked below.
            if not math.isclose(float(fields[1]), step * 0.005, rel_tol=1e-14, abs_tol=1e-15):
                raise ValueError('global time reset')
        rows[step] = ' '.join(fields[1:])
    if not rows:
        raise ValueError('missing thermo output')
    return rows


def compare(reference: dict[int, str], parts: list[dict[int, str]], total: int) -> dict:
    """Check all steps including each duplicated boundary before deduplication."""
    if set(reference) != set(range(total + 1)):
        raise ValueError('reference has missing or unexpected steps')
    joined = {}
    mismatches = set()
    boundaries = []
    end = 0
    for part in parts:
        if set(part) != set(range(end, max(part) + 1)):
            raise ValueError('segment has a gap/overlap or missing boundary')
        if joined:
            boundaries.append(end)
        for step, state in part.items():
            if step not in reference or reference[step] != state:
                mismatches.add(step)
            if step in joined and joined[step] != state:
                mismatches.add(step)
            joined[step] = state
        end = max(part)
    if set(joined) != set(reference):
        raise ValueError('segmented trajectory has missing or unexpected steps')
    return dict(passed=not mismatches, compared_steps=len(joined),
                mismatched_steps=sorted(mismatches), boundaries=boundaries,
                policy='compare both boundary frames, retain one logical frame')


def execute(argv: list[str], directory: Path, commands: list[dict]) -> None:
    """Bound an owned process session; kill/reap it on timeout or interruption."""
    with (directory / 'stdout').open('w') as out, (directory / 'stderr').open('w') as err:
        process = subprocess.Popen(argv, cwd=directory, stdout=out, stderr=err,
                                   env={**os.environ, 'OMP_NUM_THREADS': '1'},
                                   start_new_session=True)
        record = dict(argv=argv, directory=str(directory), pid=process.pid)
        commands.append(record)
        try:
            record['returncode'] = process.wait(timeout=30)
        except BaseException as exc:
            # The detached session does not receive the terminal's Ctrl+C.
            # Kill the entire group even if its leader has already exited.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # The group exited between wait() and killpg().
            record['returncode'] = process.wait()
            if isinstance(exc, subprocess.TimeoutExpired):
                record['timed_out'] = True
                raise RuntimeError('LAMMPS comparison exceeded 30 seconds') from exc
            record['interrupted'] = True
            raise
        if record['returncode'] != 0:
            raise RuntimeError(f'LAMMPS failed: see {directory}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lammps', required=True, type=Path)
    parser.add_argument('--root', required=True, type=Path)
    args = parser.parse_args()
    executable = args.lammps.resolve(strict=True)
    root = args.root.absolute()
    root.mkdir(mode=0o700)  # Refuse reuse and preserve previous output.
    report = dict(schema_version=1, recipe=Recipe().__dict__, recipe_sha256=recipe_digest(),
                  executable=str(executable), executable_sha256=digest(executable),
                  platform=platform.platform(), machine=platform.machine(),
                  environment={key: os.environ.get(key) for key in
                               ('LD_LIBRARY_PATH', 'LD_PRELOAD', 'OMP_NUM_THREADS')},
                  commands=[], passed=False, production_approved=False)
    try:
        # Preserve build/package information and linked-library provenance.
        for name, argv in [('build', [str(executable), '-help']),
                           ('libraries', ['ldd', str(executable)])]:
            result = subprocess.run(argv, capture_output=True, timeout=15)
            (root / (name + '.txt')).write_bytes(result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f'{name} inventory failed')
        libraries = {}
        for word in (root / 'libraries.txt').read_text().split():
            path = Path(word)
            if path.is_absolute() and path.is_file():
                libraries[str(path.resolve())] = digest(path)
        report['libraries_sha256'] = libraries
        reference = root / 'uninterrupted'
        reference.mkdir()
        (reference / 'in.lmp').write_text(render_input(start=0, stop=100, total=100))
        execute([str(executable), '-in', 'in.lmp'], reference, report['commands'])
        frames = read_frames(reference / 'trajectory.dump')
        thermo = read_thermo(reference / 'log.lammps')
        # Multiple boundaries, including a one-step tail, in separate OS processes.
        parts = []
        thermo_parts = []
        start = 0
        previous = None
        for stop in (37, 99, 100):
            directory = root / f'segment-{start}-{stop}'
            directory.mkdir()
            if previous:
                shutil.copyfile(previous / 'checkpoint.restart', directory / 'checkpoint.restart')
            (directory / 'in.lmp').write_text(render_input(start=start, stop=stop, total=100))
            execute([str(executable), '-in', 'in.lmp'], directory, report['commands'])
            part = read_frames(directory / 'trajectory.dump')
            if set(part) != set(range(start, stop + 1)):
                raise ValueError('segment did not retain global timestep range')
            parts.append(part)
            thermo_parts.append(read_thermo(directory / 'log.lammps'))
            previous, start = directory, stop
        report['comparison'] = compare(frames, parts, 100)
        report['thermo_comparison'] = compare(thermo, thermo_parts, 100)
        report['passed'] = (report['comparison']['passed']
                            and report['thermo_comparison']['passed'])
        if any(digest(Path(path)) != sha for path, sha in libraries.items()):
            raise RuntimeError('linked library changed during comparison')
        if digest(executable) != report['executable_sha256']:
            raise RuntimeError('executable changed during comparison')
    except Exception as exc:
        report['passed'] = False
        report['error'] = str(exc)
        raise
    finally:
        report['files'] = {str(path.relative_to(root)): dict(bytes=path.stat().st_size,
                           sha256=digest(path)) for path in root.rglob('*') if path.is_file()}
        (root / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    if not report['passed']:
        raise SystemExit('Exact comparison failed; evidence retained')


if __name__ == '__main__':
    main()
