"""Predeclared WCA-ABP scientific comparison; explicit external execution only."""
import argparse
import ast
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
from statistics import NormalDist, mean, stdev
import subprocess
import time

from abp_acceptance import student_quantile, msd_theory
from lammps_manager.continuation import fingerprint_build
from lammps_manager.recovery import process_identity, session_members

N = 256
L = math.sqrt(N / .5)
DT = .0001
WARM = 20000
LAYOUTS = ((1, 1, 1), (2, 2, 2), (4, 4, 4), (1, 2, 4), (4, 2, 1))
STRATA = [(steps, layout) for steps in (20000, 100000) for layout in LAYOUTS]
PROTOCOL = dict(version=2, namespace='v009-wca-20261023-v2', particles=N,
                density=.5, dt=DT, preparation_steps=WARM, strata=STRATA,
                pilot_pairs=8, minimum_pairs=64, maximum_pairs=16384,
                family_alpha=.05, target_power=.9, variance_inflation=4,
                endpoints=23, protocol='docs/verification/v009-wca-protocol.md')


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open('rb') as stream:
        while block := stream.read(65536):
            value.update(block)
    return value.hexdigest()


def render(*, start: int, stop: int, total: int, stream: int, orientation: int,
           interval: int, initial: bool = False) -> str:
    """Render positive engine-supported bounds without a scientific test-range cap."""
    if any(type(v) is not int for v in (start, stop, total, stream, orientation, interval)):
        raise ValueError('integer bounds/seeds/interval required')
    if not 0 <= start < stop <= total or interval <= 0 or not 0 < stream < 900000000:
        raise ValueError('invalid bounds/seed/interval')
    head = 'units lj\ndimension 2\natom_style hybrid sphere dipole\natom_modify sort 0 0.0\n'
    if initial:
        head += f'''boundary p p p
region box block 0 {L:.17g} 0 {L:.17g} -0.5 0.5
create_box 1 box
lattice sq {N / (L * L):.17g}
create_atoms 1 box
set type 1 diameter 1.0
set type 1 density 1.0
set group all dipole/random {orientation} 1.0
pair_style lj/cut 1.122462048309373
pair_coeff * * 1.0 1.0
pair_modify shift yes
timestep {DT}
'''
    else:
        head += 'read_restart checkpoint.restart\n'
    head += f'''neighbor 0.3 bin
neigh_modify delay 0 every 1 check no
fix integrate all brownian/sphere 1.0 {stream} gamma_t 10.0 gamma_r 1.0 rng gaussian
fix active all propel/self dipole 10.0
thermo {interval}
thermo_style custom step time atoms pe
thermo_modify format float %.17g
dump trajectory all custom {interval} trajectory.dump id xu yu mux muy muz
dump_modify trajectory sort id format float %.17g
'''
    # LAMMPS run accepts a bounded per-command integer; preserve bigint total clocks.
    remaining = stop - start
    while remaining:
        chunk = min(remaining, 2147483647)
        head += f'run {chunk} start 0 stop {total}\n'
        remaining -= chunk
    return head + 'write_restart checkpoint.restart\n'


def frames(path: Path, interval: int, start: int, stop: int) -> dict:
    data = {}
    for block in path.read_text().split('ITEM: TIMESTEP\n')[1:]:
        rows = block.splitlines()
        step = int(rows[0])
        if (step in data or len(rows) != N + 8 or rows[1:3] != ['ITEM: NUMBER OF ATOMS', str(N)]
                or rows[7] != 'ITEM: ATOMS id xu yu mux muy muz'):
            raise ValueError('invalid dump shape/identity')
        atoms = []
        for index, line in enumerate(rows[8:], 1):
            values = list(map(float, line.split()))
            if (len(values) != 6 or values[0] != index or not all(map(math.isfinite, values))
                    or abs(math.hypot(values[3], values[4]) - 1) > 1e-10
                    or abs(values[5]) > 1e-12):
                raise ValueError('invalid physical state')
            atoms.append(values[1:])
        data[step] = atoms
    if set(data) != set(range(start, stop + 1, interval)):
        raise ValueError('missing global frames')
    return data


def thermo(path: Path, start: int, stop: int, interval: int) -> dict:
    data = {}; active = False
    for line in path.read_text().splitlines():
        if line.split() == ['Step', 'Time', 'Atoms', 'PotEng']:
            active = True; continue
        if line.startswith('Loop time'):
            active = False
        if active:
            fields = line.split()
            if len(fields) != 4:
                raise ValueError('malformed thermo')
            step = int(fields[0]); clock, count, energy = map(float, fields[1:])
            if count != N or not math.isfinite(energy) or not math.isclose(clock, step * DT, rel_tol=1e-12, abs_tol=1e-12):
                raise ValueError('clock/atoms/energy mismatch')
            data[step] = energy
    if set(data) != set(range(start, stop + 1, interval)):
        raise ValueError('missing thermo coverage')
    return data


def observables(data: dict, energy: dict, duration: int) -> dict:
    interval = duration // 100
    first = WARM
    result = dict(energy=mean(energy[t] for t in range(first, first + duration + 1, interval)))
    structure = [data[first + duration - k * interval] for k in range(4, -1, -1)]
    # Fixed times, not a stopping rule chosen from trajectory appearance.
    bounds = (.9, 1., 2 ** (1 / 6), 1.5, 2.)
    shells = [0] * 4
    for atoms in structure:
        for i, a in enumerate(atoms):
            for b in atoms[i + 1:]:
                dx = (a[0] - b[0] + L / 2) % L - L / 2
                dy = (a[1] - b[1] + L / 2) % L - L / 2
                radius = math.hypot(dx, dy)
                for k in range(4):
                    if bounds[k] <= radius < bounds[k + 1]:
                        shells[k] += 2
    result.update({f'shell-{k}': count / (len(structure) * N) for k, count in enumerate(shells)})
    for axis in (0, 1):
        result[f'structure-{axis}'] = mean(
            (sum(math.cos(2 * math.pi * a[axis] / L) for a in atoms) ** 2
             + sum(math.sin(2 * math.pi * a[axis] / L) for a in atoms) ** 2) / N for atoms in structure)
    def dynamics(pairs, lag, name):
        result[name + '-msd'] = mean((b[0] - a[0]) ** 2 + (b[1] - a[1]) ** 2 for a, b in pairs) / msd_theory(lag * DT)
        result[name + '-acf'] = mean(a[2] * b[2] + a[3] * b[3] for a, b in pairs)
    for lag in (1000, 2000):
        # Each stratum's sparse frames must contain both endpoints at these lags.
        actual = max(interval, round(lag / interval) * interval)
        pairs = [(a, b) for t in sorted(data) if t + actual in data for a, b in zip(data[t], data[t + actual])]
        dynamics(pairs, actual, f'lag-{lag}')
    for fraction in (.3, .7):
        boundary = first + int(duration * fraction)
        for name, lo, hi in [('before', boundary - 2 * interval, boundary - interval),
                             ('cross', boundary - interval, boundary + interval),
                             ('after', boundary + interval, boundary + 2 * interval)]:
            dynamics(list(zip(data[lo], data[hi])), hi - lo, f'boundary-{fraction}-{name}')
    if len(result) != PROTOCOL['endpoints']:
        raise ValueError('protocol endpoint count changed')
    return result


def margin(name: str) -> float:
    return .05 if name == 'energy' or name.endswith(('-msd', '-acf')) else .1


def execute(argv: list[str], directory: Path, report: dict) -> None:
    record = dict(argv=argv, directory=str(directory), started=time.time())
    report['commands'].append(record)
    with (directory / 'stdout').open('wb') as out, (directory / 'stderr').open('wb') as err:
        process = subprocess.Popen(argv, cwd=directory, stdout=out, stderr=err,
                                   env={**os.environ, 'OMP_NUM_THREADS': '1', 'DISPLAY': ''}, start_new_session=True)
        record['pid'] = process.pid
        record['identity'] = process_identity(process.pid)
        try:
            record['returncode'] = process.wait(timeout=120)
        except BaseException:
            os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=10)
            raise
    record['ended'] = time.time()
    record['remaining_session_members'] = session_members(process.pid)
    if record['remaining_session_members']:
        raise ValueError('MPI session survived launcher completion; inspect original execution')
    if record['returncode']:
        raise ValueError(f'LAMMPS failed: {directory}; inspect retained stdout/stderr')


def run_pair(root: Path, lammps: Path, phase: str, replica: int, duration: int,
             layout: tuple, used: set, report: dict) -> dict:
    key = f'{duration}-' + ''.join(map(str, layout)) + f'-{phase}-{replica}'
    def seed(label):
        labels = ['orientation', 'prepare', 'reference-0', 'segmented-0', 'segmented-1', 'segmented-2']
        stratum = STRATA.index((duration, layout))
        ordinal = ((stratum * (16384 + 8) + (0 if phase == 'pilot' else 8) + replica) * 6
                   + labels.index(label))
        offset = int.from_bytes(hashlib.sha256(PROTOCOL['namespace'].encode()).digest()[:8], 'big')
        value = (offset + ordinal * 1000003) % 899999999 + 1
        if value in used:
            raise ValueError('seed collision; requires new protocol before measurement')
        used.add(value); return value
    orientation = seed('orientation')
    interval = duration // 100
    directory = root / (key + '-prepare'); directory.mkdir()
    (directory / 'in.lmp').write_text(render(start=0, stop=WARM, total=WARM + duration,
                                           stream=seed('prepare'), orientation=orientation, interval=interval, initial=True))
    execute(['/usr/bin/mpirun.openmpi', '-np', '1', '--bind-to', 'none', str(lammps), '-in', 'in.lmp'], directory, report)
    prepared = directory / 'checkpoint.restart'
    arms = {}; initial = None
    for arm in ('reference', 'segmented'):
        data = {}; energy = {}; previous = prepared; start = WARM
        ends = [WARM + duration] if arm == 'reference' else [WARM + int(duration * f) for f in (.3, .7, 1.)]
        for index, stop in enumerate(ends):
            directory = root / f'{key}-{arm}-{index}'; directory.mkdir()
            shutil.copyfile(previous, directory / 'checkpoint.restart')
            (directory / 'in.lmp').write_text(render(start=start, stop=stop, total=WARM + duration,
                                                   stream=seed(f'{arm}-{index}'), orientation=orientation, interval=interval))
            rank = 1 if arm == 'reference' else layout[index]
            execute(['/usr/bin/mpirun.openmpi', '-np', str(rank), '--bind-to', 'none', str(lammps), '-in', 'in.lmp'], directory, report)
            part = frames(directory / 'trajectory.dump', interval, start, stop)
            if start in data and data[start] != part[start]:
                raise ValueError('physical boundary state changed')
            if start == WARM:
                if initial is None:
                    initial = part[start]
                elif initial != part[start]:
                    raise ValueError('comparison arms have different initial states')
            data.update(part); energy.update(thermo(directory / 'log.lammps', start, stop, interval))
            previous = directory / 'checkpoint.restart'; start = stop
        arms[arm] = observables(data, energy, duration)
    report['replicas'].append(dict(key=key, observables=arms))
    return {name: arms['segmented'][name] - value for name, value in arms['reference'].items()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lammps', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--pilot-only', action='store_true')
    parser.add_argument('--replica-start', type=int, default=0)
    parser.add_argument('--replica-stop', type=int)
    parser.add_argument('--stratum-index', type=int, choices=range(len(STRATA)),
                        help='Confirm one frozen stratum; retain full-family error control')
    parser.add_argument('--confirm-from', type=Path, help='Complete frozen pilot report from all strata')
    parser.add_argument('--pilot-driver', type=Path, help='Original pilot driver for scientific-kernel identity checks')
    args = parser.parse_args()
    if args.replica_start < 0 or (args.replica_stop is not None and args.replica_stop <= args.replica_start):
        parser.error('invalid confirmatory replica range')
    if args.replica_start or args.replica_stop is not None:
        if args.confirm_from is None or args.stratum_index is None:
            parser.error('replica ranges require one frozen confirmatory stratum')
    if not args.pilot_only and args.confirm_from is None:
        parser.error('run --pilot-only first, then confirm the complete frozen report')
    if args.confirm_from and (args.pilot_only or args.pilot_driver is None):
        parser.error('--confirm-from requires --pilot-driver and excludes --pilot-only')
    pilot_report = None
    if args.confirm_from:
        pilot_report = json.loads(args.confirm_from.read_text())
        if (pilot_report.get('error') or len(pilot_report.get('strata', [])) != len(STRATA)
                or json.dumps(pilot_report['protocol'], sort_keys=True) != json.dumps(PROTOCOL, sort_keys=True)
                or any(not row.get('feasible') for row in pilot_report['strata'])
                or digest(args.pilot_driver) != pilot_report['driver_sha256']):
            raise ValueError('incomplete, incompatible or altered pilot evidence')
        functions = ('render', 'frames', 'thermo', 'observables', 'margin', 'run_pair')
        def kernels(path):
            return {node.name: ast.dump(node, include_attributes=False)
                    for node in ast.parse(path.read_text()).body
                    if isinstance(node, ast.FunctionDef) and node.name in functions}
        if kernels(args.pilot_driver) != kernels(Path(__file__)):
            raise ValueError('scientific kernel changed since pilot')
    root = args.root.absolute(); root.mkdir(mode=0o700)
    executable = args.lammps.resolve(strict=True)
    os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:4])
    report = dict(protocol=PROTOCOL, selected_stratum=args.stratum_index,
                  complete_family=args.stratum_index is None, protocol_sha256=digest(Path(__file__).with_name('v009-wca-protocol.md')),
                  driver_sha256=digest(Path(__file__)), build=fingerprint_build(executable),
                  boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                  commands=[], replicas=[], strata=[], passed=False)
    if pilot_report is not None:
        if report['build'] != pilot_report['build']:
            raise ValueError('build changed since pilot')
        report['pilot_report_sha256'] = digest(args.confirm_from)
        report['pilot_driver_sha256'] = digest(args.pilot_driver)
    (root / 'protocol.json').write_text(json.dumps(PROTOCOL, indent=2))
    used = set()
    def save():
        (root / 'report.json').write_text(json.dumps(report, indent=2))
    try:
        for stratum_index, (duration, layout) in enumerate(STRATA):
            if args.stratum_index is not None and stratum_index != args.stratum_index:
                continue
            count = len(STRATA) * PROTOCOL['endpoints']
            if pilot_report is None:
                pilot = []
                for replica in range(PROTOCOL['pilot_pairs']):
                    pilot.append(run_pair(root, executable, 'pilot', replica, duration, layout, used, report)); save()
                z = NormalDist().inv_cdf(1 - .05 / (2 * count)) + NormalDist().inv_cdf(.9)
                required = {name: math.ceil(4 * stdev([r[name] for r in pilot]) ** 2 * z * z / margin(name) ** 2) for name in pilot[0]}
                pairs = max(64, max(required.values()))
                frozen = dict(duration=duration, layout=layout, pairs=pairs, required=required, feasible=pairs <= PROTOCOL['maximum_pairs'])
            else:
                frozen = next(dict(row) for row in pilot_report['strata']
                              if row['duration'] == duration and tuple(row['layout']) == layout)
                pairs = frozen['pairs']
            report['strata'].append(frozen); save()
            print(json.dumps(frozen), flush=True)
            if not frozen['feasible']:
                raise ValueError('predeclared sample cap insufficient; retain inconclusive study')
            if args.pilot_only:
                continue
            stop_replica = pairs if args.replica_stop is None else args.replica_stop
            if stop_replica > pairs or args.replica_start >= pairs:
                raise ValueError('replica range exceeds frozen sample size')
            frozen['replica_range'] = [args.replica_start, stop_replica]
            differences = []
            for replica in range(args.replica_start, stop_replica):
                differences.append(run_pair(root, executable, 'confirm', replica, duration, layout, used, report)); save()
                key = f'{duration}-' + ''.join(map(str, layout)) + f'-confirm-{replica}'
                for directory in root.glob(key + '-*'):
                    trajectory = directory / 'trajectory.dump'
                    if trajectory.is_file():
                        compressed = trajectory.with_suffix('.dump.gz')
                        with trajectory.open('rb') as source, gzip.open(compressed, 'xb') as destination:
                            shutil.copyfileobj(source, destination, length=65536)
                        # Validate lossless storage before retiring the plain representation.
                        with gzip.open(compressed, 'rb') as restored:
                            original = hashlib.sha256()
                            while block := restored.read(65536):
                                original.update(block)
                        if original.hexdigest() != digest(trajectory):
                            raise ValueError('lossless trajectory archive mismatch')
                        trajectory.unlink()
                print(json.dumps(dict(duration=duration, layout=layout, confirm=replica + 1, total=pairs)), flush=True)
            frozen['measurements_complete'] = len(differences) == stop_replica - args.replica_start
            if args.replica_stop is not None or args.replica_start != 0 or stop_replica != pairs:
                frozen['partial_stratum'] = True
                save()
                continue
            critical = student_quantile(1 - .05 / (2 * count), pairs - 1)
            endpoints = {}
            for name in differences[0]:
                values = [r[name] for r in differences]; average = mean(values)
                half = critical * stdev(values) / math.sqrt(pairs)
                endpoints[name] = dict(difference=average, lower=average-half, upper=average+half,
                                       margin=margin(name), equivalent=abs(average)+half < margin(name))
            frozen['analysis'] = endpoints
            frozen['passed'] = all(e['equivalent'] for e in endpoints.values()); save()
        if fingerprint_build(executable) != report['build']:
            raise ValueError('build changed during experiment')
        if Path('/proc/sys/kernel/random/boot_id').read_text().strip() != report['boot_id']:
            raise ValueError('host boot changed')
        report['measurements_complete'] = not args.pilot_only and all(s.get('measurements_complete') for s in report['strata'])
        report['passed'] = not args.pilot_only and all(s.get('passed') for s in report['strata'])
    except BaseException as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'; raise
    finally:
        report['seed_count'] = len(used)
        report['retained_files'] = {str(path.relative_to(root)): dict(bytes=path.stat().st_size, sha256=digest(path))
                                    for path in root.rglob('*') if path.is_file() and path.name != 'report.json'}
        save()
    partial = any(s.get('partial_stratum') for s in report['strata'])
    if not args.pilot_only and not report['passed'] and not (partial and report.get('measurements_complete')):
        raise SystemExit('equivalence failed/inconclusive; retain evidence')


if __name__ == '__main__':
    main()
