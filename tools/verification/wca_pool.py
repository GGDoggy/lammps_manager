"""Run frozen WCA confirmation chunks on at most half the host's logical CPUs."""
import argparse
import fcntl
import tempfile
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

from lammps_manager.recovery import process_identity, session_members
from wca_acceptance import STRATA, PROTOCOL, digest, margin
from abp_acceptance import student_quantile
from statistics import mean, stdev
import math


ENDPOINTS = frozenset(
    ['energy'] + [f'shell-{i}' for i in range(4)] + [f'structure-{i}' for i in range(2)]
    + [f'lag-{lag}-{metric}' for lag in (1000, 2000) for metric in ('msd', 'acf')]
    + [f'boundary-{fraction}-{window}-{metric}' for fraction in (.3, .7)
       for window in ('before', 'cross', 'after') for metric in ('msd', 'acf')])


def sample_difference(row: dict, duration: int, layout: tuple,
                      start: int, stop: int) -> tuple[int, dict[str, float]]:
    """Require every frozen endpoint in both arms before computing differences."""
    prefix = f'{duration}-' + ''.join(map(str, layout)) + '-confirm-'
    key = row.get('key') if isinstance(row, dict) else None
    if not isinstance(key, str) or not key.startswith(prefix):
        raise ValueError('unexpected replica identity')
    suffix = key[len(prefix):]
    if not suffix.isascii() or not suffix.isdigit():
        raise ValueError('invalid replica ordinal')
    replica = int(suffix)
    if key != prefix + str(replica) or not start <= replica < stop:
        raise ValueError('out-of-range or noncanonical replica ordinal')
    arms = row.get('observables')
    if not isinstance(arms, dict) or set(arms) != {'reference', 'segmented'}:
        raise ValueError('both reference and segmented arms are required')
    if len(ENDPOINTS) != PROTOCOL['endpoints']:
        raise ValueError('declared endpoint schema differs from protocol')
    for measurements in arms.values():
        if not isinstance(measurements, dict) or set(measurements) != ENDPOINTS:
            raise ValueError('all declared endpoints are required in every arm')
        for value in measurements.values():
            try:
                valid = type(value) in (int, float) and math.isfinite(value)
            except OverflowError:
                valid = False
            if not valid:
                raise ValueError('scientific endpoints must be finite numbers, not booleans')
    differences = {name: arms['segmented'][name] - arms['reference'][name] for name in sorted(ENDPOINTS)}
    try:
        valid = all(math.isfinite(value) for value in differences.values())
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError('scientific endpoint difference is not finite')
    return replica, differences


@dataclass(frozen=True)
class Chunk:
    stratum: int
    start: int
    stop: int
    ranks: int

    @property
    def name(self) -> str:
        return f's{self.stratum}-{self.start}-{self.stop}'


def plan_chunks(pilot: dict, size: int = 64) -> list[Chunk]:
    """Cover each frozen replica once, retaining global stream ordinals."""
    if size <= 0 or len(pilot['strata']) != len(STRATA) or pilot.get('error'):
        raise ValueError('complete frozen pilots and positive chunk size required')
    if json.dumps(pilot['protocol'], sort_keys=True) != json.dumps(PROTOCOL, sort_keys=True):
        raise ValueError('pilot protocol differs')
    chunks = []
    for index, (duration, layout) in enumerate(STRATA):
        rows = [r for r in pilot['strata'] if r['duration'] == duration and tuple(r['layout']) == layout]
        if len(rows) != 1 or not rows[0]['feasible']:
            raise ValueError('missing/duplicate/infeasible pilot stratum')
        pairs = rows[0]['pairs']
        if type(pairs) is not int or not PROTOCOL['minimum_pairs'] <= pairs <= PROTOCOL['maximum_pairs']:
            raise ValueError('invalid frozen sample count')
        for start in range(0, pairs, size):
            chunks.append(Chunk(index, start, min(start + size, pairs), max(layout)))
    # Fill rank-four allocations first, keeping smaller allocations for spare slots.
    return sorted(chunks, key=lambda c: (-c.ranks, -STRATA[c.stratum][0], c.stratum, c.start))


def allocate(free: set[int], ranks: int) -> tuple[int, ...] | None:
    """Reserve an entire static affinity allocation or none of it."""
    if ranks <= 0 or len(free) < ranks:
        return None
    cpus = tuple(sorted(free)[:ranks])
    free.difference_update(cpus)
    return cpus


def analyze(root: Path, pilot: dict, completed: list[dict]) -> dict:
    """Require all frozen independent replicas before calculating family intervals."""
    plan_chunks(pilot)  # Validate the complete frozen design before reading measurements.
    values = {index: {} for index in range(len(STRATA))}
    reports = []
    for chunk in completed:
        index = chunk['stratum']
        if type(index) is not int or not 0 <= index < len(STRATA):
            raise ValueError('invalid chunk stratum')
        duration, layout = STRATA[index]
        frozen = next(r for r in pilot['strata'] if r['duration'] == duration and tuple(r['layout']) == layout)
        if (type(chunk['start']) is not int or type(chunk['stop']) is not int
                or not 0 <= chunk['start'] < chunk['stop'] <= frozen['pairs']
                or chunk['name'] != f"s{index}-{chunk['start']}-{chunk['stop']}"):
            raise ValueError('invalid chunk identity/range')
        path = root / 'chunks' / chunk['name'] / 'report.json'
        report = json.loads(path.read_text())
        if (report.get('error') or not report.get('measurements_complete')
                or type(report.get('selected_stratum')) is not int or report['selected_stratum'] != index
                or json.dumps(report.get('protocol'), sort_keys=True) != json.dumps(pilot['protocol'], sort_keys=True)
                or not isinstance(report.get('replicas'), list)
                or len(report['replicas']) != chunk['stop'] - chunk['start']):
            raise ValueError('incomplete/error/incompatible measurement chunk')
        for row in report['replicas']:
            replica, difference = sample_difference(row, duration, layout, chunk['start'], chunk['stop'])
            if replica in values[index]:
                raise ValueError('duplicate confirmatory replica')
            values[index][replica] = difference
        reports.append(dict(path=str(path), sha256=digest(path)))
    strata = []
    family = len(STRATA) * PROTOCOL['endpoints']
    for index, (duration, layout) in enumerate(STRATA):
        frozen = next(r for r in pilot['strata'] if r['duration'] == duration and tuple(r['layout']) == layout)
        pairs = frozen['pairs']
        if set(values[index]) != set(range(pairs)):
            raise ValueError('missing frozen confirmatory replicas')
        critical = student_quantile(1 - .05 / (2 * family), pairs - 1)
        endpoints = {}
        for name in values[index][0]:
            data = [values[index][replica][name] for replica in range(pairs)]
            average = mean(data); half = critical * stdev(data) / math.sqrt(pairs)
            endpoints[name] = dict(difference=average, lower=average-half, upper=average+half,
                                   margin=margin(name), equivalent=abs(average)+half < margin(name))
        strata.append(dict(duration=duration, layout=layout, pairs=pairs, endpoints=endpoints,
                           passed=all(v['equivalent'] for v in endpoints.values())))
    return dict(protocol=PROTOCOL, strata=strata, chunk_reports=reports,
                passed=all(r['passed'] for r in strata))


def reuse_completed(previous: Path, root: Path, pilot: dict, pilot_hash: str,
                    driver_hash: str, pending: list[Chunk]) -> list[dict]:
    """Reuse only complete, compatible, hash-verified chunks; preserve all old roots."""
    from wca_audit import verify_chunk
    prior = json.loads((previous / 'pool.json').read_text())
    if (prior['pilot_report_sha256'] != pilot_hash or prior['driver_sha256'] != driver_hash):
        raise ValueError('Prior confirmation source/pilot/boot differs')
    planned = {chunk.name: chunk for chunk in pending}
    selected = []
    for record in prior['completed']:
        chunk = planned.pop(record['name'], None)
        if chunk is None or any(record[k] != getattr(chunk, k) for k in ('stratum', 'start', 'stop')):
            raise ValueError('Duplicate or unexpected prior confirmation chunk')
        evidence = verify_chunk(previous, record, prior, pilot)
        selected.append((chunk, record, evidence))
    # Validate every selected chunk before creating any links or removing work.
    completed = []
    for chunk, record, evidence in selected:
        (root / 'chunks' / chunk.name).symlink_to((previous / 'chunks' / chunk.name).resolve(strict=True),
                                                 target_is_directory=True)
        pending.remove(chunk)
        completed.append({**record, 'reused_from': str(previous), 'reuse_audit': evidence, 'origin_boot_id': record.get('origin_boot_id', prior['boot_id'])})
    return completed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pilot-report', type=Path, required=True)
    parser.add_argument('--pilot-driver', type=Path, required=True)
    parser.add_argument('--lammps', type=Path, required=True)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--reuse-completed', type=Path)
    parser.add_argument('--defer-analysis', action='store_true')
    parser.add_argument('--enforce-rank-affinity', action='store_true')
    args = parser.parse_args()
    lock_path = Path(tempfile.gettempdir()) / f'lammps-wca-confirmation-{os.getuid()}.lock'
    lock_descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    fcntl.flock(lock_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    pilot = json.loads(args.pilot_report.read_text())
    pending = plan_chunks(pilot)
    cpus = sorted(os.sched_getaffinity(0))[:max(1, (os.cpu_count() or 1) // 2)]
    if len(cpus) < max(c.ranks for c in pending):
        raise ValueError('half-host allocation cannot fit the measured MPI layouts')
    os.sched_setaffinity(0, cpus)
    root = args.root.absolute(); root.mkdir(mode=0o700)
    (root / 'chunks').mkdir(); (root / 'logs').mkdir()
    driver = Path(__file__).with_name('wca_acceptance.py').resolve()
    driver_hash = digest(driver)
    pilot_hash = digest(args.pilot_report)
    pool_hash = digest(Path(__file__))
    free = set(cpus); active = {}; completed = []; events = []
    if args.reuse_completed is not None:
        completed = reuse_completed(args.reuse_completed.resolve(strict=True), root, pilot,
                                    pilot_hash, driver_hash, pending)
    metadata = dict(boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                    identity=process_identity(os.getpid()), allowed_cpus=cpus,
                    host_logical_cpus=os.cpu_count(), pilot_report_sha256=pilot_hash,
                    driver_sha256=driver_hash, pool_sha256=pool_hash, completed=[], passed=False)
    def save() -> None:
        metadata.update(active=[v['record'] for v in active.values()], completed=completed,
                        pending=len(pending), free_cpus=sorted(free), updated_at=time.time(), events=events)
        temporary = root / 'pool.tmp'
        with temporary.open('w') as stream:
            json.dump(metadata, stream, indent=2); stream.flush(); os.fsync(stream.fileno())
        temporary.replace(root / 'pool.json')
    guard = None
    try:
        save()
        if args.enforce_rank_affinity:
            with (root / 'affinity-guard.log').open('xb') as log:
                guard = subprocess.Popen([sys.executable, str(Path(__file__).with_name('wca_affinity_guard.py')),
                                          str(root), '--lammps', str(args.lammps.resolve(strict=True))],
                                         stdout=log, stderr=subprocess.STDOUT)
        while pending or active:
            if guard is not None and guard.poll() is not None:
                raise ValueError('Required rank affinity guard exited')
            disk_ok = shutil.disk_usage(root).free >= 20 * 1024 ** 3
            for chunk in pending[:]:
                if not disk_ok:
                    break
                allocation = allocate(free, chunk.ranks)
                if allocation is None:
                    continue
                if digest(driver) != driver_hash:
                    raise ValueError('frozen confirmation driver changed')
                argv = ['taskset', '-c', ','.join(map(str, allocation)), sys.executable,
                        str(driver), '--lammps', str(args.lammps.resolve()),
                        '--root', str(root / 'chunks' / chunk.name),
                        '--confirm-from', str(args.pilot_report.resolve()),
                        '--pilot-driver', str(args.pilot_driver.resolve()),
                        '--stratum-index', str(chunk.stratum),
                        '--replica-start', str(chunk.start), '--replica-stop', str(chunk.stop)]
                with (root / 'logs' / (chunk.name + '.log')).open('xb') as output:
                    process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT,
                                               stdin=subprocess.DEVNULL, start_new_session=True)
                record = dict(name=chunk.name, stratum=chunk.stratum, start=chunk.start,
                              stop=chunk.stop, cpus=allocation, pid=process.pid,
                              identity=process_identity(process.pid), started=time.time(), argv=argv)
                active[chunk.name] = dict(process=process, record=record)
                pending.remove(chunk); events.append(dict(event='started', name=chunk.name, time=time.time()))
            for name, value in list(active.items()):
                process = value['process']; code = process.poll()
                if code is None:
                    continue
                record = value['record']; record.update(returncode=code, ended=time.time())
                if code:
                    raise ValueError(f'measurement chunk failed: {name}; retain original root/log')
                data = json.loads((root / 'chunks' / name / 'report.json').read_text())
                if data.get('error') or not data.get('measurements_complete'):
                    raise ValueError(f'measurement chunk incomplete: {name}')
                completed.append(record); free.update(record['cpus']); del active[name]
                events.append(dict(event='completed', name=name, time=time.time()))
            if not disk_ok and not active:
                raise ValueError('20-GiB free-space guard blocks further confirmation; retain all output')
            if Path('/proc/sys/kernel/random/boot_id').read_text().strip() != metadata['boot_id']:
                raise ValueError('host boot changed')
            save()
            if pending or active:
                time.sleep(2)
        metadata['measurements_complete'] = True
        metadata['analysis_deferred'] = args.defer_analysis
        save()
        if args.defer_analysis:
            return
        analysis = analyze(root, pilot, completed)
        (root / 'analysis.json').write_text(json.dumps(analysis, indent=2))
        metadata['passed'] = analysis['passed']; save()
        if not analysis['passed']:
            raise ValueError('confirmatory equivalence failed/inconclusive; raw evidence retained')
    except BaseException as exc:
        metadata['error'] = f'{type(exc).__name__}: {exc}'
        # Interrupt drivers so each can reap its own separately isolated MPI session.
        for value in active.values():
            process = value['process']
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
        for value in active.values():
            process = value['process']
            try:
                process.wait(timeout=150)
            except subprocess.TimeoutExpired:
                metadata.setdefault('cleanup_blockers', []).append(value['record'])
        save(); raise
    finally:
        if guard is not None and guard.poll() is None:
            guard.terminate()
            try:
                guard.wait(timeout=5)
            except subprocess.TimeoutExpired:
                guard.kill(); guard.wait(timeout=5)


if __name__ == '__main__':
    main()
