"""Audit frozen WCA raw artifacts and, after completion, the full-family analysis."""
import argparse
import gzip
import hashlib
import shutil
import tempfile
import json
import os
from pathlib import Path
import stat

from wca_acceptance import digest, frames, thermo, observables, render, WARM, PROTOCOL, N, L, DT
from wca_pool import analyze, sample_difference, STRATA, ENDPOINTS


def stream_seed(stratum: int, replica: int, label: str) -> int:
    """Reconstruct the frozen confirmatory stream identity, without new sampling."""
    labels = ('orientation', 'prepare', 'reference-0', 'segmented-0', 'segmented-1', 'segmented-2')
    ordinal = ((stratum * (16384 + 8) + 8 + replica) * 6 + labels.index(label))
    offset = int.from_bytes(hashlib.sha256(PROTOCOL['namespace'].encode()).digest()[:8], 'big')
    return (offset + ordinal * 1000003) % 899999999 + 1


def recompute_replica(directory: Path, row: dict, record: dict) -> dict:
    """Reproduce every reported endpoint from unchanged raw inputs/dumps/thermo."""
    index = record['stratum']; duration, layout = STRATA[index]
    replica, _ = sample_difference(row, duration, layout, record['start'], record['stop'])
    key = row['key']; interval = duration // 100
    orientation = stream_seed(index, replica, 'orientation')
    with tempfile.TemporaryDirectory(prefix='lammps-wca-raw-audit-') as temporary:
        plain = Path(temporary) / 'trajectory.dump'
        def load(stage: Path, start: int, stop: int) -> dict:
            raw = stage / 'trajectory.dump'; compressed = stage / 'trajectory.dump.gz'
            if raw.exists() == compressed.exists():
                raise ValueError('exactly one retained trajectory representation is required')
            try:
                if compressed.exists():
                    with gzip.open(compressed, 'rb') as source, plain.open('wb') as destination:
                        shutil.copyfileobj(source, destination, length=65536)
                    path = plain
                else:
                    path = raw
                return frames(path, interval, start, stop)
            except (OSError, ValueError) as exc:
                raise ValueError(f'invalid retained scientific trajectory: {stage}') from exc
        prepare = directory / (key + '-prepare')
        expected = render(start=0, stop=WARM, total=WARM + duration,
                          stream=stream_seed(index, replica, 'prepare'), orientation=orientation,
                          interval=interval, initial=True)
        if (prepare / 'in.lmp').read_text() != expected:
            raise ValueError('raw preparation input differs from frozen model/stream')
        initial = load(prepare, 0, WARM)[WARM]
        thermo(prepare / 'log.lammps', 0, WARM, interval)
        recomputed = {}
        for arm in ('reference', 'segmented'):
            data = {}; energy = {}; start = WARM
            ends = [WARM + duration] if arm == 'reference' else [WARM + int(duration*f) for f in (.3, .7, 1.)]
            for part_index, stop in enumerate(ends):
                stage = directory / f'{key}-{arm}-{part_index}'
                expected = render(start=start, stop=stop, total=WARM + duration,
                                  stream=stream_seed(index, replica, f'{arm}-{part_index}'),
                                  orientation=orientation, interval=interval)
                if (stage / 'in.lmp').read_text() != expected:
                    raise ValueError('raw segment input differs from frozen model/stream')
                part = load(stage, start, stop)
                if (start == WARM and part[start] != initial) or (start in data and part[start] != data[start]):
                    raise ValueError('raw physical boundary/initial state differs')
                data.update(part); energy.update(thermo(stage / 'log.lammps', start, stop, interval))
                start = stop
            recomputed[arm] = observables(data, energy, duration)
            if recomputed[arm] != row['observables'][arm]:
                raise ValueError(f'raw scientific endpoints differ from report: {key}/{arm}')
    return dict(key=key, endpoints=len(recomputed['reference']), matched=True)


def verify_chunk(root: Path, record: dict, pool: dict, pilot: dict, *, recompute: bool = False) -> dict:
    """Reject damaged, missing, extra or incompatible raw scientific evidence."""
    directory = root / 'chunks' / record['name']
    path = directory / 'report.json'
    report = json.loads(path.read_text())
    expected_boot = record.get('origin_boot_id', pool['boot_id'])
    if (report.get('error') or not report.get('measurements_complete')
            or report['driver_sha256'] != pool['driver_sha256']
            or report['pilot_report_sha256'] != pool['pilot_report_sha256']
            or report['build'] != pilot['build'] or report['boot_id'] != expected_boot
            or report['selected_stratum'] != record['stratum']
            or len(report['replicas']) != record['stop'] - record['start']):
        raise ValueError(f'Incomplete/incompatible scientific chunk: {record["name"]}')
    index = record['stratum']
    if type(index) is not int or not 0 <= index < len(STRATA):
        raise ValueError('invalid scientific stratum')
    duration, layout = STRATA[index]
    seen = set()
    for row in report['replicas']:
        replica, _ = sample_difference(row, duration, layout, record['start'], record['stop'])
        if replica in seen:
            raise ValueError('duplicate scientific replica')
        seen.add(replica)
    if seen != set(range(record['start'], record['stop'])):
        raise ValueError('missing scientific replica')
    actual = {}
    for artifact in directory.rglob('*'):
        mode = artifact.lstat().st_mode
        if stat.S_ISDIR(mode):
            continue
        if not stat.S_ISREG(mode):
            raise ValueError('Unsafe scientific artifact')
        if artifact == path:
            continue
        actual[artifact.relative_to(directory).as_posix()] = dict(bytes=artifact.stat().st_size,
                                                                sha256=digest(artifact))
    if not actual or actual != report['retained_files']:
        raise ValueError(f'Raw scientific files differ from recorded inventory: {record["name"]}')
    commands = report['commands']
    if len(commands) != 5 * len(report['replicas']) or any(
            command.get('returncode') != 0 or command.get('remaining_session_members') != []
            or command.get('identity', {}).get('boot_id') != expected_boot for command in commands):
        raise ValueError('Missing successful process/session evidence')
    if recompute:
        for row in report['replicas']:
            recompute_replica(directory, row, record)
    return dict(name=record['name'], report_sha256=digest(path), files=len(actual),
                bytes=sum(item['bytes'] for item in actual.values()), replicas=len(report['replicas']),
                raw_endpoints_recomputed=recompute)


def root_processes(root: Path, *, proc_root: Path = Path('/proc'), current_pid: int | None = None) -> list[int]:
    """Find remaining root processes, excluding this observer and its ancestors."""
    pid = os.getpid() if current_pid is None else current_pid
    observers = set()
    while pid > 0 and pid not in observers:
        observers.add(pid)
        try:
            fields = (proc_root / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
            pid = int(fields[1])
        except (OSError, ValueError, IndexError):
            break
    processes = []
    for entry in proc_root.iterdir():
        if not entry.name.isdigit() or int(entry.name) in observers:
            continue
        try:
            argv = (entry / 'cmdline').read_bytes().split(bytes([0]))
            cwd = (entry / 'cwd').resolve()
            if any(str(root).encode() in part for part in argv) or cwd == root or root in cwd.parents:
                processes.append(int(entry.name))
        except OSError:
            continue
    return sorted(processes)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--pilot-report', type=Path, required=True)
    parser.add_argument('--completed-only', action='store_true')
    parser.add_argument('--recompute', action='store_true', help='also recompute endpoints during partial audits')
    parser.add_argument('--analysis-output', type=Path, help='exclusive new full-family analysis file; required if deferred')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or (args.analysis_output is not None and args.analysis_output.exists()):
        parser.error('output files must not already exist')
    if args.analysis_output is not None and (args.completed_only or args.output.absolute() == args.analysis_output.absolute()):
        parser.error('analysis-output requires a full audit and a distinct new output file')
    root = args.root.resolve(strict=True)
    pool = json.loads((root / 'pool.json').read_text())
    pilot = json.loads(args.pilot_report.read_text())
    source_root = root.parent
    expected_sources = json.loads((source_root / 'source-sha256.json').read_text())
    for name, identity in expected_sources.items():
        if digest(source_root / name) != identity:
            raise ValueError(f'Frozen scientific source changed: {name}')
    if (digest(source_root / 'wca_acceptance.py') != pool['driver_sha256']
            or digest(source_root / 'wca_pool.py') != pool['pool_sha256']):
        raise ValueError('Pool/driver source identity differs from execution record')
    if digest(args.pilot_report) != pool['pilot_report_sha256']:
        raise ValueError('Frozen pilot report changed')
    if not args.completed_only:
        if pool['pending'] or pool['active']:
            raise ValueError('Confirmation still active/incomplete; no full-family acceptance')
        if not (root / 'analysis.json').exists() and args.analysis_output is None:
            raise ValueError('Analysis was deferred; specify an exclusive --analysis-output path')
    recompute = args.recompute or not args.completed_only
    if recompute and digest(Path(observables.__code__.co_filename)) != pool['driver_sha256']:
        raise ValueError('Raw recomputation requires the original scientific kernel source')
    chunks = [verify_chunk(root, record, pool, pilot, recompute=recompute) for record in pool['completed']]
    result = dict(root=str(root), complete_family=False, scientific_passed=False,
                  pool_updated_at=pool['updated_at'], chunks=chunks,
                  verified_replicas=sum(row['replicas'] for row in chunks),
                  verified_files=sum(row['files'] for row in chunks),
                  verified_bytes=sum(row['bytes'] for row in chunks),
                  auditor_sha256=digest(Path(__file__)),
                  analyzer_sha256=digest(Path(analyze.__code__.co_filename)),
                  raw_endpoints_recomputed=recompute)
    if not args.completed_only:
        if pool['pending'] or pool['active']:
            raise ValueError('Confirmation still active/incomplete; no full-family acceptance')
        recomputed = analyze(root, pilot, pool['completed'])
        recorded = root / 'analysis.json'
        if recorded.exists():
            # JSON persists protocol/layout tuples as arrays; compare the same
            # representation while retaining exact numerical/value checks.
            if json.loads(json.dumps(recomputed)) != json.loads(recorded.read_text()):
                raise ValueError('Full-family analysis does not reproduce')
        elif args.analysis_output is None:
            raise ValueError('Analysis was deferred; specify an exclusive --analysis-output path')
        processes = root_processes(root)
        if processes:
            raise ValueError(f'Scientific-root processes still present: {processes}')
        result.update(complete_family=True, scientific_passed=recomputed['passed'], analysis=recomputed)
        if args.analysis_output is not None:
            with args.analysis_output.open('x') as stream:
                json.dump(recomputed, stream, indent=2)
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps({key: result[key] for key in ('complete_family', 'scientific_passed',
                                                  'verified_replicas', 'verified_files', 'verified_bytes')}))


if __name__ == '__main__':
    main()
