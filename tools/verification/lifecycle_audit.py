"""Read-only audit of an explicitly supplied V006 or V007 private root."""
import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import stat

from lammps_manager.recovery import session_members
from lammps_manager.results import validate_results
from lammps_manager.staging import open_regular


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(65536):
            value.update(chunk)
    return value.hexdigest()


def audit_results(root: Path) -> dict[str, dict]:
    """Verify every manager-recorded publication against its committed manifest."""
    outputs = {}
    publications = set()
    for database in sorted(root.glob('v00*/manager.sqlite')):
        with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as connection:
            rows = connection.execute(
                'SELECT a.attempt_id,a.directory,r.status,r.manifest FROM attempts a '
                'LEFT JOIN result_collections r ON r.attempt_id=a.attempt_id').fetchall()
        for attempt_id, directory, status, manifest_json in rows:
            if status != 'collected' or manifest_json is None:
                raise ValueError(f'Results not committed: {attempt_id}')
            destination = Path(directory) / 'results'
            if (not destination.is_relative_to(database.parent / 'jobs')
                    or destination.resolve(strict=True) != destination):
                raise ValueError(f'Unsafe result publication: {destination}')
            files = validate_results(json.loads(manifest_json), {})
            expected = {item.path: dict(size=item.size, sha256=item.sha256) for item in files}
            actual = {}

            def walk_error(error: OSError) -> None:
                raise error

            for parent, directories, names in os.walk(destination, onerror=walk_error):
                for name in directories + names:
                    path = Path(parent) / name
                    mode = path.lstat().st_mode
                    if stat.S_ISDIR(mode):
                        continue
                    if not stat.S_ISREG(mode):
                        raise ValueError(f'Unsafe result artifact: {path}')
                    relative = path.relative_to(destination).as_posix()
                    value = hashlib.sha256()
                    size = 0
                    with open_regular(destination, relative) as stream:
                        while chunk := stream.read(65536):
                            size += len(chunk)
                            value.update(chunk)
                    actual[relative] = dict(size=size, sha256=value.hexdigest())
            if actual != expected:
                raise ValueError(f'Result files differ from committed manifest: {attempt_id}')
            publications.add(destination)
            outputs.update({str((destination / name).relative_to(root)): identity
                            for name, identity in actual.items()})
    discovered = set(root.glob('v00*/jobs/*/*/results'))
    if not publications or publications != discovered:
        raise ValueError('Result publications differ from manager records')
    return outputs


def audit(root: Path) -> None:
    expected = json.loads((root / 'source-sha256.json').read_text())
    for name, identity in expected.items():
        assert digest(root / name) == identity, name
    reservations = {}
    sessions = []
    outputs = audit_results(root)
    for database in root.rglob('*.sqlite*'):
        if database.suffix in {'.sqlite', '.sqlite3'}:
            with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as connection:
                reservations[str(database.relative_to(root))] = connection.execute(
                    'SELECT COUNT(*) FROM reservations').fetchone()[0]
    assert reservations and not any(reservations.values())
    for path in root.glob('v00*/jobs/*/*/results/process.json'):
        identity = json.loads(path.read_text())
        members = session_members(identity['pid'])
        assert not members
        sessions.append(dict(path=str(path.relative_to(root)), identity=identity, members=members))
    assert sessions
    processes = []
    for path in Path('/proc').iterdir():
        if path.name.isdigit():
            try:
                argv = (path / 'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')
                # This auditor itself is permitted. Any service or simulation
                # retaining a root path or current directory is not.
                if int(path.name) == __import__('os').getpid():
                    continue
                cwd = (path / 'cwd').resolve()
                if str(root) in argv or cwd == root or root in cwd.parents:
                    processes.append(dict(pid=int(path.name), argv=argv))
            except (OSError, PermissionError):
                continue
    assert not processes, processes
    ports = {}
    for port in (18448, 18449):
        try:
            with socket.create_connection(('127.0.0.1', port), timeout=.1):
                ports[str(port)] = 'open'
        except ConnectionRefusedError:
            ports[str(port)] = 'closed'
    assert all(v == 'closed' for v in ports.values())
    result = dict(root=str(root), source_sha256=expected, reservations=reservations,
                  sessions=sessions, outputs=outputs, processes=processes, ports=ports,
                  boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                  python=__import__('sys').version, pyyaml=__import__('yaml').__version__)
    (root / 'final-audit.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(dict(root=str(root), files=len(outputs), sessions=len(sessions),
                          reservations=reservations, ports=ports)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    audit(args.root.resolve())
