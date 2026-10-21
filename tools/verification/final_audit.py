"""Read-only final audit of the private real-acceptance root."""
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import time

from lammps_manager.recovery import session_members
from lammps_manager.store import Store

root = Path.cwd()
source = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
          for p in (root / 'src').rglob('*.py')}
expected = json.loads((root / 'source-sha256.json').read_text())
assert all(source[p] == digest for p, digest in expected.items() if p.startswith('src/'))
processes = []
for path in Path('/proc').iterdir():
    if not path.name.isdigit() or int(path.name) == os.getpid():
        continue
    try:
        cwd = (path / 'cwd').resolve(strict=True)
        argv = (path / 'cmdline').read_bytes().replace(b'\x00', b' ').decode(errors='replace')
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        continue
    # The enclosing SSH shell is the read-only audit itself.
    if cwd.is_relative_to(root) and int(path.name) != os.getppid():
        processes.append(dict(pid=int(path.name), cwd=str(cwd), argv=argv))
assert not processes, processes
reservations = {}
for database in root.rglob('*.sqlite*'):
    if database.name.endswith(('-wal', '-shm')):
        continue
    with Store(database) as store:
        reservations[str(database.relative_to(root))] = [r.__dict__ for r in store.reservations()]
assert all(not value for value in reservations.values())
sessions = []
for name in ['v008', 'v003', 'v004', 'v005']:
    evidence = json.loads((root / (name + '.json')).read_text())
    for record in evidence['records']:
        attempt = record['attempts'][0]
        directory = Path(attempt['directory'])
        path = directory / ('results/process.json' if name == 'v005' else 'process.json')
        if path.exists():
            identity = json.loads(path.read_text())
            members = session_members(identity['pid'])
            assert not members
            sessions.append(dict(task=name, attempt=attempt['attempt_id'], identity=identity, members=members))
listeners = {}
for port in [18448, 18449]:
    with socket.socket() as sock:
        sock.settimeout(1)
        listeners[str(port)] = sock.connect_ex(('127.0.0.1', port))
assert all(code != 0 for code in listeners.values())
versions = {command[0]: subprocess.run(command, text=True, capture_output=True, check=True).stdout
            for command in [['/usr/bin/mpirun.openmpi', '--version'], ['/usr/bin/lmp', '-help']]}
result = dict(time=time.time(), host=platform.node(), boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
              root=str(root), source_sha256=source, deployed_manifest=expected,
              harness_sha256={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.glob('*.py')},
              sessions=sessions, reservations=reservations, remaining_root_processes=processes,
              closed_ports=listeners, versions=versions,
              v008_status=json.loads((root / 'v008-status.json').read_text()))
(root / 'final-audit.json').write_text(json.dumps(result, indent=2))
print('Final audit passed', len(sessions), 'ended MPI sessions')
