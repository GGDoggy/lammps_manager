"""V007: transient systemd user services using only documented CLI operations."""
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.request

import yaml
from real_acceptance import ROOT, PYTHON, COMMANDS, setup, cli, submit, inspect
from distributed_acceptance import prepare
from artifact_acceptance import wait_for, port_open, sha
from restart_acceptance import journal, collected, converged
from lammps_manager.store import Store
from lammps_manager.recovery import session_members


def run():
    root = setup('v007', mode='staged')
    prepare(root)
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    prefix = ROOT.name + '-v007'
    units = [prefix + '-' + kind + '.service' for kind in ('manager', 'worker')]
    commands = []

    def command(argv, *, check=True):
        value = subprocess.run(argv, capture_output=True, text=True, timeout=60)
        commands.append(dict(argv=argv, returncode=value.returncode,
                             stdout=value.stdout, stderr=value.stderr, time=time.time()))
        if check:
            assert value.returncode == 0, commands[-1]
        return value

    def pid(unit, *, stopped=False):
        result = command(['systemctl', '--user', 'show', unit, '--property=MainPID', '--value'], check=False)
        if result.returncode and stopped and 'could not be found' in result.stderr:
            return 0  # systemd garbage-collects stopped transient units.
        assert result.returncode == 0, result.stderr
        return int(result.stdout)

    def restart(unit):
        previous = pid(unit)
        command(['systemctl', '--user', 'restart', unit])
        current = wait_for(lambda: pid(unit), 'new service pid', 10)
        assert current != previous
        return dict(unit=unit, old_pid=previous, new_pid=current, time=time.time())

    def start_unit(kind, argv):
        unit = prefix + '-' + kind
        return command(['systemd-run', '--user', '--unit', unit,
                        '--property=WorkingDirectory=' + str(ROOT),
                        '--property=Restart=on-failure', '--property=KillMode=process',
                        '--property=TimeoutStopSec=45', '--property=RuntimeMaxSec=900',
                        '--setenv=PYTHONPATH=' + str(ROOT / 'src'),
                        '--setenv=SSL_CERT_FILE=' + str(ROOT / 'server.crt'), *argv])

    assert not port_open(18448)
    for kind in ('manager', 'worker'):
        command([PYTHON, '-m', 'lammps_manager', 'validate-config', kind,
                 str(root / (kind + '.yaml')), '--check-local-paths'])
    restarts = []
    identities = []
    jobs = []
    evidence = dict(root=str(root), boot_before=boot)
    try:
        # systemd may create the unit before its client times out or is interrupted.
        start_unit('manager', [PYTHON, '-m', 'lammps_manager', 'serve', '--config',
                               str(root / 'manager.yaml'), '--credentials', str(root / 'credentials.json'),
                               '--host', '127.0.0.1', '--port', '18448', '--tls-cert',
                               str(ROOT / 'server.crt'), '--tls-key', str(ROOT / 'server.key')])
        wait_for(lambda: port_open(18448), 'manager listener', 10)
        request = urllib.request.Request('https://127.0.0.1:18448/v1/poll', data=b'{}',
                                         headers={'Authorization': 'Bearer invalid-acceptance-credential',
                                                  'X-Worker-ID': 'v007', 'Content-Type': 'application/json'})
        try:
            urllib.request.urlopen(request, context=ssl.create_default_context(), timeout=5)
        except urllib.error.URLError as exc:
            assert isinstance(exc.reason, ssl.SSLCertVerificationError), exc
            evidence['untrusted_certificate_rejected'] = True
        else:
            raise AssertionError('Untrusted private certificate accepted')
        try:
            urllib.request.urlopen(request, context=ssl.create_default_context(
                cafile=str(ROOT / 'server.crt')), timeout=5)
        except urllib.error.HTTPError as exc:
            assert exc.code == 401
            evidence['invalid_credential_status'] = exc.code
        else:
            raise AssertionError('Invalid credential accepted')
        start_unit('worker', [PYTHON, '-m', 'lammps_manager', 'worker', '--config',
                              str(root / 'worker.yaml'), '--state-directory', str(root / 'journal')])
        wait_for(lambda: cli(root / 'manager.yaml', 'status')['workers'], 'authenticated worker', 15)
        first, = submit(root, [(1000, 30, False)])
        jobs.append(first)
        wait_for(lambda: collected(root, first), 'first smoke collected', 60)
        retained = journal(root)
        assert retained['records'][0]['exists']
        snapshots = specification_bytes(root)
        restarts.append(restart(units[0]))
        restarts.append(restart(units[1]))
        wait_for(lambda: collected(root, first), 'first smoke after restart', 15)
        assert journal(root)['records'][0]['exists']
        assert specification_bytes(root) == snapshots
        config = yaml.safe_load((root / 'worker.yaml').read_text())
        config['cleanup_staged_files'] = True
        (root / 'worker.yaml').write_text(yaml.safe_dump(config))
        restarts.append(restart(units[1]))
        wait_for(lambda: converged(root, first), 'retention enabled cleanup', 30)
        assert specification_bytes(root) == snapshots
        second, = submit(root, [(1000, 30, False)])
        jobs.append(second)
        wait_for(lambda: converged(root, second), 'second smoke collected cleaned', 60)
        records = inspect(root, jobs)
        hashes = {}
        for record in records:
            assert record['state'] == 'succeeded' and len(record['attempts']) == 1
            attempt = record['attempts'][0]
            assert attempt['termination_evidence']
            output = Path(attempt['results_directory'])
            stdout = (output / 'stdout').read_text()
            assert 'LAMMPS (' in stdout and stdout.count('Total wall time:') == 1
            identity = json.loads((output / 'process.json').read_text())
            assert not session_members(identity['pid'])
            identities.append(dict(identity=identity, members=[]))
            for item in attempt['result_collection']['manifest']['files']:
                path = output / item['path']
                assert path.stat().st_size == item['size'] and sha(path) == item['sha256']
                hashes[str(path.relative_to(root))] = item
        final = journal(root)
        assert not final['reservations']
        with Store(root / 'manager.sqlite') as store:
            assert not store.reservations()
        logs = command(['journalctl', '--user', '-u', units[0], '-u', units[1], '--no-pager', '-o', 'cat']).stdout
        token = (root / 'token').read_text()
        assert token not in logs and token not in json.dumps(COMMANDS + commands)
        (root / 'sanitized-service.log').write_text(logs)
        evidence.update(records=records, retained=retained, final_journal=final,
                        snapshots_sha256={k: hashlib.sha256(v.encode()).hexdigest() for k, v in snapshots.items()},
                        restarts=restarts, result_hashes=hashes, sessions=identities,
                        logs_sha256=sha(root / 'sanitized-service.log'))
    finally:
        command(['systemctl', '--user', 'stop', units[1], units[0]], check=False)
    evidence.update(commands=commands + COMMANDS,
                    boot_after=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                    ports_closed=not port_open(18448),
                    stopped_pids={unit: pid(unit, stopped=True) for unit in units})
    assert evidence['boot_after'] == boot and evidence['ports_closed']
    assert all(value == 0 for value in evidence['stopped_pids'].values())
    evidence['passed'] = True
    (ROOT / 'v007.json').write_text(json.dumps(evidence, indent=2))
    print('V007 passed', flush=True)


def specification_bytes(root):
    import sqlite3
    with sqlite3.connect(root / 'manager.sqlite') as connection:
        return dict(connection.execute('SELECT job_id,specification FROM jobs'))


if __name__ == '__main__':
    run()
