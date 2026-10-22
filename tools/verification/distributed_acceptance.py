"""Explicit isolated real-host concurrency/recovery verification harness."""
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import datetime

import yaml
from real_acceptance import ROOT, ENV, PYTHON, COMMANDS, setup, submit, cli, inspect, audit
from lammps_manager.store import Store

PROCESSES = []
TIMELINE = []


def start(argv, root, name):
    out = (root / (name + '.log')).open('a')
    p = subprocess.Popen(argv, env={**ENV, 'SSL_CERT_FILE': str(ROOT / 'server.crt')},
                         stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    out.close()
    PROCESSES.append(p)
    COMMANDS.append(dict(argv=argv, pid=p.pid, time=time.time()))
    return p


def stop(p):
    if p.poll() is None:
        p.send_signal(signal.SIGINT)
        try:
            p.wait(timeout=45)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait(timeout=5)
    TIMELINE.append(dict(event='stopped', pid=p.pid, code=p.returncode, time=time.time()))


def prepare(root, *, port=18448):
    token = root / 'token'
    token.write_text(secrets.token_urlsafe(32))
    token.chmod(0o600)
    (root / 'credentials.json').write_text(json.dumps({root.name: str(token)}))
    (root / 'credentials.json').chmod(0o600)
    if not (ROOT / 'server.crt').exists():
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(ROOT / 'server.key'), '-out', str(ROOT / 'server.crt'),
                        '-days', '2', '-subj', '/CN=Isolated acceptance', '-addext',
                        'subjectAltName=IP:127.0.0.1', '-addext', 'basicConstraints=critical,CA:TRUE'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        (ROOT / 'server.key').chmod(0o600)
    worker = yaml.safe_load((root / 'worker.yaml').read_text())
    worker['manager_endpoint'] = 'https://127.0.0.1:' + str(port)
    (root / 'worker.yaml').write_text(yaml.safe_dump(worker))


def manager(root):
    p = start([PYTHON, '-m', 'lammps_manager', 'serve', '--config', str(root / 'manager.yaml'),
               '--credentials', str(root / 'credentials.json'), '--host', '127.0.0.1',
               '--port', '18448', '--tls-cert', str(ROOT / 'server.crt'),
               '--tls-key', str(ROOT / 'server.key')], root, 'manager')
    # Own the spawned process until readiness returns its handle to the caller.
    # In particular, a timeout must not leave a process the caller cannot stop.
    try:
        for _ in range(80):
            assert p.poll() is None, (root / 'manager.log').read_text()
            try:
                with socket.create_connection(('127.0.0.1', 18448), timeout=.1):
                    return p
            except OSError:
                time.sleep(.05)
        raise AssertionError('Manager did not listen')
    except BaseException:
        stop(p)
        raise


def worker(root):
    return start([PYTHON, '-m', 'lammps_manager', 'worker', '--config', str(root / 'worker.yaml'),
                  '--state-directory', str(root / 'journal')], root, 'worker')


def sample(root, label):
    with Store(root / 'manager.sqlite') as store:
        reservations = [asdict(r) for r in store.reservations()]
        attempts = [asdict(a) for j in store.jobs() for a in store.job_attempts(j['job_id'])]
        jobs = store.jobs()
        workers = store.worker_records()
    assert sum(r['cpu_slots'] for r in reservations) <= 4
    assert sum(r['memory_gib'] for r in reservations) <= 1
    assert len(reservations) <= 2
    for r in reservations:
        assert r['cpu_slots'] == 2 and r['memory_gib'] == .1
    value = dict(time=time.time(), label=label, jobs=jobs, attempts=attempts,
                 reservations=reservations, workers=workers)
    TIMELINE.append(value)
    # Durable partial evidence survives a driver failure.
    with (root / 'timeline.jsonl').open('a') as out:
        out.write(json.dumps(value) + '\n')
    return value


def wait(root, predicate, label, timeout=70):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = sample(root, label)
        if predicate(value):
            return value
        time.sleep(.08)
    raise AssertionError('Timed out: ' + label)


def terminal(value, jobs):
    return all(j['state'] in {'succeeded', 'failed', 'cancelled'}
               for j in value['jobs'] if j['job_id'] in jobs)


def intervals(records):
    return [(datetime.fromisoformat(r['attempts'][0]['started_at'].replace('Z', '+00:00')).timestamp(),
             datetime.fromisoformat(r['attempts'][0]['ended_at'].replace('Z', '+00:00')).timestamp())
            for r in records]


def concurrency():
    root = setup('v003')
    prepare(root)
    m = manager(root)
    w = worker(root)
    try:
        jobs = submit(root, [(150000, 30, False)] * 3)
        full = wait(root, lambda v: len([a for a in v['attempts'] if a['state'] == 'running']) == 2,
                    'two-running-third-queued', timeout=20)
        assert next(j for j in full['jobs'] if j['job_id'] == jobs[2])['state'] == 'queued'
        wait(root, lambda v: terminal(v, jobs), 'first-batch-complete')
        records = inspect(root, jobs)
        assert all(r['state'] == 'succeeded' for r in records), records
        ranges = intervals(records)
        assert max(ranges[0][0], ranges[1][0]) < min(ranges[0][1], ranges[1][1])
        assert ranges[2][0] >= min(ranges[0][1], ranges[1][1])
        # Cancel one real MPI session while the other continues to its own timeout.
        extra = submit(root, [(100000000, 30, False), (100000000, 6, False), (150000, 30, False)])
        running = wait(root, lambda v: all(any(a['job_id'] == j and a['state'] == 'running'
                        for a in v['attempts']) for j in extra[:2]), 'cancel-pair-running', timeout=20)
        cli(root / 'manager.yaml', 'cancel', extra[0])
        after = wait(root, lambda v: any(a['job_id'] == extra[0] and a['state'] == 'cancelled'
                                       for a in v['attempts']), 'cancelled-one')
        assert any(a['job_id'] == extra[1] and a['state'] == 'running' for a in after['attempts'])
        wait(root, lambda v: terminal(v, extra), 'cancel-timeout-complete')
        records += inspect(root, extra)
        assert [r['state'] for r in records[3:]] == ['cancelled', 'cancelled', 'succeeded']
        assert 'Time limit exceeded' in str(records[4])
        assert all(len(r['attempts']) == 1 for r in records)
        for r in records:
            a = r['attempts'][0]
            assert a['termination_evidence']
            if r['state'] == 'succeeded':
                assert (Path(a['directory']) / 'work/output.data').is_file()
        cli(root / 'manager.yaml', 'status')
        result = dict(records=records, timeline=TIMELINE, commands=COMMANDS,
                      audit=audit(root), affinity=sorted(os.sched_getaffinity(0)),
                      hashes={str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in (root / 'jobs').rglob('*') if p.is_file()})
        (ROOT / 'v003.json').write_text(json.dumps(result, indent=2))
        print('V003 passed', flush=True)
    finally:
        stop(w)
        stop(m)


if __name__ == '__main__':
    # Four CPUs from the invoking allocation; child MPI inherits this affinity.
    cpus = sorted(os.sched_getaffinity(0))[:4]
    assert len(cpus) == 4
    os.sched_setaffinity(0, cpus)
    concurrency()
