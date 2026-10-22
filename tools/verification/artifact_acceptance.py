"""V006: explicit bounded staged transfer, actual process restart and audit.

Run from a new private root with src/ and the documented helper drivers.
No host reboot, system configuration changes or historical output deletion.
"""
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import threading
import time

import yaml
from real_acceptance import ROOT, PYTHON, COMMANDS, setup, cli, inspect, INPUT
from distributed_acceptance import start, stop, prepare, manager, worker
from restart_acceptance import journal, collected, converged
from lammps_manager.store import Store
from lammps_manager.recovery import session_members

BUDGET = dict(large_bytes=64 * 1024**2, small_files=1000, small_bytes=4096,
              disk_bytes=2 * 1024**3, rss_bytes=512 * 1024**2, elapsed_seconds=900,
              poll_seconds=5, heartbeat_gap_seconds=10)


def sha(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while chunk := stream.read(65536):
            digest.update(chunk)
    return digest.hexdigest()


def events(root):
    path = root / 'proxy-events.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def gate(root, mode):
    (root / 'fault').write_text(mode)


def wait_for(predicate, label, timeout=180):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(.1)
    raise AssertionError('Timed out: ' + label)


def reservations(root):
    with Store(root / 'manager.sqlite') as store:
        return [r.__dict__ for r in store.reservations()]


def run():
    begun = time.monotonic()
    root = setup('v006', mode='staged')
    prepare(root, port=18449)
    (root / 'budget.json').write_text(json.dumps(BUDGET, indent=2))
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    free = os.statvfs(root).f_bavail * os.statvfs(root).f_frsize
    assert free > BUDGET['disk_bytes'] * 2
    source = root / 'source'
    source.mkdir()
    (source / 'in.lmp').write_text(INPUT.replace('STEPS', '1000'))
    # Deterministic incompressible-looking repeated blocks, bounded generation RSS.
    block = b''.join(hashlib.sha256(str(i).encode()).digest() for i in range(2048))
    with (source / 'large.bin').open('wb') as out:
        for _ in range(BUDGET['large_bytes'] // len(block)):
            out.write(block)
    small = source / 'small'
    small.mkdir()
    for i in range(BUDGET['small_files']):
        (small / f'{i:04}.bin').write_bytes(bytes([i % 251]) * BUDGET['small_bytes'])
    inputs = {str(p.relative_to(source)): dict(size=p.stat().st_size, sha256=sha(p))
              for p in source.rglob('*') if p.is_file()}
    manifest = root / 'batch.json'
    manifest.write_text(json.dumps([dict(source_directory=str(source), input_file='in.lmp')]))
    # Reject the very same valid tree under a deliberately smaller configured limit.
    limited = yaml.safe_load((root / 'manager.yaml').read_text())
    limited['storage']['max_input_file_bytes'] = BUDGET['large_bytes'] - 1
    (root / 'limited.yaml').write_text(yaml.safe_dump(limited))
    import subprocess
    rejected = subprocess.run([PYTHON, '-m', 'lammps_manager', 'submit', '--config',
                               str(root / 'limited.yaml'), str(manifest)],
                              capture_output=True, text=True, timeout=30)
    assert rejected.returncode != 0 and 'limit' in rejected.stderr.lower(), rejected.stderr
    with Store(root / 'manager.sqlite') as store:
        assert not store.jobs() and not store.reservations()
    job, = cli(root / 'manager.yaml', 'submit', manifest)
    gate(root, 'input')
    m = proxy = w = None
    processes = []
    samples = []
    sampling = threading.Event()
    sampling.set()

    def monitor():
        while sampling.is_set():
            value = dict(time=time.time(), rss={})
            for name, process in list(processes):
                try:
                    rows = Path(f'/proc/{process.pid}/status').read_text().splitlines()
                    value['rss'][f'{name}:{process.pid}'] = next(
                        int(row.split()[1]) * 1024 for row in rows if row.startswith('VmRSS:'))
                except (OSError, StopIteration):
                    pass
            disk = 0
            for path in ROOT.rglob('*'):
                try:
                    disk += path.stat().st_blocks * 512
                except FileNotFoundError:
                    pass
            value['disk_bytes'] = disk
            samples.append(value)
            with (root / 'samples.jsonl').open('a') as stream:
                stream.write(json.dumps(value) + '\n')
            time.sleep(.2)

    observer = threading.Thread(target=monitor, daemon=True)

    def stop_sampling() -> None:
        sampling.clear()
        if observer.ident is not None:
            observer.join(timeout=10)

    cases = []
    try:
        m = manager(root)
        proxy = start([PYTHON, str(ROOT / 'artifact_proxy.py'), str(root)], root, 'proxy')
        wait_for(lambda: port_open(18449), 'proxy listening', 10)
        w = worker(root)
        processes.extend([('manager', m), ('proxy', proxy), ('worker', w)])
        observer.start()
        wait_for(lambda: any(e['event'] == 'partial_input' for e in events(root)), 'partial input')
        wait_for(lambda: local_transfer(root) == 'failed', 'failed input journal')
        before = journal(root)
        row = before['records'][0]
        assert not row['launch_claimed'] and not (Path(row['attempt']['directory']) / 'inputs').exists()
        assert len(reservations(root)) == 1
        old_pid = w.pid
        stop(w)
        gate(root, 'result')
        w = worker(root)
        processes.append(('worker', w))
        assert w.pid != old_pid
        cases.append(dict(event='input-worker-restart', old_pid=old_pid, new_pid=w.pid,
                          journal=before, time=time.time()))
        wait_for(lambda: any(e['event'] == 'partial_result' for e in events(root)), 'partial result')
        local = journal(root)
        row = local['records'][0]
        assert row['exists'] and row['collection']['status'] != 'collected'
        assert not collected(root, job)
        with Store(root / 'manager.sqlite') as store:
            attempt = store.job_attempts(job)[0]
            assert not (Path(attempt.directory) / 'results').exists()
        old_pid = m.pid
        stop(m)
        m = manager(root)
        processes.append(('manager', m))
        assert m.pid != old_pid
        cases.append(dict(event='result-manager-restart', old_pid=old_pid, new_pid=m.pid,
                          journal=local, time=time.time()))
        # Restart the responsible uploader too; reuse its journal and original attempt.
        old_pid = w.pid
        stop(w)
        gate(root, 'normal')
        w = worker(root)
        processes.append(('worker', w))
        cases.append(dict(event='result-worker-restart', old_pid=old_pid, new_pid=w.pid,
                          time=time.time()))
        wait_for(lambda: collected(root, job), 'durable results')
        retained = journal(root)
        assert retained['records'][0]['exists']
        record, = inspect(root, [job])
        assert record['state'] == 'succeeded' and len(record['attempts']) == 1
        attempt = record['attempts'][0]
        output = Path(attempt['results_directory'])
        hashes = {}
        for item in attempt['result_collection']['manifest']['files']:
            path = output / item['path']
            assert path.stat().st_size == item['size'] and sha(path) == item['sha256']
            hashes[item['path']] = dict(size=item['size'], sha256=item['sha256'])
        for relative, identity in inputs.items():
            assert hashes['work/' + relative] == identity
        assert (output / 'stdout').read_text().count('Total wall time:') == 1
        identity = json.loads((output / 'process.json').read_text())
        assert not session_members(identity['pid']) and not reservations(root)
        with Store(root / 'journal/attempts.sqlite3') as store:
            assert not store.reservations()
            execution_events = store.events(job)
            assert sum(e['state'] == 'running' for e in execution_events) == 1
        evidence_events = execution_events
        config = yaml.safe_load((root / 'worker.yaml').read_text())
        config['cleanup_staged_files'] = True
        (root / 'worker.yaml').write_text(yaml.safe_dump(config))
        stop(w)
        w = worker(root)
        processes.append(('worker', w))
        wait_for(lambda: converged(root, job), 'acknowledged cleanup', 60)
        evidence = dict(budget=BUDGET, boot_before=boot, free_before=free, inputs=inputs,
                        outputs=hashes, cases=cases, record=record,
                        rejected=dict(returncode=rejected.returncode, stderr=rejected.stderr),
                        retained_after_ack=retained, final_journal=journal(root),
                        sessions=[dict(identity=identity, members=[])],
                        execution_events=evidence_events)
    finally:
        # Attempt every cleanup even when startup, fault reset or another stop
        # fails. Only stop handles actually returned, and join a started thread.
        with ExitStack() as cleanup:
            cleanup.callback(stop_sampling)
            for process in (m, proxy, w):
                if process is not None:
                    cleanup.callback(stop, process)
            gate(root, 'normal')
    elapsed = time.monotonic() - begun
    polls = [e for e in events(root) if e['event'] == 'poll' and e['status'] == 200]
    # Gaps include actual planned transfer/process restart intervals.
    gaps = [b['time'] - a['time'] for a, b in zip(polls, polls[1:])]
    metrics = dict(elapsed_seconds=elapsed,
                   peak_rss_bytes=max(v for s in samples for v in s['rss'].values()),
                   peak_disk_bytes=max(s['disk_bytes'] for s in samples),
                   max_poll_seconds=max(e['elapsed'] for e in polls),
                   max_heartbeat_gap_seconds=max(gaps), samples=len(samples))
    evidence.update(metrics=metrics, commands=COMMANDS, proxy_events=events(root),
                    boot_after=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                    ports_closed=not port_open(18448) and not port_open(18449))
    gates = dict(elapsed=elapsed <= BUDGET['elapsed_seconds'],
                 rss=metrics['peak_rss_bytes'] <= BUDGET['rss_bytes'],
                 disk=metrics['peak_disk_bytes'] <= BUDGET['disk_bytes'],
                 poll=metrics['max_poll_seconds'] <= BUDGET['poll_seconds'],
                 heartbeat=metrics['max_heartbeat_gap_seconds'] <= BUDGET['heartbeat_gap_seconds'])
    evidence['gates'] = gates
    evidence['passed'] = all(gates.values()) and evidence['ports_closed'] and evidence['boot_after'] == boot
    (ROOT / 'v006.json').write_text(json.dumps(evidence, indent=2))
    assert evidence['passed'], metrics
    print('V006 passed', metrics, flush=True)


def local_transfer(root):
    try:
        with Store(root / 'journal/attempts.sqlite3') as store:
            records = [store.input_transfer(a.attempt_id) for j in store.jobs()
                       for a in store.job_attempts(j['job_id'])]
        return records[0]['status'] if records and records[0] else None
    except sqlite3.OperationalError:
        return None


def port_open(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=.1):
            return True
    except OSError:
        return False


if __name__ == '__main__':
    run()
