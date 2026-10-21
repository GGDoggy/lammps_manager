"""Actual manager/worker OS restarts and HTTPS transport interruption."""
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import time

import yaml
from real_acceptance import ROOT, PYTHON, COMMANDS, setup, submit, inspect
from distributed_acceptance import start, stop, prepare, manager, worker, sample, wait, TIMELINE
from lammps_manager.store import Store
from lammps_manager.recovery import session_members, process_identity

IDENTITIES = []


def journal(root):
    with Store(root / 'journal/attempts.sqlite3') as store:
        values = []
        for j in store.jobs():
            for a in store.job_attempts(j['job_id']):
                directory = Path(a.directory)
                values.append(dict(attempt=a.__dict__, collection=store.result_collection(a.attempt_id),
                                   cleanup=store.cleanup_record(a.attempt_id), exists=directory.exists(),
                                   launch_claimed=store.launch_claimed(a.attempt_id)))
                path = directory / 'process.json'
                if path.exists():
                    identity = json.loads(path.read_text())
                    if identity not in IDENTITIES:
                        IDENTITIES.append(identity)
        return dict(records=values, reservations=[r.__dict__ for r in store.reservations()])


def gate(root, mode):
    temporary = root / 'fault.tmp'
    temporary.write_text(mode)
    temporary.replace(root / 'fault')
    TIMELINE.append(dict(event='fault-mode', mode=mode, time=time.time()))


def collected(root, job):
    with Store(root / 'manager.sqlite') as store:
        attempts = store.job_attempts(job)
        if len(attempts) != 1:
            return False
        collection = store.result_collection(attempts[0].attempt_id)
        return bool(collection and collection['status'] == 'collected')


def converged(root, job):
    if not collected(root, job):
        return False
    value = journal(root)
    rows = [r for r in value['records'] if r['attempt']['job_id'] == job]
    return bool(rows and rows[0]['collection'] and rows[0]['collection']['status'] == 'collected'
                and rows[0]['cleanup'] and rows[0]['cleanup']['status'] == 'cleaned'
                and not rows[0]['exists'])


def fault_seen(root, event):
    path = root / 'proxy-events.jsonl'
    return path.exists() and any(json.loads(line).get('event') == event for line in path.read_text().splitlines())


def run():
    cpus = sorted(os.sched_getaffinity(0))[:4]
    assert len(cpus) == 4
    os.sched_setaffinity(0, cpus)
    root = setup('v005', mode='staged')
    prepare(root, port=18449)
    config = yaml.safe_load((root / 'worker.yaml').read_text())
    config['cleanup_staged_files'] = True
    (root / 'worker.yaml').write_text(yaml.safe_dump(config))
    gate(root, 'normal')
    m = manager(root)
    proxy = start([PYTHON, str(ROOT / 'fault_proxy.py'), str(root)], root, 'proxy')
    for _ in range(100):
        try:
            with socket.create_connection(('127.0.0.1', 18449), timeout=.1):
                break
        except OSError:
            time.sleep(.05)
    w = worker(root)
    jobs = []
    cases = []
    try:
        for mode in ['manager_restart', 'worker_restart', 'contact_outage', 'upload', 'commit']:
            gate(root, mode if mode in {'upload', 'commit'} else 'normal')
            job, = submit(root, [(100000000 if mode not in {'upload', 'commit'} else 1000,
                                 15 if mode == 'worker_restart' else 30, False)])
            jobs.append(job)
            if mode not in {'upload', 'commit'}:
                running = wait(root, lambda v: any(a['job_id'] == job and a['state'] == 'running'
                                                   for a in v['attempts']), mode + '-running', timeout=20)
                original = journal(root)
                if mode == 'manager_restart':
                    old_pid = m.pid
                    stop(m)
                    time.sleep(2.5)
                    during = sample(root, 'manager-process-absent')
                    assert len(during['reservations']) == 1
                    m = manager(root)
                    assert m.pid != old_pid
                    resumed = wait(root, lambda v: any(a['job_id'] == job and a['state'] == 'running'
                                                       for a in v['attempts']), 'manager-reconciled')
                elif mode == 'worker_restart':
                    old_pid = w.pid
                    w.kill()
                    w.wait(timeout=5)
                    TIMELINE.append(dict(event='worker-killed', pid=old_pid, time=time.time()))
                    time.sleep(3)
                    during = sample(root, 'worker-absent-heartbeat-expired')
                    assert any(a['job_id'] == job and a['state'] == 'unknown' for a in during['attempts'])
                    assert len(during['reservations']) == 1
                    identity = IDENTITIES[-1]
                    assert process_identity(identity['pid'])['start_ticks'] == identity['start_ticks']
                    assert session_members(identity['pid'])
                    w = worker(root)
                    assert w.pid != old_pid
                    resumed = wait(root, lambda v: any(a['job_id'] == job and a['state'] == 'running'
                                                       for a in v['attempts']), 'worker-live-identity-reconciled')
                else:
                    gate(root, 'offline')
                    time.sleep(3)
                    during = sample(root, 'contact-outage-heartbeat-expired')
                    assert len(during['reservations']) == 1
                    assert any(a['job_id'] == job and a['state'] == 'unknown' for a in during['attempts'])
                    gate(root, 'normal')
                    resumed = wait(root, lambda v: any(a['job_id'] == job and a['state'] == 'running'
                                                       for a in v['attempts']), 'contact-recovered')
                if mode != 'worker_restart':
                    from real_acceptance import cli
                    cli(root / 'manager.yaml', 'cancel', job)
                # Worker-restart case deliberately reaches the persisted time limit.
                wait(root, lambda v: converged(root, job), mode + '-collected-cleaned', timeout=40)
                cases.append(dict(mode=mode, job=job, before=original, during=during, resumed=resumed))
            else:
                event = 'partial_upload' if mode == 'upload' else 'lost_commit'
                wait(root, lambda v: fault_seen(root, event), event + '-injected', timeout=25)
                # Proxy holds subsequent result operations until this evidence is captured.
                time.sleep(.3)
                during = sample(root, event + '-held')
                local = journal(root)
                row = next(r for r in local['records'] if r['attempt']['job_id'] == job)
                assert row['exists'] and row['collection']['status'] != 'collected'
                assert row['cleanup'] is None or row['cleanup']['status'] != 'cleaned'
                if mode == 'commit':
                    assert collected(root, job)
                else:
                    assert not collected(root, job)
                gate(root, 'normal')
                wait(root, lambda v: converged(root, job), mode + '-replay-collected-cleaned', timeout=30)
                cases.append(dict(mode=mode, job=job, during=during, retained=local))
        records = inspect(root, jobs)
        assert all(len(r['attempts']) == 1 for r in records)
        assert [r['state'] for r in records] == ['cancelled'] * 3 + ['succeeded'] * 2
        hashes = {}
        for r in records:
            attempt = r['attempts'][0]
            assert attempt['termination_evidence']
            collection = attempt['result_collection']
            assert collection['status'] == 'collected'
            directory = Path(attempt['results_directory'])
            for item in collection['manifest']['files']:
                path = directory / item['path']
                assert path.stat().st_size == item['size']
                digest = hashlib.sha256(path.read_bytes()).hexdigest()
                assert digest == item['sha256']
                hashes[str(path.relative_to(root))] = digest
                if path.name == 'process.json':
                    identity = json.loads(path.read_text())
                    if identity not in IDENTITIES:
                        IDENTITIES.append(identity)
        final = journal(root)
        assert not final['reservations']
        with Store(root / 'manager.sqlite') as store:
            assert not store.reservations()
        sessions = [dict(identity=i, members=session_members(i['pid'])) for i in IDENTITIES]
        assert len(sessions) == len(jobs)
        assert all(not i['members'] for i in sessions)
        evidence = dict(cases=cases, records=records, commands=COMMANDS, timeline=TIMELINE,
                        hashes=hashes, final_journal=final, sessions=sessions,
                        affinity=cpus, proxy_events=[json.loads(line) for line in
                                                   (root / 'proxy-events.jsonl').read_text().splitlines()])
        (ROOT / 'v005.json').write_text(json.dumps(evidence, indent=2))
        print('V005 passed', flush=True)
    finally:
        gate(root, 'normal')
        stop(w)
        stop(proxy)
        stop(m)


if __name__ == '__main__':
    run()
