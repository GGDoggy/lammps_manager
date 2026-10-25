"""Explicit local fake-engine concurrency soak; never touches deployed services.

Run with PYTHONPATH=src python3 tools/verification/control_load.py --root PATH.
The large model comes from an optional SQLite API backup, never a live DB copy.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import logging
from pathlib import Path
import sqlite3
import sys
import threading
import time
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from lammps_manager.config import Execution, ManagerConfig, ManagerSettings, Scheduler, Storage, WorkerCapacity, WorkerConfig
from lammps_manager.continuation import Recipe
from lammps_manager.manager import ManagerService, make_server
from lammps_manager.models import Resources
from lammps_manager.segments import submit_run, cancel_run
from lammps_manager.store import Store
from lammps_manager.worker import Worker


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--seconds', type=float, default=1800)
    parser.add_argument('--snapshot-backup', type=Path)
    args = parser.parse_args()
    root = args.root.resolve(); root.mkdir(parents=True, exist_ok=False)
    logging.basicConfig(filename=root / 'timings.log', level=logging.DEBUG)
    work = root / 'manager'; work.mkdir()
    capacities = tuple(WorkerCapacity(name, 13, 2, 13) for name in ('a', 'b', 'c'))
    config = ManagerConfig(ManagerSettings(str(root/'state.sqlite'), .1), Storage(str(work), mode='staged'), capacities,
                           Resources(1, 1, .001), Scheduler(account_external_load=False, reserve_memory_gib=0))
    tokens = {}
    for capacity in capacities:
        token = root/(capacity.id+'.token'); token.write_text(capacity.id*40); token.chmod(0o600); tokens[capacity.id] = str(token)
    model = dict(particles=16384, density=.01,
                 initial_positions=dict(mode='explicit', coordinates=[[(i%128)*10., (i//128)*10.] for i in range(16384)]),
                 initial_orientations=dict(mode='explicit', angles=[0.]*16384))
    if args.snapshot_backup:
        with sqlite3.connect(f'file:{args.snapshot_backup.resolve()}?mode=ro', uri=True) as db:
            for row in db.execute('SELECT snapshot FROM simulation_runs ORDER BY length(snapshot) DESC LIMIT 1'):
                model = json.loads(row[0])['model_parameters']
    launcher = root/'mpi'; launcher.write_text('#!'+sys.executable+'\nimport os,sys\nos.execv(sys.argv[3],sys.argv[3:])\n'); launcher.chmod(0o700)
    engine = root/'lmp'; engine.write_text(r'''#!/usr/bin/env python3
from pathlib import Path
import re,json,time
text=Path('in.lmp').read_text(); model=json.loads(Path('model-parameters.json').read_text())
steps,total=map(int,re.findall(r'run (\d+) start 0 stop (\d+)',text)[-1])
start=0 if 'include initial-state.lmp' in text else int(Path('checkpoint.restart').read_text())
time.sleep(30)
stop=start+steps
print('1 by 1 by 1 MPI processor grid');print('using 1 OpenMP thread(s) per MPI task')
print('Step Time Atoms');print(start,start*model['dt'],model['particles']);print(stop,stop*model['dt'],model['particles']);print('Loop time')
Path('checkpoint.restart').write_text(str(stop))
with Path('invocations').open('a') as f: f.write('one launch\n')
'''); engine.chmod(0o700)
    service = ManagerService(config,tokens); server = make_server(service,'127.0.0.1',0)
    listener = threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01}); listener.start()
    workers=[]; samples=[]; control_samples=[]; peak=0; rounds=0; failures=[]; cancelled=[]; retryable=0
    started = time.monotonic()
    stop=time.monotonic()+args.seconds
    class TimedResponse:
        def __init__(self, response, began):
            self.response, self.began = response, began
        def __getattr__(self, name):
            return getattr(self.response, name)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            control_samples.append(time.monotonic() - self.began)
            self.response.close()
    def cycle(worker):
        started=time.monotonic()
        reply = worker.tick()
        nonlocal retryable
        if 'retry_after' in reply:
            retryable += 1
        samples.append(time.monotonic()-started)
    try:
        for capacity in capacities:
            (root/('worker-'+capacity.id)).mkdir()
            worker=Worker(WorkerConfig(capacity,f'http://127.0.0.1:{server.server_port}',str(root/('worker-'+capacity.id)),
                                      Execution(str(engine),str(launcher)),tokens[capacity.id],storage_mode='staged'), root/('journal-'+capacity.id))
            original=worker.monitor.sample
            def sample(config, original=original):
                sample = original(config)
                return {**sample,'detected_cpu_slots':13,'per_cpu_utilization_percent':dict(list(sample['per_cpu_utilization_percent'].items())[:13]),'detected_memory_gib':32,'available_memory_gib':32}
            worker.monitor.sample=sample
            opener = worker.opener.open
            def timed_open(request, *args, opener=opener, **kwargs):
                began = time.monotonic()
                try:
                    response = opener(request, *args, **kwargs)
                except (HTTPError, URLError):
                    if request.full_url.endswith('/v1/poll'):
                        control_samples.append(time.monotonic() - began)
                    raise
                return TimedResponse(response, began) if request.full_url.endswith('/v1/poll') else response
            worker.opener.open = timed_open
            workers.append(worker)
        with patch('lammps_manager.continuation.fingerprint_build',return_value='f'*64), ThreadPoolExecutor(max_workers=3) as pool:
            while time.monotonic()<stop:
                with Store(config.manager.database_path) as store:
                    active=store._db.execute("SELECT count(*) FROM simulation_runs WHERE status NOT IN ('succeeded','cancelled')").fetchone()[0]
                    if not active:
                        for i in range(58):
                            submit_run(store,config,total_steps=30,segment_ends=(10,20,30),run_id=f'round{rounds}-run{i}',
                                       recipe=Recipe('wca-abp-2d-v1','statistical'), model_parameters=model)
                        if rounds == 0:
                            started = time.monotonic()
                            stop = started + args.seconds
                        rounds+=1
                for future in [pool.submit(cycle,w) for w in workers]:
                    try: future.result()
                    except Exception as exc: failures.append(type(exc).__name__+': '+str(exc))
                with Store(config.manager.database_path) as store:
                    count=len(store.reservations()); peak=max(peak,count)
                    if count==39 and not cancelled:
                        identity=store._db.execute("SELECT run_id FROM simulation_runs WHERE status='running' LIMIT 1").fetchone()[0]
                        cancel_run(store,identity); cancelled.append(identity)
                time.sleep(.02)
            # All remaining work belongs to this isolated test.
            with Store(config.manager.database_path) as store:
                for row in store._db.execute("SELECT run_id FROM simulation_runs WHERE status NOT IN ('succeeded','cancelled')").fetchall():
                    cancel_run(store,row[0])
            deadline=time.monotonic()+180
            while time.monotonic()<deadline:
                for future in [pool.submit(cycle,w) for w in workers]: future.result()
                with Store(config.manager.database_path) as store:
                    pending=store._db.execute("SELECT count(*) FROM attempts a LEFT JOIN result_collections c USING(attempt_id) WHERE a.state IN ('succeeded','failed','cancelled') AND coalesce(c.status,'')!='collected'").fetchone()[0]
                    unack=store._db.execute("SELECT count(*) FROM checkpoints c JOIN attempts a USING(attempt_id) WHERE a.state='succeeded' AND c.acknowledged=0").fetchone()[0]
                    if not store.reservations() and not pending and not unack: break
                time.sleep(.05)
    finally:
        for worker in workers: worker.close()
        server.shutdown(); listener.join(); server.server_close(); service.close()
        ordered=sorted(control_samples)
        elapsed = time.monotonic() - started
        with Store(config.manager.database_path) as store:
            pending=store._db.execute("SELECT count(*) FROM attempts a LEFT JOIN result_collections c USING(attempt_id) WHERE a.state IN ('succeeded','failed','cancelled') AND coalesce(c.status,'')!='collected'").fetchone()[0]
            unack=store._db.execute("SELECT count(*) FROM checkpoints c JOIN attempts a USING(attempt_id) WHERE a.state='succeeded' AND c.acknowledged=0").fetchone()[0]
            report=dict(rounds=rounds,peak_reservations=peak,samples=len(samples),failures=failures,
                        target_seconds=args.seconds,elapsed_seconds=elapsed,retryable_responses=retryable,remaining_reservations=len(store.reservations()),
                        p50=ordered[len(ordered)//2] if ordered else None,p95=ordered[int(len(ordered)*.95)] if ordered else None,
                        maximum=max(ordered) if ordered else None,cancelled=cancelled,tick_maximum=max(samples) if samples else None,
                        heartbeat_expiries=store._db.execute("SELECT count(*) FROM events WHERE reason='Worker heartbeat expired'").fetchone()[0],
                        completed_segments=store._db.execute('SELECT count(*) FROM run_segments WHERE completed=1').fetchone()[0])
        report['latency_scope'] = 'all control responses including retryable errors'
        report['pending_results'] = pending
        report['unacknowledged_checkpoints'] = unack
        report['passed'] = bool(elapsed >= args.seconds and peak == 39 and cancelled and not failures
                                and report['remaining_reservations'] == 0 and not pending and not unack and report['heartbeat_expiries'] == 0
                                and report['completed_segments'] >= 2 and report['p95'] is not None
                                and report['p95'] <= 1 and report['maximum'] <= 5)
        (root/'report.json').write_text(json.dumps(report,indent=2)); print(json.dumps(report))
    return 0 if report['passed'] else 1

if __name__=='__main__': raise SystemExit(main())
