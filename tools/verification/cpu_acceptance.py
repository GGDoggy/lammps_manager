"""Real procfs sampling with bounded, affinity-pinned external load."""
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from dataclasses import asdict, replace

from real_acceptance import ROOT, PYTHON, ENV, setup, submit, audit, inspect
from lammps_manager.config import load_config
from lammps_manager.local import execute_attempt
from lammps_manager.scheduler import WorkerSnapshot, schedule
from lammps_manager.store import Store
from lammps_manager.worker import LinuxMonitor

DECISIONS = []


def decide(root, config, snapshot, label):
    with Store(root / 'manager.sqlite') as store:
        result = schedule(store, config, (snapshot,), now=time.time())
        reservations = [asdict(r) for r in store.reservations()]
    value = dict(label=label, time=time.time(), snapshot=asdict(snapshot),
                 config=asdict(config), result=asdict(result), reservations=reservations)
    DECISIONS.append(value)
    (ROOT / 'v004-partial.json').write_text(json.dumps(DECISIONS, indent=2))
    return result


def take(monitor, worker):
    time.sleep(.5)
    return WorkerSnapshot(**{**monitor.sample(worker), "capacity": worker.worker})


def execute(root, worker, attempt):
    with Store(root / 'manager.sqlite') as store:
        execute_attempt(store, worker, store.attempt(attempt.attempt_id))


def main():
    cpus = sorted(os.sched_getaffinity(0))[:4]
    assert len(cpus) == 4
    os.sched_setaffinity(0, cpus)
    root = setup('v004', external=True)
    config = load_config(root / 'manager.yaml', 'manager')
    config = replace(config, scheduler=replace(config.scheduler, reserve_slots=1,
                                               metrics_max_age_seconds=1))
    worker = load_config(root / 'worker.yaml', 'worker')
    monitor = LinuxMonitor()
    jobs = submit(root, [(100000000, 15, False)] * 3)
    load = []
    thread = None
    try:
        for cpu in cpus[:3]:
            code = 'import time\nend=time.monotonic()+20\nwhile time.monotonic()<end: pass\n'
            load.append(subprocess.Popen(['taskset', '-c', str(cpu), PYTHON, '-c', code]))
        take(monitor, worker)
        busy = take(monitor, worker)
        qualifying = sum(v < 80 for v in busy.per_cpu_utilization_percent.values())
        assert qualifying < 3, busy
        result = decide(root, config, busy, 'busy-floor-disabled')
        assert not result.assignments
        floor = replace(config, scheduler=replace(config.scheduler, min_slots=2))
        result = decide(root, floor, busy, 'busy-minimum-two')
        assert len(result.assignments) == 1
        first = result.assignments[0]
        thread = threading.Thread(target=execute, args=(root, worker, first))
        thread.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with Store(root / 'manager.sqlite') as store:
                if store.attempt(first.attempt_id).state == 'running':
                    break
            time.sleep(.03)
        else:
            raise AssertionError('Floor execution did not start')
        time.sleep(1.1)
        higher = replace(config, scheduler=replace(config.scheduler, min_slots=4))
        stale = decide(root, higher, busy, 'stale-despite-floor-four')
        assert not stale.assignments
        assert all('stale_metrics' in reason for reason in stale.waiting.values())
        with Store(root / 'manager.sqlite') as store:
            assert store.attempt(first.attempt_id).state == 'running'
            assert len(store.reservations()) == 1
        fresh = take(monitor, worker)
        limited = replace(higher, workers=(replace(higher.workers[0], memory_budget_gib=.15),))
        result = decide(root, limited, fresh, 'floor-memory-quota')
        assert not result.assignments and all('memory_quota' in r for r in result.waiting.values())
        limited = replace(higher, workers=(replace(higher.workers[0], max_concurrent_jobs=1),))
        result = decide(root, limited, fresh, 'floor-concurrency-quota')
        assert not result.assignments and all('concurrency_limit' in r for r in result.waiting.values())
        result = decide(root, higher, fresh, 'fresh-floor-second-then-cpu-quota')
        assert len(result.assignments) == 1
        assert all('cpu_quota' in r for r in result.waiting.values())
        second = result.assignments[0]
        with Store(root / 'manager.sqlite') as store:
            store.request_cancel(first.attempt_id, 'V004 bounded real load scenario complete')
            store.request_cancel(second.attempt_id, 'V004 unstarted assignment cleanup')
        thread.join(10)
        assert not thread.is_alive()
        execute(root, worker, second)
    finally:
        for p in load:
            if p.poll() is None:
                p.terminate()
            p.wait(timeout=5)
        if thread and thread.is_alive():
            thread.join(20)
    # Restore headroom using actual complete idle sampling intervals.
    take(monitor, worker)
    idle = take(monitor, worker)
    result = decide(root, config, idle, 'load-removed-headroom')
    assert len(result.assignments) == 1, result
    third = result.assignments[0]
    # This long case is bounded by cancellation; short real completion below.
    with Store(root / 'manager.sqlite') as store:
        store.request_cancel(third.attempt_id, 'V004 headroom admitted; no further long execution needed')
    execute(root, worker, third)
    shortroot = setup('v004-short', external=True)
    shortconfig = load_config(shortroot / 'manager.yaml', 'manager')
    shortconfig = replace(shortconfig, scheduler=replace(shortconfig.scheduler, reserve_slots=1,
                                                       metrics_max_age_seconds=10))
    shortworker = load_config(shortroot / 'worker.yaml', 'worker')
    shortmonitor = LinuxMonitor()
    shortjobs = submit(shortroot, [(1000, 15, False)] * 2)
    snap = take(shortmonitor, shortworker)
    result = decide(shortroot, shortconfig, snap, 'short-first-admitted')
    assert len(result.assignments) == 1
    firstshort = result.assignments[0]
    execute(shortroot, shortworker, firstshort)
    with Store(shortroot / 'manager.sqlite') as store:
        assert store.attempt(firstshort.attempt_id).state == 'succeeded'
    for i in range(2):
        result = decide(shortroot, shortconfig, snap, 'completed-short-same-sample-' + str(i))
        assert not result.assignments
        assert any(d['pending'] == 2 for perjob in result.cpu_diagnostics.values() for d in perjob.values())
    take(shortmonitor, shortworker)
    fresh = take(shortmonitor, shortworker)
    result = decide(shortroot, shortconfig, fresh, 'complete-post-start-sample')
    assert len(result.assignments) == 1
    execute(shortroot, shortworker, result.assignments[0])
    result = dict(affinity=cpus, decisions=DECISIONS, load_pids=[p.pid for p in load],
                  load_returncodes=[p.returncode for p in load],
                  records=inspect(root, jobs) + inspect(shortroot, shortjobs),
                  audits=[audit(root), audit(shortroot)])
    (ROOT / 'v004.json').write_text(json.dumps(result, indent=2))
    print('V004 passed', flush=True)


if __name__ == '__main__':
    main()
