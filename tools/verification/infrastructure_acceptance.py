"""Real process-only infrastructure recovery; never reboots a host."""
import json, os, signal, time, subprocess
from datetime import datetime
from pathlib import Path
import yaml
from dataclasses import asdict
from real_acceptance import ROOT, setup, submit, cli, inspect, audit, COMMANDS
from distributed_acceptance import prepare, manager, worker, stop, wait, TIMELINE
from lammps_manager.store import Store
from lammps_manager.recovery import session_members, process_identity
BOOT = Path('/proc/sys/kernel/random/boot_id').read_text().strip()

def state(root: Path, job: str) -> dict:
    with Store(root / 'manager.sqlite') as s:
        return dict(status=s.recovery_status(job), attempts=[asdict(a) for a in s.job_attempts(job)], reservations=[asdict(r) for r in s.reservations()], events=s.events(job))

def interrupt(root: Path, w: subprocess.Popen, job: str) -> dict:
    with Store(root / 'journal/attempts.sqlite3') as s:
        a = s.job_attempts(job)[-1]
        identity = json.loads((Path(a.directory) / 'process.json').read_text())
    assert process_identity(identity['pid'])['start_ticks'] == identity['start_ticks']
    w.kill()
    w.wait(timeout=5)
    current = process_identity(identity['pid'])
    assert all(current[key] == identity[key] for key in current), 'Original MPI identity changed; do not signal'
    os.killpg(identity['pid'], signal.SIGKILL)
    deadline = time.monotonic() + 10
    while session_members(identity['pid']) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not session_members(identity['pid'])
    return identity

def run() -> None:
    os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[:4])
    cases = []
    for mode in ('shared', 'staged'):
        root = setup('recovery-v2-' + mode, mode=mode)
        conf = yaml.safe_load((root / 'manager.yaml').read_text())
        conf['infrastructure_recovery'] = dict(max_retries=1, delay_seconds=5)
        (root / 'manager.yaml').write_text(yaml.safe_dump(conf))
        prepare(root)
        m = manager(root)
        w = None
        try:
            w = worker(root)
            for scenario in ('success', 'exhaustion', 'cancel'):
                (job,) = submit(root, [(150000, 30, False)])
                wait(root, lambda v: any((a['job_id'] == job and a['state'] == 'running' for a in v['attempts'])), 'original-running', timeout=20)
                identity = interrupt(root, w, job)
                w = None
                held = state(root, job)
                assert held['reservations']
                w = worker(root)
                wait(root, lambda v: state(root, job)['status']['used'] == 1, 'recovery-scheduled', timeout=20)
                scheduled = state(root, job)
                deadline = scheduled['status']['next_retry_at']
                assert deadline is not None
                assert len(scheduled['attempts']) == 1
                old = m.pid
                stop(m)
                m = manager(root)
                assert m.pid != old
                replay = state(root, job)
                assert replay['status']['next_retry_at'] == deadline and replay['status']['used'] == 1
                if scenario == 'cancel':
                    cli(root / 'manager.yaml', 'cancel', job)
                    time.sleep(5.2)
                    final = state(root, job)
                    assert len(final['attempts']) == 1
                    with Store(root / 'manager.sqlite') as s:
                        assert s.job_state(job) == 'cancelled'
                else:
                    wait(root, lambda v: len(state(root, job)['attempts']) == 2 and state(root, job)['attempts'][-1]['state'] == 'running', 'successor-running', timeout=20)
                    successor = state(root, job)
                    assert datetime.fromisoformat(successor['attempts'][-1]['created_at'].replace('Z', '+00:00')).timestamp() >= deadline
                    if scenario == 'exhaustion':
                        second = interrupt(root, w, job)
                        w = None
                        w = worker(root)
                        wait(root, lambda v: 'exhausted' in str(state(root, job)['status']), 'budget-exhausted', timeout=20)
                    else:
                        wait(root, lambda v: any((j['job_id'] == job and j['state'] == 'succeeded' for j in v['jobs'])), 'recovered-success', timeout=40)
                    final = state(root, job)
                    assert len(final['attempts']) == 2 and final['status']['used'] == 1
                if mode == 'staged':

                    def delivered(v):
                        with Store(root / 'manager.sqlite') as s:
                            return all(((s.result_collection(a.attempt_id) or {}).get('status') == 'collected' for a in s.job_attempts(job)))
                    wait(root, delivered, 'all-attempt-results-retained', timeout=30)
                assert not state(root, job)['reservations']
                cases.append(dict(mode=mode, scenario=scenario, job=job, identity=identity, held=held, scheduled=scheduled, replay=replay, final=state(root, job), record=inspect(root, [job])))
                (ROOT / 'v009-recovery-partial.json').write_text(json.dumps(cases, indent=2))
            audit(root)
        finally:
            if w is not None:
                stop(w)
            stop(m)
    assert Path('/proc/sys/kernel/random/boot_id').read_text().strip() == BOOT
    (ROOT / 'v009-recovery.json').write_text(json.dumps(dict(boot_id=BOOT, cases=cases, commands=COMMANDS, timeline=TIMELINE), indent=2))
    print('six real process recovery cases passed')
if __name__ == '__main__':
    run()
