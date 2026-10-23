"""Run a finite acceptance plan inside an expiring systemd control-group service.

The unit must set RuntimeMaxSec, KillMode=control-group, TimeoutStopSec,
SendSIGKILL=yes, Restart=no, AllowedCPUs and CPUQuota. Use this same script as
ExecStopPost with --finalize to publish status only after service cleanup.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def save(path: Path, value: dict) -> None:
    """Publish a complete status atomically."""
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(path)


def validate(plan: dict) -> None:
    """Reject unbounded or malformed executable steps before starting anything."""
    if not isinstance(plan.get('steps'), list) or not plan['steps']:
        raise ValueError('A nonempty finite steps list is required')
    for step in plan['steps']:
        if (not isinstance(step.get('argv'), list) or not step['argv']
                or not all(isinstance(arg, str) and '\0' not in arg for arg in step['argv'])
                or type(step.get('timeout_seconds')) is not int or step['timeout_seconds'] <= 0):
            raise ValueError('Each step needs an argv array and positive integer timeout')


def run(root: Path) -> int:
    """Run declared steps once, retain logs, and fail closed on any error."""
    plan = json.loads((root / 'plan.json').read_text()); validate(plan)
    record = dict(started_at=time.time(), steps=[], completed=False)
    save(root / 'progress.json', record)
    def interrupted(signum: int, frame: object) -> None:
        raise InterruptedError(f'Service received signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        for index, step in enumerate(plan['steps']):
            entry = dict(argv=step['argv'], started_at=time.time())
            record['steps'].append(entry); save(root / 'progress.json', record)
            with (root / f'step-{index}.log').open('xb') as log:
                process = subprocess.Popen(step['argv'], cwd=plan['cwd'],
                                           stdin=subprocess.DEVNULL, stdout=log,
                                           stderr=subprocess.STDOUT, start_new_session=True,
                                           env={**os.environ, **plan.get('environment', {})})
                entry['pid'] = process.pid; save(root / 'progress.json', record)
                try:
                    entry['returncode'] = process.wait(timeout=step['timeout_seconds'])
                except BaseException:
                    # Detached descendants are additionally covered by the unit cgroup.
                    try:
                        os.killpg(process.pid, signal.SIGINT)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=5)
                    raise
            entry['ended_at'] = time.time(); save(root / 'progress.json', record)
            if entry['returncode']:
                raise RuntimeError(f'Step {index} exited {entry["returncode"]}')
        record['completed'] = True
        return 0
    except BaseException as exc:
        record['error'] = f'{type(exc).__name__}: {exc}'
        return 1
    finally:
        record['ended_at'] = time.time(); save(root / 'progress.json', record)


def finalize(root: Path) -> None:
    """Record host-confirmed cgroup cleanup after success, failure or hard timeout."""
    group = next(line.split(':', 2)[2] for line in Path('/proc/self/cgroup').read_text().splitlines()
                 if line.startswith('0::'))
    directory = Path('/sys/fs/cgroup') / group.lstrip('/')
    if not directory.name.endswith('.service'):
        raise ValueError('Refuse cleanup outside a dedicated service cgroup')
    def members() -> list[int]:
        return sorted({int(pid) for path in directory.rglob('cgroup.procs')
                       for pid in path.read_text().split() if int(pid) != os.getpid()})
    # systemd may enter ExecStopPost immediately after the main process exits,
    # before a TERM-ignoring descendant has disappeared. Finish owned cleanup
    # here and verify it before publishing the final marker.
    killed = []
    for pid in members():
        try:
            os.kill(pid, signal.SIGKILL); killed.append(pid)
        except ProcessLookupError:
            pass
    end = time.monotonic() + 5
    remaining = members()
    while remaining and time.monotonic() < end:
        time.sleep(.05); remaining = members()
    progress = json.loads((root / 'progress.json').read_text()) if (root / 'progress.json').exists() else None
    save(root / 'status.json', dict(finished_at=time.time(), service_result=os.environ.get('SERVICE_RESULT'),
                                   exit_code=os.environ.get('EXIT_CODE'), exit_status=os.environ.get('EXIT_STATUS'),
                                   remaining_cgroup_pids=remaining, finalizer_killed_pids=killed, stop_verified=not remaining,
                                   completed=bool(progress and progress.get('completed')),
                                   progress=progress, boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip()))
    if remaining:
        raise RuntimeError('Owned cgroup still has processes after cleanup')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--finalize', action='store_true')
    args = parser.parse_args()
    if args.finalize:
        finalize(args.root)
    else:
        sys.exit(run(args.root))


if __name__ == '__main__':
    main()
