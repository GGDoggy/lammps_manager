"""Conservative Linux process identity, termination and journal reconciliation."""

from datetime import datetime, timezone
import errno
import json
import os
from pathlib import Path
import signal
import time
from uuid import UUID

from .store import Store, Attempt, TERMINAL


def process_identity(pid: int) -> dict:
    """Record boot, PID, start ticks and session/process group together."""
    stat = Path(f'/proc/{pid}/stat')
    try:
        fields = stat.read_text().rpartition(')')[2].split()
    except ProcessLookupError as exc:
        # procfs can return ESRCH if the process exits after the file opens.
        # Callers use the missing-file case to continue whole-session checks.
        raise FileNotFoundError(errno.ENOENT, 'Process exited during identity read', str(stat)) from exc
    return {'pid': pid, 'process_group': int(fields[2]), 'session': int(fields[3]),
            'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'start_ticks': fields[19]}


def session_members(session: int) -> tuple[tuple[int, int], ...]:
    """List live PID/group pairs; zombies have terminated and consume no CPU."""
    members = []
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            fields = (path / 'stat').read_text().rpartition(')')[2].split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        if int(fields[3]) == session and fields[0] not in {'Z', 'X'}:
            members.append((int(path.name), int(fields[2])))
    return tuple(members)


def group_alive(group: int) -> bool:
    """Check the entire launch session, including MPI-created rank groups."""
    # A permission error cannot establish termination. Also inspect the session
    # when the original group has disappeared but MPI rank groups remain.
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        pass
    return bool(session_members(group))


def signal_session(session: int, sig: int) -> None:
    """Signal all live groups in a validated dedicated launch session."""
    for group in {g for _, g in session_members(session)}:
        try:
            os.killpg(group, sig)
        except ProcessLookupError:
            pass


def terminate_group(group: int, *, grace: float = 2.0) -> bool:
    """Signal all MPI groups in the launch session and escalate after grace."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        signal_session(group, sig)
        deadline = time.monotonic() + grace
        while time.monotonic() < deadline:
            if not group_alive(group):
                return True
            time.sleep(.05)
    return not group_alive(group)


def reconcile(store: Store, attempt: Attempt) -> bool:
    """Never relaunch accepted work; absence of exit status never implies success.

    Missing/corrupt identity or a reused live PID/group retains unknown. A boot
    change proves old processes ended. Same-boot cleanup requires original PID
    identity before signaling and group disappearance before resource release.
    Return True only when a positively identified live process needs monitoring.
    """
    if attempt.state in TERMINAL:
        return False
    if store.prelaunch_accepted(attempt.attempt_id) and not store.launch_claimed(attempt.attempt_id):
        store.reject_launch(attempt.attempt_id, 'not_ready', 'Worker restarted before launch claim', time.time() + 30)
        return False
    if (store.specification(attempt.job_id).storage_mode == 'staged'
            and not store.launch_claimed(attempt.attempt_id)):
        return False  # Input preparation has not consumed a launch right.
    try:
        saved = json.loads((Path(attempt.directory) / 'process.json').read_text())
        pid = saved['pid']
        group = saved['process_group']
        if not isinstance(saved['boot_id'], str):
            raise ValueError('Invalid boot identity')
        UUID(saved['boot_id'])
        if (type(pid) is not int or pid <= 1 or type(group) is not int or group != pid
                or not isinstance(saved['start_ticks'], str) or not saved['start_ticks'].isdigit()
                or saved.get('session', pid) != pid):
            raise ValueError('Invalid saved process identity')
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        ended = saved['boot_id'] != boot
        live = False
        if not ended:
            try:
                current = process_identity(pid)
            except FileNotFoundError:
                current = None
            if current is not None and (current['boot_id'], current['start_ticks'],
                    current['process_group'], current['session']) == (saved['boot_id'], saved['start_ticks'], group, group):
                live = True
            ended = not group_alive(group)
        cancel = store.cancellation(attempt.attempt_id)
        if live and not ended:
            limit = store.specification(attempt.job_id).resources.time_limit_seconds
            # The timestamp is persisted immediately before launch; legacy records
            # can fall back to the durable running transition.
            started = saved.get('launched_at') or attempt.started_at
            if started:
                elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(
                    started.replace('Z', '+00:00'))).total_seconds()
                if elapsed >= limit and not cancel:
                    store.request_cancel(attempt.attempt_id, 'Time limit exceeded')
                    cancel = store.cancellation(attempt.attempt_id)
            if cancel:
                # Recheck the leader immediately before sending a group signal.
                if process_identity(pid)['start_ticks'] == saved['start_ticks']:
                    ended = terminate_group(group)
            elif attempt.state != 'running':
                store.transition(attempt.attempt_id, 'running')
        if ended:
            with store.scheduling_round():
                store.transition(attempt.attempt_id, 'cancelled' if cancel else 'failed',
                    reason=cancel or 'Process ended during worker outage; exit status unavailable',
                    termination_evidence='Linux boot changed' if saved['boot_id'] != boot
                        else 'Recovered identity checked; process group no longer exists')
                if not cancel:
                    store.record_interruption(attempt.attempt_id, {
                        'kind': 'boot_changed' if saved['boot_id'] != boot else 'session_ended',
                        'process': saved, 'observed_boot_id': boot})
            return False
        if live:
            return True
        reason = 'Original leader absent or reused while process group remains; manual resolution required'
    except (OSError, ValueError, KeyError, TypeError) as exc:
        reason = f'Cannot establish process identity: {type(exc).__name__}'
    if store.attempt(attempt.attempt_id).state != 'unknown':
        store.transition(attempt.attempt_id, 'unknown', reason=reason)
    return False
