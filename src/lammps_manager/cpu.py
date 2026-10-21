"""Per-logical-CPU admission shared by scheduling and local launch checks."""

from datetime import datetime, timezone
import math

from .store import Store


def validate_sample(values: dict[str, float] | None, start: float | None,
                    end: float, capacity: int) -> None:
    """Reject malformed samples; None denotes an older or unavailable sample."""
    if start is not None and (type(start) not in (int, float)
                             or not math.isfinite(start) or not 0 <= start <= end):
        raise ValueError('Invalid CPU sample start')
    if values is None:
        return
    if not isinstance(values, dict) or len(values) > capacity:
        raise ValueError('Invalid per-CPU scope')
    for key, value in values.items():
        if (not isinstance(key, str) or not key.isascii() or not key.isdigit()
                or str(int(key)) != key or type(value) not in (int, float)
                or not math.isfinite(value) or not 0 <= value <= 100):
            raise ValueError('Invalid per-CPU utilization')


def admission(values: dict[str, float] | None, start: float | None, *,
              threshold: float, pending: int, requested: int, used: int,
              minimum: int, reserve: int, capacity: int, enabled: bool) -> dict:
    """Return observable gate evidence without altering reservation accounting."""
    valid = values is not None and bool(values) and start is not None
    qualifying = sum(v < threshold for v in (values or {}).values())
    floor = used + requested <= minimum
    reason = ('minimum_exceeds_capacity' if minimum > capacity else
              'load_accounting_disabled' if not enabled else
              'missing_per_cpu_metrics' if not valid else
              'minimum_floor' if floor else
              'per_cpu_headroom' if max(0, qualifying - pending) >= requested + reserve else
              'per_cpu_load')
    return dict(scope=sorted(values or {}, key=int), qualifying=qualifying,
                pending=pending, requested=requested, reserve=reserve, minimum=minimum,
                minimum_exception=enabled and valid and floor and minimum <= capacity,
                reason=reason, allowed=reason in {
                    'load_accounting_disabled', 'minimum_floor', 'per_cpu_headroom'})


def pending_slots(store: Store, worker_id: str, sample_start: float | None, *,
                  candidate_id: str | None = None,
                  skip_unclaimed_staged: bool = False) -> int:
    """Count unreflected starts, retaining spent sample budget after completion.

    The caller serializes admissions. SQLite timestamps have millisecond
    precision; round the boundary conservatively so a same-millisecond admission
    cannot look older than the sample. A complete later interval retires charges.
    """
    if sample_start is None:
        return 0
    # Coarse SQL filtering avoids loading obsolete history or unrelated workers.
    # Round down before formatting, then retain the exact comparison below.
    boundary = math.floor((sample_start - .001) * 1000) / 1000
    since = datetime.fromtimestamp(boundary, timezone.utc).isoformat(
        timespec='milliseconds').replace('+00:00', 'Z')
    pending = 0
    for attempt, resources in store.cpu_accounting_attempts(
            worker_id, since, candidate_id=candidate_id,
            skip_unclaimed_staged=skip_unclaimed_staged):
        created = datetime.fromisoformat(attempt.created_at.replace('Z', '+00:00')).timestamp()
        started = (None if attempt.started_at is None else datetime.fromisoformat(
            attempt.started_at.replace('Z', '+00:00')).timestamp())
        if (created + .001 >= sample_start
                or (started is not None and started + .001 >= sample_start)
                or (attempt.ended_at is None and started is None)):
            pending += resources.cpu_slots
    return pending
