"""Transactional FIFO admission using explicit worker monitoring snapshots."""

from dataclasses import asdict, dataclass
from datetime import datetime
import json
import math
from pathlib import Path
from uuid import uuid4

from .config import ManagerConfig, WorkerCapacity, _parse
from .store import Attempt, Store


@dataclass(frozen=True)
class WorkerSnapshot:
    """Trusted caller-supplied limits and metrics; timestamps are Unix seconds."""

    capacity: WorkerCapacity
    detected_cpu_slots: int
    detected_memory_gib: float
    observed_at: float
    heartbeat_at: float
    cpu_utilization_percent: float
    available_memory_gib: float
    online: bool = True
    paused: bool = False

    def __post_init__(self) -> None:
        # Direct Python callers receive the same capacity checks as YAML callers.
        _parse(WorkerCapacity, {**asdict(self.capacity),
                               'labels': list(self.capacity.labels)}, 'snapshot.capacity')
        if type(self.detected_cpu_slots) is not int or self.detected_cpu_slots <= 0:
            raise ValueError('detected_cpu_slots must be a positive integer')
        for name in ('detected_memory_gib', 'observed_at', 'heartbeat_at',
                     'cpu_utilization_percent', 'available_memory_gib'):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError(f'{name} must be finite and nonnegative')
        if self.detected_memory_gib == 0 or self.cpu_utilization_percent > 100:
            raise ValueError('Invalid detected memory or CPU utilization')
        if type(self.online) is not bool or type(self.paused) is not bool:
            raise ValueError('online and paused must be booleans')


@dataclass(frozen=True)
class ScheduleResult:
    assignments: tuple[Attempt, ...]
    waiting: dict[str, str]


def schedule(store: Store, config: ManagerConfig, snapshots: tuple[WorkerSnapshot, ...],
             *, now: float) -> ScheduleResult:
    """Reserve a round atomically; never launch processes or release reservations.

    FIFO may skip blocked jobs. Rank by CPU usage, remaining CPU (descending),
    remaining memory (descending), then worker ID. Missing snapshots fail closed.
    """
    if type(now) not in (int, float) or not math.isfinite(now) or now < 0:
        raise ValueError('now must be finite nonnegative Unix seconds')
    by_id = {s.capacity.id: s for s in snapshots}
    if len(by_id) != len(snapshots):
        raise ValueError('Duplicate worker snapshots')
    registered = {w.id for w in config.workers}
    if by_id.keys() - registered:
        raise ValueError('Snapshots contain unregistered workers')
    assigned = []
    waiting = {}
    policy = config.scheduler
    if policy.policy != 'fifo_least_loaded':
        raise ValueError('Unsupported scheduling policy')
    with store.scheduling_round():
        for job in store.jobs():
            if job['state'] != 'queued':
                continue
            spec = store.specification(job['job_id'])
            if spec.storage_mode != config.storage.mode:
                reason = 'storage_mode_mismatch: existing specification requires ' + spec.storage_mode
                store.record_waiting(spec.job_id, reason)
                waiting[spec.job_id] = reason
                continue
            r = spec.resources
            candidates = []
            reasons = {}
            fits_any = False
            for worker in config.workers:
                snapshot = by_id.get(worker.id)
                cpu = worker.cpu_slots
                memory = worker.memory_budget_gib
                count = worker.max_concurrent_jobs
                if snapshot is not None:
                    cpu = min(cpu, snapshot.capacity.cpu_slots, snapshot.detected_cpu_slots)
                    memory = min(memory, snapshot.capacity.memory_budget_gib,
                                 snapshot.detected_memory_gib)
                    count = min(count, snapshot.capacity.max_concurrent_jobs)
                fits = r.cpu_slots <= cpu and r.memory_gib <= memory
                fits_any |= fits
                reason = None
                if not fits:
                    reason = 'exceeds_capacity'
                elif not worker.enabled or (snapshot and not snapshot.capacity.enabled):
                    reason = 'disabled'
                elif snapshot is None:
                    reason = 'missing_metrics'
                elif not snapshot.online:
                    reason = 'offline'
                elif snapshot.paused:
                    reason = 'paused'
                elif snapshot.observed_at > now or snapshot.heartbeat_at > now:
                    reason = 'future_timestamp'
                elif now - snapshot.heartbeat_at > config.manager.heartbeat_timeout_seconds:
                    reason = 'heartbeat_timeout'
                elif now - snapshot.observed_at > policy.metrics_max_age_seconds:
                    reason = 'stale_metrics'
                else:
                    reservations = store.reservations(worker.id)
                    used_cpu = sum(x.cpu_slots for x in reservations)
                    used_memory = sum(x.memory_gib for x in reservations)
                    # A start only removes the pending deduction once a later
                    # sample can reflect it, including unknown/cancelling work.
                    pending_memory = 0.0
                    for reservation in reservations:
                        started_at = store.attempt(reservation.attempt_id).started_at
                        if (started_at is None or datetime.fromisoformat(
                                started_at.replace('Z', '+00:00')).timestamp()
                                >= snapshot.observed_at):
                            pending_memory += reservation.memory_gib
                    if used_cpu + r.cpu_slots > cpu:
                        reason = 'cpu_quota'
                    elif used_memory + r.memory_gib > memory:
                        reason = 'memory_quota'
                    elif len(reservations) >= count:
                        reason = 'concurrency_limit'
                    elif snapshot.available_memory_gib - pending_memory < (
                            r.memory_gib + policy.reserve_memory_gib):
                        reason = 'available_memory'
                    elif (policy.account_external_load and snapshot.cpu_utilization_percent
                          >= policy.max_cpu_utilization_percent):
                        reason = 'external_cpu_load'
                    else:
                        candidates.append((snapshot.cpu_utilization_percent, -(cpu - used_cpu),
                                           -(memory - used_memory), worker.id))
                if reason:
                    reasons[worker.id] = reason
            if candidates:
                worker_id = min(candidates)[-1]
                attempt_id = uuid4().hex
                assigned.append(store.assign(spec.job_id, attempt_id, worker_id,
                                              str(Path(spec.work_directory) / attempt_id)))
            else:
                reason = json.dumps({'status': 'waiting' if fits_any else 'unschedulable',
                                     'workers': reasons}, sort_keys=True)
                store.record_waiting(spec.job_id, reason)
                waiting[spec.job_id] = reason
    return ScheduleResult(tuple(assigned), waiting)
