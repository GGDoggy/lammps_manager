# Scheduling and resource admission

T004 provides a Python API for deterministic scheduling using supplied monitoring
snapshots. It does not collect metrics, run a background manager, contact workers,
or launch processes. T005 supplies [communication and monitoring](workers.md). There are no
new YAML keys or CLI scheduling commands. The existing explicit `run-local` command
and low-level `Store.assign()` bypass scheduler admission; use this API for quota
controlled assignments. Reservations are accounting, not OS resource enforcement.

```python
import time
from lammps_manager.config import load_config
from lammps_manager.scheduler import WorkerSnapshot, schedule
from lammps_manager.store import Store

config = load_config('/etc/lammps-manager/manager.yaml', 'manager')
# Supply actual worker-config limits and detected hardware values in production.
now = time.time()
snapshot = WorkerSnapshot(
    capacity=config.workers[0], detected_cpu_slots=16, detected_memory_gib=64,
    observed_at=now, heartbeat_at=now, cpu_utilization_percent=20,
    available_memory_gib=40, online=True, paused=False,
)
with Store(config.manager.database_path) as store:
    result = schedule(store, config, (snapshot,), now=now)
    # result.assignments contains persisted attempts; nothing has been launched.
    # result.waiting maps queued job IDs to JSON reason strings.
```

Use a validated manager configuration. Snapshots are trusted caller-supplied frozen
values, not authenticated worker reports. Numeric metrics must be finite and
nonnegative; CPU utilization is at most 100, detected capacities are positive,
and status flags are booleans. Timestamps use Unix seconds on the manager clock.
Duplicate or unregistered snapshot identities fail before any writes. Missing
snapshots block admission; labels currently have no matching semantics because job
specifications contain no worker compatibility constraints.

## Ordering and admission

Jobs are examined in stable submission order (SQLite insertion order). A blocked
job may be skipped to admit smaller jobs. Eligible workers are ranked by ascending
CPU utilization, descending remaining CPU quota, descending remaining memory quota,
and lexicographic worker ID. Remaining quotas are measured before the proposed
assignment. CPU utilization still determines ordering when the external-load gate
is disabled. Input snapshot order does not affect ties.

CPU and memory capacities are the minimum of manager configuration, worker
configuration in the snapshot, and detected hardware. Concurrency is the minimum
of manager and worker configuration. A missing snapshot uses manager capacity only
to explain whether the job exceeds known limits; it never permits assignment.

Every admission requires:

- Manager and worker enabled, online and unpaused status.
- Metric age at most `metrics_max_age_seconds` and heartbeat age at most
  `heartbeat_timeout_seconds`. Future timestamps block admission.
- All active CPU reservations plus MPI ranks × OpenMP threads fit CPU quota.
- All active memory reservations plus requested memory fit memory quota.
- Active reservation count plus one fits concurrency.
- Available memory minus reservations without a recorded start, or with a start
  at or after the metric sampling time, can cover the request plus
  `reserve_memory_gib`. Only starts strictly before `observed_at` avoid the pending
  deduction, so a still-valid pre-start snapshot cannot over-admit new work.
- When `account_external_load` is true, CPU utilization is strictly below
  `max_cpu_utilization_percent`. Disabling it preserves every other admission gate.

Unknown and cancelling attempts keep all quota reservations. Those without a
recorded start, or whose start is at or after the sample, also retain the
pending-memory deduction. No snapshot marks jobs
complete, releases resources, or permits relaunch. The caller must accurately
report starts and collect fresh metrics; snapshots cannot prove process termination.

One `BEGIN IMMEDIATE` transaction covers the entire round, including reads,
assignments, reservations and waiting events. Separate Store connections serialize
rounds and observe prior reservations before admission. Any exception rolls back
the complete round; assignment directories are only strings and no output is
created or deleted. Each new assignment receives a UUID attempt ID and a directory
under the submitted work directory. Repeated rounds skip previously assigned jobs.

## Waiting reasons

Blocked jobs retain state `queued`. `result.waiting` and the latest queued event
contain a JSON string with `status` and per-worker reasons. Use the existing
`inspect --config ... JOB_ID` command to read persisted event history after reopen.
Identical consecutive reasons do not append duplicate events. New assignments add
an assigned event, superseding earlier waiting reasons without deleting history.

`status=unschedulable` means the request exceeds every known effective capacity;
it remains queued and is reconsidered each round. Limits and snapshots may change,
so this is a diagnosis for that round, not a permanent rejection. Other blocks use
`status=waiting`. Worker reason precedence is capacity, disabled, missing metrics,
offline, paused, future timestamp, heartbeat timeout, stale metrics, CPU quota,
memory quota, concurrency, available memory, then external CPU load. Only the first
reason per worker is reported. Codes are `exceeds_capacity`, `disabled`,
`missing_metrics`, `offline`, `paused`, `future_timestamp`, `heartbeat_timeout`,
`stale_metrics`, `cpu_quota`, `memory_quota`, `concurrency_limit`,
`available_memory`, and `external_cpu_load`.

Local tests use fake metrics and SQLite connections. They do not establish remote
monitoring, real worker execution, MPI / LAMMPS behavior, or deployment readiness.
