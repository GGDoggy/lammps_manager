# Scheduling and resource admission

T004 provides a Python API for deterministic scheduling using supplied monitoring
snapshots. It does not collect metrics, run a background manager, contact workers,
or launch processes. T005 supplies [communication and monitoring](workers.md). T011 adds per-CPU admission settings; there are no CLI scheduling commands. The existing explicit `run-local` command
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
    cpu_sample_started_at=now - 1,
    per_cpu_utilization_percent={str(cpu): 20 for cpu in range(16)},
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

T015 examines jobs by priority descending, total requested CPU slots descending,
then queue-entry sequence ascending and job ID. Default priority is 0, configurable
as `scheduler.default_priority` (a signed 64-bit integer). A blocked
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
- When `account_external_load` is true, apply the [per-CPU load/reserve rule and
  minimum floor](configuration.md#per-cpu-admission-t011). Host average never vetoes
  admission. Missing per-CPU data blocks even the floor.

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
memory quota, concurrency, available memory, then per-CPU admission. Only the first
reason per worker is reported. Codes are `exceeds_capacity`, `disabled`,
`missing_metrics`, `offline`, `paused`, `future_timestamp`, `heartbeat_timeout`,
`stale_metrics`, `cpu_quota`, `memory_quota`, `concurrency_limit`,
`available_memory`, `per_cpu_load`, `missing_per_cpu_metrics`, and
`minimum_exceeds_capacity`.

Local tests use fake metrics and SQLite connections. They do not establish remote
monitoring, real worker execution, MPI / LAMMPS behavior, or deployment readiness.

## Sampling budget and diagnostics

`cpu_sample_started_at` and `observed_at` bound the CPU counter interval.
`per_cpu_utilization_percent` maps canonical decimal logical CPU IDs to finite
percentages. Only CPUs online at both endpoints and within affinity at both
endpoints supply valid deltas. Invalid or reset counters provide no idle evidence.
SMT siblings each count as one slot; these are not physical cores. Iowait counts
as busy. Missing samples and an empty valid scope cannot establish headroom or a floor.

An attempt consumes pending CPU headroom until its running transition precedes
the start of a complete later sampling window. This includes slow starts and
unknown/cancelling attempts without an earlier confirmed start. Existing running
reservations still consume quotas but are not subtracted from observed idle CPU
counts again. Admissions created or started within/after a sample window remain
charged against it even if they finish: reusing an unchanged sample must not
replenish its budget. These facts come from durable attempt history, inside the
same scheduling transaction, including across restart and separate connections.
Each round queries relevant attempt history once per eligible worker, filtering
by worker and sampling boundary before decoding immutable resource snapshots.
The transaction-local pending total is charged immediately for each new assignment
and discarded at the end of the round. Local launch checks use the same filtered
history query, with candidate and unclaimed staged exclusions applied in SQL.
SQLite timestamps have millisecond precision, so boundary comparisons retain a
conservative one-millisecond margin. This accounting can defer admission until a
new complete interval.

`ScheduleResult.cpu_diagnostics` maps job IDs and worker IDs to scope, qualifying
count, pending deduction, requested slots, reserve, minimum, floor exception,
reason and allowed flag. Queued waiting events also persist that evidence under
`cpu`, available through `inspect`. Earlier exclusion gates may omit CPU evidence.
The worker exposes its latest launch decision as `local_cpu_admission` in `tick()`
results and logs changed per-CPU decisions to stderr for CLI/service operators.
CPU headroom is admission evidence, not exclusive CPU ownership. This
policy does not bind MPI/OpenMP processes; configure and verify placement and
host clock synchronization separately. Unrelated load may change after sampling.

## Manager priority controls (T015)

The configuration policy name `fifo_least_loaded` remains accepted for compatibility;
it now applies the priority/demand/FIFO ordering described above.

Use `lammps-manager priority --config manager.yaml JOB_ID VALUE` or
`Store.set_priority(job_id, value, origin="operator")` for queued work. Overrides
persist separately from immutable job JSON, with an audit event and origin.
The manager default applies at dispatch when no override exists. Assignment
priority, origin and queue sequence are snapshotted atomically with reservations
and available through `Store.assignment_priority(attempt_id)`. Legacy assignments
without this metadata return `None`. Updates serialize with scheduling; an update
after assignment fails. Running work is never preempted.

At equal priority, 2 MPI ranks × 4 OpenMP threads precedes a 4-thread job; at
equal total demand the older queue entry wins. Retry, infrastructure recovery and
prelaunch return receive a fresh sequence; waiting diagnostics and priority edits
do not reset queue age. Priority never bypasses readiness, compatibility or quota
checks. No aging or automatic priority adjustment is applied.
