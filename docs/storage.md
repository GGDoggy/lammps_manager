# SQLite persistence API

T002 adds `lammps_manager.store.Store`, using Python's standard-library `sqlite3`.
The storage module does not execute jobs, select workers, enforce capacity,
or reconcile processes. T006 adds explicit retry queuing and durable cancellation intent; process checks belong to the executor. T003 adds a separate [local batch CLI and executor](local-batches.md).
Use a database on the manager's local disk, not a shared filesystem. Its parent
directory must already exist. T002 added no configuration fields or commands;
the existing manager `database_path` is also used by the local batch CLI.

```python
from lammps_manager.store import Store

# specification is an immutable JobSpecification from create_job_specification().
with Store('/var/lib/lammps-manager/state.sqlite') as store:
    store.submit(specification)
    attempt = store.assign(specification.job_id, 'attempt-001', 'worker-01',
                           '/shared/jobs/job-001/attempt-001')
    # After external confirmation that the worker started this attempt:
    store.transition(attempt.attempt_id, 'running')
    # After external confirmation that the process group terminated:
    store.transition(attempt.attempt_id, 'succeeded', exit_code=0,
                     termination_evidence='worker confirmed process group exit')
```

## Data and transaction boundaries

- `submit()` persists a JSON copy of the complete specification and a queued
  event. Later configuration changes do not affect that snapshot. Directly
  constructed specifications also receive structural validation. Inputs are not
  read and hashes are not verified by the store.
- `assign()` creates an attempt, a CPU / memory reservation, an assigned job
  state, and an event in one `BEGIN IMMEDIATE` transaction. CPU slots equal MPI
  ranks multiplied by OpenMP threads. This serializes writers and prevents two
  connections from assigning the same job. It does not check worker quotas:
  the [scheduler](scheduling.md) uses `scheduling_round()` to hold admission reads
  and assignment writes within the same write transaction. Nested store writes
  use savepoints; any round failure rolls back all assignments and waiting events.
- `record_waiting()` records changed queued waiting reasons as events, suppressing
  identical consecutive reasons. It does not change job specifications or state.
- `transition()` updates job and attempt state, timestamps and result fields,
  appends an event, and releases a reservation for a terminal result in one
  transaction. Exceptions roll back all changes, including events.
- Identical submissions, assignment deliveries, and same-state transitions are
  no-ops. Reusing an identity with different data raises `StoreError`. An old
  assignment replay returns its current attempt, including a terminal state;
  it does not create a reservation or authorize a new process launch. Later
  worker implementation must still provide durable start idempotency.
- Each job permits multiple attempts only through explicit bounded retry queuing. Attempt IDs and normalized absolute
  attempt-directory strings are globally unique. Multiple leading slashes are
  collapsed to a single Linux root slash before storage and replay comparisons,
  so `/work/a1` and `//work/a1` cannot belong to different attempts.
  `..` paths are rejected;
  symlinks are not resolved or checked. `retry(job_id)` requires confirmed failure and remaining submitted `max_retries`; it queues a new attempt without changing earlier records. Output directories are neither created nor deleted.

Each Store owns one connection and should be used from one thread. Multiple
Store instances can share a local database; writes wait up to five seconds for
locks, then SQLite raises an error. Foreign keys are enabled. Schema version 2
is recorded in `PRAGMA user_version`; version 1 is migrated atomically without rewriting job JSON. Other unsupported versions fail.
The initial schema is created transactionally. Do not point the store at unrelated
SQLite databases. Operational database / disk / lock failures propagate as SQLite
exceptions; assignment integrity conflicts and invalid operations use `StoreError`.

## State transitions and evidence

Jobs begin `queued`. Assignment changes both job and attempt to `assigned`.
The following transitions are allowed (same-state identical replays are omitted):

| From | To |
|---|---|
| assigned | running, cancelling, unknown, failed, cancelled |
| running | cancelling, unknown, succeeded, failed |
| cancelling | unknown, cancelled, succeeded, failed |
| unknown | assigned, running, cancelling, succeeded, failed, cancelled |
| succeeded / failed / cancelled | none |

`unknown` and `cancelling` retain reservations. Returning from `unknown` requires
the caller to reconcile actual worker state; this storage primitive does not
prove liveness or authorize re-execution. Every terminal transition requires a
nonempty `termination_evidence` string from the caller. This can describe confirmed
process termination or a confirmed never-started assignment. The store only
records this evidence; Linux process verification belongs to [recovery](recovery.md). Success requires
exit code zero and failure requires a reason. Exit codes are optional for failure
and cancellation when termination can be established without an exit status.
A running process cannot jump directly to cancelled; persist cancelling first.
A cancellation racing with normal completion may finish succeeded or failed.

`started_at` records the first transition to running; `ended_at` records the
terminal transition. All timestamps are UTC database observation times, not
worker process timestamps. `events(job_id)` returns ordered dictionaries including
state, reason, exit code and evidence, so later transitions do not erase earlier
failure or disconnection explanations. `specification()`, `job_state()`,
`attempt()`, `jobs()`, `job_attempts(job_id)` and `reservations(worker_id=None)` provide read access. No credentials,
process output, arguments, or real process identities are stored by this API yet.

## Worker runtime records

T005 adds an additive `workers` table, created transactionally when opening a version-1 database. Existing jobs and submission snapshots are unchanged. `worker_records()` reads persisted pause flags and last authenticated metrics; `set_worker()` updates either value transactionally. Registration and authentication remain service responsibilities. Worker-local journals use the same job / attempt store on local durable disk; they are never shared with the manager. See [workers.md](workers.md) for execution and replay semantics.

T006 adds `cancellations`, keyed by attempt ID. `request_cancel()` commits intent and state before delivery; `cancellation()` reads it even after heartbeat expiry. `expire_workers()` changes expired active work to unknown without releasing resources. These runtime operations never rewrite submitted specifications.

## Staged result records

Opening an existing schema-2 database additively creates `launch_claims` and
`result_collections`, preserving specifications and earlier attempts.
`claim_launch()` consumes one staged launch right atomically; `launch_claimed()`
is used by recovery and launch fencing. `set_result_collection()` requires a
terminal attempt with termination evidence, binds a manifest immutably and
records delivery independently of resource reservations. A collected record
cannot regress. The manager alone records delivery after durable publication;
worker poll diagnostics cannot assert collection. `result_collection()` returns
the manifest by default; `include_manifest=False` reads only status, reason and
manifest presence for bounded per-file checks. See [staged results](staged-results.md).
