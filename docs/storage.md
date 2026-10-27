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

V010 adds an opt-in Python-model field, `JobSpecification.mpi_placement`,
an immutable tuple of `MpiParticipant(worker_id, mpi_ranks, memory_gib)` records.
Nonempty placement requires at least two distinct workers and staged storage;
rank and memory shares must sum to the specification's resource totals. This
intent is persisted and decoded for the explicit native coordinator. Ordinary
`assign()` rejects it and the scheduler records `multi_host_protocol_unavailable`
without creating an attempt or reservation. Empty placement is omitted from
stored JSON and worker payloads; legacy snapshots decode with empty placement
and are never rewritten. There is no CLI or YAML option for this contract yet.

The `lammps_manager.multihost` API provides `reserve`, `prepare`,
`authorize`, `terminate`, `finish` and retained `members` inspection. `reserve`
checks supplied effective capacities and pinned registered generations inside
one write transaction, creates a single leader attempt and reserves all participant
shares. `Store.reservations(worker_id)` includes peer shares as well as ordinary
work, while the submitted specification retains the total resources. Callers must
add fresh metric/load admission; this low-level API performs no network/process
checks. The explicit `mpi_runtime.Session` adds current admission and durable
node helper RPCs; the ordinary worker protocol and CLI do not expose it.

`prepare` binds each participant's boot/session, common executable digest and
absolute execution directory. Authorization requires every participant prepared
on its current generation and no cancellation. `terminate` accepts only fenced
never-started, ended-session or changed-boot evidence; actual process checks and
launch-fence closure are the caller's responsibility. No participant's terminal
report releases resources individually. `finish` commits the combined outcome
and releases every share atomically after all participants have termination
evidence. Direct `Store.transition` also enforces this barrier, including all-host
success/authorization checks. Participant metadata remains after release, so
CPU sampling history charges each host its own rank-times-thread share even for
a short completed group. Earlier JSON snapshots and schema version 2 remain
unchanged; new coordinator tables are additive. These are local storage
guarantees; [V010](deployment.md) separately records actual multi-host execution
acceptance. Per-host `mpi_outputs` and the immutable `mpi_runtime` plan retain
collection progress and original host bindings. Combined output publication waits
for every participant and verifies the aggregate artifact limits.

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

## Contention and initialization

Schema marker 3 records completion of all additive tables and indexes. Opening a
current database checks the marker without acquiring a writer reservation. New
and version-1/2 databases initialize under a write transaction and recheck the
marker after acquiring it; stored specifications are not rewritten. Each Store
owns one connection and must stay on its creating thread. Future schema changes must bump
the completed marker; per-open `CREATE IF NOT EXISTS` is not a migration strategy. `Store(path,
timeout=5)` preserves the Python API default; manager requests use a 0.2-second
busy wait and optional transfer telemetry uses zero wait. The journal mode is
unchanged. SQLite JSON functions are required for compact continuation snapshot
projections; both supported local test runtimes provide them.

Only SQLite BUSY/LOCKED errors are retryable. HTTP returns 503 with
`Retry-After: 1` before a response starts; other SQLite errors return 500 and are
logged. Periodic maintenance records contention and retries on a later pass.
Operations containing filesystem publication are never automatically replayed as
whole transactions. Durable results/checkpoints still require a successful
commit before acknowledgement or staged cleanup.

A downgrade to code that only accepts marker 2 requires coordinated idle
maintenance, a SQLite API backup, and restoring marker 2 in the manager database and every worker journal before
reopening with that code. Marker 3 changes initialization bookkeeping only; it does not remove
columns, tables or submission data. Retain the exact previous source/configuration
and verify schema compatibility before any downgrade. Restoring an older database
backup after new work has run would discard runtime evidence and is not an
acceptable routine rollback.

## Admission projections and contention (T026)

Schema version 5 transactionally adds small `admission_specs` and `run_controls`
projections plus worker-local result/checkpoint commit intent tables. Existing
immutable snapshots are retained byte for byte. SQL triggers maintain projections
when snapshots are inserted or explicitly updated; ordinary runtime transitions
do not rebuild them. The one-time upgrade reads existing snapshot JSON and may
need an offline maintenance window on large databases. Back up before upgrading;
older binaries must not open a version-5 database. Current-version opens require
no write transaction. SQLite JSON functions are required (already used by run
control queries in the previous schema).

Admission, reservations, CPU accounting and recovery budgets read only the small
projection. Queue ordering reuses each decoded admission record. Full sampling
validation remains at submission and full specification/input loading, including
mutation detection. Per-segment immutable snapshots are deliberately retained;
this migration does not deduplicate historical sampling arrays or reclaim their
storage.

The manager skips dispatch when no supplied snapshot is online, enabled, ready,
unpaused and not draining. Heartbeats, artifact collection, worker expiry, alerts
and continuation recovery still run. Continuation maintenance visits up to 16
runs per pass in run-ID order, wraps fairly and skips unchanged runtime state
without acquiring the writer lock. Changes racing a read-only skip are revisited
on the next pass; positive advancement/reset decisions are rechecked atomically.
This bounds run count, not the cost of copying a single large artifact.

Control/maintenance SQLite waits default to 0.2 seconds; artifact publication
waits default to 2 seconds, separately configurable under `manager`. These waits
apply to lock acquisition, not total request duration. HTTP busy responses remain
503 with Retry-After, normal worker control backoff remains bounded at 1–30
seconds, and artifact retries retain their existing collection cadence.
SQLite debug transaction wait/held measurements are separate from per-attempt
Python artifact lock wait/held measurements.

WAL is not automatically enabled: it improves reader/writer overlap but still
serializes writers and does not remove a long admission transaction. The explicit
`tools/verification/scheduler_contention.py` harness opens its source read-only,
backs it up into a disposable database, requires paused workers and compares
DELETE/WAL admission against a concurrent 0.2-second control writer. It never
launches jobs or changes the source. Memory-backed timing does not establish
production disk durability or real service throughput.
