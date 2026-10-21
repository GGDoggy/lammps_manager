# Operational monitoring (T016)

The local CLI reads the manager's SQLite database using the operator's filesystem
permissions. It does not open an unauthenticated administration endpoint. Protect
the database and manager configuration as administrative files. Worker bearer
credentials authorize only that worker's `GET /v1/health` response, input verification
reports and existing artifact/poll operations; they do not grant global job queries
or priority edits. Use TLS outside loopback, as for polling.

```bash
PYTHONPATH=src python3 -m lammps_manager status --config /etc/lammps-manager/manager.yaml --limit 100
PYTHONPATH=src python3 -m lammps_manager status --config /etc/lammps-manager/manager.yaml --state queued --after 100
PYTHONPATH=src python3 -m lammps_manager status --config /etc/lammps-manager/manager.yaml --worker-id worker-a
PYTHONPATH=src python3 -m lammps_manager inspect --config /etc/lammps-manager/manager.yaml JOB_ID --limit 100
```

## JSON contract

`status` now returns a versioned object (`schema_version: 1`), replacing the old
unbounded JSON array. Consumers should read `jobs` for the page, `counts` for global
job-state counts and `attempt_counts` for global attempt-state counts. Counts omit
states with zero records. Filters apply to the job page, not the global counts.
`observed_at` is manager/operator Unix time. `--limit` defaults to 100 and accepts
1 through 500. Job pages use stable insertion order and `next_cursor` / `--after`;
this is an operational listing, not the dispatch ordering. A worker filter matches
the latest attempt's worker. Concurrent changes can alter later pages; each page
has internally consistent counts and details, not a multi-request frozen snapshot.

`workers` and `alerts` are independently bounded by the same limit. Continue them
with `next_worker_cursor` / `--worker-after` and `next_alert_cursor` / `--alert-after`.
Workers sort by worker ID, alerts by stable alert key. Job, worker and alert cursors
are independent. An empty page and null cursor end a listing.

Job fields include state, latest attempt state/identity, attempt count, queue age,
wait reason, dispatch priority/origin/queue sequence, total CPU demand, unreleased
reservations, execution elapsed seconds, latest attempt error, prelaunch rejection,
and `infrastructure_recovery` (snapshotted budget, usage, deadline and latest decision).
A queued job's dispatch value is its current effective policy; an assigned job uses
the immutable assignment snapshot. Legacy assignments may have unknown priority.
Queue age starts at the actual queue entry, including a return/retry, and is not
reset by waiting diagnostics or priority edits. Elapsed time uses persisted start
and confirmed end timestamps; unresolved execution uses the current time and is
not evidence that the process is alive.

`process_outcome` is present only after recorded termination evidence.
`result_collection` remains separate: a succeeded process can still have pending,
uploading or failed delivery. `transfers` contains input/result progress or null
when absent. `continuation` is null: segment/checkpoint monitoring belongs to T019
and is not implemented here.

`inspect` retains specifications, output locations, rejection/recovery diagnostics
and per-attempt transfer/dispatch records after worker cleanup. CLI history pages
contain at most `--limit` attempts and events each, in chronological order, starting
with the latest page. Use `history_cursors.attempt_before` with `--attempt-before`
and `history_cursors.event_before` with `--event-before` to retrieve older pages.
A full final page may return a cursor whose next page is empty. The Python
`inspect_job` API retains its complete-history default; pass `history_limit` for
bounded queries. Job specifications can include bounded manifests but never worker
execution environment or credentials. Monitoring summaries whitelist worker fields
and omit raw polling payloads, tokens, credential paths and input contents.

## Transfer semantics and overhead

Progress is additive SQLite runtime data, separate from immutable job JSON.
Each attempt/direction has one phase row and at most one row per manifest file.
Phases are pending, transferring, verifying, ready (inputs), acknowledged (durable
manager results), and failed. Known manifest totals are bytes/files; null totals
are unknown. Last-progress time changes for payload movement or a newly verified
file, not for repeated polls or retry phase changes. `updated_at` records phase,
error or total changes. `retries` counts observed transitions into failure, not
HTTP attempts, and the latest error remains visible during retry until completion.
Worker uploading reports map to transferring. Delayed pending polls preserve the
manager's known phase, and uploading polls preserve manager-side verification;
actual artifact operations can still begin a retry after failure.

`transferred_bytes` counts HTTP payload bytes accepted by the manager's input
output stream or received by the manager for results. It excludes HTTP/TLS overhead
and is not receiver acknowledgement. Aborted writes can leave unobservable bytes;
missing counters remain null. Replays accumulate bytes and can exceed the total.
`transferred_files` counts distinct files with observed positive payload bytes;
zero-byte or unobserved transfers remain unknown. `verified_bytes/files` count
unique manifest files that passed exact size/hash checks, including worker input
verification reports. Verification can precede whole-tree atomic publication; only
ready/acknowledged phase proves the corresponding publication. A discarded input
temporary tree can therefore retain evidence that some files were previously
verified, without being runnable. `network_byte_resume` is always false: retries
can retransmit whole files, and input publication retries can retransmit a tree.

Payload counters flush at most once per second per active request and at its end;
verification updates occur at file boundaries and do not append events. Worker
input verification reports are throttled to one per second plus final completion,
carry only a bounded verified prefix index, and are authenticated against attempt
ownership and the immutable manifest. A one-second telemetry timeout does not fail
staging; the final input-ready report still establishes publication. Old workers
may provide phase information with unknown counters. Progress I/O never holds a
scheduling transaction or waits for network traffic under the poll lock. Result
collection stays in the existing background worker executor. This is local
functional verification, not a large-transfer throughput benchmark or proof of
power-loss durability of telemetry.

## Worker health and actionable alerts

Worker summaries report enabled/pause/readiness/draining flags, heartbeat and metric
age, online state, boot/session generation, configured/reported/detected and effective
CPU/memory capacity, CPU/memory/job reservations, available memory and staged-root
free disk. Free disk is sampled on staged workers; unavailable disk/legacy telemetry
is null. Missing telemetry is never reported as zero or healthy. Readiness and metric
freshness remain separate from online status. A disconnected worker does not prove
termination, and monitoring never changes reservations, retries or process state.

Manager configuration accepts:

```yaml
monitoring:
  transfer_stall_seconds: 300
  low_disk_gib: 5
```

Both values must be finite and positive. Alerts activate when transfer inactivity
is strictly greater than the stall threshold or free disk is strictly below the
disk threshold. Heartbeat and metric alerts reuse their existing configured age
limits. Conditions are unknown execution, offline worker, stale metrics, low disk,
stalled input/result transfer, and exhausted infrastructure recovery. Input stalls
stop being actionable after a terminal prelaunch return/cancellation; undelivered
results remain actionable after process termination. An exhausted-recovery alert
requires an actual exhausted decision, not merely zero remaining budget.

Alerts have a stable condition/object key, `first_seen_at`, `last_changed_at`,
`active`, `resolved_at` and an actionable operator suggestion. Repeated unchanged
observations do not grow history or change timestamps. Recovery resolves the same
row; recurrence reactivates it while preserving the first occurrence. The manager
refreshes alerts during periodic expiry/startup, including without worker polls;
CLI status also refreshes them. Resolved rows remain inspectable. No external
notification channels, Web UI, exporter, checkpoint continuation or real-host
acceptance is included in T016.
