# Polling manager and workers

T005 provides a standard-library HTTP(S) manager and Linux workers using the same absolute shared filesystem paths. SQLite databases stay on each host's local durable disk. No additional communication dependency is required. This implementation has local loopback verification with fake executables; [V001](verification/v001-two-workers.md) verifies real shared-storage dispatch on mozzarella and allegro. T006 separately verifies isolated single-host real MPI/LAMMPS cleanup; see [recovery.md](recovery.md). See [architecture.md](architecture.md) for deployment prerequisites and [../tasks.md](../tasks.md) for evidence.

Hosts without a common filesystem cannot use this execution mode. T007 now provides [staged input preparation](staged-inputs.md) between separate roots, and [durable result collection](staged-results.md) for staged execution. [T009](verification/t009-staged.md) verifies staged execution with one real remote worker; see the [architecture plan](architecture.md#planned-operation-without-shared-storage). Separate directories with the same path name, or a one-time copy, do not satisfy the shared-filesystem requirement.

## Configuration and startup

Use the existing [manager and worker configuration](configuration.md). Create a different unpredictable bearer credential of at least 32 non-whitespace ASCII characters for each worker. Store each credential in a private regular file (mode 0600 or 0400) readable by the service account, on both the manager and its worker. Never commit credentials. A manager-side JSON file maps registered IDs to absolute credential-file paths, for example:

```json
{"node-a": "/etc/lammps-manager/node-a.token"}
```

All configured workers must have credentials, and credentials must be distinct. The JSON mapping contains paths, not tokens. Tokens and mapping changes require service restart. Protect the worker journal and manager database using the service account's filesystem permissions.

Start a manager using a valid certificate and matching private key:

```bash
PYTHONPATH=src python3 -m lammps_manager serve \
  --config /etc/lammps-manager/manager.yaml \
  --credentials /etc/lammps-manager/credentials.json \
  --host 0.0.0.0 --port 8443 \
  --tls-cert /etc/lammps-manager/server.crt \
  --tls-key /etc/lammps-manager/server.key
```

Set each worker's `manager_endpoint` to the HTTPS origin, then run on that worker:

```bash
PYTHONPATH=src python3 -m lammps_manager worker \
  --config /etc/lammps-manager/worker.yaml \
  --state-directory /var/lib/lammps-worker/node-a
```

The worker checks local configured directories, executable paths and token readability at startup. The state directory is created if needed and must be durable local storage, separate from job output. Its OS lock prevents concurrent use; its identity file binds the journal to the worker ID, manager origin and shared root. Reuse it after restart. Do not replace it to bypass unresolved work.

HTTPS verifies the manager certificate and hostname through Python's default trust store. A private CA may be supplied through `SSL_CERT_FILE`. Redirects and environment-configured HTTP proxies are disabled to keep credentials at the configured manager origin. Plain HTTP is allowed only with literal loopback addresses, such as `127.0.0.1`, for local testing. The listener currently uses IPv4. Do not expose a plaintext endpoint through a network proxy or tunnel.

The manager checks existing local paths at startup; it does not deploy workers. Worker connections must reach the manager and clocks must be synchronized. Each service runs in the foreground; see [recovery and deployment operations](recovery.md) for restart supervision and upgrades.

## Polling and admission

Workers POST JSON to `/v1/poll`, identifying themselves with `X-Worker-ID` and a per-worker `Authorization: Bearer ...` header. Unknown identities and incorrect tokens receive HTTP 401. Worker reports can update only attempts assigned to that authenticated worker. Malformed polls receive HTTP 400 without secret contents. Requests have a 1 MiB body limit and a 10-second connection timeout. Accepted TLS sockets perform their handshake in their individual request threads under the same 10-second timeout, so an idle TLS peer cannot block acceptance or scheduling for other workers. HTTP requests run in separate threads, with poll state and scheduling serialized by a per-service lock; artifact responses stream independently of that lock. Server close waits for active request handlers. There is no public submission or administration endpoint; use the existing CLI on the manager host.

Before metrics or results are accepted, the worker reads a fresh manager-created challenge file directly under the configured shared root and echoes its contents. The worker checks the challenge's resolved parent. Filesystem errors while resolving the challenge or shared root, or reading the challenge, are logged and defer the poll until the next heartbeat interval. Reports remain unacknowledged and assignments are not accepted during this deferral; existing reservations and execution remain intact. Once access returns, reports are replayed and assignment processing resumes. A resolved challenge outside the configured root remains an error. After durable acceptance, the executor checks the copied input SHA-256 before launching; a mismatch is persisted as a failed attempt with never-started evidence and reported on a later poll, allowing subsequent jobs to proceed. The shared root and job trees must be trusted and writable/readable by the appropriate service accounts. These checks establish current access to shared paths; they do not prove all auxiliary inputs immutable or validate MPI installations.

Linux monitoring reads per-logical-CPU and aggregate CPU deltas from `/proc/stat`, total and available memory from `/proc/meminfo`, and CPU affinity through `sched_getaffinity`. A sample without a measurable CPU interval reports 100% usage conservatively. Hardware measurements do not detect cgroup memory quotas; configured limits must reflect those restrictions. The manager timestamps authenticated heartbeats; sample timestamps are retained so stale/future samples fail admission. Background maintenance uses persisted snapshots and the [scheduler's quota accounting](scheduling.md). A completed scheduling pass observes `manager.schedule_interval_seconds`; a full four-assignment transaction leaves the next bounded pass due. Expiry and reconciliation continue even when workers do not poll.

`heartbeat_interval_seconds` controls worker polls. Polling and reporting continue while attempts execute in a thread pool bounded by the worker’s `max_concurrent_jobs`. Each executor uses its own SQLite connection and launch session. In staged mode, result enumeration and delivery use another dedicated thread, so slow output collection does not delay heartbeats, cancellation delivery or recovered-process monitoring; see [staged results](staged-results.md). The manager stores last metrics and pause flags in SQLite; expired heartbeats and stale metrics block new admission. Disconnection never releases reservations. T006 marks expired work `unknown` while retaining reservations and adds [Linux reconciliation, cancellation, time limits and explicit bounded retries](recovery.md). Application-error retries remain explicit; T014 adds bounded infrastructure recovery only after conclusive termination evidence.

T010 supports concurrent shared and staged attempts on one worker. Each launch must fit the smaller manager / worker / detected-hardware CPU and memory quotas and the smaller manager / worker concurrency limit. CPU reservations remain MPI ranks multiplied by OpenMP threads; low measured utilization never reduces them. All unreleased local reservations, including unknown and cancelling work, consume quotas. Uncertain attempts can coexist with new work only when the remaining quotas permit it.

Before each launch, the worker rechecks enabled/pause flags, aggregate reservations, available memory, per-CPU load/reserve admission and sample freshness. Starts not yet reflected in the sample consume pending memory, including other launches admitted during the same poll. A running transition racing with sampling remains pending conservatively; starts already reflected in a later sample are not deducted again. Staged assignments without a launch claim do not consume sampled available memory while waiting for launch admission; their reservations still count against CPU, memory-budget and concurrency quotas. A durable launch claim consumes pending memory immediately, before its executor starts. Unknown and cancelling attempts retain conservative pending accounting. An already accepted staged candidate is excluded from its own pending deduction exactly once. A sample that becomes stale during input preparation defers launch until a later poll or a bounded T013 prelaunch return. When conditions deteriorate, assignments retain reservations during a bounded headroom grace period. T013 allows safe reassignment only after a durable prelaunch fence; uncertain or claimed execution remains reserved. Input staging remains synchronous, with authenticated in-transfer heartbeats and cancellation handling between files/read chunks; a blocking I/O operation can still delay polling. After any attempted in-transfer heartbeat poll, including a lost control response, all remaining staged launches in that tick wait for the next normal poll to refresh manager policy and local admission metrics, including pause state. Background result collection retains its separate single outstanding pass.

The worker tracks execution ownership by attempt ID. Recovery inspects only attempts without an owned executor, so recovered processes still receive cancellation/time-limit handling while other attempts execute. Cancellation and timeouts target each attempt's dedicated launch session. Completed futures surface unexpected executor errors during polling; shutdown joins all executors and collection before unlocking the journal. Local fake-process tests establish concurrency and isolation; real concurrent MPI placement and affinity require separate deployment verification.

If strict resolution of an assigned job directory fails with a filesystem error (for example, a missing path or denied permission), the worker logs the attempt ID and error, leaves it unaccepted with its manager reservation intact, and retries validation on later polls. Restore directory access to let that assignment proceed; it is not automatically failed or reassigned. Resolved paths outside the shared root are still rejected.

## Execution and duplicate delivery

Each accepted attempt and its immutable specification are committed to a local SQLite journal before launch. Repeated deliveries do not launch again. An exclusive attempt directory provides a second fence; an existing attempt path results in `unknown`, never an overwrite or evidence of termination. On restart, existing journal entries are reported but never relaunched, including entries recorded before a crash during launch. Such unresolved work retains reservations until conservative Linux reconciliation or an explicitly verified administrative resolution through the storage API.

Execution uses the same isolated inputs, argv arrays, `OMP_NUM_THREADS`, stdout/stderr and metadata as [local batches](local-batches.md). Exit code zero succeeds only after the launcher is reaped and both the original process group and live session members are absent. Remaining descendants or execution uncertainty retain reservations. Results and failure evidence are persisted locally before reporting; identical result replays are idempotent. A short process can report its terminal result before the manager sees `running`; the manager records the intermediate transition in the same transaction. Manager-side start/end times are report receipt times, not precise remote process timestamps. Worker-initiated timeouts can also finish between polls; the manager records missing running and cancelling transitions atomically with the terminal report. Missing termination evidence rolls back the report and retains reservations. Local journal times describe execution transitions. `process.json` records PID, process group, Linux boot ID and process start ticks durably after launch; T006 checks this identity and launch-session membership during reconciliation; lost exit codes never imply success.

Unchanged reports are suppressed after a successful response and replayed after a worker restart. When the manager returns an `unknown` assignment after heartbeat expiry, the worker invalidates that report's acknowledgement and reads its current journal state again for the next poll, even during execution or with ready staged inputs. This restores the manager's state without restarting or relaunching work; genuinely unresolved local attempts remain `unknown`. Transport failures, including incomplete HTTP response reads, leave reports unacknowledged and polling continues after the heartbeat interval; unresolved reservations remain intact. The journal keeps historical records. T012 reclaims owned worker-private staged files after durable result acknowledgement; shared output and manager results remain retained. See [cleanup controls and diagnostics](staged-results.md#worker-staged-cleanup-t012). Very large unacknowledged histories exceeding the API request limit need future report pagination. Worker shutdown waits for its active execution and any active staged collection and bounded cleanup operation before releasing the journal lock; it does not cancel the process. Do not delete journals or output while attempts may still be running.

## Pause and resume

On the manager host:

```bash
PYTHONPATH=src python3 -m lammps_manager pause --config /etc/lammps-manager/manager.yaml node-a
PYTHONPATH=src python3 -m lammps_manager resume --config /etc/lammps-manager/manager.yaml node-a
```

Pause flags persist across service restarts. Pause blocks new scheduling and prevents a worker from starting pending assignments once it receives the paused response. Work already running continues and reports completion. A launch already authorized by an earlier poll may race with a pause; pause is not cancellation. Resume allows later polls to admit/start work again. Submission snapshots are never rewritten.

## Local verification

`make check` includes temporary loopback HTTP integration with two workers, private temporary credentials, fake MPI/LAMMPS scripts and local journals. No remote hosts, real MPI or LAMMPS are contacted. Environments that prohibit all socket creation need permission to run these tests outside that restriction. Before a real deployment, separately confirm the architecture prerequisites and record a two-machine batch plus real process-group behavior in `tasks.md`.

T011 uses only valid CPU deltas from the intersection of online CPUs and worker
affinity at both endpoints; SMT siblings each count once. Idle time excludes
iowait. Newly visible CPUs, counter resets and zero intervals supply no idle
headroom. Aggregate CPU usage remains telemetry/ranking, never a second veto.
See [per-CPU settings](configuration.md#per-cpu-admission-t011) for the minimum
floor, reserve and migration rules, and [sampling budgets](scheduling.md#sampling-budget-and-diagnostics)
for pending CPU accounting. CPU accounting waits for a complete post-start
interval; memory retains its existing observation-time accounting. The worker
excludes an already accepted candidate exactly once. Unclaimed staged assignments
consume quotas but do not consume pending launch headroom until claimed, allowing
one fitting assignment to progress. A claim consumes headroom before executor
startup. Polling, cancellation, timeout and result collection continue independently.

## Prelaunch return and readiness (T013)

Polling responses advertise `prelaunch_protocol: 1`. Workers report boolean
`ready` and `draining` snapshot fields (default true and false for legacy peers).
The worker Python API exposes these flags as `Worker.ready` and
`Worker.draining`; existing CLI pause/resume still controls manager admission.
Closed workers continue heartbeats, reconciliation, cancellation and collection.

A new worker journals a rejection before reporting it. Rejection and launch claim
are mutually exclusive SQLite writes, including in shared mode. A claimed or
uncertain launch cannot be returned. A rejection contains `code`, `reason` and
`retry_at`; the authenticated manager journals the same permanent fence, ends the
old attempt and atomically requeues eligible work. Delayed reports for fenced
attempts cannot affect successors. New assignments always receive new IDs and
retain old directories. Existing legacy peers retain conservative deferral.

Codes distinguish `headroom`, `incompatible`, `not_ready` and `invalid_input`.
Invalid input remains failed. Other returns exclude the same job/worker pair for
30 seconds; three returns exhaust that pair and expose
`prelaunch_rejection_exhausted` in scheduling diagnostics. Other workers remain
eligible. These returns consume no application or infrastructure retry budget.
Temporary headroom shortage receives a durable 30-second grace period before
return; admission is checked again before every launch. Worker closure bypasses
that grace period.
Missing shared storage cannot establish safe acceptance and stays reserved.
Checkpoint-bearing work is not implemented; T018-T019 must extend this protocol
before introducing continuation copies.

## Concurrent control and maintenance

A single background manager thread owns expiry, continuation refresh, alerts and
scheduling. `make_server()` starts it; embedded Python callers must call
`start_maintenance()` and eventually `close()` themselves. The HTTP accept loop performs no database maintenance. Each scheduling
transaction admits at most four jobs, with capacity checks and reservations still
atomic; a full batch leaves scheduling due for the next pass. Normal polls commit
reports and read durable assignments, optionally waiting at most 200 ms for a
maintenance pass. Dispatch takes its comparison time after reading snapshots so
heartbeats arriving during maintenance are not treated as future observations.
Heartbeat-only polls update authenticated liveness/environment
and return cancellation commands without invoking scheduling or continuation.

Each worker has one background input preparer with its own SQLite connection.
Eligible unclaimed assignments rotate in attempt-ID order, wrapping after each
pass; failed input retries cannot continually preempt another reserved attempt.
Uncertain, cancelling, cancelled and claimed attempts are excluded from preparation.
Normal monitoring, reports and cancellation continue while a file read blocks.
Inputs becoming ready do not grant launch permission: a subsequent normal poll
must pass current cancellation, readiness and resource checks. Closing admission
can fence and return an unclaimed attempt even before preparation finishes.
Retryable 503 responses preserve unacknowledged reports and launch fences. The
worker run loop backs off from one to at most 30 seconds, never below its configured
heartbeat interval, and resets this backoff after a successful poll.

Input-transfer counters are best effort, coalesced at most once per second;
SQLite contention must not abort verified input bytes. Missing counters remain
unknown until later verified/ready reporting. File hashes and durable publication
acknowledgements remain mandatory. DEBUG logging on `lammps_manager.store` records
outer-transaction wait/hold seconds without request bodies or credentials.
