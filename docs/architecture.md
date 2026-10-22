# Architecture and requirements

## Status and scope

This is the permanent design reference for a Python / Linux dispatcher for independent LAMMPS batches. Configuration validation, CLI help / version, immutable job specifications, and the [SQLite storage API](storage.md) are implemented. See [configuration.md](configuration.md) for the actual schema and validation limits and [tasks.md](../tasks.md) for progress and verification. Explicit sequential [local batches](local-batches.md) are implemented; the [scheduler API](scheduling.md) is implemented with caller-supplied snapshots; the [polling manager and workers](workers.md) implement shared-storage execution, authentication and Linux monitoring. Linux [process recovery and operations](recovery.md) are implemented with conservative identity checks; [T009](verification/t009-staged.md) verifies staged dispatch across two hosts with one remote worker; [V001](verification/v001-two-workers.md) verifies shared dispatch on two actual workers. The storage API provides atomic assignment, recorded state transitions, and evidence-gated reservation release, but does not verify processes or enforce worker capacity by itself. Scheduler admission enforces effective quotas within the assignment transaction.

One central manager will control multiple persistent Linux workers, dispatch according to load and resource quotas, and record execution status, output, and failure reasons. Dynamic adjustment means choosing the destination and concurrency of new jobs. Running jobs retain fixed MPI ranks and OpenMP threads and are not migrated. Jobs are independent; dependency graphs are outside the first release.

The first deliverable must reliably complete an independent batch, respect dispatch quotas, avoid duplicate execution during disconnection, and retain traceable output. Example configuration values are illustrative and have not been validated for a real deployment.

## Components and data flow

```text
Python batch generator / CLI
    | job specifications and input files
    v
Single manager -- local SQLite (jobs, attempts, events, reservations)
    | scheduler creates assignments and reservations atomically
    | workers poll for assignments and acknowledge receipt
    v
Persistent worker on each Linux host
    |-- CPU, available-memory, and process monitoring
    |-- MPI / LAMMPS process management, stdout, stderr, exit code
    `-- heartbeats and results back to the manager
```

The manager is the sole dispatch authority. Workers initiate connections, reducing the need for inbound services on worker hosts. Cross-host communication must authenticate workers, protect credentials, and accept only registered identities. SSH is for deployment and maintenance, not the sole evidence that a job is alive. If an environment permits only SSH, redesign launch and reconciliation interfaces while preserving the same state and reservation model.

Use one manager and SQLite on the manager's local disk, never a shared network directory. Initially all workers access job directories through the same absolute shared-filesystem paths. Without shared storage, implement staging and result collection before multi-host acceptance. Validate shared-path permissions, connectivity, and LAMMPS / MPI versions during deployment.

Add modules only with their features:

| Module | Responsibility |
|---|---|
| `config` / `models` | Validated configuration and job / worker models |
| `generator` | Batches, separate input files and directories, missing-file validation |
| `store` | SQLite transactions, state transitions, events |
| `scheduler` | Job / worker selection, capacity checks, reservations |
| `manager` | API, scheduling loop, heartbeats, restart reconciliation |
| `worker` / `executor` | Monitoring, local durable records, process start / stop, results |
| `cli` | Submission, status, inspection, cancellation, pause / resume |

Provide CLI operations before considering a Web UI. Validation, submit, run-local, status, inspect, serve, worker, pause, resume, cancel and retry commands currently exist.

## Planned operation without shared storage

The poincare/allegro topology has no common filesystem; a separate V001 verification topology places the manager on mozzarella, which shares /mnt/data with allegro. Staged storage is therefore a prerequisite when the manager remains on poincare, scheduled as T007 (inputs), T008 (results), and T009 (real two-host acceptance) in [tasks.md](../tasks.md). T007 implements input snapshots and transfer via explicit `mode: staged`; T008 enables staged execution with [durable result collection](staged-results.md). See [staged-inputs.md](staged-inputs.md) for current settings, protocol, migration and limitations. A shared mount is not required by staged mode.

Staged submission preserves a complete immutable input-tree manifest, including auxiliary files, relative paths, sizes and SHA-256 hashes. Workers will use their own local roots; local execution paths and transfer progress must not rewrite submitted specifications. Authenticate artifact transfers against attempt ownership, stream with explicit size/count limits, reject unsafe paths and file types, and atomically publish only fully verified inputs. Persist acceptance and preserve duplicate-launch fences across transfer retries. Keep simulation files outside poll JSON and retain the existing shared mode for existing jobs.

Collect stdout, stderr, execution/process metadata and generated outputs with verified manifests, temporary uploads, atomic publication and durable acknowledgements. Track process outcome separately from result collection. A confirmed process exit can permit reservation release, while incomplete collection must remain visible and must not be represented as fully delivered success. A failed transfer never authorizes rerunning a simulation or releasing an unresolved process reservation. Retain worker artifacts until acknowledgement. T012 implements bounded, restartable cleanup of verified worker-owned staged attempt trees after durable acknowledgement; journals, launch fences and manager publications remain retained. See [staged results](staged-results.md#worker-staged-cleanup-t012).

Prefer extending the existing authenticated HTTPS polling transport for artifacts. T009 verified direct worker-to-manager HTTPS on the private LAN with a dedicated trusted test certificate; production connectivity and service topology still need deployment-specific checks. If the environment permits only SSH, a protected SSH transport needs an explicit design preserving authentication, durable journals and reservation rules. Do not assume an insecure tunnel or silently relax TLS restrictions.

[T009](verification/t009-staged.md) verified a manager on poincare and one remote worker on allegro, with distinct roots, real MPI / LAMMPS, verified artifacts and interrupted-transfer replay. [V001](verification/v001-two-workers.md) now verifies two actual workers using a manager on mozzarella and their existing common NFS mount. T006 recovery and cancellation are described in [recovery.md](recovery.md); their verification remains separate from staged dispatch. Staged storage remains necessary for the poincare-manager topology; the V001 shared-mode topology instead uses mozzarella as its manager.

## Configuration and state boundaries

Environment differences, resource limits, and policy parameters belong in configuration. Runtime facts belong in SQLite; algorithms and reliability guarantees belong in code. Simulation parameters belong in generated job specifications, rather than all being placed in central configuration.

| Information | Owner and boundary |
|---|---|
| Worker inventory | Manager configuration: identity, connection information, labels, permission to accept new work; online status is runtime state |
| Job resource defaults | Configuration: MPI ranks, threads per rank, memory, time limit; merge per-job overrides into immutable submission snapshots |
| Worker capacity | Manager and worker configuration: CPU / memory quotas and concurrency; worker detects hardware capacity and uses conservative effective limits |
| Busy-host thresholds | Configuration: CPU threshold, memory reserve, monitoring / scheduling intervals, metrics age |
| External load | Configurable admission policy; disabling CPU load checks must still retain available-memory and system safety checks |
| Scheduling | Named policy and parameters; priority / descending CPU demand / FIFO plus least-loaded eligible worker; never arbitrary Python code from config |
| Storage | Mode, work / result locations, retention policy; staged mode transfers inputs and results; retention controls remain planned |
| Execution | Worker configuration: LAMMPS / MPI paths, fixed argument arrays, environment; no shell command templates |
| Communication | Endpoint, identity, heartbeat / timeout settings, protected credential references |
| Retry policy | Explicit application retries default to zero; new submissions snapshot one infrastructure recovery with a 30-second delay; legacy snapshots retain zero automatic recovery |
| Simulation inputs | Job specification / generator: temperature, particle count, seed, template version, input identities |
| Runtime history | SQLite: queue state, worker, process identity, heartbeat, exit code, attempts, times, errors |
| Correctness | Code: atomic reservations, idempotent starts, restart reconciliation, process-group cancellation, validation |
| Secrets | Environment or protected credential files, never committed secret contents; config contains references only |

This table defines responsibilities, not additional accepted YAML keys. The exact currently accepted fields and examples are in [configuration.md](configuration.md), [manager.yaml](../examples/manager.yaml), and [worker.yaml](../examples/worker.yaml). Connection inventory, monitoring cadence, result retention controls, and retry delay / error classes may require future schema extensions; they are not all present today.

Manager configuration owns scheduling, storage, inventory, and job defaults. Worker configuration owns local identity, endpoint, executable locations, work root, and local limits. Submission combines defaults and overrides and preserves final resource values and input identity. Configuration changes take effect on service restart, without hot reload; they affect future submissions, never existing job specifications. When manager and worker limits conflict, use the smaller limit, also bounded by detected hardware capacity. Workers may refuse an excessive launch.

Validate schema, ranges, duplicate identities, paths, and executables with clear errors. Structural checks do not establish deployment readiness; current local path checking is explicitly opt-in and has the limits documented in configuration.md. CPU slots mean logical CPU quota, calculated as MPI ranks × threads per rank. Validate MPI / OpenMP affinity to avoid overlapping core use. Memory reservations and CPU quotas are accounting, not OS enforcement; cgroups require separate future work.

## Scheduling and resource accounting

Each scheduling round excludes paused, disabled, offline, stale-metric, and incompatible workers. Examine queued jobs by priority descending, total CPU demand descending, then fresh queue-entry order (T015). The first version may skip a temporarily unplaceable job to run a smaller one, but must expose its waiting reason. Long-wait fairness improvements are later work.

A worker can accept a new job only when all conditions hold:

1. CPU reservations for assigned and running jobs plus the new requirement fit the effective CPU quota.
2. Memory reservations for assigned and running jobs plus the new requirement fit the effective memory quota.
3. Latest available memory minus reservations for jobs not yet started can accommodate the new job and the configured safety reserve.
4. Assigned and running job count remains within maximum concurrency after admission.
5. If external-load accounting is enabled, the count of affinity-visible logical CPUs strictly below the utilization threshold, minus unreflected launches, fits the whole job plus `reserve_slots`. Total unreleased CPU reservations plus the job at or below `min_slots` may bypass that load/reserve gate, but never monitoring freshness or other safety checks. See [per-CPU admission](configuration.md#per-cpu-admission-t011).

Per-CPU utilization signals temporary logical CPU headroom, not exclusive core ownership. T011 counts SMT siblings separately and treats iowait as busy. Aggregate CPU utilization ranks candidates but never vetoes admission. CPU launches remain pending until a complete post-start sampling interval; durable attempt history prevents repeated spending of one sample, even after a quick completion. The reserve yields to the minimum floor; neither changes running jobs or MPI/OpenMP placement. Running jobs keep their full reservations even during low utilization. Available memory already reflects running processes' actual consumption; do not subtract all running memory reservations from it again.

Rank eligible workers by CPU utilization and remaining quota, with a fixed, testable tie-break rule. The implemented ordering is ascending CPU utilization, descending remaining CPU quota, descending remaining memory quota, then lexicographic worker ID (see [scheduling.md](scheduling.md)). Create the attempt and reservation in a database transaction before dispatch to prevent oversubscription within a round. T010 polling workers execute multiple shared or staged attempts concurrently within effective CPU, memory and concurrency quotas. Each attempt owns an isolated launch session, directory and journal record. A worker rechecks aggregate unreleased reservations and fresh local conditions before each launch; pending starts consume sampled available memory until a later sample can reflect them. Recovery continues for attempts without an owned executor while other attempts run. T005 conservatively defers insufficient-resource assignments while retaining their reservations, and rechecks on later polls. T013 adds permanent prelaunch rejection fences, bounded headroom grace and per-worker cooldown/exhaustion before safe reassignment; uncertain launches remain reserved.

Reject a job exceeding every worker's maximum capacity at submission, or expose an explicit unschedulable reason. Never leave it silently waiting forever.

## Lifecycle and reliability

Normal flow: `queued → assigned → running → succeeded / failed`. Also support `cancelling`, `cancelled`, and `unknown` for cancellation and loss of contact.

- Give each job a stable ID and each execution an independent attempt ID and directory. Retries preserve previous output.
- Workers durably record accepted attempts and process identities. Repeated delivery of the same attempt must never launch another execution. Combine PID with start time or supervisor identity to prevent PID-reuse errors.
- On heartbeat timeout, stop dispatching to that worker and mark existing work `unknown`. Disconnection does not establish termination and does not authorize immediate execution elsewhere.
- Reconcile after manager or worker restart. Confirm the original attempt ended or was terminated before releasing reservations or retrying. If termination cannot be established, retain `unknown` and permit manual resolution.
- Record argument arrays, working directory, start / end times, stdout, stderr, exit code, and completion evidence for each attempt. Initially exit code 0 means success; add explicit validators if required outputs or LAMMPS-specific checks become necessary.
- Persist cancellation requests before asking the worker to terminate the complete process group gracefully, escalating after timeout. Confirm exit before marking `cancelled`. Real MPI launcher descendant cleanup requires separate verification.
- Application retries default to zero and require explicit authorization within the submitted budget. T014 new submissions separately default to one infrastructure recovery after a 30-second delay, only with conclusive session termination evidence; legacy specifications retain zero automatic recovery. Input errors never trigger automatic recovery.
- Pause prevents new assignments while running jobs continue. Shared directory deletion and manager result retention require separate explicit operations. T012 automatically reclaims owned worker-private staged files, including failed output, only after confirmed termination and durable result acknowledgement; manager copies and journal history remain available.

## Delivery stages

The stages describe product acceptance; [tasks.md](../tasks.md) defines the implementation order and evidence. Reliability constraints apply whenever affected functionality is introduced.

| Stage | Work and acceptance |
|---|---|
| 1: Configuration and local batches | Schema, SQLite, generator, CLI, local execution. Submit a batch into separate directories, persist specifications, inspect status, and record success / nonzero exit correctly. First use fake jobs with controlled duration and exit codes, then separately run a small LAMMPS example when available. T001–T003 are implemented; [V008](verification/v008-local-batches.md) verifies real local MPI/LAMMPS success, invalid input, CLI cancellation and sequential termination ordering. |
| 2: Multi-host dispatch | Registration, authentication, heartbeat, monitoring, reservations, scheduling, shared storage, real MPI arguments. Complete a batch on at least two workers; enforce CPU / memory / concurrency limits; pause, stale metrics, and high external load block admission. T004 implements local scheduling decisions with fake-metric verification; T005 implements polling, authentication, heartbeats, monitoring and shared execution with local two-worker verification; [V001](verification/v001-two-workers.md) verifies a real shared-storage batch on two workers and quota accounting; [V003](verification/v003-concurrency.md) verifies concurrent real simulations and cancellation/timeout isolation on one worker; [V004](verification/v004-cpu-admission.md) verifies real per-CPU load, minimum/reserve, stale-metric and pending-sample admission through the scheduler/monitor/executor APIs. |
| 3: Recovery and operations | Idempotency, durable local records, reconciliation, cancellation, time limits, controlled retries, deployment / service restart documentation. Manager / worker restarts, temporary disconnection, duplicate delivery, and MPI cancellation must neither duplicate jobs nor release resources prematurely; failed output remains inspectable. T006 implements conservative Linux reconciliation, cancellation, time limits and explicit bounded retries; see [recovery.md](recovery.md). [V005](verification/v005-restart-transport.md) verifies actual manager/worker OS process restarts, heartbeat outage, truncated TLS upload and lost commit response on an isolated single-host endpoint; physical reboot/power loss and production service deployment remain unverified. |
| Deployment prerequisite for hosts without shared storage | T007–T009 add staged inputs with checksums, durable result collection and real two-host acceptance. T007 input preparation and T008 result collection / execution are implemented with local fake-process verification; T009 verifies real two-host staged dispatch with one remote worker, including successful/failed execution and interrupted-transfer replay; see [evidence](verification/t009-staged.md). Do not present results as fully delivered before collection finishes. |
| 4: Optional extensions | Web UI, priorities, fairness, cgroup enforcement, monitoring, and alerts. |

## Deployment prerequisites

Use the defaults above for planning, but confirm these before deployment and real multi-worker acceptance:

- Worker count, CPU / memory capacity, and whether other users share hosts.
- For the implemented shared mode: a shared absolute path accessible on every node. For hosts without shared storage: complete T007–T008 before deployment. Verify manager / worker network connectivity for either mode.
- LAMMPS / MPI locations and versions, user permissions, and process termination behavior.
- Representative per-job CPU, memory, and normal execution duration.
- Permission to run persistent workers; if only SSH is available, revise interfaces as described above.

Local tests do not establish real worker, MPI, LAMMPS, or deployment readiness. Record external integration evidence separately in tasks.md.

## Recovery, ordering, monitoring and simulation segmentation

T013-T016 are implemented with local verification; T017 has restricted external scientific acceptance, T018 implements [durable checkpoint publication](checkpoints.md), and T019 implements [restricted queued continuation](segmentation.md) in [tasks.md](../tasks.md). Independent-job behavior remains current; application retries remain explicit.

- Implemented T013 adds durable prelaunch rejection/revocation and readiness-based assignment exclusion. Reassignment requires a persisted fence against the old launch; uncertain execution retains reservations.
- Implemented T014 adds bounded automatic infrastructure recovery after worker/host restart with confirmed termination. Surviving execution is reconciled rather than duplicated; disconnection alone never permits retry.
- Implemented T015 adds manager dispatch-time priority metadata separate from immutable job inputs. Queued order becomes descending priority, descending total CPU thread demand (MPI ranks times OpenMP threads), then FIFO, subject to all admission gates. Running work is not preempted.
- Implemented T016 adds [bounded CLI/JSON](monitoring.md) operational monitoring of jobs, transfers, worker readiness/resources, recovery and actionable alerts. Process completion and result delivery remain separate.
- Implemented T018-T019 build on the T017 contract to add a restricted sequential continuation chain for one logical simulation, not a general dependency graph. Default step_max is 100000 integration steps; each completed segment durably publishes a verified checkpoint before its successor returns to the manager queue. Each return gets a fresh FIFO position. The manager may set continuation priority separately; by default it inherits the first segment's first effective dispatch priority.

Continuation must preserve physical state, global step/time, parameter schedules and provenance. Exact continuation includes every RNG/fix/integrator state and requires a validated build/layout contract. When exact continuity is unavailable, statistically valid continuation is acceptable only with a validated recipe and predeclared scientific equivalence criteria, including dynamical and boundary behavior; segments remain parts of the same logical run and are not independent samples. Unsupported recipes fail clearly. The official [read_restart](https://docs.lammps.org/read_restart.html) and [fix langevin](https://docs.lammps.org/fix_langevin.html) documentation describe exact-restart restrictions and absent RNG-state serialization; capability must be checked against the deployed build. A crash can recover only from the latest durable checkpoint, with uncommitted work explicitly replayed rather than counted twice.

### T017 implementation status

The [explicit continuation API and external comparison drivers](continuation.md) validate statistical `ideal-abp-2d-v1` on the fingerprinted mozzarella LAMMPS 2 Apr 2025 environment, one rank/thread and a measured 2000-step plan with boundaries at 600/1400. Eight pilot pairs fixed 256 confirmatory pairs; all 48 predeclared simultaneous equivalence intervals passed. The gate rejects exact mode and all unmeasured model/build/layout/range combinations. T018 provides [durable checkpoint publication](checkpoints.md); T019 queues the measured segmented plan and the measured single-segment reference under its default maximum. Same-host worker reassignment is gated by a pinned boot identity and actual build/layout checks. This does not establish arbitrary-script, interacting-ABP, unmeasured-range or cross-host compatibility.

## Bounded artifact and service acceptance

[V006](verification/v006-large-artifacts.md) verifies the agreed 64 MiB plus
1000-small-file workload, streamed interruption/replay and actual test-process
restarts within declared RSS, disk, latency and elapsed-time gates. Input
preparation remains synchronous but now sends authenticated heartbeat-only
polls between files and read/hash chunks, applying cancellation before launch.
Blocking I/O may still delay polling. No broader transfer-performance guarantee
is implied. [V007](verification/v007-service-lifecycle.md) verifies the
[documented transient user-service lifecycle](service-lifecycle.md), protected
TLS/authentication, CLI smoke jobs, service restarts and cleanup-setting changes
without modifying immutable specifications. Neither acceptance includes a host
reboot, physical power loss, multi-host performance or production deployment.

## Parameter-sweep submission (T020)

[Parameter sweeps](parameter-sweeps.md) generate independent jobs from a complete Cartesian product and a repetition count. The implemented submit-sweep command validates before registration, uses a verified source snapshot, renders only the primary input and persists versioned provenance with each specification. Dry-run performs no persistent writes. Each repetition uses existing scheduling, reservations and retry history; submission is not atomic across jobs. Correlated axes, multiple templated files, automatic submission resume and scientific independence guarantees remain outside this feature.
