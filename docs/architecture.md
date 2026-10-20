# Architecture and requirements

## Status and scope

This is the permanent design reference for a Python / Linux dispatcher for independent LAMMPS batches. Configuration validation, CLI help / version, immutable job specifications, and the [SQLite storage API](storage.md) are implemented. See [configuration.md](configuration.md) for the actual schema and validation limits and [tasks.md](../tasks.md) for progress and verification. Explicit sequential [local batches](local-batches.md) are implemented; scheduling, remote execution and process recovery below remain future behavior, not an available service. The storage API provides atomic assignment, recorded state transitions, and evidence-gated reservation release, but does not verify processes or enforce worker capacity.

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

Provide CLI operations before considering a Web UI. Validation, submit, run-local, status and inspect commands currently exist.

## Configuration and state boundaries

Environment differences, resource limits, and policy parameters belong in configuration. Runtime facts belong in SQLite; algorithms and reliability guarantees belong in code. Simulation parameters belong in generated job specifications, rather than all being placed in central configuration.

| Information | Owner and boundary |
|---|---|
| Worker inventory | Manager configuration: identity, connection information, labels, permission to accept new work; online status is runtime state |
| Job resource defaults | Configuration: MPI ranks, threads per rank, memory, time limit; merge per-job overrides into immutable submission snapshots |
| Worker capacity | Manager and worker configuration: CPU / memory quotas and concurrency; worker detects hardware capacity and uses conservative effective limits |
| Busy-host thresholds | Configuration: CPU threshold, memory reserve, monitoring / scheduling intervals, metrics age |
| External load | Configurable admission policy; disabling CPU load checks must still retain available-memory and system safety checks |
| Scheduling | Named policy and parameters; initially FIFO plus least-loaded eligible worker; never arbitrary Python code from config |
| Storage | Mode, work / result locations, retention policy; staged mode requires implementation, not just a changed string |
| Execution | Worker configuration: LAMMPS / MPI paths, fixed argument arrays, environment; no shell command templates |
| Communication | Endpoint, identity, heartbeat / timeout settings, protected credential references |
| Retry policy | No retries by default; future policy needs count, delay, and allowed error classes |
| Simulation inputs | Job specification / generator: temperature, particle count, seed, template version, input identities |
| Runtime history | SQLite: queue state, worker, process identity, heartbeat, exit code, attempts, times, errors |
| Correctness | Code: atomic reservations, idempotent starts, restart reconciliation, process-group cancellation, validation |
| Secrets | Environment or protected credential files, never committed secret contents; config contains references only |

This table defines responsibilities, not additional accepted YAML keys. The exact currently accepted fields and examples are in [configuration.md](configuration.md), [manager.yaml](../examples/manager.yaml), and [worker.yaml](../examples/worker.yaml). Connection inventory, monitoring cadence, result retention controls, and retry delay / error classes may require future schema extensions; they are not all present today.

Manager configuration owns scheduling, storage, inventory, and job defaults. Worker configuration owns local identity, endpoint, executable locations, work root, and local limits. Submission combines defaults and overrides and preserves final resource values and input identity. Configuration changes take effect on service restart, without hot reload; they affect future submissions, never existing job specifications. When manager and worker limits conflict, use the smaller limit, also bounded by detected hardware capacity. Workers may refuse an excessive launch.

Validate schema, ranges, duplicate identities, paths, and executables with clear errors. Structural checks do not establish deployment readiness; current local path checking is explicitly opt-in and has the limits documented in configuration.md. CPU slots mean logical CPU quota, calculated as MPI ranks × threads per rank. Validate MPI / OpenMP affinity to avoid overlapping core use. Memory reservations and CPU quotas are accounting, not OS enforcement; cgroups require separate future work.

## Scheduling and resource accounting

Each scheduling round excludes paused, disabled, offline, stale-metric, and incompatible workers. Examine queued jobs in FIFO order. The first version may skip a temporarily unplaceable job to run a smaller one, but must expose its waiting reason. Long-wait fairness improvements are later work.

A worker can accept a new job only when all conditions hold:

1. CPU reservations for assigned and running jobs plus the new requirement fit the effective CPU quota.
2. Memory reservations for assigned and running jobs plus the new requirement fit the effective memory quota.
3. Latest available memory minus reservations for jobs not yet started can accommodate the new job and the configured safety reserve.
4. Assigned and running job count remains within maximum concurrency after admission.
5. If external-load accounting is enabled, recent average CPU utilization is below its threshold.

CPU utilization signals temporary load, not an exact free-core count. Running jobs keep their full reservations even during low utilization. Available memory already reflects running processes' actual consumption; do not subtract all running memory reservations from it again.

Rank eligible workers by CPU utilization and remaining quota, with a fixed, testable tie-break rule. Detailed ordering must be defined when T004 starts. Create the attempt and reservation in a database transaction before dispatch to prevent oversubscription within a round. A worker rechecks local conditions before launching and may reject an assignment for insufficient resources, returning the job to the queue.

Reject a job exceeding every worker's maximum capacity at submission, or expose an explicit unschedulable reason. Never leave it silently waiting forever.

## Lifecycle and reliability

Normal flow: `queued → assigned → running → succeeded / failed`. Also support `cancelling`, `cancelled`, and `unknown` for cancellation and loss of contact.

- Give each job a stable ID and each execution an independent attempt ID and directory. Retries preserve previous output.
- Workers durably record accepted attempts and process identities. Repeated delivery of the same attempt must never launch another execution. Combine PID with start time or supervisor identity to prevent PID-reuse errors.
- On heartbeat timeout, stop dispatching to that worker and mark existing work `unknown`. Disconnection does not establish termination and does not authorize immediate execution elsewhere.
- Reconcile after manager or worker restart. Confirm the original attempt ended or was terminated before releasing reservations or retrying. If termination cannot be established, retain `unknown` and permit manual resolution.
- Record argument arrays, working directory, start / end times, stdout, stderr, exit code, and completion evidence for each attempt. Initially exit code 0 means success; add explicit validators if required outputs or LAMMPS-specific checks become necessary.
- Persist cancellation requests before asking the worker to terminate the complete process group gracefully, escalating after timeout. Confirm exit before marking `cancelled`. Real MPI launcher descendant cleanup requires separate verification.
- Do not retry by default. Retry only explicitly permitted errors after confirming the old process has ended; input errors must not cause endless retries.
- Pause prevents new assignments while running jobs continue. Directory deletion and result cleanup require separate explicit operations. The first version does not automatically delete failed output.

## Delivery stages

The stages describe product acceptance; [tasks.md](../tasks.md) defines the implementation order and evidence. Reliability constraints apply whenever affected functionality is introduced.

| Stage | Work and acceptance |
|---|---|
| 1: Configuration and local batches | Schema, SQLite, generator, CLI, local execution. Submit a batch into separate directories, persist specifications, inspect status, and record success / nonzero exit correctly. First use fake jobs with controlled duration and exit codes, then separately run a small LAMMPS example when available. T001–T003 are implemented locally; real LAMMPS acceptance remains unverified. |
| 2: Multi-host dispatch | Registration, authentication, heartbeat, monitoring, reservations, scheduling, shared storage, real MPI arguments. Complete a batch on at least two workers; enforce CPU / memory / concurrency limits; pause, stale metrics, and high external load block admission. T004–T005 remain pending. |
| 3: Recovery and operations | Idempotency, durable local records, reconciliation, cancellation, time limits, controlled retries, deployment / service restart documentation. Manager / worker restarts, temporary disconnection, duplicate delivery, and MPI cancellation must neither duplicate jobs nor release resources prematurely; failed output remains inspectable. T006 remains pending. |
| 4: Optional extensions | If needed, staged inputs with checksums and result collection; do not present results as deliverably complete before collection finishes. Then consider Web UI, priorities, fairness, cgroup enforcement, monitoring, and alerts. |

## Deployment prerequisites

Use the defaults above for planning, but confirm these before deployment and real multi-worker acceptance:

- Worker count, CPU / memory capacity, and whether other users share hosts.
- A shared absolute path accessible on every node and manager / worker network connectivity.
- LAMMPS / MPI locations and versions, user permissions, and process termination behavior.
- Representative per-job CPU, memory, and normal execution duration.
- Permission to run persistent workers; if only SSH is available, revise interfaces as described above.

Local tests do not establish real worker, MPI, LAMMPS, or deployment readiness. Record external integration evidence separately in tasks.md.
