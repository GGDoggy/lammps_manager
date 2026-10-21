# Recovery and operations

T006 adds Linux identity reconciliation, durable cancellation, time limits and explicit bounded retries to shared-mode execution. T008 applies the same process identity checks to staged attempts after a durable launch claim; see [staged results](staged-results.md). Default tests use fake processes and loopback services. See [tasks.md](../tasks.md) for actual local and real MPI verification; this document does not establish cross-host MPI daemon cleanup.

## Cancellation and retry

On the manager host, using its local database:

```bash
PYTHONPATH=src python3 -m lammps_manager cancel --config manager.yaml JOB_ID
PYTHONPATH=src python3 -m lammps_manager inspect --config manager.yaml JOB_ID
PYTHONPATH=src python3 -m lammps_manager retry --config manager.yaml JOB_ID
```

`cancel` requires an existing assignment. It persists cancellation intent and `cancelling` before any worker delivery; a queued successor with confirmed prior termination is cancelled directly. Repeated requests are safe; a disconnected worker leaves the request pending and the reservation intact. Cancellation is delivered even when the worker is paused. Normal completion racing cancellation can still report success or failure. The local executor reads cancellation from its database directly, without a service. Queued jobs without attempts cannot currently be cancelled.

The executor enforces the submitted `time_limit_seconds`, including after recovery when a launch timestamp is available. Time-limit expiry persists cancellation with reason `Time limit exceeded`; confirmed cleanup yields `cancelled`. TERM targets all process groups in the dedicated launch session; after two seconds it escalates to KILL and allows two more seconds for termination. MPI-created rank groups in the same session are included. A remaining live process or inspection/signaling error retains `unknown` and reservations. Zombies have already terminated and do not count as live execution. All affected processes must be visible in `/proc`; process escape into another session, remote MPI daemons and multi-node MPI require additional containment and separate verification. This executor launches single-host MPI jobs.

Application-error retries are never automatic. T014 infrastructure recovery is described below. `retry` requires a failed job with confirmed termination and remaining `max_retries` from its immutable submitted resources (default zero). It atomically returns the job to `queued`; scheduling creates a new attempt ID, directory and reservation. Previous attempts, output and events remain available. Changing configuration cannot increase an existing job's retry budget. Cancellation, success and unresolved execution cannot be retried. Fix input errors by submitting corrected input as a new job, rather than repeatedly consuming retries.

## Disconnection and restart

The manager periodically checks durable authenticated heartbeat times while serving, including when no workers poll. A newly assigned attempt without a heartbeat receives the configured timeout from its assignment time. At expiry, assigned/running/cancelling attempts become `unknown`; resources remain reserved. Startup also checks expiration. No attempt is reassigned to another worker just because transport failed. Reconnection reports and repeated delivery reconcile the same attempt identity.

The worker journal lock, immutable specification and exclusive attempt directory prevent duplicate launches. Restart the worker with its original state directory, identity, manager endpoint and work root. Before new launches it reconciles every unresolved accepted attempt, comparing `process.json` with Linux boot ID, PID, start ticks, process group and session. Never replace a journal to bypass unresolved reservations. Shared directories must be trusted; do not edit process metadata or job snapshots.

A matching live leader is monitored without relaunching. Cancellation and time limits also apply to recovered live leaders. After a local executor restart, `run-local` stays active and repeatedly reconciles all positively identified local processes, enforcing future time limits and cancellation requests until termination or identity uncertainty. Confirmed termination permits queued local work to continue; uncertain identities retain reservations and block new launches. Recovered attempts are not included in the returned list of newly executed job IDs. A changed boot ID or absence of all live launch-session members confirms termination. Since a restarted worker cannot recover a lost `wait()` exit code, it records `failed` with an explicit unavailable-exit-status reason, or `cancelled` if cancellation was pending. It never infers success from process disappearance. Report receipt times on the manager are not precise worker execution timestamps.

Missing/corrupt metadata, a reused PID with surviving session members, inaccessible process information, and a crash between durable acceptance and recording the launch identity remain conservative `unknown`. If the leader has disappeared while live descendants remain, automatic signaling is refused because identity cannot be safely re-established. Inspect process metadata, output and the full original session manually; only after verifying termination may an administrator use the low-level Store transition API with explicit evidence. There is no force-release CLI. No launch-window recovery path starts the same accepted attempt again.

## Deployment and upgrade

Use a dedicated service account and private local state/database directories. Keep SQLite on each host's local disk, and keep all attempt output. Use foreground `serve` and `worker` commands from [workers.md](workers.md) under a service supervisor such as systemd with `Restart=on-failure`. A worker unit should use `KillMode=process` so restarting the polling process does not silently kill unrelated running simulations; recovery needs their original journal. Do not use unit stop as job cancellation. Submit `cancel`, wait for confirmed terminal state and inspect reservations before maintenance that must stop simulation work. Graceful worker close waits for its executor, which may take the remaining submitted time limit.

For upgrades, pause new dispatch, preserve journals and credentials, stop services without deleting simulation output, and back up local SQLite databases with SQLite's backup API. Opening schema version 1 atomically migrates attempts to version 2, removing the one-attempt-per-job constraint while preserving IDs, reservations, history, transfer records and the exact submitted JSON. Foreign-key checks run before migration commits and enforcement is re-enabled on the connection. Do not run older binaries against migrated databases. Keep manager and workers on the same release; older workers do not implement cancellation delivery or T006 reconciliation. Upgrade/restart workers using their original state directories, then restart the manager and resume after inspection.

SSH maintenance credentials are distinct from per-worker API bearer tokens. The supplied remote login is stored in ignored `remote.local.yaml` with mode 0600; this is a local maintenance record, not an accepted manager/worker configuration file. Do not print it, commit it, or pass its password on command lines. Verify ignore rules with `git check-ignore remote.local.yaml`.

## Verification scope

The default suite remains offline from real MPI and remote hosts. An explicit local Open MPI 4.1.2 check separately verified cancellation and time limits with two TERM-resistant fake simulation ranks, each in its own group within the launcher session. Cleanup left no live session members, released reservations only after termination, and preserved output. After explicit source-upload authorization, isolated remote tests on allegro verified both cancellation and submitted time limits with Open MPI 4.1.2 and LAMMPS 29 Sep 2021 Update 2, using two ranks and 108 atoms. Read-only follow-up checks confirmed terminal state, zero reservations, no live session members, preserved artifacts and source hashes matching the current implementation. See [remote evidence](verification/t006-remote.json) and the T006 record in [tasks.md](../tasks.md). These checks establish single-host MPI cleanup on the remote machine; full manager/remote-worker dispatch remains T009 and multi-node MPI remains unverified.

## Staged worker disk reclamation

T012 cleans owned staged worker attempt trees after confirmed termination and durable manager result acknowledgement, including failed/cancelled attempts. Process evidence, manifests, acknowledgements and launch fences remain in the local journal after deletion; report replay and duplicate assignments do not read deleted files or relaunch attempts. Permission failures, partial deletion and interrupted tombstone writes retry on later bounded background passes. Legacy directories without ownership records remain retained. Set worker `cleanup_staged_files: false` and restart when local copies must be preserved; pause alone does not stop cleanup. See [staged cleanup](staged-results.md#worker-staged-cleanup-t012) for ownership boundaries, diagnostics and operator recovery.

## Automatic infrastructure recovery (T014)

New submissions default to one infrastructure recovery after a 30-second delay;
[configuration](configuration.md#infrastructure-recovery-policy-t014) describes
validation and disabling it. Original immutable inputs are replayed in a new
attempt directory. Computation since the start may repeat; no checkpoint or
committed-progress claim is made. Checkpoint continuation requires T018-T019.
Older specifications retain zero automatic recovery without changing stored JSON.

Each upgraded worker reports an authenticated Linux boot UUID and a fresh process
session UUID with `recovery_protocol: 1`. SQLite retains generation history;
changed generations, missing heartbeats and an absent process leader alone never
establish termination. A process-only restart monitors positively identified live
execution without launching another copy. Missing/corrupt identity or surviving
unidentified descendants keeps the attempt unknown and resources reserved.

Reconciliation can record infrastructure interruption only after a validated
saved process identity establishes either a different Linux boot or disappearance
of the entire dedicated launch session. The local terminal transition and its
structured evidence are one transaction. Authenticated report processing binds
that evidence, checks cancellation and the global per-job recovery budget, and
atomically records a deadline and requeues once. Duplicate reports, response loss
and manager restart retain the same deadline and budget charge. An exhausted
budget leaves a failed job with an actionable reason. Application-error retries
remain explicit and use `max_retries`; their audit/count is separate.

For shared local execution, `run-local` makes the recovery decision in the same
transaction as confirmed termination and uses the same submitted budget. It also
finishes previously detected interruptions belonging to its configured worker.
A delayed recovery remains queued when the command exits; invoke `run-local`
again after the recorded deadline to execute its new attempt. Cancellation during
that delay prevents execution, and repeated invocations retain the original
deadline and budget charge.

Both the scheduler and `run-local` respect recorded recovery deadlines. Readiness,
resource quotas and normal placement gates still apply after the delay. Workers
mirror an authenticated successor assignment only after confirming prior local
termination; only the manager counts the global recovery budget because a worker
may see only part of the job's history. Cancel during the delay prevents dispatch;
late interruption reports cannot reactivate it. Succeeded/cancelled attempts
cannot qualify for infrastructure recovery.

`status` and `inspect` JSON include `infrastructure_recovery` with frozen policy,
used recovery count and latest decision (`status`, `retry_at`, `reason`, evidence).
`inspect` retains each attempt's interruption and prelaunch rejection records,
events and artifact locations. Process outcome and staged result collection stay
separate. Retrying infrastructure failure never deletes previous output, and
old staged attempts continue collection until durable acknowledgement.

Protocol changes are additive. Legacy workers receive specification keys they
understand but cannot report structured infrastructure evidence; their execution
is not automatically recovered from a timeout. Upgraded workers retain their
legacy journal snapshots, including any omitted policy, while the manager's
immutable snapshot remains authoritative for recovery. Legacy shared acceptance
without launch claims stays conservative. New-protocol acceptance without a
consumed launch claim can use the T013 permanent prelaunch fence after restart.

Verification uses fake identities, local process trees and loopback services.
This does not establish physical reboot, power-loss durability, real MPI cleanup
or deployment readiness. See the separate [external test plan](verification/t014-restart-plan.md).
