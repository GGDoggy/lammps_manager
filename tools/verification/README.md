# Explicit real-host acceptance harnesses

These scripts are outside default unittest discovery and launch real MPI/LAMMPS. Run them only with authorization for source transfer, allocated host resources, bounded test jobs, test process control and deletion of newly acknowledged worker output. They never require changes to existing services or historical simulation directories.

Prepare a fresh private directory, copy the current `src/` tree and these six Python files into it, and provide a Python 3.10+ environment with PyYAML. The verified environment used a private `.venv --without-pip` and a copy of PyYAML 6.0.3 already present in another authorized test environment; no system installation was performed. The scripts expect `/usr/bin/lmp`, `/usr/bin/mpirun.openmpi`, `openssl`, Linux procfs and `taskset`. Confirm compatible executable paths/versions before use. A fresh root is required: existing task subdirectories cause a failure rather than overwriting prior output.

Before launch, record `source-sha256.json` mapping every deployed Python path (relative to the new root) to its SHA-256. The final audit checks source identity against this manifest. For the recorded run, the local deployment utility copied `src/` without bytecode, the initial harness and the manifest via tar over host-key-verified SSH to an explicitly authorized allegro private root, then copied later harnesses individually. The preserved evidence includes every executed harness hash.

Run in the requested order from that new directory:

```bash
PYTHONPATH=src .venv/bin/python real_acceptance.py
PYTHONPATH=src .venv/bin/python -m lammps_manager status --config v008/manager.yaml > v008-status.json
PYTHONPATH=src .venv/bin/python distributed_acceptance.py
PYTHONPATH=src .venv/bin/python cpu_acceptance.py
PYTHONPATH=src .venv/bin/python restart_acceptance.py
PYTHONPATH=src .venv/bin/python final_audit.py
```

`real_acceptance.py` supplies shared helpers as well as V008. The other drivers import it from the same directory. V003–V005 restrict inherited process affinity to the first four CPUs in the caller's allocation; select the allocation with `taskset` before starting if needed. Each real job uses two MPI ranks and one OpenMP thread. Simulation memory is reserved at 0.1 GiB per job, with four-slot/1 GiB/two-job accounting ceilings for distributed cases. Reservations are not OS memory enforcement. V004's three CPU load generators share that four-CPU affinity and have independent 20-second bounds. Simulation time limits are 6–30 seconds; cancellation may take the additional graceful shutdown/escalation interval. These limits are for small acceptance examples, not throughput benchmarks.

V003 and V005 need free loopback ports 18448/18449. The drivers generate a private two-day TLS test certificate and token files in the new root; certificate verification and authentication remain enabled. Never copy tokens, credentials or private keys into repository evidence. V005 uses actual SIGINT/SIGKILL on its own manager/worker processes and faults only its own proxy. Automatic staged cleanup removes only newly generated, verified acknowledged attempt trees. All other artifacts are retained.

Each successful driver writes `vNNN.json`. Database samples are also persisted incrementally to aid investigation if a driver fails. Do not blindly rerun a failed driver or resubmit its jobs: inspect original job/attempt/journal/process records first. Driver `finally` blocks stop owned services; actual simulations remain bounded by their persisted time limit. If a worker cannot reconcile surviving execution, retain reservations and investigate original process identity; do not treat service shutdown as proof of MPI termination.

The final read-only audit checks original source hashes, every SQLite reservation table, recorded MPI sessions, test-root processes and closed listeners. Recorded evidence and limitations live in `docs/verification/v008-local-batches.md`, `v003-concurrency.md`, `v004-cpu-admission.md` and `v005-restart-transport.md`. Default `make check` does not rerun these external tests.

## T017 continuation comparisons

`continuation_acceptance.py` and `abp_acceptance.py` are standalone standard-library
drivers (neither imports `real_acceptance.py`). See [the contract and invocations](../../docs/continuation.md)
and [scientific evidence](../../docs/verification/t017-continuation.md). Both create
fresh roots, bound each singleton LAMMPS process to 30 seconds and retain raw data.
On Ctrl+C or a failed wait, the shared execution helper kills the owned process
group and reaps its direct child before propagating the interruption; output is retained.
The NVE driver performs a strict exact comparison; the ABP driver freezes pilot /
confirmatory protocol and sample count before testing equivalence. A passing raw
report does not self-authorize production splitting or other stochastic models.

## T018/T019 checkpoint and queued continuation

`checkpoint_acceptance.py` verifies shared/staged checkpoint publication and
actual restoration with six bounded singleton MPI/LAMMPS invocations.
`segmentation_acceptance.py` runs the actual polling/queue/transfer path for shared
and staged three-segment chains (one bound worker per run), plus the measured
single-segment reference at default `step_max`, and three uninterrupted references.
Its ten invocations each have a 30-second bound. Both require a new private root,
PyYAML and a compatible LAMMPS executable; pass `--lammps` and `--root`.
T017 scientific evidence covers only its original fingerprinted build; other
builds remain `not_assessed` even if these functional checks pass.
No prior output is overwritten or deleted. Staged cleanup is disabled in the
segmentation driver so every raw artifact remains available for audit. Source,
build, checkpoint, process and output inventories are retained. References are
functional checks; scientific acceptance remains T017's independent confirmatory
study, never a single-pair mean comparison. See [checkpoints](../../docs/checkpoints.md)
and [segmentation](../../docs/segmentation.md).

## V006/V007 bounded artifacts and isolated services

`artifact_acceptance.py` and its streaming `artifact_proxy.py` reuse
`real_acceptance.py`, `distributed_acceptance.py` and `restart_acceptance.py`.
Copy those helpers and current `src/` into a fresh mode-0700 private root, record
all source hashes, and use Python 3.10+ with PyYAML 6.x. V006 expects the existing
real executable paths `/usr/bin/lmp` and `/usr/bin/mpirun.openmpi`. It uses only
private loopback TLS ports 18448/18449; establish that they are free first.

The user-approved workload is one 64 MiB file and 1000 4096-byte files, plus a
small LAMMPS input and bounded execution artifacts. The work tree becomes the
result workload. Gates are 2 GiB total test-root disk, 512 MiB RSS per process,
900 seconds overall, five seconds per successful poll and ten seconds between
successful authenticated polls, including injected service restart gaps.
These are this workload's acceptance gates, not a universal throughput promise.
The driver persists its budget before generation, samples RSS/disk every 0.2
seconds, streams hashes and proxy traffic in 64 KiB chunks, rejects the same
input under a smaller file limit before submission, interrupts both transfer
directions, restarts actual uploader/manager processes, verifies original
attempt identity and all manifest hashes, and enables acknowledged cleanup.
RSS/disk are sampled peaks rather than hard OS enforcement; individual
blocking I/O calls can still exceed heartbeat budgets. A failed gate must remain
a failed acceptance. Raw samples, transfer events and private outputs remain.

Run `PYTHONPATH=src .venv/bin/python artifact_acceptance.py` from that root.
A failed driver must be inspected before any new run: check its original
attempt/journal, reservations, process sessions and listener ownership. Do not
blindly resubmit. Source or driver fixes need new source hashes and a fresh root;
retain prior failures as evidence. V006 protects startup as well as execution:
proxy readiness or later startup failures trigger cleanup of already returned
service handles, and a fault-reset or stop error does not skip other cleanup.
The shared manager startup helper owns its process until readiness succeeds;
a readiness timeout, early exit or interruption triggers cleanup before the
helper propagates the failure, even though its caller has no process handle yet.

After V006 passes, `service_acceptance.py` verifies actual transient systemd
user units following [service-lifecycle.md](../../docs/service-lifecycle.md).
Use another fresh root with its helper drivers, current source and PyYAML;
the user bus must already be available. Run
`PYTHONPATH=src .venv/bin/python service_acceptance.py`. No unit installation,
host reboot, system package change, existing-service restart or production
configuration change is needed. It validates both configurations through CLI,
starts dedicated TLS services, rejects untrusted certificates/invalid tokens,
executes two bounded real smoke jobs, restarts named units, toggles staged
cleanup through YAML, compares exact submitted SQLite specification strings,
checks results/session/reservation evidence and stops its units. It retains
sanitized logs and machine-readable `v007.json`. A prepared driver is not proof
of successful real-host acceptance.

V007 includes manager startup in its named-unit cleanup region, so a
`systemd-run` timeout or interruption also attempts to stop the test units.
Configuration validation runs before that region and does not start services.

After a successful driver, run `PYTHONPATH=src .venv/bin/python lifecycle_audit.py PRIVATE_ROOT`
for a separate read-only source/result/session/reservation/listener audit. Include
the auditor in the recorded source manifest or record its identity separately.
The auditor opens manager databases read-only and requires every recorded attempt
to have a collected result manifest. It compares each complete publication tree
with that manifest's file set, byte sizes and SHA-256 hashes; missing trees,
unexpected publications and unsafe artifacts fail the audit. Run it only after
all attempts have completed result collection. Earlier audit JSON records produced
before this comparison was added are hash inventories, not independent manifest
comparisons; retain them as historical evidence rather than relabeling them.
Persistent acceptance: [V006](../../docs/verification/v006-large-artifacts.md),
[V007](../../docs/verification/v007-service-lifecycle.md).

Use `--output NEW_FILE.json` with `lifecycle_audit.py` when auditing historical
roots: it creates that file exclusively and preserves the historical audit.

## V009 remaining checks without host reboot

`sweep_acceptance.py` executes 24 shared and 24 staged real LJ jobs using
Cartesian parameters and independent repetitions. `sweep_audit.py` reads those
original roots without resubmitting jobs. `infrastructure_acceptance.py` tests
shared/staged process-only recovery, durable delay through manager process
restart, budget exhaustion and cancellation; it signals only its own isolated
worker and MPI process groups. It never reboots a machine. These scripts reuse
the existing helpers and executable prerequisites and require fresh roots.

`wca_acceptance.py` is a separate experimental scientific driver requiring the
fingerprinted 2025 Brownian-capable LAMMPS build. Copy
`docs/verification/v009-wca-protocol.md` beside the driver, plus its
`abp_acceptance.py` and `continuation_acceptance.py` helpers and current `src`.
Run with `--lammps /absolute/lmp --root NEW_ROOT --pilot-only` for all pilot
strata. This does not release WCA continuation. See
[V009 progress and actual evidence](../../docs/verification/v009-remaining.md)
and the [predeclared protocol](../../docs/verification/v009-wca-protocol.md).


## V009 unattended plans and stopping

`unattended_run.py` runs finite argv/timeout steps from `plan.json` inside a
unique expiring systemd user service. Configure `RuntimeMaxSec`,
`TimeoutStopSec`, `KillMode=control-group`, `SendSIGKILL=yes`, `Restart=no` and
CPU affinity. Use the same script with `--finalize ROOT` as `ExecStopPost`;
only its post-cleanup `status.json` establishes verified stopping. Never run
its finalizer in a shared service cgroup. Actual CPU-controller delegation
must be checked rather than inferred from accepted unit properties.

`wca_frozen_check.py` gates reconstructed pilot raw evidence against the saved
protocol, build and exact frozen sample table. `wca_pool.py --defer-analysis
--enforce-rank-affinity` collects the fixed measurements and stops without
scientific acceptance. Optional `--reuse-completed OLD_ROOT` requires complete,
compatible, hash-verified raw chunks, never interrupted/missing directories.
Preserve originating boot identities for reused evidence.

The pool passes its selected `--lammps` executable to the affinity guard. For
standalone guard use, run `wca_affinity_guard.py ROOT --lammps /absolute/lmp`;
the guard matches `/proc/PID/exe` file identity, including symlinked installations,
and only adjusts ranks within active chunk directories.

See the [current frozen plan and actual stop probes](../../docs/verification/v009-unattended.md).
`unattended_status.py LAUNCH_JSON` is read-only and distinguishes final cleanup
markers from live/stale progress. The user will resume result analysis after
these bounded runs; no production capability is released by launch.


Full WCA auditing now requires all 23 declared endpoints in both arms and
reproduces them from retained raw trajectories/thermo and frozen input/stream
identities. `wca_audit.py --completed-only --recompute` performs partial raw
checks without acceptance; a full audit always recomputes. When the pool used
`--defer-analysis`, pass distinct exclusive `--analysis-output NEW_ANALYSIS.json`
and `--output NEW_AUDIT.json` after all measurements and processes have ended.
Run the stricter auditor from a separate recorded bundle: never edit sources
inside a live frozen measurement root. New analyzer/auditor hashes are recorded
separately from original execution hashes. See V009's exact prepared command.


`wca_seed_audit.py --pilot-report FROZEN_V2.json --output NEW_FILE.json` is an
offline-only base/processor-rank seed audit. It reproduces the frozen v2 ordinal
mapping, rejects unsupported layouts/duplicate strata and never overwrites an
existing output. Its literal-seed collision and range checks are conditional
on the pinned upstream implementation; they do not prove PRNG independence or
attest deployed binary source. Versioned manual/source links and exact evidence
are recorded in the V009 unattended verification record.

`crosshost_acceptance.py` publishes a failed report and exits nonzero if
remote worker cleanup fails after successful transfer verification. It still
stops the local manager and retains the cleanup diagnostic. When execution
already failed, cleanup diagnostics supplement the original error without
replacing it.
Remote cleanup requires both `systemctl stop` and the bounded `systemctl show`
query to succeed, with explicit `inactive`/`failed` state, `MainPID=0` and no
remaining cgroup processes. Missing properties or an unavailable user bus leave
stopping unconfirmed and fail acceptance.

## T021 engineering continuation

`engineering_continuation_acceptance.py` is an explicit real MPI/LAMMPS smoke for
versioned WCA continuation, separate from the withdrawn V009 equivalence study.
See [procedure and limits](../../docs/engineering-continuation.md#explicit-external-engineering-smoke).
Default tests exercise its actual shared/staged restart/retry driver with fake
executables only; they do not establish real build/layout or scientific acceptance.
