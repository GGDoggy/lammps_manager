# Sequential continuation runs (T019)

T021 adds a separate [engineering contract](engineering-continuation.md) for long
ideal/WCA ABP with fresh persisted random streams. The measured/scientific scope
and legacy formats described below remain unchanged; they do not limit the new
explicit engineering contract, whose scientific evidence is `not_assessed`.

The manager can queue a restricted sequence of continuation segments as one
logical simulation. Each successor passes normal priority/demand/FIFO admission;
workers never chain executions locally. [Checkpoint publication](checkpoints.md)
and required result delivery must commit before successor creation. Independent
batch submission remains unchanged. Segments require polling workers advertising
`continuation_ready`; `run-local` leaves them queued with an explicit reason.

## Released scientific scope

The legacy recipe accepts statistical `ideal-abp-2d-v1` with one MPI rank /
OpenMP thread. T017 scientific evidence covers only its fingerprinted
executable/library/CPU environment. T023 removes build allowlist admission;
other builds can execute but have `execution_evidence: not_assessed`. The measured 2000-step
reference is available as one segment with the default maximum; the measured
600 + 800 + 600 plan is available with an 800-step maximum and explicit boundaries.
The latter is an automatically queued chain using fixed measured boundaries.
The T017 reference arm already measured the single-segment case; accepting it does
not introduce a new model, renderer, step range or scientific equivalence claim.
See [T017 evidence](verification/t017-continuation.md).

The arithmetic planner supports arbitrary positive integer intervals and defaults
to `step_max=100000`: 250001 steps plan as 100000 + 100000 + 50001. **That longer
run is not scientifically released and submission rejects it.** Other
layouts, models, ranges, arbitrary LAMMPS scripts and nonzero initial steps still
fail closed. Extending them needs new state/scientific evidence; arithmetic
coverage is not scientific acceptance. Exact continuation remains unavailable.

## Assignment environment and timeout restart (T023)

Workers advertise an actual executable/library/platform fingerprint in authenticated
polls, alongside their boot identity. The first assignment atomically binds the run
to that worker ID, Linux boot ID and actual fingerprint in SQLite. Every later
segment and retry must match, including retries of segment zero. Another worker ID
is excluded even on the same boot. Every launch independently inventories the actual
build again; a change between polling and launch fails before starting a process.
A new worker session on the same boot is allowed. This is a compatibility identity,
not remote attestation. Missing inventory blocks continuation admission but does not
block ordinary independent jobs. Managers require updated workers advertising
`continuation_fingerprint` for continuation admission; older workers remain eligible
for independent jobs.

The manager considers an environment missing when its inventory heartbeat expires,
its build/boot changes, inventory fails, or its worker is disabled/removed from the
manager inventory. Pausing/draining or busy resource quotas alone do not remove an
environment. For a disconnected worker the missing interval begins when the last
inventory exceeds `manager.heartbeat_timeout_seconds`; changed environments begin
at detection. The continuous missing timer persists across manager restarts. A return
before expiry clears it. A return after an already recorded expiry does not undo reset.

After `segmentation.environment_reset_seconds` (default 3600), the manager fences the
old generation, cancels queued work, and requests cancellation of active attempts.
**Timeout alone never establishes termination.** Until all old attempts have positive
termination evidence and all their reservations are released, the parent stays
`resetting` and no new execution is scheduled. An unreachable machine with unresolved
processes can therefore keep the reset waiting beyond one hour. Parent cancellation
wins over restart.

Once termination is established, one transaction archives old segment membership,
clears runtime worker/boot/fingerprint and committed progress, increments the generation,
and queues segment zero with no predecessor checkpoint. Generation zero retains the
original job paths; later generations use `RUN.g000001.s000000` and distinct output
paths. A new assignment may bind another host/build. Original model, segment plan,
seed allocation, snapshots, attempts, checkpoints and outputs remain retained; no
simulation output is deleted. Historical jobs cannot be retried or advance the new
chain. This is a fresh full simulation using the original initial conditions and
seeds, not a checkpoint migration. Existing chains adopt their first recorded
checkpoint environment during database upgrade without rewriting snapshots.

## Configuration and CLI

Manager YAML accepts:

```yaml
segmentation:
  step_max: 100000
  environment_reset_seconds: 3600
```

`step_max` is a positive integer that defaults to 100000 and is snapshotted per run. The submitter
may supply a per-run maximum. `environment_reset_seconds` is a positive finite number of seconds,
applied as current manager recovery policy to existing runs; it does not rewrite
run or job snapshots. A plan has at most 100000 segments and all global steps fit signed
SQLite integers. Segmentation is available only through an explicit recipe, not
by modifying a source script's `run` commands.

A `run.json` for the currently released segmented case is:

```json
{
  "run_id": "my-abp-run",
  "total_steps": 2000,
  "step_max": 800,
  "segment_ends": [600, 1400, 2000],
  "recipe": {"name": "ideal-abp-2d-v1", "mode": "statistical"},
  "resource_overrides": {"mpi_ranks": 1, "omp_threads_per_rank": 1},
  "initial_priority": 10,
  "continuation_priority": -2
}
```

Omit `step_max` and `segment_ends` for the measured 2000-step single-segment case.
Supply resource overrides when manager defaults are not singleton. `build_fingerprint` is optional; if supplied, it remains an immutable submission
hint for replay/provenance, not an execution constraint. The actual binding is
established after assignment from authenticated worker inventory. IDs are optional; retrying an identical explicit run ID is an
idempotent submission. A different immutable snapshot under that ID is rejected.

```bash
PYTHONPATH=src python3 -m lammps_manager submit-run --config manager.yaml run.json
PYTHONPATH=src python3 -m lammps_manager inspect-run --config manager.yaml my-abp-run --limit 100
PYTHONPATH=src python3 -m lammps_manager run-priority --config manager.yaml my-abp-run --continuation 5
PYTHONPATH=src python3 -m lammps_manager cancel-run --config manager.yaml my-abp-run
```

`inspect-run --after ORDINAL --limit N` pages segment history (N=1..500). It also
returns bounded recent parent events, immutable snapshot, parent state/reason,
committed steps, runtime `environment`, `execution_evidence`, and the latest retired
segment `history` (up to the same limit; the active segment cursor does not page retired
generations). Compact job monitoring includes environment generation, missing-since
time and reset-pending state. Existing `status` / `inspect` job monitoring exposes a compact
`continuation` object: parent total/committed steps, segment/range, maximum, mode,
checkpoint identity/step/health, blocking reason and next-segment eligibility.
Checkpoint failure diagnostics persist separately from process/result state.
Both shared and staged continuation segments publish verified result trees under
`ATTEMPT/results/`; all result files and directory ancestry are synced before the
parent can count a completed boundary. Shared raw output also remains retained.

## Atomic completion, priority and recovery

Run-level SQLite metadata freezes physical settings, global start/total steps,
maximum, renderer/layout, resource and per-segment recovery policies and namespaced
stream provenance. Immutable segment specifications include generated input hashes
and the exact parent checkpoint files. RNG seeds derive from the documented T017
SHA-256 namespace/phase/arm/replica/segment policy; collisions fail submission.
Each resume uses the fixed recipe, restores physical state and global numbering,
and recreates only the audited Brownian/self-propulsion fixes. It does not create
atoms, reinitialize velocities or repeat setup/equilibration. No parameter ramp
or untracked external state is supported.

Completion first verifies durable checkpoint and result state, then publishes
complete successor inputs, then uses one SQLite transaction to count the boundary,
create one successor job/dependency, and append a new FIFO queue entry. A crash
between filesystem publication and DB commit leaves a retained verified tree;
replay verifies it and commits exactly one successor. A failed transfer does not
rerun a completed segment. Reservations still release only on confirmed process
termination, independently of checkpoint and result acknowledgement.

The first dispatch records its effective priority once, including a queued T015
job override or manager default. Absent a continuation override, every successor
inherits that recorded value. `run-priority --initial` is allowed only before the
first dispatch; `--continuation` changes future/queued successors without affecting
running assignments. Explicit changes and assignment priorities are audited outside
immutable specifications. Every return competes by priority descending, total
MPI-times-OpenMP demand descending, then its fresh FIFO position.

Only one segment of a run may hold a reservation. Unknown execution retains it;
disconnection alone cannot create a successor. T014 infrastructure recovery and
explicit application retries operate within each segment's immutable budgets.
A retry reuses that segment's last committed input checkpoint and seed provenance,
preserves earlier attempts/output, and replays uncommitted computation without
counting it twice. A missing/corrupt checkpoint or unsafe input blocks progress with
a reason. Parent cancellation persists intent before process-group cancellation,
cancels queued work and prevents all future successor creation and retries. The
parent remains cancelling until unresolved reservations are conclusively released.
The parent succeeds only after its final checkpoint and required results commit.

## Output and retention

Segment trajectories keep their global step numbering. Both physical boundary
frames are retained; monitoring/history explicitly mark every continuation
segment's initial frame as a duplicate. Compare physical copies before treating
them as one logical frame. Do not count segments, frames or correlated time origins
as independent statistical replicas. The explicit real driver verifies all boundary
copies and combines exactly the 101 global frames for the measured plan.

All manager checkpoint copies, segment outputs, failed attempts and launch fences
remain retained. Dependency release never deletes a checkpoint. No manager result
or checkpoint pruning operation is added. Worker staged cleanup remains subject
to both independent result and checkpoint acknowledgements.

Local tests use byte stand-ins and fake executables. The separate authorized
`tools/verification/segmentation_acceptance.py` driver uses actual polling manager,
workers, MPI/LAMMPS, shared/staged storage, a fixed worker per run and
uninterrupted reference runs; it preserves raw data and source/build inventories.
It runs three bounded scenarios (shared chain, staged bound-worker chain, default
single segment), with ten singleton process invocations in total, each limited to
30 seconds. The three new reference pairs are functional comparisons, not a new
statistical equivalence inference; T017's frozen 256-pair confirmatory evidence still covers only its original
renderer/model/build/step plan. The historical alternating-worker measurement
remains recorded, but T023 now pins worker identity during each generation.
Physical reboot, power loss, arbitrary ranges and production deployment remain
outside this evidence. See [tasks.md](../tasks.md) for actual verification records.
