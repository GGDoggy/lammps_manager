# Sequential continuation runs (T019)

The manager can queue a restricted sequence of continuation segments as one
logical simulation. Each successor passes normal priority/demand/FIFO admission;
workers never chain executions locally. [Checkpoint publication](checkpoints.md)
and required result delivery must commit before successor creation. Independent
batch submission remains unchanged. Segments require polling workers advertising
`continuation_ready`; `run-local` leaves them queued with an explicit reason.

## Released scientific scope

Only statistical `ideal-abp-2d-v1`, the T017 fingerprinted executable/library/CPU
environment and one MPI rank / OpenMP thread are accepted. The measured 2000-step
reference is available as one segment with the default maximum; the measured
600 + 800 + 600 plan is available with an 800-step maximum and explicit boundaries.
The latter is an automatically queued chain using fixed measured boundaries.
The T017 reference arm already measured the single-segment case; accepting it does
not introduce a new model, renderer, step range or scientific equivalence claim.
See [T017 evidence](verification/t017-continuation.md).

The arithmetic planner supports arbitrary positive integer intervals and defaults
to `step_max=100000`: 250001 steps plan as 100000 + 100000 + 50001. **That longer
run is not scientifically released and submission rejects it.** Other builds,
layouts, models, ranges, arbitrary LAMMPS scripts and nonzero initial steps also
fail closed. Extending them needs new state/scientific evidence; arithmetic
coverage is not scientific acceptance. Exact continuation remains unavailable.

A successor can use its predecessor's worker, or another authenticated worker on
the **same pinned Linux host boot**, with the actual released executable inventory
checked again before launch. The contract records the boot identity at assignment;
every consumer must report that same boot, including a reused worker ID. A new
session on the pinned boot is allowed; a changed or missing boot identity blocks
continuation. Checkpoints without a pinned boot cannot authorize restoration.
This is verified same-host worker reassignment, not cross-host compatibility or
remote attestation. Cross-host
reassignment remains blocked even when a peer claims an equal build fingerprint.

## Configuration and CLI

Manager YAML accepts:

```yaml
segmentation:
  step_max: 100000
```

This positive integer defaults to 100000 and is snapshotted per run. The submitter
may supply a per-run maximum. Changing configuration does not rewrite run or job
snapshots. A plan has at most 100000 segments and all global steps fit signed
SQLite integers. Segmentation is available only through an explicit recipe, not
by modifying a source script's `run` commands.

A `run.json` for the currently released segmented case is:

```json
{
  "run_id": "my-abp-run",
  "total_steps": 2000,
  "step_max": 800,
  "segment_ends": [600, 1400, 2000],
  "build_fingerprint": "aa8a1af68131f97d13fd4959031c557681b46c9eee23dddef26ca75c7eae0695",
  "recipe": {"name": "ideal-abp-2d-v1", "mode": "statistical"},
  "resource_overrides": {"mpi_ranks": 1, "omp_threads_per_rank": 1},
  "initial_priority": 10,
  "continuation_priority": -2
}
```

Omit `step_max` and `segment_ends` for the measured 2000-step single-segment case.
Supply resource overrides when manager defaults are not singleton. The build must
also match the actual execution host; a matching string in JSON cannot authorize
a different binary. IDs are optional; retrying an identical explicit run ID is an
idempotent submission. A different immutable snapshot under that ID is rejected.

```bash
PYTHONPATH=src python3 -m lammps_manager submit-run --config manager.yaml run.json
PYTHONPATH=src python3 -m lammps_manager inspect-run --config manager.yaml my-abp-run --limit 100
PYTHONPATH=src python3 -m lammps_manager run-priority --config manager.yaml my-abp-run --continuation 5
PYTHONPATH=src python3 -m lammps_manager cancel-run --config manager.yaml my-abp-run
```

`inspect-run --after ORDINAL --limit N` pages segment history (N=1..500). It also
returns bounded recent parent events, immutable snapshot, parent state/reason and
committed steps. Existing `status` / `inspect` job monitoring exposes a compact
`continuation` object: parent total/committed steps, segment/range, maximum, mode,
checkpoint identity/step/health, blocking reason and next-segment eligibility.
Checkpoint failure diagnostics persist separately from process/result state.
Both shared and staged continuation segments publish verified result trees under
`ATTEMPT/results/`; all result files and directory ancestry are synced before the
parent can count a completed boundary. Shared raw output also remains retained.

## Atomic completion, priority and recovery

Run-level SQLite metadata freezes physical settings, global start/total steps,
maximum, renderer/build/layout, resource and recovery policies and namespaced
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
workers, MPI/LAMMPS, shared/staged storage, same-host worker reassignment and
uninterrupted reference runs; it preserves raw data and source/build inventories.
It runs three bounded scenarios (shared chain, staged alternating workers, default
single segment), with ten singleton process invocations in total, each limited to
30 seconds. The three new reference pairs are functional comparisons, not a new
statistical equivalence inference; the release still relies on T017's frozen
256-pair confirmatory evidence for exactly this renderer/model/build/step plan.
Physical reboot, power loss, arbitrary ranges and production deployment remain
outside this evidence. See [tasks.md](../tasks.md) for actual verification records.
