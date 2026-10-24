# Engineering ABP continuation (T021)

`submit-run` supports a separate `engineering-abp-v1` contract for long
`ideal-abp-2d-v1` and `wca-abp-2d-v1` statistical runs. WCA selects this contract
by default; ideal ABP requires the explicit version. Omitting that version for
ideal ABP retains the measured T017/T019 contract, deterministic seeds, snapshot
format and duration gate. `require_validated` retains recipe/plan validation and
historical evidence lookup; T023 admits other builds as `not_assessed`.

Engineering metadata says `evidence_status: not_assessed`. This workflow preserves
physical state and renews Brownian RNG seeds; it promises neither trajectory
identity, independent PRNG streams nor scientific equivalence. The withdrawn
V009 endpoint study remains withdrawn. Default tests use fake executables and
cannot establish real LAMMPS restart acceptance.

## Submission

A WCA request for `submit-run --config manager.yaml run.json` is:

```json
{
  "run_id": "wca-long-001",
  "total_steps": 250001,
  "recipe": {"name": "wca-abp-2d-v1", "mode": "statistical", "mpi_ranks": 1, "omp_threads": 1},
  "resource_overrides": {"mpi_ranks": 1, "omp_threads_per_rank": 1}
}
```

This queues 100000 + 100000 + 50001 integration steps. Supply
`"contract_version": "engineering-abp-v1"` to select long ideal ABP. `step_max`
defaults to the manager's snapshotted `segmentation.step_max` (100000); explicit
increasing `segment_ends` may shorten any segment, including runs shorter than the
maximum. The last end must equal `total_steps`. There are at most 100000 segments;
global steps are bounded to `2^63-2`, within SQLite and the supported engine's
signed bigint clock. A segment splits engine `run` commands at `2^31-1` steps,
with at most 100000 production commands per segment. Excessive commands fail explicitly.
The checkpoint manifest endpoint allows 4 MiB for the complete contract, including
all 100000 global boundaries, without changing submitted snapshots or plan limits.
The simulation always starts at global step/time zero.

`inspect-run` reports the immutable contract, physical model, renderer hash,
layout, stream allocation and evidence status, along with ordinary parent and
segment history. Priority, cancellation, retry and monitoring commands are the
same as [T019](segmentation.md). Only one segment may retain reservations;
disconnection never creates a successor. Required checkpoints and collected
results must commit before parent progress or successor creation.

## Fixed v1 physical model and compatibility

WCA uses 256 diameter-one particles, periodic 2D square geometry at number density
0.5, a nonoverlapping square lattice and random planar unit dipoles. Shifted
Lennard-Jones interactions have cutoff `2^(1/6)`, epsilon=sigma=kBT=1,
gamma_t=10, gamma_r=1, propulsion=10, and dt=0.0001. There is no implicit
20000-step preparation. These are the defaults and the immutable v1 model; v2
allows submission-time overrides as described below. Engineering ideal ABP retains the 1024-particle random
initial ensemble in a 32 by 32 box, zero interactions and dt=0.005.

Only initialization creates atoms/orientations. Successors read the binary
restart's box, atoms, positions/images, dipoles and global timestep. Both branches
explicitly apply the recipe interactions and dt, then recreate Brownian and
propulsion fixes. No velocities, equilibration, arbitrary commands, parameter
ramps or external state are added. Every run command retains `start 0 stop TOTAL`.
Before production integration, the initial script runs a two-step restart smoke
inside the same tracked MPI session: save the initialized step-zero state, run one
step, restart and run one more, then restore the saved step-zero state and recreate
the integration fixes. No atoms are recreated. These discarded smoke steps add no
preparation or production progress; their separate dumps/restarts remain retained.
This bounded smoke exercises the actual recipe/build/fixed rank layout before a
long computation, under the attempt's normal cancellation and time-limit fences.
Successors restore only their committed production boundary.
Boundary validation checks particle count, actual MPI grid/OpenMP thread evidence
and physical time against the recipe. Its [clock rounding budget](checkpoints.md)
accounts for completed segments and split engine run commands, so ordinary
accumulation error does not block a long chain. Step/count checks remain exact.
Legacy snapshots without sampling retain both boundary copies. New runs use
[global sampling](trajectory-sampling.md) with one separate initial state and
no extra segment-start trajectory frames.

T023 removes the build allowlist. The first assignment binds actual worker, boot
and executable/library/platform fingerprint; later segments and retries must match.
Launch inventories the actual executable again. New builds remain scientifically
`not_assessed`; removing admission restrictions does not validate compatibility. The initial tracked MPI session's
bounded restart smoke exercises the actual required atom/pair/fix styles before
production integration; unsupported commands fail the attempt. Engineering layouts use positive fixed MPI ranks (up to the engine/launcher integer bound `2^31-1`)
and one OpenMP thread, with matching job resources. The local MPI launcher profile
accepts no extra MPI arguments, LAMMPS arguments or execution environment overrides.
A failed actual MPI startup remains a failed attempt, never a successful checkpoint.
Run the explicit bounded smoke below for each intended rank count/build environment;
local fake tests do not certify those layouts. New environments should undergo the explicit bounded smoke before long runs;
there is no hard-coded fingerprint allowlist.

Every checkpoint consumer must remain on the first assignment's worker ID and
pinned Linux host boot and use the same build/layout. Changed/missing boot identity blocks restoration,
even under a reused worker ID. Cross-host portability, multi-host MPI continuation,
layout changes, exact mode and arbitrary thread counts remain unavailable.
After a continuously missing environment exceeds the configurable timeout (default
one hour), the manager cancels the old generation and restarts from step zero only
after confirmed process termination, clearing its runtime fingerprint for reassignment.
See [environment binding and reset](segmentation.md#assignment-environment-and-timeout-restart-t023).

## Random allocation and durable replay

The new `fresh-random-per-segment-v1` policy draws standard-library
`secrets.randbelow` seeds in 1..899999999. Initialization and every logical segment
have distinct seeds within the run; collisions redraw. WCA reserves both setup
seed slots for a common schema although its lattice does not use the position seed.
The full allocation, policy and SHA-256 digest commit in SQLite before first input
publication. Concurrent identical submissions allocate only once. A publication
failure retains the allocation for identical run-ID replay; changed intent is
rejected before drawing entropy. Retries and reopened managers use identical input
hashes and seeds. Different runs draw fresh entropy, without a claim of mathematical
stream independence or cross-run seed uniqueness.

Each checkpoint contains a bounded versioned reference to the allocation digest,
segment ordinal and its three setup/segment seeds, plus model/renderer/evidence
metadata. It does not copy the whole run allocation. The manager verifies the
reference against its immutable stored allocation before assignment; worker and
upload paths enforce the retained authenticated contract. Legacy provenance keeps
its original derivation and serialization.

## Submission-time physical parameters (T022)

Add `model_parameters` to `run.json` to select `engineering-abp-v2` automatically
for either ideal or WCA ABP. The version can also be supplied explicitly. Omitted
fields take the recipe defaults above. Requests with no parameters/version retain
the exact previous v1 or measured-contract behavior. Custom parameters with an
explicit older version are rejected. No new manager/worker YAML keys are added.

[examples/wca-run.json](../examples/wca-run.json) is a complete editable request:

```bash
PYTHONPATH=src python3 -m lammps_manager submit-run --config manager.yaml examples/wca-run.json
```

Its optional `build_fingerprint` is a submission provenance hint; the actual
environment is inventoried and bound at assignment. Change the run ID for
a new model. Resubmitting an existing ID with different parameters is rejected.
`inspect-run` shows the complete normalized `model_parameters` and compact `model`
summary. These remain immutable across all segments, retries and manager restarts.

```json
"model_parameters": {
  "particles": 400,
  "density": 0.4,
  "initial_positions": {"mode": "square-lattice"},
  "initial_orientations": {"mode": "aligned", "angle": 1.5707963267948966},
  "epsilon": 2.0,
  "sigma": 1.0,
  "kBT": 1.0,
  "gamma_t": 10.0,
  "gamma_r": 2.0,
  "propulsion": 5.0,
  "dt": 0.00005
}
```

| Field | Meaning and accepted values |
|---|---|
| `particles` | Integer 1..1000000; booleans are rejected |
| `density` | Positive 2D number density N/area; box side is sqrt(N/density) |
| `initial_positions` | Structured square-lattice, random or explicit position specification |
| `initial_orientations` | Structured random, aligned or explicit planar unit-dipole specification |
| `epsilon` | Nonnegative LJ energy scale for WCA; ideal ABP requires zero |
| `sigma` | Positive LJ length scale, also the particle diameter; WCA cutoff is sigma * 2^(1/6) |
| `kBT` | Positive thermal energy; equals the Brownian temperature in reduced LJ units |
| `gamma_t`, `gamma_r` | Positive independent translational/rotational friction coefficients |
| `propulsion` | Nonnegative self-propulsion force magnitude |
| `dt` | Positive integration timestep; every boundary checks time = global_step * dt |

All numbers must be finite JSON numbers. Unknown fields, strings used as numbers,
invalid array lengths, duplicate/out-of-box explicit positions, invalid geometry
and nonfinite final physical time are rejected before run/seed persistence.
Positivity and bounds are input checks, not a stability or scientific-equivalence
claim for every parameter combination. No ramps or parameter changes within a run
are introduced. Previously submitted jobs are never rewritten.

Position modes:

- `{"mode":"square-lattice"}` fills exactly N cell centers, in row-major order,
  on a ceil(sqrt(N)) by ceil(sqrt(N)) square grid. Unused cells remain vacant for
  nonsquare N. Density sets box area; high densities may produce initially
  overlapping particles. No hidden minimization/preparation changes that state.
- `{"mode":"random"}` uses the persisted position seed. WCA asks LAMMPS for a
  minimum separation sigma with 1000 insertion attempts per particle; ideal ABP
  permits overlaps. A fixed count check fails the attempt if WCA insertion cannot
  place all N particles, rather than silently simulating a smaller system.
- `{"mode":"explicit","coordinates":[[1,1],[3,1],[1,3],[3,3]]}` supplies one
  `[x,y]` pair per particle (this example uses N=4). Coordinates are in box units,
  within `[0, sqrt(N/density))`; z=0. Particle IDs follow list order.

Orientation modes:

- `{"mode":"random"}` uses the persisted setup orientation seed.
- `{"mode":"aligned","angle":1.5707963267948966}` gives every particle the
  same direction. Angles are radians measured from +x toward +y; negative angles
  are allowed. Dipoles are `(cos(angle), sin(angle), 0)`.
- `{"mode":"explicit","angles":[0,1.5707963267948966,3.141592653589793,4.71238898038469]}`
  supplies one angle per particle, in the same ID order as explicit positions.

The manager generates `initial-state.lmp` from validated structured values; callers
cannot supply executable script fragments. Only the first segment includes it.
Every segment carries hashed `model-parameters.json`; the worker verifies its
manifest, full parameter digest, model summary and generated script before launch.
Checkpoint provenance contains bounded runtime physics and initializer-array
hashes, never full per-particle arrays. Large explicit arrays therefore do not
exceed the 16384-byte provenance limit. Checkpoint particle counts and clocks use
the submitted model; all existing result/termination/reservation/boot fences apply.

Implementation syntax references: LAMMPS [create_atoms](https://docs.lammps.org/create_atoms.html)
for exact single-position insertion and bounded random insertion, and
[set](https://docs.lammps.org/set.html) for planar dipoles. Runtime style/restart
checks remain under the tracked initial-session smoke. Local v2 verification uses
fake executables only; no real v2 parameter/build/layout acceptance is claimed.
The explicit external driver below retains its fixed v1 smoke scope.

## Explicit external engineering smoke

This separately invoked driver launches real MPI/LAMMPS in a new isolated root:

```bash
PYTHONPATH=src python3 tools/verification/engineering_continuation_acceptance.py \
  --lammps /absolute/path/lmp --mpi /usr/bin/mpirun.openmpi \
  --ranks 1 --root /tmp/new-engineering-smoke
```

Use only with execution authorization and an allocated host/resource budget.
It runs shared and staged chains of 40 + 60 + 80 steps, verifies the explicit
global sampling schedule and saved initial/preflight state, validates step/time
and durable outputs, restarts its isolated
manager, and forces the second launcher invocation to fail before MPI starts.
The explicit retry must preserve inputs and seed allocation. Each simulation has
a 30-second limit; the driver retains failures and reports source/output hashes,
checkpoints, process evidence and reservations. It neither changes deployed
services nor runs the withdrawn scientific study. An offline fake-engine test
exercises the driver itself; no real T021 smoke is claimed in this change.
The updated driver does not compare duplicate successor-start physical frames,
because the new output policy deliberately omits them. Historical verification
records retain their original scope; this change adds no real-build evidence.
