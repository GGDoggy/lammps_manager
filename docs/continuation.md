# Explicit continuation contracts (T017)

T017 validates one restricted **statistical** continuation contract:
`ideal-abp-2d-v1` on the fingerprinted mozzarella LAMMPS 2 Apr 2025 environment,
one MPI rank and one OpenMP thread, 2000 total steps split at 600 and 1400
or the measured uninterrupted reference as a single segment.
The [scientific protocol/results](verification/t017-continuation.md) and
[machine-readable evidence](verification/t017-continuation.json) define its scope.
No exact recipe is accepted. Unsupported builds, models, layouts, parameter changes
and segment ranges fail the capability gate. [T018 checkpoint publication](checkpoints.md) is implemented; [T019 queued continuation](segmentation.md) is implemented
for these measured plans; existing independent-job behavior and
configuration/submission snapshots are unchanged.

## Capability API

The standard-library `lammps_manager.continuation` API checks a fixed renderer,
execution environment and measured step plan, rather than rewriting arbitrary
LAMMPS scripts. It exposes `Recipe`, `validate_candidate`, `require_validated`,
`ValidatedContinuation`, `render_input`, `render_abp_input`, `recipe_digest`,
`abp_recipe_digest` and `fingerprint_build`.

```python
from pathlib import Path
from lammps_manager.continuation import Recipe, fingerprint_build, require_validated

# Inventory on the actual execution host; not a remote attestation mechanism.
identity = fingerprint_build(Path('/usr/local/bin/lmp'))
scope = require_validated(
    Recipe(name='ideal-abp-2d-v1', mode='statistical'),
    build_fingerprint=identity,
    total_steps=2000,
    segment_ends=(600, 1400, 2000),
)
```

`fingerprint_build` hashes the executable, resolved linked-library files, platform,
CPU model and architecture. A version banner alone is insufficient. The identity
covers `ldd` dependencies, not arbitrary future runtime-loaded plugins; extensions
and alternative runtime environments require their own audit. Layout is a separate
explicit recipe constraint. The launcher computes the actual identity,
enforces the returned scope, stream policy and checkpoint contract, and uses the
fixed renderer. Passing a recipe name, caller-supplied report or executable hash
alone cannot register acceptance. The built-in matrix also checks the renderer
digest, so editing the model invalidates acceptance until reviewed and revalidated.

`validate_candidate` only identifies comparison candidates; it does not grant
acceptance. Both renderers can produce bounded exploratory inputs outside the
released scope; rendering is not authorization to split a production run.
`require_validated` always rejects the NVE exact candidate and all other unmeasured
requests. It returns immutable scope/provenance metadata for the single measured
ABP case. It does not create jobs, publish checkpoints or launch simulations.

## Accepted ABP state and stream audit

The candidate is 1024 noninteracting planar active Brownian particles in a periodic
32 by 32 box, `atom_style hybrid sphere dipole`, with `pair_style zero`,
`fix integrate all brownian/sphere` and `fix active all propel/self dipole`.
Reduced temperature is 1, translational friction 10, rotational friction 1,
propulsion force 10 and fixed timestep 0.005. These are phenomenological ABP
parameters, not a no-slip hydrodynamic sphere constraint. Additional interactions,
constraints, callbacks, ramps or commands are outside this recipe.

| State | Contract and verification |
|---|---|
| Physical state | Binary restart preserves box, atoms, unwrapped positions/image information and unit planar dipole orientations. Both copies of every boundary must match before deduplication. All 53,856 output boxes were audited. |
| Integrator/ensemble | Recreate the same Brownian fix ID/style/parameters and self-propulsion fix. Overdamped positions/orientations are the dynamical state; inertial velocities are not independent state in this model. No thermostat/barostat, constraint history or analysis fix state is added. |
| Clock/schedules | Restore step/time and fixed dt; every run uses `start 0 stop 2000`. Logged time must agree with global step times dt within floating precision. No time-dependent parameter schedule is accepted. |
| RNG | Re-create the Brownian fix with a fresh, recorded seed per segment. RNG/cached generator state is not claimed to be restored. SHA-256 namespace/phase/arm/replica/segment derivation, collision rejection and sampled angular-noise repetition checks define the tested stream policy. See the verification protocol for exact derivation and limitations. |
| Rank/thread mapping | Same fingerprinted binary/library/CPU platform and singleton rank/thread layout. Every measured log confirmed one rank and one thread; T019 separately verifies reassignment between worker identities on the same pinned host boot; other physical hosts remain unvalidated. |
| External state | None. No external potentials, coupled solvers or untracked input state. |
| Output/statistics | Global numbering, one stationary initial ensemble, no renewed equilibration. Check both boundary copies, retain one logical frame; time origins/segments are never independent statistical replicas. |

Independent matched replica pairs compare the stationary spatial/orientation
histograms and harmonics, MSD and orientation correlations at one/two persistence
times, and separate before/cross/after-boundary windows. All 48 simultaneous 95%
confidence intervals lie within the predeclared 0.03 stationary-observable or 5%
normalized-dynamics margins. The protocol used eight pilot pairs to freeze 256
confirmatory pairs. This establishes equivalence at that benchmark resolution;
it does not establish path identity, interacting-ABP phase behavior or all possible
long-time observables. Numeric power assumptions and limitations are in the
[verification record](verification/t017-continuation.md).

## Exact-mode decision and version limits

The NVE candidate (`lj-nve-v1`) is a fixed 108-atom periodic FCC Lennard-Jones fluid
with `lj/cut 2.5`, density 0.8442, mass 1, initial temperature 1.44, initialization
seed 87287 and velocity-Verlet `fix nve`. Initial velocities are generated once;
resumes never recreate atoms, initialize velocities or reset the timestep.
Neighbor lists rebuild every step with atom sorting disabled.

External 100 versus 37+62+1-step comparisons found matching full-precision atomic
trajectories for every-step neighbor rebuilds, but elapsed-time equality failed:
the tested 2021 candidate reset elapsed time; the 2025 candidate differed in last
printed time bits due to floating evaluation across segments. The first
conditional-neighbor candidate also differed in force bits at a boundary. These
failures remain visible; the gate has not been loosened after measurement.
No exact extension serializing RNG/fix/cache state has been implemented. The
user-approved fallback is the measured statistical ABP contract, with the same
physical model on each side of the comparison.

LAMMPS documents [restart restrictions](https://docs.lammps.org/read_restart.html)
and omitted state/commands; see also the
[deployed 2021 restart manual](https://raw.githubusercontent.com/lammps/lammps/stable_29Sep2021_update2/doc/src/read_restart.rst),
[NVE](https://docs.lammps.org/fix_nve.html),
[Langevin](https://docs.lammps.org/fix_langevin.html),
[Brownian](https://docs.lammps.org/fix_brownian.html) and
[self-propulsion](https://docs.lammps.org/fix_propel_self.html) documentation,
consulted 2026-10-21. The current Brownian manual describes RNG serialization added
4 Jul 2026 and a rotation-scheme default change on 11 Feb 2026; neither is assumed
for the pinned 2025 binary. A newer build may support a better exact contract, but
requires its own complete state audit and actual comparison.

## Explicit external drivers

These commands launch real simulations and are separate from default `make check`.
Use an authorized host/resource allocation and a new private root:

```bash
PYTHONPATH=src python3 tools/verification/continuation_acceptance.py \
  --lammps /absolute/path/lmp --root /tmp/new-nve-comparison
PYTHONPATH=src python3 tools/verification/abp_acceptance.py \
  --lammps /absolute/path/lmp --root /tmp/new-abp-comparison
```

The NVE driver compares every full-precision atom/thermo frame and intentionally
fails its strict gate on the tested builds. The ABP driver writes its protocol
before eight pilot pairs, freezes the sample count before confirmatory runs,
rejects insufficient power at its predeclared cap, and performs the simultaneous
equivalence analysis. It retains seeds, replica measurements, commands, build and
recipe fingerprints and file hashes; every simulation has a 30-second session
limit. Failed/inconclusive runs retain output and never widen margins, add
post-hoc samples or self-register acceptance. Reports always say
`production_approved: false`; releasing a contract requires repository review.

All earlier outputs remain retained. A future scientific extension needs a new
versioned model/state/stream audit, justified observables/margins and frozen power
protocol, independent confirmatory data and an updated reviewed matrix. Merely
reusing a seed, matching a final mean or failing a difference test is insufficient.
