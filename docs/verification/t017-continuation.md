# T017 continuation verification

Verified on 2026-10-21 (Asia/Taipei). The restricted statistical ideal-ABP contract
is accepted; exact candidates remain rejected. See [the contract](../continuation.md)
and [machine-readable evidence](t017-continuation.json).

## Authorization and isolation

The user authorized T017, selected active Brownian particles as the representative
stochastic workload, and explicitly authorized source transfer and bounded singleton
LAMMPS tests on allegro and mozzarella. Host-key-verified SSH used existing protected
credential helpers; credentials are not included in evidence. All test roots are
new mode-0700 directories under `/tmp`; historical simulations and deployments are
untouched. Each LAMMPS process has `OMP_NUM_THREADS=1`, a separate process session
and a 30-second timeout. Raw inputs, logs, dumps and binary restarts are retained.

The standalone scientific drivers live in `tools/verification/`, outside default
unittest discovery. They use Python's standard library and argument arrays.

## Build and failed exact candidates

| Host/build | Candidate/result |
|---|---|
| allegro, `/usr/bin/lmp`, 29 Sep 2021 Update 2 | Initial NVE with conditional neighbor rebuilds failed at step 37: a force differed in its last printed bits. Required ABP fixes are absent. |
| allegro, same build, every-step neighbor rebuild | All 101 atom snapshots matched for 100 versus 37+62+1 steps; adding the full clock audit found elapsed time resetting to zero after restart. This is not an accepted exact contract. |
| mozzarella, `/usr/local/bin/lmp`, 2 Apr 2025 | Same NVE atom snapshots matched; strict thermo equality failed on elapsed-time floating evaluation (e.g. 0.20500000000000002 versus 0.20499999999999999 at step 41). The exact candidate remains rejected rather than relaxing the gate after measurement. |

Test roots, original reports, commands and source/build hashes will be retained in
the machine-readable evidence. A checkpoint preserving positions and velocities
alone is insufficient for general exact continuation. No RNG serialization
extension has been implemented; no alternate physical model is substituted for an
exact stochastic simulation.

## Predeclared ideal-ABP model

The user-selected representative is a fixed **noninteracting** two-dimensional ABP
model, not an interacting suspension or a phase-separation model. There are 1024
independent particles in a periodic 32 by 32 box. Sphere diameter 1 is an orientation
carrier; `pair_style zero` introduces no excluded-volume interaction. Commands use
`atom_style hybrid sphere dipole`, `fix brownian/sphere` with Gaussian noise,
`fix propel/self dipole 10`, temperature 1, translational friction 10 and rotational
friction 1. Thus translational diffusion is 0.1, rotational diffusion is 1 and
self-propulsion speed is 1 in reduced units. The timestep is 0.005.

Each reference runs 2000 steps (time 10) without interruption. Each continuation
runs 600 + 800 + 600 steps, using separate OS processes and binary restart files.
Positions/image counters, dipole orientations, global step/time and constant
physical parameters are preserved. The Brownian fix is re-created with a fresh
stream at each boundary. RNG state and cached Gaussian values are deliberately
not claimed to be restored; this is statistical continuation of a Markov model,
not an exact path restart. There are no ramps, adaptive timesteps, constraints,
external coupling or history-bearing analysis fixes. Any addition needs a new
recipe and scientific validation.

Initial positions and planar orientations are uniform, independently initialized
once per replica; this is the exact stationary distribution of this noninteracting
periodic model. No equilibration reset is needed or permitted. Reference and
segmented arms have matched initial states and independent stochastic streams;
pairs are independent of other pairs. A fresh stream derives from SHA-256 of
`[namespace, phase, arm, replica, segment]`, reduced into 1..899999999. All derived
seeds (including setup seeds) must be distinct. Pilot and confirmatory namespaces
are disjoint. Duplicate seeds or repeated sampled angular-increment fingerprints
fail the harness. Distinct seeds and this finite diagnostic do not mathematically
prove global PRNG subsequence disjointness; they specify and check this bounded
LAMMPS recipe's stream-renewal policy.

The current Brownian manual documents RNG restart serialization added on
4 Jul 2026 and a rotational-scheme default change on 11 Feb 2026. Neither is
assumed for the pinned 2 Apr 2025 binary. A newer build must be audited and tested
separately; current online documentation does not certify an older executable.

## Frozen scientific endpoints and margins

The exact protocol is written to `protocol.json` **before** the pilot. Eight
independent matched pilot pairs estimate variances. Before confirmatory launches,
`frozen-sample-size.json` records the chosen count: at least 64, at most 256 pairs.
The required count is the maximum across all endpoints of
`ceil(4 * pilot_variance * (z_(1-.05/(2*m)) + z_.90)^2 / margin^2)`.
The factor four is a conservative design inflation, not a proven variance bound.
The normal-approximation .90 power target applies to each one-sided constraint
under zero true difference with the inflated pilot variance; it is not a promise
of 90% family-wise equivalence power. The two-constraint lower bound under that
approximation is 80%; the actual equivalence decision depends on simultaneous
confidence intervals, not a nonsignificant difference or a power estimate.

There are 48 predeclared endpoints:

- Stationary distributions: first/second orientation sine/cosine harmonics,
  first spatial sine/cosine harmonics in both axes, eight angle bins and sixteen
  joint position bins. The margin is 0.03 in harmonic expectation or bin
  probability: a declared 3-percentage-point resolution for this ideal-gas
  benchmark, not a tolerance for detecting phase boundaries or high harmonics.
- Dynamics: MSD and orientation autocorrelation at lags 1 and 2, normalized by
  their noninteracting continuum values. The margin is 0.05 of those values.
  These lags span one and two persistence times; an autocorrelation change of
  5% at lag 2 corresponds to roughly a 2.5% change in rotational diffusion.
  This is a benchmark sensitivity requirement, not a universal scientific margin.
- Boundary transients: separately at both boundaries, MSD and orientation
  correlation for a preceding 0.1-time window, a crossing 0.2-time window and
  a following 0.1-time window. The same relative 5% bound constrains localized
  restart artifacts rather than letting a long trajectory average hide them.

For reference, the model has orientation correlation `exp(-t)` and unwrapped MSD
`0.4*t + 2*(t - 1 + exp(-t))`. These formulas set physical scales for the margin;
finite-timestep integrator bias is shared by both arms and is not removed by this
comparison. The fixed friction values define the chosen phenomenological ABP;
they do not impose the no-slip Stokes-Einstein friction relation for diameter 1. See the analytical treatment by [ten Hagen et al.](https://arxiv.org/abs/1005.1343),
and LAMMPS's [Brownian integration](https://docs.lammps.org/fix_brownian.html) and
[self-propulsion](https://docs.lammps.org/fix_propel_self.html) documentation.

Each observable first averages frames, correlated time origins and particles
**within** a replica. Inference uses the independent paired replica differences;
frames and segments never increase the statistical sample count. Simultaneous
95% two-sided Student intervals use Bonferroni correction across all 48 endpoints.
Every interval must lie strictly inside its predeclared margin. The Student
approximation applies to independent replica averages (large particle and replica
counts); no assumption of independent consecutive frames is made.

All global output steps must be present. Both physical boundary frames must match
before deduplication into one logical frame. Direction must remain planar and unit
length; matched initial conditions, clock continuity, duplicate streams and build
identity are separate mandatory checks. Pilot replicas never enter confirmatory
confidence intervals. Failure/inconclusive results do not trigger margin changes,
extra samples or automatic acceptance.

## Results

All 48 predeclared equivalence endpoints passed for the frozen 256 confirmatory
pairs (512 logical trajectories), separately from the eight pilot pairs. There were 1056 bounded
LAMMPS process invocations in total: one uninterrupted and three segmented
processes per pair. Every invocation exited 0. A Student critical value of
3.3172025276783756 gives simultaneous family-wise 95% intervals across the 48
endpoints. Selected normalized segmented-minus-reference intervals:

| Endpoint | Simultaneous interval | Required margin |
|---|---|---|
| MSD, lag 1 | [-0.00204027, 0.00235117] | +/-0.05 |
| MSD, lag 2 | [-0.00280678, 0.00313027] | +/-0.05 |
| Orientation ACF, lag 1 | [-0.00213578, 0.00494327] | +/-0.05 |
| Orientation ACF, lag 2 | [-0.01197455, 0.01767665] | +/-0.05 |

The furthest stationary interval endpoint was 0.00361584 (margin 0.03); the
furthest boundary-window endpoint was 0.01479353 (margin 0.05). The largest
fraction of any permitted margin was 0.353533, for lag-2 orientation correlation.
Thus acceptance is based on intervals contained inside margins, not merely a
failure to reject a difference. This is statistical equivalence at the declared
resolution, never bitwise identity or exact RNG continuity.

As a descriptive integrator check, reference means divided by continuum values
were 1.00364 / 1.00676 for MSD at lags 1 / 2, and 1.01887 / 1.03917 for orientation
ACF. The latter exposes finite-timestep rotational integration bias in this build;
segmentation equivalence does not establish timestep convergence or remove that
bias. These descriptive means are not post-hoc acceptance endpoints.

The retained remote root is
`/tmp/lammps-t017-abp-validation-zLtoYRZt` on mozzarella. The final read-only audit
rehashed all 6339 manifest files (4,876,587,426 bytes), checked all 53,856 output box
frames and confirmed every log used one MPI rank and one OpenMP thread. All owned
execution sessions and test-root processes were absent. No output was deleted.
The executable/library/CPU fingerprint was unchanged across the recorded runtime
inventories:
`aa8a1af68131f97d13fd4959031c557681b46c9eee23dddef26ca75c7eae0695`.
The initial library/CPU inventory was taken during the run, after pilot completion;
the executable was hashed before simulations. Do not claim a library-upgrade
fault-injection or power-loss test from this inventory comparison.

Local recomputation verified all 1056 actual input hashes against the final
renderer, all 1584 distinct derived setup/stream seeds, the pilot sample-size
decision and the full confirmatory analysis. Reported replica-level measurements,
all seeds, commands, original source hashes, raw file manifests and exact-mode
failures are retained in the JSON evidence; full trajectories/restarts remain in
the private remote roots. A separate fresh capability-audit root
`/tmp/lammps-t017-capability-7ZYD4Fkc` tested the final API against the actual build:
the measured scope was accepted and an unknown build rejected, without launching
another simulation. The final API/driver adds environment fingerprints and box
checks to the original scientific driver; its renderer hashes and recomputed
analysis match the measured scripts/results, and the separate full raw-file audit
checked the original boxes. Earlier deployed sources remain unmodified.

The local SSH control connection remained open after the scientific driver and
all owned simulations had exited. It was closed only after independently proving
remote completion and zero live sessions/root processes; no simulations were
terminated or resubmitted. This control-channel issue did not authorize a rerun.

Exact-mode evidence roots on allegro include
`/tmp/lammps-t017-UVaWiFdn` (conditional-neighbor failure),
`/tmp/lammps-t017-xMEWqxFa` (atomic snapshots only), and
`/tmp/lammps-t017-c8mmiCTu` (full clock rejection). The 2025 exact-mode failure is
under `nve-comparison` in the ABP validation root. All roots remain retained.

The released gate is intentionally restricted to this model/build/layout and
2000-step plan (boundaries 600/1400), with the tested stream/output policy. No
automatic splitting is implemented. Cross-worker restart, multiple MPI
ranks/threads, other stochastic integrators, interacting ABPs, global parameter
ramps, long-time transport beyond the measured lags and power-loss recovery are
outside this validation. T018/T019 require separate implementation and checks.

Local verification and final self-review results are recorded in `tasks.md`.
