# V009 WCA-ABP continuation protocol (predeclared)

## Current disposition: study withdrawn

Planning clarification (2026-10-24): the user explicitly requests skipping this
study and proceeding with WCA ABP segmented continuation using fresh random
streams per segment. The withdrawn study is not an implementation prerequisite.
[T021](../../tasks.md) records the new engineering contract and acceptance plan.
Current runtime gates remain unchanged until implementation; the historical
scientific evidence and partial-data disposition below remain unchanged.

User decision (2026-10-24): the WCA endpoint-equivalence confirmation study is withdrawn, not passed or statistically failed. The user identified finite-time metastable-state selection after changing stochastic streams as an attribution concern and requested proceeding from existing engineering evidence. Nonequilibrium alone does not invalidate ensemble comparisons, and metastability has not been established for these exact parameters; nevertheless the present global endpoint gate does not uniquely diagnose continuation defects. No replacement study or automatic resubmission is authorized. The exact original service was stopped after matching PID/start ticks/boot identity; MainPID=0, its cgroup is empty and stop_verified=true. Forty complete chunks (2560 pairs), all pilot/raw data and interrupted work are retained. Partial samples must not be relabeled as full-family acceptance. Existing transfer/recovery/lifecycle and raw-integrity evidence remains applicable only to its measured scope; production scientific capability gates remain unchanged. Full 29426-pair collection and its acceptance analysis are no longer pending requirements. See v009-wca-study-cancelled.json. General goal remains paused; remaining review proceeds from retained evidence without restarting this study.

The historical protocol and checkpoints below are retained as evidence, not instructions to resume collection.

Status: preparation; no scientific acceptance is claimed. Authorized on 2026-10-23.
The user requests WCA-interacting ABP with no artificial step or MPI-rank limit
and accepts default physical parameters and predeclared equivalence criteria.
No host reboot or power-loss experiment is permitted.

## Model and state contract

Use 256 identical diameter-one particles in a periodic 2D square, number density
0.5, epsilon=sigma=kBT=1, WCA cutoff 2^(1/6) sigma and shifted energy. Use the
fingerprinted mozzarella LAMMPS 2 Apr 2025 build, brownian/sphere Gaussian noise,
gamma_t=10, gamma_r=1, dipole propulsion force 10, dt=0.0001, one OpenMP thread.
Begin on a nonoverlapping square lattice with independently randomized planar
unit dipoles. Use 20000 preparatory steps per independent pair. This preparation
is not a claim of equilibrium. Both comparison arms restore the same prepared
restart; preparation is never replayed at a continuation boundary.

Persist positions, images, unit dipoles, box, atom IDs/types, masses/diameters,
interaction parameters, dt and global timestep/time. Recreate only the Brownian
and propulsion fixes. Renew Gaussian streams with recorded SHA-256-derived seeds
for every arm/segment. Reject collisions. Statistical continuation does not claim
trajectory identity, restored RNG state, timestep convergence or a phase diagram.

The current online [Brownian documentation](https://docs.lammps.org/fix_brownian.html)
changed RNG restart serialization on 4 Jul 2026 and rotation defaults on
11 Feb 2026. Those changes must not be attributed to the deployed 2025 build.
Inspect deployed/version-matched source and preserve actual build identity.
[lj/cut](https://docs.lammps.org/pair_lj.html) with the energy shift and cutoff at
the minimum defines WCA; [propel/self](https://docs.lammps.org/fix_propel_self.html)
uses the recorded unit dipoles.

## Experiments and frozen analysis

Strata: measured durations 20000 and 100000 steps, each with segment MPI layouts
(1,1,1), (2,2,2), (4,4,4), (1,2,4), and (4,2,1). Each reference uses one MPI rank;
segment boundaries are 30% and 70% of the measured duration. This tests changes
of decomposition as well as fixed multi-rank continuation. The renderer and
future capability design must not impose these test durations/ranks as arbitrary
user limits. Finite measurements do not prove all possible durations or hardware
layouts; report measured coverage and conditional Markov/kernel assumptions.

For each stratum, run eight independent pilot pairs; estimate a fixed confirmatory
sample count before any confirmatory data in that stratum. Minimum 64, maximum
512 pairs, 90% target power, pilot variance inflation four. If the estimated count
exceeds 512, report insufficient power; never enlarge equivalence margins after
seeing results. Confirmatory seeds are disjoint from pilot seeds.

Use independent replica-level paired differences (segmented minus reference),
not correlated frames/particles as independent samples. Control family error at
0.05 across every endpoint in all ten strata with Bonferroni two-sided Student
intervals. Require each entire interval strictly within its predeclared margin.
Pilot selection uses the same family count and target power. Failed/inconclusive
strata remain failed/inconclusive and retain all raw data.

Endpoints: mean per-particle potential energy (absolute margin 0.05 LJ units);
mean coordination in radial shells [0.9,1.0), [1.0,1.122462048309373),
[1.122462048309373,1.5), [1.5,2.0) (absolute margin 0.1 neighbors/particle);
structure factor at the first x/y wave vectors (margin 0.1); MSD at lags 0.1 and
0.2 physical time, normalized by free-ABP MSD (margin 0.05); orientational ACF at
the same lags (absolute margin 0.05); and before/crossing/after EACH boundary MSD
and orientation ACF (same margins). Use 101 equally spaced trajectory frames;
short boundary windows use one frame before and after the boundary, with
normalization at their actual physical lag. Structure endpoints use the final
five equally spaced frames. Keep the duplicate initial boundary frame in raw
files and check it before retaining one global analysis frame.

Every invocation is bounded to 120 seconds and at most four allocated logical
CPUs during this experiment. Keep original process/output identities, failure
records, full source/protocol hashes and boot identities. Pilot and confirmatory
sampling is sequential and explicitly separate. No production capability gate
is changed merely because a driver reports a pass.

## Version 2: pilot-informed sample budget (2026-10-23)

Version 1 is retained as inconclusive: the first 20000-step singleton pilot
required 848 pairs, exceeding its 512-pair cap, driven by structure factor
variance. No confirmatory data were collected and no margin is changed.
Version 2 uses new disjoint seed identities, pilots all ten strata before any
confirmatory execution, and permits up to 16384 pairs per stratum. Freeze the
complete sample-size table and assess measured runtime/storage before launching
confirmation. This is a new study, not a relabeling of the failed first study.
Seed derivation is an injective affine permutation modulo 899999999, with a
SHA-256 namespace offset, multiplier 1000003 (coprime to the modulus) and unique
stratum/phase/replica/stream ordinal. This eliminates collisions within the
predeclared maximum study size; it is not a proof of independent PRNG streams.
Keep all physical parameters, endpoints, equivalence margins, family confidence,
variance inflation and target power unchanged. No production release follows
from pilot evidence.

## Frozen confirmation and resource constraint

All ten version-2 pilots completed (80 independent pairs). The frozen total is
29426 confirmatory pairs; see `v009-wca-frozen-samples.json`. No confirmatory
observations informed that table. On 2026-10-23 the user required no more than
half of each machine's CPUs. Discovery found mozzarella 20 logical CPUs
(confirmation cap 10), allegro 16 (cap 8), and poincare 16 (cap 8).
This supersedes the original four-CPU aggregate experiment allocation without
changing any physical model, replica, endpoint, margin or family-error rule.

Confirmatory replicas may execute in disjoint 64-pair measurement chunks. Every
chunk retains its original global replica ordinals and stream identities;
chunking never changes the sample count or stops early based on measurements.
Static exclusive affinity sets reserve each chunk's maximum simultaneous MPI
rank demand. The pool is pinned to at most half the machine's visible CPUs and
never allocates overlapping sets. Per-invocation MPI ranks remain as frozen in
the measured layout matrix. Chunks do not claim standalone scientific acceptance.
The final analysis must recover each required replica exactly once, reject
missing/duplicate/error chunks, and calculate the original full-family Student
intervals using each stratum's entire frozen sample count.

Store completed dump files as lossless gzip after checking restored-byte SHA-256
against the original dump. Retain all information, scripts, logs, restart files,
metrics, source identities and compressed raw trajectories. A chunk is complete
only after all its measurements and inventory are saved. Keep at least 20 GiB
free disk before starting additional chunks; never delete earlier output to
satisfy this guard. A blocked resource guard is not scientific acceptance.
