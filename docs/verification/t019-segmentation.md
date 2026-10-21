# T019 queued continuation verification

Verified 2026-10-22 (Asia/Taipei), using explicit user authorization to transfer
project source, drivers and PyYAML to new private `/tmp` directories on mozzarella
and execute bounded singleton MPI/LAMMPS tests. No existing simulation, output or
deployment was changed. Raw files remain retained. [Machine-readable evidence](t019-segmentation.json)
contains the actual manifests, source/driver hashes, processes and parent histories.

## Functional results

The final root is `/tmp/lammps-t019-pw8Nv0Y1/segmentation`. The driver exercised the
actual polling manager, scheduler, worker journals/executor, HTTP transfers and
SQLite checkpoint/progress/enqueue transactions:

| Scenario | Plan | Worker placement | Result |
|---|---|---|---|
| Shared chain | 600 + 800 + 600 | a / a / a | Parent succeeded; three checkpoint identities and fresh queue entries |
| Staged chain | 600 + 800 + 600 | a / b / a, same Linux host boot | Parent succeeded after all required result acknowledgements; both reassignment boundaries restored |
| Default maximum | 2000, `step_max=100000` | a | Exactly one segment, final checkpoint and parent completion |

All seven segment processes and three independent uninterrupted references exited
zero. Each invocation had one MPI rank, one OpenMP thread and a 30-second limit.
Initial priority was 11; continuations used -3, with distinct queue-entry sequences
and recorded dispatch values. Both shared and staged segments published complete
verified, synced result trees before advancing their parents. All parent progress reached exactly 2000 committed
steps; each job had one attempt. Resources were unreserved only after conclusive
session termination. There was no worker-local chaining.

Both copies of every restored boundary matched in full unwrapped particle position
and orientation data. Each combined trajectory contains exactly 101 global frames
at 20-step intervals, including its single logical copy of each boundary. Paired
uninterrupted references shared the initial physical ensemble and used distinct
recorded Brownian streams; the evidence retains their observable summaries.

These three new pairs are **functional verification, not a new statistical
inference**. Scientific release still relies on T017's frozen 256 independent
confirmatory pairs for this exact renderer/model/build/2000-step plan. T019 also
admits the T017 measured uninterrupted reference as a single segment. Its renderer
and physical parameters are unchanged. Exact mode, unmeasured ranges and builds
remain rejected. The generic 250001-step arithmetic plan is covered locally but
is not released for actual execution. See [scope](../segmentation.md).

## Final audit

A separate read-only SSH audit rehashed all 254 retained manifest files,
116,004,089 bytes, and checked all 19 deployed source files against their initial
and final hashes, plus all three driver hashes. All ten original sessions were
empty; there were no test-root processes. Every actual stdout log showed one rank
and one thread. The executable/library/CPU fingerprint before and after execution
and during the final audit was:

`aa8a1af68131f97d13fd4959031c557681b46c9eee23dddef26ca75c7eae0695`

Local recomputation matched final deployed source and all seven actual segment
input hashes against the released renderer and recorded stream seeds. It checked
that each run's derived seeds are distinct. The earlier eight-invocation functional
run under `/tmp/lammps-t019-9CUZ74zI/segmentation` and the superseded ten-invocation `/tmp/lammps-t019-KTko22hT/segmentation` run remain
retained, alongside the
separate [T018 publication/restoration evidence](t018-checkpoints.json). No failed
or superseded output was removed.

## Local reliability evidence and limits

Local tests use byte stand-ins and fake MPI/LAMMPS executables. They cover exact
interval arithmetic, configuration validation, default single segments, immutable
snapshots, first-dispatch priority inheritance and queued-only overrides, fresh
FIFO competition, checkpoint/result gates, enqueue rollback after filesystem
publication, database reopen/replay, corruption, unknown execution, explicit retry
budgets, parent cancellation, persisted diagnostics and bounded monitoring/CLI.
Loopback integration covers shared/staged pipelines, lost checkpoint acknowledgements,
complete manager/worker reconstruction, and actual local process-group cancellation.
Actual commands, counts and outcomes are recorded in [tasks.md](../../tasks.md).

Same-host worker identities are not separate physical machines. Cross-host
compatibility, real host reboot/power loss, large artifact performance and production
deployment remain unverified. The real reference runs do not expand the scientific
matrix to longer durations, interacting ABPs, new layouts or new builds.
