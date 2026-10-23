# V009 unattended finite verification

## Current disposition: study withdrawn

User decision (2026-10-24): the WCA endpoint-equivalence confirmation study is withdrawn, not passed or statistically failed. The user identified finite-time metastable-state selection after changing stochastic streams as an attribution concern and requested proceeding from existing engineering evidence. Nonequilibrium alone does not invalidate ensemble comparisons, and metastability has not been established for these exact parameters; nevertheless the present global endpoint gate does not uniquely diagnose continuation defects. No replacement study or automatic resubmission is authorized. The exact original service was stopped after matching PID/start ticks/boot identity; MainPID=0, its cgroup is empty and stop_verified=true. Forty complete chunks (2560 pairs), all pilot/raw data and interrupted work are retained. Partial samples must not be relabeled as full-family acceptance. Existing transfer/recovery/lifecycle and raw-integrity evidence remains applicable only to its measured scope; production scientific capability gates remain unchanged. Full 29426-pair collection and its acceptance analysis are no longer pending requirements. See v009-wca-study-cancelled.json. General goal remains paused; remaining review proceeds from retained evidence without restarting this study.

The historical protocol and checkpoints below are retained as evidence, not instructions to resume collection.

## Version-specific continuation and ranked-stream review (2026-10-23)

The [version review record](v009-brownian-version-review.json) pins the upstream
`patch_2Apr2025` tag to commit `7b4c33630d544953d2abe12eccdf0fab5ba0e7d1` and
records the fetched source blob identities. Its
[versioned Brownian manual](https://github.com/lammps/lammps/blob/7b4c33630d544953d2abe12eccdf0fab5ba0e7d1/doc/src/fix_brownian.rst)
does not save fix state in restart files. The
[current manual](https://docs.lammps.org/latest/fix_brownian.html) explicitly
marks RNG restart-state serialization as changed on 4 July 2026 and rotation
integration/defaults as changed on 11 February 2026. Current documentation
cannot establish the older binary's exact continuation or a new build's
compatibility. The embedded deployed-source paths were not readable to the
test account; upstream commit identity is not attested for that binary. The
measured executable/library/CPU fingerprint remains the actual subject.

The pinned [base implementation](https://github.com/lammps/lammps/blob/7b4c33630d544953d2abe12eccdf0fab5ba0e7d1/src/BROWNIAN/fix_brownian_base.cpp)
constructs processor-local RNGs from base seed plus MPI rank. Therefore checking
only distinct base seeds is insufficient in principle. The offline
[rank-expanded seed audit](v009-ranked-seed-audit.json) checked all 177036 actual
pilot/confirmation stream contexts and 292665 expanded seed integers under
that mapping, with no duplicates or engine-bound violations. An inverse-modulo
bound also proves no overlapping rank offsets for all 983520 permitted v2 base
ordinals at the measured maximum four ranks. This is conditional on the
recorded upstream mapping; distinct integers do not prove independent or
nonoverlapping PRNG sequences. No unmeasured rank range is certified.

The [reproduction record](v009-ranked-seed-audit-provenance.json) pins the offline
auditor source and input/result hashes, verifies exact result reproduction,
refuses unmeasured profiles/duplicate strata and preserves existing output.
It reads no confirmatory endpoint values and changes no sampling or stopping
rule. Reproduce it with a new output filename:

```bash
python3 tools/verification/wca_seed_audit.py \
  --pilot-report docs/verification/v009-wca-v2-pilot.json \
  --output NEW_SEED_AUDIT.json
```

A conditional kernel argument can guide later range/layout review: for fixed
physical parameters and step size, assume the restored physical state is
complete and fresh noise has the same independent law. If every segment then
implements the same transition kernel K, the composition identity
`K^a K^b = K^(a+b)` gives equality in law across a boundary. This is an
inference under those assumptions, not an exact-trajectory or PRNG-independence
proof. The pinned [sphere update](https://github.com/lammps/lammps/blob/7b4c33630d544953d2abe12eccdf0fab5ba0e7d1/src/BROWNIAN/fix_brownian_sphere.cpp)
uses forces/torques and present positions/dipoles, with processor-local noise;
checkpoint preservation, numerical/decomposition effects, global clocks and
those noise assumptions still need empirical and compatibility evidence.
Finite observed durations/layouts cannot certify every build, parameter set or
arbitrarily long floating-point evolution. Production gates remain unchanged.

## Rebuilt pilot completion and confirmed live wait (2026-10-23)

All 80 reconstructed pilots completed and the frozen gate matched all ten
strata and the unchanged total of 29426 confirmatory pairs. The same finite
supervisor automatically entered confirmation. The
[rebuilt-pilot audit](v009-rebuilt-pilot-audit.json) verified 2401 raw files /
486520289 bytes, all 74 frozen source files, 400 successful/reaped MPI commands,
480 unique stream uses, zero remaining pilot processes and the pool's exact
pilot-report hash. All 3680 reported pilot endpoint values are numerically
identical to the retained original pilot report, including TCP/yield execution.
This restores auditable pilot evidence without counting the missing old 704
confirmation pairs. Pilot completion remains distinct from confirmation.

The [analysis-audit checkpoint](v009-analysis-audit-checkpoint.json) captures
the live supervisor identity, finite no-restart policy, half-host affinity and
active disjoint confirmation allocations. This is a verified wait on an actual
live service, not an inference from a lock/progress file. Full raw confirmation
reproduction, statistical assumption/power-scope review and full-family
acceptance remain outstanding; no partial endpoint inspection informs sample
counts or scientific stopping rules.

## Strict analysis and raw reproduction follow-up (2026-10-23)

Analysis review reproduced a false-acceptance path in the original aggregator:
complete-size reports with missing/empty endpoint dictionaries could pass
because endpoint names were taken from a measurement row. New regressions
failed before the fix. The updated analyzer requires the exact 23 declared
endpoint names in both arms of every replica, finite numeric values (excluding
booleans), canonical replica ordinals, matching protocol/stratum, valid chunk
ranges, and complete unique frozen sample coverage. A valid complete family
still uses the unchanged Bonferroni Student intervals and original margins;
a 0.06 energy shift fails its original 0.05 margin.

The read-only auditor can now reproduce every endpoint from retained plain or
lossless-gzip trajectories and thermo logs. It checks exact frozen model/stream
inputs, preparation/initial state and duplicated physical boundaries, then
requires reproduced measurements to equal the reported values. Full audits
always perform this raw recomputation. Partial `--completed-only --recompute`
audits can validate completed chunks without any scientific acceptance claim.
An observer's own SSH/shell ancestors are excluded from the final process scan;
remaining simulation children and unrelated root processes are still detected.

For deferred full-family analysis, require an exclusive new `--analysis-output`
file as well as a new audit output. Existing evidence is never overwritten;
full analysis refuses a still-active/incomplete pool before raw processing.
The audit records both its own source hash and the analyzer source hash.
Measurement inputs and the live frozen pool/driver sources were not modified.
The new code is deployed separately in
`/home/ted/lammps-verification/v009-strict-auditor-ienin65o`; its
[deployment manifest](v009-strict-auditor-deployment.json) verifies source
identities and exact equality with the running scientific measurement kernel.

Focused checks passed seven pool tests and four raw/process-audit tests.
`taskset -c 0-7 make check` then passed 338 tests, compileall and whitespace
checks in 79.094 seconds (`/tmp/v009-strict-scientific-audit-check.log`). Raw
audit fixtures use generated stationary data and no MPI/LAMMPS: they verify
reproduction and rejection of report drift, changed input, damaged/missing
trajectories and observer/child distinction. They are not scientific evidence.
Actual full-sample raw recomputation remains pending measurement completion.

After the current service has terminated and all fixed measurements are
complete, run the following on mozzarella from outside the measurement root
(after rechecking both frozen manifests and the final service/process state):

```bash
taskset -c 0-9 env PYTHONPATH=/home/ted/lammps-verification/v009-strict-auditor-ienin65o/src \
  python3 /home/ted/lammps-verification/v009-strict-auditor-ienin65o/wca_audit.py \
  /home/ted/lammps-verification/v009-wca-tcp-yield-3jff7no4/confirmation \
  --pilot-report /home/ted/lammps-verification/v009-wca-tcp-yield-3jff7no4/pilot/report.json \
  --analysis-output /home/ted/lammps-verification/v009-strict-auditor-ienin65o/analysis.json \
  --output /home/ted/lammps-verification/v009-strict-auditor-ienin65o/audit.json
```

This command has not yet run on the complete experiment. Keep execution
bounded and within the half-host allocation; retain any failed audit and use
new output paths for a later explicitly reconciled audit. Scientific acceptance
remains unproven until full raw reproduction and every original family interval
have passed. Earlier checks and launch references below are historical.


## Resumed audit and operational amendment (2026-10-23)

The initial large cross-host test finished in 1037.734 seconds, with one
successful attempt, 3009 collected files (280763884 bytes), no transfer retries,
zero reservations and stopped service groups. Its [completed report](v009-crosshost-3600-completed.json)
and [separate read-only two-host audit](v009-crosshost-3600-audit.json) verify
all retained output hashes, source equality and the absent original MPI session.
This satisfies the declared 3600-second scenario, not the earlier 900-second
performance gate. Maximum sampled heartbeat age was 12.689 seconds; no ten-second
heartbeat claim is made for this larger cross-host workload.

The original WCA reconstruction stopped after 56 complete pilot pairs. A
rank-four segmented command exceeded the unchanged 120-second bound. Its
[failure record](v009-pilot-reconstruction-failed.json) and final service marker
confirm automatic cleanup; confirmation did not start. The original failed root
remains retained. No original terminal service was restarted.

A distinct diagnostic copied the exact failed input and checkpoint. With the
default MPI transport it completed in 100.786 seconds, without an affinity
correction; sampled rank masks were inside the four-CPU allocation. During the
slow interval all four [rank backtraces](v009-mpi-stall-backtraces.json) were in
LAMMPS border communication / MPI progress. This is evidence of severe waiting,
not proof of a permanent deadlock or of a single underlying library defect.
The default replay included short read-only debugger attachments, so its timing
is diagnostic rather than a controlled benchmark. A separate TCP replay completed
in 12.477 seconds. Three further TCP/yield
replays completed in 5.181, 10.581 and 5.284 seconds, with every complete raw
trajectory byte-identical to the default replay. See
[transport observations](v009-mpi-stall-diagnostics.json) and
[repeated TCP/yield evidence](v009-mpi-yield-diagnostics.json).
[Open MPI's scheduling documentation](https://docs.open-mpi.org/en/main/launching-apps/scheduling.html)
describes `mpi_yield_when_idle=1` as yielding during blocking MPI waits. These
small diagnostic runs support an operational mitigation; they do not establish
scientific equivalence or general performance guarantees.

A fresh persistent run now uses `OMPI_MCA_btl=self,tcp` and
`OMPI_MCA_mpi_yield_when_idle=1`, explicitly frozen in its plan. All scientific
inputs, build, seeds, endpoints, margins, fixed sample table, 120-second command
bounds, half-host allocation and automatic stopping protections are unchanged.
The plan again reconstructs all pilots, requires exact frozen-table agreement,
then collects all confirmation samples with analysis deferred. The current root
is `/home/ted/lammps-verification/v009-wca-tcp-yield-3jff7no4` and its unit is
`v009-wca-tcp-yield-3jff7no4-mozzarella.service`. Its hard runtime deadline is
2026-10-26 20:23:07 Asia/Taipei, followed by at most 180 seconds stopping.
The [current launch record](v009-unattended-launch.json) is used by the status
command; the [initial launch record](v009-unattended-initial-launch.json) is
preserved separately. Subsequent sections describe the original launch unless
explicitly stated otherwise. The overall verification remains incomplete.


## Authorization and scope

On 2026-10-23 the user requested finite test design, launch, demonstrated
self-stopping, then goal pause while the tests run. The user will resume the
goal for result analysis. Never reboot a host or modify production deployments.
Every host remains limited to half its logical CPUs: mozzarella 10/20,
allegro 8/16 and poincare 8/16. Scientific criteria and the frozen 29426-pair
sample table remain unchanged. Production continuation gates remain unchanged.

## Frozen run plan

| Test | Host | Work and acceptance | Automatic bound |
| --- | --- | --- | --- |
| Local checks | poincare | Current complete suite, compilation and whitespace checks | Completed: 333 tests passed in 83.304 seconds |
| Delayed control response | allegro | Two-second delayed/lost authenticated heartbeat response; intact input stream, one execution, hashes, empty reservations | Completed successfully; final cgroup stop verified |
| Cross-host smoke | poincare manager / allegro worker | 1 MiB plus two small files; original execution, round-trip hashes, zero reservations, worker unit stopped | Completed successfully; 120-second observation bound |
| Large cross-host transfer | poincare manager / allegro worker | 256 MiB plus 3000 small files, authenticated TLS, one attempt, original manifests, round-trip hashes, zero reservations | 3600-second observation; worker independently expires after 3720 seconds; outer local service expires after 4500 seconds from plan start |
| Reconstruct WCA pilot evidence | mozzarella | 80 original pilot streams; verify unchanged build/protocol, all per-endpoint required counts and the full frozen sample table | 7200-second step bound |
| WCA confirmation | mozzarella | Only after frozen-pilot gate passes: all 29426 pairs, ten strata, 465 chunks, disjoint static allocations, raw output inventories | 250000-second step bound; entire service expires after 259200 seconds (72 hours), plus at most 180 seconds stopping |

WCA measurements stop automatically after the fixed sample set. Full-family
scientific analysis is explicitly deferred with `--defer-analysis`. A mismatched
pilot table, changed build/source, failed chunk, lost required affinity guard,
20-GiB free-disk guard or timeout stops the plan rather than adjusting criteria,
resubmitting jobs or restarting. Failed/partial output is retained for review.

At launch (Asia/Taipei), the local cross-host service's hard runtime deadline is
2026-10-23 21:14:41, followed by at most 60 seconds stopping. WCA's hard runtime
deadline is 2026-10-26 19:59:41, followed by at most 180 seconds stopping. These
are upper bounds, not completion-time promises. Exact UTC timestamps, frozen
argv, source manifests, unit names, CPU masks and roots are in the
[launch record](v009-unattended-launch.json).

## Evidence reconstruction and persistent storage

On rediscovery mozzarella had boot ID `918bcd8d-289b-471a-9d32-0ae1324eefd9`,
different from the earlier checkpoint. The agent issued no reboot. The earlier
`/tmp/lammps-v009-confirm-O7zrjHxK` and pilot roots no longer exist. Therefore
the earlier 704 complete confirmation pairs cannot be reused or fully audited.
Repository copies of pilot measurements and frozen counts remain available.
The current LAMMPS fingerprint matches that saved pilot. Reconstruct the pilot
raw evidence with its original streams, require exact frozen-table agreement,
and collect all confirmation pairs in the new persistent root; never count the
missing original confirmation as retained raw evidence.

Private persistent roots:

- mozzarella: `/home/ted/lammps-verification/v009-unattended-4zo9y9bv`
- allegro: `/home/ted/lammps-verification/v009-unattended-a1564iys`
- poincare: `/home/ted/task_manager/var/verification/v009-unattended-y3rc__m8`

Each contains frozen source hashes, plan, incremental progress and logs.
`status.json` is published by `ExecStopPost` after checking cleanup. Files under
`pilot/`, `confirmation/` and `data/` retain scientific and transfer evidence.
There is no output deletion on failure. Earlier local and allegro roots remain
retained; the earlier mozzarella temporary roots are explicitly unavailable.

## Self-stopping and resource verification

[Normal and forced-deadline probes](v009-unattended-stop-probes.json) passed on
all three hosts. The forced probe created a detached session with a child that
ignored SIGINT/SIGTERM; the expiring service and finalizer stopped it and
confirmed no remaining cgroup processes. The initial finalizer reported a live
child too early; its [failed observation](v009-unattended-stop-probe-initial.json)
is retained. The corrected finalizer kills and waits for remaining owned cgroup
members before publishing `stop_verified=true`, and refuses cleanup outside a
dedicated service cgroup.

Every live test process is contained by a unique transient systemd unit with
`Restart=no`, `RuntimeMaxSec`, `KillMode=control-group`, `SendSIGKILL=yes` and a
finite `TimeoutStopSec`. MPI's detached sessions remain inside that cgroup.
The cross-host worker has an independent host-side expiry even if SSH or the
manager disappears. On ordinary failure the driver requests original-protocol
cancellation before stopping services; an unconfirmed reservation is retained
rather than cleared on disconnection.

Service CPU affinity and fixed MPI-slot allocations restrict work to CPUs 0..9
on mozzarella and 0..7 on the other hosts. The pool's required guard preserves
per-chunk rank affinities. [Actual task-affinity sampling](v009-unattended-affinity.json)
found no task outside those masks. `AllowedCPUs`/`CPUQuota` properties were also
requested, but these user managers do not delegate the CPU/cpuset controllers;
those requests are not claimed as kernel quota/cpuset enforcement. Existing
user sessions are active; linger settings and existing services were not changed.
Unexpected host/user-manager shutdown may interrupt work; persistent output
remains, and units do not restart automatically.

## Observation and later analysis

From the repository, this read-only command checks service state and final
markers without starting tests or performing scientific analysis:

```bash
python3 tools/verification/unattended_status.py docs/verification/v009-unattended-launch.json
```

It uses the existing verified SSH endpoints and protected askpass references;
no credential contents are stored in evidence. `finished; stop_verified=True`
means the declared plan exited successfully and cleanup was verified. It does
not mean WCA scientific equivalence passed. A timeout/error prints
`stopped: review required`; missing/unreachable state remains unknown.

When the user resumes the goal: inspect all final statuses, original journals,
reservations and process/session records; verify frozen sources/builds and all
raw inventories; reconcile partial chunks before any rerun; reproduce the full
WCA family intervals and assess against unchanged criteria. Review the large
transfer's observed deadline, hashes and heartbeat behavior separately. Retain
earlier failed acceptance gates. Do not infer unlimited duration/rank support,
cross-build compatibility, multi-node MPI or production acceptance from this run.

## Local verification and startup evidence

`taskset -c 0-7 make check` passed all 333 tests, compileall and Git whitespace
checks in 83.304 seconds (`/tmp/v009-unattended-make-check-final3.log`). A first
sandbox run could not create loopback sockets; the subsequent real-host run
exposed two import-path errors in newly added reuse tests, which were corrected
before the passing runs (332, then 333 with the frozen-pilot gate). All harnesses
and the later read-only status helper passed `python3 -m py_compile`.

[Startup snapshot](v009-unattended-startup.json) records the live plans and the
completed cross-host smoke with an empty reservation table and stopped worker.
[Delayed heartbeat evidence](v009-input-heartbeat-delayed.json) records the
completed final-source fault test. No scientific acceptance or long-transfer
success is claimed by startup checks. Goal pause leaves only the bounded host
services running, with analysis reserved for user resumption.
