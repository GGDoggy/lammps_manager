# V001: Real shared-storage dispatch on two workers

Verified on 2026-10-21 (Asia/Taipei). A manager on mozzarella dispatched four real MPI/LAMMPS jobs to two distinct physical worker hosts, mozzarella and allegro. This satisfies V001's original shared-filesystem/two-worker acceptance scope. See [machine-readable evidence](v001-two-workers.json) for configuration, source hashes, queue/reservation snapshots, attempt histories, output hashes, distinct Linux boot identities and final process audits. [T009](t009-staged.md) remains the separate staged-storage acceptance record.

## Topology and isolation

| Role | Host | Private local state root |
|---|---|---|
| Manager and worker | mozzarella | `/tmp/lammps-v001-mozzarella-I9rVloa1` |
| Worker | allegro | `/tmp/lammps-v001-allegro-jAX0yXba` |
| Local orchestration/evidence | poincare | `/tmp/lammps-v001-control-bdmk5und` |

Both workers already mounted `192.168.50.103:/data_parmesan` at `/mnt/data`. The new mode-0700 shared test root was `/mnt/data/lammps-v001-o0xDecbM`. Fresh unpredictable files written on each host were read from the other at the same absolute path, establishing bidirectional shared visibility; the normal manager filesystem challenge was also read by both workers. No mount or existing deployment was changed. Manager SQLite and worker journals remained on their hosts' local `/tmp` filesystems, outside NFS.

The dedicated manager listened on `https://192.168.50.136:18444`. Both workers used explicit `SSL_CERT_FILE` trust for a dedicated two-day test certificate with that IP in subjectAltName. Normal certificate/hostname verification, distinct private bearer credentials and no-proxy/no-redirect restrictions remained enabled. SSH carried isolated deployment/control/audit traffic; the actual worker protocol used direct authenticated HTTPS. No plaintext exception or tunnel was used.

Both hosts ran Python 3.10.12, isolated PyYAML 6.0.3 and `/usr/bin/mpirun.openmpi` (OpenRTE 4.1.2). Allegro used `/usr/bin/lmp` (LAMMPS 29 Sep 2021 Update 2); mozzarella used `/usr/local/bin/lmp` (LAMMPS 2 Apr 2025, linked to Open MPI). Both remote source manifests matched the current local Python modules. A downloaded CPython-3.10 x86-64 PyYAML wheel was unpacked into each new `venv --without-pip` environment because mozzarella had no system pip. No system packages were installed or changed.

## Admission, execution and output

Each worker had a four-slot CPU quota, a 0.3-GiB memory reservation budget and maximum concurrency one. Each submitted job requested two MPI ranks × two OpenMP threads = four slots, 0.2 GiB and a 60-second limit. The scheduler retained a 0.1-GiB available-memory safety reserve. External CPU-load admission was explicitly disabled for these bounded tests; hardware CPU/memory monitoring and reservation accounting remained active.

With both workers paused, four submitted jobs stayed queued after authenticated polls and no reservations were made. Resuming both allowed a scheduling round to reserve exactly one four-slot/0.2-GiB attempt per worker; the other two jobs stayed queued. Every observed manager round stayed within CPU, memory and concurrency quotas. Completed attempts released reservations only with termination evidence, allowing the second wave. These snapshots inspect quota accounting, not OS enforcement, memory peaks or independent isolation of each admission limit.

All four inputs created a 108-atom FCC Lennard-Jones system, included `auxiliary.inc`, ran 5000 NVE steps and wrote `output.data`. The execution metadata and LAMMPS stdout establish two MPI tasks and two OpenMP threads for each job.

| Worker | Attempt | Outcome |
|---|---|---|
| allegro | `246a4187fc73464694dfeb01391e735b` | succeeded, exit 0 |
| mozzarella | `77fa1c30371742d4bcab197c440b0d35` | succeeded, exit 0 |
| mozzarella | `ebbe6b71352445dea87f90cfa6d6d9d3` | succeeded, exit 0 |
| allegro | `902c9ccff49042f28186463ac0d3f866` | succeeded, exit 0 |

The manager audited all 32 files across the four attempts: stdout/stderr, execution/process metadata, copied inputs, logs and generated scientific outputs. Worker-side hashes matched the manager's view of the shared files; the final remote audit recorded sizes and rechecked hashes. Primary inputs matched submitted SHA-256. Each stdout contained one LAMMPS banner and the expected atom/step counts. Exactly four jobs and four attempts existed, two attempts per worker, with zero final reservations in manager and worker databases. Distinct hostname and Linux boot IDs establish two actual worker hosts rather than two processes on one host.

## Harness recovery and cleanup

The explicit harness lived outside default test discovery and used the actual manager service, scheduler, worker/executor and SQLite journal APIs. The first preparation failed because mozzarella lacked pip; no simulations started in that preparation, and its roots were retained. Wheel unpacking resolved the dependency prerequisite.

During the successful batch's second wave, the test driver's 45-second future observation timed out before all completion reporting finished. The harness did not resubmit jobs. Read-only journal/process audits then proved both original attempts on each worker had succeeded and all original launch sessions had ended. The test driver control pipe needed an additional close request to finish its shutdown; this affected test control only. Reopening the same manager and worker journals replayed terminal reports, preserving all four attempt IDs and outputs. This is evidence of terminal-report replay, not a broad claim about every restart failure window.

Final read-only audits verified all four original process/session identities had no live members, no test processes remained under either private state root, and the dedicated manager listener was closed. All shared/private test inputs, outputs, databases and earlier preparation roots were retained. No real output was deleted; no existing service was stopped or modified.

## Commands and checks

The temporary scripts, archives, transcript files and logs remain under `/tmp`; persistent evidence contains no credential contents. Principal commands were:

```bash
python3 -m pip download --only-binary=:all: --no-deps 'PyYAML==6.0.3' -d /tmp/v001-wheels
python3 /tmp/v001-run.py > /tmp/v001-two-workers-second.log 2>&1
python3 /tmp/v001-resume.py > /tmp/v001-two-workers-replay.log 2>&1
python3 /tmp/v001-audit.py
make check > /tmp/v001-final-check-authorized.log 2>&1
```

The initial preparation log is `/tmp/v001-two-workers-run.log`. Complete control snapshots are `transcript.json` and `transcript-complete.json` under the poincare evidence root. Source identities, versions, exact argv/environment, state transitions, reservation snapshots and file hashes are retained in [v001-two-workers.json](v001-two-workers.json).

Sandboxed `make check` ran 144 tests with 75 loopback socket permission errors (`/tmp/v001-final-check-sandbox.log`). The authorized run passed all 144 tests, syntax compilation and Git whitespace checks (`/tmp/v001-final-check-authorized.log`). Default tests remain offline from real hosts. Final documentation/evidence checks also covered links, whitespace, source-manifest consistency and exclusion of passwords, bearer tokens and private keys. No commits or pushes were made.

This is small-batch shared dispatch on two workers, with MPI ranks confined to their respective worker hosts. Real high-external-load/stale-metric exclusion, large-transfer performance, physical power loss, multi-node MPI and OS resource enforcement were not verified by this run. Production service deployment and the dedicated temporary test credentials/certificate remain separate concerns.
