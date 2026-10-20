# T009: Real two-host staged dispatch

Verified on 2026-10-21 (Asia/Taipei). The manager ran on poincare and one worker ran on allegro. This completes T009. The later [V001 verification](v001-two-workers.md) separately satisfies the two-worker shared-storage requirement. Machine-readable evidence, manifests, process identities, versions, event histories and hashes are in [t009-staged.json](t009-staged.json).

## Environment and isolation

- Manager root: `/tmp/lammps-t009-manager-80jnzgiy`; local SQLite and `jobs/` remain there.
- Worker root: `/tmp/lammps-t009-worker-wsUMz3qp`; its independent `work/`, journal, inputs and outputs remain there. There was no shared filesystem.
- Transport: direct `https://192.168.50.112:18443` from allegro on the private LAN. A dedicated two-day self-signed test certificate had an IP subjectAltName; `SSL_CERT_FILE` supplied explicit trust. The worker verified the certificate/hostname, used a private bearer credential and the normal no-proxy/no-redirect transport. No tunnel or plaintext exception was used.
- SSH was used for isolated deployment, test-driver control and read-only audit, using the previously recorded host key. Simulation input/result transfer used the implemented HTTPS protocol, not SSH copying.
- Worker: Python 3.10.12, PyYAML 6.0.3 in a new private venv, `/usr/bin/mpirun.openmpi` (OpenRTE 4.1.2), `/usr/bin/lmp` (29 Sep 2021 Update 2). Source hashes matched the current repository on both hosts.
- Observed clock difference was bounded by approximately 0.011 seconds when the worker response arrived. This checks the test interval, not ongoing NTP synchronization.
- Limits: two CPU slots, one concurrent job, 1 GiB memory budget; each job requested two MPI ranks, one OpenMP thread per rank, 0.1 GiB and a 40-second time limit. External CPU-load admission was disabled explicitly; memory admission and reservation quotas remained enabled. These are accounting settings, not measurements of peak memory or OS enforcement.

## Procedure and observed results

The explicit temporary harness used `ManagerService`, `make_server`, `submit_batch` and the actual `Worker.tick`/executor, with a persistent remote driver controlled over SSH. It was outside default test discovery. Temporary scripts and complete request/result snapshots remain in `/tmp/t009-*.py`, `/tmp/t009-payload/` and the manager root's `transcript.json`. Commands executed:

```bash
python3 /tmp/t009-prepare.py
python3 /tmp/t009-deploy.py
PYTHONPATH=src python3 /tmp/t009-manager.py > /tmp/t009-run-third.log 2>&1
PYTHONPATH=src python3 /tmp/t009-audit.py
python3 /tmp/t009-cleanup-audit.py
make check > /tmp/t009-make-check-authorized.log 2>&1
```

The deployment created a mode-0700 remote directory and a venv with `python3 -m venv --without-pip .venv`; system pip installed only `PyYAML>=6.0,<7` into that venv's site-packages. No existing deployment or system package was changed. Dedicated token/key files were private and are excluded from persistent evidence.

Both submissions included `in.lmp` and an auxiliary `auxiliary.inc` consumed by LAMMPS `include`. The input created a 108-atom FCC Lennard-Jones system with NVE integration. The successful job ran 5000 steps and wrote `output.data`; the failing job replaced `run 5000` with `invalid_t009_command`, producing a real LAMMPS input error and nonzero exit.

| Case | Attempt | Process | Delivery |
|---|---|---|---|
| Success | `2d568d67637e4022aa9ce52125e7b38d` | succeeded, exit 0 | collected |
| Invalid input | `2789e857a3fd4b8e880492d2118b86c5` | failed, exit 1 | collected |

The harness closed the dedicated manager listener after launch. The worker observed connection refusal while its original launch session was still live. After heartbeat expiry the manager marked the attempt `unknown`, retaining its two-slot/0.1-GiB reservation. Even after the remote process ended locally, the manager retained the reservation until it received termination evidence after reconnecting. The same attempt recovered and delivered its output.

On the first result PUT, a test-only handler read eight payload bytes and shut down that TLS socket before replying. The normal worker collector persisted `RemoteDisconnected` as a retryable delivery failure. Later ticks replayed the upload and commit successfully. No network settings or product source were changed for fault injection.

All 17 manifest-selected result files across both jobs were checked against exact sizes and SHA-256 on both hosts. This included stdout/stderr, execution/process/outcome metadata, copied inputs, logs and the successful scientific output. Both staged inputs also matched their immutable submission hashes in the worker input and execution trees. Repeated polls left exactly one attempt per job; each stdout contained one LAMMPS banner. Final reservations were empty, and both launch sessions had no live members. A final read-only audit found no processes with a working directory under any T009 worker test root, and the dedicated manager port was closed. All outputs were retained.

## Earlier runs and limitations

Initial environment preparation attempted to reuse the earlier T006 venv's PyYAML, but those paths were absent; the new venv installation resolved this. The first harness run did not handle the deliberately induced poll connection refusal; its shutdown waited for execution and retained the output. The second used a 100000-step input that exceeded its 40-second submitted limit and was cancelled with termination evidence. Those private roots and logs were retained. The final run used fresh roots and the smaller 5000-step input; no prior output was overwritten or deleted.

Sandboxed `make check` ran 144 tests with 75 socket-permission errors. The authorized run allowing local sockets passed all 144 tests, syntax compilation and Git whitespace checks. Default tests still use local fake processes and never connect to these hosts.

This evidence covers one remote worker and small artifacts. It does not establish two-worker acceptance, multi-node MPI, physical power-loss durability, full service restart recovery, large-transfer performance or production service deployment. T006's cancellation evidence remains separate. The test TLS key/certificate and temporary credentials are not production configuration. No commits or pushes were made.
