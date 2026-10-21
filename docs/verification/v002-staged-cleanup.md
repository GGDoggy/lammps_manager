# V002: Real remote staged cleanup

Verified on 2026-10-21 (Asia/Taipei) against the current uncommitted Python source. The manager ran on poincare; the worker ran on allegro with Open MPI 4.1.2 and LAMMPS 29 Sep 2021 Update 2. [Machine-readable evidence](v002-staged-cleanup.json) records source hashes, immutable result manifests, events, worker cleanup records and final process audits.

## Isolation and transport

The new private manager root was `/tmp/lammps-v002-manager-_pyhd1kt`; the new worker root was `/tmp/lammps-v002-worker-rAIN1ZQz`. SSH used the previously recorded host key for deployment and control. Actual staged inputs, reports and results used authenticated HTTPS at `https://192.168.50.112:18445`, with a dedicated IP-SAN test certificate explicitly trusted through `SSL_CERT_FILE`. Certificate verification and the normal no-proxy/no-redirect policy remained enabled. No existing service or simulation directory was changed.

The remote source manifest was checked before worker construction and matched all current local Python modules again during the final audit. A new private `venv --without-pip` reused a copy of PyYAML 6.0.3 from the earlier isolated test environment; no system package was installed. Credentials and TLS private keys remain outside repository evidence.

## Cases and observed behavior

Each execution requested two MPI ranks and one OpenMP thread per rank, 0.1 GiB and a 40-second time limit. The worker quota was two slots, 1 GiB and one concurrent execution. External CPU-load admission was explicitly disabled for this small cleanup test; memory and reservation accounting remained active. The successful input created 108 Lennard-Jones atoms, included an auxiliary input file, ran 1000 steps and wrote `output.data`.

| Case | Original attempt | Process outcome | Collected files | Cleanup |
|---|---|---|---:|---|
| Real MPI/LAMMPS success | `ee430b49c0e64f43b40427fd4101c54d` | succeeded, exit 0 | 9 | cleaned |
| Real invalid LAMMPS command | `90196a84d53f4b038568ef41807bcc33` | failed, exit 1 | 8 | cleaned |
| Cancellation before launch | `e771e8df220549eeba9b54b24a98a68d` | cancelled, no launch claim | 1 | cleaned |

For success, the test driver discarded a successful commit response by raising a test-only `ConnectionError` after the normal authenticated commit operation returned. The manager had durably collected results, while the worker persisted a retryable collection failure and retained its attempt tree. A later normal poll/collection pass replayed commit successfully without rerunning the simulation.

All three cases initially used `cleanup_staged_files=False`. Their worker files remained present after durable collection, with cleanup state `retained`. The driver then closed the worker, reconstructed it with the same journal and cleanup enabled, and polled until cleanup became `cleaned` and the attempt tree disappeared. Another same-journal worker reconstruction and poll preserved each original attempt and launch fence without recreating the deleted tree. The test driver changed the immutable in-memory worker configuration through `dataclasses.replace` at reconstruction; this verifies worker construction/restart behavior, rather than the CLI/service configuration deployment path.

All 18 manager result files matched their manifests by size and SHA-256 and matched the worker bytes recorded before cleanup. Hashes were checked again after cleanup and replay. The successful stdout contained one LAMMPS banner, 108 atoms and two MPI tasks. Manager and worker final reservations were empty; the two original MPI launch sessions had no live members, and no remaining process had a working directory under the private worker root. All manager publications, source copies, journals and tombstones remain retained. Only the three newly generated eligible worker attempt trees were reclaimed.

## Commands, recovery and limits

The explicit temporary harness used the actual `ManagerService`, `make_server`, `submit_batch`, `Worker.tick`, executor and SQLite APIs. It remains outside default test discovery:

```bash
python3 /tmp/v002-probe.py
PYTHONPATH=src python3 /tmp/v002-run.py > /tmp/v002-run.log 2>&1
PYTHONPATH=src python3 /tmp/v002-audit.py
make check > /tmp/v002-make-check.log 2>&1
```

The initial automatic approval review rejected remote side effects. Execution began only after the user explicitly authorized isolated remote verification and cleanup of the new test copies. The first harness completed all three cases but its final read-only process audit returned a nonzero status. The separate audit resumed the same completed records, used guarded `/proc` working-directory reads and proved termination without resubmitting jobs. The transcript and remote stderr remain under the manager root. Final authorized `make check` passed all 187 tests, syntax compilation and Git whitespace checks; evidence/source consistency and final documentation whitespace checks also passed.

This verifies one real remote worker, small artifacts, acknowledgement replay, cleanup disable/enable and same-journal worker reconstruction. It does not establish concurrent real simulations, per-CPU load admission, full manager restart, network-level commit-response loss, physical power-loss durability, large-transfer performance, multi-node MPI or production service deployment. Existing deterministic tests separately cover filesystem failures and path replacement. No commits or pushes were made.
