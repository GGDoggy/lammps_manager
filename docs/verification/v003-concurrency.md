# V003: Real concurrent simulations

Passed on 2026-10-21 (Asia/Taipei), after V008, on allegro using the fresh root `/tmp/lammps-v008-v005-eOAQn2xX/v003`. See [JSON evidence](v003-concurrency.json), the common [final audit](v008-v005-final-audit.json) and the exact executed [harness](../../tools/verification/distributed_acceptance.py).

The manager and worker were separate CLI OS processes. Communication used bearer authentication and certificate-verified loopback HTTPS on the dedicated port 18448. The source copy matched all 15 local Python modules. Open MPI 4.1.2 and LAMMPS 29 Sep 2021 Update 2 executed within inherited affinity CPUs 0–3. Worker limits were four CPU slots, 1 GiB reserved memory and two concurrent jobs; each job requested two ranks × one thread, 0.1 GiB and a 30-second limit unless noted. These are application reservations, not cgroup enforcement. External CPU-load accounting was explicitly disabled here and verified separately by V004.

The first three 256-atom jobs each ran 150000 steps successfully. Their first two intervals overlapped for 11.057 seconds (07:38:46.746–07:38:57.803 UTC). The third was observed queued with two running reservations and started at 07:38:57.916 UTC, after the first completed. Timestamped database samples preserve attempts, reservations and worker snapshots throughout.

The next batch contained a long cancellation target, a long job with a six-second time limit and another 150000-step success case. Cancelling the first left the second running. The third started after cancellation released a slot, remained running across the second job's timeout, and completed successfully. Each cancellation/timeout has termination evidence, preserved diagnostics and an empty final MPI session. All six jobs retained exactly one original attempt and separate input/output trees; all expected successful `output.data` files exist. Shared output is directly available rather than staged/collected.

```bash
PYTHONPATH=src .venv/bin/python distributed_acceptance.py
```

The driver records exact manager/worker/submit/inspect/cancel/status CLI commands, samples and output hashes. Final manager/worker reservations were empty, all six MPI sessions ended and the dedicated manager listener closed. The test uses one actual worker and two-rank single-host MPI; it does not establish multi-node MPI, cgroup isolation or large-workload performance. Existing deployments and historical results were untouched.
