# V008: Real local batches

Passed on 2026-10-21 (Asia/Taipei), first in the requested V008 → V003 → V004 → V005 sequence. A fresh private source copy on allegro used Open MPI 4.1.2 and LAMMPS 29 Sep 2021 Update 2. The test root was `/tmp/lammps-v008-v005-eOAQn2xX`; existing services and historical output were untouched. The user explicitly authorized source transfer, bounded real test jobs, test process control and cleanup of new test copies after automatic review initially rejected source transfer.

[JSON evidence](v008-local-batches.json) retains exact CLI arguments/output, all four job/attempt records, events, artifact hashes and process identities. [Final audit](v008-v005-final-audit.json) records deployed source/harness hashes, executable version output, the separate `status` output and final termination checks. The persisted [harness](../../tools/verification/real_acceptance.py) is byte-identical to the executed copy.

The harness ran the documented `submit`, `run-local`, `inspect`, `cancel` and `status` operations with fresh YAML/database/work roots. Each execution requested two ranks, one OpenMP thread per rank, 0.1 GiB reservation and a 30-second limit. The 256-atom Lennard-Jones success case ran 1000 steps, exited 0 and wrote `output.data`; invalid input exited 1 and retained its stdout/stderr diagnostics. Both have distinct submission inputs, attempt work directories, process/execution metadata and outputs.

A separate long input was cancelled through the CLI after its attempt entered `running`. Its complete recorded MPI session ended before the next 1000-step job started. Repeating `run-local` left every inspected record unchanged and created no second attempt. Final local reservations were empty and all four MPI sessions had no live members. All test artifacts remain retained.

Reproduce only in a newly prepared private root as described in the [harness instructions](../../tools/verification/README.md):

```bash
PYTHONPATH=src .venv/bin/python real_acceptance.py
PYTHONPATH=src .venv/bin/python -m lammps_manager status --config v008/manager.yaml
```

This verifies supported CLI cancellation and confirmed termination on one host. It does not inject uncertain identity into a real local MPI session; the conservative stop-on-unknown branch remains covered by local regression tests. Local execution does not enforce scheduler quotas; these are separately verified by V003/V004. No package implementation changes, commits or pushes were required.
