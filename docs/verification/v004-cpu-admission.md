# V004: Real per-CPU admission and freshness

Passed on 2026-10-21 (Asia/Taipei), after V003, on allegro within fresh `v004` and `v004-short` roots under `/tmp/lammps-v008-v005-eOAQn2xX`. [JSON evidence](v004-cpu-admission.json) retains each real Linux sample, sample boundaries, effective policy, decision diagnostics, reservations and attempt records. The [common audit](v008-v005-final-audit.json) verifies source identity and final process/resource cleanup. The exact executed [harness](../../tools/verification/cpu_acceptance.py) is retained.

The process and child executions inherited affinity CPUs 0–3. Three test-only Python load generators were pinned individually to CPUs 0, 1 and 2 and capped at 20 seconds; the driver terminated and waited for them sooner. `LinuxMonitor` read actual `/proc/stat` and `/proc/meminfo` with complete approximately 0.5-second intervals. No utilization or available-memory values were fabricated. Scheduler policies/capacities were explicitly varied through the Python API, preserving submitted specifications. The test calls the real scheduler and executor directly to hold/reuse publication samples deterministically; it does not modify a running production worker's telemetry or host-wide networking.

| Gate | Observed evidence |
|---|---|
| Floor disabled, reserve 1, request 2 | Busy CPUs sampled 100%, 100%, 100%, 2.04%; one qualifying CPU blocked every assignment. |
| Minimum 2 | The same busy sample admitted one two-slot job and blocked the next, with a two-slot pending charge. The first ran real MPI/LAMMPS. |
| Stale metrics | Retaining the original sample beyond the one-second maximum age blocked admission even with minimum 4. The running attempt retained its reservation. |
| Other quotas | Fresh busy samples with minimum 4 still respected a 0.15 GiB manager memory budget and one-job concurrency cap; both blocked the next job. Restoring limits admitted the second reservation; the third hit CPU quota. |
| Load removal | After terminating all load generators and taking full later intervals, four CPUs qualified (2.04–4% utilization); request 2 plus reserve 1 admitted normally. |
| Completed short job | One real 1000-step job completed successfully, but two repeated scheduling rounds using its original sample still charged two pending slots and blocked its successor. |
| Complete post-start sample | A subsequent full interval retired the charge to zero; the second 1000-step job was admitted and succeeded. |

All three long-scenario jobs have one attempt: the running first was cancelled with confirmed MPI termination; the other two were cancelled before launch after their admission decisions were recorded. Both short jobs exited 0. No input specification was rewritten. Final reservations were empty; all three actual MPI sessions and all three pinned load processes ended. Memory and concurrency tests change configured accounting ceilings, not OS limits. Samples near a threshold were not used as decisive evidence. This is real-host scheduler/monitor/executor verification; HTTPS interruption and OS restart behavior are verified separately by V005.

```bash
PYTHONPATH=src .venv/bin/python cpu_acceptance.py
```
