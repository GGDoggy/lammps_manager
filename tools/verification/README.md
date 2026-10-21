# Explicit real-host acceptance harnesses

These scripts are outside default unittest discovery and launch real MPI/LAMMPS. Run them only with authorization for source transfer, allocated host resources, bounded test jobs, test process control and deletion of newly acknowledged worker output. They never require changes to existing services or historical simulation directories.

Prepare a fresh private directory, copy the current `src/` tree and these six Python files into it, and provide a Python 3.10+ environment with PyYAML. The verified environment used a private `.venv --without-pip` and a copy of PyYAML 6.0.3 already present in another authorized test environment; no system installation was performed. The scripts expect `/usr/bin/lmp`, `/usr/bin/mpirun.openmpi`, `openssl`, Linux procfs and `taskset`. Confirm compatible executable paths/versions before use. A fresh root is required: existing task subdirectories cause a failure rather than overwriting prior output.

Before launch, record `source-sha256.json` mapping every deployed Python path (relative to the new root) to its SHA-256. The final audit checks source identity against this manifest. For the recorded run, the local deployment utility copied `src/` without bytecode, the initial harness and the manifest via tar over host-key-verified SSH to an explicitly authorized allegro private root, then copied later harnesses individually. The preserved evidence includes every executed harness hash.

Run in the requested order from that new directory:

```bash
PYTHONPATH=src .venv/bin/python real_acceptance.py
PYTHONPATH=src .venv/bin/python -m lammps_manager status --config v008/manager.yaml > v008-status.json
PYTHONPATH=src .venv/bin/python distributed_acceptance.py
PYTHONPATH=src .venv/bin/python cpu_acceptance.py
PYTHONPATH=src .venv/bin/python restart_acceptance.py
PYTHONPATH=src .venv/bin/python final_audit.py
```

`real_acceptance.py` supplies shared helpers as well as V008. The other drivers import it from the same directory. V003–V005 restrict inherited process affinity to the first four CPUs in the caller's allocation; select the allocation with `taskset` before starting if needed. Each real job uses two MPI ranks and one OpenMP thread. Simulation memory is reserved at 0.1 GiB per job, with four-slot/1 GiB/two-job accounting ceilings for distributed cases. Reservations are not OS memory enforcement. V004's three CPU load generators share that four-CPU affinity and have independent 20-second bounds. Simulation time limits are 6–30 seconds; cancellation may take the additional graceful shutdown/escalation interval. These limits are for small acceptance examples, not throughput benchmarks.

V003 and V005 need free loopback ports 18448/18449. The drivers generate a private two-day TLS test certificate and token files in the new root; certificate verification and authentication remain enabled. Never copy tokens, credentials or private keys into repository evidence. V005 uses actual SIGINT/SIGKILL on its own manager/worker processes and faults only its own proxy. Automatic staged cleanup removes only newly generated, verified acknowledged attempt trees. All other artifacts are retained.

Each successful driver writes `vNNN.json`. Database samples are also persisted incrementally to aid investigation if a driver fails. Do not blindly rerun a failed driver or resubmit its jobs: inspect original job/attempt/journal/process records first. Driver `finally` blocks stop owned services; actual simulations remain bounded by their persisted time limit. If a worker cannot reconcile surviving execution, retain reservations and investigate original process identity; do not treat service shutdown as proof of MPI termination.

The final read-only audit checks original source hashes, every SQLite reservation table, recorded MPI sessions, test-root processes and closed listeners. Recorded evidence and limitations live in `docs/verification/v008-local-batches.md`, `v003-concurrency.md`, `v004-cpu-admission.md` and `v005-restart-transport.md`. Default `make check` does not rerun these external tests.

## T017 continuation comparisons

`continuation_acceptance.py` and `abp_acceptance.py` are standalone standard-library
drivers (neither imports `real_acceptance.py`). See [the contract and invocations](../../docs/continuation.md)
and [scientific evidence](../../docs/verification/t017-continuation.md). Both create
fresh roots, bound each singleton LAMMPS process to 30 seconds and retain raw data.
On Ctrl+C or a failed wait, the shared execution helper kills the owned process
group and reaps its direct child before propagating the interruption; output is retained.
The NVE driver performs a strict exact comparison; the ABP driver freezes pilot /
confirmatory protocol and sample count before testing equivalence. A passing raw
report does not self-authorize production splitting or other stochastic models.
