# LAMMPS Task Manager

A central manager dispatches independent LAMMPS simulations according to Linux worker load and resource quotas. See [architecture requirements](docs/architecture.md) for the permanent design reference and planned behavior.

The CLI provides help, version information, and strict manager / worker YAML validation. The Python API creates immutable job specification snapshots and persists jobs, attempts, events, and resource reservations in SQLite. See the [storage API](docs/storage.md) for atomic operations and state transitions. The CLI can submit, execute and inspect sequential [local batches](docs/local-batches.md), with isolated inputs and preserved output. The [scheduler API](docs/scheduling.md) admits queued jobs atomically using supplied per-logical-CPU metrics, configurable slot minimum/reserve and resource quotas. The [polling manager and workers](docs/workers.md) provide authentication, heartbeats, Linux monitoring, pause / resume and concurrent execution on each worker within CPU / memory / job-count quotas. Authenticated [staged execution](docs/staged-inputs.md) supports separate roots with [durable result collection](docs/staged-results.md), including failed-job output, followed by safe worker-private staged cleanup after durable acknowledgement. [Dispatch priorities](docs/scheduling.md#manager-priority-controls-t015) and bounded [operational monitoring](docs/monitoring.md) are available through CLI/JSON. Durable [prelaunch returns and readiness](docs/workers.md#prelaunch-return-and-readiness-t013), Linux [recovery, cancellation, time limits, explicit application retries and bounded automatic infrastructure recovery](docs/recovery.md) are implemented; [V001](docs/verification/v001-two-workers.md) verifies real shared dispatch on two workers. See [configuration documentation](docs/configuration.md) and [examples](examples/manager.yaml).

## Local development

[Global trajectory sampling and compression](docs/trajectory-sampling.md) provide
immutable Dr*t/step schedules and verified streaming gzip for trajectory dumps.
See the feature document for user-script includes, result manifests and verification scope.

Python 3.10+ is required. Configuration validation requires PyYAML (`python3 -m pip install 'PyYAML>=6.0,<7'`); LAMMPS and MPI are not required.

```bash
make help
make check
# Without make:
PYTHONPATH=src python3 -m lammps_manager --help
PYTHONPATH=src python3 -m lammps_manager --version
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Alternatively, install the package in a virtual environment to use the `lammps-manager` command:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
lammps-manager --help
```

Installation may download setuptools build tools. Running directly with `PYTHONPATH=src` requires PyYAML to be installed. `make check` runs tests, syntax compilation, and Git whitespace checks. Initially untracked files also need manual inspection. CI runs the same command after the repository is pushed to GitHub.

## Development guide

- [AGENTS.md](AGENTS.md): agent development and review rules, including language policy.
- [docs/workflow.md](docs/workflow.md): workflow, sources, and task template.
- [tasks.md](tasks.md): task backlog; T001 records configuration validation and job specifications; T002 records SQLite persistence; T003 records local batches; T004 records scheduling and quota admission; T005 records polling workers and local communication verification.
- `src/lammps_manager/`: Python package; add modules as their features are implemented.
- `tests/`: automated tests that do not depend on remote machines.

Execution requires one central manager, local SQLite and persistent workers. Shared mode uses a common filesystem; staged mode transfers immutable inputs and collects verified results between distinct local roots. [T009 two-host acceptance](docs/verification/t009-staged.md) verified staged dispatch to one remote worker with real MPI / LAMMPS. [V001](docs/verification/v001-two-workers.md) also verified shared-storage dispatch with real MPI / LAMMPS on mozzarella and allegro.

Permanent repository documentation and agent-facing material use English. Temporary user-facing material uses Traditional Chinese. Transfer relevant decisions into permanent English documentation and update references before retiring a temporary draft.

Real-host acceptance records: [local CLI batches (V008)](docs/verification/v008-local-batches.md), [one-worker concurrency (V003)](docs/verification/v003-concurrency.md), [per-CPU admission (V004)](docs/verification/v004-cpu-admission.md), and [OS restarts / TLS interruption (V005)](docs/verification/v005-restart-transport.md). Explicit external [harnesses](tools/verification/README.md) are retained outside default tests; these checks do not establish production deployment, large-transfer performance or power-loss durability.

[Bounded large-artifact recovery (V006)](docs/verification/v006-large-artifacts.md) and
[isolated systemd service lifecycle (V007)](docs/verification/v007-service-lifecycle.md)
are verified with real MPI/LAMMPS. See the reproducible
[service procedure](docs/service-lifecycle.md); these checks restart test
processes only and do not establish physical reboot or production readiness.

[Continuation contracts (T017)](docs/continuation.md) validate a restricted statistical ideal-ABP recipe on a fingerprinted LAMMPS build, with a fail-closed capability API and explicit scientific comparison drivers. [Durable checkpoint publication (T018)](docs/checkpoints.md) retains verified state independently of process/result completion. [Queued continuation runs (T019)](docs/segmentation.md) support the measured recipe/plan, parent monitoring and cancellation, priority inheritance and assignment-time environment binding. [Environment recovery (T023)](docs/segmentation.md#assignment-environment-and-timeout-restart-t023) removes the build allowlist and restarts expired environments from step zero after confirmed termination, with a configurable one-hour default and retained history/output. Exact continuation and cross-host continuation remain unavailable; scientific acceptance retains its measured scope.

[Engineering continuation (T021)](docs/engineering-continuation.md) adds versioned long ideal/WCA ABP contracts with persisted fresh segment seeds and recipe clocks. Scientific evidence remains `not_assessed`; real T021 restart/layout acceptance must be run separately. Existing measured contracts remain available. Submission-time [physical parameters and initial conditions](docs/engineering-continuation.md#submission-time-physical-parameters-t022) use a separate v2 contract; see the [editable request](examples/wca-run.json).

[Parameter sweeps (T020)](docs/parameter-sweeps.md) generate Cartesian batches with independent repetitions, deterministic seeds, immutable provenance and read-only previews for existing shared/staged execution.

The [current three-host deployment](docs/current-deployment.md) runs a manager on
poincare and staged workers on poincare, mozzarella and allegro under
`~/lammps_manager/manager` and `~/lammps_manager/worker`, with verified private
HTTPS over the LAN and systemd user supervision. Services use the root Git
checkout and `.venv` directly; release-based operation was replaced on 2026-10-25.

The [retired home-directory deployment](docs/deployment.md) ran one manager and
three staged workers on poincare, mozzarella and allegro under `~/lammps_manager`.
Real per-worker jobs, result integrity and automatic systemd worker restart are
verified. Those independent worker jobs do not establish single-job multi-host MPI;
the separate V010 acceptance below verifies that capability.

The [V010 home-directory deployment](docs/deployment.md) previously ran a manager and
three supervised workers; its systemd units were removed at the user’s request on
2026-10-24, with deployment data retained. Its [explicit multi-host MPI Python API](docs/multi-host-mpi.md)
passed bounded real two-host success, cancellation and manager-transport-loss
acceptance. Automatic multi-host scheduling and CLI submission remain unavailable.
