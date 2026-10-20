# LAMMPS Task Manager

A central manager dispatches independent LAMMPS simulations according to Linux worker load and resource quotas. See [architecture requirements](docs/architecture.md) for the permanent design reference and planned behavior.

The CLI provides help, version information, and strict manager / worker YAML validation. The Python API creates immutable job specification snapshots and persists jobs, attempts, events, and resource reservations in SQLite. See the [storage API](docs/storage.md) for atomic operations and state transitions. The CLI can submit, execute and inspect sequential [local batches](docs/local-batches.md), with isolated inputs and preserved output. The [scheduler API](docs/scheduling.md) admits queued jobs atomically using supplied worker metrics and resource quotas. The [polling manager and workers](docs/workers.md) provide authentication, heartbeats, Linux monitoring, pause / resume and shared-storage execution. Authenticated [staged input preparation](docs/staged-inputs.md) supports separate roots; staged execution remains disabled until result collection (T008). Real multi-host acceptance and recovery remain pending. See [configuration documentation](docs/configuration.md) and [examples](examples/manager.yaml).

## Local development

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

Execution currently requires one central manager, local SQLite, persistent workers, and a shared filesystem. Hosts without shared storage can snapshot and transfer inputs using explicit staged mode (T007), but execution remains disabled pending result collection (T008) and real two-host acceptance (T009 in [tasks.md](tasks.md)).

Permanent repository documentation and agent-facing material use English. Temporary user-facing material uses Traditional Chinese. Transfer relevant decisions into permanent English documentation and update references before retiring a temporary draft.
