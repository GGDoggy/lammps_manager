# LAMMPS Task Manager

A central manager dispatches independent LAMMPS simulations according to Linux worker load and resource quotas. See [architecture requirements](docs/architecture.md) for the permanent design reference and planned behavior.

The CLI provides help, version information, and strict manager / worker YAML validation. The Python API creates immutable job specification snapshots and persists jobs, attempts, events, and resource reservations in SQLite. See the [storage API](docs/storage.md) for atomic operations and state transitions. The CLI can submit, execute and inspect sequential [local batches](docs/local-batches.md), with isolated inputs and preserved output. Scheduling, remote workers and recovery remain planned features. See [configuration documentation](docs/configuration.md) and [examples](examples/manager.yaml).

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
- [tasks.md](tasks.md): task backlog; T001 records configuration validation and job specifications; T002 records SQLite persistence; T003 records local batches.
- `src/lammps_manager/`: Python package; add modules as their features are implemented.
- `tests/`: automated tests that do not depend on remote machines.

The first release targets one central manager, SQLite, persistent workers, and a shared filesystem.

Permanent repository documentation and agent-facing material use English. Temporary user-facing material uses Traditional Chinese. Transfer relevant decisions into permanent English documentation and update references before retiring a temporary draft.
