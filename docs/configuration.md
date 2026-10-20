# Configuration and submission specifications

T001 provides validation and in-memory submission snapshots. Loading configuration does not start a manager, contact workers, execute commands, or submit jobs. SQLite persistence, local execution, and snapshot-driven scheduling are now implemented separately; see [storage](storage.md), [local batches](local-batches.md), and [scheduling](scheduling.md).

Install the declared dependency (`python3 -m pip install 'PyYAML>=6.0,<7'`) or install the project in a virtual environment. PyYAML is used because Python's standard library has no YAML parser. The loader uses SafeLoader and rejects duplicate keys, non-string mapping keys, unknown fields, missing required fields, incorrect scalar types (including booleans as integers), non-finite numbers, and invalid ranges. Argument arrays must be YAML lists of strings; shell templates are unsupported.

```bash
PYTHONPATH=src python3 -m lammps_manager validate-config manager examples/manager.yaml
PYTHONPATH=src python3 -m lammps_manager validate-config worker examples/worker.yaml
# Run on the corresponding deployment host after installing paths:
PYTHONPATH=src python3 -m lammps_manager validate-config worker /etc/lammps-manager/worker.yaml --check-local-paths
```

Validation errors use exit status 2. Default validation checks structure and absolute path syntax, without requiring deployment paths to exist. `--check-local-paths` checks only the current host: manager database parent and work root must be directories; worker work root must be a directory, launchers executable files, and token file readable. It does not check remote hosts, shared filesystem consistency, database writability, executable versions, credentials' validity, or connectivity. Example paths are deployment placeholders. A successful structural check is not deployment readiness.

## Manager schema

See [manager.yaml](../examples/manager.yaml). Every nested section rejects unknown keys.

| Section | Fields and defaults |
|---|---|
| `manager` (required) | Required absolute `database_path`; positive `schedule_interval_seconds=10`, `heartbeat_timeout_seconds=60` |
| `storage` (required) | Required absolute `work_root`; `mode=shared` (`shared` or `staged`); boolean `preserve_failed_jobs=true`; positive integers `max_input_files=10000`, `max_input_file_bytes=1073741824`, `max_input_total_bytes=10737418240` |
| `workers` (required nonempty list) | Each item requires unique string `id`, positive integer `cpu_slots`, positive `memory_budget_gib`, positive integer `max_concurrent_jobs`; boolean `enabled=true`, string list `labels=[]` |
| `job_defaults` | Positive integers `mpi_ranks=4`, `omp_threads_per_rank=1`, `time_limit_seconds=86400`; positive `memory_gib=8`; nonnegative integer `max_retries=0` |
| `scheduler` | `policy=fifo_least_loaded` (only supported policy); boolean `account_external_load=true`; `max_cpu_utilization_percent=80` in [0,100]; nonnegative `reserve_memory_gib=4`; positive `metrics_max_age_seconds=30` |

Memory and timing settings accept finite integers or floats except fields explicitly described as integers. Resource CPU slots equal MPI ranks multiplied by OpenMP threads per rank. These values describe reservations, not enforced OS limits. The scheduler API applies these settings to supplied snapshots; the [polling services](workers.md) supply network dispatch and Linux monitoring.

## Worker schema

See [worker.yaml](../examples/worker.yaml). Required fields: `worker` (same capacity schema as each manager worker), `manager_endpoint` (HTTP(S) URL without embedded credentials), absolute `work_root`, absolute `token_file`, and `execution`. Positive `heartbeat_interval_seconds` defaults to 10. `storage_mode` defaults to `shared` and accepts `shared` or `staged`. Positive integer `max_input_files=10000`, `max_input_file_bytes=1073741824` and `max_input_total_bytes=10737418240` independently bound staged downloads. [Staged mode](staged-inputs.md) prepares inputs only; execution awaits T008.

`execution` requires absolute `lammps_path` and `mpi_launcher_path`; `lammps_args` and `mpi_args` default to empty string lists. `environment` is a string-to-string mapping, copied into immutable pairs. Values may be empty (for example, `CUDA_VISIBLE_DEVICES: ""`) or contain only whitespace and are preserved exactly; NUL characters and non-string values are rejected. Names must remain nonempty strings without NUL. Keep secrets out of environment entries committed to version control; reference a protected token file using `token_file`. No secret contents are loaded by validation. No arbitrary shell command is accepted.

Elements of `lammps_args` and `mpi_args` may be empty or whitespace-only strings and are preserved exactly. NUL characters and non-string elements remain invalid; required paths and labels still require nonempty strings.

Configuration is intended to take effect on service restart once services exist. The scheduler reconciles worker, manager, and detected hardware capacity conservatively; callers supply worker snapshots.

## Submission API

Input paths that normalize to the work directory itself (`.`, `./`, or `././`) are rejected. Relative file paths such as `./in.lmp` remain valid in this API. Batch submission normalizes them before recording file identity.

`load_config(path, 'manager')` returns a frozen `ManagerConfig`. `create_job_specification(config, job_id=..., work_directory=..., input_file=..., input_sha256=..., resource_overrides=...)` creates a frozen `JobSpecification` with a separately copied, validated `Resources` value. Work directories must be absolute, input files relative within them, and input identifiers 64 hexadecimal SHA-256 characters. The caller supplies the digest; this API does not inspect remote files or verify input contents. The local batch submission and executor calculate and verify actual input identity.

Overrides accept only resource fields. Reloading defaults or mutating the original override mapping cannot alter an existing specification. Job identifiers and input identities are recorded here; uniqueness, persistence, attempts, and runtime state are handled by the SQLite store. Executable settings remain worker configuration. The submit command creates and persists specifications from batch manifests.

## Polling services

The existing worker endpoint, token, heartbeat and execution settings are consumed by the [manager / worker services](workers.md). `serve` receives the credential-file mapping and TLS listener settings as CLI arguments; `worker` requires a durable local `--state-directory`. T007 adds the storage mode and input-limit fields above, with no submission resource changes. See [staged inputs](staged-inputs.md) for protocol and migration details. Workers execute sequentially, bounded by all admission quotas. Recovery remains T006.
