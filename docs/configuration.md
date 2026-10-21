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
| `storage` (required) | Required absolute `work_root`; `mode=shared` (`shared` or `staged`); boolean `preserve_failed_jobs=true`; positive integers `max_input_files=10000`, `max_input_file_bytes=1073741824`, `max_input_total_bytes=10737418240`; positive output limits `max_output_files=10000`, `max_output_file_bytes=1073741824`, `max_output_total_bytes=10737418240` |
| `workers` (required nonempty list) | Each item requires unique string `id`, positive integer `cpu_slots`, positive `memory_budget_gib`, positive integer `max_concurrent_jobs`; boolean `enabled=true`, string list `labels=[]` |
| `job_defaults` | Positive integers `mpi_ranks=4`, `omp_threads_per_rank=1`, `time_limit_seconds=86400`; positive `memory_gib=8`; nonnegative integer `max_retries=0` |
| `scheduler` | `policy=fifo_least_loaded` (only supported policy); boolean `account_external_load=true`; `max_cpu_utilization_percent=80` in [0,100]; nonnegative integer `min_slots=0`, `reserve_slots=0`; nonnegative `reserve_memory_gib=4`; positive `metrics_max_age_seconds=30` |

Memory and timing settings accept finite integers or floats except fields explicitly described as integers. Resource CPU slots equal MPI ranks multiplied by OpenMP threads per rank. These values describe reservations, not enforced OS limits. The scheduler API applies these settings to supplied snapshots; the [polling services](workers.md) supply network dispatch and Linux monitoring.

## Worker schema

See [worker.yaml](../examples/worker.yaml). Required fields: `worker` (same capacity schema as each manager worker), `manager_endpoint` (HTTP(S) URL without embedded credentials), absolute `work_root`, absolute `token_file`, and `execution`. Positive `heartbeat_interval_seconds` defaults to 10. Positive finite `result_request_timeout_seconds` defaults to 300 and controls staged result-request socket waits, including commit and replay; increase it when output verification or manager contention takes longer. It does not change poll or input-download timeouts. `storage_mode` defaults to `shared` and accepts `shared` or `staged`. Positive integer `max_input_files=10000`, `max_input_file_bytes=1073741824` and `max_input_total_bytes=10737418240` independently bound staged downloads. Positive integer `max_output_files=10000`, `max_output_file_bytes=1073741824` and `max_output_total_bytes=10737418240` bound result enumeration and uploads independently. [Staged mode](staged-inputs.md) executes after input verification and [collects results](staged-results.md).

`execution` requires absolute `lammps_path` and `mpi_launcher_path`; `lammps_args` and `mpi_args` default to empty string lists. `environment` is a string-to-string mapping, copied into immutable pairs. Values may be empty (for example, `CUDA_VISIBLE_DEVICES: ""`) or contain only whitespace and are preserved exactly; NUL characters and non-string values are rejected. Names must remain nonempty strings without NUL. Keep secrets out of environment entries committed to version control; reference a protected token file using `token_file`. No secret contents are loaded by validation. No arbitrary shell command is accepted.

Elements of `lammps_args` and `mpi_args` may be empty or whitespace-only strings and are preserved exactly. NUL characters and non-string elements remain invalid; required paths and labels still require nonempty strings.

Configuration is intended to take effect on service restart once services exist. The scheduler reconciles worker, manager, and detected hardware capacity conservatively; callers supply worker snapshots.

## Submission API

Input paths that normalize to the work directory itself (`.`, `./`, or `././`) are rejected. Relative file paths such as `./in.lmp` remain valid in this API. Batch submission normalizes them before recording file identity.

`load_config(path, 'manager')` returns a frozen `ManagerConfig`. `create_job_specification(config, job_id=..., work_directory=..., input_file=..., input_sha256=..., resource_overrides=...)` creates a frozen `JobSpecification` with a separately copied, validated `Resources` value. Work directories must be absolute, input files relative within them, and input identifiers 64 hexadecimal SHA-256 characters. The caller supplies the digest; this API does not inspect remote files or verify input contents. The local batch submission and executor calculate and verify actual input identity.

Overrides accept only resource fields. Reloading defaults or mutating the original override mapping cannot alter an existing specification. Job identifiers and input identities are recorded here; uniqueness, persistence, attempts, and runtime state are handled by the SQLite store. Executable settings remain worker configuration. The submit command creates and persists specifications from batch manifests.

## Polling services

The existing worker endpoint, token, heartbeat and execution settings are consumed by the [manager / worker services](workers.md). `serve` receives the credential-file mapping and TLS listener settings as CLI arguments; `worker` requires a durable local `--state-directory`. T007 adds the storage mode and input-limit fields above, with no submission resource changes. See [staged inputs](staged-inputs.md) for protocol and migration details. Polling workers execute shared and staged attempts concurrently, bounded by effective CPU, memory and `max_concurrent_jobs` limits on both manager and worker. Each launch rechecks aggregate unreleased reservations and fresh local metrics; setting `max_concurrent_jobs: 1` retains sequential polling-worker execution. The `run-local` command remains sequential. No new configuration keys are needed for T010. T006 consumes submitted time limits and retry budgets; see [recovery and operations](recovery.md).

## Per-CPU admission (T011)

`max_cpu_utilization_percent` applies strictly to each eligible logical CPU,
not to the host average. `min_slots` and `reserve_slots` default to zero. YAML
rejects negative, boolean and noninteger slot values and a minimum above any
configured worker CPU quota. At runtime a minimum above the smaller manager,
worker or detected capacity blocks admission (`minimum_exceeds_capacity`).
Configuration changes take effect on restart and never rewrite submitted jobs.

When `account_external_load=true`, count CPUs below the threshold as F and deduct
unreflected starts P. A job requesting J slots needs `max(0, F-P) >= J+reserve_slots`.
An admission with total unreleased reservations R satisfying `R+J <= min_slots`
may bypass that load/reserve gate, but still needs valid, fresh per-CPU monitoring
and all quota, memory, concurrency and online/pause checks. A job crossing the
minimum must fit normally in full. The reserve is best effort and yields to the
minimum. No running work is stopped or resized when load increases.

Setting `account_external_load=false` disables both the per-CPU load gate and its
floor exception; quotas, memory, freshness and all other safety checks remain.
Aggregate CPU utilization remains telemetry and a worker ranking input only.
Upgrade managers and workers together: older metrics lacking per-CPU data fail
closed under the enabled policy, even with a positive minimum. There is no
fallback to host-average admission. Older managers do not enforce these settings.

## Worker staged cleanup (T012)

Worker top-level `cleanup_staged_files` is a strict boolean (default `true`). It enables bounded background reclamation of worker-owned staged attempt files only after confirmed process termination and durable manager result acknowledgement. Shared mode, manager results and retained worker journal/launch fences are unaffected. Set it to `false` and restart to retain staged files; reenable to resume pending cleanup. Existing trees without durable ownership records remain retained. Configuration changes do not rewrite submission snapshots. See [eligibility, restart, safety and diagnostics](staged-results.md#worker-staged-cleanup-t012).

## Infrastructure recovery policy (T014)

New manager submissions snapshot this top-level policy into each immutable job
specification, independently of `job_defaults.max_retries`:

```yaml
infrastructure_recovery:
  max_retries: 1
  delay_seconds: 30
```

`max_retries` is a nonnegative integer; zero disables automatic recovery.
`delay_seconds` is a finite positive number. Defaults are one recovery and a
30-second delay. Configuration changes affect future submissions only. Existing
snapshots without this field decode with zero infrastructure recoveries and are
never rewritten or implicitly opted in. No legacy opt-in command is provided.
Application failures and input errors do not consume this separate policy or
trigger automatic retries. Prelaunch returns consume neither retry budget.
See [recovery](recovery.md#automatic-infrastructure-recovery-t014) for evidence,
inspection and limitations.

`scheduler.default_priority` defaults to 0 and accepts
a signed 64-bit integer. It controls dispatch ordering only; queued per-job
overrides and assignment snapshots are runtime SQLite data, not job specifications.

## Monitoring thresholds (T016)

Optional manager `monitoring.transfer_stall_seconds` (default 300) and
`monitoring.low_disk_gib` (default 5) must be finite positive numbers. They affect
runtime alert evaluation, never submitted job specifications. See
[monitoring](monitoring.md) for JSON fields, pagination, transfer accounting and
alert deduplication/recovery semantics.
