# Local batches

T003 provides explicit synchronous local execution, without a scheduling service
or remote connections. Use only trusted inputs, source directories and execution
configuration: simulations execute with the invoking user's permissions.

Create a JSON manifest containing a nonempty list:

```json
[
  {"source_directory": "case-a", "input_file": "in.lmp"},
  {"source_directory": "case-b", "input_file": "in.lmp",
   "resources": {"mpi_ranks": 2, "omp_threads_per_rank": 2}}
]
```

Source directories resolve relative to the manifest. Each tree must contain only
regular files and directories, without symlinks. Include all required auxiliary
inputs. Input files must be relative paths inside their source directory.
All entries are validated before copying; filesystem or database failures during
copying/submission can leave partial batches and preserved orphan directories.
Inspect status before resubmitting: each submission creates fresh job UUIDs.
Never place the storage work root inside a source tree.

The manager YAML supplies the local database path, storage work root and resource
defaults. The database parent must already exist. Submission creates work-root
parents as needed. The worker YAML supplies the allowed work root, execution paths,
fixed argument arrays and environment. Endpoint and token fields are structurally
validated but not used for local execution; no token is read or network contacted.

```bash
PYTHONPATH=src python3 -m lammps_manager submit --config manager.yaml batch.json
PYTHONPATH=src python3 -m lammps_manager status --config manager.yaml
PYTHONPATH=src python3 -m lammps_manager run-local --config manager.yaml --worker-config worker.yaml
PYTHONPATH=src python3 -m lammps_manager inspect --config manager.yaml JOB_ID
```

Commands return JSON. `run-local` visits queued jobs in submission order and runs
one at a time; it returns completed job IDs, including nonzero failures. Its zero
CLI exit status means the command finished; inspect job states to assess simulation
success. Launch errors produce failed attempts with reasons. Existing assigned,
running, unknown and terminal jobs are skipped. Running two invocations cannot
assign the same job twice; an assignment conflict terminates the losing invocation.
Use one local executor at a time. This is not capacity-aware scheduling: worker
CPU / memory limits, enabled flags, external load and concurrency policies are not
enforced. Time limits, cancellation, automatic retries and restart reconciliation
remain T004–T006 work; `max_retries` and time limits do not activate those features.

Each job has this layout under `storage.work_root`:

```text
JOB_ID/
  inputs/                  # copied submission inputs
  ATTEMPT_ID/
    execution.json         # argv, cwd, explicit environment overrides, output paths
    stdout
    stderr
    work/                  # separate execution copy; simulation output stays here
```

The specification stores a SHA-256 digest of the primary input file. Submission
checks the copied digest; execution checks it again before launch. Auxiliary files
are copied but not hashed. These directories are ordinary user-owned files, not
an OS-enforced immutable store. Do not modify submitted inputs. No output is
removed automatically. Resubmission creates new jobs rather than retrying an old
attempt, preserving earlier directories and database history.

Execution uses a subprocess argument array with a new process session:
`[mpi_launcher_path, *mpi_args, '-np', mpi_ranks, lammps_path, *lammps_args,
'-in', input_file]`. Empty arguments remain intact. The inherited environment is
merged with configured overrides; `OMP_NUM_THREADS` always uses the submission
snapshot. Only explicit overrides are recorded, not the full inherited environment.

SQLite records assignment and reservations before launch, then start / end times,
exit code, reasons and termination evidence. The executor waits for the launcher
and verifies that its process group no longer exists before recording success or
nonzero failure and releasing reservations. A remaining group or interrupted
execution becomes `unknown` and retains reservations. The current invocation then
stops starting jobs; subsequent jobs remain queued, and completed job IDs are
still returned. Interrupts continue to propagate to the caller. Failures confirmed
to occur before launch allow the batch to continue. This stop applies to the
current invocation only; it is not restart reconciliation. A crash can leave assigned
or running state; repeating `run-local` never launches that attempt again. There
is no recovery or manual-resolution CLI yet. Group existence checking is a local
safety check, not proof of real MPI descendant cleanup across hosts or daemons.
Inspect exposes specification, attempts, artifact paths and event history; artifacts
may be absent when preparation failed before opening output files.

Default tests use temporary fixtures and fake executable scripts. They do not run
real MPI / LAMMPS. Real integrations require separate explicit authorization and
environment evidence.
