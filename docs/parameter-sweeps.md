# Cartesian parameter sweeps (T020)

`submit-sweep` generates independent queued jobs; it never launches simulations.
Existing list-form `submit` manifests remain unchanged.

```bash
PYTHONPATH=src python3 -m lammps_manager submit-sweep --config manager.yaml sweep.json --dry-run
PYTHONPATH=src python3 -m lammps_manager submit-sweep --config manager.yaml sweep.json
```

Example `sweep.json` (source paths are relative to this manifest):

```json
{
  "version": 1,
  "source_directory": "abp-template",
  "input_file": "in.lmp",
  "template_version": "ideal-abp-v1",
  "parameters": {"temperature": [0.5, 1.0], "density": [0.1, 0.2, 0.3]},
  "repetitions": 4,
  "seed_base": 1000,
  "max_jobs": 10000,
  "resources": {"mpi_ranks": 1, "omp_threads_per_rank": 1}
}
```

The UTF-8 primary input might contain:

```text
variable temperature equal {{temperature}}
variable density equal {{density}}
variable seed equal {{seed}}
variable repetition equal {{repetition}}
```

All fields above except `max_jobs` and `resources` are required. Unknown fields,
duplicate JSON keys, empty parameter maps/lists, reserved or invalid parameter
names, nested/null/nonfinite values and invalid resource overrides are rejected.
Parameter names match `[A-Za-z_][A-Za-z0-9_]*`; `seed` and `repetition` are reserved.
Strings render verbatim, booleans as `1`/`0`, integers in decimal and finite floats
with Python's locale-independent `str(float)` representation (including exponent
notation). Strings cannot contain NUL, CR, LF or `{{` / `}}` delimiters. Quoting
for LAMMPS is the template author's responsibility. Only literal `{{name}}`
placeholders are supported, without whitespace or expressions. Every parameter
must appear, `{{seed}}` is required, and unknown/malformed placeholders fail.
Auxiliary file contents are copied without rendering.

Parameter names are sorted lexicographically; list order is preserved and the
last axis advances fastest. Each combination emits repetitions `1..repetitions`.
Combination indices are 1-based; selected list indices and the global ordinal
are 0-based. Repeated values at different list positions remain distinct.
The example creates 6 combinations and 24 jobs, with seeds 1000..1023. Execution
order is determined by the existing scheduler, independently of expansion order.

`repetitions`, `max_jobs` and `seed_base` are positive integers, excluding booleans.
The default maximum is 10000 jobs; an explicit positive override permits larger
plans. Counts are checked before bounded expansion. The complete seed range
`seed_base + ordinal` must fit `1..900000000`. This supplies different seeds within
one sweep, without proving statistical independence or capturing RNG state.
Separate submissions can intentionally reuse seeds. Multiple RNG streams require
a future extension.

Dry-run validates the entire plan, source, resources, placeholders and rendered
sizes. It prints JSON counts and the first 20 expansion coordinates, without
creating database state, temporary input trees or job directories. Submission
first captures and verifies the complete source file manifest, then uses that
private snapshot for every job. A changed source fails rather than mixing input
identities. Symlinks, special files and unsafe relative paths are rejected;
work_root cannot be inside the source. Only regular-file contents contribute to
identity; empty directories are not part of the manifest.

Each job gets a fresh ID, isolated input/output paths, immutable resource snapshot,
reservation and attempt history. Existing shared publication and staged transfer
are used. Staged limits apply to both the source and each rendered job. Input
storage scales with job count, and execution/results require additional space.
Preparation keeps one temporary source snapshot, not an in-memory Cartesian tree.

Submission JSON includes `sweep_id`, requested counts and `submitted` coordinates
mapped to job IDs. `inspect` exposes `specification.sweep`: the versions of the
manifest, expansion and seed derivation, combination/list/repetition coordinates,
values, seed, template version, original absolute source locator, complete sorted
source file manifest and source/template/rendered SHA-256 digests. The source tree
digest is SHA-256 of UTF-8 JSON of the sorted manifest records, using sorted keys
and compact separators. Input identity does not depend on the source locator.
Provenance resides in the stored submission specification; later configuration
or source changes do not rewrite it. Legacy snapshots decode with `sweep: null`.

Submission is **not atomic across jobs**. Validation failures submit nothing.
Filesystem/database failures or interruption during submission report a JSON
outcome in the CLI error, including sweep ID, committed mappings and the failed
coordinate. Completed jobs and outputs remain intact. Unregistered job trees may
remain after a publication/database failure; do not treat their presence as proof
of registration. Inspect persisted job specifications (`status`, then `inspect`)
to locate a sweep after interruption, including abrupt termination. A hard kill
can prevent the final error report. Check actual database state before manually
recovering missing coordinates. Repeating the command creates an entirely new
sweep; automatic deduplication/resume is unavailable.

Repetitions are independent jobs, not retry budgets. Retrying an eligible failed
job preserves its parameters, seed, specification and earlier output; it neither
replaces a repetition nor changes another job. `run-local` supports shared jobs;
staged jobs use the polling manager/worker. No new scheduling, continuation
capability, parameter-dependent resources, zipped axes, filters, multi-file
rendering or result analysis is introduced.
