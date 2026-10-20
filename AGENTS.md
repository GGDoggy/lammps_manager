# Agent Development Rules

## Project context and reading order

This is a Python / Linux dispatcher for LAMMPS simulation batches. Read `README.md`, `docs/architecture.md`, and `tasks.md` before the relevant code and tests. `docs/workflow.md` describes the development process. `docs/architecture.md` is the permanent requirements and design reference; it distinguishes planned behavior from implemented capabilities. Treat `tasks.md` as the source of progress and verification evidence. Report discrepancies between documentation and code; describe planned capabilities accurately.

## Language policy

- Use Traditional Chinese for temporary user-facing material, including chat replies, progress updates, clarification questions, completion summaries, and design drafts prepared for user discussion.
- Use English for persistent repository documentation and agent-facing material, including README files, files under `docs/`, agent instructions, task records, implementation notes, and review records.
- A file's location in the repository does not make it permanent documentation. Before retiring a temporary draft, transfer decisions still needed into permanent English documentation and update references. Do not delete the draft while requirements still depend on it.
- Preserve technical identifiers, commands, paths, and quoted source text as needed. Follow any explicit language instruction from the user for the current task.

## Working process

- Use one agent by default to plan, implement, verify, and self-review in sequence. A multi-agent orchestration framework is unnecessary.
- Check Git status before editing and preserve existing user changes. Select one task within the user's authorized scope, mark it `in_progress`, and describe its scope, steps, and observable acceptance criteria.
- Handle small fixes directly. For changes across modules or affecting reliability, add an execution plan and material decisions to the corresponding entry in `tasks.md`. Avoid duplicate planning documents.
- Run relevant tests and `make check`, then review the changes. Fix findings and repeat affected checks. Record actual commands, outcomes, and limitations before marking a task `done`.
- Record blockers and next steps. When real machines or LAMMPS are unavailable, report only the local verification actually performed.
- Complete work within the authorized scope without requesting approval at every step. A backlog entry alone does not authorize implementing all later features.

## Implementation conventions

- Target Python 3.10+ with a `src/lammps_manager` layout. Prefer the standard library; add dependencies when needed and document the reason.
- Add modules with their features. Avoid speculative plugins, repository interfaces, dependency injection, or microservice layers.
- Annotate public functions with types. Surface clear failure reasons and avoid silently swallowing errors.
- Use `unittest` under `tests/`. Test observable behavior such as state transitions, resource accounting, and process management. Cover the reproduction scenario when fixing a bug.
- Default tests must not connect to workers, launch real MPI / LAMMPS, or require credentials. Run external integration tests explicitly and separately.
- Document the availability of new commands and settings. New configuration must not rewrite previously submitted job specifications.

## Code Review Rules

- Dispatch must reserve resources atomically. Repeated delivery of an attempt must not start another execution.
- Worker disconnection does not establish job completion. Confirm process termination before releasing reservations or rerunning on another machine.
- Reserve CPU slots using MPI ranks × OpenMP threads. Low instantaneous CPU utilization must not offset reservations.
- Use subprocess argument arrays. Do not concatenate job inputs into shell commands.
- Cancellation must cover the complete MPI process group. Failures and retries must preserve earlier output.
- Keep settings in config, submission snapshots in job specifications, and runtime state in SQLite. Keep credentials out of version control.
- Only delete real simulation output, start or stop real jobs, or change external deployments when the current user authorization covers those actions.

## Common commands

```bash
make check
make help
# Without make:
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m lammps_manager --help
```
