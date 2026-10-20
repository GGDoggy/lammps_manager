# Development Tasks

See `docs/workflow.md` for instructions and the task template. Current architecture requirements come from the temporary user-facing design draft `plan.md`. Transfer lasting requirements into permanent English documentation and update task references before retiring that draft. Task status does not imply remote integration has been verified.

## T000: Initialize the repository and agent workflow

- Status: done
- Dependencies: none
- Goal and scope: establish persistent agent guidance, the task cycle, Python package entry points, and local / CI checks.
- Acceptance criteria: CLI help and version work without third-party runtime dependencies; unsupported commands are rejected; `make check` passes; README distinguishes current capabilities from plans.
- Execution plan: consult official workflow and AGENTS.md documentation; preserve plan.md; add workflow and task documents; create the src package, CLI, tests, and CI; verify and review all new files.
- Decisions: use one agent and one task list; Python 3.10+ matches the current environment; start with unittest. No empty modules for future features or agent SDK were introduced.
- Verification record (2026-10-20): `make check` passed 3 unittest tests, syntax compilation, and Git whitespace checks. `make help` and `PYTHONPATH=src python3 -m lammps_manager --version` worked. Additional checks of 13 new / updated files found no trailing whitespace or broken local Markdown links. Self-review confirmed README accurately described dispatch as pending and plan.md reflected the scaffold. An assertion was adjusted to ignore argparse's automatic whitespace wrapping before tests passed. Editable installation, GitHub CI, and remote / LAMMPS integration were not run. Git had no commits, so new files were inspected separately. No commit or push was performed.

## T001: Validate configuration and create job specifications

- Status: todo
- Dependencies: T000
- Goal and scope: implement manager / worker YAML configuration, type and value validation, resource defaults, and immutable submission specifications; provide example configuration and a validate-config CLI.
- Acceptance criteria: valid examples pass; negative values, incorrect types, unknown fields, duplicate worker IDs, and unsupported policies produce clear errors; MPI ranks × threads is correct; new config does not modify existing specifications; missing path / executable checks distinguish local validation from remote deployment checks.
- Execution plan: refine configuration structures, the YAML dependency choice, affected files, and test commands when starting.
- Decisions / blockers: follow section 3 of plan.md; remote paths that are not yet deployed cannot be assumed directly inspectable by the manager.
- Verification record: not started.

## T002: Persist SQLite jobs and attempt state

- Status: todo
- Dependencies: T001
- Goal and scope: persist jobs, attempts, events, and resource reservations; define legal state transitions and atomic dispatch transactions.
- Acceptance criteria: data survives restarts; illegal transitions are rejected; rolled-back transactions leave no reservations; repeated operations on the same attempt create no duplicate records.
- Execution plan: add the schema, transaction boundaries, and tests when starting.
- Verification record: not started.

## T003: Generate, execute, and inspect local batches

- Status: todo
- Dependencies: T002
- Goal and scope: separate working directories, submit / status / inspect commands, and a local executor; verify the full flow with fake jobs first.
- Acceptance criteria: a batch containing successful and failing jobs records correct states, times, exit codes, stdout, stderr, and separate outputs; reruns preserve earlier output; execution uses no shell command strings. When LAMMPS is available, run a separate small example and record the environment.
- Execution plan: add the CLI contract, directory layout, and tests when starting.
- Verification record: not started.

## T004: Implement scheduling logic and resource quotas

- Status: todo
- Dependencies: T003
- Goal and scope: FIFO, deterministic worker selection, CPU / memory / concurrency reservations, and external load policies; test with fake metrics first.
- Acceptance criteria: cover all thresholds in section 5 of plan.md; exclude stale, offline, and paused workers; low instantaneous CPU usage does not offset reservations; available memory is not deducted twice; unschedulable reasons can be inspected.
- Execution plan: define ordering rules, a test matrix, and store integration when starting.
- Verification record: not started.

## T005: Add workers and communication across machines

- Status: todo
- Dependencies: T004
- Goal and scope: manager API, worker polling, authentication, heartbeats, system monitoring, local resource rechecks, pause / resume, and shared storage checks.
- Acceptance criteria: unregistered workers are rejected; duplicate assignment does not duplicate execution; local tests with multiple workers finish a batch. Before deployment, confirm section 8 of plan.md, then verify with two real workers and record that evidence separately.
- Execution plan: choose minimal communication dependencies and define the API and worker persistence when starting.
- Verification record: not started.

## T006: Add recovery, cancellation, time limits, and retries

- Status: todo
- Dependencies: T005
- Goal and scope: unknown state on disconnection, manager / worker restart reconciliation, process identity checks, full process group cancellation, controlled retries, and deployment documentation.
- Acceptance criteria: disconnections and restarts do not duplicate execution; reservations are released only after confirming termination; cancellation and time limits clean up child processes; failure output is preserved; separately verify process cleanup with real MPI.
- Execution plan: describe failure scenarios, recovery protocols, and verification steps when starting; split into smaller tasks if needed.
- Verification record: not started.

After T001–T006, plan staged storage, a Web UI, fairness, and cgroups according to actual needs. Do not create speculative scaffolding for those features now.

## Documentation language update (2026-10-20)

Translated README.md, AGENTS.md, docs/workflow.md, and this agent-facing task list into English. AGENTS.md requires Traditional Chinese for temporary user-facing material and English for permanent documentation and agent-facing records. The user subsequently clarified that plan.md is a temporary user-facing design draft, so its Chinese language follows the temporary-material policy. Updated related descriptions and documented the requirement to transfer lasting decisions and update references before retiring it. These updates change documentation only.
