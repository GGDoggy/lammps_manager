# Development Tasks

See `docs/workflow.md` for instructions and the task template. Current architecture requirements are maintained in `docs/architecture.md`; exact implemented configuration behavior is documented in `docs/configuration.md`. Task status does not imply remote integration has been verified.

## T000: Initialize the repository and agent workflow

- Status: done
- Dependencies: none
- Goal and scope: establish persistent agent guidance, the task cycle, Python package entry points, and local / CI checks.
- Acceptance criteria: CLI help and version work without third-party runtime dependencies; unsupported commands are rejected; `make check` passes; README distinguishes current capabilities from plans.
- Execution plan: consult official workflow and AGENTS.md documentation; preserve plan.md; add workflow and task documents; create the src package, CLI, tests, and CI; verify and review all new files.
- Decisions: use one agent and one task list; Python 3.10+ matches the current environment; start with unittest. No empty modules for future features or agent SDK were introduced.
- Verification record (2026-10-20): `make check` passed 3 unittest tests, syntax compilation, and Git whitespace checks. `make help` and `PYTHONPATH=src python3 -m lammps_manager --version` worked. Additional checks of 13 new / updated files found no trailing whitespace or broken local Markdown links. Self-review confirmed README accurately described dispatch as pending and plan.md reflected the scaffold. An assertion was adjusted to ignore argparse's automatic whitespace wrapping before tests passed. Editable installation, GitHub CI, and remote / LAMMPS integration were not run. Git had no commits, so new files were inspected separately. No commit or push was performed.

## T001: Validate configuration and create job specifications

- Status: done
- Dependencies: T000
- Goal and scope: implement manager / worker YAML configuration, type and value validation, resource defaults, and immutable submission specifications; provide example configuration and a validate-config CLI.
- Acceptance criteria: valid examples pass; negative values, incorrect types, unknown fields, duplicate worker IDs, and unsupported policies produce clear errors; MPI ranks × threads is correct; new config does not modify existing specifications; missing path / executable checks distinguish local validation from remote deployment checks.
- Execution plan: add strict YAML loading and frozen configuration / submission models in config.py and models.py; expose validate-config with explicit local filesystem checks; add portable examples, permanent schema documentation, and unittest coverage; run make check and review the diff. Acceptance includes invalid types / values / fields, duplicate IDs, unsupported modes / policies, CPU multiplication, immutable snapshots, and local versus remote path validation.
- Decisions / blockers: implemented section 3 of plan.md and transferred the schema / validation boundaries into docs/configuration.md. Use PyYAML>=6.0,<7 because the standard library does not parse YAML; safe loading rejects duplicate keys. Frozen dataclasses and copied resource overrides preserve submission values. Only shared storage and fifo_least_loaded are accepted; their execution behavior remains later work. Structural validation never probes remote paths; --check-local-paths checks the current host explicitly. Input SHA-256 is caller-supplied at this stage; actual generation / verification belongs to T003. CI now installs project dependencies before checking.
- Verification record (2026-10-20): make check passed initially with system PyYAML 5.4.1; downloaded declared PyYAML 6.0.3 into /tmp/task-manager-t001-deps and ran PYTHONPATH=/tmp/task-manager-t001-deps make check successfully (11 unittest tests, syntax compilation, git diff --check). Both example validate-config commands passed. Tests cover CLI errors, malformed / unsafe YAML, duplicate keys / workers, missing fields, unknown fields, types and ranges, unsupported policy / storage, endpoint validation, argument arrays, CPU multiplication, frozen snapshots after defaults / overrides change, and local directory / executable / token checks using temporary fixtures and /bin/true without launching it. Self-review covered tracked diffs and six new files, including separate whitespace inspection; corrected README dependency wording and CI installation. No remote workers, real LAMMPS / MPI, external deployment, GitHub CI, commit, or push were performed. Next authorized task would be T002; it was not started.

## T001 follow-up: Allow empty environment values

- Status: done
- Goal and scope: fix rejection of valid empty worker environment values while preserving strict validation elsewhere.
- Acceptance criteria: empty and whitespace-only environment values survive loading unchanged; NUL and non-string values remain rejected; empty names and required paths remain rejected.
- Execution plan: add a regression test and confirm failure, allow empty strings only for environment values, document the behavior, run targeted tests and make check, and self-review.
- Verification record (2026-10-20): `PYTHONPATH=src python3 -m unittest discover -s tests -p test_config.py -v` first reproduced the empty-value failure, then passed all 8 configuration tests after the fix. `make check` passed all 12 tests, syntax compilation, and Git whitespace checks. Self-review confirmed only environment values opt into empty strings; names and required paths retain their existing checks. Regression coverage also rejects NUL, null, boolean, and numeric values and preserves whitespace exactly. Updated docs/configuration.md. No remote or LAMMPS integration was run.

## T001 review fixes: Argument values and input path validation

- Status: done
- Goal and scope: address both review findings without changing unrelated pending work.
- Acceptance criteria: both launcher argument lists preserve empty and whitespace-only strings, reject NUL and non-string elements, and leave required strings strict; submission rejects `.`, `./`, and `././` while accepting relative input files.
- Execution plan: reproduce both failures with regression tests; restrict empty-string support to argument fields; compare normalized input paths; update configuration documentation; run targeted tests and make check, then self-review.
- Verification record (2026-10-20): `PYTHONPATH=src python3 -m unittest discover -s tests -p test_config.py -v` reproduced rejection of empty launcher arguments and acceptance of `./` and `././` before the fix; after the fix all 10 configuration tests passed. `make check` passed all 14 tests, syntax compilation, and Git whitespace checks. Self-review confirmed both argument fields preserve values exactly while NUL / non-string elements, empty labels, and empty executable paths remain rejected; normalized directory-only input paths are rejected while relative file spellings are preserved. Updated docs/configuration.md. Preserved existing uncommitted work; no real MPI / LAMMPS execution or remote integration was performed.

## T002: Persist SQLite jobs and attempt state

- Status: done
- Dependencies: T001
- Goal and scope: persist jobs, attempts, events, and resource reservations; define legal state transitions and atomic dispatch transactions.
- Acceptance criteria: data survives restarts; illegal transitions are rejected; rolled-back transactions leave no reservations; repeated operations on the same attempt create no duplicate records.
- Execution plan: implement a standard-library SQLite store with immutable specification snapshots, attempts, events, and reservations; use immediate transactions for atomic assignment and transitions; test restart persistence, rollback, concurrent assignment, idempotency, legal transitions, and termination evidence; document the Python API and boundaries; run make check and self-review.
- Decisions / blockers: T002 provides storage primitives only; scheduler admission policy, execution, automatic retries, and network recovery remain later tasks. Standard-library SQLite uses schema version 1, foreign keys, and BEGIN IMMEDIATE write transactions. Unknown and cancelling attempts retain reservations until caller-supplied termination evidence is recorded. Evidence is not independently verified by the store. Each job supports one attempt for now; retry support will require an explicit API and schema migration in T006. Attempt directories are unique normalized absolute strings, with no filesystem creation or deletion. Future scheduler capacity checks must join the assignment transaction. See docs/storage.md for the Python API and exact transition table. No new CLI commands or configuration keys were added.
- Verification record (2026-10-20): targeted PYTHONPATH=src python3 -m unittest discover -s tests -p test_store.py -v passed the initial 9 storage tests. Final make check passed all 27 tests (13 storage tests plus 14 existing tests), syntax compilation, and git diff --check. Storage coverage includes close/reopen durability, immutable snapshots, legal and illegal transitions, conflicting identities, identical and concurrent replay, concurrent competing assignments, late injected assignment/completion failures with complete rollback, terminal evidence, resource retention for unknown/cancelling, event reasons/results, invalid direct submissions, and unsupported schema versions. A separate Python check passed whitespace and local Markdown links across all six changed/new files. Self-review inspected tracked diffs and new store, tests, and storage documentation; added direct submission validation, durable event details, and explicit close/reopen verification. README and architecture now identify SQLite persistence as implemented while keeping execution and scheduling pending. Initial Git status was clean. No remote workers, real MPI / LAMMPS, external deployments, commit, or push were performed. T003 was not started.

## T002 review fix: Normalize Linux attempt-directory roots

- Status: done
- Goal and scope: prevent leading double slashes from bypassing attempt-directory uniqueness while preserving existing uncommitted work.
- Acceptance criteria: single- and multiple-slash roots identify the same stored directory; equivalent assignment replays remain idempotent; conflicting assignments leave job state, reservations, and events unchanged.
- Execution plan: add a regression test and reproduce the failure; normalize leading slashes without accessing the filesystem; document the behavior; run storage tests and make check, then self-review.
- Decisions: collapse all leading slashes to one before Path normalization and assignment identity checks. No filesystem access, schema change, or submission snapshot changes are needed.
- Verification record (2026-10-20): `PYTHONPATH=src python3 -m unittest discover -s tests -p test_store.py -v` first reproduced the root-alias bug (one failure and two replay errors), then passed all 14 storage tests after the fix. `make check` passed all 28 tests, syntax compilation, and Git whitespace checks. The regression covers single-, double-, and triple-slash initial assignments, equivalent replays, conflicting directory assignments, and unchanged job state, reservations, and event history after conflicts. Self-review inspected the changed implementation, regression, and documentation, with a separate whitespace check for untracked files. Existing uncommitted changes were preserved. No real output directories, jobs, remote workers, or deployments were touched.

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
- Acceptance criteria: cover all thresholds in docs/architecture.md under Scheduling and resource accounting; exclude stale, offline, and paused workers; low instantaneous CPU usage does not offset reservations; available memory is not deducted twice; unschedulable reasons can be inspected.
- Execution plan: define ordering rules, a test matrix, and store integration when starting.
- Verification record: not started.

## T005: Add workers and communication across machines

- Status: todo
- Dependencies: T004
- Goal and scope: manager API, worker polling, authentication, heartbeats, system monitoring, local resource rechecks, pause / resume, and shared storage checks.
- Acceptance criteria: unregistered workers are rejected; duplicate assignment does not duplicate execution; local tests with multiple workers finish a batch. Before deployment, confirm Deployment prerequisites in docs/architecture.md, then verify with two real workers and record that evidence separately.
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

## D001: Consolidate the design draft into permanent documentation

- Status: done
- Dependencies: T001
- Goal and scope: preserve the draft's lasting requirements in English documentation, update active references and reading instructions, then delete the temporary draft. No pending runtime features are authorized by this documentation task.
- Acceptance criteria: architecture, configuration boundaries, scheduling rules, recovery guarantees, milestones, and deployment questions remain discoverable; current capabilities remain distinct from plans; no active references require the deleted draft; make check passes.
- Execution plan: compare the draft with configuration documentation and code; write docs/architecture.md for remaining design requirements; update README, workflow, agent instructions, and backlog references; remove the draft only after transfer; check links and whitespace, run make check, and self-review coverage.
- Decisions: docs/architecture.md now owns the lasting design requirements; docs/configuration.md remains the source for accepted schema fields. The design explicitly identifies requirements without current schema or runtime support. Removed plan.md after transfer; remaining mentions in earlier task records are historical evidence, not active dependencies. No runtime features were implemented.
- Verification record (2026-10-20): make check passed all 14 unittest tests, syntax compilation, and git diff --check. A Python documentation check verified local Markdown link targets, whitespace (including the new untracked architecture document), and draft removal. rg located only historical draft mentions in earlier task records. Self-review compared all eight draft sections with architecture.md, configuration.md, and the backlog, and inspected tracked diffs and the new document. Updated the stale workflow example that still requested completed T001. No remote, MPI / LAMMPS, or deployment checks were performed. T002 remains pending and requires separate authorization.
