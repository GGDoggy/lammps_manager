# Lightweight Agent Workflow

## Approach and sources

Use one agent in a plan → implement → verify → review → update records loop. This document-based workflow is adapted to this project and requires no agent framework, model API calls, or persistent services for multiple agent roles. It fits a single Python repository that starts with local execution and adds workers and recovery incrementally, with independently verifiable steps.

References: OpenAI's [Iterating Development Workflows with Codex](https://developers.openai.com/cookbook/examples/codex/iterating-development-workflows-with-codex) and [AGENTS.md documentation](https://learn.chatgpt.com/docs/agent-configuration/agents-md), consulted on 2026-10-20. These describe AGENTS.md as persistent guidance and other planning files as adaptable conventions. This repository combines phase plans, decisions, and results in `tasks.md` and retains the existing `plan.md` to reduce duplication. It adapts the example without adopting its layered harness or approval gate at every step.

## Document ownership

| File | Purpose |
|---|---|
| `README.md` | Available capabilities, execution, and verification |
| `AGENTS.md` | Agent instructions, language policy, and reliability checks |
| `plan.md` | Temporary user-facing design draft in Chinese: proposed goals, architecture, configuration boundaries, and milestones |
| `tasks.md` | Small tasks, acceptance criteria, execution plans, decisions, and verification records |

Use English for permanent repository documentation and agent-facing records. Use Traditional Chinese for temporary user-facing material, including progress reports, completion reports, and design drafts such as `plan.md`. This classification depends on purpose and lifecycle, not file location. See `AGENTS.md` for the full policy.

## Development cycle

1. Read the context documents and Git status to establish the authorized scope and current capabilities.
2. Select a task and mark it `in_progress`. Describe expected behavior, affected files, implementation steps, and verification. Record unconfirmed environment assumptions.
3. Implement the minimum functionality needed for that task. For bugs or significant logic, first add a test that reproduces the issue, then implement and refactor.
4. Run relevant tests and `make check`. Review the actual changes. Before the first commit, also inspect new files directly because ordinary `git diff` omits untracked files.
5. Review reliability, error paths, and documentation consistency using `AGENTS.md`. Fix findings and rerun affected checks. This is self-review by the same agent unless explicitly arranged otherwise; do not label it independent review.
6. Record actual commands, outcomes, limitations, and next steps in `tasks.md`. Mark the task `done` only after meeting its acceptance criteria. Continue to the next task when it is within the current authorized scope.

Use `todo`, `in_progress`, `blocked`, and `done` statuses. Keep at most one task `in_progress`. While `plan.md` is the current planning reference, keep its proposed architecture consistent with implementation decisions. As features are implemented, document lasting decisions in permanent English documentation. Before retiring the temporary draft, transfer any requirements still needed and update all references, including agent reading instructions and task acceptance criteria.

## Task template

```markdown
### Txxx: Observable outcome

- Status: todo
- Dependencies: prerequisite task IDs or none
- Goal and scope:
- Acceptance criteria:
- Execution plan: add steps and expected affected files when starting
- Decisions / blockers:
- Verification record: add actual commands, outcomes, and unverified areas when finishing
```

## Starting the next task

Ask the agent to read `AGENTS.md`, `plan.md`, and `tasks.md`, implement T001, run tests and review, and update the records. You can authorize multiple tasks while retaining separate acceptance records for each. Use the current checkout for routine work. Create a `codex/` branch when isolation is useful; a new chat or worktree for each step is unnecessary.
