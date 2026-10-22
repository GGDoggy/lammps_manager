# V007: isolated documented service lifecycle

Passed on allegro on 2026-10-22, after V006, in private root
`/tmp/lammps-v007-rvzEw87u`. This verifies an actual disposable deployment under
systemd user services. No machine reboot, production change, installed unit file
or host configuration change was required.

The new [operator procedure](../service-lifecycle.md) fills the earlier gap
between foreground CLI examples and reproducible service supervision/restart.
The host already provided a running user bus, Python 3.10+, Open MPI, LAMMPS,
OpenSSL, local storage and an available loopback port. A private venv used
PyYAML 6.0.3. The driver generated protected credential references and a dedicated
TLS certificate/key, then used CLI operations only for validation, service
startup and job submission. SQLite reads were used solely for evidence/audits.

## Reproduction and results

From the new source root:

```bash
PYTHONPATH=src .venv/bin/python service_acceptance.py
PYTHONPATH=src .venv/bin/python lifecycle_audit.py /tmp/lammps-v007-rvzEw87u
```

[Machine-readable evidence](v007-service-lifecycle.json) contains the exact
configuration-validation, `systemd-run`, CLI, service-control, PID and log
commands. The unique transient units were:

- `lammps-v007-rvzEw87u-v007-manager.service`
- `lammps-v007-rvzEw87u-v007-worker.service`

Each unit ran the documented foreground CLI, with explicit source/trust paths,
`Restart=on-failure`, `KillMode=process`, a 45-second stop timeout and a
900-second runtime bound. Manager and worker kept their original SQLite state
and configuration identity through every restart. The private TLS listener was
`127.0.0.1:18448`. Trust verification rejected the self-signed certificate
without the dedicated CA, and verified HTTPS with an invalid bearer credential
returned HTTP 401. Actual workers authenticated successfully.

Two independent 1000-step real LAMMPS jobs each used two MPI ranks × one thread,
0.1 GiB and a 30-second submitted limit. Both succeeded with one original
attempt, one completion log, durable verified results and confirmed termination.
The first job's acknowledged worker tree remained while cleanup was disabled,
including after restarting both services. Changing only
`cleanup_staged_files: true` in the actual worker YAML and restarting its unit
removed that eligible tree. The second job was also collected and cleaned.
Manager results and both worker journals/launch fences remained.

The first submitted SQLite specification string was compared byte-for-byte
after both ordinary restarts and the configuration-change restart; it was
unchanged. Its SHA-256 remains in the evidence. No submission snapshot was
rewritten. Manager PID changed 236726 → 236768; worker PID changed
236730 → 236772 → 236776. The unchanged host boot identity was
`c5831636-0ddc-4c03-9c62-de8bd57e8277`.

[Sanitized service logs](v007-service.log) contain startup/stop and supervisor
messages. The driver checked that its bearer token was absent from all captured
commands and logs. Only token-file paths and TLS key paths appear in evidence;
no credential or private-key contents were exported.

## Shutdown and verification limits

Both units were explicitly stopped; their final MainPIDs were zero.
The separate [read-only audit](v007-final-audit.json) verified all 26 deployed
implementation/helper hashes, all 16 published result-file identities, both
empty reservation tables, two empty original MPI sessions, no root-owned test
process and closed ports 18448/18449. Every deployed source hash was also
recomputed against the local final source.

The [combined final audit](v006-v007-final-audit.json) also covers both earlier
V006 roots, including safe cancellation of its never-launched failed test and
convergence of its earlier successful original attempt. All reservations and
process sessions across the complete test scope are empty.

Final `make check` passed all 285 local tests, compileall and Git whitespace
checks. New drivers passed compilation; Markdown links, untracked-file
whitespace and evidence/source identity were checked separately. This is
self-review by one agent. The acceptance covers explicit service restarts and
configuration application, not physical reboot, power-loss durability,
production readiness, service-manager absence, multi-host MPI or upgrades.

Audit correction (2026-10-22): the historical standalone audit recorded output
hashes without comparing them with committed result manifests. The service
acceptance driver did compare its results with the manifests, but the historical
standalone audit is only an output hash inventory. The corrected auditor now
requires exact file sets, sizes and SHA-256 from manager SQLite manifests and
detects missing publication trees. This correction is locally regression-tested;
no corrected external-root audit was performed. Historical evidence and source
identities remain unchanged.
