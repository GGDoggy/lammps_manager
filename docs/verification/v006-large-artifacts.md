# V006: bounded large-artifact transfer and recovery

Passed on allegro on 2026-10-22 in fresh private root
`/tmp/lammps-v006-lh8858Dt`, using real two-rank Open MPI / LAMMPS and distinct
manager/worker roots. The user agreed to the workload and numerical gates before
execution. No machine reboot or existing deployment change was performed.

| Gate | Declared maximum | Observed |
|---|---:|---:|
| Whole driver elapsed | 900 s | 47.280 s |
| RSS per manager/worker process | 512 MiB | all processes <= 32.28 MiB |
| Allocated test-root disk | 2 GiB | 429.85 MiB |
| Successful authenticated poll latency | 5 s | 0.059874 s |
| Gap between successful authenticated polls | 10 s | 0.326891 s |

RSS and disk are sampled peaks, not enforced cgroup limits. The conservative
RSS maximum includes the proxy as well as every manager/worker incarnation.
Disk sampling includes files and directory blocks under the complete private
root, including the source, venv, source data, input snapshots, temporary trees,
worker copies, upload spool and published results. Poll gaps include the planned
process-restart intervals. There were 187 resource samples, approximately 0.2 s
apart; raw `v006/samples.jsonl` remains in the test root.

## Workload and commands

The deterministic source generator in
`tools/verification/artifact_acceptance.py` writes one 67,108,864-byte file by
repeating 64 KiB SHA-256-derived blocks, 1000 4096-byte files, and the small
LAMMPS script. The 1002 input files total 71,205,190 bytes. LAMMPS runs 1000
steps, preserving these inputs in `work` and generating a data file and logs;
the 1009 result files total 71,244,839 bytes. This is the agreed workload only,
not evidence of universal throughput or arbitrary scientific-output scaling.

A fresh mode-0700 root received the current Python sources and explicit
acceptance drivers. A private `venv --without-pip` used PyYAML 6.0.3, without
system package changes. The root's `source-sha256.json` records all 24 executed
implementation/helper files. Commands from that root were:

```bash
PYTHONPATH=src .venv/bin/python artifact_acceptance.py
PYTHONPATH=src .venv/bin/python lifecycle_audit.py /tmp/lammps-v006-lh8858Dt
```

The driver invokes documented `submit`, `inspect`, `serve` and `worker` CLI
commands, retaining exact argv/PIDs in [machine-readable evidence](v006-large-artifacts.json).
It creates private tokens and a dedicated loopback certificate, keeps credentials
out of arguments/evidence, verifies both TLS legs, and uses ports 18448/18449.
Simulation accounting is two ranks × one thread, 0.1 GiB, with a 30-second limit.
The final [read-only audit](v006-final-audit.json) verifies all deployed source
hashes, 1009 result hashes, the empty original MPI session, both empty
reservation tables and closed listeners. Local recomputation matches every
recorded deployed source hash.

## Faults and observable acceptance

The same valid source tree was rejected under a 67,108,863-byte per-file input
limit: CLI exit 2, `Input snapshot count or size limit exceeded`, no submitted
job or reservation and no simulation launch. Accepted input retained its exact
immutable manifest.

The streaming test proxy cut the large input after 65,536 bytes and held later
input requests. No `inputs` tree was published, no launch was claimed and the
original reservation remained. An actual worker-process restart using its
original journal resumed the same attempt. The proxy later truncated the large
result upload after 65,536 bytes and held result operations. The manager had
not published `results`, collection was incomplete, and worker output remained.
Actual manager and uploader-process restarts recovered the original attempt.
All accepted files then matched their sizes and SHA-256 identities, including
all 1002 input identities in the published work tree.

There was one original attempt and one worker-journal `running` event, with
exit zero, one LAMMPS completion log and a confirmed empty process session.
After durable acknowledgement, worker YAML changed from
`cleanup_staged_files: false` to `true`; restarting the worker removed only the
new acknowledged worker tree, preserving its journal/launch fence and all
manager results. Both reservations tables were empty at shutdown.

Linux boot identity was unchanged throughout:
`c5831636-0ddc-4c03-9c62-de8bd57e8277`. Only test service processes restarted.

## Findings, fixes and earlier runs

The first root, `/tmp/lammps-v006-jXBMecYE`, exposed a real reliability defect:
synchronous input downloads outlasted heartbeat expiry, so the manager marked
the original attempt unknown and rejected subsequent downloads with HTTP 400.
No simulation started. The worker now sends authenticated heartbeat-only polls
between files and read/hash chunks, and applies cancellation commands. New
assignments stay durable until the normal poll accepts them. Individual
blocking I/O operations can still delay polling; this change does not claim
fully asynchronous inputs. A slow-download regression failed before the fix;
slow-download and cancellation/no-publication regressions pass afterward.

The second root, `/tmp/lammps-v006-jRpE142p`, reached successful simulation and
result interruption, but an acceptance-driver assertion incorrectly treated
the upload container as the complete result publication. The corrected check
uses `ATTEMPT/results`. Its original attempt was subsequently collected and
hash/session-audited without rerunning the simulation; outputs remain retained.
Neither earlier run is represented as a passing performance acceptance.

An initial dependency-copy preparation failed before execution because an older
private environment was unavailable. Later, temporary SSH trust files became
unavailable; automatic approval review rejected unverified reconnection. After
the user explicitly authorized first trust of the endpoint's current key, its
ED25519 fingerprint was pinned as
`SHA256:Q79M0dKjmxWwbxYTMUkfX+rfnOvSJXCt6CWxckILL1k` and strict checking resumed.
No independent historic host-key validation is claimed.

Local verification: the staging/results/cleanup focused run passed 70 tests;
the two final focused heartbeat/cancellation regressions passed; final
`make check` passed all 285 tests, compileall and Git whitespace checks
(`/tmp/v006-v007-final-check.log`). Self-review checked unchanged specifications,
atomic resource semantics, prelaunch cancellation, no partial publication,
original-attempt replay, bounded proxy buffers and manifest correspondence.
The never-launched first failed attempt was later cancelled through the normal
CLI/worker path after checking its absent launch claim, start time and process
metadata; its reservations are now empty and all raw files remain. See
[failed-run retirement](v006-failed-run-retirement.json),
[earlier original-attempt convergence](v006-earlier-convergence.json),
[raw final resource samples](v006-resource-samples.jsonl) and the
[combined shutdown audit](v006-v007-final-audit.json). Across all four V006/V007
roots, allocated disk was 949,600,256 bytes, every reservation table was empty,
all four actual MPI sessions were empty, no test process remained and both
ports were closed. Open MPI was 4.1.2 and LAMMPS 29 Sep 2021 Update 2.

Physical power loss, cross-host transport performance, production deployment,
larger files and other workload distributions remain unverified.

Audit correction (2026-10-22): the historical standalone audit above recorded
output hashes but did not compare them with committed result manifests. The
acceptance driver did perform manifest comparisons; the separate historical
audit is only a hash inventory for output integrity. The corrected auditor now
checks complete file sets, sizes and SHA-256 against manager SQLite manifests,
including missing publication trees. Local regressions cover the correction;
the corrected auditor has not been rerun on this external root. Historical JSON
and deployed source hashes remain unchanged.
