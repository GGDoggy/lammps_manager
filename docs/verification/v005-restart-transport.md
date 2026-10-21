# V005: Process restart and transport interruption

Passed on 2026-10-21 (Asia/Taipei), after V004, using fresh staged roots under `/tmp/lammps-v008-v005-eOAQn2xX/v005` on allegro. [JSON evidence](v005-restart-transport.json) preserves exact OS process commands/PIDs, fault timeline, manager/worker durable records, result manifests/hashes and proxy events. The [final audit](v008-v005-final-audit.json) verifies all source modules, all ended sessions, empty reservations and closed listeners. The executed [restart harness](../../tools/verification/restart_acceptance.py) and [proxy](../../tools/verification/fault_proxy.py) are retained byte-for-byte.

The CLI manager and worker ran as independent OS processes with separate manager/worker storage roots. A third OS process served a test-owned TLS proxy on loopback port 18449 forwarding to the dedicated TLS manager on 18448. Both HTTPS legs verified the trusted dedicated certificate; bearer authentication remained active. Fault controls applied only to this private proxy. No firewall, existing service or host-wide network configuration changed. Four affinity-visible CPUs, four CPU slots, 1 GiB reserved-memory budget and two-job capacity were configured; cases ran sequentially with two ranks × one thread, 0.1 GiB per job and at most a 30-second limit.

| Case | Fault and observed recovery |
|---|---|
| Manager restart | Stopped the actual manager process while real MPI ran, waited 2.5 seconds and confirmed its unresolved reservation remained. A new manager PID reopened the original database and reconciled the same running attempt; explicit CLI cancellation then terminated and collected it. |
| Worker restart | SIGKILL ended the actual worker process while MPI survived. After three seconds the manager marked the original attempt unknown and retained its reservation. Boot/PID/start-tick/session evidence positively identified the live launcher. A new worker PID reopened the same journal, reconciled it as running and enforced its persisted 15-second time limit. No replacement execution was created. |
| Contact outage | Proxy returned 503 for all traffic beyond the two-second heartbeat timeout. Manager state became unknown with the original reservation retained. Restoring traffic recovered the same running attempt, which was then explicitly cancelled and collected. |
| Partial artifact upload | Proxy forwarded 291 of a declared 583 bytes then closed the actual upstream TLS connection. Manager did not publish complete results. Subsequent result requests were held while worker output and incomplete collection were recorded. Restoring traffic replayed upload/commit successfully. |
| Lost successful commit response | Proxy received upstream HTTP 200 with `status: collected`, then closed downstream without sending the response. Manager publication was durable while worker collection remained unacknowledged and its files remained present. Held retries made this state observable. Restoring traffic replayed acknowledgement and allowed cleanup without relaunch. |

Every job retained one original attempt. The three operational interruption cases finished cancelled with termination evidence; the two 1000-step artifact cases succeeded with exit 0 and expected output. Every manager publication file matched its manifest size and SHA-256. Cleanup was enabled through worker YAML from initial launch: all five worker trees were removed only after acknowledgement, with durable journal records/launch fences retained. The worker-restart case's recovered exit status is not claimed as success; it ends through confirmed time-limit cancellation.

Final manager and worker reservations were empty. All five recorded MPI sessions had no live members, no test-root process remained and both dedicated listeners were closed. These are real OS process restarts and real TLS transport failures, not in-memory object reconstruction or a Python exception substituted for response loss. They do not establish host reboot/power-loss durability, cross-host network behavior, production deployment readiness or large-artifact performance.

```bash
PYTHONPATH=src .venv/bin/python restart_acceptance.py
PYTHONPATH=src .venv/bin/python final_audit.py
```

The initial automatic review rejected source transfer; the user explicitly authorized the destination and bounded isolated operations before any transfer or real test execution. Historical outputs and deployments remain untouched. Only new, acknowledged V005 worker copies were reclaimed. No commits or pushes were made.
