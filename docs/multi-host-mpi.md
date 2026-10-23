# Explicit multi-host MPI

The opt-in Python API coordinates one staged job/attempt across registered workers.
The ordinary polling scheduler deliberately rejects nonempty `mpi_placement` with
`multi_host_protocol_unavailable`; CLI batch/sweep submission does not expose this
field. Independent jobs continue to use the existing worker protocol.

## Submission and lifecycle

Create a new `JobSpecification` with verified immutable staged `input_manifest`
and `input_sha256`. Supply a tuple of `MpiParticipant(worker_id, mpi_ranks,
memory_gib)` records: at least two distinct workers, rank and memory shares summing
to `Resources` totals. Existing specifications must never be rewritten to add
placement. Disable automatic infrastructure/application retries for this explicit
workflow; a disconnected attempt must be reconciled before considering another.

Construct `mpi_runtime.Session(manager_config, hosts)` on the manager host. Each
`Host` holds a literal IPv4 address, absolute installation root, common LAMMPS
SHA-256 and an operator-owned SSH argument tuple. Secrets belong in protected
files/SSH agent, not job specifications or command arguments.

1. Submit the immutable specification once with `Store.submit`.
2. Persist a unique attempt ID, then call `Session.reserve(job_id,
   attempt_id=attempt_id)`. This checks current generations, readiness, per-CPU
   admission, quotas and available memory, reserving all shares atomically.
3. Call `prepare(attempt_id)` to stage every participant and record readiness.
4. Call `start(attempt_id)` to recheck admission and authorize the single native
   MPI launch. First placement entry is leader.
5. Repeatedly call `observe(attempt_id)`. Transport errors retain the original
   attempt and reservations. Resume observation after interruption; do not submit
   a replacement or infer termination from disconnected SSH/worker heartbeats.
6. `cancel(attempt_id)` persists whole-group cancellation before delivery.
   Continue observing until every participant has positive termination evidence.
7. After a terminal group state, `collect(attempt_id)` retries original output
   delivery. `group_collected` must be true separately from process success.

Node helpers persist staging, launch claims, irreversible fences, boot/PID/start
and session identities, deadlines and outcomes in installation-local state.
A late/repeated launch cannot create another execution. Global release requires
all participant fences closed and termination confirmed. Missing process identity
remains unknown. Run or resume the coordinator to converge cancellation and
collection; node monitors enforce the submitted deadline independently.

Coordinator closure and cancellation publish irreversible node fences before
waiting for an ongoing staging transfer's lock. Staging checks the fence at
transfer progress boundaries; a blocked network read still needs to return or
time out before local closure can finish confirming never-started termination.
Existing retained request identities are checked before fencing and again under
the state lock. Repeated closure retains the same attempt and launch fence.

Before the first durable launch authorization, `prepare` and `start` can restore
an original allocation marked `unknown` by heartbeat expiry. Restoration checks
fresh admission, the original participant generations and retained reservations
in one SQLite transaction. Cancellation, terminated members and prior launch
authorization prevent this restoration; an authorized disconnected attempt must
continue through observation rather than obtain new launch rights.

Node deadlines continue through launcher exit while ranks remain. Cancellation
may terminate a remaining launch session using a retained, still-live rank whose
boot/PID/start/session identity matches. Missing or reused identities do not grant
signaling rights or establish termination; unresolved reservations remain held.

## Runtime and results

The measured profile uses OpenMPI 4.1.2, an identical LAMMPS executable on both
hosts, identical absolute installation paths, direct IPv4 MPI TCP and staged
local filesystems. `--noprefix` keeps OpenMPI's generated shell environment prefix
outside the strict daemon-only launcher. A dedicated peer key has a forced
`mpi-peer-gate`, restricted source and no forwarding/PTY; only prepared attempt
ORTE daemon launches are accepted. Installations require the `mpi-ssh`,
`mpi-peer-gate` and `mpi-rank` wrappers. This is an explicit operator API, not an
automatic key-provisioning or public worker command service.

All participants' collected files appear below
`results/work/mpi-participants/<worker-id>/`, preserving their original relative
paths. Leader stdout/stderr/process/outcome aliases support existing inspection.
Publication waits for all manifests and enforces combined result limits. Worker
copies remain retained in this deployment. Rank proof files record actual MPI
rank/world size, host, boot/process identity and CPU affinity before LAMMPS exec.

[V010 deployment](deployment.md) records real two-host success, cancellation and
manager-transport interruption. It establishes this bounded profile, not arbitrary
MPI distributions, scientific workloads, power-loss durability or automatic
multi-host scheduling.
