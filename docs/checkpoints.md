# Durable continuation checkpoints (T018)

The `lammps_manager.checkpoints` API publishes checkpoints separately from process
termination and ordinary result collection. It currently accepts only T017's
released statistical ideal-ABP build, singleton layout and 600/1400/2000-step
boundaries or the measured uninterrupted 2000-step reference. Exact recipes,
other state sets and cross-host restoration fail closed. See [continuation](continuation.md) for the scientific limits.

A manager calls `expect(store, CheckpointContract(...))` for an assigned attempt
before launch. The immutable contract binds run/segment/job/attempt, global step
and time, recipe and renderer digest, executable/library/CPU fingerprint, complete
resource snapshot, optional parent checkpoint identity, and derived RNG stream
provenance. The released recipe requires one nonempty `checkpoint.restart`; it
has no omitted external, RNG or history-bearing fix state files. Statistical RNG
renewal is explicit. A new recipe needs a new reviewed state contract.

Workers persist the contract before consuming a launch right. The executor checks
the actual build and fixed rendered input before starting. After confirmed exit,
the worker checks the safe-step thermo clock and enumerates checkpoint state.
The manager accepts only the registered contract, successful terminated ownership
and the latest attempt. A complete size/SHA-256 manifest binds one content identity.
Missing, extra, unsafe, corrupt or incompatible state fails publication.

Authenticated endpoints use the existing bearer/TLS rules:

- `POST /v2/checkpoints/ATTEMPT/manifest`: bind the complete contract/file manifest.
- `PUT /v2/checkpoints/ATTEMPT/INDEX`: upload a bounded verified state file.
- `POST /v2/checkpoints/ATTEMPT/commit`: reverify, atomically publish and acknowledge.
- `GET /v2/checkpoints/CONSUMER_ATTEMPT/files/INDEX`: fetch only an owned, pending,
  pinned successor with `X-Build-Fingerprint` matching the released environment.

In shared mode commit copies state from the manager-owned assignment's `work/`;
in staged mode it uses independently uploaded files. Both publish under
`JOB_ROOT/.checkpoints/ATTEMPT/results/`. Files and directory ancestry are synced
before SQLite acknowledgement. A crash after rename or a lost response is repaired
by revalidating the same publication, including directory syncing, and replaying
one identity. Upload state cannot release process reservations or rerun a job.

`pin` records an acknowledged checkpoint dependency before successor assignment;
`release` requires conclusive termination of all consumer attempts. Fetch rechecks
the whole publication for disk corruption. T019 additionally verifies worker reassignment on the same pinned Linux host
boot, with actual executable inventory before launch. All consumers, including the
original worker ID, must report the checkpoint's pinned boot; changed or missing
boot provenance blocks restoration. Matching a peer-supplied fingerprint alone
cannot authorize reassignment to another physical host.

Manager copies and dependency history are retained indefinitely. There is no
checkpoint deletion API. Worker staged cleanup requires both ordinary result
acknowledgement and its separately persisted checkpoint acknowledgement. Journals,
launch fences, earlier checkpoints and failed output remain available. Failed
attempts with an expected but uncommitted checkpoint retain worker state
conservatively. A crash can resume only from a committed safe step; uncommitted
computation is never represented as durable progress.

Local byte-stand-in tests cover transfer, ownership, corrupt/missing state,
publication/acknowledgement crashes, restart/replay, environment rejection and
cleanup gates. These do not parse or certify arbitrary LAMMPS binary restarts.
The explicit `tools/verification/checkpoint_acceptance.py` driver separately runs
six bounded singleton MPI/LAMMPS processes through shared/staged publications,
restores both boundaries in each mode, compares their full physical frames, and
retains manifests, source hashes and process records. Run it only with authorized
remote resources, a compatible released executable, and a fresh private root:

```bash
PYTHONPATH=src python3 tools/verification/checkpoint_acceptance.py \
  --lammps /usr/local/bin/lmp --root /tmp/new-checkpoint-acceptance
```

Actual checks and limitations belong in [tasks.md](../tasks.md).
