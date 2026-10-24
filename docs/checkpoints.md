# Durable continuation checkpoints (T018)

T021 adds a separate [engineering contract](engineering-continuation.md) for long
ideal/WCA ABP with fresh persisted random streams. The measured/scientific scope
and legacy formats described below remain unchanged; they do not limit the new
explicit engineering contract, whose scientific evidence is `not_assessed`.

The `lammps_manager.checkpoints` API publishes checkpoints separately from process
termination and ordinary result collection. Legacy contracts accept T017's
released statistical ideal-ABP build, singleton layout and 600/1400/2000-step
boundaries or the measured uninterrupted 2000-step reference. Versioned T021
contracts additionally enforce their recipe-specific dt/digest/layout and bounded
random-allocation references. Exact recipes, other state sets and cross-host
restoration fail closed. See [continuation](continuation.md) for the scientific limits.

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

Engineering thermo clock checks allow accumulated binary64 rounding: LAMMPS
[adds each completed engine run's elapsed time](https://github.com/lammps/lammps/blob/patch_2Apr2025/src/update.cpp#L502-L505)
to the clock retained in its restart.
For `C` production `run` commands through the current boundary, including commands
split at `2^31-1` steps, the relative tolerance is
`max(1e-14, n*u/(1-n*u))`, where `n = 2*C+4` and `u = 2^-53`.
This bounds integer conversion, multiplication, accumulation and comparison with
the separately rounded `global_step*dt`. Future segments add no allowance;
admission limits keep the bound finite. Absolute tolerance remains `1e-14`.
The step and particle count remain exact checks; altered contract clocks remain
invalid, and legacy scientific contracts retain their original tolerance.
This numerical allowance does not establish scientific equivalence or detect
clock discrepancies smaller than its rounding budget.

Authenticated endpoints use the existing bearer/TLS rules:

- `POST /v2/checkpoints/ATTEMPT/manifest`: bind the complete contract/file manifest.
- `PUT /v2/checkpoints/ATTEMPT/INDEX`: upload a bounded verified state file.
- `POST /v2/checkpoints/ATTEMPT/commit`: reverify, atomically publish and acknowledge.
- `GET /v2/checkpoints/CONSUMER_ATTEMPT/files/INDEX`: fetch only an owned, pending,
  pinned successor with `X-Build-Fingerprint` matching the released environment.

Checkpoint manifest requests have a fixed 4 MiB JSON body limit. The complete
contract retains up to 100000 global step boundaries; even 19-digit signed-bigint
boundaries fit this allowance together with bounded provenance and the required
state-file manifest. This preserves existing contract and snapshot formats.
Checkpoint commit requests, ordinary result manifests and polling requests retain
their 1 MiB limits. Oversized requests fail before their bodies are read.

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
