# Isolated user-service lifecycle

Use a Linux host with systemd user services, Python 3.10+, PyYAML, Open MPI,
LAMMPS, OpenSSL, local SQLite storage and an allocated loopback port. This is a
single-host disposable deployment procedure, not a production installation.
No machine reboot or system-wide service change is needed. On hosts without a
user bus, use the foreground commands in [workers.md](workers.md); that fallback
does not verify systemd supervision. Do not enable lingering or change host
configuration merely to run this acceptance.

Prepare a new mode-0700 directory and a source copy with recorded SHA-256 hashes.
Create distinct `jobs` and `worker` roots. Adapt the checked-in example YAML:
set manager `storage.mode: staged`, worker `storage_mode: staged`, absolute
local paths, registered worker identity and an allocated HTTPS origin. For a
bounded smoke test use two MPI ranks, one OpenMP thread, 0.1 GiB per job and a
30-second time limit; reserve at most four slots, 1 GiB and two jobs. Configure
real executable paths. Set `cleanup_staged_files: false` initially.

Generate an unpredictable worker token in a mode-0600 file and a protected JSON
mapping from worker identity to its absolute token-file path. Never put token
contents in YAML, command arguments, logs or evidence. Supply a valid matching
TLS certificate/key; for a private loopback acceptance certificate, include
`subjectAltName=IP:127.0.0.1` and trust only that certificate with `SSL_CERT_FILE`.
Keep the private key mode 0600. See [workers.md](workers.md) for credential and
TLS requirements. A self-signed acceptance certificate is not a production CA.

From the private source root, set absolute `ROOT`, `PYTHON`, `CONFIG_ROOT`, an
unused `PORT` and a unique `PREFIX` in the operator shell. `ROOT/src` must contain
the source; `CONFIG_ROOT` contains the prepared YAML, credentials mapping,
certificate and token. Pre-create the configured work roots. Validate both:

```bash
"$PYTHON" -m lammps_manager validate-config manager "$CONFIG_ROOT/manager.yaml" --check-local-paths
"$PYTHON" -m lammps_manager validate-config worker "$CONFIG_ROOT/worker.yaml" --check-local-paths
```

Set `PYTHONPATH="$ROOT/src"` for validation and CLI operations. Start independent
foreground commands under transient user units (use an absolute Python path):

```bash
systemd-run --user --unit "$PREFIX-manager" \
  --property="WorkingDirectory=$ROOT" --property=Restart=on-failure \
  --property=KillMode=process --property=TimeoutStopSec=45 \
  --property=RuntimeMaxSec=900 \
  --setenv="PYTHONPATH=$ROOT/src" \
  --setenv="SSL_CERT_FILE=$CONFIG_ROOT/server.crt" \
  "$PYTHON" -m lammps_manager serve \
  --config "$CONFIG_ROOT/manager.yaml" \
  --credentials "$CONFIG_ROOT/credentials.json" \
  --host 127.0.0.1 --port "$PORT" \
  --tls-cert "$CONFIG_ROOT/server.crt" --tls-key "$CONFIG_ROOT/server.key"
systemd-run --user --unit "$PREFIX-worker" \
  --property="WorkingDirectory=$ROOT" --property=Restart=on-failure \
  --property=KillMode=process --property=TimeoutStopSec=45 \
  --property=RuntimeMaxSec=900 \
  --setenv="PYTHONPATH=$ROOT/src" \
  --setenv="SSL_CERT_FILE=$CONFIG_ROOT/server.crt" \
  "$PYTHON" -m lammps_manager worker \
  --config "$CONFIG_ROOT/worker.yaml" --state-directory "$CONFIG_ROOT/journal"
```

Check readiness through `systemctl --user show UNIT --property=MainPID` and
`journalctl --user -u UNIT --no-pager`. A running unit alone does not establish
worker readiness; inspect CLI `status` and its authenticated worker record.
Confirm the certificate fails without the dedicated trust root, and an invalid
bearer credential receives HTTP 401. Keep credentials out of captured requests.

Submit a JSON batch as documented in [local-batches.md](local-batches.md), then
use the manager-host CLI:

```bash
"$PYTHON" -m lammps_manager submit --config "$CONFIG_ROOT/manager.yaml" "$CONFIG_ROOT/batch.json"
"$PYTHON" -m lammps_manager status --config "$CONFIG_ROOT/manager.yaml"
"$PYTHON" -m lammps_manager inspect --config "$CONFIG_ROOT/manager.yaml" JOB_ID
systemctl --user restart "$PREFIX-manager.service"
systemctl --user restart "$PREFIX-worker.service"
```

Reuse the original database, journal, identity, endpoint and roots on every
restart. Compare stored specification bytes before and after restarting. Wait
for smoke results to be durably collected; with cleanup disabled their worker
attempt trees remain. Change only `cleanup_staged_files: true` in worker YAML,
restart its unit and verify the acknowledged worker trees disappear while manager
results and the journal remain. Submit another bounded smoke batch and confirm
one original attempt per job, real LAMMPS output and matching result hashes.
Retention changes affect cleanup, never submitted specifications.

When jobs and result collection are complete, stop only these named units:

```bash
systemctl --user stop "$PREFIX-worker.service" "$PREFIX-manager.service"
journalctl --user -u "$PREFIX-manager.service" -u "$PREFIX-worker.service" --no-pager
```

`KillMode=process` preserves simulation sessions if the worker service crashes;
unit stop is not job cancellation. Use CLI `cancel` and wait for termination if
maintenance requires stopping an active job. Do not remove journals. Audit both
reservation tables, every recorded MPI session and the allocated listener before
claiming shutdown; unit inactivity alone is insufficient. Retain private test
artifacts and sanitized evidence. Transient units need no installed unit files
or enable/disable operations. Never restart the machine for this procedure.
