# Current three-host deployment

Deployed on 2026-10-24 and switched to direct Git checkout operation on
2026-10-25 with user authorization. Poincare runs the manager and a
worker; mozzarella and allegro each run a worker. All services run as `ted` under
systemd user supervision, with existing account lingering retained.

## Layout and services

All hosts use `/home/ted/lammps_manager`:

- `manager/config/manager.yaml`: manager configuration on poincare.
- `manager/config/credentials.json`: private worker ID to token-path mapping.
- `manager/state/manager.sqlite3`: retained manager database on local disk.
- `manager/jobs`: immutable staged inputs and collected simulation results.
- `worker/config/worker.yaml`: each host's worker configuration.
- `worker/config/ca.crt` and `worker/config/worker.token`: manager trust and private authentication.
- `worker/state`: durable worker identity, journal and launch fences; reuse on restart.
- `worker/work`: staged execution files, eligible for cleanup after durable result acknowledgement.
- `src/`: active Python source from the root Git clone.
- `.venv/`: root Python 3.10 environment used by all services.
- No `releases/` directory or transition backups remain; removed at the user's request.

Poincare has `lammps-manager.service` and `lammps-worker.service`;
mozzarella and allegro have `lammps-worker.service`. Units are installed under
`~/.config/systemd/user` and enabled for the user default target. `Restart=always`
restarts the service after an unexpected exit; `KillMode=process` preserves
simulation sessions for journal-based recovery. Stopping a worker service is not
job cancellation. Use the manager CLI to cancel a job and confirm termination
before maintenance that requires idle workers. Physical reboot is not tested.

All services use the root Git checkout directly through `PYTHONPATH` pointing to
`/home/ted/lammps_manager/src` and `/home/ted/lammps_manager/.venv/bin/python3`.
The deployed revision is `af7041a4c5fc9e53a2a18d87f91985eb73ade7bd` on all three
hosts. A pull changes the source used by subsequent imports and new processes;
update only during coordinated idle maintenance, then restart all affected
services. No release creation or service-path change is needed for later updates.
All service environments use system Python 3.10.12 and PyYAML 6.0.3. Root
`.venv` environments contain an independent PyYAML installation. Old releases,
prior root virtual environments and transition source/unit backups were removed
at the user's request after switching services. Private runtime directories are
excluded through each clone's `.git/info/exclude`.
The poincare worker uses `worker/bin/lmp`, copied from the prior accepted allegro
build after checking local dynamic dependencies. Mozzarella uses
`/usr/local/bin/lmp`; allegro uses `/usr/bin/lmp`. These builds differ; basic smoke
execution does not establish scientific equivalence or continuation compatibility.

## Network and quotas

Workers use **`https://192.168.50.112:8443`** in staged mode. The listener binds
`0.0.0.0:8443`. Both remote hosts passed HTTPS certificate verification over the
LAN. A dedicated private CA signs the server certificate, whose SAN includes
`192.168.50.112`, `140.114.80.212` and `127.0.0.1`; each worker trusts only the
configured CA via `SSL_CERT_FILE`. Private keys and per-worker bearer credentials
remain outside version control. The server certificate and CA are valid for
365 days from issuance; the server certificate expires on 2027-10-24 at
10:47:59 UTC. Renew before expiration and restart affected services.

The public `140.114.80.212:8443` timed out from mozzarella before deployment;
this installation does not rely on public ingress, NAT forwarding or SSH tunnels.
It does not change host firewall or routing configuration.

Existing manager limits are retained: each worker reserves up to 10 logical CPU
slots, 10 GiB and 10 concurrent jobs. Actual admission also checks detected
capacity, available memory, per-CPU external load and all unreleased reservations.
CPU reservations equal MPI ranks multiplied by OpenMP threads. These settings
provide accounting, not cgroup enforcement. Submission defaults remain one MPI
rank, one OpenMP thread, 1 GiB and a 3600-second deadline. The default memory
was reduced from 10 GiB on 2026-10-25 for new submissions only; existing job
specifications retain their submitted resources.

Pre-existing configuration files were backed up before replacement. Old allegro
results and its terminal journal under `state/journal` remain untouched. The
new manager database was empty and old journals had no reservations or live
simulation processes before creating the new deployment's worker identities.
No old journal was deleted or rewritten to bypass uncertain work.

## Operation

On poincare, use the root checkout CLI:

```bash
ROOT="$HOME/lammps_manager"
export PYTHONPATH="$ROOT/src"
"$ROOT/.venv/bin/python3" -m lammps_manager status \
  --config "$ROOT/manager/config/manager.yaml"
"$ROOT/.venv/bin/python3" -m lammps_manager submit \
  --config "$ROOT/manager/config/manager.yaml" /absolute/path/batch.json
systemctl --user status lammps-manager.service lammps-worker.service
journalctl --user -u lammps-manager.service -u lammps-worker.service -n 50 --no-pager
```

On each worker host:

```bash
systemctl --user status lammps-worker.service
journalctl --user -u lammps-worker.service -n 50 --no-pager
```

For subsequent code updates, first confirm that the queue is idle, every process
has confirmed termination and all reservations are released. Stop the manager to
prevent new dispatch and stop the workers before pulling. On each host, use
`git status` and `git pull --ff-only` in `/home/ted/lammps_manager`; preserve local
changes rather than overwriting them. Update dependencies if `pyproject.toml`
changes and follow any documented database/configuration migration. Run relevant
checks, ensure all hosts use the same revision, then start the manager and workers.
For this idle deployment, ordinary service operations are:

```bash
# Poincare: stop manager first, then worker.
systemctl --user stop lammps-manager.service lammps-worker.service
# Remote workers: stop before updating their checkout.
systemctl --user stop lammps-worker.service
# After all three checkouts have been updated and checked, on poincare:
systemctl --user start lammps-manager.service lammps-worker.service
# On mozzarella and allegro:
systemctl --user start lammps-worker.service
```

A fresh installation with the system `python3-venv` package available can use
`git clone`, `/usr/bin/python3 -m venv .venv` and
`.venv/bin/python3 -m pip install -e .`, followed by host-specific `manager/`
and `worker/` configuration, protected credentials/TLS and systemd units using
the root paths above. See [configuration](configuration.md) and
[workers](workers.md). Do not replace existing runtime identities or databases
when updating an established installation.

Changes to configuration, credentials or certificates require service restart.
Always retain the original database, endpoint, roots and journal identity on
ordinary restarts; changing the endpoint requires an explicit state migration.
See [workers](workers.md), [recovery](recovery.md) and [staged results](staged-results.md).

## Verification

Private machine-readable evidence is under
`var/verification/deployment-20261024/`: release hashes, bounded smoke job IDs,
termination evidence, collected result manifests and final service/worker audit.
All three smoke jobs passed using two MPI ranks, one thread, 0.1 GiB, ten steps
and a 60-second limit, one job on each host. Each has a single attempt, confirmed
termination and eight collected files verified against their SHA-256 manifests.
Idle service restarts preserved submitted specifications and the original journals;
all three workers returned online with no reservations. Results are retained in
`manager/jobs`.
This establishes basic independent per-worker execution and staged collection,
not single-job multi-host MPI, production load capacity or physical reboot recovery.

The full repository `make check` passed all 395 tests, compileall and Git whitespace
checks using the deployed native Python 3.10 environment. Private log:
`/tmp/three-host-deployment-make-check-native310.log`. The corrected local runtime
also passed a fourth bounded smoke job (`smoke-python310.json`).


The direct-checkout transition on 2026-10-25 retained the original four job
specifications, worker identities, CA and tokens. Four enabled/running services
were audited through systemd and `/proc` to confirm root `.venv` execution without
release paths. All three clones have identical Python module hashes and revision.
Three new real two-rank, one-thread, ten-step MPI/LAMMPS jobs each succeeded on
the intended host, with one attempt, confirmed termination, eight hash-verified
collected files and zero remaining reservations. The original pause flags were
restored and all workers reported fresh online/ready heartbeats.
Private evidence is under `var/verification/direct-checkout-20261025/`
(`switch.json`, `prior-specifications.json`, `smoke.json`, `audit.json`).
Configuration and storage paths required no changes. At the user's subsequent
request, all three `releases/` directories and the transition source, environment
and unit backups were removed. Simulation outputs and journals remain intact.
