# Retired home-directory deployment

For the new manager/worker directory layout and active services, see the
[current deployment](current-deployment.md). This page retains the historical V010 installation.

## Removal status

On 2026-10-24, at the user's request, all five V010 systemd user units
were stopped, disabled and removed on poincare, mozzarella and allegro,
including the manager, three workers and SSH transport. User managers were
reloaded; every unit reported `LoadState=not-found`, `ActiveState=inactive`
and `MainPID=0`. Retained source unit definitions were also removed.
Deployment data, simulation results, databases, journals, credentials, source
and virtual environments remain under `~/lammps_manager`. Account lingering
and peer SSH authorization were left unchanged. The sections below describe
the historical installation and acceptance, not currently running services.
Removal evidence: `var/verification/v010/systemd-removal.json` (private local record).

## Measured target and operation

On 2026-10-24 the user approved deployment under `~/lammps_manager` on
poincare, mozzarella and allegro, and requested that passing installations
remain running. Each home is on its host's local ext4 filesystem. The source
repository remains `/home/ted/task_manager` on poincare; services execute
fingerprinted source copies under `/home/ted/lammps_manager/src` with private
Python 3.10 environments and PyYAML 6.0.3.

| Host | Role | User units |
| --- | --- | --- |
| poincare | Manager and local worker | `lammps-v010-manager.service`, `lammps-v010-worker.service` |
| poincare | Supervised transport to mozzarella | `lammps-v010-mozzarella-transport.service` |
| mozzarella | Worker | `lammps-v010-worker.service` |
| allegro | Worker | `lammps-v010-worker.service` |

The units are enabled under each user's `~/.config/systemd/user`, with source
unit files retained under `~/lammps_manager/units`. The deployment account is
`ted`. Lingering is enabled for that account on all three hosts. All three
workers use `Restart=always`; automatic recovery after killing each idle worker's
main process was measured. Startup after physical reboot has not been tested.
No host reboot was performed.

The manager database is `~/lammps_manager/state/manager.sqlite`; submitted
inputs and collected results are under `~/lammps_manager/jobs`. Workers use
their own `~/lammps_manager/worker` roots and durable journals under
`~/lammps_manager/state/journal`. Mozzarella uses `state/journal-forwarded` for
its final manager origin; its original unused direct-origin journal is retained.
Never delete or replace a journal to resolve uncertain execution.

Each worker currently has two CPU slots, a 1 GiB memory budget and one concurrent
job. Submission defaults are two MPI ranks, one OpenMP thread per rank, 0.1 GiB
and a 60-second limit. These are the installed settings, not a capacity benchmark.
Local worker affinity is CPUs 2-3; remote worker affinity is CPUs 0-1; manager
affinity is CPUs 0-1 on poincare. Resource reservations are accounting, not cgroup
memory/CPU enforcement.

## Transport and credentials

The manager listens on `https://192.168.50.112:18460`. Poincare and allegro use
that origin directly. Mozzarella's attempted direct connection returned
`No route to host`; host routes were left unchanged. A dedicated persistent SSH
connection from poincare to mozzarella exposes a remote loopback forward at
`127.0.0.1:18461`, forwarding to the manager. Mozzarella's worker origin is
`https://127.0.0.1:18461`. This additional transport service is a deployment
dependency and must remain running.

The forward retains end-to-end HTTPS verification. A dedicated private CA is
trusted through `SSL_CERT_FILE`; the manager certificate includes the actual
LAN IP and `127.0.0.1` in its SAN. The SSH transport enforces pinned host keys
and server-alive checks with automatic restart. There is no plaintext worker
endpoint or disabled certificate verification.

Credentials, CA keys, worker tokens and transport login files are private under
the mode-0700 deployment root and its `config` directory; secret files are
mode 0600. Never copy their contents into evidence, command arguments or source
control. The current manager certificate expires on **2027-01-21 UTC**; renew it
before expiry and restart the manager after checking the new certificate/key.
Do not silently replace pinned SSH keys or bypass certificate failures.

The manager-side `config/credentials.json` maps all three identities to separate
token-file paths. The manager uses `config/manager.yaml`; each worker uses its
local `config/worker.yaml`. Configuration changes require service restart and
must not rewrite submitted job specifications.

## Inspection and maintenance

On poincare, inspect the actual manager state:

```bash
PYTHONPATH="$HOME/lammps_manager/src" \
  "$HOME/lammps_manager/venv/bin/python3" -m lammps_manager status \
  --config "$HOME/lammps_manager/config/manager.yaml"
systemctl --user status lammps-v010-manager.service lammps-v010-worker.service \
  lammps-v010-mozzarella-transport.service
```

On each worker host:

```bash
systemctl --user is-enabled lammps-v010-worker.service
systemctl --user show lammps-v010-worker.service \
  --property=ActiveState --property=MainPID --property=Restart --property=NRestarts
journalctl --user -u lammps-v010-worker.service --no-pager -n 50
loginctl show-user ted -p Linger
```

Pause admission before maintenance, and inspect reservations and live attempts.
Unit stop is not simulation cancellation: `KillMode=process` preserves execution
sessions during worker replacement. Use CLI cancellation and wait for confirmed
termination when active simulations must be stopped. Keep original databases,
journals, roots, credentials and origins when restarting services. See
[recovery](recovery.md) and [worker operations](workers.md).

## Acceptance and remaining scope

Five original staged jobs completed with real MPI/LAMMPS: two jobs initially
verified the remote installations, then three jobs verified poincare and both
remote workers using a common isolated LAMMPS executable. The common executable
was prepared from mozzarella's installation using `strip --strip-debug` without
changing its original binary; its SHA-256 is
`4d812f3ddbc98453c89ef55c26f82281425b130a3c11a678bc42aa86663e1597`.
Each job used two ranks on its selected worker and 1000 integration steps.

The initial deployment acceptance also verified invalid-token HTTP 401, rejection
of an untrusted private CA, actual manager/worker restarts, unchanged submitted
specifications and one original attempt per job. The local worker passed
foreground acceptance before its systemd installation. Subsequent worker crash
probes verified automatic restart of all three workers, fresh authenticated
readiness and unchanged attempt counts.

Private executed drivers and evidence remain under
`/home/ted/task_manager/var/verification/v010`. This directory includes
`deployment-acceptance.json`, `three-workers.json`, `deployment-audit.json`,
`supervision.json`, source fingerprints and the common-executable dependency
record. Secret login files are segregated in its private `auth` directory and
are excluded from public records.

**Single-job multi-host MPI passed the bounded V010 acceptance.** The deployment includes the
locally tested native runtime, with the previous source retained under
`releases`. Explicit Python coordination provides atomic participant reservations,
staging, launch fences, per-host termination evidence and combined result
collection. Automatic scheduling and CLI submission remain unavailable.

The user authorized a persistent dedicated peer key on mozzarella. Allegro's
existing `authorized_keys` was preserved and a source-restricted key was appended
with forced `mpi-peer-gate`, no forwarding or PTY. The gate accepts only a
prepared attempt's monitored ORTE daemon. Direct peer host keys match the pinned
original SSH endpoint; private keys remain outside version control.

The first real two-host attempt failed before peer launch because OpenMPI added
a shell environment prefix rejected by the strict gate. Both launch fences were
closed, termination confirmed, reservations released and failed output collected.
The launcher correction uses `--noprefix` with the existing explicit daemon path;
it does not broaden the gate to shell commands. A subsequent single MPI invocation completed 1000 steps with rank 0 on
mozzarella and rank 1 on allegro, world size 2. Separate live-rank cancellation
and manager-transport interruption cases passed; the latter retained both resource
shares beyond the 30-second heartbeat timeout and resumed the original attempt
without duplicate execution. All participant sessions ended and combined results
were collected with verified hashes. See the [explicit Python API](multi-host-mpi.md). Physical reboot/power-loss behavior, unrestricted scientific
continuation and unmeasured workload performance are not established by this
acceptance. See [tasks](../tasks.md) for actual evidence and remaining work.
