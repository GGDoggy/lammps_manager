# Staged execution and durable results

T008 enables polling-worker execution without shared storage. Configure staged mode on both peers as described in [staged inputs](staged-inputs.md). Existing shared snapshots and commands retain their behavior; `run-local` continues to defer staged jobs. [T009](verification/t009-staged.md) verifies one remote worker with real MPI / LAMMPS, distinct roots, successful/failed jobs and interrupted-upload replay.

## Process and delivery states

Process state (`assigned`, `running`, `unknown`, `succeeded`, `failed`, `cancelled`) is independent of result collection (`pending`, `uploading`, `failed`, `collected`). A successful process with pending or failed collection does not mean its artifacts are available on the manager. `status` reports both process state and a collection summary; `inspect` includes the full result manifest, omissions and diagnostic reason. Manager log paths and `results_directory` are available only after durable collection; omitted logs have null paths and explicit omission reasons.

The worker consumes a durable, unique SQLite launch claim before submitting the executor. Repeated deliveries and worker restart cannot consume another launch right. A crash between claim and process identity persistence leaves the attempt unknown with reservations retained, requiring identity evidence or manual resolution. Ready inputs without a launch claim can safely continue after restart. The executor copies and verifies every input into `ATTEMPT/work` before starting an argument-array subprocess. Input staging paths never rewrite submitted specifications.

Only confirmed termination allows output enumeration or reservation release. Linux process/session reconciliation, time limits and whole-session cancellation apply to claimed staged attempts. A recovered exit without an available exit code is a failure, never inferred success. Transfer failure never launches a new attempt. Unknown/running attempts retain reservations on disconnect. Once termination is confirmed, reservations may be released while delivery is still pending, allowing other work to be scheduled.

Result enumeration, hashing, uploads and commit replay run in a dedicated single-thread executor, independently of polling and process execution. At most one collection pass is outstanding; later polls do not queue duplicate passes. Each pass includes only attempts whose terminal reports the manager has already acknowledged. Attempts that finish during a pass wait for a later successful poll. Slow collection therefore does not delay heartbeats, cancellation delivery or recovered-process time-limit checks. Each collection thread owns its SQLite connection and uses a separate HTTP opener with the same authentication and transport restrictions. Expected transfer failures remain durable and retry on later polls; unexpected background failures surface on the next tick.

Shutdown asks collection to stop between attempts and transfer operations, waits for any active enumeration or request and for execution to finish, then releases the journal lock. It does not cancel a simulation. It also joins the separate bounded cleanup executor before releasing the journal lock; unfinished cleanup resumes after restart. An interrupted collection remains pending/uploading and resumes from durable state after restart; shutdown can still wait for a slow in-flight filesystem or network operation.

## Output selection and bounds

T024 compresses `work/**/*.dump` trajectories to verified gzip before manifest
binding. Archives are selected only when artifact savings exceed their added
manifest metadata (including MPI publication paths); other trajectories stay
raw. The selected files may mix raw dumps and archives; optional `compression` records
bind original and compressed size/SHA-256 identities. Raw trajectories remain
worker-private until acknowledged cleanup. See [sampling and compression](trajectory-sampling.md)
for conversion, replay and failure behavior. Ordinary shared polling jobs now
also use durable collection; shared cleanup remains disabled.

Collection includes `stdout`, `stderr`, `execution.json`, `process.json`, a journal-derived `outcome.json`, and every regular file under `work/` (including copied inputs). Missing logs, metadata or the work directory are reported explicitly in `omitted`; no scientific work files are silently excluded. Empty directories are not artifacts. Worker-private `inputs`, hidden temporary files outside `work`, journals and upload spools are excluded by this fixed selection policy. Symlinks and special files anywhere in selected output fail collection. Stop other writers to the private attempt directory; output enumeration requires a terminated launch session.

Both manager `storage` and worker top-level configuration accept positive integers:

| Setting | Default |
|---|---:|
| `max_output_files` | 10000 |
| `max_output_file_bytes` | 1073741824 |
| `max_output_total_bytes` | 10737418240 |

The worker setting `result_request_timeout_seconds` is a positive finite number (default 300). It controls socket waits for all result requests, including waiting for the manager result lock and for commit/replay verification, copying and fsync to finish. Choose a value above the expected storage and contention delay; this is a socket inactivity timeout, not a total transfer deadline. Poll and input-download socket timeouts remain ten seconds. Configuration changes take effect after worker restart and do not rewrite submitted specifications. A timeout retains a retryable collection failure; slow commit replay uses the same setting. A larger setting can also increase shutdown wait while a result request is stalled.

Both peers enforce limits independently. Oversized output fails visibly; it is never truncated or treated as collected. Ordinary and MPI result manifest JSON requests have an additional 16 MiB bound, accommodating compression identity records above the previous 1 MiB limit. Authentication and body-length checks precede reading the request. Result commits and polls retain their 1 MiB bounds; checkpoint manifests retain 4 MiB. File count/size limits still apply independently, and unusually large path metadata can still exceed the finite manifest allowance. Increasing limits does not change a submitted specification. Once persisted, the result manifest is immutable: missing or changed bound files keep delivery failed until the original bytes are available. An enumeration failure before binding can retry enumeration. Collection errors appear in the worker journal immediately and in manager status on the next successful poll; manager storage errors can also appear as HTTP errors in the worker diagnostic.

## Authenticated publication protocol

Polls remain protocol 2; the manager advertises `results_protocol: 1` and `staged_launch_enabled: true`. A worker requires both before claiming a staged launch. Result endpoints use the same worker identity, bearer credential, TLS and no-redirect/no-proxy rules as input transfer. Each operation checks attempt ownership, staged specification and confirmed terminal process state. Client paths never choose a manager destination.

1. `POST /v2/results/ATTEMPT/manifest` binds `{files, omitted}` with optional `compression` records durably. Entries contain canonical relative POSIX paths, exact sizes and SHA-256. Duplicate, traversal, absolute, conflicting and unselected paths fail. Conflicting replay cannot replace a bound manifest.
2. `PUT /v2/results/ATTEMPT/INDEX` streams one manifest-selected file with exact Content-Length and bounded chunks. The server retains its ten-second socket inactivity timeout; the worker uses `result_request_timeout_seconds` for its result requests. Hash/size validation and file fsync precede replacing a flat, manager-selected spool entry under `JOB/ATTEMPT/.results-upload`. Partial temporary files cannot become results. HTTP chunked uploads are rejected.
3. `POST /v2/results/ATTEMPT/commit` with `{}` revalidates all spool files into a fresh temporary tree, fsyncs its files and directories, atomically publishes `JOB/ATTEMPT/results`, then fsyncs the entire ancestor chain. Only afterward does SQLite persist `collected` and the manager acknowledge it. No earlier attempt or existing publication is overwritten. Replay verifies an existing publication and synchronizes ancestors again before acknowledgement.

Acknowledgement responses are compact status/diagnostic objects. A bounded 32-entry cache avoids decoding and validating the immutable full manifest for every file; ownership, terminal state and current limits remain checked on each operation. The last reported failure stays visible during retries until collection succeeds. Worker and manager restart reuse their durable manifest and spool/publication. A lost final response retries delivery and commit, never simulation execution. An already acknowledged manager skips redundant worker file uploads and verifies commit replay. Disk-full, corrupt/missing files or failed directory sync leave delivery incomplete. A crash after publication but before SQLite acknowledgement leaves a tree on disk that `inspect` does not advertise as delivered until replay succeeds.

Worker output is retained until the worker durably records the successful commit response. T012 then automatically reclaims eligible worker-private files as described below. Manager publications, upload spools and manager crash temporaries remain retained. No simulation retry or deployment change is implied. Protect roots and journals against other filesystem writers.

## Worker staged cleanup (T012)

The worker setting `cleanup_staged_files` is a strict boolean, default `true`. It has no effect in shared mode. Set it to `false` and restart the worker to retain staged files, including already eligible attempts; reenabling it resumes cleanup without changing submitted specifications. Before upgrading an existing installation where worker copies must be retained, set it to `false`. Manager results are retained in either case.

Eligibility requires a terminal process outcome with confirmed termination, the immutable result manifest, and a `collected` commit response durably stored in the worker journal. A successful HTTP exchange with any other status is insufficient. A lost response, failed upload, unknown/running process or unacknowledged manifest never authorizes deletion. Successful, failed and cancelled attempts follow the same rule. Result publication and its manifest remain on the manager; worker deletion never changes the process outcome or collection status.

New staged assignments create a private `WORK_ROOT/JOB/ATTEMPT` directory and durably bind the device/inode identities of the root, job and attempt before staging. Only this owned attempt tree is eligible: copied `inputs`, execution `work`, logs, process/outcome metadata and hidden transfer temporaries inside that tree. Empty job directories remain. Legacy trees without this ownership record, including a crash between directory creation and ownership persistence, are conservatively retained; they require explicit operator inspection outside automatic cleanup. They are not adopted merely because the pathname matches.

A separate single-thread executor performs at most one cleanup pass at a time, independently of polling, execution and result transfers. Each pass visits at most 32 eligible attempts, rotating a cursor so failed early attempts do not starve later ones; each attempt removes at most 256 file/directory entries inside its tree, plus its empty attempt root. Further polls continue partial work. The bounds limit filesystem operations, not wall-clock latency: an individual filesystem operation can still stall, and shutdown waits for an active pass. Restart reconstructs eligibility from SQLite and retries partial deletion.

Cleanup opens every ancestor without following symlinks and walks relative to pinned directory descriptors. It verifies the recorded root/job/attempt identities and refuses filesystem boundaries, symlinks, multiply linked files, special files, changed directories and trees containing the worker state directory or configured token file, and any overlap with the manager job directory. Such errors can follow partial reclamation of other eligible files; they remain visible and retryable. Never place state, credentials or unrelated data in attempt directories, and prevent other users/processes from writing worker-private roots. These checks do not provide isolation from privileged filesystem mutation or malicious mount operations.

The local worker SQLite table `staged_cleanup` records `retained`, `deleting`, `failed` (with diagnostic `reason`) or `cleaned`. `retained` means no cleanup pass has begun, including when disabled or not yet acknowledged. `deleting` survives a partial pass or crash. A failure retries on subsequent passes; correct the permission/path problem without changing ownership records. Changed-directory failures require restoring the original owned directory, not replacing the saved inode identities. A missing owned attempt after its parent is verified and synchronized completes the tombstone, handling crashes after `rmdir`. Directory deletions are synchronized before recording `cleaned`.

Operators can inspect the worker journal, independently of manager collection status:

```bash
sqlite3 /path/to/worker-state/attempts.sqlite3 \
  'SELECT attempt_id,status,reason FROM staged_cleanup;'
```

`Store.cleanup_record(attempt_id)` and `inspect_job()` against the worker journal also expose this state (`worker_cleanup`); the manager journal does not mirror worker cleanup records. An absent record identifies a retained legacy/unowned tree. New cleanup failure reasons are written to worker stderr. No manual force-delete or journal-reset command is added. To preserve current worker files, disable cleanup and restart; a service pause only stops dispatch and does not disable cleanup.

Journal specifications, attempts, launch claims, termination evidence, immutable manifests, durable acknowledgements and cleanup tombstones are retained indefinitely. Ordinary terminal report replay and duplicate assignments after restart use those records without requiring deleted worker files. Never remove the journal to reclaim simulation storage: it supplies the duplicate-launch fence.

## Local verification

`tests/test_results.py` uses fake executable processes, temporary SQLite journals, distinct manager/worker roots and authenticated loopback HTTP. It covers success/nonzero exit, output hashes, omissions, limits, unsafe paths/types, ownership, partial/corrupt uploads, injected disk and publication/acknowledgement failures, mid-transfer restart, lost acknowledgements, durable launch fences and prelaunch cancellation. `tests/test_staging.py` retains isolated input preparation coverage. Background-collection regressions block enumeration, upload and commit while checking continued heartbeats, cancellation and recovered-process timeouts, and cover single-pass scheduling, terminal-report acknowledgement ordering, unexpected errors and shutdown/restart. `tests/test_cleanup.py` additionally checks acknowledgement gates, bounded partial cleanup, injected permission/crash failures, restart after removal, path/ancestor replacement (including a symlink race), protected files, legacy/shared retention, disabled cleanup, manager result hashes and progress during blocked cleanup. These checks do not establish real network performance, physical power-loss durability or MPI cleanup on remote machines.

## File-level resumption (T026)

Authenticated manifest responses add `verified_files`, a list of file indices
whose complete size and SHA-256 match the bound immutable manifest on the manager.
The manager checks spool/published bytes outside SQLite transactions, including
after restart; missing or changed files are omitted. Workers skip those indices
and upload only missing files. Older peers without this field retain full-file
replay behavior. This is file-level resumption, not byte-range resumption.

Before sending result or checkpoint commit, the worker durably records commit
intent. A lost response/restart replays commit first, without another manifest
or artifact upload. Explicit HTTP 400 rejection clears the intent so a later
pass can obtain a fresh verified inventory. Cleanup/continuation still require
the matching durable acknowledgement; local intent alone authorizes neither.
Commit replay continues to verify and synchronize published files.

Results, checkpoints and native MPI artifact operations serialize under one lock
per attempt. Distinct attempts may upload/verify concurrently. Lock registry
entries are removed only after the owner and every waiter leave; a small separate
mutex protects the shared manifest cache. Verification/publication of the same
attempt remains ordered across protocols.
