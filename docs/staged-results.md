# Staged execution and durable results

T008 enables polling-worker execution without shared storage. Configure staged mode on both peers as described in [staged inputs](staged-inputs.md). Existing shared snapshots and commands retain their behavior; `run-local` continues to defer staged jobs. Real hosts and MPI / LAMMPS acceptance remain T009.

## Process and delivery states

Process state (`assigned`, `running`, `unknown`, `succeeded`, `failed`, `cancelled`) is independent of result collection (`pending`, `uploading`, `failed`, `collected`). A successful process with pending or failed collection does not mean its artifacts are available on the manager. `status` reports both process state and a collection summary; `inspect` includes the full result manifest, omissions and diagnostic reason. Manager log paths and `results_directory` are available only after durable collection; omitted logs have null paths and explicit omission reasons.

The worker consumes a durable, unique SQLite launch claim before submitting the executor. Repeated deliveries and worker restart cannot consume another launch right. A crash between claim and process identity persistence leaves the attempt unknown with reservations retained, requiring identity evidence or manual resolution. Ready inputs without a launch claim can safely continue after restart. The executor copies and verifies every input into `ATTEMPT/work` before starting an argument-array subprocess. Input staging paths never rewrite submitted specifications.

Only confirmed termination allows output enumeration or reservation release. Linux process/session reconciliation, time limits and whole-session cancellation apply to claimed staged attempts. A recovered exit without an available exit code is a failure, never inferred success. Transfer failure never launches a new attempt. Unknown/running attempts retain reservations on disconnect. Once termination is confirmed, reservations may be released while delivery is still pending, allowing other work to be scheduled.

Result enumeration, hashing, uploads and commit replay run in a dedicated single-thread executor, independently of polling and process execution. At most one collection pass is outstanding; later polls do not queue duplicate passes. Each pass includes only attempts whose terminal reports the manager has already acknowledged. Attempts that finish during a pass wait for a later successful poll. Slow collection therefore does not delay heartbeats, cancellation delivery or recovered-process time-limit checks. Each collection thread owns its SQLite connection and uses a separate HTTP opener with the same authentication and transport restrictions. Expected transfer failures remain durable and retry on later polls; unexpected background failures surface on the next tick.

Shutdown asks collection to stop between attempts and transfer operations, waits for any active enumeration or request and for execution to finish, then releases the journal lock. It does not cancel a simulation or delete output. An interrupted collection remains pending/uploading and resumes from durable state after restart; shutdown can still wait for a slow in-flight filesystem or network operation.

## Output selection and bounds

Collection includes `stdout`, `stderr`, `execution.json`, `process.json`, a journal-derived `outcome.json`, and every regular file under `work/` (including copied inputs). Missing logs, metadata or the work directory are reported explicitly in `omitted`; no scientific work files are silently excluded. Empty directories are not artifacts. Worker-private `inputs`, hidden temporary files outside `work`, journals and upload spools are excluded by this fixed selection policy. Symlinks and special files anywhere in selected output fail collection. Stop other writers to the private attempt directory; output enumeration requires a terminated launch session.

Both manager `storage` and worker top-level configuration accept positive integers:

| Setting | Default |
|---|---:|
| `max_output_files` | 10000 |
| `max_output_file_bytes` | 1073741824 |
| `max_output_total_bytes` | 10737418240 |

The worker setting `result_request_timeout_seconds` is a positive finite number (default 300). It controls socket waits for all result requests, including waiting for the manager result lock and for commit/replay verification, copying and fsync to finish. Choose a value above the expected storage and contention delay; this is a socket inactivity timeout, not a total transfer deadline. Poll and input-download socket timeouts remain ten seconds. Configuration changes take effect after worker restart and do not rewrite submitted specifications. A timeout retains a retryable collection failure; slow commit replay uses the same setting. A larger setting can also increase shutdown wait while a result request is stalled.

Both peers enforce limits independently. Oversized output fails visibly; it is never truncated or treated as collected. Manifest JSON requests have an additional 1 MiB bound. Increasing limits does not change a submitted specification. Once persisted, the result manifest is immutable: missing or changed bound files keep delivery failed until the original bytes are available. An enumeration failure before binding can retry enumeration. Collection errors appear in the worker journal immediately and in manager status on the next successful poll; manager storage errors can also appear as HTTP errors in the worker diagnostic.

## Authenticated publication protocol

Polls remain protocol 2; the manager advertises `results_protocol: 1` and `staged_launch_enabled: true`. A worker requires both before claiming a staged launch. Result endpoints use the same worker identity, bearer credential, TLS and no-redirect/no-proxy rules as input transfer. Each operation checks attempt ownership, staged specification and confirmed terminal process state. Client paths never choose a manager destination.

1. `POST /v2/results/ATTEMPT/manifest` binds `{files, omitted}` durably. Entries contain canonical relative POSIX paths, exact sizes and SHA-256. Duplicate, traversal, absolute, conflicting and unselected paths fail. Conflicting replay cannot replace a bound manifest.
2. `PUT /v2/results/ATTEMPT/INDEX` streams one manifest-selected file with exact Content-Length and bounded chunks. The server retains its ten-second socket inactivity timeout; the worker uses `result_request_timeout_seconds` for its result requests. Hash/size validation and file fsync precede replacing a flat, manager-selected spool entry under `JOB/ATTEMPT/.results-upload`. Partial temporary files cannot become results. HTTP chunked uploads are rejected.
3. `POST /v2/results/ATTEMPT/commit` with `{}` revalidates all spool files into a fresh temporary tree, fsyncs its files and directories, atomically publishes `JOB/ATTEMPT/results`, then fsyncs the entire ancestor chain. Only afterward does SQLite persist `collected` and the manager acknowledge it. No earlier attempt or existing publication is overwritten. Replay verifies an existing publication and synchronizes ancestors again before acknowledgement.

Acknowledgement responses are compact status/diagnostic objects. A bounded 32-entry cache avoids decoding and validating the immutable full manifest for every file; ownership, terminal state and current limits remain checked on each operation. The last reported failure stays visible during retries until collection succeeds. Worker and manager restart reuse their durable manifest and spool/publication. A lost final response retries delivery and commit, never simulation execution. An already acknowledged manager skips redundant worker file uploads and verifies commit replay. Disk-full, corrupt/missing files or failed directory sync leave delivery incomplete. A crash after publication but before SQLite acknowledgement leaves a tree on disk that `inspect` does not advertise as delivered until replay succeeds.

Worker output is retained after acknowledgement as well as before it. Manager spool files and abandoned crash temporary files are retained. No automatic output deletion, garbage collection, simulation retry, external deployment or real-host job control is introduced. Protect roots and journals against other filesystem writers.

## Local verification

`tests/test_results.py` uses fake executable processes, temporary SQLite journals, distinct manager/worker roots and authenticated loopback HTTP. It covers success/nonzero exit, output hashes, omissions, limits, unsafe paths/types, ownership, partial/corrupt uploads, injected disk and publication/acknowledgement failures, mid-transfer restart, lost acknowledgements, durable launch fences and prelaunch cancellation. `tests/test_staging.py` retains isolated input preparation coverage. Background-collection regressions block enumeration, upload and commit while checking continued heartbeats, cancellation and recovered-process timeouts, and cover single-pass scheduling, terminal-report acknowledgement ordering, unexpected errors and shutdown/restart. These checks do not establish real network performance, physical power-loss durability or MPI cleanup on remote machines.
