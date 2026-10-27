"""Concurrent persistent Linux worker with polling independent of execution."""

from typing import BinaryIO

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import datetime
import fcntl
from http.client import IncompleteRead
import json
import os
from pathlib import Path
import sys
from threading import Event
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .cpu import admission, pending_slots, validate_sample
from .config import WorkerConfig
from .local import execute_attempt
from .manager import is_loopback, read_token
from .models import JobSpecification, Resources, specification_from_dict
from .store import Attempt, Store, StoreError, TERMINAL


class ArtifactRequestError(ValueError):
    """Explicit artifact rejection with HTTP status retained for retry decisions."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        super().__init__(f'Result transfer HTTP {status_code}: {detail}')


class LinuxMonitor:
    """Sample affinity-scoped per-CPU deltas and MemAvailable from Linux procfs."""

    def __init__(self) -> None:
        self.previous_at = time.time()
        self.previous = self._cpu()
        self.previous_affinity = {cpu for cpu in os.sched_getaffinity(0)
                                  if str(cpu) in self.previous}

    @staticmethod
    def _cpu() -> dict[str, tuple[int, ...]]:
        return {parts[0][3:]: tuple(int(v) for v in parts[1:9])
                for line in Path('/proc/stat').read_text().splitlines()
                if (parts := line.split()) and parts[0].startswith('cpu')
                and (parts[0] == 'cpu' or parts[0][3:].isdigit())}

    def sample(self, worker: WorkerConfig) -> dict:
        # Timestamp precedes the read: a racing start is never considered reflected.
        end = time.time()
        current = self._cpu()
        affinity = {cpu for cpu in os.sched_getaffinity(0) if str(cpu) in current}
        scope = affinity & self.previous_affinity
        values = {}
        for cpu in scope:
            key = str(cpu)
            old, new = self.previous.get(key), current.get(key)
            if old is None or new is None or len(old) != 8 or len(new) != 8:
                continue
            deltas = [n - o for n, o in zip(new, old)]
            total = sum(deltas)
            # Counter reset and zero interval provide no idle evidence.
            # Only idle is idle; iowait remains busy for conservative admission.
            if min(deltas) >= 0 and total > 0:
                values[key] = 100 * (1 - deltas[3] / total)
        old, new = self.previous.get('', ()), current.get('', ())
        delta = sum(new) - sum(old)
        utilization = (100.0 if delta <= 0 or len(new) < 4 or len(old) < 4 else
                       max(0.0, min(100.0, 100 * (1 - (new[3] - old[3]) / delta))))
        start = self.previous_at
        self.previous, self.previous_affinity, self.previous_at = current, affinity, end
        memory = {line.split(':')[0]: int(line.split()[1]) * 1024 / 2**30
                  for line in Path('/proc/meminfo').read_text().splitlines()
                  if line.startswith(('MemTotal:', 'MemAvailable:'))}
        return {'capacity': asdict(worker.worker),
                'detected_cpu_slots': len(affinity),
                'detected_memory_gib': memory['MemTotal'],
                'observed_at': end, 'heartbeat_at': time.time(),
                'cpu_sample_started_at': start, 'per_cpu_utilization_percent': values,
                'cpu_utilization_percent': utilization,
                'available_memory_gib': memory['MemAvailable']}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req: Request, fp: object, code: int, msg: str,
                         headers: object, newurl: str) -> None:
        raise ValueError('Manager redirects are not permitted')


class Worker:
    """One journal and exclusive OS lock per worker, kept on local durable disk.

    Only newly accepted attempts may start. Existing accepted records are reported
    after restart and reconciled against Linux process identities, never relaunched.
    """

    def __init__(self, config: WorkerConfig, state_directory: str | Path) -> None:
        self.config = config
        endpoint = urlsplit(config.manager_endpoint)
        if (endpoint.scheme not in {'http', 'https'} or not endpoint.hostname
                or endpoint.port == 0 or endpoint.path not in ('', '/')
                or endpoint.query or endpoint.fragment or endpoint.username or endpoint.password):
            raise ValueError('Manager endpoint must be an HTTP(S) origin')
        if endpoint.scheme == 'http' and not is_loopback(endpoint.hostname or ''):
            raise ValueError('Plain HTTP workers require a literal loopback manager')
        self.token = read_token(config.token_file)
        state = Path(state_directory).resolve()
        state.mkdir(parents=True, exist_ok=True)
        self.lock = (state / 'worker.lock').open('a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.lock.close()
            raise ValueError('Worker state directory is already locked') from None
        identity = state / 'identity.json'
        expected = {'worker_id': config.worker.id, 'manager': config.manager_endpoint,
                    'work_root': str(Path(config.work_root).resolve())}
        if config.storage_mode != 'shared':
            expected.update(storage_mode=config.storage_mode, protocol=2)
        try:
            if identity.exists():
                if json.loads(identity.read_text()) != expected:
                    raise ValueError('Worker journal identity differs from configuration')
            else:
                with identity.open('x') as stream:
                    json.dump(expected, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                from .staging import sync_directory
                sync_directory(state)
            self.database = state / 'attempts.sqlite3'
            with Store(self.database):
                pass
        except BaseException:
            self.lock.close()
            raise
        self.pool = ThreadPoolExecutor(max_workers=config.worker.max_concurrent_jobs)
        self.futures: dict[str, Future[None]] = {}
        self.sample_started: set[str] = set()
        self.collection_pool = ThreadPoolExecutor(max_workers=1)
        self.collection_future: Future | None = None
        self.collection_stop = Event()
        self.cleanup_pool = ThreadPoolExecutor(max_workers=1)
        self.cleanup_future: Future | None = None
        self.cleanup_cursor = 0
        self.input_pool = ThreadPoolExecutor(max_workers=1)
        self.input_future: Future | None = None
        self.input_attempt: str | None = None
        self.input_cursor = ''
        self.monitor = LinuxMonitor()
        from uuid import uuid4
        self.session_id = str(uuid4())
        self.boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.ready = True
        self.draining = False
        self.proof = None
        self.acknowledged: dict[str, dict] = {}
        # Do not send bearer credentials through environment-configured proxies.
        from urllib.request import ProxyHandler
        self.opener = build_opener(ProxyHandler({}), NoRedirect())
        self.result_opener = build_opener(ProxyHandler({}), NoRedirect())

    def close(self) -> None:
        """Join execution and collection before releasing the journal lock."""
        self.collection_stop.set()
        try:
            self.input_pool.shutdown(wait=True)
            self.pool.shutdown(wait=True)
        finally:
            try:
                self.collection_pool.shutdown(wait=True)
            finally:
                try:
                    self.cleanup_pool.shutdown(wait=True)
                finally:
                    self.lock.close()

    def _queue_input(self, store: Store) -> None:
        """Rotate eligible reservations so failed inputs cannot starve peers."""
        if self.input_future is not None or self.collection_stop.is_set():
            return
        candidates = []
        for reserved in store.reservations():
            attempt_id = reserved.attempt_id
            if (store.attempt(attempt_id).state != 'assigned'
                    or store.launch_claimed(attempt_id) or store.cancellation(attempt_id)):
                continue
            transfer = store.input_transfer(attempt_id)
            if not transfer or transfer['status'] != 'ready':
                candidates.append(attempt_id)
        if candidates:
            attempt_id = next((value for value in candidates if value > self.input_cursor), candidates[0])
            self.input_cursor = attempt_id
            self.input_attempt = attempt_id
            self.input_future = self.input_pool.submit(self._prepare_input, attempt_id)

    def _prepare_input(self, attempt_id: str) -> None:
        with Store(self.database) as store:
            self._stage(store, attempt_id)

    def _execute(self, attempt_id: str) -> None:
        with Store(self.database) as store:
            execute_attempt(store, self.config, store.attempt(attempt_id))

    def tick(self) -> dict:
        """Send one heartbeat, report durable outcomes, and accept eligible concurrent jobs."""
        if self.input_future is not None and self.input_future.done():
            self.input_future.result()
            self.input_future = None
            self.input_attempt = None
        if self.cleanup_future is not None and self.cleanup_future.done():
            self.cleanup_future.result()
            self.cleanup_future = None
        if self.collection_future is not None and self.collection_future.done():
            self.collection_future.result()  # Surface unexpected collection failures.
            self.collection_future = None
        for attempt_id, future in tuple(self.futures.items()):
            if future.done():
                future.result()  # Surface unexpected executor failures.
                del self.futures[attempt_id]
        from .recovery import reconcile
        with Store(self.database) as store:
            for reservation in store.reservations():
                if reservation.attempt_id not in self.futures:
                    reconcile(store, store.attempt(reservation.attempt_id))
        # Snapshot the starts before sampling. A start racing with the sample
        # must still consume pending memory, including sub-millisecond starts.
        with Store(self.database) as store:
            reflected_starts = {r.attempt_id for r in store.reservations()
                                if store.attempt(r.attempt_id).started_at is not None}
        snapshot = self.monitor.sample(self.config)
        snapshot.update(ready=self.ready, draining=self.draining, continuation_ready=True)
        if self.config.storage_mode == 'staged':
            try:
                import shutil
                snapshot['staged_free_disk_gib'] = shutil.disk_usage(self.config.work_root).free / 2**30
            except OSError:
                snapshot['staged_free_disk_gib'] = None
        self.sample_started = reflected_starts
        if (self.config.storage_mode == 'staged' and self.config.cleanup_staged_files
                and self.cleanup_future is None and not self.collection_stop.is_set()):
            self.cleanup_future = self.cleanup_pool.submit(self._cleanup_results)
        reports = []
        with Store(self.database) as store:
            for attempt in (a for job in store.jobs() for a in store.job_attempts(job['job_id'])):
                report = {'attempt_id': attempt.attempt_id, 'state': attempt.state,
                          'started': attempt.started_at is not None, 'exit_code': attempt.exit_code,
                          'termination_evidence': attempt.termination_evidence, 'reason': attempt.reason}
                interruption = store.interruption(attempt.attempt_id)
                if interruption:
                    report['interruption'] = interruption
                from .checkpoints import error as checkpoint_error
                checkpoint_failure = checkpoint_error(store, attempt.attempt_id)
                if checkpoint_failure:
                    report['checkpoint_error'] = checkpoint_failure
                rejection = store.rejection(attempt.attempt_id)
                if rejection:
                    report['rejection'] = rejection
                from .checkpoints import record as checkpoint_record
                if attempt.termination_evidence:
                    collection = store.result_collection(attempt.attempt_id)
                    if collection is None:
                        store.set_result_collection(attempt.attempt_id, 'pending')
                        collection = store.result_collection(attempt.attempt_id)
                    report['result_collection'] = {key: collection[key] for key in ('status', 'reason')}
                transfer = store.input_transfer(attempt.attempt_id)
                if transfer is not None:
                    report['input_transfer'] = transfer
                if attempt.state == 'assigned':
                    if store.launch_claimed(attempt.attempt_id):
                        phase, reason = 'launch_claimed', 'Executor owns launch; running not yet recorded'
                    elif transfer and transfer['status'] == 'failed':
                        phase, reason = 'input_failed', transfer.get('reason') or 'Input preparation failed'
                    elif self.config.storage_mode == 'staged' and (not transfer or transfer['status'] != 'ready'):
                        phase, reason = 'preparing_inputs', 'Waiting for input preparer or verified publication'
                    else:
                        phase, reason = 'waiting_admission', getattr(self, 'launch_wait', {}).get(
                            attempt.attempt_id, 'Waiting for a normal poll and launch admission')
                    report['launch_progress'] = {'phase': phase, 'reason': reason[:512]}
                if self.acknowledged.get(attempt.attempt_id) != report:
                    reports.append(report)
        inventory = self._continuation_inventory()
        request = Request(self.config.manager_endpoint.rstrip('/') + '/v1/poll',
                          data=json.dumps({'proof': self.proof, 'snapshot': snapshot,
                                           'continuation_fingerprint': inventory,
                                           'reports': reports, 'protocol': 2, 'recovery_protocol': 1,
                                           'generation': {'boot_id': self.boot_id, 'session_id': self.session_id},
                                           'storage_mode': self.config.storage_mode}).encode(),
                          headers={'Content-Type': 'application/json',
                                   'X-Worker-ID': self.config.worker.id,
                                   'Authorization': 'Bearer ' + self.token})
        try:
            with self.opener.open(request, timeout=10) as response:
                result = json.load(response)
        except HTTPError as exc:
            if exc.code == 503:
                exc.close()
                return {'assignments': [], 'retry_after': 1}
            if exc.code == 409:
                exc.close()
                raise ValueError('Incompatible manager storage protocol or mode') from None
            raise
        if self.config.storage_mode == 'staged':
            root = Path(self.config.work_root).resolve(strict=True)
            if result.get('protocol') != 2 or result.get('storage_mode') != 'staged':
                raise ValueError('Staged inputs require a protocol 2 staged manager')
        else:
            if result.get('storage_mode', 'shared') != 'shared':
                raise ValueError('Manager and worker storage modes differ')
            # The challenge path must itself be contained in the configured root.
            try:
                challenge = Path(result['challenge']).resolve(strict=True)
                root = Path(self.config.work_root).resolve(strict=True)
                if challenge.parent != root or not challenge.name.startswith('.shared-check-'):
                    raise ValueError('Manager and worker shared roots differ')
                self.proof = challenge.read_text()
            except OSError as exc:
                # Access failure neither acknowledges reports nor authorizes a launch.
                print(f'Shared storage check deferred: {type(exc).__name__}: {exc}', file=sys.stderr)
                return result
        # Challenge-only replies have not processed reports.
        if 'policy' not in result:
            return result
        for report in reports:
            self.acknowledged[report['attempt_id']] = report
        # Expiry can invalidate a previously acknowledged report while the
        # journal stays unchanged. Re-read and resend it on the next poll,
        # including while an executor is active or staged inputs are ready.
        for assignment in result['assignments']:
            if assignment['attempt']['state'] == 'unknown':
                self.acknowledged.pop(assignment['attempt']['attempt_id'], None)
        with Store(self.database) as store:
            for command in result.get('cancellations', []):
                try:
                    store.attempt(command['attempt_id'])
                except ValueError:
                    continue  # Pending assignments are accepted below before cancellation.
                store.request_cancel(command['attempt_id'], command['reason'])
                if self.config.storage_mode == 'staged' and not store.launch_claimed(command['attempt_id']):
                    store.transition(command['attempt_id'], 'cancelled', reason=command['reason'],
                                     termination_evidence='Launch did not start a process')
        if (self.collection_future is None
                and not self.collection_stop.is_set()):
            # Freeze eligibility after the manager has accepted terminal reports.
            # A process finishing during this pass must wait for its own poll.
            eligible = tuple(attempt_id for attempt_id, report in self.acknowledged.items()
                             if report['state'] in TERMINAL and report['termination_evidence'])
            if eligible:
                self.collection_future = self.collection_pool.submit(self._collect_results, eligible)
        with Store(self.database) as store:
            if self.config.storage_mode == 'staged':
                closed = (result['paused'] or not self.ready or self.draining
                          or not self.config.worker.enabled or not result['manager_capacity']['enabled'])
                for reserved in store.reservations():
                    if reserved.attempt_id not in self.futures:
                        transfer = store.input_transfer(reserved.attempt_id)
                        if closed or (transfer and transfer['status'] == 'ready'
                                      and self.input_attempt != reserved.attempt_id):
                            self._launch_staged(store, reserved.attempt_id, result, snapshot)
            for assignment in result['assignments']:
                spec_data = assignment['specification']
                spec = specification_from_dict(spec_data)
                if spec.storage_mode != self.config.storage_mode:
                    raise ValueError('Assignment storage mode differs from worker')
                attempt = assignment['attempt']
                if attempt['worker_id'] != self.config.worker.id:
                    raise ValueError('Assignment identity mismatch')
                try:
                    store.attempt(attempt['attempt_id'])
                except ValueError:
                    pass
                else:
                    continue  # Existing journals are reconciled above, never reaccepted.
                if (result.get('prelaunch_protocol') == 1
                        and (result['paused'] or not self.ready or self.draining
                             or not self.config.worker.enabled or not result['manager_capacity']['enabled'])):
                    directory = (root / spec.job_id / attempt['attempt_id']
                                 if spec.storage_mode == 'staged' else Path(attempt['directory']))
                    # Preserve containment checks even for never-launched work.
                    if not directory.is_relative_to(root) or '..' in directory.parts:
                        raise ValueError('Rejected assignment outside worker root')
                    with store.scheduling_round():
                        self._submit_specification(store, spec)
                        self._accept_retry(store, spec.job_id)
                        self._assign(store, spec, attempt, directory, assignment)
                        if directory.exists():
                            store.transition(attempt['attempt_id'], 'unknown',
                                             reason='Existing runtime directory; launch identity uncertain')
                        else:
                            store.reject_launch(attempt['attempt_id'], 'not_ready',
                                                'Worker closed to new launches', time.time() + 30)
                    continue
                directory = Path(attempt['directory'])
                cancelled = next((c for c in result.get('cancellations', [])
                                  if c['attempt_id'] == attempt['attempt_id']), None)
                if spec.storage_mode == 'staged':
                    if result['paused'] and not cancelled:
                        continue
                    from .staging import safe_path
                    for identifier in (spec.job_id, attempt['attempt_id']):
                        if len(safe_path(identifier).parts) != 1:
                            raise ValueError('Invalid staged assignment identifier')
                    job_root = root / spec.job_id
                    directory = job_root / attempt['attempt_id']
                    if any(p.is_symlink() for p in (job_root, directory)):
                        raise ValueError('Unsafe staged runtime directory')
                    with store.scheduling_round():
                        self._submit_specification(store, spec)
                        self._accept_retry(store, spec.job_id)
                        self._assign(store, spec, attempt, directory, assignment)
                    if cancelled:
                        self._prepare_staged_directory(store, attempt['attempt_id'])
                        store.request_cancel(attempt['attempt_id'], cancelled['reason'])
                        store.transition(attempt['attempt_id'], 'cancelled', reason=cancelled['reason'],
                            termination_evidence='Launch did not start a process')
                    continue
                try:
                    job_root = Path(spec.work_directory).resolve(strict=True)
                except OSError as exc:
                    # Access failure is not evidence that an attempt has ended.
                    # Leave it unaccepted and reserved until validation can pass.
                    print(f'Assignment {attempt["attempt_id"]} deferred: '
                          f'{type(exc).__name__}: {exc}', file=sys.stderr)
                    continue
                if (not job_root.is_relative_to(root) or job_root == root
                        or directory != job_root / attempt['attempt_id']
                        or Path(attempt['attempt_id']).name != attempt['attempt_id']):
                    raise ValueError('Assignment outside shared job directory')
                if result['paused'] and not cancelled:
                    continue
                r = spec.resources
                capacity = self.config.worker
                incompatible = (r.cpu_slots > capacity.cpu_slots
                                or r.memory_gib > capacity.memory_budget_gib)
                if not cancelled and not self._can_launch(store, spec.resources, result, snapshot):
                    if (result.get('prelaunch_protocol') != 1
                            or (not incompatible and store.launch_deferred(attempt['attempt_id'], time.time()))):
                        continue
                # Persist acceptance before the executor validates copied inputs,
                # so pre-launch failures can be durably reported to the manager.
                with store.scheduling_round():
                    self._submit_specification(store, spec)
                    self._accept_retry(store, spec.job_id)
                    accepted = self._assign(store, spec, attempt, directory, assignment)
                    can_launch = not directory.exists()
                    if can_launch and result.get('prelaunch_protocol') == 1:
                        store.accept_prelaunch(accepted.attempt_id)
                    if not can_launch:
                        store.transition(accepted.attempt_id, 'unknown',
                                         reason='Attempt directory already exists; launch identity uncertain')
                if cancelled and can_launch:
                    store.request_cancel(accepted.attempt_id, cancelled['reason'])
                    store.transition(accepted.attempt_id, 'cancelled', reason=cancelled['reason'],
                                     termination_evidence='Launch did not start a process')
                elif can_launch:
                    if not self._can_launch(store, spec.resources, result, snapshot, accepted.attempt_id):
                        if result.get('prelaunch_protocol') == 1:
                            store.reject_launch(accepted.attempt_id,
                                                'incompatible' if incompatible else 'headroom',
                                                'Local launch admission unavailable', time.time() + 30)
                        continue
                    if store.claim_launch(accepted.attempt_id):
                        self.futures[accepted.attempt_id] = self.pool.submit(self._execute, accepted.attempt_id)
            if self.config.storage_mode == 'staged' and not closed:
                self._queue_input(store)
        if hasattr(self, 'cpu_admission'):
            result['local_cpu_admission'] = self.cpu_admission
        return result

    def _continuation_inventory(self) -> str | None:
        """Advertise actual build identity; ordinary jobs survive inventory failures."""
        from .continuation import fingerprint_build, ContinuationError
        try:
            identity = fingerprint_build(Path(self.config.execution.lammps_path))
        except ContinuationError as exc:
            if getattr(self, 'continuation_inventory_error', None) != str(exc):
                print(f'Continuation inventory unavailable: {exc}', file=sys.stderr)
            self.continuation_inventory_error = str(exc)
            return None
        self.continuation_inventory_error = None
        return identity

    def _assign(self, store: Store, spec: JobSpecification, attempt: dict,
                directory: Path, assignment: dict) -> 'Attempt':
        """Persist the manager's continuation contract before any launch right."""
        accepted = store.assign(spec.job_id, attempt['attempt_id'], self.config.worker.id, str(directory))
        if 'checkpoint_contract' in assignment:
            from .checkpoints import decode_contract, expect
            expect(store, decode_contract(assignment['checkpoint_contract']))
        return accepted

    def _submit_specification(self, store: Store, spec: JobSpecification) -> None:
        """Preserve legacy local snapshots; recovery authorization lives on the manager."""
        try:
            existing = store.specification(spec.job_id)
        except StoreError:
            store.submit(spec)
        else:
            # Legacy transports omitted this manager-owned policy. Upgrading a
            # worker must not rewrite its old snapshot or invalidate identical inputs.
            store.submit(replace(spec, infrastructure_recovery=existing.infrastructure_recovery))

    def _accept_retry(self, store: Store, job_id: str) -> None:
        """Mirror a manager-authorized successor without spending budgets locally."""
        store.accept_successor(job_id)

    def _prepare_staged_directory(self, store: Store, attempt_id: str) -> bool:
        """Defer filesystem failures without losing acceptance or stopping polls."""
        from .cleanup import prepare_directory
        try:
            prepare_directory(store, attempt_id, Path(self.config.work_root).resolve(strict=True))
        except (OSError, ValueError) as exc:
            destination = Path(store.attempt(attempt_id).directory) / 'inputs'
            store.set_input_transfer(attempt_id, str(destination), 'failed',
                                     f'{type(exc).__name__}: {exc}')
            return False
        return True

    def _stage(self, store: Store, attempt_id: str) -> None:
        """Prepare inputs independently; only a later normal poll may authorize launch."""
        from .staging import publish_inputs
        attempt = store.attempt(attempt_id)
        if attempt.state != 'assigned':
            return
        previous = store.input_transfer(attempt_id)
        if previous and previous['status'] == 'ready':
            return
        if not self._prepare_staged_directory(store, attempt_id):
            return
        destination = Path(attempt.directory) / 'inputs'
        store.set_input_transfer(attempt_id, str(destination), 'downloading')
        spec = store.specification(attempt.job_id)

        def check_cancelled() -> None:
            if self.collection_stop.is_set() or store.attempt(attempt_id).state != 'assigned':
                raise ValueError('Input preparation stopped')

        def fetch(index: int) -> BinaryIO:
            check_cancelled()
            request = Request(self.config.manager_endpoint.rstrip('/') +
                              f'/v2/inputs/{attempt_id}/files/{index}',
                              headers={'X-Worker-ID': self.config.worker.id,
                                       'Authorization': 'Bearer ' + self.token})
            return self.opener.open(request, timeout=10)

        last_report = 0.0

        def verified(index: int) -> None:
            nonlocal last_report
            check_cancelled()
            if time.monotonic() - last_report < 1 and index != len(spec.input_manifest) - 1:
                return
            last_report = time.monotonic()
            # Telemetry failure must not invalidate a verified simulation input.
            from urllib.error import URLError
            request = Request(self.config.manager_endpoint.rstrip('/') +
                              f'/v2/input-verified/{attempt_id}', data=json.dumps({'index': index}).encode(),
                              headers={'X-Worker-ID': self.config.worker.id,
                                       'Authorization': 'Bearer ' + self.token,
                                       'Content-Type': 'application/json'})
            try:
                with self.opener.open(request, timeout=1) as response:
                    response.read(128)
            except (OSError, URLError):
                pass  # Missing verification telemetry remains unknown until the ready report.

        try:
            from urllib.error import URLError
            root = Path(self.config.work_root).resolve(strict=True)
            for parent in (destination.parent.parent, destination.parent):
                if parent.is_symlink() or parent.resolve() != parent or not parent.is_relative_to(root):
                    raise ValueError('Unsafe staged runtime directory')
            publish_inputs(destination, spec.input_manifest, fetch,
                           dict(max_files=self.config.max_input_files,
                                max_file_bytes=self.config.max_input_file_bytes,
                                max_total_bytes=self.config.max_input_total_bytes), verified=verified, progress=check_cancelled)
        except (OSError, ValueError, IncompleteRead, URLError) as exc:
            store.set_input_transfer(attempt_id, str(destination), 'failed',
                                     f'{type(exc).__name__}: {exc}')
            return
        store.set_input_transfer(attempt_id, str(destination), 'ready',
                                 'Inputs verified')
        # The normal polling thread consumes this future before launch admission.

    def _launch_staged(self, store: Store, attempt_id: str, result: dict, snapshot: dict) -> None:
        """Consume a durable launch right only after resource and input checks."""
        attempt = store.attempt(attempt_id)
        if attempt.state != 'assigned' or store.launch_claimed(attempt_id):
            return
        if not hasattr(self, 'launch_wait'):
            self.launch_wait = {}
        self.launch_wait[attempt_id] = 'Waiting for current input/result protocol readiness'
        closed = (result['paused'] or not self.ready or self.draining
                  or not self.config.worker.enabled or not result['manager_capacity']['enabled'])
        if closed:
            self.launch_wait[attempt_id] = 'Worker paused, draining, disabled or not ready'
        if closed and result.get('prelaunch_protocol') == 1:
            store.reject_launch(attempt_id, 'not_ready', 'Worker closed to new launches', time.time() + 30)
            return
        transfer = store.input_transfer(attempt_id)
        if (not transfer or transfer['status'] != 'ready'
                or not result.get('staged_launch_enabled') or result.get('results_protocol') != 1):
            return
        directory = Path(attempt.directory)
        root = Path(self.config.work_root).resolve(strict=True)
        if directory.resolve() != directory or not directory.is_relative_to(root):
            store.transition(attempt_id, 'unknown', reason='Unsafe staged runtime directory before launch')
            return
        r = store.specification(attempt.job_id).resources
        if not self._can_launch(store, r, result, snapshot, attempt_id):
            self.launch_wait[attempt_id] = 'Waiting for fresh metrics, CPU/memory headroom or executor capacity'
            incompatible = (r.cpu_slots > min(self.config.worker.cpu_slots,
                                result['manager_capacity']['cpu_slots'], snapshot['detected_cpu_slots'])
                            or r.memory_gib > min(self.config.worker.memory_budget_gib,
                                result['manager_capacity']['memory_budget_gib'], snapshot['detected_memory_gib']))
            if (result.get('prelaunch_protocol') == 1
                    and (incompatible or not store.launch_deferred(attempt_id, time.time()))):
                store.reject_launch(attempt_id, 'incompatible' if incompatible else 'headroom',
                                    'Local launch admission unavailable', time.time() + 30)
            return
        if store.claim_launch(attempt_id):
            self.futures[attempt_id] = self.pool.submit(self._execute, attempt_id)

    def _can_launch(self, store: Store, resources: Resources, result: dict,
                    snapshot: dict, candidate_id: str | None = None) -> bool:
        """Recheck total reservations and consume pending headroom per launch.

        A candidate already reserved locally is excluded exactly once. Starts
        after the sample remain pending even if their running transition wins
        the race with this check. Unknown/cancelling reservations are retained.
        """
        manager = result['manager_capacity']
        policy = result['policy']
        capacity = self.config.worker
        now = time.time()
        observed = snapshot['observed_at']
        if (result['paused'] or not self.ready or self.draining or not capacity.enabled or not manager['enabled']
                or observed > now or now - observed > policy['metrics_max_age_seconds']):
            return False
        reservations = [r for r in store.reservations() if r.attempt_id != candidate_id]
        pending = 0.0
        for reservation in reservations:
            attempt = store.attempt(reservation.attempt_id)
            # Staged acceptance reserves quotas before launch admission. Other
            # unclaimed assignments have not consumed sampled memory yet; making
            # them consume it here would let fitting jobs block each other.
            # Keep uncertain/cancelling records conservative, and count a claim
            # immediately, even before its executor starts.
            if (self.config.storage_mode == 'staged' and attempt.state == 'assigned'
                    and not store.launch_claimed(reservation.attempt_id)):
                continue
            started = attempt.started_at
            if (reservation.attempt_id not in self.sample_started or started is None
                    or datetime.fromisoformat(started.replace('Z', '+00:00')).timestamp()
                    >= observed):
                pending += reservation.memory_gib
        values = snapshot.get('per_cpu_utilization_percent')
        start = snapshot.get('cpu_sample_started_at')
        validate_sample(values, start, observed, snapshot['detected_cpu_slots'])
        pending_cpu = (pending_slots(
            store, capacity.id, start, candidate_id=candidate_id,
            skip_unclaimed_staged=self.config.storage_mode == 'staged')
            if policy['account_external_load'] else 0)
        self.cpu_admission = admission(
            values, start, threshold=policy['max_cpu_utilization_percent'],
            pending=pending_cpu, requested=resources.cpu_slots,
            used=sum(r.cpu_slots for r in reservations), minimum=policy.get('min_slots', 0),
            reserve=policy.get('reserve_slots', 0),
            capacity=min(capacity.cpu_slots, manager['cpu_slots'], snapshot['detected_cpu_slots']),
            enabled=policy['account_external_load'])
        if (policy['account_external_load']
                and self.cpu_admission != getattr(self, '_last_cpu_admission', None)):
            print('CPU admission: ' + json.dumps(self.cpu_admission, sort_keys=True), file=sys.stderr)
            self._last_cpu_admission = self.cpu_admission
        return (sum(r.cpu_slots for r in reservations) + resources.cpu_slots
                    <= min(capacity.cpu_slots, manager['cpu_slots'], snapshot['detected_cpu_slots'])
                and sum(r.memory_gib for r in reservations) + resources.memory_gib
                    <= min(capacity.memory_budget_gib, manager['memory_budget_gib'],
                           snapshot['detected_memory_gib'])
                and len(reservations) + 1 <= min(capacity.max_concurrent_jobs,
                                                manager['max_concurrent_jobs'])
                and len(self.futures) < capacity.max_concurrent_jobs
                and pending + resources.memory_gib + policy['reserve_memory_gib']
                    <= snapshot['available_memory_gib']
                and self.cpu_admission['allowed'])

    def _collect_results(self, attempt_ids: tuple[str, ...]) -> None:
        """Retry delivery of terminal journals; never rerun a simulation."""
        from .results import enumerate_results, open_regular, output_limits
        from urllib.error import URLError
        with Store(self.database) as store:
            for attempt_id in attempt_ids:
                attempt = store.attempt(attempt_id)
                if self.collection_stop.is_set():
                    return
                if attempt.state not in TERMINAL or not attempt.termination_evidence:
                    continue
                from .checkpoints import record as checkpoint_record
                checkpoint = checkpoint_record(store, attempt_id)
                if checkpoint and attempt.state == 'succeeded' and not checkpoint['acknowledged']:
                    try:
                        self._collect_checkpoint(store, attempt_id)
                    except (OSError, ValueError, IncompleteRead, URLError) as exc:
                        from .checkpoints import error, set_error
                        reason = f'{type(exc).__name__}: {exc}'[:2048]
                        if error(store, attempt_id) != reason:
                            print(f'Checkpoint {attempt_id} deferred: {reason}', file=sys.stderr)
                        set_error(store, attempt_id, reason)
                collection = store.result_collection(attempt.attempt_id)
                if collection and collection['status'] == 'collected':
                    continue
                try:
                    manifest = collection['manifest'] if collection else None
                    if manifest is None:
                        directory = Path(attempt.directory)
                        if directory.resolve() != directory:
                            raise ValueError('Unsafe result runtime directory')
                        directory.mkdir(parents=True, exist_ok=True)
                        from .staging import sync_directory
                        outcome = directory / 'outcome.json'
                        if not outcome.exists():
                            # A crash before publication leaves only a hidden
                            # temporary file; never freeze a partial outcome.
                            import tempfile
                            fd, temporary = tempfile.mkstemp(prefix='.outcome-', dir=directory)
                            try:
                                with os.fdopen(fd, 'w') as metadata:
                                    json.dump(asdict(attempt), metadata)
                                    metadata.flush()
                                    os.fsync(metadata.fileno())
                                os.replace(temporary, outcome)
                                sync_directory(directory)
                            finally:
                                Path(temporary).unlink(missing_ok=True)
                        manifest = enumerate_results(Path(attempt.directory), output_limits(self.config))
                    if self.collection_stop.is_set():
                        return
                    if store._db.execute('SELECT 1 FROM result_commit_intents WHERE attempt_id=?',
                                         (attempt.attempt_id,)).fetchone():
                        acknowledgement = self._result_request(attempt.attempt_id, 'commit', {})
                        if acknowledgement.get('status') != 'collected':
                            raise ValueError('Manager did not acknowledge durable result publication')
                        store.set_result_collection(attempt.attempt_id, 'collected')
                        store._db.execute('DELETE FROM result_commit_intents WHERE attempt_id=?', (attempt.attempt_id,))
                        continue
                    store.set_result_collection(attempt.attempt_id, 'uploading', manifest)
                    reply = self._result_request(attempt.attempt_id, 'manifest', manifest)
                    if reply['status'] != 'collected':
                        verified = set(reply.get('verified_files', ()))
                        for index, item in enumerate(manifest['files']):
                            if index in verified:
                                continue
                            if self.collection_stop.is_set():
                                return
                            with open_regular(Path(attempt.directory), item['path']) as incoming:
                                self._result_request(attempt.attempt_id, str(index), incoming, item['size'])
                    # Always replay commit: publication/DB acknowledgement may
                    # have survived an earlier lost response.
                    if self.collection_stop.is_set():
                        return
                    store._db.execute('INSERT OR IGNORE INTO result_commit_intents VALUES(?)', (attempt.attempt_id,))
                    acknowledgement = self._result_request(attempt.attempt_id, 'commit', {})
                    if acknowledgement.get('status') != 'collected':
                        raise ValueError('Manager did not acknowledge durable result publication')
                    store.set_result_collection(attempt.attempt_id, 'collected')
                    store._db.execute('DELETE FROM result_commit_intents WHERE attempt_id=?', (attempt.attempt_id,))
                except (OSError, ValueError, IncompleteRead, URLError) as exc:
                    # An explicit rejection (e.g. missing/corrupt spool) needs a
                    # new verified inventory. A lost response retains commit intent.
                    if isinstance(exc, ArtifactRequestError) and exc.status_code == 400:
                        store._db.execute('DELETE FROM result_commit_intents WHERE attempt_id=?', (attempt.attempt_id,))
                    store.set_result_collection(attempt.attempt_id, 'failed',
                                                reason=f'{type(exc).__name__}: {exc}')

    def _collect_checkpoint(self, store: Store, attempt_id: str) -> None:
        """Retain local state until the manager acknowledges the exact identity."""
        import hashlib
        from .checkpoints import record, bind_manifest, validate_boundary
        from .staging import open_regular
        from .results import output_limits
        from .models import InputFile
        checkpoint = record(store, attempt_id)
        directory = Path(store.attempt(attempt_id).directory)
        validate_boundary(directory, checkpoint['contract'])
        manifest = checkpoint['manifest']
        if manifest is None:
            files = []
            for name in checkpoint['contract']['required_state']:
                with open_regular(directory / 'work', name) as incoming:
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := incoming.read(65536):
                        size += len(chunk)
                        if size > self.config.max_output_file_bytes:
                            raise ValueError('Checkpoint exceeds output size limit')
                        digest.update(chunk)
                files.append(asdict(InputFile(name, size, digest.hexdigest())))
            manifest = dict(contract=checkpoint['contract'], files=files)
        bind_manifest(store, self.config.worker.id, attempt_id, manifest, output_limits(self.config))
        pending_commit = store._db.execute('SELECT 1 FROM checkpoint_commit_intents WHERE attempt_id=?',
                                           (attempt_id,)).fetchone()
        try:
            if not pending_commit:
                reply = self._result_request(attempt_id, 'manifest', manifest, checkpoint=True)
                if not reply.get('acknowledged') and self.config.storage_mode == 'staged':
                    verified = set(reply.get('verified_files', ()))
                    for index, item in enumerate(manifest['files']):
                        if index in verified:
                            continue
                        if self.collection_stop.is_set():
                            return
                        with open_regular(directory / 'work', item['path']) as incoming:
                            self._result_request(attempt_id, str(index), incoming, item['size'], checkpoint=True)
                store._db.execute('INSERT OR IGNORE INTO checkpoint_commit_intents VALUES(?)', (attempt_id,))
            reply = self._result_request(attempt_id, 'commit', {}, checkpoint=True)
        except ArtifactRequestError as exc:
            if exc.status_code == 400:
                store._db.execute('DELETE FROM checkpoint_commit_intents WHERE attempt_id=?', (attempt_id,))
            raise
        if not reply.get('acknowledged') or reply.get('identity') != record(store, attempt_id)['identity']:
            raise ValueError('Manager did not acknowledge matching checkpoint')
        with store.scheduling_round():
            store._db.execute('UPDATE checkpoints SET acknowledged=1 WHERE attempt_id=?', (attempt_id,))
            store._db.execute('DELETE FROM checkpoint_commit_intents WHERE attempt_id=?', (attempt_id,))
            from .checkpoints import set_error
            set_error(store, attempt_id, None)

    def _cleanup_results(self) -> None:
        """One bounded filesystem pass, independent of polling and result uploads."""
        from .cleanup import cleanup_attempt
        root = Path(self.config.work_root).resolve()
        protected = (self.database.parent, Path(self.config.token_file))
        with Store(self.database) as store:
            rows = store.cleanup_candidates(self.cleanup_cursor)
            if not rows:
                self.cleanup_cursor = 0
                rows = store.cleanup_candidates()
            for cursor, attempt_id in rows:
                if self.collection_stop.is_set():
                    return
                manager_directory = Path(store.specification(store.attempt(attempt_id).job_id).work_directory)
                cleanup_attempt(store, attempt_id, root, protected,
                                manager_directory=manager_directory)
                self.cleanup_cursor = cursor

    def _result_request(self, attempt_id: str, operation: str, payload: object,
                        size: int | None = None, *, checkpoint: bool = False) -> dict:
        data = json.dumps(payload).encode() if size is None else payload
        namespace = 'checkpoints' if checkpoint else 'results'
        request = Request(self.config.manager_endpoint.rstrip('/') + f'/v2/{namespace}/{attempt_id}/{operation}',
                          data=data, method='POST' if size is None else 'PUT',
                          headers={'X-Worker-ID': self.config.worker.id,
                                   'Authorization': 'Bearer ' + self.token,
                                   'Content-Length': str(len(data) if size is None else size),
                                   'Content-Type': 'application/json' if size is None else 'application/octet-stream'})
        try:
            with self.result_opener.open(request, timeout=self.config.result_request_timeout_seconds) as response:
                return json.load(response)
        except HTTPError as exc:
            if exc.code == 503:
                exc.close()
                raise TimeoutError('Manager database busy; retry later') from None
            try:
                detail = json.loads(exc.read(4096)).get('error', 'Artifact request rejected')
            except (ValueError, AttributeError):
                detail = 'Artifact request rejected'
            finally:
                exc.close()
            raise ArtifactRequestError(exc.code, detail) from None

    def run(self) -> None:
        """Poll continuously; transport errors retain reservations and local work."""
        from urllib.error import URLError
        busy_backoff = 1.0
        while True:
            delay = self.config.heartbeat_interval_seconds
            try:
                result = self.tick()
                if result.get('retry_after'):
                    delay = max(delay, busy_backoff)
                    busy_backoff = min(30.0, busy_backoff * 2)
                else:
                    busy_backoff = 1.0
            except (URLError, TimeoutError, ConnectionError, IncompleteRead) as exc:
                print(f'Worker poll failed: {type(exc).__name__}', file=sys.stderr)
            time.sleep(delay)
