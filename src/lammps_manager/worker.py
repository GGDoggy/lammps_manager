"""Concurrent persistent Linux worker with polling independent of execution."""

from typing import BinaryIO

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict
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

from .config import WorkerConfig
from .local import execute_attempt
from .manager import is_loopback, read_token
from .models import Resources, specification_from_dict
from .store import Store, TERMINAL


class LinuxMonitor:
    """Sample aggregate CPU deltas and MemAvailable from Linux procfs."""

    def __init__(self) -> None:
        self.previous = self._cpu()

    @staticmethod
    def _cpu() -> tuple[int, int]:
        values = [int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
        return sum(values), values[3] + values[4]

    def sample(self, worker: WorkerConfig) -> dict:
        total, idle = self._cpu()
        old_total, old_idle = self.previous
        self.previous = total, idle
        delta = total - old_total
        # No measurable interval is not evidence of an idle host.
        utilization = 100.0 if delta <= 0 else max(0.0, min(100.0, 100 * (1 - (idle - old_idle) / delta)))
        memory = {line.split(':')[0]: int(line.split()[1]) * 1024 / 2**30
                  for line in Path('/proc/meminfo').read_text().splitlines()
                  if line.startswith(('MemTotal:', 'MemAvailable:'))}
        return {'capacity': asdict(worker.worker),
                'detected_cpu_slots': len(os.sched_getaffinity(0)),
                'detected_memory_gib': memory['MemTotal'],
                'observed_at': time.time(), 'heartbeat_at': time.time(),
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
        self.monitor = LinuxMonitor()
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
            self.pool.shutdown(wait=True)
        finally:
            try:
                self.collection_pool.shutdown(wait=True)
            finally:
                self.lock.close()

    def _execute(self, attempt_id: str) -> None:
        with Store(self.database) as store:
            execute_attempt(store, self.config, store.attempt(attempt_id))

    def tick(self) -> dict:
        """Send one heartbeat, report durable outcomes, and accept eligible concurrent jobs."""
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
        self.sample_started = reflected_starts
        reports = []
        with Store(self.database) as store:
            for attempt in (a for job in store.jobs() for a in store.job_attempts(job['job_id'])):
                report = {'attempt_id': attempt.attempt_id, 'state': attempt.state,
                          'started': attempt.started_at is not None, 'exit_code': attempt.exit_code,
                          'termination_evidence': attempt.termination_evidence, 'reason': attempt.reason}
                if self.config.storage_mode == 'staged' and attempt.termination_evidence:
                    collection = store.result_collection(attempt.attempt_id)
                    if collection is None:
                        store.set_result_collection(attempt.attempt_id, 'pending')
                        collection = store.result_collection(attempt.attempt_id)
                    report['result_collection'] = {key: collection[key] for key in ('status', 'reason')}
                transfer = store.input_transfer(attempt.attempt_id)
                if transfer is not None:
                    report['input_transfer'] = transfer
                if self.acknowledged.get(attempt.attempt_id) != report:
                    reports.append(report)
        request = Request(self.config.manager_endpoint.rstrip('/') + '/v1/poll',
                          data=json.dumps({'proof': self.proof, 'snapshot': snapshot,
                                           'reports': reports, 'protocol': 2,
                                           'storage_mode': self.config.storage_mode}).encode(),
                          headers={'Content-Type': 'application/json',
                                   'X-Worker-ID': self.config.worker.id,
                                   'Authorization': 'Bearer ' + self.token})
        try:
            with self.opener.open(request, timeout=10) as response:
                result = json.load(response)
        except HTTPError as exc:
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
        if (self.config.storage_mode == 'staged' and self.collection_future is None
                and not self.collection_stop.is_set()):
            # Freeze eligibility after the manager has accepted terminal reports.
            # A process finishing during this pass must wait for its own poll.
            eligible = tuple(attempt_id for attempt_id, report in self.acknowledged.items()
                             if report['state'] in TERMINAL and report['termination_evidence'])
            if eligible:
                self.collection_future = self.collection_pool.submit(self._collect_results, eligible)
        with Store(self.database) as store:
            if self.config.storage_mode == 'staged':
                for reserved in store.reservations():
                    if reserved.attempt_id not in self.futures:
                        self._stage(store, reserved.attempt_id)
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
                        store.submit(spec)
                        if store.job_state(spec.job_id) == 'failed':
                            store.retry(spec.job_id)
                        store.assign(spec.job_id, attempt['attempt_id'], self.config.worker.id, str(directory))
                    if cancelled:
                        store.request_cancel(attempt['attempt_id'], cancelled['reason'])
                        store.transition(attempt['attempt_id'], 'cancelled', reason=cancelled['reason'],
                            termination_evidence='Launch did not start a process')
                    else:
                        self._stage(store, attempt['attempt_id'])
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
                if not cancelled and not self._can_launch(store, spec.resources, result, snapshot):
                    continue  # Keep the manager reservation pending for a later poll.
                # Persist acceptance before the executor validates copied inputs,
                # so pre-launch failures can be durably reported to the manager.
                with store.scheduling_round():
                    store.submit(spec)
                    if store.job_state(spec.job_id) == 'failed':
                        store.retry(spec.job_id)
                    accepted = store.assign(spec.job_id, attempt['attempt_id'], self.config.worker.id,
                                            str(directory))
                    can_launch = not directory.exists()
                    if not can_launch:
                        store.transition(accepted.attempt_id, 'unknown',
                                         reason='Attempt directory already exists; launch identity uncertain')
                if cancelled and can_launch:
                    store.request_cancel(accepted.attempt_id, cancelled['reason'])
                    store.transition(accepted.attempt_id, 'cancelled', reason=cancelled['reason'],
                                     termination_evidence='Launch did not start a process')
                elif can_launch:
                    self.futures[accepted.attempt_id] = self.pool.submit(self._execute, accepted.attempt_id)
        return result

    def _stage(self, store: Store, attempt_id: str) -> None:
        """Retry input transfer independently of process acceptance; never launch."""
        from .staging import publish_inputs
        attempt = store.attempt(attempt_id)
        if attempt.state != 'assigned':
            return
        previous = store.input_transfer(attempt_id)
        if previous and previous['status'] == 'ready':
            return
        destination = Path(attempt.directory) / 'inputs'
        store.set_input_transfer(attempt_id, str(destination), 'downloading')
        spec = store.specification(attempt.job_id)

        def fetch(index: int) -> BinaryIO:
            request = Request(self.config.manager_endpoint.rstrip('/') +
                              f'/v2/inputs/{attempt_id}/files/{index}',
                              headers={'X-Worker-ID': self.config.worker.id,
                                       'Authorization': 'Bearer ' + self.token})
            return self.opener.open(request, timeout=10)

        try:
            from urllib.error import URLError
            root = Path(self.config.work_root).resolve(strict=True)
            for parent in (destination.parent.parent, destination.parent):
                if parent.is_symlink() or parent.resolve() != parent or not parent.is_relative_to(root):
                    raise ValueError('Unsafe staged runtime directory')
            publish_inputs(destination, spec.input_manifest, fetch,
                           dict(max_files=self.config.max_input_files,
                                max_file_bytes=self.config.max_input_file_bytes,
                                max_total_bytes=self.config.max_input_total_bytes))
        except (OSError, ValueError, IncompleteRead, URLError) as exc:
            store.set_input_transfer(attempt_id, str(destination), 'failed',
                                     f'{type(exc).__name__}: {exc}')
            return
        store.set_input_transfer(attempt_id, str(destination), 'ready',
                                 'Inputs verified')

    def _launch_staged(self, store: Store, attempt_id: str, result: dict, snapshot: dict) -> None:
        """Consume a durable launch right only after resource and input checks."""
        attempt = store.attempt(attempt_id)
        transfer = store.input_transfer(attempt_id)
        if (attempt.state != 'assigned' or not transfer or transfer['status'] != 'ready'
                or result['paused'] or not result.get('staged_launch_enabled')
                or result.get('results_protocol') != 1 or store.launch_claimed(attempt_id)):
            return
        directory = Path(attempt.directory)
        root = Path(self.config.work_root).resolve(strict=True)
        if directory.resolve() != directory or not directory.is_relative_to(root):
            store.transition(attempt_id, 'unknown', reason='Unsafe staged runtime directory before launch')
            return
        r = store.specification(attempt.job_id).resources
        if not self._can_launch(store, r, result, snapshot, attempt_id):
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
        if (result['paused'] or not capacity.enabled or not manager['enabled']
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
                and (not policy['account_external_load'] or snapshot['cpu_utilization_percent']
                     < policy['max_cpu_utilization_percent']))

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
                    store.set_result_collection(attempt.attempt_id, 'uploading', manifest)
                    reply = self._result_request(attempt.attempt_id, 'manifest', manifest)
                    if reply['status'] != 'collected':
                        for index, item in enumerate(manifest['files']):
                            if self.collection_stop.is_set():
                                return
                            with open_regular(Path(attempt.directory), item['path']) as incoming:
                                self._result_request(attempt.attempt_id, str(index), incoming, item['size'])
                    # Always replay commit: publication/DB acknowledgement may
                    # have survived an earlier lost response.
                    if self.collection_stop.is_set():
                        return
                    self._result_request(attempt.attempt_id, 'commit', {})
                    store.set_result_collection(attempt.attempt_id, 'collected')
                except (OSError, ValueError, IncompleteRead, URLError) as exc:
                    store.set_result_collection(attempt.attempt_id, 'failed',
                                                reason=f'{type(exc).__name__}: {exc}')

    def _result_request(self, attempt_id: str, operation: str, payload: object,
                        size: int | None = None) -> dict:
        data = json.dumps(payload).encode() if size is None else payload
        request = Request(self.config.manager_endpoint.rstrip('/') + f'/v2/results/{attempt_id}/{operation}',
                          data=data, method='POST' if size is None else 'PUT',
                          headers={'X-Worker-ID': self.config.worker.id,
                                   'Authorization': 'Bearer ' + self.token,
                                   'Content-Length': str(len(data) if size is None else size),
                                   'Content-Type': 'application/json' if size is None else 'application/octet-stream'})
        try:
            with self.result_opener.open(request, timeout=self.config.result_request_timeout_seconds) as response:
                return json.load(response)
        except HTTPError as exc:
            try:
                detail = json.loads(exc.read(4096)).get('error', 'Artifact request rejected')
            except (ValueError, AttributeError):
                detail = 'Artifact request rejected'
            finally:
                exc.close()
            raise ValueError(f'Result transfer HTTP {exc.code}: {detail}') from None

    def run(self) -> None:
        """Poll continuously; transport errors retain reservations and local work."""
        from urllib.error import URLError
        while True:
            try:
                self.tick()
            except (URLError, TimeoutError, ConnectionError, IncompleteRead) as exc:
                print(f'Worker poll failed: {type(exc).__name__}', file=sys.stderr)
            time.sleep(self.config.heartbeat_interval_seconds)
