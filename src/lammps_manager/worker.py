"""Sequential persistent Linux worker with polling independent of execution."""

from typing import BinaryIO

from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import asdict
import fcntl
from http.client import IncompleteRead
import json
import os
from pathlib import Path
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .config import WorkerConfig
from .local import execute_attempt
from .manager import is_loopback, read_token
from .models import specification_from_dict
from .store import Store


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
        self.pool = ThreadPoolExecutor(max_workers=1)
        self.future: Future | None = None
        self.monitor = LinuxMonitor()
        self.proof = None
        self.acknowledged: dict[str, dict] = {}
        # Do not send bearer credentials through environment-configured proxies.
        from urllib.request import ProxyHandler
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def close(self) -> None:
        """Wait for the current process; closing never kills a simulation."""
        self.pool.shutdown(wait=True)
        self.lock.close()

    def _execute(self, attempt_id: str) -> None:
        with Store(self.database) as store:
            execute_attempt(store, self.config, store.attempt(attempt_id))

    def tick(self) -> dict:
        """Send one heartbeat, report durable outcomes, and accept at most one job."""
        if self.future is not None and self.future.done():
            self.future.result()  # Surface unexpected executor failures.
            self.future = None
        if self.future is None:
            from .recovery import reconcile
            with Store(self.database) as store:
                for reservation in store.reservations():
                    reconcile(store, store.attempt(reservation.attempt_id))
        snapshot = self.monitor.sample(self.config)
        reports = []
        with Store(self.database) as store:
            for attempt in (a for job in store.jobs() for a in store.job_attempts(job['job_id'])):
                report = {'attempt_id': attempt.attempt_id, 'state': attempt.state,
                          'started': attempt.started_at is not None, 'exit_code': attempt.exit_code,
                          'termination_evidence': attempt.termination_evidence, 'reason': attempt.reason}
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
                if self.config.storage_mode == 'staged':
                    store.transition(command['attempt_id'], 'cancelled', reason=command['reason'],
                                     termination_evidence='Staged execution is disabled; process never started')
        if self.future is not None:
            return result
        with Store(self.database) as store:
            # An accepted but unresolved old attempt blocks all fresh execution.
            if store.reservations():
                if self.config.storage_mode == 'staged':
                    for reserved in store.reservations():
                        self._stage(store, reserved.attempt_id)
                return result
            for assignment in result['assignments']:
                spec_data = assignment['specification']
                spec = specification_from_dict(spec_data)
                if spec.storage_mode != self.config.storage_mode:
                    raise ValueError('Assignment storage mode differs from worker')
                attempt = assignment['attempt']
                if attempt['worker_id'] != self.config.worker.id:
                    raise ValueError('Assignment identity mismatch')
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
                        store.assign(spec.job_id, attempt['attempt_id'], self.config.worker.id, str(directory))
                    if cancelled:
                        store.request_cancel(attempt['attempt_id'], cancelled['reason'])
                        store.transition(attempt['attempt_id'], 'cancelled', reason=cancelled['reason'],
                            termination_evidence='Staged execution is disabled; process never started')
                    else:
                        self._stage(store, attempt['attempt_id'])
                    break
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
                if any(a.attempt_id == attempt['attempt_id']
                       for j in store.jobs() for a in store.job_attempts(j['job_id'])):
                    continue
                if result['paused'] and not cancelled:
                    continue
                policy = result['policy']
                manager = result['manager_capacity']
                r = spec.resources
                if not cancelled and (not self.config.worker.enabled or not manager['enabled']
                        or r.cpu_slots > min(self.config.worker.cpu_slots, manager['cpu_slots'],
                                             snapshot['detected_cpu_slots'])
                        or r.memory_gib > min(self.config.worker.memory_budget_gib,
                                             manager['memory_budget_gib'], snapshot['detected_memory_gib'])
                        or r.memory_gib + policy['reserve_memory_gib'] > snapshot['available_memory_gib']
                        or (policy['account_external_load'] and snapshot['cpu_utilization_percent']
                            >= policy['max_cpu_utilization_percent'])):
                    continue  # Keep reservation pending until a later local recheck passes.
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
                    self.future = self.pool.submit(self._execute, accepted.attempt_id)
                break
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
                                 'Inputs verified; staged execution requires T008 result collection')

    def run(self) -> None:
        """Poll continuously; transport errors retain reservations and local work."""
        from urllib.error import URLError
        while True:
            try:
                self.tick()
            except (URLError, TimeoutError, ConnectionError, IncompleteRead) as exc:
                print(f'Worker poll failed: {type(exc).__name__}', file=sys.stderr)
            time.sleep(self.config.heartbeat_interval_seconds)
