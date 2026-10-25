"""Authenticated polling API. SQLite remains local to the manager host."""

from collections import OrderedDict
from dataclasses import asdict
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
import ipaddress
import json
import logging
import sqlite3
import os
from pathlib import Path
import socket
import ssl
import stat
import threading
import time
from typing import BinaryIO
from uuid import uuid4

from .config import ManagerConfig, WorkerCapacity
from .models import JobSpecification
from .scheduler import WorkerSnapshot, schedule
from .store import Store, TERMINAL, is_busy


# A checkpoint contract can contain 100000 positive signed-bigint boundaries:
# at most 2.1 MB for their JSON array, plus bounded provenance and one state file.
# Keep the existing wire format and a finite allowance for all supported plans.
MAX_CHECKPOINT_MANIFEST_BYTES = 4 * 1024 * 1024
# Compression records repeat archive and original identities. Allow bounded
# result metadata above the old 1 MiB limit for the default 10000 output files;
# artifact count/size, ownership and termination checks still apply separately.
MAX_RESULT_MANIFEST_BYTES = 16 * 1024 * 1024


class StorageProtocolError(ValueError):
    """A peer cannot safely participate in the configured storage protocol."""


def read_token(path: str | Path) -> str:
    """Read a private regular credential file; never include its contents in errors."""
    path = Path(path)
    if not path.is_absolute():
        raise ValueError('Credential file paths must be absolute')
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise ValueError('Credential files must be regular and private (mode 0600 or 0400)')
    token = path.read_text().strip()
    if len(token) < 32 or any(c.isspace() for c in token) or not token.isascii():
        raise ValueError('Credential must contain at least 32 non-whitespace ASCII characters')
    return token


def is_loopback(host: str) -> bool:
    """Accept only literal loopback addresses for plaintext transport."""
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class ManagerService:
    """Serve control and artifacts independently of periodic maintenance."""

    def __init__(self, config: ManagerConfig, token_files: dict[str, str]) -> None:
        self.config = config
        registered = {w.id for w in config.workers}
        if (not isinstance(token_files, dict)
                or any(not isinstance(v, str) for v in token_files.values())
                or set(token_files) != registered):
            raise ValueError('Credential mapping must match registered worker IDs')
        self.tokens = {key: read_token(value) for key, value in token_files.items()}
        if len(set(self.tokens.values())) != len(self.tokens):
            raise ValueError('Workers require distinct credentials')
        root = Path(config.storage.work_root).resolve(strict=True)
        self.challenge = root / ('.shared-check-' + uuid4().hex)
        self.proof = uuid4().hex
        if config.storage.mode == 'shared':
            self.challenge.write_text(self.proof)
        self.last_schedule = 0.0
        self._result_lock = threading.Lock()
        self._result_cache: OrderedDict[tuple, tuple] = OrderedDict()
        self._maintenance_stop = threading.Event()
        self._maintenance_wake = threading.Event()
        self._maintenance_done = threading.Event()
        self._maintenance_thread: threading.Thread | None = None
        try:
            with Store(config.manager.database_path, timeout=.2) as store:
                store.expire_workers(time.time(), config.manager.heartbeat_timeout_seconds)
        except sqlite3.Error as exc:
            if not is_busy(exc):
                raise
            logging.getLogger(__name__).warning('Startup maintenance deferred: SQLite busy')
        self._input_cache_lock = threading.Lock()
        self._input_cache: OrderedDict[tuple[str, str, int, int, int], JobSpecification] = OrderedDict()

    def expire_workers(self) -> None:
        """Run at startup and periodically even while all workers are disconnected."""
        with Store(self.config.manager.database_path, timeout=.2) as store:
            now = time.time()
            store.expire_workers(now, self.config.manager.heartbeat_timeout_seconds)
            from .segments import refresh
            refresh(store, self.config)
            from .monitoring import refresh_alerts
            now = time.time()
            refresh_alerts(store, self.config, now)
            if now - self.last_schedule >= self.config.manager.schedule_interval_seconds:
                snapshots = []
                for worker_id, record in store.worker_records().items():
                    if worker_id in self.tokens and record['snapshot']:
                        saved = record['snapshot']
                        snapshots.append(WorkerSnapshot(**{**saved,
                            'capacity': WorkerCapacity(**saved['capacity']),
                            'paused': record['paused']}))
                if snapshots:
                    # Polls remain live during maintenance and snapshot reads.
                    # Compare their timestamps against time after those reads.
                    now = time.time()
                    result = schedule(store, self.config, tuple(snapshots), now=now, max_assignments=4)
                    if len(result.assignments) < 4:
                        self.last_schedule = now

    def start_maintenance(self) -> None:
        """Start exactly one maintenance owner; request threads never run it."""
        if self._maintenance_thread is not None:
            return
        def maintain() -> None:
            while not self._maintenance_stop.is_set():
                self._maintenance_wake.clear()
                self._maintenance_done.clear()
                try:
                    self.expire_workers()
                except sqlite3.Error as exc:
                    if is_busy(exc):
                        logging.getLogger(__name__).warning('Maintenance deferred: SQLite busy')
                    else:
                        logging.getLogger(__name__).exception('Maintenance database failure')
                except Exception:
                    logging.getLogger(__name__).exception('Maintenance failure')
                self._maintenance_done.set()
                self._maintenance_wake.wait(max(.05, min(1, self.config.manager.schedule_interval_seconds)))
        self._maintenance_thread = threading.Thread(target=maintain, name='manager-maintenance')
        self._maintenance_thread.start()

    def close(self) -> None:
        self._maintenance_stop.set()
        self._maintenance_wake.set()
        if self._maintenance_thread is not None:
            self._maintenance_thread.join()
        self.challenge.unlink(missing_ok=True)

    def authenticate(self, worker_id: str, authorization: str) -> bool:
        expected = self.tokens.get(worker_id)
        return expected is not None and hmac.compare_digest(
            authorization.encode(), ('Bearer ' + expected).encode())

    def poll(self, worker_id: str, payload: dict) -> dict:
        """Accept metrics/results only for the authenticated worker, then dispatch."""
        began = time.monotonic()
        try:
            return self._control_poll(worker_id, payload)
        finally:
            logging.getLogger(__name__).debug('Control poll worker=%s elapsed=%.6f', worker_id,
                                              time.monotonic() - began)

    def _control_poll(self, worker_id: str, payload: dict) -> dict:
        result = self._poll(worker_id, payload)
        # A bounded normal-poll wait reduces an extra network roundtrip after
        # reporting capacity. Heartbeat-only requests never wait for maintenance.
        if 'policy' in result and not payload.get('heartbeat_only') and self._maintenance_thread is not None:
            self._maintenance_done.clear()
            self._maintenance_wake.set()
            self._maintenance_done.wait(.2)
            with Store(self.config.manager.database_path, timeout=.2) as store:
                result = self._reply(store, worker_id, payload)
        return result

    def _poll(self, worker_id: str, payload: dict) -> dict:
        """Commit reports atomically and return durable dispatch/control state."""
        if worker_id not in self.tokens:
            raise ValueError('Unregistered worker')
        heartbeat_only = payload.get('heartbeat_only', False)
        if type(heartbeat_only) is not bool:
            raise ValueError('heartbeat_only must be a boolean')
        now = time.time()
        if self.config.storage.mode == 'staged':
            if payload.get('protocol') != 2 or payload.get('storage_mode') != 'staged':
                raise StorageProtocolError('Staged inputs require protocol 2 and staged worker mode')
        elif payload.get('storage_mode', 'shared') != 'shared':
            raise StorageProtocolError('Manager and worker storage modes differ')
        # Require the manager-created shared file before shared scheduling.
        if self.config.storage.mode == 'shared' and payload.get('proof') != self.proof:
            return {'challenge': str(self.challenge), 'assignments': []}
        data = payload['snapshot']
        capacity = WorkerCapacity(**{**data['capacity'],
                                     'labels': tuple(data['capacity'].get('labels', ()))})
        if capacity.id != worker_id:
            raise ValueError('Snapshot identity mismatch')
        snapshot = WorkerSnapshot(**{**data, 'capacity': capacity, 'heartbeat_at': now,
                                     'paused': False, 'online': True})
        with Store(self.config.manager.database_path, timeout=.2) as store:
            # Validate the whole report before applying any state changes.
            reports = [] if heartbeat_only else payload.get('reports', [])
            for report in reports:
                attempt = store.attempt(report['attempt_id'])
                if attempt.worker_id != worker_id:
                    raise ValueError('Attempt ownership mismatch')
            with store.scheduling_round():
                if payload.get('recovery_protocol') == 1:
                    generation = payload['generation']
                    store.worker_generation(worker_id, generation['boot_id'], generation['session_id'])
                    from .segments import observe_environment
                    observe_environment(store, worker_id, generation['boot_id'],
                                        payload.get('continuation_fingerprint'), now)
                for report in reports:
                    if report.get('checkpoint_error'):
                        from .checkpoints import set_error, record as checkpoint_record
                        checkpoint = checkpoint_record(store, report['attempt_id'])
                        if checkpoint and not checkpoint['acknowledged']:
                            set_error(store, report['attempt_id'], report['checkpoint_error'])
                    if 'rejection' in report:
                        rejected = report['rejection']
                        if store.reject_launch(report['attempt_id'], rejected['code'],
                                               rejected['reason'], rejected['retry_at']):
                            store.return_rejected(report['attempt_id'])
                        continue
                    # A delayed report for a fenced attempt cannot affect its successor.
                    if store.rejection(report['attempt_id']):
                        continue
                    if 'input_transfer' in report:
                        transfer = report['input_transfer']
                        store.set_input_transfer(report['attempt_id'], transfer['directory'],
                                                 transfer['status'], transfer.get('reason'))
                        from .monitoring import transfer as progress
                        input_spec = store.specification(store.attempt(report['attempt_id']).job_id)
                        progress(store, report['attempt_id'], 'input',
                                 {'ready': 'ready', 'failed': 'failed'}.get(transfer['status'], 'transferring'),
                                 error='Input transfer failed; inspect attempt diagnostics' if transfer['status'] == 'failed' else None,
                                 total_bytes=sum(f.size for f in input_spec.input_manifest),
                                 total_files=len(input_spec.input_manifest))
                    old = store.attempt(report['attempt_id'])
                    state = report['state']
                    if state not in {'assigned', 'running', 'cancelling', 'unknown', *TERMINAL}:
                        raise ValueError('Unsupported worker state')
                    if store.cancellation(old.attempt_id) and state not in TERMINAL:
                        continue  # A stale report cannot undo durable cancellation intent.
                    if state == 'assigned':
                        if old.state == 'unknown':
                            store.transition(old.attempt_id, 'assigned')
                        continue
                    if old.state == state and state not in TERMINAL:
                        continue
                    # A short execution may finish between consecutive polls.
                    if old.state == 'assigned' and state in TERMINAL and report.get('started'):
                        old = store.transition(old.attempt_id, 'running')
                    # Worker timeouts may finish without a cancelling poll.
                    if old.state == 'running' and state == 'cancelled':
                        store.transition(old.attempt_id, 'cancelling', reason=report.get('reason'))
                    store.transition(old.attempt_id, state, exit_code=report.get('exit_code'),
                                     reason=report.get('reason'),
                                     termination_evidence=report.get('termination_evidence'))
                    if 'interruption' in report:
                        store.record_interruption(old.attempt_id, report['interruption']['evidence'])
                        store.recover_interrupted(old.attempt_id, now)
                    if 'result_collection' in report and state in TERMINAL:
                        collection = report['result_collection']
                        status = collection['status']
                        if status != 'collected':
                            store.set_result_collection(old.attempt_id, status, reason=collection.get('reason'))
                            from .monitoring import transfer as progress
                            current_collection = store.result_collection(old.attempt_id, include_manifest=False)
                            if current_collection['status'] != 'collected':
                                phase = {'uploading': 'transferring', 'failed': 'failed'}.get(status, 'pending')
                                previous = store._db.execute(
                                    "SELECT phase FROM transfer_progress WHERE attempt_id=? AND direction='result'",
                                    (old.attempt_id,)).fetchone()
                                # Polls can lag behind artifact requests. Preserve progress
                                # already observed by the manager, including verification.
                                if previous and (phase == 'pending' or (
                                        phase == 'transferring' and previous['phase'] == 'verifying')):
                                    phase = previous['phase']
                                progress(store, old.attempt_id, 'result',
                                         phase,
                                         error='Worker reports result collection failure' if status == 'failed' else None)
                saved_snapshot = asdict(snapshot)
                for optional in ('ready', 'draining', 'continuation_ready'):
                    if optional not in data:
                        saved_snapshot.pop(optional, None)
                store.set_worker(worker_id, snapshot=saved_snapshot)
            self._maintenance_wake.set()
            return self._reply(store, worker_id, payload)

    def _reply(self, store: Store, worker_id: str, payload: dict) -> dict:
        assignments = []
        for reservation in (() if payload.get('heartbeat_only', False) else store.reservations(worker_id)):
            attempt = store.attempt(reservation.attempt_id)
            if store._db.execute('SELECT 1 FROM mpi_groups WHERE attempt_id=?',(attempt.attempt_id,)).fetchone():
                continue  # Native MPI groups use the explicit coordinator, not ordinary worker launches.
            if attempt.state in {'assigned', 'unknown', 'cancelling'}:
                specification = asdict(store.specification(attempt.job_id))
                # Submission provenance stays on the manager; workers need only rendered inputs.
                specification.pop('sweep', None)
                if specification['sampling'] is None:
                    specification.pop('sampling')
                if not specification['mpi_placement']:
                    specification.pop('mpi_placement')
                if payload.get('recovery_protocol') != 1:
                    specification.pop('infrastructure_recovery', None)
                if payload.get('protocol') != 2 and specification['storage_mode'] == 'shared':
                    # Legacy shared workers accept only the original specification keys.
                    specification.pop('storage_mode')
                    specification.pop('input_manifest')
                from .checkpoints import record as checkpoint_record
                checkpoint = checkpoint_record(store, attempt.attempt_id)
                assignments.append({'attempt': asdict(attempt), 'specification': specification,
                                    **({'checkpoint_contract': checkpoint['contract']} if checkpoint else {})})
        policy = self.config.scheduler
        return {'challenge': str(self.challenge), 'assignments': assignments,
                'protocol': 2, 'storage_mode': self.config.storage.mode,
                'staged_launch_enabled': True, 'results_protocol': 1,
                'cancellations': [{'attempt_id': r.attempt_id, 'reason': store.cancellation(r.attempt_id)}
                                  for r in store.reservations(worker_id) if store.cancellation(r.attempt_id)],
                'paused': store.worker_records()[worker_id]['paused'],
                'prelaunch_protocol': 1, 'recovery_protocol': 1,
                'policy': asdict(policy),
                'manager_capacity': asdict(next(w for w in self.config.workers if w.id == worker_id))}


    def input_verified(self, worker_id: str, attempt_id: str, index: int) -> dict:
        """Accept only bounded input verification identities owned by this worker."""
        from .monitoring import file_progress, transfer
        with Store(self.config.manager.database_path, timeout=0) as store:
            attempt = store.attempt(attempt_id)
            spec = store.specification(attempt.job_id)
            if attempt.worker_id != worker_id or spec.storage_mode != 'staged':
                raise ValueError('Input progress ownership mismatch')
            if type(index) is not int or not 0 <= index < len(spec.input_manifest):
                raise ValueError('Invalid input progress index')
            transfer(store, attempt_id, 'input', 'transferring',
                     total_bytes=sum(f.size for f in spec.input_manifest), total_files=len(spec.input_manifest))
            existing = {row[0] for row in store._db.execute(
                "SELECT file_index FROM transfer_files WHERE attempt_id=? AND direction='input' AND verified_bytes IS NOT NULL",
                (attempt_id,))}
            for completed in range(index + 1):
                if completed not in existing:
                    file_progress(store, attempt_id, 'input', completed,
                                  verified_bytes=spec.input_manifest[completed].size)
        return {'accepted': True}

    def health(self, worker_id: str) -> dict:
        """Worker credentials expose only that worker's health, without job inputs."""
        from .monitoring import worker_view
        if worker_id not in self.tokens:
            raise ValueError('Unregistered worker')
        with Store(self.config.manager.database_path, timeout=.2) as store:
            return next(row for row in worker_view(store, self.config, time.time()) if row['worker_id'] == worker_id)

    def input_artifact(self, worker_id: str, attempt_id: str, index: int) -> tuple[BinaryIO, int]:
        """Authorize an assigned attempt before opening one bounded input file."""
        from .staging import open_regular, validate_manifest
        with Store(self.config.manager.database_path, timeout=.2) as store:
            attempt = store.attempt(attempt_id)
            participant = store._db.execute('SELECT 1 FROM mpi_members WHERE attempt_id=? AND worker_id=?',
                                            (attempt_id,worker_id)).fetchone()
            if (attempt.worker_id != worker_id and not participant) or attempt.state != 'assigned':
                raise ValueError('Input artifact ownership or state mismatch')
            limits = self.config.storage
            key = (self.config.manager.database_path, attempt.job_id, limits.max_input_files,
                   limits.max_input_file_bytes, limits.max_input_total_bytes)
            # Specifications are immutable. Cache decoding as well as validation;
            # neither may scan the entire manifest for every individual artifact.
            # Serialize fills so concurrent requests do this work only once.
            with self._input_cache_lock:
                spec = self._input_cache.get(key)
                if spec is None:
                    spec = store.specification(attempt.job_id)
                    if spec.storage_mode != 'staged':
                        raise ValueError('Input artifacts require a staged specification')
                    validate_manifest(spec.input_manifest, max_files=limits.max_input_files,
                                      max_file_bytes=limits.max_input_file_bytes,
                                      max_total_bytes=limits.max_input_total_bytes)
                    self._input_cache[key] = spec
                    if len(self._input_cache) > 32:
                        self._input_cache.popitem(last=False)
                self._input_cache.move_to_end(key)
        if not 0 <= index < len(spec.input_manifest):
            raise ValueError('Unknown input index')
        from .monitoring import transfer
        try:
            self._input_telemetry(attempt_id, spec)
        except sqlite3.Error as exc:
            if not is_busy(exc):
                raise
        item = spec.input_manifest[index]
        stream = open_regular(Path(spec.work_directory) / 'inputs', item.path)
        if os.fstat(stream.fileno()).st_size != item.size:
            stream.close()
            raise ValueError('Stored input size differs from snapshot')
        return stream, item.size

    def _input_telemetry(self, attempt_id: str, spec: JobSpecification) -> None:
        from .monitoring import transfer
        with Store(self.config.manager.database_path, timeout=.2) as telemetry:
            telemetry._db.execute('PRAGMA busy_timeout = 0')
            previous = telemetry._db.execute("SELECT total_bytes FROM transfer_progress WHERE attempt_id=? AND direction='input'",
                                              (attempt_id,)).fetchone()
            transfer(telemetry, attempt_id, 'input', 'transferring',
                     total_bytes=sum(f.size for f in spec.input_manifest) if not previous or previous[0] is None else None,
                     total_files=len(spec.input_manifest))
    def mpi_result_operation(self, worker_id: str, attempt_id: str, operation: str,
                             payload: dict | None=None, stream: BinaryIO | None=None,
                             length: int | None=None) -> dict:
        """Bind authenticated participant output to one terminated native MPI group."""
        from .multihost import result_operation
        with self._result_lock,Store(self.config.manager.database_path, timeout=.2) as store:
            return result_operation(store,self.config.storage,worker_id,attempt_id,operation,payload,stream,length)

    def result_operation(self, worker_id: str, attempt_id: str, operation: str,
                         payload: dict | None = None, stream: BinaryIO | None = None,
                         length: int | None = None) -> dict:
        """Serialize manifest binding, bounded file writes and durable acknowledgement."""
        from .results import authorize, commit_results, output_limits, upload_file, validate_results
        with self._result_lock, Store(self.config.manager.database_path, timeout=.2) as store:
            directory = authorize(store, worker_id, attempt_id)
            limits = output_limits(self.config.storage)
            from .monitoring import transfer, file_progress, ProgressReader
            if operation == 'manifest':
                validate_results(payload, limits)
                previous = store.result_collection(attempt_id)
                # Preserve the last reported failure during idempotent retries,
                # so repeated manifest delivery cannot hide a persistent error.
                failed = previous and previous['status'] == 'failed'
                store.set_result_collection(attempt_id, 'failed' if failed else 'uploading', payload,
                                            previous['reason'] if failed else None)
            collection = store.result_collection(attempt_id, include_manifest=False)
            if collection is None or not collection['has_manifest']:
                raise ValueError('Result manifest required')
            key = (self.config.manager.database_path, attempt_id, *limits.values())
            files = self._result_cache.get(key)
            if files is None:
                files = validate_results(store.result_collection(attempt_id)['manifest'], limits)
                self._result_cache[key] = files
                if len(self._result_cache) > 32:
                    self._result_cache.popitem(last=False)
            self._result_cache.move_to_end(key)
            transfer(store, attempt_id, 'result',
                     'acknowledged' if collection['status'] == 'collected' else 'transferring',
                     total_bytes=sum(f.size for f in files), total_files=len(files))
            if operation == 'commit':
                if collection['status'] != 'collected':
                    transfer(store, attempt_id, 'result', 'verifying')
                try:
                    commit_results(directory, files, limits)
                except (OSError, ValueError):
                    transfer(store, attempt_id, 'result', 'failed', error='Result publication or verification failed')
                    raise
                store.set_result_collection(attempt_id, 'collected')
                transfer(store, attempt_id, 'result', 'acknowledged')
            elif operation != 'manifest':
                index = int(operation)
                if not 0 <= index < len(files) or length != files[index].size:
                    raise ValueError('Invalid result file index or length')
                if collection['status'] == 'collected':
                    raise ValueError('Result already published; replay commit instead')
                incoming = ProgressReader(stream, store, attempt_id, 'result', index)
                try:
                    upload_file(directory, files[index], incoming)
                    file_progress(store, attempt_id, 'result', index, verified_bytes=files[index].size)
                except (OSError, ValueError):
                    transfer(store, attempt_id, 'result', 'failed', error='Result file transfer or verification failed')
                    raise
                finally:
                    incoming.flush()
            saved = store.result_collection(attempt_id, include_manifest=False)
            return {'status': saved['status'], 'reason': saved['reason']}

    def checkpoint_operation(self, worker_id: str, attempt_id: str, action: str,
                             payload: dict | None = None, stream: BinaryIO | None = None,
                             length: int | None = None) -> dict:
        """Use the artifact lock and authenticated assignment for checkpoint writes."""
        from .checkpoints import operation
        from .results import output_limits
        with self._result_lock, Store(self.config.manager.database_path, timeout=.2) as store:
            shared_source = None
            if self.config.storage.mode == 'shared' and action == 'commit':
                shared_source = Path(store.attempt(attempt_id).directory) / 'work'
            from .checkpoints import set_error, record as checkpoint_record
            try:
                reply = operation(store, worker_id, attempt_id, action, output_limits(self.config.storage),
                                  payload=payload, stream=stream, length=length, shared_source=shared_source)
            except (OSError, ValueError) as exc:
                if checkpoint_record(store, attempt_id) and store.attempt(attempt_id).worker_id == worker_id:
                    set_error(store, attempt_id, f'{type(exc).__name__}: {exc}'[:2048])
                raise
            if reply['acknowledged']:
                set_error(store, attempt_id, None)
            return reply

    def checkpoint_artifact(self, worker_id: str, attempt_id: str, index: int,
                            build_fingerprint: str) -> tuple[BinaryIO, int]:
        """Authorize a committed dependency and verify it before sending bytes."""
        from .checkpoints import fetch
        with Store(self.config.manager.database_path, timeout=.2) as store:
            stream, item = fetch(store, worker_id, attempt_id, index, build_fingerprint)
            return stream, item.size


def make_server(service: ManagerService, host: str, port: int, *,
                certfile: str | None = None, keyfile: str | None = None) -> HTTPServer:
    """Build an HTTP(S) server; plaintext is restricted to literal loopback."""
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError('Listener port must be between 0 and 65535')
    if bool(certfile) != bool(keyfile):
        raise ValueError('TLS requires both certificate and key')
    if not certfile and not is_loopback(host):
        raise ValueError('Non-loopback listeners require TLS')

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass  # Do not log credentials or request bodies.

        def do_GET(self) -> None:
            self.connection.settimeout(10)
            stream = None
            try:
                worker_id = self.headers.get('X-Worker-ID', '')
                if not service.authenticate(worker_id, self.headers.get('Authorization', '')):
                    self.send_error(401, 'Unauthorized worker')
                    return
                if self.path == '/v1/health':
                    body = json.dumps(service.health(worker_id)).encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                parts = self.path.split('/')
                checkpoint = len(parts) == 6 and parts[:3] == ['', 'v2', 'checkpoints'] and parts[4] == 'files'
                if checkpoint:
                    stream, size = service.checkpoint_artifact(worker_id, parts[3], int(parts[5]),
                        self.headers.get('X-Build-Fingerprint', ''))
                elif len(parts) != 6 or parts[:3] != ['', 'v2', 'inputs'] or parts[4] != 'files':
                    raise ValueError('Unknown input endpoint')
                else:
                    stream, size = service.input_artifact(worker_id, parts[3], int(parts[5]))
            except sqlite3.Error as exc:
                if not is_busy(exc):
                    logging.getLogger(__name__).exception('Artifact database failure')
                self.send_response(503 if is_busy(exc) else 500)
                if is_busy(exc):
                    self.send_header('Retry-After', '1')
                self.send_header('Content-Length', '0')
                self.end_headers()
                return
            except (ValueError, KeyError, TypeError, OSError):
                self.send_error(400, 'Input artifact unavailable or unauthorized')
                return
            progress_store = None
            reader = None
            try:
                if not checkpoint:
                    from .monitoring import ProgressReader
                    try:
                        progress_store = Store(service.config.manager.database_path, timeout=0)
                        reader = ProgressReader(stream, progress_store, parts[3], 'input', int(parts[5]))
                    except sqlite3.Error as exc:
                        if not is_busy(exc):
                            raise
                        # Optional telemetry cannot prevent an authorized download.
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Length', str(size))
                self.end_headers()
                remaining = size
                while remaining:
                    chunk = stream.read(min(65536, remaining))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    if reader:
                        reader.record(len(chunk))
                    remaining -= len(chunk)
            except OSError:
                pass  # The receiver verifies length/hash and retries without launching.
            finally:
                try:
                    if reader:
                        reader.flush()
                finally:
                    if progress_store is not None:
                        progress_store.close()
                    stream.close()

        def do_PUT(self) -> None:
            self.do_POST()

        def do_POST(self) -> None:
            self.connection.settimeout(10)
            status = 200
            try:
                worker_id = self.headers.get('X-Worker-ID', '')
                if not service.authenticate(worker_id, self.headers.get('Authorization', '')):
                    status, result = 401, {'error': 'Unauthorized worker'}
                elif self.path.startswith('/v2/input-verified/'):
                    length = int(self.headers.get('Content-Length', '-1'))
                    if not 0 < length <= 128 or self.headers.get('Transfer-Encoding'):
                        raise ValueError('Invalid progress request size')
                    data = json.loads(self.rfile.read(length))
                    if set(data) != {'index'}:
                        raise ValueError('Invalid progress fields')
                    result = service.input_verified(worker_id, self.path.split('/')[-1], data['index'])
                elif self.path.startswith(('/v2/results/', '/v2/checkpoints/', '/v2/mpi-results/')):
                    parts = self.path.split('/')
                    if len(parts) != 5:
                        raise ValueError('Invalid result endpoint')
                    length = int(self.headers.get('Content-Length', '-1'))
                    operation = parts[4]
                    handler = (service.checkpoint_operation if parts[2]=='checkpoints' else
                               service.mpi_result_operation if parts[2]=='mpi-results' else service.result_operation)
                    if self.headers.get('Transfer-Encoding'):
                        raise ValueError('Chunked requests are not supported')
                    if operation in {'manifest', 'commit'}:
                        limit = 1048576
                        if operation == 'manifest':
                            limit = (MAX_CHECKPOINT_MANIFEST_BYTES if parts[2] == 'checkpoints'
                                     else MAX_RESULT_MANIFEST_BYTES)
                        if not 0 < length <= limit:
                            raise ValueError('Invalid result request size')
                        payload = json.loads(self.rfile.read(length))
                        result = handler(worker_id, parts[3], operation, payload)
                    else:
                        # Limit reads to Content-Length, including the verifier's
                        # final EOF probe; never read into the next request.
                        class Body:
                            remaining = length

                            def read(body, size: int) -> bytes:
                                data = self.rfile.read(min(size, max(0, body.remaining)))
                                body.remaining -= len(data)
                                return data

                        result = handler(worker_id, parts[3], operation,
                                                          stream=Body(), length=length)
                elif self.path != '/v1/poll':
                    status, result = 404, {'error': 'Unknown endpoint'}
                else:
                    length = int(self.headers.get('Content-Length', '0'))
                    if not 0 < length <= 1048576:
                        raise ValueError('Request size must be between 1 and 1048576 bytes')
                    payload = json.loads(self.rfile.read(length))
                    if not isinstance(payload, dict):
                        raise ValueError('Expected JSON object')
                    result = service.poll(worker_id, payload)
            except sqlite3.Error as exc:
                status, result = (503, {'error': 'Database busy; retry request'}) if is_busy(exc) else (500, {'error': 'Database operation failed'})
                if not is_busy(exc):
                    logging.getLogger(__name__).exception('Request database failure')
            except StorageProtocolError as exc:
                status, result = 409, {'error': str(exc)}
            except OSError as exc:
                status, result = 400, {'error': f'Artifact or journal storage unavailable (errno={exc.errno})'}
            except (ValueError, KeyError, TypeError):
                status, result = 400, {'error': 'Invalid poll or artifact request, or incompatible storage protocol'}
            body = json.dumps(result, allow_nan=False).encode()
            self.send_response(status)
            if status == 503:
                self.send_header('Retry-After', '1')
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except OSError:
                pass  # Lost response is retried against durable operation identities.

    context = None
    if certfile:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile, keyfile)

    class BoundedServer(ThreadingHTTPServer):
        # Join in-flight handlers on close before service storage is cleaned up.
        daemon_threads = False

        def service_actions(self) -> None:
            pass  # Maintenance runs outside the accept loop.

        def get_request(self) -> tuple:
            connection, address = super().get_request()
            connection.settimeout(10)
            return connection, address

        def process_request_thread(self, connection: socket.socket, address: tuple) -> None:
            # A slow TLS peer must not occupy the accept/scheduling thread.
            # Handshakes retain the accepted socket's bounded timeout and are
            # joined with other handlers before service storage is closed.
            if context:
                try:
                    connection = context.wrap_socket(connection, server_side=True)
                except OSError:
                    connection.close()
                    return
                except BaseException:
                    connection.close()
                    raise
            super().process_request_thread(connection, address)

    server = BoundedServer((host, port), Handler)
    service.start_maintenance()
    return server
