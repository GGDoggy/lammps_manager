"""Authenticated polling API. SQLite remains local to the manager host."""

from collections import OrderedDict
from dataclasses import asdict
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
import ipaddress
import json
import os
from pathlib import Path
import ssl
import stat
import threading
import time
from typing import BinaryIO
from uuid import uuid4

from .config import ManagerConfig, WorkerCapacity
from .models import JobSpecification
from .scheduler import WorkerSnapshot, schedule
from .store import Store, TERMINAL


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
    """Serialize poll state while serving artifacts independently."""

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
        self._poll_lock = threading.Lock()
        self._input_cache_lock = threading.Lock()
        self._input_cache: OrderedDict[tuple[str, str, int, int, int], JobSpecification] = OrderedDict()

    def close(self) -> None:
        self.challenge.unlink(missing_ok=True)

    def authenticate(self, worker_id: str, authorization: str) -> bool:
        expected = self.tokens.get(worker_id)
        return expected is not None and hmac.compare_digest(
            authorization.encode(), ('Bearer ' + expected).encode())

    def poll(self, worker_id: str, payload: dict) -> dict:
        """Accept metrics/results only for the authenticated worker, then dispatch."""
        with self._poll_lock:
            return self._poll(worker_id, payload)

    def _poll(self, worker_id: str, payload: dict) -> dict:
        """Keep reports, scheduling cadence and assignments in one serialized poll."""
        if worker_id not in self.tokens:
            raise ValueError('Unregistered worker')
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
        with Store(self.config.manager.database_path) as store:
            # Validate the whole report before applying any state changes.
            reports = payload.get('reports', [])
            for report in reports:
                attempt = store.attempt(report['attempt_id'])
                if attempt.worker_id != worker_id:
                    raise ValueError('Attempt ownership mismatch')
            with store.scheduling_round():
                for report in reports:
                    if 'input_transfer' in report:
                        transfer = report['input_transfer']
                        store.set_input_transfer(report['attempt_id'], transfer['directory'],
                                                 transfer['status'], transfer.get('reason'))
                    old = store.attempt(report['attempt_id'])
                    state = report['state']
                    if state not in {'assigned', 'running', 'unknown', *TERMINAL}:
                        raise ValueError('Unsupported worker state')
                    if state == 'assigned':
                        continue
                    # A short execution may finish between consecutive polls.
                    if old.state == 'assigned' and state in {'succeeded', 'failed'} and report.get('started'):
                        store.transition(old.attempt_id, 'running')
                    store.transition(old.attempt_id, state, exit_code=report.get('exit_code'),
                                     reason=report.get('reason'),
                                     termination_evidence=report.get('termination_evidence'))
                store.set_worker(worker_id, snapshot=asdict(snapshot))
            if now - self.last_schedule >= self.config.manager.schedule_interval_seconds:
                snapshots = []
                for saved_id, record in store.worker_records().items():
                    if saved_id in self.tokens and record['snapshot']:
                        saved = record['snapshot']
                        snapshots.append(WorkerSnapshot(**{**saved,
                            'capacity': WorkerCapacity(**saved['capacity']),
                            'paused': record['paused']}))
                schedule(store, self.config, tuple(snapshots), now=now)
                self.last_schedule = now
            assignments = []
            for reservation in store.reservations(worker_id):
                attempt = store.attempt(reservation.attempt_id)
                if attempt.state == 'assigned':
                    specification = asdict(store.specification(attempt.job_id))
                    if payload.get('protocol') != 2 and specification['storage_mode'] == 'shared':
                        # Legacy shared workers accept only the original specification keys.
                        specification.pop('storage_mode')
                        specification.pop('input_manifest')
                    assignments.append({'attempt': asdict(attempt), 'specification': specification})
            policy = self.config.scheduler
            return {'challenge': str(self.challenge), 'assignments': assignments,
                    'protocol': 2, 'storage_mode': self.config.storage.mode,
                    'staged_launch_enabled': False,
                    'paused': store.worker_records()[worker_id]['paused'],
                    'policy': asdict(policy),
                    'manager_capacity': asdict(next(w for w in self.config.workers if w.id == worker_id))}

    def input_artifact(self, worker_id: str, attempt_id: str, index: int) -> tuple[BinaryIO, int]:
        """Authorize an assigned attempt before opening one bounded input file."""
        from .staging import open_regular, validate_manifest
        with Store(self.config.manager.database_path) as store:
            attempt = store.attempt(attempt_id)
            if attempt.worker_id != worker_id or attempt.state != 'assigned':
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
        item = spec.input_manifest[index]
        stream = open_regular(Path(spec.work_directory) / 'inputs', item.path)
        if os.fstat(stream.fileno()).st_size != item.size:
            stream.close()
            raise ValueError('Stored input size differs from snapshot')
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
                parts = self.path.split('/')
                if len(parts) != 6 or parts[:3] != ['', 'v2', 'inputs'] or parts[4] != 'files':
                    raise ValueError('Unknown input endpoint')
                stream, size = service.input_artifact(worker_id, parts[3], int(parts[5]))
            except (ValueError, KeyError, TypeError, OSError):
                self.send_error(400, 'Input artifact unavailable or unauthorized')
                return
            try:
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
                    remaining -= len(chunk)
            except (OSError, TimeoutError):
                pass  # The receiver verifies length/hash and retries without launching.
            finally:
                stream.close()

        def do_POST(self) -> None:
            self.connection.settimeout(10)
            status = 200
            try:
                worker_id = self.headers.get('X-Worker-ID', '')
                if not service.authenticate(worker_id, self.headers.get('Authorization', '')):
                    status, result = 401, {'error': 'Unauthorized worker'}
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
            except StorageProtocolError as exc:
                status, result = 409, {'error': str(exc)}
            except (ValueError, KeyError, TypeError, OSError):
                status, result = 400, {'error': 'Invalid poll, incompatible storage protocol, or storage unavailable'}
            body = json.dumps(result, allow_nan=False).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    context = None
    if certfile:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile, keyfile)

    class BoundedServer(ThreadingHTTPServer):
        # Join in-flight handlers on close before service storage is cleaned up.
        daemon_threads = False

        def get_request(self) -> tuple:
            connection, address = super().get_request()
            connection.settimeout(10)
            if context:
                try:
                    connection = context.wrap_socket(connection, server_side=True)
                except BaseException:
                    connection.close()
                    raise
            return connection, address

    return BoundedServer((host, port), Handler)
