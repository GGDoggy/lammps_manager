"""Authenticated polling API. SQLite remains local to the manager host."""

from dataclasses import asdict
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import ipaddress
import json
from pathlib import Path
import ssl
import stat
import time
from uuid import uuid4

from .config import ManagerConfig, WorkerCapacity
from .scheduler import WorkerSnapshot, schedule
from .store import Store, TERMINAL


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
    """Serialized HTTP handler state, with persisted metrics and pause flags."""

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
        self.challenge.write_text(self.proof)
        self.last_schedule = 0.0

    def close(self) -> None:
        self.challenge.unlink(missing_ok=True)

    def authenticate(self, worker_id: str, authorization: str) -> bool:
        expected = self.tokens.get(worker_id)
        return expected is not None and hmac.compare_digest(
            authorization.encode(), ('Bearer ' + expected).encode())

    def poll(self, worker_id: str, payload: dict) -> dict:
        """Accept metrics/results only for the authenticated worker, then dispatch."""
        if worker_id not in self.tokens:
            raise ValueError('Unregistered worker')
        now = time.time()
        # Require the manager-created shared file before scheduling any work.
        if payload.get('proof') != self.proof:
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
                    assignments.append({'attempt': asdict(attempt),
                                        'specification': asdict(store.specification(attempt.job_id))})
            policy = self.config.scheduler
            return {'challenge': str(self.challenge), 'assignments': assignments,
                    'paused': store.worker_records()[worker_id]['paused'],
                    'policy': asdict(policy),
                    'manager_capacity': asdict(next(w for w in self.config.workers if w.id == worker_id))}


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
            except (ValueError, KeyError, TypeError, OSError):
                status, result = 400, {'error': 'Invalid poll or shared storage unavailable'}
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

    class BoundedServer(HTTPServer):
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
