"""Local SQLite persistence. This module neither launches nor schedules processes."""

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
import json
import math
import re
import sqlite3
from pathlib import Path
from typing import Iterator

from .models import JobSpecification, Resources, specification_from_dict


class StoreError(ValueError):
    """A conflicting operation, missing record, or invalid transition."""


TERMINAL = frozenset({'succeeded', 'failed', 'cancelled'})
TRANSITIONS = {
    'assigned': {'running', 'cancelling', 'unknown', 'failed', 'cancelled'},
    'running': {'cancelling', 'unknown', 'succeeded', 'failed'},
    'cancelling': {'unknown', 'cancelled', 'succeeded', 'failed'},
    'unknown': {'assigned', 'running', 'cancelling', 'succeeded', 'failed', 'cancelled'},
}


@dataclass(frozen=True)
class Attempt:
    attempt_id: str
    job_id: str
    worker_id: str
    directory: str
    state: str
    created_at: str
    started_at: str | None
    ended_at: str | None
    exit_code: int | None
    termination_evidence: str | None
    reason: str | None


@dataclass(frozen=True)
class Reservation:
    attempt_id: str
    worker_id: str
    cpu_slots: int
    memory_gib: float


SCHEMA = """
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY,
    specification TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN
        ('queued','assigned','running','cancelling','unknown','succeeded','failed','cancelled')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE attempts (
    attempt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    worker_id TEXT NOT NULL,
    directory TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK(state IN
        ('assigned','running','cancelling','unknown','succeeded','failed','cancelled')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    started_at TEXT,
    ended_at TEXT,
    exit_code INTEGER,
    termination_evidence TEXT,
    reason TEXT
);
CREATE TABLE reservations (
    attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id),
    worker_id TEXT NOT NULL,
    cpu_slots INTEGER NOT NULL CHECK(cpu_slots > 0),
    memory_gib REAL NOT NULL CHECK(memory_gib > 0)
);
CREATE TABLE events (
    event_id INTEGER PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    attempt_id TEXT REFERENCES attempts(attempt_id),
    state TEXT NOT NULL,
    reason TEXT,
    exit_code INTEGER,
    termination_evidence TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
PRAGMA user_version = 2;
"""


class Store:
    """One connection per Store; separate instances may share a local database.

    Public writes commit atomically. Replays must carry identical identities and
    result values. A terminal transition requires caller-supplied termination
    evidence; the store cannot verify a remote process. Retries require explicit authorization.
    """

    def __init__(self, path: str | Path) -> None:
        self._db = sqlite3.connect(str(path), isolation_level=None, timeout=5)
        self._db.row_factory = sqlite3.Row
        # Version-1 attempts had a UNIQUE job_id; rebuild without rewriting snapshots.
        self._db.execute('PRAGMA foreign_keys = OFF')
        try:
            with self._transaction():
                version = self._db.execute('PRAGMA user_version').fetchone()[0]
                if version == 0:
                    # executescript would implicitly commit the schema transaction.
                    for statement in SCHEMA.split(';'):
                        if statement.strip():
                            self._db.execute(statement)
                elif version == 1:
                    definition = SCHEMA.split('CREATE TABLE attempts (', 1)[1].split(';', 1)[0]
                    self._db.execute('CREATE TABLE attempts_v2 (' + definition)
                    self._db.execute('INSERT INTO attempts_v2 SELECT * FROM attempts')
                    self._db.execute('DROP TABLE attempts')
                    self._db.execute('ALTER TABLE attempts_v2 RENAME TO attempts')
                    self._db.execute('PRAGMA user_version = 2')
                elif version != 2:
                    raise StoreError(f'Unsupported database schema version: {version}')
                self._db.execute("CREATE TABLE IF NOT EXISTS input_transfers ("
                                 "attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), "
                                 "directory TEXT NOT NULL, status TEXT NOT NULL, reason TEXT)")
                self._db.execute("CREATE TABLE IF NOT EXISTS workers ("
                                 "worker_id TEXT PRIMARY KEY, paused INTEGER NOT NULL DEFAULT 0, "
                                 "snapshot TEXT)")
                self._db.execute('CREATE TABLE IF NOT EXISTS cancellations ('
                                 'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), '
                                 'reason TEXT NOT NULL)')
                if version in (0, 1) and self._db.execute('PRAGMA foreign_key_check').fetchone():
                    raise StoreError('Database foreign key check failed')
            self._db.execute('PRAGMA foreign_keys = ON')
        except BaseException:
            self._db.close()
            raise

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> 'Store':
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        nested = self._db.in_transaction
        self._db.execute('SAVEPOINT store_write' if nested else 'BEGIN IMMEDIATE')
        try:
            yield
            self._db.execute('RELEASE store_write' if nested else 'COMMIT')
        except BaseException:
            self._db.execute('ROLLBACK TO store_write' if nested else 'ROLLBACK')
            if nested:
                self._db.execute('RELEASE store_write')
            raise

    @contextmanager
    def scheduling_round(self) -> Iterator[None]:
        """Serialize admission reads and assignment writes in one transaction."""
        with self._transaction():
            yield

    def worker_records(self) -> dict[str, dict[str, object]]:
        """Read durable pause flags and last authenticated monitoring snapshots."""
        return {row['worker_id']: {'paused': bool(row['paused']),
                'snapshot': json.loads(row['snapshot']) if row['snapshot'] else None}
                for row in self._db.execute('SELECT * FROM workers')}

    def set_worker(self, worker_id: str, *, paused: bool | None = None,
                   snapshot: dict[str, object] | None = None) -> None:
        """Update runtime worker state without rewriting submission snapshots."""
        if not isinstance(worker_id, str) or not worker_id.strip() or '\x00' in worker_id:
            raise StoreError('worker_id must be a nonempty string without NUL')
        if paused is not None and type(paused) is not bool:
            raise StoreError('paused must be a boolean')
        if snapshot is not None and not isinstance(snapshot, dict):
            raise StoreError('snapshot must be a mapping')
        with self._transaction():
            self._db.execute('INSERT OR IGNORE INTO workers(worker_id) VALUES(?)', (worker_id,))
            if paused is not None:
                self._db.execute('UPDATE workers SET paused=? WHERE worker_id=?',
                                 (int(paused), worker_id))
            if snapshot is not None:
                self._db.execute('UPDATE workers SET snapshot=? WHERE worker_id=?',
                                 (json.dumps(snapshot, allow_nan=False), worker_id))

    def record_waiting(self, job_id: str, reason: str) -> None:
        """Persist a changed queued waiting reason without growing identical history."""
        if not isinstance(reason, str) or not reason.strip() or '\x00' in reason:
            raise StoreError('Waiting reason must be a nonempty string without NUL')
        with self._transaction():
            if self.job_state(job_id) != 'queued':
                raise StoreError('Only queued jobs can have waiting reasons')
            latest = self.events(job_id)[-1]
            if latest['reason'] != reason:
                self._event(job_id, None, 'queued', reason)

    def submit(self, specification: JobSpecification) -> None:
        """Persist a snapshot; identical submission is a no-op."""
        if not isinstance(specification, JobSpecification):
            raise StoreError('Expected a JobSpecification')
        for name in ('job_id', 'work_directory', 'input_file', 'input_sha256'):
            value = getattr(specification, name)
            if not isinstance(value, str) or not value.strip() or '\x00' in value:
                raise StoreError(f'{name} must be a nonempty string without NUL')
        if not Path(specification.work_directory).is_absolute():
            raise StoreError('work_directory must be absolute')
        input_path = Path(specification.input_file)
        if input_path.is_absolute() or '..' in input_path.parts or input_path == Path('.'):
            raise StoreError('input_file must be a relative file inside the work directory')
        if re.fullmatch(r'[0-9a-fA-F]{64}', specification.input_sha256) is None:
            raise StoreError('input_sha256 must be a 64-digit hexadecimal digest')
        resources = specification.resources
        if not isinstance(resources, Resources):
            raise StoreError('Expected Resources')
        for name in ('mpi_ranks', 'omp_threads_per_rank', 'time_limit_seconds', 'max_retries'):
            value = getattr(resources, name)
            minimum = 0 if name == 'max_retries' else 1
            if type(value) is not int or value < minimum:
                raise StoreError(f'{name} must be an integer >= {minimum}')
        if (type(resources.memory_gib) not in (int, float)
                or not math.isfinite(resources.memory_gib) or resources.memory_gib <= 0):
            raise StoreError('memory_gib must be finite and positive')
        if specification.storage_mode not in ('shared', 'staged'):
            raise StoreError('Unsupported storage mode')
        if specification.storage_mode == 'staged':
            from .staging import validate_manifest
            validate_manifest(specification.input_manifest)
            if not any(f.path == specification.input_file and f.sha256 == specification.input_sha256
                       for f in specification.input_manifest):
                raise StoreError('Primary input missing from manifest')
        payload = json.dumps(asdict(specification), sort_keys=True, allow_nan=False)
        with self._transaction():
            row = self._db.execute('SELECT specification FROM jobs WHERE job_id=?',
                                   (specification.job_id,)).fetchone()
            if row:
                if specification_from_dict(json.loads(row['specification'])) != specification:
                    raise StoreError('Job ID already has a different specification')
                return
            self._db.execute('INSERT INTO jobs(job_id,specification,state) VALUES(?,?,?)',
                             (specification.job_id, payload, 'queued'))
            self._event(specification.job_id, None, 'queued')

    def specification(self, job_id: str) -> JobSpecification:
        row = self._job(job_id)
        data = json.loads(row['specification'])
        return specification_from_dict(data)

    def jobs(self) -> tuple[dict[str, str], ...]:
        """List jobs in stable submission order."""
        return tuple(dict(row) for row in self._db.execute(
            'SELECT job_id,state,created_at FROM jobs ORDER BY rowid'))

    def job_attempts(self, job_id: str) -> tuple[Attempt, ...]:
        """Read attempts belonging to a known job."""
        self._job(job_id)
        return tuple(Attempt(**dict(row)) for row in self._db.execute(
            'SELECT * FROM attempts WHERE job_id=? ORDER BY rowid', (job_id,)))

    def job_state(self, job_id: str) -> str:
        return str(self._job(job_id)['state'])

    def _job(self, job_id: str) -> sqlite3.Row:
        row = self._db.execute('SELECT * FROM jobs WHERE job_id=?', (job_id,)).fetchone()
        if row is None:
            raise StoreError(f'Unknown job: {job_id}')
        return row

    def attempt(self, attempt_id: str) -> Attempt:
        row = self._db.execute('SELECT * FROM attempts WHERE attempt_id=?',
                               (attempt_id,)).fetchone()
        if row is None:
            raise StoreError(f'Unknown attempt: {attempt_id}')
        return Attempt(**dict(row))

    def assign(self, job_id: str, attempt_id: str, worker_id: str,
               directory: str) -> Attempt:
        """Atomically assign a queued job and reserve its snapshot resources.

        Admission and capacity decisions belong to the scheduler; this low-level
        API does not enforce capacity itself. An existing attempt ID is replayable
        only with the same assignment values.
        """
        for name, value in (('attempt_id', attempt_id), ('worker_id', worker_id),
                            ('directory', directory)):
            if not isinstance(value, str) or not value.strip() or '\x00' in value:
                raise StoreError(f'{name} must be a nonempty string without NUL')
        if not Path(directory).is_absolute() or '..' in Path(directory).parts:
            raise StoreError('Attempt directory must be an absolute path without ..')
        # Linux treats multiple leading slashes as the same root, but Path
        # preserves exactly two. Collapse them before identity/uniqueness checks.
        directory = str(Path('/' + directory.lstrip('/')))
        try:
            with self._transaction():
                existing = self._db.execute('SELECT * FROM attempts WHERE attempt_id=?',
                                            (attempt_id,)).fetchone()
                if existing:
                    if (existing['job_id'], existing['worker_id'], existing['directory']) != (
                            job_id, worker_id, directory):
                        raise StoreError('Attempt ID already has a different assignment')
                    return Attempt(**dict(existing))
                if self.job_state(job_id) != 'queued':
                    raise StoreError('Only queued jobs can be assigned')
                resources = self.specification(job_id).resources
                self._db.execute(
                    'INSERT INTO attempts(attempt_id,job_id,worker_id,directory,state) '
                    'VALUES(?,?,?,?,?)', (attempt_id, job_id, worker_id, directory, 'assigned'))
                self._db.execute('INSERT INTO reservations VALUES(?,?,?,?)',
                                 (attempt_id, worker_id, resources.cpu_slots, resources.memory_gib))
                self._db.execute('UPDATE jobs SET state=? WHERE job_id=?', ('assigned', job_id))
                self._event(job_id, attempt_id, 'assigned')
        except sqlite3.IntegrityError as exc:
            raise StoreError(f'Assignment conflicts with stored data: {exc}') from exc
        return self.attempt(attempt_id)

    def transition(self, attempt_id: str, state: str, *, exit_code: int | None = None,
                   termination_evidence: str | None = None,
                   reason: str | None = None) -> Attempt:
        """Apply a legal transition, retaining reservations until confirmed exit.

        Success requires exit code zero; failure requires a reason. Evidence may
        also describe a confirmed never-started assignment. No remote checks run.
        """
        if exit_code is not None and type(exit_code) is not int:
            raise StoreError('exit_code must be an integer')
        for name, value in (('termination_evidence', termination_evidence), ('reason', reason)):
            if value is not None and (not isinstance(value, str) or not value.strip() or '\x00' in value):
                raise StoreError(f'{name} must be a nonempty string without NUL')
        if state in TERMINAL:
            if not termination_evidence:
                raise StoreError('Terminal transitions require termination evidence')
            if state == 'succeeded' and exit_code != 0:
                raise StoreError('Success requires exit code 0')
            if state == 'failed' and not reason:
                raise StoreError('Failure requires a reason')
        elif exit_code is not None or termination_evidence is not None:
            raise StoreError('Exit information is only valid for terminal transitions')
        with self._transaction():
            old = self.attempt(attempt_id)
            if old.state == state:
                if (old.exit_code, old.termination_evidence, old.reason) != (
                        exit_code, termination_evidence, reason):
                    raise StoreError('Repeated transition has different result values')
                return old
            if state not in TRANSITIONS.get(old.state, set()):
                raise StoreError(f'Illegal transition: {old.state} -> {state}')
            self._db.execute(
                "UPDATE attempts SET state=?,exit_code=?,termination_evidence=?,reason=?, "
                "started_at=CASE WHEN ?='running' THEN COALESCE(started_at, "
                "strftime('%Y-%m-%dT%H:%M:%fZ','now')) ELSE started_at END, "
                "ended_at=CASE WHEN ? THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                'ELSE ended_at END WHERE attempt_id=?',
                (state, exit_code, termination_evidence, reason, state, state in TERMINAL, attempt_id))
            self._db.execute('UPDATE jobs SET state=? WHERE job_id=?', (state, old.job_id))
            if state in TERMINAL:
                self._db.execute('DELETE FROM reservations WHERE attempt_id=?', (attempt_id,))
            self._event(old.job_id, attempt_id, state, reason, exit_code, termination_evidence)
        return self.attempt(attempt_id)

    def request_cancel(self, attempt_id: str, reason: str = 'Cancellation requested') -> None:
        """Persist intent before delivery; completion still requires process evidence."""
        if not isinstance(reason, str) or not reason.strip() or '\x00' in reason:
            raise StoreError('Cancellation reason must be nonempty without NUL')
        with self._transaction():
            attempt = self.attempt(attempt_id)
            if attempt.state in TERMINAL:
                return
            self._db.execute('INSERT OR IGNORE INTO cancellations VALUES(?,?)', (attempt_id, reason))
            if attempt.state != 'cancelling':
                self.transition(attempt_id, 'cancelling', reason=reason)

    def cancellation(self, attempt_id: str) -> str | None:
        """Read durable cancellation intent, including after an unknown transition."""
        row = self._db.execute('SELECT reason FROM cancellations WHERE attempt_id=?',
                               (attempt_id,)).fetchone()
        return row['reason'] if row else None

    def retry(self, job_id: str) -> None:
        """Explicitly queue a confirmed failure within the submitted retry budget."""
        with self._transaction():
            attempts = self.job_attempts(job_id)
            if self.job_state(job_id) != 'failed' or not attempts:
                raise StoreError('Retry requires a failed job')
            if not attempts[-1].termination_evidence:
                raise StoreError('Retry requires confirmed termination')
            if len(attempts) > self.specification(job_id).resources.max_retries:
                raise StoreError('Submitted max_retries budget exhausted')
            if any(a.attempt_id in {r.attempt_id for r in self.reservations()} for a in attempts):
                raise StoreError('Retry blocked by unresolved reservation')
            self._db.execute("UPDATE jobs SET state='queued' WHERE job_id=?", (job_id,))
            self._event(job_id, None, 'queued', 'Explicit retry requested')

    def expire_workers(self, now: float, timeout: float) -> None:
        """Mark disconnected work unknown, retaining every reservation."""
        with self._transaction():
            records = self.worker_records()
            for reservation in self.reservations():
                record = records.get(reservation.worker_id)
                snapshot = record['snapshot'] if record else None
                attempt = self.attempt(reservation.attempt_id)
                observed = (snapshot['heartbeat_at'] if snapshot else datetime.fromisoformat(
                    attempt.created_at.replace('Z', '+00:00')).timestamp())
                if now - observed <= timeout:
                    continue
                if attempt.state in {'assigned', 'running', 'cancelling'}:
                    self.transition(attempt.attempt_id, 'unknown', reason='Worker heartbeat expired')

    def set_input_transfer(self, attempt_id: str, directory: str, status: str,
                           reason: str | None = None) -> None:
        """Record transfer progress without asserting process termination."""
        if (not isinstance(directory, str) or not Path(directory).is_absolute()
                or '\x00' in directory or '..' in Path(directory).parts):
            raise StoreError('Input transfer directory must be an absolute path without .. or NUL')
        if reason is not None and (not isinstance(reason, str) or '\x00' in reason):
            raise StoreError('Input transfer reason must be a string without NUL')
        if not isinstance(status, str) or status not in {'downloading', 'failed', 'ready'}:
            raise StoreError('Invalid input transfer status')
        with self._transaction():
            self.attempt(attempt_id)
            self._db.execute('INSERT INTO input_transfers VALUES(?,?,?,?) '
                             'ON CONFLICT(attempt_id) DO UPDATE SET '
                             'directory=excluded.directory,status=excluded.status,reason=excluded.reason',
                             (attempt_id, directory, status, reason))

    def input_transfer(self, attempt_id: str) -> dict | None:
        """Read durable worker-local paths and transfer diagnostics."""
        row = self._db.execute('SELECT * FROM input_transfers WHERE attempt_id=?',
                               (attempt_id,)).fetchone()
        return dict(row) if row else None

    def reservations(self, worker_id: str | None = None) -> tuple[Reservation, ...]:
        """Read active reservations, including unknown and cancelling attempts."""
        query = 'SELECT * FROM reservations'
        args: tuple[str, ...] = ()
        if worker_id is not None:
            query += ' WHERE worker_id=?'
            args = (worker_id,)
        return tuple(Reservation(**dict(row)) for row in
                     self._db.execute(query + ' ORDER BY attempt_id', args))

    def events(self, job_id: str) -> tuple[dict[str, object], ...]:
        """Return ordered immutable-history snapshots for a known job."""
        self._job(job_id)
        return tuple(dict(row) for row in self._db.execute(
            'SELECT * FROM events WHERE job_id=? ORDER BY event_id', (job_id,)))

    def _event(self, job_id: str, attempt_id: str | None, state: str,
               reason: str | None = None, exit_code: int | None = None,
               termination_evidence: str | None = None) -> None:
        self._db.execute(
            'INSERT INTO events(job_id,attempt_id,state,reason,exit_code,termination_evidence) '
            'VALUES(?,?,?,?,?,?)',
            (job_id, attempt_id, state, reason, exit_code, termination_evidence))
