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
from uuid import UUID

from .models import InfrastructureRecovery, JobSpecification, Resources, specification_from_dict


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
                self._db.execute("CREATE TABLE IF NOT EXISTS launch_claims ("
                                 "attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id))")
                self._db.execute('CREATE INDEX IF NOT EXISTS attempts_worker '
                                 'ON attempts(worker_id)')
                self._db.execute('CREATE INDEX IF NOT EXISTS attempts_job '
                                 'ON attempts(job_id)')
                self._db.execute('CREATE INDEX IF NOT EXISTS events_job_event '
                                 'ON events(job_id,event_id)')
                self._db.execute("CREATE TABLE IF NOT EXISTS result_collections ("
                                 "attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), "
                                 "status TEXT NOT NULL, manifest TEXT, reason TEXT)")
                self._db.execute("CREATE TABLE IF NOT EXISTS staged_cleanup ("
                                 "attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), "
                                 "identity TEXT NOT NULL, status TEXT NOT NULL, reason TEXT)")
                self._db.execute("CREATE TABLE IF NOT EXISTS workers ("
                                 "worker_id TEXT PRIMARY KEY, paused INTEGER NOT NULL DEFAULT 0, "
                                 "snapshot TEXT)")
                self._db.execute('CREATE TABLE IF NOT EXISTS cancellations ('
                                 'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), '
                                 'reason TEXT NOT NULL)')
                self._db.execute('CREATE TABLE IF NOT EXISTS worker_generations ('
                                 'generation INTEGER PRIMARY KEY, worker_id TEXT NOT NULL, '
                                 'boot_id TEXT NOT NULL, session_id TEXT NOT NULL, '
                                 'UNIQUE(worker_id, boot_id, session_id))')
                self._db.execute('CREATE TABLE IF NOT EXISTS explicit_retries ('
                                 'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id))')
                self._db.execute('CREATE TABLE IF NOT EXISTS interruptions ('
                                 'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), '
                                 'evidence TEXT NOT NULL, status TEXT NOT NULL, retry_at REAL, reason TEXT)')
                self._db.execute('CREATE TABLE IF NOT EXISTS launch_deferrals ('
                                 'attempt_id TEXT PRIMARY KEY, since REAL NOT NULL)')
                self._db.execute('CREATE TABLE IF NOT EXISTS prelaunch_acceptances ('
                                 'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id))')
                self._db.execute('CREATE TABLE IF NOT EXISTS rejections ('
                                 'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), '
                                 'code TEXT NOT NULL, reason TEXT NOT NULL, retry_at REAL NOT NULL)')
                self._db.execute('CREATE TABLE IF NOT EXISTS dispatch_priorities ('
                                 'job_id TEXT PRIMARY KEY REFERENCES jobs(job_id), '
                                 'priority INTEGER NOT NULL, origin TEXT NOT NULL)')
                self._db.execute('CREATE TABLE IF NOT EXISTS assignment_priorities ('
                                 'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), '
                                 'priority INTEGER NOT NULL, origin TEXT NOT NULL, queue_sequence INTEGER NOT NULL)')
                from .checkpoints import initialize as initialize_checkpoints
                initialize_checkpoints(self)
                from .segments import initialize as initialize_segments
                initialize_segments(self)
                from .monitoring import initialize
                initialize(self)
                from .multihost import initialize as initialize_multihost
                initialize_multihost(self)
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
    def _transaction(self, *, immediate: bool = True) -> Iterator[None]:
        nested = self._db.in_transaction
        self._db.execute('SAVEPOINT store_write' if nested else ('BEGIN IMMEDIATE' if immediate else 'BEGIN'))
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
        policy = specification.infrastructure_recovery
        if (not isinstance(policy, InfrastructureRecovery) or type(policy.max_retries) is not int or policy.max_retries < 0
                or type(policy.delay_seconds) not in (int, float)
                or not math.isfinite(policy.delay_seconds) or policy.delay_seconds <= 0):
            raise StoreError('Invalid infrastructure recovery policy')
        snapshot = asdict(specification)
        if specification.sampling is None:
            snapshot.pop('sampling')
        if not specification.mpi_placement:
            snapshot.pop('mpi_placement')
        payload = json.dumps(snapshot, sort_keys=True, allow_nan=False)
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

    def set_priority(self, job_id: str, priority: int, *, origin: str = 'manager_cli') -> None:
        """Serialize a queued override with dispatch and retain its audit origin."""
        if type(priority) is not int or not -(2**63) <= priority < 2**63:
            raise StoreError('priority must be a signed 64-bit integer')
        if not isinstance(origin, str) or not origin.strip() or '\x00' in origin or len(origin) > 256:
            raise StoreError('origin must be a nonempty string of at most 256 characters')
        with self._transaction():
            if self.job_state(job_id) != 'queued':
                raise StoreError('Only queued jobs can change priority')
            self._db.execute('INSERT INTO dispatch_priorities VALUES(?,?,?) '
                             'ON CONFLICT(job_id) DO UPDATE SET priority=excluded.priority,origin=excluded.origin',
                             (job_id, priority, origin))
            self._event(job_id, None, 'queued', f'Priority set to {priority} by {origin}')

    def scheduling_metadata(self, job_id: str, default: int = 0) -> dict:
        """Return mutable policy separately from immutable simulation inputs."""
        self._job(job_id)
        row = self._db.execute('SELECT priority,origin FROM dispatch_priorities WHERE job_id=?',
                               (job_id,)).fetchone()
        queue = self._db.execute("SELECT event_id,created_at FROM events WHERE job_id=? AND state='queued' "
                                 "AND (reason IS NULL OR reason IN ('Explicit retry requested', "
                                 "'Authenticated manager assigned a successor', 'Automatic infrastructure recovery scheduled') "
                                 "OR reason LIKE 'Prelaunch rejection:%') ORDER BY event_id DESC LIMIT 1",
                                 (job_id,)).fetchone()
        return {**(dict(row) if row else {'priority': default, 'origin': 'manager_default'}),
                'queue_sequence': queue['event_id'], 'queue_entered_at': queue['created_at']}

    def queued_jobs(self, default_priority: int = 0) -> tuple[dict, ...]:
        """Priority, total CPU demand descending, then fresh queue entry and ID."""
        jobs = [{**job, **self.scheduling_metadata(job['job_id'], default_priority),
                 'cpu_demand': self.specification(job['job_id']).resources.cpu_slots}
                for job in self.jobs() if job['state'] == 'queued']
        return tuple(sorted(jobs, key=lambda j: (-j['priority'], -j['cpu_demand'],
                                                 j['queue_sequence'], j['job_id'])))

    def assignment_priority(self, attempt_id: str) -> dict | None:
        self.attempt(attempt_id)
        row = self._db.execute('SELECT priority,origin,queue_sequence FROM assignment_priorities '
                               'WHERE attempt_id=?', (attempt_id,)).fetchone()
        return dict(row) if row else None

    def job_attempts(self, job_id: str, *, limit: int | None = None,
                     before: str | None = None) -> tuple[Attempt, ...]:
        """Read attempts; optional bounded history pages run newest to oldest."""
        self._job(job_id)
        args: list = [job_id]
        query = 'SELECT * FROM attempts WHERE job_id=?'
        if before is not None:
            anchor = self.attempt(before)
            if anchor.job_id != job_id:
                raise StoreError('Attempt cursor belongs to another job')
            query += ' AND rowid<(SELECT rowid FROM attempts WHERE attempt_id=?)'
            args.append(before)
        if limit is not None:
            if type(limit) is not int or not 1 <= limit <= 500:
                raise StoreError('History limit must be 1..500')
            rows = self._db.execute(query + ' ORDER BY rowid DESC LIMIT ?', (*args, limit)).fetchall()
            return tuple(Attempt(**dict(row)) for row in reversed(rows))
        return tuple(Attempt(**dict(row)) for row in self._db.execute(query + ' ORDER BY rowid', args))

    def job_state(self, job_id: str) -> str:
        return str(self._job(job_id)['state'])

    def cpu_accounting_attempts(self, worker_id: str, since: str, *,
                                candidate_id: str | None = None,
                                skip_unclaimed_staged: bool = False
                                ) -> tuple[tuple[Attempt, Resources], ...]:
        """Read recent or unresolved starts and immutable resources in one query.

        ``since`` is an inclusive UTC SQLite timestamp. Callers may round it
        down and then apply their exact sampling boundary to the returned rows.
        Completed history still matters when its creation/start overlaps a sample.
        """
        rows = self._db.execute(
            'SELECT a.*, j.specification, m.mpi_ranks AS participant_ranks, '
            'm.memory_gib AS participant_memory FROM attempts a '
            'JOIN jobs j ON j.job_id=a.job_id '
            'LEFT JOIN mpi_members m ON m.attempt_id=a.attempt_id AND m.worker_id=? '
            'WHERE a.attempt_id IN (SELECT attempt_id FROM attempts WHERE worker_id=? '
            'UNION SELECT attempt_id FROM mpi_members WHERE worker_id=?) '
            'AND (a.created_at>=? OR a.started_at>=? '
            'OR (a.ended_at IS NULL AND a.started_at IS NULL)) '
            'AND (? IS NULL OR a.attempt_id!=?) '
            'AND NOT (? AND a.state=\'assigned\' AND NOT EXISTS '
            '(SELECT 1 FROM launch_claims c WHERE c.attempt_id=a.attempt_id))',
            (worker_id, worker_id, worker_id, since, since, candidate_id, candidate_id, skip_unclaimed_staged))
        result = []
        for row in rows:
            data = dict(row)
            resources = specification_from_dict(json.loads(data.pop('specification'))).resources
            ranks = data.pop('participant_ranks')
            memory = data.pop('participant_memory')
            if ranks is not None:
                from dataclasses import replace
                resources = replace(resources, mpi_ranks=ranks, memory_gib=memory)
            result.append((Attempt(**data), resources))
        return tuple(result)

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
               directory: str, *, default_priority: int = 0) -> Attempt:
        """Atomically assign a queued job and reserve its snapshot resources.

        Admission and capacity decisions belong to the scheduler; this low-level
        API does not enforce capacity itself. An existing attempt ID is replayable
        only with the same assignment values.
        """
        return self._assign(job_id, attempt_id, worker_id, directory,
                            default_priority=default_priority)

    def _assign(self, job_id: str, attempt_id: str, worker_id: str, directory: str, *,
                default_priority: int = 0, coordinated: bool = False) -> Attempt:
        """Common atomic assignment write; coordinated callers hold their own transaction."""
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
                from .segments import retry_blocker
                blocked = retry_blocker(self, job_id)
                if blocked:
                    raise StoreError(blocked)
                specification = self.specification(job_id)
                if specification.mpi_placement and not coordinated:
                    raise StoreError('Multi-host MPI requires coordinated assignment')
                resources = specification.resources
                if coordinated:
                    from dataclasses import replace
                    if not specification.mpi_placement or specification.mpi_placement[0].worker_id != worker_id:
                        raise StoreError('Coordinated assignment requires its declared leader')
                    leader = specification.mpi_placement[0]
                    resources = replace(resources, mpi_ranks=leader.mpi_ranks, memory_gib=leader.memory_gib)
                self._db.execute(
                    'INSERT INTO attempts(attempt_id,job_id,worker_id,directory,state) '
                    'VALUES(?,?,?,?,?)', (attempt_id, job_id, worker_id, directory, 'assigned'))
                metadata = self.scheduling_metadata(job_id, default_priority)
                self._db.execute('INSERT INTO assignment_priorities VALUES(?,?,?,?)',
                                 (attempt_id, metadata['priority'], metadata['origin'], metadata['queue_sequence']))
                self._db.execute('INSERT INTO reservations VALUES(?,?,?,?)',
                                 (attempt_id, worker_id, resources.cpu_slots, resources.memory_gib))
                from .segments import on_assignment
                on_assignment(self, job_id, attempt_id)
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
            if state in TERMINAL:
                from .multihost import require_terminated
                require_terminated(self, attempt_id, state)
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
                self._db.execute('UPDATE mpi_members SET released=1 WHERE attempt_id=?', (attempt_id,))
            self._event(old.job_id, attempt_id, state, reason, exit_code, termination_evidence)
        return self.attempt(attempt_id)

    def request_cancel(self, attempt_id: str, reason: str = 'Cancellation requested') -> None:
        """Persist intent before delivery; completion still requires process evidence."""
        if not isinstance(reason, str) or not reason.strip() or '\x00' in reason:
            raise StoreError('Cancellation reason must be nonempty without NUL')
        with self._transaction():
            attempt = self.attempt(attempt_id)
            if attempt.state in TERMINAL:
                if (self.job_state(attempt.job_id) == 'queued'
                        and self.job_attempts(attempt.job_id)[-1].attempt_id == attempt_id):
                    self._db.execute('INSERT OR IGNORE INTO cancellations VALUES(?,?)', (attempt_id, reason))
                    self._db.execute("UPDATE jobs SET state='cancelled' WHERE job_id=?", (attempt.job_id,))
                    self._event(attempt.job_id, attempt_id, 'cancelled', reason)
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
            from .segments import retry_blocker
            blocker = retry_blocker(self, job_id)
            if blocker:
                raise StoreError(blocker)
            if self.job_state(job_id) != 'failed' or not attempts:
                raise StoreError('Retry requires a failed job')
            if not attempts[-1].termination_evidence:
                raise StoreError('Retry requires confirmed termination')
            charged = [a for a in attempts if not self.rejection(a.attempt_id)
                       and not self.interruption(a.attempt_id)]
            explicit = self._db.execute('SELECT COUNT(*) FROM explicit_retries e JOIN attempts a '
                                        'USING(attempt_id) WHERE a.job_id=?', (job_id,)).fetchone()[0]
            if max(explicit, len(charged) - 1) >= self.specification(job_id).resources.max_retries:
                raise StoreError('Submitted max_retries budget exhausted')
            if any(a.attempt_id in {r.attempt_id for r in self.reservations()} for a in attempts):
                raise StoreError('Retry blocked by unresolved reservation')
            self._db.execute("UPDATE jobs SET state='queued' WHERE job_id=?", (job_id,))
            self._db.execute('INSERT INTO explicit_retries VALUES(?)', (attempts[-1].attempt_id,))
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

    def claim_launch(self, attempt_id: str) -> bool:
        """Durably consume a launch right before executor submission."""
        with self._transaction():
            if self.attempt(attempt_id).state != 'assigned' or self.rejection(attempt_id):
                return False
            cursor = self._db.execute('INSERT OR IGNORE INTO launch_claims VALUES(?)', (attempt_id,))
            return cursor.rowcount == 1

    def accept_successor(self, job_id: str) -> None:
        """Mirror an authenticated manager assignment in a worker-local journal.

        This is not retry authorization on the manager. Worker history may be
        incomplete after placement on other workers, so only the manager counts
        the global budgets. Prior local executions must still be conclusively stopped.
        """
        with self._transaction():
            if self.job_state(job_id) != 'failed':
                return
            attempts = self.job_attempts(job_id)
            if not attempts or not attempts[-1].termination_evidence:
                raise StoreError('Successor requires confirmed local termination')
            owned = {a.attempt_id for a in attempts}
            if any(r.attempt_id in owned for r in self.reservations()):
                raise StoreError('Successor blocked by local reservation')
            self._db.execute("UPDATE jobs SET state='queued' WHERE job_id=?", (job_id,))
            self._event(job_id, None, 'queued', 'Authenticated manager assigned a successor')

    def worker_generation(self, worker_id: str, boot_id: str, session_id: str) -> None:
        """Persist authenticated generations; a change never establishes termination."""
        UUID(boot_id)
        UUID(session_id)
        with self._transaction():
            self._db.execute('INSERT OR IGNORE INTO worker_generations(worker_id,boot_id,session_id) '
                             'VALUES(?,?,?)', (worker_id, boot_id, session_id))

    def generations(self, worker_id: str) -> tuple[dict, ...]:
        return tuple(dict(row) for row in self._db.execute(
            'SELECT * FROM worker_generations WHERE worker_id=? ORDER BY generation', (worker_id,)))

    def interruption(self, attempt_id: str) -> dict | None:
        row = self._db.execute('SELECT * FROM interruptions WHERE attempt_id=?', (attempt_id,)).fetchone()
        if row is None:
            return None
        return {**dict(row), 'evidence': json.loads(row['evidence'])}

    def record_interruption(self, attempt_id: str, evidence: dict) -> None:
        """Bind conclusive worker reconciliation evidence to a confirmed failure."""
        if not isinstance(evidence, dict) or evidence.get('kind') not in {'boot_changed', 'session_ended'}:
            raise StoreError('Invalid infrastructure interruption evidence')
        saved = evidence.get('process')
        if not isinstance(saved, dict):
            raise StoreError('Missing process identity')
        UUID(saved['boot_id'])
        UUID(evidence['observed_boot_id'])
        if (type(saved.get('pid')) is not int or saved['pid'] <= 1
                or saved.get('session', saved['pid']) != saved['pid']
                or saved.get('process_group') != saved['pid']
                or not isinstance(saved.get('start_ticks'), str) or not saved['start_ticks'].isdigit()):
            raise StoreError('Invalid interruption process identity')
        changed = saved['boot_id'] != evidence['observed_boot_id']
        if changed != (evidence['kind'] == 'boot_changed'):
            raise StoreError('Contradictory interruption evidence')
        with self._transaction():
            old = self.interruption(attempt_id)
            if old:
                if old['evidence'] != evidence:
                    raise StoreError('Conflicting interruption replay')
                return
            attempt = self.attempt(attempt_id)
            expected = ('Linux boot changed' if changed else
                        'Recovered identity checked; process group no longer exists')
            if attempt.state != 'failed' or attempt.termination_evidence != expected:
                raise StoreError('Interruption requires confirmed session termination')
            self._db.execute('INSERT INTO interruptions VALUES(?,?,?,?,?)',
                             (attempt_id, json.dumps(evidence, sort_keys=True), 'detected', None, None))

    def recover_interrupted(self, attempt_id: str, now: float) -> None:
        """Atomically consume one independent infrastructure budget and queue once."""
        if type(now) not in (int, float) or not math.isfinite(now) or now < 0:
            raise StoreError('Invalid recovery time')
        with self._transaction():
            record = self.interruption(attempt_id)
            if not record or record['status'] != 'detected':
                return
            attempt = self.attempt(attempt_id)
            attempts = self.job_attempts(attempt.job_id)
            policy = self.specification(attempt.job_id).infrastructure_recovery
            used = self.recovery_status(attempt.job_id)['used']
            from .segments import retry_blocker
            reason = retry_blocker(self, attempt.job_id)
            if (attempts[-1].attempt_id != attempt_id or self.job_state(attempt.job_id) != 'failed'
                    or self.cancellation(attempt_id)):
                reason = 'Recovery blocked by cancellation or a newer job transition'
            elif any(r.attempt_id in {a.attempt_id for a in attempts} for r in self.reservations()):
                reason = 'Recovery blocked by unresolved reservations'
            elif used >= policy.max_retries:
                reason = 'Infrastructure recovery budget exhausted'
            if reason:
                self._db.execute("UPDATE interruptions SET status='blocked',reason=? WHERE attempt_id=?",
                                 (reason, attempt_id))
                self._event(attempt.job_id, attempt_id, attempt.state, reason)
                return
            self._db.execute("UPDATE interruptions SET status='queued',retry_at=?,reason=? WHERE attempt_id=?",
                             (now + policy.delay_seconds, 'Confirmed infrastructure interruption', attempt_id))
            self._db.execute("UPDATE jobs SET state='queued' WHERE job_id=?", (attempt.job_id,))
            self._event(attempt.job_id, attempt_id, 'queued', 'Automatic infrastructure recovery scheduled')

    def recovery_status(self, job_id: str) -> dict:
        """Summarize the frozen budget, consumed recoveries and latest decision."""
        records = [self.interruption(a.attempt_id) for a in self.job_attempts(job_id)]
        records = [record for record in records if record is not None]
        policy = self.specification(job_id).infrastructure_recovery
        used = sum(record['status'] == 'queued' for record in records)
        latest = records[-1] if records else None
        return {'policy': asdict(policy), 'used': used, 'remaining': max(0, policy.max_retries - used),
                'next_retry_at': latest['retry_at'] if (latest and latest['status'] == 'queued'
                    and self.job_state(job_id) == 'queued') else None,
                'latest': latest}

    def recovery_blocker(self, job_id: str, now: float) -> str | None:
        attempts = self.job_attempts(job_id)
        record = self.interruption(attempts[-1].attempt_id) if attempts else None
        if record and record['status'] == 'queued' and now < record['retry_at']:
            return 'infrastructure_recovery_delay'
        return None

    def launch_deferred(self, attempt_id: str, now: float) -> bool:
        """Allow 30 seconds for transient headroom before returning an assignment."""
        with self._transaction():
            self._db.execute('INSERT OR IGNORE INTO launch_deferrals VALUES(?,?)', (attempt_id, now))
            since = self._db.execute('SELECT since FROM launch_deferrals WHERE attempt_id=?',
                                     (attempt_id,)).fetchone()[0]
            return now - since < 30

    def accept_prelaunch(self, attempt_id: str) -> None:
        """Identify new-protocol acceptance, unlike uncertain legacy shared journals."""
        with self._transaction():
            if self.attempt(attempt_id).state != 'assigned':
                raise StoreError('Prelaunch acceptance requires assigned state')
            self._db.execute('INSERT OR IGNORE INTO prelaunch_acceptances VALUES(?)', (attempt_id,))

    def prelaunch_accepted(self, attempt_id: str) -> bool:
        return self._db.execute('SELECT 1 FROM prelaunch_acceptances WHERE attempt_id=?',
                                (attempt_id,)).fetchone() is not None

    def rejection(self, attempt_id: str) -> dict | None:
        """Read the permanent prelaunch fence, retained across reassignment."""
        row = self._db.execute('SELECT * FROM rejections WHERE attempt_id=?', (attempt_id,)).fetchone()
        return dict(row) if row else None

    def reject_launch(self, attempt_id: str, code: str, reason: str, retry_at: float) -> bool:
        """Fence an unclaimed launch atomically; a claimed launch needs reconciliation."""
        if code not in {'headroom', 'incompatible', 'not_ready', 'invalid_input'}:
            raise StoreError('Invalid rejection code')
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 2048:
            raise StoreError('Invalid rejection reason')
        if type(retry_at) not in (float, int) or not math.isfinite(retry_at) or retry_at < 0:
            raise StoreError('Invalid rejection deadline')
        with self._transaction():
            previous = self.rejection(attempt_id)
            if previous:
                if (previous['code'], previous['reason'], previous['retry_at']) != (code, reason, retry_at):
                    raise StoreError('Conflicting rejection replay')
                return True
            attempt = self.attempt(attempt_id)
            if (attempt.state not in {'assigned', 'unknown', 'cancelling'}
                    or attempt.started_at is not None or self.launch_claimed(attempt_id)):
                return False
            self._db.execute('INSERT INTO rejections VALUES(?,?,?,?)', (attempt_id, code, reason, retry_at))
            self.transition(attempt_id, 'cancelled' if self.cancellation(attempt_id) else 'failed',
                            reason=reason, termination_evidence='Durable prelaunch rejection fence')
            return True

    def return_rejected(self, attempt_id: str) -> None:
        """Requeue once after receiving a durable worker fence; never spend retry budget."""
        with self._transaction():
            attempt = self.attempt(attempt_id)
            rejection = self.rejection(attempt_id)
            if not rejection or attempt.state != 'failed' or self.cancellation(attempt_id):
                return
            if self.job_attempts(attempt.job_id)[-1].attempt_id != attempt_id:
                return
            if self.job_state(attempt.job_id) != 'failed' or rejection['code'] == 'invalid_input':
                return
            self._db.execute("UPDATE jobs SET state='queued' WHERE job_id=?", (attempt.job_id,))
            self._event(attempt.job_id, attempt_id, 'queued', 'Prelaunch rejection: ' + rejection['code'])

    def rejection_blocker(self, job_id: str, worker_id: str, now: float) -> str | None:
        rows = self._db.execute('SELECT r.* FROM rejections r JOIN attempts a USING(attempt_id) '
                                'WHERE a.job_id=? AND a.worker_id=?', (job_id, worker_id)).fetchall()
        if len(rows) >= 3:
            return 'prelaunch_rejection_exhausted'
        if any(row['retry_at'] > now for row in rows):
            return 'prelaunch_rejection_cooldown'
        return None

    def launch_claimed(self, attempt_id: str) -> bool:
        return self._db.execute('SELECT 1 FROM launch_claims WHERE attempt_id=?',
                                (attempt_id,)).fetchone() is not None

    def set_result_collection(self, attempt_id: str, status: str,
                              manifest: dict | None = None, reason: str | None = None) -> None:
        """Bind one immutable result manifest; delivery never changes process state."""
        if status not in {'pending', 'uploading', 'failed', 'collected'}:
            raise StoreError('Invalid result collection status')
        with self._transaction():
            attempt = self.attempt(attempt_id)
            if attempt.state not in TERMINAL or not attempt.termination_evidence:
                raise StoreError('Results require confirmed process termination')
            old = self.result_collection(attempt_id)
            if old and old['manifest'] is not None:
                if manifest is not None and manifest != old['manifest']:
                    raise StoreError('Conflicting result manifest')
                manifest = old['manifest']
            if old and old['status'] == 'collected':
                return
            self._db.execute('INSERT INTO result_collections VALUES(?,?,?,?) '
                             'ON CONFLICT(attempt_id) DO UPDATE SET '
                             'status=excluded.status,manifest=excluded.manifest,reason=excluded.reason',
                             (attempt_id, status, json.dumps(manifest) if manifest is not None else None, reason))

    def result_collection(self, attempt_id: str, *, include_manifest: bool = True) -> dict | None:
        """Read collection independently of the process outcome."""
        columns = '*' if include_manifest else 'attempt_id,status,reason,manifest IS NOT NULL AS has_manifest'
        row = self._db.execute(f'SELECT {columns} FROM result_collections WHERE attempt_id=?',
                               (attempt_id,)).fetchone()
        if row is None:
            return None
        result = dict(row)
        if include_manifest:
            result['manifest'] = json.loads(result['manifest']) if result['manifest'] is not None else None
        return result

    def cleanup_record(self, attempt_id: str) -> dict | None:
        """Read the retained worker directory identity and cleanup tombstone."""
        row = self._db.execute('SELECT * FROM staged_cleanup WHERE attempt_id=?',
                               (attempt_id,)).fetchone()
        return None if row is None else {**dict(row), 'identity': json.loads(row['identity'])}

    def own_staged_directory(self, attempt_id: str, identity: list) -> None:
        """Bind directory ownership once, before worker files are created."""
        if (len(identity) != 3 or any(not isinstance(pair, list) or len(pair) != 2
                or any(type(value) is not int or value < 0 for value in pair)
                for pair in identity)):
            raise StoreError('Invalid staged directory identity')
        with self._transaction():
            attempt = self.attempt(attempt_id)
            if self.specification(attempt.job_id).storage_mode != 'staged':
                raise StoreError('Cleanup ownership requires staged storage')
            old = self.cleanup_record(attempt_id)
            if old and old['identity'] != identity:
                raise StoreError('Staged directory identity changed')
            self._db.execute('INSERT OR IGNORE INTO staged_cleanup VALUES(?,?,?,NULL)',
                             (attempt_id, json.dumps(identity), 'retained'))

    def cleanup_candidates(self, after: int = 0, limit: int = 32) -> tuple[tuple[int, str], ...]:
        """Select bounded eligible work; callers rotate the cursor to avoid starvation."""
        return tuple((row[0], row[1]) for row in self._db.execute(
            "SELECT c.rowid,c.attempt_id FROM staged_cleanup c "
            "JOIN result_collections r ON r.attempt_id=c.attempt_id "
            "JOIN attempts a ON a.attempt_id=c.attempt_id "
            "WHERE r.status='collected' AND r.manifest IS NOT NULL "
            "AND a.state IN ('succeeded','failed','cancelled') "
            "AND a.termination_evidence IS NOT NULL AND c.status!='cleaned' "
            "AND NOT EXISTS (SELECT 1 FROM checkpoints k WHERE k.attempt_id=c.attempt_id AND k.acknowledged=0) "
            "AND c.rowid>? ORDER BY c.rowid LIMIT ?", (after, limit)))

    def set_cleanup(self, attempt_id: str, status: str, reason: str | None = None) -> None:
        """Cleanup never changes process outcome, collection or launch fences."""
        if status not in {'deleting', 'failed', 'cleaned'}:
            raise StoreError('Invalid cleanup status')
        with self._transaction():
            attempt = self.attempt(attempt_id)
            collection = self.result_collection(attempt_id)
            record = self.cleanup_record(attempt_id)
            if (attempt.state not in TERMINAL or not attempt.termination_evidence
                    or not collection or collection['status'] != 'collected'
                    or collection['manifest'] is None or record is None):
                raise StoreError('Cleanup requires owned files and durable result acknowledgement')
            from .checkpoints import record as checkpoint_record
            checkpoint = checkpoint_record(self, attempt_id)
            if checkpoint and not checkpoint['acknowledged']:
                raise StoreError('Cleanup requires durable checkpoint acknowledgement')
            if record['status'] != 'cleaned':
                self._db.execute('UPDATE staged_cleanup SET status=?,reason=? WHERE attempt_id=?',
                                 (status, reason, attempt_id))

    def reservations(self, worker_id: str | None = None) -> tuple[Reservation, ...]:
        """Read active reservations, including unknown and cancelling attempts."""
        query = ('SELECT attempt_id,worker_id,cpu_slots,memory_gib FROM reservations')
        peer_query = ('SELECT m.attempt_id,m.worker_id,m.cpu_slots,m.memory_gib '
                      'FROM mpi_members m JOIN attempts a USING(attempt_id) '
                      'WHERE m.released=0 AND m.worker_id!=a.worker_id')
        args: tuple[str, ...] = ()
        if worker_id is not None:
            query += ' WHERE worker_id=?'
            peer_query += ' AND m.worker_id=?'
            args = (worker_id, worker_id)
        return tuple(Reservation(**dict(row)) for row in
                     self._db.execute(query + ' UNION ALL ' + peer_query + ' ORDER BY attempt_id,worker_id', args))

    def events(self, job_id: str, *, limit: int | None = None,
               before: int | None = None) -> tuple[dict[str, object], ...]:
        """Return ordered history, optionally bounded before an event ID."""
        self._job(job_id)
        query = 'SELECT * FROM events WHERE job_id=?'
        args: list = [job_id]
        if before is not None:
            if type(before) is not int or before < 1:
                raise StoreError('Event cursor must be positive')
            query += ' AND event_id<?'
            args.append(before)
        if limit is not None:
            if type(limit) is not int or not 1 <= limit <= 500:
                raise StoreError('History limit must be 1..500')
            rows = self._db.execute(query + ' ORDER BY event_id DESC LIMIT ?', (*args, limit)).fetchall()
            return tuple(dict(row) for row in reversed(rows))
        return tuple(dict(row) for row in self._db.execute(query + ' ORDER BY event_id', args))

    def _event(self, job_id: str, attempt_id: str | None, state: str,
               reason: str | None = None, exit_code: int | None = None,
               termination_evidence: str | None = None) -> None:
        self._db.execute(
            'INSERT INTO events(job_id,attempt_id,state,reason,exit_code,termination_evidence) '
            'VALUES(?,?,?,?,?,?)',
            (job_id, attempt_id, state, reason, exit_code, termination_evidence))
