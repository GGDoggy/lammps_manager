"""Bounded operational views and durable, deduplicated runtime telemetry."""
from dataclasses import asdict
from datetime import datetime
import json
import time
from typing import BinaryIO

from .config import ManagerConfig
from .store import Store


def initialize(store: Store) -> None:
    """Add runtime tables without modifying submitted specifications."""
    store._db.execute('CREATE TABLE IF NOT EXISTS transfer_progress ('
                      'attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id), direction TEXT NOT NULL, '
                      'phase TEXT NOT NULL, total_bytes INTEGER, total_files INTEGER, '
                      'last_progress_at REAL, updated_at REAL NOT NULL, error TEXT, retries INTEGER NOT NULL DEFAULT 0, '
                      'PRIMARY KEY(attempt_id,direction))')
    store._db.execute('CREATE TABLE IF NOT EXISTS transfer_files ('
                      'attempt_id TEXT NOT NULL REFERENCES attempts(attempt_id), direction TEXT NOT NULL, '
                      'file_index INTEGER NOT NULL, transferred INTEGER, '
                      'verified_bytes INTEGER, PRIMARY KEY(attempt_id,direction,file_index))')
    store._db.execute('CREATE TABLE IF NOT EXISTS monitoring_alerts ('
                      'alert_key TEXT PRIMARY KEY, kind TEXT NOT NULL, subject TEXT NOT NULL, '
                      'first_seen_at REAL NOT NULL, last_changed_at REAL NOT NULL, '
                      'active INTEGER NOT NULL, resolved_at REAL)')


def transfer(store: Store, attempt_id: str, direction: str, phase: str, *,
             total_bytes: int | None = None, total_files: int | None = None,
             error: str | None = None, now: float | None = None) -> None:
    """Upsert one phase row; retries count transitions into failed, not polls."""
    store.attempt(attempt_id)
    if direction not in ('input', 'result') or phase not in (
            'pending', 'transferring', 'verifying', 'ready', 'acknowledged', 'failed'):
        raise ValueError('Invalid transfer direction or phase')
    for value in (total_bytes, total_files):
        if value is not None and (type(value) is not int or not 0 <= value < 2**63):
            raise ValueError('Transfer totals must be nonnegative integers or unknown')
    timestamp = time.time() if now is None else now
    with store._transaction():
        previous = store._db.execute('SELECT * FROM transfer_progress WHERE attempt_id=? AND direction=?',
                                     (attempt_id, direction)).fetchone()
        if previous and previous['phase'] in ('ready', 'acknowledged') and phase not in ('ready', 'acknowledged'):
            return
        if error is None and previous and phase not in ('ready', 'acknowledged'):
            error = previous['error']
        if previous and previous['phase'] == phase and previous['error'] == error and (
                total_bytes is None or total_bytes == previous['total_bytes']) and (
                total_files is None or total_files == previous['total_files']):
            return
        retries = (previous['retries'] if previous else 0) + int(
            phase == 'failed' and (not previous or previous['phase'] != 'failed'))
        last = previous['last_progress_at'] if previous else timestamp
        store._db.execute('INSERT INTO transfer_progress VALUES(?,?,?,?,?,?,?,?,?) '
                          'ON CONFLICT(attempt_id,direction) DO UPDATE SET '
                          'phase=excluded.phase,total_bytes=COALESCE(excluded.total_bytes,total_bytes),'
                          'total_files=COALESCE(excluded.total_files,total_files),'
                          'last_progress_at=excluded.last_progress_at,updated_at=excluded.updated_at,'
                          'error=excluded.error,retries=excluded.retries',
                          (attempt_id, direction, phase, total_bytes, total_files, last, timestamp,
                           error[:512] if error else None, retries))


def file_progress(store: Store, attempt_id: str, direction: str, index: int, *,
                  transferred: int = 0, verified_bytes: int | None = None,
                  now: float | None = None) -> None:
    """Count actual transferred bytes including replay; verified files deduplicate."""
    if direction not in ('input', 'result') or type(index) is not int or index < 0:
        raise ValueError('Invalid transfer file identity')
    for value in (transferred, verified_bytes):
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError('Invalid transfer counters')
    with store._transaction():
        previous = store._db.execute('SELECT verified_bytes FROM transfer_files WHERE attempt_id=? AND direction=? AND file_index=?',
                                     (attempt_id, direction, index)).fetchone()
        store._db.execute('INSERT INTO transfer_files VALUES(?,?,?,?,?) '
                          'ON CONFLICT(attempt_id,direction,file_index) DO UPDATE SET '
                          'transferred=CASE WHEN excluded.transferred IS NULL THEN transferred '
                          'ELSE COALESCE(transferred,0)+excluded.transferred END,'
                          'verified_bytes=COALESCE(excluded.verified_bytes,verified_bytes)',
                          (attempt_id, direction, index, transferred or None, verified_bytes))
        if transferred or (verified_bytes is not None and (not previous or previous[0] != verified_bytes)):
            store._db.execute('UPDATE transfer_progress SET last_progress_at=? '
                              'WHERE attempt_id=? AND direction=?',
                              (time.time() if now is None else now, attempt_id, direction))


def transfers(store: Store, attempt_id: str) -> dict:
    """Return unknown for missing telemetry, never an invented successful zero."""
    result = {}
    for direction in ('input', 'result'):
        row = store._db.execute('SELECT * FROM transfer_progress WHERE attempt_id=? AND direction=?',
                                 (attempt_id, direction)).fetchone()
        if not row:
            result[direction] = None
            continue
        counts = store._db.execute('SELECT SUM(transferred),SUM(verified_bytes),COUNT(verified_bytes),COUNT(*),SUM(transferred>0) '
                                   'FROM transfer_files WHERE attempt_id=? AND direction=?',
                                   (attempt_id, direction)).fetchone()
        verified_bytes = row['total_bytes'] if row['phase'] in ('ready', 'acknowledged') else counts[1]
        verified_files = row['total_files'] if row['phase'] in ('ready', 'acknowledged') else (counts[2] if counts[3] else None)
        result[direction] = {**dict(row), 'transferred_bytes': counts[0],
                             'verified_bytes': verified_bytes, 'verified_files': verified_files,
                             'transferred_files': counts[4] if counts[0] else None,
                             'acknowledged': row['phase'] == 'acknowledged',
                             'network_byte_resume': False}
    return result


class ProgressReader:
    """Report at most once per second and at close, with no byte/poll event log."""
    def __init__(self, stream: BinaryIO, store: Store, attempt_id: str, direction: str, index: int) -> None:
        self.stream, self.store, self.attempt_id = stream, store, attempt_id
        self.direction, self.index = direction, index
        self.pending = 0
        self.updated = time.monotonic()

    def read(self, size: int = -1) -> bytes:
        data = self.stream.read(size)
        self.record(len(data))
        return data

    def record(self, size: int) -> None:
        """Count payload accepted by the HTTP stream, excluding protocol overhead."""
        self.pending += size
        if time.monotonic() - self.updated >= 1:
            self.flush()

    def flush(self) -> None:
        if self.pending:
            file_progress(self.store, self.attempt_id, self.direction, self.index, transferred=self.pending)
            self.pending = 0
        self.updated = time.monotonic()


def _epoch(value: str | None) -> float | None:
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() if value else None


def _age(value: float | None, now: float) -> float | None:
    return now - value if value is not None and value <= now else None


def job_view(store: Store, job_id: str, config: ManagerConfig, now: float) -> dict:
    """Summarize operational state without credentials, inputs or environment."""
    spec = store.specification(job_id)
    attempts = store.job_attempts(job_id, limit=1)
    latest = attempts[-1] if attempts else None
    attempt_count = store._db.execute('SELECT COUNT(*) FROM attempts WHERE job_id=?', (job_id,)).fetchone()[0]
    metadata = store.scheduling_metadata(job_id, config.scheduler.default_priority)
    state = store.job_state(job_id)
    event = store._db.execute(
        "SELECT reason FROM events WHERE job_id=? AND state='queued' AND event_id>? "
        "AND reason NOT LIKE 'Priority set to %' ORDER BY event_id DESC LIMIT 1",
        (job_id, metadata['queue_sequence'])).fetchone()
    from .segments import monitoring_view
    return {'job_id': job_id, 'state': state, 'attempt_count': attempt_count,
            'queue_age_seconds': _age(_epoch(metadata['queue_entered_at']), now) if state == 'queued' else None,
            'wait_reason': event['reason'] if state == 'queued' and event else None,
            'dispatch': (store.assignment_priority(latest.attempt_id) if latest and state != 'queued' else metadata),
            'cpu_demand': spec.resources.cpu_slots,
            'reservations': [asdict(r) for r in store.reservations(latest.worker_id) if r.attempt_id == latest.attempt_id] if latest else [],
            'worker_id': latest.worker_id if latest else None,
            'attempt_id': latest.attempt_id if latest else None,
            'attempt_state': latest.state if latest else None,
            'execution_elapsed_seconds': (max(0, (_epoch(latest.ended_at) or now) - _epoch(latest.started_at))
                                          if latest and latest.started_at else None),
            'latest_error': latest.reason if latest else None,
            'rejection': store.rejection(latest.attempt_id) if latest else None,
            'infrastructure_recovery': store.recovery_status(job_id),
            'process_outcome': latest.state if latest and latest.termination_evidence else None,
            'result_collection': (store.result_collection(latest.attempt_id, include_manifest=False)
                                  or ({'status': 'pending', 'reason': 'Results have not been collected'}
                                      if spec.storage_mode == 'staged' else None)) if latest else None,
            'transfers': transfers(store, latest.attempt_id) if latest else None,
            'continuation': monitoring_view(store, job_id)}


def worker_view(store: Store, config: ManagerConfig, now: float) -> list[dict]:
    """Missing metrics stay unknown; freshness and readiness remain separate."""
    records = store.worker_records()
    result = []
    for capacity in config.workers:
        record = records.get(capacity.id, {})
        sample = record.get('snapshot') or {}
        heartbeat_age = _age(sample.get('heartbeat_at'), now)
        generations = store.generations(capacity.id)
        reservations = store.reservations(capacity.id)
        result.append({'worker_id': capacity.id, 'enabled': capacity.enabled,
                       'paused': record.get('paused'), 'ready': sample.get('ready'),
                       'draining': sample.get('draining'), 'heartbeat_age_seconds': heartbeat_age,
                       'online': heartbeat_age <= config.manager.heartbeat_timeout_seconds if heartbeat_age is not None else None,
                       'metric_age_seconds': _age(sample.get('observed_at'), now),
                       'generation': generations[-1] if generations else None,
                       'configured_capacity': asdict(capacity),
                       'reported_capacity': ({key: sample['capacity'].get(key) for key in
                                              ('id', 'cpu_slots', 'memory_budget_gib', 'max_concurrent_jobs', 'enabled')}
                                             if sample.get('capacity') else None),
                       'effective_cpu_slots': (min(capacity.cpu_slots, sample['capacity']['cpu_slots'], sample['detected_cpu_slots'])
                                               if sample.get('capacity') and sample.get('detected_cpu_slots') else None),
                       'effective_memory_gib': (min(capacity.memory_budget_gib, sample['capacity']['memory_budget_gib'], sample['detected_memory_gib'])
                                                if sample.get('capacity') and sample.get('detected_memory_gib') else None),
                       'detected_cpu_slots': sample.get('detected_cpu_slots'),
                       'detected_memory_gib': sample.get('detected_memory_gib'),
                       'reserved_cpu_slots': sum(r.cpu_slots for r in reservations),
                       'reserved_memory_gib': sum(r.memory_gib for r in reservations),
                       'reserved_jobs': len(reservations),
                       'available_memory_gib': sample.get('available_memory_gib'),
                       'staged_free_disk_gib': sample.get('staged_free_disk_gib')})
    return result


def refresh_alerts(store: Store, config: ManagerConfig, now: float) -> None:
    """One durable row per condition/object, updated only on active/resolved edges."""
    active = set()
    for worker in worker_view(store, config, now):
        identity = worker['worker_id']
        if worker['online'] is False:
            active.add(('offline_worker', identity))
        age = worker['metric_age_seconds']
        if age is not None and age > config.scheduler.metrics_max_age_seconds:
            active.add(('stale_metrics', identity))
        disk = worker['staged_free_disk_gib']
        if disk is not None and disk < config.monitoring.low_disk_gib:
            active.add(('low_disk', identity))
    for row in store._db.execute("SELECT job_id FROM jobs WHERE state='unknown'"):
        active.add(('unknown_execution', row['job_id']))
    for row in store._db.execute("SELECT a.job_id FROM interruptions i JOIN attempts a USING(attempt_id) "
                                 "WHERE i.status='blocked' AND i.reason='Infrastructure recovery budget exhausted' AND a.rowid=(SELECT MAX(rowid) FROM attempts WHERE job_id=a.job_id)"):
        active.add(('exhausted_recovery', row['job_id']))
    for row in store._db.execute("SELECT p.* FROM transfer_progress p JOIN attempts a USING(attempt_id) "
                                 "WHERE p.phase NOT IN ('ready','acknowledged') "
                                 "AND (p.direction='result' OR a.state IN ('assigned','unknown','cancelling'))"):
        if now - row['last_progress_at'] > config.monitoring.transfer_stall_seconds:
            active.add(('stalled_' + row['direction'], row['attempt_id']))
    with store._transaction():
        for kind, subject in active:
            key = json.dumps([kind, subject])
            store._db.execute('INSERT INTO monitoring_alerts VALUES(?,?,?,?,?,1,NULL) '
                              'ON CONFLICT(alert_key) DO UPDATE SET active=1,last_changed_at=excluded.last_changed_at,'
                              'resolved_at=NULL WHERE active=0', (key, kind, subject, now, now))
        for row in store._db.execute('SELECT * FROM monitoring_alerts WHERE active=1').fetchall():
            if (row['kind'], row['subject']) not in active:
                store._db.execute('UPDATE monitoring_alerts SET active=0,last_changed_at=?,resolved_at=? WHERE alert_key=?',
                                  (now, now, row['alert_key']))


def status(store: Store, config: ManagerConfig, *, limit: int = 100, after: int = 0,
           state: str | None = None, worker_id: str | None = None,
           worker_after: str = "", alert_after: str = "", now: float | None = None) -> dict:
    """Bounded keyset page; counts and details share a consistent SQLite snapshot."""
    if type(limit) is not int or not 1 <= limit <= 500 or type(after) is not int or after < 0:
        raise ValueError('limit must be 1..500 and after must be nonnegative')
    if state is not None and state not in ('queued','assigned','running','cancelling','unknown','succeeded','failed','cancelled'):
        raise ValueError('Invalid state filter')
    timestamp = time.time() if now is None else now
    refresh_alerts(store, config, timestamp)
    with store._transaction(immediate=False):
        where, args = ['j.rowid>?'], [after]
        if state:
            where.append('j.state=?')
            args.append(state)
        if worker_id:
            where.append('EXISTS(SELECT 1 FROM attempts a WHERE a.job_id=j.job_id AND a.worker_id=? '
                         'AND a.rowid=(SELECT MAX(rowid) FROM attempts WHERE job_id=j.job_id))')
            args.append(worker_id)
        rows = store._db.execute('SELECT j.rowid AS cursor,j.job_id FROM jobs j WHERE ' + ' AND '.join(where)
                                 + ' ORDER BY j.rowid LIMIT ?', (*args, limit + 1)).fetchall()
        workers = sorted(worker_view(store, config, timestamp), key=lambda w: w['worker_id'])
        workers = [w for w in workers if w['worker_id'] > worker_after]
        alerts = [dict(row) for row in store._db.execute(
            'SELECT * FROM monitoring_alerts WHERE alert_key>? ORDER BY alert_key LIMIT ?',
            (alert_after, limit + 1))]
        actions = {'unknown_execution': 'Reconcile the owning worker process identity before releasing reservations.',
                   'offline_worker': 'Restore worker connectivity; do not infer process termination.',
                   'stale_metrics': 'Check the worker monitor and clock before admitting new work.',
                   'low_disk': 'Free staged-root space while retaining unacknowledged artifacts and launch journals.',
                   'exhausted_recovery': 'Inspect interruption evidence and retry policy before explicit recovery.',
                   'stalled_input': 'Check input connectivity, disk capacity and verification errors.',
                   'stalled_result': 'Check result delivery and durable acknowledgement; do not rerun completed work.'}
        return {'schema_version': 1, 'observed_at': timestamp,
                'counts': dict(store._db.execute('SELECT state,COUNT(*) FROM jobs GROUP BY state').fetchall()),
                'attempt_counts': dict(store._db.execute('SELECT state,COUNT(*) FROM attempts GROUP BY state').fetchall()),
                'jobs': [job_view(store, row['job_id'], config, timestamp) for row in rows[:limit]],
                'next_cursor': rows[limit-1]['cursor'] if len(rows) > limit else None,
                'workers': workers[:limit],
                'next_worker_cursor': workers[limit-1]['worker_id'] if len(workers) > limit else None,
                'alerts': [{**row, 'action': actions[row['kind']]} for row in alerts[:limit]],
                'next_alert_cursor': alerts[limit-1]['alert_key'] if len(alerts) > limit else None}
