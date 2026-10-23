"""Durable all-participant accounting; worker launch transport is a separate layer."""

from dataclasses import asdict
import json
from pathlib import Path
import re
from uuid import UUID

from .config import WorkerCapacity, _parse
from .store import Attempt, Store, StoreError, TERMINAL


def initialize(store: Store) -> None:
    """Add participant records without migrating or rewriting prior snapshots."""
    store._db.execute('CREATE TABLE IF NOT EXISTS mpi_groups ('
                      'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), '
                      'authorized INTEGER NOT NULL DEFAULT 0)')
    store._db.execute('CREATE TABLE IF NOT EXISTS mpi_members ('
                      'attempt_id TEXT NOT NULL REFERENCES mpi_groups(attempt_id), '
                      'worker_id TEXT NOT NULL, mpi_ranks INTEGER NOT NULL CHECK(mpi_ranks>0), '
                      'cpu_slots INTEGER NOT NULL CHECK(cpu_slots>0), '
                      'memory_gib REAL NOT NULL CHECK(memory_gib>0), '
                      'boot_id TEXT NOT NULL, session_id TEXT NOT NULL, '
                      "phase TEXT NOT NULL DEFAULT 'reserved', directory TEXT, executable_sha256 TEXT, "
                      'termination TEXT, status TEXT, exit_code INTEGER, released INTEGER NOT NULL DEFAULT 0, '
                      'PRIMARY KEY(attempt_id,worker_id))')
    store._db.execute('CREATE INDEX IF NOT EXISTS mpi_members_worker ON mpi_members(worker_id,released)')
    store._db.execute('CREATE TABLE IF NOT EXISTS mpi_outputs ('
                      'attempt_id TEXT NOT NULL, worker_id TEXT NOT NULL, status TEXT NOT NULL, '
                      'manifest TEXT NOT NULL, PRIMARY KEY(attempt_id,worker_id), '
                      'FOREIGN KEY(attempt_id,worker_id) REFERENCES mpi_members(attempt_id,worker_id))')
    store._db.execute('CREATE TABLE IF NOT EXISTS mpi_runtime ('
                      'attempt_id TEXT PRIMARY KEY REFERENCES mpi_groups(attempt_id), plan TEXT NOT NULL)')


def members(store: Store, attempt_id: str) -> tuple[dict, ...]:
    """Read retained per-host state; an ordinary attempt has no participants."""
    return tuple(dict(row) for row in store._db.execute(
        'SELECT * FROM mpi_members WHERE attempt_id=? ORDER BY worker_id', (attempt_id,)))


def _member(store: Store, attempt_id: str, worker_id: str) -> dict:
    row = store._db.execute('SELECT * FROM mpi_members WHERE attempt_id=? AND worker_id=?',
                            (attempt_id, worker_id)).fetchone()
    if row is None:
        raise StoreError('Worker is not an MPI participant')
    return dict(row)


def _current(store: Store, worker_id: str, boot_id: str, session_id: str) -> bool:
    generations = store.generations(worker_id)
    return bool(generations and (generations[-1]['boot_id'], generations[-1]['session_id']) == (boot_id, session_id))


def _complete_membership(store: Store, attempt_id: str) -> tuple[dict, ...]:
    group = members(store, attempt_id)
    spec = store.specification(store.attempt(attempt_id).job_id)
    expected = {p.worker_id: (p.mpi_ranks, p.mpi_ranks * spec.resources.omp_threads_per_rank, p.memory_gib)
                for p in spec.mpi_placement}
    observed = {m['worker_id']: (m['mpi_ranks'], m['cpu_slots'], m['memory_gib']) for m in group}
    if not expected or expected != observed:
        raise StoreError('MPI participant membership differs from immutable placement')
    return group


def reserve(store: Store, job_id: str, attempt_id: str, directory: str,
            capacities: dict[str, WorkerCapacity], identities: dict[str, tuple[str, str]]) -> Attempt:
    """Reserve all shares atomically against supplied effective capacity limits.

    The caller must additionally check fresh metrics/load policy before calling.
    This API never launches work or substitutes a heartbeat for process evidence.
    """
    if (not isinstance(directory, str) or not Path(directory).is_absolute()
            or '..' in Path(directory).parts or '\x00' in directory):
        raise StoreError('MPI attempt directory must be absolute and safe')
    with store.scheduling_round():
        spec = store.specification(job_id)
        placement = spec.mpi_placement
        if not placement:
            raise StoreError('MPI reservation requires immutable multi-host placement')
        expected = {p.worker_id for p in placement}
        if expected - capacities.keys() or expected != identities.keys():
            raise StoreError('MPI participants need registered capacities and pinned identities')
        for worker_id in expected:
            identity = identities[worker_id]
            if not isinstance(identity, tuple) or len(identity) != 2:
                raise StoreError('MPI participant identity requires boot and session UUIDs')
            try:
                UUID(identity[0]); UUID(identity[1])
            except (ValueError, TypeError, AttributeError):
                raise StoreError('Invalid MPI participant generation') from None
        previous = members(store, attempt_id)
        if previous:
            _complete_membership(store, attempt_id)
            attempt = store.attempt(attempt_id)
            normalized = str(Path('/' + directory.lstrip('/')))
            if (attempt.job_id != job_id or attempt.directory != normalized
                    or attempt.worker_id != placement[0].worker_id
                    or {m['worker_id']: (m['boot_id'], m['session_id']) for m in previous} != identities):
                raise StoreError('Conflicting coordinated assignment replay')
            return attempt
        for participant in placement:
            capacity = capacities[participant.worker_id]
            if not isinstance(capacity, WorkerCapacity) or capacity.id != participant.worker_id:
                raise StoreError('Invalid MPI participant capacity identity')
            _parse(WorkerCapacity, {**asdict(capacity), 'labels': list(capacity.labels)}, 'mpi.capacity')
            if not capacity.enabled or not _current(store, participant.worker_id, *identities[participant.worker_id]):
                raise StoreError('MPI participant is disabled or its generation is stale')
            held = store.reservations(participant.worker_id)
            if (sum(r.cpu_slots for r in held) + participant.mpi_ranks * spec.resources.omp_threads_per_rank > capacity.cpu_slots
                    or sum(r.memory_gib for r in held) + participant.memory_gib > capacity.memory_budget_gib
                    or len(held) + 1 > capacity.max_concurrent_jobs):
                raise StoreError('MPI participant resource quota exceeded: ' + participant.worker_id)
        attempt = store._assign(job_id, attempt_id, placement[0].worker_id, directory, coordinated=True)
        store._db.execute('INSERT INTO mpi_groups(attempt_id) VALUES(?)', (attempt_id,))
        for participant in placement:
            boot, session = identities[participant.worker_id]
            store._db.execute('INSERT INTO mpi_members '
                '(attempt_id,worker_id,mpi_ranks,cpu_slots,memory_gib,boot_id,session_id) VALUES(?,?,?,?,?,?,?)',
                (attempt_id, participant.worker_id, participant.mpi_ranks,
                 participant.mpi_ranks * spec.resources.omp_threads_per_rank, participant.memory_gib, boot, session))
        _complete_membership(store, attempt_id)
        return attempt


def prepare(store: Store, attempt_id: str, worker_id: str, *, boot_id: str, session_id: str,
            directory: str, executable_sha256: str) -> None:
    """Bind authenticated, verified staged readiness to its reserved generation."""
    if (not isinstance(directory, str) or not Path(directory).is_absolute()
            or '..' in Path(directory).parts or '\x00' in directory
            or not isinstance(executable_sha256, str) or not re.fullmatch('[0-9a-f]{64}', executable_sha256)):
        raise StoreError('Invalid MPI execution directory or executable digest')
    directory = str(Path('/' + directory.lstrip('/')))
    with store.scheduling_round():
        row = _member(store, attempt_id, worker_id)
        if (row['boot_id'], row['session_id']) != (boot_id, session_id) or not _current(store, worker_id, boot_id, session_id):
            raise StoreError('MPI preparation generation differs from reservation')
        if store.cancellation(attempt_id) or row['phase'] == 'terminated':
            raise StoreError('MPI launch is already fenced or terminated')
        if row['phase'] == 'prepared':
            if (row['directory'], row['executable_sha256']) != (directory, executable_sha256):
                raise StoreError('Conflicting MPI preparation replay')
            return
        for peer in members(store, attempt_id):
            if peer['phase'] == 'prepared' and (peer['directory'], peer['executable_sha256']) != (directory, executable_sha256):
                raise StoreError('MPI participants need matching execution paths and executable fingerprints')
        store._db.execute("UPDATE mpi_members SET phase='prepared',directory=?,executable_sha256=? "
                          'WHERE attempt_id=? AND worker_id=?', (directory, executable_sha256, attempt_id, worker_id))


def recover_prelaunch(store: Store, attempt_id: str) -> None:
    """Restore an expired allocation only when no group launch was authorized.

    The coordinator must check fresh admission inside the same scheduling round.
    A prior authorization may have reached a node despite a lost RPC reply.
    """
    with store.scheduling_round():
        if store.attempt(attempt_id).state != 'unknown':
            return
        group = _complete_membership(store, attempt_id)
        authorized = store._db.execute('SELECT authorized FROM mpi_groups WHERE attempt_id=?',
                                       (attempt_id,)).fetchone()[0]
        if (authorized or store.cancellation(attempt_id)
                or any(m['phase'] not in {'reserved', 'prepared'} or m['released']
                       or not _current(store, m['worker_id'], m['boot_id'], m['session_id'])
                       for m in group)):
            raise StoreError('MPI allocation cannot safely return to prelaunch state')
        store.transition(attempt_id, 'assigned', reason='Original unlaunched MPI allocation reconnected')


def authorize(store: Store, attempt_id: str) -> bool:
    """Durably authorize one group only after every current participant is ready."""
    with store.scheduling_round():
        group = _complete_membership(store, attempt_id)
        if (not group or store.cancellation(attempt_id) or store.attempt(attempt_id).state != 'assigned'
                or any(m['phase'] != 'prepared' or not _current(store, m['worker_id'], m['boot_id'], m['session_id']) for m in group)):
            return False
        store._db.execute('UPDATE mpi_groups SET authorized=1 WHERE attempt_id=?', (attempt_id,))
        return True


def terminate(store: Store, attempt_id: str, worker_id: str, *, evidence: dict,
              status: str, exit_code: int | None = None) -> None:
    """Retain per-host fenced termination evidence; never release only one share.

    Callers must have checked actual processes and closed their durable launch
    fence. A boot change alone cannot authorize launch on the replacement boot.
    """
    if (not isinstance(evidence, dict) or evidence.get('launch_fenced') is not True
            or evidence.get('kind') not in {'never_started', 'session_ended', 'boot_changed'}
            or status not in TERMINAL or (exit_code is not None and type(exit_code) is not int)):
        raise StoreError('Invalid MPI participant termination evidence')
    try:
        UUID(evidence['observed_boot_id'])
    except (KeyError, ValueError, TypeError, AttributeError):
        raise StoreError('Missing MPI termination boot identity') from None
    if status == 'succeeded' and (exit_code != 0 or evidence['kind'] != 'session_ended'):
        raise StoreError('MPI success requires a confirmed successful execution')
    payload = json.dumps(evidence, sort_keys=True, allow_nan=False)
    with store.scheduling_round():
        row = _member(store, attempt_id, worker_id)
        changed = evidence['observed_boot_id'] != row['boot_id']
        if changed != (evidence['kind'] == 'boot_changed'):
            raise StoreError('Contradictory MPI termination boot evidence')
        if evidence['kind'] == 'session_ended':
            process = evidence.get('process')
            if (not isinstance(process, dict) or type(process.get('pid')) is not int or process['pid'] <= 1
                    or process.get('session') != process['pid'] or process.get('process_group') != process['pid']
                    or process.get('boot_id') != row['boot_id']
                    or not isinstance(process.get('start_ticks'), str) or not process['start_ticks'].isdigit()):
                raise StoreError('Invalid MPI participant process identity')
        if row['phase'] == 'terminated':
            if (row['termination'], row['status'], row['exit_code']) != (payload, status, exit_code):
                raise StoreError('Conflicting MPI participant termination replay')
            return
        store._db.execute("UPDATE mpi_members SET phase='terminated',termination=?,status=?,exit_code=? "
                          'WHERE attempt_id=? AND worker_id=?', (payload, status, exit_code, attempt_id, worker_id))


def require_terminated(store: Store, attempt_id: str, state: str) -> None:
    """Prevent ordinary terminal paths from releasing an unresolved MPI group."""
    if not store._db.execute('SELECT 1 FROM mpi_groups WHERE attempt_id=?', (attempt_id,)).fetchone():
        return
    group = _complete_membership(store, attempt_id)
    if any(m['phase'] != 'terminated' or not m['termination'] for m in group):
        raise StoreError('Every MPI participant needs fenced termination evidence')
    if group and state == 'succeeded' and (store.cancellation(attempt_id)
            or any(m['status'] != 'succeeded' for m in group)
            or not store._db.execute('SELECT authorized FROM mpi_groups WHERE attempt_id=?', (attempt_id,)).fetchone()[0]):
        raise StoreError('Every MPI participant must succeed after group authorization')


def finish(store: Store, attempt_id: str) -> Attempt | None:
    """Commit a whole-group outcome and release all shares only after the barrier."""
    with store.scheduling_round():
        group = _complete_membership(store, attempt_id)
        old = store.attempt(attempt_id)
        if old.state in TERMINAL:
            return old
        if any(m['phase'] != 'terminated' or not m['termination'] for m in group):
            return None
        if store.cancellation(attempt_id):
            state, code, reason = 'cancelled', None, store.cancellation(attempt_id)
        elif all(m['status'] == 'succeeded' for m in group):
            if not store._db.execute('SELECT authorized FROM mpi_groups WHERE attempt_id=?', (attempt_id,)).fetchone()[0]:
                raise StoreError('MPI success requires group launch authorization')
            state, code, reason = 'succeeded', 0, None
            if old.state == 'assigned':
                store.transition(attempt_id, 'running')
        else:
            state, code, reason = 'failed', None, 'MPI participant failed or never completed'
        return store.transition(attempt_id, state, exit_code=code, reason=reason,
                                termination_evidence='Every MPI participant launch fenced and termination confirmed')


def result_operation(store: Store, storage, worker_id: str, attempt_id: str, operation: str,
                     payload: dict | None=None, stream=None, length: int | None=None) -> dict:
    """Collect bounded peer trees and acknowledge complete delivery only for the whole group.

    The authenticated manager serializes this with its artifact lock. No SQLite
    write transaction is held during filesystem publication or large transfers.
    """
    from .models import InputFile
    from .results import output_limits, validate_results, upload_file, commit_results
    from .staging import safe_path, open_regular, publish_inputs, validate_manifest
    group=_complete_membership(store,attempt_id)
    member=_member(store,attempt_id,worker_id)
    attempt=store.attempt(attempt_id)
    if attempt.state not in TERMINAL or member['phase']!='terminated' or not attempt.termination_evidence:
        raise StoreError('MPI results require confirmed whole-group termination')
    if len(safe_path(worker_id).parts)!=1 or len(safe_path(attempt_id).parts)!=1:
        raise StoreError('Unsafe MPI artifact identity')
    directory=Path(store.specification(attempt.job_id).work_directory)/attempt_id
    if directory.resolve()!=directory or directory.is_symlink():
        raise StoreError('Unsafe MPI result directory')
    peer_root=directory/'.mpi-outputs'/worker_id
    if peer_root.resolve()!=peer_root:
        raise StoreError('Unsafe MPI participant output directory')
    peer_root.mkdir(mode=0o700,parents=True,exist_ok=True)
    limits=output_limits(storage)
    row=store._db.execute('SELECT * FROM mpi_outputs WHERE attempt_id=? AND worker_id=?',
                          (attempt_id,worker_id)).fetchone()
    if operation=='manifest':
        validate_results(payload,limits)
        if row and json.loads(row['manifest'])!=payload:
            raise StoreError('Conflicting MPI participant result manifest')
        if not row:
            store._db.execute('INSERT INTO mpi_outputs VALUES(?,?,?,?)',
                              (attempt_id,worker_id,'uploading',json.dumps(payload,sort_keys=True)))
        if store.result_collection(attempt_id) is None:
            store.set_result_collection(attempt_id,'pending',reason='Awaiting all MPI participant outputs')
    row=store._db.execute('SELECT * FROM mpi_outputs WHERE attempt_id=? AND worker_id=?',
                          (attempt_id,worker_id)).fetchone()
    if row is None: raise StoreError('MPI participant result manifest required')
    files=validate_results(json.loads(row['manifest']),limits)
    if operation=='commit':
        commit_results(peer_root,files,limits)
        store._db.execute("UPDATE mpi_outputs SET status='collected' WHERE attempt_id=? AND worker_id=?",
                          (attempt_id,worker_id))
        outputs={r['worker_id']:dict(r) for r in store._db.execute('SELECT * FROM mpi_outputs WHERE attempt_id=?',(attempt_id,))}
        if all(m['worker_id'] in outputs and outputs[m['worker_id']]['status']=='collected' for m in group):
            combined=[]; sources=[]; omitted=[]
            for actor,output in sorted(outputs.items()):
                manifest=json.loads(output['manifest'])
                base=directory/'.mpi-outputs'/actor/'results'
                prefix='work/mpi-participants/'+actor+'/'
                for entry in manifest['files']:
                    combined.append(InputFile(prefix+entry['path'],entry['size'],entry['sha256']))
                    sources.append((base,entry['path']))
                    if actor==attempt.worker_id and entry['path'] in {'stdout','stderr','execution.json','process.json','outcome.json'}:
                        combined.append(InputFile(**entry));sources.append((base,entry['path']))
                omitted.extend(dict(path=prefix+entry['path'],reason=entry['reason']) for entry in manifest['omitted'])
            validate_manifest(tuple(combined),**limits)
            publish_inputs(directory/'results',tuple(combined),
                           lambda index:open_regular(*sources[index]),limits)
            store.set_result_collection(attempt_id,'collected',dict(files=[asdict(f) for f in combined],omitted=omitted))
    elif operation!='manifest':
        if row['status']=='collected': raise StoreError('MPI participant results already collected')
        index=int(operation)
        if not 0<=index<len(files) or length!=files[index].size:
            raise StoreError('Invalid MPI result index or length')
        upload_file(peer_root,files[index],stream)
    final=store._db.execute('SELECT status FROM mpi_outputs WHERE attempt_id=? AND worker_id=?',
                            (attempt_id,worker_id)).fetchone()[0]
    overall=store.result_collection(attempt_id)
    return {'status':final,'group_collected':bool(overall and overall['status']=='collected')}
