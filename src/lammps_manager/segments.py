"""Restricted sequential continuation runs; every successor returns to dispatch.

No arbitrary-script rewriting or worker-local chaining. Filesystem publication
precedes the single SQLite transaction that counts progress and enqueues once.
"""
from dataclasses import asdict
import hashlib
from io import BytesIO
import json
import secrets
import time
from pathlib import Path
from uuid import uuid4

from . import checkpoints as cp
from .config import ManagerConfig, create_job_specification
from .continuation import (Recipe, render_abp_input, require_contract, ENGINEERING_VERSION,
    FRESH_STREAM_POLICY, engineering_parameters, engineering_digest, render_engineering_input)
from .abp_parameters import (PARAMETERIZED_VERSION, normalize_parameters, model_summary,
    render_parameterized_input, render_initialization, parameterized_digest)
from .models import InputFile
from .staging import open_regular, publish_inputs, safe_path
from .store import Store, StoreError, TERMINAL


def plan_steps(total_steps: int, step_max: int = 100000, start_step: int = 0) -> tuple[tuple[int, int], ...]:
    """Cover a global interval exactly, with no overlap or zero-length tail."""
    if (any(type(v) is not int for v in (total_steps, step_max, start_step))
            or total_steps <= 0 or step_max <= 0 or start_step < 0
            or total_steps + start_step >= 2**63):
        raise ValueError('Steps must be positive integers with a nonnegative start and fit SQLite integers')
    count = (total_steps + step_max - 1) // step_max
    if count > 100000:
        raise ValueError('Segment plan exceeds 100000 segments')
    end = start_step + total_steps
    return tuple((start, min(start + step_max, end)) for start in range(start_step, end, step_max))


def initialize(store: Store) -> None:
    """Install additive run state; simulation snapshots never change."""
    store._db.execute('CREATE TABLE IF NOT EXISTS simulation_runs ('
        'run_id TEXT PRIMARY KEY,snapshot TEXT NOT NULL,status TEXT NOT NULL,'
        'committed_step INTEGER NOT NULL,first_priority INTEGER,initial_override INTEGER,'
        'continuation_override INTEGER,cancelled INTEGER NOT NULL DEFAULT 0,reason TEXT)')
    store._db.execute('CREATE TABLE IF NOT EXISTS run_segments ('
        'run_id TEXT NOT NULL REFERENCES simulation_runs(run_id),ordinal INTEGER NOT NULL,'
        'job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),start_step INTEGER NOT NULL,'
        'stop_step INTEGER NOT NULL,checkpoint_id TEXT REFERENCES checkpoints(identity),'
        'completed INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(run_id,ordinal))')
    store._db.execute('CREATE TABLE IF NOT EXISTS run_events ('
        'event_id INTEGER PRIMARY KEY,run_id TEXT NOT NULL REFERENCES simulation_runs(run_id),'
        'description TEXT NOT NULL,created_at TEXT NOT NULL DEFAULT (strftime(\'%Y-%m-%dT%H:%M:%fZ\',\'now\')))')

    store._db.execute('CREATE TABLE IF NOT EXISTS continuation_environments ('
        'worker_id TEXT PRIMARY KEY,boot_id TEXT NOT NULL,fingerprint TEXT,observed_at REAL NOT NULL)')
    new_bindings = store._db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='run_environments'").fetchone() is None
    store._db.execute('CREATE TABLE IF NOT EXISTS run_environments ('
        'run_id TEXT PRIMARY KEY REFERENCES simulation_runs(run_id),generation INTEGER NOT NULL DEFAULT 0,'
        'worker_id TEXT,boot_id TEXT,fingerprint TEXT,missing_since REAL,reset_pending INTEGER NOT NULL DEFAULT 0)')
    store._db.execute('CREATE TABLE IF NOT EXISTS run_segment_history ('
        'run_id TEXT NOT NULL,generation INTEGER NOT NULL,ordinal INTEGER NOT NULL,'
        'job_id TEXT NOT NULL UNIQUE,start_step INTEGER NOT NULL,stop_step INTEGER NOT NULL,'
        'checkpoint_id TEXT,completed INTEGER NOT NULL,PRIMARY KEY(run_id,generation,ordinal))')
    # Upgrade already assigned chains without changing their immutable contracts.
    legacy = store._db.execute('SELECT c.run_id,c.contract,a.worker_id FROM checkpoints c '
            'JOIN attempts a USING(attempt_id) JOIN run_segments s ON s.job_id=a.job_id '
            'ORDER BY s.ordinal').fetchall() if new_bindings else ()
    for row in legacy:
        contract = json.loads(row['contract'])
        store._db.execute('INSERT OR IGNORE INTO run_environments(run_id,worker_id,boot_id,fingerprint) VALUES(?,?,?,?)',
            (row['run_id'], row['worker_id'], contract.get('execution_boot_id'), contract['build_fingerprint']))


def _run(store: Store, run_id: str) -> dict:
    row = store._db.execute('SELECT * FROM simulation_runs WHERE run_id=?', (run_id,)).fetchone()
    if row is None:
        raise StoreError('Unknown simulation run: ' + run_id)
    result = dict(row)
    result['snapshot'] = json.loads(result['snapshot'])
    return result


def segment(store: Store, job_id: str) -> dict | None:
    """Resolve membership separately from the immutable job specification."""
    row = store._db.execute('SELECT * FROM run_segments WHERE job_id=?', (job_id,)).fetchone()
    return dict(row) if row else None


def _priority(value: int | None) -> None:
    if value is not None and (type(value) is not int or not -(2**63) <= value < 2**63):
        raise ValueError('Priority must be a signed 64-bit integer')


def submit_run(store: Store, config: ManagerConfig, *, total_steps: int,
               build_fingerprint: str | None = None, recipe: Recipe = Recipe('ideal-abp-2d-v1', 'statistical'),
               step_max: int | None = None, start_step: int = 0,
               segment_ends: tuple[int, ...] | None = None, run_id: str | None = None,
               resource_overrides: dict | None = None,
               initial_priority: int | None = None, continuation_priority: int | None = None,
               contract_version: str | None = None, model_parameters: dict | None = None) -> str:
    """Snapshot a supported recipe/plan, publish its first inputs and queue once.

    Explicit measured boundaries may shorten segments below the maximum. Generic
    arithmetic planning never grants scientific acceptance for an unmeasured run.
    """
    version = (ENGINEERING_VERSION if recipe.name == 'wca-abp-2d-v1' else None) if contract_version is None else contract_version
    if model_parameters is not None and contract_version not in (None, PARAMETERIZED_VERSION):
        raise ValueError('Custom physical parameters require engineering-abp-v2')
    if model_parameters is not None or version == PARAMETERIZED_VERSION:
        version = PARAMETERIZED_VERSION
        full_model = normalize_parameters(recipe, {} if model_parameters is None else model_parameters)
        summary = model_summary(recipe, full_model)
    else:
        full_model = summary = None
    maximum = config.segmentation.step_max if step_max is None else step_max
    ranges = plan_steps(total_steps, maximum, start_step)
    if segment_ends is not None:
        if type(segment_ends) is not tuple or not segment_ends:
            raise ValueError('Explicit segment ends must be a nonempty tuple')
        starts = (start_step, *segment_ends[:-1])
        ranges = tuple(zip(starts, segment_ends))
        if (any(type(end) is not int or not start < end <= start + maximum for start, end in ranges)
                or segment_ends[-1] != start_step + total_steps
                or (not version and total_steps <= maximum and len(ranges) != 1) or len(ranges) > 100000):
            raise ValueError('Explicit boundaries must exactly cover the interval within step_max')
    ends = tuple(stop for _, stop in ranges)
    if start_step != 0:
        raise ValueError('The released recipe requires its original stationary initial ensemble at step 0')
    require_contract(recipe, build_fingerprint='0' * 64 if build_fingerprint is None else build_fingerprint,
                     total_steps=total_steps, segment_ends=ends, version=version, parameters=summary)
    _priority(initial_priority)
    _priority(continuation_priority)
    identity = run_id or uuid4().hex
    if len(safe_path(identity).parts) != 1 or len(identity) > 128:
        raise ValueError('Invalid run identity')
    namespace = 'lammps-manager-run-v1:' + identity
    seeds = []
    if not version:
        identities = [('setup', 'positions'), ('setup', 'orientations')]
        identities += [('segmented', str(start)) for start, _ in ranges]
        seeds = [int.from_bytes(hashlib.sha256(json.dumps([namespace, 'run', arm, 0, boundary]).encode())
                 .digest()[:8], 'big') % 899999999 + 1 for arm, boundary in identities]
        if len(set(seeds)) != len(seeds):
            raise ValueError('Derived seed collision; submit with a new run identity')
    # Validate resource/recovery policy once and retain it at run level too.
    prototype = create_job_specification(config, job_id=identity,
        work_directory=str(Path(config.storage.work_root) / identity), input_file='in.lmp',
        input_sha256='0' * 64, resource_overrides=resource_overrides)
    if (prototype.resources.mpi_ranks != recipe.mpi_ranks
            or prototype.resources.omp_threads_per_rank != recipe.omp_threads):
        raise ValueError('Run resources differ from released continuation layout')
    snapshot = dict(run_id=identity, total_steps=total_steps, start_step=start_step, step_max=maximum,
        segment_ends=ends, recipe=asdict(recipe), build_fingerprint=build_fingerprint,
        resources=asdict(prototype.resources), infrastructure_recovery=asdict(prototype.infrastructure_recovery),
        work_root=config.storage.work_root, storage_mode=config.storage.mode,
        input_limits=dict(max_files=config.storage.max_input_files,
            max_file_bytes=config.storage.max_input_file_bytes, max_total_bytes=config.storage.max_input_total_bytes),
        stream_provenance=cp.canonical(dict(namespace=namespace, phase='run', replica=0, seeds=seeds)))
    if version:
        snapshot.update(contract_version=version, stream_policy=FRESH_STREAM_POLICY,
            evidence_status='not_assessed', model=summary if summary is not None else engineering_parameters(recipe),
            recipe_sha256=parameterized_digest(recipe, summary) if summary is not None else engineering_digest(recipe),
            requested_priorities=[initial_priority, continuation_priority])
        if full_model is not None:
            snapshot['model_parameters'] = full_model
        snapshot.pop('stream_provenance')
        # Commit entropy before filesystem publication. An interrupted publication
        # leaves this allocation available to an identical replay, never redrawn.
        with store.scheduling_round():
            existing = store._db.execute('SELECT snapshot FROM simulation_runs WHERE run_id=?', (identity,)).fetchone()
            if existing:
                old = json.loads(existing['snapshot'])
                intent = {k: v for k, v in old.items() if k != 'stream_provenance'}
                if cp.canonical(intent) != cp.canonical(snapshot):
                    raise StoreError('Run ID already has a different snapshot')
            else:
                seeds = []
                used = set()
                while len(seeds) < len(ranges) + 2:
                    seed = secrets.randbelow(899999999) + 1
                    if seed not in used:
                        used.add(seed)
                        seeds.append(seed)
                allocation = dict(policy=FRESH_STREAM_POLICY, seeds=seeds)
                allocation['sha256'] = hashlib.sha256(cp.canonical(allocation).encode()).hexdigest()
                snapshot['stream_provenance'] = cp.canonical(allocation)
                store._db.execute('INSERT INTO simulation_runs(run_id,snapshot,status,committed_step,initial_override,'
                    'continuation_override) VALUES(?,?,?,?,?,?)',
                    (identity, cp.canonical(snapshot), 'queued', start_step, initial_priority, continuation_priority))
        with store.scheduling_round():
            if not store._db.execute('SELECT 1 FROM run_segments WHERE run_id=?', (identity,)).fetchone():
                run = _run(store, identity)
                if run['cancelled']:
                    raise StoreError('Run cancelled before input publication')
                _enqueue(store, run, 0, None)
                store._db.execute('INSERT INTO run_events(run_id,description) VALUES(?,?)', (identity, 'Run submitted'))
        return identity
    with store.scheduling_round():
        existing = store._db.execute('SELECT snapshot FROM simulation_runs WHERE run_id=?', (identity,)).fetchone()
        if existing:
            if existing['snapshot'] != cp.canonical(snapshot):
                raise StoreError('Run ID already has a different snapshot')
            return identity
        store._db.execute('INSERT INTO simulation_runs(run_id,snapshot,status,committed_step,initial_override,'
            'continuation_override) VALUES(?,?,?,?,?,?)',
            (identity, cp.canonical(snapshot), 'queued', start_step, initial_priority, continuation_priority))
        _enqueue(store, _run(store, identity), 0, None)
        store._db.execute('INSERT INTO run_events(run_id,description) VALUES(?,?)', (identity, 'Run submitted'))
    return identity


def _enqueue(store: Store, run: dict, ordinal: int, checkpoint: dict | None) -> None:
    """Publish complete inputs before creating a unique queued segment transactionally."""
    from .models import JobSpecification, Resources, InfrastructureRecovery
    snapshot = run['snapshot']
    ends = snapshot['segment_ends']
    start = snapshot['start_step'] if ordinal == 0 else ends[ordinal - 1]
    stop = ends[ordinal]
    generation = environment(store, run['run_id'])['generation']
    suffix = f'.g{generation:06d}' if generation else ''
    job = f"{run['run_id']}{suffix}.s{ordinal:06d}"
    root = Path(snapshot['work_root']) / job
    if any(path.is_symlink() or path.resolve() != path for path in (root.parent, root)):
        raise ValueError('Unsafe segment input root')
    seeds = json.loads(snapshot['stream_provenance'])['seeds']
    renderer = (lambda **kw: render_engineering_input(Recipe(**snapshot['recipe']), **kw)) if snapshot.get('contract_version') else render_abp_input
    if snapshot.get('contract_version') == PARAMETERIZED_VERSION:
        renderer = lambda **kw: render_parameterized_input(Recipe(**snapshot['recipe']), snapshot['model'], **kw)
    script = renderer(start=start, stop=stop, total=snapshot['total_steps'],
        stream_seed=seeds[2 + ordinal], position_seed=seeds[0], orientation_seed=seeds[1]).encode()
    primary = InputFile('in.lmp', len(script), hashlib.sha256(script).hexdigest())
    generated = {'in.lmp': script}
    if snapshot.get('contract_version') == PARAMETERIZED_VERSION:
        generated['model-parameters.json'] = cp.canonical(snapshot['model_parameters']).encode()
        if start == 0:
            generated['initial-state.lmp'] = render_initialization(Recipe(**snapshot['recipe']),
                snapshot['model_parameters'], position_seed=seeds[0], orientation_seed=seeds[1]).encode()
    files = tuple(InputFile(path, len(data), hashlib.sha256(data).hexdigest()) for path, data in generated.items())
    if checkpoint:
        if not checkpoint['acknowledged'] or not checkpoint['directory']:
            raise StoreError('Successor requires committed checkpoint publication')
        files += tuple(InputFile(**entry) for entry in checkpoint['manifest']['files'])
    root.mkdir(exist_ok=True)
    publish_inputs(root / 'inputs', files,
        lambda index: BytesIO(generated[files[index].path]) if files[index].path in generated else open_regular(Path(checkpoint['directory']), files[index].path),
        snapshot['input_limits'])
    spec = JobSpecification(job, str(root), 'in.lmp', primary.sha256,
        Resources(**snapshot['resources']), snapshot['storage_mode'], files,
        InfrastructureRecovery(**snapshot['infrastructure_recovery']))
    store.submit(spec)
    store._db.execute('INSERT INTO run_segments VALUES(?,?,?,?,?,?,0)',
        (run['run_id'], ordinal, job, start, stop, checkpoint['identity'] if checkpoint else None))
    if checkpoint:
        cp.pin(store, checkpoint['identity'], job)
    priority = run['initial_override'] if ordinal == 0 else (
        run['continuation_override'] if run['continuation_override'] is not None else run['first_priority'])
    if priority is not None:
        store.set_priority(job, priority, origin='run_initial' if ordinal == 0 else 'run_continuation')


def checkpoint_provenance(snapshot: dict, ordinal: int) -> str:
    """Bound each checkpoint to one leaf of the immutable random allocation."""
    if not snapshot.get('contract_version'):
        return snapshot['stream_provenance']
    allocation = json.loads(snapshot['stream_provenance'])
    seeds = allocation['seeds']
    return cp.canonical(dict(version=snapshot['contract_version'], policy=allocation['policy'],
        allocation_sha256=allocation['sha256'], segment=ordinal,
        seeds=[seeds[0], seeds[1], seeds[2 + ordinal]],
        recipe_sha256=snapshot['recipe_sha256'], model=snapshot['model'], evidence_status='not_assessed'))


def on_assignment(store: Store, job_id: str, attempt_id: str) -> None:
    """Enforce chain exclusivity and register the immutable output checkpoint."""
    item = segment(store, job_id)
    if item is None:
        return
    run = _run(store, item['run_id'])
    if run['cancelled'] or item['completed']:
        raise StoreError('Run cancelled or segment already committed')
    other = store._db.execute('SELECT 1 FROM run_segments s JOIN attempts a USING(job_id) '
        'JOIN reservations r USING(attempt_id) WHERE s.run_id=? AND a.attempt_id!=?',
        (run['run_id'], attempt_id)).fetchone()
    if other:
        raise StoreError('Another segment retains a reservation')
    snapshot = run['snapshot']
    binding = bind_assignment(store, run['run_id'], attempt_id)
    if item['ordinal']:
        dependency = store._db.execute('SELECT * FROM checkpoints WHERE identity=? AND acknowledged=1',
                                      (item['checkpoint_id'],)).fetchone()
        if dependency is None:
            raise StoreError('Missing committed checkpoint dependency')
        parent = store.attempt(dependency['attempt_id'])
        if not cp.compatible_worker(store, dependency['attempt_id'], store.attempt(attempt_id).worker_id):
            raise StoreError('Cross-worker continuation requires the pinned compatible host boot')
    if run['first_priority'] is None:
        priority = store.assignment_priority(attempt_id)['priority']
        store._db.execute('UPDATE simulation_runs SET first_priority=? WHERE run_id=?', (priority, run['run_id']))
    contract = cp.CheckpointContract(run['run_id'], item['ordinal'], attempt_id, job_id,
        Recipe(**snapshot['recipe']), binding['fingerprint'], snapshot['total_steps'],
        tuple(snapshot['segment_ends']), item['stop_step'], item['stop_step'] * snapshot.get('model', {}).get('dt', .005),
        checkpoint_provenance(snapshot, item['ordinal']), resources=store.specification(job_id).resources,
        recipe_sha256=snapshot.get('recipe_sha256', cp.abp_recipe_digest()),
        parent_checkpoint_id=item['checkpoint_id'],
        execution_boot_id=(store.generations(store.attempt(attempt_id).worker_id)[-1]['boot_id']
            if store.generations(store.attempt(attempt_id).worker_id) else None))
    cp.expect(store, contract)
    store._db.execute("UPDATE simulation_runs SET status='running',reason=NULL WHERE run_id=?", (run['run_id'],))


def worker_blocker(store: Store, job_id: str, worker_id: str, *,
                   now: float | None = None, max_age: float | None = None) -> str | None:
    """Exclude unsupported destinations before reserving resources."""
    item = segment(store, job_id)
    if item is None:
        if store._db.execute('SELECT 1 FROM run_segment_history WHERE job_id=?', (job_id,)).fetchone():
            return 'retired_continuation_generation'
        return None
    run = _run(store, item['run_id'])
    if run['cancelled']:
        return 'parent_cancelled'
    binding = environment(store, run['run_id'])
    if binding['reset_pending']:
        return 'environment_reset_pending'
    observed = worker_environment(store, worker_id)
    if observed is None or (now is not None and max_age is not None
                            and now - observed['observed_at'] > max_age):
        return 'continuation_environment_unavailable'
    if binding['fingerprint'] and any(binding[key] != observed[key] for key in ('worker_id', 'boot_id', 'fingerprint')):
        return 'bound_continuation_environment'
    if item['checkpoint_id']:
        row = store._db.execute('SELECT attempt_id FROM checkpoints WHERE identity=? AND acknowledged=1',
                                (item['checkpoint_id'],)).fetchone()
        if row is None:
            return 'missing_checkpoint'
        if not cp.compatible_worker(store, row['attempt_id'], worker_id):
            return 'unvalidated_continuation_worker'
    return None


def advance(store: Store, run_id: str) -> None:
    """Count one committed boundary and enqueue one successor, or complete parent."""
    with store.scheduling_round():
        run = _run(store, run_id)
        if run['cancelled']:
            _finish_cancel(store, run)
            return
        if environment(store, run_id)['reset_pending']:
            return
        if run['status'] == 'succeeded':
            return
        rows = store._db.execute('SELECT * FROM run_segments WHERE run_id=? AND completed=0 ORDER BY ordinal',
                                  (run_id,)).fetchall()
        if not rows:
            return
        item = dict(rows[0])
        attempts = store.job_attempts(item['job_id'])
        if not attempts:
            return
        attempt = attempts[-1]
        if attempt.state not in TERMINAL:
            store._db.execute('UPDATE simulation_runs SET status=?,reason=? WHERE run_id=?',
                              (attempt.state, attempt.reason, run_id))
            return
        if store.job_state(item['job_id']) == 'queued':
            store._db.execute("UPDATE simulation_runs SET status='queued',reason=? WHERE run_id=?",
                              ('Segment recovery queued from its last committed input checkpoint', run_id))
            return
        if attempt.state != 'succeeded':
            store._db.execute("UPDATE simulation_runs SET status=?,reason=? WHERE run_id=?",
                              (attempt.state, attempt.reason, run_id))
            return
        checkpoint = cp.record(store, attempt.attempt_id)
        collection = store.result_collection(attempt.attempt_id)
        reason = None
        if not attempt.termination_evidence:
            reason = 'Awaiting confirmed process termination'
        elif not checkpoint or not checkpoint['acknowledged']:
            reason = 'Awaiting committed checkpoint acknowledgement'
            failure = cp.error(store, attempt.attempt_id)
            if failure:
                reason += ': ' + failure
        elif not collection or collection['status'] != 'collected':
            reason = 'Awaiting durable segment results'
        if reason:
            store._db.execute("UPDATE simulation_runs SET status='blocked',reason=? WHERE run_id=?", (reason, run_id))
            return
        # Reverify committed state, including already published input copies after
        # a database rollback. Corruption must not create runnable successor input.
        files = tuple(InputFile(**entry) for entry in checkpoint['manifest']['files'])
        publish_inputs(Path(checkpoint['directory']), files, lambda _: None, {})
        final = item['ordinal'] + 1 == len(run['snapshot']['segment_ends'])
        if not final:
            _enqueue(store, run, item['ordinal'] + 1, checkpoint)
        store._db.execute('UPDATE run_segments SET completed=1 WHERE run_id=? AND ordinal=?',
                           (run_id, item['ordinal']))
        store._db.execute('UPDATE simulation_runs SET committed_step=?,status=?,reason=NULL WHERE run_id=?',
                           (item['stop_step'], 'succeeded' if final else 'queued', run_id))
        if item['checkpoint_id']:
            cp.release(store, item['job_id'])
        store._db.execute('INSERT INTO run_events(run_id,description) VALUES(?,?)',
                           (run_id, f"Committed segment {item['ordinal']} at step {item['stop_step']}"))


def refresh(store: Store, config: ManagerConfig | None = None, *, now: float | None = None) -> None:
    """Reconcile durable boundaries after polling, transfer or manager restart."""
    reset_blocked = (reconcile_environments(store, config, now=time.time() if now is None else now)
                     if config is not None else set())
    for row in store._db.execute("SELECT run_id FROM simulation_runs WHERE status NOT IN ('succeeded','cancelled')").fetchall():
        if row['run_id'] in reset_blocked:
            continue
        try:
            advance(store, row['run_id'])
        except (OSError, ValueError) as exc:
            with store.scheduling_round():
                store._db.execute("UPDATE simulation_runs SET status='blocked',reason=? WHERE run_id=?",
                                  (f'Checkpoint/enqueue blocked: {exc}', row['run_id']))


def set_priorities(store: Store, run_id: str, *, initial: int | None = None,
                   continuation: int | None = None) -> None:
    """Change explicit scheduling policy without mutating any simulation snapshot."""
    _priority(initial)
    _priority(continuation)
    if initial is None and continuation is None:
        raise ValueError('Supply an initial or continuation priority')
    with store.scheduling_round():
        run = _run(store, run_id)
        if initial is not None:
            if run['first_priority'] is not None:
                raise StoreError('Initial priority already frozen at first dispatch')
            store._db.execute('UPDATE simulation_runs SET initial_override=? WHERE run_id=?', (initial, run_id))
        if continuation is not None:
            store._db.execute('UPDATE simulation_runs SET continuation_override=? WHERE run_id=?', (continuation, run_id))
        for item in store._db.execute('SELECT * FROM run_segments WHERE run_id=?', (run_id,)).fetchall():
            value = initial if item['ordinal'] == 0 else continuation
            if value is not None and store.job_state(item['job_id']) == 'queued':
                store.set_priority(item['job_id'], value, origin='run_priority_override')
        store._db.execute('INSERT INTO run_events(run_id,description) VALUES(?,?)',
            (run_id, f'Priority override: initial={initial}, continuation={continuation}'))


def cancel_run(store: Store, run_id: str, reason: str = 'Parent run cancellation requested') -> None:
    """Persist cancellation before asking any active process group to terminate."""
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 2048:
        raise ValueError('Invalid parent cancellation reason')
    with store.scheduling_round():
        run = _run(store, run_id)
        if run['status'] == 'succeeded':
            raise StoreError('Completed run cannot be cancelled')
        store._db.execute("UPDATE simulation_runs SET cancelled=1,status='cancelling',reason=? WHERE run_id=?", (reason, run_id))
        for item in store._db.execute('SELECT * FROM run_segments WHERE run_id=?', (run_id,)).fetchall():
            if store.job_state(item['job_id']) == 'queued':
                store._db.execute("UPDATE jobs SET state='cancelled' WHERE job_id=?", (item['job_id'],))
                store._event(item['job_id'], None, 'cancelled', reason)
            for attempt in store.job_attempts(item['job_id']):
                if attempt.state not in TERMINAL:
                    store.request_cancel(attempt.attempt_id, reason)
        store._db.execute('INSERT INTO run_events(run_id,description) VALUES(?,?)', (run_id, reason))
        _finish_cancel(store, _run(store, run_id))


def _finish_cancel(store: Store, run: dict) -> None:
    unresolved = store._db.execute('SELECT 1 FROM reservations r JOIN attempts a USING(attempt_id) '
        'JOIN run_segments s USING(job_id) WHERE s.run_id=?', (run['run_id'],)).fetchone()
    if not unresolved:
        store._db.execute("UPDATE simulation_runs SET status='cancelled' WHERE run_id=?", (run['run_id'],))


def inspect_run(store: Store, run_id: str, *, limit: int = 100, after: int = -1) -> dict:
    """Bounded parent monitoring; progress counts committed integration steps once."""
    if type(limit) is not int or not 1 <= limit <= 500 or type(after) is not int or after < -1:
        raise ValueError('Run inspection requires limit 1..500 and a nonnegative segment cursor')
    result = _run(store, run_id)
    result['environment'] = environment(store, run_id)
    binding = result['environment']
    snapshot = result['snapshot']
    result['execution_evidence'] = 'unbound'
    if binding['fingerprint']:
        try:
            result['execution_evidence'] = require_contract(Recipe(**snapshot['recipe']),
                build_fingerprint=binding['fingerprint'], total_steps=snapshot['total_steps'],
                segment_ends=tuple(snapshot['segment_ends']), version=snapshot.get('contract_version'),
                parameters=snapshot.get('model') if snapshot.get('contract_version') == PARAMETERIZED_VERSION else None).evidence
        except ValueError as exc:
            result['execution_evidence'] = 'not_assessed'
            result['execution_evidence_error'] = str(exc)
    result['history'] = [dict(row) for row in store._db.execute(
        'SELECT * FROM run_segment_history WHERE run_id=? ORDER BY generation DESC,ordinal LIMIT ?', (run_id, limit))]
    rows = store._db.execute('SELECT s.*,j.state FROM run_segments s JOIN jobs j USING(job_id) '
        'WHERE s.run_id=? AND s.ordinal>? ORDER BY ordinal LIMIT ?', (run_id, after, limit)).fetchall()
    result['segments'] = []
    for row in rows:
        attempts = store.job_attempts(row['job_id'], limit=1)
        checkpoint = cp.record(store, attempts[-1].attempt_id) if attempts else None
        result['segments'].append({**dict(row), 'duplicate_initial_boundary_frame': row['ordinal'] > 0,
            'checkpoint': ({'identity': checkpoint['identity'], 'acknowledged': bool(checkpoint['acknowledged']),
                'error': cp.error(store, attempts[-1].attempt_id)} if checkpoint else None)})
    result['output_policy'] = 'Segment trajectories retain both boundary copies; duplicate initial frames are explicitly marked by segment ordinal. Segments are one logical trajectory, never independent replicas.'
    result['committed_steps'] = result['committed_step'] - result['snapshot']['start_step']
    result['next_after'] = rows[-1]['ordinal'] if len(rows) == limit else None
    result['events'] = [dict(row) for row in store._db.execute('SELECT * FROM run_events WHERE run_id=? '
        'ORDER BY event_id DESC LIMIT ?', (run_id, limit))]
    return result


def retry_blocker(store: Store, job_id: str) -> str | None:
    """Cancelled parents and committed boundaries cannot be resurrected by retries."""
    if store._db.execute('SELECT 1 FROM run_segment_history WHERE job_id=?', (job_id,)).fetchone():
        return 'Retired continuation generation cannot be retried'
    item = segment(store, job_id)
    if item is not None and (environment(store, item['run_id'])['reset_pending'] or item['completed'] or _run(store, item['run_id'])['cancelled']):
        return 'Parent run cancelled or segment already committed'
    return None


def monitoring_view(store: Store, job_id: str) -> dict | None:
    """Compact T016 continuation fields without input, seed or environment contents."""
    item = segment(store, job_id)
    if item is None:
        return None
    run = _run(store, item['run_id'])
    attempts = store.job_attempts(job_id, limit=1)
    latest = attempts[-1] if attempts else None
    checkpoint = cp.record(store, latest.attempt_id) if latest else None
    collection = store.result_collection(latest.attempt_id, include_manifest=False) if latest else None
    ready = bool(latest and latest.state == 'succeeded' and latest.termination_evidence
                 and checkpoint and checkpoint['acknowledged']
                 and collection and collection['status'] == 'collected'
                 and not run['cancelled'] and not cp.error(store, latest.attempt_id)
                 and not (run['reason'] or '').startswith('Checkpoint/enqueue blocked:'))
    binding = environment(store, run['run_id'])
    return dict(run_id=run['run_id'], parent_state=run['status'], total_steps=run['snapshot']['total_steps'],
        environment_generation=binding['generation'], environment_missing_since=binding['missing_since'],
        environment_reset_pending=bool(binding['reset_pending']),
        start_step=run['snapshot']['start_step'], committed_step=run['committed_step'],
        committed_steps=run['committed_step'] - run['snapshot']['start_step'],
        segment=item['ordinal'], segment_start=item['start_step'], segment_stop=item['stop_step'],
        step_max=run['snapshot']['step_max'], mode=run['snapshot']['recipe']['mode'],
        checkpoint_identity=checkpoint['identity'] if checkpoint else None,
        checkpoint_step=checkpoint['contract']['global_step'] if checkpoint and checkpoint['acknowledged'] else None,
        checkpoint_acknowledged=bool(checkpoint and checkpoint['acknowledged']),
        checkpoint_error=cp.error(store, latest.attempt_id) if latest else None,
        next_segment_eligible=ready and not item['completed'] and item['ordinal'] + 1 < len(run['snapshot']['segment_ends']),
        blocked_reason=run['reason'], duplicate_initial_boundary_frame=item['ordinal'] > 0)


def observe_environment(store: Store, worker_id: str, boot_id: str,
                        fingerprint: str | None, now: float) -> None:
    """Persist an authenticated worker inventory; unavailable inventory clears it."""
    from .continuation import valid_fingerprint
    from uuid import UUID
    import math
    UUID(boot_id)
    if fingerprint is not None and not valid_fingerprint(fingerprint):
        raise ValueError('Invalid continuation environment fingerprint')
    if type(now) not in (int, float) or not math.isfinite(now) or now < 0:
        raise ValueError('Invalid environment observation time')
    store._db.execute('INSERT INTO continuation_environments VALUES(?,?,?,?) '
        'ON CONFLICT(worker_id) DO UPDATE SET boot_id=excluded.boot_id,'
        'fingerprint=excluded.fingerprint,observed_at=excluded.observed_at',
        (worker_id, boot_id, fingerprint, now))


def worker_environment(store: Store, worker_id: str) -> dict | None:
    """Return an inventoried environment only for the current authenticated boot."""
    row = store._db.execute('SELECT * FROM continuation_environments WHERE worker_id=?', (worker_id,)).fetchone()
    generations = store.generations(worker_id)
    if not row or not row['fingerprint'] or not generations or generations[-1]['boot_id'] != row['boot_id']:
        return None
    return dict(row)


def environment(store: Store, run_id: str) -> dict:
    """Read the runtime binding separately from immutable submission intent."""
    row = store._db.execute('SELECT * FROM run_environments WHERE run_id=?', (run_id,)).fetchone()
    return dict(row) if row else dict(run_id=run_id, generation=0, worker_id=None,
        boot_id=None, fingerprint=None, missing_since=None, reset_pending=0)


def bind_assignment(store: Store, run_id: str, attempt_id: str) -> dict:
    """Bind the first assignment, or enforce the original worker/build/boot atomically."""
    attempt = store.attempt(attempt_id)
    observed = worker_environment(store, attempt.worker_id)
    binding = environment(store, run_id)
    if not observed or binding['reset_pending']:
        raise StoreError('Continuation environment unavailable or resetting')
    if binding['fingerprint']:
        if any(binding[key] != observed[key] for key in ('worker_id', 'boot_id', 'fingerprint')):
            raise StoreError('Assignment differs from bound continuation environment')
    else:
        # Existing chains must not silently adopt a new environment during upgrade.
        previous = store._db.execute('SELECT c.contract,a.worker_id FROM checkpoints c '
            'JOIN attempts a USING(attempt_id) JOIN run_segments s ON s.job_id=a.job_id '
            'WHERE s.run_id=? ORDER BY s.ordinal LIMIT 1', (run_id,)).fetchone()
        if previous:
            contract = json.loads(previous['contract'])
            if (previous['worker_id'] != observed['worker_id'] or contract['execution_boot_id'] != observed['boot_id']
                    or contract['build_fingerprint'] != observed['fingerprint']):
                raise StoreError('Existing continuation chain differs from observed environment')
        store._db.execute('INSERT INTO run_environments(run_id,generation,worker_id,boot_id,fingerprint) '
            'VALUES(?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET worker_id=excluded.worker_id,'
            'boot_id=excluded.boot_id,fingerprint=excluded.fingerprint,missing_since=NULL',
            (run_id, binding['generation'], observed['worker_id'], observed['boot_id'], observed['fingerprint']))
        store._db.execute('INSERT INTO run_events(run_id,description) VALUES(?,?)',
            (run_id, 'Environment bound: ' + cp.canonical(observed)))
    return environment(store, run_id)


def reconcile_environments(store: Store, config: ManagerConfig, *, now: float) -> set[str]:
    """Fence expired generations and restart only after positive process termination."""
    bindings = store._db.execute('SELECT e.run_id,e.generation FROM run_environments e JOIN simulation_runs r USING(run_id) '
        "WHERE e.fingerprint IS NOT NULL AND r.cancelled=0 AND r.status NOT IN ('succeeded','cancelled')").fetchall()
    blocked = set()
    for row in bindings:
        try:
            with store.scheduling_round():
                # Candidate discovery holds no write lock. Another Store may
                # reset, cancel or finish the run before this transaction starts.
                current = store._db.execute('SELECT e.* FROM run_environments e '
                    'JOIN simulation_runs r USING(run_id) WHERE e.run_id=? AND e.generation=? '
                    "AND e.fingerprint IS NOT NULL AND r.cancelled=0 AND r.status NOT IN ('succeeded','cancelled')",
                    (row['run_id'], row['generation'])).fetchone()
                if current is not None:
                    _reconcile_environment(store, config, dict(current), now)
        except (OSError, ValueError) as exc:
            blocked.add(row['run_id'])
            with store.scheduling_round():
                store._db.execute("UPDATE simulation_runs SET status='blocked',reason=? WHERE run_id=? "
                    "AND cancelled=0 AND status NOT IN ('succeeded','cancelled') AND EXISTS "
                    '(SELECT 1 FROM run_environments e WHERE e.run_id=simulation_runs.run_id '
                    'AND e.generation=? AND e.fingerprint IS NOT NULL)',
                    (f'Environment reset blocked: {exc}', row['run_id'], row['generation']))
    return blocked


def _reconcile_environment(store: Store, config: ManagerConfig, binding: dict, now: float) -> None:
    """One generation reset commits atomically, including new input publication."""
    run_id = binding['run_id']
    observed = worker_environment(store, binding['worker_id'])
    available = (any(w.id == binding['worker_id'] and w.enabled for w in config.workers)
        and observed is not None and now - observed['observed_at'] <= config.manager.heartbeat_timeout_seconds
        and all(binding[key] == observed[key] for key in ('worker_id', 'boot_id', 'fingerprint')))
    expired = (binding['missing_since'] is not None
               and now - binding['missing_since'] >= config.segmentation.environment_reset_seconds)
    if available and not binding['reset_pending'] and not expired:
        store._db.execute('UPDATE run_environments SET missing_since=NULL WHERE run_id=?', (run_id,))
        return
    since = binding['missing_since']
    if since is None:
        # Start at the last inventory's expiry for disconnections, but at
        # detection time for a changed build/boot or administratively removed host.
        same = observed and all(binding[key] == observed[key] for key in ('worker_id', 'boot_id', 'fingerprint'))
        since = min(now, observed['observed_at'] + config.manager.heartbeat_timeout_seconds) if same else now
        store._db.execute('UPDATE run_environments SET missing_since=? WHERE run_id=?', (since, run_id))
    if not binding['reset_pending'] and now - since < config.segmentation.environment_reset_seconds:
        return
    store._db.execute('UPDATE run_environments SET reset_pending=1 WHERE run_id=?', (run_id,))
    reason = 'Bound environment expired; awaiting confirmed termination before full restart'
    store._db.execute("UPDATE simulation_runs SET status='resetting',reason=? WHERE run_id=?", (reason, run_id))
    items = store._db.execute('SELECT * FROM run_segments WHERE run_id=?', (run_id,)).fetchall()
    unresolved = False
    for item in items:
        if store.job_state(item['job_id']) == 'queued':
            store._db.execute("UPDATE jobs SET state='cancelled' WHERE job_id=?", (item['job_id'],))
            store._event(item['job_id'], None, 'cancelled', reason)
        for attempt in store.job_attempts(item['job_id']):
            if attempt.state not in TERMINAL:
                store.request_cancel(attempt.attempt_id, reason)
            if not attempt.termination_evidence:
                unresolved = True
    reservations = store._db.execute('SELECT 1 FROM reservations r JOIN attempts a USING(attempt_id) '
        'JOIN run_segments s USING(job_id) WHERE s.run_id=?', (run_id,)).fetchone()
    if unresolved or reservations:
        return
    store._db.execute('INSERT INTO run_segment_history SELECT run_id,?,ordinal,job_id,start_step,'
        'stop_step,checkpoint_id,completed FROM run_segments WHERE run_id=?', (binding['generation'], run_id))
    store._db.execute('UPDATE checkpoint_consumers SET released=1 WHERE consumer_job IN '
        '(SELECT job_id FROM run_segments WHERE run_id=?)', (run_id,))
    store._db.execute('DELETE FROM run_segments WHERE run_id=?', (run_id,))
    store._db.execute('UPDATE run_environments SET generation=generation+1,worker_id=NULL,boot_id=NULL,'
        'fingerprint=NULL,missing_since=NULL,reset_pending=0 WHERE run_id=?', (run_id,))
    store._db.execute("UPDATE simulation_runs SET committed_step=0,status='queued',reason=NULL WHERE run_id=?", (run_id,))
    _enqueue(store, _run(store, run_id), 0, None)
    store._db.execute('INSERT INTO run_events(run_id,description) VALUES(?,?)',
        (run_id, f"Environment timeout: retired generation {binding['generation']}; restarted from step zero"))
