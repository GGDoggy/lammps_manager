"""Immutable continuation checkpoints, independent of process/result completion.

Only manager-registered contracts may receive uploads. Publication precedes the
SQLite acknowledgement; replay reconciles an already published tree. Manager
copies are retained indefinitely: no checkpoint deletion API is provided.
"""

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
from typing import BinaryIO

from .continuation import Recipe, require_validated, abp_recipe_digest
from .models import InputFile, Resources
from .staging import open_regular, publish_inputs, safe_path, validate_manifest
from .store import Store, StoreError, TERMINAL


@dataclass(frozen=True)
class CheckpointContract:
    """Manager-owned provenance for one safe integration boundary."""

    run_id: str
    segment: int
    attempt_id: str
    job_id: str
    recipe: Recipe
    build_fingerprint: str
    total_steps: int
    segment_ends: tuple[int, ...]
    global_step: int
    global_time: float
    stream_provenance: str
    required_state: tuple[str, ...] = ('checkpoint.restart',)
    resources: Resources = Resources(1, 1)
    recipe_sha256: str = abp_recipe_digest()
    parent_checkpoint_id: str | None = None
    execution_boot_id: str | None = None


def canonical(value: object) -> str:
    """Stable finite JSON for content identities and immutable replay checks."""
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def decode_contract(value: dict) -> CheckpointContract:
    """Decode the wire representation without weakening strict type validation."""
    return CheckpointContract(**{**value, 'recipe': Recipe(**value['recipe']),
        'segment_ends': tuple(value['segment_ends']),
        'required_state': tuple(value['required_state']),
        'resources': Resources(**value['resources'])})


def validate_contract(contract: CheckpointContract) -> None:
    """Enforce the released state/layout/clock contract, never a peer's claim."""
    if not isinstance(contract, CheckpointContract):
        raise ValueError('Expected checkpoint contract')
    for name in ('run_id', 'attempt_id', 'job_id'):
        value = getattr(contract, name)
        if len(safe_path(value).parts) != 1 or len(value) > 256:
            raise ValueError('Invalid checkpoint identity')
    scope = require_validated(contract.recipe, build_fingerprint=contract.build_fingerprint,
                              total_steps=contract.total_steps, segment_ends=contract.segment_ends)
    if (type(contract.segment) is not int or not 0 <= contract.segment < len(scope.segment_ends)
            or type(contract.global_step) is not int
            or contract.global_step != scope.segment_ends[contract.segment]
            or type(contract.global_time) not in (int, float)
            or contract.global_time != contract.global_step * 0.005):
        raise ValueError('Checkpoint boundary/clock mismatch')
    # This released recipe has no external/fix/RNG state file. RNG renewal is
    # explicit provenance, never a claim that the restart serializes RNG state.
    if (contract.recipe_sha256 != scope.recipe_sha256
            or not isinstance(contract.resources, Resources)
            or contract.resources.mpi_ranks != contract.recipe.mpi_ranks
            or contract.resources.omp_threads_per_rank != contract.recipe.omp_threads):
        raise ValueError('Checkpoint recipe/resource provenance mismatch')
    if contract.execution_boot_id is not None:
        from uuid import UUID
        if not isinstance(contract.execution_boot_id, str):
            raise ValueError('Invalid execution boot identity')
        UUID(contract.execution_boot_id)
    if contract.parent_checkpoint_id is not None:
        import re
        if not isinstance(contract.parent_checkpoint_id, str) or not re.fullmatch('[0-9a-f]{64}', contract.parent_checkpoint_id):
            raise ValueError('Invalid parent checkpoint identity')
    if contract.required_state != ('checkpoint.restart',):
        raise ValueError('Unvalidated checkpoint state set')
    if (not isinstance(contract.stream_provenance, str)
            or not 0 < len(contract.stream_provenance) <= 16384):
        raise ValueError('Missing bounded stream provenance')
    provenance = json.loads(contract.stream_provenance)
    if (not isinstance(provenance, dict) or set(provenance) != {'namespace', 'phase', 'replica', 'seeds'}
            or not isinstance(provenance['namespace'], str) or not provenance['namespace']
            or not isinstance(provenance['phase'], str) or not provenance['phase']
            or type(provenance['replica']) is not int or provenance['replica'] < 0):
        raise ValueError('Invalid stream provenance')
    seeds = provenance['seeds']
    starts = (0, *scope.segment_ends[:-1])
    identities = [('setup', 'positions'), ('setup', 'orientations')]
    identities += [('segmented', str(start)) for start in starts]
    derived = [int.from_bytes(hashlib.sha256(json.dumps([
        provenance['namespace'], provenance['phase'], arm, provenance['replica'], segment
    ]).encode()).digest()[:8], 'big') % 899999999 + 1
        for arm, segment in identities]
    if (not isinstance(seeds, list) or any(type(seed) is not int for seed in seeds)
            or seeds != derived or len(set(seeds)) != len(seeds)):
        raise ValueError('Stream seeds do not match derivation or collide')


def initialize(store: Store) -> None:
    """Install additive checkpoint state without rewriting submission snapshots."""
    store._db.execute('CREATE TABLE IF NOT EXISTS checkpoints ('
        'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id), '
        'run_id TEXT NOT NULL, segment INTEGER NOT NULL, contract TEXT NOT NULL, '
        'manifest TEXT, identity TEXT UNIQUE, directory TEXT, acknowledged INTEGER NOT NULL DEFAULT 0)')
    store._db.execute('CREATE INDEX IF NOT EXISTS checkpoints_run ON checkpoints(run_id,segment)')
    store._db.execute('CREATE TABLE IF NOT EXISTS checkpoint_errors ('
        'attempt_id TEXT PRIMARY KEY REFERENCES attempts(attempt_id),reason TEXT NOT NULL)')
    store._db.execute('CREATE TABLE IF NOT EXISTS checkpoint_consumers ('
        'consumer_job TEXT PRIMARY KEY REFERENCES jobs(job_id), '
        'checkpoint_id TEXT NOT NULL REFERENCES checkpoints(identity), released INTEGER NOT NULL DEFAULT 0)')


def record(store: Store, attempt_id: str) -> dict | None:
    """Read bounded metadata; a published directory alone is never an ack."""
    row = store._db.execute('SELECT * FROM checkpoints WHERE attempt_id=?', (attempt_id,)).fetchone()
    if row is None:
        return None
    result = dict(row)
    result['contract'] = json.loads(result['contract'])
    result['manifest'] = json.loads(result['manifest']) if result['manifest'] else None
    return result


def expect(store: Store, contract: CheckpointContract) -> None:
    """Register a contract before launch using manager-owned run metadata."""
    validate_contract(contract)
    with store.scheduling_round():
        attempt = store.attempt(contract.attempt_id)
        spec = store.specification(attempt.job_id)
        if (attempt.job_id != contract.job_id or attempt.state != 'assigned'
                or spec.resources != contract.resources
                or spec.resources.mpi_ranks != contract.recipe.mpi_ranks
                or spec.resources.omp_threads_per_rank != contract.recipe.omp_threads):
            raise StoreError('Checkpoint contract differs from assignment/layout')
        old = record(store, attempt.attempt_id)
        if old:
            if canonical(old['contract']) != canonical(asdict(contract)):
                raise StoreError('Conflicting checkpoint contract')
            return
        store._db.execute('INSERT INTO checkpoints(attempt_id,run_id,segment,contract) VALUES(?,?,?,?)',
            (attempt.attempt_id, contract.run_id, contract.segment, canonical(asdict(contract))))


def authorize(store: Store, worker_id: str, attempt_id: str) -> dict:
    """Late data from obsolete attempts never acquires a runnable identity."""
    item = record(store, attempt_id)
    attempt = store.attempt(attempt_id)
    if item is None or attempt.worker_id != worker_id:
        raise StoreError('Checkpoint ownership mismatch or missing contract')
    if (attempt.state not in TERMINAL or not attempt.termination_evidence
            or attempt.state != 'succeeded' or attempt.exit_code != 0):
        raise StoreError('Checkpoint requires successful confirmed termination')
    if store.job_attempts(attempt.job_id)[-1].attempt_id != attempt_id:
        raise StoreError('Obsolete checkpoint attempt')
    return item


def bind_manifest(store: Store, worker_id: str, attempt_id: str, manifest: dict, limits: dict) -> dict:
    """Bind one complete immutable checkpoint including its contract and files."""
    if not isinstance(manifest, dict) or set(manifest) != {'contract', 'files'}:
        raise ValueError('Invalid checkpoint manifest')
    contract = decode_contract(manifest['contract'])
    validate_contract(contract)
    files = tuple(InputFile(**value) for value in manifest['files'])
    validate_manifest(files, **limits)
    if ({file.path for file in files} != set(contract.required_state)
            or any(file.size <= 0 for file in files)):
        raise ValueError('Incomplete checkpoint state set')
    with store.scheduling_round():
        item = authorize(store, worker_id, attempt_id)
        if canonical(manifest['contract']) != canonical(item['contract']):
            raise StoreError('Checkpoint provenance mismatch')
        if item['manifest'] is not None and canonical(manifest) != canonical(item['manifest']):
            raise StoreError('Conflicting checkpoint manifest')
        digest = hashlib.sha256(canonical(manifest).encode()).hexdigest()
        store._db.execute('UPDATE checkpoints SET manifest=?,identity=? WHERE attempt_id=?',
                          (canonical(manifest), digest, attempt_id))
    return record(store, attempt_id)


def destination(store: Store, attempt_id: str) -> Path:
    """Select a private manager destination, never one supplied by a worker."""
    attempt = store.attempt(attempt_id)
    root = Path(store.specification(attempt.job_id).work_directory)
    path = root / '.checkpoints' / attempt_id
    if any(parent.is_symlink() or parent.resolve() != parent for parent in (root, path.parent, path)):
        raise ValueError('Unsafe checkpoint destination')
    return path


def operation(store: Store, worker_id: str, attempt_id: str, action: str,
              limits: dict, *, payload: dict | None = None, stream: BinaryIO | None = None,
              length: int | None = None, shared_source: Path | None = None) -> dict:
    """Upload or publish shared state, acknowledging only after filesystem + DB commit."""
    from .results import upload_file, commit_results
    item = authorize(store, worker_id, attempt_id)
    if action == 'manifest':
        item = bind_manifest(store, worker_id, attempt_id, payload, limits)
    if item['manifest'] is None:
        raise ValueError('Checkpoint manifest required')
    files = tuple(InputFile(**entry) for entry in item['manifest']['files'])
    validate_manifest(files, **limits)
    path = destination(store, attempt_id)
    path.mkdir(parents=True, exist_ok=True)
    if action == 'commit':
        if shared_source is None:
            commit_results(path, files, limits)
        else:
            # The caller obtains this path from the manager-owned assignment.
            publish_inputs(path / 'results', files,
                lambda index: open_regular(shared_source, files[index].path), limits)
        with store.scheduling_round():
            current = authorize(store, worker_id, attempt_id)
            if current['identity'] != item['identity']:
                raise StoreError('Checkpoint changed during publication')
            store._db.execute('UPDATE checkpoints SET directory=?,acknowledged=1 WHERE attempt_id=?',
                              (str(path / 'results'), attempt_id))
    elif action != 'manifest':
        index = int(action)
        if not 0 <= index < len(files) or length != files[index].size:
            raise ValueError('Invalid checkpoint file index/size')
        if item['acknowledged']:
            raise ValueError('Checkpoint committed; replay commit')
        upload_file(path, files[index], stream)
    saved = record(store, attempt_id)
    return {'identity': saved['identity'], 'acknowledged': bool(saved['acknowledged'])}


def pin(store: Store, checkpoint_id: str, consumer_job: str) -> None:
    """Retain dependency before successor dispatch; parent copies are never pruned."""
    with store.scheduling_round():
        store.specification(consumer_job)
        row = store._db.execute('SELECT * FROM checkpoints WHERE identity=? AND acknowledged=1',
                                 (checkpoint_id,)).fetchone()
        if row is None:
            raise StoreError('Checkpoint is not durably acknowledged')
        old = store._db.execute('SELECT * FROM checkpoint_consumers WHERE consumer_job=?',
                               (consumer_job,)).fetchone()
        if old and old['checkpoint_id'] != checkpoint_id:
            raise StoreError('Conflicting checkpoint dependency')
        store._db.execute('INSERT OR IGNORE INTO checkpoint_consumers VALUES(?,?,0)',
                           (consumer_job, checkpoint_id))


def fetch(store: Store, worker_id: str, consumer_attempt: str, index: int,
          build_fingerprint: str) -> tuple[BinaryIO, InputFile]:
    """Fetch only for an owned successor and an actually compatible environment.

    Worker launch must independently inventory its executable, revalidate the
    contract, and verify the complete download before consuming it.
    """
    attempt = store.attempt(consumer_attempt)
    if (attempt.worker_id != worker_id or attempt.state != 'assigned'
            or store.cancellation(consumer_attempt) or store.rejection(consumer_attempt)):
        raise StoreError('Checkpoint consumer is not an owned pending assignment')
    row = store._db.execute('SELECT c.* FROM checkpoints c JOIN checkpoint_consumers d '
        'ON d.checkpoint_id=c.identity WHERE d.consumer_job=? AND d.released=0 AND c.acknowledged=1',
        (attempt.job_id,)).fetchone()
    if row is None:
        raise StoreError('Missing committed checkpoint dependency')
    contract = decode_contract(json.loads(row['contract']))
    validate_contract(contract)
    spec = store.specification(attempt.job_id)
    # Only the pinned host boot can support a different worker identity. A
    # claimed fingerprint never extends this to another physical host.
    if (not compatible_worker(store, row['attempt_id'], worker_id) or build_fingerprint != contract.build_fingerprint
            or spec.resources.mpi_ranks != contract.recipe.mpi_ranks
            or spec.resources.omp_threads_per_rank != contract.recipe.omp_threads):
        raise StoreError('Unvalidated checkpoint environment or worker reassignment')
    files = tuple(InputFile(**entry) for entry in json.loads(row['manifest'])['files'])
    if type(index) is not int or not 0 <= index < len(files):
        raise ValueError('Invalid checkpoint file index')
    # Reverify the entire tree on every fetch: disk corruption must fail closed.
    directory = Path(row['directory'])
    publish_inputs(directory, files, lambda _: None, {})
    return open_regular(directory, files[index].path), files[index]


def release(store: Store, consumer_job: str) -> None:
    """Release a dependency only after all consumer processes conclusively end."""
    with store.scheduling_round():
        attempts = store.job_attempts(consumer_job)
        if (store.job_state(consumer_job) not in TERMINAL or not attempts
                or any(not a.termination_evidence for a in attempts)
                or any(r.attempt_id in {a.attempt_id for a in attempts} for r in store.reservations())):
            raise StoreError('Checkpoint dependency still needed')
        store._db.execute('UPDATE checkpoint_consumers SET released=1 WHERE consumer_job=?', (consumer_job,))


def prepare_launch(store: Store, worker: object, attempt_id: str, work: Path) -> None:
    """Inventory the actual executable and reject edited continuation inputs."""
    from .continuation import fingerprint_build, render_abp_input
    item = record(store, attempt_id)
    if item is None:
        return
    contract = decode_contract(item['contract'])
    validate_contract(contract)
    if contract.execution_boot_id is not None and Path('/proc/sys/kernel/random/boot_id').read_text().strip() != contract.execution_boot_id:
        raise ValueError('Continuation execution host boot identity changed')
    if (worker.execution.lammps_args or worker.execution.environment
            or fingerprint_build(Path(worker.execution.lammps_path)) != contract.build_fingerprint):
        raise ValueError('Continuation execution environment is unvalidated')
    seeds = json.loads(contract.stream_provenance)['seeds']
    start = 0 if contract.segment == 0 else contract.segment_ends[contract.segment - 1]
    expected = render_abp_input(start=start, stop=contract.global_step, total=contract.total_steps,
                               stream_seed=seeds[2 + contract.segment],
                               position_seed=seeds[0], orientation_seed=seeds[1])
    spec = store.specification(contract.job_id)
    with open_regular(work, spec.input_file) as source:
        if source.read(len(expected.encode()) + 1) != expected.encode():
            raise ValueError('Continuation input differs from the released renderer')
    if start:
        restart = next((f for f in spec.input_manifest if f.path == 'checkpoint.restart'), None)
        if restart is None:
            raise ValueError('Continuation input lacks immutable checkpoint identity')
        # Shared jobs also carry a complete continuation input manifest.
        publish_inputs(work, spec.input_manifest, lambda _: None, {})


def validate_boundary(directory: Path, contract_data: dict) -> None:
    """Check fixed-renderer completion and clock before publishing safe-step state.

    This checks the emitted thermo boundary, not arbitrary binary restart formats.
    The fixed renderer writes its restart after the run; restoration is separately
    exercised with the actual build by the explicit scientific driver.
    """
    import math
    contract = decode_contract(contract_data)
    validate_contract(contract)
    observed = False
    with open_regular(directory, 'stdout') as source:
        thermo = False
        for raw in iter(lambda: source.readline(65537), b''):
            if len(raw) > 65536:
                raise ValueError('Oversized checkpoint thermo line')
            words = raw.decode('utf-8', errors='replace').split()
            if words == ['Step', 'Time', 'Atoms']:
                thermo = True
                continue
            if thermo and len(words) == 3:
                try:
                    step, elapsed, atoms = int(words[0]), float(words[1]), int(words[2])
                except ValueError:
                    thermo = False
                    continue
                if step == contract.global_step:
                    if atoms != 1024 or not math.isclose(elapsed, contract.global_time, rel_tol=1e-14, abs_tol=1e-14):
                        raise ValueError('Checkpoint thermo state/clock mismatch')
                    observed = True
            elif thermo:
                thermo = False
    if not observed:
        raise ValueError('Missing safe-step checkpoint completion evidence')


def compatible_worker(store: Store, parent_attempt: str, worker_id: str) -> bool:
    """Require every authenticated consumer to report the pinned host boot.

    Cross-host continuation remains unreleased. Every launcher still inventories
    the actual executable/library/CPU identity and enforces the singleton layout.
    """
    store.attempt(parent_attempt)
    checkpoint = record(store, parent_attempt)
    boot = checkpoint['contract'].get('execution_boot_id') if checkpoint else None
    generations = store.generations(worker_id)
    return bool(boot and generations and generations[-1]['boot_id'] == boot)


def error(store: Store, attempt_id: str) -> str | None:
    """Read the latest checkpoint failure independently of process/result state."""
    row = store._db.execute('SELECT reason FROM checkpoint_errors WHERE attempt_id=?', (attempt_id,)).fetchone()
    return row['reason'] if row else None


def set_error(store: Store, attempt_id: str, reason: str | None) -> None:
    """Persist actionable publication/transfer diagnostics without changing progress."""
    if reason is not None and (not isinstance(reason, str) or not 0 < len(reason) <= 2048):
        raise ValueError('Invalid checkpoint error')
    with store.scheduling_round():
        if record(store, attempt_id) is None:
            raise StoreError('Checkpoint contract required for diagnostics')
        if reason is None:
            store._db.execute('DELETE FROM checkpoint_errors WHERE attempt_id=?', (attempt_id,))
        else:
            store._db.execute('INSERT INTO checkpoint_errors VALUES(?,?) ON CONFLICT(attempt_id) '
                              'DO UPDATE SET reason=excluded.reason', (attempt_id, reason))
