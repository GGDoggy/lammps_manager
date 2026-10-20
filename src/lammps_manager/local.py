"""Batch submission, isolated attempt execution and durable inspection."""

from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from threading import Event
from datetime import datetime, timezone
from uuid import uuid4

from .config import ManagerConfig, WorkerConfig, create_job_specification
from .store import Attempt, Store


def submit_batch(store: Store, config: ManagerConfig, manifest: str | Path) -> tuple[str, ...]:
    """Copy manifest source trees into fresh job directories and submit snapshots."""
    manifest = Path(manifest).resolve()
    entries = json.loads(manifest.read_text())
    if not isinstance(entries, list) or not entries:
        raise ValueError('Batch manifest must be a nonempty list')
    prepared = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) - {'source_directory', 'input_file', 'resources'}:
            raise ValueError('Each job accepts source_directory, input_file and optional resources')
        source_value = entry.get('source_directory')
        input_value = entry.get('input_file')
        if not isinstance(source_value, str) or not isinstance(input_value, str):
            raise ValueError('source_directory and input_file must be strings')
        source = manifest.parent / source_value
        if source.is_symlink():
            raise ValueError('Source root must not be a symlink')
        source = source.resolve()
        relative = Path(input_value)
        if relative.is_absolute() or '..' in relative.parts or relative == Path('.'):
            raise ValueError('input_file must be a relative file inside source_directory')
        if not source.is_dir() or not (source / relative).is_file():
            raise ValueError(f'Missing source directory or input file: {source / relative}')
        if any(p.is_symlink() or not (p.is_file() or p.is_dir()) for p in source.rglob('*')):
            raise ValueError('Source trees must contain only regular files and directories, without symlinks')
        job_id = uuid4().hex
        directory = Path(config.storage.work_root).resolve() / job_id
        # Validate all entries before creating output or submitting any jobs.
        spec = create_job_specification(config, job_id=job_id, work_directory=str(directory),
                input_file=str(relative), input_sha256=('0' * 64 if config.storage.mode == 'staged'
                    else hashlib.sha256((source / relative).read_bytes()).hexdigest()),
                resource_overrides=entry.get('resources'))
        if directory == source or source in directory.parents:
            raise ValueError('Storage work_root must not be inside a source tree')
        prepared.append((source, directory, spec))
    submitted = []
    for source, directory, spec in prepared:
        directory.mkdir(parents=True, exist_ok=False)
        if config.storage.mode == 'staged':
            from .staging import snapshot_inputs, sync_directory
            limits = dict(max_files=config.storage.max_input_files,
                          max_file_bytes=config.storage.max_input_file_bytes,
                          max_total_bytes=config.storage.max_input_total_bytes)
            files = snapshot_inputs(source, directory / 'inputs', limits)
            sync_directory(directory)
            sync_directory(directory.parent)
            primary = next((item for item in files if item.path == spec.input_file), None)
            if primary is None:
                raise ValueError('Primary input disappeared during submission')
            spec = replace(spec, storage_mode='staged', input_manifest=files,
                           input_sha256=primary.sha256)
        else:
            shutil.copytree(source, directory / 'inputs')
        if spec.storage_mode == 'shared' and hashlib.sha256(
                (directory / 'inputs' / spec.input_file).read_bytes()).hexdigest() != spec.input_sha256:
            raise ValueError(f'Input changed during submission: {spec.job_id}')
        store.submit(spec)
        submitted.append(spec.job_id)
    return tuple(submitted)


def run_local(store: Store, worker: WorkerConfig) -> tuple[str, ...]:
    """Execute queued snapshots once, sequentially, retaining uncertain reservations."""
    from .recovery import reconcile
    while True:
        monitoring = False
        for reservation in store.reservations(worker.worker.id):
            previous = store.attempt(reservation.attempt_id)
            if reconcile(store, previous):
                monitoring = True
        if not monitoring:
            break
        Event().wait(.05)
    if store.reservations():
        for row in store.jobs():
            if row['state'] == 'queued' and store.specification(row['job_id']).storage_mode == 'staged':
                store.record_waiting(row['job_id'], 'Staged execution requires the polling worker and result collection')
        return ()
    completed = []
    for row in store.jobs():
        if row['state'] != 'queued':
            continue
        spec = store.specification(row['job_id'])
        if spec.storage_mode == 'staged':
            store.record_waiting(spec.job_id, 'Staged execution requires the polling worker and result collection')
            continue
        root = Path(spec.work_directory).resolve()
        if not root.is_relative_to(Path(worker.work_root).resolve()):
            raise ValueError(f'Job outside worker work_root: {root}')
        attempt_id = uuid4().hex
        directory = root / attempt_id
        attempt = store.assign(spec.job_id, attempt_id, worker.worker.id, str(directory))
        if execute_attempt(store, worker, attempt):
            completed.append(spec.job_id)
        elif store.attempt(attempt_id).state == 'unknown':
            break
    return tuple(completed)


def execute_attempt(store: Store, worker: WorkerConfig, attempt: 'Attempt') -> bool:
    """Execute one reserved attempt; callers must durably prevent duplicate starts."""
    if attempt.state not in {'assigned', 'cancelling'}:
        raise ValueError('Only a newly accepted assigned attempt can execute')
    spec = store.specification(attempt.job_id)
    staged = spec.storage_mode == 'staged'
    if staged and (not store.launch_claimed(attempt.attempt_id)
                   or not store.input_transfer(attempt.attempt_id)
                   or store.input_transfer(attempt.attempt_id)['status'] != 'ready'):
        raise ValueError('Staged execution requires verified inputs and a durable launch claim')
    root = Path(attempt.directory) if staged else Path(spec.work_directory).resolve()
    directory = Path(attempt.directory)
    process = None
    try:
        collision = (directory / 'work').exists() if staged else directory.exists()
        if collision:
            store.transition(attempt.attempt_id, 'unknown', reason='Attempt directory already exists')
            return False
        if store.cancellation(attempt.attempt_id):
            store.transition(attempt.attempt_id, 'cancelled',
                             reason=store.cancellation(attempt.attempt_id),
                             termination_evidence='Launch did not start a process')
            return False
        directory.mkdir(exist_ok=staged)
        if staged:
            from .staging import publish_inputs, open_regular
            limits = dict(max_files=worker.max_input_files, max_file_bytes=worker.max_input_file_bytes,
                          max_total_bytes=worker.max_input_total_bytes)
            publish_inputs(directory / 'work', spec.input_manifest,
                           lambda index: open_regular(root / 'inputs', spec.input_manifest[index].path), limits)
        else:
            shutil.copytree(root / 'inputs', directory / 'work')
        work = directory / 'work'
        if hashlib.sha256((work / spec.input_file).read_bytes()).hexdigest() != spec.input_sha256:
            raise ValueError('Input SHA-256 differs from submitted snapshot')
        args = [worker.execution.mpi_launcher_path, *worker.execution.mpi_args,
                '-np', str(spec.resources.mpi_ranks), worker.execution.lammps_path,
                *worker.execution.lammps_args, '-in', spec.input_file]
        environment = dict(worker.execution.environment)
        environment['OMP_NUM_THREADS'] = str(spec.resources.omp_threads_per_rank)
        (directory / 'execution.json').write_text(json.dumps({
            'argv': args, 'cwd': str(work), 'environment': environment,
            'stdout': str(directory / 'stdout'), 'stderr': str(directory / 'stderr'),
        }, indent=2) + '\n')
        with (directory / 'stdout').open('xb') as stdout, (directory / 'stderr').open('xb') as stderr:
            launched_at = datetime.now(timezone.utc).isoformat()
            process = subprocess.Popen(args, cwd=work, env={**os.environ, **environment},
                                       stdout=stdout, stderr=stderr, start_new_session=True)
            from .recovery import process_identity
            identity = {**process_identity(process.pid), 'launched_at': launched_at}
            with (directory / 'process.json').open('x') as metadata:
                json.dump(identity, metadata)
                metadata.flush()
                os.fsync(metadata.fileno())
            from .staging import sync_directory
            sync_directory(directory)
            if store.attempt(attempt.attempt_id).state != 'cancelling':
                store.transition(attempt.attempt_id, 'running')
            deadline = time.monotonic() + spec.resources.time_limit_seconds
            while process.poll() is None:
                if time.monotonic() >= deadline and not store.cancellation(attempt.attempt_id):
                    store.request_cancel(attempt.attempt_id, 'Time limit exceeded')
                if store.cancellation(attempt.attempt_id):
                    from .recovery import terminate_group
                    terminate_group(process.pid)
                    process.wait(timeout=2)
                    break
                Event().wait(.02)
            code = process.wait()
        # A launcher exit alone is insufficient if descendants remain alive.
        try:
            from .recovery import session_members
            if store.cancellation(attempt.attempt_id) and not session_members(process.pid):
                raise ProcessLookupError
            os.killpg(process.pid, 0)
            if session_members(process.pid):
                # A different MPI rank group may outlive its launcher group.
                raise ValueError('Launch session still contains live processes')
        except ProcessLookupError:
            from .recovery import session_members
            if session_members(process.pid):
                store.transition(attempt.attempt_id, 'unknown', reason='MPI session remains alive')
                return False
            cancel = store.cancellation(attempt.attempt_id)
            store.transition(attempt.attempt_id, 'cancelled' if cancel else ('succeeded' if code == 0 else 'failed'),
                exit_code=code, reason=cancel or (None if code == 0 else f'Process exited with code {code}'),
                termination_evidence='wait() reaped launcher; no live launch-session processes remain')
            return True
        else:
            store.transition(attempt.attempt_id, 'unknown', reason='Launcher exited but process group remains')
            return False
    except BaseException as exc:
        if process is None and isinstance(exc, FileExistsError):
            store.transition(attempt.attempt_id, 'unknown', reason='Attempt path collision; launch identity uncertain')
        elif process is None:
            store.transition(attempt.attempt_id, 'failed', reason=str(exc) or type(exc).__name__,
                             termination_evidence='Launch did not start a process')
        else:
            store.transition(attempt.attempt_id, 'unknown', reason=str(exc) or type(exc).__name__)
        if not isinstance(exc, (OSError, ValueError)):
            raise
        if process is not None:
            return False
    return False


def inspect_job(store: Store, job_id: str) -> dict[str, object]:
    """Return durable specification, state, attempts and history for inspection."""
    spec = store.specification(job_id)
    attempts = []
    for attempt in store.job_attempts(job_id):
        collection = store.result_collection(attempt.attempt_id)
        staged = spec.storage_mode == 'staged'
        delivered = collection is not None and collection['status'] == 'collected'
        available = {item['path'] for item in collection['manifest']['files']} if (
            delivered and collection['manifest']) else set()
        directory = Path(attempt.directory) / 'results' if staged else Path(attempt.directory)

        def artifact(name: str) -> str | None:
            return str(directory / name) if not staged or delivered and name in available else None

        attempts.append({**asdict(attempt), 'stdout': artifact('stdout'), 'stderr': artifact('stderr'),
                         'execution_metadata': artifact('execution.json'),
                         'result_collection': collection or (dict(status='pending',
                             reason='Results have not been collected') if staged else None),
                         'results_directory': str(directory) if staged and delivered else None,
                         'input_transfer': store.input_transfer(attempt.attempt_id)})
    return {'specification': asdict(spec), 'state': store.job_state(job_id),
            'attempts': attempts, 'events': store.events(job_id)}
