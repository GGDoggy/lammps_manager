"""Batch submission, isolated attempt execution and durable inspection."""

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
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
        source = (manifest.parent / source_value).resolve()
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
                input_file=input_value, input_sha256=hashlib.sha256((source / relative).read_bytes()).hexdigest(),
                resource_overrides=entry.get('resources'))
        if directory == source or source in directory.parents:
            raise ValueError('Storage work_root must not be inside a source tree')
        prepared.append((source, directory, spec))
    submitted = []
    for source, directory, spec in prepared:
        directory.mkdir(parents=True, exist_ok=False)
        shutil.copytree(source, directory / 'inputs')
        if hashlib.sha256((directory / 'inputs' / spec.input_file).read_bytes()).hexdigest() != spec.input_sha256:
            raise ValueError(f'Input changed during submission: {spec.job_id}')
        store.submit(spec)
        submitted.append(spec.job_id)
    return tuple(submitted)


def run_local(store: Store, worker: WorkerConfig) -> tuple[str, ...]:
    """Execute queued snapshots once, sequentially, retaining uncertain reservations."""
    completed = []
    for row in store.jobs():
        if row['state'] != 'queued':
            continue
        spec = store.specification(row['job_id'])
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
    if attempt.state != 'assigned':
        raise ValueError('Only a newly accepted assigned attempt can execute')
    spec = store.specification(attempt.job_id)
    root = Path(spec.work_directory).resolve()
    directory = Path(attempt.directory)
    process = None
    try:
        if directory.exists():
            store.transition(attempt.attempt_id, 'unknown', reason='Attempt directory already exists')
            return False
        directory.mkdir(exist_ok=False)
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
            process = subprocess.Popen(args, cwd=work, env={**os.environ, **environment},
                                       stdout=stdout, stderr=stderr, start_new_session=True)
            # Record Linux identity for later recovery without interpreting it here.
            identity = {'pid': process.pid, 'process_group': process.pid,
                        'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                        'start_ticks': Path(f'/proc/{process.pid}/stat').read_text().rpartition(')')[2].split()[19]}
            with (directory / 'process.json').open('x') as metadata:
                json.dump(identity, metadata)
                metadata.flush()
                os.fsync(metadata.fileno())
            store.transition(attempt.attempt_id, 'running')
            code = process.wait()
        # A launcher exit alone is insufficient if descendants remain alive.
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            store.transition(attempt.attempt_id, 'succeeded' if code == 0 else 'failed',
                exit_code=code, reason=None if code == 0 else f'Process exited with code {code}',
                termination_evidence='wait() reaped launcher and process group no longer exists')
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
    return {'specification': asdict(store.specification(job_id)), 'state': store.job_state(job_id),
            'attempts': [{**asdict(a), 'stdout': str(Path(a.directory) / 'stdout'),
                          'stderr': str(Path(a.directory) / 'stderr'),
                          'execution_metadata': str(Path(a.directory) / 'execution.json')}
                         for a in store.job_attempts(job_id)], 'events': store.events(job_id)}
