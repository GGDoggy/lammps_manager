"""Bounded staged results, authenticated by the manager before every operation."""

from dataclasses import asdict
import hashlib
import os
from pathlib import Path
import stat
import tempfile
from typing import BinaryIO

from .config import Storage, WorkerConfig
from .models import InputFile
from .staging import (CHUNK, copy_verified, open_regular, publish_inputs, safe_path,
                      sync_directory, validate_manifest)
from .store import Store, TERMINAL


def output_limits(config: Storage | WorkerConfig) -> dict:
    """Use the same independent output limits on both transfer peers."""
    return dict(max_files=config.max_output_files,
                max_file_bytes=config.max_output_file_bytes,
                max_total_bytes=config.max_output_total_bytes)


def validate_results(manifest: dict, limits: dict) -> tuple[InputFile, ...]:
    """Validate the complete bounded wire manifest, including omissions."""
    if not isinstance(manifest, dict) or set(manifest) not in ({'files', 'omitted'}, {'files', 'omitted', 'compression'}):
        raise ValueError('Invalid result manifest')
    files = tuple(InputFile(**entry) for entry in manifest['files'])
    validate_manifest(files, **limits)
    compressed = manifest.get('compression', [])
    if not isinstance(compressed, list) or len(compressed) > len(files):
        raise ValueError('Invalid compression records')
    by_path = {item.path: item for item in files}
    seen = set()
    for entry in compressed:
        if (not isinstance(entry, dict) or set(entry) != {'path', 'size', 'sha256', 'compression', 'original'}
                or entry['compression'] != 'gzip' or entry['path'] in seen):
            raise ValueError('Invalid compression record')
        original = InputFile(**entry['original'])
        validate_manifest((original,))
        if (not original.path.startswith('work/') or not original.path.endswith('.dump')
                or entry['path'] != original.path + '.gz' or original.path in by_path
                or by_path.get(entry['path']) != InputFile(entry['path'], entry['size'], entry['sha256'])):
            raise ValueError('Compression record does not match result files')
        seen.add(entry['path'])
    omitted = manifest['omitted']
    if not isinstance(omitted, list) or len(omitted) > 6:
        raise ValueError('Too many omitted artifacts')
    for entry in omitted:
        if (not isinstance(entry, dict) or set(entry) != {'path', 'reason'}
                or not isinstance(entry['reason'], str) or not 0 < len(entry['reason']) <= 256):
            raise ValueError('Invalid artifact omission')
        safe_path(entry['path'])
    for item in files:
        if item.path not in {'stdout', 'stderr', 'execution.json', 'process.json', 'outcome.json'} and not item.path.startswith('work/'):
            raise ValueError('Result outside selected outputs')
    return files


def enumerate_results(directory: Path, limits: dict, *, publication_prefix: str = '') -> dict:
    """Select logs, metadata and the complete work tree after confirmed exit.

    Missing logs/metadata are explicit omissions. Unsafe or oversized work trees
    fail collection rather than silently dropping scientific output.
    """
    files = []
    omitted = []
    total = 0
    compression = []
    originals = set()

    def add(relative: str) -> None:
        nonlocal total
        with open_regular(directory, relative) as stream:
            size = os.fstat(stream.fileno()).st_size
            total += size
            if (len(files) >= limits['max_files'] or size > limits['max_file_bytes']
                    or total > limits['max_total_bytes']):
                raise ValueError('Output count or size limit exceeded')
            digest = hashlib.sha256()
            count = 0
            while chunk := stream.read(min(CHUNK, size - count + 1)):
                count += len(chunk)
                if count > size:
                    raise ValueError('Output changed during enumeration')
                digest.update(chunk)
            if count != size:
                raise ValueError('Output changed during enumeration')
        files.append(InputFile(relative, size, digest.hexdigest()))

    for name in ('stdout', 'stderr', 'execution.json', 'process.json', 'outcome.json'):
        try:
            add(name)
        except FileNotFoundError:
            omitted.append(dict(path=name, reason='Artifact unavailable: file missing'))
    work = directory / 'work'
    if work.is_symlink():
        raise ValueError('Unsafe output work directory')
    if work.exists():
        if not work.is_dir():
            raise ValueError('Output work path must be a directory')

        def walk_error(error: OSError) -> None:
            raise error

        for parent, directories, names in os.walk(work, followlinks=False, onerror=walk_error):
            for name in directories + names:
                path = Path(parent) / name
                mode = path.lstat().st_mode
                if stat.S_ISDIR(mode):
                    continue
                if not stat.S_ISREG(mode):
                    raise ValueError('Output contains symlink or special file')
                relative = path.relative_to(directory).as_posix()
                if relative.endswith('.dump'):
                    from .compression import compress_trajectory
                    record = compress_trajectory(directory, relative, publication_prefix=publication_prefix)
                    if record is not None:
                        compression.append(record)
                        originals.add(relative)
        # Compression precedes all work-tree hashing and manifest binding. Walk
        # again so newly published gzip files appear exactly once.
        for parent, directories, names in os.walk(work, followlinks=False, onerror=walk_error):
            for name in directories + names:
                path = Path(parent) / name
                mode = path.lstat().st_mode
                if stat.S_ISDIR(mode):
                    continue
                if not stat.S_ISREG(mode):
                    raise ValueError('Output contains symlink or special file')
                relative = path.relative_to(directory).as_posix()
                if relative not in originals:
                    add(relative)
    else:
        omitted.append(dict(path='work', reason='Artifact unavailable: work directory missing'))
    if not files:
        raise ValueError('No result files available')
    manifest = dict(files=[asdict(item) for item in sorted(files, key=lambda item: item.path)], omitted=omitted)
    if compression:
        manifest['compression'] = sorted(compression, key=lambda entry: entry['path'])
    validate_results(manifest, limits)
    return manifest


def authorize(store: Store, worker_id: str, attempt_id: str) -> Path:
    """Return a manager-owned destination for an owned, terminated attempt."""
    attempt = store.attempt(attempt_id)
    spec = store.specification(attempt.job_id)
    if (attempt.worker_id != worker_id
            or attempt.state not in TERMINAL or not attempt.termination_evidence):
        raise ValueError('Result ownership or termination mismatch')
    root = Path(spec.work_directory)
    safe_path(attempt_id)
    if len(Path(attempt_id).parts) != 1:
        raise ValueError('Invalid attempt identifier')
    directory = root / attempt_id
    for parent in (root, directory):
        if parent.is_symlink() or parent.resolve() != parent:
            raise ValueError('Unsafe result destination')
    directory.mkdir(exist_ok=True)
    return directory


def upload_file(directory: Path, item: InputFile, stream: BinaryIO) -> None:
    """Replace only an uncommitted spool file after exact size/hash verification."""
    spool = directory / '.results-upload'
    if spool.is_symlink():
        raise ValueError('Unsafe upload spool')
    spool.mkdir(exist_ok=True)
    # Use a flat manager-selected hash name, never a client path for writes.
    target = spool / hashlib.sha256(item.path.encode()).hexdigest()
    fd, temporary = tempfile.mkstemp(prefix='.part-', dir=spool)
    try:
        with os.fdopen(fd, 'wb') as outgoing:
            copy_verified(stream, outgoing, item)
        os.replace(temporary, target)
        sync_directory(spool)
    finally:
        Path(temporary).unlink(missing_ok=True)


def commit_results(directory: Path, files: tuple[InputFile, ...], limits: dict) -> None:
    """Publish a whole verified tree; replay revalidates and resynchronizes it."""
    spool = directory / '.results-upload'
    publish_inputs(directory / 'results', files,
                   lambda index: open_regular(spool, hashlib.sha256(files[index].path.encode()).hexdigest()),
                   limits)
