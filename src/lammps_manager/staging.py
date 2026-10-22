"""Bounded input snapshots and verified atomic worker-local publication."""

import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tempfile
from typing import BinaryIO, Callable

from .models import InputFile

CHUNK = 65536


def safe_path(value: str) -> Path:
    """Accept canonical relative POSIX paths, never platform-dependent aliases."""
    if (not isinstance(value, str) or not value or '\x00' in value or '\\' in value
            or PurePosixPath(value).is_absolute()
            or any(part in ('', '.', '..') for part in value.split('/'))):
        raise ValueError('Unsafe input path')
    return Path(value)


def validate_manifest(files: tuple[InputFile, ...], *, max_files: int | None = None,
                      max_file_bytes: int | None = None,
                      max_total_bytes: int | None = None) -> None:
    """Reject duplicate, conflicting, oversized and malformed manifest entries."""
    if not files or (max_files is not None and len(files) > max_files):
        raise ValueError('Input file count exceeds limit or is empty')
    paths = set()
    total = 0
    for item in files:
        safe_path(item.path)
        if (item.path in paths or type(item.size) is not int or item.size < 0
                or (max_file_bytes is not None and item.size > max_file_bytes)
                or not isinstance(item.sha256, str)
                or not re.fullmatch('[0-9a-f]{64}', item.sha256)):
            raise ValueError('Invalid input manifest or per-file size limit exceeded')
        paths.add(item.path)
        total += item.size
        if max_total_bytes is not None and total > max_total_bytes:
            raise ValueError('Total input size exceeds limit')
    for name in paths:
        if any(str(parent) in paths for parent in Path(name).parents if parent != Path('.')):
            raise ValueError('Conflicting input paths')


def open_regular(root: Path, relative: str) -> BinaryIO:
    """Walk without following symlinks, including intermediate directories."""
    parts = safe_path(relative).parts
    if not root.is_absolute() or '..' in root.parts:
        raise ValueError('Input root must be an absolute path without ..')
    fd = os.open(root.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in (*root.parts[1:], *parts[:-1]):
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        file_fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        if not stat.S_ISREG(os.fstat(file_fd).st_mode):
            os.close(file_fd)
            raise ValueError('Input must be a regular file')
        return os.fdopen(file_fd, 'rb')
    finally:
        os.close(fd)


def copy_verified(stream: BinaryIO, target: BinaryIO, item: InputFile,
                  progress: Callable[[], None] | None = None) -> None:
    """Read at most the declared size plus one byte and verify before publication."""
    digest = hashlib.sha256()
    remaining = item.size
    while remaining:
        chunk = stream.read(min(CHUNK, remaining))
        if not chunk:
            raise ValueError('Incomplete input download')
        target.write(chunk)
        if progress:
            progress()
        digest.update(chunk)
        remaining -= len(chunk)
    if stream.read(1) or digest.hexdigest() != item.sha256:
        raise ValueError('Input size or SHA-256 mismatch')
    target.flush()
    os.fsync(target.fileno())


def sync_directory(path: Path) -> None:
    """Persist a directory entry before acknowledging publication."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _sync_publication_ancestors(destination: Path) -> None:
    """Persist the entire path, including directories left by failed retries."""
    for parent in destination.parents:
        sync_directory(parent)


def snapshot_inputs(source: Path, destination: Path, limits: dict) -> tuple[InputFile, ...]:
    """Copy all regular inputs with bounded memory and immutable content identities."""
    if source.is_symlink():
        raise ValueError('Source root must not be a symlink')
    files = []
    total = 0
    for path in source.rglob('*'):
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('Source trees must contain only regular files and directories')
        total += info.st_size
        if (len(files) >= limits['max_files'] or info.st_size > limits['max_file_bytes']
                or total > limits['max_total_bytes']):
            raise ValueError('Input snapshot count or size limit exceeded')
        files.append(InputFile(path.relative_to(source).as_posix(), info.st_size, '0' * 64))
    files.sort(key=lambda item: item.path)
    validate_manifest(tuple(files), **limits)
    destination.mkdir()
    result = []
    for item in files:
        target = destination / safe_path(item.path)
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        count = 0
        with open_regular(source, item.path) as incoming, target.open('xb') as outgoing:
            while chunk := incoming.read(min(CHUNK, item.size - count + 1)):
                count += len(chunk)
                if count > item.size:
                    raise ValueError('Source changed during snapshot')
                digest.update(chunk)
                outgoing.write(chunk)
            outgoing.flush()
            os.fsync(outgoing.fileno())
        if count != item.size:
            raise ValueError('Source changed during snapshot')
        result.append(InputFile(item.path, count, digest.hexdigest()))
    for directory, _, _ in os.walk(destination, topdown=False):
        sync_directory(Path(directory))
    return tuple(result)


def publish_inputs(destination: Path, files: tuple[InputFile, ...],
                   fetch: Callable[[int], BinaryIO], limits: dict,
                   verified: Callable[[int], None] | None = None,
                   progress: Callable[[], None] | None = None) -> None:
    """Verify retries or atomically publish a fully verified temporary tree."""
    validate_manifest(files, **limits)
    if destination.is_symlink():
        raise ValueError('Published input directory must not be a symlink')
    if destination.exists():
        expected = {item.path for item in files}
        for path in destination.rglob('*'):
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                continue
            if not stat.S_ISREG(info.st_mode) or path.relative_to(destination).as_posix() not in expected:
                raise ValueError('Published inputs contain unexpected files or unsafe types')
        for index, item in enumerate(files):
            with open_regular(destination, item.path) as stream:
                digest = hashlib.sha256()
                count = 0
                while chunk := stream.read(min(CHUNK, item.size - count + 1)):
                    count += len(chunk)
                    if count > item.size:
                        raise ValueError('Published input size mismatch')
                    digest.update(chunk)
                    if progress:
                        progress()
                if count != item.size or digest.hexdigest() != item.sha256:
                    raise ValueError('Published input hash mismatch')
            if verified:
                verified(index)
        # Existence does not prove durability: a previous call may have failed
        # while synchronizing any ancestor after creating directories or rename.
        _sync_publication_ancestors(destination)
        return
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix='.inputs-', dir=destination.parent))
    try:
        for index, item in enumerate(files):
            target = temporary / safe_path(item.path)
            target.parent.mkdir(parents=True, exist_ok=True)
            with fetch(index) as incoming, target.open('xb') as outgoing:
                copy_verified(incoming, outgoing, item, progress)
            if verified:
                verified(index)
        for directory, _, _ in os.walk(temporary, topdown=False):
            sync_directory(Path(directory))
        os.rename(temporary, destination)
        _sync_publication_ancestors(destination)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
