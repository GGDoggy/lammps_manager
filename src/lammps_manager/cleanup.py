"""Bounded worker-private staged cleanup using pinned, no-follow directories."""

from contextlib import contextmanager
import os
from pathlib import Path
import stat
import sys
from typing import Iterator

from .staging import safe_path
from .store import Store

FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW


@contextmanager
def directory_fd(path: Path) -> Iterator[int]:
    """Pin an absolute directory without following any ancestor symlink."""
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('Unsafe cleanup root')
    fd = os.open('/', FLAGS)
    try:
        for name in path.parts[1:]:
            child = os.open(name, FLAGS, dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd
    finally:
        os.close(fd)


def identity(fd: int) -> list[int]:
    info = os.fstat(fd)
    return [info.st_dev, info.st_ino]


def names(store: Store, attempt_id: str, root: Path) -> tuple[str, str]:
    attempt = store.attempt(attempt_id)
    for value in (attempt.job_id, attempt_id):
        if len(safe_path(value).parts) != 1:
            raise ValueError('Unsafe cleanup identifier')
    if Path(attempt.directory) != root / attempt.job_id / attempt_id:
        raise ValueError('Cleanup directory differs from worker-private path')
    return attempt.job_id, attempt_id


def prepare_directory(store: Store, attempt_id: str, root: Path) -> None:
    """Own only a freshly created attempt; legacy/replaced trees are retained."""
    job, name = names(store, attempt_id, root)
    if store.cleanup_record(attempt_id) is not None:
        return
    with directory_fd(root) as root_fd:
        try:
            os.mkdir(job, mode=0o700, dir_fd=root_fd)
        except FileExistsError:
            pass
        job_fd = os.open(job, FLAGS, dir_fd=root_fd)
        try:
            try:
                os.mkdir(name, mode=0o700, dir_fd=job_fd)
            except FileExistsError:
                # Never adopt an existing tree without durable ownership evidence.
                return
            attempt_fd = os.open(name, FLAGS, dir_fd=job_fd)
            try:
                os.fsync(attempt_fd)
                os.fsync(job_fd)
                os.fsync(root_fd)
                store.own_staged_directory(attempt_id,
                    [identity(root_fd), identity(job_fd), identity(attempt_fd)])
            finally:
                os.close(attempt_fd)
        finally:
            os.close(job_fd)


def _empty(fd: int, device: int, budget: list[int]) -> bool:
    # scandir is lazy: a huge output tree cannot allocate an unbounded name list.
    with os.scandir(fd) as entries:
        for entry in entries:
            if budget[0] <= 0:
                return False
            info = os.stat(entry.name, dir_fd=fd, follow_symlinks=False)
            if info.st_dev != device:
                raise ValueError('Cleanup refuses filesystem boundaries')
            if stat.S_ISDIR(info.st_mode):
                child = os.open(entry.name, FLAGS, dir_fd=fd)
                try:
                    if identity(child) != [info.st_dev, info.st_ino]:
                        raise ValueError('Cleanup directory replaced')
                    if not _empty(child, device, budget):
                        return False
                    if budget[0] <= 0:
                        return False
                    current = os.stat(entry.name, dir_fd=fd, follow_symlinks=False)
                    if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
                        raise ValueError('Cleanup directory replaced')
                    os.rmdir(entry.name, dir_fd=fd)
                finally:
                    os.close(child)
            elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                os.unlink(entry.name, dir_fd=fd)
            else:
                raise ValueError('Cleanup refuses symlinks, hardlinks and special files')
            budget[0] -= 1
            os.fsync(fd)
    return True


def cleanup_attempt(store: Store, attempt_id: str, root: Path,
                    protected: tuple[Path, ...], budget: int = 256, *,
                    manager_directory: Path | None = None) -> None:
    """Remove at most budget inner entries plus the empty root; retain a tombstone."""
    record = store.cleanup_record(attempt_id)
    if record is None or record['status'] == 'cleaned':
        return
    store.set_cleanup(attempt_id, 'deleting')  # Eligibility and durable ack gate.
    try:
        job, name = names(store, attempt_id, root)
        target = root / job / name
        if any(path.is_relative_to(target) or path.resolve().is_relative_to(target)
               for path in protected):
            raise ValueError('Cleanup directory contains protected worker state or credentials')
        if manager_directory is not None:
            for manager_path in (manager_directory, manager_directory.resolve()):
                if target.is_relative_to(manager_path) or manager_path.is_relative_to(target):
                    raise ValueError('Cleanup directory overlaps manager job artifacts')
        expected = record['identity']
        if len({pair[0] for pair in expected}) != 1:
            raise ValueError('Cleanup refuses filesystem boundaries')
        with directory_fd(root) as root_fd:
            if identity(root_fd) != expected[0]:
                raise ValueError('Cleanup root replaced')
            job_fd = os.open(job, FLAGS, dir_fd=root_fd)
            try:
                if identity(job_fd) != expected[1]:
                    raise ValueError('Cleanup job directory replaced')
                try:
                    attempt_fd = os.open(name, FLAGS, dir_fd=job_fd)
                except FileNotFoundError:
                    # A crash after rmdir but before the SQLite tombstone.
                    os.fsync(job_fd)
                else:
                    try:
                        if identity(attempt_fd) != expected[2]:
                            raise ValueError('Cleanup attempt directory replaced')
                        if not _empty(attempt_fd, expected[2][0], [budget]):
                            return
                        current = os.stat(name, dir_fd=job_fd, follow_symlinks=False)
                        if [current.st_dev, current.st_ino] != expected[2]:
                            raise ValueError('Cleanup attempt directory replaced')
                        os.rmdir(name, dir_fd=job_fd)
                        os.fsync(job_fd)
                    finally:
                        os.close(attempt_fd)
            finally:
                os.close(job_fd)
        store.set_cleanup(attempt_id, 'cleaned')
    except (OSError, ValueError, RecursionError) as exc:
        reason = f'{type(exc).__name__}: {exc}'
        store.set_cleanup(attempt_id, 'failed', reason)
        if record['reason'] != reason:
            print(f'Staged cleanup {attempt_id} deferred: {reason}', file=sys.stderr)
