"""Streaming, verified gzip publication with original trajectories retained."""

import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import BinaryIO
import zlib

from .staging import CHUNK, open_regular, sync_directory


def _identity(stream: BinaryIO) -> tuple[int, str]:
    size = 0
    digest = hashlib.sha256()
    while chunk := stream.read(CHUNK):
        size += len(chunk)
        digest.update(chunk)
    return size, digest.hexdigest()


def _metadata_overhead(record: dict, publication_prefix: str) -> int:
    """Bound added default-JSON bytes, including combined MPI result paths."""
    original = {**record['original'], 'path': publication_prefix + record['original']['path']}
    archive = {key: record[key] for key in ('path', 'size', 'sha256')}
    archive['path'] = publication_prefix + archive['path']
    published = {**record, 'path': archive['path'], 'original': original}
    # Charge every candidate for the entire optional field/list wrapper, plus
    # a list separator. This overestimates the shared overhead for several dumps
    # and guarantees a strict net saving without depending on enumeration order.
    return (len(json.dumps(archive).encode()) - len(json.dumps(original).encode())
            + len(json.dumps(published).encode()) + len(', "compression": []') + 2)


def compress_trajectory(directory: Path, relative: str, *, publication_prefix: str = '') -> dict | None:
    """Publish gzip only after byte-for-byte hash/size roundtrip verification.

    The caller must establish process termination. Original files remain until
    acknowledged attempt cleanup. A replay creates the same gzip bytes; an
    existing conflicting destination is never overwritten. Return None without
    publishing an archive when artifact savings do not cover its manifest cost.
    publication_prefix accounts for manager-added MPI participant paths.
    """
    if not relative.startswith('work/') or not relative.endswith('.dump'):
        raise ValueError('Compression requires a work/*.dump trajectory')
    target_relative = relative + '.gz'
    # Validate and pin the source through the same no-follow path policy used by
    # result uploads. The output parent is the pinned source's trusted tree.
    with open_regular(directory, relative) as source:
        original_stat = os.fstat(source.fileno())
        target = directory / target_relative
        fd, temporary = tempfile.mkstemp(prefix='.trajectory-', dir=directory)
        try:
            size = 0
            digest = hashlib.sha256()
            with os.fdopen(fd, 'wb') as outgoing:
                with gzip.GzipFile(filename='', mode='wb', fileobj=outgoing, mtime=0) as compressed:
                    while chunk := source.read(CHUNK):
                        size += len(chunk)
                        digest.update(chunk)
                        compressed.write(chunk)
                outgoing.flush()
                os.fsync(outgoing.fileno())
            identity = size, digest.hexdigest()
            current = os.fstat(source.fileno())
            if size != original_stat.st_size or any(getattr(current, key) != getattr(original_stat, key)
                    for key in ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')):
                raise ValueError('Trajectory changed during compression')
            try:
                with gzip.open(temporary, 'rb') as restored:
                    if _identity(restored) != identity:
                        raise ValueError('Trajectory decompression verification failed')
            except (EOFError, zlib.error) as exc:
                raise ValueError('Trajectory decompression verification failed: corrupt gzip stream') from exc
            # Also reread the original: same-length mutation must not be hidden
            # by retaining only the size observed before compression.
            source.seek(0)
            if _identity(source) != identity:
                raise ValueError('Trajectory changed during verification')
            with open(temporary, 'rb') as compressed:
                compressed_identity = _identity(compressed)
            record = dict(path=target_relative, size=compressed_identity[0], sha256=compressed_identity[1],
                          compression='gzip', original=dict(path=relative, size=size, sha256=identity[1]))
            if size - compressed_identity[0] <= _metadata_overhead(record, publication_prefix):
                return None
            try:
                os.link(temporary, target)
            except FileExistsError:
                with open_regular(directory, target_relative) as existing:
                    if _identity(existing) != compressed_identity:
                        raise ValueError('Conflicting compressed trajectory exists')
            # A crash between link publication and unlinking its temporary name
            # can leave two links. Recover only matching private temporary links
            # after verifying the exact archive; otherwise cleanup would refuse
            # the multiply linked archive indefinitely.
            target_stat = target.lstat()
            for leftover in directory.glob('.trajectory-*'):
                leftover_stat = leftover.lstat()
                if ((leftover_stat.st_dev, leftover_stat.st_ino)
                        == (target_stat.st_dev, target_stat.st_ino)):
                    leftover.unlink()
            sync_directory(target.parent)
            sync_directory(directory)
            return record
        finally:
            Path(temporary).unlink(missing_ok=True)
