"""Lossless trajectory compression, deterministic replay and collision safety."""

import gzip
import hashlib
import json
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch

from lammps_manager.compression import compress_trajectory
from lammps_manager.results import enumerate_results, validate_results


class CompressionTests(unittest.TestCase):
    def test_non_saving_trajectories_remain_raw_without_an_archive(self):
        samples = (b'', b'tiny trajectory\n', b'x' * 200,
                   random.Random(17).randbytes(8192))
        for original in samples:
            with self.subTest(size=len(original)), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                (root / 'work').mkdir()
                source = root / 'work/a.dump'
                source.write_bytes(original)
                if original == b'x' * 200:
                    # The archive itself is smaller, but its metadata costs more
                    # than the saved artifact bytes.
                    self.assertLess(len(gzip.compress(original)), len(original))
                self.assertIsNone(compress_trajectory(root, 'work/a.dump'))
                self.assertEqual(source.read_bytes(), original)
                self.assertEqual(list((root / 'work').iterdir()), [source])
                self.assertEqual(list(root.glob('.trajectory-*')), [])

    def test_mixed_results_save_bytes_including_the_manifest_and_replay(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'work').mkdir()
            originals = {'work/large.dump': b'precise trajectory\n' * 2000,
                         'work/small.dump': b'x' * 200,
                         'work/empty.dump': b'',
                         'work/random.dump': random.Random(23).randbytes(8192),
                         'work/checkpoint.restart': b'checkpoint'}
            for name, data in originals.items():
                (root / name).write_bytes(data)
            limits = dict(max_files=100, max_file_bytes=1000000, max_total_bytes=1000000)
            manifest = enumerate_results(root, limits)
            raw = dict(files=[dict(path=name, size=len(data), sha256=hashlib.sha256(data).hexdigest())
                              for name, data in sorted(originals.items())], omitted=manifest['omitted'])
            def delivered_size(value):
                return sum(item['size'] for item in value['files']) + len(json.dumps(value).encode())
            self.assertLess(delivered_size(manifest), delivered_size(raw))
            self.assertEqual([item['original']['path'] for item in manifest['compression']], ['work/large.dump'])
            self.assertEqual({item['path'] for item in manifest['files']},
                (set(originals) - {'work/large.dump'}) | {'work/large.dump.gz'})
            self.assertEqual(gzip.decompress((root / 'work/large.dump.gz').read_bytes()), originals['work/large.dump'])
            self.assertEqual(enumerate_results(root, limits), manifest)
            for name, data in originals.items():
                self.assertEqual((root / name).read_bytes(), data)
            self.assertFalse((root / 'work/small.dump.gz').exists())

    def test_mpi_publication_prefix_is_included_in_savings(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'work').mkdir()
            (root / 'work/a.dump').write_bytes(b'x' * 600)
            self.assertIsNone(compress_trajectory(root, 'work/a.dump',
                publication_prefix='work/mpi-participants/' + 'a' * 240 + '/'))
            self.assertFalse((root / 'work/a.dump.gz').exists())
            self.assertIsNotNone(compress_trajectory(root, 'work/a.dump'))

    def test_truncated_stream_is_a_retryable_verification_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'work').mkdir()
            source = root / 'work/a.dump'
            source.write_bytes(b'original')
            with patch('lammps_manager.compression.gzip.open', side_effect=EOFError('truncated')):
                with self.assertRaisesRegex(ValueError, 'corrupt gzip'):
                    compress_trajectory(root, 'work/a.dump')
            self.assertEqual(source.read_bytes(), b'original')
            self.assertFalse((root / 'work/a.dump.gz').exists())
            self.assertEqual(list(root.glob('.trajectory-*')), [])

    def test_replay_recovers_crash_leftover_hardlink_before_cleanup(self):
        import os
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'work').mkdir()
            (root / 'work/a.dump').write_bytes(b'trajectory' * 1000)
            first = compress_trajectory(root, 'work/a.dump')
            archive = root / first['path']
            leftover = root / '.trajectory-crash'
            os.link(archive, leftover)
            self.assertEqual(archive.stat().st_nlink, 2)
            self.assertEqual(compress_trajectory(root, 'work/a.dump'), first)
            self.assertFalse(leftover.exists())
            self.assertEqual(archive.stat().st_nlink, 1)

    def test_manifest_contains_only_archive_and_binds_both_identities(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'work').mkdir()
            (root / 'work/a.dump').write_bytes(b'precise trajectory\n' * 1000)
            (root / 'work/checkpoint.restart').write_bytes(b'restart')
            limits = dict(max_files=100, max_file_bytes=1000000, max_total_bytes=1000000)
            manifest = enumerate_results(root, limits)
            paths = {entry['path'] for entry in manifest['files']}
            self.assertEqual(paths, {'work/a.dump.gz', 'work/checkpoint.restart'})
            self.assertEqual(manifest['compression'][0]['original']['path'], 'work/a.dump')
            self.assertEqual(enumerate_results(root, limits), manifest)
            validate_results(manifest, limits)
            manifest['compression'][0]['size'] += 1
            with self.assertRaises(ValueError):
                validate_results(manifest, limits)

    def test_roundtrip_and_replay_preserve_original(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'work').mkdir()
            original = b'id x y ix iy mux muy muz\n1 0.12345678901234567 -0 1 -2 1e-300 0 0\n' * 30000
            path = root / 'work/trajectory.dump'
            path.write_bytes(original)
            metadata = compress_trajectory(root, 'work/trajectory.dump')
            self.assertEqual(gzip.decompress((root / metadata['path']).read_bytes()), original)
            self.assertEqual(metadata['original']['sha256'], hashlib.sha256(original).hexdigest())
            self.assertEqual(metadata['original']['size'], len(original))
            self.assertEqual(compress_trajectory(root, 'work/trajectory.dump'), metadata)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(list(root.glob('.trajectory-*')), [])

    def test_collision_and_symlink_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'work').mkdir()
            (root / 'work/a.dump').write_bytes(b'original' * 1000)
            target = root / 'work/a.dump.gz'
            target.write_bytes(b'user data')
            with self.assertRaisesRegex(ValueError, 'Conflicting'):
                compress_trajectory(root, 'work/a.dump')
            self.assertEqual(target.read_bytes(), b'user data')
            target.unlink()
            target.symlink_to(root / 'work/a.dump')
            with self.assertRaises((OSError, ValueError)):
                compress_trajectory(root, 'work/a.dump')

    def test_empty_trajectory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'work').mkdir()
            (root / 'work/a.dump').touch()
            self.assertIsNone(compress_trajectory(root, 'work/a.dump'))
            self.assertFalse((root / 'work/a.dump.gz').exists())


if __name__ == '__main__':
    unittest.main()
