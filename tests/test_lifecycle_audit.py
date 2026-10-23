"""Local result-integrity regressions for the explicit lifecycle auditor."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from tools.verification import lifecycle_audit


class LifecycleAuditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / 'v006/manager.sqlite'
        self.database.parent.mkdir()
        self.output = self.database.parent / 'jobs/job/attempt/results'
        self.output.mkdir(parents=True)
        (self.output / 'stdout').write_bytes(b'original output')
        self.manifest = {'files': [dict(path='stdout', size=15,
                                      sha256=hashlib.sha256(b'original output').hexdigest())],
                         'omitted': []}
        with sqlite3.connect(self.database) as db:
            db.execute('CREATE TABLE attempts (attempt_id TEXT, directory TEXT)')
            db.execute('CREATE TABLE result_collections (attempt_id TEXT, status TEXT, manifest TEXT)')
            db.execute('INSERT INTO attempts VALUES (?, ?)', ('attempt', str(self.output.parent)))
            db.execute('INSERT INTO result_collections VALUES (?, ?, ?)',
                       ('attempt', 'collected', json.dumps(self.manifest)))

    def test_committed_files_pass_without_database_changes(self) -> None:
        before = self.database.read_bytes()
        outputs = lifecycle_audit.audit_results(self.root)
        self.assertEqual(outputs, {'v006/jobs/job/attempt/results/stdout':
                                  {'size': 15, 'sha256': self.manifest['files'][0]['sha256']}})
        self.assertEqual(self.database.read_bytes(), before)

    def test_corruption_missing_and_unexpected_files_fail(self) -> None:
        for change in ('same-size', 'truncated', 'missing', 'extra', 'symlink', 'missing-tree'):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as external:
                target = self.output / 'stdout'
                if change == 'same-size':
                    target.write_bytes(b'corrupted data!')
                elif change == 'truncated':
                    target.write_bytes(b'short')
                elif change == 'missing':
                    target.unlink()
                elif change == 'extra':
                    (self.output / 'unexpected').write_bytes(b'extra')
                elif change == 'symlink':
                    outside = Path(external) / 'stdout'
                    outside.write_bytes(b'original output')
                    target.unlink()
                    target.symlink_to(outside)
                else:
                    target.unlink()
                    self.output.rmdir()
                with self.assertRaises((ValueError, OSError)):
                    lifecycle_audit.audit_results(self.root)
                self.output.mkdir(exist_ok=True)
                target.unlink(missing_ok=True)
                target.write_bytes(b'original output')
                (self.output / 'unexpected').unlink(missing_ok=True)

    def test_missing_or_uncommitted_manifest_fails(self) -> None:
        for status, manifest in (('pending', self.manifest), ('collected', None)):
            with self.subTest(status=status, manifest=manifest):
                with sqlite3.connect(self.database) as db:
                    db.execute('UPDATE result_collections SET status=?, manifest=?',
                               (status, json.dumps(manifest) if manifest else None))
                with self.assertRaises(ValueError):
                    lifecycle_audit.audit_results(self.root)

    def test_multiple_attempts_and_unrecorded_publication(self) -> None:
        second = self.output.parent.parent / 'second/results'
        second.mkdir(parents=True)
        (second / 'stdout').write_bytes(b'original output')
        with self.assertRaisesRegex(ValueError, 'publications differ'):
            lifecycle_audit.audit_results(self.root)
        with sqlite3.connect(self.database) as db:
            db.execute('INSERT INTO attempts VALUES (?, ?)', ('second', str(second.parent)))
            db.execute('INSERT INTO result_collections VALUES (?, ?, ?)',
                       ('second', 'collected', json.dumps(self.manifest)))
        self.assertEqual(len(lifecycle_audit.audit_results(self.root)), 2)
        (second / 'stdout').write_bytes(b'corrupted data!')
        with self.assertRaisesRegex(ValueError, 'second'):
            lifecycle_audit.audit_results(self.root)

    def test_full_audit_rejects_corruption_and_deleted_publication(self) -> None:
        (self.root / 'source-sha256.json').write_text('{}')
        process = b'{"pid": 999999999}'
        (self.output / 'process.json').write_bytes(process)
        self.manifest['files'].append(dict(path='process.json', size=len(process),
                                          sha256=hashlib.sha256(process).hexdigest()))
        with sqlite3.connect(self.database) as db:
            db.execute('CREATE TABLE reservations (attempt_id TEXT)')
            db.execute('UPDATE result_collections SET manifest=?', (json.dumps(self.manifest),))
        original_iterdir = Path.iterdir

        def iterdir(path):
            return iter(()) if path == Path('/proc') else original_iterdir(path)

        # Stub external shutdown checks only; database, tree and hash checks are real.
        with patch.object(Path, 'iterdir', iterdir), \
                patch.object(lifecycle_audit, 'session_members', return_value=[]), \
                patch.object(lifecycle_audit.socket, 'create_connection',
                             side_effect=ConnectionRefusedError), patch('builtins.print'):
            lifecycle_audit.audit(self.root)
            historical = (self.root / 'final-audit.json').read_bytes()
            separate = self.root / 'corrected-audit.json'
            lifecycle_audit.audit(self.root, output=separate)
            self.assertEqual((self.root / 'final-audit.json').read_bytes(), historical)
            self.assertEqual(json.loads(separate.read_text())['outputs'],
                             json.loads(historical)['outputs'])
            with self.assertRaises(FileExistsError):
                lifecycle_audit.audit(self.root, output=separate)
            (self.output / 'stdout').write_bytes(b'corrupted data!')
            with self.assertRaises(ValueError):
                lifecycle_audit.audit(self.root)
            (self.output / 'stdout').unlink()
            with self.assertRaises(ValueError):
                lifecycle_audit.audit(self.root)
            (self.output / 'process.json').unlink()
            self.output.rmdir()
            with self.assertRaises((ValueError, OSError)):
                lifecycle_audit.audit(self.root)


if __name__ == '__main__':
    unittest.main()
