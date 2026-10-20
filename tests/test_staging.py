"""Staged input tests use temporary roots and loopback only; never MPI / LAMMPS."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
import hashlib
from io import BytesIO
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from lammps_manager.config import (Execution, ManagerConfig, ManagerSettings, Scheduler,
                                   Storage, WorkerCapacity, WorkerConfig, load_config)
from lammps_manager.local import inspect_job, run_local, submit_batch
from lammps_manager.manager import ManagerService, make_server
from lammps_manager.models import InputFile, JobSpecification, Resources
from lammps_manager.scheduler import schedule
from lammps_manager.staging import publish_inputs, snapshot_inputs, sync_directory, validate_manifest
from lammps_manager.store import Store
from lammps_manager.worker import LinuxMonitor, Worker


def file_entry(path: str, data: bytes) -> InputFile:
    return InputFile(path, len(data), hashlib.sha256(data).hexdigest())


class SnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / 'source'
        self.source.mkdir()
        (self.source / 'in.lmp').write_bytes(b'input')
        (self.source / 'nested').mkdir()
        (self.source / 'nested/potential').write_bytes(b'potential')
        self.limits = dict(max_files=3, max_file_bytes=20, max_total_bytes=30)

    def test_complete_snapshot_and_limits(self) -> None:
        files = snapshot_inputs(self.source, self.root / 'snapshot', self.limits)
        self.assertEqual(files, (file_entry('in.lmp', b'input'),
                                 file_entry('nested/potential', b'potential')))
        (self.source / 'nested/potential').write_bytes(b'changed')
        self.assertEqual((self.root / 'snapshot/nested/potential').read_bytes(), b'potential')
        for change in (dict(max_files=1), dict(max_file_bytes=2), dict(max_total_bytes=5)):
            with self.subTest(change=change), self.assertRaises(ValueError):
                snapshot_inputs(self.source, self.root / 'rejected', {**self.limits, **change})
        self.assertFalse((self.root / 'rejected').exists())

    def test_reject_unsafe_paths_and_types(self) -> None:
        for path in ('../bad', '/bad', 'a/../bad', './bad', 'a//bad', 'a\\bad', 'bad\x00'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                validate_manifest((file_entry(path, b''),))
        for files in ((file_entry('a', b''), file_entry('a', b'')),
                      (file_entry('a', b''), file_entry('a/b', b''))):
            with self.assertRaises(ValueError):
                validate_manifest(files)
        for kind in ('symlink', 'fifo'):
            path = self.source / 'unsafe'
            if kind == 'symlink':
                path.symlink_to(self.source / 'in.lmp')
            else:
                os.mkfifo(path)
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                snapshot_inputs(self.source, self.root / 'rejected', self.limits)
            path.unlink()

    def test_partial_corrupt_and_replayed_publication(self) -> None:
        files = (file_entry('nested/input', b'abc'),)
        destination = self.root / 'worker/inputs'
        for data in (b'ab', b'xxx', b'abcd'):
            with self.subTest(data=data), self.assertRaises(ValueError):
                publish_inputs(destination, files, lambda index: BytesIO(data), self.limits)
            self.assertFalse(destination.exists())
            self.assertEqual(list(destination.parent.glob('.inputs-*')), [])
        publish_inputs(destination, files, lambda index: BytesIO(b'abc'), self.limits)
        publish_inputs(destination, files, lambda index: self.fail('Replay downloaded again'), self.limits)
        (destination / 'nested/input').write_bytes(b'xxx')
        with self.assertRaisesRegex(ValueError, 'hash mismatch'):
            publish_inputs(destination, files, lambda index: self.fail('Unexpected download'), self.limits)
        (destination / 'nested/input').unlink()
        (destination / 'nested/input').symlink_to(self.source / 'in.lmp')
        with self.assertRaises((OSError, ValueError)):
            publish_inputs(destination, files, lambda index: self.fail('Unexpected download'), self.limits)

    def test_replay_rejects_unmanifested_files(self) -> None:
        files = (file_entry('input', b'abc'),)
        destination = self.root / 'inputs'
        publish_inputs(destination, files, lambda index: BytesIO(b'abc'), self.limits)
        (destination / 'extra').write_text('unexpected')
        with self.assertRaisesRegex(ValueError, 'unexpected'):
            publish_inputs(destination, files, lambda index: self.fail('Unexpected download'), self.limits)
        self.assertEqual((destination / 'extra').read_text(), 'unexpected')

    def test_publication_retry_requires_parent_sync(self) -> None:
        destination = self.root / 'attempt/inputs'
        destination.parent.mkdir()
        files = (file_entry('input', b'abc'),)

        def fail_publication_sync(path: Path) -> None:
            if path == destination.parent:
                raise OSError('Publication directory sync failed')
            sync_directory(path)

        with patch('lammps_manager.staging.sync_directory', side_effect=fail_publication_sync):
            with self.assertRaisesRegex(OSError, 'directory sync failed'):
                publish_inputs(destination, files, lambda index: BytesIO(b'abc'), self.limits)
            self.assertTrue(destination.exists())
            with self.assertRaisesRegex(OSError, 'directory sync failed'):
                publish_inputs(destination, files,
                               lambda index: self.fail('Retry downloaded again'), self.limits)
        publish_inputs(destination, files,
                       lambda index: self.fail('Retry downloaded again'), self.limits)
        self.assertEqual((destination / 'input').read_bytes(), b'abc')

    def test_publication_retry_requires_ancestor_sync(self) -> None:
        destination = self.root / 'job/attempt/inputs'
        files = (file_entry('input', b'abc'),)

        def fail_ancestor_sync(path: Path) -> None:
            if path == self.root:
                raise OSError('Ancestor directory sync failed')
            sync_directory(path)

        with patch('lammps_manager.staging.sync_directory', side_effect=fail_ancestor_sync):
            for retry in range(2):
                with self.subTest(retry=retry), self.assertRaisesRegex(OSError, 'Ancestor'):
                    publish_inputs(destination, files, lambda index: BytesIO(b'abc'), self.limits)
        # Whether the failed sync happened before or after rename, recovery must
        # synchronize every ancestor before acknowledging the publication.
        publish_inputs(destination, files, lambda index: BytesIO(b'abc'), self.limits)
        with patch('lammps_manager.staging.sync_directory', side_effect=fail_ancestor_sync):
            with self.assertRaisesRegex(OSError, 'Ancestor'):
                publish_inputs(destination, files,
                               lambda index: self.fail('Replay downloaded again'), self.limits)
        publish_inputs(destination, files,
                       lambda index: self.fail('Recovery downloaded again'), self.limits)
        self.assertEqual((destination / 'input').read_bytes(), b'abc')

    def test_symlink_in_snapshot_root_ancestors_is_rejected(self) -> None:
        alias = self.root / 'alias'
        alias.symlink_to(self.source, target_is_directory=True)
        destination = self.root / 'inputs'
        files = (file_entry('in.lmp', b'input'),)
        from lammps_manager.staging import open_regular
        with self.assertRaises(OSError):
            open_regular(alias / 'nested', 'potential')
        destination.symlink_to(alias, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'symlink'):
            publish_inputs(destination, files, lambda index: self.fail('Unexpected fetch'), self.limits)

    def test_old_json_and_database_preserved(self) -> None:
        database = self.root / 'old.sqlite'
        spec = JobSpecification('old', '/shared/old', 'in.lmp', 'a' * 64)
        with Store(database) as store:
            store.submit(spec)
        payload = asdict(spec)
        del payload['storage_mode'], payload['input_manifest']
        serialized = json.dumps(payload, sort_keys=True)
        with sqlite3.connect(database) as connection:
            connection.execute('UPDATE jobs SET specification=?', (serialized,))
            connection.execute('DROP TABLE input_transfers')
        with Store(database) as store:
            self.assertEqual(store.specification('old'), spec)
            store.submit(spec)
        with sqlite3.connect(database) as connection:
            self.assertEqual(connection.execute('SELECT specification FROM jobs').fetchone()[0], serialized)
            self.assertEqual(connection.execute('PRAGMA user_version').fetchone()[0], 1)

    def test_staged_yaml_settings(self) -> None:
        import yaml
        manager = yaml.safe_load(Path('examples/manager.yaml').read_text())
        manager['storage'].update(mode='staged', max_input_files=5)
        worker = yaml.safe_load(Path('examples/worker.yaml').read_text())
        worker.update(storage_mode='staged', max_input_total_bytes=100)
        for kind, data in (('manager', manager), ('worker', worker)):
            path = self.root / (kind + '.yaml')
            path.write_text(yaml.safe_dump(data))
            config = load_config(path, kind)
            self.assertEqual(config.storage.mode if kind == 'manager' else config.storage_mode, 'staged')
            if kind == 'manager':
                data['storage']['max_input_files'] = 0
            else:
                data['max_input_files'] = True
            path.write_text(yaml.safe_dump(data))
            with self.assertRaises(ValueError):
                load_config(path, kind)


class StagedWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.manager_root = self.root / 'manager-files'
        self.worker_root = self.root / 'worker-files'
        self.manager_root.mkdir()
        self.worker_root.mkdir()
        self.capacity = WorkerCapacity('a', 2, 2, 1)
        self.config = ManagerConfig(ManagerSettings(str(self.root / 'manager.sqlite'), .001),
            Storage(str(self.manager_root), mode='staged'), (self.capacity, replace(self.capacity, id='b')),
            Resources(1, 1, .01), Scheduler(account_external_load=False, reserve_memory_gib=0))
        self.tokens = {}
        for name in ('a', 'b'):
            token = self.root / (name + '.token')
            token.write_text(name * 40)
            token.chmod(0o600)
            self.tokens[name] = str(token)
        self.service = ManagerService(self.config, self.tokens)
        self.addCleanup(self.service.close)
        self.server = make_server(self.service, '127.0.0.1', 0)
        self.endpoint = f'http://127.0.0.1:{self.server.server_port}'
        thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01})
        thread.start()
        def stop() -> None:
            self.server.shutdown()
            thread.join(timeout=5)
            self.server.server_close()
        self.addCleanup(stop)
        self.worker_config = WorkerConfig(self.capacity, self.endpoint, str(self.worker_root),
            Execution('/bin/false', '/bin/false'), self.tokens['a'], storage_mode='staged')
        source = self.root / 'source'
        source.mkdir()
        (source / 'in.lmp').write_text('input')
        (source / 'potential').write_text('potential')
        manifest = self.root / 'batch.json'
        manifest.write_text(json.dumps([dict(source_directory=str(source), input_file='./in.lmp')]))
        with Store(self.config.manager.database_path) as store:
            self.job_id, = submit_batch(store, self.config, manifest)
            self.spec = store.specification(self.job_id)
        (source / 'in.lmp').write_text('changed')

    def worker(self) -> Worker:
        worker = Worker(self.worker_config, self.root / 'journal')
        self.addCleanup(worker.close)
        return worker

    def assert_pending(self, worker: Worker, status: str) -> dict:
        self.assertIsNone(worker.future)
        with Store(worker.database) as journal:
            attempt, = journal.job_attempts(self.job_id)
            transfer = journal.input_transfer(attempt.attempt_id)
            self.assertEqual(transfer['status'], status)
            self.assertEqual(attempt.state, 'assigned')
            self.assertIsNone(attempt.started_at)
            self.assertEqual(len(journal.reservations()), 1)
            self.assertEqual(journal.specification(self.job_id), self.spec)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state(self.job_id), 'assigned')
            self.assertEqual(len(store.reservations()), 1)
        return transfer

    def test_separate_roots_replay_restart_and_launch_gate(self) -> None:
        worker = self.worker()
        result = worker.tick()
        self.assertFalse(result['staged_launch_enabled'])
        transfer = self.assert_pending(worker, 'ready')
        self.assertTrue(Path(transfer['directory']).is_relative_to(self.worker_root))
        self.assertEqual((Path(transfer['directory']) / 'in.lmp').read_text(), 'input')
        self.assertEqual((Path(transfer['directory']) / 'potential').read_text(), 'potential')
        for _ in range(3):
            worker.tick()
        with Store(self.config.manager.database_path) as store:
            attempt, = store.job_attempts(self.job_id)
            self.assertEqual(store.input_transfer(attempt.attempt_id)['status'], 'ready')
            self.assertEqual(inspect_job(store, self.job_id)['attempts'][0]['input_transfer']['status'], 'ready')
        worker.close()
        restarted = self.worker()
        restarted.tick()
        self.assert_pending(restarted, 'ready')
        self.assertFalse(list(self.worker_root.rglob('process.json')))
        self.assertFalse(list(self.worker_root.rglob('execution.json')))
        self.assertFalse(self.service.challenge.exists())
        with Store(self.config.manager.database_path) as store:
            # Local execution also cannot bypass the results gate.
            other = replace(self.spec, job_id='other', work_directory=str(self.manager_root / 'other'))
            store.submit(other)
            self.assertEqual(run_local(store, self.worker_config), ())
            self.assertIn('T008', store.events('other')[-1]['reason'])

    def test_missing_and_corrupt_inputs_retry_without_release(self) -> None:
        worker = self.worker()
        incoming = Path(self.spec.work_directory) / 'inputs/potential'
        incoming.unlink()
        worker.tick()
        self.assert_pending(worker, 'failed')
        incoming.write_text('xxxxxxxxx')
        worker.tick()
        transfer = self.assert_pending(worker, 'failed')
        self.assertIn('SHA-256', transfer['reason'])
        self.assertFalse(Path(transfer['directory']).exists())
        incoming.write_text('potential')
        worker.tick()
        self.assert_pending(worker, 'ready')
        worker.tick()

    def test_interrupted_download_is_not_published_and_restart_retries(self) -> None:
        worker = self.worker()
        original = worker.opener.open
        def interrupted(request: Request, **kwargs: object) -> object:
            if '/v2/inputs/' in request.full_url:
                return BytesIO(b'')
            return original(request, **kwargs)
        with patch.object(worker.opener, 'open', side_effect=interrupted):
            worker.tick()
        transfer = self.assert_pending(worker, 'failed')
        self.assertIn('Incomplete', transfer['reason'])
        self.assertFalse(Path(transfer['directory']).exists())
        worker.close()
        worker = self.worker()
        worker.tick()
        self.assert_pending(worker, 'ready')

    def test_publication_before_ready_record_recovers_without_download(self) -> None:
        worker = self.worker()
        worker.tick()
        transfer = self.assert_pending(worker, 'ready')
        with Store(worker.database) as journal:
            attempt, = journal.job_attempts(self.job_id)
            journal.set_input_transfer(attempt.attempt_id, transfer['directory'], 'downloading')
        worker.close()
        # Reconstruct the crash window: publication exists but readiness was not recorded.
        for item in self.spec.input_manifest:
            (Path(self.spec.work_directory) / 'inputs' / item.path).unlink()
        restarted = self.worker()
        restarted.tick()
        self.assert_pending(restarted, 'ready')

    def test_publication_sync_failure_never_reports_ready_on_retry(self) -> None:
        worker = self.worker()
        original = sync_directory

        def fail_publication_sync(path: Path) -> None:
            if (path / 'inputs').is_dir():
                raise OSError('Publication directory sync failed')
            original(path)

        with patch('lammps_manager.staging.sync_directory', side_effect=fail_publication_sync):
            worker.tick()
            transfer = self.assert_pending(worker, 'failed')
            self.assertTrue(Path(transfer['directory']).exists())
            worker.tick()
            self.assert_pending(worker, 'failed')
        worker.tick()
        self.assert_pending(worker, 'ready')

    def test_ancestor_sync_failure_retries_until_durable(self) -> None:
        worker = self.worker()

        def fail_ancestor_sync(path: Path) -> None:
            if path == self.worker_root:
                raise OSError('Worker root sync failed')
            sync_directory(path)

        with patch('lammps_manager.staging.sync_directory', side_effect=fail_ancestor_sync):
            for retry in range(2):
                with self.subTest(retry=retry):
                    worker.tick()
                    transfer = self.assert_pending(worker, 'failed')
                    self.assertIn('Worker root sync failed', transfer['reason'])
        # Published content must recover without another artifact download.
        for item in self.spec.input_manifest:
            (Path(self.spec.work_directory) / 'inputs' / item.path).unlink()
        worker.tick()
        self.assert_pending(worker, 'ready')
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            attempt, = store.job_attempts(self.job_id)
            self.assertEqual(store.input_transfer(attempt.attempt_id)['status'], 'ready')

    def test_stalled_artifact_allows_other_poll_and_download(self) -> None:
        worker = self.worker()
        result = worker.tick()
        attempt = result['assignments'][0]['attempt']['attempt_id']
        reading = threading.Event()
        release = threading.Event()
        original = self.service.input_artifact

        class StalledInput(BytesIO):
            def read(self, size: int = -1) -> bytes:
                reading.set()
                if not release.wait(5):
                    raise TimeoutError('Test did not release artifact stream')
                return super().read(size)

        def artifact(worker_id: str, attempt_id: str, index: int) -> tuple:
            stream, size = original(worker_id, attempt_id, index)
            if index == 0:
                with stream:
                    stream = StalledInput(stream.read())
            return stream, size

        def download(index: int) -> bytes:
            request = Request(self.endpoint + f'/v2/inputs/{attempt}/files/{index}',
                headers={'X-Worker-ID': 'a', 'Authorization': 'Bearer ' + 'a' * 40})
            with urlopen(request, timeout=2) as response:
                return response.read()

        with patch.object(self.service, 'input_artifact', side_effect=artifact):
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(download, 0)
                try:
                    self.assertTrue(reading.wait(2), 'Artifact stream did not start')
                    other_config = replace(self.worker_config,
                        worker=replace(self.capacity, id='b'), token_file=self.tokens['b'])
                    payload = {'protocol': 2, 'storage_mode': 'staged', 'reports': [],
                               'snapshot': LinuxMonitor().sample(other_config)}
                    request = Request(self.endpoint + '/v1/poll', data=json.dumps(payload).encode(),
                        headers={'X-Worker-ID': 'b', 'Authorization': 'Bearer ' + 'b' * 40})
                    with urlopen(request, timeout=2) as response:
                        self.assertEqual(json.load(response)['assignments'], [])
                    self.assertEqual(download(1), b'potential')
                    with Store(self.config.manager.database_path) as store:
                        self.assertIsNotNone(store.worker_records()['b']['snapshot'])
                        self.assertEqual(store.attempt(attempt).worker_id, 'a')
                        self.assertEqual(len(store.reservations()), 1)
                finally:
                    release.set()
                self.assertEqual(pending.result(timeout=3), b'input')

    def test_concurrent_polls_preserve_scheduling_interval_and_reservation(self) -> None:
        self.service.config = replace(self.config,
            manager=replace(self.config.manager, schedule_interval_seconds=60))
        entered = threading.Event()
        release = threading.Event()
        overlapping = threading.Event()

        def delayed_schedule(*args: object, **kwargs: object) -> object:
            if entered.is_set():
                overlapping.set()
            entered.set()
            if not release.wait(5):
                raise TimeoutError('Test did not release scheduling round')
            return schedule(*args, **kwargs)

        def poll(worker_id: str) -> dict:
            config = replace(self.worker_config, worker=replace(self.capacity, id=worker_id))
            return self.service.poll(worker_id, {'protocol': 2, 'storage_mode': 'staged',
                'snapshot': LinuxMonitor().sample(config), 'reports': []})

        with patch('lammps_manager.manager.schedule', side_effect=delayed_schedule) as scheduling:
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(poll, 'a')
                try:
                    self.assertTrue(entered.wait(2), 'First scheduling round did not start')
                    second = pool.submit(poll, 'b')
                    self.assertFalse(overlapping.wait(.2), 'Scheduling rounds overlapped')
                finally:
                    release.set()
                self.assertEqual(len(first.result(timeout=3)['assignments']), 1)
                self.assertEqual(second.result(timeout=3)['assignments'], [])
            self.assertEqual(scheduling.call_count, 1)
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(len(store.reservations()), 1)
            attempts = store.job_attempts(self.job_id)
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0].worker_id, 'a')
            self.assertEqual(set(store.worker_records()), {'a', 'b'})

    def test_transfer_limits_and_disk_failure_retain_reservations(self) -> None:
        self.worker_config = replace(self.worker_config, max_input_files=1)
        worker = self.worker()
        worker.tick()
        self.assert_pending(worker, 'failed')
        worker.config = replace(worker.config, max_input_files=10)
        with patch('lammps_manager.staging.copy_verified', side_effect=OSError('Disk full')):
            worker.tick()
        transfer = self.assert_pending(worker, 'failed')
        self.assertIn('Disk full', transfer['reason'])
        worker.tick()
        self.assert_pending(worker, 'ready')

    def test_artifact_manifest_cached_with_live_security_checks(self) -> None:
        from lammps_manager.staging import validate_manifest
        worker = self.worker()
        attempt = worker.tick()['assignments'][0]['attempt']['attempt_id']
        original = Store.specification

        def download(index: int) -> bytes:
            stream, _ = self.service.input_artifact('a', attempt, index)
            with stream:
                return stream.read()

        with patch.object(Store, 'specification', autospec=True, side_effect=original) as decode, \
                patch('lammps_manager.staging.validate_manifest', wraps=validate_manifest) as validate:
            # Start with an empty cache; concurrent fills must perform full work once.
            self.service.close()
            self.service = ManagerService(self.config, self.tokens)
            self.addCleanup(self.service.close)
            with ThreadPoolExecutor(max_workers=4) as pool:
                self.assertEqual(list(pool.map(download, [0, 1] * 4)),
                                 [b'input', b'potential'] * 4)
            self.assertEqual(decode.call_count, 1)
            self.assertEqual(validate.call_count, 1)
            with self.assertRaisesRegex(ValueError, 'ownership'):
                self.service.input_artifact('b', attempt, 0)
            path = Path(self.spec.work_directory) / 'inputs/in.lmp'
            path.unlink()
            path.symlink_to(self.root / 'source/in.lmp')
            with self.assertRaises(OSError):
                download(0)
            path.unlink()
            path.write_text('input')
            self.service.config = replace(self.config,
                storage=replace(self.config.storage, max_input_files=1))
            for _ in range(2):
                with self.assertRaisesRegex(ValueError, 'count'):
                    download(0)
            self.assertEqual(validate.call_count, 3)
            self.service.config = self.config
            self.assertEqual(download(0), b'input')
            self.assertEqual(validate.call_count, 3)
            with Store(self.config.manager.database_path) as store:
                store.transition(attempt, 'unknown', reason='Test state check')
            with self.assertRaisesRegex(ValueError, 'state'):
                download(0)

    def test_artifact_cache_evicts_old_specifications(self) -> None:
        from lammps_manager.staging import validate_manifest
        with Store(self.config.manager.database_path) as store:
            for index in range(33):
                job_id = f'cache-job-{index}'
                store.submit(replace(self.spec, job_id=job_id))
                store.assign(job_id, f'cache-attempt-{index}', 'a',
                             str(self.worker_root / job_id))

        def download(index: int) -> None:
            stream, size = self.service.input_artifact('a', f'cache-attempt-{index}', 0)
            with stream:
                self.assertEqual(stream.read(), b'input')
                self.assertEqual(size, 5)

        with patch('lammps_manager.staging.validate_manifest', wraps=validate_manifest) as validate:
            for index in range(33):
                download(index)
            self.assertEqual(validate.call_count, 33)
            download(32)
            self.assertEqual(validate.call_count, 33)
            download(0)
            self.assertEqual(validate.call_count, 34)

    def test_unauthorized_and_nonowner_downloads(self) -> None:
        worker = self.worker()
        result = worker.tick()
        attempt = result['assignments'][0]['attempt']['attempt_id']
        for name, token in (('a', 'bad'), ('b', 'b' * 40)):
            request = Request(self.endpoint + f'/v2/inputs/{attempt}/files/0',
                headers={'X-Worker-ID': name, 'Authorization': 'Bearer ' + token})
            with self.subTest(name=name), self.assertRaises(HTTPError):
                urlopen(request, timeout=3)
        for suffix in ('../files/0', f'{attempt}/files/-1', f'{attempt}/files/999'):
            request = Request(self.endpoint + '/v2/inputs/' + suffix,
                headers={'X-Worker-ID': 'a', 'Authorization': 'Bearer ' + 'a' * 40})
            with self.assertRaises(HTTPError):
                urlopen(request, timeout=3)

    def test_protocol_and_journal_mode_mismatch_fail_clearly(self) -> None:
        worker = self.worker()
        with self.assertRaisesRegex(ValueError, 'protocol 2'):
            self.service.poll('a', {})
        with patch.object(self.service, 'poll', return_value={'assignments': []}):
            with self.assertRaisesRegex(ValueError, 'protocol 2'):
                worker.tick()
        shared = Worker(replace(self.worker_config, storage_mode='shared'), self.root / 'shared-journal')
        self.addCleanup(shared.close)
        with self.assertRaisesRegex(ValueError, 'Incompatible manager storage'):
            shared.tick()
        worker.close()
        with self.assertRaisesRegex(ValueError, 'identity differs'):
            Worker(replace(self.worker_config, storage_mode='shared'), self.root / 'journal')

    def test_old_shared_jobs_are_not_assigned_to_staged_workers(self) -> None:
        with Store(self.config.manager.database_path) as store:
            store.submit(JobSpecification('old', '/shared/old', 'in.lmp', 'a' * 64))
        worker = self.worker()
        worker.tick()
        with Store(self.config.manager.database_path) as store:
            self.assertEqual(store.job_state('old'), 'queued')
            self.assertIn('storage_mode_mismatch', store.events('old')[-1]['reason'])

    def test_worker_runtime_symlink_is_rejected_on_retry(self) -> None:
        worker = self.worker()
        path = Path(self.spec.work_directory) / 'inputs/potential'
        path.unlink()
        worker.tick()
        transfer = self.assert_pending(worker, 'failed')
        runtime = Path(transfer['directory']).parent
        for partial in runtime.iterdir():
            self.fail(f'Unexpected retained partial tree: {partial}')
        runtime.rmdir()
        outside = self.root / 'outside'
        outside.mkdir()
        runtime.symlink_to(outside, target_is_directory=True)
        path.write_text('potential')
        worker.tick()
        transfer = self.assert_pending(worker, 'failed')
        self.assertIn('Unsafe', transfer['reason'])
        self.assertEqual(list(outside.iterdir()), [])

    def test_symlink_download_rejected(self) -> None:
        worker = self.worker()
        path = Path(self.spec.work_directory) / 'inputs/potential'
        path.unlink()
        path.symlink_to(self.root / 'source/potential')
        worker.tick()
        self.assert_pending(worker, 'failed')
