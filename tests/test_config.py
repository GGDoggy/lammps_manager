"""Configuration acceptance tests; no workers or simulation processes needed."""
from dataclasses import FrozenInstanceError
from pathlib import Path
import tempfile
import unittest

import yaml

from lammps_manager.config import ConfigurationError, create_job_specification, load_config


class ConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'config.yaml'
        self.manager = yaml.safe_load(Path('examples/manager.yaml').read_text())

    def load(self, data, kind='manager', local=False):
        self.path.write_text(yaml.safe_dump(data))
        return load_config(self.path, kind, check_local_paths=local)

    def test_examples(self):
        for kind in ('manager', 'worker'):
            load_config(f'examples/{kind}.yaml', kind)

    def test_invalid_fields_and_values(self):
        cases = [('job_defaults', 'mpi_ranks', -1), ('job_defaults', 'mpi_ranks', True),
                 ('job_defaults', 'mpi_ranks', 1.5), ('job_defaults', 'memory_gib', float('inf')),
                 ('job_defaults', 'memory_gib', '8'), ('job_defaults', 'max_retries', -1),
                 ('scheduler', 'policy', 'arbitrary'), ('scheduler', 'reserve_memory_gib', -1),
                 ('scheduler', 'max_cpu_utilization_percent', 101),
                 ('scheduler', 'min_slots', -1), ('scheduler', 'min_slots', True),
                 ('scheduler', 'min_slots', 1.5), ('scheduler', 'min_slots', 1000000),
                 ('scheduler', 'reserve_slots', -1), ('scheduler', 'reserve_slots', True),
                 ('storage', 'mode', 'unsupported'), ('storage', 'work_root', 'relative'),
                 ('manager', 'unknown', 1), ('manager', 'schedule_interval_seconds', 0)]
        for section, key, value in cases:
            with self.subTest(section=section, key=key, value=value):
                data = yaml.safe_load(yaml.safe_dump(self.manager))
                data[section][key] = value
                with self.assertRaisesRegex(ConfigurationError, key):
                    self.load(data)

    def test_duplicate_workers_and_keys(self):
        self.manager['workers'] *= 2
        with self.assertRaisesRegex(ConfigurationError, 'unique'):
            self.load(self.manager)
        self.path.write_text('storage: {}\nstorage: {}\n')
        with self.assertRaisesRegex(ConfigurationError, 'duplicate YAML key'):
            load_config(self.path, 'manager')

    def test_bad_documents(self):
        for document in ('[]', '{}', '!!python/object:os.system {}', 'manager: ['):
            self.path.write_text(document)
            with self.assertRaises(ConfigurationError):
                load_config(self.path, 'manager')
        with self.assertRaises(ConfigurationError):
            load_config('/missing/config.yaml', 'worker')

    def test_staged_cleanup_setting(self):
        worker = yaml.safe_load(Path('examples/worker.yaml').read_text())
        worker.pop('cleanup_staged_files', None)
        self.assertTrue(self.load(worker, 'worker').cleanup_staged_files)
        for enabled in (True, False):
            worker['cleanup_staged_files'] = enabled
            self.assertEqual(self.load(worker, 'worker').cleanup_staged_files, enabled)
        for invalid in (0, 1, 'false', None):
            worker['cleanup_staged_files'] = invalid
            with self.assertRaisesRegex(ConfigurationError, 'cleanup_staged_files'):
                self.load(worker, 'worker')

    def test_snapshot_and_cpu_slots(self):
        config = self.load(self.manager)
        overrides = {'mpi_ranks': 3, 'omp_threads_per_rank': 2}
        spec = create_job_specification(config, job_id='j1', work_directory='/shared/job1',
                                        input_file='in.lmp', input_sha256='a'*64,
                                        resource_overrides=overrides)
        overrides['mpi_ranks'] = 99
        self.manager['job_defaults']['mpi_ranks'] = 12
        updated = self.load(self.manager)
        self.assertEqual(updated.job_defaults.mpi_ranks, 12)
        self.assertEqual(spec.resources.cpu_slots, 6)
        with self.assertRaises(FrozenInstanceError):
            spec.resources.mpi_ranks = 99
        for change in ({'resource_overrides': {'extra': 1}}, {'input_sha256': 'bad'},
                       {'input_file': '../in.lmp'}, {'work_directory': 'relative'}):
            args = dict(job_id='j1', work_directory='/shared/job1', input_file='in.lmp', input_sha256='a'*64)
            args.update(change)
            with self.assertRaises(ConfigurationError):
                create_job_specification(config, **args)

    def test_worker_rejects_invalid_settings(self) -> None:
        for key, value in [('manager_endpoint', 'ssh://host'),
                           ('manager_endpoint', 'https://user:secret@host'),
                           ('heartbeat_interval_seconds', False),
                           ('token_file', 'relative')]:
            worker = yaml.safe_load(Path('examples/worker.yaml').read_text())
            worker[key] = value
            with self.subTest(key=key, value=value):
                with self.assertRaisesRegex(ConfigurationError, key):
                    self.load(worker, 'worker')
        worker = yaml.safe_load(Path('examples/worker.yaml').read_text())
        worker['execution']['mpi_args'] = '--bind-to core'
        with self.assertRaisesRegex(ConfigurationError, 'mpi_args'):
            self.load(worker, 'worker')

    def test_result_request_timeout(self) -> None:
        worker = yaml.safe_load(Path('examples/worker.yaml').read_text())
        worker.pop('result_request_timeout_seconds', None)
        self.assertEqual(self.load(worker, 'worker').result_request_timeout_seconds, 300)
        worker['result_request_timeout_seconds'] = 42.5
        self.assertEqual(self.load(worker, 'worker').result_request_timeout_seconds, 42.5)
        for value in (0, -1, True, '300', float('inf'), float('nan')):
            with self.subTest(value=value):
                worker['result_request_timeout_seconds'] = value
                with self.assertRaisesRegex(ConfigurationError, 'result_request_timeout_seconds'):
                    self.load(worker, 'worker')

    def test_environment_values_allow_empty_strings(self) -> None:
        worker = yaml.safe_load(Path('examples/worker.yaml').read_text())
        environment = {'CUDA_VISIBLE_DEVICES': '', 'SPACES': '  ', 'NORMAL': 'value'}
        worker['execution']['environment'] = environment
        config = self.load(worker, 'worker')
        self.assertEqual(dict(config.execution.environment), environment)
        for value in ('bad\x00value', None, False, 1):
            with self.subTest(value=value):
                worker['execution']['environment'] = {'VARIABLE': value}
                with self.assertRaisesRegex(ConfigurationError, 'environment'):
                    self.load(worker, 'worker')
        worker['execution']['environment'] = {'': 'value'}
        with self.assertRaisesRegex(ConfigurationError, 'environment'):
            self.load(worker, 'worker')
        worker['execution']['environment'] = environment
        worker['token_file'] = ''
        with self.assertRaisesRegex(ConfigurationError, 'token_file'):
            self.load(worker, 'worker')

    def test_argument_values_preserved(self) -> None:
        worker = yaml.safe_load(Path('examples/worker.yaml').read_text())
        for name in ('lammps_args', 'mpi_args'):
            with self.subTest(name=name):
                arguments = ['--option', '', '  ', 'value']
                worker['execution'][name] = arguments
                config = self.load(worker, 'worker')
                self.assertEqual(getattr(config.execution, name), tuple(arguments))
                for invalid in ('bad\x00value', None, False, 1):
                    with self.subTest(invalid=invalid):
                        worker['execution'][name] = [invalid]
                        with self.assertRaisesRegex(ConfigurationError, name):
                            self.load(worker, 'worker')
                worker['execution'][name] = []
        worker['worker']['labels'] = ['']
        with self.assertRaisesRegex(ConfigurationError, 'labels'):
            self.load(worker, 'worker')
        worker['worker']['labels'] = []
        worker['execution']['lammps_path'] = ''
        with self.assertRaisesRegex(ConfigurationError, 'lammps_path'):
            self.load(worker, 'worker')

    def test_input_path_rejects_work_directory(self) -> None:
        config = self.load(self.manager)
        args = dict(job_id='j1', work_directory='/shared/job1', input_sha256='a'*64)
        for input_file in ('.', './', '././', '../in.lmp', '/in.lmp'):
            with self.subTest(input_file=input_file):
                with self.assertRaisesRegex(ConfigurationError, 'input_file'):
                    create_job_specification(config, input_file=input_file, **args)
        for input_file in ('in.lmp', './in.lmp', 'inputs/in.lmp'):
            with self.subTest(input_file=input_file):
                spec = create_job_specification(config, input_file=input_file, **args)
                self.assertEqual(spec.input_file, input_file)

    def test_local_checks(self):
        self.load(self.manager)
        with self.assertRaisesRegex(ConfigurationError, 'local directory'):
            self.load(self.manager, local=True)
        self.manager['manager']['database_path'] = str(Path(self.temp.name) / 'state.sqlite3')
        self.manager['storage']['work_root'] = self.temp.name
        self.load(self.manager, local=True)
        worker = yaml.safe_load(Path('examples/worker.yaml').read_text())
        worker['work_root'] = self.temp.name
        with self.assertRaisesRegex(ConfigurationError, 'local executable'):
            self.load(worker, 'worker', True)
        worker['execution']['lammps_path'] = '/bin/true'
        worker['execution']['mpi_launcher_path'] = '/bin/true'
        with self.assertRaisesRegex(ConfigurationError, 'local token'):
            self.load(worker, 'worker', True)
        worker['token_file'] = str(self.path)
        config = self.load(worker, 'worker', True)
        self.assertEqual(config.execution.environment, (('OMP_PROC_BIND', 'close'),))
