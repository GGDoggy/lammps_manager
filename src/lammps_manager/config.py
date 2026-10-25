"""Strict YAML configuration loading and submission snapshot creation."""

from dataclasses import dataclass, field, fields, is_dataclass
import math
import os
from pathlib import Path
import re
from typing import Any, get_args, get_origin, get_type_hints
from urllib.parse import urlsplit

import yaml

from .models import InfrastructureRecovery, JobSpecification, Resources


class ConfigurationError(ValueError):
    """Configuration or submission validation failed."""


@dataclass(frozen=True)
class ManagerSettings:
    database_path: str
    schedule_interval_seconds: float = 10
    heartbeat_timeout_seconds: float = 60


@dataclass(frozen=True)
class Storage:
    work_root: str
    mode: str = 'shared'
    preserve_failed_jobs: bool = True
    max_input_files: int = 10000
    max_input_file_bytes: int = 1073741824
    max_input_total_bytes: int = 10737418240
    max_output_files: int = 10000
    max_output_file_bytes: int = 1073741824
    max_output_total_bytes: int = 10737418240


@dataclass(frozen=True)
class Scheduler:
    policy: str = 'fifo_least_loaded'
    account_external_load: bool = True
    max_cpu_utilization_percent: float = 80
    min_slots: int = 0
    reserve_slots: int = 0
    reserve_memory_gib: float = 4
    metrics_max_age_seconds: float = 30
    default_priority: int = 0


@dataclass(frozen=True)
class WorkerCapacity:
    id: str
    cpu_slots: int
    memory_budget_gib: float
    max_concurrent_jobs: int
    enabled: bool = True
    labels: tuple[str, ...] = ()


@dataclass(frozen=True)
class Monitoring:
    transfer_stall_seconds: float = 300
    low_disk_gib: float = 5


@dataclass(frozen=True)
class Segmentation:
    step_max: int = 100000
    environment_reset_seconds: float = 3600


@dataclass(frozen=True)
class ManagerConfig:
    manager: ManagerSettings
    storage: Storage
    workers: tuple[WorkerCapacity, ...]
    job_defaults: Resources = field(default_factory=Resources)
    scheduler: Scheduler = field(default_factory=Scheduler)
    infrastructure_recovery: InfrastructureRecovery = field(default_factory=InfrastructureRecovery)
    monitoring: Monitoring = field(default_factory=Monitoring)
    segmentation: Segmentation = field(default_factory=Segmentation)


@dataclass(frozen=True)
class Execution:
    lammps_path: str
    mpi_launcher_path: str
    lammps_args: tuple[str, ...] = ()
    mpi_args: tuple[str, ...] = ()
    environment: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class WorkerConfig:
    worker: WorkerCapacity
    manager_endpoint: str
    work_root: str
    execution: Execution
    token_file: str
    heartbeat_interval_seconds: float = 10
    storage_mode: str = 'shared'
    max_input_files: int = 10000
    max_input_file_bytes: int = 1073741824
    max_input_total_bytes: int = 10737418240
    max_output_files: int = 10000
    max_output_file_bytes: int = 1073741824
    max_output_total_bytes: int = 10737418240
    result_request_timeout_seconds: float = 300
    cleanup_staged_files: bool = True


class StrictLoader(yaml.SafeLoader):
    """Reject duplicate keys instead of silently replacing values."""


def _mapping(loader: StrictLoader, node: yaml.MappingNode) -> dict:
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if not isinstance(key, str):
            raise ConfigurationError('YAML mapping keys must be strings')
        if key in result:
            raise ConfigurationError(f'duplicate YAML key: {key}')
        result[key] = loader.construct_object(value_node, deep=True)
    return result


StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _parse(cls: type, value: Any, location: str) -> Any:
    if not isinstance(value, dict):
        raise ConfigurationError(f'{location}: expected a mapping')
    hints = get_type_hints(cls)
    unknown = value.keys() - hints.keys()
    if unknown:
        raise ConfigurationError(f'{location}: unknown fields: {sorted(unknown)}')
    parsed = {}
    for name, item in value.items():
        kind = hints[name]
        path = f'{location}.{name}'
        if is_dataclass(kind):
            parsed[name] = _parse(kind, item, path)
        elif get_origin(kind) is tuple:
            if name == 'environment':
                if not isinstance(item, dict):
                    raise ConfigurationError(f'{path}: expected a string mapping')
                parsed[name] = tuple((_scalar(str, k, path),
                                      _scalar(str, v, path, allow_empty=True))
                                     for k, v in item.items())
                continue
            if not isinstance(item, list):
                raise ConfigurationError(f'{path}: expected a list')
            subtype = get_args(kind)[0]
            allow_empty = cls is Execution and name in ('lammps_args', 'mpi_args')
            parsed[name] = tuple(_parse(subtype, x, path) if is_dataclass(subtype)
                                 else _scalar(subtype, x, path, allow_empty=allow_empty)
                                 for x in item)
        else:
            parsed[name] = _scalar(kind, item, path)
    try:
        instance = cls(**parsed)
    except TypeError as exc:
        raise ConfigurationError(f'{location}: {exc}') from exc
    _validate(instance, location)
    return instance


def _scalar(kind: type, value: Any, path: str, *, allow_empty: bool = False) -> Any:
    valid = (type(value) in (int, float) and (type(value) is int or math.isfinite(value))) if kind is float else type(value) is kind
    if not valid:
        raise ConfigurationError(f'{path}: expected {kind.__name__}')
    if kind is str and ('\x00' in value or (not allow_empty and not value.strip())):
        requirement = 'string without NUL' if allow_empty else 'nonempty string without NUL'
        raise ConfigurationError(f'{path}: expected {requirement}')
    return value


def _validate(instance: Any, location: str) -> None:
    if isinstance(instance, Execution):
        from .mpi_launch import effective_mpi_args
        try:
            effective_mpi_args(instance.mpi_args)
        except ValueError as exc:
            raise ConfigurationError(f'{location}.{exc}') from exc
    for entry in fields(instance):
        name, value = entry.name, getattr(instance, entry.name)
        if type(value) in (int, float) and name != 'default_priority':
            allow_zero = name in ('max_retries', 'reserve_memory_gib', 'max_cpu_utilization_percent', 'min_slots', 'reserve_slots')
            if value < 0 or (value == 0 and not allow_zero):
                reason = 'must not be negative' if allow_zero else 'must be positive'
                raise ConfigurationError(f'{location}.{name}: {reason}')
        if name in ('database_path', 'work_root', 'lammps_path', 'mpi_launcher_path', 'token_file', 'work_directory'):
            if not Path(value).is_absolute():
                raise ConfigurationError(f'{location}.{name}: must be an absolute path')
    if isinstance(instance, Storage) and instance.mode not in ('shared', 'staged'):
        raise ConfigurationError(f'{location}.mode: expected shared or staged')
    if isinstance(instance, WorkerConfig) and instance.storage_mode not in ('shared', 'staged'):
        raise ConfigurationError(f'{location}.storage_mode: expected shared or staged')
    if isinstance(instance, Scheduler):
        if not -(2**63) <= instance.default_priority < 2**63:
            raise ConfigurationError(f'{location}.default_priority: must be a signed 64-bit integer')
        if instance.policy != 'fifo_least_loaded':
            raise ConfigurationError(f'{location}.policy: unsupported policy {instance.policy!r}')
        if instance.max_cpu_utilization_percent > 100:
            raise ConfigurationError(f'{location}.max_cpu_utilization_percent: must be at most 100')
    if isinstance(instance, WorkerConfig):
        try:
            endpoint = urlsplit(instance.manager_endpoint)
            valid = endpoint.scheme in ('http', 'https') and endpoint.hostname and endpoint.port != 0
            valid = valid and not endpoint.username and not endpoint.password
        except ValueError:
            valid = False
        if not valid:
            raise ConfigurationError(f'{location}.manager_endpoint: expected HTTP(S) URL without credentials')
    if isinstance(instance, ManagerConfig):
        if any(instance.scheduler.min_slots > w.cpu_slots for w in instance.workers):
            raise ConfigurationError('scheduler.min_slots: exceeds worker CPU capacity')
        ids = [worker.id for worker in instance.workers]
        if not ids or len(ids) != len(set(ids)):
            raise ConfigurationError('manager.workers: require at least one worker and unique worker IDs')


def load_config(path: str | Path, kind: str, *, check_local_paths: bool = False) -> ManagerConfig | WorkerConfig:
    """Validate structure everywhere; inspect only this host when explicitly requested."""
    if kind not in ('manager', 'worker'):
        raise ConfigurationError('kind must be manager or worker')
    try:
        with Path(path).open(encoding='utf-8') as stream:
            data = yaml.load(stream, Loader=StrictLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ConfigurationError(f'{path}: {exc}') from exc
    config = _parse(ManagerConfig if kind == 'manager' else WorkerConfig, data, kind)
    if check_local_paths:
        _check_paths(config)
    return config


def _check_paths(config: ManagerConfig | WorkerConfig) -> None:
    directories = ([Path(config.manager.database_path).parent, Path(config.storage.work_root)]
                   if isinstance(config, ManagerConfig) else [Path(config.work_root)])
    for directory in directories:
        if not directory.is_dir():
            raise ConfigurationError(f'local directory does not exist: {directory}')
    if isinstance(config, WorkerConfig):
        for executable in (config.execution.lammps_path, config.execution.mpi_launcher_path):
            if not Path(executable).is_file() or not os.access(executable, os.X_OK):
                raise ConfigurationError(f'local executable missing or not executable: {executable}')
        if not Path(config.token_file).is_file() or not os.access(config.token_file, os.R_OK):
            raise ConfigurationError(f'local token file missing or unreadable: {config.token_file}')


def create_job_specification(config: ManagerConfig, *, job_id: str, work_directory: str,
                             input_file: str, input_sha256: str,
                             resource_overrides: dict[str, Any] | None = None) -> JobSpecification:
    """Copy validated defaults into a frozen submission; do not read remote inputs."""
    for name, value in dict(job_id=job_id, work_directory=work_directory,
                            input_file=input_file, input_sha256=input_sha256).items():
        _scalar(str, value, name)
    if not Path(work_directory).is_absolute():
        raise ConfigurationError('work_directory: must be an absolute path')
    input_path = Path(input_file)
    if input_path.is_absolute() or '..' in input_path.parts or input_path == Path('.'):
        raise ConfigurationError('input_file: must be a relative file inside the work directory')
    if re.fullmatch(r'[0-9a-fA-F]{64}', input_sha256) is None:
        raise ConfigurationError('input_sha256: expected a 64-digit hexadecimal SHA-256 digest')
    values = {entry.name: getattr(config.job_defaults, entry.name) for entry in fields(Resources)}
    if resource_overrides is not None:
        if not isinstance(resource_overrides, dict):
            raise ConfigurationError('resource_overrides: expected a mapping')
        values.update(resource_overrides)
    return JobSpecification(job_id, work_directory, input_file, input_sha256,
                            _parse(Resources, values, 'resources'),
                            infrastructure_recovery=config.infrastructure_recovery)
