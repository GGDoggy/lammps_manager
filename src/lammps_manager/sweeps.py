"""Validated, deterministic Cartesian sweeps with immutable input publication."""

from dataclasses import asdict, dataclass, replace
import hashlib
import io
import itertools
import json
import math
from pathlib import Path
import re
import tempfile
from typing import BinaryIO, Iterator
from uuid import uuid4

from .config import ManagerConfig, create_job_specification
from .models import InputFile, SweepProvenance
from .staging import open_regular, publish_inputs, safe_path, snapshot_inputs, validate_manifest
from .store import Store


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _pairs(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'Duplicate JSON key: {key}')
        result[key] = value
    return result


def _positive(value: object, field: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f'{field}: expected a positive integer')
    return value


def _scalar(value: object) -> str:
    if isinstance(value, str):
        if any(c in value for c in ('\x00', '\n', '\r', '{{', '}}')):
            raise ValueError('parameters: strings cannot contain NUL, newlines or placeholder delimiters')
        return value
    if type(value) is bool:
        return '1' if value else '0'
    if type(value) in (int, float) and (type(value) is int or math.isfinite(value)):
        return str(value)
    raise ValueError('parameters: expected finite number, boolean or string scalar')


@dataclass(frozen=True)
class SweepPlan:
    """Validated source identity and bounded expansion description."""
    source: Path
    source_locator: str
    input_file: str
    template_version: str
    template: str
    parameters: dict
    repetitions: int
    seed_base: int
    combinations: int
    jobs: int
    resources: dict | None
    source_manifest: tuple[InputFile, ...]
    sampling: dict | None = None

    def coordinates(self) -> Iterator[dict]:
        """Yield last-axis-fastest coordinates, then independent repetitions."""
        names = sorted(self.parameters)
        axes = (range(len(self.parameters[name])) for name in names)
        ordinal = 0
        for combination, indices in enumerate(itertools.product(*axes), 1):
            for repetition in range(1, self.repetitions + 1):
                yield dict(combination_index=combination, selected_indices=dict(zip(names, indices)),
                           selected_values={n: self.parameters[n][i] for n, i in zip(names, indices)},
                           repetition_index=repetition, ordinal=ordinal, seed=self.seed_base + ordinal)
                ordinal += 1

    def render(self, coordinate: dict) -> bytes:
        """Replace literal placeholders once without interpreting selected values."""
        values = {**coordinate['selected_values'], 'seed': coordinate['seed'],
                  'repetition': coordinate['repetition_index']}
        return re.sub(r'\{\{([A-Za-z_][A-Za-z0-9_]*)\}\}',
                      lambda m: _scalar(values[m[1]]), self.template).encode('utf-8')

    def preview(self, limit: int = 20) -> dict:
        """Return counts and a bounded read-only expansion preview."""
        return dict(combinations=self.combinations, jobs=self.jobs,
                    preview=list(itertools.islice(self.coordinates(), max(0, min(limit, 100)))))


def _source_files(source: Path) -> tuple[InputFile, ...]:
    files = []
    if source.is_symlink() or not source.is_dir():
        raise ValueError('source_directory: expected a directory without symlinks')
    for path in sorted(source.rglob('*')):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ValueError('source_directory: only regular files and directories are allowed')
        if path.is_file():
            relative = path.relative_to(source).as_posix()
            safe_path(relative)
            digest = hashlib.sha256()
            size = 0
            with open_regular(source, relative) as stream:
                while chunk := stream.read(65536):
                    size += len(chunk)
                    digest.update(chunk)
            files.append(InputFile(relative, size, digest.hexdigest()))
    return tuple(sorted(files, key=lambda item: item.path))


def load_sweep(config: ManagerConfig, manifest: str | Path) -> SweepPlan:
    """Validate every expansion before creating database or input directories."""
    manifest = Path(manifest).resolve()
    data = json.loads(manifest.read_text(encoding='utf-8'), object_pairs_hook=_pairs)
    required = {'version', 'source_directory', 'input_file', 'template_version', 'parameters',
                'repetitions', 'seed_base'}
    if not isinstance(data, dict):
        raise ValueError('sweep: expected an object')
    missing = required - data.keys()
    unknown = data.keys() - required - {'resources', 'max_jobs', 'sampling'}
    if missing or unknown:
        raise ValueError(f'sweep: missing fields {sorted(missing)}; unknown fields {sorted(unknown)}')
    if type(data['version']) is not int or data['version'] != 1:
        raise ValueError('version: expected 1')
    for name in ('source_directory', 'input_file', 'template_version'):
        if not isinstance(data[name], str) or not data[name] or '\x00' in data[name]:
            raise ValueError(f'{name}: expected nonempty string')
    relative = safe_path(data['input_file']).as_posix()
    source = manifest.parent / data['source_directory']
    if source.is_symlink():
        raise ValueError('source_directory: symlink root')
    source = source.resolve()
    work = Path(config.storage.work_root).resolve()
    if work == source or source in work.parents:
        raise ValueError('work_root must not be inside source_directory')
    parameters = data['parameters']
    if not isinstance(parameters, dict) or not parameters:
        raise ValueError('parameters: expected nonempty mapping')
    combinations = 1
    for name, values in parameters.items():
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name) or name in ('seed', 'repetition'):
            raise ValueError(f'parameters.{name}: invalid or reserved name')
        if not isinstance(values, list) or not values:
            raise ValueError(f'parameters.{name}: expected nonempty list')
        for value in values:
            _scalar(value)
        combinations *= len(values)
    repetitions = _positive(data['repetitions'], 'repetitions')
    jobs = combinations * repetitions
    if jobs > _positive(data.get('max_jobs', 10000), 'max_jobs'):
        raise ValueError('max_jobs: expansion exceeds limit')
    seed = _positive(data['seed_base'], 'seed_base')
    if seed + jobs - 1 > 900000000:
        raise ValueError('seed_base: complete seed range exceeds 900000000')
    create_job_specification(config, job_id='validation', work_directory=str(work / 'validation'),
                             input_file=relative, input_sha256='0' * 64,
                             resource_overrides=data.get('resources'))
    files = _source_files(source)
    if config.storage.mode == 'staged':
        validate_manifest(files, **_limits(config))
    with open_regular(source, relative) as stream:
        template_bytes = stream.read()
    primary = next((f for f in files if f.path == relative), None)
    if primary is None or _digest(template_bytes) != primary.sha256:
        raise ValueError('input_file: source changed during validation')
    template = template_bytes.decode('utf-8')
    pattern = r'\{\{([A-Za-z_][A-Za-z0-9_]*)\}\}'
    names = set(re.findall(pattern, template))
    residue = re.sub(pattern, '', template)
    if '{{' in residue or '}}' in residue or names - set(parameters) - {'seed', 'repetition'}:
        raise ValueError('input_file: unknown or malformed placeholder')
    if set(parameters) - names or 'seed' not in names:
        raise ValueError('input_file: every parameter and seed require a placeholder')
    plan = SweepPlan(source, str(source), relative, data['template_version'], template,
                     parameters, repetitions, seed, combinations, jobs, data.get('resources'), files)
    if data.get('sampling') is not None:
        from .sampling import external_sampling, external_files
        schedule = external_sampling(data['sampling'])
        external_files(schedule, template_bytes, {item.path for item in files})
        plan = replace(plan, sampling=schedule)
    # Check all rendered sizes before any job is registered; iteration is capped by max_jobs.
    for coordinate in plan.coordinates():
        rendered = plan.render(coordinate)
        rendered_files = tuple(replace(f, size=len(rendered), sha256=_digest(rendered))
                               if f.path == relative else f for f in files)
        if plan.sampling is not None:
            generated = external_files(plan.sampling, rendered, {item.path for item in files})
            rendered_files += tuple(InputFile(path, len(content), _digest(content)) for path, content in generated.items())
        if config.storage.mode == 'staged':
            validate_manifest(rendered_files, **_limits(config))
    return plan


def _limits(config: ManagerConfig) -> dict:
    return dict(max_files=config.storage.max_input_files,
                max_file_bytes=config.storage.max_input_file_bytes,
                max_total_bytes=config.storage.max_input_total_bytes)


class SweepSubmissionError(ValueError):
    """Partial outcome, including committed job mappings and failed coordinate."""

    def __init__(self, result: dict, coordinate: dict, cause: BaseException) -> None:
        self.result = {**result, 'failed_coordinate': coordinate, 'error': str(cause) or type(cause).__name__}
        super().__init__(json.dumps(self.result))


def submit_sweep(store: Store, config: ManagerConfig, plan: SweepPlan) -> dict:
    """Publish verified per-job inputs and submit independent immutable snapshots."""
    sweep_id = uuid4().hex
    result = dict(sweep_id=sweep_id, combinations=plan.combinations, jobs=plan.jobs, submitted=[])
    coordinate = next(plan.coordinates())
    try:
        with tempfile.TemporaryDirectory(prefix='lammps-sweep-') as temporary:
            snapshot = Path(temporary) / 'source'
            # Shared mode has no configured transfer cap; the validated source sizes bound copying.
            limits = _limits(config) if config.storage.mode == 'staged' else dict(
                max_files=len(plan.source_manifest), max_file_bytes=max(f.size for f in plan.source_manifest),
                max_total_bytes=sum(f.size for f in plan.source_manifest))
            if _source_files(plan.source) != plan.source_manifest:
                raise ValueError('Source changed since sweep validation')
            files = snapshot_inputs(plan.source, snapshot, limits)
            if files != plan.source_manifest or _source_files(plan.source) != files:
                raise ValueError('Source changed since sweep validation')
            source_hash = _digest(json.dumps([asdict(f) for f in files], sort_keys=True,
                                            separators=(',', ':')).encode())
            for coordinate in plan.coordinates():
                rendered = plan.render(coordinate)
                rendered_hash = _digest(rendered)
                rendered_files = tuple(replace(f, size=len(rendered), sha256=rendered_hash)
                                       if f.path == plan.input_file else f for f in files)
                generated = {}
                if plan.sampling is not None:
                    from .sampling import external_files
                    generated = external_files(plan.sampling, rendered, {item.path for item in files})
                    rendered_files += tuple(InputFile(path, len(content), _digest(content)) for path, content in generated.items())
                job_id = uuid4().hex
                directory = Path(config.storage.work_root).resolve() / job_id
                spec = create_job_specification(config, job_id=job_id, work_directory=str(directory),
                    input_file=plan.input_file, input_sha256=rendered_hash, resource_overrides=plan.resources)
                provenance = SweepProvenance(sweep_id=sweep_id, template_version=plan.template_version,
                    source_locator=plan.source_locator, source_manifest=files, source_sha256=source_hash,
                    template_sha256=next(f.sha256 for f in files if f.path == plan.input_file),
                    rendered_sha256=rendered_hash, repetition_count=plan.repetitions, **coordinate)
                spec = replace(spec, storage_mode=config.storage.mode, input_manifest=rendered_files,
                               sweep=provenance, sampling=plan.sampling)
                directory.mkdir(parents=True, exist_ok=False)
                def fetch(index: int) -> BinaryIO:
                    item = rendered_files[index]
                    if item.path in generated:
                        return io.BytesIO(generated[item.path])
                    return io.BytesIO(rendered) if item.path == plan.input_file else open_regular(snapshot, item.path)
                publication_limits = _limits(config) if config.storage.mode == 'staged' else {}
                publish_inputs(directory / 'inputs', rendered_files, fetch, publication_limits)
                store.submit(spec)
                result['submitted'].append(dict(job_id=job_id, **coordinate))
    except (Exception, KeyboardInterrupt) as exc:
        raise SweepSubmissionError(result, coordinate, exc) from exc
    return result
