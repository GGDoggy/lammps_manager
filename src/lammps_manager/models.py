"""Immutable submission values, separate from configuration and runtime state."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Resources:
    mpi_ranks: int = 4
    omp_threads_per_rank: int = 1
    memory_gib: float = 8.0
    time_limit_seconds: int = 86400
    max_retries: int = 0

    @property
    def cpu_slots(self) -> int:
        return self.mpi_ranks * self.omp_threads_per_rank


@dataclass(frozen=True)
class InputFile:
    path: str
    size: int
    sha256: str


@dataclass(frozen=True)
class InfrastructureRecovery:
    max_retries: int = 1
    delay_seconds: float = 30


@dataclass(frozen=True)
class SweepProvenance:
    sweep_id: str
    template_version: str
    source_locator: str
    source_manifest: tuple[InputFile, ...]
    source_sha256: str
    template_sha256: str
    rendered_sha256: str
    combination_index: int
    selected_indices: dict
    selected_values: dict
    repetition_index: int
    repetition_count: int
    ordinal: int
    seed: int
    manifest_version: int = 1
    expansion_version: int = 1
    seed_derivation_version: int = 1


@dataclass(frozen=True)
class JobSpecification:
    job_id: str
    work_directory: str
    input_file: str
    input_sha256: str
    resources: Resources = field(default_factory=Resources)
    sweep: SweepProvenance | None = field(default=None, kw_only=True)
    storage_mode: str = 'shared'
    input_manifest: tuple[InputFile, ...] = ()
    infrastructure_recovery: InfrastructureRecovery = field(default_factory=lambda: InfrastructureRecovery(0))


def specification_from_dict(data: dict) -> JobSpecification:
    """Decode old shared snapshots and new immutable manifests."""
    data = dict(data)
    if data.get('sweep') is not None:
        metadata = dict(data['sweep'])
        metadata['source_manifest'] = tuple(InputFile(**item) for item in metadata['source_manifest'])
        data['sweep'] = SweepProvenance(**metadata)
    return JobSpecification(**{**data, 'resources': Resources(**data['resources']),
        'infrastructure_recovery': InfrastructureRecovery(**data.get('infrastructure_recovery', {'max_retries': 0})),
        'input_manifest': tuple(InputFile(**item) for item in data.get('input_manifest', ()))})
