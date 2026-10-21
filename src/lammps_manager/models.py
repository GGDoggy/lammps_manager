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
class JobSpecification:
    job_id: str
    work_directory: str
    input_file: str
    input_sha256: str
    resources: Resources = field(default_factory=Resources)
    storage_mode: str = 'shared'
    input_manifest: tuple[InputFile, ...] = ()
    infrastructure_recovery: InfrastructureRecovery = field(default_factory=lambda: InfrastructureRecovery(0))


def specification_from_dict(data: dict) -> JobSpecification:
    """Decode old shared snapshots and new immutable manifests."""
    return JobSpecification(**{**data, 'resources': Resources(**data['resources']),
        'infrastructure_recovery': InfrastructureRecovery(**data.get('infrastructure_recovery', {'max_retries': 0})),
        'input_manifest': tuple(InputFile(**item) for item in data.get('input_manifest', ()))})
