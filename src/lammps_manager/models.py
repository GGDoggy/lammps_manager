"""Immutable submission values, separate from configuration and runtime state."""

from dataclasses import dataclass, field
import math


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
class MpiParticipant:
    """Immutable requested rank and memory share on one registered worker."""

    worker_id: str
    mpi_ranks: int
    memory_gib: float

    def __post_init__(self) -> None:
        if (not isinstance(self.worker_id, str) or not self.worker_id.strip()
                or self.worker_id in {'.','..'} or '/' in self.worker_id or '\\' in self.worker_id
                or any(c.isspace() or c == '\x00' for c in self.worker_id)):
            raise ValueError('MPI participant requires a nonempty worker identity without whitespace')
        if type(self.mpi_ranks) is not int or self.mpi_ranks <= 0:
            raise ValueError('MPI participant ranks must be a positive integer')
        if (type(self.memory_gib) not in (int, float) or not math.isfinite(self.memory_gib)
                or self.memory_gib <= 0):
            raise ValueError('MPI participant memory must be finite and positive')


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
    mpi_placement: tuple[MpiParticipant, ...] = field(default=(), kw_only=True)

    def __post_init__(self) -> None:
        if not isinstance(self.mpi_placement, tuple):
            raise ValueError('MPI placement must be an immutable tuple')
        if not self.mpi_placement:
            return
        if self.storage_mode != 'staged':
            raise ValueError('Multi-host MPI requires staged storage')
        if len(self.mpi_placement) < 2 or not all(isinstance(p, MpiParticipant) for p in self.mpi_placement):
            raise ValueError('Multi-host MPI requires at least two participant records')
        if len({p.worker_id for p in self.mpi_placement}) != len(self.mpi_placement):
            raise ValueError('MPI placement contains duplicate worker identities')
        if sum(p.mpi_ranks for p in self.mpi_placement) != self.resources.mpi_ranks:
            raise ValueError('MPI placement ranks must equal submitted total ranks')
        if not math.isclose(sum(p.memory_gib for p in self.mpi_placement),
                            self.resources.memory_gib, rel_tol=1e-12, abs_tol=1e-12):
            raise ValueError('MPI placement memory must equal submitted total memory')


def specification_from_dict(data: dict) -> JobSpecification:
    """Decode old shared snapshots and new immutable manifests."""
    data = dict(data)
    data['mpi_placement'] = tuple(MpiParticipant(**item) for item in data.get('mpi_placement', ()))
    if data.get('sweep') is not None:
        metadata = dict(data['sweep'])
        metadata['source_manifest'] = tuple(InputFile(**item) for item in metadata['source_manifest'])
        data['sweep'] = SweepProvenance(**metadata)
    return JobSpecification(**{**data, 'resources': Resources(**data['resources']),
        'infrastructure_recovery': InfrastructureRecovery(**data.get('infrastructure_recovery', {'max_retries': 0})),
        'input_manifest': tuple(InputFile(**item) for item in data.get('input_manifest', ()))})
