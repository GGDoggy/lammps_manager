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
class JobSpecification:
    job_id: str
    work_directory: str
    input_file: str
    input_sha256: str
    resources: Resources = field(default_factory=Resources)
