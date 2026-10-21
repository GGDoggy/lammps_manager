"""Explicit continuation candidates and a fail-closed scientific release gate.

This module does not parse arbitrary LAMMPS inputs or enable automatic splitting.
"""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import platform
import subprocess


class ContinuationError(ValueError):
    """The requested scientific continuation has not been validated."""


@dataclass(frozen=True)
class Recipe:
    """A fixed model, integrator and layout; arbitrary additions are unsupported."""

    name: str = "lj-nve-v1"
    mode: str = "exact"
    mpi_ranks: int = 1
    omp_threads: int = 1


SETUP = """units lj
atom_style atomic
boundary p p p
atom_modify sort 0 0.0
lattice fcc 0.8442
region box block 0 3 0 3 0 3
create_box 1 box
create_atoms 1 box
mass 1 1.0
pair_style lj/cut 2.5
pair_coeff 1 1 1.0 1.0 2.5
velocity all create 1.44 87287 loop all
"""
DYNAMICS = """neighbor 0.3 bin
neigh_modify delay 0 every 1 check no
timestep 0.005
fix integrate all nve
thermo 1
thermo_style custom step time atoms temp pe ke etotal press
thermo_modify format float %.17g
dump trajectory all custom 1 trajectory.dump id type x y z vx vy vz ix iy iz fx fy fz
dump_modify trajectory sort id format float %.17g
"""


def validate_candidate(recipe: Recipe) -> None:
    """Check the fixed comparison candidate, without claiming scientific acceptance."""
    candidates = (Recipe(), Recipe(name="ideal-abp-2d-v1", mode="statistical"))
    if (not isinstance(recipe, Recipe) or recipe not in candidates
            or type(recipe.mpi_ranks) is not int or type(recipe.omp_threads) is not int):
        raise ContinuationError(
            "Only lj-nve-v1 exact and ideal-abp-2d-v1 statistical are candidates, "
            "with one MPI rank and one OpenMP thread; other models/layouts are unvalidated"
        )


def render_input(*, start: int, stop: int, total: int) -> str:
    """Render a fixed candidate segment with one global clock and output numbering."""
    if any(type(value) is not int for value in (start, stop, total)):
        raise ContinuationError("step bounds must be integers")
    if not 0 <= start < stop <= total <= 1000:
        raise ContinuationError("require 0 <= start < stop <= total <= 1000")
    prefix = SETUP if start == 0 else (
        "units lj\natom_style atomic\natom_modify sort 0 0.0\n"
        "read_restart checkpoint.restart\n"
    )
    dynamics = DYNAMICS if start == 0 else DYNAMICS.replace("timestep 0.005\n", "")
    return (prefix + dynamics + f"run {stop - start} start 0 stop {total}\n"
            "write_restart checkpoint.restart\n")


def recipe_digest() -> str:
    """Bind evidence to the actual renderer rather than a name alone."""
    return hashlib.sha256((SETUP + DYNAMICS + render_input(start=0, stop=1, total=2)
                           + render_input(start=1, stop=2, total=2)).encode()).hexdigest()


@dataclass(frozen=True)
class ValidatedContinuation:
    """The exact scientific scope a segment launcher must enforce."""

    recipe: Recipe
    build_fingerprint: str
    recipe_sha256: str
    total_steps: int
    segment_ends: tuple[int, ...]
    evidence: str


# Filled only from reviewed repository evidence, never from caller-supplied reports.
_ACCEPTED_ABP_BUILDS: dict[str, str] = {
    "aa8a1af68131f97d13fd4959031c557681b46c9eee23dddef26ca75c7eae0695":
        "docs/verification/t017-continuation.json",
}
_ACCEPTED_ABP_RECIPE_SHA256 = "12c14ef448256a135d024580e1b0b3f2f5108dacde769b750b54c5f06c3b6fa8"


def require_validated(recipe: Recipe, *, build_fingerprint: str,
                      total_steps: int | None = None,
                      segment_ends: tuple[int, ...] | None = None) -> ValidatedContinuation:
    """Accept only a released model/build/step plan; reject every other request.

    A caller-supplied passing report must never self-authorize production use.
    Checkpoint publication and queued segments enforce this returned scope.
    """
    validate_candidate(recipe)
    if (recipe == Recipe(name="ideal-abp-2d-v1", mode="statistical")
            and build_fingerprint in _ACCEPTED_ABP_BUILDS
            and abp_recipe_digest() == _ACCEPTED_ABP_RECIPE_SHA256
            and type(total_steps) is int and total_steps == 2000
            and type(segment_ends) is tuple and segment_ends in ((2000,), (600, 1400, 2000))
            and all(type(step) is int for step in segment_ends)):
        return ValidatedContinuation(recipe, build_fingerprint, abp_recipe_digest(),
                                     total_steps, segment_ends,
                                     _ACCEPTED_ABP_BUILDS[build_fingerprint])
    raise ContinuationError(
        f"No released continuation acceptance for {recipe.name} on the requested "
        "build/step plan; exact or other stochastic models, layouts, parameters "
        "and segment ranges require new scientific validation"
    )


def render_abp_input(*, start: int, stop: int, total: int,
                     stream_seed: int, position_seed: int, orientation_seed: int) -> str:
    """Render a noninteracting 2D ABP statistical candidate.

    Renewing the Brownian RNG is intentional statistical continuation, never exact.
    The caller retains every seed and the global run bounds as provenance.
    """
    if any(type(x) is not int for x in
           (start, stop, total, stream_seed, position_seed, orientation_seed)):
        raise ContinuationError('ABP bounds and seeds must be integers')
    if not 0 <= start < stop <= total <= 4000:
        raise ContinuationError('require 0 <= start < stop <= total <= 4000')
    if not all(0 < x < 900000000 for x in (stream_seed, position_seed, orientation_seed)):
        raise ContinuationError('ABP seeds must be in 1..899999999')
    if start == 0:
        prefix = f'''units lj
dimension 2
atom_style hybrid sphere dipole
boundary p p p
atom_modify sort 0 0.0
region box block 0 32 0 32 -0.5 0.5
create_box 1 box
create_atoms 1 random 1024 {position_seed} box
set type 1 diameter 1.0
set type 1 density 1.0
set group all dipole/random {orientation_seed} 1.0
pair_style zero 1.0
pair_coeff * *
timestep 0.005
'''
    else:
        prefix = '''units lj
dimension 2
atom_style hybrid sphere dipole
atom_modify sort 0 0.0
read_restart checkpoint.restart
'''
    return prefix + f'''neighbor 0.3 bin
neigh_modify delay 0 every 1 check no
fix integrate all brownian/sphere 1.0 {stream_seed} gamma_t 10.0 gamma_r 1.0 rng gaussian
fix active all propel/self dipole 10.0
thermo 100
thermo_style custom step time atoms
thermo_modify format float %.17g
dump trajectory all custom 20 trajectory.dump id xu yu mux muy muz
dump_modify trajectory sort id format float %.17g
run {stop - start} start 0 stop {total}
write_restart checkpoint.restart
'''


def abp_recipe_digest() -> str:
    """Bind statistical evidence to both initial and resumed script bodies."""
    scripts = [render_abp_input(start=start, stop=stop, total=2000,
                               stream_seed=1, position_seed=2, orientation_seed=3)
               for start, stop in ((0, 2000), (0, 600), (600, 1400), (1400, 2000))]
    return hashlib.sha256("\n".join(scripts).encode()).hexdigest()


def fingerprint_build(executable: Path) -> str:
    """Inventory the Linux executable/linked-library dependencies and CPU platform.

    Call explicitly on the execution host. This is a compatibility identity, not
    remote attestation, a result report validator or authorization to run jobs.
    """
    def file_hash(path: Path) -> str:
        result = hashlib.sha256()
        with path.open('rb') as source:
            for block in iter(lambda: source.read(1024 * 1024), b''):
                result.update(block)
        return result.hexdigest()

    try:
        executable = executable.resolve(strict=True)
        linked = subprocess.run(['ldd', str(executable)], check=True, capture_output=True,
                                text=True, timeout=15)
        libraries = {str(Path(word).resolve()): file_hash(Path(word))
                     for word in linked.stdout.split()
                     if word.startswith('/') and Path(word).is_file()}
        if not libraries:
            raise ContinuationError('no linked-library inventory; build is unvalidated')
        models = sorted({line.split(':', 1)[1].strip()
                         for line in Path('/proc/cpuinfo').read_text().splitlines()
                         if line.startswith('model name')})
        identity = dict(executable_sha256=file_hash(executable),
                        libraries_sha256=libraries, platform=platform.platform(),
                        cpu_models=models, machine=platform.machine())
        return hashlib.sha256(json.dumps(identity, sort_keys=True,
                                         separators=(',', ':')).encode()).hexdigest()
    except (OSError, subprocess.SubprocessError) as exc:
        raise ContinuationError(f'cannot inventory continuation build: {exc}') from exc
