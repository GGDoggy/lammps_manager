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


ENGINEERING_VERSION = 'engineering-abp-v1'
FRESH_STREAM_POLICY = 'fresh-random-per-segment-v1'
MAX_ENGINE_STEP = 2**63 - 2
MAX_RUN_COMMAND = 2**31 - 1


def engineering_parameters(recipe: Recipe) -> dict:
    """Immutable physical model and supported layout for the engineering contract."""
    if (not isinstance(recipe, Recipe) or recipe.name not in ('ideal-abp-2d-v1', 'wca-abp-2d-v1')
            or recipe.mode != 'statistical' or type(recipe.mpi_ranks) is not int
            or not 0 < recipe.mpi_ranks <= MAX_RUN_COMMAND or type(recipe.omp_threads) is not int or recipe.omp_threads != 1):
        raise ContinuationError('Engineering continuation requires ideal/WCA ABP, statistical mode, positive fixed ranks and one OpenMP thread')
    wca = recipe.name == 'wca-abp-2d-v1'
    return dict(particles=256 if wca else 1024, density=0.5 if wca else 1.0,
                dt=0.0001 if wca else 0.005, diameter=1.0, kBT=1.0,
                gamma_t=10.0, gamma_r=1.0, propulsion=10.0,
                epsilon=1.0 if wca else 0.0, sigma=1.0,
                cutoff=2**(1/6) if wca else 1.0,
                initialization='square-lattice' if wca else 'random-positions',
                preflight='two-step-restart-restored-initial-v1')


def render_engineering_input(recipe: Recipe, *, start: int, stop: int, total: int,
                             stream_seed: int, position_seed: int, orientation_seed: int) -> str:
    """Render fixed physical state with renewed RNG and a continuous global clock."""
    model = engineering_parameters(recipe)
    if (any(type(v) is not int for v in (start, stop, total, stream_seed, position_seed, orientation_seed))
            or not 0 <= start < stop <= total <= MAX_ENGINE_STEP
            or not all(0 < v < 900000000 for v in (stream_seed, position_seed, orientation_seed))):
        raise ContinuationError('Invalid engineering step bounds or seeds')
    wca = recipe.name == 'wca-abp-2d-v1'
    head = 'units lj\ndimension 2\natom_style hybrid sphere dipole\natom_modify sort 0 0.0\n'
    if start == 0:
        length = (model['particles'] / model['density']) ** .5
        head += f'boundary p p p\nregion box block 0 {length:.17g} 0 {length:.17g} -0.5 0.5\ncreate_box 1 box\n'
        head += ('lattice sq 0.5\ncreate_atoms 1 box\n' if wca else
                 f'create_atoms 1 random 1024 {position_seed} box\n')
        head += f'''set type 1 diameter 1.0
set type 1 density 1.0
set group all dipole/random {orientation_seed} 1.0
'''
    else:
        head += 'read_restart checkpoint.restart\n'
    # Reassert interactions and dt on both sides of a restart. read_restart keeps
    # atoms, box, images, dipoles and the global timestep; no reset_timestep.
    head += ('pair_style lj/cut 1.122462048309373\npair_coeff * * 1.0 1.0\npair_modify shift yes\n'
             if wca else 'pair_style zero 1.0\npair_coeff * *\n')
    head += f'''timestep {model['dt']}
neighbor 0.3 bin
neigh_modify delay 0 every 1 check no
fix integrate all brownian/sphere 1.0 {stream_seed} gamma_t 10.0 gamma_r 1.0 rng gaussian
fix active all propel/self dipole 10.0
thermo 100
thermo_style custom step time atoms
thermo_modify format float %.17g
dump trajectory all custom 20 trajectory.dump id xu yu mux muy muz
dump_modify trajectory sort id format float %.17g
'''
    if start == 0:
        # Smoke the actual styles/layout and binary restart in the SAME tracked
        # MPI session. Discard both smoke steps by restoring the saved step-zero
        # state before production integration. Never recreate atoms or equilibrate.
        dynamics_at = head.index('neighbor 0.3 bin\n')
        physics_at = head.index('pair_style ')
        physics = head[physics_at:dynamics_at]
        dynamics = head[dynamics_at:]
        restore = ('units lj\ndimension 2\natom_style hybrid sphere dipole\n'
                   'atom_modify sort 0 0.0\nread_restart ')
        head = (head[:dynamics_at] + dynamics.replace('trajectory.dump', 'engineering-smoke-first.dump')
                + 'run 0\nwrite_restart engineering-initial.restart\n'
                + 'run 1 start 0 stop 2\nwrite_restart engineering-smoke.restart\nclear\n'
                + restore + 'engineering-smoke.restart\n' + physics
                + dynamics.replace('trajectory.dump', 'engineering-smoke-second.dump')
                + 'run 1 start 0 stop 2\nclear\n'
                + restore + 'engineering-initial.restart\n' + physics + dynamics)
    remaining = stop - start
    # Bound renderer size as well as the integer accepted by each engine command.
    if (remaining + MAX_RUN_COMMAND - 1) // MAX_RUN_COMMAND > 100000:
        raise ContinuationError('Segment requires more than 100000 engine run commands')
    while remaining:
        chunk = min(remaining, MAX_RUN_COMMAND)
        head += f'run {chunk} start 0 stop {total}\n'
        remaining -= chunk
    return head + 'write_restart checkpoint.restart\n'


def engineering_digest(recipe: Recipe) -> str:
    """Bind the physical schema and both renderer branches to immutable metadata."""
    scripts = [render_engineering_input(recipe, start=a, stop=b, total=11,
               stream_seed=1, position_seed=2, orientation_seed=3) for a, b in ((0, 4), (4, 11))]
    return hashlib.sha256(json.dumps([engineering_parameters(recipe), scripts], sort_keys=True).encode()).hexdigest()


def require_contract(recipe: Recipe, *, build_fingerprint: str, total_steps: int,
                     segment_ends: tuple[int, ...], version: str | None = None,
                     parameters: dict | None = None) -> ValidatedContinuation:
    """Keep scientific admission intact; engineering compatibility is explicit."""
    from .abp_parameters import PARAMETERIZED_VERSION, validate_summary, parameterized_digest
    if parameters is not None and version != PARAMETERIZED_VERSION:
        raise ContinuationError('Custom physical parameters require engineering-abp-v2')
    if version is None:
        return require_validated(recipe, build_fingerprint=build_fingerprint,
                                 total_steps=total_steps, segment_ends=segment_ends)
    if version not in (ENGINEERING_VERSION, PARAMETERIZED_VERSION):
        raise ContinuationError('Unsupported continuation contract version')
    engineering_parameters(recipe)
    if version == PARAMETERIZED_VERSION:
        validate_summary(recipe, parameters)
    if build_fingerprint not in _ACCEPTED_ABP_BUILDS:
        raise ContinuationError('Unsupported engineering executable/library/platform fingerprint')
    if (type(total_steps) is not int or not 0 < total_steps <= MAX_ENGINE_STEP
            or type(segment_ends) is not tuple or not 0 < len(segment_ends) <= 100000
            or any(type(v) is not int for v in segment_ends)
            or any(a >= b for a, b in zip((0, *segment_ends[:-1]), segment_ends))
            or segment_ends[-1] != total_steps
            or any((b - a + MAX_RUN_COMMAND - 1) // MAX_RUN_COMMAND > 100000
                   for a, b in zip((0, *segment_ends[:-1]), segment_ends))):
        raise ContinuationError('Invalid engineering global step plan')
    if version == PARAMETERIZED_VERSION:
        import math
        if not math.isfinite(max(2, total_steps) * parameters['dt']):
            raise ContinuationError('Physical final time must be finite')
    return ValidatedContinuation(recipe, build_fingerprint,
                                 parameterized_digest(recipe, parameters) if version == PARAMETERIZED_VERSION else engineering_digest(recipe),
                                 total_steps, segment_ends, 'not_assessed')
