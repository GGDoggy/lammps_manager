"""Versioned per-run ABP physical parameters and fixed LAMMPS input generation."""
import hashlib
import json
import math
import re

from .continuation import Recipe, engineering_parameters, MAX_ENGINE_STEP, MAX_RUN_COMMAND

PARAMETERIZED_VERSION = 'engineering-abp-v2'
NUMERIC_FIELDS = ('density', 'epsilon', 'sigma', 'kBT', 'gamma_t', 'gamma_r', 'propulsion', 'dt')
MAX_PARTICLES = 1000000


def canonical(value: object) -> str:
    """Finite stable JSON for parameter identities and immutable input files."""
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def sha256(value: object) -> str:
    """Bind structured initialization data without copying arrays into checkpoints."""
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _number(value: object, name: str, *, zero: bool = False) -> float:
    if type(value) not in (int, float):
        raise ValueError(f'{name} must be a finite number')
    try:
        result = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f'{name} must be a finite number') from exc
    if not math.isfinite(result) or (result < 0 if zero else result <= 0):
        raise ValueError(f'{name} must be finite and {"nonnegative" if zero else "positive"}')
    return 0.0 if result == 0 else result


def normalize_parameters(recipe: Recipe, parameters: dict) -> dict:
    """Resolve defaults and reject unsupported data before allocating a run."""
    defaults = engineering_parameters(recipe)
    allowed = {'particles', *NUMERIC_FIELDS, 'initial_positions', 'initial_orientations'}
    if not isinstance(parameters, dict) or parameters.keys() - allowed:
        raise ValueError('Unknown or invalid model_parameters fields')
    n = parameters.get('particles', defaults['particles'])
    if type(n) is not int or not 1 <= n <= MAX_PARTICLES:
        raise ValueError(f'particles must be an integer in 1..{MAX_PARTICLES}')
    result = dict(particles=n)
    for field in NUMERIC_FIELDS:
        result[field] = _number(parameters.get(field, defaults[field]), field,
                                zero=field in ('epsilon', 'propulsion'))
    if recipe.name == 'ideal-abp-2d-v1' and result['epsilon'] != 0:
        raise ValueError('ideal ABP has no pair interactions and requires epsilon=0; use WCA for nonzero epsilon')
    try:
        length = math.sqrt(n / result['density'])
        cutoff = result['sigma'] * (2**(1/6) if recipe.name == 'wca-abp-2d-v1' else 1)
    except (OverflowError, ValueError) as exc:
        raise ValueError('Particle count/density produces an invalid box') from exc
    if not math.isfinite(length) or length <= 0 or not math.isfinite(cutoff) or cutoff <= 0:
        raise ValueError('Physical parameters produce invalid box/cutoff geometry')
    positions = parameters.get('initial_positions', {'mode': 'square-lattice' if recipe.name == 'wca-abp-2d-v1' else 'random'})
    if not isinstance(positions, dict) or positions.get('mode') not in ('square-lattice', 'random', 'explicit'):
        raise ValueError('initial_positions requires square-lattice, random or explicit mode')
    mode = positions['mode']
    if set(positions) != ({'mode', 'coordinates'} if mode == 'explicit' else {'mode'}):
        raise ValueError('Invalid initial_positions fields')
    normalized_positions = dict(mode=mode)
    if mode == 'explicit':
        coordinates = positions['coordinates']
        if not isinstance(coordinates, list) or len(coordinates) != n:
            raise ValueError('initial_positions.coordinates must contain one [x,y] pair per particle')
        normalized = []
        for row in coordinates:
            if not isinstance(row, list) or len(row) != 2:
                raise ValueError('Each initial position must be [x,y]')
            point = [_number(value, 'initial coordinate', zero=True) for value in row]
            if any(value >= length for value in point):
                raise ValueError('Initial coordinates must lie in [0, box_length)')
            normalized.append(point)
        if len(set(map(tuple, normalized))) != n:
            raise ValueError('Initial coordinates must be distinct within the periodic box')
        normalized_positions['coordinates'] = normalized
    orientations = parameters.get('initial_orientations', {'mode': 'random'})
    if not isinstance(orientations, dict) or orientations.get('mode') not in ('random', 'aligned', 'explicit'):
        raise ValueError('initial_orientations requires random, aligned or explicit mode')
    mode = orientations['mode']
    if set(orientations) != ({'mode', 'angle'} if mode == 'aligned' else {'mode', 'angles'} if mode == 'explicit' else {'mode'}):
        raise ValueError('Invalid initial_orientations fields')
    normalized_orientations = dict(mode=mode)
    # Angles are finite radians; negative angles are meaningful.
    def angle(value: object) -> float:
        if type(value) not in (int, float):
            raise ValueError('Initial angles must be finite radians')
        try:
            value = float(value)
        except (OverflowError, ValueError) as exc:
            raise ValueError('Initial angles must be finite radians') from exc
        if not math.isfinite(value):
            raise ValueError('Initial angles must be finite radians')
        return 0.0 if value == 0 else value
    if mode == 'aligned':
        normalized_orientations['angle'] = angle(orientations['angle'])
    elif mode == 'explicit':
        values = orientations['angles']
        if not isinstance(values, list) or len(values) != n:
            raise ValueError('initial_orientations.angles must contain one angle per particle')
        normalized_orientations['angles'] = [angle(value) for value in values]
    result.update(initial_positions=normalized_positions, initial_orientations=normalized_orientations)
    return result


def model_summary(recipe: Recipe, parameters: dict) -> dict:
    """Keep runtime physics and initialization digests in bounded provenance."""
    full = normalize_parameters(recipe, parameters)
    result = dict(full)
    for field, key in (('initial_positions', 'coordinates'), ('initial_orientations', 'angles')):
        if key in full[field]:
            result[field] = dict(mode='explicit', count=full['particles'], sha256=sha256(full[field][key]))
    result.update(box_length=math.sqrt(full['particles'] / full['density']),
        diameter=full['sigma'], cutoff=full['sigma'] * (2**(1/6) if recipe.name == 'wca-abp-2d-v1' else 1),
        parameters_sha256=sha256(full), preflight='two-step-restart-restored-initial-v1')
    return result


def validate_summary(recipe: Recipe, summary: dict) -> None:
    """Validate bounded checkpoint physics; full initialization is checked at launch."""
    fields = {'particles', *NUMERIC_FIELDS, 'initial_positions', 'initial_orientations'}
    if not isinstance(summary, dict) or set(summary) != fields | {'box_length', 'diameter', 'cutoff', 'parameters_sha256', 'preflight'}:
        raise ValueError('Invalid parameterized model summary')
    basic = {key: summary[key] for key in fields}
    explicit = False
    for field in ('initial_positions', 'initial_orientations'):
        value = basic[field]
        if isinstance(value, dict) and value.get('mode') == 'explicit':
            if (set(value) != {'mode', 'count', 'sha256'} or type(value['count']) is not int
                    or value['count'] != summary['particles'] or not isinstance(value['sha256'], str)
                    or not re.fullmatch('[0-9a-f]{64}', value['sha256'])):
                raise ValueError('Invalid explicit initializer digest')
            basic[field] = {'mode': 'random'}
            explicit = True
    expected = model_summary(recipe, basic)
    if (any(summary[key] != expected[key] for key in summary if key not in ('initial_positions', 'initial_orientations', 'parameters_sha256'))
            or not isinstance(summary['parameters_sha256'], str)
            or not re.fullmatch('[0-9a-f]{64}', summary['parameters_sha256'])
            or (not explicit and summary != expected)):
        raise ValueError('Parameterized model summary does not match physical parameters')


def render_initialization(recipe: Recipe, parameters: dict, *, position_seed: int, orientation_seed: int) -> str:
    """Generate only validated atom creation/orientations, applied once per run."""
    model = normalize_parameters(recipe, parameters)
    if any(type(v) is not int or not 0 < v < 900000000 for v in (position_seed, orientation_seed)):
        raise ValueError('Initialization seeds must be in 1..899999999')
    n, positions = model['particles'], model['initial_positions']
    length = math.sqrt(n / model['density'])
    lines = []
    if positions['mode'] == 'random':
        overlap = f" overlap {model['sigma']:.17g} maxtry 1000" if recipe.name == 'wca-abp-2d-v1' else ''
        lines.append(f'create_atoms 1 random {n} {position_seed} box{overlap} units box')
    else:
        if positions['mode'] == 'explicit':
            points = positions['coordinates']
        else:
            nx = math.isqrt(n)
            nx += nx * nx < n
            points = (((index % nx + .5) * length / nx, (index // nx + .5) * length / nx) for index in range(n))
        lines.extend(f'create_atoms 1 single {x:.17g} {y:.17g} 0 units box' for x, y in points)
    lines += [f'if "$(count(all)) != {n}" then "quit 1"',
              f"set type 1 diameter {model['sigma']:.17g}", 'set type 1 density 1.0']
    orientation = model['initial_orientations']
    if orientation['mode'] == 'random':
        lines.append(f'set group all dipole/random {orientation_seed} 1.0')
    elif orientation['mode'] == 'aligned':
        theta = orientation['angle']
        lines.append(f'set group all dipole {math.cos(theta):.17g} {math.sin(theta):.17g} 0')
    else:
        lines.extend(f'set atom {index} dipole {math.cos(theta):.17g} {math.sin(theta):.17g} 0'
                     for index, theta in enumerate(orientation['angles'], 1))
    return '\n'.join(lines) + '\n'


def render_parameterized_input(recipe: Recipe, summary: dict, *, start: int, stop: int, total: int,
                               stream_seed: int, position_seed: int, orientation_seed: int) -> str:
    """Fixed recipe renderer; input manifests bind the generated initializer file."""
    validate_summary(recipe, summary)
    if (any(type(v) is not int for v in (start, stop, total, stream_seed, position_seed, orientation_seed))
            or not 0 <= start < stop <= total <= MAX_ENGINE_STEP
            or any(not 0 < v < 900000000 for v in (stream_seed, position_seed, orientation_seed))):
        raise ValueError('Invalid parameterized bounds or seeds')
    common = 'units lj\ndimension 2\natom_style hybrid sphere dipole\natom_modify sort 0 0.0\n'
    if start == 0:
        length = summary['box_length']
        prefix = common + f'boundary p p p\nregion box block 0 {length:.17g} 0 {length:.17g} -0.5 0.5 units box\ncreate_box 1 box\ninclude initial-state.lmp\n'
    else:
        prefix = common + 'read_restart checkpoint.restart\n'
    if recipe.name == 'wca-abp-2d-v1':
        physics = f"pair_style lj/cut {summary['cutoff']:.17g}\npair_coeff * * {summary['epsilon']:.17g} {summary['sigma']:.17g}\npair_modify shift yes\n"
    else:
        physics = f"pair_style zero {summary['cutoff']:.17g}\npair_coeff * *\n"
    physics += f"timestep {summary['dt']:.17g}\n"
    dynamics = f'''neighbor {0.3 * summary['sigma']:.17g} bin
neigh_modify delay 0 every 1 check no
fix integrate all brownian/sphere {summary['kBT']:.17g} {stream_seed} gamma_t {summary['gamma_t']:.17g} gamma_r {summary['gamma_r']:.17g} rng gaussian
fix active all propel/self dipole {summary['propulsion']:.17g}
thermo 100
thermo_style custom step time atoms
thermo_modify format float %.17g
dump trajectory all custom 20 trajectory.dump id xu yu mux muy muz
dump_modify trajectory sort id format float %.17g
'''
    if start == 0:
        head = (prefix + physics + dynamics.replace('trajectory.dump', 'engineering-smoke-first.dump')
            + 'run 0\nwrite_restart engineering-initial.restart\nrun 1 start 0 stop 2\n'
            + 'write_restart engineering-smoke.restart\nclear\n'
            + common + 'read_restart engineering-smoke.restart\n' + physics
            + dynamics.replace('trajectory.dump', 'engineering-smoke-second.dump')
            + 'run 1 start 0 stop 2\nclear\n'
            + common + 'read_restart engineering-initial.restart\n' + physics + dynamics)
    else:
        head = prefix + physics + dynamics
    remaining = stop - start
    if (remaining + MAX_RUN_COMMAND - 1) // MAX_RUN_COMMAND > 100000:
        raise ValueError('Segment requires more than 100000 engine run commands')
    while remaining:
        chunk = min(remaining, MAX_RUN_COMMAND)
        head += f'run {chunk} start 0 stop {total}\n'
        remaining -= chunk
    return head + 'write_restart checkpoint.restart\n'


def parameterized_digest(recipe: Recipe, summary: dict) -> str:
    """Bind model and renderer branches while preserving every prior v1 digest."""
    scripts = [render_parameterized_input(recipe, summary, start=a, stop=b, total=11,
               stream_seed=1, position_seed=2, orientation_seed=3) for a, b in ((0, 4), (4, 11))]
    # Include representative initialization output to bind the generation rules
    # without placing per-particle arrays in each checkpoint.
    sample = normalize_parameters(recipe, {'particles': 4})
    initializer = render_initialization(recipe, sample, position_seed=2, orientation_seed=3)
    return sha256([PARAMETERIZED_VERSION, summary, scripts, initializer])
