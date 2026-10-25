"""Immutable global trajectory schedules, independent of segment boundaries."""

from collections import OrderedDict
from threading import Lock
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, localcontext
from bisect import bisect_right
import hashlib
import json
import math
import re

from .continuation import MAX_ENGINE_STEP, MAX_RUN_COMMAND

MAX_SAMPLES = 1000000
_VALIDATED: OrderedDict[bytes, None] = OrderedDict()
_VALIDATED_LOCK = Lock()


def _positive(value: object, name: str) -> Decimal:
    if type(value) not in (int, float):
        raise ValueError(f'{name} must be a finite positive number')
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f'{name} must be a finite positive number') from exc
    if not result.is_finite() or result <= 0:
        raise ValueError(f'{name} must be a finite positive number')
    return result


def normalize_sampling(settings: dict | None, *, dt: float, dr: float) -> dict:
    """Resolve explicit steps or Dr*t positions into a reproducible global list.

    Decimal arithmetic applies round-half-up to submitted decimal values, avoiding
    binary64 errors at half-step boundaries. Lists are never silently deduplicated.
    """
    timestep = _positive(dt, 'dt')
    diffusion = _positive(dr, 'Dr')
    if settings is None:
        settings = dict(unit='Dr', interval=0.01, count=30000)
    if not isinstance(settings, dict) or settings.get('unit') not in ('Dr', 'step'):
        raise ValueError('sampling.unit must be Dr or step')
    explicit = 'positions' in settings
    expected = {'unit', 'positions'} if explicit else {'unit', 'interval', 'count'}
    if set(settings) != expected:
        raise ValueError('sampling requires positions or interval/count')
    if explicit:
        positions = settings['positions']
        if not isinstance(positions, list) or not 1 <= len(positions) <= MAX_SAMPLES:
            raise ValueError(f'sampling.positions requires 1..{MAX_SAMPLES} values')
    else:
        count = settings['count']
        if type(count) is not int or not 1 <= count <= MAX_SAMPLES:
            raise ValueError(f'sampling.count must be in 1..{MAX_SAMPLES}')
        interval = _positive(settings['interval'], 'sampling.interval')
        if settings['unit'] == 'step' and type(settings['interval']) is not int:
            raise ValueError('step interval must be an integer')
        positions = None
    steps = []
    with localcontext() as context:
        context.prec = 80
        scale = diffusion * timestep
        values = positions if explicit else (interval * k for k in range(1, count + 1))
        previous_position = Decimal(0)
        for value in values:
            position = value if isinstance(value, Decimal) else _positive(value, 'sampling position')
            if position <= previous_position:
                raise ValueError('sampling positions must be strictly increasing')
            previous_position = position
            if settings['unit'] == 'step':
                if explicit and type(value) is not int:
                    raise ValueError('step positions must be integers')
                rounded = position
            else:
                rounded = (position / scale + Decimal('0.5')).to_integral_value(rounding=ROUND_FLOOR)
            if rounded < 1 or rounded > MAX_ENGINE_STEP:
                raise ValueError('sampling rounds to step zero or exceeds the engine step limit')
            step = int(rounded)
            if int(float(step)) != step:
                raise ValueError('Sampling step cannot be represented exactly by a LAMMPS equal-style variable')
            if steps and step <= steps[-1]:
                raise ValueError('sampling positions round to duplicate steps')
            steps.append(step)
    data = steps_bytes(steps)
    return dict(version=1, request=json.loads(json.dumps(settings)), dt=dt, Dr=dr, steps=steps,
                steps_sha256=hashlib.sha256(data).hexdigest())


def steps_bytes(steps: list[int]) -> bytes:
    """Canonical LAMMPS file-variable representation, one global step per line."""
    return ''.join(f'{step}\n' for step in steps).encode('ascii')


def segment_steps(schedule: dict, *, start: int, stop: int) -> bytes:
    """Select points in (start, stop]; a segment never repeats its initial frame."""
    if type(start) is not int or type(stop) is not int or not 0 <= start < stop <= MAX_ENGINE_STEP:
        raise ValueError('Invalid sampling segment bounds')
    points = schedule['steps']
    return steps_bytes(points[bisect_right(points, start):bisect_right(points, stop)])


def sampling_bytes(schedule: dict) -> bytes:
    """Canonical specification bytes suitable for the immutable input manifest."""
    return (json.dumps(schedule, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()


def sampling_files(schedule: dict, *, start: int, stop: int) -> dict[str, bytes]:
    """Generate bounded-segment includes and a beyond-stop EOF guard.

    LAMMPS evaluates next() after its final requested frame. The guard is not a
    sample and cannot be reached by this segment's production run.
    """
    segment_steps(schedule, start=start, stop=stop)  # Validate bounds even for empty schedules.
    if (stop - start + MAX_RUN_COMMAND - 1) // MAX_RUN_COMMAND > 100000:
        raise ValueError('Sampling requires more than 100000 engine run commands')
    files = {'sampling.json': sampling_bytes(schedule)}
    previous_dump = False
    any_dump = False
    index = 0
    cursor = start
    while cursor < stop:
        end = min(stop, cursor + MAX_RUN_COMMAND)
        suffix = f'-{index:06d}' if index else ''
        points = segment_steps(schedule, start=cursor, stop=end)
        output = ''
        if previous_dump:
            output += 'undump trajectory\nvariable next_snapshot delete\nvariable snapshot_steps delete\n'
        if cursor == 0:
            output += ('write_dump all custom initial.dump id x y ix iy mux muy muz '
                       'modify sort id format float %.17g\n')
        if points:
            output += (f'variable snapshot_steps file snapshot-steps{suffix}.txt\n'
                       'variable next_snapshot equal next(snapshot_steps)\n'
                       'dump trajectory all custom 1 trajectory.dump id x y ix iy mux muy muz\n'
                       'dump_modify trajectory every v_next_snapshot first no sort id format float %.17g\n')
            if any_dump:
                output += 'dump_modify trajectory append yes\n'
            any_dump = True
        guard = end + 1
        if points:
            # Equal-style variables use binary64 even on a bigint engine.
            guard_float = float(guard)
            if int(guard_float) <= end:
                guard_float = math.nextafter(guard_float, math.inf)
            guard = int(guard_float)
            if guard > MAX_ENGINE_STEP + 1:
                raise ValueError('No representable LAMMPS sampling guard beyond segment end')
        files[f'snapshot-steps{suffix}.txt'] = points + steps_bytes([guard])
        files[f'sampling{suffix}.lmp'] = output.encode()
        previous_dump = bool(points)
        cursor = end
        index += 1
    return files


def apply_sampling(script: str) -> str:
    """Replace only the built-in production dump; smoke remains independent."""
    old = ('dump trajectory all custom 20 trajectory.dump id xu yu mux muy muz\n'
           'dump_modify trajectory sort id format float %.17g\n')
    if script.count(old) != 1:
        raise ValueError('Built-in renderer lacks a unique production dump')
    prefix, production = script.split(old)
    count = 0

    def include_chunk(match: re.Match) -> str:
        nonlocal count
        include = f'include sampling-{count:06d}.lmp\n' if count else ''
        count += 1
        return include + match.group(0)

    production = re.sub(r'^run \d+ start 0 stop \d+\n', include_chunk, production, flags=re.MULTILINE)
    return prefix + 'include sampling.lmp\n' + production


def validate_sampling(schedule: dict) -> None:
    """Reject inconsistent stored lists, physics and hashes before execution."""
    if not isinstance(schedule, dict):
        raise ValueError('Invalid sampling specification')
    # Cache only a digest of fully validated content, never caller-owned lists.
    # Mutation changes the digest and must pass the original validation again.
    digest = hashlib.sha256(json.dumps(schedule, sort_keys=True, allow_nan=False).encode()).digest()
    with _VALIDATED_LOCK:
        if digest in _VALIDATED:
            _VALIDATED.move_to_end(digest)
            return
    try:
        expected = normalize_sampling(schedule['request'], dt=schedule['dt'], dr=schedule['Dr'])
    except (KeyError, TypeError) as exc:
        raise ValueError('Invalid sampling specification') from exc
    if 'stop_step' in schedule:
        stop = schedule['stop_step']
        if type(stop) is not int or not 0 < stop <= MAX_ENGINE_STEP:
            raise ValueError('Invalid sampling total_steps')
        expected['stop_step'] = stop
    if schedule != expected:
        raise ValueError('Sampling specification differs from its request')
    with _VALIDATED_LOCK:
        _VALIDATED[digest] = None
        if len(_VALIDATED) > 128:
            _VALIDATED.popitem(last=False)


def external_sampling(settings: dict) -> dict:
    """Normalize self-supplied script physics and its declared production end."""
    if not isinstance(settings, dict) or not {'dt', 'Dr', 'total_steps'} <= settings.keys():
        raise ValueError('Script sampling requires dt, Dr and total_steps')
    request = {key: value for key, value in settings.items() if key not in ('dt', 'Dr', 'total_steps')}
    schedule = normalize_sampling(request, dt=settings['dt'], dr=settings['Dr'])
    schedule['stop_step'] = settings['total_steps']
    validate_sampling(schedule)
    return schedule


def external_files(schedule: dict, script: bytes, existing: set[str]) -> dict[str, bytes]:
    """Require an explicit include contract instead of rewriting user scripts."""
    if 'include sampling.lmp' not in [line.strip() for line in script.decode().splitlines()]:
        raise ValueError('Sampling script must explicitly include sampling.lmp before its production run')
    files = sampling_files(schedule, start=0, stop=schedule['stop_step'])
    if existing & files.keys():
        raise ValueError('Source collides with manager-generated sampling files')
    return files
