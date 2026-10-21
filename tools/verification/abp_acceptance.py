"""Explicit ideal-ABP statistical comparison; never part of default tests.

A pilot freezes confirmatory sample size before any confirmatory simulations.
No result applies to interacting ABPs or another integrator/build/layout.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
from statistics import NormalDist, mean, stdev
from collections import Counter
import subprocess

from lammps_manager.continuation import abp_recipe_digest, fingerprint_build, render_abp_input
from continuation_acceptance import digest, execute

# Predeclared before pilot/confirmatory measurements. No post-hoc margin changes.
PROTOCOL = dict(version=1, model='ideal-abp-2d-v1', particles=1024,
                dt=0.005, steps=2000, boundaries=[600, 1400],
                temperature=1.0, gamma_t=10.0, gamma_r=1.0, propulsion_force=10.0,
                box_length=32.0, pilot_pairs=8, minimum_pairs=64, maximum_pairs=256,
                family_confidence=0.95, target_power=0.90,
                pilot_variance_inflation=4.0, stationary_margin=0.03,
                relative_dynamics_margin=0.05,
                analysis='paired replica-level means; simultaneous two-sided Student intervals',
                seed_namespace='t017-abp-v1-20261021',
                output_policy='check physical boundary state then retain one global frame',
                equilibration='none: initial independent uniform positions/orientations are stationary')


def seed(phase: str, arm: str, replica: int, segment: str, used: set[int]) -> int:
    """Derive auditable streams; reject collisions rather than silently reuse seeds."""
    identity = [PROTOCOL['seed_namespace'], phase, arm, replica, segment]
    value = int.from_bytes(hashlib.sha256(json.dumps(identity).encode()).digest()[:8], 'big')
    value = value % 899999999 + 1
    if value in used:
        raise ValueError('seed collision: select a new versioned protocol before running')
    used.add(value)
    return value


def frames(path: Path) -> dict[int, list[list[float]]]:
    result = {}
    text = path.read_text()
    if not text.startswith('ITEM: TIMESTEP\n'):
        raise ValueError('missing dump')
    for block in text.split('ITEM: TIMESTEP\n')[1:]:
        lines = block.splitlines()
        step = int(lines[0])
        if (len(lines) != 1032 or lines[1:3] != ['ITEM: NUMBER OF ATOMS', '1024']
                or lines[7] != 'ITEM: ATOMS id xu yu mux muy muz' or step in result):
            raise ValueError('malformed ABP dump')
        if lines[3] != 'ITEM: BOX BOUNDS pp pp pp':
            raise ValueError('ABP box boundary changed')
        bounds = [[float(x) for x in row.split()] for row in lines[4:7]]
        if bounds != [[0.0, 32.0], [0.0, 32.0], [-0.5, 0.5]]:
            raise ValueError('ABP box size changed')
        atoms = []
        for index, row in enumerate(lines[8:], 1):
            values = [float(x) for x in row.split()]
            if len(values) != 6 or values[0] != index or not all(map(math.isfinite, values)):
                raise ValueError('malformed ABP atom')
            if abs(math.hypot(values[3], values[4]) - 1) > 1e-10 or abs(values[5]) > 1e-12:
                raise ValueError('orientation not planar/unit length')
            atoms.append(values[1:])
        result[step] = atoms
    return result


def msd_theory(time: float) -> float:
    # Free 2D ABP: D_t=0.1, D_r=1, v_0=1. Used only to normalize margins.
    return 0.4 * time + 2 * (time - (1 - math.exp(-time)))


def observables(data: dict[int, list[list[float]]]) -> dict[str, float]:
    """Summarize correlated frames inside each independent replica, never as n."""
    if set(data) != set(range(0, 2001, 20)):
        raise ValueError('global ABP frame coverage mismatch')
    result = {}
    samples = [atom for step in range(0, 2001, 20) for atom in data[step]]
    for order in (1, 2):
        for kind, func in [('cos', math.cos), ('sin', math.sin)]:
            result[f'orientation-{kind}{order}'] = mean(
                func(order * math.atan2(a[3], a[2])) for a in samples)
    for axis in (0, 1):
        for kind, func in [('cos', math.cos), ('sin', math.sin)]:
            result[f'position-{axis}-{kind}'] = mean(func(2 * math.pi * a[axis] / 32) for a in samples)
    angles = Counter(int((math.atan2(a[3], a[2]) % (2*math.pi)) / (math.pi/4)) for a in samples)
    positions = Counter((int(a[0] % 32 / 8), int(a[1] % 32 / 8)) for a in samples)
    for index in range(8):
        result[f'orientation-bin-{index}'] = angles[index]/len(samples)
    for x in range(4):
        for y in range(4):
            result[f'position-bin-{x}-{y}'] = positions[x,y]/len(samples)
    for lag in (200, 400):
        time = lag * 0.005
        pairs = [(a, b) for step in range(0, 2001-lag, 20)
                 for a, b in zip(data[step], data[step+lag])]
        result[f'msd-{time}'] = mean((b[0]-a[0])**2 + (b[1]-a[1])**2 for a, b in pairs) / msd_theory(time)
        result[f'acf-{time}'] = mean(a[2]*b[2]+a[3]*b[3] for a, b in pairs) / math.exp(-time)
    # Windows before, crossing and after EACH boundary are separate endpoints.
    for boundary in (600, 1400):
        for name, lo, hi in [('before', boundary-40, boundary-20),
                             ('cross', boundary-20, boundary+20),
                             ('after', boundary+20, boundary+40)]:
            time = (hi-lo)*0.005
            pairs = list(zip(data[lo], data[hi]))
            result[f'boundary-{boundary}-{name}-msd'] = mean(
                (b[0]-a[0])**2+(b[1]-a[1])**2 for a,b in pairs) / msd_theory(time)
            result[f'boundary-{boundary}-{name}-acf'] = mean(
                a[2]*b[2]+a[3]*b[3] for a,b in pairs) / math.exp(-time)
    return result


def margin(name: str) -> float:
    return (PROTOCOL['relative_dynamics_margin'] if name.startswith(('msd-', 'acf-', 'boundary-'))
            else PROTOCOL['stationary_margin'])


def student_quantile(probability: float, df: int) -> float:
    """Invert the Student CDF by Simpson integration (positive upper quantiles)."""
    coefficient = math.exp(math.lgamma((df+1)/2)-math.lgamma(df/2)) / math.sqrt(df*math.pi)
    def cdf(x):
        intervals = 2048
        width = x/intervals
        total = 0.0
        for i in range(intervals+1):
            weight = 1 if i in (0,intervals) else (4 if i % 2 else 2)
            total += weight * coefficient * (1+(i*width)**2/df)**(-(df+1)/2)
        return 0.5 + width*total/3
    lo, hi = 0.0, 100.0
    for _ in range(45):
        mid = (lo+hi)/2
        if cdf(mid) < probability:
            lo = mid
        else:
            hi = mid
    return (lo+hi)/2


def choose_size(pilot: list[dict[str, float]]) -> dict:
    count = len(pilot[0])
    z = NormalDist().inv_cdf(1-0.05/(2*count)) + NormalDist().inv_cdf(0.90)
    needed = {name: math.ceil(4 * stdev([row[name] for row in pilot])**2 * z*z / margin(name)**2)
              for name in pilot[0]}
    pairs = max(PROTOCOL['minimum_pairs'], max(needed.values()))
    return dict(pairs=pairs, required_by_observable=needed,
                feasible=pairs <= PROTOCOL['maximum_pairs'])


def analyze(differences: list[dict[str, float]]) -> dict:
    n = len(differences)
    critical = student_quantile(1-0.05/(2*len(differences[0])), n-1)
    results = {}
    for name in differences[0]:
        values = [row[name] for row in differences]
        average = mean(values)
        half = critical * stdev(values)/math.sqrt(n)
        bound = margin(name)
        results[name] = dict(difference=average, lower=average-half, upper=average+half,
                             margin=bound, equivalent=abs(average)+half < bound)
    return dict(pairs=n, simultaneous_critical=critical, endpoints=results,
                passed=all(row['equivalent'] for row in results.values()))


def run_pair(root: Path, executable: str, phase: str, replica: int,
             used: set[int], report: dict) -> dict[str, float]:
    position_seed = seed(phase,'setup',replica,'positions',used)
    orientation_seed = seed(phase,'setup',replica,'orientations',used)
    records = {}
    starts = {}
    for arm, ends in [('reference',[2000]),('segmented',[600,1400,2000])]:
        complete = {}
        previous = None
        start = 0
        streams = []
        for stop in ends:
            directory = root/f'{phase}-{replica}-{arm}-{start}-{stop}'
            directory.mkdir()
            stream = seed(phase,arm,replica,str(start),used)
            streams.append(stream)
            if previous:
                shutil.copyfile(previous/'checkpoint.restart',directory/'checkpoint.restart')
            (directory/'in.lmp').write_text(render_abp_input(start=start,stop=stop,total=2000,
                stream_seed=stream,position_seed=position_seed,orientation_seed=orientation_seed))
            execute([executable,'-in','in.lmp'],directory,report['commands'])
            # The deployed build must preserve elapsed time, not only step numbers.
            active = False
            clock_steps = []
            for line in (directory/'log.lammps').read_text().splitlines():
                if line.split() == ['Step','Time','Atoms']:
                    active = True
                    continue
                if line.startswith('Loop time'):
                    active = False
                if active:
                    fields = line.split()
                    if (len(fields) != 3 or int(fields[2]) != 1024
                            or not math.isclose(float(fields[1]),int(fields[0])*.005,
                                                rel_tol=1e-14,abs_tol=1e-15)):
                        raise ValueError('ABP global time reset')
                    clock_steps.append(int(fields[0]))
            if set(clock_steps) != set(range(start,stop+1,100)):
                raise ValueError('ABP global clock coverage mismatch')
            data = frames(directory/'trajectory.dump')
            increments = [round(math.atan2(a[2]*b[3]-a[3]*b[2],
                                          a[2]*b[2]+a[3]*b[3]), 8)
                          for a,b in zip(data[start],data[start+20])]
            noise_digest = hashlib.sha256(json.dumps(increments).encode()).hexdigest()
            if noise_digest in report['angular_increment_fingerprints']:
                raise ValueError('repeated sampled angular noise stream')
            report['angular_increment_fingerprints'].append(noise_digest)
            if set(data) != set(range(start,stop+1,20)):
                raise ValueError('ABP segment clock/coverage changed')
            # Restarts must preserve positions and dipoles exactly, despite RNG renewal.
            if previous and complete[start] != data[start]:
                raise ValueError('physical ABP boundary state changed')
            if not previous:
                starts[arm] = data[0]
            complete.update(data)
            previous,start = directory,stop
        records[arm] = observables(complete)
        report['replicas'].append(dict(phase=phase,arm=arm,replica=replica,
            position_seed=position_seed,orientation_seed=orientation_seed,stream_seeds=streams,
            observables=records[arm]))
    if starts['reference'] != starts['segmented']:
        raise ValueError('replica starting states differ')
    return {name: records['segmented'][name]-value for name,value in records['reference'].items()}


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--lammps',type=Path,required=True)
    parser.add_argument('--root',type=Path,required=True)
    args=parser.parse_args()
    executable=args.lammps.resolve(strict=True)
    root=args.root.absolute()
    root.mkdir(mode=0o700)
    report=dict(protocol=PROTOCOL,executable=str(executable),executable_sha256=digest(executable),
                platform=platform.platform(),
                recipe_sha256=abp_recipe_digest(), commands=[],replicas=[],passed=False,
                angular_increment_fingerprints=[],
                production_approved=False)
    (root/'protocol.json').write_text(json.dumps(PROTOCOL,indent=2)+'\n')
    used=set()
    def save():
        (root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    try:
        report['build_fingerprint'] = fingerprint_build(executable)
        build=subprocess.run([str(executable),'-help'],capture_output=True,timeout=15,
                             env={**os.environ,'OMP_NUM_THREADS':'1','DISPLAY':''})
        (root/'build.txt').write_bytes(build.stdout+build.stderr)
        if build.returncode or any(x not in build.stdout.decode() for x in ('brownian/sphere','propel/self')):
            raise ValueError('ABP styles unavailable')
        pilot=[]
        for replica in range(PROTOCOL['pilot_pairs']):
            pilot.append(run_pair(root,str(executable),'pilot',replica,used,report))
            save()
        report['sample_size']=choose_size(pilot)
        (root/'frozen-sample-size.json').write_text(json.dumps(report['sample_size'],indent=2)+'\n')
        if not report['sample_size']['feasible']:
            raise ValueError('predeclared sample cap insufficient for pilot power estimate')
        confirm=[]
        for replica in range(report['sample_size']['pairs']):
            confirm.append(run_pair(root,str(executable),'confirm',replica,used,report))
            save()
            print(json.dumps(dict(phase='confirm',pair=replica+1,
                                 total=report['sample_size']['pairs'])),flush=True)
        report['analysis']=analyze(confirm)
        report['passed']=report['analysis']['passed']
        if fingerprint_build(executable) != report['build_fingerprint']:
            raise ValueError('runtime libraries/platform changed during measurement')
        if digest(executable) != report['executable_sha256']:
            raise ValueError('build changed during measurement')
    except Exception as exc:
        report['error']=str(exc)
        report['passed']=False
        raise
    finally:
        report['files']={str(p.relative_to(root)):dict(bytes=p.stat().st_size,sha256=digest(p))
                         for p in root.rglob('*') if p.is_file() and p.name!='report.json'}
        save()
    if not report['passed']:
        raise SystemExit('ABP equivalence failed or inconclusive; raw evidence retained')


if __name__=='__main__':
    main()
