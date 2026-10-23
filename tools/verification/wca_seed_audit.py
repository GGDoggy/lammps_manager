"""Offline frozen-study seed-plus-rank collision audit; never launches jobs.

The seed-plus-rank mapping and engine bound are conditional on the recorded
upstream source. Distinct seed integers do not establish PRNG independence.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path


def audit(pilot: dict) -> dict:
    """Check frozen contexts and all maximum v2 ordinals without sampling."""
    protocol = pilot['protocol']
    modulus, multiplier = 899999999, 1000003
    layouts = [[1, 1, 1], [2, 2, 2], [4, 4, 4], [1, 2, 4], [4, 2, 1]]
    expected = [[duration, layout] for duration in (20000, 100000) for layout in layouts]
    if (pilot.get('error') or protocol.get('version') != 2
            or protocol.get('maximum_pairs') != 16384 or protocol.get('pilot_pairs') != 8
            or protocol.get('strata') != expected or len(pilot['strata']) != 10):
        raise ValueError('A complete frozen v2 pilot design is required')
    if math.gcd(modulus, multiplier) != 1:
        raise ValueError('Noninvertible seed permutation')
    maximum = protocol['maximum_pairs']
    pilots = protocol['pilot_pairs']
    strata = protocol['strata']
    labels = ('orientation', 'prepare', 'reference-0', 'segmented-0', 'segmented-1', 'segmented-2')
    offset = int.from_bytes(hashlib.sha256(protocol['namespace'].encode()).digest()[:8], 'big')
    seen, base = set(), set()
    expanded, contexts = 0, 0
    bounds = []
    for index, (duration, layout) in enumerate(strata):
        rows = [r for r in pilot['strata'] if r['duration'] == duration and r['layout'] == layout]
        if (len(rows) != 1 or not rows[0]['feasible'] or type(rows[0]['pairs']) is not int
                or not 64 <= rows[0]['pairs'] <= maximum):
            raise ValueError('Missing/duplicate/infeasible frozen stratum')
        for phase, count in [('pilot', pilots), ('confirm', rows[0]['pairs'])]:
            for replica in range(count):
                for label_index, label in enumerate(labels):
                    ordinal = ((index * (maximum + pilots) + (0 if phase == 'pilot' else pilots)
                                + replica) * len(labels) + label_index)
                    value = (offset + ordinal * multiplier) % modulus + 1
                    if value in base:
                        raise ValueError('Repeated base stream seed')
                    base.add(value)
                    contexts += 1
                    ranks = layout[int(label[-1])] if label.startswith('segmented-') else 1
                    for rank in range(ranks):
                        rank_seed = value + rank
                        if not 0 < rank_seed <= 900000000:
                            raise ValueError(f'Invalid rank seed: {index}/{phase}/{replica}/{label}/{rank_seed}')
                        if rank_seed in seen:
                            raise ValueError(f'Repeated rank seed: {index}/{phase}/{replica}/{label}/{rank}/{rank_seed}')
                        seen.add(rank_seed)
                        expanded += 1
    maximum_ranks = max(max(layout) for _, layout in strata)
    ordinals = len(strata) * (maximum + pilots) * len(labels)
    maximum_base = max((offset + ordinal * multiplier) % modulus + 1 for ordinal in range(ordinals))
    inverse = pow(multiplier, -1, modulus)
    for delta in range(1, maximum_ranks):
        distance = delta * inverse % modulus
        distance = min(distance, modulus - distance)
        if distance <= ordinals - 1:
            raise ValueError('Allowed ordinal/rank expansion is not proven disjoint')
        bounds.append(dict(rank_seed_difference=delta, minimum_ordinal_distance=distance))
    if maximum_base + maximum_ranks - 1 > 900000000:
        raise ValueError('Allowed rank expansion exceeds engine seed bound')
    prefix = 'https://github.com/lammps/lammps/blob/7b4c33630d544953d2abe12eccdf0fab5ba0e7d1/'
    return dict(
        scope='Frozen v2 pilot/confirmation contexts and conditional upstream seed-plus-rank mapping; no independence claim',
        passed=True, namespace=protocol['namespace'],
        frozen_confirmatory_pairs=sum(r['pairs'] for r in pilot['strata']),
        pilot_pairs=pilots * len(strata), base_stream_contexts=contexts,
        unique_expanded_seed_values=expanded, maximum_measured_ranks=maximum_ranks,
        minimum_expanded_seed=min(seen), maximum_expanded_seed=max(seen),
        collisions=0, engine_seed_bound_violations=0, all_allowed_v2_ordinal_count=ordinals,
        all_allowed_v2_maximum_base_seed=maximum_base, rank_offset_inverse_bounds=bounds,
        upstream_ref='7b4c33630d544953d2abe12eccdf0fab5ba0e7d1',
        upstream_seed_source=prefix + 'src/BROWNIAN/fix_brownian_base.cpp',
        upstream_seed_bound_source=prefix + 'src/random_mars.cpp',
        limitations=[
            'Distinct seed integers do not prove independent/nonoverlapping PRNG sequences.',
            'Mapping was verified in pinned upstream source; compiled deployed-source identity is unavailable.',
            'No claim for ranks or protocols outside the frozen matrix.'])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pilot-report', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit(json.loads(args.pilot_report.read_text()))
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps({key: result[key] for key in (
        'passed', 'base_stream_contexts', 'unique_expanded_seed_values',
        'collisions', 'engine_seed_bound_violations')}))


if __name__ == '__main__':
    main()
