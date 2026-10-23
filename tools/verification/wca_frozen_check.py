"""Gate reconstruction of missing pilot raw evidence against the frozen design."""
import argparse
import json
from pathlib import Path


def check(rebuilt: dict, frozen: dict) -> dict:
    """Never silently change a frozen build, protocol or confirmatory sample size."""
    if rebuilt.get('error') or frozen.get('error'):
        raise ValueError('Incomplete pilot evidence')
    for field in ('build', 'protocol'):
        if rebuilt[field] != frozen[field]:
            raise ValueError(f'Frozen pilot {field} changed')
    if len(rebuilt['replicas']) != rebuilt['protocol']['pilot_pairs'] * len(rebuilt['protocol']['strata']):
        raise ValueError('Reconstructed pilot replicas incomplete')
    def rows(report: dict) -> list[dict]:
        return [{key: row[key] for key in ('duration', 'layout', 'pairs', 'required', 'feasible')}
                for row in report['strata']]
    if rows(rebuilt) != rows(frozen) or not all(row['feasible'] for row in rebuilt['strata']):
        raise ValueError('Frozen sample table differs; stop for review without confirmation')
    return dict(matched=True, total_pairs=sum(row['pairs'] for row in rebuilt['strata']),
                rebuilt_driver_sha256=rebuilt['driver_sha256'], frozen_driver_sha256=frozen['driver_sha256'])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pilot', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = check(json.loads(args.pilot.read_text()), json.loads(args.reference.read_text()))
    with args.output.open('x') as stream:
        json.dump(result, stream, indent=2)
    print(json.dumps(result))


if __name__ == '__main__':
    main()
