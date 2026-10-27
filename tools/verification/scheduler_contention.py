"""Measure admission on a disposable copy of a paused database; never dispatch externally."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
from unittest.mock import patch

from lammps_manager.config import load_config, WorkerCapacity
from lammps_manager.scheduler import schedule, WorkerSnapshot
from lammps_manager.store import Store


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    config = load_config(args.config, 'manager')
    reports = []
    with tempfile.TemporaryDirectory(prefix='scheduler-contention-') as temporary:
        path = Path(temporary) / 'copy.sqlite3'
        # Back up only via a read-only source connection. All upgrades/writes target the copy.
        with sqlite3.connect(args.database.resolve().as_uri() + '?mode=ro', uri=True) as source:
            with sqlite3.connect(path) as destination:
                source.backup(destination)
        config = replace(config, manager=replace(config.manager, database_path=str(path)))
        with Store(path) as store:
            records = store.worker_records()
            if not records or not all(r['paused'] for r in records.values()):
                raise ValueError('Verification requires all source workers paused')
            snapshots = tuple(WorkerSnapshot(**{**r['snapshot'],
                'capacity': WorkerCapacity(**r['snapshot']['capacity']), 'paused': True})
                for r in records.values() if r['snapshot'])
            queued = store._db.execute("SELECT job_id,specification FROM jobs WHERE state='queued'").fetchall()
            points = [len(json.loads(row['specification']).get('sampling', {}).get('steps', [])) for row in queued]
            for mode in ('DELETE', 'WAL'):
                store._db.execute('PRAGMA journal_mode=' + mode)
                with patch.object(Store, 'specification', side_effect=AssertionError('Full specification decoded')), \
                     patch('lammps_manager.sampling.validate_sampling', side_effect=AssertionError('Sampling validated')):
                    schedule(store, config, snapshots, now=time.time(), max_assignments=4)
                    for _ in range(3):
                        entered = threading.Event()
                        def contender() -> float:
                            if not entered.wait(5):
                                raise TimeoutError('Missing admission transaction')
                            began = time.perf_counter()
                            with Store(path, timeout=.2) as other:
                                other.set_worker(next(iter(records)), paused=True)
                            return time.perf_counter() - began
                        store._db.set_trace_callback(lambda sql: entered.set() if sql == 'BEGIN IMMEDIATE' else None)
                        try:
                            with ThreadPoolExecutor(max_workers=1) as pool:
                                future = pool.submit(contender)
                                began = time.perf_counter()
                                result = schedule(store, config, snapshots, now=time.time(), max_assignments=4)
                                elapsed = time.perf_counter() - began
                                if result.assignments:
                                    raise AssertionError('Paused workers assigned')
                                reports.append(dict(journal_mode=mode, schedule_seconds=elapsed,
                                    competing_control_seconds=future.result(timeout=5)))
                        finally:
                            store._db.set_trace_callback(None)
    args.output.write_text(json.dumps(dict(queued_jobs=len(queued), sampling_points=points,
        full_specification_decodes=0, sampling_validations=0, rounds=reports,
        scope='Disposable copy only; no processes launched; timings depend on host/storage'), indent=2) + '\n')
    print(args.output)


if __name__ == '__main__':
    main()
