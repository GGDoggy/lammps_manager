"""Real TLS response-loss during intact staged input streaming, without reboot."""
import hashlib
import json
import os
from pathlib import Path

from real_acceptance import ROOT, PYTHON, setup, INPUT, cli, inspect
from distributed_acceptance import prepare, manager, worker, start, stop, wait
from restart_acceptance import collected
from artifact_acceptance import port_open, wait_for
from lammps_manager.store import Store
from lammps_manager.recovery import session_members


def run() -> None:
    cpus = sorted(os.sched_getaffinity(0))[:min(4, max(1, os.cpu_count() // 2))]
    os.sched_setaffinity(0, cpus)
    root = setup('heartbeat', mode='staged')
    prepare(root, port=18449)
    source = root / 'source'; source.mkdir()
    (source / 'in.lmp').write_text(INPUT.replace('STEPS', '1000'))
    with (source / 'blob').open('wb') as stream:
        for _ in range(64):
            stream.write(bytes(range(256)) * 256)
    manifest = root / 'batch.json'
    manifest.write_text(json.dumps([dict(source_directory=str(source), input_file='in.lmp')]))
    job, = cli(root / 'manager.yaml', 'submit', manifest)
    (root / 'fault').write_text('heartbeat')
    m = manager(root); proxy = None; w = None
    try:
        proxy = start([PYTHON, str(ROOT / 'artifact_proxy.py'), str(root)], root, 'proxy')
        wait_for(lambda: port_open(18449), 'private TLS proxy', 10)
        w = worker(root)
        wait(root, lambda value: collected(root, job), 'original-results-collected', timeout=60)
        record, = inspect(root, [job])
        assert record['state'] == 'succeeded' and len(record['attempts']) == 1
        attempt = record['attempts'][0]
        assert attempt['termination_evidence']
        events = [json.loads(line) for line in (root / 'proxy-events.jsonl').read_text().splitlines()]
        lost = [event for event in events if event['event'] == 'lost_input_heartbeat']
        assert len(lost) == 1 and lost[0]['status'] == 200
        requests = [event['path'] for event in events if event['event'] == 'input_request']
        assert len(requests) == 2 and len(set(requests)) == 2, requests
        assert 'Input heartbeat deferred' in (root / 'worker.log').read_text()
        destination = Path(attempt['results_directory'])
        hashes = {}
        for item in attempt['result_collection']['manifest']['files']:
            path = destination / item['path']
            assert path.stat().st_size == item['size']
            value = hashlib.sha256(path.read_bytes()).hexdigest()
            assert value == item['sha256']
            hashes[item['path']] = value
        identity = json.loads((destination / 'process.json').read_text())
        assert not session_members(identity['pid'])
        with Store(root / 'manager.sqlite') as store:
            assert not store.reservations()
        with Store(root / 'journal/attempts.sqlite3') as store:
            assert not store.reservations()
        report = dict(root=str(root), record=record, events=events, hashes=hashes,
                      affinity=cpus, boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip())
        (ROOT / 'v009-input-heartbeat.json').write_text(json.dumps(report, indent=2))
        print('Lost real TLS heartbeat response preserved original input stream and one execution', flush=True)
    finally:
        if w is not None:
            stop(w)
        if proxy is not None:
            stop(proxy)
        stop(m)


if __name__ == '__main__':
    run()
