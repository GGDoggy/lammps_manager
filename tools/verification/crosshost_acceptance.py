"""Explicit bounded large-artifact acceptance across two authenticated hosts.

Provide an SSH argv JSON file; credentials stay in protected files/SSH_ASKPASS.
This driver controls only its new private worker and its local manager process.
"""
import argparse
from collections.abc import Callable
from dataclasses import asdict
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import shlex
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time

import yaml
from lammps_manager.store import Store
from lammps_manager.local import inspect_job
from lammps_manager.monitoring import transfers

REPOSITORY = Path(__file__).resolve().parents[2]


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open('rb') as stream:
        while block := stream.read(65536):
            value.update(block)
    return value.hexdigest()


def write_final_report(root: Path, job: str, report: dict) -> None:
    """Retain failure evidence even when unresolved reservations remain."""
    with Store(root / 'manager.sqlite') as store:
        report['final_state'] = store.job_state(job)
        report['final_reservations'] = [asdict(item) for item in store.reservations()]
    report['final_boot_id'] = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    (root / 'report.json').write_text(json.dumps(report, indent=2))


def finish_run(root: Path, job: str, report: dict,
               manager_process: subprocess.Popen | None,
               stop_worker: Callable[[], subprocess.CompletedProcess]) -> None:
    """Publish cleanup evidence and fail an otherwise successful run on stop errors."""
    execution_passed = report['passed']
    cleanup_error = None
    try:
        stopped = stop_worker()
        report['worker_stop_requested'] = json.loads(stopped.stdout)
    except Exception as exc:
        cleanup_error = exc
        report['worker_cleanup_error'] = str(exc)
        report['passed'] = False
        report.setdefault('error', f'Worker cleanup failed: {type(exc).__name__}: {exc}')
    if manager_process is not None:
        if manager_process.poll() is None:
            manager_process.send_signal(signal.SIGINT)
        try:
            manager_process.wait(timeout=45)
        except subprocess.TimeoutExpired:
            manager_process.kill(); manager_process.wait(timeout=5)
            report['manager_escalation'] = True
    write_final_report(root, job, report)
    print(json.dumps({key: report.get(key) for key in ('root', 'remote_root', 'passed', 'error', 'elapsed_seconds')}), flush=True)
    if cleanup_error is not None and execution_passed:
        raise RuntimeError('Worker cleanup failed; inspect retained report.json') from cleanup_error


def worker_stop_script(unit: str) -> str:
    """Build the remote cleanup program used by cross-host acceptance."""
    return '''import subprocess,json
from pathlib import Path
unit=''' + repr(unit) + '''
p=subprocess.run(['systemctl','--user','stop',unit],capture_output=True,text=True,timeout=25,check=True)
s=subprocess.run(['systemctl','--user','show',unit,'--property=ActiveState','--property=MainPID','--property=ControlGroup'],capture_output=True,text=True,timeout=10,check=True)
values=dict(line.split('=',1) for line in s.stdout.splitlines() if '=' in line)
if (not {'ActiveState','MainPID','ControlGroup'}.issubset(values)
    or values['ActiveState'] not in ('inactive','failed') or values['MainPID'] != '0'):
 raise RuntimeError('Worker service stop state unconfirmed; preserve reservations')
group=values.get('ControlGroup','')
path=Path('/sys/fs/cgroup')/group.lstrip('/')/'cgroup.procs'
members=path.read_text().split() if group and path.exists() else []
if values.get('ActiveState') in ('active','activating','deactivating') or members:
 raise RuntimeError('Worker service not fully stopped; preserve reservations')
print(json.dumps(dict(unit=unit,properties=values,remaining_cgroup_pids=members,stop_returncode=p.returncode)))
'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ssh-argv', type=Path, required=True)
    parser.add_argument('--remote-source-root', type=Path, required=True)
    parser.add_argument('--manager-address', required=True)
    parser.add_argument('--port', type=int, default=18446)
    parser.add_argument('--file-mib', type=int, default=256)
    parser.add_argument('--small-files', type=int, default=3000)
    parser.add_argument('--deadline', type=int, default=900)
    args = parser.parse_args()
    if args.file_mib <= 0 or args.small_files < 0 or args.deadline <= 0:
        parser.error('positive file size/deadline and nonnegative file count required')
    ssh = json.loads(args.ssh_argv.read_text())
    if not isinstance(ssh, list) or not ssh or not all(isinstance(s, str) and '\0' not in s for s in ssh):
        parser.error('SSH argv must be a nonempty string array')
    def remote(command: str, **kwargs) -> subprocess.CompletedProcess:
        return subprocess.run(ssh + [command], check=True, timeout=45, **kwargs)
    cpus = sorted(os.sched_getaffinity(0))[:max(1, (os.cpu_count() or 1) // 2)]
    os.sched_setaffinity(0, cpus)
    root = Path(tempfile.mkdtemp(prefix='lammps-v009-crosshost-')); root.chmod(0o700)
    allocate = ('import tempfile;from pathlib import Path;p=Path(' + repr(str(args.remote_source_root))
                + ')/"runs";p.mkdir(mode=0o700,exist_ok=True);print(tempfile.mkdtemp(prefix="crosshost-",dir=p))')
    remote_root = remote('python3 -c ' + shlex.quote(allocate), capture_output=True, text=True).stdout.strip()
    report = dict(root=str(root), remote_root=remote_root, passed=False,
                  manager_affinity=cpus, configuration=vars(args).copy(),
                  boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                  driver_sha256=digest(Path(__file__)))
    report['configuration'] = {k: str(v) if isinstance(v, Path) else v
                               for k, v in report['configuration'].items()}
    (root / 'configuration.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(dict(root=str(root), remote_root=remote_root)), flush=True)
    source = root / 'source'; source.mkdir(); (root / 'jobs').mkdir()
    text = (REPOSITORY / 'tools/verification/real_acceptance.py').read_text().split("INPUT = '''", 1)[1].split("'''", 1)[0].replace('STEPS', '1000')
    (source / 'in.lmp').write_text(text)
    block = bytes(range(256)) * 256
    with (source / 'large.bin').open('wb') as output:
        for _ in range(args.file_mib * 16):
            output.write(block)
    small = source / 'small'; small.mkdir()
    for index in range(args.small_files):
        (small / f'{index:04d}').write_bytes(block[:4096])
    report['input_bytes'] = sum(p.stat().st_size for p in source.rglob('*') if p.is_file())
    report['input_files'] = args.small_files + 2
    slots = min(4, len(cpus))
    if slots < 2:
        raise ValueError('half-host allocation cannot fit a two-rank smoke job')
    capacity = dict(id='crosshost', cpu_slots=slots, memory_budget_gib=1, max_concurrent_jobs=1)
    manager = dict(manager=dict(database_path=str(root / 'manager.sqlite'), schedule_interval_seconds=.1,
                                heartbeat_timeout_seconds=15),
                   storage=dict(mode='staged', work_root=str(root / 'jobs')),
                   job_defaults=dict(mpi_ranks=2, omp_threads_per_rank=1, memory_gib=.1, time_limit_seconds=30),
                   scheduler=dict(account_external_load=False, reserve_memory_gib=.1), workers=[capacity])
    worker = dict(worker=capacity, manager_endpoint=f'https://{args.manager_address}:{args.port}',
                  storage_mode='staged', work_root=remote_root + '/worker', token_file=remote_root + '/token',
                  heartbeat_interval_seconds=.5, cleanup_staged_files=False,
                  execution=dict(lammps_path='/usr/bin/lmp', mpi_launcher_path='/usr/bin/mpirun.openmpi',
                                 mpi_args=['--bind-to', 'none']))
    (root / 'manager.yaml').write_text(yaml.safe_dump(manager))
    token = root / 'token'; token.write_text(secrets.token_urlsafe(32)); token.chmod(0o600)
    credentials = root / 'credentials.json'; credentials.write_text(json.dumps({'crosshost': str(token)})); credentials.chmod(0o600)
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                    '-keyout', str(root / 'server.key'), '-out', str(root / 'server.crt'), '-days', '2',
                    '-subj', '/CN=V009 crosshost', '-addext', f'subjectAltName=IP:{args.manager_address}',
                    '-addext', 'basicConstraints=critical,CA:TRUE'], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20)
    (root / 'server.key').chmod(0o600)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w:gz') as archive:
        for name, data in [('worker.yaml', yaml.safe_dump(worker).encode()), ('token', token.read_bytes()),
                           ('server.crt', (root / 'server.crt').read_bytes())]:
            item = tarfile.TarInfo(name); item.size = len(data); item.mode = 0o600
            archive.addfile(item, io.BytesIO(data))
    remote('tar xzf - -C ' + shlex.quote(remote_root) + ' && mkdir ' + shlex.quote(remote_root + '/worker'), input=buffer.getvalue())
    manifest = root / 'batch.json'; manifest.write_text(json.dumps([dict(source_directory=str(source), input_file='in.lmp')]))
    environment = {**os.environ, 'PYTHONPATH': str(REPOSITORY / 'src')}
    result = subprocess.run([sys.executable, '-m', 'lammps_manager', 'submit', '--config',
                             str(root / 'manager.yaml'), str(manifest)], env=environment,
                            check=True, capture_output=True, text=True, timeout=90)
    job, = json.loads(result.stdout); report['job'] = job
    manager_process = None; worker_identity = None
    worker_unit = Path(remote_root).name + '-worker.service'
    report['worker_unit'] = worker_unit
    started = time.monotonic()
    try:
        with (root / 'manager.log').open('wb') as output:
            manager_process = subprocess.Popen([sys.executable, '-m', 'lammps_manager', 'serve', '--config',
                str(root / 'manager.yaml'), '--credentials', str(credentials), '--host', '0.0.0.0',
                '--port', str(args.port), '--tls-cert', str(root / 'server.crt'), '--tls-key', str(root / 'server.key')],
                env=environment, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        for _ in range(100):
            if manager_process.poll() is not None:
                raise ValueError('manager exited before readiness')
            try:
                with socket.create_connection(('127.0.0.1', args.port), timeout=.1):
                    break
            except OSError:
                time.sleep(.05)
        else:
            raise TimeoutError('manager readiness deadline')
        launch = '''import os,json,subprocess,time
from pathlib import Path
from lammps_manager.recovery import process_identity
r=Path(''' + repr(remote_root) + ''')
source=Path(''' + repr(str(args.remote_source_root)) + ''')
unit=''' + repr(worker_unit) + '''
cpus=sorted(os.sched_getaffinity(0))[:max(1,os.cpu_count()//2)]
mask=','.join(map(str,cpus))
argv=['systemd-run','--user','--unit='+unit,'--property=Restart=no',
      '--property=RuntimeMaxSec=' + str(''' + repr(args.deadline + 120) + '''),
      '--property=TimeoutStopSec=15','--property=KillMode=control-group',
      '--property=SendSIGKILL=yes','--property=AllowedCPUs='+mask,
      '--property=CPUAffinity='+' '.join(map(str,cpus)),
      '--property=CPUQuota='+str(len(cpus)*100)+'%',
      '--property=WorkingDirectory='+str(source),
      '--property=StandardOutput=append:'+str(r/'worker.log'),
      '--property=StandardError=append:'+str(r/'worker.log'),
      '--setenv=PYTHONPATH='+str(source/'src'),
      '--setenv=SSL_CERT_FILE='+str(r/'server.crt'),
      str(source/'.venv/bin/python'),'-m','lammps_manager','worker',
      '--config',str(r/'worker.yaml'),'--state-directory',str(r/'journal')]
subprocess.run(argv,check=True,capture_output=True,timeout=20)
for _ in range(100):
 p=subprocess.run(['systemctl','--user','show',unit,'--property=MainPID','--value'],check=True,capture_output=True,text=True)
 pid=int(p.stdout.strip())
 if pid:break
 time.sleep(.05)
else:raise RuntimeError('No worker service PID')
record=dict(identity=process_identity(pid),affinity=cpus,unit=unit)
(r/'worker-launch.json').write_text(json.dumps(record,indent=2));print(json.dumps(record))
'''
        launched = remote('cd ' + shlex.quote(str(args.remote_source_root)) + ' && PYTHONPATH=src .venv/bin/python -',
                          input=launch.encode(), capture_output=True)
        worker_identity = json.loads(launched.stdout); report['worker_launch'] = worker_identity
        heartbeat_ages = []
        with (root / 'samples.jsonl').open('w') as samples:
            while time.monotonic() - started < args.deadline:
                if manager_process.poll() is not None:
                    raise ValueError('manager exited')
                with Store(root / 'manager.sqlite') as store:
                    attempts = store.job_attempts(job)
                    snapshot = store.worker_records().get('crosshost', {}).get('snapshot')
                    age = time.time() - snapshot['heartbeat_at'] if snapshot else None
                    if age is not None:
                        heartbeat_ages.append(age)
                    value = dict(time=time.time(), elapsed=time.monotonic() - started, state=store.job_state(job),
                                 reserved=len(store.reservations()), heartbeat_age=age,
                                 transfers=transfers(store, attempts[0].attempt_id) if attempts else {})
                    collection = store.result_collection(attempts[0].attempt_id) if attempts else None
                    samples.write(json.dumps(value) + '\n'); samples.flush()
                    (root / 'progress.json').write_text(json.dumps(value))
                    if collection and collection['status'] == 'collected':
                        record = inspect_job(store, job)
                        if record['state'] != 'succeeded' or len(attempts) != 1 or store.reservations():
                            raise ValueError('unexpected execution/resource outcome')
                        break
                time.sleep(.5)
            else:
                raise TimeoutError('cross-host deadline exceeded; preserve original attempt for reconciliation')
        base = Path(record['attempts'][0]['results_directory'])
        files = record['attempts'][0]['result_collection']['manifest']['files']
        for item in files:
            path = base / item['path']
            if path.stat().st_size != item['size'] or digest(path) != item['sha256']:
                raise ValueError('committed result integrity mismatch')
        for path in source.rglob('*'):
            if path.is_file() and digest(path) != digest(base / 'work' / path.relative_to(source)):
                raise ValueError('source/result round-trip mismatch')
        report.update(record=record, result_files=len(files), result_bytes=sum(f['size'] for f in files),
                      elapsed_seconds=time.monotonic() - started, max_sampled_heartbeat_age=max(heartbeat_ages), passed=True)
    except BaseException as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        # Cancel through the original protocol before retiring a failed service.
        # Unconfirmed reservations remain recorded; disconnection never releases them.
        if not report['passed'] and manager_process is not None and manager_process.poll() is None:
            try:
                subprocess.run([sys.executable, '-m', 'lammps_manager', 'cancel', '--config',
                                str(root / 'manager.yaml'), job], env=environment, check=True,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15)
                end = time.monotonic() + 30
                while time.monotonic() < end:
                    with Store(root / 'manager.sqlite') as store:
                        if not store.reservations():
                            break
                    time.sleep(.2)
            except Exception as exc:
                report['cancellation_error'] = str(exc)
        # The unique expiring unit exists even if its SSH launch reply was lost.
        stop = worker_stop_script(worker_unit)
        finish_run(root, job, report, manager_process,
                   lambda: remote('python3 -', input=stop.encode(), capture_output=True))


if __name__ == '__main__':
    main()
