"""Durable, bounded node helpers for explicitly coordinated native MPI work."""

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import ssl
import subprocess
import sys
import tempfile
import time
from typing import BinaryIO, Iterator
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener
from uuid import UUID

from .config import WorkerConfig, load_config
from .manager import is_loopback, read_token
from .models import specification_from_dict
from .recovery import process_identity, session_members, terminate_group
from .staging import open_regular, publish_inputs, safe_path, sync_directory
from .worker import NoRedirect


def _identifier(value: str) -> str:
    if len(safe_path(value).parts) != 1:
        raise ValueError('MPI identity must be one safe path component')
    return value


def _write(path: Path, data: dict) -> None:
    descriptor, temporary = tempfile.mkstemp(prefix='.state-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as stream:
            json.dump(data, stream, sort_keys=True, allow_nan=False)
            stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _same_process(identity: dict) -> bool:
    try:
        current = process_identity(identity['pid'])
    except FileNotFoundError:
        return False
    return all(current[key] == identity[key] for key in ('pid','boot_id','start_ticks','session','process_group'))


def _live_rank(identity: dict) -> bool:
    return (_same_process(identity)
            and any(pid == identity['pid'] for pid, _ in session_members(identity['session'])))


def _digest(path: Path) -> str:
    value=hashlib.sha256()
    with path.open('rb') as stream:
        while chunk:=stream.read(65536): value.update(chunk)
    return value.hexdigest()


class Node:
    """One durable launch fence per attempt; SSH reconnection never grants a new start."""

    def __init__(self, root: Path, config: WorkerConfig) -> None:
        self.root = root.resolve(strict=True)
        self.config = config
        if config.storage_mode != 'staged':
            raise ValueError('Native MPI node helpers require staged mode')
        self.controls = self.root / 'state/mpi'
        self.controls.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.controls.resolve() != self.controls:
            raise ValueError('Unsafe MPI journal root')

    def control(self, attempt_id: str) -> Path:
        """Locate a private, non-symlink attempt journal."""
        path = self.controls / _identifier(attempt_id)
        if path.is_symlink():
            raise ValueError('Unsafe MPI attempt journal')
        return path

    def fence(self, attempt_id: str, *, cancel: bool=False) -> None:
        """Irrevocably close launch rights before waiting for a staging/process lock."""
        directory=self.control(attempt_id)
        if not directory.is_dir(): raise ValueError('MPI attempt journal does not exist')
        for name in (('closed','cancel') if cancel else ('closed',)):
            try: descriptor=os.open(directory/name,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
            except FileExistsError: continue
            with os.fdopen(descriptor,'w') as stream:
                stream.write('closed\n');stream.flush();os.fsync(stream.fileno())
        sync_directory(directory)

    @contextmanager
    def locked(self, attempt_id: str, *, create: bool = False) -> Iterator[dict]:
        """Serialize durable fence/process updates, including concurrent SSH launch calls."""
        directory = self.control(attempt_id)
        if create:
            directory.mkdir(mode=0o700, exist_ok=True)
        if not directory.is_dir():
            raise ValueError('MPI attempt journal does not exist')
        descriptor = os.open(directory/'lock', os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'r+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            path = directory/'state.json'
            if path.is_symlink():
                raise ValueError('Unsafe MPI state file')
            state = json.loads(path.read_text()) if path.exists() else {'phase':'new'}
            yield state
            _write(path, state)

    def request(self, suffix: str, data: dict | BinaryIO | None = None,
                length: int | None = None) -> BinaryIO:
        """Use the configured authenticated HTTPS origin, without proxies or redirects."""
        from urllib.parse import urlsplit
        endpoint = urlsplit(self.config.manager_endpoint)
        if endpoint.scheme != 'https' and not (endpoint.scheme == 'http' and is_loopback(endpoint.hostname or '')):
            raise ValueError('MPI artifacts require verified HTTPS or literal loopback HTTP')
        token = read_token(self.config.token_file)
        headers = {'X-Worker-ID':self.config.worker.id, 'Authorization':'Bearer '+token}
        if isinstance(data, dict):
            data = json.dumps(data).encode()
            headers['Content-Type']='application/json'
        if length is not None:
            headers['Content-Length']=str(length)
            headers['Content-Type']='application/octet-stream'
        context = ssl.create_default_context(cafile=str(self.root/'config/ca.crt')
                                             if (self.root/'config/ca.crt').exists() else None)
        opener = build_opener(ProxyHandler({}), NoRedirect(), HTTPSHandler(context=context))
        request = Request(self.config.manager_endpoint.rstrip('/')+suffix, data=data, headers=headers)
        return opener.open(request, timeout=self.config.result_request_timeout_seconds if data is not None else 10)

    def health(self) -> dict:
        """Read the manager's authenticated current worker generation."""
        with self.request('/v1/health') as response:
            return json.load(response)

    def download(self, attempt_id: str, index: int) -> BinaryIO:
        """Open one manager-owned immutable input, scoped to this participant."""
        return self.request(f'/v2/inputs/{attempt_id}/files/{index}')

    def prepare(self, request: dict) -> dict:
        """Verify generation, capacity, executable and all staged input bytes before readiness."""
        request=json.loads(json.dumps(request,sort_keys=True,allow_nan=False))
        attempt_id = _identifier(request['attempt_id'])
        spec = specification_from_dict(request['specification'])
        _identifier(spec.job_id)
        if request['worker_id'] != self.config.worker.id or request['role'] not in {'leader','peer'}:
            raise ValueError('MPI helper worker identity or role mismatch')
        share = next((p for p in spec.mpi_placement if p.worker_id == self.config.worker.id), None)
        if spec.mpi_placement and (request['role']=='leader') != (spec.mpi_placement[0].worker_id==self.config.worker.id):
            raise ValueError('MPI helper role differs from immutable placement')
        if (share is None or share.mpi_ranks*spec.resources.omp_threads_per_rank > self.config.worker.cpu_slots
                or share.memory_gib > self.config.worker.memory_budget_gib):
            raise ValueError('MPI participant exceeds local configured capacity')
        cpus = request['cpus']
        if (not isinstance(cpus,list) or not cpus or any(type(cpu) is not int for cpu in cpus)
                or len(set(cpus)) != len(cpus) or not set(cpus)<=os.sched_getaffinity(0)
                or len(cpus) < share.mpi_ranks*spec.resources.omp_threads_per_rank):
            raise ValueError('MPI participant CPU affinity cannot fit its share')
        generation = request['generation']
        UUID(generation['session_id']); UUID(generation['boot_id'])
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        if generation['boot_id'] != boot:
            raise ValueError('MPI reservation belongs to a different boot')
        actual = _digest(Path(self.config.execution.lammps_path))
        if request['executable_sha256'] != actual:
            raise ValueError('MPI executable fingerprint mismatch')
        directory = Path(self.config.work_root).resolve(strict=True)/spec.job_id/attempt_id
        if any(p.is_symlink() for p in (directory.parent,directory)):
            raise ValueError('Unsafe MPI runtime directory')
        with self.locked(attempt_id,create=True) as state:
            if 'request' in state and state['request'] != request:
                raise ValueError('Conflicting MPI preparation replay')
            if (self.control(attempt_id)/'closed').exists():
                raise ValueError('MPI preparation launch fence is closed')
            if state['phase'] != 'new':
                if 'request' not in state:
                    raise ValueError('MPI preparation identity is uncertain')
                if state['phase'] != 'staging':
                    return {key:state[key] for key in ('phase','directory','executable_sha256')}
            elif directory.exists():
                raise ValueError('Existing unowned MPI runtime directory')
            health = self.health()
            if (not self.config.worker.enabled or not health.get('ready') or not health.get('online') or health.get('paused')
                    or health.get('draining') or health.get('enabled') is False
                    or not health.get('generation') or any(health['generation'][k]!=v for k,v in generation.items())):
                raise ValueError('MPI worker readiness/generation changed')
            # Persist ownership before any transfer. A failed stage can be retried with this exact request.
            state.update(request=request,phase='staging',directory=str(directory/'work'),
                         executable_sha256=actual,closed=False)
        directory.mkdir(mode=0o700,parents=True,exist_ok=True)
        return self._stage(attempt_id)

    def _stage(self, attempt_id: str) -> dict:
        with self.locked(attempt_id) as state:
            request = state['request']
            spec = specification_from_dict(request['specification'])
            directory = Path(state['directory']).parent
            limits=dict(max_files=self.config.max_input_files,max_file_bytes=self.config.max_input_file_bytes,
                        max_total_bytes=self.config.max_input_total_bytes)
            def progress():
                if (self.control(attempt_id)/'closed').exists():
                    raise ValueError('MPI launch fence closed during staging')
            progress()
            def fetch(index):
                return self.download(attempt_id,index)
            publish_inputs(directory/'inputs',spec.input_manifest,fetch,limits,progress=progress)
            publish_inputs(directory/'work',spec.input_manifest,
                           lambda index:open_regular(directory/'inputs',spec.input_manifest[index].path),limits,progress=progress)
            state['phase']='prepared'
            return {key:state[key] for key in ('phase','directory','executable_sha256')}

    def start(self, attempt_id: str, argv: list[str]) -> dict:
        """Claim one launch durably, then start its detached bounded monitor exactly once."""
        if not isinstance(argv,list) or not argv or any(not isinstance(arg,str) or '\x00' in arg for arg in argv):
            raise ValueError('MPI launch requires an argument array')
        with self.locked(attempt_id) as state:
            if 'argv' in state:
                if state['argv'] != argv:
                    raise ValueError('Conflicting MPI launch replay')
                return dict(state)
            if state['phase'] != 'prepared' or state.get('closed') or (self.control(attempt_id)/'closed').exists():
                raise ValueError('MPI launch is not prepared or its fence is closed')
            permitted = self.config.execution.mpi_launcher_path if state['request']['role']=='leader' else '/usr/bin/orted'
            if Path(argv[0]).resolve(strict=True) != Path(permitted).resolve(strict=True):
                raise ValueError('MPI helper cannot execute an unconfigured launcher')
            health=self.health()
            if (not self.config.worker.enabled or not health.get('ready') or not health.get('online') or health.get('paused')
                    or health.get('draining') or health.get('enabled') is False
                    or not health.get('generation') or any(health['generation'][k]!=v for k,v in state['request']['generation'].items())):
                raise ValueError('MPI worker generation/readiness changed before launch')
            spec=specification_from_dict(state['request']['specification'])
            share=next(p for p in spec.mpi_placement if p.worker_id==self.config.worker.id)
            if (share.mpi_ranks*spec.resources.omp_threads_per_rank>self.config.worker.cpu_slots
                    or share.memory_gib>self.config.worker.memory_budget_gib):
                raise ValueError('MPI participant capacity changed before launch')
            if _digest(Path(self.config.execution.lammps_path))!=state['executable_sha256']:
                raise ValueError('MPI executable changed after readiness')
            for item in spec.input_manifest:
                with open_regular(Path(state['directory']),item.path) as stream:
                    digest=hashlib.sha256();count=0
                    while chunk:=stream.read(min(65536,item.size-count+1)):
                        count+=len(chunk);digest.update(chunk)
                        if count>item.size: break
                if count!=item.size or digest.hexdigest()!=item.sha256:
                    raise ValueError('MPI input changed after readiness')
            state.update(phase='claimed',argv=argv)
            _write(self.control(attempt_id)/'state.json',state)
            env={**os.environ,'PYTHONPATH':str(Path(__file__).resolve().parents[1])}
            with (self.control(attempt_id)/'monitor.log').open('ab') as log, open(os.devnull,'rb') as null:
                argv=[sys.executable,'-m','lammps_manager.mpi_agent','monitor',
                      '--root',str(self.root),'--attempt',attempt_id]
                pid=os.posix_spawn(sys.executable,argv,env,setsid=True,file_actions=[
                    (os.POSIX_SPAWN_DUP2,null.fileno(),0),(os.POSIX_SPAWN_DUP2,log.fileno(),1),
                    (os.POSIX_SPAWN_DUP2,log.fileno(),2)])
            state['supervisor']=process_identity(pid)
            return dict(state)

    def close(self, request: dict, *, cancel: bool=False) -> dict:
        """Fence an allocation even when preparation never reached this node."""
        request=json.loads(json.dumps(request,sort_keys=True,allow_nan=False))
        attempt_id=_identifier(request['attempt_id'])
        spec=specification_from_dict(request['specification'])
        if request['worker_id']!=self.config.worker.id or not any(p.worker_id==self.config.worker.id for p in spec.mpi_placement):
            raise ValueError('MPI closure identity mismatch')
        _identifier(spec.job_id)
        directory=Path(self.config.work_root).resolve(strict=True)/spec.job_id/attempt_id
        control=self.control(attempt_id)
        control.mkdir(mode=0o700,exist_ok=True)
        # State is atomically published, so check retained ownership without
        # waiting for staging's lock. Recheck under the lock below as well.
        state_path=control/'state.json'
        if state_path.is_symlink():
            raise ValueError('Unsafe MPI state file')
        if state_path.exists():
            saved=json.loads(state_path.read_text())
            if 'request' in saved and saved['request']!=request:
                raise ValueError('Conflicting MPI closure identity')
        self.fence(attempt_id,cancel=cancel)
        with self.locked(attempt_id) as state:
            if state['phase']=='new':
                state.update(request=request,directory=str(directory/'work'),closed=True,
                             executable_sha256=request['executable_sha256'],phase='prepared')
                if directory.exists():
                    # An unowned path cannot be treated as proof of no execution.
                    state.update(phase='unknown',argv=['unresolved existing directory'])
            elif state['request']!=request:
                raise ValueError('Conflicting MPI closure identity')
        return self.status(attempt_id,close=True,cancel=cancel)

    def monitor(self, attempt_id: str) -> None:
        """Own wait(), deadline enforcement and durable process/outcome publication."""
        process=None
        try:
            with self.locked(attempt_id) as state:
                if state['phase']!='claimed':
                    return
                if state.get('closed') or (self.control(attempt_id)/'closed').exists():
                    state.update(phase='exited',launch_error='Launch fence closed before process start')
                    return
                request=state['request']; spec=specification_from_dict(request['specification'])
                if request['generation']['boot_id']!=Path('/proc/sys/kernel/random/boot_id').read_text().strip():
                    state.update(phase='exited',closed=True,launch_error='Worker boot changed before launch')
                    return
                os.sched_setaffinity(0,request['cpus'])
                directory=Path(state['directory']).parent
                environment={**dict(self.config.execution.environment),'OMP_NUM_THREADS':str(spec.resources.omp_threads_per_rank),
                             'OPENBLAS_NUM_THREADS':'1','LAMMPS_MPI_ROOT':str(self.root),'LAMMPS_MPI_ATTEMPT':attempt_id}
                _write(directory/'execution.json',dict(argv=state['argv'],cwd=state['directory'],environment=environment))
                with (directory/'stdout').open('xb') as stdout,(directory/'stderr').open('xb') as stderr:
                    process=subprocess.Popen(state['argv'],cwd=state['directory'],env={**os.environ,**environment},
                        stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,start_new_session=True)
                identity=process_identity(process.pid)
                _write(directory/'process.json',identity)
                state.update(phase='running',process=identity)
            deadline=time.monotonic()+spec.resources.time_limit_seconds
            reason=None
            while True:
                if process.poll() is not None and not session_members(identity['session']):
                    # Rank wrappers publish proofs under this same lock. Never read
                    # a partially written proof while checking escaped sessions.
                    with self.locked(attempt_id) as state:
                        if not any(_live_rank(rank) for rank in self.rank_processes(state)):
                            break
                if time.monotonic()>=deadline or (self.control(attempt_id)/'cancel').exists():
                    reason='MPI node deadline exceeded' if time.monotonic()>=deadline else 'MPI node cancellation requested'
                    self.fence(attempt_id,cancel=True)
                    with self.locked(attempt_id) as state:
                        state.update(closed=True,cancelled=True,reason=reason)
                    self.status(attempt_id,cancel=True)
                    break
                time.sleep(.02)
            code=process.wait(timeout=5)
            self.fence(attempt_id)
            with self.locked(attempt_id) as state:
                state.update(phase='exited',exit_code=code,closed=True)
                if reason: state['reason']=reason
                _write(Path(state['directory']).parent/'outcome.json',dict(exit_code=code,reason=state.get('reason')))
        except BaseException as error:
            self.fence(attempt_id)
            with self.locked(attempt_id) as state:
                state.update(closed=True,phase='unknown' if process else 'exited',
                             launch_error=type(error).__name__+': '+str(error))
            raise

    def rank_processes(self, state: dict) -> tuple[dict, ...]:
        """Read retained rank identities; callers verify them before signaling."""
        return tuple(json.loads(proof.read_text())['process']
                     for proof in Path(state['directory']).glob('.mpi-rank-*.json'))

    def status(self, attempt_id: str, *, close: bool=False, cancel: bool=False) -> dict:
        """Close launch rights before checking or cancelling the full known process session."""
        if close or cancel: self.fence(attempt_id,cancel=cancel)
        with self.locked(attempt_id) as state:
            if state.get('confirmed'):
                return state['confirmed']
            if close or cancel:
                state['closed']=True
                if cancel: state.update(cancelled=True,reason='MPI node cancellation requested')
                _write(self.control(attempt_id)/'state.json',state)
            result={k:state[k] for k in ('phase','directory','executable_sha256','reason') if k in state}
            result.update(terminated=False,session_members=[])
            identity=state.get('process')
            boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            if identity is None:
                if state['request']['generation']['boot_id']!=boot and state.get('closed'):
                    result.update(terminated=True,status='failed',exit_code=None,
                        evidence=dict(kind='boot_changed',observed_boot_id=boot,launch_fenced=True))
                elif ('argv' not in state and state.get('closed')) or state['phase']=='exited':
                    result.update(terminated=True,status='cancelled' if state.get('cancelled') else 'failed',exit_code=None,
                        evidence=dict(kind='never_started',observed_boot_id=boot,launch_fenced=True))
                elif state['phase'] in {'claimed','unknown'} and not (state.get('supervisor') and _same_process(state['supervisor'])):
                    result['phase']='unknown'
                if result['terminated']:
                    self.fence(attempt_id)
                    state['confirmed']=result
                return result
            if identity['boot_id']!=boot:
                self.fence(attempt_id)
                state['closed']=True
                result.update(terminated=True,status='failed',exit_code=None,
                    evidence=dict(kind='boot_changed',observed_boot_id=boot,launch_fenced=True,process=identity))
                state['confirmed']=result
                return result
            try:
                current=process_identity(identity['pid'])
            except FileNotFoundError:
                current=None
            if current is not None and not _same_process(identity):
                result['phase']='unknown'; result['reason']='MPI launch PID identity changed'
                return result
            if cancel and current is not None and _same_process(identity):
                terminate_group(identity['session'])
            # A rank wrapper records every native rank before exec. Preserve unknown
            # on escaped sessions; cancellation may signal only a verified rank's own session.
            for rank in self.rank_processes(state):
                if cancel and rank['session']==identity['session'] and _live_rank(rank):
                    terminate_group(identity['session'])
                if rank['session']!=identity['session'] and _live_rank(rank):
                    if cancel and rank['session']==rank['pid']==rank['process_group']:
                        terminate_group(rank['session'])
                    if _live_rank(rank):
                        result.update(phase='unknown',reason='MPI rank remains outside launch session')
                        return result
            live=session_members(identity['session'])
            result['session_members']=[list(member) for member in live]
            if live:
                return result
            if 'exit_code' not in state and state.get('supervisor') and _same_process(state['supervisor']):
                result['phase']='finishing'
                return result
            state['closed']=True
            self.fence(attempt_id)
            status='cancelled' if state.get('cancelled') else ('succeeded' if state.get('exit_code')==0 else 'failed')
            result.update(terminated=True,status=status,exit_code=state.get('exit_code'),
                evidence=dict(kind='session_ended',observed_boot_id=boot,launch_fenced=True,process=identity))
            state['confirmed']=result
            return result

    def collect(self, attempt_id: str) -> dict:
        """Retry per-participant verified result delivery without changing execution or deleting output."""
        from .results import enumerate_results, output_limits
        status=self.status(attempt_id)
        if not status.get('terminated'):
            raise ValueError('MPI results require confirmed local termination')
        directory=Path(status['directory']).parent
        directory.mkdir(mode=0o700,parents=True,exist_ok=True)
        _write(directory/'outcome.json',status)
        manifest=enumerate_results(directory,output_limits(self.config))
        suffix=f'/v2/mpi-results/{attempt_id}/'
        with self.request(suffix+'manifest',manifest) as response:
            reply=json.load(response)
        if reply['status']!='collected':
            for index,item in enumerate(manifest['files']):
                with open_regular(directory,item['path']) as stream, self.request(suffix+str(index),stream,item['size']) as response:
                    json.load(response)
        with self.request(suffix+'commit',{}) as response:
            reply=json.load(response)
        if reply['status']!='collected':
            raise ValueError('MPI results lack durable acknowledgement')
        return reply

    def launch_peer(self, argv: list[str]) -> None:
        """Forward only the active attempt's native ORTE launch to a configured pinned peer."""
        attempt_id=_identifier(os.environ['LAMMPS_MPI_ATTEMPT'])
        transport=json.loads((self.root/'config/mpi-ssh.json').read_text())
        while argv and argv[0] in {'-x','-X','-q'}: argv=argv[1:]
        if len(argv)<2 or argv[0] not in transport['peers']:
            raise ValueError('MPI remote shell requested an unconfigured peer')
        host=argv[0]
        command=shlex.split(' '.join(argv[1:]))
        if command and command[0]=='exec': command=command[1:]
        if not command or command[0] not in {'orted','/usr/bin/orted'}:
            raise ValueError('MPI remote shell accepts only native ORTE daemons')
        command=['/usr/bin/orted',*(arg for arg in command[1:] if arg!='--daemonize')]
        with self.locked(attempt_id) as state:
            if state['request']['role']!='leader' or state.get('closed') or (self.control(attempt_id)/'closed').exists():
                raise ValueError('MPI leader launch fence is closed')
        remote_command=shlex.join(['remote-mpi',attempt_id,json.dumps(command,separators=(',',':'))])
        subprocess.run([*transport['peers'][host]['ssh_argv'],remote_command],check=True,
                       stdin=subprocess.DEVNULL,timeout=20)

    def peer_gate(self, command: str) -> None:
        """Forced-key entry point; accept one configured ORTE launch for a prepared peer."""
        words=shlex.split(command)
        if len(words)!=3 or words[0]!='remote-mpi':
            raise ValueError('Restricted MPI key accepts only an attempt-scoped daemon launch')
        attempt_id=_identifier(words[1]); argv=json.loads(words[2])
        with self.locked(attempt_id) as state:
            if state['request']['role']!='peer': raise ValueError('MPI forced key cannot launch a leader')
        if not isinstance(argv,list) or not argv or argv[0]!='/usr/bin/orted' or '--daemonize' in argv:
            raise ValueError('Restricted MPI key requires a monitored non-daemonizing ORTE process')
        self.start(attempt_id,argv)

    def rank(self, argv: list[str]) -> None:
        """Record actual rank/host/boot placement and exec the fingerprinted application."""
        import socket
        attempt_id=_identifier(os.environ['LAMMPS_MPI_ATTEMPT'])
        rank=int(os.environ['OMPI_COMM_WORLD_RANK']); size=int(os.environ['OMPI_COMM_WORLD_SIZE'])
        with self.locked(attempt_id) as state:
            if state.get('closed') or (self.control(attempt_id)/'closed').exists():
                raise ValueError('MPI rank launch fence is closed')
            spec=specification_from_dict(state['request']['specification'])
            offset=0; allowed=False
            for member in spec.mpi_placement:
                if member.worker_id==self.config.worker.id:
                    allowed=offset<=rank<offset+member.mpi_ranks
                    break
                offset+=member.mpi_ranks
            if size!=spec.resources.mpi_ranks or not allowed:
                raise ValueError('Actual MPI rank placement differs from reserved shares')
            if _digest(Path(self.config.execution.lammps_path))!=state['executable_sha256']:
                raise ValueError('MPI application executable changed before rank exec')
            if Path('/proc/sys/kernel/random/boot_id').read_text().strip()!=state['request']['generation']['boot_id']:
                raise ValueError('MPI rank boot differs from reserved generation')
            proof=Path(state['directory'])/f'.mpi-rank-{rank:06d}.json'
            with proof.open('x') as stream:
                json.dump(dict(rank=rank,size=size,worker_id=self.config.worker.id,hostname=socket.gethostname(),
                               process=process_identity(os.getpid()),affinity=sorted(os.sched_getaffinity(0))),stream)
                stream.flush();os.fsync(stream.fileno())
            sync_directory(proof.parent)
        os.execv(self.config.execution.lammps_path,[self.config.execution.lammps_path,*self.config.execution.lammps_args,*argv])


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('rpc','monitor','ssh','peer-gate','rank'))
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--attempt')
    args,arguments=parser.parse_known_args()
    if arguments[:1]==['--']: arguments=arguments[1:]
    if arguments and args.action not in {'ssh','rank'}:
        parser.error('Unexpected MPI helper arguments')
    node=Node(args.root,load_config(args.root/'config/worker.yaml','worker',check_local_paths=True))
    if args.action=='monitor':
        node.monitor(args.attempt)
        return
    if args.action=='ssh': node.launch_peer(arguments);return
    if args.action=='peer-gate': node.peer_gate(os.environ.get('SSH_ORIGINAL_COMMAND',''));return
    if args.action=='rank': node.rank(arguments);return
    request=json.load(sys.stdin)
    action=request.pop('action')
    if action=='prepare': result=node.prepare(request)
    elif action=='start': result=node.start(request['attempt_id'],request['argv'])
    elif action=='status': result=node.status(request['attempt_id'],close=request.get('close',False),cancel=request.get('cancel',False))
    elif action=='collect': result=node.collect(request['attempt_id'])
    elif action=='close': result=node.close(request['request'],cancel=request.get('cancel',False))
    else: raise ValueError('Unsupported MPI node action')
    print(json.dumps(result,sort_keys=True))


if __name__=='__main__': main()
