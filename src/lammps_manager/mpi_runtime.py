"""Explicit manager-side native MPI coordination using durable node helpers."""

from dataclasses import asdict, dataclass, replace
from datetime import datetime
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import time
from uuid import uuid4

from .config import ManagerConfig
from .cpu import admission,pending_slots
from .models import specification_from_dict
from .multihost import reserve,prepare,authorize,terminate,finish,members,recover_prelaunch
from .store import Store,StoreError,TERMINAL


@dataclass(frozen=True)
class Host:
    """Operator-owned transport settings; secret values belong in protected SSH files."""

    address: str
    root: str
    executable_sha256: str
    ssh_argv: tuple[str,...] = ()

    def __post_init__(self) -> None:
        if ipaddress.ip_address(self.address).version!=4:
            raise ValueError('Native MPI transport currently requires literal IPv4 addresses')
        if (not Path(self.root).is_absolute() or '..' in Path(self.root).parts or '\x00' in self.root
                or not re.fullmatch('[0-9a-f]{64}',self.executable_sha256)
                or not isinstance(self.ssh_argv,tuple) or any(not isinstance(a,str) or '\x00' in a for a in self.ssh_argv)):
            raise ValueError('Invalid MPI host root, executable identity or SSH argument array')


class Session:
    """One manager SQLite authority; retries observe the original attempt, never resubmit."""

    def __init__(self,config: ManagerConfig,hosts: dict[str,Host],*,rpc_timeout_seconds: float=30) -> None:
        if config.storage.mode!='staged' or not 0<rpc_timeout_seconds<=300:
            raise ValueError('MPI coordination requires staged mode and a bounded RPC timeout')
        self.config=config;self.hosts=hosts;self.rpc_timeout_seconds=rpc_timeout_seconds

    def rpc(self,worker_id: str,request: dict) -> dict:
        """Run one bounded authenticated helper command; transport success is not process evidence."""
        host=self.hosts[worker_id];root=Path(host.root)
        command=['env','PYTHONPATH='+str(root/'src'),str(root/'venv/bin/python3'),
                 '-m','lammps_manager.mpi_agent','rpc','--root',host.root]
        argv=[*host.ssh_argv,shlex.join(command)] if host.ssh_argv else command
        process=subprocess.Popen(argv,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                                 start_new_session=True)
        try:
            stdout,stderr=process.communicate(json.dumps(request,allow_nan=False).encode(),timeout=self.rpc_timeout_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid,signal.SIGKILL);process.communicate()
            raise TimeoutError('MPI helper observation timed out on '+worker_id+'; retain original attempt') from None
        if process.returncode:
            raise RuntimeError('MPI helper failed on '+worker_id+': '+stderr.decode(errors='replace')[-4000:])
        return json.loads(stdout)

    def _plan(self,store: Store,attempt_id: str) -> dict:
        row=store._db.execute('SELECT plan FROM mpi_runtime WHERE attempt_id=?',(attempt_id,)).fetchone()
        if row is None: raise StoreError('MPI attempt has no durable execution plan')
        plan=json.loads(row[0])
        for worker,settings in plan['hosts'].items():
            if worker not in self.hosts or settings!={key:getattr(self.hosts[worker],key) for key in ('address','root','executable_sha256')}:
                raise StoreError('MPI host settings differ from original execution plan')
        return plan

    def reserve(self,job_id: str,*,attempt_id: str | None=None) -> str:
        """Apply fresh metric/load and effective-quota checks, then atomically reserve the whole group."""
        attempt_id=attempt_id or uuid4().hex
        with Store(self.config.manager.database_path) as store,store.scheduling_round():
            if store._db.execute('SELECT 1 FROM mpi_runtime WHERE attempt_id=?',(attempt_id,)).fetchone():
                self._plan(store,attempt_id)
                if store.attempt(attempt_id).job_id!=job_id:raise StoreError('Conflicting MPI attempt identity')
                return attempt_id
            spec=store.specification(job_id)
            configured={w.id:w for w in self.config.workers}
            records=store.worker_records();policy=self.config.scheduler;now=time.time()
            limits={};identities={};requests={};host_plan={}
            if not spec.mpi_placement:raise StoreError('MPI coordination requires explicit immutable placement')
            fingerprints={self.hosts[p.worker_id].executable_sha256 for p in spec.mpi_placement if p.worker_id in self.hosts}
            if len(fingerprints)!=1:raise StoreError('MPI participants need one common application executable')
            for index,participant in enumerate(spec.mpi_placement):
                worker=participant.worker_id
                if worker not in configured or worker not in self.hosts or worker not in records:
                    raise StoreError('MPI participant is not configured/online: '+worker)
                record=records[worker];sample=record['snapshot'];capacity=configured[worker]
                if (not sample or record['paused'] or not capacity.enabled or not sample.get('ready',True)
                        or sample.get('draining',False) or not sample['capacity']['enabled']
                        or not 0<=now-sample['heartbeat_at']<=self.config.manager.heartbeat_timeout_seconds
                        or not 0<=now-sample['observed_at']<=policy.metrics_max_age_seconds):
                    raise StoreError('MPI participant metrics/readiness are unavailable: '+worker)
                effective=replace(capacity,cpu_slots=min(capacity.cpu_slots,sample['capacity']['cpu_slots'],sample['detected_cpu_slots']),
                    memory_budget_gib=min(capacity.memory_budget_gib,sample['capacity']['memory_budget_gib'],sample['detected_memory_gib']),
                    max_concurrent_jobs=min(capacity.max_concurrent_jobs,sample['capacity']['max_concurrent_jobs']))
                held=store.reservations(worker);pending_memory=0
                for reservation in held:
                    started=store.attempt(reservation.attempt_id).started_at
                    if started is None or datetime.fromisoformat(started.replace('Z','+00:00')).timestamp()>=sample['observed_at']:
                        pending_memory+=reservation.memory_gib
                if sample['available_memory_gib']-pending_memory < participant.memory_gib+policy.reserve_memory_gib:
                    raise StoreError('MPI participant available-memory reserve is insufficient: '+worker)
                charge=pending_slots(store,worker,sample.get('cpu_sample_started_at')) if policy.account_external_load else 0
                evidence=admission(sample.get('per_cpu_utilization_percent'),sample.get('cpu_sample_started_at'),
                    threshold=policy.max_cpu_utilization_percent,pending=charge,
                    requested=participant.mpi_ranks*spec.resources.omp_threads_per_rank,
                    used=sum(r.cpu_slots for r in held),minimum=policy.min_slots,reserve=policy.reserve_slots,
                    capacity=effective.cpu_slots,enabled=policy.account_external_load)
                if not evidence['allowed']:raise StoreError('MPI participant CPU admission blocked: '+worker)
                generations=store.generations(worker)
                if not generations:raise StoreError('MPI participant lacks a pinned generation')
                generation=generations[-1]
                identities[worker]=(generation['boot_id'],generation['session_id']);limits[worker]=effective
                cpus=sorted(map(int,sample.get('per_cpu_utilization_percent') or ()))
                if len(cpus)<participant.mpi_ranks*spec.resources.omp_threads_per_rank:
                    raise StoreError('MPI participant lacks a verified CPU affinity scope')
                requests[worker]=dict(attempt_id=attempt_id,worker_id=worker,specification=asdict(spec),
                    generation={key:generation[key] for key in ('boot_id','session_id')},
                    executable_sha256=self.hosts[worker].executable_sha256,
                    cpus=cpus[:effective.cpu_slots],role='leader' if index==0 else 'peer')
                host_plan[worker]={key:getattr(self.hosts[worker],key) for key in ('address','root','executable_sha256')}
            reserve(store,job_id,attempt_id,str(Path(spec.work_directory)/attempt_id),limits,identities)
            plan=dict(requests=requests,hosts=host_plan,order=[p.worker_id for p in spec.mpi_placement])
            store._db.execute('INSERT INTO mpi_runtime VALUES(?,?)',(attempt_id,json.dumps(plan,sort_keys=True)))
        return attempt_id

    def prepare(self,attempt_id: str) -> dict:
        """Stage every original participant; identical retries retain their original fences."""
        with Store(self.config.manager.database_path) as store,store.scheduling_round():
            plan=self._plan(store,attempt_id)
            if store.attempt(attempt_id).state=='unknown':
                self._launch_admission(store,attempt_id,plan)
                recover_prelaunch(store,attempt_id)
            existing={m['worker_id']:m for m in members(store,attempt_id)}
        results={}
        for worker in plan['order']:
            if existing[worker]['phase']!='reserved':continue
            reply=self.rpc(worker,{'action':'prepare',**plan['requests'][worker]})
            if reply.get('phase')!='prepared':raise StoreError('MPI helper is not ready: '+worker)
            with Store(self.config.manager.database_path) as store:
                request=plan['requests'][worker]
                prepare(store,attempt_id,worker,**request['generation'],directory=reply['directory'],executable_sha256=reply['executable_sha256'])
            results[worker]=reply
        return results

    def start(self,attempt_id: str) -> dict:
        """Authorize one native MPI invocation only after the durable readiness barrier."""
        with Store(self.config.manager.database_path) as store,store.scheduling_round():
            plan=self._plan(store,attempt_id)
            self._launch_admission(store,attempt_id,plan)
            recover_prelaunch(store,attempt_id)
            if not authorize(store,attempt_id):raise StoreError('MPI group is not authorized for a new launch')
            spec=store.specification(store.attempt(attempt_id).job_id)
        leader=plan['order'][0];root=Path(self.hosts[leader].root)
        destinations=','.join(self.hosts[p.worker_id].address+':'+str(p.mpi_ranks) for p in spec.mpi_placement)
        argv=['/usr/bin/mpirun.openmpi','--noprefix','--host',destinations,'--map-by','slot','--rank-by','slot','--bind-to','none',
            '--mca','btl','self,tcp','--mca','pml','ob1','--mca','mpi_yield_when_idle','1','--mca','plm_rsh_agent',str(root/'bin/mpi-ssh'),
            '--mca','plm_rsh_no_tree_spawn','1','--mca','plm_rsh_assume_same_shell','1',
            '-x','LAMMPS_MPI_ROOT','-x','LAMMPS_MPI_ATTEMPT','-np',str(spec.resources.mpi_ranks),
            str(root/'bin/mpi-rank'),'-in',spec.input_file]
        return self.rpc(leader,dict(action='start',attempt_id=attempt_id,argv=argv))

    def _launch_admission(self,store: Store,attempt_id: str,plan: dict) -> None:
        """Recheck all current safety gates, excluding the candidate's held shares exactly once."""
        now=time.time();policy=self.config.scheduler
        records=store.worker_records();configured={w.id:w for w in self.config.workers}
        spec=store.specification(store.attempt(attempt_id).job_id)
        for participant in spec.mpi_placement:
            worker=participant.worker_id;record=records[worker];sample=record['snapshot'];capacity=configured[worker]
            if (not sample or record['paused'] or not capacity.enabled or not sample['capacity']['enabled']
                    or not sample.get('ready',True) or sample.get('draining',False)
                    or not 0<=now-sample['heartbeat_at']<=self.config.manager.heartbeat_timeout_seconds
                    or not 0<=now-sample['observed_at']<=policy.metrics_max_age_seconds
                    or not set(plan['requests'][worker]['cpus'])<=set(map(int,sample.get('per_cpu_utilization_percent') or ()))):
                raise StoreError('MPI participant safety state changed before launch: '+worker)
            held=[r for r in store.reservations(worker) if r.attempt_id!=attempt_id]
            cpu=min(capacity.cpu_slots,sample['capacity']['cpu_slots'],sample['detected_cpu_slots'])
            memory=min(capacity.memory_budget_gib,sample['capacity']['memory_budget_gib'],sample['detected_memory_gib'])
            count=min(capacity.max_concurrent_jobs,sample['capacity']['max_concurrent_jobs'])
            if (sum(r.cpu_slots for r in held)+participant.mpi_ranks*spec.resources.omp_threads_per_rank>cpu
                    or sum(r.memory_gib for r in held)+participant.memory_gib>memory or len(held)+1>count):
                raise StoreError('MPI participant quota changed before launch: '+worker)
            pending_memory=0
            for reservation in held:
                started=store.attempt(reservation.attempt_id).started_at
                if started is None or datetime.fromisoformat(started.replace('Z','+00:00')).timestamp()>=sample['observed_at']:
                    pending_memory+=reservation.memory_gib
            if sample['available_memory_gib']-pending_memory<participant.memory_gib+policy.reserve_memory_gib:
                raise StoreError('MPI participant available memory changed before launch: '+worker)
            pending=pending_slots(store,worker,sample.get('cpu_sample_started_at'),candidate_id=attempt_id) if policy.account_external_load else 0
            gate=admission(sample.get('per_cpu_utilization_percent'),sample.get('cpu_sample_started_at'),
                threshold=policy.max_cpu_utilization_percent,pending=pending,
                requested=participant.mpi_ranks*spec.resources.omp_threads_per_rank,used=sum(r.cpu_slots for r in held),
                minimum=policy.min_slots,reserve=policy.reserve_slots,capacity=cpu,enabled=policy.account_external_load)
            if not gate['allowed']:raise StoreError('MPI participant CPU headroom changed before launch: '+worker)

    def observe(self,attempt_id: str) -> dict:
        """Observe the original group; errors retain reservations and never create successors."""
        with Store(self.config.manager.database_path) as store:
            plan=self._plan(store,attempt_id);group={m['worker_id']:m for m in members(store,attempt_id)}
            cancellation=store.cancellation(attempt_id)
        abort=bool(cancellation) or any(m['phase']=='terminated' and m['status']!='succeeded' for m in group.values())
        results={};errors={};leader_done=group[plan['order'][0]]['phase']=='terminated'
        for worker in plan['order']:
            saved=group[worker]
            if saved['phase']=='terminated':
                results[worker]=dict(terminated=True,status=saved['status'],exit_code=saved['exit_code'],evidence=json.loads(saved['termination']))
                continue
            try:
                if abort or leader_done:
                    reply=self.rpc(worker,dict(action='close',request=plan['requests'][worker],cancel=abort))
                else:reply=self.rpc(worker,dict(action='status',attempt_id=attempt_id))
                results[worker]=reply
                if reply.get('terminated'):
                    with Store(self.config.manager.database_path) as store:
                        terminate(store,attempt_id,worker,evidence=reply['evidence'],status=reply['status'],exit_code=reply['exit_code'])
                    if worker==plan['order'][0]:leader_done=True
                    if reply['status']!='succeeded':abort=True
                elif worker==plan['order'][0] and reply.get('phase') in {'running','finishing'}:
                    with Store(self.config.manager.database_path) as store:
                        if store.attempt(attempt_id).state=='assigned':store.transition(attempt_id,'running')
            except (OSError,ValueError,RuntimeError) as error:
                errors[worker]=type(error).__name__+': '+str(error)
        with Store(self.config.manager.database_path) as store:
            completed=finish(store,attempt_id)
            state=store.attempt(attempt_id).state
        return dict(attempt_id=attempt_id,state=state,terminated=completed is not None,participants=results,errors=errors)

    def cancel(self,attempt_id: str) -> dict:
        """Persist whole-group cancellation intent before delivering any node command."""
        with Store(self.config.manager.database_path) as store:
            self._plan(store,attempt_id);store.request_cancel(attempt_id)
        return self.observe(attempt_id)

    def collect(self,attempt_id: str) -> dict:
        """Retry only original result delivery; process success remains separate from collection."""
        with Store(self.config.manager.database_path) as store:
            plan=self._plan(store,attempt_id)
            if store.attempt(attempt_id).state not in TERMINAL:raise StoreError('MPI group has not terminated')
        replies={worker:self.rpc(worker,dict(action='collect',attempt_id=attempt_id)) for worker in plan['order']}
        with Store(self.config.manager.database_path) as store:
            collection=store.result_collection(attempt_id)
        return dict(participants=replies,group_collected=bool(collection and collection['status']=='collected'))
