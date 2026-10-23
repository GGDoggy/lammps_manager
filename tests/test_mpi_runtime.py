"""Original-attempt coordination and conservative transport-failure handling."""

from dataclasses import asdict,replace
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

from lammps_manager.config import ManagerConfig,ManagerSettings,Storage,Scheduler,WorkerCapacity
from lammps_manager.models import JobSpecification,InputFile,MpiParticipant,Resources
from lammps_manager.mpi_runtime import Host,Session
from lammps_manager.scheduler import WorkerSnapshot
from lammps_manager.store import Store,StoreError


class MpiRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'manager.sqlite'
        capacities=tuple(WorkerCapacity(n,2,1,1) for n in ('a','b'))
        self.config=ManagerConfig(ManagerSettings(str(self.path)),Storage('/jobs',mode='staged'),capacities,
                                  scheduler=Scheduler(reserve_memory_gib=.1))
        self.hosts={n:Host('192.0.2.'+str(i+1),'/opt/mpi','b'*64,('ssh',n)) for i,n in enumerate(('a','b'))}
        self.session=Session(self.config,self.hosts)
        self.spec=JobSpecification('job','/jobs/job','in','a'*64,Resources(mpi_ranks=2,memory_gib=.2),
            storage_mode='staged',input_manifest=(InputFile('in',1,'a'*64),),
            mpi_placement=(MpiParticipant('a',1,.1),MpiParticipant('b',1,.1)))
        self.identities={n:(str(uuid4()),str(uuid4())) for n in ('a','b')}
        with Store(self.path) as store:
            store.submit(self.spec)
            for capacity in capacities:
                store.worker_generation(capacity.id,*self.identities[capacity.id])
                now=time.time()
                snapshot=WorkerSnapshot(capacity,2,8,now,now,0,8,per_cpu_utilization_percent={'0':0,'1':0},cpu_sample_started_at=now-.1)
                store.set_worker(capacity.id,snapshot=asdict(snapshot))

    def reserve(self):return self.session.reserve('job',attempt_id='attempt')

    def ready(self):
        with patch.object(self.session,'rpc',return_value=dict(phase='prepared',directory='/node/job/attempt/work',executable_sha256='b'*64)):
            self.session.prepare('attempt')

    def outcome(self,worker):
        boot=self.identities[worker][0]
        return dict(terminated=True,status='succeeded',exit_code=0,evidence=dict(kind='session_ended',observed_boot_id=boot,
            launch_fenced=True,process=dict(pid=20,session=20,process_group=20,start_ticks='1',boot_id=boot)))

    def test_durable_plan_replay_and_host_root_binding(self):
        self.reserve();self.reserve()
        with Store(self.path) as store:self.assertEqual(len(store.reservations()),2)
        changed=Session(self.config,{**self.hosts,'b':replace(self.hosts['b'],root='/different')})
        with self.assertRaises(StoreError):changed.prepare('attempt')

    def test_launch_rechecks_paused_or_stale_worker_before_rpc(self):
        self.reserve();self.ready()
        with Store(self.path) as store:store.set_worker('b',paused=True)
        with patch.object(self.session,'rpc') as rpc:
            with self.assertRaises(StoreError):self.session.start('attempt')
            rpc.assert_not_called()
        with Store(self.path) as store:
            store.set_worker('b',paused=False)
            record=store.worker_records()['b']['snapshot'];record['observed_at']=0
            store.set_worker('b',snapshot=record)
        with patch.object(self.session,'rpc') as rpc:
            with self.assertRaises(StoreError):self.session.start('attempt')
            rpc.assert_not_called()

    def test_missing_peer_after_leader_exit_holds_every_share_then_converges(self):
        self.reserve();self.ready()
        with patch.object(self.session,'rpc',return_value={'phase':'claimed'}) as launch:
            self.session.start('attempt');self.assertEqual(launch.call_count,1)
            command=launch.call_args.args[1]['argv']
            self.assertIn('192.0.2.1:1,192.0.2.2:1',command)
            self.assertIn('--noprefix',command)
        def first(worker,request):
            if worker=='b':raise TimeoutError('disconnected')
            return self.outcome(worker)
        with patch.object(self.session,'rpc',side_effect=first):
            report=self.session.observe('attempt')
        self.assertFalse(report['terminated']);self.assertIn('b',report['errors'])
        with Store(self.path) as store:self.assertEqual(len(store.reservations()),2)
        with patch.object(self.session,'rpc',side_effect=lambda worker,request:self.outcome(worker)) as rpc:
            report=self.session.observe('attempt')
            self.assertTrue(report['terminated']);self.assertEqual(report['state'],'succeeded')
            self.assertEqual(rpc.call_count,1)
            self.assertEqual(rpc.call_args.args[1]['action'],'close')
        with Store(self.path) as store:self.assertEqual(store.reservations(),())

    def test_preparation_absence_can_be_fenced_on_all_nodes_before_release(self):
        self.reserve()
        def reply(worker,request):
            self.assertEqual(request['action'],'close');self.assertTrue(request['cancel'])
            return dict(terminated=True,status='cancelled',exit_code=None,
                evidence=dict(kind='never_started',launch_fenced=True,observed_boot_id=self.identities[worker][0]))
        with patch.object(self.session,'rpc',side_effect=reply):result=self.session.cancel('attempt')
        self.assertTrue(result['terminated']);self.assertEqual(result['state'],'cancelled')
        with Store(self.path) as store:self.assertEqual(store.reservations(),())

    def reconnect(self):
        with Store(self.path) as store:
            store.expire_workers(time.time()+100,1)
            self.assertEqual(store.attempt('attempt').state,'unknown')
            for worker,record in store.worker_records().items():
                sample=record['snapshot']
                sample.update(heartbeat_at=time.time(),observed_at=time.time())
                store.set_worker(worker,snapshot=sample)

    def assert_recovered_start(self):
        with patch.object(self.session,'rpc',return_value={'phase':'claimed'}) as rpc:
            self.session.start('attempt')
            self.assertEqual(rpc.call_count,1)
        with Store(self.path) as store:
            self.assertEqual(len(store.job_attempts('job')),1)
            self.assertEqual(len(store.reservations()),2)

    def test_prepared_attempt_recovers_after_heartbeat_expiry(self):
        self.reserve();self.ready();self.reconnect()
        self.assert_recovered_start()

    def test_unprepared_attempt_recovers_after_heartbeat_expiry(self):
        self.reserve();self.reconnect();self.ready()
        self.assert_recovered_start()

    def test_partial_preparation_resumes_only_original_remaining_peer(self):
        self.reserve()
        def first(worker,request):
            if worker=='b':raise TimeoutError('staging interrupted')
            return dict(phase='prepared',directory='/node/job/attempt/work',executable_sha256='b'*64)
        with patch.object(self.session,'rpc',side_effect=first):
            with self.assertRaises(TimeoutError):self.session.prepare('attempt')
        self.reconnect()
        reply=dict(phase='prepared',directory='/node/job/attempt/work',executable_sha256='b'*64)
        with patch.object(self.session,'rpc',return_value=reply) as rpc:
            self.session.prepare('attempt')
            self.assertEqual(rpc.call_count,1)
            self.assertEqual(rpc.call_args.args[0],'b')
        self.assert_recovered_start()

    def test_authorized_unknown_attempt_cannot_be_restored_for_launch(self):
        self.reserve();self.ready()
        with patch.object(self.session,'rpc',side_effect=TimeoutError):
            with self.assertRaises(TimeoutError):self.session.start('attempt')
        self.reconnect()
        with patch.object(self.session,'rpc') as rpc:
            with self.assertRaises(StoreError):self.session.start('attempt')
            with self.assertRaises(StoreError):self.session.prepare('attempt')
            rpc.assert_not_called()
        with Store(self.path) as store:
            self.assertEqual(store.attempt('attempt').state,'unknown')
            self.assertEqual(len(store.reservations()),2)

    def test_recovery_rejects_cancellation_changed_generation_and_failed_admission(self):
        self.reserve();self.ready();self.reconnect()
        with Store(self.path) as store:store.set_worker('b',paused=True)
        with patch.object(self.session,'rpc') as rpc:
            with self.assertRaises(StoreError):self.session.start('attempt')
            with self.assertRaises(StoreError):self.session.prepare('attempt')
            rpc.assert_not_called()
        with Store(self.path) as store:
            store.set_worker('b',paused=False)
            store.worker_generation('b',str(uuid4()),str(uuid4()))
        with patch.object(self.session,'rpc') as rpc:
            with self.assertRaises(StoreError):self.session.start('attempt')
            rpc.assert_not_called()
        with Store(self.path) as store:
            store.worker_generation('b',*self.identities['b'])
            store.request_cancel('attempt')
        with patch.object(self.session,'rpc') as rpc:
            with self.assertRaises(StoreError):self.session.start('attempt')
            rpc.assert_not_called()
        with Store(self.path) as store:self.assertEqual(len(store.reservations()),2)


if __name__=='__main__':unittest.main()
