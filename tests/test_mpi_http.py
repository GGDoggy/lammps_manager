"""MPI artifact ownership, complete result publication and ordinary-poll isolation."""

from dataclasses import asdict
import hashlib
import io
import json
from pathlib import Path
import tempfile
import os
import sys
import threading
import time
import unittest
from uuid import uuid4

from lammps_manager.config import Execution, ManagerConfig, ManagerSettings, Storage, WorkerCapacity, WorkerConfig, Scheduler
from lammps_manager.manager import ManagerService,make_server
from lammps_manager.mpi_agent import Node
from lammps_manager.models import InputFile, JobSpecification, MpiParticipant, Resources
from lammps_manager.multihost import reserve,prepare,authorize,terminate,finish
from lammps_manager.scheduler import WorkerSnapshot
from lammps_manager.store import Store


class MpiHTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);(self.root/'jobs').mkdir()
        self.capacities={name:WorkerCapacity(name,2,1,1) for name in ('a','b','c')}
        self.config=ManagerConfig(ManagerSettings(str(self.root/'db.sqlite'),.01),
            Storage(str(self.root/'jobs'),mode='staged'),tuple(self.capacities.values()),
            scheduler=Scheduler(account_external_load=False,reserve_memory_gib=0))
        tokens={}
        for name in self.capacities:
            p=self.root/(name+'.token');p.write_text(name*40);p.chmod(0o600);tokens[name]=str(p)
        self.service=ManagerService(self.config,tokens);self.addCleanup(self.service.close)
        self.identities={name:(str(uuid4()),str(uuid4())) for name in ('a','b')}
        self.identities['a']=(Path('/proc/sys/kernel/random/boot_id').read_text().strip(),self.identities['a'][1])
        digest=hashlib.sha256(b'x').hexdigest()
        self.spec=JobSpecification('job',str(self.root/'jobs/job'),'in',digest,
            Resources(mpi_ranks=2,memory_gib=.2),storage_mode='staged',
            input_manifest=(InputFile('in',1,digest),),
            mpi_placement=(MpiParticipant('a',1,.1),MpiParticipant('b',1,.1)))
        (Path(self.spec.work_directory)/'inputs').mkdir(parents=True)
        (Path(self.spec.work_directory)/'inputs/in').write_bytes(b'x')
        with Store(self.config.manager.database_path) as store:
            store.submit(self.spec)
            for name,identity in self.identities.items():
                store.worker_generation(name,*identity)
                now=time.time()
                store.set_worker(name,snapshot=asdict(WorkerSnapshot(self.capacities[name],2,8,now,now,0,8)))
            reserve(store,'job','attempt',str(Path(self.spec.work_directory)/'attempt'),self.capacities,self.identities)

    def end(self):
        with Store(self.config.manager.database_path) as store:
            for name in ('a','b'):
                prepare(store,'attempt',name,boot_id=self.identities[name][0],session_id=self.identities[name][1],directory='/worker/job/attempt/work',executable_sha256='b'*64)
            authorize(store,'attempt');store.transition('attempt','running')
            for name in ('a','b'):
                identity=dict(pid=10,process_group=10,session=10,start_ticks='10',boot_id=self.identities[name][0])
                terminate(store,'attempt',name,evidence=dict(kind='session_ended',observed_boot_id=self.identities[name][0],process=identity,launch_fenced=True),status='succeeded',exit_code=0)
            finish(store,'attempt')

    def manifest(self,content):
        return {'files':[dict(path='stdout',size=len(content),sha256=hashlib.sha256(content).hexdigest())],'omitted':[]}

    def upload(self,name,content):
        manifest=self.manifest(content)
        self.service.mpi_result_operation(name,'attempt','manifest',manifest)
        self.service.mpi_result_operation(name,'attempt','0',stream=io.BytesIO(content),length=len(content))
        return self.service.mpi_result_operation(name,'attempt','commit',{})

    def test_peer_inputs_and_no_ordinary_assignment(self):
        with self.service.input_artifact('b','attempt',0)[0] as stream:self.assertEqual(stream.read(),b'x')
        with self.assertRaises(ValueError):self.service.input_artifact('c','attempt',0)
        now=time.time()
        snapshot=WorkerSnapshot(self.capacities['a'],2,8,now,now,0,8)
        response=self.service.poll('a',dict(protocol=2,storage_mode='staged',snapshot=asdict(snapshot),reports=[]))
        self.assertEqual(response['assignments'],[])
        with Store(self.config.manager.database_path) as store:self.assertEqual(len(store.reservations()),2)

    def test_requires_every_participant_then_publishes_idempotently(self):
        with self.assertRaises(ValueError):self.service.mpi_result_operation('a','attempt','manifest',self.manifest(b'a'))
        self.end()
        with self.assertRaises(ValueError):self.service.mpi_result_operation('c','attempt','manifest',self.manifest(b'a'))
        self.assertEqual(self.upload('a',b'leader')['status'],'collected')
        with Store(self.config.manager.database_path) as store:self.assertNotEqual(store.result_collection('attempt')['status'],'collected')
        self.assertEqual(self.upload('b',b'peer')['status'],'collected')
        self.assertEqual(self.service.mpi_result_operation('b','attempt','commit',{})['status'],'collected')
        with Store(self.config.manager.database_path) as store:
            result=store.result_collection('attempt')
            self.assertEqual(result['status'],'collected')
            directory=Path(store.attempt('attempt').directory)/'results'
            self.assertEqual((directory/'stdout').read_bytes(),b'leader')
            self.assertEqual((directory/'work/mpi-participants/b/stdout').read_bytes(),b'peer')
            for item in result['manifest']['files']:
                self.assertEqual(hashlib.sha256((directory/item['path']).read_bytes()).hexdigest(),item['sha256'])
        with self.assertRaises(ValueError):self.service.mpi_result_operation('b','attempt','manifest',self.manifest(b'changed'))

    def test_bad_hash_or_missing_peer_never_claims_complete_delivery(self):
        self.end()
        self.service.mpi_result_operation('a','attempt','manifest',self.manifest(b'valid'))
        with self.assertRaises(ValueError):self.service.mpi_result_operation('a','attempt','0',stream=io.BytesIO(b'wrong'),length=5)
        with self.assertRaises((OSError,ValueError)):self.service.mpi_result_operation('a','attempt','commit',{})
        with Store(self.config.manager.database_path) as store:
            result=store.result_collection('attempt')
            self.assertTrue(result is None or result['status']!='collected')

    def test_real_loopback_agent_stages_runs_fake_process_and_collects_group(self):
        server=make_server(self.service,'127.0.0.1',0)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01});thread.start()
        def stop():server.shutdown();thread.join(timeout=5);server.server_close()
        self.addCleanup(stop)
        root=self.root/'node-a';(root/'config').mkdir(parents=True);(root/'worker').mkdir()
        program=root/'lmp';program.write_text('#!/bin/sh\nexit 0\n');program.chmod(0o700)
        config=WorkerConfig(self.capacities['a'],f'http://127.0.0.1:{server.server_port}',str(root/'worker'),
            Execution(str(program),sys.executable),str(self.root/'a.token'),storage_mode='staged')
        data=asdict(config);data['execution']['environment']={}
        (root/'config/worker.yaml').write_text(json.dumps(data))
        node=Node(root,config)
        digest=hashlib.sha256(program.read_bytes()).hexdigest()
        generation=dict(zip(('boot_id','session_id'),self.identities['a']))
        ready=node.prepare(dict(attempt_id='attempt',specification=asdict(self.spec),worker_id='a',
            generation=generation,executable_sha256=digest,cpus=sorted(os.sched_getaffinity(0))[:1],role='leader'))
        with Store(self.config.manager.database_path) as store:
            for name in ('a','b'):
                prepare(store,'attempt',name,boot_id=self.identities[name][0],session_id=self.identities[name][1],
                        directory=ready['directory'],executable_sha256=digest)
            authorize(store,'attempt')
        started=node.start('attempt',[sys.executable,'-c',"print('fake MPI result')"])
        try:
            deadline=time.monotonic()+10
            while time.monotonic()<deadline:
                result=node.status('attempt')
                if result['terminated']:break
                time.sleep(.02)
            self.assertTrue(result['terminated'])
            self.assertEqual(result['status'],'succeeded')
            with Store(self.config.manager.database_path) as store:
                store.transition('attempt','running')
                terminate(store,'attempt','a',evidence=result['evidence'],status=result['status'],exit_code=result['exit_code'])
                process=dict(pid=10,session=10,process_group=10,start_ticks='1',boot_id=self.identities['b'][0])
                terminate(store,'attempt','b',evidence=dict(kind='session_ended',observed_boot_id=self.identities['b'][0],launch_fenced=True,process=process),status='succeeded',exit_code=0)
                finish(store,'attempt')
            self.upload('b',b'synthetic peer fixture')
            self.assertTrue(node.collect('attempt')['group_collected'])
            self.assertTrue(node.collect('attempt')['group_collected'])
            with Store(self.config.manager.database_path) as store:
                self.assertEqual(store.result_collection('attempt')['status'],'collected')
                directory=Path(store.attempt('attempt').directory)/'results'
                self.assertIn('fake MPI result',(directory/'stdout').read_text())
        finally:
            node.status('attempt',cancel=True)
            try:os.waitpid(started['supervisor']['pid'],0)
            except ChildProcessError:pass


if __name__=='__main__': unittest.main()
