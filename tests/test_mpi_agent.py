"""Durable node launch fences and real local fake-process termination."""

from dataclasses import asdict
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

from lammps_manager.config import Execution, WorkerCapacity, WorkerConfig
from lammps_manager.models import InputFile, JobSpecification, MpiParticipant, Resources
from lammps_manager.mpi_agent import Node


class MpiAgentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root/'config').mkdir()
        (self.root/'worker').mkdir()
        token = self.root/'config/token'; token.write_text('a'*40); token.chmod(0o600)
        self.program = self.root/'program'
        self.program.write_text('#!/bin/sh\nexit 0\n'); self.program.chmod(0o700)
        self.config = WorkerConfig(WorkerCapacity('a',2,1,1),'http://127.0.0.1:1',
            str(self.root/'worker'), Execution(str(self.program),sys.executable),str(token),
            storage_mode='staged')
        config = asdict(self.config); config['execution']['environment']={}
        (self.root/'config/worker.yaml').write_text(json.dumps(config))
        self.node = Node(self.root, self.config)
        content = b'input'
        self.spec = JobSpecification('job','/manager/job','in',hashlib.sha256(content).hexdigest(),
            Resources(mpi_ranks=2,memory_gib=.2,time_limit_seconds=2),storage_mode='staged',
            input_manifest=(InputFile('in',len(content),hashlib.sha256(content).hexdigest()),),
            mpi_placement=(MpiParticipant('a',1,.1),MpiParticipant('b',1,.1)))
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.generation = {'boot_id':self.boot,'session_id':str(uuid4())}
        self.request = dict(attempt_id='attempt',specification=asdict(self.spec),worker_id='a',
            generation=self.generation,executable_sha256=hashlib.sha256(self.program.read_bytes()).hexdigest(),
            cpus=sorted(os.sched_getaffinity(0))[:1],role='leader')
        self.content=content
        self.supervisors=[]
        self.addCleanup(self.cleanup_processes)

    def cleanup_processes(self):
        try: self.node.status('attempt',cancel=True)
        except (ValueError,OSError): pass
        for pid in self.supervisors:
            try: os.waitpid(pid,0)
            except ChildProcessError: pass

    def prepare(self):
        with patch.object(self.node,'health',return_value={'generation':self.generation,'ready':True,'online':True}), \
             patch.object(self.node,'download',side_effect=lambda attempt,index:io.BytesIO(self.content)):
            return self.node.prepare(self.request)

    def start(self, text):
        with patch.object(self.node,'health',return_value={'generation':self.generation,'ready':True,'online':True}):
            result=self.node.start('attempt',[sys.executable,'-c',text])
        if result.get('supervisor'): self.supervisors.append(result['supervisor']['pid'])
        return result

    def wait(self, timeout=8):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            result=self.node.status('attempt')
            if result.get('terminated'): return result
            time.sleep(.02)
        self.fail('Original fake process did not reach confirmed termination')

    def test_verified_preparation_idempotence_and_identity_validation(self):
        first=self.prepare()
        self.assertEqual(self.prepare(),first)
        self.assertEqual((Path(first['directory'])/'in').read_bytes(),self.content)
        with self.assertRaises(ValueError):
            self.node.prepare({**self.request,'generation':{**self.generation,'boot_id':str(uuid4())}})
        with self.assertRaises(ValueError):
            self.node.prepare({**self.request,'executable_sha256':'b'*64})

    def test_exactly_one_start_and_preserved_output(self):
        self.prepare()
        code="from pathlib import Path; import time; Path('invocations').write_text('once'); print('retained',flush=True); time.sleep(.15)"
        first=self.start(code)
        self.assertEqual(self.node.start('attempt',[sys.executable,'-c',code])['supervisor'],first['supervisor'])
        result=self.wait()
        self.assertEqual(result['status'],'succeeded')
        self.assertEqual(self.node.status('attempt',cancel=True),result)
        self.assertTrue(result['evidence']['launch_fenced'])
        self.assertEqual((Path(result['directory']).parent/'stdout').read_text().strip(),'retained')
        self.assertEqual(self.node.start('attempt',[sys.executable,'-c',code])['supervisor'],first['supervisor'])
        with self.assertRaises(ValueError): self.node.start('attempt',[sys.executable,'-c','different'])

    def test_prelaunch_closure_prevents_late_launch(self):
        self.prepare()
        result=self.node.status('attempt',cancel=True)
        self.assertTrue(result['terminated'])
        self.assertEqual(result['evidence']['kind'],'never_started')
        with self.assertRaises(ValueError): self.start('print(1)')

    def test_timeout_kills_complete_local_session(self):
        self.prepare()
        self.start("import subprocess,time,sys; subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); time.sleep(60)")
        result=self.wait()
        self.assertEqual(result['status'],'cancelled')
        self.assertEqual(result['session_members'],[])
        self.assertIn('deadline',result['reason'])

    def orphan_rank(self):
        self.prepare()
        child = ("import os,time,json; from pathlib import Path; "
                 "from lammps_manager.recovery import process_identity; "
                 "Path('.mpi-rank-000000.json').write_text(json.dumps({'process':process_identity(os.getpid())})); "
                 "time.sleep(60)")
        self.start("import subprocess,sys,time; from pathlib import Path; "
                   f"subprocess.Popen([sys.executable,'-c',{child!r}]); "
                   "\nwhile not Path('.mpi-rank-000000.json').exists(): time.sleep(.01)")
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            with self.node.locked('attempt') as state:
                identity=state.get('process')
                directory=Path(state['directory'])
            if identity and not Path(f"/proc/{identity['pid']}").exists() and (directory/'.mpi-rank-000000.json').exists():
                rank=json.loads((directory/'.mpi-rank-000000.json').read_text())['process']
                from lammps_manager.recovery import terminate_group
                self.addCleanup(terminate_group,rank['session'])
                return rank
            time.sleep(.01)
        self.fail('Fake launcher did not exit leaving its rank')

    def test_cancel_terminates_rank_after_launcher_exit(self):
        self.orphan_rank()
        self.node.status('attempt',cancel=True)
        result=self.wait()
        self.assertEqual(result['status'],'cancelled')
        self.assertEqual(result['session_members'],[])

    def test_deadline_terminates_rank_after_launcher_exit(self):
        self.orphan_rank()
        result=self.wait()
        self.assertEqual(result['status'],'cancelled')
        self.assertIn('deadline',result['reason'])

    def test_orphan_without_matching_rank_identity_stays_unresolved(self):
        self.prepare()
        leader=dict(pid=999999,session=999999,process_group=999999,
                    boot_id=self.boot,start_ticks='1')
        rank={**leader,'pid':999998}
        with self.node.locked('attempt') as state:
            state.update(phase='exited',exit_code=1,process=leader)
            proof=Path(state['directory'])/'.mpi-rank-000000.json'
        for saved in (None,rank):
            with self.subTest(proof=saved):
                if saved:proof.write_text(json.dumps({'process':saved}))
                def identity(pid):
                    if pid==leader['pid']:raise FileNotFoundError
                    return {**rank,'start_ticks':'2'}
                with patch('lammps_manager.mpi_agent.process_identity',side_effect=identity), \
                     patch('lammps_manager.mpi_agent.session_members',return_value=((999998,999999),)), \
                     patch('lammps_manager.mpi_agent.terminate_group') as kill:
                    result=self.node.status('attempt',cancel=True)
                    self.assertFalse(result['terminated'])
                    kill.assert_not_called()

    def test_claim_without_identity_is_unknown_and_never_restarted(self):
        self.prepare()
        with self.node.locked('attempt') as state:
            state.update(phase='claimed',argv=[sys.executable,'-c','print(1)'])
        result=self.node.status('attempt',close=True)
        self.assertFalse(result['terminated'])
        self.assertEqual(result['phase'],'unknown')
        replay=self.node.start('attempt',[sys.executable,'-c','print(1)'])
        self.assertNotIn('supervisor',replay)

    def test_interrupted_staging_resumes_same_owned_snapshot(self):
        with patch.object(self.node,'health',return_value={'generation':self.generation,'ready':True,'online':True}), \
             patch.object(self.node,'download',side_effect=OSError('interrupted transfer')):
            with self.assertRaises(OSError):self.node.prepare(self.request)
        result=self.prepare()
        self.assertEqual(result['phase'],'prepared')
        self.assertEqual((Path(result['directory'])/'in').read_bytes(),self.content)

    def test_cancel_during_staging_closes_irrevocable_fence(self):
        def incoming(attempt,index):
            self.node.fence(attempt,cancel=True)
            return io.BytesIO(self.content)
        with patch.object(self.node,'health',return_value={'generation':self.generation,'ready':True,'online':True}), \
             patch.object(self.node,'download',side_effect=incoming):
            with self.assertRaisesRegex(ValueError,'fence closed'):self.node.prepare(self.request)
        result=self.node.status('attempt',cancel=True)
        self.assertTrue(result['terminated'])
        self.assertEqual(result['evidence']['kind'],'never_started')
        with self.assertRaises(ValueError):self.prepare()
        with self.assertRaises(ValueError):self.start('print(1)')

    def test_close_fences_before_waiting_for_staging(self):
        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                attempt = 'cancel-staging' if cancel else 'close-staging'
                request = {**self.request, 'attempt_id': attempt}
                incoming = threading.Event()
                release = threading.Event()
                fenced = threading.Event()
                stage_errors = []
                close_errors = []
                replies = []
                original_fence = self.node.fence

                def download(*args):
                    incoming.set()
                    if not release.wait(5):
                        raise TimeoutError('Test download was not released')
                    return io.BytesIO(self.content)

                def fence(*args, **kwargs):
                    original_fence(*args, **kwargs)
                    fenced.set()

                def stage():
                    try:
                        self.node.prepare(request)
                    except Exception as error:
                        stage_errors.append(error)

                def close():
                    try:
                        replies.append(self.node.close(request, cancel=cancel))
                    except Exception as error:
                        close_errors.append(error)

                with patch.object(self.node, 'health', return_value={
                        'generation': self.generation, 'ready': True, 'online': True}), \
                     patch.object(self.node, 'download', side_effect=download), \
                     patch.object(self.node, 'fence', side_effect=fence):
                    staging = threading.Thread(target=stage)
                    closing = threading.Thread(target=close)
                    staging.start()
                    try:
                        self.assertTrue(incoming.wait(3))
                        closing.start()
                        self.assertTrue(fenced.wait(1), 'Closure waited for the staging lock before fencing')
                        self.assertTrue(staging.is_alive())
                        self.assertTrue((self.node.control(attempt)/'closed').exists())
                        self.assertEqual((self.node.control(attempt)/'cancel').exists(), cancel)
                    finally:
                        release.set()
                        staging.join(5)
                        if closing.ident is not None:
                            closing.join(5)
                    self.assertFalse(staging.is_alive())
                    self.assertFalse(closing.is_alive())
                self.assertEqual(close_errors, [])
                self.assertEqual(len(stage_errors), 1)
                self.assertRegex(str(stage_errors[0]), 'fence closed')
                self.assertTrue(replies[0]['terminated'])
                self.assertEqual(replies[0]['evidence']['kind'], 'never_started')
                self.assertEqual(replies[0]['status'], 'cancelled' if cancel else 'failed')
                with self.assertRaisesRegex(ValueError, 'fence'):
                    self.node.prepare(request)
                with self.assertRaises(ValueError):
                    self.node.start(attempt, [sys.executable, '-c', 'print(1)'])

    def test_conflicting_close_does_not_fence_prepared_allocation(self):
        self.prepare()
        conflicting = {**self.request, 'generation': {**self.generation, 'session_id': str(uuid4())}}
        with self.assertRaisesRegex(ValueError, 'Conflicting MPI closure'):
            self.node.close(conflicting, cancel=True)
        self.assertFalse((self.node.control('attempt')/'closed').exists())
        self.assertFalse((self.node.control('attempt')/'cancel').exists())
        self.assertEqual(self.node.status('attempt')['phase'], 'prepared')

    def test_restricted_peer_gate_rejects_general_commands_and_leader_role(self):
        self.prepare()
        for command in ('touch /tmp/unauthorized', 'remote-mpi attempt []',
                        'remote-mpi ../escape []'):
            with self.subTest(command=command),self.assertRaises(ValueError):self.node.peer_gate(command)

    def test_unprepared_closure_blocks_all_late_preparation(self):
        result=self.node.close(self.request,cancel=True)
        self.assertTrue(result['terminated'])
        self.assertEqual(result['evidence']['kind'],'never_started')
        with self.assertRaises(ValueError):self.prepare()

    def test_launch_rechecks_generation_and_does_not_claim_on_change(self):
        self.prepare()
        with patch.object(self.node,'health',return_value={'generation':{**self.generation,'session_id':str(uuid4())},'ready':True,'online':True}):
            with self.assertRaises(ValueError):self.node.start('attempt',[sys.executable,'-c','print(1)'])
        self.assertEqual(self.node.status('attempt')['phase'],'prepared')


if __name__=='__main__': unittest.main()
