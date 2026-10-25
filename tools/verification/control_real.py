"""Explicit isolated three-host control soak. Requires user-authorized SSH config.

Uses new /tmp roots, transient units and tokens. Never changes production services.
Keeps all output. A 31-slot run cannot establish the requested 39-slot acceptance.
"""
import argparse
from dataclasses import asdict
import io
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import tarfile
import time
from uuid import uuid4
import yaml

from lammps_manager.config import load_config
from lammps_manager.continuation import Recipe
from lammps_manager.segments import submit_run, cancel_run
from lammps_manager.store import Store, is_busy


WORKER_BOOTSTRAP = r'''import json,logging,runpy,sys,time,urllib.request
from http.client import IncompleteRead
from urllib.error import HTTPError,URLError
from pathlib import Path
root=Path(__file__).parent
logging.basicConfig(filename=root/'transactions.log',level=logging.DEBUG)
original=urllib.request.OpenerDirector.open
def record(started,**fields):
 with (root/'control-latency.jsonl').open('a') as f:
  f.write(json.dumps(dict(seconds=time.monotonic()-started,**fields))+'\n')
class Response:
 def __init__(self,r,t): self.r,self.t,self.header=r,t,time.monotonic()-t
 def __getattr__(self,n): return getattr(self.r,n)
 def __enter__(self): return self
 def __exit__(self,kind,error,traceback):
  try:
   record(self.t,header_seconds=self.header,status=self.r.status,
          outcome='transport_error' if error is not None else 'success',
          **({'error':type(error).__name__} if error is not None else {}))
  finally:
   self.r.close()
def timed(self,request,*args,**kwargs):
 if not getattr(request,'full_url','').endswith('/v1/poll'):
  return original(self,request,*args,**kwargs)
 t=time.monotonic()
 try:
  r=original(self,request,*args,**kwargs)
 except (HTTPError,URLError,OSError,IncompleteRead) as error:
  record(t,outcome='http_error' if isinstance(error,HTTPError) else 'transport_error',
         error=type(error).__name__,**({'status':error.code} if isinstance(error,HTTPError) else {}))
  raise
 return Response(r,t)
urllib.request.OpenerDirector.open=timed
runpy.run_module('lammps_manager',run_name='__main__')
'''


def pending_delivery(store: Store) -> dict[str, int]:
    """Termination alone cannot establish durable output/checkpoint delivery."""
    return dict(
        reservations=len(store.reservations()),
        results=store._db.execute("SELECT count(*) FROM attempts a LEFT JOIN result_collections c USING(attempt_id) "
            "WHERE a.state IN ('succeeded','failed','cancelled') AND coalesce(c.status,'')!='collected'").fetchone()[0],
        checkpoints=store._db.execute("SELECT count(*) FROM checkpoints c JOIN attempts a USING(attempt_id) "
            "WHERE a.state='succeeded' AND c.acknowledged=0").fetchone()[0])


def manager_service_audit(unit: str) -> dict:
    """Capture pre-stop manager health; missing evidence cannot pass acceptance."""
    try:
        result = subprocess.run(
            ['systemctl', '--user', 'show', unit, '-p', 'NRestarts', '-p', 'ActiveState'],
            text=True, capture_output=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return dict(healthy=False, error=type(exc).__name__)
    audit = dict(healthy=False, returncode=result.returncode, properties=result.stdout)
    if result.returncode:
        audit['error'] = 'Manager state query failed'
        return audit
    fields = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition('=')
        if not separator or key in fields:
            audit['error'] = 'Malformed manager state properties'
            return audit
        fields[key] = value
    restarts = fields.get('NRestarts', '')
    if not restarts.isascii() or not restarts.isdecimal() or not fields.get('ActiveState'):
        audit['error'] = 'Missing or malformed manager restart/state evidence'
        return audit
    audit.update(restarts=int(restarts), active_state=fields['ActiveState'])
    audit['healthy'] = audit['restarts'] == 0 and audit['active_state'] == 'active'
    return audit


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--remote-config',type=Path,required=True)
    parser.add_argument('--known-hosts',type=Path,required=True)
    parser.add_argument('--seconds',type=float,default=3600)
    args=parser.parse_args()
    remotes=yaml.safe_load(args.remote_config.read_text())
    if not isinstance(remotes,list) or len(remotes)!=2:
        raise ValueError('Expected exactly two explicitly configured remote accounts')
    tag=uuid4().hex[:10]; root=Path('/tmp/lammps-control-real-'+tag)
    root.mkdir(mode=0o700); print(str(root),flush=True)
    askpass=root/'askpass'
    askpass.write_text('#!/usr/bin/python3\nimport os,yaml\nfrom pathlib import Path\nprint(yaml.safe_load(Path('+repr(str(args.remote_config.resolve()))+').read_text())[int(os.environ["CONTROL_REMOTE_INDEX"])]["password"])\n')
    askpass.chmod(0o700)
    hosts={'poincare':None}
    def run(index, code, *, binary=None):
        if index is None:
            argv=['/usr/bin/python3','-'] if binary is None else ['tar','-xf','-','-C',str(root)]
            env=os.environ.copy()
        else:
            cfg=remotes[index]
            argv=['ssh','-o','ConnectTimeout=10','-o','NumberOfPasswordPrompts=1','-o','StrictHostKeyChecking=yes',
                  '-o','UserKnownHostsFile='+str(args.known_hosts.resolve()),'-p',str(cfg['port']),cfg['username']+'@'+cfg['host'],
                  'python3 -' if binary is None else 'tar -xf - -C '+shlex.quote(str(root))]
            env={**os.environ,'SSH_ASKPASS':str(askpass),'SSH_ASKPASS_REQUIRE':'force','DISPLAY':':0','CONTROL_REMOTE_INDEX':str(index)}
        result=subprocess.run(argv,input=code if binary is None else binary,text=binary is None,capture_output=True,timeout=90,env=env)
        if result.returncode:
            raise RuntimeError('Isolated command failed: '+str(result.stderr)[-1000:])
        return json.loads(result.stdout) if binary is None and result.stdout.strip() else None
    facts={}
    for index in range(2):
        value=run(index,"import json,socket,os;print(json.dumps(dict(host=socket.gethostname(),cpus=len(os.sched_getaffinity(0)))))")
        hosts[value['host']]=index; facts[value['host']]=value
    if set(hosts)!={'poincare','allegro','mozzarella'}:
        raise ValueError('Unexpected remote host identities')
    facts['poincare']=dict(host='poincare',cpus=len(os.sched_getaffinity(0)))
    production='/home/ted/lammps_manager/manager/state/manager.sqlite3'
    with sqlite3.connect('file:'+production+'?mode=ro',uri=True) as db:
        reserved=dict(db.execute('SELECT worker_id,sum(cpu_slots) FROM reservations GROUP BY worker_id').fetchall())
    slots={host:min(14,max(0,facts[host]['cpus']-reserved.get(host,0)-2)) for host in hosts}
    while sum(slots.values()) > 39:
        host = max(slots, key=slots.get)
        slots[host] -= 1
    if any(value<1 for value in slots.values()):
        raise RuntimeError('Insufficient unreserved CPU slots on a required host')
    manager=dict(manager=dict(database_path=str(root/'manager.sqlite'),schedule_interval_seconds=.2,heartbeat_timeout_seconds=30),
                 storage=dict(mode='staged',work_root=str(root/'manager-work')),
                 workers=[dict(id=h,cpu_slots=n,memory_budget_gib=1,max_concurrent_jobs=n) for h,n in slots.items()],
                 job_defaults=dict(mpi_ranks=1,omp_threads_per_rank=1,memory_gib=.01,time_limit_seconds=300,max_retries=0),
                 scheduler=dict(policy='fifo_least_loaded',account_external_load=False,reserve_memory_gib=2,reserve_slots=0,min_slots=0),
                 segmentation=dict(step_max=100000,environment_reset_seconds=3600))
    (root/'manager-work').mkdir(); (root/'manager.yaml').write_text(yaml.safe_dump(manager))
    from secrets import token_hex
    tokens={h:token_hex(32) for h in hosts}
    credential_paths={h:str(root/(h+'.token')) for h in hosts}
    for h,t in tokens.items():
        path=Path(credential_paths[h]);path.write_text(t);path.chmod(0o600)
    (root/'credentials.json').write_text(json.dumps(credential_paths));(root/'credentials.json').chmod(0o600)
    import hashlib
    source = {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path('src').rglob('*.py'))}
    (root/'source-fingerprints.json').write_text(json.dumps(source,indent=2))
    archive=io.BytesIO()
    with tarfile.open(fileobj=archive,mode='w') as tar:
        for path in sorted(Path('src').rglob('*.py')): tar.add(path,arcname=str(path))
    # Explicit CA file; no TLS relaxation or credential forwarding.
    ca=Path('/home/ted/lammps_manager/manager/config/deployment-ca.crt').read_text()
    units={h:'lammps-control-'+tag+'-worker' for h in hosts}; manager_unit='lammps-control-'+tag+'-manager'
    port=18443
    with __import__('socket').socket() as probe: probe.bind(('0.0.0.0',port))
    for host,index in hosts.items():
        if index is not None: run(index,'from pathlib import Path;Path('+repr(str(root))+').mkdir(mode=0o700);print("null")')
        run(index,'',binary=archive.getvalue())
        payload=dict(root=str(root),host=host,slots=slots[host],token=tokens[host],ca=ca,bootstrap=WORKER_BOOTSTRAP)
        code='''import json,yaml
from pathlib import Path
v=json.loads('''+repr(json.dumps(payload))+''')
r=Path(v['root']);(r/'bootstrap.py').write_text(v['bootstrap']);(r/'ca.crt').write_text(v['ca'])
p=r/'worker.token';p.write_text(v['token']);p.chmod(0o600)
c=yaml.safe_load(Path('/home/ted/lammps_manager/worker/config/worker.yaml').read_text())
c['worker'].update(id=v['host'],cpu_slots=v['slots'],max_concurrent_jobs=v['slots'],memory_budget_gib=1,enabled=True)
c.update(manager_endpoint='https://192.168.50.112:18443',work_root=str(r/'worker-work'),token_file=str(p),heartbeat_interval_seconds=1,storage_mode='staged',cleanup_staged_files=False)
(r/'worker-work').mkdir();(r/'worker.yaml').write_text(yaml.safe_dump(c));print('null')
'''
        run(index,code)
    python='/home/ted/lammps_manager/.venv/bin/python3'
    started_units=[]; began=None; cancel_sent=None; report={}; failure=None
    def start(index,unit,arguments):
        command=['systemd-run','--user','--unit='+unit,'--property=RuntimeMaxSec=4500','--property=KillMode=control-group',
                 '--property=Restart=on-failure','--setenv=PYTHONPATH='+str(root/'src'),'--setenv=SSL_CERT_FILE='+str(root/'ca.crt'),
                 python,str(root/'bootstrap.py'),*arguments]
        run(index,'import subprocess,json;subprocess.run('+repr(command)+',check=True,capture_output=True);print("null")')
        started_units.append((index,unit))
    try:
        start(None,manager_unit,['serve','--config',str(root/'manager.yaml'),'--credentials',str(root/'credentials.json'),
              '--host','0.0.0.0','--port',str(port),'--tls-cert','/home/ted/lammps_manager/manager/config/server.crt',
              '--tls-key','/home/ted/lammps_manager/manager/config/server.key'])
        for host,index in hosts.items(): start(index,units[host],['worker','--config',str(root/'worker.yaml'),'--state-directory',str(root/'journal')])
        config=load_config(root/'manager.yaml','manager')
        deadline=time.monotonic()+30
        while time.monotonic()<deadline:
            with Store(root/'manager.sqlite') as store:
                if len(store.worker_records())==3: break
            time.sleep(.5)
        else: raise RuntimeError('Workers did not register')
        with Store(root/'manager.sqlite') as store:
            for i in range(58):
                submit_run(store,config,total_steps=100000000,step_max=100000,run_id=f'control-real-{i:02}',
                           recipe=Recipe('wca-abp-2d-v1','statistical'),model_parameters=dict(particles=64,density=.1),
                           sampling=dict(unit='step',positions=[100000000]))
        began=time.monotonic(); peak=0
        while time.monotonic()-began<args.seconds:
            with Store(root/'manager.sqlite') as store:
                reservations=store.reservations();peak=max(peak,len(reservations))
                if cancel_sent is None and any(store.attempt(r.attempt_id).state=='running' for r in reservations):
                    row=store._db.execute("SELECT s.run_id FROM run_segments s JOIN attempts a USING(job_id) WHERE a.state='running' ORDER BY a.started_at LIMIT 1").fetchone()
                    cancel_run(store,row[0]);cancel_sent=dict(run_id=row[0],seconds=time.monotonic()-began)
                progress=dict(elapsed=time.monotonic()-began,peak=peak,current=len(reservations),
                              completed_segments=store._db.execute('SELECT count(*) FROM run_segments WHERE completed=1').fetchone()[0])
            (root/'progress.json').write_text(json.dumps(progress));print(json.dumps(progress),flush=True)
            time.sleep(10)
        # Cancel only this test's unfinished parents; termination gates remain active.
        with Store(root/'manager.sqlite') as store:
            for row in store._db.execute("SELECT run_id FROM simulation_runs WHERE status NOT IN ('succeeded','cancelled')").fetchall(): cancel_run(store,row[0])
        deadline=time.monotonic()+360
        while time.monotonic()<deadline:
            with Store(root/'manager.sqlite') as store:
                if not any(pending_delivery(store).values()):break
            time.sleep(1)
        report.update(elapsed_seconds=time.monotonic()-began,peak_reservations=peak,cancel_sent=cancel_sent)
    except BaseException as exc:
        failure=type(exc).__name__+': '+str(exc);report['error']=failure
    finally:
        # On an observer failure, request cancellation before stopping isolated units.
        if failure and (root/'manager.sqlite').exists():
            with Store(root/'manager.sqlite') as store:
                for row in store._db.execute("SELECT run_id FROM simulation_runs WHERE status NOT IN ('succeeded','cancelled')").fetchall():cancel_run(store,row[0])
            deadline=time.monotonic()+360
            while time.monotonic()<deadline:
                with Store(root/'manager.sqlite') as store:
                    if not any(pending_delivery(store).values()):break
                time.sleep(1)
        audits={}
        for host,index in hosts.items():
            code='''import json,sqlite3,subprocess
from pathlib import Path
r=Path('''+repr(str(root))+''')
lines=(r/'control-latency.jsonl').read_text().splitlines() if (r/'control-latency.jsonl').exists() else []
samples=[json.loads(line) for line in lines]
values=sorted(s['seconds'] for s in samples)
headers=sorted(s.get('header_seconds',s['seconds']) for s in samples)
unit='''+repr(units[host])+'''
s=subprocess.run(['systemctl','--user','show',unit,'-p','NRestarts','-p','ActiveState'],text=True,capture_output=True)
print(json.dumps(dict(count=len(values),p95=values[int(len(values)*.95)] if values else None,maximum=max(values) if values else None,header_p95=headers[int(len(headers)*.95)] if headers else None,service=s.stdout)))
'''
            try:audits[host]=run(index,code)
            except Exception as exc:audits[host]=dict(error=type(exc).__name__)
        report['manager_service_before_stop'] = manager_service_audit(manager_unit)
        cleanup_errors=[]
        for index,unit in reversed(started_units):
            try:
                run(index,'import subprocess;subprocess.run(["systemctl","--user","stop",'+repr(unit)+'],check=True);print("null")')
            except Exception as exc:
                cleanup_errors.append(dict(unit=unit,host_index=index,error=type(exc).__name__))
        report['cleanup_errors']=cleanup_errors
        report['latency_scope'] = 'all control requests including HTTP and transport failures'
        report.update(root=str(root),slots=slots,requested_peak=39,hosts=audits,production_reservations_at_start=reserved)
        with Store(root/'manager.sqlite') as store:
            report['remaining_reservations']=len(store.reservations())
            report['pending_delivery']=pending_delivery(store)
            report['completed_segments']=store._db.execute('SELECT count(*) FROM run_segments WHERE completed=1').fetchone()[0]
            report['heartbeat_expiries']=store._db.execute("SELECT count(*) FROM events WHERE reason='Worker heartbeat expired'").fetchone()[0]
        import hashlib
        integrity = dict(files=0, errors=[])
        with Store(root/'manager.sqlite') as store:
            for row in store._db.execute("SELECT c.attempt_id,c.manifest,a.directory FROM result_collections c JOIN attempts a USING(attempt_id) WHERE c.status='collected'"):
                for item in json.loads(row['manifest'])['files']:
                    path = Path(row['directory'])/'results'/item['path']
                    if (not path.is_file() or path.stat().st_size != item['size']
                            or hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']):
                        integrity['errors'].append(str(path))
                    integrity['files'] += 1
            for row in store._db.execute('SELECT directory,manifest FROM checkpoints WHERE acknowledged=1'):
                for item in json.loads(row['manifest'])['files']:
                    path = Path(row['directory'])/item['path']
                    if (not path.is_file() or path.stat().st_size != item['size']
                            or hashlib.sha256(path.read_bytes()).hexdigest() != item['sha256']):
                        integrity['errors'].append(str(path))
                    integrity['files'] += 1
        report['integrity'] = integrity
        import re
        durations=[float(v) for v in re.findall(r'Control poll worker=\S+ elapsed=([0-9.]+)',(root/'transactions.log').read_text())]
        durations.sort()
        report['manager_processing'] = dict(count=len(durations),p95=durations[int(len(durations)*.95)] if durations else None,maximum=max(durations) if durations else None)
        report['bounded_acceptance'] = bool(not failure and report['remaining_reservations'] == 0
            and report.get('manager_service_before_stop', {}).get('healthy') is True
            and report.get('elapsed_seconds', 0) >= args.seconds and report['completed_segments'] >= 2
            and not cleanup_errors and not integrity['errors'] and not any(report['pending_delivery'].values()) and report['heartbeat_expiries'] == 0
            and all(a.get('p95') is not None and a['p95'] <= 1 and a['maximum'] <= 5
                    and 'NRestarts=0' in a.get('service', '') for a in audits.values()))
        report['full_39_acceptance']=bool(report['bounded_acceptance'] and report.get('elapsed_seconds',0)>=3600 and report.get('peak_reservations',0)>=39)
        (root/'report.json').write_text(json.dumps(report,indent=2));print(json.dumps(report),flush=True)
    accepted = report['full_39_acceptance'] if args.seconds >= 3600 else report['bounded_acceptance']
    return 0 if accepted else 1

if __name__=='__main__':raise SystemExit(main())
