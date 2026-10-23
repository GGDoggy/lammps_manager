"""Read unattended status files without starting tests or analyzing science."""
import argparse
import json
import os
from pathlib import Path
import subprocess


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('launch', type=Path)
    args = parser.parse_args()
    launch = json.loads(args.launch.read_text())
    for host, record in launch.items():
        code = '''import json,subprocess
from pathlib import Path
r=Path(''' + repr(record['root']) + ''')
s=r/'status.json';p=r/'progress.json'
out=dict(status=json.loads(s.read_text()) if s.exists() else None,
         progress=json.loads(p.read_text()) if p.exists() else None)
for name in ['pilot/report.json','confirmation/pool.json']:
 f=r/name
 if f.exists():
  v=json.loads(f.read_text())
  out[name]=dict(replicas=len(v.get('replicas',[])),completed_chunks=len(v.get('completed',[])),error=v.get('error'))
u=''' + repr(record['unit']) + '''
p=subprocess.run(['systemctl','--user','show',u,'--property=ActiveState','--value'],capture_output=True,text=True)
out['active_state']=p.stdout.strip()
print(json.dumps(out))
'''
        try:
            if record.get('ssh_argv'):
                environment = {**os.environ, 'SSH_ASKPASS': record['ssh_askpass'], 'SSH_ASKPASS_REQUIRE': 'force'}
                result = subprocess.run(record['ssh_argv'] + ['python3 -'], input=code, text=True,
                                        capture_output=True, timeout=25, check=True, env=environment)
            else:
                result = subprocess.run(['python3', '-'], input=code, text=True, capture_output=True,
                                        timeout=25, check=True)
            value = json.loads(result.stdout); status = value['status']; progress = value['progress']
            if status:
                state = 'finished' if status['completed'] and status['stop_verified'] else 'stopped: review required'
                detail = 'stop_verified=' + str(status['stop_verified'])
            elif progress and value['active_state'] == 'active':
                state = 'running'
                detail = 'step ' + str(len(progress['steps']))
            else:
                state = 'unknown'; detail = 'no final marker and no active service'
            measurements = value.get('confirmation/pool.json') or value.get('pilot/report.json')
            print(f'{host}: {state}; {detail}' + (f'; {measurements}' if measurements else ''))
        except (OSError, ValueError, subprocess.SubprocessError, KeyError) as exc:
            print(f'{host}: unknown; {type(exc).__name__}: {exc}')


if __name__ == '__main__':
    main()
