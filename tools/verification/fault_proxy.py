"""Test-owned HTTPS proxy; faults are limited to this private endpoint."""
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import ssl
import sys
import threading
import time

ROOT = Path(sys.argv[1])
BASE = ROOT.parent
LOCK = threading.Lock()
CONTEXT = ssl.create_default_context(cafile=str(BASE / 'server.crt'))
FIRED = set()


def record(value):
    with (ROOT / 'proxy-events.jsonl').open('a') as out:
        out.write(json.dumps(dict(time=time.time(), **value)) + '\n')


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.forward()

    def do_PUT(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def forward(self):
        self.connection.settimeout(10)
        mode = (ROOT / 'fault').read_text().strip()
        result = self.path.startswith('/v2/results/')
        with LOCK:
            triggered = mode in FIRED
            if mode == 'upload' and result and self.path.rsplit('/', 1)[-1].isdigit() and not triggered:
                FIRED.add(mode)
                fault = 'partial_upload'
            elif mode == 'commit' and self.path.endswith('/commit') and not triggered:
                FIRED.add(mode)
                fault = 'lost_commit'
            else:
                fault = None
        if mode == 'offline' or (mode in FIRED and result and fault is None):
            self.send_error(503, 'Isolated verification outage')
            return
        connection = http.client.HTTPSConnection('127.0.0.1', 18448, context=CONTEXT, timeout=10)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in {'host', 'connection'}}
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length) if length else None
        try:
            if fault == 'partial_upload':
                connection.putrequest(self.command, self.path)
                for key, value in headers.items():
                    connection.putheader(key, value)
                connection.endheaders()
                partial = body[:max(1, len(body) // 2)]
                connection.send(partial)
                connection.sock.shutdown(socket.SHUT_RDWR)
                connection.close()
                record(dict(event=fault, path=self.path, expected_bytes=length, sent_bytes=len(partial)))
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
                return
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            data = response.read()
            if fault == 'lost_commit':
                record(dict(event=fault, path=self.path, upstream_status=response.status,
                            upstream_reply=json.loads(data)))
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
                return
            self.send_response(response.status)
            self.send_header('Content-Type', response.getheader('Content-Type', 'application/octet-stream'))
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (OSError, http.client.HTTPException) as exc:
            record(dict(event='transport_error', error=type(exc).__name__, path=self.path))
            self.close_connection = True
        finally:
            connection.close()


server = ThreadingHTTPServer(('127.0.0.1', 18449), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(str(BASE / 'server.crt'), str(BASE / 'server.key'))
server.socket = context.wrap_socket(server.socket, server_side=True)
try:
    server.serve_forever(poll_interval=.05)
except KeyboardInterrupt:
    pass
finally:
    server.server_close()
