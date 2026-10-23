"""Streaming private TLS fault proxy for explicit artifact acceptance only."""
import argparse
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import ssl
import threading
import time

PARSER = argparse.ArgumentParser(description=__doc__)
PARSER.add_argument('root', type=Path)
PARSER.add_argument('--upstream-port', type=int, default=18448)
PARSER.add_argument('--listen-port', type=int, default=18449)
ARGS = PARSER.parse_args()
ROOT = ARGS.root
CONTEXT = ssl.create_default_context(cafile=str(ROOT.parent / 'server.crt'))
LOCK = threading.Lock()
FIRED = set()


def record(**value):
    with LOCK:
        with (ROOT / 'proxy-events.jsonl').open('a') as stream:
            stream.write(json.dumps(dict(time=time.time(), **value)) + '\n')


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.forward()

    def do_POST(self):
        self.forward()

    def do_PUT(self):
        self.forward()

    def forward(self):
        started = time.monotonic()
        self.connection.settimeout(10)
        mode = (ROOT / 'fault').read_text().strip()
        is_input = self.path.startswith('/v2/inputs/')
        is_result = self.path.startswith('/v2/results/')
        is_file = self.path.rsplit('/', 1)[-1].isdigit()
        connection = http.client.HTTPSConnection('127.0.0.1', ARGS.upstream_port,
                                                  context=CONTEXT, timeout=10)
        headers = {k: v for k, v in self.headers.items()
                   if k.lower() not in {'host', 'connection'}}
        drop_heartbeat = False
        try:
            if (mode == 'hold-input' and is_input) or (mode == 'hold-result' and is_result):
                self.send_error(503, 'Private acceptance transfer hold')
                return
            length = int(self.headers.get('Content-Length', 0))
            fault = None
            with LOCK:
                if (mode == 'heartbeat' and self.path == '/v1/poll'
                        and 'streaming_input' in FIRED and 'heartbeat' not in FIRED):
                    FIRED.add('heartbeat')
                    drop_heartbeat = True
                if mode not in FIRED and is_file:
                    if mode == 'input' and is_input:
                        fault = 'partial_input'
                    if mode == 'result' and is_result and length >= 64 * 1024 * 1024:
                        fault = 'partial_result'
                    if fault:
                        FIRED.add(mode)
            if fault == 'partial_input':
                # Select the large file by response size, without buffering it.
                connection.request(self.command, self.path, headers=headers)
                response = connection.getresponse()
                total = int(response.getheader('Content-Length', '0'))
                if total < 64 * 1024 * 1024:
                    with LOCK:
                        FIRED.discard(mode)
                    fault = None
                else:
                    self.send_response(response.status)
                    self.send_header('Content-Length', str(total))
                    self.end_headers()
                    chunk = response.read(65536)
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    (ROOT / 'fault').write_text('hold-input')
                    record(event=fault, expected_bytes=total, sent_bytes=len(chunk), path=self.path)
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection = True
                    return
            else:
                connection.putrequest(self.command, self.path)
                for key, value in headers.items():
                    connection.putheader(key, value)
                connection.endheaders()
                remaining = min(length, 65536) if fault else length
                while remaining:
                    chunk = self.rfile.read(min(65536, remaining))
                    if not chunk:
                        raise OSError('Incomplete incoming request')
                    connection.send(chunk)
                    remaining -= len(chunk)
                if fault:
                    connection.sock.shutdown(socket.SHUT_RDWR)
                    (ROOT / 'fault').write_text('hold-result')
                    record(event=fault, expected_bytes=length, sent_bytes=min(length, 65536), path=self.path)
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection = True
                    return
                response = connection.getresponse()
            if drop_heartbeat:
                record(event='lost_input_heartbeat', path=self.path, status=response.status,
                       delayed_seconds=2)
                time.sleep(2)
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
                return
            if mode == 'heartbeat' and is_input and is_file:
                record(event='input_request', path=self.path)
                if int(response.getheader('Content-Length', '0')) >= 65536:
                    with LOCK:
                        FIRED.add('streaming_input')
            self.send_response(response.status)
            self.send_header('Content-Type', response.getheader('Content-Type', 'application/octet-stream'))
            self.send_header('Content-Length', response.getheader('Content-Length', '0'))
            self.end_headers()
            while chunk := response.read(65536):
                self.wfile.write(chunk)
                if mode == 'heartbeat' and is_input and is_file:
                    self.wfile.flush()
                    time.sleep(.02)
            if self.path == '/v1/poll':
                record(event='poll', elapsed=time.monotonic() - started, status=response.status)
        except (OSError, http.client.HTTPException):
            record(event='transport_error', path=self.path)
            self.close_connection = True
        finally:
            connection.close()


if __name__ == '__main__':
    server = ThreadingHTTPServer(('127.0.0.1', ARGS.listen_port), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(ROOT.parent / 'server.crt'), str(ROOT.parent / 'server.key'))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    listener = ROOT / 'proxy-listener.json'
    temporary_listener = ROOT / 'proxy-listener.tmp'
    temporary_listener.write_text(json.dumps(dict(port=server.server_port)))
    temporary_listener.replace(listener)
    try:
        server.serve_forever(poll_interval=.05)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
