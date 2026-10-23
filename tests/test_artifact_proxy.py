"""Actual loopback TLS proxy regressions; no workers, MPI or external hosts."""
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import signal
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen


@unittest.skipUnless(shutil.which('openssl'), 'openssl is needed to create a temporary TLS certificate')
class ArtifactProxyTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name); self.case = self.root / 'case'; self.case.mkdir()
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(self.root / 'server.key'), '-out', str(self.root / 'server.crt'),
                        '-days', '1', '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1'],
                       check=True, capture_output=True, timeout=10)
        self.payload = b'x' * 131072; self.posts = 0; self.large = False
        owner = self
        class Upstream(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                data = owner.payload
                self.send_response(200)
                self.send_header('Content-Length', str(64 * 1024 ** 2 if owner.large else len(data)))
                self.end_headers(); self.wfile.write(data)
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length'])); owner.posts += 1
                data = b'{"ok": true}'
                self.send_response(200); self.send_header('Content-Length', str(len(data)))
                self.end_headers(); self.wfile.write(data)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(self.root / 'server.crt'), str(self.root / 'server.key'))
        self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}); thread.start()
        def stop_server():
            self.server.shutdown(); thread.join(timeout=5); self.server.server_close()
        self.addCleanup(stop_server)
        self.context = ssl.create_default_context(cafile=str(self.root / 'server.crt'))

    def start(self, mode: str) -> str:
        (self.case / 'fault').write_text(mode)
        log = (self.case / 'driver.log').open('wb')
        script = Path(__file__).resolve().parents[1] / 'tools/verification/artifact_proxy.py'
        process = subprocess.Popen([sys.executable, str(script), str(self.case), '--upstream-port',
                                    str(self.server.server_port), '--listen-port', '0'], stdout=log, stderr=log)
        def stop():
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
            process.wait(timeout=5); log.close()
        self.addCleanup(stop)
        listener = self.case / 'proxy-listener.json'
        for _ in range(100):
            if process.poll() is not None:
                self.fail('Proxy exited before readiness')
            if listener.exists():
                return f'https://127.0.0.1:{json.loads(listener.read_text())["port"]}'
            time.sleep(.01)
        self.fail('Proxy listener did not become ready')

    def test_intact_input_and_exactly_one_lost_control_response(self) -> None:
        endpoint = self.start('heartbeat')
        with urlopen(endpoint + '/v2/inputs/attempt/files/0', context=self.context, timeout=2) as response:
            self.assertEqual(response.read(), self.payload)
        request = Request(endpoint + '/v1/poll', data=b'{}')
        with self.assertRaises((OSError, http.client.HTTPException)):
            urlopen(request, context=self.context, timeout=2)
        self.assertEqual(self.posts, 1)
        with urlopen(request, context=self.context, timeout=2) as response:
            self.assertTrue(json.load(response)['ok'])
        self.assertEqual(self.posts, 2)
        events = [json.loads(line) for line in (self.case / 'proxy-events.jsonl').read_text().splitlines()]
        self.assertEqual(sum(e['event'] == 'input_request' for e in events), 1)
        self.assertEqual(sum(e['event'] == 'lost_input_heartbeat' for e in events), 1)

    def test_existing_partial_input_fault_still_truncates_and_holds(self) -> None:
        self.large = True; self.payload = b'x' * 65536
        endpoint = self.start('input')
        with urlopen(endpoint + '/v2/inputs/attempt/files/0', context=self.context, timeout=2) as response:
            with self.assertRaises(http.client.IncompleteRead) as caught:
                response.read()
        self.assertEqual(len(caught.exception.partial), 65536)
        self.assertEqual((self.case / 'fault').read_text(), 'hold-input')
        with self.assertRaises(HTTPError) as caught:
            urlopen(endpoint + '/v2/inputs/attempt/files/0', context=self.context, timeout=2)
        self.assertEqual(caught.exception.code, 503)


if __name__ == '__main__':
    unittest.main()
