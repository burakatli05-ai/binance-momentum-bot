"""Temporary authenticated ZIP endpoint behind Railway HTTPS; no request logging."""
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import re
import socket
import sys
import time

import runner_export_once as once

PORT = 8787


def status():
    # Only a narrow manifest summary; never open the source database here.
    from runner_snapshot_export import checked_path
    path = checked_path(once.ROOT / once.SNAPSHOT_ID / 'manifest.json')
    if path.stat().st_size > 4 * 1024**2:
        raise ValueError('manifest size')
    manifest = json.loads(path.read_text())
    digest = manifest.get('sha256', '')
    if (manifest.get('valid') is not True or manifest.get('snapshot_id') != once.SNAPSHOT_ID
            or not re.fullmatch('[0-9a-f]{64}', digest)):
        raise ValueError('manifest')
    size = checked_path(once.SOURCE).stat().st_size
    result = {'snapshot_id': once.SNAPSHOT_ID, 'source_sha256': digest,
              'source_bytes': size, 'resource_ready': False}
    try:
        memory = once.memory_available()
        disk = once.shutil.disk_usage(once.OUTPUT.parent).free
        state_disk = once.shutil.disk_usage(once.STATE.parent).free
        result.update(memory_available=memory, tmp_free=disk, state_free=state_disk,
                      resource_ready=(memory >= once.MEMORY_BYTES + 256 * 1024**2
                                      and disk >= max(1024**3, size * 3)
                                      and state_disk >= 16 * 1024**2))
    except Exception:
        pass
    return result


class Handler(BaseHTTPRequestHandler):
    server_version = 'RunnerDownload'
    sys_version = ''

    def setup(self):
        self.request.settimeout(5)
        super().setup()

    def log_message(self, *args):
        pass

    def reply(self, code, data=b'', content_type='application/json'):
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        try:
            auth = self.headers.get('Authorization', '')
            digest = hashlib.sha256(auth.removeprefix('Bearer ').encode()).hexdigest()
            if (not auth.startswith('Bearer ') or not hmac.compare_digest(digest, self.server.token_hash)
                    or time.time() >= self.server.expires):
                self.reply(404)
                return
            if self.path == '/health':
                self.reply(200, b'{"ready":true}')
            elif self.path == '/snapshot':
                self.reply(200, json.dumps(status()).encode())
            elif self.path == '/runner.zip':
                self.send_zip()
            else:
                self.reply(404)
        except (BrokenPipeError, ConnectionResetError, socket.timeout):
            pass
        except Exception:
            self.reply(503)

    def send_zip(self):
        from runner_snapshot_export import checked_path
        receipt_path = checked_path(once.STATE / (once.SNAPSHOT_ID + '.success.json'))
        if receipt_path.stat().st_size > 65536:
            raise ValueError('receipt size')
        receipt = json.loads(receipt_path.read_text())
        path = checked_path(once.OUTPUT)
        # Validate the same open descriptor that is subsequently streamed.
        with path.open('rb') as stream:
            size = os.fstat(stream.fileno()).st_size
            if size > once.MAX_FILE_BYTES or size != receipt['zip_bytes']:
                raise ValueError('size')
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            if not hmac.compare_digest(digest, receipt['zip_sha256']):
                raise ValueError('hash')
            stream.seek(0)
            self.send_response(200)
            self.send_header('Content-Type', 'application/zip')
            self.send_header('Content-Length', str(size))
            self.send_header('X-Checksum-SHA256', digest)
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Disposition', 'attachment; filename="runner-' + once.SNAPSHOT_ID + '.zip"')
            self.send_header('Connection', 'close')
            self.end_headers()
            while time.time() < self.server.expires:
                chunk = stream.read(65536)
                if not chunk:
                    return
                self.wfile.write(chunk)


def serve(token_hash, expires):
    if not re.fullmatch('[0-9a-f]{64}', token_hash) or not time.time() < expires <= time.time() + 7200:
        return 1
    once.limits(128 * 1024**2, 120)
    with HTTPServer(('0.0.0.0', PORT), Handler) as server:
        server.token_hash, server.expires = token_hash, expires
        server.timeout = 1
        while time.time() < expires:
            server.handle_request()
    return 0


if __name__ == '__main__':
    try:
        sys.exit(serve(sys.argv[1], int(sys.argv[2])))
    except Exception:
        # No request headers, tokens, file contents or exception text in logs.
        sys.exit(1)
