"""Opt-in, one-attempt snapshot export; the parent always execs startup.py."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import zipfile

SNAPSHOT_ID = '20260916T190728Z-6cf5b8ff'
ROOT = Path('/data/research_exports')
SOURCE = ROOT / SNAPSHOT_ID / 'signals.db'
STATE = Path('/data/runner-export-once')
OUTPUT = Path('/tmp/runner-' + SNAPSHOT_ID + '.zip')
SCRIPT = Path(__file__).resolve()
DELAY_SECONDS = 90
TIMEOUT_SECONDS = 600
CPU_SECONDS = 120
MEMORY_BYTES = 512 * 1024**2
MAX_FILE_BYTES = 256 * 1024**2
CLEAN_ENV = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C.UTF-8',
             'TMPDIR': '/tmp', 'PYTHONUNBUFFERED': '1', 'PYTHONDONTWRITEBYTECODE': '1'}


def event(status, **metadata):
    # Never include exception messages, env, source manifests or payload here.
    try:
        print(json.dumps({'runner_export_once': status, 'snapshot_id': SNAPSHOT_ID,
                          **metadata}, sort_keys=True), flush=True)
    except Exception:
        pass


def private_state():
    from runner_snapshot_export import checked_path
    STATE.mkdir(mode=0o700, exist_ok=True)
    checked_path(STATE)
    if STATE.stat().st_mode & 0o077:
        raise ValueError('state permissions')


def claim(expected_hash):
    private_state()
    marker = STATE / (SNAPSHOT_ID + '.attempt.json')
    # O_EXCL rejects even a dangling symlink; never remove/overwrite this marker.
    fd = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump({'snapshot_id': SNAPSHOT_ID, 'source_sha256': expected_hash}, stream)
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(STATE, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def memory_available():
    available = None
    for line in Path('/proc/meminfo').read_text().splitlines():
        if line.startswith('MemAvailable:'):
            available = int(line.split()[1]) * 1024
    if available is None:
        raise ValueError('memory unknown')
    # Account for container limits, including nested cgroup v2 parents.
    mount = Path('/sys/fs/cgroup')
    relative = next((line[3:] for line in Path('/proc/self/cgroup').read_text().splitlines()
                     if line.startswith('0::')), None)
    if relative is None:
        raise ValueError('cgroup v2 required')
    directory = mount / relative.lstrip('/')
    if '..' in directory.parts:
        raise ValueError('unknown cgroup layout')
    while True:
        limit_path = directory / 'memory.max'
        if not limit_path.exists():
            raise ValueError('memory limit unknown')
        limit = limit_path.read_text().strip()
        if limit != 'max':
            available = min(available, int(limit) - int((directory / 'memory.current').read_text()))
        if directory == mount:
            break
        directory = directory.parent
    return available


def preflight():
    from runner_snapshot_export import checked_path
    checked_path(SOURCE)
    if not SOURCE.is_file():
        raise ValueError('missing snapshot')
    # Conservative scratch budget; insufficient/unknown resources fail closed.
    if shutil.disk_usage(OUTPUT.parent).free < max(1024**3, SOURCE.stat().st_size * 3):
        raise OSError('disk budget')
    if shutil.disk_usage(STATE).free < 16 * 1024**2:
        raise OSError('state disk budget')
    if memory_available() < MEMORY_BYTES + 256 * 1024**2:
        raise MemoryError('memory budget')


def limits(memory, cpu):
    import resource
    os.umask(0o077)
    os.nice(15)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_AS, (memory, memory))
    resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_FILE_BYTES, MAX_FILE_BYTES))


def verify_zip(expected_hash):
    from runner_snapshot_export import FIELDS, sha256
    with zipfile.ZipFile(OUTPUT) as archive:
        if sorted(archive.namelist()) != ['README.txt', 'manifest.json', 'payload.json']:
            raise ValueError('ZIP entries')
        if sum(info.file_size for info in archive.infolist()) > MAX_FILE_BYTES:
            raise ValueError('ZIP size')
        manifest = json.loads(archive.read('manifest.json'))
        payload = archive.read('payload.json')
        digest = hashlib.sha256(payload).hexdigest()
        if (digest != manifest['payload_sha256'] or len(payload) != manifest['payload_bytes']
                or manifest['source_sha256'] != expected_hash
                or manifest['snapshot_id'] != SNAPSHOT_ID
                or manifest['source_hash_unchanged'] is not True):
            raise ValueError('ZIP verification')
        tables = json.loads(payload)['tables']
        if set(tables) != set(FIELDS) or set(manifest['tables']) != set(FIELDS):
            raise ValueError('table allowlist')
        for table, fields in FIELDS.items():
            if tables[table]['columns'] != fields or len(tables[table]['rows']) != manifest['tables'][table]['rows']:
                raise ValueError('table validation')
        if sha256(SOURCE) != expected_hash:
            raise ValueError('source changed')
    return {'source_sha256': expected_hash, 'payload_sha256': digest,
            'zip_sha256': sha256(OUTPUT), 'zip_bytes': OUTPUT.stat().st_size,
            'row_counts': {k: len(v['rows']) for k, v in tables.items()}}


def worker(expected_hash):
    try:
        limits(MEMORY_BYTES, CPU_SECONDS)
        from runner_snapshot_export import export_snapshot, source_files
        source, manifest = source_files(SNAPSHOT_ID, ROOT)
        if source != SOURCE or manifest['sha256'] != expected_hash:
            raise ValueError('pinned hash mismatch')
        export_snapshot(SNAPSHOT_ID, OUTPUT, root=ROOT)
        receipt = verify_zip(expected_hash)
        with (STATE / (SNAPSHOT_ID + '.success.json')).open('x') as stream:
            json.dump(receipt, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        event('SUCCESS', **receipt)
        return 0
    except BaseException:
        event('WORKER_FAILED')
        return 1


def supervise(expected_hash):
    child = None
    try:
        limits(MEMORY_BYTES, CPU_SECONDS + 30)
        claim(expected_hash)
        time.sleep(DELAY_SECONDS)
        preflight()
        child = subprocess.Popen([sys.executable, str(SCRIPT), '--worker', expected_hash],
                                 env=dict(CLEAN_ENV), stdin=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, close_fds=True,
                                 start_new_session=True, shell=False)
        try:
            code = child.wait(timeout=TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=10)
            event('TIMEOUT')
            return 1
        if code != 0:
            event('CHILD_FAILED')
        return int(code != 0)
    except FileExistsError:
        event('ALREADY_ATTEMPTED')
        return 1
    except BaseException:
        if child is not None and child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except OSError:
                pass
        event('HELPER_FAILED')
        return 1


def startup():
    try:
        download_hash = os.environ.get('RUNNER_DOWNLOAD_SHA256', '')
        expires = os.environ.get('RUNNER_DOWNLOAD_EXPIRES', '')
        if re.fullmatch('[0-9a-f]{64}', download_hash) and expires.isdigit():
            subprocess.Popen([sys.executable, str(SCRIPT.with_name('runner_download_once.py')),
                              download_hash, expires], env=dict(CLEAN_ENV),
                             stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             close_fds=True, start_new_session=True, shell=False)
    except BaseException:
        event('DOWNLOAD_SPAWN_FAILED')
    try:
        expected_hash = os.environ.get('RUNNER_EXPORT_SOURCE_SHA256', '')
        if (os.environ.get('RUNNER_EXPORT_ONCE') == SNAPSHOT_ID
                and re.fullmatch('[0-9a-f]{64}', expected_hash)):
            subprocess.Popen([sys.executable, str(SCRIPT), '--supervise', expected_hash],
                             env=dict(CLEAN_ENV), stdin=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, close_fds=True,
                             start_new_session=True, shell=False)
    except BaseException:
        event('SPAWN_FAILED')
    finally:
        # Exact original startup argv/environment/cwd; no wait in this process.
        os.execv(sys.executable, [sys.executable, 'startup.py'])


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] in ('--supervise', '--worker'):
        if not re.fullmatch('[0-9a-f]{64}', sys.argv[2]):
            sys.exit(2)
        sys.exit((supervise if sys.argv[1] == '--supervise' else worker)(sys.argv[2]))
    startup()
