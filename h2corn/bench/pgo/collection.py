"""Reconcile useful work with image-owned profiles and supervisor joins."""

from __future__ import annotations

import hashlib
import json
import os
import re
import selectors
import signal
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from h2corn import _lib


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@contextmanager
def server(
    root: Path, config: dict, *, files: Path, pipe: bool = True, unix: bool = False
):
    root.mkdir(parents=True)
    package = Path(__file__).resolve().parents[2]
    exited, exit_signal = socket.socketpair()
    with (
        exited,
        exit_signal,
        socket.socket(socket.AF_UNIX if unix else socket.AF_INET) as listener,
        socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as notify,
        (root / 'server.log').open('wb') as log,
    ):
        endpoint = root / 'server.sock' if unix else None
        listener.bind(str(endpoint) if endpoint is not None else ('127.0.0.1', 0))
        listener.listen(2048)
        port: int | Path = (
            endpoint if endpoint is not None else listener.getsockname()[1]
        )
        notify_path = root / 'notify.sock'
        notify.bind(str(notify_path))
        config = {
            'bind': [f'fd://{listener.fileno()}'],
            'workers': 1,
            'runtime_threads': 2,
            'loop': 'asyncio',
            'lifespan': 'on',
            'timeout_lifespan_shutdown': 60,
            'timeout_graceful_shutdown': 30,
            'access_log': False,
            'log_format': 'json',
            'max_requests': 0,
            **config,
        }
        config_path = root / 'server.toml'
        config_path.write_text(
            ''.join(f'{key} = {json.dumps(value)}\n' for key, value in config.items())
        )
        command = [
            sys.executable,
            '-m',
            'h2corn',
            'bench.pgo.workload:app',
            '--app-dir',
            str(package),
            '--config',
            str(config_path),
        ]
        process = subprocess.Popen(
            command,
            cwd=root,
            stdout=subprocess.PIPE if pipe else log,
            stderr=subprocess.STDOUT,
            pass_fds=(listener.fileno(),),
            start_new_session=True,
            env={
                **os.environ,
                'NOTIFY_SOCKET': str(notify_path),
                'PGO_WORKER_OUTPUT': str(root),
                'PGO_FILES': str(files),
                'LLVM_PROFILE_FILE': str(root / 'parent-%p.profraw'),
            },
        )
        errors = []

        def drain(output):
            try:
                for line in output:
                    log.write(line)
            except OSError as error:
                errors.append(error)

        stdout = process.stdout
        drainer = (
            threading.Thread(target=drain, args=(stdout,))
            if stdout is not None
            else None
        )
        if drainer is not None:
            drainer.start()

        def signal_exit():
            process.wait()
            exit_signal.sendall(b'\0')

        waiter = threading.Thread(target=signal_exit)
        waiter.start()
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(notify, selectors.EVENT_READ)
                selector.register(exited, selectors.EVENT_READ)
                deadline = time.monotonic() + 60
                while True:
                    events = selector.select(max(0, deadline - time.monotonic()))
                    if not events:
                        raise TimeoutError('PGO fleet readiness deadline expired')
                    if any(key.fileobj is exited for key, _ in events):
                        raise RuntimeError(
                            f'PGO supervisor exited before readiness; see {root / "server.log"}'
                        )
                    if b'READY=1' in notify.recv(4096):
                        break
            yield port
            process.send_signal(signal.SIGTERM)
            if process.wait(timeout=120) != 0:
                raise RuntimeError('PGO supervisor exited unsuccessfully')
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=15)
            waiter.join()
            if drainer is not None:
                drainer.join(timeout=15)
                if drainer.is_alive():
                    raise RuntimeError('PGO log pipe did not close')
            if stdout is not None:
                stdout.close()
            notify_path.unlink()
            if endpoint is not None:
                endpoint.unlink()
        if errors:
            raise ExceptionGroup('PGO log drain failed', errors)
    (root / 'server.json').write_text(
        json.dumps(
            {
                'command': command,
                'config': config,
                'supervisor_pid': process.pid,
                'exitcode': process.returncode,
                'image_sha256': sha256(Path(_lib.__file__)),
            },
            indent=2,
        )
        + '\n'
    )


def reconcile(root: Path, completed: dict[int, int], workers: int) -> dict:
    owner = json.loads((root / 'server.json').read_text())
    starts = [json.loads(path.read_text()) for path in root.glob('*.start.json')]
    if len(starts) != workers or len(completed) != workers:
        raise RuntimeError(
            f'expected useful work in every one of {workers} workers: {completed}'
        )
    log = (root / 'server.log').read_text()
    started = []
    access_records = 0
    for line in log.splitlines():
        if line.startswith('{'):
            event = json.loads(line)
            if event.get('event') == 'worker_started':
                started.append(event['worker_pid'])
            if event.get('dropped_lines'):
                raise RuntimeError('PGO access log records were dropped')
            if event.get('event') == 'request':
                access_records += 1
        elif match := re.fullmatch(r'Started worker \[(\d+)\]', line):
            started.append(int(match[1]))
        elif 'access log lines' in line and 'dropped' in line:
            raise RuntimeError('PGO access log records were dropped')
        elif re.search(r'"(?:GET|POST) /[^"]+ HTTP/[12](?:\.[01])?" \d{3}', line):
            access_records += 1
    if sorted(started) != sorted(completed):
        raise RuntimeError(
            'supervisor generation inventory differs from completed work'
        )
    if owner['config']['access_log'] and access_records != sum(completed.values()):
        raise RuntimeError(
            f'access log completion mismatch: {access_records} versus {sum(completed.values())}'
        )
    raw_files = []
    for start in starts:
        pid = start['pid']
        finish = json.loads(
            (root / f'worker-{start["generation"]}.finish.json').read_text()
        )
        exit_record = json.loads((root / f'exit-{pid}.json').read_text())
        work = json.loads((root / f'work-{pid}.json').read_text())
        if (
            finish != start
            or exit_record['exitcode'] != 0
            or exit_record['forced_retirement_reap']
            or start['ppid'] != owner['supervisor_pid']
            or start['image_sha256'] != owner['image_sha256']
            or not start['gil_enabled']
        ):
            raise RuntimeError(f'unclean or wrong-image PGO worker: {pid}')
        if work['completed'] != completed[pid]:
            raise RuntimeError(f'worker/client completed-work mismatch: {pid}')
        raw = Path(start['raw'])
        if raw.parent != root or raw.stat().st_size == 0:
            raise RuntimeError(f'missing or misplaced worker profile: {pid}')
        raw_files.append({'path': str(raw), 'sha256': sha256(raw)})
    if {entry['path'] for entry in raw_files} != {
        str(path) for path in root.glob('worker-*.profraw')
    }:
        raise RuntimeError('unknown worker profile')
    return {
        'workers': starts,
        'completed': completed,
        'raw_files': raw_files,
        'server': owner,
    }
