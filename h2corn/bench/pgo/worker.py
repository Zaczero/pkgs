"""Generator-wheel overlay; never installed in ordinary or profile-use wheels."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import time
from functools import wraps
from pathlib import Path

from h2corn import _lib


def _write(path: Path, record: dict) -> None:
    with path.open('x') as output:
        json.dump(record, output, sort_keys=True)
        output.write('\n')


def collect_worker(function):
    @wraps(function)
    def run(*args, **kwargs):
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError('PGO collection must begin on the worker main thread')
        root = Path(os.environ['PGO_WORKER_OUTPUT'])
        pid = os.getpid()
        token = f'{pid}-{time.monotonic_ns()}'
        image = Path(_lib.__file__).resolve()
        raw = root / f'worker-{token}.profraw'
        record = {
            'pid': pid,
            'ppid': os.getppid(),
            'generation': token,
            'image': str(image),
            'image_sha256': hashlib.sha256(image.read_bytes()).hexdigest(),
            'python': sys.version,
            'executable': sys.executable,
            'gil_enabled': getattr(sys, '_is_gil_enabled', lambda: False)(),
            'raw': str(raw),
        }
        _write(root / f'worker-{token}.start.json', record)
        _lib._pgo_begin(str(raw))  # pyright: ignore[reportAttributeAccessIssue] -- generator-only export
        result = function(*args, **kwargs)
        # multiprocessing's fork exit uses _exit, so C exit handlers never run.
        _lib._pgo_finish()  # pyright: ignore[reportAttributeAccessIssue] -- generator-only export
        pending = root / f'worker-{token}.pending.json'
        _write(pending, record)
        pending.rename(root / f'worker-{token}.finish.json')
        return result

    return run


def observe_exit(worker) -> None:
    root = Path(os.environ['PGO_WORKER_OUTPUT'])
    try:
        _write(
            root / f'exit-{worker.process.pid}.json',
            {
                'pid': worker.process.pid,
                'exitcode': worker.process.exitcode,
                'forced_retirement_reap': worker.forced_retirement_reap,
            },
        )
    except OSError as error:
        # Retirement must still close descriptors and join the remaining workers.
        print(f'PGO exit receipt failed: {error}', file=sys.stderr)
