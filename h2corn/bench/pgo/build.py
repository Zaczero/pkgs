"""Collection-only staging around the ordinary Maturin wheel builder."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


@contextmanager
def generator_overlay(package: Path):
    supervisor = package / 'python/h2corn/_supervisor.py'
    helper = package / 'python/h2corn/_pgo_collect.py'
    original = supervisor.read_bytes()
    text = original.decode()
    replacements = (
        (
            'def _worker_entry(\n',
            'from h2corn._pgo_collect import collect_worker, observe_exit\n'
            '\n\n@collect_worker\n'
            'def _worker_entry(\n',
        ),
        (
            '        worker.process.join()\n        self.retire_worker(sentinel, worker)\n',
            '        worker.process.join()\n'
            '        observe_exit(worker)\n'
            '        self.retire_worker(sentinel, worker)\n',
        ),
    )
    for old, new in replacements:
        if text.count(old) != 1:
            raise RuntimeError(f'PGO supervisor staging anchor changed: {old!r}')
        text = text.replace(old, new)
    with helper.open('xb') as output:
        output.write((package / 'bench/pgo/worker.py').read_bytes())
    try:
        supervisor.write_text(text)
        yield
    finally:
        supervisor.write_bytes(original)
        helper.unlink()
