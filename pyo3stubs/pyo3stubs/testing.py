"""One-call pytest integration: every pyo3stubs gate as a parametrized test.

A consuming package writes two lines::

    # tests/test_stubs.py
    from pyo3stubs.testing import gate_test

    test_pyo3stubs_gate = gate_test('tools/stubconfig.py')

and gets one test per gate in :data:`pyo3stubs.gates.GATES`, addressable by its
plain name (``test_pyo3stubs_gate[validity]``). A gate that cannot run — wrong
interpreter, disabled, or not configured — is *skipped* with its reason, never
reported as a pass.

A relative config path is resolved against the calling test file and its
parent directories, so the suite runs the same from the repository root and
from inside ``tests/``.
"""

from __future__ import annotations

import inspect
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

from pyo3stubs.gates import GATES, Status, gate

if TYPE_CHECKING:
    from collections.abc import Callable

    from pyo3stubs.config import StubConfig


def _find_upward(relative: Path, start: Path) -> Path:
    """``relative`` resolved against ``start`` or one of its ancestors."""
    for directory in (start, *start.parents):
        candidate = directory / relative
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f'{relative}: no such config shim under {start} or any parent'
    )


@cache
def _load(path: Path) -> StubConfig:
    """One import per shim per session — it is the same config every gate reads."""
    from pyo3stubs.cli import load_config

    return load_config(path)


def gate_test(config: str | Path | StubConfig) -> Callable[..., None]:
    """A parametrized pytest test running every gate against ``config``.

    ``config`` is a :class:`~pyo3stubs.config.StubConfig`, or the path to a
    shim exposing ``config()`` or ``CONFIG``.
    """
    import pytest

    from pyo3stubs.config import StubConfig

    shim = '<shim>'
    if not isinstance(config, StubConfig):
        # The caller's frame only exists now, so the path is resolved here --
        # but the config is loaded per test, so a shim that cannot import (an
        # unbuilt extension) fails each gate with its reason instead of taking
        # collection of the whole module down.
        path = Path(config)
        caller = Path(inspect.stack(0)[1].filename).resolve().parent
        config = path if path.is_absolute() else _find_upward(path, caller)
        shim = str(config)

    @pytest.mark.parametrize(
        'name', [pytest.param(entry.name, id=entry.name) for entry in GATES]
    )
    def test_pyo3stubs_gate(name: str) -> None:
        cfg = config if isinstance(config, StubConfig) else _load(config)
        result = gate(name).run(cfg)
        if result.status in (Status.OK, Status.NOTHING_TO_CHECK):
            return
        if result.status is not Status.VIOLATIONS:
            pytest.skip(result.line)
        pytest.fail(
            f'{result.line}\n'
            + '\n'.join(f'  {error}' for error in result.errors)
            + f'\n  -> {result.gate.remedy.replace("<shim>", shim)}'
        )

    return test_pyo3stubs_gate
