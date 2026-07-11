"""One-call pytest integration: every pyo3stubs gate as parametrized tests.

A consuming package writes two lines::

    # tests/test_stubs.py
    from pyo3stubs.testing import gate_test

    test_pyo3stubs_gate = gate_test('tools/stubconfig.py')

and gets one parametrized test per gate in :data:`pyo3stubs.gates.REGISTRY`.
The config path resolves relative to the caller's working directory.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable

from pyo3stubs.gates import REGISTRY

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

#: Re-export for callers that inspect the registry (tests, docs).
GATES = REGISTRY


def gate_test(config: str | Path | StubConfig) -> Callable[..., None]:
    """A parametrized pytest test running every gate against ``config``.

    ``config`` is a ``StubConfig``, or a path to a config shim (a file
    exposing ``config()`` or ``CONFIG``), resolved relative to the caller's
    working directory at collection time.
    """
    import pytest

    def _resolve() -> StubConfig:
        from pyo3stubs.config import StubConfig

        if isinstance(config, StubConfig):
            return config
        from pyo3stubs.cli import load_config

        return load_config(str(config))

    @pytest.mark.parametrize('gate', list(REGISTRY))
    def test_pyo3stubs_gate(gate: str) -> None:
        cfg = _resolve()
        errors = REGISTRY[gate](cfg)
        assert not errors, f'pyo3stubs {gate} violations:\n' + '\n'.join(
            f'  {error}' for error in errors
        )

    return test_pyo3stubs_gate
