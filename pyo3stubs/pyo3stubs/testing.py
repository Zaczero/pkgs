"""One-call pytest integration: every pyo3stubs gate as parametrized tests.

A consuming package writes two lines::

    # tests/test_stubs.py
    from pyo3stubs.testing import gate_test

    test_pyo3stubs_gate = gate_test('tools/stubconfig.py')

and gets one parametrized test per gate (validity, structural, surface,
leaked-types, rust-nullability, plugins, stubtest, doc-contract, gen-docs
sync). The config path resolves relative to the consuming test file's
repository root (the directory containing the path as given).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable

import pyo3stubs

if TYPE_CHECKING:
    from pyo3stubs.config import StubConfig

#: gate name -> collector(cfg) -> list[str]
GATES: dict[str, Callable[[StubConfig], list[str]]] = {
    'validity': pyo3stubs.collect_validity_errors,
    'structural': pyo3stubs.collect_structural_errors,
    'surface': pyo3stubs.collect_surface_parity_errors,
    'leaked-types': pyo3stubs.collect_leaked_types_errors,
    'rust-nullability': pyo3stubs.collect_rust_nullability_errors,
    'plugins': pyo3stubs.collect_plugin_errors,
    'doc-contract': pyo3stubs.collect_doc_contract_errors,
    'stubtest': pyo3stubs.collect_stubtest_errors,
}


def _gen_docs_sync(cfg: StubConfig) -> list[str]:
    rendered = pyo3stubs.render_stub_with_docs(cfg)
    if cfg.stub_path.read_text(encoding='utf-8') != rendered:
        return [f'{cfg.stub_path} is out of sync; run `pyo3stubs gen-docs`']
    return []


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

    names = [*GATES, 'gen-docs-sync']

    @pytest.mark.parametrize('gate', names)
    def test_pyo3stubs_gate(gate: str) -> None:
        cfg = _resolve()
        collector = GATES.get(gate, _gen_docs_sync)
        errors = collector(cfg)
        assert not errors, f'pyo3stubs {gate} violations:\n' + '\n'.join(
            f'  {error}' for error in errors
        )

    return test_pyo3stubs_gate
