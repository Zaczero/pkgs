"""PyO3 stub toolkit: generate ``.pyi`` docstrings from Rust ``///`` comments and
check stub/runtime parity, for any maturin/PyO3 package.

Configure a project with :class:`StubConfig`, then run
``pyo3stubs <command> --config tools/stubconfig.py`` or integrate with pytest
through :func:`pyo3stubs.testing.gate_test`.

Checks cover types, signatures, finality, Rust exports, nullability, and
docstrings. Cross-surface, scalar/array, and token-vocabulary checks are
enabled through their optional configuration records.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyo3stubs.config import (
    DEFAULT_IGNORED_TYPE_NAMES,
    DualityConfig,
    MacroExport,
    Reasons,
    StubConfig,
    SurfaceConfig,
    TextSignatureDefault,
    TokenConfig,
)

if TYPE_CHECKING:
    from pyo3stubs.gen import render_stub_with_docs

__all__ = [
    'DEFAULT_IGNORED_TYPE_NAMES',
    'DualityConfig',
    'MacroExport',
    'Reasons',
    'StubConfig',
    'SurfaceConfig',
    'TextSignatureDefault',
    'TokenConfig',
    'render_stub_with_docs',
]


def __getattr__(name: str) -> object:
    # The generator is the only libcst consumer; loading it lazily keeps
    # check-only environments (no libcst installed) fully functional.
    if name == 'render_stub_with_docs':
        from pyo3stubs.gen import render_stub_with_docs

        return render_stub_with_docs
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
