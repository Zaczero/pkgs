"""PyO3 stub toolkit: generate ``.pyi`` docstrings from Rust ``///`` comments and
check stub/runtime parity, for any maturin/PyO3 package.

A project supplies one :class:`StubConfig` (typically via a
``tools/stubconfig.py`` shim) and drives everything through the CLI —
``pyo3stubs <command> --config tools/stubconfig.py`` — or through pytest with
:func:`pyo3stubs.testing.gate_test`. :data:`pyo3stubs.gates.GATES` is the
single source of truth for both.

Detection is layered: mypy does the heavy lifting (``validity`` type-checks the
stub itself; ``stubtest`` compares it against the compiled runtime), and the
toolkit adds only what mypy cannot see — overload hygiene, runtime finality,
signature coverage, ``__match_args__`` parity, Rust-source leak/registration
and nullability scans, cross-surface option parity, ``text_signature`` honesty,
and the docstring pipeline. Optional gates (surface, duality, tokens) are named
gates that report themselves inactive until configured — not a plugin bag.

What a project imports from here is the config vocabulary below. The gate
machinery — :data:`~pyo3stubs.gates.GATES`, :class:`~pyo3stubs.gates.Gate`,
:class:`~pyo3stubs.gates.Status` — lives in :mod:`pyo3stubs.gates`, and each
gate's collector in its own module; a run reaches them through the registry
rather than by name.
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
