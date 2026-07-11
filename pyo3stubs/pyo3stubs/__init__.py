"""PyO3 stub toolkit: generate ``.pyi`` docstrings from Rust ``///`` comments and
check stub/runtime parity, for any maturin/PyO3 package.

A project supplies one :class:`StubConfig` (typically via a ``tools/stubconfig.py``
shim) and drives everything through the CLI: ``python -m pyo3stubs <command>
--config tools/stubconfig.py``. The gate registry in :mod:`pyo3stubs.gates` is
the single source of truth for CLI, pytest (``gate_test``), and ``check-all``.

Detection is layered: mypy does the heavy lifting (``validity`` type-checks the
stub itself; ``stubtest`` compares it against the compiled runtime), and the
toolkit adds only what mypy cannot see — overload hygiene, runtime finality,
signature coverage, ``__match_args__`` parity, Rust-source leak/registration
scans, cross-surface option parity, text_signature honesty, and the docstring
pipeline. Config-activated features (duality, namespaces, tokens) are named
gates that no-op until configured — not a plugin bag.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyo3stubs.config import DualityConfig, NamespaceConfig, StubConfig, TokenConfig
from pyo3stubs.context import CheckContext
from pyo3stubs.doc_contract import collect_doc_contract_errors
from pyo3stubs.duality import collect_errors as collect_duality_errors
from pyo3stubs.leaked_types import collect_errors as collect_leaked_types_errors
from pyo3stubs.namespace_facade import collect_errors as collect_namespace_errors
from pyo3stubs.oracle import collect_stubtest_errors, collect_validity_errors
from pyo3stubs.rust_nullability import (
    collect_errors as collect_rust_nullability_errors,
)
from pyo3stubs.structural import collect_errors as collect_structural_errors
from pyo3stubs.surface import collect_errors as collect_surface_parity_errors
from pyo3stubs.text_signature import collect_errors as collect_text_signature_errors
from pyo3stubs.token_vocabulary import collect_errors as collect_token_errors

if TYPE_CHECKING:
    from pyo3stubs.gen import render_stub_with_docs

__all__ = [
    'CheckContext',
    'DualityConfig',
    'NamespaceConfig',
    'StubConfig',
    'TokenConfig',
    'collect_doc_contract_errors',
    'collect_duality_errors',
    'collect_leaked_types_errors',
    'collect_namespace_errors',
    'collect_rust_nullability_errors',
    'collect_structural_errors',
    'collect_stubtest_errors',
    'collect_surface_parity_errors',
    'collect_text_signature_errors',
    'collect_token_errors',
    'collect_validity_errors',
    'render_stub_with_docs',
]


def __getattr__(name: str) -> object:
    # The generator is the only libcst consumer; loading it lazily keeps
    # check-only environments (no libcst installed) fully functional.
    if name == 'render_stub_with_docs':
        from pyo3stubs.gen import render_stub_with_docs

        return render_stub_with_docs
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')
