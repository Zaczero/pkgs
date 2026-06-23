"""PyO3 stub toolkit: generate ``.pyi`` docstrings from Rust ``///`` comments and
check stub/runtime parity, for any maturin/PyO3 package.

A project supplies one :class:`StubConfig` (typically via a ``tools/stubconfig.py``
shim) and drives everything through the CLI: ``python -m pyo3stubs <command>
--config tools/stubconfig.py``. The ``collect_*`` functions and
:func:`render_stub_with_docs` are the library entry points the CLI wraps.
"""

from pyo3stubs.config import StubConfig
from pyo3stubs.context import CheckContext
from pyo3stubs.doc_contract import collect_doc_contract_errors
from pyo3stubs.gen import render_stub_with_docs
from pyo3stubs.leaked_types import collect_errors as collect_leaked_types_errors
from pyo3stubs.parity import collect_errors as collect_stub_parity_errors
from pyo3stubs.protocols import Check
from pyo3stubs.surface import collect_errors as collect_surface_parity_errors

__all__ = [
    'Check',
    'CheckContext',
    'StubConfig',
    'collect_doc_contract_errors',
    'collect_leaked_types_errors',
    'collect_stub_parity_errors',
    'collect_surface_parity_errors',
    'render_stub_with_docs',
]
