"""Project-agnostic PyO3 stub/runtime parity checkers."""

from pyo3stubcheck.config import StubCheckConfig
from pyo3stubcheck.context import CheckContext
from pyo3stubcheck.leaked_types import collect_errors as collect_leaked_types_errors
from pyo3stubcheck.protocols import Check
from pyo3stubcheck.stub_docs import collect_doc_contract_errors
from pyo3stubcheck.stub_parity import collect_errors as collect_stub_parity_errors
from pyo3stubcheck.surface_parity import collect_errors as collect_surface_parity_errors

__all__ = [
    'Check',
    'CheckContext',
    'StubCheckConfig',
    'collect_doc_contract_errors',
    'collect_leaked_types_errors',
    'collect_stub_parity_errors',
    'collect_surface_parity_errors',
]