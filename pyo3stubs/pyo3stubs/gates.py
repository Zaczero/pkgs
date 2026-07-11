"""Single gate registry: CLI, pytest, and check-all share this list.

Every gate is ``name -> collect(cfg) -> list[str]``. Adding a monorepo
standard means adding one entry here — not a plugin bag.

Tier A (always-on): free or universal PyO3 boundary rules.
Tier B (config-activated): no-op when the matching StubConfig field is empty.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

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
    from pyo3stubs.config import StubConfig

Collector = Callable[['StubConfig'], list[str]]


def _gen_docs_sync(cfg: StubConfig) -> list[str]:
    from pyo3stubs.gen import render_stub_with_docs

    rendered = render_stub_with_docs(cfg)
    if cfg.stub_path.read_text(encoding='utf-8') != rendered:
        return [f'{cfg.stub_path} is out of sync; run `pyo3stubs gen-docs`']
    return []


#: Ordered gate name -> collector.
REGISTRY: dict[str, Collector] = {
    'validity': collect_validity_errors,
    'structural': collect_structural_errors,
    'text-signature': collect_text_signature_errors,
    'surface': collect_surface_parity_errors,
    'duality': collect_duality_errors,
    'namespace-facade': collect_namespace_errors,
    'token-vocabulary': collect_token_errors,
    'leaked-types': collect_leaked_types_errors,
    'rust-nullability': collect_rust_nullability_errors,
    'doc-contract': collect_doc_contract_errors,
    'gen-docs-sync': _gen_docs_sync,
    'stubtest': collect_stubtest_errors,
}

SUCCESS: dict[str, str] = {
    'validity': 'stub validity OK: mypy-clean',
    'structural': 'structural checks OK',
    'text-signature': 'text_signature OK: matches signature attributes',
    'surface': 'surface parity OK',
    'duality': 'scalar↔array duality OK',
    'namespace-facade': 'namespace facades OK',
    'token-vocabulary': 'token vocabulary OK',
    'leaked-types': 'no leaked types',
    'rust-nullability': 'rust nullability OK: every Option surface admits None',
    'doc-contract': 'doc contract OK',
    'gen-docs-sync': 'stub docstrings in sync',
    'stubtest': 'stubtest OK: stub matches the runtime',
}


def run_gate(name: str, cfg: StubConfig) -> list[str]:
    """Run one named gate; raises ``KeyError`` for unknown names."""
    return REGISTRY[name](cfg)


def run_all(cfg: StubConfig) -> dict[str, list[str]]:
    """Run every gate in registry order; values are violation lists."""
    return {name: collect(cfg) for name, collect in REGISTRY.items()}
