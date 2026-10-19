from __future__ import annotations

from pathlib import Path

from pyo3stubs.config import StubConfig
from pyo3stubs.rust_scan import constructor_doc_sources

GOMETRY_ROOT = Path(__file__).resolve().parents[2]


def test_h3_constructor_docs_are_recovered_from_real_macro_invocations() -> None:
    cfg = StubConfig(
        module='gometry._lib',
        stub_path=GOMETRY_ROOT / 'python/gometry/_lib.pyi',
        src_root=GOMETRY_ROOT / 'src',
        constructor_doc_macros=frozenset({'h3_index_array_common'}),
    )
    sources = constructor_doc_sources(cfg).by_class
    vertex = sources['H3VertexArray']
    edge = sources['H3EdgeArray']

    assert vertex.path == 'src/py/cells/h3/vertex_edge_array.rs'
    assert edge.path == vertex.path
    assert vertex.prose is not None
    assert edge.prose is not None
    assert 'Build an array of H3 vertices' in vertex.prose
    assert 'Build an array of H3 directed edges' in edge.prose
    assert vertex.prose != edge.prose
    assert vertex.line < edge.line
