"""Per-check fixture suite: one clean case + one seeded violation each.

The fake runtime (``fixtures/fakepkg``) and fixture Rust source stand in for a
PyO3 package; mutated copies land in ``tmp_path`` so the pristine fixtures are
never written.
"""

from __future__ import annotations

from conftest import make_config
from pyo3stubs.doc_contract import collect_doc_contract_errors
from pyo3stubs.gen import render_stub_with_docs
from pyo3stubs.leaked_types import collect_errors as leaked_types
from pyo3stubs.rust_nullability import collect_errors as rust_nullability
from pyo3stubs.structural import collect_errors as structural


def test_all_source_gates_clean(tmp_path):
    cfg = make_config(tmp_path)
    assert structural(cfg) == []
    assert leaked_types(cfg) == []
    assert rust_nullability(cfg) == []
    assert collect_doc_contract_errors(cfg) == []


def test_overload_hygiene_flags_undecorated_dup(tmp_path, pristine_stub):
    mutated = pristine_stub.replace(
        '@overload\ndef overloaded(value: str) -> str:',
        'def overloaded(value: str) -> str:',
    )
    assert mutated != pristine_stub
    errors = structural(make_config(tmp_path, stub_text=mutated))
    assert any('without\n@overload' in e or 'without @overload' in e for e in errors)


def test_single_overload_flagged(tmp_path, pristine_stub):
    mutated = pristine_stub.replace(
        '@overload\ndef overloaded(value: int) -> int: ...\n', ''
    )
    errors = structural(make_config(tmp_path, stub_text=mutated))
    assert any('single @overload' in e for e in errors)


def test_finality_flags_missing_final(tmp_path, pristine_stub):
    mutated = pristine_stub.replace('@final\nclass Sealed:', 'class Sealed:')
    errors = structural(make_config(tmp_path, stub_text=mutated))
    assert any('Sealed' in e and '@final' in e for e in errors)


def test_finality_flags_spurious_final(tmp_path, pristine_stub):
    mutated = pristine_stub.replace('class Open:', '@final\nclass Open:')
    errors = structural(make_config(tmp_path, stub_text=mutated))
    assert any('Open' in e and 'subclassable' in e for e in errors)


def test_signature_coverage_requires_allowlist(tmp_path):
    cfg = make_config(tmp_path, uninspectable_allowlist={})
    errors = structural(cfg)
    assert any('hidden_callable' in e and 'inspect.signature' in e for e in errors)


def test_signature_coverage_flags_stale_allowlist(tmp_path):
    cfg = make_config(
        tmp_path,
        uninspectable_allowlist={
            'hidden_callable': 'fixture: __signature__ raises',
            'documented': 'stale: perfectly inspectable',
        },
    )
    errors = structural(cfg)
    assert any('documented' in e and 'inspectable again' in e for e in errors)


def test_match_args_value_drift(tmp_path, pristine_stub):
    mutated = pristine_stub.replace("('x', 'y')", "('y', 'x')")
    errors = structural(make_config(tmp_path, stub_text=mutated))
    assert any('__match_args__' in e for e in errors)


def test_leaked_types_flags_unregistered_unnamed_pyclass(tmp_path):
    errors = leaked_types(make_config(tmp_path, leak_allowlist={}))
    assert any("'Orphan' is not registered" in e for e in errors)


def test_leaked_types_sees_annotation_leaks(tmp_path, pristine_stub):
    mutated = pristine_stub.replace(
        "def documented(a: int, b: str = 'x') -> str:",
        "def documented(a: Orphan, b: str = 'x') -> str:",
    )
    errors = leaked_types(make_config(tmp_path, stub_text=mutated, leak_allowlist={}))
    assert any('leaked pyclass' in e and 'Orphan' in e for e in errors)


def test_rust_nullability_flags_denied_none(tmp_path, pristine_stub):
    mutated = pristine_stub.replace(
        'def maybe(self) -> int | None:', 'def maybe(self) -> int:'
    )
    errors = rust_nullability(make_config(tmp_path, stub_text=mutated))
    assert any('Sealed.maybe' in e and 'Option' in e for e in errors)


def test_doc_contract_flags_missing_runtime_doc(tmp_path, monkeypatch):
    import fakepkg._lib as lib

    monkeypatch.setattr(lib.Sealed.method, '__doc__', None, raising=False)
    errors = collect_doc_contract_errors(make_config(tmp_path))
    assert any('Sealed.method' in e for e in errors)


def test_gen_docs_places_docstring_on_last_overload(tmp_path, pristine_stub):
    # Move the docstring onto the FIRST variant; the renderer must put it back
    # on the last (the canonical carrier) and strip earlier variants.
    mutated = pristine_stub.replace(
        'def overloaded(value: int) -> int: ...',
        'def overloaded(value: int) -> int:\n'
        '    """Misplaced docstring."""',
    ).replace(
        'def overloaded(value: str) -> str:\n'
        '    """Distinct real variants narrow independently."""',
        'def overloaded(value: str) -> str: ...',
    )
    cfg = make_config(tmp_path, stub_text=mutated)
    rendered = render_stub_with_docs(cfg)
    assert 'Misplaced docstring' not in rendered
    assert (
        'def overloaded(value: str) -> str:\n'
        '    """Scalar-or-batch fixture for overload checks."""'
    ) in rendered


def test_text_signature_flags_param_drift(tmp_path, pristine_src):
    from pyo3stubs.text_signature import collect_errors

    mutated = pristine_src.replace('text_signature = "(a, b=\'x\')"', 'text_signature = "(a, c=\'x\')"')
    assert mutated != pristine_src
    errors = collect_errors(make_config(tmp_path, src_text=mutated))
    assert any('text_signature params' in e for e in errors)


def test_text_signature_flags_literal_default_drift(tmp_path, pristine_src):
    from pyo3stubs.text_signature import collect_errors

    mutated = pristine_src.replace('b = "x"', 'b = "y"')
    assert mutated != pristine_src
    errors = collect_errors(make_config(tmp_path, src_text=mutated))
    assert any('default' in e for e in errors)


def test_surface_flags_option_block_drift(tmp_path):
    class A:
        def op(self, data, *, tolerance=0.5):
            del data, tolerance

    class B:
        def op(self, data, *, tolerance=0.7):
            del data, tolerance

    from pyo3stubs.surface import collect_errors as surface

    cfg = make_config(tmp_path, surfaces=(('A', A), ('B', B)))
    errors = surface(cfg)
    assert any('option parameters diverge' in e for e in errors)


_DUALITY_STUB = '''\
from typing import Self, final

@final
class Leaf:
    def move(self) -> Self:
        """Kind-preserving."""
    def hull(self) -> Wide:
        """Kind-changing."""
    def pieces(self) -> LeafArray[Wide]:
        """Expansion: scalar returns an array."""

@final
class Wide: ...

@final
class LeafArray:
    def move(self) -> Self:
        """Kind-preserving twin."""
    def hull(self) -> LeafArray[Wide]:
        """Kind-changing twin."""
    def pieces(self) -> LeafArray[Wide]:
        """Expansion twin (flattened)."""
'''


def _duality(tmp_path, stub_text, **duality_kwargs):
    from pyo3stubs.config import DualityConfig
    from pyo3stubs.duality import collect_errors

    cfg = make_config(
        tmp_path,
        stub_text=stub_text,
        duality=DualityConfig(pairs=(('Leaf', 'LeafArray'),), **duality_kwargs),
    )
    return collect_errors(cfg)


def test_return_parity_clean(tmp_path):
    assert _duality(tmp_path, _DUALITY_STUB) == []


def test_return_parity_flags_self_on_kind_changing(tmp_path):
    mutated = _DUALITY_STUB.replace(
        'def hull(self) -> LeafArray[Wide]:', 'def hull(self) -> Self:'
    )
    errors = _duality(tmp_path, mutated)
    assert len(errors) == 1
    assert 'LeafArray.hull' in errors[0]
    assert 'must return `LeafArray[Wide]`' in errors[0]


def test_return_parity_flags_non_self_on_kind_preserving(tmp_path):
    mutated = _DUALITY_STUB.replace(
        '    def move(self) -> Self:\n        """Kind-preserving twin."""',
        '    def move(self) -> LeafArray[Wide]:\n        """Kind-preserving twin."""',
    )
    errors = _duality(tmp_path, mutated)
    assert len(errors) == 1
    assert 'LeafArray.move' in errors[0]
    assert 'must return `Self`' in errors[0]


def test_return_parity_exempt_silences(tmp_path):
    mutated = _DUALITY_STUB.replace(
        'def hull(self) -> LeafArray[Wide]:', 'def hull(self) -> Self:'
    )
    errors = _duality(
        tmp_path, mutated, exempt={'hull': 'fixture: deliberate divergence'}
    )
    assert errors == []


def test_gen_docs_is_idempotent(tmp_path):
    """Rendering an already-rendered stub must be a fixed point."""
    from pyo3stubs.gen import render_stub_with_docs

    cfg = make_config(tmp_path)
    once = render_stub_with_docs(cfg)
    cfg2 = make_config(tmp_path, stub_text=once)
    twice = render_stub_with_docs(cfg2)
    assert once == twice


def test_testing_gate_test_shape(tmp_path):
    """`pyo3stubs.testing.gate_test` yields one parametrized pytest test."""
    from pyo3stubs.gates import REGISTRY
    from pyo3stubs.testing import gate_test

    cfg = make_config(tmp_path)
    test = gate_test(cfg)
    marks = [m for m in getattr(test, 'pytestmark', [])]
    assert marks and marks[0].name == 'parametrize'
    names = marks[0].args[1]
    assert list(REGISTRY) == list(names)


def test_rust_class_map_honors_pyclass_patterns(tmp_path):
    """Macro-export patterns contribute to rust_class_map (nullability needs this)."""
    import re

    from pyo3stubs.rust_scan import pyclass_names, rust_class_map

    src = 'geometry_leaf!(PyPoint, "Point", "doc");\n'
    pattern = re.compile(r'geometry_leaf!\s*\(\s*(\w+)\s*,\s*"([^"]+)"')
    cfg = make_config(tmp_path, src_text=src, pyclass_patterns=(pattern,))
    assert rust_class_map(cfg)['PyPoint'] == 'Point'
    assert pyclass_names(cfg)['Point'].endswith('lib.rs')


def test_rust_scan_skips_cfg_test_modules(tmp_path):
    """A pyclass inside a ``#[cfg(test)]`` module never ships; the scan skips it."""
    from pyo3stubs.rust_scan import pyclass_names, rust_class_map

    src = (
        '#[pyclass]\n'
        'struct Shipped;\n'
        '\n'
        '#[cfg(test)]\n'
        'mod tests {\n'
        '    // a "{" inside comments or strings must not break brace matching\n'
        '    const BRACE: &str = "{";\n'
        '\n'
        '    #[pyclass(frozen)]\n'
        '    struct TestOnlyBenchmark;\n'
        '}\n'
    )
    cfg = make_config(tmp_path, src_text=src)
    assert set(rust_class_map(cfg)) == {'Shipped'}
    assert 'TestOnlyBenchmark' not in pyclass_names(cfg)
    assert pyclass_names(cfg)['Shipped'].endswith('lib.rs')
