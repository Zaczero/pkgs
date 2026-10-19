from __future__ import annotations

from typing import TYPE_CHECKING

from tests._support import load_tool

if TYPE_CHECKING:
    from pathlib import Path


def test_typed_return_gate_catches_real_bindings_not_literal_text(tmp_path: Path):
    gate = load_tool('_check_typed_returns')
    src_root = tmp_path / 'src'
    src_root.mkdir()
    source = """\
const DOC: &str = r###"#[pyfunction] fn spoof() -> PyGeometry { }"###;

#[pyo3::pyfunction]
fn direct() -> ::pyo3::PyResult<Option<crate::PyGeometry>> { todo!() }

#[cfg(test)]
mod tests {
    #[pyo3::pyfunction]
    fn test_only() -> crate::PyGeometry { todo!() }
}

frozen_pymethods! {
    impl PyGeometry {
        fn wrapped(&self) -> crate::PyResult<crate::PyGeometry> { todo!() }
    }
}
"""
    (src_root / 'lib.rs').write_text(source)

    violations = gate.collect_violations(src_root)

    assert [(item.name, item.surface, item.path, item.line) for item in violations] == [
        ('direct', '#[pyfunction]', 'lib.rs', 4),
        ('test_only', '#[pyfunction]', 'lib.rs', 9),
        ('wrapped', '#[pymethods]', 'lib.rs', 14),
    ]


def test_algebraic_gate_scans_test_bodies_not_literal_text(tmp_path: Path):
    gate = load_tool('_check_algebraic_float')
    src_root = tmp_path / 'src'
    source_dir = src_root / 'predicates'
    source_dir.mkdir(parents=True)
    source = """\
#[cfg(test)]
fn forbidden() {
    // .algebraic_sub() is only a comment.
    let _text = r###".algebraic_mul area > 0.0"###;
    let _ = left.algebraic_add(right);
}
"""
    (source_dir / 'fixture.rs').write_text(source)

    errors, total_uses, whitelisted_functions = gate.collect_algebraic_errors(src_root)

    assert errors == ['src/predicates/fixture.rs:5:forbidden']
    assert total_uses == 1
    assert whitelisted_functions == 0


def test_decision_surface_gate_ignores_literal_text(tmp_path: Path):
    gate = load_tool('_check_algebraic_float')
    src_root = tmp_path / 'src'
    source_dir = src_root / 'predicates'
    source_dir.mkdir(parents=True)
    source = """\
const DOC: &str = r###"if area > 0.0 { }"###;

fn decision() {
    if area > 0.0 {}
}
"""
    (source_dir / 'fixture.rs').write_text(source)

    errors = gate.collect_decision_surface_errors(src_root)

    assert errors == [
        'src/predicates/fixture.rs:4: raw area float branch in decision module'
    ]
