"""Typed-return gate: PyO3 surfaces must return ``Typed``, not bare ``PyGeometry``.

Mirrors ``tools/gates/_check_typed_returns.py`` so the typed-hierarchy seam cannot
silently regress (bare ``PyGeometry`` compiles but yields base ``Geometry``).
"""

from __future__ import annotations

from conftest import load_tool


def test_typed_returns_gate_is_green() -> None:
    gate = load_tool('_check_typed_returns')
    violations = gate.collect_violations()
    assert violations == [], [item.format() for item in violations]
