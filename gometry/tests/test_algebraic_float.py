"""Gate: algebraic-float placement and topology decision-surface rules."""

from __future__ import annotations

from conftest import load_tool


def test_algebraic_float_gate_is_green() -> None:
    gate = load_tool('_check_algebraic_float')
    algebraic_errors, _, _ = gate.collect_algebraic_errors()
    decision_errors = gate.collect_decision_surface_errors()
    assert algebraic_errors == []
    assert decision_errors == []
