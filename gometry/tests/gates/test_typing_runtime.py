"""Mirrors the ``tools/gates/_check_typing_runtime.py`` static/runtime gate."""

from __future__ import annotations

from conftest import load_tool


def test_runtime_typing_gate_passes() -> None:
    gate = load_tool('_check_typing_runtime')
    assert gate.main() == 0
