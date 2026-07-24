"""Packed-column execution gate: detached logical-column seam must stay enforced.

Mirrors ``tools/gates/_check_packed_execution.py`` so a reintroduced
``map_packed_storage(``, identity-only ``.xy_columns()`` fast path, or direct
packed-kernel call from method surfaces cannot ship silently.
"""

from __future__ import annotations

from conftest import load_tool


def test_packed_execution_gate_is_green() -> None:
    gate = load_tool('_check_packed_execution')
    errors = gate.collect_errors()
    assert errors == [], errors
