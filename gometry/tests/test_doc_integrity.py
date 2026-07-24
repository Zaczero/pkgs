"""Enforce numpydoc-block integrity (no duplicated/orphaned sections).

Wraps ``tools/gates/_check_doc_integrity.py`` so CI rejects the doc-injection
corruption class (duplicated ``Parameters``/``Returns`` sections, a summary
leaked into the trailing ``Raises`` entry) that the parity and ``>>>``-example
gates cannot see.
"""

from __future__ import annotations

from conftest import load_tool


def test_doc_integrity_gate_passes() -> None:
    assert load_tool('_check_doc_integrity').main() == 0
