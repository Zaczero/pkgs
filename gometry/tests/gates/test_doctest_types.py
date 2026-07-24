"""Mirrors ``tools/gates/_check_doctest_types.py`` so docstring examples stay
statically valid against the stubs (their execution twin is
``tests/test_docstring_examples.py``).
"""

from __future__ import annotations

from conftest import load_tool


def test_doctest_corpus_typechecks() -> None:
    gate = load_tool('_check_doctest_types')
    errors = gate.collect_errors()
    assert not errors, 'doctest corpus fails type-checking:\n' + '\n'.join(errors)
