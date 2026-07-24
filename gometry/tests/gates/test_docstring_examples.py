"""Run the ``Examples`` doctests embedded in gometry's public docstrings.

Wraps ``tools/gates/_check_docstring_examples.py`` so the NumPy/Shapely-style examples in
every public docstring are executed in CI and cannot silently drift from behavior.
"""

from __future__ import annotations

from conftest import load_tool


def test_docstring_examples_execute() -> None:
    documented, examples, failures = load_tool('_check_docstring_examples').run()
    assert documented > 0, 'expected at least one documented example to run'
    assert failures == 0, f'{failures} of {examples} docstring examples failed'
