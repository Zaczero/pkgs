"""pyo3stubs gates for gometry (one parametrized test per gate).

Operator repair::

    python -m pyo3stubs gen-docs --config tools/stubs/stubconfig.py
    python -m pyo3stubs check-all --config tools/stubs/stubconfig.py
"""

from __future__ import annotations

from pyo3stubs.testing import gate_test

test_pyo3stubs_gate = gate_test('tools/stubs/stubconfig.py')


def test_public_toplevel_documented() -> None:
    """Top-level package re-exports must carry runtime docs (beyond `_lib`)."""
    import gometry as gm

    undocumented = [
        n
        for n in gm.__all__
        if callable(getattr(gm, n, None))
        and not isinstance(getattr(gm, n), type)
        and not (getattr(gm, n).__doc__ or '').strip()
    ]
    assert not undocumented, f'undocumented top-level functions: {undocumented}'
