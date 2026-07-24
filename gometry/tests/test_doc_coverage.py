"""Run the public-surface docstring contract gate.

Wraps ``tools/stubs/_doc_coverage.py`` so CI enforces the documentation contract on
every public callable (parameters, returns, raises, defaults).
"""

from __future__ import annotations

import gometry as gm
from conftest import load_tool


def test_doc_coverage_gate_passes() -> None:
    assert load_tool('_doc_coverage').main() == 0


def test_coverage_ops_required_raises_floors() -> None:
    """Polygonal coverage free duals + array methods document their floors.

    Free surfaces require CRSMismatchError (mixed iterables); array methods share
    one frame. InvalidGeometryError is content-only (simplify/union/clean).
    """
    gate = load_tool('_doc_coverage')
    cases = {
        'coverage_is_valid': {
            '': frozenset({'GeometryTypeError', 'GeometryError', 'CRSMismatchError'}),
            'GeometryArray': frozenset({'GeometryTypeError', 'GeometryError'}),
        },
        'coverage_invalid_edges': {
            '': frozenset({'GeometryTypeError', 'GeometryError', 'CRSMismatchError'}),
            'GeometryArray': frozenset({'GeometryTypeError', 'GeometryError'}),
        },
        'coverage_simplify': {
            '': frozenset({
                'GeometryTypeError',
                'GeometryError',
                'CRSMismatchError',
                'InvalidGeometryError',
            }),
            'GeometryArray': frozenset({
                'GeometryTypeError',
                'GeometryError',
                'InvalidGeometryError',
            }),
        },
        'coverage_union': {
            '': frozenset({'CRSMismatchError', 'InvalidGeometryError'}),
            'GeometryArray': frozenset({'InvalidGeometryError'}),
        },
        'coverage_clean': {
            '': frozenset({
                'GeometryTypeError',
                'GeometryError',
                'CRSMismatchError',
                'InvalidGeometryError',
            }),
            'GeometryArray': frozenset({
                'GeometryTypeError',
                'GeometryError',
                'InvalidGeometryError',
            }),
        },
    }
    for name, owners in cases.items():
        for owner, expected in owners.items():
            qual = name if owner == '' else f'{owner}.{name}'
            assert gate.required_raises(qual) == expected, qual
            obj = (
                getattr(gm, name)
                if owner == ''
                else getattr(gm.GeometryArray, name)
            )
            documented = set(gate.parse_doc_contract(obj.__doc__).raises)
            missing = expected - documented
            assert not missing, f'{qual} docs missing required Raises: {sorted(missing)}'
