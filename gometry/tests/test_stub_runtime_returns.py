"""Overloaded ops must return the narrowed type their stub declares.

PyO3 already guarantees signatures (parameter names/kinds/defaults are checked
against the runtime by ``pyo3stubs`` stubtest). What that cannot see is
whether the *hand-authored* return-type narrowing in ``_lib.pyi`` — the overload
that promises ``buffer(Point) -> Polygon`` or ``area(Geometry) -> float`` — still
matches what the runtime actually hands back. This guard exercises a curated set
of those overloads on representative inputs and asserts the runtime result is an
instance of the declared type, so a stub that drifts from runtime behavior fails
loudly instead of silently misleading IDEs and type checkers.

It is deliberately a small, explicit table rather than a general type engine: the
goal is practical coverage of the narrowing that users feel, not re-proving what
PyO3 enforces.
"""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest

_POINT = gm.Point(0.0, 0.0)
_POINT2 = gm.Point(3.0, 4.0)
_POLY = gm.box(0.0, 0.0, 2.0, 2.0)
_LINE = gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)])
_ARRAY = gm.GeometryArray([_POINT, _POINT2])


@pytest.mark.parametrize(
    ('value', 'expected'),
    [
        (_POINT.buffer(1.0), gm.Polygon),
        (_POLY.centroid(), gm.Point),
        (_POLY.convex_hull(), gm.Polygon),
        (_LINE.envelope(), gm.Polygon),
        (gm.Point(0.0, 0.0, crs=4326).to_crs(3857), gm.Point),
        (gm.union_all([_POLY, gm.box(1.0, 1.0, 3.0, 3.0)]), gm.Geometry),
        (_ARRAY.buffer(1.0), gm.GeometryArray),
    ],
)
def test_scalar_return_types(value: object, expected: type) -> None:
    assert isinstance(value, expected)


def test_scalar_measures_are_python_floats() -> None:
    for value in (_POLY.area, _LINE.length, gm.distance(_POINT, _POINT2)):
        assert type(value) is float


def test_scalar_predicates_are_python_bools() -> None:
    for value in (gm.intersects(_POLY, _POINT), gm.contains(_POLY, _POINT)):
        assert type(value) is bool


def test_array_measures_return_float64_ndarray() -> None:
    out = _ARRAY.buffer(1.0).area
    assert isinstance(out, np.ndarray)
    assert out.dtype == np.float64
    assert out.shape == (len(_ARRAY),)


def test_array_predicates_return_bool_ndarray() -> None:
    out = gm.intersects(_ARRAY, _POINT)
    assert isinstance(out, np.ndarray)
    assert out.dtype == np.bool_
    assert out.shape == (len(_ARRAY),)


def test_nearest_points_returns_point_pair() -> None:
    a, b = gm.nearest_points(_POINT, _POLY)
    assert isinstance(a, gm.Point)
    assert isinstance(b, gm.Point)
