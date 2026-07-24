"""point_on_surface scanline + degenerate fallbacks — covers() invariant."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import cast

import gometry as gm
import pytest


@dataclass(frozen=True)
class SurfaceCase:
    name: str
    geom: gm.Geometry


def _holed_donut() -> gm.Geometry:
    return gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        holes=[[(3, 3), (7, 3), (7, 7), (3, 7), (3, 3)]],
    )


SURFACE_CASES = [
    SurfaceCase('thin_sliver', gm.box(0, 0, 100, 0.001)),
    SurfaceCase('holed_donut', _holed_donut()),
    SurfaceCase('zero_area_collinear', gm.Polygon([(0, 0), (1, 0), (2, 0), (0, 0)])),
    SurfaceCase('tall_aspect', gm.box(0, 0, 1, 1000)),
    SurfaceCase('wide_aspect', gm.box(0, 0, 1000, 1)),
]


def _assert_covers_invariant(geom: gm.Geometry, point: gm.Point) -> None:
    assert gm.covered_by(point, geom)
    if geom.geometry_type == 'Polygon':
        poly = cast('gm.Polygon', geom)
        assert gm.covers(poly, point)
        for hole in poly.interiors:
            assert not gm.covers(hole, point)


@pytest.mark.parametrize('case', SURFACE_CASES, ids=lambda c: c.name)
def test_point_on_surface_satisfies_covers_invariant(case: SurfaceCase) -> None:
    point = (case.geom).point_on_surface()
    assert point.geometry_type == 'Point'
    assert math.isfinite(point.x)
    assert math.isfinite(point.y)
    _assert_covers_invariant(case.geom, point)


def test_holed_point_on_surface_is_inside_shell_outside_holes() -> None:
    donut = _holed_donut()
    point = (donut).point_on_surface()
    assert gm.covers(donut, point)
    hole = gm.box(3, 3, 7, 7)
    assert not gm.contains(hole, point)
