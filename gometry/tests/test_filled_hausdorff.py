"""Public filled-set Hausdorff contract."""

from __future__ import annotations

from math import sqrt

import gometry as gm


def test_filled_polygon_hole_center_is_a_source_interior_maximum() -> None:
    source = gm.box(2, 2, 8, 8)
    target = gm.from_wkt(
        "POLYGON ((2 2, 8 2, 8 8, 2 8, 2 2), "
        "(3 3, 7 3, 7 7, 3 7, 3 3))"
    )

    assert gm.hausdorff_distance(source, target) == 2.0


def test_filled_square_against_its_boundary_uses_the_inradius() -> None:
    square = gm.box(0, 0, 2, 2)
    boundary = gm.LineString([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])

    assert gm.hausdorff_distance(square, boundary) == 1.0


def test_parallel_target_ridge_is_exact() -> None:
    source = gm.box(-1, -1, 1, 1)
    target = gm.GeometryCollection(
        [
            gm.LineString([(-2, -1), (2, -1)]),
            gm.LineString([(-2, 1), (2, 1)]),
        ]
    )

    assert gm.hausdorff_distance(source, target) == 1.0
