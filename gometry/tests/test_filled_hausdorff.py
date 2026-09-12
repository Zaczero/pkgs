"""Public filled-set Hausdorff contract."""

from __future__ import annotations

from math import isfinite, sqrt
from pathlib import Path

import gometry as gm
import pytest


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


@pytest.mark.parametrize(
    ("width", "height"),
    [
        (4.0, 3.0),
        (4.0, 1.0),
        (4.0, 2.0),
        (5.0, 3.0),
        (3.0, 2.0),
        (6.0, 4.0),
        (4.0, 3.5),
        (2.0, 1.0),
    ],
)
def test_filled_l_shape_matches_feasible_inradius(width: float, height: float) -> None:
    coordinates = [
        (0.0, 0.0),
        (9.0, 0.0),
        (9.0, height),
        (width, height),
        (width, 8.0),
        (0.0, 8.0),
        (0.0, 0.0),
    ]
    polygon = gm.Polygon(coordinates)
    boundary = gm.LineString(coordinates)
    candidate = width + height - sqrt(2.0 * width * height)
    expected = candidate if candidate <= height else width / 2.0

    assert gm.hausdorff_distance(polygon, boundary) == pytest.approx(
        expected,
        abs=1e-12,
    )


def test_filled_l_shape_reflex_vertex_is_an_attained_interior_maximum() -> None:
    coordinates = [
        (0.0, 0.0),
        (9.0, 0.0),
        (9.0, 3.0),
        (4.0, 3.0),
        (4.0, 8.0),
        (0.0, 8.0),
        (0.0, 0.0),
    ]
    polygon = gm.Polygon(coordinates)
    boundary = gm.LineString(coordinates)
    expected = 7.0 - sqrt(24.0)
    witness = gm.Point(expected, expected)
    actual = gm.hausdorff_distance(polygon, boundary)

    assert gm.contains(polygon, witness)
    assert gm.distance(witness, boundary) == pytest.approx(expected, abs=1e-12)
    assert actual == pytest.approx(expected, abs=1e-12)

    sample_distances = [
        gm.distance(gm.Point(x / 10.0, y / 10.0), boundary)
        for x in range(91)
        for y in range(81)
        if y <= 30 or x <= 40
    ]
    assert max(sample_distances) <= actual + 1e-12


def test_point_inside_filled_square_has_zero_directed_contribution() -> None:
    square = gm.box(2, 2, 8, 8)
    point = gm.Point(5, 5)

    # The public API is symmetric: the reverse direction is the square's
    # distance to its point, while the point-to-square contribution is zero.
    assert gm.hausdorff_distance(point, square) == sqrt(18.0)


def test_parallel_target_ridge_is_exact() -> None:
    source = gm.box(-1, -1, 1, 1)
    target = gm.GeometryCollection(
        [
            gm.LineString([(-2, -1), (2, -1)]),
            gm.LineString([(-2, 1), (2, 1)]),
        ]
    )

    assert gm.hausdorff_distance(source, target) == 1.0


def test_multiple_holes_and_hole_outside_source() -> None:
    source = gm.box(0, 0, 10, 10)
    target = gm.from_wkt(
        "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), "
        "(1 1, 3 1, 3 3, 1 3, 1 1), "
        "(6 6, 9 6, 9 9, 6 9, 6 6))"
    )
    assert gm.hausdorff_distance(source, target) == 1.5

    small_source = gm.box(0, 0, 1, 1)
    # The small source is covered by the target; the symmetric result is
    # therefore set by the target's farthest outer corner.
    assert gm.hausdorff_distance(small_source, target) == sqrt(162.0)


def test_mixed_dimension_collection_keeps_polygon_fill_and_point_union() -> None:
    source = gm.GeometryCollection([gm.box(0, 0, 2, 2), gm.Point(10, 0)])
    target = gm.Point(0, 0)

    assert gm.hausdorff_distance(source, target) == 10.0


def test_empty_operands_are_rejected() -> None:
    empty = gm.from_wkt("POLYGON EMPTY")
    point = gm.Point(0, 0)

    with pytest.raises(gm.InvalidGeometryError, match="non-empty"):
        gm.hausdorff_distance(empty, point)


def test_zero_area_polygon_keeps_closed_boundary_semantics() -> None:
    degenerate = gm.from_wkt("POLYGON ((0 0, 1 0, 2 0, 0 0))")
    line = gm.LineString([(0, 0), (2, 0)])

    assert gm.hausdorff_distance(degenerate, line) == 0.0


def test_near_degenerate_filled_polygon_hausdorff_terminates() -> None:
    fixtures = Path(__file__).with_name("fixtures")
    left = gm.from_wkt((fixtures / "hausdorff_stall_left.wkt").read_text())
    right = gm.from_wkt((fixtures / "hausdorff_stall_right.wkt").read_text())

    assert isfinite(gm.hausdorff_distance(left, right))
