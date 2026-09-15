"""Public filled-set Hausdorff contract."""

from __future__ import annotations

from math import inf, isfinite, isnan, nan, sqrt
from pathlib import Path

import gometry as gm
import pytest

PLANAR_ERROR = 1e-12


def test_filled_polygon_hole_center_is_a_source_interior_maximum() -> None:
    source = gm.box(2, 2, 8, 8)
    target = gm.from_wkt(
        "POLYGON ((2 2, 8 2, 8 8, 2 8, 2 2), "
        "(3 3, 7 3, 7 7, 3 7, 3 3))"
    )

    assert gm.hausdorff_distance(source, target, max_error=PLANAR_ERROR) == 2.0


def test_filled_square_against_its_boundary_uses_the_inradius() -> None:
    square = gm.box(0, 0, 2, 2)
    boundary = gm.LineString([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])

    assert gm.hausdorff_distance(square, boundary, max_error=PLANAR_ERROR) == 1.0


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

    assert gm.hausdorff_distance(
        polygon, boundary, max_error=PLANAR_ERROR
    ) == pytest.approx(
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
    actual = gm.hausdorff_distance(polygon, boundary, max_error=PLANAR_ERROR)

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
    assert gm.hausdorff_distance(point, square, max_error=PLANAR_ERROR) == sqrt(18.0)


def test_parallel_target_ridge_is_exact() -> None:
    source = gm.box(-1, -1, 1, 1)
    target = gm.GeometryCollection(
        [
            gm.LineString([(-2, -1), (2, -1)]),
            gm.LineString([(-2, 1), (2, 1)]),
        ]
    )

    assert gm.hausdorff_distance(source, target, max_error=PLANAR_ERROR) == 1.0


def test_multiple_holes_and_hole_outside_source() -> None:
    source = gm.box(0, 0, 10, 10)
    target = gm.from_wkt(
        "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), "
        "(1 1, 3 1, 3 3, 1 3, 1 1), "
        "(6 6, 9 6, 9 9, 6 9, 6 6))"
    )
    assert gm.hausdorff_distance(source, target, max_error=PLANAR_ERROR) == 1.5

    small_source = gm.box(0, 0, 1, 1)
    # The small source is covered by the target; the symmetric result is
    # therefore set by the target's farthest outer corner.
    assert gm.hausdorff_distance(small_source, target, max_error=PLANAR_ERROR) == sqrt(
        162.0
    )


def test_mixed_dimension_collection_keeps_polygon_fill_and_point_union() -> None:
    source = gm.GeometryCollection([gm.box(0, 0, 2, 2), gm.Point(10, 0)])
    target = gm.Point(0, 0)

    assert gm.hausdorff_distance(source, target, max_error=PLANAR_ERROR) == 10.0


def test_empty_operands_are_rejected() -> None:
    empty = gm.from_wkt("POLYGON EMPTY")
    point = gm.Point(0, 0)

    with pytest.raises(gm.InvalidGeometryError, match="non-empty"):
        gm.hausdorff_distance(empty, point, max_error=PLANAR_ERROR)


def test_same_storage_empty_rows_are_rejected_before_zero_shortcut() -> None:
    values = gm.GeometryArray([gm.from_wkt("LINESTRING EMPTY")])

    with pytest.raises(gm.InvalidGeometryError, match="non-empty"):
        gm.hausdorff_distance(values, values, max_error=PLANAR_ERROR)


def test_planar_hausdorff_requires_a_positive_finite_scalar_bound() -> None:
    line = gm.LineString([(0, 0), (1, 0)])

    with pytest.raises(ValueError, match="requires max_error"):
        gm.hausdorff_distance(line, line)
    for value in (0.0, -1.0, nan, inf, -inf):
        with pytest.raises(ValueError, match="max_error"):
            gm.hausdorff_distance(line, line, max_error=value)
    with pytest.raises(ValueError, match="scalar"):
        gm.hausdorff_distance(line, line, max_error=[PLANAR_ERROR])


def test_geodesic_hausdorff_rejects_a_supplied_bound() -> None:
    left = gm.Point(0.0, 0.0, crs=4326)
    right = gm.Point(1.0, 0.0, crs=4326)

    with pytest.raises(ValueError, match="only supported for planar"):
        gm.hausdorff_distance(left, right, max_error=1.0)
    assert gm.hausdorff_distance(left, right) > 0.0


def test_planar_hausdorff_bound_is_in_final_projected_units() -> None:
    left = gm.Point(0.0, 0.0, crs=2263)
    right = gm.Point(10.0, 0.0, crs=2263)

    assert gm.hausdorff_distance(left, right, max_error=1e-12) == pytest.approx(10.0)
    assert gm.hausdorff_distance(
        left,
        right,
        max_error=1e-12,
        unit="meters",
    ) == pytest.approx(3.048006096012192)


def test_planar_hausdorff_preserves_broadcast_and_missing_rows() -> None:
    line = gm.LineString([(0, 0), (1, 0)])
    shifted = gm.LineString([(0, 1), (1, 1)])
    values = gm.GeometryArray([line, None, shifted])

    result = gm.hausdorff_distance(values, line, max_error=PLANAR_ERROR)
    assert result[0] == pytest.approx(0.0)
    assert isnan(result[1])
    assert result[2] == pytest.approx(1.0)


def test_planar_hausdorff_distinguishes_exact_zero_from_a_close_unequal_pair() -> None:
    point = gm.Point(0.0, 0.0)
    close = gm.Point(1e-9, 0.0)
    segmented = gm.LineString([(0.0, 0.0), (0.5, 0.0), (1.0, 0.0)])
    whole = gm.LineString([(0.0, 0.0), (1.0, 0.0)])

    assert gm.hausdorff_distance(point, close, max_error=1e-6) > 0.0
    assert gm.hausdorff_distance(whole, segmented, max_error=PLANAR_ERROR) == 0.0


def test_planar_identity_does_not_bypass_cancelled_filled_support() -> None:
    cancelled = gm.from_wkt(
        "POLYGON ((0 0, 2 0, 0 2, 0 0), (0 0, 2 0, 0 2, 0 0))"
    )
    with pytest.raises(ValueError, match="non-empty"):
        gm.hausdorff_distance(cancelled, cancelled, max_error=PLANAR_ERROR)

    values = gm.GeometryArray([cancelled])
    with pytest.raises(ValueError, match="non-empty"):
        gm.hausdorff_distance(values, values, max_error=PLANAR_ERROR)


def test_zero_area_polygon_keeps_closed_boundary_semantics() -> None:
    degenerate = gm.from_wkt("POLYGON ((0 0, 1 0, 2 0, 0 0))")
    line = gm.LineString([(0, 0), (2, 0)])

    assert gm.hausdorff_distance(degenerate, line, max_error=PLANAR_ERROR) == 0.0


def test_near_degenerate_filled_polygon_hausdorff_terminates() -> None:
    fixtures = Path(__file__).with_name("fixtures")
    left = gm.from_wkt((fixtures / "hausdorff_stall_left.wkt").read_text())
    right = gm.from_wkt((fixtures / "hausdorff_stall_right.wkt").read_text())

    assert isfinite(gm.hausdorff_distance(left, right, max_error=PLANAR_ERROR))
