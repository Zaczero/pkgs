"""Maximum-inscribed-circle output containment and representation failures."""

import math
from fractions import Fraction

import gometry as gm
import pytest


def test_maximum_inscribed_circle_covers_axis_aligned_boxes_scalar_and_array() -> None:
    sources = [gm.box(0, 0, 2, 2), gm.box(0, 0, 4, 2)]
    outputs = [source.maximum_inscribed_circle(tolerance=1e-12) for source in sources]
    assert [
        gm.covers(source, output)
        for source, output in zip(sources, outputs, strict=True)
    ] == [True, True]

    rows = gm.GeometryArray(sources)
    batch = rows.maximum_inscribed_circle(tolerance=[1e-12, 1e-12])
    assert [
        gm.covers(source, output) for source, output in zip(rows, batch, strict=True)
    ] == [True, True]
    assert [
        gm.equals_exact(output, batch[index]) for index, output in enumerate(outputs)
    ] == [
        True,
        True,
    ]


def test_maximum_inscribed_radius_uses_the_stored_coordinate_clearance() -> None:
    square = gm.box(0.1, 0.1, 0.3, 0.3)
    center = square.polylabel(tolerance=1e-12)
    assert square.maximum_inscribed_radius(tolerance=1e-12) == 0.09999999999999998
    assert square.maximum_inscribed_radius(tolerance=1e-12) == gm.distance(
        center, square.boundary()
    )
    assert gm.covers(square, square.maximum_inscribed_circle(tolerance=1e-12))


def test_maximum_inscribed_circle_respects_an_enclosed_hole() -> None:
    source = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10)],
        holes=[[(4.5, 4.5), (5.5, 4.5), (5.5, 5.5), (4.5, 5.5)]],
    )
    circle = source.maximum_inscribed_circle(tolerance=1e-12)
    assert gm.covers(source, circle)
    assert not gm.covers(circle, gm.Point(5, 5))


def test_maximum_inscribed_circle_emits_boundary_point_for_degenerate_ring() -> None:
    source = gm.from_wkt('POLYGON ((0 0, 1 0, 0 0, 1 0, 0 0))')
    result = source.maximum_inscribed_circle(tolerance=1e-12)
    assert result.geometry_type == 'Point'
    assert gm.covers(source, result)


def test_maximum_inscribed_circle_repairs_diamond_vertex_rounding() -> None:
    diamond = gm.Polygon([(0, 1), (-1, 0), (0, -1), (1, 0)])
    circle = diamond.maximum_inscribed_circle(tolerance=1e-12)
    assert gm.covers(diamond, circle)
    assert diamond.maximum_inscribed_radius(tolerance=1e-12) == pytest.approx(
        2**-0.5, abs=1e-12
    )


def test_maximum_inscribed_circle_repairs_rotated_vertex_rounding() -> None:
    rotated = gm.Polygon([
        (0.00000000000000008659560562354934, 1.4142135623730951),
        (-1.4142135623730951, 0.00000000000000017319121124709868),
        (-0.000000000000000259786816870648, -1.4142135623730951),
        (1.4142135623730951, -0.00000000000000034638242249419736),
    ])
    circle = rotated.maximum_inscribed_circle(tolerance=1e-12)
    assert gm.covers(rotated, circle)


def test_maximum_inscribed_radius_certifies_the_square_inradius() -> None:
    source = gm.box(0, 0, 4, 4)
    tolerance = 1e-12
    radius = Fraction(source.maximum_inscribed_radius(tolerance=tolerance))
    error = Fraction(tolerance)
    assert Fraction(2) - error <= radius <= Fraction(2)


def test_maximum_inscribed_circle_keeps_a_deterministic_parallel_plateau_center() -> (
    None
):
    source = gm.box(0, 0, 8, 4)
    first = source.maximum_inscribed_circle(tolerance=1e-12)
    second = source.maximum_inscribed_circle(tolerance=1e-12)
    assert first.to_wkt() == second.to_wkt()
    assert source.maximum_inscribed_radius(tolerance=1e-12) == 2.0
    assert first.centroid().x == pytest.approx(4.0)
    assert first.centroid().y == pytest.approx(2.0)


def test_maximum_inscribed_radius_certifies_the_reflex_l_shape_analytically() -> None:
    source = gm.Polygon([(0, 0), (5, 0), (5, 2), (2, 2), (2, 5), (0, 5)])
    tolerance = 1e-9
    radius = Fraction(source.maximum_inscribed_radius(tolerance=tolerance))
    error = Fraction(tolerance)
    # The two outer sides meet the reflex corner at 45 degrees:
    # r = 4 - 2 * sqrt(2). These squared inequalities
    # avoid importing a geometric oracle or rounding the irrational answer.
    assert (4 - radius) ** 2 >= 8
    assert (4 - radius - error) ** 2 <= 8
    assert gm.covers(source, source.maximum_inscribed_circle(tolerance=tolerance))


def test_maximum_inscribed_circle_selects_the_first_equal_multipart_winner() -> None:
    source = gm.from_wkt(
        'MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((10 0,14 0,14 4,10 4,10 0)))'
    )
    first = source.maximum_inscribed_circle(tolerance=1e-9)
    second = source.maximum_inscribed_circle(tolerance=1e-9)
    assert first.to_wkt() == second.to_wkt()
    assert first.centroid().x < 4.0
    assert source.maximum_inscribed_radius(tolerance=1e-9) <= 2.0


def test_maximum_inscribed_circle_handles_reciprocal_coordinate_scales() -> None:
    source = gm.box(-1e150, -1e-150, 1e150, 1e-150)
    tolerance = 1e-153
    radius = Fraction(source.maximum_inscribed_radius(tolerance=tolerance))
    expected = Fraction(1e-150)
    error = Fraction(tolerance)
    assert expected - error <= radius <= expected
    assert gm.covers(source, source.maximum_inscribed_circle(tolerance=tolerance))


def test_maximum_inscribed_circle_reports_unrepresentable_tight_translated_accuracy() -> (
    None
):
    source = gm.box(
        1e300,
        1e-300,
        math.nextafter(1e300, math.inf),
        2e-300,
    )
    with pytest.raises(gm.GeometryError):
        source.maximum_inscribed_radius(tolerance=1e-301)
    assert source.maximum_inscribed_radius(tolerance=1e-300) == 0.0
    point = source.maximum_inscribed_circle(tolerance=1e-300)
    assert point.geometry_type == 'Point'
    assert gm.covers(source, point)


def test_maximum_inscribed_circle_radius_matches_the_emitted_polygon() -> None:
    source = gm.box(0, 0, 8, 4)
    circle = source.maximum_inscribed_circle(tolerance=1e-12)
    radius = source.maximum_inscribed_radius(tolerance=1e-12)
    inferred = math.sqrt(2.0 * circle.area / (64.0 * math.sin(2.0 * math.pi / 64.0)))
    assert inferred == pytest.approx(radius, abs=1e-12)


def test_maximum_inscribed_circle_centroid_and_radius_recover_the_disk() -> None:
    source = gm.Polygon([(0, 0), (5, 0), (5, 2), (2, 2), (2, 5), (0, 5)])
    tolerance = 1e-12
    disk = source.maximum_inscribed_circle(tolerance=tolerance)
    center = disk.centroid()
    radius = source.maximum_inscribed_radius(tolerance=tolerance)

    assert gm.covers(source, center)
    assert gm.distance(center, source.boundary()) == pytest.approx(
        radius, abs=tolerance
    )


def test_maximum_inscribed_circle_omitted_and_explicit_tolerances_are_certified() -> (
    None
):
    source = gm.Polygon([(0, 0), (5, 0), (5, 2), (2, 2), (2, 5), (0, 5)])
    omitted = source.maximum_inscribed_radius()
    explicit = source.maximum_inscribed_radius(tolerance=1e-3)
    expected = 4.0 - 2.0 * math.sqrt(2.0)
    assert expected - 1e-3 <= explicit <= expected
    assert expected - 0.01 <= omitted <= expected


def test_maximum_inscribed_circle_rejects_a_disk_below_the_coordinate_spacing() -> None:
    # An inradius of about 2,000 where coordinates are spaced 256 apart has
    # no representable contained 64-gon. Shrinking one ulp of the radius per
    # retry once exhausted the generated-item budget before saying so.
    base = 2.0**60
    source = gm.Polygon([(base, base), (base + 2.0**20, base), (base, base + 2.0**12)])
    with pytest.raises(gm.GeometryError, match='contained geometry'):
        source.maximum_inscribed_circle()
