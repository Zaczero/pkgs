"""Narrowest enclosing strip, independent of rectangle area and clearance."""

import math

import gometry as gm
import numpy as np
import pytest
from numpy.testing import assert_allclose


@pytest.mark.parametrize(
    ('source', 'expected'),
    [
        (gm.Point(7, 9), 0),
        (gm.LineString([(1, 2), (3, 4), (5, 6)]), 0),
        (gm.box(0, 0, 4, 2), 2),
        (gm.Polygon([(0, 0), (4, 0), (0, 3)]), 2.4),
        (gm.MultiPoint([(0, 0), (4, 0), (0, 3), (0, 0)]), 2.4),
    ],
)
def test_minimum_width_known_supports(source: gm.Geometry, expected: float) -> None:
    assert source.minimum_width() == expected


def test_minimum_width_is_not_the_minimum_rectangle_short_side() -> None:
    trapezoid = gm.Polygon([(0, 1), (8, 1), (8, 9), (2, 9)])
    rectangle = trapezoid.minimum_rotated_rectangle()
    assert rectangle.area == 64
    assert trapezoid.minimum_width() == pytest.approx(32 / math.sqrt(17))
    assert trapezoid.minimum_width() < 8


def test_minimum_width_uses_complete_support_and_ignores_holes() -> None:
    solid = gm.box(0, 0, 8, 4)
    holed = gm.Polygon(
        [(0, 0), (8, 0), (8, 4), (0, 4)],
        [
            [(1, 1), (1, 1.01), (2, 1.01), (2, 1)],
        ],
    )
    corners = gm.GeometryCollection([
        gm.Point(0, 0),
        gm.Point(8, 0),
        gm.Point(8, 4),
        gm.Point(0, 4),
    ])
    assert holed.minimum_width() == solid.minimum_width()
    assert corners.minimum_width() == 4
    assert holed.minimum_clearance() < holed.minimum_width()


def test_minimum_width_array_units_empty_and_missing() -> None:
    source = gm.GeometryArray([
        gm.box(0, 0, 10, 4, crs=2263),
        None,
        gm.Polygon(crs=2263),
        gm.Point(1, 2, crs=2263),
    ])
    assert_allclose(source.minimum_width(), [4, np.nan, np.nan, 0])
    assert_allclose(
        source.minimum_width(unit='meters'),
        [4 * 0.30480060960121924, np.nan, np.nan, 0],
    )
    assert math.isnan(gm.Polygon().minimum_width())


@pytest.mark.parametrize('scale', [2.0**-500, 2.0**500])
def test_minimum_width_exact_comparisons_handle_extreme_scales(scale: float) -> None:
    triangle = gm.Polygon([(0, 0), (4 * scale, 0), (0, 3 * scale)])
    assert triangle.minimum_width() == 2.4 * scale


def test_minimum_width_preserves_translated_hull_supports() -> None:
    base = 2.0**60
    source = gm.Polygon([
        (base, base + 256),
        (base + 2048, base + 256),
        (base + 2048, base + 2304),
        (base + 512, base + 2304),
    ])
    assert source.minimum_width() == pytest.approx(8192 / math.sqrt(17))


def test_minimum_width_geographic_frame_is_metric_and_planar_is_explicit() -> None:
    source = gm.box(2.9, 39, 3.1, 39.2, crs=4326)
    local = source.estimate_local_crs()
    assert source.minimum_width() == pytest.approx(
        source.to_crs(local).minimum_width(),
        rel=1e-5,
    )
    assert source.minimum_width(unit='planar') == pytest.approx(0.2)
    with pytest.raises(gm.GeometryError):
        gm.box(0, 0, 4, 2).minimum_width(unit='meters')
