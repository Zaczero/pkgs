"""Focused semantic guards for ellipsoidal area of straight chart edges."""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest

from tests._support import chart_zone_area


def test_chart_area_is_ellipsoidal_and_keeps_holes() -> None:
    shell = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]
    hole = [(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)]
    polygon = gm.Polygon(shell, holes=[hole], crs=4326)

    shell_area = chart_zone_area(
        0.0, 4.0, 0.0, 4.0, semi_major=6378137.0, flattening=1.0 / 298.257223563
    )
    expected = 147_584_819_008.449_501_378_451_988_766_978_276_231_871_241_968_190_528_449_191_370_78

    assert polygon.area == pytest.approx(expected, rel=1e-13, abs=1e-7)
    assert polygon.area < shell_area
    assert polygon.area != pytest.approx(12.0, rel=1e-6)


def test_geographic_box_uses_four_corners_and_zone_area() -> None:
    polygon = gm.box(-45.0, 70.0, 45.0, 80.0, crs=4326)
    assert gm.get_coordinates(polygon).tolist() == [
        [-45.0, 70.0],
        [45.0, 70.0],
        [45.0, 80.0],
        [-45.0, 80.0],
        [-45.0, 70.0],
    ]
    expected = chart_zone_area(
        -45.0,
        45.0,
        70.0,
        80.0,
        semi_major=6_378_137.0,
        flattening=1.0 / 298.257223563,
    )
    assert polygon.area == pytest.approx(expected, rel=1e-13)


def test_full_world_box_uses_minimal_valid_chart_band() -> None:
    band = gm.box(-180.0, 70.0, 180.0, 80.0, crs=4326)
    assert gm.get_coordinates(band).tolist() == [
        [-180.0, 70.0],
        [-60.0, 70.0],
        [60.0, 70.0],
        [-180.0, 70.0],
        [-180.0, 80.0],
        [60.0, 80.0],
        [-60.0, 80.0],
        [-180.0, 80.0],
        [-180.0, 70.0],
    ]
    assert band.is_valid is True
    expected = chart_zone_area(
        -180.0,
        180.0,
        70.0,
        80.0,
        semi_major=6_378_137.0,
        flattening=1.0 / 298.257223563,
    )
    assert band.area == pytest.approx(expected, rel=1e-13)


def test_chart_area_preserves_empty_and_missing_rows() -> None:
    polygon = gm.box(0.0, 0.0, 1.0, 1.0, crs=4326)
    values = gm.GeometryArray(
        [polygon, gm.GeometryCollection([], crs=4326), None], crs=4326
    )

    np.testing.assert_allclose(values.area[:1], [polygon.area])
    assert values.area[1] == 0.0
    assert np.isnan(values.area[2])


def test_chart_measures_reject_exact_half_period_edges() -> None:
    line = gm.LineString([(0.0, 10.0), (180.0, 40.0)], crs=4326)
    polygon = gm.Polygon(
        [(0.0, 10.0), (180.0, 10.0), (180.0, 40.0), (0.0, 40.0)],
        crs=4326,
    )

    message = 'edge spans exactly half the longitude period; add a vertex'
    with pytest.raises(gm.GeometryError, match=message):
        _ = line.length
    with pytest.raises(gm.GeometryError, match=message):
        _ = polygon.area
    with pytest.raises(gm.GeometryError, match=message):
        _ = gm.GeometryArray([line], crs=4326).length
    with pytest.raises(gm.GeometryError, match=message):
        _ = gm.GeometryArray([polygon], crs=4326).area


def test_chart_area_and_length_conserve_chart_edge_refinement_and_overlay() -> None:
    original_line = gm.LineString([(-10.0, 70.0), (10.0, 80.0)], crs=4326)
    refined_line = gm.LineString([(-10.0, 70.0), (0.0, 75.0), (10.0, 80.0)], crs=4326)
    assert refined_line.length == pytest.approx(original_line.length, rel=1e-14)

    original_polygon = gm.Polygon([(0.0, 10.0), (30.0, 10.0), (15.0, 50.0)], crs=4326)
    refined_polygon = gm.Polygon(
        [
            (0.0, 10.0),
            (15.0, 10.0),
            (30.0, 10.0),
            (22.5, 30.0),
            (15.0, 50.0),
            (7.5, 30.0),
        ],
        crs=4326,
    )
    assert refined_polygon.area == pytest.approx(original_polygon.area, rel=1e-14)
    assert refined_polygon.length == pytest.approx(original_polygon.length, rel=1e-14)

    left = gm.box(0.0, 0.0, 2.0, 2.0, crs=4326)
    right = gm.box(1.0, 0.0, 3.0, 2.0, crs=4326)
    intersection = gm.intersection(left, right)
    union = gm.union(left, right)
    symmetric_difference = gm.symmetric_difference(left, right)
    assert left.area + right.area == pytest.approx(
        intersection.area + union.area, rel=1e-13
    )
    assert symmetric_difference.area == pytest.approx(
        left.area + right.area - 2.0 * intersection.area, rel=1e-13
    )
