"""Focused semantic guards for ellipsoidal area of straight chart edges."""

from __future__ import annotations

import math

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


def test_chart_area_integrates_original_endpoints_before_shell_hole_rounding() -> None:
    slanted = gm.Polygon(
        [
            (13.0, 52.5),
            (13.001, 52.501),
            (13.001, 52.5010000001),
            (13.0, 52.5000000001),
        ],
        crs=4326,
    )
    donut = gm.Polygon(
        [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0)],
        holes=[
            [
                (1.0e-12, 1.0e-12),
                (4.0 - 1.0e-12, 1.0e-12),
                (4.0 - 1.0e-12, 4.0 - 1.0e-12),
                (1.0e-12, 4.0 - 1.0e-12),
            ]
        ],
        crs=4326,
    )

    assert slanted.area == pytest.approx(0.0007556885474199303, rel=1e-14, abs=0.0)
    assert donut.area == pytest.approx(0.1967593223485717, rel=1e-14, abs=0.0)

    values = gm.GeometryArray([slanted, donut], crs=4326)
    np.testing.assert_allclose(
        values.area,
        [0.0007556885474199303, 0.1967593223485717],
        rtol=1e-14,
        atol=0.0,
    )
    collection_area = gm.GeometryCollection([slanted, donut], crs=4326).area
    assert collection_area == pytest.approx(
        0.0007556885474199303 + 0.1967593223485717,
        rel=1e-14,
        abs=0.0,
    )


def test_chart_area_keeps_the_fifth_moment_after_an_exact_zero_prefix() -> None:
    points = [(0.0, 0.0)]
    for index, weight in enumerate((1.0, 4.0, 6.0, 4.0, 1.0)):
        low = index / 8.0
        high = low + 1.0 / 256.0
        width = weight / 256.0
        rectangle = [
            (0.0, low),
            (width, low),
            (width, high),
            (0.0, high),
            (0.0, low),
        ]
        if index % 2:
            rectangle.reverse()
        points.extend(rectangle)
        points.append((0.0, 0.0))

    polygon = gm.Polygon(points, crs=4326)
    expected = 3.129_371_369_696_864_181_844_455_016_408_56e-6
    assert polygon.area == pytest.approx(expected, rel=1e-12, abs=0.0)


def test_chart_area_uses_stable_phase_at_both_physical_poles() -> None:
    north_inside = math.nextafter(90.0, 0.0)
    south_inside = math.nextafter(-90.0, 0.0)
    north = gm.Polygon(
        [(13.0, north_inside), (13.01, north_inside), (13.01, 90.0), (13.0, 90.0)],
        crs=4326,
    )
    south = gm.Polygon(
        [(13.0, -90.0), (13.01, -90.0), (13.01, south_inside), (13.0, south_inside)],
        crs=4326,
    )

    expected = 2.198_605_450_772_230_784_075_320_676_741_271_294_1e-22
    assert north.area == pytest.approx(expected, rel=1e-14, abs=0.0)
    assert south.area == pytest.approx(expected, rel=1e-14, abs=0.0)

    values = gm.GeometryArray([north, south], crs=4326)
    np.testing.assert_allclose(values.area, [expected, expected], rtol=1e-14, atol=0.0)


def test_chart_area_scales_a_finite_result_after_the_combined_scale_overflows() -> None:
    crs = '+proj=longlat +a=1e153 +b=1e153 +type=crs'
    width = 1.2964554952662327e-149
    polygon = gm.Polygon(
        [(0.0, 0.0), (width, 0.0), (width, width), (0.0, width)],
        crs=crs,
    )
    expected = 51_200.000_000_000_01

    assert polygon.area == pytest.approx(expected, rel=1e-14, abs=0.0)


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


def test_ambiguous_full_turn_is_rejected_before_area_measurement() -> None:
    polygon = gm.Polygon(
        [(0.0, 10.0), (120.0, -5.0), (-120.0, 10.0), (0.0, 10.0)],
        crs=4326,
    )

    with pytest.raises(
        gm.InvalidGeometryError,
        match='ring winds around the globe and spans the equator',
    ):
        _ = polygon.area


@pytest.mark.parametrize(
    'reverse', [False, True], ids=['positive-turn', 'negative-turn']
)
def test_ambiguous_multiturn_area_is_rejected_on_scalar_and_array_paths(
    reverse: bool,
) -> None:
    points = [
        (0.0, 20.0),
        (120.0, -20.0),
        (240.0, 20.0),
        (360.0, -20.0),
        (480.0, 20.0),
        (600.0, -20.0),
        (0.0, 20.0),
    ]
    if reverse:
        points.reverse()
    polygon = gm.Polygon(points, crs='OGC:CRS84')
    valid = gm.box(0.0, 0.0, 1.0, 1.0, crs='OGC:CRS84')

    with pytest.raises(
        gm.InvalidGeometryError, match='ring winds around the globe'
    ) as scalar:
        _ = polygon.area
    assert 'geographic shape, ring 0, coordinate 0' in str(scalar.value)
    assert 'latitudes -20 to 20' in str(scalar.value)
    with pytest.raises(
        gm.InvalidGeometryError, match='ring winds around the globe'
    ) as scalar:
        _ = polygon.length
    assert 'geographic shape, ring 0, coordinate 0' in str(scalar.value)

    rows = gm.GeometryArray([valid, polygon], crs='OGC:CRS84')
    with pytest.raises(
        gm.InvalidGeometryError, match='ring winds around the globe'
    ) as array:
        _ = rows.area
    assert array.value.__notes__ == ['while processing array element 1']
    assert 'latitudes -20 to 20' in str(array.value)
    with pytest.raises(
        gm.InvalidGeometryError, match='ring winds around the globe'
    ) as array:
        _ = rows.length
    assert array.value.__notes__ == ['while processing array element 1']


@pytest.mark.parametrize(
    ('invalid_coordinate', 'expected_detail'),
    [
        ((10.0, 91.0), r'invalid longitude/latitude \(10, 91\)'),
        ((10.0, -91.0), r'invalid longitude/latitude \(10, -91\)'),
    ],
    ids=['later-north-out-of-range-latitude', 'later-south-out-of-range-latitude'],
)
def test_later_coordinate_error_precedes_pending_ambiguous_turn_gate(
    invalid_coordinate: tuple[float, float],
    expected_detail: str,
) -> None:
    shell = [
        (0.0, 20.0),
        (120.0, -20.0),
        (240.0, 20.0),
        (360.0, -20.0),
        (480.0, 20.0),
        (600.0, -20.0),
        (0.0, 20.0),
    ]
    hole = [
        (1.0, 1.0),
        (2.0, 1.0),
        invalid_coordinate,
        (1.0, 2.0),
        (1.0, 1.0),
    ]
    polygon = gm.Polygon(shell, holes=[hole], crs='OGC:CRS84')
    valid = gm.box(0.0, 0.0, 1.0, 1.0, crs='OGC:CRS84')

    with pytest.raises(
        gm.InvalidGeometryError,
        match=expected_detail,
    ) as scalar:
        _ = polygon.area
    assert 'geographic shape, ring 1, coordinate 2' in str(scalar.value)

    rows = gm.GeometryArray([valid, polygon], crs='OGC:CRS84')
    with pytest.raises(
        gm.InvalidGeometryError,
        match=expected_detail,
    ) as array:
        _ = rows.area
    assert array.value.__notes__ == ['while processing array element 1']


def test_full_turn_area_uses_the_summary_pole_when_the_first_vertex_is_equatorial() -> (
    None
):
    polygon = gm.Polygon(
        [(0.0, 0.0), (90.0, -80.0), (180.0, -80.0), (270.0, -80.0)],
        crs=4326,
    )

    assert polygon.area == pytest.approx(54_138_230_010_739.305, rel=1e-14)


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
