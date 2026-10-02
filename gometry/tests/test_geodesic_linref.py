"""Geodesic linear referencing on arrays and MultiLineString."""

from __future__ import annotations

import math

import gometry as gm
import pytest

from tests._support import canon


def test_geodesic_line_cache_isolated_across_frame_retag() -> None:
    coordinates = [(-100.0, 30.0), (-80.0, 45.0)]
    original = gm.LineString(coordinates, crs=4326)
    # Warm WGS84 before the metadata-only retag shares the immutable shape.
    original.line_interpolate(500_000.0)
    retagged = original.set_crs(4267, overwrite=True)
    fresh = gm.LineString(coordinates, crs=4267)
    assert (
        retagged.line_interpolate(500_000.0).to_wkb()
        == fresh.line_interpolate(500_000.0).to_wkb()
    )


def _geo_meridian() -> gm.LineString:
    return gm.LineString([(0, 0), (0, 10)], crs=4326)


def _geo_parallel() -> gm.LineString:
    return gm.LineString([(0, 0), (10, 0)], crs=4326)


def _planar_line() -> gm.LineString:
    return gm.LineString([(0, 0), (0, 10)], crs=3857)


def test_multi_line_string_geodesic_interpolate_locate_round_trip() -> None:
    multi = gm.MultiLineString([[(0, 0), (0, 5)], [(0, 5), (0, 10)]], crs=4326)
    _ = multi.length
    half = (multi).line_interpolate(0.5, normalized=True)
    loc = (multi).line_locate(half, normalized=True)
    assert loc == pytest.approx(0.5, rel=1e-06)
    assert half.y == pytest.approx(5.0, abs=0.1)


def test_array_geodesic_lrs_fraction_round_trip() -> None:
    rows = gm.GeometryArray([_geo_meridian(), _geo_parallel()])
    fractions = [0.25, 0.75]
    points = (rows).line_interpolate(fractions, normalized=True)
    located = (rows).line_locate(points, normalized=True)
    assert list(located) == pytest.approx(fractions, rel=1e-05)


def test_array_geodesic_lrs_absolute_round_trip() -> None:
    rows = gm.GeometryArray([_geo_meridian(), _geo_meridian()])
    lengths = rows.length
    distances = [length * 0.4 for length in lengths]
    points = (rows).line_interpolate(distances)
    relocated = (rows).line_locate(points)
    assert list(relocated) == pytest.approx(distances, rel=1e-05)


def test_planar_array_lrs_uses_coordinate_units() -> None:
    rows = gm.GeometryArray([_planar_line(), _planar_line()])
    point = (rows).line_interpolate(5.0)[0]
    assert point.y == pytest.approx(5.0)
    assert (rows).line_locate(point)[0] == pytest.approx(5.0)


def test_geodesic_vs_planar_lrs_diverge_by_crs() -> None:
    geo = _geo_meridian()
    planar = _planar_line()
    assert geo.length > 1000000.0
    assert planar.length == pytest.approx(10.0)
    geo_mid = (geo).line_interpolate(500000.0)
    planar_mid = (planar).line_interpolate(5.0)
    assert geo_mid.y == pytest.approx(4.5, abs=0.2)
    assert planar_mid.y == pytest.approx(5.0)


def test_multi_line_string_substring_preserves_geodesic_length() -> None:
    multi = gm.MultiLineString([[(0, 0), (0, 5)], [(0, 5), (0, 10)]], crs=4326)
    part = (multi).line_substring(0.2, 0.8, normalized=True)
    assert part.length == pytest.approx(multi.length * 0.6, rel=1e-05)


def test_array_multi_line_string_lrs_matches_scalar() -> None:
    multi = gm.MultiLineString([[(0, 0), (0, 5)], [(0, 5), (0, 10)]], crs=4326)
    arr = gm.GeometryArray([multi, multi])
    scalar_point = (multi).line_interpolate(100000.0)
    array_points = (arr).line_interpolate([100000.0, 100000.0])
    assert canon(array_points) == canon(gm.GeometryArray([scalar_point, scalar_point]))


def test_geographic_fraction_densify_keeps_chart_edges_scalar_and_bulk() -> None:
    line = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=4326)
    scalar = line.segmentize(fraction=0.5)
    bulk = gm.GeometryArray([line]).segmentize(fraction=0.5)[0]
    length_scalar = line.segmentize(1_000_000.0)
    length_bulk = gm.GeometryArray([line]).segmentize(1_000_000.0)[0]

    assert scalar.coords[1][0] == scalar.coords[1][1]
    assert tuple(bulk.coords[1]) == tuple(scalar.coords[1])
    assert tuple(length_scalar.coords[1]) == tuple(scalar.coords[1])
    assert tuple(length_bulk.coords[1]) == tuple(scalar.coords[1])

    geodesic_midpoint = line.line_interpolate(0.5, normalized=True)
    assert abs(scalar.coords[1][0] - geodesic_midpoint.x) > 0.01

    piece_lengths = [
        gm.LineString([scalar.coords[index], scalar.coords[index + 1]], crs=4326).length
        for index in range(2)
    ]
    assert piece_lengths[0] == pytest.approx(piece_lengths[1], rel=2e-15)


def test_geographic_refinement_validates_coordinates_and_skips_missing_rows() -> None:
    invalid = gm.LineString([(0.0, 95.0), (1.0, 95.0)], crs=4326)
    for refine in (
        lambda: invalid.segmentize(1_000.0),
        lambda: invalid.segmentize(fraction=0.5),
        lambda: gm.GeometryArray([invalid]).segmentize(1_000.0),
        lambda: gm.GeometryArray([invalid]).segmentize(fraction=0.5),
    ):
        with pytest.raises(ValueError, match='invalid longitude/latitude'):
            refine()

    valid_row = gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326)
    with pytest.raises(ValueError, match='invalid longitude/latitude') as error:
        gm.GeometryArray([valid_row, invalid], crs=4326).segmentize(100_000.0)
    assert error.value.__notes__ == ['while processing array element 1']

    valid = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=4326)
    refined = gm.GeometryArray([valid, None], crs=4326).segmentize(fraction=0.5)
    assert refined.is_missing.tolist() == [False, True]


def test_geographic_segmentize_preserves_xyzm_and_array_metadata_with_missing_row() -> (
    None
):
    valid = gm.LineString(
        [(0.0, 0.0), (10.0, 10.0)],
        z=[10.0, 30.0],
        m=[100.0, 300.0],
        crs=4326,
        epoch=2020.0,
    )
    refined = gm.GeometryArray([valid, None], crs=4326, epoch=2020.0).segmentize(
        1_000_000.0
    )

    assert refined.is_missing.tolist() == [False, True]
    assert refined.crs is not None and refined.crs.to_epsg() == 4326
    assert refined.epoch == 2020.0
    coordinates = refined[0].coords.to_nested()
    assert coordinates[0] == (0.0, 0.0, 10.0, 100.0)
    assert coordinates[1][0] == coordinates[1][1]
    assert coordinates[1][2:] == (20.0, 200.0)
    assert coordinates[2] == (10.0, 10.0, 30.0, 300.0)


def test_geographic_segmentize_conserves_chart_shape_and_piece_lengths() -> None:
    polygon = gm.Polygon(
        [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)],
        crs=4326,
    )
    refined = polygon.segmentize(500_000.0)

    assert gm.equals(polygon, refined)
    assert refined.area == pytest.approx(polygon.area, rel=3e-15)
    assert refined.length == pytest.approx(polygon.length, rel=3e-15)
    ring = refined.exterior.coords
    lengths = [
        gm.LineString([ring[index], ring[index + 1]], crs=4326).length
        for index in range(len(ring) - 1)
    ]
    assert max(lengths) <= 500_000.0
    for start in range(0, len(lengths), 3):
        edge_lengths = lengths[start : start + 3]
        assert max(edge_lengths) == pytest.approx(min(edge_lengths), rel=3e-15)


def test_geographic_segmentize_insertions_stay_on_sloped_chart_edge() -> None:
    line = gm.LineString([(0.0, 10.0), (120.0, 60.0)], crs=4326)
    refined = line.segmentize(1_500_000.0)
    coordinates = refined.coords

    assert coordinates[0] == (0.0, 10.0)
    assert coordinates[-1] == (120.0, 60.0)
    assert len(coordinates) > 2
    for index in range(1, len(coordinates) - 1):
        longitude, latitude = coordinates[index]
        expected_latitude = 10.0 + 50.0 * (longitude / 120.0)
        assert abs(latitude - expected_latitude) <= 4.0 * math.ulp(expected_latitude)
    assert max(
        gm.LineString([coordinates[index], coordinates[index + 1]], crs=4326).length
        for index in range(len(coordinates) - 1)
    ) <= 1_500_000.0


def test_geographic_frechet_densify_uses_segmentize_chart_placement() -> None:
    left = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=4326)
    geodesic_midpoint = left.line_interpolate(0.5, normalized=True)
    right = gm.LineString(
        [(0.0, 0.0), (geodesic_midpoint.x, geodesic_midpoint.y), (10.0, 10.0)],
        crs=4326,
    )
    left_points = list(left.segmentize(fraction=0.5).coords)
    right_points = list(right.segmentize(fraction=0.5).coords)

    previous = [math.inf] * len(right_points)
    for left_index, left_xy in enumerate(left_points):
        current = [math.inf] * len(right_points)
        for right_index, right_xy in enumerate(right_points):
            distance = gm.distance(
                gm.Point(*left_xy, crs=4326),
                gm.Point(*right_xy, crs=4326),
            )
            if left_index == 0 and right_index == 0:
                current[right_index] = distance
            else:
                current[right_index] = max(
                    distance,
                    min(
                        previous[right_index],
                        current[right_index - 1] if right_index else math.inf,
                        previous[right_index - 1] if right_index else math.inf,
                    ),
                )
        previous = current

    assert gm.frechet_distance(left, right, densify=0.5) == pytest.approx(
        previous[-1], abs=1e-8, rel=0.0
    )


def test_fraction_densify_keeps_step_count_independent_of_shared_budget() -> None:
    line = gm.LineString([(0.0, 0.0), (10.0, 10.0)], crs=4326)
    collection = gm.GeometryCollection([line, line])

    refined = collection.segmentize(fraction=0.5)
    assert gm.get_coordinates(refined).shape == (6, 2)


@pytest.mark.parametrize(
    ('coordinates', 'expected_xy', 'expected_substring_xy'),
    [
        (
            [(179.0, 10.0), (-179.0, 10.0)],
            (-180.0, 10.001502283693659),
            [
                (179.49999885157385, 10.001126705155835),
                (-179.49999885157385, 10.001126705155837),
            ],
        ),
        (
            [(0.0, 80.0), (180.0, 80.0)],
            (0.0, 90.0),
            [(0.0, 85.00038196538807), (-180.0, 85.00038196538809)],
        ),
    ],
)
def test_geographic_lrs_scalar_and_bulk_keep_the_same_seam_or_pole_path(
    coordinates: list[tuple[float, float]],
    expected_xy: tuple[float, float],
    expected_substring_xy: list[tuple[float, float]],
) -> None:
    line = gm.LineString(coordinates, crs=4326, m=[0.0, 100.0])
    expected = gm.Point(*expected_xy, crs=4326)
    array = gm.GeometryArray([line])

    scalar = line.line_interpolate(50.0, basis='m')
    bulk = array.line_interpolate([50.0], basis='m')[0]
    assert (scalar.x, scalar.y) == pytest.approx(expected_xy, abs=1e-12, rel=0.0)
    assert (bulk.x, bulk.y) == pytest.approx(expected_xy, abs=1e-12, rel=0.0)
    for substring in (
        line.line_substring(25.0, 75.0, basis='m'),
        array.line_substring(25.0, 75.0, basis='m')[0],
    ):
        assert [tuple(point[:2]) for point in substring.coords] == pytest.approx(
            expected_substring_xy, abs=1e-12, rel=0.0
        )
    assert line.line_locate(expected, basis='m') == pytest.approx(50.0, abs=1e-7)
    assert array.line_locate(gm.GeometryArray([expected]), basis='m')[
        0
    ] == pytest.approx(50.0, abs=1e-7)


def test_geographic_lrs_substring_preserves_original_seam_endpoint_spelling() -> None:
    line = gm.LineString([(0.0, 80.0), (180.0, 80.0)], crs=4326, m=[0.0, 100.0])
    expected = [(0.0, 80.0, 0.0), (180.0, 80.0, 100.0)]
    scalar = line.line_substring(0.0, 100.0, basis='m')
    bulk = gm.GeometryArray([line]).line_substring(0.0, 100.0, basis='m')[0]
    assert scalar.coords.to_nested() == expected
    assert bulk.coords.to_nested() == expected
    assert line.line_interpolate(100.0, basis='m').x == 180.0
    assert gm.GeometryArray([line]).line_interpolate([100.0], basis='m')[0].x == 180.0
