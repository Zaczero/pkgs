from __future__ import annotations

import struct
from math import isclose

import gometry as gm
import pytest

CRS_CASES = (
    'OGC:CRS84',
    '+proj=longlat +a=7000000 +rf=150 +type=crs',
)
# Independent 8192-panel Simpson results for the WGS84 chart edge (0°, 10°)→(120°, 70°).
OBLIQUE_WGS84_HALF_CHART_PARAMETER = 0.57396247625753438
OBLIQUE_WGS84_HALF_ARCLENGTH_PARAMETER = 0.42925274533794278


def _chart_length(
    start: tuple[float, float],
    end: tuple[float, float],
    parameter: float,
    crs: str,
) -> float:
    pieces = 8
    coordinates = [
        (
            start[0] + (end[0] - start[0]) * parameter * index / pieces,
            start[1] + (end[1] - start[1]) * parameter * index / pieces,
        )
        for index in range(pieces + 1)
    ]
    wkt = 'LINESTRING (' + ','.join(f'{x:.17g} {y:.17g}' for x, y in coordinates) + ')'
    return gm.from_wkt(wkt, crs=crs).length


def _assert_root_sample(
    shape: gm.Geometry,
    *,
    start: tuple[float, float, float, float],
    end: tuple[float, float, float, float],
    parameter: float,
    crs: str,
) -> None:
    if isinstance(shape, gm.Point):
        coordinates = [(shape.x, shape.y, shape.z, shape.m)]
    elif isinstance(shape, gm.Polygon):
        coordinates = list(shape.exterior.coords)
    elif isinstance(shape, gm.LineString):
        coordinates = list(shape.coords)
    else:
        raise TypeError(f'expected a point, polygon, or linestring, got {shape!r}')
    y = start[1] + (end[1] - start[1]) * parameter
    point = next(
        coordinate
        for coordinate in coordinates
        if isclose(coordinate[1], y, rel_tol=0.0, abs_tol=1e-13)
    )
    fraction = _chart_length(start[:2], end[:2], parameter, crs) / _chart_length(
        start[:2], end[:2], 1.0, crs
    )
    expected_z = start[2] + (end[2] - start[2]) * fraction
    expected_m = start[3] + (end[3] - start[3]) * fraction
    assert isclose(point[2], expected_z, rel_tol=0.0, abs_tol=2e-8)
    assert isclose(point[3], expected_m, rel_tol=0.0, abs_tol=2e-7)


def _assert_source_point_ordinates(
    point: gm.Point,
    *,
    start: tuple[float, float, float, float],
    end: tuple[float, float, float, float],
    crs: str,
) -> None:
    parameter = (point.y - start[1]) / (end[1] - start[1])
    fraction = _chart_length(start[:2], end[:2], parameter, crs) / _chart_length(
        start[:2], end[:2], 1.0, crs
    )
    expected_z = start[2] + (end[2] - start[2]) * fraction
    expected_m = start[3] + (end[3] - start[3]) * fraction
    assert isclose(point.z, expected_z, rel_tol=0.0, abs_tol=2e-8)
    assert isclose(point.m, expected_m, rel_tol=0.0, abs_tol=2e-7)


def _planar_source(crs: str) -> tuple[gm.LineString, gm.Point]:
    return (
        gm.from_wkt('LINESTRING ZM (0 0 10 100, 120 60 210 500)', crs=crs),
        gm.from_wkt('POINT ZM (60 31 7 9)', crs=crs),
    )


def _assert_planar_source_witness(point: gm.Point, crs: str) -> None:
    assert isclose(point.x, 60.4, rel_tol=0.0, abs_tol=1e-13)
    assert isclose(point.y, 30.2, rel_tol=0.0, abs_tol=1e-13)
    _assert_source_point_ordinates(
        point,
        start=(0.0, 0.0, 10.0, 100.0),
        end=(120.0, 60.0, 210.0, 500.0),
        crs=crs,
    )


def test_raw_hull_and_degenerate_rectangle_samples_use_the_producer_root() -> None:
    for crs in CRS_CASES:
        source = gm.from_wkt(
            'LINESTRING ZM (0 0 10 100, 180 60 210 500)',
            crs=crs,
        )
        results = (
            source.convex_hull(),
            source.minimum_rotated_rectangle(),
            *gm.GeometryArray([source, source], crs=crs).convex_hull(),
            *gm.GeometryArray([source, source], crs=crs).minimum_rotated_rectangle(),
        )
        for result in results:
            _assert_root_sample(
                result,
                start=(0.0, 0.0, 10.0, 100.0),
                end=(180.0, 60.0, 210.0, 500.0),
                parameter=0.5,
                crs=crs,
            )

        lines = gm.GeometryArray([source, None, source], crs=crs)[[2, 1, 0]]
        for operation in (lines.convex_hull(), lines.minimum_rotated_rectangle()):
            assert operation[1] is None
            for index in (0, 2):
                _assert_root_sample(
                    operation[index],
                    start=(0.0, 0.0, 10.0, 100.0),
                    end=(180.0, 60.0, 210.0, 500.0),
                    parameter=0.5,
                    crs=crs,
                )


def test_full_turn_hull_keeps_the_producer_turn_for_all_inserted_samples() -> None:
    for crs in CRS_CASES:
        source = gm.from_wkt(
            'LINESTRING ZM (0 1 10 100, 360 60 210 500)',
            crs=crs,
        )

        results = (
            source.convex_hull(),
            source.minimum_rotated_rectangle(),
            *gm.GeometryArray([source, source], crs=crs).convex_hull(),
            *gm.GeometryArray([source, source], crs=crs).minimum_rotated_rectangle(),
        )
        for result in results:
            for parameter in (0.25, 0.5, 0.75):
                _assert_root_sample(
                    result,
                    start=(0.0, 1.0, 10.0, 100.0),
                    end=(360.0, 60.0, 210.0, 500.0),
                    parameter=parameter,
                    crs=crs,
                )


def test_point_chart_hull_closing_chord_uses_the_retained_long_root() -> None:
    expected = (120.0, 60.0, 210.0, 500.0), (-120.0, 10.0, 40.0, 160.0)

    for crs in CRS_CASES:
        source = gm.from_wkt(
            'MULTIPOINT ZM ((0 0 10 100), (120 60 210 500), (-120 10 40 160))',
            crs=crs,
        )

        _assert_root_sample(
            source.convex_hull(),
            start=expected[0],
            end=expected[1],
            parameter=0.5,
            crs=crs,
        )

        packed = gm.GeometryArray([source, source], crs=crs).convex_hull()
        for result in packed:
            _assert_root_sample(
                result,
                start=expected[0],
                end=expected[1],
                parameter=0.5,
                crs=crs,
            )

        gathered = gm.GeometryArray([source, None, source], crs=crs)[[2, 1, 0]]
        results = gathered.convex_hull()
        assert results[1] is None
        for index in (0, 2):
            _assert_root_sample(
                results[index],
                start=expected[0],
                end=expected[1],
                parameter=0.5,
                crs=crs,
            )


def test_planar_nearest_points_scalar_keeps_xy_and_resolves_source_roots() -> None:
    for crs in CRS_CASES:
        source, query = _planar_source(crs)
        planar_left, planar_right = gm.nearest_points(source, query, unit='planar')

        _assert_planar_source_witness(planar_left, crs)
        assert (planar_right.x, planar_right.y, planar_right.z, planar_right.m) == (
            60.0,
            31.0,
            7.0,
            9.0,
        )
        planar_line = gm.shortest_line(source, query, unit='planar')
        assert tuple(planar_line.coords[0]) == (
            planar_left.x,
            planar_left.y,
            planar_left.z,
            planar_left.m,
        )
        assert tuple(planar_line.coords[-1]) == (
            planar_right.x,
            planar_right.y,
            planar_right.z,
            planar_right.m,
        )

        geodesic_left, geodesic_right = gm.nearest_points(source, query)
        assert (geodesic_left.x, geodesic_left.y) != (planar_left.x, planar_left.y)
        _assert_source_point_ordinates(
            geodesic_left,
            start=(0.0, 0.0, 10.0, 100.0),
            end=(120.0, 60.0, 210.0, 500.0),
            crs=crs,
        )
        geodesic_line = gm.shortest_line(source, query)
        assert tuple(geodesic_line.coords[0]) == (
            geodesic_left.x,
            geodesic_left.y,
            geodesic_left.z,
            geodesic_left.m,
        )
        assert tuple(geodesic_line.coords[-1]) == (
            geodesic_right.x,
            geodesic_right.y,
            geodesic_right.z,
            geodesic_right.m,
        )


def test_planar_nearest_points_fixed_left_array_keeps_source_roots() -> None:
    for crs in CRS_CASES:
        source, query = _planar_source(crs)
        source_rows = gm.GeometryArray([source, None, source, source], crs=crs)[
            [3, 1, 0]
        ]
        left, right = gm.nearest_points(source_rows, query, unit='planar')

        assert left[1] is None
        assert right[1] is None
        for index in (0, 2):
            _assert_planar_source_witness(left[index], crs)
            assert (right[index].x, right[index].y, right[index].z, right[index].m) == (
                60.0,
                31.0,
                7.0,
                9.0,
            )


def test_planar_nearest_points_fixed_right_array_keeps_source_roots() -> None:
    for crs in CRS_CASES:
        source, query = _planar_source(crs)
        source_rows = gm.GeometryArray([source, None, source, source], crs=crs)[
            [3, 1, 0]
        ]
        left, right = gm.nearest_points(query, source_rows, unit='planar')

        assert left[1] is None
        assert right[1] is None
        for index in (0, 2):
            assert (left[index].x, left[index].y, left[index].z, left[index].m) == (
                60.0,
                31.0,
                7.0,
                9.0,
            )
            _assert_planar_source_witness(right[index], crs)


def test_planar_nearest_points_paired_arrays_keep_source_roots() -> None:
    for crs in CRS_CASES:
        source, query = _planar_source(crs)
        source_rows = gm.GeometryArray([source, None, source, source], crs=crs)[
            [3, 1, 0]
        ]
        query_rows = gm.GeometryArray([query, query, query], crs=crs)
        left, right = gm.nearest_points(source_rows, query_rows, unit='planar')

        assert left[1] is None
        assert right[1] is None
        for index in (0, 2):
            _assert_planar_source_witness(left[index], crs)
            assert (right[index].x, right[index].y, right[index].z, right[index].m) == (
                60.0,
                31.0,
                7.0,
                9.0,
            )


def test_shortest_line_samples_follow_the_producer_chord_for_all_pair_forms() -> None:
    start = (0.0, 0.0, 10.0, 100.0)
    end = (180.0, 60.0, 210.0, 500.0)

    for crs in CRS_CASES:
        left = gm.from_wkt('POINT ZM (0 0 10 100)', crs=crs)
        right = gm.from_wkt('POINT ZM (180 60 210 500)', crs=crs)
        left_rows = gm.GeometryArray([left, None, left], crs=crs)
        right_rows = gm.GeometryArray([right, None, right], crs=crs)

        for unit in (None, 'planar'):
            scalar = gm.shortest_line(left, right, unit=unit)
            _assert_root_sample(
                scalar,
                start=start,
                end=end,
                parameter=0.5,
                crs=crs,
            )

            arrays = (
                gm.shortest_line(left_rows, right, unit=unit),
                gm.shortest_line(left, right_rows, unit=unit),
                gm.shortest_line(left_rows, right_rows, unit=unit),
            )
            for result in arrays:
                assert len(result) == 3
                assert result[1] is None
                for index in (0, 2):
                    _assert_root_sample(
                        result[index],
                        start=start,
                        end=end,
                        parameter=0.5,
                        crs=crs,
                    )
        full_turn_start = (0.0, 1.0, 10.0, 100.0)
        full_turn_end = (360.0, 60.0, 210.0, 500.0)
        full_turn_line = gm.shortest_line(
            gm.from_wkt('POINT ZM (0 1 10 100)', crs=crs),
            gm.from_wkt('POINT ZM (360 60 210 500)', crs=crs),
            unit='planar',
        )
        for parameter in (0.25, 0.5, 0.75):
            _assert_root_sample(
                full_turn_line,
                start=full_turn_start,
                end=full_turn_end,
                parameter=parameter,
                crs=crs,
            )


def test_oblique_wgs84_lrs_and_substring_use_independent_chart_arclength() -> None:
    line = gm.from_wkt(
        'LINESTRING ZM (0 10 10 100, 120 70 210 500)',
        crs='OGC:CRS84',
    )
    fraction = OBLIQUE_WGS84_HALF_CHART_PARAMETER
    expected_z = 124.79249525150688
    expected_m = 329.5849905030137

    point = line.line_interpolate(fraction, normalized=True)
    assert isclose(point.x, 60.0, rel_tol=0.0, abs_tol=2e-12)
    assert isclose(point.y, 40.0, rel_tol=0.0, abs_tol=2e-12)
    assert isclose(point.z, expected_z, rel_tol=0.0, abs_tol=2e-8)
    assert isclose(point.m, expected_m, rel_tol=0.0, abs_tol=2e-7)

    substring = line.line_substring(0.0, fraction, normalized=True)
    endpoint = list(substring.coords)[-1]
    assert isclose(endpoint[0], 60.0, rel_tol=0.0, abs_tol=2e-12)
    assert isclose(endpoint[1], 40.0, rel_tol=0.0, abs_tol=2e-12)
    assert isclose(endpoint[2], expected_z, rel_tol=0.0, abs_tol=2e-8)
    assert isclose(endpoint[3], expected_m, rel_tol=0.0, abs_tol=2e-7)


def test_oblique_wgs84_intersection_uses_original_root_arclength() -> None:
    source = gm.from_wkt(
        'LINESTRING ZM (0 10 10 100, 120 70 210 500)',
        crs='OGC:CRS84',
    )
    crossing = gm.from_wkt('LINESTRING (60 0, 60 80)', crs='OGC:CRS84')
    intersection = gm.intersection(source, crossing)
    assert isinstance(intersection, gm.Point)
    assert isclose(intersection.x, 60.0, rel_tol=0.0, abs_tol=2e-12)
    assert isclose(intersection.y, 40.0, rel_tol=0.0, abs_tol=2e-12)
    assert isclose(intersection.z, 124.79249525150688, rel_tol=0.0, abs_tol=2e-8)
    assert isclose(intersection.m, 329.5849905030137, rel_tol=0.0, abs_tol=2e-7)


@pytest.mark.parametrize(
    ('z', 'm', 'expected_ordinates'),
    [
        pytest.param([10.0, 210.0], None, (110.0,), id='xyz'),
        pytest.param(None, [100.0, 500.0], (300.0,), id='xym'),
        pytest.param([10.0, 210.0], [100.0, 500.0], (110.0, 300.0), id='xyzm'),
    ],
)
@pytest.mark.parametrize('array', [False, True], ids=['scalar', 'array'])
@pytest.mark.parametrize('fractional', [False, True], ids=['max-length', 'fraction'])
def test_oblique_wgs84_segmentize_uses_independent_chart_arclength(
    z: list[float] | None,
    m: list[float] | None,
    expected_ordinates: tuple[float, ...],
    array: bool,
    fractional: bool,
) -> None:
    line = gm.LineString(
        [(0.0, 10.0), (120.0, 70.0)],
        z=z,
        m=m,
        crs='OGC:CRS84',
        epoch=2020.0,
    )
    target = (
        gm.GeometryArray([line, None], crs=line.crs, epoch=line.epoch)
        if array
        else line
    )
    result = (
        target.segmentize(fraction=0.5)
        if fractional
        else target.segmentize(line.length * 0.5001)
    )
    assert result.crs == line.crs
    assert result.epoch == line.epoch
    if isinstance(result, gm.GeometryArray):
        assert result.is_missing.tolist() == [False, True]
        result = result[0]
    assert isinstance(result, gm.LineString)
    assert result.coordinate_axes == line.coordinate_axes
    segmented = list(result.coords)
    assert len(segmented) == 3
    for actual, source in zip(
        (segmented[0], segmented[-1]), (line.coords[0], line.coords[-1]), strict=True
    ):
        assert struct.pack(f'{len(actual)}d', *actual) == struct.pack(
            f'{len(source)}d', *source
        )
    midpoint_parameter = OBLIQUE_WGS84_HALF_ARCLENGTH_PARAMETER
    midpoint = segmented[1]
    assert isclose(midpoint[0], 120.0 * midpoint_parameter, rel_tol=0.0, abs_tol=2e-8)
    assert isclose(
        midpoint[1], 10.0 + 60.0 * midpoint_parameter, rel_tol=0.0, abs_tol=2e-8
    )
    for actual, expected in zip(midpoint[2:], expected_ordinates, strict=True):
        assert isclose(actual, expected, rel_tol=0.0, abs_tol=2e-8)
