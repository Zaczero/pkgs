from __future__ import annotations

from math import isclose

import gometry as gm

CRS_CASES = (
    'OGC:CRS84',
    '+proj=longlat +a=7000000 +rf=150 +type=crs',
)


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
    if isinstance(shape, gm.Polygon):
        coordinates = list(shape.exterior.coords)
    elif isinstance(shape, gm.LineString):
        coordinates = list(shape.coords)
    else:
        raise TypeError(f'expected a polygon or linestring, got {shape!r}')
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
    fraction = _chart_length(start[:2], (point.x, point.y), 1.0, crs) / _chart_length(
        start[:2], end[:2], 1.0, crs
    )
    expected_z = start[2] + (end[2] - start[2]) * fraction
    expected_m = start[3] + (end[3] - start[3]) * fraction
    assert isclose(point.z, expected_z, rel_tol=0.0, abs_tol=2e-8)
    assert isclose(point.m, expected_m, rel_tol=0.0, abs_tol=2e-7)


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


def test_planar_nearest_points_keep_the_selected_xy_and_resolve_source_roots() -> None:
    start = (0.0, 0.0, 10.0, 100.0)
    end = (120.0, 60.0, 210.0, 500.0)

    for crs in CRS_CASES:
        source = gm.from_wkt(
            'LINESTRING ZM (0 0 10 100, 120 60 210 500)',
            crs=crs,
        )
        query = gm.from_wkt('POINT ZM (60 31 7 9)', crs=crs)
        planar_left, planar_right = gm.nearest_points(source, query, unit='planar')

        assert isclose(planar_left.x, 60.4, rel_tol=0.0, abs_tol=1e-13)
        assert isclose(planar_left.y, 30.2, rel_tol=0.0, abs_tol=1e-13)
        _assert_source_point_ordinates(planar_left, start=start, end=end, crs=crs)
        assert (planar_right.x, planar_right.y, planar_right.z, planar_right.m) == (
            60.0,
            31.0,
            7.0,
            9.0,
        )

        geodesic_left, _ = gm.nearest_points(source, query)
        assert (geodesic_left.x, geodesic_left.y) != (planar_left.x, planar_left.y)
        _assert_source_point_ordinates(geodesic_left, start=start, end=end, crs=crs)

        source_rows = gm.GeometryArray([source, None, source, source], crs=crs)[
            [3, 1, 0]
        ]
        query_rows = gm.GeometryArray([query, query, query], crs=crs)
        fixed_left, _ = gm.nearest_points(source_rows, query, unit='planar')
        _, fixed_right = gm.nearest_points(query, source_rows, unit='planar')
        paired_left, _ = gm.nearest_points(source_rows, query_rows, unit='planar')

        for result in (fixed_left, fixed_right, paired_left):
            assert result[1] is None
            for index in (0, 2):
                _assert_source_point_ordinates(
                    result[index],
                    start=start,
                    end=end,
                    crs=crs,
                )
