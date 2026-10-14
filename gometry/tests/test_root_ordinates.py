from __future__ import annotations

from math import cos, fsum, isclose, radians, sin, sqrt

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
    if crs == 'OGC:CRS84':
        semi_major, flattening = 6_378_137.0, 1.0 / 298.257_223_563
    else:
        parameters = dict(
            token.split('=', maxsplit=1) for token in crs.split() if '=' in token
        )
        semi_major = float(parameters['+a'])
        flattening = 1.0 / float(parameters['+rf'])

    longitude_delta = radians(end[0] - start[0])
    latitude_start = radians(start[1])
    latitude_delta = radians(end[1] - start[1])
    eccentricity_squared = flattening * (2.0 - flattening)
    panels = 4096
    step = parameter / panels

    def speed(chart_parameter: float) -> float:
        latitude = latitude_start + chart_parameter * latitude_delta
        sine = sin(latitude)
        denominator = 1.0 - eccentricity_squared * sine * sine
        prime_vertical = semi_major / sqrt(denominator)
        meridional = semi_major * (1.0 - eccentricity_squared) / denominator**1.5
        return sqrt(
            (meridional * latitude_delta) ** 2
            + (prime_vertical * cos(latitude) * longitude_delta) ** 2
        )

    weighted_speed = fsum([
        speed(0.0),
        speed(parameter),
        *(
            (4.0 if index % 2 else 2.0) * speed(index * step)
            for index in range(1, panels)
        ),
    ])
    return step * weighted_speed / 3.0


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
    start = (0.0, 10.0, 10.0, 100.0)
    end = (120.0, 70.0, 210.0, 500.0)
    line = gm.from_wkt(
        'LINESTRING ZM (0 10 10 100, 120 70 210 500)',
        crs='OGC:CRS84',
    )
    parameter = 0.5
    fraction = _chart_length(
        start[:2], end[:2], parameter, 'OGC:CRS84'
    ) / _chart_length(start[:2], end[:2], 1.0, 'OGC:CRS84')
    assert isclose(fraction, 0.5739624762575344, rel_tol=0.0, abs_tol=2e-15)
    expected_z = start[2] + (end[2] - start[2]) * fraction
    expected_m = start[3] + (end[3] - start[3]) * fraction

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
    intersection = source.intersection(crossing)
    _assert_root_sample(
        intersection,
        start=(0.0, 10.0, 10.0, 100.0),
        end=(120.0, 70.0, 210.0, 500.0),
        parameter=0.5,
        crs='OGC:CRS84',
    )


def test_oblique_wgs84_segmentize_uses_independent_chart_arclength() -> None:
    start = (0.0, 10.0, 10.0, 100.0)
    end = (120.0, 70.0, 210.0, 500.0)
    line = gm.from_wkt(
        'LINESTRING ZM (0 10 10 100, 120 70 210 500)',
        crs='OGC:CRS84',
    )
    total_length = _chart_length(start[:2], end[:2], 1.0, 'OGC:CRS84')
    segmented = list(line.segmentize(total_length * 0.5001).coords)
    assert len(segmented) == 3
    low, high = 0.0, 1.0
    for _ in range(48):
        middle = (low + high) / 2.0
        if _chart_length(start[:2], end[:2], middle, 'OGC:CRS84') / total_length < 0.5:
            low = middle
        else:
            high = middle
    midpoint_parameter = (low + high) / 2.0
    midpoint = segmented[1]
    assert isclose(midpoint[0], 120.0 * midpoint_parameter, rel_tol=0.0, abs_tol=2e-8)
    assert isclose(
        midpoint[1], 10.0 + 60.0 * midpoint_parameter, rel_tol=0.0, abs_tol=2e-8
    )
    assert isclose(midpoint[2], 110.0, rel_tol=0.0, abs_tol=2e-8)
    assert isclose(midpoint[3], 300.0, rel_tol=0.0, abs_tol=2e-7)
