"""Packed LRS — column-direct interpolate/substring/locate on Lines storage."""

from __future__ import annotations

import math

import gometry as gm
import pytest

from tests._support import canon, geo_line_storage_twins, line_storage_twins

_WGS84_PARALLEL_20_6_M = 627_882.517_859_171_7
_WGS84_PARALLEL_10_2_M = 219_278.728_136_305_96
_WGS84_PARALLEL_60_10_M = 558_000.015_724_361_4
_WGS84_PARALLEL_20_6_STATION_BUDGET_M = 2.5608939952816194e-8


def _parallel_xy_budget(
    source_longitude: float,
    expected_longitude: float,
    latitude: float,
    longitude_span: float,
) -> tuple[float, float]:
    return (
        4.0 * max(math.ulp(source_longitude), math.ulp(expected_longitude))
        + abs(longitude_span) * 4.0 * math.ulp(1.0),
        4.0 * math.ulp(latitude),
    )


def _long_packed_lines() -> gm.GeometryArray:
    return gm.GeometryArray([
        gm.LineString([(0, 0), (10, 0)], crs=3857),
        gm.LineString([(0, 0), (0, 8)], crs=3857),
    ])


def test_packed_line_interpolate_point_exports_geoarrow_point() -> None:
    lines = _long_packed_lines()
    assert lines.to_arrow().type.extension_name == 'geoarrow.linestring'
    out = (lines).line_interpolate([3.0, 4.0])
    assert out.to_arrow().type.extension_name == 'geoarrow.point'
    assert [p.x for p in out] == [3.0, 0.0]
    assert [p.y for p in out] == [0.0, 4.0]


def test_packed_line_substring_exports_geoarrow_linestring() -> None:
    lines = _long_packed_lines()
    out = (lines).line_substring([1.0, 2.0], [5.0, 6.0])
    assert out.to_arrow().type.extension_name == 'geoarrow.linestring'
    assert (out[0]).to_wkt() == 'LINESTRING (1 0, 5 0)'
    assert (out[1]).to_wkt() == 'LINESTRING (0 2, 0 6)'


def test_packed_lrs_preserves_zero_length_endpoint_vertices() -> None:
    line = gm.from_wkt('LINESTRING ZM (0 0 0 0, 0 0 10 1, 1 0 20 2, 1 0 30 3)')
    lines = gm.GeometryArray([line])
    expected = 'LINESTRING ZM (0 0 0 0, 0 0 10 1, 1 0 20 2, 1 0 30 3)'

    assert lines.to_arrow().type.extension_name == 'geoarrow.linestring'
    assert lines.line_interpolate(0.0)[0].to_wkt() == 'POINT ZM (0 0 0 0)'
    assert lines.line_interpolate(1.0, normalized=True)[0].to_wkt() == (
        'POINT ZM (1 0 30 3)'
    )
    assert lines.line_substring(0.0, 1.0, normalized=True)[0].to_wkt() == expected


def test_packed_interpolate_matches_mixed_storage() -> None:
    packed, mixed = line_storage_twins()
    distances = [0.5, 1.0]
    assert canon((packed).line_interpolate(distances)) == canon(
        (mixed).line_interpolate(distances)
    )


def test_packed_substring_matches_mixed_storage() -> None:
    packed, mixed = line_storage_twins()
    assert canon((packed).line_substring(0.2, 0.8)) == canon(
        (mixed).line_substring(0.2, 0.8)
    )
    assert canon(
        (packed).line_substring([0.1, 0.2], [0.5, 0.6], normalized=True)
    ) == canon((mixed).line_substring([0.1, 0.2], [0.5, 0.6], normalized=True))


def test_packed_interpolate_per_row_distance_param() -> None:
    lines = _long_packed_lines()
    scalar = (lines).line_interpolate(4.0)
    per_row = (lines).line_interpolate([4.0, 4.0])
    assert canon(scalar) == canon(per_row)
    varied = (lines).line_interpolate([2.0, 6.0])
    assert [p.x for p in varied] == [2.0, 0.0]
    assert [p.y for p in varied] == [0.0, 6.0]


def test_packed_gathered_nullable_interpolation_keeps_logical_rows() -> None:
    rows = gm.GeometryArray([
        gm.LineString([(0, 0), (10, 0)]),
        gm.LineString([(0, 10), (0, 18)]),
        None,
        gm.LineString([(20, 0), (24, 0)]),
    ])
    selected = rows[[3, 0, 2, 3]]

    groups = selected.line_interpolate(count=2)

    assert [
        None if group is None else [(point.x, point.y) for point in group]
        for group in groups
    ] == [
        [(20.0, 0.0), (24.0, 0.0)],
        [(0.0, 0.0), (10.0, 0.0)],
        None,
        [(20.0, 0.0), (24.0, 0.0)],
    ]


def test_packed_substring_per_row_distance_params() -> None:
    lines = _long_packed_lines()
    scalar = (lines).line_substring(1.0, 3.0)
    per_row = (lines).line_substring([1.0, 1.0], [3.0, 3.0])
    assert canon(scalar) == canon(per_row)


def test_packed_geographic_interpolate_matches_mixed_storage() -> None:
    packed, mixed = geo_line_storage_twins()
    distances = [100000.0, 200000.0]
    assert canon((packed).line_interpolate(distances)) == canon(
        (mixed).line_interpolate(distances)
    )


def test_packed_geographic_substring_matches_mixed_storage() -> None:
    packed, mixed = geo_line_storage_twins()
    assert canon((packed).line_substring(50000.0, 150000.0)) == canon(
        (mixed).line_substring(50000.0, 150000.0)
    )
    assert canon(
        (packed).line_substring([0.1, 0.2], [0.5, 0.6], normalized=True)
    ) == canon((mixed).line_substring([0.1, 0.2], [0.5, 0.6], normalized=True))


def test_packed_geographic_locate_matches_mixed_storage() -> None:
    packed, mixed = geo_line_storage_twins()
    queries = gm.GeometryArray([
        gm.Point(0.0, 5.0, crs=4326),
        gm.Point(5.0, 0.0, crs=4326),
    ])
    assert list((packed).line_locate(queries)) == list((mixed).line_locate(queries))


def test_packed_and_mixed_chart_lrs_match_fixed_sloped_edge_oracles() -> None:
    line = gm.LineString([(0.0, 10.0), (120.0, 60.0)], crs=4326)
    packed = gm.GeometryArray([line, line])
    mixed = gm.from_wkt([line.to_wkt(), line.to_wkt()], crs=4326)
    distances = [3_002_430.3752147846, 9_007_291.125644354]
    expected = [
        (25.725961510550192, 20.719150629395913),
        (83.51027086740391, 44.79594619475163),
    ]
    coordinate_budgets = [
        (2.45731802264673e-13, 1.10677916194148e-13),
        (2.88364366410279e-13, 1.24888770909350e-13),
    ]
    station_budgets_m = (2.61104038767807e-8, 3.54020085158243e-8)
    substring_budget_m = 4.091897092081309e-8

    for lines in (packed, mixed):
        points = lines.line_interpolate(distances)
        for point, coordinate, budget in zip(
            points, expected, coordinate_budgets, strict=True
        ):
            assert abs(point.x - coordinate[0]) <= budget[0]
            assert abs(point.y - coordinate[1]) <= budget[1]
        substrings = lines.line_substring(
            [distances[0], distances[0]],
            [distances[1], distances[1]],
        )
        for substring in substrings:
            for point, coordinate, budget in zip(
                substring.coords, expected, coordinate_budgets, strict=True
            ):
                assert abs(point[0] - coordinate[0]) <= budget[0]
                assert abs(point[1] - coordinate[1]) <= budget[1]
            assert substring.length == pytest.approx(
                distances[1] - distances[0],
                rel=0.0,
                abs=substring_budget_m,
            )
        queries = gm.GeometryArray(
            [gm.Point(*coordinate, crs=4326) for coordinate in expected],
            crs=4326,
        )
        for actual, expected_distance, budget in zip(
            lines.line_locate(queries), distances, station_budgets_m, strict=True
        ):
            assert abs(actual - expected_distance) <= budget


def test_packed_geographic_interpolate_exports_geoarrow_point() -> None:
    packed, _ = geo_line_storage_twins()
    assert packed.to_arrow().type.extension_name == 'geoarrow.linestring'
    out = (packed).line_interpolate([100000.0, 200000.0])
    assert out.to_arrow().type.extension_name == 'geoarrow.point'


def test_packed_geographic_substring_exports_geoarrow_linestring() -> None:
    packed, _ = geo_line_storage_twins()
    out = (packed).line_substring(50000.0, 150000.0)
    assert out.to_arrow().type.extension_name == 'geoarrow.linestring'


def test_geographic_chart_lrs_handles_packed_slices_gathers_and_missing_rows() -> None:
    rows = gm.GeometryArray([
        gm.LineString([(0.0, 60.0), (10.0, 60.0)], crs=4326),
        gm.LineString([(370.0, 20.0), (376.0, 20.0)], crs=4326),
        None,
        gm.LineString([(179.0, 10.0), (-179.0, 10.0)], crs=4326),
    ])
    assert rows.to_arrow().type.extension_name == 'geoarrow.linestring'

    cases = [
        (
            rows[1:4],
            [(13.0, 20.0), None, (-180.0, 10.0)],
            [
                [(371.5, 20.0), (374.5, 20.0)],
                None,
                [(179.5, 10.0), (180.5, 10.0)],
            ],
            [
                (_WGS84_PARALLEL_20_6_M, _WGS84_PARALLEL_20_6_STATION_BUDGET_M),
                None,
                (_WGS84_PARALLEL_10_2_M, 1.32e-8),
            ],
            [
                _parallel_xy_budget(370.0, 13.0, 20.0, 6.0),
                None,
                _parallel_xy_budget(179.0, -180.0, 10.0, 2.0),
            ],
            [
                [
                    _parallel_xy_budget(370.0, 371.5, 20.0, 6.0),
                    _parallel_xy_budget(370.0, 374.5, 20.0, 6.0),
                ],
                None,
                [
                    _parallel_xy_budget(179.0, 179.5, 10.0, 2.0),
                    _parallel_xy_budget(179.0, 180.5, 10.0, 2.0),
                ],
            ],
        ),
        (
            rows[[3, 0, 2, 3]],
            [(-180.0, 10.0), (5.0, 60.0), None, (-180.0, 10.0)],
            [
                [(179.5, 10.0), (180.5, 10.0)],
                [(2.5, 60.0), (7.5, 60.0)],
                None,
                [(179.5, 10.0), (180.5, 10.0)],
            ],
            [
                (_WGS84_PARALLEL_10_2_M, 1.32e-8),
                (_WGS84_PARALLEL_60_10_M, 4.6e-9),
                None,
                (_WGS84_PARALLEL_10_2_M, 1.32e-8),
            ],
            [
                _parallel_xy_budget(179.0, -180.0, 10.0, 2.0),
                _parallel_xy_budget(0.0, 5.0, 60.0, 10.0),
                None,
                _parallel_xy_budget(179.0, -180.0, 10.0, 2.0),
            ],
            [
                [
                    _parallel_xy_budget(179.0, 179.5, 10.0, 2.0),
                    _parallel_xy_budget(179.0, 180.5, 10.0, 2.0),
                ],
                [
                    _parallel_xy_budget(0.0, 2.5, 60.0, 10.0),
                    _parallel_xy_budget(0.0, 7.5, 60.0, 10.0),
                ],
                None,
                [
                    _parallel_xy_budget(179.0, 179.5, 10.0, 2.0),
                    _parallel_xy_budget(179.0, 180.5, 10.0, 2.0),
                ],
            ],
        ),
    ]
    for (
        selected,
        expected_points,
        expected_substrings,
        station_rows,
        point_budgets,
        substring_budgets,
    ) in cases:
        fractions = [0.5] * len(selected)
        points = selected.line_interpolate(fractions, normalized=True)
        for point, expected, budget in zip(
            points, expected_points, point_budgets, strict=True
        ):
            if expected is None:
                assert point is None
            else:
                assert point is not None
                assert abs(point.x - expected[0]) <= budget[0]
                assert abs(point.y - expected[1]) <= budget[1]

        substrings = selected.line_substring(
            [0.25] * len(selected), [0.75] * len(selected), normalized=True
        )
        for substring, expected, budgets in zip(
            substrings, expected_substrings, substring_budgets, strict=True
        ):
            if expected is None:
                assert substring is None
                continue
            for point, expected_point, budget in zip(
                substring.coords, expected, budgets, strict=True
            ):
                assert abs(point[0] - expected_point[0]) <= budget[0]
                assert abs(point[1] - expected_point[1]) <= budget[1]

        located = selected.line_locate(points, normalized=True)
        for row, (expected, station_row) in enumerate(
            zip(expected_points, station_rows, strict=True)
        ):
            if expected is None:
                assert math.isnan(located[row])
            else:
                length, budget = station_row
                assert abs(located[row] * length - length / 2.0) <= budget
