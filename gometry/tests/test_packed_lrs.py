"""Packed LRS — column-direct interpolate/substring/locate on Lines storage."""

from __future__ import annotations

import gometry as gm

from tests._support import canon, geo_line_storage_twins, line_storage_twins


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


def test_packed_substring_per_row_distance_params() -> None:
    lines = _long_packed_lines()
    scalar = (lines).line_substring(1.0, 3.0)
    per_row = (lines).line_substring([1.0, 1.0], [3.0, 3.0])
    assert canon(scalar) == canon(per_row)


def test_packed_geodesic_interpolate_matches_mixed_storage() -> None:
    packed, mixed = geo_line_storage_twins()
    distances = [100000.0, 200000.0]
    assert canon((packed).line_interpolate(distances)) == canon(
        (mixed).line_interpolate(distances)
    )


def test_packed_geodesic_substring_matches_mixed_storage() -> None:
    packed, mixed = geo_line_storage_twins()
    assert canon((packed).line_substring(50000.0, 150000.0)) == canon(
        (mixed).line_substring(50000.0, 150000.0)
    )
    assert canon(
        (packed).line_substring([0.1, 0.2], [0.5, 0.6], normalized=True)
    ) == canon((mixed).line_substring([0.1, 0.2], [0.5, 0.6], normalized=True))


def test_packed_geodesic_locate_matches_mixed_storage() -> None:
    packed, mixed = geo_line_storage_twins()
    queries = gm.GeometryArray([
        gm.Point(0.0, 5.0, crs=4326),
        gm.Point(5.0, 0.0, crs=4326),
    ])
    assert list((packed).line_locate(queries)) == list((mixed).line_locate(queries))


def test_packed_geodesic_interpolate_exports_geoarrow_point() -> None:
    packed, _ = geo_line_storage_twins()
    assert packed.to_arrow().type.extension_name == 'geoarrow.linestring'
    out = (packed).line_interpolate([100000.0, 200000.0])
    assert out.to_arrow().type.extension_name == 'geoarrow.point'


def test_packed_geodesic_substring_exports_geoarrow_linestring() -> None:
    packed, _ = geo_line_storage_twins()
    out = (packed).line_substring(50000.0, 150000.0)
    assert out.to_arrow().type.extension_name == 'geoarrow.linestring'
