"""Packed subdivide — ST_Subdivide parity on Lines/Polygons storage twins."""

from __future__ import annotations

import gometry as gm

from tests._support import canon, polygon_storage_twins


def _dense_line_twins() -> tuple[gm.GeometryArray, gm.GeometryArray]:
    coords = [(float(i), float(i % 3)) for i in range(24)]
    packed = gm.GeometryArray([
        gm.LineString(coords, crs=3857),
        gm.LineString([(x + 10.0, y) for x, y in coords], crs=3857),
    ])
    mixed = gm.from_wkt(
        [
            'LINESTRING (' + ', '.join((f'{x} {y}' for x, y in coords)) + ')',
            'LINESTRING (' + ', '.join((f'{x + 10.0} {y}' for x, y in coords)) + ')',
        ],
        crs=3857,
    )
    return (packed, mixed)


def test_packed_lines_subdivide_matches_mixed_storage() -> None:
    packed, mixed = _dense_line_twins()
    assert canon((packed).subdivide(max_vertices=8)) == canon(
        (mixed).subdivide(max_vertices=8)
    )


def test_packed_polygons_subdivide_matches_mixed_storage() -> None:
    packed, mixed = polygon_storage_twins()
    dense = (gm.Point(0.5, 0.5, crs=3857)).buffer(0.4, quadrant_segments=32)
    packed_dense = gm.GeometryArray([dense, gm.box(1.1, 1.1, 1.9, 1.9, crs=3857)])
    mixed_dense = gm.from_wkt(
        [(dense).to_wkt(), 'POLYGON ((1.1 1.1, 1.9 1.1, 1.9 1.9, 1.1 1.9, 1.1 1.1))'],
        crs=3857,
    )
    assert canon((packed_dense).subdivide(max_vertices=16)) == canon(
        (mixed_dense).subdivide(max_vertices=16)
    )
    assert canon((packed).subdivide(max_vertices=64)) == canon(
        (mixed).subdivide(max_vertices=64)
    )
