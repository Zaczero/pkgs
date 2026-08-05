"""Packed ``num_geometries`` — O(1) CSR paths on Lines/Polygons."""

from __future__ import annotations

import gometry as gm

from tests._support import ints, line_storage_twins, polygon_storage_twins


def test_packed_lines_num_geometries_matches_mixed_storage() -> None:
    packed, mixed = line_storage_twins()
    assert ints(packed.num_geometries) == [1, 1]
    assert ints(packed.num_geometries) == ints(mixed.num_geometries)


def test_packed_polygons_num_geometries_matches_mixed_storage() -> None:
    packed, mixed = polygon_storage_twins()
    assert ints(packed.num_geometries) == [1, 1]
    assert ints(packed.num_geometries) == ints(mixed.num_geometries)


def test_packed_num_geometries_matches_per_row_scalar() -> None:
    packed, _ = polygon_storage_twins()
    assert ints(packed.num_geometries) == [geom.num_geometries for geom in list(packed)]
    holey = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4)],
        holes=[[(1, 1), (3, 1), (3, 3), (1, 3)]],
        crs=3857,
    )
    array = gm.GeometryArray([holey])
    assert ints(array.num_geometries) == [holey.num_geometries] == [1]
    multi = gm.MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]], crs=3857)
    mixed = gm.from_wkt([(multi).to_wkt()], crs=3857)
    assert ints(mixed.num_geometries) == [2]
    assert mixed[0].num_geometries == 2
