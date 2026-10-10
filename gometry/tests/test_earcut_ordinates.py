"""Earcut keeps source-vertex Z/M instead of XY recarry."""

from __future__ import annotations

import gometry as gm


def _corner_xyz(triangle: gm.Polygon) -> list[tuple[float, float, float | None]]:
    return [
        (float(x), float(y), None if z is None else float(z))
        for x, y, *rest in triangle.exterior.coords[:3]
        for z in [rest[0] if rest else None]
    ]


def test_earcut_keeps_first_duplicate_xy_z() -> None:
    poly = gm.Polygon([
        (0.0, 0.0, 1.0),
        (0.0, 0.0, 2.0),
        (1.0, 0.0, 3.0),
        (0.0, 1.0, 4.0),
    ])
    triangles = poly.triangulate(method='earcut')
    assert len(triangles) > 0
    origins = [
        z
        for triangle in triangles
        for x, y, z in _corner_xyz(triangle)
        if x == 0.0 and y == 0.0
    ]
    assert origins
    assert all(z == 1.0 for z in origins)


def test_earcut_keeps_first_duplicate_xy_m() -> None:
    poly = gm.from_wkt('POLYGON M ((0 0 1, 0 0 2, 1 0 3, 0 1 4, 0 0 1))')
    triangles = poly.triangulate(method='earcut')
    assert len(triangles) > 0
    assert all(triangle.coordinate_axes == 'XYM' for triangle in triangles)
    origins = [
        coords[2]
        for triangle in triangles
        for coords in triangle.exterior.coords[:3]
        if coords[0] == 0.0 and coords[1] == 0.0
    ]
    assert origins
    assert all(m == 1.0 for m in origins)


def test_earcut_multipolygon_parts_keep_distinct_z() -> None:
    left = gm.Polygon([(0.0, 0.0, 1.0), (1.0, 0.0, 1.0), (0.0, 1.0, 1.0)])
    right = gm.Polygon([(0.0, 0.0, 9.0), (2.0, 0.0, 9.0), (0.0, 2.0, 9.0)])
    triangles = gm.MultiPolygon([left, right]).triangulate(method='earcut')
    zs = [
        z
        for triangle in triangles
        for x, y, z in _corner_xyz(triangle)
        if x == 0.0 and y == 0.0
    ]
    assert 1.0 in zs
    assert 9.0 in zs


def test_earcut_donut_bridge_keeps_source_z() -> None:
    poly = gm.Polygon(
        [(0.0, 0.0, 1.0), (4.0, 0.0, 2.0), (4.0, 4.0, 3.0), (0.0, 4.0, 4.0)],
        holes=[[(1.0, 1.0, 5.0), (2.0, 1.0, 6.0), (1.0, 2.0, 7.0)]],
    )
    source = {
        (float(x), float(y), float(z))
        for ring in [poly.exterior, *poly.interiors]
        for x, y, z, *_ in ring.coords
    }
    triangles = poly.triangulate(method='earcut')
    assert len(triangles) > 0
    for triangle in triangles:
        assert triangle.coordinate_axes == 'XYZ'
        for x, y, z in _corner_xyz(triangle):
            assert (x, y, z) in source


def test_earcut_array_mixed_rows_keep_per_row_axes() -> None:
    xy_poly = gm.Polygon([(0.0, 0.0), (2.0, 0.0), (0.0, 2.0)])
    xyz_poly = gm.Polygon([(0.0, 0.0, 1.0), (2.0, 0.0, 2.0), (0.0, 2.0, 3.0)])
    groups = gm.GeometryArray([xy_poly, xyz_poly]).triangulate(method='earcut')
    assert all(triangle.coordinate_axes == 'XY' for triangle in groups[0])
    assert all(triangle.coordinate_axes == 'XYZ' for triangle in groups[1])
    assert len(groups[0]) > 0
    assert len(groups[1]) > 0


def test_earcut_missing_and_empty_groups() -> None:
    poly = gm.Polygon([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)])
    groups = gm.GeometryArray([poly, None]).triangulate(method='earcut')
    assert len(groups[0]) > 0
    assert groups[1] is None
    empty = gm.from_wkt('POLYGON EMPTY').triangulate(method='earcut')
    assert len(empty) == 0
