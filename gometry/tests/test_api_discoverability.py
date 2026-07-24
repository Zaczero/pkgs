"""API discoverability and consistency contracts."""

from __future__ import annotations

import gometry as gm
import pytest


def test_crosses_antimeridian_is_property() -> None:
    line = gm.LineString([(170.0, 0.0), (-170.0, 0.0)], crs=4326)
    assert line.crosses_antimeridian is True
    with pytest.raises(TypeError):
        line.crosses_antimeridian()
    arr = gm.GeometryArray([line], crs=4326)
    assert arr.crosses_antimeridian.tolist() == [True]


def test_array_hausdorff_frechet_densify_per_row() -> None:
    left = gm.GeometryArray(
        [
            gm.LineString([(0, 0), (10, 0)], crs=3857),
            gm.LineString([(0, 0), (0, 10)], crs=3857),
        ],
        crs=3857,
    )
    right = gm.LineString([(0, 1), (10, 1)], crs=3857)
    expected_h = [
        gm.hausdorff_distance(left[0], right, densify=0.5),
        gm.hausdorff_distance(left[1], right, densify=1.0),
    ]
    expected_f = [
        gm.frechet_distance(left[0], right, densify=0.5),
        gm.frechet_distance(left[1], right, densify=1.0),
    ]
    assert gm.hausdorff_distance(left, right, densify=[0.5, 1.0]) == pytest.approx(
        expected_h
    )
    assert gm.frechet_distance(left, right, densify=[0.5, 1.0]) == pytest.approx(
        expected_f
    )


def test_cell_children_neighbors_return_cell_array() -> None:
    h3 = gm.H3Cell(13.4, 52.5, resolution=7)
    children = h3.children(8)
    assert type(children).__name__ == 'CellArray'
    assert len(children) == 7
    assert type(children[0]).__name__ == 'H3Cell'
    neighbors = h3.neighbors
    assert type(neighbors).__name__ == 'CellArray'
    assert all(type(n).__name__ == 'H3Cell' for n in neighbors)


def test_geometry_dir_lists_only_real_members() -> None:
    pt = gm.Point(0, 0)
    names = dir(pt)
    assert 'buffer' in names
    assert callable(type(pt).buffer)
    assert 'distance' not in names
    assert 'intersects' not in names


def test_geometry_array_dir_lists_only_real_members() -> None:
    arr = gm.points([0, 1], [0, 1])
    names = dir(arr)
    assert 'buffer' in names
    assert callable(type(arr).buffer)
    assert 'dwithin' not in names
