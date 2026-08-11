"""R20-B canonical shared-vertex Voronoi regressions."""

from __future__ import annotations

from fractions import Fraction
from itertools import combinations

import gometry as gm
import pytest

SCALES = [
    1e6,
    1e12,
    1e13,
    1e14,
    1e15,
    1e16,
    1e18,
    4e153,
    6.35e307,
    1.7e308,
]


def _carrier(points: list[tuple[float, float]], kind: str) -> gm.Geometry:
    sites = gm.MultiPoint(points)
    return sites if kind == 'multipart' else gm.GeometryCollection([sites])


def _cells(source: gm.Geometry, clip: gm.Polygon, frontend: str) -> gm.GeometryArray:
    if frontend == 'scalar':
        return source.voronoi_polygons(clip=clip)
    return gm.GeometryArray([source]).voronoi_polygons(clip=clip)[0]


def _assert_partition(cells: gm.GeometryArray, clip: gm.Polygon) -> None:
    union = cells.union_all()
    assert gm.difference(clip, union).is_empty
    assert gm.difference(union, clip).is_empty
    assert gm.equals(union, clip)
    assert all(cell.is_valid for cell in cells)
    for left, right in combinations(cells, 2):
        assert gm.relate_pattern(left, right, 'F********'), gm.relate(left, right)


def _near_vertices(cells: gm.GeometryArray) -> set[tuple[float, float]]:
    return {
        tuple(xy)
        for cell in cells
        for xy in cell.coords
        if -2.0 <= xy[0] <= 2.0 and -2.0 <= xy[1] <= 2.0
    }


@pytest.mark.parametrize('half_extent', SCALES)
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
def test_three_site_vertex_is_site_scale_and_shared_across_clip_scale_sweep(
    half_extent: float, frontend: str, carrier: str
) -> None:
    points = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]
    source = _carrier(points, carrier)
    clip = gm.box(-half_extent, -half_extent, half_extent, half_extent)
    cells = _cells(source, clip, frontend)
    midpoint = float(
        (Fraction.from_float(points[0][0]) + Fraction.from_float(points[1][0])) / 2
    )
    observed = _near_vertices(cells)
    assert (midpoint, midpoint) in observed, (half_extent, observed)
    _assert_partition(cells, clip)


@pytest.mark.parametrize('half_extent', SCALES)
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
def test_five_site_circumcenter_is_one_canonical_stored_double(
    half_extent: float, frontend: str, carrier: str
) -> None:
    points = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.5, 0.4)]
    cells = _cells(
        _carrier(points, carrier),
        gm.box(-half_extent, -half_extent, half_extent, half_extent),
        frontend,
    )
    one = Fraction.from_float(points[1][0])
    half = Fraction.from_float(points[4][0])
    center_y = Fraction.from_float(points[2][1]) / 2
    inner_y = Fraction.from_float(points[4][1])
    expected = (float(one - (half - inner_y) ** 2), float(center_y))
    near = {
        xy for xy in _near_vertices(cells) if 0.9 < xy[0] < 1.0 and 0.4 < xy[1] < 0.6
    }
    assert near == {expected}, (half_extent, expected, near)


@pytest.mark.parametrize('offset', [0.0, 1e6, 1e9])
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
def test_five_site_cells_form_a_topological_partition_at_offsets(
    offset: float, frontend: str, carrier: str
) -> None:
    points = [
        (offset + x, y)
        for x, y in ((0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.5, 0.4))
    ]
    clip = gm.box(offset - 1.0, -1.0, offset + 2.0, 2.0)
    _assert_partition(_cells(_carrier(points, carrier), clip, frontend), clip)


@pytest.mark.parametrize('half_extent', [*SCALES, 6.34e307, 6.36e307, 8e307])
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
@pytest.mark.parametrize('collinear, expected_count', [(False, 3), (True, 2)])
def test_extreme_three_site_and_collinear_edges_keep_exact_count(
    half_extent: float,
    frontend: str,
    carrier: str,
    collinear: bool,
    expected_count: int,
) -> None:
    points = (
        [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]
        if collinear
        else [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)]
    )
    source = _carrier(points, carrier)
    clip = gm.box(-half_extent, -half_extent, half_extent, half_extent)
    edges = (
        source.voronoi_edges(clip=clip)
        if frontend == 'scalar'
        else gm.GeometryArray([source]).voronoi_edges(clip=clip)[0]
    )
    assert len(edges) == expected_count, (
        half_extent,
        [list(edge.coords) for edge in edges],
    )


@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
def test_nonrectangular_clip_is_one_shared_noded_partition(
    frontend: str, carrier: str
) -> None:
    points = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.5, 0.4)]
    clip = gm.Polygon([
        (-1.0, -1.0),
        (2.0, -1.0),
        (2.0, 0.75),
        (0.75, 0.75),
        (0.75, 2.0),
        (-1.0, 2.0),
        (-1.0, -1.0),
    ])
    _assert_partition(_cells(_carrier(points, carrier), clip, frontend), clip)
