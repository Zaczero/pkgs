"""R21-B Voronoi/tessellation blocker regressions."""

from __future__ import annotations

import itertools
import random
import sys
from itertools import combinations

import gometry as gm
import pytest


def _partition(cells: gm.GeometryArray, clip: gm.Geometry) -> None:
    union = gm.union_all(cells)
    assert gm.difference(clip, union).is_empty
    assert gm.difference(union, clip).is_empty
    assert gm.equals(union, clip)
    assert all(cell.is_valid for cell in cells)
    for left, right in combinations(cells, 2):
        assert gm.relate_pattern(left, right, 'F********')


@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
def test_cocircular_faces_share_one_vertex_for_every_site_order(
    carrier: str, frontend: str
) -> None:
    sites = [(22.0, -61.0), (62.0, -19.0), (-22.0, 61.0), (-62.0, 19.0)]
    diagrams = set()
    for order in itertools.permutations(sites):
        source: gm.Geometry = gm.MultiPoint(order)
        if carrier == 'collection':
            source = gm.GeometryCollection([source])
        edges = (
            source.voronoi_edges()
            if frontend == 'scalar'
            else gm.GeometryArray([source]).voronoi_edges()[0]
        )
        assert len(edges) == 4
        assert all((0.0, 0.0) in list(edge.coords) for edge in edges)
        diagrams.add(tuple(edge.to_wkt() for edge in edges))
    assert len(diagrams) == 1


@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize(
    'holes',
    [
        [(-1, -1, 1, 1)],  # isolated child face
        [(-12, -1, 12, 1)],  # crossed by multiple Voronoi edges
        [(-6, -6, 6, 6)],  # nested beneath the owning cell face
        [(-13, -13, -8, -8), (8, -13, 13, -8)],  # disconnected children
    ],
    ids=['isolated', 'crossed', 'nested-face', 'disconnected'],
)
def test_shared_arrangement_preserves_clip_holes(
    carrier: str, frontend: str, holes: list[tuple[int, int, int, int]]
) -> None:
    clip: gm.Geometry = gm.box(-20, -20, 20, 20)
    for hole in holes:
        clip = gm.difference(clip, gm.box(*hole))
    source: gm.Geometry = gm.MultiPoint([(-10, -10), (10, -10), (0, 10)])
    if carrier == 'collection':
        source = gm.GeometryCollection([source])
    cells = (
        source.voronoi_polygons(clip=clip)
        if frontend == 'scalar'
        else gm.GeometryArray([source]).voronoi_polygons(clip=clip)[0]
    )
    _partition(cells, clip)


def test_reciprocal_sites_use_actual_nonrectangular_clip() -> None:
    clip = gm.Polygon([(0, 0), (3, 0), (3, 1.5), (1.5, 1.5), (1.5, 3), (0, 3), (0, 0)])
    cells = gm.MultiPoint([(0, 0), (1e150, 0), (0, 1e-150)]).voronoi_polygons(clip=clip)
    _partition(cells, clip)


@pytest.mark.parametrize('site_count', [10, 100, 1_000])
def test_convex_quad_clip_keeps_every_cell_valid_at_site_count(
    site_count: int,
) -> None:
    rng = random.Random(7)
    sites = [
        (rng.uniform(0, 1_000), rng.uniform(0, 1_000)) for _ in range(site_count)
    ]
    clip = gm.Polygon([(50, 50), (950, 120), (900, 900), (120, 850), (50, 50)])
    cells = gm.MultiPoint(sites).voronoi_polygons(clip=clip)
    assert cells
    assert all(cell.is_valid for cell in cells)


def test_tolerance_grid_never_overflows_at_finite_extremes() -> None:
    edges = gm.MultiPoint([
        (-sys.float_info.max, 0),
        (sys.float_info.max, 0),
    ]).voronoi_edges(tolerance=1.0, clip='envelope')
    assert len(edges) == 0


def test_voronoi_tolerance_keeps_strict_radius_boundary_sites() -> None:
    sites = gm.MultiPoint([(0.0, 0.0), (0.0, 1.0)])
    without_tolerance = sites.voronoi_polygons(tolerance=0.0, clip='padded')
    at_boundary = sites.voronoi_polygons(tolerance=1.0, clip='padded')

    assert len(at_boundary) == 2
    assert [cell.to_wkb() for cell in at_boundary] == [
        cell.to_wkb() for cell in without_tolerance
    ]


def test_voronoi_tolerance_uses_exact_distance_for_near_ulp_sites() -> None:
    sites = gm.MultiPoint([(0.0, 1.0), (0.0, 1.0e20)])
    without_tolerance = sites.voronoi_polygons(tolerance=0.0, clip='padded')
    snapped = sites.voronoi_polygons(tolerance=1.0e20, clip='padded')

    assert len(without_tolerance) == 2
    assert len(snapped) == 0


def test_voronoi_tolerance_preserves_4097_equal_x_sparse_y_sites() -> None:
    sites = gm.MultiPoint([(0.0, 2.0 * index) for index in range(4_097)])
    without_tolerance = sites.voronoi_polygons(tolerance=0.0, clip='padded')
    sparse_y = sites.voronoi_polygons(tolerance=1.0, clip='padded')

    assert len(sparse_y) == 4_097
    assert [cell.to_wkb() for cell in sparse_y] == [
        cell.to_wkb() for cell in without_tolerance
    ]


@pytest.mark.parametrize('exponent', [76, 77, 100, 153, 154])
def test_reciprocal_concave_hull_covers_every_input_site(exponent: int) -> None:
    x = 10.0**exponent
    y = 10.0**-exponent
    sites = [(0, 0), (4 * x, 0), (4 * x, 4 * y), (0, 4 * y), (2 * x, 2 * y)]
    hull = gm.MultiPoint(sites).concave_hull(concavity=0)
    assert hull.is_valid
    assert all(gm.covers(hull, gm.Point(*site)) for site in sites)
    if exponent == 100:
        assert hull.area == 12.0


@pytest.mark.parametrize('exponent', [*range(150, 161), *range(-160, -149)])
def test_uniform_scale_voronoi_partition_has_no_arrangement_gap(exponent: int) -> None:
    side = 10.0**exponent
    sites = [(0, 0), (side, 0), (side, side), (0, side), (side / 2, side / 2)]
    envelope = gm.box(0, 0, side, side)
    cells = gm.MultiPoint(sites).voronoi_polygons(clip='envelope')
    merged = gm.union_all(cells)
    assert gm.covers(merged, envelope)
    assert gm.covers(envelope, merged)
    assert gm.difference(envelope, merged).is_empty
    for left, right in combinations(cells, 2):
        assert gm.relate_pattern(left, right, 'F********')


@pytest.mark.parametrize(
    ('sites', 'expected'),
    [
        (
            'MULTIPOINT ((0 0), (4 0))',
            [
                'POLYGON ((-2 -2, 2 -2, 2 2, -2 2, -2 -2))',
                'POLYGON ((2 -2, 6 -2, 6 2, 2 2, 2 -2))',
            ],
        ),
        (
            'MULTIPOINT ((0 0), (1 0), (5 0))',
            [
                'POLYGON ((-2.5 -2.5, 0.5 -2.5, 0.5 2.5, -2.5 2.5, -2.5 -2.5))',
                'POLYGON ((0.5 -2.5, 3 -2.5, 3 2.5, 0.5 2.5, 0.5 -2.5))',
                'POLYGON ((3 -2.5, 7.5 -2.5, 7.5 2.5, 3 2.5, 3 -2.5))',
            ],
        ),
        (
            'LINESTRING (0 0, 3 4)',
            [
                'POLYGON ((-2 -2, 5 -2, 5 -0.625, -2 4.625, -2 -2))',
                'POLYGON ((-2 4.625, 5 -0.625, 5 6, -2 6, -2 4.625))',
            ],
        ),
    ],
)
def test_collinear_sites_split_the_frame_at_their_bisectors(
    sites: str, expected: list[str]
) -> None:
    # The cells are the strips between consecutive perpendicular bisectors
    # (x = 2; x = 0.5 and x = 3; the line 3x + 4y = 12.5), cut from the padded
    # frame of half the larger span.
    cells = gm.from_wkt(sites).voronoi_polygons()
    assert cells.to_wkt() == expected
    frame = gm.union_all(cells)
    assert frame.area == sum(cell.area for cell in cells)
    assert gm.equals(frame, frame.envelope())
    for site in gm.from_wkt(sites).unique_points().parts:
        assert sum(gm.covers(cell, site) for cell in cells) == 1


def test_collinear_sites_follow_the_envelope_and_polygon_clips() -> None:
    diagonal = gm.MultiPoint([(0, 0), (4, 2)])
    assert diagonal.voronoi_polygons(clip='envelope').to_wkt() == [
        'POLYGON ((0 0, 2.5 0, 1.5 2, 0 2, 0 0))',
        'POLYGON ((1.5 2, 2.5 0, 4 0, 4 2, 1.5 2))',
    ]
    # A horizontal site row has a flat envelope, which bounds no area.
    assert len(gm.MultiPoint([(0, 0), (4, 0)]).voronoi_polygons(clip='envelope')) == 0
    holed = gm.from_wkt(
        'POLYGON ((-2 -2, 6 -2, 6 2, -2 2, -2 -2), (1.5 -1, 2.5 -1, 2.5 1, 1.5 1, 1.5 -1))'
    )
    cells = gm.MultiPoint([(0, 0), (4, 0)]).voronoi_polygons(clip=holed)
    assert [cell.area for cell in cells] == [15.0, 15.0]
    assert gm.equals(gm.union_all(cells), holed)


def test_a_lone_site_owns_the_whole_clip_polygon() -> None:
    clip = gm.box(0, 0, 3, 3)
    assert gm.Point(1, 1).voronoi_polygons(clip=clip).to_wkt() == [clip.to_wkt()]
    # The padded and envelope frames of one point have no area.
    assert len(gm.Point(1, 1).voronoi_polygons()) == 0
    assert len(gm.Point(1, 1).voronoi_polygons(clip='envelope')) == 0
