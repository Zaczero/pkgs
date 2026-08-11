"""Certified-construction and materialization guards for Voronoi Pass C."""

from __future__ import annotations

import itertools
import math
import random
from itertools import combinations

import gometry as gm
import numpy as np
import pytest

from tests.test_voronoi_shared_vertices import _assert_partition
from tests.test_voronoi_tessellation import _partition


def _carrier(points: tuple[tuple[float, float], ...], carrier: str) -> gm.Geometry:
    source: gm.Geometry = gm.MultiPoint(points)
    return gm.GeometryCollection([source]) if carrier == 'collection' else source


def _edges(
    source: gm.Geometry, clip: gm.Geometry | str, frontend: str
) -> gm.GeometryArray:
    if frontend == 'packed':
        return gm.GeometryArray([source]).voronoi_edges(clip=clip)[0]
    return source.voronoi_edges(clip=clip)


def _cells(source: gm.Geometry, clip: gm.Geometry, frontend: str) -> gm.GeometryArray:
    if frontend == 'packed':
        return gm.GeometryArray([source]).voronoi_polygons(clip=clip)[0]
    return source.voronoi_polygons(clip=clip)


def _normalized_edges(
    edges: gm.GeometryArray,
) -> tuple[tuple[tuple[float, float], ...], ...]:
    normalized = []
    for edge in edges:
        coordinates = tuple(tuple(xy) for xy in edge.coords)
        normalized.append(min(coordinates, tuple(reversed(coordinates))))
    return tuple(sorted(normalized))


def test_combinatorial_dual_is_byte_identical_to_live_reference(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def outcome(points: list[tuple[float, float]], clip: str) -> tuple[str, object]:
        try:
            source = gm.MultiPoint(points)
            return (
                'ok',
                tuple(cell.to_wkb() for cell in source.voronoi_polygons(clip=clip)),
                tuple(edge.to_wkb() for edge in source.voronoi_edges(clip=clip)),
            )
        except gm.InvalidGeometryError as error:
            return ('error', str(error))

    cases: list[tuple[list[tuple[float, float]], str]] = []
    for count in (7, 20, 60, 137, 400, 1_000):
        for seed in (0, 1, 2):
            points = (np.random.default_rng(seed).random((count, 2)) * 1_000.0).tolist()
            cases.extend((points, clip) for clip in ('envelope', 'padded'))
    grid12 = [(float(x), float(y)) for x in range(12) for y in range(12)]
    grid30 = [(float(x), float(y)) for x in range(30) for y in range(30)]
    cocircular13 = [
        (math.cos(index * math.pi / 6) * 100, math.sin(index * math.pi / 6) * 100)
        for index in range(12)
    ] + [(0.0, 0.0)]
    cocircular41 = [
        (
            math.cos(index * 2 * math.pi / 40) * 100,
            math.sin(index * 2 * math.pi / 40) * 100,
        )
        for index in range(40)
    ] + [(0.0, 0.0)]
    cases.extend([
        (grid12, 'envelope'),
        (grid12, 'padded'),
        (grid30, 'envelope'),
        (cocircular13, 'envelope'),
        (cocircular41, 'envelope'),
        (
            [
                (0.0, 0.0),
                (1.7e308, 0.0),
                (0.0, 1.7e308),
                (-1.7e308, -1.0),
                (3.0, 7.0),
            ],
            'envelope',
        ),
        (
            [
                (0.0, 0.0),
                (5e-324, 0.0),
                (0.0, 5e-324),
                (1e-300, 1e-300),
                (2e-300, 3e-300),
            ],
            'envelope',
        ),
    ])
    for count in (1_000, 2_000):
        for seed in (0, 1):
            points = (np.random.default_rng(seed).random((count, 2)) * 1_000.0).tolist()
            cases.extend((points, clip) for clip in ('envelope', 'padded'))

    for points, clip in cases:
        monkeypatch.setenv('GOMETRY_VORO_REFERENCE', '1')
        reference = outcome(points, clip)
        monkeypatch.delenv('GOMETRY_VORO_REFERENCE')
        dual = outcome(points, clip)
        assert dual == reference, (len(points), clip, points[:3])

    families = [
        tuple((np.random.default_rng(9).random((5, 2)) * 100).tolist()),
        tuple(
            (math.cos(index * math.pi / 2) * 10, math.sin(index * math.pi / 2) * 10)
            for index in range(4)
        ),
        ((0.0, 0.0), (0.09, 0.0), (0.18, 0.0), (0.0, 1.0), (1.0, 0.0)),
    ]
    for family in families:
        orders = itertools.permutations(family)
        monkeypatch.setenv('GOMETRY_VORO_REFERENCE', '1')
        reference = tuple(outcome(list(order), 'envelope') for order in orders)
        monkeypatch.delenv('GOMETRY_VORO_REFERENCE')
        dual = tuple(
            outcome(list(order), 'envelope') for order in itertools.permutations(family)
        )
        assert dual == reference


def _assert_topological_partition(
    cells: gm.GeometryArray,
    clip: gm.Geometry,
    *,
    active_sites: tuple[tuple[float, float], ...],
    require_exact_equality: bool,
) -> None:
    """Extend the R20/R21 helpers with spill, validity, and completeness."""
    if require_exact_equality:
        _assert_partition(cells, clip)
    else:
        union = cells.union_all()
        assert gm.difference(clip, union).is_empty
        assert gm.difference(union, clip).is_empty
        assert all(cell.is_valid for cell in cells)
        for left, right in combinations(cells, 2):
            assert gm.relate_pattern(left, right, 'F********'), gm.relate(left, right)
    assert len(cells) == len(active_sites)
    assert all(
        sum(gm.covers(cell, gm.Point(*site)) for cell in cells) == 1
        for site in active_sites
    )


@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
def test_noncocircular_near_diagram_is_exact_and_permutation_invariant(
    carrier: str, frontend: str
) -> None:
    sites = (
        (22.0, -61.0),
        (62.0, -19.0),
        (-22.0, 61.0),
        (-62.0, math.nextafter(19.0, math.inf)),
    )
    center = (-1.2240175843429788e-15, -4.414489648450088e-16)
    expected = tuple(
        sorted((
            ((-62.0, 59.04761904761905), center),
            ((-58.0952380952381, -61.0), center),
            (center, (0.0, 0.0)),
            ((0.0, 0.0), (58.095238095238095, 61.0)),
            ((0.0, 0.0), (62.0, -59.04761904761905)),
        ))
    )
    observed = {
        _normalized_edges(_edges(_carrier(order, carrier), 'envelope', frontend))
        for order in itertools.permutations(sites)
    }
    assert observed == {expected}


def test_tolerance_clustering_is_permutation_invariant() -> None:
    sites = ((0.0, 0.0), (0.09, 0.0), (0.18, 0.0), (0.0, 1.0), (1.0, 0.0))
    clip = gm.box(-1, -1, 2, 2)
    diagrams = {
        _normalized_edges(gm.MultiPoint(order).voronoi_edges(tolerance=0.1, clip=clip))
        for order in itertools.permutations(sites)
    }
    assert len(diagrams) == 1


def test_ordinary_five_site_cells_are_valid_and_permutation_invariant() -> None:
    sites = ((0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.5, 0.4))
    clip = gm.box(-1, -1, 2, 2)
    diagrams = set()
    for order in itertools.permutations(sites):
        cells = gm.MultiPoint(order).voronoi_polygons(clip=clip)
        _assert_topological_partition(
            cells,
            clip,
            active_sites=sites,
            require_exact_equality=True,
        )
        assert all(cell.is_valid for cell in cells)
        diagrams.add(tuple(sorted(cell.to_wkt() for cell in cells)))
    assert len(diagrams) == 1


@pytest.mark.parametrize(
    'reverse',
    [
        False,
        True,
    ],
)
@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
def test_mixed_outer_and_unit_sites_keep_two_active_cells(
    reverse: bool, carrier: str, frontend: str
) -> None:
    outer = [(-1e20, -1e20), (1e20, -1e20), (1e20, 1e20), (-1e20, 1e20)]
    sites = [*outer, (0.0, 0.0), (1.0, 0.0)]
    if reverse:
        sites.reverse()
    source = _carrier(tuple(sites), carrier)
    clip = gm.box(-2, -2, 2, 2)
    cells = _cells(source, clip, frontend)
    assert len(cells) == 2
    _assert_partition(cells, clip)
    assert _normalized_edges(_edges(source, clip, frontend)) == (
        ((0.5, -2.0), (0.5, 2.0)),
    )


@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
def test_sloped_clip_has_one_valid_topological_snap_partition_for_every_order(
    carrier: str, frontend: str
) -> None:
    sites = ((0.0, -0.2), (0.6, -0.2), (0.3, -0.8))
    clip = gm.Polygon([
        (0.0, 0.0),
        (1.0, 1.0 / 3.0),
        (1.0, -1.0),
        (0.0, -1.0),
        (0.0, 0.0),
    ])
    signatures = set()
    for order in itertools.permutations(sites):
        cells = _cells(_carrier(order, carrier), clip, frontend)
        _assert_topological_partition(
            cells, clip, active_sites=sites, require_exact_equality=False
        )
        signatures.add(tuple(sorted(cell.to_wkt() for cell in cells)))
    assert len(signatures) == 1


@pytest.mark.parametrize('carrier', ['multipart', 'collection'])
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize(
    ('sites', 'clip', 'probes', 'expected_edges'),
    [
        (
            ((0.0, 0.0), (1.0, 0.0), (0.0, 1.0)),
            gm.box(-1e308, -1e308, 1e308, 1e308),
            ((0.0, 0.0), (1.0, 0.0), (0.0, 1.0)),
            (
                ((-1e308, 0.5), (0.5, 0.5)),
                ((0.5, -1e308), (0.5, 0.5)),
                ((0.5, 0.5), (1e308, 1e308)),
            ),
        ),
        (
            ((-1e200, 0.0), (1e200, 0.0), (0.0, 1e200)),
            gm.box(-1, -1, 1, 1),
            ((-0.5, 0.0), (0.5, 0.0), (0.0, 0.5)),
            (
                ((-1.0, 1.0), (0.0, 0.0)),
                ((0.0, -1.0), (0.0, 0.0)),
                ((0.0, 0.0), (1.0, 1.0)),
            ),
        ),
    ],
    ids=['huge-clip', 'huge-sites'],
)
def test_mixed_magnitude_partition_is_complete(
    sites: tuple[tuple[float, float], ...],
    clip: gm.Geometry,
    probes: tuple[tuple[float, float], ...],
    expected_edges: tuple[tuple[tuple[float, float], tuple[float, float]], ...],
    carrier: str,
    frontend: str,
) -> None:
    source = _carrier(sites, carrier)
    cells = _cells(source, clip, frontend)
    _assert_topological_partition(
        cells, clip, active_sites=probes, require_exact_equality=True
    )
    assert _normalized_edges(_edges(source, clip, frontend)) == tuple(
        sorted(expected_edges)
    )


def test_cocircular_diagram_keeps_one_vertex_for_all_orders() -> None:
    sites = ((22.0, -61.0), (62.0, -19.0), (-22.0, 61.0), (-62.0, 19.0))
    diagrams = set()
    for order in itertools.permutations(sites):
        edges = gm.MultiPoint(order).voronoi_edges()
        assert len(edges) == 4
        assert all((0.0, 0.0) in list(edge.coords) for edge in edges)
        diagrams.add(_normalized_edges(edges))
    assert len(diagrams) == 1


def test_hole_and_l_clip_guards_use_full_topology_oracle() -> None:
    clip = gm.difference(gm.box(-20, -20, 20, 20), gm.box(-1, -1, 1, 1))
    cells = gm.MultiPoint([(-10, -10), (10, -10), (0, 10)]).voronoi_polygons(clip=clip)
    _partition(cells, clip)
    assert cells.union_all().area == 1596.0
    assert not any(gm.covers(cell, gm.Point(0, 0)) for cell in cells)

    l_clip = gm.Polygon([
        (0, 0),
        (3, 0),
        (3, 1.5),
        (1.5, 1.5),
        (1.5, 3),
        (0, 3),
        (0, 0),
    ])
    l_cells = gm.MultiPoint([(0, 0), (1e150, 0), (0, 1e-150)]).voronoi_polygons(
        clip=l_clip
    )
    _partition(l_cells, l_clip)


@pytest.mark.parametrize('site_count', [400, 800])
def test_convex_clip_accepts_topological_snap_partition(site_count: int) -> None:
    rng = random.Random(7)
    clip = gm.Polygon([(50, 50), (950, 120), (900, 900), (120, 850), (50, 50)])
    sites = [(rng.uniform(0, 1000), rng.uniform(0, 1000)) for _ in range(site_count)]

    cells = gm.MultiPoint(sites).voronoi_polygons(clip=clip)

    assert cells
    assert cells.coverage_is_valid()
    assert all(cell.is_valid for cell in cells)
    for left, right in combinations(cells, 2):
        assert gm.relate_pattern(left, right, 'F********'), gm.relate(left, right)
    for site in sites:
        if gm.covers(clip, gm.Point(*site)):
            assert sum(gm.covers(cell, gm.Point(*site)) for cell in cells) == 1


def test_seeded_4097_site_padded_diagram_completes_under_the_public_budget() -> None:
    rng = random.Random(0xC0DEC0DE)
    sites = [(rng.random(), rng.random()) for _ in range(4097)]

    cells = gm.MultiPoint(sites).voronoi_polygons(clip='padded')

    assert len(cells) == len(sites)
    assert all(cell.is_valid for cell in cells)


def test_4000_site_convex_position_completes_with_certified_frame_walk() -> None:
    sites = [(float(i), float(i * i)) for i in range(4000)]

    cells = gm.MultiPoint(sites).voronoi_polygons(clip='padded')

    assert len(cells) == len(sites)
    assert all(cell.is_valid for cell in cells)
