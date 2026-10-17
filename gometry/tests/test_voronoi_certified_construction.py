"""Certified-construction and materialization guards for Voronoi Pass C."""

from __future__ import annotations

import itertools
import math
import operator
import random
from fractions import Fraction
from itertools import combinations, pairwise

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
        return gm.GeometryArray([source]).voronoi(output='edges', clip=clip)[0]
    return source.voronoi(output='edges', clip=clip)


def _cells(source: gm.Geometry, clip: gm.Geometry, frontend: str) -> gm.GeometryArray:
    if frontend == 'packed':
        return gm.GeometryArray([source]).voronoi(output='polygons', clip=clip)[0]
    return source.voronoi(output='polygons', clip=clip)


def _normalized_edges(
    edges: gm.GeometryArray,
) -> tuple[tuple[tuple[float, float], ...], ...]:
    normalized = []
    for edge in edges:
        coordinates = tuple(tuple(xy) for xy in edge.coords)
        normalized.append(min(coordinates, tuple(reversed(coordinates))))
    return tuple(sorted(normalized))


def _outcome(
    points: list[tuple[float, float]], clip: gm.Geometry | str
) -> tuple[str, object]:
    # Cells are byte-identical between the lanes. Edges are the same segments,
    # but each lane orders and directs its list by its own construction (the
    # subdivision walk versus the dual's lexicographic canon), so they compare
    # as a set of undirected segments.
    try:
        source = gm.MultiPoint(points)
        segments = set()
        for edge in source.voronoi(output='edges', clip=clip):
            coords = [tuple(xy[:2]) for xy in edge.coords]
            segments.update(
                (start, end) if start <= end else (end, start)
                for start, end in pairwise(coords)
            )
        return (
            'ok',
            tuple(
                cell.to_wkb() for cell in source.voronoi(output='polygons', clip=clip)
            ),
            frozenset(segments),
        )
    except gm.GeometryError as error:
        return ('error', str(error))


def _rectangle_clip(points: list[tuple[float, float]], mode: str) -> gm.Geometry:
    source = gm.MultiPoint(points)
    bounds = source.bounds
    assert bounds is not None
    min_x, min_y, max_x, max_y = bounds
    if mode == 'envelope':
        return gm.box(min_x, min_y, max_x, max_y)

    assert mode == 'padded'
    span = max(
        Fraction(max_x) - Fraction(min_x),
        Fraction(max_y) - Fraction(min_y),
    )
    half = span / 2

    def outward(base: float, add: bool) -> float:
        exact = Fraction(base) + (half if add else -half)
        rounded = float(exact)
        if add and Fraction(rounded) < exact:
            rounded = math.nextafter(rounded, math.inf)
        elif not add and Fraction(rounded) > exact:
            rounded = math.nextafter(rounded, -math.inf)
        return rounded

    return gm.box(
        outward(min_x, False),
        outward(min_y, False),
        outward(max_x, True),
        outward(max_y, True),
    )


def _differential_cases() -> list[tuple[str, list[tuple[float, float]], str]]:
    # The dual branch is selected for at least three normal sites. One random
    # Frozen coordinates from the prior random six-site case cover both finite
    # boundary spellings; a cocircular case keeps the exact tie topology
    # without replaying production-scale inputs.
    random6 = [
        (636.9616873214543, 269.7867137638703),
        (40.97352393619469, 16.527635528529096),
        (813.2702392002724, 912.7555772777217),
        (606.6357757671799, 729.4965609839984),
        (543.6249914654229, 935.0724237877682),
        (815.8535541215322, 2.738500170148095),
    ]
    cocircular6 = [
        (
            math.cos(index * 2 * math.pi / 5) * 100,
            math.sin(index * 2 * math.pi / 5) * 100,
        )
        for index in range(5)
    ] + [(0.0, 0.0)]
    return [
        ('random6-seed0-envelope', random6, 'envelope'),
        ('random6-seed0-padded', random6, 'padded'),
        ('cocircular6-envelope', cocircular6, 'envelope'),
        (
            'near-overflow-envelope',
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
            'subnormal-envelope',
            [
                (0.0, 0.0),
                (5e-324, 0.0),
                (0.0, 5e-324),
                (1e-300, 1e-300),
                (2e-300, 3e-300),
            ],
            'envelope',
        ),
    ]


DIFFERENTIAL_CASES = _differential_cases()

PERMUTATION_FAMILIES: list[tuple[str, tuple[tuple[float, float], ...]]] = [
    (
        'random5',
        (
            (87.02492039700846, 28.681720908755537),
            (60.314815005156184, 77.75340829201788),
            (71.60746296035653, 91.53801204905075),
            (86.03936491828847, 91.82376290487623),
            (2.6587734467597213, 43.72480001295087),
        ),
    ),
    (
        'cocircular4',
        tuple(
            (math.cos(index * math.pi / 2) * 10, math.sin(index * math.pi / 2) * 10)
            for index in range(4)
        ),
    ),
    (
        'collinear-triple-plus-2',
        ((0.0, 0.0), (0.09, 0.0), (0.18, 0.0), (0.0, 1.0), (1.0, 0.0)),
    ),
]


# One case per branch rather than one for the old production-scale sweep. The
# case id names the input that diverged and keeps the named/polygon comparison
# independently reportable under xdist.
@pytest.mark.parametrize(
    ('points', 'clip'),
    [
        pytest.param(points, clip, id=label)
        for label, points, clip in DIFFERENTIAL_CASES
    ],
)
def test_combinatorial_dual_is_byte_identical_to_explicit_rectangle(
    points: list[tuple[float, float]],
    clip: str,
) -> None:
    named = _outcome(points, clip)
    explicit = _outcome(points, _rectangle_clip(points, clip))
    assert named == explicit


@pytest.mark.parametrize(
    'family',
    [pytest.param(family, id=label) for label, family in PERMUTATION_FAMILIES],
)
def test_voronoi_is_byte_identical_to_explicit_rectangle_under_representative_site_orders(
    family: tuple[tuple[float, float], ...],
) -> None:
    # Original, cyclic shift, and reverse cover order direction and a changed
    # insertion start without replaying all n! equivalent permutations.
    orders = (family, family[1:] + family[:1], family[::-1])
    assert all(
        _outcome(list(order), 'envelope')
        == _outcome(list(order), _rectangle_clip(list(order), 'envelope'))
        for order in orders
    )


def _assert_topological_partition(
    cells: gm.GeometryArray,
    clip: gm.Geometry,
    *,
    active_sites: tuple[tuple[float, float], ...],
    require_exact_equality: bool,
) -> None:
    """Certify Voronoi cells as a coverage of ``clip``.

    The R20/R21 helpers keep overlay equality for dyadic axis-aligned clips.
    Otherwise the certificate is validity, ``coverage_is_valid``, and
    interior-disjointness of the rounded partition, not source-clip membership.
    """
    if require_exact_equality:
        _assert_partition(cells, clip)
    else:
        assert all(cell.is_valid for cell in cells)
        assert gm.coverage_is_valid(cells)
        for left, right in combinations(cells, 2):
            assert gm.relate_pattern(left, right, 'F********'), gm.relate(left, right)
    assert len(cells) == len(active_sites)
    assert all(
        sum(gm.covers(cell, gm.Point(*site)) for cell in cells) == 1
        for site in active_sites
    )


def test_seeded_binary64_witnesses_are_repaired_for_both_products() -> None:
    witnesses = (
        (
            (0, 1),
            (0, 2),
            (1, 0),
            (1, 3),
            (3, 1),
            (4, 8),
            (6, 1),
            (6, 6),
            (8, 0),
            (8, 4),
        ),
        ((2, 0), (5, 7), (5, 8), (7, 6), (8, 7)),
        ((0, 7), (2, 1), (2, 2), (3, 7), (4, 4), (4, 8), (7, 1), (8, 3)),
        ((0, 0), (0, 7), (2, 0), (5, 2), (5, 3), (7, 1), (7, 7), (8, 1), (8, 6)),
        ((0, 1), (1, 6), (3, 2), (4, 2), (5, 7), (7, 1)),
        ((0, 3), (3, 4), (5, 4), (8, 3), (8, 6)),
        ((0, 0), (1, 2), (3, 3), (3, 7), (4, 3)),
        (
            (0, 2),
            (0, 8),
            (3, 8),
            (4, 3),
            (4, 4),
            (4, 8),
            (5, 3),
            (5, 4),
            (5, 8),
            (6, 0),
        ),
        ((0, 3), (0, 7), (1, 4), (3, 2), (3, 5), (3, 7), (4, 8), (7, 7), (8, 7)),
        ((0, 0), (1, 0), (1, 5), (2, 2), (2, 4), (5, 1), (6, 1), (8, 2)),
        ((0, 0), (0, 3), (0, 4), (1, 1), (2, 8), (3, 7), (7, 3), (8, 6)),
        ((0, 7), (1, 6), (3, 3), (4, 0), (5, 2), (8, 2)),
        ((0, 2), (1, 3), (4, 3), (4, 5), (4, 8), (5, 2)),
        (
            (0, 0),
            (1, 0),
            (1, 5),
            (1, 6),
            (3, 3),
            (4, 6),
            (7, 1),
            (7, 8),
            (8, 1),
            (8, 2),
        ),
        ((3, 1), (3, 8), (4, 3), (5, 4), (7, 5), (8, 6)),
    )

    for index, points in enumerate(witnesses):
        source = gm.MultiPoint(points)
        cells = source.voronoi(output='polygons', clip='padded')
        assert len(cells) == len(points), index
        assert all(cell.is_valid for cell in cells), index
        assert gm.coverage_is_valid(cells), index
        assert all(
            sum(gm.covers(cell, gm.Point(*site)) for cell in cells) == 1
            for site in points
        ), index
        edges = source.voronoi(output='edges', clip='padded')
        assert all(edge.geometry_type == 'LineString' for edge in edges), index


@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
def test_strip_cells_take_adjacent_floats_beside_colliding_clip_vertices(
    frontend: str,
) -> None:
    points = [(0, -1), (0, 0.1), (0, 1), (100, 0)]
    sites = gm.MultiPoint(points)
    clip = gm.Polygon([
        (-1, -2),
        (1, -2),
        (1, -0.45),
        (1, -0.25),
        (1, 0.25),
        (1, 0.55),
        (1, 2),
        (-1, 2),
        (-1, 0.55),
        (-1, 0.25),
        (-1, -0.25),
        (-1, -0.45),
        (-1, -2),
    ])
    cells = _cells(sites, clip, frontend)

    _assert_partition(cells, clip)
    assert gm.coverage_is_valid(cells)
    # The exact bisector heights (stored(-1) + stored(0.1)) / 2 and
    # (stored(1) + stored(0.1)) / 2 round to the stored clip vertices -0.45 and
    # 0.55, so the only topology-preserving assignment is the adjacent float on
    # the bisector's side, splitting the rectangle into three horizontal strips.
    lower = math.nextafter(-0.45, math.inf)
    upper = math.nextafter(0.55, -math.inf)
    assert sorted((cell.bounds for cell in cells), key=operator.itemgetter(1)) == [
        (-1.0, -2.0, 1.0, lower),
        (-1.0, lower, 1.0, upper),
        (-1.0, upper, 1.0, 2.0),
    ]
    for point in points[:3]:
        assert sum(gm.covers(cell, gm.Point(*point)) for cell in cells) == 1
    assert not any(gm.covers(cell, gm.Point(*points[3])) for cell in cells)


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
        _normalized_edges(
            gm.MultiPoint(order).voronoi(output='edges', tolerance=0.1, clip=clip)
        )
        for order in itertools.permutations(sites)
    }
    assert len(diagrams) == 1


def test_ordinary_five_site_cells_are_valid_and_permutation_invariant() -> None:
    sites = ((0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.5, 0.4))
    clip = gm.box(-1, -1, 2, 2)
    diagrams = set()
    for order in itertools.permutations(sites):
        cells = gm.MultiPoint(order).voronoi(output='polygons', clip=clip)
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
def test_sloped_clip_has_one_valid_checked_rounded_partition_for_every_order(
    carrier: str, frontend: str
) -> None:
    sites = ((0.0, -0.2), (0.6, -0.2), (0.3, -0.8))
    stored_far = 1.0 / 3.0
    clip = gm.Polygon([
        (0.0, 0.0),
        (1.0, stored_far),
        (1.0, -1.0),
        (0.0, -1.0),
        (0.0, 0.0),
    ])
    contact_x = 0.3
    exact_y = Fraction(contact_x) * Fraction(stored_far)
    contact_y = float(exact_y)
    assert contact_y == float.fromhex('0x1.9999999999999p-4')
    contact_err = abs(exact_y - Fraction(contact_y))
    for neighbor in (
        math.nextafter(contact_y, -math.inf),
        math.nextafter(contact_y, math.inf),
    ):
        assert contact_err < abs(exact_y - Fraction(neighbor))
    contact = (contact_x, contact_y)
    slope = gm.LineString([(0.0, 0.0), (1.0, stored_far)])
    assert not gm.covers(slope, gm.Point(*contact))
    signatures = set()
    for order in itertools.permutations(sites):
        cells = _cells(_carrier(order, carrier), clip, frontend)
        _assert_topological_partition(
            cells, clip, active_sites=sites, require_exact_equality=False
        )
        assert contact in {tuple(xy) for cell in cells for xy in cell.coords}
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


def test_array_voronoi_edge_lines_concat_preserves_missing_and_empty_groups() -> None:
    arr = gm.GeometryArray([
        gm.MultiPoint([(0.0, 0.0), (2.0, 2.0)]),
        None,
        gm.MultiPoint([(0.0, 0.0)]),
        gm.MultiPoint([(0.0, 0.0), (4.0, 4.0)]),
    ])
    edges = arr.voronoi(output='edges', clip='envelope')
    assert isinstance(edges, gm.Groups)
    assert len(edges) == 4
    assert edges.offsets.tolist() == [0, 1, 1, 1, 2]
    assert edges[1] is None
    assert len(edges[2]) == 0

    def undirected(line: gm.Geometry) -> tuple[tuple[float, float], ...]:
        coordinates = tuple(tuple(xy[:2]) for xy in line.coords)
        return min(coordinates, tuple(reversed(coordinates)))

    assert undirected(edges[0][0]) == ((0.0, 2.0), (2.0, 0.0))
    assert undirected(edges[3][0]) == ((0.0, 4.0), (4.0, 0.0))


def test_array_voronoi_edge_lines_match_scalar_packed_chains() -> None:
    sites = gm.MultiPoint([
        (-4.0, -3.0),
        (4.0, -2.0),
        (3.0, 4.0),
        (-3.0, 3.0),
        (0.0, 0.0),
    ])
    clip = gm.box(-6.0, -6.0, 6.0, 6.0)
    scalar = sites.voronoi(output='edges', clip=clip)
    grouped = gm.GeometryArray([sites]).voronoi(output='edges', clip=clip)
    assert len(grouped) == 1
    packed = grouped[0]
    assert _normalized_edges(scalar) == _normalized_edges(packed)
    assert [tuple(tuple(xy) for xy in edge.coords) for edge in scalar] == [
        tuple(tuple(xy) for xy in edge.coords) for edge in packed
    ]


def test_cocircular_diagram_keeps_one_vertex_for_all_orders() -> None:
    sites = ((22.0, -61.0), (62.0, -19.0), (-22.0, 61.0), (-62.0, 19.0))
    diagrams = set()
    for order in itertools.permutations(sites):
        edges = gm.MultiPoint(order).voronoi(
            output='edges',
        )
        assert len(edges) == 4
        assert all((0.0, 0.0) in list(edge.coords) for edge in edges)
        diagrams.add(_normalized_edges(edges))
    assert len(diagrams) == 1


def test_hole_and_l_clip_guards_use_full_topology_oracle() -> None:
    clip = gm.difference(gm.box(-20, -20, 20, 20), gm.box(-1, -1, 1, 1))
    cells = gm.MultiPoint([(-10, -10), (10, -10), (0, 10)]).voronoi(
        output='polygons', clip=clip
    )
    _partition(cells, clip)
    assert gm.union_all(cells).area == 1596.0
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
    l_cells = gm.MultiPoint([(0, 0), (1e150, 0), (0, 1e-150)]).voronoi(
        output='polygons', clip=l_clip
    )
    _partition(l_cells, l_clip)


@pytest.mark.parametrize('site_count', [400, 800])
def test_convex_clip_accepts_topological_snap_partition(site_count: int) -> None:
    rng = random.Random(7)
    clip = gm.Polygon([(50, 50), (950, 120), (900, 900), (120, 850), (50, 50)])
    sites = [(rng.uniform(0, 1000), rng.uniform(0, 1000)) for _ in range(site_count)]

    cells = gm.MultiPoint(sites).voronoi(output='polygons', clip=clip)

    assert cells
    assert gm.coverage_is_valid(cells)
    assert all(cell.is_valid for cell in cells)
    site_coordinates = np.asarray(sites, dtype=np.float64)
    site_points = gm.points(site_coordinates[:, 0], site_coordinates[:, 1])
    covered_sites = np.asarray(gm.covers(clip, site_points))
    matches = gm.SpatialIndex(cells).query(site_points, predicate='covered_by')
    assert np.all(matches.counts[covered_sites] == 1)


def test_seeded_4097_site_padded_diagram_completes_under_the_public_budget() -> None:
    rng = random.Random(0xC0DEC0DE)
    sites = [(rng.random(), rng.random()) for _ in range(4097)]

    cells = gm.MultiPoint(sites).voronoi(output='polygons', clip='padded')

    assert len(cells) == len(sites)
    assert all(cell.is_valid for cell in cells)


def test_convex_position_completes_with_certified_frame_walk() -> None:
    # Six is the smallest input that retains the public dual (>5 sites) and
    # puts every site on the same convex parabola chain.
    sites = [(float(i), float(i * i)) for i in range(6)]

    cells = gm.MultiPoint(sites).voronoi(output='polygons', clip='padded')

    assert len(cells) == len(sites)
    assert all(cell.is_valid for cell in cells)
