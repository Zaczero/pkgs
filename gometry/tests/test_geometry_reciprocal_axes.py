"""R19-A geometry-owner numerical regressions with stored-double oracles."""

from __future__ import annotations

import math
from fractions import Fraction
from itertools import pairwise

import gometry as gm
import pytest


def _ring_area(polygon: gm.Geometry) -> Fraction:
    coords = [tuple(map(Fraction, xy)) for xy in polygon.exterior.coords]
    return abs(
        sum(left[0] * right[1] - left[1] * right[0] for left, right in pairwise(coords))
        / 2
    )


@pytest.mark.parametrize('exponent', [159, 161, 162, 163, 199, 200, 201])
@pytest.mark.parametrize('offset_sign', [-1.0, 1.0])
@pytest.mark.parametrize('span_sign', [-1.0, 1.0])
@pytest.mark.parametrize('swap_axes', [False, True])
@pytest.mark.parametrize('wrapper', ['multipart', 'collection'])
@pytest.mark.parametrize(
    'frontend',
    [
        'relate',
        'equals',
        'covers',
        'within',
        'hausdorff',
        'equals_prepared',
        'covers_prepared',
        'within_prepared',
        'packed_equals',
        'packed_covers',
        'index_equals',
        'index_covers',
        'index_within',
        'index_intersects',
    ],
)
def test_reciprocal_axis_identical_linework_has_no_exterior_residue(
    exponent: int,
    offset_sign: float,
    span_sign: float,
    swap_axes: bool,
    wrapper: str,
    frontend: str,
) -> None:
    """Each frontend independently reaches endpoint projection at the transition."""
    large = offset_sign * 10.0**exponent
    tiny = span_sign * 10.0**-exponent
    coords = [(large, 0.0), (large, tiny)]
    if swap_axes:
        coords = [(y, x) for x, y in coords]
    line = gm.LineString(coords)
    wrapped = (
        gm.MultiLineString([coords])
        if wrapper == 'multipart'
        else gm.GeometryCollection([line])
    )

    for left, right in ((line, wrapped), (wrapped, line)):
        if frontend == 'relate':
            assert gm.relate(left, right) == '1FFF0FFF2'
        elif frontend == 'hausdorff':
            assert gm.hausdorff_distance(left, right) == 0.0
        elif frontend == 'equals_prepared':
            assert gm.equals(left.prepare(), right)
        elif frontend == 'covers_prepared':
            assert gm.covers(left.prepare(), right)
        elif frontend == 'within_prepared':
            assert gm.within(left.prepare(), right)
        elif frontend.startswith('packed_'):
            operation = getattr(gm, frontend.removeprefix('packed_'))
            assert operation(gm.GeometryArray([left]), right).tolist() == [True]
        elif frontend.startswith('index_'):
            predicate = frontend.removeprefix('index_')
            assert gm.SpatialIndex([left]).query(
                right, predicate=predicate
            ).tolist() == [0]
        else:
            assert getattr(gm, frontend)(left, right)


@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize(
    'offset', [-1_000_001.0, -1_000_000.0, 0.0, 1_000_000.0, 1_000_001.0]
)
@pytest.mark.parametrize('translation_axis', [0, 1])
@pytest.mark.parametrize('site_scale', [0.125, 0.25, 0.49, 0.5, 0.75, 1.0, 1.25])
def test_framed_voronoi_sites_and_explicit_clip_partition_the_same_rectangle(
    offset: float, translation_axis: int, site_scale: float, frontend: str
) -> None:
    """Exact shoelace area enters native framed half-plane polygon clipping."""
    translate = lambda x, y: (
        (offset + site_scale * x, site_scale * y)
        if translation_axis == 0
        else (site_scale * x, offset + site_scale * y)
    )
    stored_sites = [
        translate(x, y)
        for x, y in ((0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.5, 0.4))
    ]
    clip = (
        gm.box(offset - 1.0, -1.0, offset + 2.0, 2.0)
        if translation_axis == 0
        else gm.box(-1.0, offset - 1.0, 2.0, offset + 2.0)
    )
    sites = gm.MultiPoint(stored_sites)
    cells = (
        sites.voronoi_polygons(clip=clip)
        if frontend == 'scalar'
        else gm.GeometryArray([sites]).voronoi_polygons(clip=clip)[0]
    )
    expected = _ring_area(clip)
    assert len(cells) == 5
    observed = sum((_ring_area(cell) for cell in cells), Fraction())
    # Stored output vertices are rounded binary64 intersections, so an exact
    # equality oracle would demand impossible rational bisectors. The exact
    # Fraction tolerance is five orders tighter than the reproduced 27.6% gap.
    assert abs(observed - expected) <= expected * Fraction(1, 10**14)


@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize('site_scale', [0.125, 0.25, 0.49, 0.5, 0.75, 1.0, 1.25])
@pytest.mark.parametrize('swap_axes', [False, True])
def test_finite_extreme_voronoi_polygon_clip_retains_the_full_partition(
    frontend: str, site_scale: float, swap_axes: bool
) -> None:
    """Constraint intersections survive an edge fraction rounded to an endpoint."""
    sites = [
        (site_scale * x, site_scale * y)
        for x, y in ((0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.5, 0.4))
    ]
    if swap_axes:
        sites = [(y, x) for x, y in sites]
    extent = 8e307
    clip = gm.box(-extent, -extent, extent, extent)
    source = gm.MultiPoint(sites)
    cells = (
        source.voronoi_polygons(clip=clip)
        if frontend == 'scalar'
        else gm.GeometryArray([source]).voronoi_polygons(clip=clip)[0]
    )
    assert len(cells) == len(sites)
    expected = _ring_area(clip)
    observed = sum((_ring_area(cell) for cell in cells), Fraction())
    assert abs(observed - expected) <= expected * Fraction(1, 10**14)


@pytest.mark.parametrize('half_extent', [4.49e307, 4.5e307, 6.35e307, 6.36e307, 8e307])
@pytest.mark.parametrize('frontend', ['scalar', 'packed'])
@pytest.mark.parametrize('site_scale', [0.125, 0.25, 0.49, 0.5, 0.75, 1.0, 1.25])
@pytest.mark.parametrize('swap_axes', [False, True])
@pytest.mark.parametrize(
    'kind',
    ['triangle', 'collinear'],
)
def test_finite_voronoi_clip_retains_every_analytic_hull_bisector(
    half_extent: float, kind: str, site_scale: float, frontend: str, swap_axes: bool
) -> None:
    """Finite hull rays enter both inner-outer and collinear outer-outer arms."""
    sites = (
        [(0.0, 0.0), (site_scale, 0.0), (0.0, site_scale)]
        if kind == 'triangle'
        else [(0.0, 0.0), (site_scale, 0.0), (2.0 * site_scale, 0.0)]
    )
    if swap_axes:
        sites = [(y, x) for x, y in sites]
    clip = gm.box(-half_extent, -half_extent, half_extent, half_extent)
    source = gm.MultiPoint(sites)
    result = (
        source.voronoi_edges(clip=clip)
        if frontend == 'scalar'
        else gm.GeometryArray([source]).voronoi_edges(clip=clip)[0]
    )
    bisector_axis = 1 if swap_axes else 0
    midpoint = float(
        (Fraction(sites[0][bisector_axis]) + Fraction(sites[1][bisector_axis])) / 2
    )
    endpoints = [set(edge.coords) for edge in result]
    if kind == 'triangle':
        center = midpoint
        expected_boundary = {
            (center, -half_extent),
            (-half_extent, center),
            (half_extent, half_extent),
        }
        assert len(result) == 3
        assert {
            next(point for point in edge if point != (center, center))
            for edge in endpoints
        } == expected_boundary
    else:
        expected_axis = {
            midpoint,
            float(
                (
                    Fraction(sites[1][1 if swap_axes else 0])
                    + Fraction(sites[2][1 if swap_axes else 0])
                )
                / 2
            ),
        }
        assert len(result) == 2
        observed = {
            (
                start[1 if swap_axes else 0],
                frozenset((start[0 if swap_axes else 1], end[0 if swap_axes else 1])),
            )
            for start, end in (tuple(edge.coords) for edge in result)
        }
        assert observed == {
            (coordinate, frozenset((-half_extent, half_extent)))
            for coordinate in expected_axis
        }


def test_voronoi_ray_starting_on_clip_boundary_is_a_dropped_edge_not_an_error() -> None:
    sites = gm.MultiPoint([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)])
    edges = sites.voronoi_edges(clip=gm.box(0.5, -1.0, 2.0, 2.0))
    assert len(edges) == 2
    assert all(len(set(edge.coords)) == 2 for edge in edges)


def test_zm_interpolation_uses_the_convex_form_on_every_path() -> None:
    """Z/M interpolation must not overflow through `b - a`.

    There were two implementations: a convex `a*(1-t) + b*t` whose own comment
    exists to forbid the overflow class, and a `start + (end - start) * ratio`
    that IS that class. The unsafe one backed `point_between`/`destination`, so
    on a geographic CRS an extreme-Z pair produced a non-finite ordinate and
    the call RAISED, while the CRS-free path answered correctly.
    """
    big = 1e308
    assert big - (-big) == math.inf  # the trap the convex form avoids

    for crs in (None, 4326, 3857):
        low = gm.Point(0.0, 0.0, z=-big, crs=crs)
        high = gm.Point(1.0, 0.0, z=big, crs=crs)
        assert gm.point_between(low, high, 0.5, normalized=True).z == 0.0, crs

        low_m = gm.Point(0.0, 0.0, m=-big, crs=crs)
        high_m = gm.Point(1.0, 0.0, m=big, crs=crs)
        assert gm.point_between(low_m, high_m, 0.5, normalized=True).m == 0.0, crs

    # ordinary magnitudes are unchanged
    a = gm.Point(0, 0, z=10.0, crs=4326)
    b = gm.Point(1, 0, z=20.0, crs=4326)
    assert gm.point_between(a, b, 0.5, normalized=True).z == 15.0


def test_planar_interpolation_agrees_across_packed_and_fallback_lanes() -> None:
    """The packed lane and the fallback lane must answer identically.

    `point_between` has two planar implementations. The packed fast path used
    the convex `interpolate_f64`; the fallback used `from + (to - from) * ratio`
    and so overflowed at extreme endpoints. A MIXED-AXES array of Points
    (XY + XYZ) has no packed point rows, so it takes the fallback — and the
    same call that answered `POINT (0 0)` on a homogeneous array raised
    `coordinates must be finite`. Mixed-axes Point arrays are legal and arrive
    routinely from WKB/Arrow with uneven Z.
    """
    big = 1e308
    expected = 'POINT (0 0)'

    scalar = gm.point_between(
        gm.Point(-big, 0.0), gm.Point(big, 0.0), 0.5, normalized=True
    )
    assert scalar.to_wkt() == expected

    packed = gm.point_between(
        gm.GeometryArray([gm.Point(-big, 0.0)]),
        gm.GeometryArray([gm.Point(big, 0.0)]),
        0.5,
        normalized=True,
    )
    assert [p.to_wkt() for p in packed] == [expected]

    # mixed axes -> no packed point rows -> the fallback lane
    mixed = gm.point_between(
        gm.GeometryArray([gm.Point(-big, 0.0), gm.Point(-big, 0.0, z=0.0)]),
        gm.GeometryArray([gm.Point(big, 0.0), gm.Point(big, 0.0, z=0.0)]),
        0.5,
        normalized=True,
    )
    assert [p.to_wkt() for p in mixed] == [expected, 'POINT Z (0 0 0)']

    # ordinary magnitudes are untouched
    assert (
        gm.point_between(gm.Point(0, 0), gm.Point(10, 0), 0.5, normalized=True).to_wkt()
        == 'POINT (5 0)'
    )
