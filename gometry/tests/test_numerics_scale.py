"""R15-B numerics findings 3-19: deterministic exact-ref regressions.

No wall-clock assertions. Fraction/mpmath/math references only.
Findings 1-2 live in test_numerics_blockers.py.
"""

from __future__ import annotations

import math
from fractions import Fraction
from itertools import combinations, pairwise

import gometry as gm
import numpy as np
import pytest


def _stored_fraction(value: float) -> Fraction:
    """The reference is the stored IEEE-754 value, never its source literal."""
    return Fraction(value)


def _assert_mrr_encloses_exactly(
    rectangle: gm.Geometry,
    points: list[tuple[float, float]],
) -> None:
    """Independent dyadic half-plane oracle for the emitted f64 ring."""
    corners = list(rectangle.exterior.coords)
    assert len(corners) == 5 and corners[0] == corners[-1]
    ring = [(_stored_fraction(x), _stored_fraction(y)) for x, y in corners]
    signed_area = sum(
        left[0] * right[1] - left[1] * right[0] for left, right in pairwise(ring)
    )
    assert signed_area > 0
    for x, y in points:
        point = (_stored_fraction(x), _stored_fraction(y))
        for left, right in pairwise(ring):
            cross = (right[0] - left[0]) * (point[1] - left[1]) - (
                right[1] - left[1]
            ) * (point[0] - left[0])
            assert cross >= 0, (point, left, right, cross)


def _triangle_edges(
    triangles: list[gm.Geometry],
) -> set[frozenset[tuple[float, float]]]:
    edges: set[frozenset[tuple[float, float]]] = set()
    for triangle in triangles:
        coords = list(triangle.exterior.coords)
        edges.update(frozenset((left, right)) for left, right in pairwise(coords))
    return edges


def _assert_voronoi_edges_are_source_bisectors(
    edges: list[gm.Geometry],
    sites: list[tuple[float, float]],
) -> None:
    """The known four source-double bisectors span the exact envelope.

    A mathematical bisector can lie strictly between adjacent binary64 values;
    exact equality of two squared distances at its *stored* rendering would be
    an impossible oracle.  Instead this computes the four analytic bisector
    coordinates from the stored source doubles and requires every emitted
    edge to use one, with its other axis spanning the stored envelope.
    """
    dominant = max(range(2), key=lambda axis: max(abs(point[axis]) for point in sites))
    minor = 1 - dominant
    expected = {
        float(
            (
                _stored_fraction(sites[left][dominant])
                + _stored_fraction(sites[right][dominant])
            )
            / 2
        )
        for left, right in ((0, 1), (1, 4), (4, 2), (2, 3))
    }
    extent = {
        min(point[minor] for point in sites),
        max(point[minor] for point in sites),
    }
    observed = set()
    for edge in edges:
        start, end = list(edge.coords)
        assert start[dominant] == end[dominant]
        assert {start[minor], end[minor]} == extent
        observed.add(start[dominant])
    assert observed == expected


# ---------------------------------------------------------------------------
# Finding 3 — tessellation scale completeness
# ---------------------------------------------------------------------------


def test_delaunay_square_tiny_and_huge_is_euler_complete() -> None:
    """Delaunay of a square must emit two triangles totaling side² at both extremes."""
    for side in (4.4e-16, 1e110):
        square = gm.Polygon([(0.0, 0.0), (side, 0.0), (side, side), (0.0, side)])
        tris = square.triangulate(method='delaunay')
        # n=4 and h=4: full Euler, T = 2n - 2 - h = 2.
        assert len(tris) == 2 * 4 - 2 - 4, f'side={side}: T={len(tris)}'
        total = sum((_stored_fraction(triangle.area) for triangle in tris), Fraction())
        exact = _stored_fraction(side) ** 2
        assert abs(total - exact) * 10**12 <= exact, (
            f'side={side}: sum={float(total)!r} exact={float(exact)!r}'
        )


def test_spade_voronoi_affine_extremes_succeed() -> None:
    """Spade Voronoi must not reject affine copies at 1e-100 / 1e155."""
    for side in (1e-100, 1e155):
        sites = gm.MultiPoint([
            (0.0, 0.0),
            (side, 0.0),
            (side, side),
            (0.0, side),
            (0.5 * side, 0.5 * side),
        ])
        edges = sites.voronoi_edges(clip='envelope')
        assert len(edges) >= 1, f'side={side}: no edges'


def test_delaunay_and_raw_voronoi_keep_the_source_euclidean_metric() -> None:
    """An anisotropic frame may not redefine Delaunay topology or bisectors."""
    hull = [(-34.0, -4.0), (99.0, -11.0), (69.0, 4.0), (-5.0, 2.0)]
    triangles = gm.Polygon(hull).triangulate(method='delaunay')
    edges = _triangle_edges(triangles)
    assert frozenset(((-5.0, 2.0), (99.0, -11.0))) in edges

    sites = gm.MultiPoint([(0.0, 0.0), (10.0, 0.0), (4.0, 1.0)])
    segments = sites.voronoi_edges(clip='envelope')
    assert {tuple(sorted(line.coords)) for line in segments} == {
        ((1.875, 1.0), (2.125, 0.0)),
        ((6.916666666666667, 0.0), (7.083333333333333, 1.0)),
    }


def test_concave_hull_tiny_square_area() -> None:
    """Concave hull of a 1e-100 square recovers exact area 1e-200."""
    s = 1e-100
    pts = gm.MultiPoint([(0.0, 0.0), (s, 0.0), (s, s), (0.0, s)])
    hull = pts.concave_hull()
    exact = s * s
    assert hull.area == exact or abs(hull.area - exact) / exact < 1e-9


def test_native_voronoi_five_site_tiny_partition() -> None:
    """Five-site envelope Voronoi cell areas must sum to the envelope."""
    s = 1e-20
    sites = gm.MultiPoint([(0.0, 0.0), (s, 0.0), (s, s), (0.0, s), (0.5 * s, 0.5 * s)])
    cells = sites.voronoi_polygons(clip='envelope')
    total = sum(c.area for c in cells)
    envelope = s * s
    assert abs(total - envelope) / envelope < 1e-9, f'sum={total} env={envelope}'


def test_native_voronoi_polygon_clip_spans_the_requested_polygon() -> None:
    """An explicit clip owns the finite native construction rectangle.

    The analytic oracle is the supplied 20 by 20 rectangle, deliberately far
    larger than the sites' padded default envelope.  Unioning the per-site
    cells must recover that exact requested region rather than a hidden
    site-derived box.
    """
    sites = gm.MultiPoint([(0.0, 0.0), (2.0, 0.0), (1.0, 2.0)])
    clip = gm.box(-10.0, -10.0, 10.0, 10.0)
    cells = sites.voronoi_polygons(clip=clip)
    merged = gm.GeometryArray(cells).union_all()
    assert len(cells) == 3
    assert sum(cell.area for cell in cells) == clip.area == 400.0, (
        'native cells did not partition the requested polygon area'
    )
    assert gm.equals(merged, clip), 'native union did not recover the requested clip'


def _fraction_ring_area(geometry: gm.Geometry) -> Fraction:
    points = [
        (_stored_fraction(x), _stored_fraction(y))
        for x, y in list(geometry.exterior.coords)[:-1]
    ]
    return (
        abs(
            sum(
                x1 * y2 - y1 * x2
                for (x1, y1), (x2, y2) in pairwise(points + points[:1])
            )
        )
        / 2
    )


@pytest.mark.parametrize('exponent', [159, 162, 200, 300])
def test_reciprocal_axis_delaunay_and_voronoi_stay_in_the_source_metric(
    exponent: int,
) -> None:
    """A reciprocal rectangle has positive exact area and five Voronoi cells.

    The oracle is the exact arithmetic of the values CPython stored, not an
    anisotropically scaled proxy coordinate system.  A rectangle is cyclic, so
    either emitted diagonal is Delaunay; the five cells must partition its
    source envelope exactly.
    """
    large = 10.0**exponent
    tiny = 10.0**-exponent
    rectangle = gm.Polygon([
        (-large, -tiny),
        (large, -tiny),
        (large, tiny),
        (-large, tiny),
    ])
    exact_area = 4 * _stored_fraction(large) * _stored_fraction(tiny)
    assert exact_area > 0
    triangles = rectangle.triangulate(method='delaunay')
    assert len(triangles) == 2
    assert (
        sum((_fraction_ring_area(triangle) for triangle in triangles), Fraction())
        == exact_area
    )

    sites = gm.MultiPoint([
        (-large, -tiny),
        (large, -tiny),
        (large, tiny),
        (-large, tiny),
        (0.0, 0.0),
    ])
    cells = sites.voronoi_polygons(clip='envelope')
    assert len(cells) == 5
    assert sum((_fraction_ring_area(cell) for cell in cells), Fraction()) == exact_area


def _reciprocal_pentagon(
    exponent: int, *, swap_axes: bool
) -> list[tuple[float, float]]:
    large = 10.0**exponent
    tiny = 10.0**-exponent
    points = [
        (-large, 0.0),
        (-0.5 * large, -tiny),
        (0.5 * large, -tiny),
        (large, 0.0),
        (0.0, tiny),
    ]
    return [(y, x) for x, y in points] if swap_axes else points


@pytest.mark.parametrize('exponent', [77, 159, 200, 300])
@pytest.mark.parametrize('swap_axes', [False, True])
def test_reciprocal_tessellation_family_has_no_similarity_axis_loss(
    exponent: int,
    swap_axes: bool,
) -> None:
    """Every public tessellator sees the five stored sites in both axis orders."""
    points = _reciprocal_pentagon(exponent, swap_axes=swap_axes)
    polygon = gm.Polygon(points)
    sources = [
        polygon,
        gm.MultiPoint(points),
        gm.MultiLineString([points]),
        gm.GeometryCollection([gm.Point(x, y) for x, y in points]),
    ]
    exact_area = _fraction_ring_area(polygon)
    assert exact_area > 0
    for source in sources:
        triangles = source.triangulate(method='delaunay')
        assert len(triangles) == 3
        assert (
            sum((_fraction_ring_area(triangle) for triangle in triangles), Fraction())
            == exact_area
        )
        # Voronoi sites are the same five source vertices regardless of their
        # carrier.  Exercise every public extraction route rather than giving
        # MultiPoint a private regression lane.
        cells = source.voronoi_polygons(clip='envelope')
        edges = source.voronoi_edges(clip='envelope')
        assert len(cells) == 5
        assert edges
        assert all(
            math.isfinite(value)
            for edge in edges
            for coordinate in edge.coords
            for value in coordinate
        )

    constrained = polygon.triangulate(method='constrained')
    assert len(constrained) == 3
    assert (
        sum((_fraction_ring_area(triangle) for triangle in constrained), Fraction())
        == exact_area
    )

    # A finite stored translation is an exact Hausdorff result for every
    # geometry carrier.  The reference is the source-double offset, not the
    # Hausdorff implementation or an anisotropic framed proxy.  In particular,
    # returning +inf is a failure: that was the round-20 guard's vacuity.
    offset = 4.0 * (10.0**exponent)
    shifted_points = [
        (x + offset, y) if not swap_axes else (x, y + offset) for x, y in points
    ]
    polygon_shifted = gm.Polygon(shifted_points)
    point = gm.Point(*points[0])
    point_shifted = gm.Point(*shifted_points[0])
    family = (
        ('Point', point, point_shifted),
        ('MultiPoint', gm.MultiPoint(points), gm.MultiPoint(shifted_points)),
        ('LineString', gm.LineString(points), gm.LineString(shifted_points)),
        (
            'MultiLineString',
            gm.MultiLineString([points]),
            gm.MultiLineString([shifted_points]),
        ),
        ('Polygon', polygon, polygon_shifted),
        (
            'MultiPolygon',
            gm.MultiPolygon([polygon]),
            gm.MultiPolygon([polygon_shifted]),
        ),
        (
            'GeometryCollection',
            gm.GeometryCollection([gm.Point(x, y) for x, y in points]),
            gm.GeometryCollection([gm.Point(x, y) for x, y in shifted_points]),
        ),
    )
    expected = _stored_fraction(offset)
    for name, source, shifted in family:
        for label, left, right in (
            (name, source, shifted),
            (
                f'one-member {name}',
                gm.GeometryCollection([source]),
                gm.GeometryCollection([shifted]),
            ),
        ):
            forward = float(gm.hausdorff_distance(left, right))
            reverse = float(gm.hausdorff_distance(right, left))
            packed = float(
                gm.hausdorff_distance(
                    gm.GeometryArray([left]), gm.GeometryArray([right])
                )[0]
            )
            for value in (forward, reverse, packed):
                assert math.isfinite(value), (
                    f'{label}: non-finite Hausdorff result {value!r}'
                )
                assert _stored_fraction(value) == expected, (
                    label,
                    value,
                    float(expected),
                )


def test_extreme_hausdorff_general_finisher_is_finite_for_every_kind() -> None:
    """A finite source-double far corner remains finite beyond special cases.

    The independent analytic answer is the largest norm from the origin.  It
    deliberately uses non-translation carriers, so the general continuous
    finisher—not the point-only, segment, or matching-translation reduction—
    must produce it.
    """
    far = 1e200
    origin = gm.Point(0.0, 0.0)
    square = gm.box(0.0, 0.0, far, far)
    family = (
        ('Point', gm.Point(far, far)),
        ('MultiPoint', gm.MultiPoint([(0.0, 0.0), (far, far)])),
        ('LineString', gm.LineString([(0.0, 0.0), (far, 0.0), (far, far)])),
        (
            'MultiLineString',
            gm.MultiLineString([[(0.0, 0.0), (far, 0.0), (far, far)]]),
        ),
        ('Polygon', square),
        ('MultiPolygon', gm.MultiPolygon([square])),
        (
            'GeometryCollection',
            gm.GeometryCollection([
                gm.Point(0.0, 0.0),
                gm.LineString([(far, 0.0), (far, far)]),
            ]),
        ),
    )
    expected = math.hypot(far, far)
    for name, source in family:
        for label, left in (
            (name, source),
            (f'one-member {name}', gm.GeometryCollection([source])),
        ):
            right = origin if left is source else gm.GeometryCollection([origin])
            values = (
                float(gm.hausdorff_distance(left, right)),
                float(gm.hausdorff_distance(right, left)),
                float(
                    gm.hausdorff_distance(
                        gm.GeometryArray([left]), gm.GeometryArray([right])
                    )[0]
                ),
            )
            for value in values:
                assert math.isfinite(value), (
                    f'{label}: non-finite Hausdorff result {value!r}'
                )
                assert value == expected, (label, value, expected)


def test_reciprocal_hausdorff_to_point_is_finite_for_every_kind() -> None:
    """A point target has an exact stored-double vertex-max oracle.

    The 1e159/1e-159 pair makes squared-space arithmetic non-finite while its
    Euclidean norm remains finite.  This is intentionally a different
    property from the non-reciprocal general-finisher case above.
    """
    wide, thin = 1e159, 1e-159
    origin = gm.Point(0.0, 0.0)
    rectangle = gm.box(0.0, 0.0, wide, thin)
    family = (
        ('Point', gm.Point(wide, thin)),
        ('MultiPoint', gm.MultiPoint([(0.0, 0.0), (wide, thin)])),
        ('LineString', gm.LineString([(0.0, 0.0), (wide, 0.0), (wide, thin)])),
        (
            'MultiLineString',
            gm.MultiLineString([[(0.0, 0.0), (wide, 0.0), (wide, thin)]]),
        ),
        ('Polygon', rectangle),
        ('MultiPolygon', gm.MultiPolygon([rectangle])),
        (
            'GeometryCollection',
            gm.GeometryCollection([
                gm.Point(0.0, 0.0),
                gm.LineString([(wide, 0.0), (wide, thin)]),
            ]),
        ),
    )
    expected = math.hypot(wide, thin)
    for name, source in family:
        for label, left in (
            (name, source),
            (f'one-member {name}', gm.GeometryCollection([source])),
        ):
            right = origin if left is source else gm.GeometryCollection([origin])
            values = (
                float(gm.hausdorff_distance(left, right)),
                float(gm.hausdorff_distance(right, left)),
                float(
                    gm.hausdorff_distance(
                        gm.GeometryArray([left]), gm.GeometryArray([right])
                    )[0]
                ),
            )
            for value in values:
                assert math.isfinite(value), (
                    f'{label}: non-finite Hausdorff result {value!r}'
                )
                assert value == expected, (label, value, expected)


def test_hausdorff_cross_kind_identity_and_reciprocal_shared_baseline() -> None:
    """Two independent continuous-Hausdorff source-metric reductions.

    A topologically identical LineString/MultiLineString pair must be zero.
    For the second carrier pair, every baseline point has a line point above
    it and the analytic maximum is the stored apex ordinate, 1e-200.
    """
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    multiline = gm.MultiLineString([[(0.0, 0.0), (1.0, 0.0)]])
    assert gm.hausdorff_distance(line, multiline) == 0.0
    assert gm.hausdorff_distance(multiline, line) == 0.0

    length, offset = 1e200, 1e-200
    baseline = gm.LineString([(0.0, 0.0), (length, 0.0)])
    bend = gm.LineString([(0.0, 0.0), (length / 2.0, offset), (length, 0.0)])
    values = (
        float(gm.hausdorff_distance(baseline, bend)),
        float(gm.hausdorff_distance(bend, baseline)),
        float(
            gm.hausdorff_distance(
                gm.GeometryArray([baseline]), gm.GeometryArray([bend])
            )[0]
        ),
    )
    assert values == (offset, offset, offset)


@pytest.mark.parametrize('exponent', [77, 159, 200, 300])
@pytest.mark.parametrize('swap_axes', [False, True])
def test_reciprocal_delaunay_uses_the_source_incircle_sign(
    exponent: int,
    swap_axes: bool,
) -> None:
    """The affine seed mesh must be legalized by the stored-double circle.

    D lies strictly inside the source circumcircle ABC because ``D * tiny² <
    large²``.  A non-uniform frame changes that comparison and picks AC; this
    literal diagonal oracle is independent of both tessellation engines.
    """
    large = 10.0**exponent
    tiny = 10.0**-exponent
    points = [(-large, 0.0), (0.0, -tiny), (large, 0.0), (0.0, 2.0 * tiny)]
    if swap_axes:
        points = [(y, x) for x, y in points]
    vertical = frozenset((points[1], points[3]))
    horizontal = frozenset((points[0], points[2]))
    for source in (
        gm.Polygon(points),
        gm.MultiPoint(points),
        gm.MultiLineString([points]),
        gm.GeometryCollection([gm.Point(x, y) for x, y in points]),
    ):
        edges = _triangle_edges(source.triangulate(method='delaunay'))
        assert vertical in edges
        assert horizontal not in edges


@pytest.mark.parametrize('swap_axes', [False, True])
def test_reciprocal_constrained_delaunay_uses_the_source_incircle_sign(
    swap_axes: bool,
) -> None:
    """CDT keeps constraints, but its free diagonal is source-metric Delaunay.

    This is an independent dyadic incircle oracle over the stored doubles.
    It exercises scalar and packed Polygon/MultiPolygon (the only public
    constrained carriers); all other public geometry kinds are asserted to
    retain their documented typed rejection rather than silently taking a
    different constrained path.
    """
    large, tiny = 1e159, 1e-159
    points = [(-large, 0.0), (0.0, -tiny), (large, 0.0), (0.0, 2.0 * tiny)]
    if swap_axes:
        points = [(y, x) for x, y in points]
    q = _stored_fraction
    a, b, c, d = [tuple(map(q, point)) for point in points]
    incircle = (
        ((a[0] - d[0]) ** 2 + (a[1] - d[1]) ** 2)
        * ((b[0] - d[0]) * (c[1] - d[1]) - (b[1] - d[1]) * (c[0] - d[0]))
        + ((b[0] - d[0]) ** 2 + (b[1] - d[1]) ** 2)
        * ((c[0] - d[0]) * (a[1] - d[1]) - (c[1] - d[1]) * (a[0] - d[0]))
        + ((c[0] - d[0]) ** 2 + (c[1] - d[1]) ** 2)
        * ((a[0] - d[0]) * (b[1] - d[1]) - (a[1] - d[1]) * (b[0] - d[0]))
    )
    orientation = (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])
    assert incircle * orientation > 0
    legal = frozenset((points[1], points[3]))
    illegal = frozenset((points[0], points[2]))
    polygon = gm.Polygon(points)
    for source in (polygon, gm.MultiPolygon([polygon])):
        for triangles in (
            source.triangulate(method='constrained'),
            gm.GeometryArray([source]).triangulate(method='constrained')[0],
        ):
            edges = _triangle_edges(triangles)
            assert legal in edges, 'stored-double incircle diagonal is absent'
            assert illegal not in edges, (
                'reciprocal affine diagonal survived legalization'
            )
    for source in (
        gm.Point(*points[0]),
        gm.MultiPoint(points),
        gm.LineString(points),
        gm.MultiLineString([points]),
        gm.GeometryCollection([gm.Point(*point) for point in points]),
        gm.GeometryCollection([polygon]),
        gm.GeometryCollection([gm.MultiPolygon([polygon])]),
    ):
        for carrier in (source, gm.GeometryArray([source])):
            with pytest.raises(gm.GeometryTypeError, match='Polygon or MultiPolygon'):
                carrier.triangulate(method='constrained')


@pytest.mark.parametrize('swap_axes', [False, True])
def test_reciprocal_constrained_delaunay_preserves_hole_constraints(
    swap_axes: bool,
) -> None:
    """Source-metric legalization cannot flip an outer or hole constraint."""
    large, tiny = 1e159, 1e-159
    outer = [
        (-large, -4.0 * tiny),
        (large, -4.0 * tiny),
        (large, 4.0 * tiny),
        (-large, 4.0 * tiny),
    ]
    hole = [
        (-0.25 * large, -tiny),
        (0.25 * large, -tiny),
        (0.25 * large, tiny),
        (-0.25 * large, tiny),
    ]
    if swap_axes:
        outer = [(y, x) for x, y in outer]
        hole = [(y, x) for x, y in hole]
    polygon = gm.Polygon(outer, holes=[hole])
    constraints = {
        frozenset((start, end))
        for ring in (outer, hole)
        for start, end in pairwise([*ring, ring[0]])
    }
    hole_center = gm.Point(
        sum(x for x, _ in hole) / len(hole),
        sum(y for _, y in hole) / len(hole),
    )
    for source in (polygon, gm.MultiPolygon([polygon])):
        for triangles in (
            source.triangulate(method='constrained'),
            gm.GeometryArray([source]).triangulate(method='constrained')[0],
        ):
            edges = _triangle_edges(triangles)
            assert constraints <= edges
            assert not any(gm.covers(triangle, hole_center) for triangle in triangles)


@pytest.mark.parametrize('swap_axes', [False, True])
def test_reciprocal_voronoi_polygon_clip_and_unsnapped_tolerance_share_metric_path(
    swap_axes: bool,
) -> None:
    """Equivalent clip syntax and a no-op tolerance cannot change Voronoi cells.

    The envelope equality and pair-distance proof are independent of the
    tessellator.  Point has one unique site and therefore correctly produces
    no cells; every other public geometry carrier reaches the same five-site
    result in scalar, packed, and one-member-wrapper form.
    """
    points = _reciprocal_pentagon(77, swap_axes=swap_axes)
    xs, ys = zip(*points, strict=True)
    clip = gm.box(min(xs), min(ys), max(xs), max(ys))
    tolerance = 1e71
    assert (
        min(
            math.hypot(x1 - x2, y1 - y2)
            for (x1, y1), (x2, y2) in combinations(points, 2)
        )
        > tolerance
    )
    polygon = gm.Polygon(points)
    family = (
        gm.MultiPoint(points),
        gm.LineString(points),
        gm.MultiLineString([points]),
        polygon,
        gm.MultiPolygon([polygon]),
        gm.GeometryCollection([gm.Point(*point) for point in points]),
    )
    for source in (gm.Point(*points[0]), *family):
        expected = 0 if isinstance(source, gm.Point) else 5
        for carrier in (source, gm.GeometryCollection([source])):
            for kwargs in (
                {'clip': clip},
                {'clip': 'envelope', 'tolerance': tolerance},
            ):
                for result in (
                    carrier.voronoi_polygons(**kwargs),
                    gm.GeometryArray([carrier]).voronoi_polygons(**kwargs)[0],
                ):
                    assert len(result) == expected
                for result in (
                    carrier.voronoi_edges(**kwargs),
                    gm.GeometryArray([carrier]).voronoi_edges(**kwargs)[0],
                ):
                    assert len(result) == (0 if expected == 0 else 4)
                    if expected:
                        _assert_voronoi_edges_are_source_bisectors(result, points)


def test_native_voronoi_affine_copies_partition_their_stored_envelope() -> None:
    """The five convex cells have no material gap/overlap after unframing."""
    for side in (1e-100, 1e100, 1e155):
        points = [
            (0.0, 0.0),
            (side, 0.0),
            (side, side),
            (0.0, side),
            (side / 2.0, side / 2.0),
        ]
        envelope = gm.box(0.0, 0.0, side, side)
        cells = gm.MultiPoint(points).voronoi_polygons(clip='envelope')
        exact_envelope = _stored_fraction(side) ** 2
        exact_cells = sum((_fraction_ring_area(cell) for cell in cells), Fraction())
        # Unframing one cell boundary costs at most a few stored-coordinate
        # ULPs; this is far below a geometric gap/overlap.
        assert abs(exact_cells - exact_envelope) * 10**14 <= exact_envelope
        merged = gm.GeometryArray(cells).union_all()
        assert gm.covers(merged, envelope) and gm.covers(envelope, merged)


# ---------------------------------------------------------------------------
# Finding 4 — CDT absolute snap removed
# ---------------------------------------------------------------------------


def test_cdt_small_square_keeps_constraints() -> None:
    """A valid 5e-5 square must produce constrained triangles of total area s²."""
    s = 5e-5
    boundary = [(0.0, 0.0), (s, 0.0), (s, s), (0.0, s)]
    poly = gm.Polygon(boundary)
    tris = poly.triangulate(method='constrained')
    assert len(tris) == 2
    total = sum((_stored_fraction(triangle.area) for triangle in tris), Fraction())
    exact = _stored_fraction(s) ** 2
    assert abs(total - exact) * 10**12 <= exact
    edges = _triangle_edges(tris)
    assert all(
        frozenset((left, right)) in edges
        for left, right in pairwise(boundary + boundary[:1])
    )


def test_cdt_thin_rectangle_5e_5_by_1() -> None:
    s = 5e-5
    boundary = [(0.0, 0.0), (s, 0.0), (s, 1.0), (0.0, 1.0)]
    poly = gm.Polygon(boundary)
    tris = poly.triangulate(method='constrained')
    assert len(tris) == 2
    total = sum((_stored_fraction(triangle.area) for triangle in tris), Fraction())
    exact = _stored_fraction(s) * _stored_fraction(1.0)
    assert abs(total - exact) * 10**12 <= exact
    edges = _triangle_edges(tris)
    assert all(
        frozenset((left, right)) in edges
        for left, right in pairwise(boundary + boundary[:1])
    )


# ---------------------------------------------------------------------------
# Finding 5 — Hausdorff / Fréchet extreme finishers
# ---------------------------------------------------------------------------


def test_hausdorff_frechet_huge_parallel_finite() -> None:
    base = 1e200
    sep = math.ulp(base) * 16
    a = gm.LineString([(0.0, base), (1.0, base)])
    b = gm.LineString([(0.0, base + sep), (1.0, base + sep)])
    hd = gm.hausdorff_distance(a, b)
    fd = gm.frechet_distance(a, b)
    exact = _stored_fraction(base + sep) - _stored_fraction(base)
    assert math.isfinite(hd) and math.isfinite(fd)
    assert _stored_fraction(float(hd)) == exact
    assert _stored_fraction(float(fd)) == exact


def test_hausdorff_line_vs_endpoints_tiny_and_huge() -> None:
    for length in (1e-200, 1e200):
        line = gm.LineString([(0.0, 0.0), (length, 0.0)])
        ends = gm.MultiPoint([(0.0, 0.0), (length, 0.0)])
        got = gm.hausdorff_distance(line, ends)
        exact = _stored_fraction(length) / 2
        assert math.isfinite(got)
        assert _stored_fraction(float(got)) == exact, f'L={length}: got={got}'


def test_hausdorff_parallel_unit_lines_1e_200() -> None:
    sep = 1e-200
    a = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    b = gm.LineString([(0.0, sep), (1.0, sep)])
    got = gm.hausdorff_distance(a, b)
    assert _stored_fraction(float(got)) == _stored_fraction(sep)


def test_hausdorff_packed_array_shares_extreme_finisher() -> None:
    """GeometryArray/packed HD must share the continuous frame finisher."""
    base = 1e200
    sep = math.ulp(base) * 16
    left = gm.GeometryArray([gm.LineString([(0.0, base), (1.0, base)])])
    right = gm.GeometryArray([gm.LineString([(0.0, base + sep), (1.0, base + sep)])])
    arr = gm.hausdorff_distance(left, right)
    frechet = gm.frechet_distance(left, right)
    exact = _stored_fraction(base + sep) - _stored_fraction(base)
    assert math.isfinite(float(arr[0])), f'packed HD nonfinite: {arr[0]!r}'
    assert math.isfinite(float(frechet[0])), f'packed Fréchet nonfinite: {frechet[0]!r}'
    assert _stored_fraction(float(arr[0])) == exact
    assert _stored_fraction(float(frechet[0])) == exact


def test_hausdorff_four_vertex_packed_exit_shares_extreme_finisher() -> None:
    """The specialized four-vertex packed exit must not bypass framing."""
    base = 1e200
    sep = math.ulp(base) * 16
    coords = [0.0, 0.25, 0.75, 1.0]
    left = gm.LineString([(x, base) for x in coords])
    right = gm.LineString([(x, base + sep) for x in coords])
    packed = gm.hausdorff_distance(
        gm.GeometryArray([left]),
        gm.GeometryArray([right]),
    )
    exact = _stored_fraction(base + sep) - _stored_fraction(base)
    assert _stored_fraction(float(gm.hausdorff_distance(left, right))) == exact
    assert _stored_fraction(float(packed[0])) == exact


@pytest.mark.parametrize('exponent', [77, 159, 200, 300])
def test_hausdorff_collinear_polyline_reduction_is_exact_and_symmetric(
    exponent: int,
) -> None:
    """Subdividing a segment cannot turn a continuous HD into a frame proxy.

    The reference is endpoint-to-segment geometry on the stored doubles:
    these two horizontal segments have Hausdorff distance ``hypot(large,
    tiny)``.  It deliberately covers both operand orders and the packed call
    that shares the scalar finisher.
    """
    large = 10.0**exponent
    tiny = 10.0**-exponent
    left = gm.LineString([(0.0, 0.0), (large, 0.0), (2.0 * large, 0.0)])
    right = gm.LineString([
        (large, tiny),
        (1.5 * large, tiny),
        (2.0 * large, tiny),
        (3.0 * large, tiny),
    ])
    expected = math.hypot(large, tiny)
    forward = float(gm.hausdorff_distance(left, right))
    reverse = float(gm.hausdorff_distance(right, left))
    packed = float(
        gm.hausdorff_distance(gm.GeometryArray([left]), gm.GeometryArray([right]))[0]
    )
    assert _stored_fraction(forward) == _stored_fraction(expected)
    assert _stored_fraction(reverse) == _stored_fraction(expected)
    assert _stored_fraction(packed) == _stored_fraction(expected)


def test_hausdorff_backtracking_collinear_line_is_not_reduced_to_endpoints() -> None:
    """A collinear vertex outside the endpoint interval changes continuous HD."""
    left = gm.LineString([(0.0, 0.0), (10.0, 0.0), (-5.0, 0.0)])
    right = gm.LineString([(-5.0, 1.0), (0.0, 1.0)])
    expected = math.sqrt(101.0)
    assert gm.hausdorff_distance(left, right) == expected
    assert gm.hausdorff_distance(right, left) == expected


@pytest.mark.parametrize('power', [26, 27, 29])
def test_inert_constrained_max_area_never_refines_or_leaks(power: int) -> None:
    """Exact source area admits this max-area request without refinement.

    The exact dyadic ring oracle makes a raw-coordinate area underflow unable
    to reintroduce a refinement loop or emit a triangle outside the source.
    """
    epsilon = 2.0**-power
    source = gm.Polygon([(0.0, 0.0), (1.0, 1.0 + epsilon), (1.0 - epsilon, 1.0)])
    triangles = source.triangulate(method='constrained', max_area=2.0 * source.area)
    assert len(triangles) == 1
    assert _fraction_ring_area(triangles[0]) == _fraction_ring_area(source)
    union = gm.GeometryArray(triangles).union_all()
    assert gm.covers(source, union)
    assert gm.covers(union, source)


@pytest.mark.parametrize('exponent', [77, 159, 200, 300])
@pytest.mark.parametrize('swap_axes', [False, True])
def test_active_constrained_max_area_is_scaled_from_the_source_metric(
    exponent: int,
    swap_axes: bool,
) -> None:
    """An active source-space area limit reaches Spade in local units exactly."""
    large = 10.0**exponent
    tiny = 10.0**-exponent
    points = [(0.0, 0.0), (large, 0.0), (large, tiny), (0.0, tiny)]
    if swap_axes:
        points = [(y, x) for x, y in points]
    source = gm.Polygon(points)
    limit = 0.4
    triangles = source.triangulate(method='constrained', max_area=limit)
    assert len(triangles) == 4
    assert all(
        _fraction_ring_area(triangle) <= _stored_fraction(limit)
        for triangle in triangles
    )
    # Exact source-area conservation plus per-face source-rectangle admission
    # is the analytic postcondition.  It cannot be made vacuous by the
    # overlay implementation that happens to consume the output later.
    assert sum(
        (_fraction_ring_area(triangle) for triangle in triangles), Fraction()
    ) == (_fraction_ring_area(source))
    min_x, min_y, max_x, max_y = source.bounds
    for triangle in triangles:
        assert all(
            _stored_fraction(min_x) <= _stored_fraction(x) <= _stored_fraction(max_x)
            and _stored_fraction(min_y)
            <= _stored_fraction(y)
            <= _stored_fraction(max_y)
            for x, y in triangle.exterior.coords
        )


# ---------------------------------------------------------------------------
# Finding 7 — clip + buffer normals
# ---------------------------------------------------------------------------


def test_clip_diagonal_through_unit_box() -> None:
    line = gm.LineString([(-(2**53), -(2**53)), (2**53, 2**53)])
    clipped = line.clip_by_rect(0.0, 0.0, 1.0, 1.0)
    assert not clipped.is_empty
    # Exact segment (0,0)→(1,1).
    coords = list(clipped.coords)
    assert len(coords) >= 2
    assert coords[0] == (0.0, 0.0) or abs(coords[0][0]) < 1e-9
    assert abs(coords[-1][0] - 1.0) < 1e-9 and abs(coords[-1][1] - 1.0) < 1e-9


def test_buffer_extreme_segment_lengths_nonempty() -> None:
    for length in (1e-200, 1e155):
        stadium = gm.LineString([(0.0, 0.0), (length, 0.0)]).buffer(1.0)
        assert not stadium.is_empty, f'L={length}'
        assert stadium.area > 0.0


# ---------------------------------------------------------------------------
# Finding 8 — 3D distance + affine/snap rescues
# ---------------------------------------------------------------------------


def test_distance_3d_crossing_huge_axes_is_zero() -> None:
    a = gm.from_wkt('LINESTRING Z (-1e308 0 0, 1e308 0 0)')
    b = gm.from_wkt('LINESTRING Z (0 -1e308 0, 0 1e308 0)')
    assert gm.distance_3d(a, b) == 0.0


def test_distance_3d_huge_segment_to_offset_point() -> None:
    a = gm.from_wkt('LINESTRING Z (-1e308 0 0, 1e308 0 0)')
    p = gm.from_wkt('POINT Z (0 1 0)')
    got = gm.distance_3d(a, p)
    assert got == 1.0 or abs(got - 1.0) < 1e-9


def test_scale_zero_about_extreme_origin() -> None:
    p = gm.Point(1e308, 0.0)
    r = p.scale(0.0, origin=(-1e308, 0.0))
    assert r.x == -1e308
    assert r.y == 0.0


def test_affine_cancelling_products_at_1e308() -> None:
    p = gm.Point(1e308, 1e308)
    r = p.affine_transform([2.0, -2.0, 0.0, 1.0, 0.0, 0.0])
    assert r.x == 0.0
    assert r.y == 1e308


def test_scalar_affine_keeps_the_reciprocal_axis_end_to_end() -> None:
    """A scalar PyO3 affine call may not discard an independently live Y axis."""
    result = gm.Point(1e308, 1e-300).scale(0.5, origin=(-1e308, 0.0))
    assert _stored_fraction(result.x) == Fraction(0)
    assert _stored_fraction(result.y) == _stored_fraction(0.5 * 1e-300)


def test_snap_to_grid_extreme_origin_and_size() -> None:
    p = gm.Point(1e308, 0.0)
    r = p.snap_to_grid(1e308, origin=(-1e308, 0.0))
    # k = round((1e308 - (-1e308)) / 1e308) = round(2) = 2
    # result = -1e308 + 2*1e308 = 1e308
    assert r.x == 1e308


# ---------------------------------------------------------------------------
# Finding 9 — native Voronoi / MRR local frame
# ---------------------------------------------------------------------------


def test_native_voronoi_shifted_unrepresentable_partition_is_rejected() -> None:
    offset = 1e16
    sites = gm.MultiPoint([
        (offset + x, offset + y)
        for x, y in [
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (5.0, 5.0),
            (2.0, 8.0),
        ]
    ])
    with pytest.raises(
        gm.InvalidGeometryError,
        match='no topology-preserving binary64 embedding',
    ):
        sites.voronoi_polygons(clip='envelope')


def _independent_mrr_reference(
    points: list[tuple[float, float]],
) -> tuple[Fraction, tuple[Fraction, Fraction]]:
    """Exact stored-double MRR reference; no sqrt or decimal rounding."""
    stored = [(_stored_fraction(x), _stored_fraction(y)) for x, y in points]
    best: tuple[Fraction, tuple[Fraction, Fraction]] | None = None
    for index, (x1, y1) in enumerate(stored):
        for x2, y2 in stored[index + 1 :]:
            dx, dy = x2 - x1, y2 - y1
            norm_squared = dx * dx + dy * dy
            if norm_squared == 0:
                continue
            along = [dx * x + dy * y for x, y in stored]
            outward = [-dy * x + dx * y for x, y in stored]
            candidate = (
                (max(along) - min(along))
                * (max(outward) - min(outward))
                / norm_squared,
                (dx, dy),
            )
            if best is None or candidate[0] < best[0]:
                best = candidate
    assert best is not None
    return best


def test_mrr_framed_offsets_are_enclosing_and_reference_accurate() -> None:
    angles = (0.3, 1.1, 2.0, 2.9, 3.7, 4.5, 5.3)
    for base in (0.0, 5e6, 1e12, 1e15):
        points = [
            (base + 3.0 * math.cos(angle), 2.0 * math.sin(angle)) for angle in angles
        ]
        reference_area, reference_along = _independent_mrr_reference(points)
        result = gm.MultiPoint(points).minimum_rotated_rectangle()
        assert all(
            math.isfinite(value) for point in result.exterior.coords for value in point
        )
        _assert_mrr_encloses_exactly(result, points)
        expected_area = float(reference_area)
        # At 1e15 the x lattice itself is spaced by 0.125.  Exact enclosure
        # remains mandatory, but a finite binary64 corner cannot promise the
        # ideal real-valued area to a 0.1% tolerance.
        if base != 1e15:
            assert abs(result.area - expected_area) / expected_area <= 1e-3
        first, second = list(result.exterior.coords)[:2]
        length = math.hypot(second[0] - first[0], second[1] - first[1])
        if base != 1e15:
            reference_length = math.hypot(
                float(reference_along[0]), float(reference_along[1])
            )
            orientation = abs(
                (second[0] - first[0])
                / length
                * float(reference_along[0])
                / reference_length
                + (second[1] - first[1])
                / length
                * float(reference_along[1])
                / reference_length
            )
            assert orientation >= 1.0 - 1e-7


def test_mrr_extreme_aspect_boxes_stay_finite_and_enclosing() -> None:
    for rectangle in (
        gm.box(-1e308, -1.0, 1e308, 1.0),
        gm.box(-1e300, -1e-200, 1e300, 1e-200),
    ):
        result = rectangle.minimum_rotated_rectangle()
        assert all(
            math.isfinite(value) for point in result.exterior.coords for value in point
        )
        source = list(rectangle.exterior.coords)[:-1]
        _assert_mrr_encloses_exactly(result, source)


def test_mrr_extreme_box_is_identical_and_scaled_triangle_is_typed_error() -> None:
    source = gm.box(-1e308, -1.0, 1e308, 1.0)
    result = source.minimum_rotated_rectangle()
    assert gm.equals_identical(result, source)
    assert result.to_wkb() == source.to_wkb()
    assert result.area == math.inf

    scale = 1.01e308
    triangle = gm.MultiPoint([
        (-1.75 * scale, -1.75 * scale),
        (-1.5 * scale, -0.25 * scale),
        (-1.75 * scale, -1.5 * scale),
    ])
    with pytest.raises(
        gm.InvalidGeometryError,
        match='minimum_rotated_rectangle result is not representable with finite coordinates',
    ):
        triangle.minimum_rotated_rectangle()
    with pytest.raises(gm.InvalidGeometryError, match='result is not representable'):
        gm.GeometryArray([triangle]).minimum_rotated_rectangle()


def test_mrr_does_not_materialize_a_whole_hull_frame() -> None:
    points = [(-1e300, 0.0), (1e300, 0.0), (0.0, 1e-200)]
    result = gm.MultiPoint(points).minimum_rotated_rectangle()
    assert all(
        math.isfinite(value) for point in result.exterior.coords for value in point
    )
    _assert_mrr_encloses_exactly(result, points)


def test_mrr_anisotropic_support_frame_stays_minimal_and_finite() -> None:
    """A 1e300:1 oblique frame must not widen into an f64::MAX wedge."""
    points = [
        (-1.0359315529048135e-149, 8.846666935396596e149),
        (-9.300667887561583e-150, -9.452377501112966e149),
        (-9.298577042776529e-150, -4.7823143293451495e148),
        (-9.334344634826811e-150, 8.39652977224566e149),
    ]
    reference_area, _ = _independent_mrr_reference(points)
    result = gm.MultiPoint(points).minimum_rotated_rectangle()
    corners = list(result.exterior.coords)
    assert all(math.isfinite(value) for point in corners for value in point)
    assert all(
        abs(value) < float.fromhex('0x1.ffffffffffffep+1023')
        for point in corners
        for value in point
    )
    _assert_mrr_encloses_exactly(result, points)
    # The oracle derives its candidates from the stored binary64 coordinates,
    # not the decimal literals above. The emitted finite ring is within a few
    # arithmetic ulps of that exact-support optimum.
    assert result.area == pytest.approx(float(reference_area), rel=2e-14)


def test_mrr_ordinary_result_keeps_its_frozen_wkb() -> None:
    result = gm.box(0.0, 0.0, 10.0, 5.0)
    assert result.minimum_rotated_rectangle().to_wkb().hex() == (
        '01030000000100000005000000000000'
        '00000000000000000000000000000000'
        '00000024400000000000000000000000'
        '00000024400000000000001440000000'
        '00000000000000000000001440000000'
        '00000000000000000000000000'
    )


def test_mrr_nonzero_origin_unframe_keeps_its_frozen_wkb() -> None:
    """The scale-one path must still translate a nonzero support origin."""
    points = [(-1.75, -1.75), (-1.5, -0.25), (-1.75, -1.5)]
    result = gm.MultiPoint(points).minimum_rotated_rectangle()
    _assert_mrr_encloses_exactly(result, points)
    assert (
        result.to_wkb().hex()
        == '01030000000100000005000000000000000000fcbf000000000000fcbffffffffffffff7bff8ffffffffffcfbf618a7cd60da6f8bf20f259379822cfbf628a7cd60da6fcbf453eeb0653e4fbbf000000000000fcbf000000000000fcbf'
    )


# ---------------------------------------------------------------------------
# Finding 10 — DP scale invariance
# ---------------------------------------------------------------------------


def test_dp_keeps_bend_at_tiny_unit_and_huge_scale() -> None:
    """DP scale-invariance: 2x-tolerance bend kept at tiny, unit, AND huge."""
    # 1e160: tol² overflows to +inf — cold path must use raw tolerance.
    for scale in (1.0, 1e-100, 1e160):
        tol = scale * 0.1
        # Single bend of height 2*tol on a 3-vertex polyline — must keep the peak.
        coords = [
            (0.0, 0.0),
            (scale, 2.0 * tol),
            (2.0 * scale, 0.0),
        ]
        simplified = gm.LineString(coords).simplify(
            tol, method='dp', preserve_topology=False
        )
        n = len(list(simplified.coords))
        assert n == 3, f'scale={scale}: kept {n} vertices (bend removed)'


def test_vw_keeps_structure_at_tiny_unit_and_huge_scale() -> None:
    """VW chain frame: same keep count at unit and 1e±170 scales."""
    # 5-vertex polyline with one clear interior peak of height 2*tol.
    for scale in (1.0, 1e-100, 1e170):
        tol = 0.05 * scale
        coords = [
            (0.0, 0.0),
            (scale, 0.0),
            (2.0 * scale, 2.0 * tol),
            (3.0 * scale, 0.0),
            (4.0 * scale, 0.0),
        ]
        simplified = gm.LineString(coords).simplify(
            tol, method='vw', preserve_topology=False
        )
        n = len(list(simplified.coords))
        # Peak height 2*tol exceeds VW area threshold; must not collapse to ends only.
        assert n >= 3, f'scale={scale}: kept {n} vertices (over-collapsed)'


def test_vw_keeps_the_reciprocal_axis_peak() -> None:
    """VW's scalar public path retains a 1e-300 peak across a 1e300 span."""
    source = gm.LineString([(-1e300, 0.0), (0.0, 1e-300), (1e300, 0.0)])
    result = source.simplify(1.0, method='vw', preserve_topology=False)
    assert list(result.coords) == [(-1e300, 0.0), (0.0, 1e-300), (1e300, 0.0)]


def test_constrained_triangle_uses_the_exact_stored_double_sign() -> None:
    """Three reciprocal-axis corners are one constrained face, not an empty mesh."""
    corners = [(-1e300, -1e-300), (1e300, -1e-300), (0.0, 1e-300)]
    source = gm.Polygon(corners)
    triangles = source.triangulate(method='constrained')
    assert len(triangles) == 1
    assert _fraction_ring_area(triangles[0]) == _fraction_ring_area(source)
    assert gm.covers(source, triangles[0]) and gm.covers(triangles[0], source)


# ---------------------------------------------------------------------------
# Finding 12 — rhumb adjacent floats
# ---------------------------------------------------------------------------


def test_rhumb_adjacent_longitude_nonzero() -> None:
    lon1 = 45.0
    lon2 = math.nextafter(45.0, -math.inf)
    d = gm.rhumb_distance(
        gm.Point(lon1, 0.0, crs=4326),
        gm.Point(lon2, 0.0, crs=4326),
    )
    assert d > 0.0
    assert math.isfinite(d)
    # Δlon ~ 7e-15 deg → ~7e-10 m on the equator.
    assert 1e-12 < d < 1e-6


# ---------------------------------------------------------------------------
# Finding 14 — Catmull / miter relative
# ---------------------------------------------------------------------------


def test_catmull_rom_tiny_scale_midpoint() -> None:
    scale = 1e-40
    line = gm.LineString([
        (0.0, 0.0),
        (scale, 0.0),
        (2.0 * scale, 0.0),
        (3.0 * scale, 0.0),
    ])
    smooth = line.smooth(iterations=1, method='catmull_rom')
    # Must produce finite non-empty geometry (no absolute-epsilon collapse).
    assert not smooth.is_empty
    for x, y in smooth.coords:
        assert math.isfinite(x) and math.isfinite(y)


def test_miter_join_at_1e_6_scale() -> None:
    s = 1e-6
    # Right-angle polyline; buffer with miter should not silently bevel away area.
    line = gm.LineString([(0.0, 0.0), (s, 0.0), (s, s)])
    buf = line.buffer(s * 0.1, join_style='miter')
    assert not buf.is_empty
    assert buf.area > 0.0


# ---------------------------------------------------------------------------
# Finding 15 — polar stereographic
# ---------------------------------------------------------------------------


def test_polar_stereographic_inverse_huge_and_small_rho() -> None:
    # EPSG:3413 polar stereo → WGS84.
    lon, lat = gm.crs_transform(3413, 4326, 1e200, 1e200)
    assert math.isfinite(lon) and math.isfinite(lat)
    assert abs(abs(lat) - 90.0) < 1e-6 or abs(lat) > 89.0


# ---------------------------------------------------------------------------
# Finding 17 — roundtrip residual norms
# ---------------------------------------------------------------------------


def test_crs_roundtrip_tiny_and_huge_residuals() -> None:
    tiny = gm.crs_roundtrip(4326, 3857, 1e-200, 0.0)
    assert isinstance(tiny, (float, np.floating)) or hasattr(tiny, '__float__')
    # May be scalar or array depending on API.
    val = float(np.asarray(tiny).reshape(-1)[0])
    assert val > 0.0 or val == 0.0  # residual may be exactly representable
    # Non-zero residual at 1e-200 input is the interesting case when non-zero.
    assert math.isfinite(val)


# ---------------------------------------------------------------------------
# Finding 19 — S2 leaf area + geodesic Kahan (smoke)
# ---------------------------------------------------------------------------


def test_s2_leaf_area_positive_finite() -> None:
    cell = gm.S2Cell('80855c')
    assert cell.area > 0.0 and math.isfinite(cell.area)


def test_geodesic_shape_length_compensated_multipart() -> None:
    """Geodesic MultiLineString length (measure.rs compensated_sum) exceeds big-only."""
    # Shape-length fold only — NOT LRS prefixes (N11 closed; no blanket Kahan there).
    big = gm.LineString([(-10.0, 0.0), (10.0, 0.0)], crs=4326)
    tiny_coords = [(0.0, i * 1e-9) for i in range(101)]
    ml = gm.MultiLineString([list(big.coords), tiny_coords], crs=4326)
    assert ml.length > big.length


# ---------------------------------------------------------------------------
# Ordinary-magnitude bit-identity smoke (free-path claim)
# ---------------------------------------------------------------------------


def test_ordinary_delaunay_unit_square() -> None:
    square = gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)])
    tris = square.triangulate(method='delaunay')
    assert len(tris) == 2
    assert sum(t.area for t in tris) == 1.0


def test_ordinary_distance_bit_identical() -> None:
    assert gm.distance(gm.Point(0.0, 0.0), gm.Point(3.0, 4.0)) == 5.0
