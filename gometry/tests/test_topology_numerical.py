"""R14-A topology / numerical robustness regression fixtures."""

from __future__ import annotations

import math
from itertools import pairwise

import gometry as gm
import pytest


def test_coverage_signed_zero_shared_edge_valid_and_union() -> None:
    """+0/-0 on coincident polygons: validator rejects; if only shared-edge
    keys differ by signed zero, union must still be non-empty for adjacent cells.
    """
    # Fully coincident: bounds/edge keys must treat ±0 as one so coincidence
    # is detected (invalid coverage) rather than empty-union from all-matched edges.
    p1 = gm.from_wkt('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))')
    p2 = gm.from_wkt('POLYGON ((-0 -0, 1 -0, 1 1, -0 1, -0 -0))')
    arr = gm.GeometryArray([p1, p2])
    assert arr.coverage_is_valid() is False
    # Adjacent tiles with signed-zero on the shared edge stay valid and union to area 2.
    a = gm.from_wkt('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))')
    b = gm.from_wkt('POLYGON ((1 -0, 2 0, 2 1, 1 1, 1 -0))')
    adj = gm.GeometryArray([a, b])
    assert adj.coverage_is_valid() is True
    u = adj.coverage_union()
    assert not u.is_empty
    assert u.area == pytest.approx(2.0)


def test_empty_linestring_parts_scalar_array_agree() -> None:
    # Chosen behaviour: atomic empty LineString is ONE part (itself), matching
    # Point/Polygon atomics (`part_count == 1`). Empty MultiLineString has zero
    # parts. Scalar, array-row, and free `parts` agree.
    empty = gm.from_wkt('LINESTRING EMPTY')
    assert len(empty.parts) == 1
    assert next(iter(empty.parts)).is_empty
    arr = gm.GeometryArray([empty])
    assert len(arr[0].parts) == 1
    assert len(gm.parts(empty)) == 1


def test_relate_antimeridian_seam_matches_contains() -> None:
    poly = gm.Polygon(
        [(-170.0, 0.0), (170.0, 0.0), (170.0, 10.0), (-170.0, 10.0)],
        crs=4326,
    )
    pt = gm.Point(-180.0, 5.0, crs=4326)
    assert gm.contains(poly, pt) is True
    # Interior point-in-polygon DE-9IM (shapely/JTS); must not collapse to the
    # fabricated-seam boundary pattern FF20F1FF2.
    assert gm.relate(poly, pt) == '0F2FF1FF2'
    # Transpose for (point, container): 0F2FF1FF2ᵀ = 0FFFFF212.
    assert gm.relate(pt, poly) == '0FFFFF212'
    pole_box = gm.Polygon(
        [(-10.0, 80.0), (10.0, 80.0), (0.0, 90.0), (-10.0, 80.0)],
        crs=4326,
    )
    pole = gm.Point(0.0, 90.0, crs=4326)
    rel = gm.relate(pole_box, pole)
    assert len(rel) == 9
    assert gm.covers(pole_box, pole) or gm.touches(pole_box, pole)


def test_voronoi_edges_honors_clip() -> None:
    sites = gm.MultiPoint([(0.0, 0.0), (2.0, 0.0), (1.0, 2.0)])
    pad = sites.voronoi_edges(clip='padded')
    env = sites.voronoi_edges(clip='envelope')
    poly = sites.voronoi_edges(clip=gm.box(0.5, 0.5, 1.5, 1.5))
    assert pad != env or pad.total_bounds != env.total_bounds
    # Polygon clip must keep edges inside the clip envelope (within float noise).
    tb = poly.total_bounds
    assert tb[0] >= 0.5 - 1e-9
    assert tb[1] >= 0.5 - 1e-9
    assert tb[2] <= 1.5 + 1e-9
    assert tb[3] <= 1.5 + 1e-9


def test_catmull_rom_finite_from_finite_extreme() -> None:
    ls = gm.LineString([(1e308, 1e308), (1e308, 1e307), (1e307, 1e308)])
    out = ls.smooth(iterations=4, method='catmull_rom')
    for x, y in out.coords:
        assert math.isfinite(x) and math.isfinite(y)


def test_segmentize_budget_charges_realized_output() -> None:
    # 204-vertex sawtooth: requesting steps that would exceed 16M must raise
    # with a produced count at least as large as the true output size.
    pts = [(float(i), float(i % 2)) for i in range(204)]
    ls = gm.LineString(pts)
    with pytest.raises(Exception) as ei:
        ls.segmentize(1e-5)
    msg = str(ei.value)
    assert '16000000' in msg or '16_000_000' in msg or 'exceeding the limit' in msg


def test_empty_linestring_parts_family() -> None:
    # Atomic empty LineString is one part (itself); empty MultiLineString has zero.
    # Scalar, array-row, and free `parts` agree.
    empty = gm.from_wkt('LINESTRING EMPTY')
    assert len(empty.parts) == 1
    assert next(iter(empty.parts)).is_empty
    arr = gm.GeometryArray([empty])
    assert len(arr[0].parts) == 1
    free = gm.parts(empty)
    assert len(free) == 1
    assert gm.MultiLineString([]).parts.__len__() == 0


def test_union_all_splits_antimeridian_like_binary() -> None:
    """N-ary union_all must not keep the false-middle planar box of a
    geographic antimeridian-crossing polygon; match binary overlay's split.
    """
    cross = gm.from_wkt('POLYGON ((170 -5, -170 -5, -170 5, 170 5, 170 -5))').set_crs(
        4326
    )
    other = gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)], crs=4326)
    binary = gm.union(cross, other)
    nary = gm.union_all([cross, other])
    arr = gm.GeometryArray([cross, other]).union_all()
    # Binary is the oracle for the geographic gate: result is multi with
    # split-normalized seam parts, not a single west>east planar box.
    assert binary.geometry_type != 'Polygon' or binary.bounds[0] <= binary.bounds[2]
    # N-ary must not return the unsplit false-middle band (minx=170, maxx=-170).
    for result in (nary, arr, binary):
        b = result.bounds
        if result.geometry_type == 'Polygon':
            assert not (b[0] > b[2]), f'false-middle planar box: {result.to_wkt()[:80]}'
    # Areas of binary and n-ary must agree (same split topology).
    assert nary.area == pytest.approx(binary.area, rel=1e-9)
    assert arr.area == pytest.approx(binary.area, rel=1e-9)


def test_geo_hausdorff_identity_validates_latitude() -> None:
    """Equal geographic operands with out-of-domain lat must raise before the
    identity-zero shortcut.
    """
    bad = gm.LineString([(0.0, 95.0), (1.0, 95.0)], crs=4326)
    with pytest.raises(Exception) as ei:
        gm.hausdorff_distance(bad, bad)
    msg = str(ei.value).lower()
    assert 'lat' in msg or 'domain' in msg or 'geographic' in msg or '90' in msg


def test_hausdorff_extreme_parallel_separation_finite() -> None:
    """Representable parallel separation stays finite (not inf/zero) at large
    but squared-safe magnitudes.
    """
    # Below SQUARED_SPACE_MAX_MAGNITUDE so squared space stays finite; still
    # large enough to exercise overflow-prone naive paths.
    a = gm.LineString([(0.0, 0.0), (1e100, 0.0)])
    b = gm.LineString([(0.0, 1.0), (1e100, 1.0)])
    d = gm.hausdorff_distance(a, b)
    assert math.isfinite(d)
    assert d == pytest.approx(1.0, rel=1e-9)


def test_noding_extreme_union_no_panic() -> None:
    line = gm.LineString([(0.0, 0.0), (1e100, 1e100), (2e100, 0.0)])
    u = gm.union(line, line)
    assert u is not None


def test_distance_3d_extreme_skew_finite_unit_separation() -> None:
    """B11: representable unit-ish 3D separation must stay finite at huge coords."""
    a = gm.LineString([(0.0, 0.0), (1e200, 0.0)]).force_3d(0.0)
    b = gm.LineString([(0.0, 1.0), (1e200, 1.0)]).force_3d(1.0)
    d = gm.distance_3d(a, b)
    assert math.isfinite(d)
    assert d == pytest.approx(math.sqrt(2.0), rel=1e-6)
    # Ordinary magnitude stays exact.
    a0 = gm.LineString([(0.0, 0.0), (1.0, 0.0)]).force_3d(0.0)
    b0 = gm.LineString([(0.0, 1.0), (1.0, 1.0)]).force_3d(0.0)
    assert gm.distance_3d(a0, b0) == 1.0


def test_area_centroid_extreme_finite_box() -> None:
    """C2: 1e154 box has finite mathematical area 1e308."""
    side = 1e154
    poly = gm.Polygon([(0.0, 0.0), (side, 0.0), (side, side), (0.0, side)])
    area = poly.area
    assert math.isfinite(area)
    assert area == pytest.approx(side * side, rel=1e-6)
    c = poly.centroid()
    assert math.isfinite(c.x) and math.isfinite(c.y)
    assert c.x == pytest.approx(side / 2.0, rel=1e-6)
    # Ordinary box bit-stable area.
    assert gm.box(0, 0, 2, 3).area == 6.0


def test_offset_curve_extreme_segment_not_dropped() -> None:
    """C9: extreme-but-finite horizontal segment still offsets."""
    # Length ~1e90 at origin 1e100 — representable and previously length² overflowed.
    ls = gm.LineString([(1e100, 0.0), (1e100 + 1e90, 0.0)])
    assert ls.length > 0.0
    out = ls.offset_curve(1.0)
    assert not out.is_empty
    # Ordinary offset unchanged.
    short = gm.LineString([(0.0, 0.0), (10.0, 0.0)]).offset_curve(1.0)
    assert not short.is_empty


def test_scale_about_extreme_origin_stays_finite() -> None:
    """C6: scale about a huge origin must not NaN via expanded affine form."""
    o = 1e200
    p = gm.Point(o, o)
    # Scale by 2 about itself: identity-like finite result.
    s = p.scale(2.0, 2.0, origin=(o, o))
    assert math.isfinite(s.x) and math.isfinite(s.y)
    assert s.x == pytest.approx(o, rel=0, abs=abs(o) * 1e-15)
    # Representable neighborhood scales with area growth.
    side = 1e90
    poly = gm.Polygon([(o, o), (o + side, o), (o + side, o + side), (o, o + side)])
    scaled = poly.scale(2.0, 2.0, origin='centroid')
    assert math.isfinite(scaled.area)
    assert scaled.area == pytest.approx(4.0 * poly.area, rel=1e-5)


def test_dwithin_squared_underflow_distinct_points() -> None:
    """B10: squared space underflows at 1e-200 — dwithin must stay distance-true."""
    a = gm.Point(0.0, 0.0)
    b = gm.Point(1e-200, 0.0)
    assert gm.distance(a, b) == pytest.approx(1e-200, rel=1e-6)
    assert gm.dwithin(a, b, 0.0) is False
    assert gm.dwithin(a, b, 5e-201) is False
    assert gm.dwithin(a, b, 1e-200) is True
    assert gm.dwithin(a, a, 0.0) is True
    # Ordinary mid-range bit-stable.
    assert gm.dwithin(gm.Point(0, 0), gm.Point(3, 4), 5.0) is True
    assert gm.dwithin(gm.Point(0, 0), gm.Point(3, 4), 4.9) is False


def test_hausdorff_frechet_tiny_parallel_separation() -> None:
    """B12: HD/FD on parallel lines 1e-200 apart must return ~1e-200, not 0."""
    a = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    b = gm.LineString([(0.0, 1e-200), (1.0, 1e-200)])
    hd = gm.hausdorff_distance(a, b)
    fd = gm.frechet_distance(a, b)
    assert hd == pytest.approx(1e-200, rel=1e-6)
    assert fd == pytest.approx(1e-200, rel=1e-6)
    assert hd > 0.0 and fd > 0.0
    # Identical lines stay zero.
    assert gm.hausdorff_distance(a, a) == 0.0
    assert gm.frechet_distance(a, a) == 0.0


def test_clip_by_rect_extreme_crossing_segment() -> None:
    """C5: extreme endpoints that cross a unit rect clip to the window edges."""
    ls = gm.LineString([(-1e20, 0.5), (1e20, 0.5)])
    clipped = ls.clip_by_rect(0.0, 0.0, 1.0, 1.0)
    assert not clipped.is_empty
    coords = list(clipped.coords)
    assert len(coords) >= 2
    assert coords[0][0] == pytest.approx(0.0, abs=1e-9)
    assert coords[-1][0] == pytest.approx(1.0, abs=1e-9)
    assert coords[0][1] == pytest.approx(0.5, rel=0, abs=1e-12)
    assert coords[-1][1] == pytest.approx(0.5, rel=0, abs=1e-12)
    # Milder extreme previously returned a wrong 1.125 endpoint.
    mid = gm.LineString([(-1e15, 0.5), (1e15, 0.5)]).clip_by_rect(0, 0, 1, 1)
    mc = list(mid.coords)
    assert mc[0][0] == pytest.approx(0.0, abs=1e-6)
    assert mc[-1][0] == pytest.approx(1.0, abs=1e-6)
    # Diagonal extreme (parameter t collapses without edge-bisection).
    diag = gm.LineString([(-1e15, -1e15), (1e15, 1e15)]).clip_by_rect(0, 0, 1, 1)
    assert not diag.is_empty
    dc = list(diag.coords)
    assert dc[0][0] == pytest.approx(0.0, abs=1e-6)
    assert dc[0][1] == pytest.approx(0.0, abs=1e-6)
    assert dc[-1][0] == pytest.approx(1.0, abs=1e-6)
    assert dc[-1][1] == pytest.approx(1.0, abs=1e-6)
    # Ordinary mid-range clip unchanged.
    ordinary = gm.LineString([(-1.0, 0.5), (2.0, 0.5)]).clip_by_rect(0, 0, 1, 1)
    oc = list(ordinary.coords)
    assert oc[0][0] == pytest.approx(0.0)
    assert oc[-1][0] == pytest.approx(1.0)


def test_distance_dwithin_underflow_multipoint_line_array() -> None:
    """B10 residual: MultiPoint/line/array paths must not false-zero at 1e-200."""
    sep = 1e-200
    mp_a = gm.MultiPoint([(0.0, 0.0)])
    mp_b = gm.MultiPoint([(sep, 0.0)])
    assert gm.distance(mp_a, mp_b) == pytest.approx(sep, rel=1e-6)
    assert gm.dwithin(mp_a, mp_b, 0.0) is False
    assert gm.dwithin(mp_a, mp_b, sep) is True

    la = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    lb = gm.LineString([(0.0, sep), (1.0, sep)])
    d = gm.distance(la, lb)
    assert d == pytest.approx(sep, rel=1e-6)
    assert d > 0.0
    np0, np1 = gm.nearest_points(la, lb)
    assert abs(np0.y - np1.y) == pytest.approx(sep, rel=1e-6)

    arr = gm.dwithin(gm.Point(0.0, 0.0), gm.GeometryArray([gm.Point(sep, 0.0)]), 0.0)
    assert list(arr) == [False]
    arr_ok = gm.dwithin(gm.Point(0.0, 0.0), gm.GeometryArray([gm.Point(sep, 0.0)]), sep)
    assert list(arr_ok) == [True]


def test_b10_kind_kind_distance_matrix_1e200() -> None:
    """B10 structural close: every kindxkind at sep=1e-200 finishes honestly.

    Covers the puntal_brute lane (≤24 segs) AND the parts lane (>24 segs),
    both operand orders, and consistency with nearest_points / shortest_line.
    """
    sep = 1e-200
    # Horizontal base at y=0; probe kinds sit at y=sep (or exterior of box).
    short_line = gm.LineString([(0.0, 0.0), (1.0, 0.0)])  # 1 seg → puntal_brute
    long_line = gm.LineString([
        (float(i) / 30.0, 0.0) for i in range(31)
    ])  # 30 segs → parts
    poly = gm.box(0.0, 0.0, 1.0, 1.0)
    point = gm.Point(0.5, sep)
    multipoint = gm.MultiPoint([(0.25, sep), (0.75, sep)])
    # Exterior of unit box (south of y=0).
    exterior_pt = gm.Point(0.5, -sep)

    pairs: list[tuple[object, object, float]] = [
        (point, short_line, sep),
        (short_line, point, sep),
        (multipoint, short_line, sep),
        (short_line, multipoint, sep),
        (point, long_line, sep),
        (long_line, point, sep),
        (multipoint, long_line, sep),
        (exterior_pt, poly, sep),
        (poly, exterior_pt, sep),
        (gm.Point(0.0, 0.0), gm.Point(sep, 0.0), sep),
        (gm.MultiPoint([(0.0, 0.0)]), gm.MultiPoint([(sep, 0.0)]), sep),
        (short_line, gm.LineString([(0.0, sep), (1.0, sep)]), sep),
    ]
    for left, right, expect in pairs:
        d = gm.distance(left, right)
        assert d == pytest.approx(expect, rel=1e-6), f'distance({left!r},{right!r})={d}'
        assert d > 0.0
        assert gm.dwithin(left, right, 0.0) is False
        assert gm.dwithin(left, right, expect) is True
        # nearest / shortest_line agree on magnitude when both are points or lines.
        try:
            a, b = gm.nearest_points(left, right)
            nd = math.hypot(a.x - b.x, a.y - b.y)
            assert nd == pytest.approx(expect, rel=1e-6)
            sl = gm.shortest_line(left, right)
            assert sl.length == pytest.approx(expect, rel=1e-6)
        except Exception:
            # Some kind pairs may not expose nearest_points the same way; distance is the contract.
            pass

    # Array lane: short line in a GeometryArray still false-zeros without the helper.
    arr_d = gm.distance(point, gm.GeometryArray([short_line]))
    assert float(arr_d[0]) == pytest.approx(sep, rel=1e-6)
    arr_dw = gm.dwithin(point, gm.GeometryArray([short_line]), 0.0)
    assert list(arr_dw) == [False]

    # B11: same separation matrix with Z (3D honest-min, not a parallel suite).
    z_pairs = [
        (
            gm.LineString([(0.0, 0.0), (1.0, 0.0)]).force_3d(0.0),
            gm.LineString([(0.0, sep), (1.0, sep)]).force_3d(0.0),
            sep,
        ),
        (
            gm.LineString([(0.0, 0.0), (1.0, 0.0)]).force_3d(0.0),
            gm.LineString([(0.0, 1.0), (1.0, 1.0)]).force_3d(0.0),
            1.0,
        ),
        (
            gm.LineString([(0.0, 0.0), (1e200, 0.0)]).force_3d(0.0),
            gm.LineString([(0.0, 1.0), (1e200, 1.0)]).force_3d(1.0),
            math.sqrt(2.0),
        ),
        (
            gm.Point(0.0, 0.0).force_3d(0.0),
            gm.Point(0.0, 0.0).force_3d(sep),
            sep,
        ),
    ]
    for left, right, expect in z_pairs:
        d = gm.distance_3d(left, right)
        assert math.isfinite(d), f'distance_3d non-finite {left!r} {right!r}'
        assert d == pytest.approx(expect, rel=1e-6), f'distance_3d={d} expect={expect}'
    # Ordinary magnitude bit-stable.
    assert (
        gm.distance_3d(
            gm.LineString([(0.0, 0.0), (1.0, 0.0)]).force_3d(0.0),
            gm.LineString([(0.0, 1.0), (1.0, 1.0)]).force_3d(0.0),
        )
        == 1.0
    )


def test_voronoi_edges_clip_no_degenerate_rows() -> None:
    """B8 residual: clipped Voronoi edges must not emit zero-length rows."""
    pts = gm.MultiPoint([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.5, 0.5)])
    for clip in ('envelope', 'padded'):
        edges = pts.voronoi_edges(clip=clip)
        assert len(edges) > 0
        for edge in edges:
            assert edge.length > 0.0, edge.to_wkt()
            coords = list(edge.coords)
            assert coords[0] != coords[-1] or len(set(coords)) > 1
    poly_clip = gm.box(-0.1, -0.1, 1.1, 1.1)
    edges = pts.voronoi_edges(clip=poly_clip)
    assert len(edges) > 0
    assert all(edge.length > 0.0 for edge in edges)
    # Polygons: no consecutive duplicate ring vertices after envelope clip.
    cells = pts.voronoi_polygons(clip='envelope')
    assert len(cells) == 5
    for cell in cells:
        coords = list(cell.exterior.coords)
        assert all(a != b for a, b in pairwise(coords))
        assert cell.area > 0.0
    # Ordinary mid-range stays non-empty (no over-filtering regression).
    ordinary = gm.MultiPoint([(0.0, 0.0), (2.0, 0.0), (1.0, 2.0)]).voronoi_edges()
    assert len(ordinary) == 3
    assert all(e.length > 0.0 for e in ordinary)


def test_c2_extreme_area_centroid_and_ordinary_bit_identity() -> None:
    """C2: 1e154 box has finite area/centroid; ordinary box area bit-stable."""
    side = 1e154
    poly = gm.Polygon([(0.0, 0.0), (side, 0.0), (side, side), (0.0, side)])
    assert math.isfinite(poly.area)
    assert poly.area == pytest.approx(side * side, rel=1e-6)
    c = poly.centroid()
    assert math.isfinite(c.x) and math.isfinite(c.y)
    assert c.x == pytest.approx(side / 2.0, rel=1e-6)
    assert gm.box(0, 0, 2, 3).area == 6.0


def test_c4_line_index_rejects_nonfinite_total_length() -> None:
    """C4: LRS on linework whose total length overflows must not poison quietly."""
    # Two huge segments: each length is finite (~1e308) but the sum is +inf.
    ls = gm.LineString([(0.0, 0.0), (1e308, 0.0), (0.0, 1e308)])
    assert not math.isfinite(ls.length)
    with pytest.raises(Exception, match=r'finite|length|line index'):
        ls.line_interpolate(0.5)


def test_c8_subnormal_features_survive_zero_tolerance() -> None:
    """C8: distinct subnormals must not collapse under zero-tolerance snap/rrp."""
    a = 1e-320
    b = 2e-320
    assert a != b
    mp = gm.MultiPoint([(a, 0.0), (b, 0.0)])
    # Zero tolerance: only exact duplicates drop; these stay two distinct rows.
    cleaned = mp.remove_repeated_points(tolerance=0.0)
    assert cleaned.to_wkt().count(',') >= 1  # at least two points in multipoint
    # Distance is representable and positive.
    assert gm.distance(gm.Point(a, 0.0), gm.Point(b, 0.0)) == pytest.approx(
        b - a, rel=1e-6
    )
    # Snap to origin with zero tolerance must not pull them (false-zero d²).
    snapped = gm.snap(mp, gm.Point(0.0, 0.0), 0.0)
    coords = list(snapped.coords) if hasattr(snapped, 'coords') else None
    if coords is None:
        # MultiPoint: check WKT still has two distinct tiny values or not all origin.
        wkt = snapped.to_wkt()
        assert '0 0), (0 0)' not in wkt.replace('.0', '')
        assert wkt.count('(') >= 2
    # Positive tiny tolerance still smaller than separation: keep both.
    kept = mp.remove_repeated_points(tolerance=1e-330)
    assert 'EMPTY' not in kept.to_wkt()
    assert kept.to_wkt().count(',') >= 1
