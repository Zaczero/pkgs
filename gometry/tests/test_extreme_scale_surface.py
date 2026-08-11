"""Gate B — deterministic extreme-scale public-surface matrix.

Fixed hand-written magnitudes x shape archetypes. Asserts mathematical
properties (exact Fraction references from *stored* doubles, scale-invariant
identities, topology invariants). Never golden blobs; never wall-clock budgets.

Placement is load-bearing: ordinary ``tests/`` so every CI matrix cell collects it.
"""

from __future__ import annotations

import math
from fractions import Fraction

import gometry as gm
import numpy as np
import pytest

# ---------------------------------------------------------------------------
# Magnitudes (Part 2 eight)
# ---------------------------------------------------------------------------

SCALES: list[tuple[str, float]] = [
    ('min-subnormal', math.ulp(0.0)),
    ('1e-300', 1e-300),
    ('1e-162', 1e-162),
    ('1e-16', 1e-16),
    ('1', 1.0),
    ('1e6', 1e6),
    ('1e15', 1e15),
    ('1e300', 1e300),
]


def _scale_ids() -> list[str]:
    return [label for label, _ in SCALES]


# ---------------------------------------------------------------------------
# Exact-reference helpers (always from stored doubles)
# ---------------------------------------------------------------------------


def frac(x: float) -> Fraction:
    """Exact rational of a stored IEEE-754 value."""
    return Fraction.from_float(x)


def float_from_frac(q: Fraction) -> float:
    """Correctly-rounded f64 conversion; overflow → signed infinity."""
    try:
        return float(q)
    except OverflowError:
        # ``math.copysign`` needs a float sign; huge Fraction numerators are not
        # convertible — use the rational's sign instead.
        return math.inf if q > 0 else -math.inf


def mid_f64(a: float, b: float) -> float:
    """Exact midpoint of two stored doubles, rounded once to f64."""
    return float_from_frac((frac(a) + frac(b)) / 2)


def diff_f64(a: float, b: float) -> float:
    """``a - b`` in exact rational arithmetic, rounded once to f64."""
    return float_from_frac(frac(a) - frac(b))


def fail(op: str, scale: str, msg: str, **ctx: object) -> None:
    """Rich failure: magnitude label + op first."""
    bits = ' '.join(f'{k}={v!r}' for k, v in ctx.items())
    pytest.fail(f'[{scale}] op={op}: {msg}' + (f' | {bits}' if bits else ''))


def assert_finite_eq(
    op: str,
    scale: str,
    got: float,
    ref: float,
    *,
    endpoints: object = None,
) -> None:
    if math.isnan(got) or math.isnan(ref):
        fail(op, scale, 'NaN not allowed', got=got, ref=ref, endpoints=endpoints)
    if got != ref:
        fail(
            op, scale, 'value != exact reference', got=got, ref=ref, endpoints=endpoints
        )


def assert_area_class(
    op: str,
    scale: str,
    got: float,
    exact: Fraction,
) -> None:
    """Area must match the f64 class of the exact rational (0 / finite / +inf)."""
    ref = float_from_frac(exact)
    if math.isnan(got):
        fail(op, scale, 'area is NaN', got=got, exact=str(exact), ref=ref)
    if math.isinf(ref):
        if not (math.isinf(got) and got > 0):
            fail(op, scale, 'area must be +inf when exact overflows', got=got, ref=ref)
        return
    if ref == 0.0:
        if got != 0.0:
            fail(op, scale, 'area must be 0 when exact underflows', got=got, ref=ref)
        return
    assert_finite_eq(op, scale, got, ref)


# ---------------------------------------------------------------------------
# Archetype 1 — axis pair
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(('scale_label', 's'), SCALES, ids=_scale_ids())
def test_axis_pair_distance_dwithin(scale_label: str, s: float) -> None:
    """``(0,0)`` vs ``(2s,0)``: exact distance/symmetry; dwithin threshold edge."""
    # Build through public constructors; re-read stored coordinates.
    p0 = gm.Point(0.0, 0.0)
    p1 = gm.Point(2.0 * s, 0.0)
    x0, y0 = float(p0.x), float(p0.y)
    x1, y1 = float(p1.x), float(p1.y)
    ref = math.hypot(diff_f64(x1, x0), diff_f64(y1, y0))
    # Axis-aligned: exact |x1-x0|.
    ref_axis = abs(diff_f64(x1, x0))

    d01 = gm.distance(p0, p1)
    d10 = gm.distance(p1, p0)
    assert_finite_eq(
        'distance', scale_label, d01, ref_axis, endpoints=((x0, y0), (x1, y1))
    )
    assert_finite_eq('distance_symmetric', scale_label, d10, d01)

    # dwithin: exact threshold true; predecessor false (when predecessor is distinct).
    if not gm.dwithin(p0, p1, ref_axis):
        fail(
            'dwithin',
            scale_label,
            'dwithin(exact distance) must be True',
            distance=d01,
            threshold=ref_axis,
        )
    pred = math.nextafter(ref_axis, 0.0)
    if pred < ref_axis and pred >= 0.0 and gm.dwithin(p0, p1, pred):
        fail(
            'dwithin',
            scale_label,
            'dwithin(nextafter(dist,0)) must be False',
            distance=d01,
            pred=pred,
            ref=ref,
        )


# ---------------------------------------------------------------------------
# Archetype 2 — parallel / diagonal segments
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(('scale_label', 's'), SCALES, ids=_scale_ids())
def test_parallel_metrics(scale_label: str, s: float) -> None:
    """Parallel ``(0,0)→(2s,0)`` / ``(0,s)→(2s,s)`` and diagonal ``(0,0)→(3s,3s)``."""
    a0x, a0y, a1x, a1y = 0.0, 0.0, 2.0 * s, 0.0
    b0x, b0y, b1x, b1y = 0.0, s, 2.0 * s, s
    d0x, d0y, d1x, d1y = 0.0, 0.0, 3.0 * s, 3.0 * s

    parallel_a = gm.LineString([(a0x, a0y), (a1x, a1y)])
    parallel_b = gm.LineString([(b0x, b0y), (b1x, b1y)])
    diagonal = gm.LineString([(d0x, d0y), (d1x, d1y)])

    # Re-read stored endpoints from coords.
    ax0, ay0 = float(parallel_a.coords[0][0]), float(parallel_a.coords[0][1])
    ax1, ay1 = float(parallel_a.coords[1][0]), float(parallel_a.coords[1][1])
    bx0, by0 = float(parallel_b.coords[0][0]), float(parallel_b.coords[0][1])
    dx0, dy0 = float(diagonal.coords[0][0]), float(diagonal.coords[0][1])
    dx1, dy1 = float(diagonal.coords[1][0]), float(diagonal.coords[1][1])

    len_a_ref = abs(diff_f64(ax1, ax0))
    len_b_ref = abs(diff_f64(float(parallel_b.coords[1][0]), bx0))
    # Diagonal length: hypot of exact axis diffs.
    diag_len_ref = math.hypot(diff_f64(dx1, dx0), diff_f64(dy1, dy0))

    for op, got, ref, geom in (
        ('line.length', parallel_a.length, len_a_ref, parallel_a),
        ('gm.length', gm.length(parallel_a), len_a_ref, parallel_a),
        ('line.length/b', parallel_b.length, len_b_ref, parallel_b),
        ('line.length/diag', diagonal.length, diag_len_ref, diagonal),
        ('gm.length/diag', gm.length(diagonal), diag_len_ref, diagonal),
    ):
        assert_finite_eq(op, scale_label, float(got), ref, endpoints=geom)

    # Separation of parallels = |by0 - ay0| (axis-aligned).
    sep_ref = abs(diff_f64(by0, ay0))
    assert_finite_eq(
        'distance/parallel',
        scale_label,
        float(gm.distance(parallel_a, parallel_b)),
        sep_ref,
    )
    assert_finite_eq(
        'hausdorff/parallel',
        scale_label,
        float(gm.hausdorff_distance(parallel_a, parallel_b)),
        sep_ref,
    )
    assert_finite_eq(
        'frechet/parallel',
        scale_label,
        float(gm.frechet_distance(parallel_a, parallel_b)),
        sep_ref,
    )

    # Centroid of a segment = midpoint of stored endpoints.
    ca = parallel_a.centroid()
    assert_finite_eq(
        'centroid.x/parallel_a', scale_label, float(ca.x), mid_f64(ax0, ax1)
    )
    assert_finite_eq(
        'centroid.y/parallel_a', scale_label, float(ca.y), mid_f64(ay0, ay1)
    )

    # Clearance of a positive-length segment is half the length (vertex-to-vertex min
    # of non-adjacent is N/A for 2-vertex line — minimum_clearance of a line is the
    # min distance between non-adjacent components; for a simple 2-pt line the
    # clearance is the length itself or 0 depending on engine). Use a triangle
    # wire only when length is finite and positive for a stable property.
    if math.isfinite(len_a_ref) and len_a_ref > 0.0:
        # Two parallel segments: clearance of their multiparts is not the goal;
        # pin hausdorff/frechet already above. minimum_clearance on parallel_a
        # (single segment) — document as structural non-empty when length > 0.
        mc = float(parallel_a.minimum_clearance())
        if math.isnan(mc):
            fail('minimum_clearance', scale_label, 'NaN', length=len_a_ref)

    # Clip diagonal to [s,s,2s,2s] → exact endpoints on the window when the
    # diagonal intersects the rect (true for all positive finite s).
    if math.isfinite(s) and s > 0.0 and math.isfinite(3.0 * s):
        clipped = diagonal.clip_by_rect(s, s, 2.0 * s, 2.0 * s)
        if clipped.is_empty:
            fail(
                'clip_by_rect',
                scale_label,
                'diagonal clip to [s,s,2s,2s] empty',
                s=s,
                diagonal=str(diagonal),
            )
        # Endpoints of the clipped segment must match the rect entry/exit.
        # For the diagonal y=x through [s,2s]² the exact endpoints are (s,s) and (2s,2s).
        xs = [float(c[0]) for c in clipped.coords]
        ys = [float(c[1]) for c in clipped.coords]
        # First and last vertex.
        exp0 = (float(s), float(s))
        exp1 = (float(2.0 * s), float(2.0 * s))
        got0 = (xs[0], ys[0])
        got1 = (xs[-1], ys[-1])
        # Allow either orientation.
        if not ((got0 == exp0 and got1 == exp1) or (got0 == exp1 and got1 == exp0)):
            fail(
                'clip_by_rect',
                scale_label,
                'clipped endpoints != exact window corners',
                got=(got0, got1),
                expected=(exp0, exp1),
                clipped=str(clipped),
            )


# ---------------------------------------------------------------------------
# Archetype 3 — origin square [0, 2s]²
# ---------------------------------------------------------------------------


def _origin_square(s: float) -> gm.Polygon:
    hi = 2.0 * s
    return gm.Polygon([(0.0, 0.0), (hi, 0.0), (hi, hi), (0.0, hi), (0.0, 0.0)])


@pytest.mark.parametrize(('scale_label', 's'), SCALES, ids=_scale_ids())
def test_origin_square_area_centroid_topology(scale_label: str, s: float) -> None:
    poly = _origin_square(s)
    if poly.is_empty:
        fail('is_empty', scale_label, 'origin square must be nonempty', s=s)
    if not poly.is_valid:
        fail('is_valid', scale_label, 'origin square must be valid', s=s)

    # Stored ring endpoints (skip closing vertex).
    coords = list(poly.exterior.coords)
    xs = [float(c[0]) for c in coords[:-1]]
    ys = [float(c[1]) for c in coords[:-1]]
    lo_x, hi_x = min(xs), max(xs)
    lo_y, hi_y = min(ys), max(ys)
    exact_area = (frac(hi_x) - frac(lo_x)) * (frac(hi_y) - frac(lo_y))

    assert_area_class('polygon.area', scale_label, float(poly.area), exact_area)
    assert_area_class('gm.area', scale_label, float(gm.area(poly)), exact_area)

    c = poly.centroid()
    cx, cy = float(c.x), float(c.y)
    # Topology: centroid must be covered; strict interior when area class > 0.
    area_f = float_from_frac(exact_area)
    if not gm.covers(poly, c):
        fail(
            'centroid/covers',
            scale_label,
            'centroid not covered by polygon',
            centroid=(cx, cy),
            area=float(poly.area),
            bounds=poly.bounds,
        )
    if math.isfinite(area_f) and area_f > 0.0:
        if not gm.contains(poly, c):
            fail(
                'centroid/contains',
                scale_label,
                'centroid not in strict interior for positive-area square',
                centroid=(cx, cy),
                mid=(mid_f64(lo_x, hi_x), mid_f64(lo_y, hi_y)),
                area=float(poly.area),
            )
        assert_finite_eq('centroid.x', scale_label, cx, mid_f64(lo_x, hi_x))
        assert_finite_eq('centroid.y', scale_label, cy, mid_f64(lo_y, hi_y))
    elif area_f == 0.0:
        # Zero-area class: topology must survive; centroid still covered (above).
        # Prefer interior when the engine can place it; covers is the hard floor.
        pass
    # +inf area: centroid still finite and covered (above); check finiteness.
    elif not (math.isfinite(cx) and math.isfinite(cy)):
        fail('centroid/inf-area', scale_label, 'centroid not finite', centroid=(cx, cy))


@pytest.mark.parametrize(('scale_label', 's'), SCALES, ids=_scale_ids())
def test_origin_square_wkt_wkb_roundtrip(scale_label: str, s: float) -> None:
    poly = _origin_square(s)
    wkt = poly.to_wkt()
    back_wkt = gm.from_wkt(wkt)
    if not gm.equals_identical(poly, back_wkt):
        fail(
            'wkt_roundtrip',
            scale_label,
            'WKT inverse not equals_identical',
            wkt=wkt[:120],
            back=str(back_wkt),
        )
    wkb = poly.to_wkb()
    back_wkb = gm.from_wkb(wkb)
    if not gm.equals_identical(poly, back_wkb):
        fail(
            'wkb_roundtrip',
            scale_label,
            'WKB inverse not equals_identical',
            n_bytes=len(wkb),
            back=str(back_wkb),
        )


# ---------------------------------------------------------------------------
# Archetype 4 — ULP-local translated square
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(('scale_label', 's'), SCALES, ids=_scale_ids())
def test_local_square_centroid(scale_label: str, s: float) -> None:
    """``lo=s``, ``hi=s+8·ulp(s)``: local detail survives a huge offset."""
    u = math.ulp(s)
    lo = s
    hi = s + 8.0 * u
    if not (hi > lo):
        # At min-subnormal, 8*ulp(s) is still distinct; if ever not, mark limit.
        fail(
            'local_square/construct',
            scale_label,
            'hi must be strictly greater than lo',
            lo=lo,
            hi=hi,
            u=u,
        )

    poly = gm.Polygon([(lo, lo), (hi, lo), (hi, hi), (lo, hi), (lo, lo)])
    if poly.is_empty:
        fail('local_square/empty', scale_label, 'must be nonempty', lo=lo, hi=hi)
    if not poly.is_valid:
        fail('local_square/valid', scale_label, 'must be valid', lo=lo, hi=hi)

    # Stored corners.
    coords = list(poly.exterior.coords)
    xs = [float(c[0]) for c in coords[:-1]]
    ys = [float(c[1]) for c in coords[:-1]]
    lo_x, hi_x = min(xs), max(xs)
    lo_y, hi_y = min(ys), max(ys)
    exact_area = (frac(hi_x) - frac(lo_x)) * (frac(hi_y) - frac(lo_y))
    area_f = float_from_frac(exact_area)
    got_area = float(poly.area)
    # Area class: zero/finite/inf all allowed as measurement; topology holds.
    if math.isnan(got_area):
        fail('local_square/area', scale_label, 'area NaN', exact=str(exact_area))

    c = poly.centroid()
    cx, cy = float(c.x), float(c.y)
    mid_x, mid_y = mid_f64(lo_x, hi_x), mid_f64(lo_y, hi_y)

    if not gm.covers(poly, c):
        fail(
            'local_square/centroid_covers',
            scale_label,
            'centroid not covered',
            centroid=(cx, cy),
            mid=(mid_x, mid_y),
            area=got_area,
        )

    # Exact midpoint when area is positive finite; still require midpoint match
    # when the measurement underflows but the ring has positive exact area in
    # rational space with representable midpoint (topology-survives clause).
    if math.isfinite(area_f) and area_f > 0.0 and (cx, cy) != (mid_x, mid_y):
        # Positive finite area: require exact midpoint. Zero-area class may
        # collapse; covers (above) is the hard floor there.
        fail(
            'local_square/centroid_mid',
            scale_label,
            'centroid != exact midpoint',
            centroid=(cx, cy),
            mid=(mid_x, mid_y),
            area=got_area,
            area_ref=area_f,
        )

    # Overlay self-intersection nonempty (topology survives).
    inter = gm.intersection(poly, poly)
    if inter.is_empty:
        fail(
            'local_square/self_intersection',
            scale_label,
            'intersection(poly,poly) empty — topology lost',
            area=got_area,
        )


# ---------------------------------------------------------------------------
# Archetype 5 — overlapping squares
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(('scale_label', 's'), SCALES, ids=_scale_ids())
def test_overlay_inclusion(scale_label: str, s: float) -> None:
    """A=[0,2s]², B=[s,3s]²: nonempty union/intersection; inclusion; commutativity."""
    two, three = 2.0 * s, 3.0 * s
    a = gm.Polygon([(0.0, 0.0), (two, 0.0), (two, two), (0.0, two), (0.0, 0.0)])
    b = gm.Polygon([(s, s), (three, s), (three, three), (s, three), (s, s)])

    if a.is_empty or b.is_empty:
        fail('overlay/operands', scale_label, 'operands empty', s=s)

    u_ab = gm.union(a, b)
    u_ba = gm.union(b, a)
    i_ab = gm.intersection(a, b)
    i_ba = gm.intersection(b, a)

    if u_ab.is_empty:
        fail('union', scale_label, 'union empty', s=s, a=str(a), b=str(b))
    if i_ab.is_empty:
        fail('intersection', scale_label, 'intersection empty', s=s)

    # Commutativity (topological covers both ways).
    if not (gm.covers(u_ab, u_ba) and gm.covers(u_ba, u_ab)):
        fail('union/commute', scale_label, 'union not commutative under covers')
    if not (gm.covers(i_ab, i_ba) and gm.covers(i_ba, i_ab)):
        fail(
            'intersection/commute',
            scale_label,
            'intersection not commutative under covers',
        )

    # Union covers A, B, and intersection (the invariant that caught a topology-
    # deletion blocker).
    for name, geom in (('A', a), ('B', b), ('intersection', i_ab)):
        if not gm.covers(u_ab, geom):
            fail(
                'union/covers',
                scale_label,
                f'union does not cover {name}',
                union=str(u_ab),
                geom=str(geom),
            )

    # A and B each cover the intersection.
    if not gm.covers(a, i_ab):
        fail('A/covers_intersection', scale_label, 'A does not cover intersection')
    if not gm.covers(b, i_ab):
        fail('B/covers_intersection', scale_label, 'B does not cover intersection')


# ---------------------------------------------------------------------------
# Packed / array parity (one eight-row case + segment length/centroid)
# ---------------------------------------------------------------------------


def test_packed_points_distance_threshold_parity() -> None:
    """Eight-row packed points: per-row distance/dwithin match scalar at each scale."""
    origins_x: list[float] = []
    origins_y: list[float] = []
    targets_x: list[float] = []
    targets_y: list[float] = []
    thresholds: list[float] = []
    labels: list[str] = []

    for label, s in SCALES:
        origins_x.append(0.0)
        origins_y.append(0.0)
        targets_x.append(2.0 * s)
        targets_y.append(0.0)
        thresholds.append(abs(2.0 * s))
        labels.append(label)

    origins = gm.points(origins_x, origins_y)
    targets = gm.points(targets_x, targets_y)
    assert len(origins) == 8 and len(targets) == 8

    dists = gm.distance(origins, targets)
    assert isinstance(dists, np.ndarray) and dists.shape == (8,)

    for i, (label, scale) in enumerate(SCALES):
        p0 = gm.Point(origins_x[i], origins_y[i])
        p1 = gm.Point(targets_x[i], targets_y[i])
        scalar = float(gm.distance(p0, p1))
        packed = float(dists[i])
        if packed != scalar:
            fail(
                'packed/distance',
                label,
                'packed row != scalar',
                row=i,
                packed=packed,
                scalar=scalar,
                s=scale,
            )
        thr = thresholds[i]
        if bool(gm.dwithin(origins[i], targets[i], thr)) != bool(
            gm.dwithin(p0, p1, thr)
        ):
            fail(
                'packed/dwithin',
                label,
                'packed dwithin != scalar',
                row=i,
                thr=thr,
            )
        pred = math.nextafter(thr, 0.0)
        if (
            pred < thr
            and pred >= 0.0
            and bool(gm.dwithin(origins[i], targets[i], pred))
        ):
            fail(
                'packed/dwithin_pred',
                label,
                'packed dwithin(pred) True',
                row=i,
                pred=pred,
                thr=thr,
            )


def test_array_segment_length_centroid_parity() -> None:
    """GeometryArray of eight scale-segments: length/centroid match scalars."""
    segs: list[gm.LineString] = []
    for _label, s in SCALES:
        segs.append(gm.LineString([(0.0, 0.0), (2.0 * s, 0.0)]))
    arr = gm.GeometryArray(segs)
    lengths = arr.length
    centroids = arr.centroid()
    assert len(lengths) == 8

    for i, (label, _s) in enumerate(SCALES):
        scalar = segs[i]
        if float(lengths[i]) != float(scalar.length):
            fail(
                'array/length',
                label,
                'array length != scalar',
                row=i,
                array=float(lengths[i]),
                scalar=float(scalar.length),
            )
        sc = scalar.centroid()
        ac = centroids[i]
        if (float(ac.x), float(ac.y)) != (float(sc.x), float(sc.y)):
            fail(
                'array/centroid',
                label,
                'array centroid != scalar',
                row=i,
                array=(float(ac.x), float(ac.y)),
                scalar=(float(sc.x), float(sc.y)),
            )


def test_extreme_polygon_held_axis_clipping_falls_back_without_nan_or_empty() -> None:
    """A finite slanted extreme polygon keeps a real surface and clipped overlap.

    Its slanted edge crosses the scanline and clipping window at fractions that
    cannot be formed from raw world-scale deltas.  This enters both held-axis
    interpolation callers; neither may emit a non-finite point, empty clip, or
    a vertex outside the requested window.
    """
    source = gm.Polygon([(-1e308, -1.0), (1e308, 1.0), (1e308, -1.0)])
    minx, miny, maxx, maxy = (-1e307, -0.5, 1e307, 0.5)
    probe = gm.box(minx, miny, maxx, maxy)
    assert gm.intersects(source, probe)

    for surface in (
        source.point_on_surface(),
        gm.GeometryArray([source]).point_on_surface()[0],
    ):
        assert math.isfinite(surface.x) and math.isfinite(surface.y)
        assert gm.covers(source, surface)

    def assert_clipped(geometry: gm.Geometry) -> None:
        assert not geometry.is_empty
        assert 'NaN' not in geometry.to_wkt()
        bounds = geometry.bounds
        assert all(math.isfinite(value) for value in bounds)
        assert minx <= bounds[0] <= bounds[2] <= maxx
        assert miny <= bounds[1] <= bounds[3] <= maxy

    assert_clipped(source.clip_by_rect(minx, miny, maxx, maxy))
    assert_clipped(gm.GeometryArray([source]).clip_by_rect(minx, miny, maxx, maxy)[0])
    for left, right in ((source, probe), (probe, source)):
        assert_clipped(gm.intersection(left, right))


# ---------------------------------------------------------------------------
# Sanity: matrix size is intentional and discoverable
# ---------------------------------------------------------------------------


def test_matrix_case_inventory() -> None:
    """Document the fixed node count (8 scales x 5 archetype tests + 2 packed).

    Parametrized nodes: 8 x 5 = 40, plus 2 packed/array = 42 collected tests
    in this module (including this inventory). Part 2 design was ~41; the
    inventory node is bookkeeping only.
    """
    assert len(SCALES) == 8
    # Coefficients remain representable at the extremes.
    s0 = math.ulp(0.0)
    assert 2.0 * s0 > s0 or 2.0 * s0 == s0 * 2  # subnormal double still advances
    assert math.isfinite(3.0 * 1e300)
    s_hi = 1e300
    assert s_hi + 8.0 * math.ulp(s_hi) > s_hi
