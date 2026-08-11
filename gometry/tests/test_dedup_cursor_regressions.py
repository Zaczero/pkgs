"""Lane R6-L2 / R7-L2: correctness locks for the regression fixes.

Semantic / algebraic invariants live here. Wall-clock scaling assertions for
the same fixes live in ``benches/cases/`` (not pytest gates).
"""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest


def _ring32(ox: float = 0.0, oy: float = 0.0):
    # 32-vertex closed ring used by the algorithmic-cliff corpus.
    n = 31
    coords = [
        (ox + np.cos(2 * np.pi * i / n), oy + np.sin(2 * np.pi * i / n))
        for i in range(n)
    ]
    coords.append(coords[0])
    return gm.Polygon(coords)


# --- R1: noding dedup ordinary + duplicate-rich ---


def test_unique_linework_node_preserves_length():
    line = gm.LineString([(float(i), float(i % 3)) for i in range(32)])
    noded = line.node()
    # Unique input: noding must not invent/drop measure; length is preserved.
    assert noded.length == pytest.approx(line.length)
    assert not noded.is_empty


# Wall-clock scaling for duplicate-rich noding lives in
# `benches/cases/case_duplicate_noding_scale.py` (not a pytest gate).


def test_overlay_operators_provenance_shared_edge():
    left = gm.box(0, 0, 2, 2)
    right = gm.box(2, 0, 4, 2)
    assert gm.equals(left | right, gm.box(0, 0, 4, 2))
    assert (left & right).area == 0.0
    a = gm.box(0, 0, 2, 2)
    b = gm.box(1, 1, 3, 3)
    assert gm.equals(a & b, gm.box(1, 1, 2, 2))


# --- R2: Coordinates early-exit + linear generic path (ragged/mixed/masked) ---


def _line_array(n: int, *, distinct: bool = False) -> gm.GeometryArray:
    """Packed lines — the storage shape whose flat index is CSR (log rows).

    When ``distinct`` is True each row has a unique offset so window/gather
    slices are value-unequal when their row sets differ.
    """
    if distinct:
        return gm.GeometryArray([
            gm.LineString([(float(i + r), float((i + r) % 5)) for i in range(16)])
            for r in range(n)
        ])
    line = gm.LineString([(float(i), float(i % 5)) for i in range(16)])
    return gm.GeometryArray([line] * n)


def test_coordinates_index_stops_at_first_hit_on_gathered_lines():
    """Gathered packed lines: first-hit index must not re-walk the tail."""
    # Distinct per-row geometry so a gather drops coordinates that only
    # lived on the excluded rows.
    arr = gm.GeometryArray([
        gm.LineString([(float(i), 0.0), (float(i), 1.0)]) for i in range(500)
    ])
    gathered = arr[::2].coords  # even rows only
    assert gathered.index((0.0, 0.0)) == 0
    assert gathered.index((2.0, 0.0)) == 2  # second even row, first vertex
    with pytest.raises(ValueError, match='not in Coordinates'):
        gathered.index((1.0, 0.0))  # odd row dropped by gather
    # Large gather: first hit is still position 0.
    big = gm.GeometryArray([
        gm.LineString([(float(i), 0.0), (float(i), 1.0)]) for i in range(4000)
    ])[::2].coords
    assert big.index(big[0]) == 0


def test_coordinates_equality_and_count_across_storage_shapes():
    """Equal-value equality / count across identity/window/gather/mixed/masked.

    Pins the bulk path, not a packed-points-only shortcut: windowed and
    gathered line coords, mixed multiparts, and missing masks must agree.
    """
    lines = _line_array(200, distinct=True)
    window = lines[10:50].coords
    assert window == lines[10:50].coords
    assert window != lines[11:51].coords  # first-mismatch (distinct rows)

    gather = lines[::2].coords
    assert gather == lines[::2].coords
    assert len(gather) == 100 * 16
    assert gather.count(gather[0]) >= 1

    # Mixed multiparts
    mixed = gm.GeometryArray([
        gm.Point(float(i), 0.0)
        if i % 2 == 0
        else gm.LineString([(0.0, 0.0), (1.0, 1.0)])
        for i in range(40)
    ])
    assert mixed.coords == mixed.coords
    assert mixed.coords != lines[:20].coords

    # Masked: missing rows contribute no coordinates
    masked = gm.GeometryArray([
        None if i % 3 == 0 else gm.Point(float(i), 1.0) for i in range(30)
    ])
    dense = gm.GeometryArray([gm.Point(float(i), 1.0) for i in range(30) if i % 3 != 0])
    assert list(masked.coords) == list(dense.coords)
    assert masked.coords == dense.coords

    # Z/M equality (finite ordinates — constructors reject non-finite)
    zm = gm.LineString([(0.0, 0.0), (1.0, 1.0)], z=[1.5, 2.0], m=[3.0, 4.0])
    zm2 = gm.LineString([(0.0, 0.0), (1.0, 1.0)], z=[1.5, 2.0], m=[3.0, 4.0])
    assert zm.coords == zm2.coords
    assert (
        zm.coords
        != gm.LineString([(0.0, 0.0), (1.0, 1.0)], z=[1.5, 9.0], m=[3.0, 4.0]).coords
    )

    # Reversed / slice stay consistent with the flattened order
    rev = list(reversed(list(window)))
    assert list(window)[::-1] == rev
    assert window[:5] == list(window)[:5]
    assert list(window)[:16] == list(lines[10:11].coords)


def test_coordinates_equality_cross_row_map_storage():
    """Value-equal Coordinates must compare equal across row-map shapes.

    A bulk path that answers False solely because RowSelection maps differ
    (identity vs full window, window/gather vs rebuilt identity) is wrong:
    flattened list equality is the oracle.
    """
    lines = _line_array(80, distinct=True)
    # Identity vs full-span slice (often Window {0, n} rather than Identity).
    assert list(lines.coords) == list(lines[:].coords)
    assert lines.coords == lines[:].coords

    window = lines[10:40].coords
    rebuilt_window = gm.GeometryArray(list(lines[10:40])).coords
    assert list(window) == list(rebuilt_window)
    assert window == rebuilt_window

    gather = lines[::2].coords
    rebuilt_gather = gm.GeometryArray(list(lines[::2])).coords
    assert list(gather) == list(rebuilt_gather)
    assert gather == rebuilt_gather

    # Packed points: same cross-map contract.
    pts = gm.GeometryArray([gm.Point(float(i), float(i % 3)) for i in range(60)])
    assert list(pts.coords) == list(pts[:].coords)
    assert pts.coords == pts[:].coords
    g_pts = pts[::2].coords
    rebuilt_pts = gm.GeometryArray(list(pts[::2])).coords
    assert list(g_pts) == list(rebuilt_pts)
    assert g_pts == rebuilt_pts

    # Polygons: window vs rebuild.
    polys = gm.GeometryArray([
        gm.box(float(i), 0.0, float(i) + 1.0, 1.0) for i in range(40)
    ])
    pw = polys[5:25].coords
    pr = gm.GeometryArray(list(polys[5:25])).coords
    assert list(pw) == list(pr)
    assert pw == pr

    # Unequal content still False across maps (distinct rows required).
    assert lines[10:40].coords != lines[11:41].coords
    assert lines[::2].coords != lines[1::2].coords


def test_coordinates_equality_ignores_csr_absolute_offsets():
    """Flattened equality must not require matching CSR absolute offsets.

    Different row partitions (or different window prefixes) can share the same
    visible vertex stream. A bulk path that compares absolute offset tables
    returns a false negative against ``list(coords)`` as the oracle.
    """
    # Identity: one 4-vertex line vs two 2-vertex lines — same verts in order.
    one = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)])
    ])
    two = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
        gm.LineString([(2.0, 2.0), (3.0, 3.0)]),
    ])
    assert list(one.coords) == list(two.coords)
    assert one.coords == two.coords

    # Windows with different prefix lengths → different absolute CSR bases.
    left = gm.GeometryArray([
        gm.LineString([(9.0, 9.0), (9.0, 8.0)]),  # 2-vert prefix
        gm.LineString([(1.0, 0.0), (2.0, 0.0), (3.0, 0.0)]),
        gm.LineString([(4.0, 0.0), (5.0, 0.0)]),
    ])
    right = gm.GeometryArray([
        gm.LineString([(8.0, 8.0), (8.0, 7.0), (8.0, 6.0)]),  # 3-vert prefix
        gm.LineString([(1.0, 0.0), (2.0, 0.0), (3.0, 0.0)]),
        gm.LineString([(4.0, 0.0), (5.0, 0.0)]),
    ])
    lw, rw = left[1:].coords, right[1:].coords
    assert list(lw) == list(rw)
    assert lw == rw

    # Distinct trailing content still compares unequal.
    other = gm.GeometryArray([
        gm.LineString([(8.0, 8.0), (8.0, 7.0), (8.0, 6.0)]),
        gm.LineString([(1.0, 0.0), (2.0, 0.0), (9.0, 9.0)]),  # last vertex differs
        gm.LineString([(4.0, 0.0), (5.0, 0.0)]),
    ])
    assert list(left[1:].coords) != list(other[1:].coords)
    assert left[1:].coords != other[1:].coords


# Wall-clock scaling for gathered/windowed coords equality lives in
# `benches/cases/case_coordinates_equality_scale.py` (not a pytest gate).
# Deterministic value equality across storage shapes is covered by
# test_coordinates_equality_* above.


# --- R3: is_simple dense + nullable ---


def test_is_simple_dense_and_nullable_agree():
    simple = gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)])
    complex_ = gm.LineString([(0.0, 0.0), (1.0, 0.0), (0.5, 0.0), (1.5, 0.0)])
    dense = gm.GeometryArray([simple, complex_] * 50)
    d = dense.is_simple
    assert bool(d[0]) is True
    assert bool(d[1]) is False
    # Nullable: missing rows → False sentinel; present rows match dense pattern
    mixed = [None if i % 3 == 0 else ([simple, complex_][i % 2]) for i in range(100)]
    nullable = gm.GeometryArray(mixed)
    n = nullable.is_simple
    for i, row in enumerate(mixed):
        if row is None:
            assert bool(n[i]) is False
        else:
            assert bool(n[i]) is bool(d[i % 2])


# --- R4: small GC WKB round-trip ---


def test_small_gc_and_large_line_to_wkb_roundtrip():
    tiny = gm.GeometryCollection([gm.Point(1.0, 2.0), gm.Point(3.0, 4.0)])
    back = gm.from_wkb(tiny.to_wkb())
    assert gm.equals(back, tiny)
    large = gm.LineString([(float(i), float(i % 7)) for i in range(2000)])
    assert gm.equals(gm.from_wkb(large.to_wkb()), large)


# --- R5: missing-row pairwise — no materialization / cache warming ---


def test_fully_missing_pairwise_skips_materialization():
    """All-missing partner must leave the receiver's retained size unchanged.

    The observable consequence of skipping missing-row materialization and
    cache warming: ``__sizeof__`` of a mixed receiver is bit-stable across an
    all-missing pairwise predicate. Boolean all-False alone would stay green
    under a path that still warms every left-hand ShapeData cache — a present
    partner on the same left grows retained size substantially, so the flat
    all-missing result is the property under test.
    """
    # Mixed storage (heterogeneous kinds) so a present partner materializes
    # per-row ShapeData handles; packed homogeneous columns would not.
    rows = []
    for i in range(200):
        if i % 2 == 0:
            rows.append(gm.box(float(i), 0.0, float(i) + 1.0, 1.0))
        else:
            rows.append(
                gm.LineString([(float(i), 0.0), (float(i), 1.0), (float(i) + 1.0, 1.0)])
            )
    left = gm.GeometryArray(rows)
    miss = gm.GeometryArray([None] * 200)
    before = left.__sizeof__()
    out = gm.intersects(left, miss)
    after = left.__sizeof__()
    assert len(out) == 200
    assert not out.any()
    assert after == before, (
        f'all-missing partner grew receiver retained size {before} -> {after} '
        '(missing rows must not materialize / warm caches)'
    )
    # Control: a present partner on the same left MUST grow retained size
    # (otherwise the flat all-missing result is vacuous).
    present = gm.GeometryArray(rows)
    _ = gm.intersects(left, present)
    assert left.__sizeof__() > before, 'present partner should materialize caches'
    # Fresh left: second all-missing call still stable.
    left2 = gm.GeometryArray(rows)
    b2 = left2.__sizeof__()
    _ = gm.intersects(left2, miss)
    assert left2.__sizeof__() == b2


def test_partial_missing_pairwise_elementwise():
    left = gm.GeometryArray([gm.Point(0, 0), None, gm.Point(1, 1), gm.Point(2, 2)])
    right = gm.GeometryArray([gm.Point(0, 0), gm.Point(9, 9), None, gm.Point(2, 2)])
    out = list(gm.intersects(left, right))
    assert out == [True, False, False, True]
