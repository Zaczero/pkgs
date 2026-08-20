"""Hierarchical prepared point-containment index (Lane Q4).

Covers hole-envelope skip semantics, multipolygon union (not XOR), many
sparse holes vs unprepared, and tall-edge construction that must stay
proportional (no dense-band O(E²) blow-up).
"""

from __future__ import annotations

import gometry as gm
import numpy as np


def _closed_square(x0, y0, x1, y1):
    return [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]


def test_prepared_holed_matches_unprepared_batch():
    outer = _closed_square(0, 0, 10, 10)
    hole = _closed_square(3, 3, 7, 7)
    poly = gm.Polygon(outer, [hole])
    prep = poly.prepare()
    xs = np.array([1.0, 5.0, 0.0, 20.0, 5.0, 3.0], dtype=np.float64)
    ys = np.array([1.0, 5.0, 5.0, 20.0, 1.0, 5.0], dtype=np.float64)
    np.testing.assert_array_equal(
        gm.contains_xy(prep, xs, ys), gm.contains_xy(poly, xs, ys)
    )
    np.testing.assert_array_equal(
        gm.intersects_xy(prep, xs, ys), gm.intersects_xy(poly, xs, ys)
    )


def test_multipolygon_overlap_is_union_not_xor():
    a = gm.Polygon(_closed_square(0, 0, 1, 1))
    b = gm.Polygon(_closed_square(0.5, 0.5, 1.5, 1.5))
    multi = gm.MultiPolygon([a, b])
    prep = multi.prepare()
    # Overlap is interior of the union.
    assert gm.contains_xy(prep, 0.75, 0.75)
    assert not gm.contains_xy(prep, 3.0, 3.0)


def test_many_sparse_holes_match_unprepared():
    shell = _closed_square(0, 0, 1000, 1000)
    holes = []
    for i in range(100):
        cx = 50.0 + (i % 10) * 90.0
        cy = 50.0 + (i // 10) * 90.0
        r = 10.0
        holes.append(_closed_square(cx - r, cy - r, cx + r, cy + r))
    poly = gm.Polygon(shell, holes)
    prep = poly.prepare()
    # Explicit deterministic mesh (no seeded RNG): a regular grid plus a few
    # hole-boundary and shell-edge probes that exercise interior/boundary/exterior.
    grid = np.linspace(0.0, 1000.0, 45)
    gx, gy = np.meshgrid(grid, grid, indexing='xy')
    xs = gx.ravel()
    ys = gy.ravel()
    extra_x = np.array([50.0, 50.0, 0.0, 1000.0, 500.0, 140.0], dtype=np.float64)
    extra_y = np.array([50.0, 60.0, 500.0, 500.0, 0.0, 50.0], dtype=np.float64)
    xs = np.concatenate([xs, extra_x])
    ys = np.concatenate([ys, extra_y])
    np.testing.assert_array_equal(
        gm.contains_xy(prep, xs, ys), gm.contains_xy(poly, xs, ys)
    )


def test_tall_edge_prepared_constructs_and_classifies():
    # Densified tall rectangle: many short vertical segments that would
    # amplify under unchecked dense-band replication.
    n_side = 5000
    pts = [(0.0, float(i)) for i in range(n_side + 1)]
    pts.append((1.0, float(n_side)))
    pts.extend((1.0, float(i)) for i in range(n_side - 1, -1, -1))
    pts.append((0.0, 0.0))
    poly = gm.Polygon(pts)
    prep = poly.prepare()
    assert gm.contains_xy(prep, 0.5, n_side * 0.5)
    assert not gm.contains_xy(prep, -1.0, n_side * 0.5)
    assert not gm.contains_xy(prep, 0.5, -1.0)
    # Batch query stays responsive (no blow-up).
    xs = np.linspace(0.1, 0.9, 2000)
    ys = np.linspace(1.0, n_side - 1.0, 2000)
    mask = gm.contains_xy(prep, xs, ys)
    assert mask.all()


def test_prepared_point_index_preserves_contains_predicate_behavior():
    polygon = gm.box(0, 0, 2, 2)
    prepared = polygon.prepare()
    assert gm.contains(prepared, gm.Point(1, 1))
    assert not gm.contains(prepared, gm.Point(3, 3))
