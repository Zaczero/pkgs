"""Hierarchical point-containment index.

Covers hole-envelope skip semantics, multipolygon union (not XOR), many
sparse holes against an independent classification, repeated scalar probes
that switch from source scans to the index, and tall-edge construction that
must stay proportional (no dense-band O(E²) blow-up).
"""

from __future__ import annotations

import gometry as gm
import numpy as np


def _closed_square(x0, y0, x1, y1):
    return [(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]


def test_holed_polygon_batch_classification():
    outer = _closed_square(0, 0, 10, 10)
    hole = _closed_square(3, 3, 7, 7)
    poly = gm.Polygon(outer, [hole])
    # interior, in the hole, on the shell, outside, interior, on the hole edge
    xs = np.array([1.0, 5.0, 0.0, 20.0, 5.0, 3.0], dtype=np.float64)
    ys = np.array([1.0, 5.0, 5.0, 20.0, 1.0, 5.0], dtype=np.float64)
    np.testing.assert_array_equal(
        gm.contains(poly, x=xs, y=ys), [True, False, False, False, True, False]
    )
    np.testing.assert_array_equal(
        gm.intersects(poly, x=xs, y=ys), [True, False, True, False, True, True]
    )


def test_repeated_scalar_probes_answer_the_same_after_indexing():
    """The first probes scan the source, later ones use the index it builds."""
    poly = gm.Polygon(_closed_square(0, 0, 10, 10), [_closed_square(3, 3, 7, 7)])
    probes = [(1.0, 1.0, True, True), (5.0, 5.0, False, False), (3.0, 5.0, False, True)]
    for _ in range(4):
        for x, y, contains, intersects in probes:
            point = gm.Point(x, y)
            assert gm.contains(poly, point) is contains
            assert gm.intersects(poly, point) is intersects


def test_multipolygon_overlap_is_union_not_xor():
    a = gm.Polygon(_closed_square(0, 0, 1, 1))
    b = gm.Polygon(_closed_square(0.5, 0.5, 1.5, 1.5))
    multi = gm.MultiPolygon([a, b])
    # Overlap is interior of the union.
    assert gm.contains(multi, x=0.75, y=0.75)
    assert not gm.contains(multi, x=3.0, y=3.0)


def test_many_sparse_holes_match_an_independent_classification():
    shell = _closed_square(0, 0, 1000, 1000)
    holes = []
    for i in range(100):
        cx = 50.0 + (i % 10) * 90.0
        cy = 50.0 + (i // 10) * 90.0
        r = 10.0
        holes.append(_closed_square(cx - r, cy - r, cx + r, cy + r))
    poly = gm.Polygon(shell, holes)
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
    in_hole = np.zeros(xs.shape, dtype=bool)
    on_hole_edge = np.zeros(xs.shape, dtype=bool)
    for i in range(100):
        cx = 50.0 + (i % 10) * 90.0
        cy = 50.0 + (i // 10) * 90.0
        inside = (np.abs(xs - cx) < 10.0) & (np.abs(ys - cy) < 10.0)
        closed = (np.abs(xs - cx) <= 10.0) & (np.abs(ys - cy) <= 10.0)
        in_hole |= inside
        on_hole_edge |= closed & ~inside
    in_shell = (xs > 0.0) & (xs < 1000.0) & (ys > 0.0) & (ys < 1000.0)
    np.testing.assert_array_equal(
        gm.contains(poly, x=xs, y=ys), in_shell & ~in_hole & ~on_hole_edge
    )


def test_tall_edge_index_constructs_and_classifies():
    # Densified tall rectangle: many short vertical segments that would
    # amplify under unchecked dense-band replication.
    n_side = 5000
    pts = [(0.0, float(i)) for i in range(n_side + 1)]
    pts.append((1.0, float(n_side)))
    pts.extend((1.0, float(i)) for i in range(n_side - 1, -1, -1))
    pts.append((0.0, 0.0))
    poly = gm.Polygon(pts)
    assert gm.contains(poly, x=0.5, y=n_side * 0.5)
    assert not gm.contains(poly, x=-1.0, y=n_side * 0.5)
    assert not gm.contains(poly, x=0.5, y=-1.0)
    # Batch query stays responsive (no blow-up).
    xs = np.linspace(0.1, 0.9, 2000)
    ys = np.linspace(1.0, n_side - 1.0, 2000)
    mask = gm.contains(poly, x=xs, y=ys)
    assert mask.all()


def test_large_intersects_batch_keeps_boundary_semantics():
    ring = [
        (-4.0, -2.0),
        (-1.0, -4.0),
        (3.0, -3.0),
        (5.0, 0.0),
        (2.0, 4.0),
        (-2.0, 3.0),
        (-5.0, 1.0),
    ]
    polygon = gm.Polygon(ring)
    probes = [
        (0.0, 0.0),
        (1.0, 1.0),
        ring[0],
        (1.0, -3.5),
        (10.0, 10.0),
    ] * 2_400
    points = gm.GeometryArray([gm.Point(x, y) for x, y in probes])
    np.testing.assert_array_equal(
        gm.intersects(polygon, points),
        np.tile([True, True, True, True, False], 2_400),
    )
    np.testing.assert_array_equal(
        gm.contains(polygon, points),
        np.tile([True, True, False, False, False], 2_400),
    )
