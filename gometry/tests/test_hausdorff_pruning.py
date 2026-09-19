"""Analytic bounded-contract checks for the pruned Hausdorff kernel."""

from __future__ import annotations

import math

import gometry as gm

_ANALYTIC_CASES = (
    (
        'convex_boxes',
        gm.box(0, 0, 10, 10),
        gm.box(5, 5, 15, 15),
        math.sqrt(50.0),
    ),
    (
        'long_segment',
        gm.LineString([(0, 0), (100, 0)]),
        gm.Point(50, 1),
        math.sqrt(2501.0),
    ),
    (
        'identical',
        gm.box(0, 0, 10, 10),
        gm.box(0, 0, 10, 10),
        0.0,
    ),
    (
        'interior_peak',
        gm.LineString([(0, 0), (10, 0)]),
        gm.LineString([(0, 1), (5, 8), (10, 1)]),
        8.0,
    ),
    (
        'nested_holes',
        gm.Polygon(
            [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
            holes=[[(2, 2), (8, 2), (8, 8), (2, 8), (2, 2)]],
        ),
        gm.Polygon(
            [(1, 1), (11, 1), (11, 11), (1, 11), (1, 1)],
            holes=[[(3, 3), (9, 3), (9, 9), (3, 9), (3, 3)]],
        ),
        math.sqrt(2.0),
    ),
    ('single_point', gm.Point(0, 0), gm.Point(3, 4), 5.0),
    (
        'two_vertex',
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(0, 1), (1, 1)]),
        1.0,
    ),
)


def test_hausdorff_pruning_matches_independent_analytic_bounds() -> None:
    max_error = 1e-12
    for key, left, right, expected in _ANALYTIC_CASES:
        actual = gm.hausdorff_distance(left, right, max_error=max_error)
        if expected == 0.0:
            assert actual == 0.0, (key, actual)
        else:
            assert abs(actual - expected) <= max_error, (
                key,
                actual,
                expected,
                max_error,
            )


def test_continuous_hausdorff_retains_discrete_vertex_control() -> None:
    """The discrete metric is a control, never the bounded-contract oracle."""
    vertices = [(0.0, 0.0), (100.0, 0.0)]
    long_line = gm.LineString(vertices)
    endpoints = gm.MultiPoint(vertices)
    max_error = 1e-12
    actual = gm.hausdorff_distance(long_line, endpoints, max_error=max_error)
    expected = 50.0
    assert abs(actual - expected) <= max_error

    # A vertex-only metric sees the same two stored points and returns zero;
    # the continuous support includes the segment midpoint, 50 units from
    # either target point.
    discrete = max(
        min(math.dist(source, target) for target in vertices) for source in vertices
    )
    assert discrete == 0.0
    assert actual > discrete


def test_hausdorff_empty_and_degenerate_edges() -> None:
    assert (
        gm.hausdorff_distance(
            gm.Point(0, 0), gm.GeometryCollection([]), max_error=1e-12
        )
        == math.inf
    )
    assert gm.hausdorff_distance(gm.Point(0, 0), gm.Point(0, 0), max_error=1e-12) == 0.0


def test_hausdorff_packed_array_matches_scalar() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (10.0, 0.0), (10.0, 5.0)]),
        gm.LineString([(0.0, 1.0), (5.0, 8.0), (10.0, 1.0)]),
    ])
    max_error = 1e-12
    scalar = [
        gm.hausdorff_distance(a, b, max_error=max_error)
        for a, b in zip(lines, lines[::-1], strict=True)
    ]
    bulk = gm.hausdorff_distance(lines, lines[::-1], max_error=max_error).tolist()
    # This is a scalar/packed dispatch parity check, not a geometry oracle.
    for left, right in zip(scalar, bulk, strict=True):
        assert abs(left - right) <= max_error
