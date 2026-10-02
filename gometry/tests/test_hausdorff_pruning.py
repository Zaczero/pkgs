"""Analytic bounded-contract checks for the pruned Hausdorff kernel."""

from __future__ import annotations

import math
import subprocess
import sys
import textwrap

import gometry as gm
import pytest
import shapely

_HAUSDORFF_CHILD_TIMEOUT_SECONDS = 10.0

_INVALID_POLYGON_POINT_CASES = (
    pytest.param(
        'POLYGON ((0 0, 4 4, 0 4, 4 0, 0 0))',
        'POINT (10 10)',
        id='bow-tie',
    ),
    pytest.param(
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0, -3 0, 0 0))',
        'POINT (-100 0)',
        id='spike',
    ),
    pytest.param(
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (6 1, 6 2, 7 2, 7 1, 6 1))',
        'POINT (100 0)',
        id='hole-outside-shell',
    ),
)


def _run_hausdorff_child(left_wkt: str, right_wkt: str) -> tuple[str, str]:
    script = textwrap.dedent(
        f"""
        import gometry as gm

        left = gm.from_wkt({left_wkt!r})
        right = gm.from_wkt({right_wkt!r})
        try:
            value = gm.hausdorff_distance(left, right)
        except gm.GeometryError as error:
            print(f"error:{{type(error).__name__}}:{{error}}")
        else:
            print(f"answer:{{value:.17g}}")
        """
    )
    completed = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        check=False,
        text=True,
        timeout=_HAUSDORFF_CHILD_TIMEOUT_SECONDS,
    )
    assert completed.returncode == 0, completed.stderr
    mode, separator, result = completed.stdout.strip().partition(':')
    assert separator and mode in {'answer', 'error'}, completed.stdout
    return mode, result


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


def test_filled_square_to_its_boundary_attains_the_interior_center() -> None:
    square = gm.box(0.0, 0.0, 4.0, 4.0)
    boundary = gm.LineString([
        (0.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (0.0, 4.0),
        (0.0, 0.0),
    ])
    center = gm.Point(2.0, 2.0)

    actual = gm.hausdorff_distance(square, boundary, max_error=1e-12)

    assert actual == pytest.approx(2.0, abs=1e-12)
    assert gm.distance(center, boundary, unit='planar') == 2.0


def test_hausdorff_empty_and_degenerate_edges() -> None:
    assert (
        gm.hausdorff_distance(
            gm.Point(0, 0), gm.GeometryCollection([]), max_error=1e-12
        )
        == math.inf
    )
    assert gm.hausdorff_distance(gm.Point(0, 0), gm.Point(0, 0), max_error=1e-12) == 0.0


def test_invalid_polygon_pair_hausdorff_finishes_with_answer_or_typed_error() -> None:
    mode, result = _run_hausdorff_child(
        'POLYGON ((6 5, 4 6, 2 2, 6 6, 7 7, 7 8, 7 0, 6 5))',
        'POLYGON ((5 3, 0 2, 1 8, 1 0, 3 1, 5 3), (8 2, 8 2, 1 6, 2 3, 2 1, 8 2))',
    )

    if mode == 'answer':
        assert math.isfinite(float(result))
    else:
        assert result.startswith(('GeometryError:', 'InvalidGeometryError:'))


def test_hausdorff_f64_max_endpoint_returns_a_finite_answer() -> None:
    mode, result = _run_hausdorff_child(
        'POINT (-1 1e-15)',
        'LINESTRING (1e15 0, 1.7976931348623157e308 0)',
    )

    assert mode == 'answer'
    assert float(result) == sys.float_info.max


def test_hausdorff_unrepresentable_f64_max_diagonal_returns_typed_error() -> None:
    mode, result = _run_hausdorff_child(
        'POINT (0 0)',
        'LINESTRING (1 1, 1.7976931348623157e308 1.7976931348623157e308)',
    )

    assert mode == 'error'
    assert result.startswith(
        'InvalidGeometryError:hausdorff_distance cannot represent the effective accuracy'
    )


@pytest.mark.parametrize(('polygon_wkt', 'point_wkt'), _INVALID_POLYGON_POINT_CASES)
def test_invalid_integer_polygon_hausdorff_matches_densified_shapely(
    polygon_wkt: str,
    point_wkt: str,
) -> None:
    mode, result = _run_hausdorff_child(polygon_wkt, point_wkt)
    assert mode == 'answer', result

    expected = shapely.hausdorff_distance(
        shapely.from_wkt(polygon_wkt),
        shapely.from_wkt(point_wkt),
        densify=0.01,
    )
    assert float(result) == pytest.approx(expected, rel=1e-9, abs=1e-12)


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
