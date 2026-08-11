"""Behavioral matrix for CRS-aware metric and constructive operations.

For every practical metric operation family:

* US-survey-foot CRS (EPSG:2263) returns/accepts **native feet** by default
* ``unit='meters'`` scales by the foot→metre factor
* CRS-free geometries reject ``unit='meters'``
* local-projection ops accept foot inputs at a plausible magnitude

The explicit operation sets make failures local and readable without shipping
a private runtime registry solely to inspect our own API.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any

import gometry as gm
import numpy as np
import pytest

FOOT_CRS = 2263
FOOT_UNIT_M = 0.3048006096012192


EXACT_OPS = sorted({
    'area',
    'length',
    'length_3d',
    'distance',
    'distance_3d',
    'dwithin',
    'hausdorff_distance',
    'frechet_distance',
    'nearest_points',
    'shortest_line',
    'interpolate_m',
    'destination',
    'point_between',
    'line_interpolate',
    'line_substring',
    'line_locate',
})
LOCAL_OPS = sorted({
    'buffer',
    'offset_curve',
    'concave_hull',
    'polylabel',
    'maximum_inscribed_circle',
    'maximum_inscribed_radius',
    'minimum_bounding_circle',
    'minimum_bounding_radius',
    'minimum_clearance',
    'minimum_clearance_line',
})


# ---------------------------------------------------------------------------
# Fixtures shared across ops
# ---------------------------------------------------------------------------


def _foot_point(x: float = 0.0, y: float = 0.0) -> gm.Point:
    return gm.Point(x, y, crs=FOOT_CRS)


def _foot_line(length: float = 10.0) -> gm.LineString:
    return gm.LineString([(0.0, 0.0), (length, 0.0)], crs=FOOT_CRS)


def _foot_line_offset(dy: float = 1.0) -> gm.LineString:
    return gm.LineString([(0.0, dy), (10.0, dy)], crs=FOOT_CRS)


def _foot_box(side: float = 10.0) -> gm.Polygon:
    return gm.box(0.0, 0.0, side, side, crs=FOOT_CRS)


def _free_point(x: float = 0.0, y: float = 0.0) -> gm.Point:
    return gm.Point(x, y)


def _free_line(length: float = 10.0) -> gm.LineString:
    return gm.LineString([(0.0, 0.0), (length, 0.0)])


def _free_box(side: float = 10.0) -> gm.Polygon:
    return gm.box(0.0, 0.0, side, side)


def _as_length(value: Any) -> float:
    """Normalize a metric result to a comparable length/distance magnitude."""
    if isinstance(value, (int, float, np.floating)):
        return float(value)
    if isinstance(value, gm.Geometry):
        # Geometry-valued metrics (shortest_line, circles, clearance line, …)
        # report length/radius via .length or bounds diagonal when empty.
        if value.geometry_type == 'Point':
            return 0.0
        length = value.length
        if math.isfinite(length) and length > 0.0:
            return float(length)
        # Circles / polygons: use sqrt(area/pi) as a radius proxy when needed.
        area = value.area
        if math.isfinite(area) and area > 0.0:
            return math.sqrt(area / math.pi)
        return 0.0
    if isinstance(value, tuple) and len(value) == 2:
        left, right = value
        if isinstance(left, gm.Geometry) and isinstance(right, gm.Geometry):
            return float(gm.distance(left, right))
    raise TypeError(f'cannot extract length from {type(value)!r}: {value!r}')


# Each exact op: (native_runner, meters_runner, free_meters_runner, expected_native)
# runners take no args; free_meters_runner must raise on unit='meters'.
ExactCase = tuple[
    str,
    Callable[[], Any],
    Callable[[], Any],
    Callable[[], Any],
    float,
]


def _exact_cases() -> list[ExactCase]:
    cases: list[ExactCase] = []

    # area — free function only for unit= override (property is unit-free)
    unit_sq = _foot_box(1.0)
    free_sq = _free_box(1.0)
    cases.append((
        'area',
        lambda: unit_sq.area,
        lambda: gm.area(unit_sq, unit='meters'),
        lambda: gm.area(free_sq, unit='meters'),
        1.0,
    ))

    line = _foot_line(10.0)
    free_line = _free_line(10.0)
    cases.append((
        'length',
        lambda: line.length,
        lambda: gm.length(line, unit='meters'),
        lambda: gm.length(free_line, unit='meters'),
        10.0,
    ))

    # The 3D family belongs to this matrix for the same reason the 2D one does,
    # and its absence is exactly why `length_3d` shipped returning SI metres
    # from a foot CRS while `length` returned feet — for the very same line.
    line_3d = gm.LineString([(0.0, 0.0, 0.0), (10.0, 0.0, 0.0)], crs=FOOT_CRS)
    free_line_3d = gm.LineString([(0.0, 0.0, 0.0), (10.0, 0.0, 0.0)])
    cases.append((
        'length_3d',
        lambda: line_3d.length_3d,
        lambda: gm.length_3d(line_3d, unit='meters'),
        lambda: gm.length_3d(free_line_3d, unit='meters'),
        10.0,
    ))

    a_3d = gm.Point(0.0, 0.0, z=0.0, crs=FOOT_CRS)
    b_3d = gm.Point(10.0, 0.0, z=0.0, crs=FOOT_CRS)
    free_a_3d = gm.Point(0.0, 0.0, z=0.0)
    free_b_3d = gm.Point(10.0, 0.0, z=0.0)
    cases.append((
        'distance_3d',
        lambda: gm.distance_3d(a_3d, b_3d),
        lambda: gm.distance_3d(a_3d, b_3d, unit='meters'),
        lambda: gm.distance_3d(free_a_3d, free_b_3d, unit='meters'),
        10.0,
    ))

    a, b = _foot_point(0, 0), _foot_point(10, 0)
    fa, fb = _free_point(0, 0), _free_point(10, 0)
    cases.append((
        'distance',
        lambda: gm.distance(a, b),
        lambda: gm.distance(a, b, unit='meters'),
        lambda: gm.distance(fa, fb, unit='meters'),
        10.0,
    ))
    cases.append((
        'dwithin',
        lambda: float(gm.dwithin(a, b, 10.0)),
        # Use the measured metre distance so float rounding cannot miss the edge.
        lambda: float(
            gm.dwithin(a, b, gm.distance(a, b, unit='meters'), unit='meters')
        ),
        lambda: gm.dwithin(fa, fb, 10.0, unit='meters'),
        1.0,  # True
    ))

    line2 = _foot_line_offset(1.0)
    free_line2 = gm.LineString([(0.0, 1.0), (10.0, 1.0)])
    cases.append((
        'hausdorff_distance',
        lambda: gm.hausdorff_distance(line, line2),
        lambda: gm.hausdorff_distance(line, line2, unit='meters'),
        lambda: gm.hausdorff_distance(free_line, free_line2, unit='meters'),
        1.0,
    ))
    cases.append((
        'frechet_distance',
        lambda: gm.frechet_distance(line, line2),
        lambda: gm.frechet_distance(line, line2, unit='meters'),
        lambda: gm.frechet_distance(free_line, free_line2, unit='meters'),
        1.0,
    ))
    cases.append((
        'nearest_points',
        lambda: gm.nearest_points(a, b),
        lambda: gm.nearest_points(a, b, unit='meters'),
        lambda: gm.nearest_points(fa, fb, unit='meters'),
        10.0,
    ))
    cases.append((
        'shortest_line',
        lambda: gm.shortest_line(a, b),
        lambda: gm.shortest_line(a, b, unit='meters'),
        lambda: gm.shortest_line(fa, fb, unit='meters'),
        10.0,
    ))

    cases.append((
        'line_locate',
        lambda: line.line_locate(_foot_point(5, 0)),
        lambda: line.line_locate(_foot_point(5, 0), unit='meters'),
        lambda: free_line.line_locate(_free_point(5, 0), unit='meters'),
        5.0,
    ))
    cases.append((
        'line_interpolate',
        lambda: line.line_interpolate(1.0).x,
        lambda: line.line_interpolate(FOOT_UNIT_M, unit='meters').x,
        lambda: free_line.line_interpolate(1.0, unit='meters'),
        1.0,
    ))
    cases.append((
        'line_substring',
        lambda: line.line_substring(0.0, 1.0).length,
        lambda: line.line_substring(0.0, FOOT_UNIT_M, unit='meters').length,
        lambda: free_line.line_substring(0.0, 1.0, unit='meters'),
        1.0,
    ))
    cases.append((
        'interpolate_m',
        lambda: line.interpolate_m(0.0, 10.0).max_m,
        # Measure values are the caller-supplied start/end; unit= scales how
        # those map onto the line length, not the stored M labels.
        lambda: line.interpolate_m(0.0, FOOT_UNIT_M, unit='meters').max_m,
        lambda: free_line.interpolate_m(0.0, 10.0, unit='meters'),
        10.0,
    ))
    cases.append((
        'destination',
        lambda: gm.destination(a, 90.0, 1.0).x,
        lambda: gm.destination(a, 90.0, FOOT_UNIT_M, unit='meters').x,
        lambda: gm.destination(fa, 90.0, 1.0, unit='meters'),
        1.0,
    ))
    cases.append((
        'point_between',
        lambda: gm.point_between(a, b, 1.0).x,
        lambda: gm.point_between(a, b, FOOT_UNIT_M, unit='meters').x,
        lambda: gm.point_between(fa, fb, 1.0, unit='meters'),
        1.0,
    ))
    return cases


_EXACT_BY_NAME = {c[0]: c for c in _exact_cases()}


@pytest.mark.parametrize('op', EXACT_OPS)
def test_exact_crs_metric_foot_native_and_meters_and_crs_free(op: str) -> None:
    """Foot CRS: native units; unit='meters' scales; CRS-free rejects meters."""
    assert op in _EXACT_BY_NAME, f'missing behavioral driver for exact op {op!r}'
    _name, native_run, meters_run, free_meters_run, expected_native = _EXACT_BY_NAME[op]

    native = native_run()
    if op == 'dwithin':
        assert native == pytest.approx(expected_native)
        assert meters_run() == pytest.approx(1.0)
    elif op in {
        'line_interpolate',
        'destination',
        'point_between',
    }:
        # Coordinate results: 1 foot of travel lands at x=1 under both unit modes
        # when the metre input is the foot→metre conversion of 1 foot.
        assert float(native) == pytest.approx(expected_native)
        assert float(meters_run()) == pytest.approx(expected_native)
    elif op == 'interpolate_m':
        assert float(native) == pytest.approx(expected_native)
        assert float(meters_run()) == pytest.approx(FOOT_UNIT_M)
    elif op in {'nearest_points', 'shortest_line'}:
        native_len = _as_length(native)
        meters_len = _as_length(meters_run())
        assert native_len == pytest.approx(expected_native)
        # Geometry-valued results keep coordinate units; unit= scales the
        # distance interpretation for locate-style ops. For nearest/shortest
        # the returned geometries sit at the same foot coordinates — the free
        # distance between them is still 10 feet; unit='meters' on the free
        # function scales the reported separation when extracted via distance.
        if op == 'nearest_points':
            n1, n2 = meters_run()
            assert gm.distance(n1, n2, unit='meters') == pytest.approx(
                expected_native * FOOT_UNIT_M
            )
        else:
            # shortest_line returns a LineString in the source CRS (feet).
            assert meters_len == pytest.approx(expected_native)
            assert gm.length(meters_run(), unit='meters') == pytest.approx(
                expected_native * FOOT_UNIT_M
            )
        assert native_len == pytest.approx(expected_native)
    elif op == 'line_locate':
        assert float(native) == pytest.approx(expected_native)
        assert float(meters_run()) == pytest.approx(expected_native * FOOT_UNIT_M)
    elif op == 'line_substring':
        assert float(native) == pytest.approx(expected_native)
        # substring with meter bounds of 1 foot-equivalent yields ~1 foot length
        assert float(meters_run()) == pytest.approx(expected_native)
    elif op == 'area':
        assert float(native) == pytest.approx(expected_native)
        assert float(meters_run()) == pytest.approx(expected_native * FOOT_UNIT_M**2)
    else:
        # length, distance, hausdorff, frechet — scale by linear foot factor
        assert float(native) == pytest.approx(expected_native)
        assert float(meters_run()) == pytest.approx(expected_native * FOOT_UNIT_M)

    with pytest.raises(gm.GeometryError, match="unit='meters' requires a CRS"):
        free_meters_run()


# ---------------------------------------------------------------------------
# Local-projection constructive metrics
# ---------------------------------------------------------------------------


LocalCase = tuple[str, Callable[[], Any], float, float]


def _local_cases() -> list[LocalCase]:
    """(op, runner, min_plausible, max_plausible) in foot CRS native units."""
    point = _foot_point(0, 0)
    line = _foot_line(10.0)
    box = _foot_box(10.0)
    multipoint = gm.MultiPoint(
        [(0, 0), (10, 0), (10, 10), (0, 10), (3, 3)],
        crs=FOOT_CRS,
    )
    return [
        ('buffer', lambda: point.buffer(1.0).area, math.pi * 0.5, math.pi * 2.0),
        ('offset_curve', lambda: line.offset_curve(1.0).length, 5.0, 20.0),
        ('concave_hull', lambda: multipoint.concave_hull().area, 1.0, 200.0),
        ('polylabel', lambda: box.polylabel(tolerance=0.1), 0.0, 10.0),  # point in box
        (
            'maximum_inscribed_circle',
            lambda: box.maximum_inscribed_circle(tolerance=0.01),
            4.0,
            6.0,
        ),
        ('maximum_inscribed_radius', box.maximum_inscribed_radius, 4.0, 6.0),
        ('minimum_bounding_circle', box.minimum_bounding_circle, 0.0, 50.0),
        ('minimum_bounding_radius', box.minimum_bounding_radius, 5.0, 10.0),
        ('minimum_clearance', box.minimum_clearance, 5.0, 15.0),
        (
            'minimum_clearance_line',
            lambda: box.minimum_clearance_line().length,
            5.0,
            15.0,
        ),
    ]


_LOCAL_BY_NAME = {c[0]: c for c in _local_cases()}


@pytest.mark.parametrize('op', LOCAL_OPS)
def test_local_projection_metric_accepts_foot_inputs(op: str) -> None:
    """Local-projection constructive ops accept US-foot inputs at plausible scale."""
    assert op in _LOCAL_BY_NAME, f'missing behavioral driver for local op {op!r}'
    _name, runner, lo, hi = _LOCAL_BY_NAME[op]
    result = runner()
    if op == 'polylabel':
        assert isinstance(result, gm.Point)
        assert 0.0 <= result.x <= 10.0
        assert 0.0 <= result.y <= 10.0
        return
    if op == 'maximum_inscribed_circle':
        # Returns a circle Polygon; radius ~ sqrt(area/pi) ~ 5 for the 10x10 box.
        assert isinstance(result, gm.Geometry)
        assert result.geometry_type in ('Polygon', 'MultiPolygon')
        radius = math.sqrt(result.area / math.pi)
        assert lo <= radius <= hi
        return
    if op == 'minimum_bounding_circle':
        assert isinstance(result, gm.Geometry)
        assert result.area > 0.0
        return
    magnitude = float(result)
    assert math.isfinite(magnitude)
    assert lo <= magnitude <= hi, (op, magnitude, lo, hi)


@pytest.mark.parametrize('op', LOCAL_OPS)
def test_local_projection_metric_crs_free_rejects_unit_meters(op: str) -> None:
    """unit='meters' on a CRS-free local-projection op raises."""
    free_point = _free_point()
    free_line = _free_line()
    free_box = _free_box()
    free_mp = gm.MultiPoint([(0, 0), (10, 0), (10, 10), (0, 10), (3, 3)])
    callers: dict[str, Callable[[], Any]] = {
        'buffer': lambda: free_point.buffer(1.0, unit='meters'),
        'offset_curve': lambda: free_line.offset_curve(1.0, unit='meters'),
        'concave_hull': lambda: free_mp.concave_hull(
            length_threshold=1.0, unit='meters'
        ),
        'polylabel': lambda: free_box.polylabel(tolerance=0.1, unit='meters'),
        'maximum_inscribed_circle': lambda: free_box.maximum_inscribed_circle(
            tolerance=0.01, unit='meters'
        ),
        'maximum_inscribed_radius': lambda: free_box.maximum_inscribed_radius(
            unit='meters'
        ),
        'minimum_bounding_circle': lambda: free_box.minimum_bounding_circle(
            unit='meters'
        ),
        'minimum_bounding_radius': lambda: free_box.minimum_bounding_radius(
            unit='meters'
        ),
        'minimum_clearance': lambda: free_box.minimum_clearance(unit='meters'),
        'minimum_clearance_line': lambda: free_box.minimum_clearance_line(
            unit='meters'
        ),
    }
    with pytest.raises(gm.GeometryError, match="unit='meters' requires a CRS"):
        callers[op]()
