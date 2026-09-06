"""Exact 3D segment-distance regressions."""

from __future__ import annotations

import math

import gometry as gm
import numpy as np
import pytest


def _line(a: tuple[float, float, float], b: tuple[float, float, float]) -> gm.LineString:
    return gm.LineString([(a[0], a[1]), (b[0], b[1])], z=[a[2], b[2]])


def _point(value: tuple[float, float, float]) -> gm.Point:
    return gm.Point(value[0], value[1], z=value[2])


@pytest.mark.parametrize('span', [1e77, 1e150])
def test_segment_distance_rescues_dot_product_overflow(span: float) -> None:
    left = _line((-span, 0.0, 0.0), (span, 0.0, 0.0))
    right = _line((0.0, -span, 1.0), (0.0, span, 1.0))
    assert gm.distance_3d(left, right) == 1.0


def test_segment_distance_rescues_tiny_direction_endpoint_contact() -> None:
    tiny = 2.0**-538
    segment = _line((0.0, 0.0, 0.0), (tiny, 0.0, 0.0))
    assert gm.distance_3d(segment, _point((tiny, 0.0, 0.0))) == 0.0


@pytest.mark.parametrize('offset', [2.0**-28, 0.1])
def test_segment_distance_rescues_near_parallel_exact_contact(offset: float) -> None:
    left = _line((0.0, 0.0, 0.0), (1.0, 0.0, 0.0))
    right = _line((0.0, offset, 0.0), (1.0, -offset, 0.0))
    assert gm.distance_3d(left, right) == 0.0


def test_segment_distance_preserves_large_witness_residual() -> None:
    base = 2.0**53
    left = _line((base, base + 2.0, 0.0), (base - 2.0, base - 2.0, 0.0))
    right = _line((base + 4.0, base - 4.0, 0.0), (base - 4.0, base - 2.0, 0.0))
    assert gm.distance_3d(left, right) == pytest.approx(math.sqrt(4.0 / 17.0))


def test_segment_distance_preserves_reciprocal_axis_separation() -> None:
    left = _line((-1e200, 0.0, 0.0), (1e200, 0.0, 0.0))
    right = _line((0.0, -1e200, 1e-150), (0.0, 1e200, 1e-150))
    assert gm.distance_3d(left, right) == 1e-150


def test_segment_distance_handles_overflow_and_subnormal_rounding() -> None:
    maximum = float.fromhex('0x1.fffffffffffffp+1023')
    left = _line((-maximum, 0.0, 0.0), (maximum, 0.0, 0.0))
    crossing = _line((0.0, -maximum, 0.0), (0.0, maximum, 0.0))
    offset = _line((0.0, -maximum, 1.0), (0.0, maximum, 1.0))
    assert gm.distance_3d(left, crossing) == 0.0
    assert gm.distance_3d(left, offset) == 1.0

    minimum = math.ulp(0.0)
    assert gm.distance_3d(_point((0.0, 0.0, 0.0)), _point((minimum, 0.0, 0.0))) == minimum


def test_segment_distance_scalar_and_array_result_paths_match() -> None:
    left = _line((0.0, 0.0, 0.0), (10.0, 0.0, 0.0))
    right = _line((5.0, -5.0, 1.0), (5.0, 5.0, 1.0))
    values = gm.GeometryArray([left, right])
    target = _point((5.0, 0.0, 0.0))
    expected = [gm.distance_3d(value, target) for value in values]
    np.testing.assert_array_equal(gm.distance_3d(values, target), expected)


def test_segment_distance_handles_degenerate_parallel_and_outside_cases() -> None:
    assert gm.distance_3d(
        _line((0.0, 0.0, 0.0), (0.0, 0.0, 0.0)),
        _line((1.0, 2.0, 3.0), (1.0, 2.0, 3.0)),
    ) == pytest.approx(math.sqrt(14.0))
    assert gm.distance_3d(
        _line((0.0, 0.0, 0.0), (10.0, 0.0, 0.0)),
        _line((5.0, 0.0, 0.0), (15.0, 0.0, 0.0)),
    ) == 0.0
    assert gm.distance_3d(
        _line((0.0, 0.0, 0.0), (1.0, 0.0, 0.0)),
        _line((2.0, 1.0, 0.0), (2.0, 2.0, 0.0)),
    ) == pytest.approx(math.sqrt(2.0))
