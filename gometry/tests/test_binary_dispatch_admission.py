"""Binary metric admission keeps operand order and row sentinels stable."""

from __future__ import annotations

import gometry as gm
import numpy as np


def test_scalar_array_metric_directions_preserve_missing_and_empty_rows() -> None:
    scalar = gm.Point(0.0, 0.0)
    values = gm.GeometryArray([
        gm.Point(3.0, 0.0),
        None,
        gm.from_wkt('POINT EMPTY'),
    ])

    for operands in ((scalar, values), (values, scalar)):
        np.testing.assert_allclose(
            gm.distance(*operands),
            [3.0, np.nan, np.inf],
            equal_nan=True,
        )
        np.testing.assert_array_equal(
            gm.dwithin(*operands, 3.0),
            [True, False, False],
        )


def test_three_d_no_fast_scalar_array_lane_preserves_direction_and_missing() -> None:
    scalar = gm.Point(0.0, 0.0, z=0.0, crs=3857)
    values = gm.GeometryArray([
        gm.Point(0.0, 0.0, z=3.0, crs=3857),
        None,
    ])

    for operands in ((scalar, values), (values, scalar)):
        np.testing.assert_allclose(
            gm.distance_3d(*operands),
            [3.0, np.nan],
            equal_nan=True,
        )


def test_geographic_filled_metric_results_are_order_independent() -> None:
    polygon = gm.box(0.0, 0.0, 1.0, 1.0, crs=4326)
    point = gm.Point(0.5, 0.5, crs=4326)

    distances = [gm.distance(*operands) for operands in ((polygon, point), (point, polygon))]
    within = [gm.dwithin(*operands, 0.0) for operands in ((polygon, point), (point, polygon))]

    assert distances == [0.0, 0.0]
    assert within == [True, True]
