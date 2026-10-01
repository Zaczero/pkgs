"""Extreme finite-coordinate behavior for minimum rotated rectangles."""

import gometry as gm
import pytest


def test_minimum_rotated_rectangle_reports_unrepresentable_extreme_input() -> None:
    polygon = gm.from_wkt(
        'POLYGON((1.7976931348623157e308 1e300,'
        '3 3,'
        '3 2.2250738585072014e-308,'
        '1.7976931348623157e308 1e300))'
    )

    with pytest.raises(
        gm.InvalidGeometryError,
        match='minimum_rotated_rectangle result is not representable with finite coordinates',
    ):
        polygon.minimum_rotated_rectangle()
