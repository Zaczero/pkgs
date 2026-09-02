"""Focused semantic guards for the scalar WGS84 geodesic-area kernel."""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest


def test_geodesic_area_is_ellipsoidal_and_keeps_holes() -> None:
    shell = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]
    hole = [(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)]
    polygon = gm.Polygon(shell, holes=[hole], crs=4326)

    from pyproj import Geod

    geod = Geod(ellps='WGS84')
    shell_area = abs(geod.polygon_area_perimeter(*zip(*shell, strict=True))[0])
    hole_area = abs(geod.polygon_area_perimeter(*zip(*hole, strict=True))[0])
    expected = shell_area - hole_area

    assert polygon.area == pytest.approx(expected, rel=2e-12, abs=1e-3)
    assert polygon.area < shell_area
    assert polygon.area != pytest.approx(12.0, rel=1e-6)


def test_geodesic_area_preserves_empty_and_missing_rows() -> None:
    polygon = gm.box(0.0, 0.0, 1.0, 1.0, crs=4326)
    values = gm.GeometryArray([polygon, gm.GeometryCollection([], crs=4326), None], crs=4326)

    np.testing.assert_allclose(values.area[:1], [polygon.area])
    assert values.area[1] == 0.0
    assert np.isnan(values.area[2])
