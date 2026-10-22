"""Randomness and CRS-aware measures on the source geometry."""

import math

import gometry as gm
import numpy as np
import pytest


def test_sample_points_accepts_fresh_entropy_and_numpy_generator() -> None:
    shape = gm.box(0, 0, 10, 10)
    assert len(shape.sample_points(3)) == 3
    assert len(shape.sample_points(3, seed=None)) == 3
    first = np.random.default_rng(197)
    second = np.random.default_rng(197)
    assert (
        shape.sample_points(12, seed=first).to_wkt()
        == shape.sample_points(12, seed=second).to_wkt()
    )
    rows = gm.GeometryArray([shape, shape])
    assert [group.to_wkt() for group in rows.sample_points(3, seed=first)] == [
        group.to_wkt() for group in rows.sample_points(3, seed=second)
    ]


def test_geographic_area_sampling_is_uniform_on_sphere_not_chart() -> None:
    band = gm.box(0, 0, 20, 80, crs='+proj=longlat +R=6371000 +type=crs')
    samples = band.sample_points(4096, seed=17)
    # Surface area is d(longitude) d(sin(latitude)) on a sphere.
    mean = sum(math.sin(math.radians(point.y)) for point in samples) / len(samples)
    assert mean == pytest.approx(math.sin(math.radians(80)) / 2, abs=0.025)
    chart = band.sample_points(4096, seed=17, unit='planar')
    chart_mean = sum(math.sin(math.radians(point.y)) for point in chart) / len(chart)
    assert chart_mean == pytest.approx(
        (1 - math.cos(math.radians(80))) / math.radians(80), abs=0.025
    )


def test_geographic_line_sampling_weights_physical_source_arclength() -> None:
    lines = gm.MultiLineString(
        [[(0, 0), (20, 0)], [(0, 60), (20, 60)]],
        crs='+proj=longlat +R=6371000 +type=crs',
    )
    samples = lines.sample_points(2048, seed=37)
    fraction = sum(point.y == 0 for point in samples) / len(samples)
    assert fraction == pytest.approx(2 / 3, abs=0.04)
    chart = lines.sample_points(2048, seed=37, unit='planar')
    assert sum(point.y == 0 for point in chart) / len(chart) == pytest.approx(
        0.5, abs=0.04
    )


def test_geographic_physical_sampling_keeps_seams_poles_and_row_streams() -> None:
    line = gm.LineString([(170, 80), (-170, 85)], crs=4326)
    points = line.sample_points(24, seed=13)
    assert all(abs(point.x) >= 170 and 80 <= point.y <= 85 for point in points)
    rows = gm.GeometryArray([line, None, line])
    groups = rows.sample_points([24, 0, 24], seed=[13, 99, 14])
    assert groups[0].to_wkt() == points.to_wkt()
    assert groups[2].to_wkt() == line.sample_points(24, seed=14).to_wkt()
    pole = gm.LineString([(0, 90), (90, 90)], crs=4326)
    assert all(point.y == 90 for point in pole.sample_points(4, seed=1))
