"""Packed centroid / point_on_surface — column-direct paths on Lines/Polygons."""

from __future__ import annotations

import gometry as gm
import pytest
from conftest import canon, line_storage_twins, polygon_storage_twins


def test_multipoint_centroid_survives_overflow_scale_coordinates() -> None:
    # Regression: the flat Σ of the puntal mean overflowed f64 range at 1e308
    # (2e308 == inf) though the true centroid is finite and exactly
    # representable; the online-mean rescue must recover POINT (1e308 1) rather
    # than raise "coordinates must be finite".
    centroid = gm.MultiPoint([(1e308, 0.0), (1e308, 2.0)]).centroid()
    assert centroid.x == 1e308
    assert centroid.y == pytest.approx(1.0)


def test_line_centroid_survives_overflow_scale_coordinates() -> None:
    # Regression: the length-weighted lineal centroid overflowed Σ length·midpoint
    # for a vertical line at x=1e308 (2·1e308 == inf); the weighted online-mean
    # rescue recovers the finite centroid.
    centroid = gm.LineString([(1e308, 0.0), (1e308, 2.0)]).centroid()
    assert centroid.x == 1e308
    assert centroid.y == pytest.approx(1.0)


def test_packed_lines_centroid_exports_geoarrow_point() -> None:
    packed, _ = line_storage_twins()
    assert packed.to_arrow().type.extension_name == 'geoarrow.linestring'
    out = (packed).centroid()
    assert out.to_arrow().type.extension_name == 'geoarrow.point'


def test_packed_polygons_centroid_exports_geoarrow_point() -> None:
    packed, _ = polygon_storage_twins()
    assert packed.to_arrow().type.extension_name == 'geoarrow.polygon'
    out = (packed).centroid()
    assert out.to_arrow().type.extension_name == 'geoarrow.point'


def test_packed_lines_centroid_matches_mixed_storage() -> None:
    packed, mixed = line_storage_twins()
    assert canon((packed).centroid()) == canon((mixed).centroid())


def test_packed_polygons_centroid_matches_mixed_storage() -> None:
    packed, mixed = polygon_storage_twins()
    assert canon((packed).centroid()) == canon((mixed).centroid())


def test_packed_lines_point_on_surface_matches_mixed_storage() -> None:
    packed, mixed = line_storage_twins()
    assert canon((packed).point_on_surface()) == canon((mixed).point_on_surface())


def test_packed_polygons_point_on_surface_matches_mixed_storage() -> None:
    packed, mixed = polygon_storage_twins()
    assert canon((packed).point_on_surface()) == canon((mixed).point_on_surface())
