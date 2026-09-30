"""Packed centroid / point_on_surface — column-direct paths on Lines/Polygons."""

from __future__ import annotations

import gometry as gm
import pytest

from tests._support import canon, line_storage_twins, polygon_storage_twins


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


def test_packed_lines_point_on_surface_stays_on_linework() -> None:
    for values in line_storage_twins():
        result = values.point_on_surface()
        assert result.crs == values.crs
        assert result.epoch == values.epoch
        for source, point in zip(values, result, strict=True):
            assert gm.equals(source.point_on_surface(), point)
            assert point.coordinate_axes == source.coordinate_axes
            assert gm.covers(source, point)
            assert gm.distance(source, point, unit='planar') == 0.0


def test_packed_polygons_point_on_surface_stays_inside() -> None:
    for values in polygon_storage_twins():
        result = values.point_on_surface()
        assert result.crs == values.crs
        assert result.epoch == values.epoch
        for source, point in zip(values, result, strict=True):
            assert gm.equals(source.point_on_surface(), point)
            assert point.coordinate_axes == source.coordinate_axes
            assert gm.within(point, source)


def test_packed_point_on_surface_stays_inside_shell_and_outside_hole() -> None:
    donut = gm.Polygon(
        [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)],
        holes=[[(3, 3), (7, 3), (7, 7), (3, 7), (3, 3)]],
        crs=3857,
        epoch=2020.0,
    )
    concave = gm.Polygon(
        [(20, 0), (30, 0), (30, 4), (24, 4), (24, 10), (20, 10), (20, 0)],
        crs=3857,
        epoch=2020.0,
    )
    values = gm.GeometryArray([donut, concave], crs=3857, epoch=2020.0)

    result = values.point_on_surface()

    assert result.crs == values.crs
    assert result.epoch == values.epoch
    assert all(
        gm.covered_by(point, source)
        for source, point in zip(values, result, strict=True)
    )
    hole = gm.box(3, 3, 7, 7, crs=3857, epoch=2020.0)
    assert not gm.covers(hole, result[0])


def test_point_on_surface_preserves_empty_missing_alignment_and_frame() -> None:
    values = gm.GeometryArray(
        [gm.Polygon([], crs=3857, epoch=2020.0), None, gm.box(0, 0, 2, 2)],
        crs=3857,
        epoch=2020.0,
    )

    result = values.point_on_surface()

    assert result.is_missing.tolist() == [False, True, False]
    assert result[0].is_empty
    assert result[2].geometry_type == 'Point'
    assert (result.crs, result.epoch) == (values.crs, values.epoch)
