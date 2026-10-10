import math

import gometry as gm
import numpy as np
import pytest


def test_scalar_and_array_unary_facts_and_measures_are_identical() -> None:
    values = [
        gm.box(0, 0, 1, 1, crs=4326),
        gm.LineString([(0, 0), (1, 0)], crs=4326),
        gm.Point(0, 0, crs=4326),
    ]
    array = gm.GeometryArray(values)

    for name in ('is_empty', 'is_closed', 'is_ring', 'is_ccw', 'is_simple', 'is_valid'):
        assert getattr(array, name).tolist() == [
            getattr(value, name) for value in values
        ]
    np.testing.assert_allclose(array.area, [value.area for value in values])
    np.testing.assert_allclose(array.length, [value.length for value in values])
    np.testing.assert_allclose(array.bounds, [value.bounds for value in values])


def test_native_array_unary_paths_preserve_missing_rows_and_frame() -> None:
    polygon = gm.box(0, 0, 1, 1, crs=4326)
    array = gm.GeometryArray([polygon, None, polygon])

    assert array.crs == polygon.crs
    assert array.is_missing.tolist() == [False, True, False]
    assert array.is_empty.tolist() == [False, False, False]
    assert array.is_valid.tolist() == [True, False, True]
    assert math.isnan(array.area[1])
    assert math.isnan(array.length[1])
    assert np.isnan(array.bounds[1]).all()


def test_free_measures_normalize_nullable_native_iterables() -> None:
    geometries = [gm.box(0, 0, 2, 2), None, gm.LineString([(0, 0), (3, 4)])]

    areas = gm.area(geometries, unit='planar')
    lengths = gm.length(iter(geometries), unit='planar')
    assert areas[0] == 4.0 and math.isnan(areas[1]) and areas[2] == 0.0
    assert lengths[0] == 8.0 and math.isnan(lengths[1]) and lengths[2] == 5.0


def test_native_antimeridian_errors_match_for_scalar_and_array() -> None:
    projected = gm.LineString([(0, 0), (1, 1)], crs=3857)
    with pytest.raises(gm.CRSError, match='geographic'):
        _ = projected.crosses_antimeridian
    with pytest.raises(gm.CRSError, match='geographic'):
        _ = gm.GeometryArray([projected]).crosses_antimeridian


def test_distance_3d_array_metric_resolution_error_stays_public() -> None:
    values = gm.GeometryArray([gm.Point(0, 0, z=0, crs=4326)])
    target = gm.Point(1, 0, z=0, crs=4326)

    with pytest.raises(gm.CRSError) as raised:
        gm.distance_3d(values, target)
    assert type(raised.value) is gm.CRSError


def test_native_line_reference_methods_keep_typed_results_and_frames() -> None:
    line = gm.LineString([(0, 0), (2, 0)], crs=3857)
    points = line.line_interpolate(count=3)

    assert isinstance(points, gm.GeometryArray)
    assert points.crs == line.crs
    assert all(isinstance(point, gm.Point) for point in points)
    assert [point.x for point in points] == [0.0, 1.0, 2.0]

    rows = gm.GeometryArray([line, None, line])
    probes = gm.GeometryArray([
        gm.Point(1, 0, crs=3857),
        None,
        gm.Point(2, 0, crs=3857),
    ])
    located = rows.line_locate(probes)
    assert located[0] == pytest.approx(1.0)
    assert math.isnan(located[1])
    assert located[2] == pytest.approx(2.0)


def test_geometry_collection_boundary_accepts_one_sequence_and_generator() -> None:
    left = gm.box(0, 0, 1, 1, crs=3857)
    right = gm.box(1, 0, 2, 1, crs=3857)

    single = gm.union_all(left)
    sequence = gm.union_all([left, right])
    generated = gm.union_all(value for value in (left, right))

    assert isinstance(single, gm.Polygon)
    assert single == left
    assert sequence == generated
    assert sequence.crs == left.crs


def test_pooled_iterables_use_the_geometry_array_boundary() -> None:
    with pytest.raises(TypeError, match='expected Geometry'):
        gm.union_all([gm.Point(0, 0), object()])
    with pytest.raises(gm.CRSMismatchError):
        gm.union_all([gm.Point(0, 0), gm.Point(1, 1, crs=4326)])
