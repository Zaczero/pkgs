"""Retained circle solutions, frames, and deferred polygon realization."""

import copy
import math
import types
from collections.abc import Sequence

import gometry as gm
import numpy as np
import pytest
from numpy.testing import assert_array_equal


def test_circle_array_generic_alias_is_a_native_runtime_contract() -> None:
    dense = gm.CircleArray[gm.Circle]
    nullable = gm.CircleArray[gm.Circle | None]
    assert isinstance(dense, types.GenericAlias)
    assert dense.__origin__ is gm.CircleArray
    assert dense.__args__ == (gm.Circle,)
    assert nullable.__origin__ is gm.CircleArray
    assert nullable.__args__ == (gm.Circle | None,)


@pytest.mark.parametrize(
    ('crs', 'unit'),
    [(None, None), (None, 'planar'), (4326, None), (4326, 'planar'), (4326, 'meters')],
)
def test_single_site_bounding_circle_preserves_the_first_source_record(
    crs: int | None,
    unit: str | None,
) -> None:
    source = gm.Point(370.125, -0.0, z=7.25, m=13.5, crs=crs)
    duplicate = gm.Point(370.125, -0.0, z=8.5, m=19.25, crs=crs)
    for shape in [source, gm.MultiPoint([source, duplicate])]:
        circle = shape.minimum_bounding_circle(unit=unit)
        assert circle.radius == 0
        assert circle.center.to_wkb() == source.to_wkb()
        assert circle.to_polygon().to_wkb() == source.to_wkb()
        assert circle.center.crs == source.crs


@pytest.mark.parametrize('crs', [None, 4326])
def test_circle_array_preserves_selected_records_beside_computed_centers(
    crs: int | None,
) -> None:
    first = gm.Point(0, -0.0, z=7.25, m=13.5, crs=crs)
    second = gm.Point(0.001, 0.001, z=8.5, m=19.25, crs=crs)
    circles = gm.GeometryArray([
        gm.MultiPoint([first]),
        gm.MultiPoint([first, second]),
        None,
    ]).minimum_bounding_circle()
    assert circles.center[0].to_wkb() == first.to_wkb()
    assert circles.to_polygon()[0].to_wkb() == first.to_wkb()
    assert not circles.center[1].has_z
    assert not circles.center[1].has_m
    assert circles.center[2] is None


@pytest.mark.parametrize(
    'operation', ['maximum_inscribed_circle', 'minimum_bounding_circle']
)
def test_circle_exposes_the_solved_center_and_radius_at_multiple_resolutions(
    operation: str,
) -> None:
    source = gm.box(0, 0, 4, 4)
    circle = getattr(source, operation)()
    assert isinstance(circle, gm.Circle)
    assert isinstance(circle.center, gm.Point)
    assert gm.equals(circle.center, gm.Point(2, 2))
    expected_radius = 2 if operation == 'maximum_inscribed_circle' else math.sqrt(8)
    assert circle.radius == pytest.approx(expected_radius)
    assert circle.unit == 'planar'
    assert circle.working_crs is None
    for resolution in [1, 8, 16, 32]:
        polygon = circle.to_polygon(quadrant_segments=resolution)
        assert len(polygon.coords) == 4 * resolution + 1
        if operation == 'maximum_inscribed_circle':
            assert gm.covers(source, polygon)
        else:
            assert gm.covers(polygon, source)
    assert circle.radius == pytest.approx(expected_radius)
    assert copy.copy(circle) is circle
    assert copy.deepcopy(circle) is circle
    assert 'radius=' in repr(circle)


@pytest.mark.parametrize(
    'operation', ['maximum_inscribed_circle', 'minimum_bounding_circle']
)
def test_circle_array_preserves_missing_empty_and_sliced_rows(operation: str) -> None:
    source = gm.GeometryArray([gm.box(0, 0, 4, 4), None, gm.Polygon()])
    circles = getattr(source, operation)()
    assert isinstance(circles, gm.CircleArray)
    assert isinstance(circles, Sequence)
    assert len(circles) == 3
    assert isinstance(circles[0], gm.Circle)
    assert circles[0] == circles[0]
    assert circles[0] in circles
    assert circles.count(circles[0]) == 1
    assert circles.count(None) == 1
    assert circles.index(None) == 1
    assert circles.index(circles[0], -3, -1) == 0
    assert None in circles
    assert circles[:] == circles
    assert circles != circles[::-1]
    assert circles[1] is None
    assert circles[-1].center.is_empty
    assert math.isnan(circles[-1].radius)
    assert_array_equal(circles.is_missing, [False, True, False])
    assert_array_equal(np.isnan(circles.radius), [False, True, True])
    assert_array_equal(circles.center.is_missing, [False, True, False])
    assert circles.center[2].is_empty
    assert circles.to_polygon()[2].to_wkt() == 'POLYGON EMPTY'
    assert circles.to_polygon()[1] is None
    assert_array_equal(circles[::-1].is_missing, [False, True, False])
    assert_array_equal(circles[1:].is_missing, [True, False])
    assert len(circles[2:1]) == 0
    assert len(circles[:0]) == 0
    assert not circles[:0]
    assert list(circles)[1] is None
    assert next(reversed(circles)).center.is_empty
    with pytest.raises(IndexError):
        _ = circles[3]
    with pytest.raises(ValueError, match='not in CircleArray'):
        circles.index(circles[0], 1)


def test_circle_value_equality_uses_the_exact_disk() -> None:
    source = gm.MultiPoint([(0, 0), (4, 0), (1, 3)])
    first = source.minimum_bounding_circle()
    second = gm.MultiPoint([(1, 3), (4, 0), (0, 0)]).minimum_bounding_circle()
    assert first == second
    assert first != gm.MultiPoint([(0, 0), (5, 0)]).minimum_bounding_circle()
    assert first != object()
    with pytest.raises(TypeError):
        hash(first)


def test_finite_numeric_circle_survives_failed_polygon_realization() -> None:
    center = 2.0**60
    source = gm.Polygon([
        (center, center),
        (center + 2.0**20, center),
        (center, center + 2.0**12),
    ])
    circle = source.maximum_inscribed_circle()
    assert math.isfinite(circle.radius)
    assert circle.radius > 0
    before = circle.radius
    with pytest.raises(OverflowError):
        circle.to_polygon()
    assert circle.radius == before
    assert not circle.center.is_empty


@pytest.mark.parametrize(
    ('coordinates', 'expected_radius'),
    [
        ([(0, 0), (0, 1)], 55287.19427889939),
        ([(0, 0), (120, 0)], 6679169.447596414),
        ([(0, 0), (360, 0), (120, 0)], 6679169.447596414),
        ([(0, 0), (180, 0)], 10001965.729312724),
        ([(179.8, 0), (-179.8, 0)], 22263.898158653446),
    ],
)
def test_geographic_two_point_circle_retains_ellipsoidal_midpoint_and_radius(
    coordinates: list[tuple[float, float]],
    expected_radius: float,
) -> None:
    points = gm.MultiPoint(coordinates, crs=4326)
    circle = points.minimum_bounding_circle()
    assert circle.unit == 'meters'
    assert circle.working_crs is None
    assert circle.radius == pytest.approx(expected_radius, abs=1e-8)
    assert circle.center.crs == points.crs
    for point in coordinates:
        assert gm.distance(circle.center, gm.Point(point, crs=4326)) == pytest.approx(
            circle.radius,
            abs=1e-8,
        )


def test_geographic_local_two_point_circle_can_realize_its_retained_disk() -> None:
    points = gm.MultiPoint([(0, 0), (0, 1)], crs=4326)
    circle = points.minimum_bounding_circle()
    polygon = circle.to_polygon()
    assert polygon.crs == points.crs
    assert polygon.geometry_type == 'Polygon'
    assert circle.radius == pytest.approx(55287.19427889939, abs=1e-8)


def test_geographic_two_point_circle_honors_native_angular_axes() -> None:
    # EPSG:4807 uses grads and the Paris prime meridian.
    left = gm.Point(0, 0, crs=4807)
    right = gm.Point(0, 1, crs=4807)
    circle = gm.MultiPoint([left, right]).minimum_bounding_circle()
    assert circle.unit == 'meters'
    assert circle.center.crs == left.crs
    assert circle.radius == gm.distance(left, right) / 2
    assert gm.distance(circle.center, left) == pytest.approx(circle.radius, abs=1e-8)
    assert gm.distance(circle.center, right) == pytest.approx(circle.radius, abs=1e-8)


def test_geographic_wide_two_point_radius_survives_unavailable_polygon_frame() -> None:
    circle = gm.MultiPoint([(0, 0), (120, 0)], crs=4326).minimum_bounding_circle()
    with pytest.raises(gm.CRSError):
        circle.to_polygon()
    assert circle.radius == pytest.approx(6679169.447596414, abs=1e-8)
    assert circle.center.x == pytest.approx(60)
    assert circle.center.y == pytest.approx(0, abs=1e-12)


def test_circle_projected_units_remain_explicit() -> None:
    source = gm.box(0, 0, 10, 10, crs=2263)
    native = source.maximum_inscribed_circle()
    meters = source.maximum_inscribed_circle(unit='meters')
    assert native.unit == 'planar'
    assert meters.unit == 'meters'
    assert meters.radius == pytest.approx(native.radius * 0.30480060960121924)
    assert native.center.crs == source.crs
    assert meters.center.crs == source.crs
    assert native.working_crs == meters.working_crs
    assert gm.equals(native.center, meters.center)


def test_circle_center_and_polygon_preserve_source_epoch() -> None:
    source = gm.Point(2, 39, crs=7912, epoch=2020)
    circle = source.minimum_bounding_circle()
    assert circle.center.epoch == source.epoch
    assert circle.center.crs == source.crs
    assert circle.to_polygon().epoch == source.epoch


@pytest.mark.parametrize('resolution', [0, -1])
def test_circle_rejects_nonpositive_polygon_resolution(resolution: int) -> None:
    circle = gm.box(0, 0, 2, 2).maximum_inscribed_circle()
    with pytest.raises(gm.GeometryError):
        circle.to_polygon(quadrant_segments=resolution)
