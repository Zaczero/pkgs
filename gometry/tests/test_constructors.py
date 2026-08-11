"""Geometry constructors — metadata, box orientation and antimeridian
splitting, explicit Z/M axes, lon/lat range validation, and point equality.
"""

from typing import Any, cast

import gometry as gm
import numpy as np
import pytest


def test_point_and_crs_metadata() -> None:
    point = gm.Point(21.0, 52.0, crs=4326)
    assert point.geometry_type == 'Point'
    assert point.crs == 'EPSG:4326'
    assert point.x == 21.0
    assert point.y == 52.0
    assert point.bounds == (21.0, 52.0, 21.0, 52.0)
    assert point.__geo_interface__ == {'type': 'Point', 'coordinates': [21.0, 52.0]}


def test_box_ccw_controls_ring_orientation() -> None:
    assert gm.box(0, 0, 2, 1).to_wkt() == 'POLYGON ((0 0, 2 0, 2 1, 0 1, 0 0))'
    assert (
        gm.box(0, 0, 2, 1, ccw=False).to_wkt() == 'POLYGON ((0 0, 0 1, 2 1, 2 0, 0 0))'
    )
    assert gm.equals(gm.box(0, 0, 2, 1), gm.box(0, 0, 2, 1, ccw=False))


def test_box_can_explicitly_split_antimeridian_crossing_bounds() -> None:
    wrapped = gm.box(170, -10, -170, 10, crs=4326, wrap='split')
    whole_world = gm.box(-200, -10, 200, 10, crs=4326, wrap='split')
    assert wrapped.geometry_type == 'MultiPolygon'
    assert wrapped.crs == 'EPSG:4326'
    assert wrapped.bounds == (-180.0, -10.0, 180.0, 10.0)
    assert gm.LineString([(170, 0), (-170, 0)], crs=4326).crosses_antimeridian
    assert gm.LineString([(170, 0), (-170, 0)], crs=4326).crosses_antimeridian
    assert np.array_equal(
        gm.GeometryArray([wrapped, whole_world], crs=4326).crosses_antimeridian,
        [False, False],
    )
    assert [part.to_wkt() for part in gm.parts(wrapped)] == [
        'POLYGON ((170 -10, 180 -10, 180 10, 170 10, 170 -10))',
        'POLYGON ((-180 -10, -170 -10, -170 10, -180 10, -180 -10))',
    ]
    assert (
        whole_world.to_wkt()
        == 'POLYGON ((-180 -10, 180 -10, 180 10, -180 10, -180 -10))'
    )
    with pytest.raises(ValueError, match='geographic CRS'):
        _ = gm.LineString([(170, 0), (-170, 0)], crs=3857).crosses_antimeridian
    assert gm.LineString([(170, 0), (-170, 0)]).crosses_antimeridian
    with pytest.raises(ValueError, match="wrap='split'"):
        gm.box(170, -10, -170, 10)
    with pytest.raises(ValueError, match='requires crs=4326'):
        gm.box(170, -10, -170, 10, crs=3857, wrap='split')
    with pytest.raises(ValueError, match='requires crs=4326'):
        gm.box(170, -10, -170, 10, wrap=cast('Any', 'split'))
    with pytest.raises(ValueError, match='unknown box wrap'):
        gm.box(0, 0, 1, 1, wrap=cast('Any', 'clip'))


def test_geographic_box_preserves_wide_directed_longitude_interval() -> None:
    """Geographic boxes are latitude bands, without a 180-degree area cliff."""
    # The representation for normal city/tile/small-country extents is exactly
    # the old four-corner polygon: coordinates, WKB, and geodesic metrics all
    # remain bit-identical.
    for extent in [
        (0.0, 50.0, 0.01, 50.01),
        (0.0, 50.0, 0.1, 50.1),
        (0.0, 0.0, 0.35, 0.35),
        (0.0, 50.0, 1.0, 51.0),
        (0.0, 50.0, 5.0, 55.0),
        (14.0, 49.0, 24.0, 55.0),
    ]:
        minx, miny, maxx, maxy = extent
        legacy = gm.Polygon(
            [(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny)],
            crs=4326,
        )
        actual = gm.box(*extent, crs=4326)
        assert len(actual.exterior.coords) == 5
        assert actual.to_wkb() == legacy.to_wkb()
        assert actual.area == legacy.area
        assert actual.length == legacy.length

    # The equal-chord limit, rather than a span switch, keeps the normal path
    # byte-identical on both sides of one degree at a typical latitude.
    near_one = [0.9999, 1.0, 1.0001]
    near_one_boxes = [
        gm.box(20.0, 52.0, 20.0 + span, 53.0, crs=4326) for span in near_one
    ]
    assert [len(box.exterior.coords) for box in near_one_boxes] == [5, 5, 5]
    near_one_areas = np.array([box.area for box in near_one_boxes])
    assert np.all(np.diff(near_one_areas) > 0.0)
    assert np.ptp(near_one_areas / near_one) / np.mean(near_one_areas / near_one) < 1e-6

    # Continental boxes and long/thin bands cross the materiality threshold.
    for extent in [
        (0.0, 40.0, 20.0, 60.0),
        (-10.0, 35.0, 30.0, 65.0),
        (-30.0, -30.0, 30.0, 30.0),
        (0.0, 50.0, 20.0, 50.1),
    ]:
        assert len(gm.box(*extent, crs=4326).exterior.coords) > 5

    # The previous four-corner expectation at a hemisphere described geodesic
    # edges instead of this latitude rectangle, so it remains tessellated.
    half = gm.box(-90.0, -10.0, 90.0, 10.0, crs=4326)
    assert len(half.exterior.coords) > 5

    # Span sampling includes both sides of the former threshold. A true
    # latitude rectangle has area proportional to longitude span, so this
    # catches both the old complement and any representation-fidelity cliff.
    spans = np.unique(np.r_[np.linspace(0.01, 360.0, 3601), [179.9, 180.0, 180.1]])
    areas = np.array([
        gm.box(-span / 2.0, -10.0, span / 2.0, 10.0, crs=4326).area for span in spans
    ])
    # Equal-chord refinements can produce a tiny backward blip when the
    # partition gains a chord. The contract is no material discontinuity.
    assert np.all(np.diff(areas) > -0.005 * areas[:-1])
    # A representative partition transition remains within the same budget
    # of the increment for a true latitude rectangle.
    before = gm.box(0.0, -10.0, 56.608, 10.0, crs=4326)
    after = gm.box(0.0, -10.0, 56.708, 10.0, crs=4326)
    expected_increment = before.area * (56.708 - 56.608) / 56.608
    assert abs(after.area - before.area - expected_increment) / before.area < 0.005

    # Equal longitude chords approximate the parallel edges within the
    # materiality budget against a much denser polygonal reference.
    for extent in [
        (0.0, 40.0, 20.0, 60.0),
        (-10.0, 35.0, 30.0, 65.0),
        (-100.0, -10.0, 100.0, 10.0),
        (-179.0, -10.0, 179.0, 10.0),
        (-180.0, -10.0, 180.0, 10.0),
    ]:
        minx, miny, maxx, maxy = extent
        longitudes = np.linspace(minx, maxx, 10_001)
        reference = gm.Polygon(
            [(float(lon), miny) for lon in longitudes]
            + [(maxx, maxy)]
            + [(float(lon), maxy) for lon in longitudes[-2::-1]]
            + [(minx, miny)],
            crs=4326,
        )
        actual = gm.box(*extent, crs=4326)
        assert abs(actual.area / reference.area - 1.0) < 0.005

    # `box(-100, ..., 100, ...)` is the 200-degree band, not the 160-degree
    # complement, and every public region consumer sees that same band.
    assert (
        gm.box(-100.0, -10.0, 100.0, 10.0, crs=4326).area
        > gm.box(-80.0, -10.0, 80.0, 10.0, crs=4326).area
    )
    for span in (160.0, 180.1, 200.0, 358.0, 360.0):
        band = gm.box(-span / 2.0, -10.0, span / 2.0, 10.0, crs=4326)
        point = gm.Point(0.0, 0.0, crs=4326)
        h3 = gm.h3_cover(band, resolution=0)
        s2 = gm.s2_cover(band, level=1)
        assert band.bounds == (-span / 2.0, -10.0, span / 2.0, 10.0)
        assert gm.contains(band, point)
        assert band.area > 0.0
        assert band.length > 0.0
        assert len(h3.cells) > 0
        assert len(s2.cells) > 0
        assert h3.contains(point)
        assert s2.contains(point)


def test_geographic_box_tessellation_leaves_projected_and_split_boxes_unchanged() -> (
    None
):
    projected = gm.box(0.0, 0.0, 1000.0, 1000.0, crs=3857)
    assert projected.to_wkt() == 'POLYGON ((0 0, 1000 0, 1000 1000, 0 1000, 0 0))'
    split = gm.box(170.0, -10.0, -170.0, 10.0, crs=4326, wrap='split')
    assert [part.to_wkt() for part in gm.parts(split)] == [
        'POLYGON ((170 -10, 180 -10, 180 10, 170 10, 170 -10))',
        'POLYGON ((-180 -10, -170 -10, -170 10, -180 10, -180 -10))',
    ]

    values = gm.boxes(
        [-100.0, 20.0], [-10.0, 51.0], [100.0, 21.0], [10.0, 52.0], crs=4326
    )
    assert values[0].to_wkb() == gm.box(-100.0, -10.0, 100.0, 10.0, crs=4326).to_wkb()
    assert values[1].to_wkb() == gm.box(20.0, 51.0, 21.0, 52.0, crs=4326).to_wkb()


def test_explicit_z_m_point_constructors_preserve_axes_without_affecting_xy_topology() -> (
    None
):
    xy = gm.Point(1, 2, crs=4326)
    xyz = gm.Point(1, 2, z=3, crs=4979)
    xym = gm.Point(1, 2, m=4, crs=4326)
    xyzm = gm.Point(1, 2, z=3, m=4, crs=4326)
    line = gm.LineString([(0, 0), (1, 1)], z=[10, 11], m=[20, 21])
    values = gm.GeometryArray([xy, xyzm])
    assert xyz.coordinate_axes == 'XYZ'
    assert xyz.topological_dimension == 0
    assert xyz.has_z
    assert not xyz.has_m
    assert xyz.z == 3
    assert xyz.coords.to_nested() == [1.0, 2.0, 3.0]
    assert xym.coordinate_axes == 'XYM'
    assert xym.has_m
    assert xym.m == 4
    assert xyzm.coordinate_axes == 'XYZM'
    assert xyzm.coords.to_nested() == [1.0, 2.0, 3.0, 4.0]
    assert values.common_coordinate_axes is None
    assert values.any_has_z
    assert values.any_has_m
    assert gm.equals(xy, xyzm)
    assert gm.equals_exact(xy, xyzm, include_z=False, include_m=False)
    assert not gm.equals_exact(xy, xyzm)
    with pytest.raises(ValueError, match='matching CRS'):
        gm.equals_exact(xyzm, gm.Point(1, 2, z=3, m=4, crs=3857))
    assert not gm.equals_exact(xyzm, gm.Point(1, 2, z=3, m=5, crs=4326))
    assert list(xyzm.coords.select('XYZM')) == [(1.0, 2.0, 3.0, 4.0)]
    assert xyzm.quantize(0).coords.to_nested() == [1.0, 2.0, 3.0, 4.0]
    assert xyzm.set_z(None).set_m(None).coordinate_axes == 'XY'
    assert xyzm.set_z(None).set_m(None).coords.to_nested() == [1.0, 2.0]
    assert xyz.to_wkt() == 'POINT Z (1 2 3)'
    assert xyz.to_wkt(output_dimension=2) == 'POINT (1 2)'
    assert xym.to_wkt(output_dimension=3) == 'POINT M (1 2 4)'
    assert xyzm.to_wkt(output_dimension=3) == 'POINT Z (1 2 3)'
    assert xyzm.to_wkt(output_dimension=4) == 'POINT ZM (1 2 3 4)'
    assert line.to_wkt(output_dimension=2) == 'LINESTRING (0 0, 1 1)'
    with pytest.raises(ValueError, match='output_dimension'):
        xyz.to_wkt(output_dimension=5)
    assert gm.from_wkt('POINT M (1 2 4)').coords.to_nested() == [1.0, 2.0, 4.0]
    assert gm.from_wkt('POINT ZM (1 2 3 4)').coords.to_nested() == [1.0, 2.0, 3.0, 4.0]
    assert gm.from_geojson(xyz.to_geojson(), crs=4979).coords.to_nested() == [
        1.0,
        2.0,
        3.0,
    ]
    assert gm.from_geojson(
        xyz.to_geojson(include_z=False), crs=4326
    ).coords.to_nested() == [1.0, 2.0]
    with pytest.raises(ValueError, match='XY or XYZ only'):
        gm.from_geojson({'type': 'Point', 'coordinates': [1, 2, 3, 4]})
    with pytest.raises(ValueError, match='expected axes'):
        gm.require(xyz, axes='XY')
    wkb = xyzm.to_wkb()
    assert int.from_bytes(wkb[1:5], 'little') == 3001
    assert gm.from_wkb(wkb).coords.to_nested() == [1.0, 2.0, 3.0, 4.0]
    ewkb = xyzm.to_wkb(include_srid=True)
    assert int.from_bytes(ewkb[1:5], 'little') == 3758096385
    assert gm.from_wkb(ewkb).crs == 'EPSG:4326'
    assert gm.from_wkb(ewkb).coords.to_nested() == [1.0, 2.0, 3.0, 4.0]
    with pytest.raises(gm.InvalidGeometryError, match='GeoJSON has no M'):
        xym.to_geojson()


def test_sequence_and_column_constructors_preserve_explicit_axes() -> None:
    assert gm.LineString([], z=[]).coordinate_axes == 'XYZ'
    assert gm.MultiPoint([], m=[]).coordinate_axes == 'XYM'
    assert gm.LineString(np.empty((0, 4))).coordinate_axes == 'XYZM'

    polygon = gm.Polygon([(0, 0), (1, 0), (0, 1)], z=[1, 2, 3])
    assert polygon.coordinate_axes == 'XYZ'
    assert polygon.to_wkt().startswith('POLYGON Z')
    assert list(polygon.exterior.coords)[-1] == (0.0, 0.0, 1.0)

    empty = gm.Polygon([], z=[])
    assert empty.coordinate_axes == 'XYZ'
    assert empty.to_wkt() == 'POLYGON Z EMPTY'
    with pytest.raises(ValueError, match='z must have the same length'):
        gm.Polygon(z=[1])
    with pytest.raises(ValueError, match='m must have the same length'):
        gm.Polygon(m=[1])


def test_topological_point_equality_canonicalizes_signed_zero() -> None:
    assert gm.equals(gm.Point(-0.0, 0.0), gm.Point(0.0, 0.0))
    np.testing.assert_array_equal(
        gm.contains(gm.points([-0.0], [0.0]), gm.Point(0.0, 0.0)), [True]
    )


def test_lonlat_range_validation_on_grid_paths() -> None:
    with pytest.raises(ValueError, match='invalid longitude/latitude'):
        gm.h3_cover(gm.box(181, 0, 182, 1, crs=4326), resolution=1)
    with pytest.raises(ValueError, match='invalid longitude/latitude'):
        gm.s2_cover(gm.box(181, 0, 182, 1, crs=4326), level=1)
    # A reflected selection image requires |latitude| > 90. Cover ingress
    # rejects it, so it can never replace raw source authority on a public
    # H3/S2 path.
    with pytest.raises(ValueError, match='invalid longitude/latitude'):
        gm.h3_cover(gm.box(0, 91, 1, 92, crs=4326), resolution=1)
    with pytest.raises(ValueError, match='invalid longitude/latitude'):
        gm.s2_cover(gm.box(0, 91, 1, 92, crs=4326), level=1)


def test_d21_polygon_mixed_shell_hole_axes_rejected_at_construction() -> None:
    """D21/G2: Polygon(xy_shell, xyz_hole) rejects at construction (writer parity).

    Writers refuse mixed ring axes rather than invent Z/M; construction matches.
    Promote with force_3d/set_m (or build homogeneous rings) first.
    """
    shell = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 0.0)]
    hole = [(1.0, 1.0, 1.0), (2.0, 1.0, 1.0), (1.0, 2.0, 1.0), (1.0, 1.0, 1.0)]
    with pytest.raises(gm.InvalidGeometryError, match=r'share one coordinate axes'):
        gm.Polygon(shell, holes=[hole])

    # Within-sequence mixed vertices still reject (uniformity preserved).
    with pytest.raises(gm.InvalidGeometryError, match='axis layout'):
        gm.Polygon([(0.0, 0.0), (1.0, 0.0, 1.0), (1.0, 1.0), (0.0, 0.0)])

    uniform = gm.Polygon(
        [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)],
        holes=[[(0.2, 0.2), (0.4, 0.2), (0.2, 0.4), (0.2, 0.2)]],
    )
    assert uniform.coordinate_axes == 'XY'


def test_m05_mixed_multipart_rejects_at_construction() -> None:
    """m05/G2: mixed-axes MultiLineString / MultiPolygon reject at construction.

    Writers refuse mixed members rather than invent Z/M; construction matches.
    Homogeneous XYZ multiparts still expose NaN-free Z columns.
    """
    with pytest.raises(gm.InvalidGeometryError, match=r'share one coordinate axes'):
        gm.MultiLineString([
            [(0.0, 0.0), (1.0, 1.0)],
            [(0.0, 0.0, 5.0), (1.0, 1.0, 6.0)],
        ])

    mls = gm.MultiLineString([
        [(0.0, 0.0, 0.0), (1.0, 1.0, 0.0)],
        [(0.0, 0.0, 5.0), (1.0, 1.0, 6.0)],
    ])
    assert mls.coordinate_axes == 'XYZ'
    assert [part.coordinate_axes for part in mls.parts] == ['XYZ', 'XYZ']
    mls_coords = np.asarray(mls.coords)
    np.testing.assert_allclose(mls_coords[:, 2], [0.0, 0.0, 5.0, 6.0])
    _ = mls.length_3d  # all members carry Z

    with pytest.raises(gm.InvalidGeometryError, match=r'share one coordinate axes'):
        gm.MultiPolygon([
            [[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]],
            [[(0.0, 0.0, 1.0), (1.0, 0.0, 1.0), (1.0, 1.0, 1.0), (0.0, 0.0, 1.0)]],
        ])
    mpoly = gm.MultiPolygon([
        [[(0.0, 0.0, 0.0), (1.0, 0.0, 0.0), (1.0, 1.0, 0.0), (0.0, 0.0, 0.0)]],
        [[(0.0, 0.0, 1.0), (1.0, 0.0, 1.0), (1.0, 1.0, 1.0), (0.0, 0.0, 1.0)]],
    ])
    assert mpoly.coordinate_axes == 'XYZ'
    assert [part.coordinate_axes for part in mpoly.parts] == ['XYZ', 'XYZ']
    mpoly_coords = np.asarray(mpoly.coords)
    np.testing.assert_allclose(mpoly_coords[0, 2], 0.0)
    np.testing.assert_allclose(mpoly_coords[4, 2], 1.0)
