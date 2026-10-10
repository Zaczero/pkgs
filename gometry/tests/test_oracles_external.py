import math
from typing import Any, cast

import gometry as gm
import h3
import pyproj
import pytest
import s2sphere
import shapely

from tests._support import (
    chart_parallel_length,
    chart_rectangle_perimeter,
    chart_zone_area,
)


def test_overlay_restores_zm_and_force_2d_flattens() -> None:
    """Overlay restores resolvable Z/M across scalar, aggregate, and array
    surfaces; ``force_2d`` explicitly flattens.
    """
    a = gm.from_wkt('POLYGON Z ((0 0 5, 2 0 5, 2 2 5, 0 2 5, 0 0 5))')
    b = gm.from_wkt('POLYGON Z ((1 1 5, 3 1 5, 3 3 5, 1 3 5, 1 1 5))')
    for name in ('intersection', 'union', 'difference', 'symmetric_difference'):
        assert getattr(gm, name)(a, b).coords.coordinate_axes == 'XYZ'
        assert getattr(gm, name)(a, b).force_2d().coords.coordinate_axes == 'XY'
    assert gm.union_all([a, b]).coords.coordinate_axes == 'XYZ'
    assert gm.union_all([a, b]).force_2d().coords.coordinate_axes == 'XY'
    pair = gm.GeometryArray([a])
    assert gm.intersection(pair, b)[0].coords.coordinate_axes == 'XYZ'
    assert gm.intersection(pair, b).force_2d()[0].coords.coordinate_axes == 'XY'


def test_h3_cells_hierarchy_and_compaction_match_h3_py_oracle() -> None:
    cell = gm.H3Cell(21.0, 52.0, resolution=7)
    coverage = gm.h3_cover(
        gm.box(20.99, 51.99, 21.01, 52.01, crs=4326), resolution=8, cell_rule='center'
    )
    tokens = [str(value) for value in coverage]
    assert str(cell) == h3.latlng_to_cell(52.0, 21.0, 7)
    assert str(cell.parent(6)) == h3.cell_to_parent(str(cell), 6)
    assert sorted(str(value) for value in cell.parent(6).children(7)) == sorted(
        h3.cell_to_children(str(cell.parent(6)), 7)
    )
    ring = cell.grid_ring(1)
    assert sorted(str(value) for value in ring) == sorted(h3.grid_ring(str(cell), 1))
    assert cell.grid_distance(ring[0]) == h3.grid_distance(str(cell), str(ring[0]))
    assert [str(value) for value in cell.grid_path(ring[0])] == h3.grid_path_cells(
        str(cell), str(ring[0])
    )
    assert sorted(str(value) for value in coverage.compact().uncompact(8)) == sorted(
        tokens
    )
    assert cell.is_pentagon == h3.is_pentagon(str(cell))
    assert cell.area == pytest.approx(h3.cell_area(str(cell), unit='m^2'))
    assert gm.H3Cell(str(cell)) == cell
    assert gm.H3Cell(cell.id) == cell


def test_s2_cells_and_hierarchy_match_s2sphere_oracle() -> None:
    lat_lng = s2sphere.LatLng.from_degrees(52.0, 21.0)
    oracle = s2sphere.CellId.from_lat_lng(lat_lng).parent(12)
    cell = gm.S2Cell(21.0, 52.0, level=12)
    _ = gm.s2_cover(gm.box(20.99, 51.99, 21.01, 52.01, crs=4326), level=12)
    assert cell.id == oracle.id()
    assert cell.token == oracle.to_token()
    assert cell.parent(10).id == oracle.parent(10).id()
    assert [value.id for value in cell.parent(11).children()] == [
        oracle.parent(11).child_begin().advance(offset).id() for offset in range(4)
    ]
    neighbors = cell.neighbors
    assert len(neighbors) == 4
    assert {n.id for n in neighbors} == {c.id() for c in oracle.get_edge_neighbors()}
    assert all(n.level == cell.level for n in neighbors)
    assert gm.S2Cell(cell.token) == cell
    assert gm.S2Cell(cell.id) == cell


def test_geodesic_measurements_match_pyproj_oracle() -> None:
    geod = pyproj.Geod(ellps='WGS84')
    point = gm.Point(21.0, 52.0, crs=4326)
    other = gm.Point(22.0, 52.0, crs=4326)
    square = gm.Polygon(
        [(21.0, 52.0), (21.1, 52.0), (21.1, 52.1), (21.0, 52.1), (21.0, 52.0)], crs=4326
    )
    assert gm.distance(point, other) == pytest.approx(
        geod.inv(21.0, 52.0, 22.0, 52.0)[2], rel=1e-09
    )
    azimuth, _, distance = geod.inv(21.0, 52.0, 22.0, 52.0)
    assert gm.bearing(point, other) == pytest.approx(azimuth % 360.0, rel=1e-09)
    destination_lon, destination_lat, _ = geod.fwd(21.0, 52.0, azimuth, distance)
    destination = point.destination(azimuth, distance)
    assert destination.x == pytest.approx(destination_lon, rel=1e-09)
    assert destination.y == pytest.approx(destination_lat, rel=1e-09)
    midpoint_lon, midpoint_lat, _ = geod.fwd(21.0, 52.0, azimuth, distance / 2.0)
    midpoint = gm.point_between(point, other, distance / 2.0)
    assert midpoint.x == pytest.approx(midpoint_lon, rel=1e-09)
    assert midpoint.y == pytest.approx(midpoint_lat, rel=1e-09)
    expected_area = chart_zone_area(
        21.0,
        21.1,
        52.0,
        52.1,
        semi_major=geod.a,
        flattening=geod.f,
    )
    expected_perimeter = chart_rectangle_perimeter(geod, 21.0, 21.1, 52.0, 52.1)
    # This small box subtracts close authalic-q values in both binary64 paths.
    assert square.area == pytest.approx(expected_area, rel=2e-13)
    assert square.length == pytest.approx(expected_perimeter, rel=1e-13)
    assert point.buffer(100).area == pytest.approx(math.pi * 100 * 100, rel=0.01)


def test_utm_transforms_match_pyproj_oracle_across_zones() -> None:
    samples = []
    for zone in range(1, 61):
        lon = -183.0 + zone * 6.0
        samples.append((f'EPSG:{32600 + zone}', lon, 40.0))
        samples.append((f'EPSG:{32700 + zone}', lon, -40.0))
    samples.extend([
        ('EPSG:32632', 6.0, 60.0),
        ('EPSG:32633', 9.0, 78.0),
        ('EPSG:32635', 21.0, 78.0),
        ('EPSG:32637', 33.0, 78.0),
    ])
    for crs, lon, lat in samples:
        transformer = pyproj.Transformer.from_crs(4326, crs, always_xy=True)
        expected_x, expected_y = transformer.transform(lon, lat)
        point = gm.Point(lon, lat, z=3.0, m=4.0, crs=4326).to_crs(crs)
        assert point.x == pytest.approx(expected_x, abs=0.02)
        assert point.y == pytest.approx(expected_y, abs=0.02)
        assert point.z == 3.0
        assert point.m == 4.0
        inverse = point.to_crs(4326)
        assert inverse.x == pytest.approx(lon, abs=2e-06)
        assert inverse.y == pytest.approx(lat, abs=2e-06)
        assert inverse.z == 3.0
        assert inverse.m == 4.0
    beyond = gm.Point(0.0, 85.0, crs=4326).to_crs(32631)
    beyond_x, beyond_y = pyproj.Transformer.from_crs(
        4326, 32631, always_xy=True
    ).transform(0.0, 85.0)
    assert beyond.x == pytest.approx(beyond_x, abs=0.02)
    assert beyond.y == pytest.approx(beyond_y, abs=0.02)


def test_utm_catalog_matches_pyproj_database_oracle() -> None:
    from pyproj.aoi import AreaOfInterest
    from pyproj.database import query_utm_crs_info

    area = AreaOfInterest(20.0, 51.0, 22.0, 53.0)
    pyproj_items = query_utm_crs_info(
        datum_name='WGS 84', area_of_interest=area, contains=True
    )
    gometry_items = gm.crs_utm_zones(
        datum_name='WGS 84',
        area_of_interest=(20.0, 51.0, 22.0, 53.0),
        contains_area_of_interest=True,
    )
    assert [item['crs'] for item in gometry_items] == [
        f'{item.auth_name}:{item.code}' for item in pyproj_items
    ]
    assert [item['name'] for item in gometry_items] == [
        item.name for item in pyproj_items
    ]


def test_estimated_polar_crs_matches_pyproj_oracle() -> None:
    for lon, lat in [(0.0, 89.0), (0.0, -89.0)]:
        actual = gm.Point(lon, lat, crs=4326).estimate_local_crs()
        crs = pyproj.CRS.from_user_input(str(actual))
        assert crs.is_projected
        assert crs.name == gm.CRS(actual).info['name']
        assert [axis.abbrev.lower() for axis in crs.axis_info] == ['e', 'n']
        expected_xy = pyproj.Transformer.from_crs(4326, crs, always_xy=True).transform(
            lon, lat
        )
        assert gm.crs_transform(4326, actual, lon, lat) == pytest.approx(expected_xy)


def test_estimated_local_crs_is_projected_across_domains() -> None:
    cases = [
        gm.box(20.0, 51.0, 22.0, 53.0, crs=4326),
        gm.Point(0.0, 89.0, crs=4326),
        gm.Point(0.0, -89.0, crs=4326),
    ]
    for geometry in cases:
        actual = geometry.estimate_local_crs()
        assert pyproj.CRS.from_user_input(str(actual)).is_projected


def test_raw_crs_transform_matches_pyproj_for_3d_coordinates() -> None:
    transformer = pyproj.Transformer.from_crs(4979, 4978, always_xy=True)
    xs = [21.0, -73.9857]
    ys = [52.0, 40.7484]
    zs = [100.0, 381.0]
    ts = [2020.0, 2021.5]
    expected = transformer.transform(xs, ys, zs)
    expected_4d = transformer.transform(xs, ys, zs, tt=ts)
    # Scalars in, scalar tuple out (t is an input epoch, never a returned ordinate).
    assert gm.crs_transform(4979, 4978, xs[0], ys[0], zs[0]) == pytest.approx((
        expected[0][0],
        expected[1][0],
        expected[2][0],
    ))
    # Lane input returns an interleaved (N, 3) float64 matrix.
    actual = gm.crs_transform(4979, 4978, xs, ys, zs)
    assert actual.shape == (len(xs), 3)
    assert actual[:, 0] == pytest.approx(expected[0])
    assert actual[:, 1] == pytest.approx(expected[1])
    assert actual[:, 2] == pytest.approx(expected[2])
    # `t` steers the transform but is not echoed: scalar stays a 3-tuple,
    assert gm.crs_transform(4979, 4978, xs[0], ys[0], zs[0], t=ts[0]) == pytest.approx((
        expected_4d[0][0],
        expected_4d[1][0],
        expected_4d[2][0],
    ))
    # and lane input stays an (N, 3) matrix (no time column).
    actual_t = gm.crs_transform(4979, 4978, xs, ys, zs, t=ts)
    assert actual_t.shape == (len(xs), 3)
    assert actual_t[:, 0] == pytest.approx(expected_4d[0])
    assert actual_t[:, 1] == pytest.approx(expected_4d[1])
    assert actual_t[:, 2] == pytest.approx(expected_4d[2])


def test_geometry_to_crs_transforms_xyz_with_proj_oracle() -> None:
    transformer = pyproj.Transformer.from_crs(4979, 4978, always_xy=True)
    xs = [21.0, 22.0]
    ys = [52.0, 53.0]
    zs = [100.0, 200.0]
    expected_xs, expected_ys, expected_zs = transformer.transform(xs, ys, zs)
    point = gm.Point(xs[0], ys[0], z=zs[0], m=7.0, crs=4979).to_crs(4978)
    line = gm.LineString(list(zip(xs, ys, strict=True)), z=zs, crs=4979).to_crs(4978)
    assert point.crs == gm.CRS('EPSG:4978')
    assert list(point.coords[0]) == pytest.approx([
        expected_xs[0],
        expected_ys[0],
        expected_zs[0],
        7.0,
    ])
    assert line.crs == gm.CRS('EPSG:4978')
    assert line.coordinate_axes == 'XYZ'
    assert list(line.coords) == pytest.approx(
        list(zip(expected_xs, expected_ys, expected_zs, strict=True))
    )


def test_crs_constrained_transform_numeric_paths_match_pyproj() -> None:
    from pyproj.aoi import AreaOfInterest

    area = (-75.0, 40.0, -73.0, 42.0)
    bounds = (900000.0, 100000.0, 1100000.0, 300000.0)
    options = {
        'area_of_interest': area,
        'authority': 'EPSG',
        'accuracy': 10.0,
        'allow_ballpark': False,
    }
    transformer = pyproj.Transformer.from_crs(
        2263,
        4326,
        always_xy=True,
        area_of_interest=AreaOfInterest(*area),
        authority='EPSG',
        accuracy=10.0,
        allow_ballpark=False,
    )
    # The outputs are geographic coordinates in the [-128, 128] degree
    # interval. Eight binary64 ULPs at that scale is 2**-43 degrees; it
    # admits independent operation-order rounding without deriving a bound
    # from this implementation's observed discrepancy.
    transform_tolerance = 8.0 * math.ulp(64.0)

    point = gm.Point(980000.0, 190000.0, crs=2263)
    expected_point = transformer.transform(point.x, point.y)
    with pytest.warns(gm.AccuracyWarning, match='us_noaa_njhpgn'):
        actual_point = point.to_crs(4326, **options)
    assert list(actual_point.coords[0]) == pytest.approx(
        expected_point, rel=0.0, abs=transform_tolerance
    )

    xs = [980000.0, 990000.0]
    ys = [190000.0, 200000.0]
    expected_xs, expected_ys = transformer.transform(xs, ys)
    with pytest.warns(gm.AccuracyWarning, match='us_noaa_njhpgn'):
        points = gm.points(xs, ys, crs=2263).to_crs(4326, **options)
    for actual_row, expected_row in zip(
        points.coords.select('XY'),
        zip(expected_xs, expected_ys, strict=True),
        strict=True,
    ):
        assert actual_row == pytest.approx(
            expected_row, rel=0.0, abs=transform_tolerance
        )

    with pytest.warns(gm.AccuracyWarning, match='us_noaa_njhpgn'):
        actual_bounds = gm.crs_transform_bounds(2263, 4326, bounds, **options)
    assert actual_bounds == pytest.approx(
        transformer.transform_bounds(*bounds),
        rel=0.0,
        abs=transform_tolerance,
    )


def test_crs_constrained_transform_refusal_preserves_context() -> None:
    area = (-1.0, 50.0, 1.0, 52.0)
    options = {'area_of_interest': area, 'allow_ballpark': False}

    with pytest.raises(
        gm.TransformError, match='cannot create CRS transform'
    ) as scalar_error:
        gm.Point(980000.0, 190000.0, crs=2263).to_crs(4326, **options)
    assert scalar_error.value.source == 'EPSG:2263'
    assert scalar_error.value.target == 'EPSG:4326'

    with pytest.raises(
        gm.TransformError, match='cannot create CRS transform'
    ) as packed_error:
        gm.points([980000.0, 990000.0], [190000.0, 200000.0], crs=2263).to_crs(
            4326, **options
        )
    assert packed_error.value.source == 'EPSG:2263'
    assert packed_error.value.target == 'EPSG:4326'


def test_crs_projection_factors_match_pyproj_oracle() -> None:
    for crs, lon, lat in [(3857, -73.0, 41.0), (32618, -73.0, 41.0)]:
        actual = gm.CRS(crs).factors(lon, lat)
        expected = pyproj.Proj(f'EPSG:{crs}').get_factors(lon, lat)
        assert actual['meridional_scale'] == pytest.approx(expected.meridional_scale)
        assert actual['parallel_scale'] == pytest.approx(expected.parallel_scale)
        assert actual['areal_scale'] == pytest.approx(expected.areal_scale)
        assert actual['angular_distortion'] == pytest.approx(
            expected.angular_distortion
        )
        assert actual['meridian_parallel_angle'] == pytest.approx(
            expected.meridian_parallel_angle
        )
        assert actual['meridian_convergence'] == pytest.approx(
            expected.meridian_convergence
        )


def test_geodesic_point_navigation_matches_pyproj_ellipsoid_oracle() -> None:
    for crs in [4326, 4267]:
        ellipsoid = gm.CRS(crs).info['ellipsoid']
        assert ellipsoid is not None
        geod = pyproj.Geod(
            a=ellipsoid['semi_major_metre'], rf=ellipsoid['inverse_flattening']
        )
        start = gm.Point(-73.0, 41.0, crs=crs)
        end = gm.Point(-74.0, 42.0, crs=crs)
        forward, back, distance = geod.inv(-73.0, 41.0, -74.0, 42.0)
        assert gm.bearing(start, end) == pytest.approx(forward % 360.0)
        assert gm.bearing(end, start) == pytest.approx(back % 360.0)
        assert gm.distance(start, end) == pytest.approx(distance)
        expected_lon, expected_lat, expected_back = geod.fwd(-73.0, 41.0, 45.0, 1000.0)
        direct = start.destination(45.0, 1000.0)
        assert direct.x == pytest.approx(expected_lon)
        assert direct.y == pytest.approx(expected_lat)
        assert gm.bearing(direct, start) == pytest.approx(expected_back % 360.0)
        expected_lons, expected_lats, _ = geod.fwd(
            [-73.0, -73.0], [41.0, 41.0], [45.0, 90.0], [1000.0, 2000.0]
        )
        direct_batch = start.destination([45.0, 90.0], [1000.0, 2000.0])
        assert [point.x for point in direct_batch] == pytest.approx(expected_lons)
        assert [point.y for point in direct_batch] == pytest.approx(expected_lats)
        expected_midpoint = geod.npts(-73.0, 41.0, -74.0, 42.0, 1)[0]
        midpoint = gm.point_between(start, end, 0.5, normalized=True)
        assert midpoint.x == pytest.approx(expected_midpoint[0])
        assert midpoint.y == pytest.approx(expected_midpoint[1])
        assert gm.distance(start, midpoint) == pytest.approx(distance / 2)
        expected_points = geod.npts(-73.0, 41.0, -74.0, 42.0, 3)
        points = gm.point_between(start, end, [0.25, 0.5, 0.75], normalized=True)
        assert [point.x for point in points] == pytest.approx([
            point[0] for point in expected_points
        ])
        assert [point.y for point in points] == pytest.approx([
            point[1] for point in expected_points
        ])


def test_crs_chart_geometry_measurements_match_ellipsoid_oracles() -> None:
    coordinates = [
        (-73.0, 41.0),
        (-72.0, 41.0),
        (-72.0, 42.0),
        (-73.0, 42.0),
        (-73.0, 41.0),
    ]
    hole = [(-72.7, 41.3), (-72.3, 41.3), (-72.3, 41.7), (-72.7, 41.7), (-72.7, 41.3)]
    line = gm.LineString(coordinates[:3], crs=4267)
    polygon = gm.Polygon(coordinates, holes=[hole], crs=4267)
    # PROJ lacks ca_nrc_ntv2_0.tif, so this NAD27 transform degrades explicitly.
    with pytest.warns(gm.AccuracyWarning, match='ca_nrc_ntv2_0[.]tif'):
        projected_polygon = polygon.to_crs(3857)
    ellipsoid = gm.CRS(4267).info['ellipsoid']
    assert ellipsoid is not None
    geod = pyproj.Geod(
        a=ellipsoid['semi_major_metre'], rf=ellipsoid['inverse_flattening']
    )
    semi_major = float(ellipsoid['semi_major_metre'])
    flattening = 1.0 / float(ellipsoid['inverse_flattening'])
    expected_area = chart_zone_area(
        -73.0, -72.0, 41.0, 42.0, semi_major=semi_major, flattening=flattening
    ) - chart_zone_area(
        -72.7, -72.3, 41.3, 41.7, semi_major=semi_major, flattening=flattening
    )
    expected_perimeter = chart_rectangle_perimeter(
        geod, -73.0, -72.0, 41.0, 42.0
    ) + chart_rectangle_perimeter(geod, -72.7, -72.3, 41.3, 41.7)
    expected_length = (
        chart_parallel_length(41.0, 1.0, semi_major=semi_major, flattening=flattening)
        + geod.inv(-72.0, 41.0, -72.0, 42.0)[2]
    )
    assert polygon.area == pytest.approx(abs(expected_area))
    with pytest.warns(gm.AccuracyWarning, match='ca_nrc_ntv2_0[.]tif'):
        restored_area = projected_polygon.to_crs(4267).area
    assert restored_area == pytest.approx(abs(expected_area))
    assert polygon.length == pytest.approx(expected_perimeter)
    assert line.length == pytest.approx(expected_length)


def test_crs_coordinate_epoch_metadata_controls_dynamic_operations() -> None:
    dynamic = gm.CRS(7789).operation(7660)
    with_source_epoch = gm.CRS(7789).operation(7660, source_epoch=2010.0)
    with_both_epochs = gm.CRS(7789).operation(
        7660, source_epoch=2010.0, target_epoch=2020.0
    )
    assert dynamic['requires_coordinate_epoch'] is True
    assert with_source_epoch['requires_coordinate_epoch'] is False
    assert with_source_epoch['source_epoch'] == 2010.0
    assert with_source_epoch['target_epoch'] is None
    assert with_both_epochs['requires_coordinate_epoch'] is False
    assert with_both_epochs['source_epoch'] == 2010.0
    assert with_both_epochs['target_epoch'] == 2020.0
    x_epoch, y_epoch, z_epoch = gm.crs_transform(
        7789, 7660, 3657660.66, 255768.55, 5201382.11, source_epoch=2010.0
    )
    assert all(math.isfinite(value) for value in (x_epoch, y_epoch, z_epoch))


def test_dynamic_crs_transform_threads_source_epoch_to_time_lane() -> None:
    transformer = pyproj.Transformer.from_crs(9990, 9000, always_xy=True)
    lon, lat, epoch = 10.0, 50.0, 2020.0
    expected_lon, expected_lat, _ = transformer.transform(lon, lat, tt=epoch)
    no_time_lon, no_time_lat = transformer.transform(lon, lat)

    got_lon, got_lat = gm.crs_transform(9990, 9000, lon, lat, source_epoch=epoch)
    assert (got_lon, got_lat) == pytest.approx((expected_lon, expected_lat), abs=1e-12)
    assert abs(got_lon - no_time_lon) > 1e-9
    assert abs(got_lat - no_time_lat) > 1e-9

    point = gm.Point(lon, lat, crs=9990).set_epoch(epoch)
    transformed = point.to_crs(9000)
    assert (transformed.x, transformed.y) == pytest.approx(
        (expected_lon, expected_lat), abs=1e-12
    )


def test_dynamic_crs_transform_requires_epoch_when_operation_needs_time() -> None:
    with pytest.raises(gm.TransformError, match='requires a coordinate epoch'):
        gm.crs_transform(9990, 9000, 10.0, 50.0)


def test_raw_crs_apply_matches_pyproj_pipeline_oracle() -> None:
    from pyproj.enums import TransformDirection

    operation = '+proj=pipeline +step +proj=affine +xoff=1 +yoff=2 +zoff=3'
    transformer = pyproj.Transformer.from_pipeline(operation)
    xs = [1.0, 10.0]
    ys = [2.0, 20.0]
    zs = [3.0, 30.0]
    ts = [2020.0, 2021.0]
    expected = transformer.transform(xs, ys, zs, tt=ts)
    assert gm.crs_apply(operation, xs[0], ys[0]) == pytest.approx((
        expected[0][0],
        expected[1][0],
    ))
    assert gm.crs_apply(operation, xs[0], ys[0], zs[0], t=ts[0]) == pytest.approx((
        expected[0][0],
        expected[1][0],
        expected[2][0],
        expected[3][0],
    ))
    actual_xs, actual_ys, actual_zs, actual_ts = gm.crs_apply(
        operation, xs, ys, zs, t=ts
    )
    assert actual_xs == pytest.approx(expected[0])
    assert actual_ys == pytest.approx(expected[1])
    assert actual_zs == pytest.approx(expected[2])
    assert actual_ts == pytest.approx(expected[3])
    inverse_expected = transformer.transform(
        expected[0],
        expected[1],
        expected[2],
        tt=expected[3],
        direction=TransformDirection.INVERSE,
    )
    inverse_xs, inverse_ys, inverse_zs, inverse_ts = gm.crs_apply(
        operation,
        expected[0],
        expected[1],
        expected[2],
        t=expected[3],
        direction='inverse',
    )
    assert inverse_xs == pytest.approx(inverse_expected[0])
    assert inverse_ys == pytest.approx(inverse_expected[1])
    assert inverse_zs == pytest.approx(inverse_expected[2])
    assert inverse_ts == pytest.approx(inverse_expected[3])


def test_bundled_aea_validates_apex_before_zero_radius_shortcut() -> None:
    invalid_apex = '+proj=aea +lat_0=23 +lat_1=29.5 +lat_2=45.5 +lon_0=-96 +ellps=GRS80'
    with pytest.raises(gm.TransformError, match='projection'):
        gm.crs_apply(invalid_apex, 0.0, 9928937.004248787, direction='inverse')

    polar = '+proj=aea +lat_0=90 +lat_1=90 +lat_2=90 +lon_0=0 +ellps=GRS80'
    assert gm.crs_apply(polar, 0.0, 0.0, direction='inverse') == pytest.approx(
        (0.0, math.pi / 2.0), abs=1.0e-15
    )


@pytest.mark.parametrize(
    ('operation', 'expected_northing'),
    [
        (
            '+proj=aea +lat_0=23 +lat_1=29.5 +lat_2=45.5 +lon_0=-96 +R=6371000',
            1892300.4087848007,
        ),
        (
            '+proj=aea +lat_0=23 +lat_1=29.5 +lat_2=45.5 +lon_0=-96 +a=7000000 +rf=10',
            1826866.9650963421,
        ),
    ],
    ids=['sphere', 'strong-flattening'],
)
def test_bundled_aea_keeps_sphere_and_strong_flattening_paths(
    operation: str, expected_northing: float
) -> None:
    longitude = math.radians(-96.0)
    latitude = math.radians(40.0)
    projected = gm.crs_apply(operation, longitude, latitude)
    assert projected == pytest.approx((0.0, expected_northing), abs=1.0e-9)
    assert gm.crs_apply(operation, *projected, direction='inverse') == pytest.approx(
        (longitude, latitude), abs=1.0e-14
    )


def test_bundled_aea_strong_flattening_bracket_matches_high_precision_endpoint() -> (
    None
):
    operation = (
        '+proj=aea +lat_0=23 +lat_1=29.5 +lat_2=45.5 +lon_0=-96 +a=7000000 +rf=10'
    )
    projected = gm.crs_apply(operation, -2.0, math.radians(-89.9999))
    recovered = gm.crs_apply(operation, *projected, direction='inverse')
    # Independent 100-digit mpmath q inversion of these exact binary64
    # projected coordinates.
    assert recovered[1] == pytest.approx(-1.570794581068056, abs=3.0e-10)


def test_affine_transforms_match_shapely_affinity_oracle() -> None:
    from shapely import get_coordinates
    from shapely.affinity import rotate, scale, translate
    from shapely.wkt import loads

    polygon = gm.Polygon([(0, 0), (4, 0), (4, 3), (0, 3), (0, 0)])
    oracle = loads(polygon.to_wkt())

    def gometry_coords(geometry: gm.Geometry) -> list[tuple[float, ...]]:
        loaded = cast('Any', loads(geometry.to_wkt()))
        return [tuple(point) for point in get_coordinates(loaded)]

    def oracle_coords(geometry: object) -> list[Any]:
        return [pytest.approx(tuple(p)) for p in get_coordinates(cast('Any', geometry))]

    rotated = polygon.rotate(30.0, origin=(1.0, 2.0))
    oracle_rotated = rotate(oracle, 30.0, origin=(1.0, 2.0), use_radians=False)
    assert gometry_coords(rotated) == oracle_coords(oracle_rotated)
    scaled = polygon.scale(2.0, 3.0, origin=(1.0, 2.0))
    oracle_scaled = scale(oracle, xfact=2.0, yfact=3.0, origin=(1.0, 2.0))
    assert gometry_coords(scaled) == oracle_coords(oracle_scaled)
    translated = polygon.translate(5.0, -7.0)
    oracle_translated = translate(oracle, xoff=5.0, yoff=-7.0)
    assert gometry_coords(translated) == oracle_coords(oracle_translated)
    rotated_default = polygon.rotate(45.0)
    oracle_rotated_default = rotate(oracle, 45.0, origin='centroid')
    assert gometry_coords(rotated_default) == oracle_coords(oracle_rotated_default)


def test_reverse_and_segmentize_carry_z_like_shapely() -> None:
    """Gometry matches shapely when an op carries Z through unchanged."""
    from shapely import from_wkt, get_coordinates, segmentize

    def shy_coords(geometry: Any) -> list[Any]:
        return [tuple(point) for point in get_coordinates(geometry, include_z=True)]

    def sha_coords(geometry: Any) -> list[Any]:
        return [tuple(point) for point in geometry.coords.select('XYZ')]

    wkt = 'LINESTRING Z (0 0 0, 10 0 10)'
    sha = gm.from_wkt(wkt)
    shy = from_wkt(wkt)
    rev = sha.reverse()
    shy_rev = shapely.reverse(shy)
    assert rev.coordinate_axes == 'XYZ'
    assert shy_rev.has_z
    assert sha_coords(rev) == shy_coords(shy_rev)
    seg = sha.segmentize(2.0)
    shy_seg = segmentize(shy, max_segment_length=2.0)
    assert seg.coordinate_axes == 'XYZ'
    assert shy_seg.has_z
    assert sha_coords(seg) == shy_coords(shy_seg)


def test_empty_result_typing_regression() -> None:
    """Vanishing results carry the op's natural typed empty, never an untyped
    GEOMETRYCOLLECTION EMPTY (overlay/clip/boundary/envelope/MRR/reductions).
    """
    poly = 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
    far = 'POLYGON ((5 5, 6 5, 6 6, 5 6, 5 5))'
    line = 'LINESTRING (0.2 0.2, 0.8 0.8)'
    assert (
        gm.intersection(gm.from_wkt(poly), gm.from_wkt(far)).to_wkt() == 'POLYGON EMPTY'
    )
    assert (
        gm.difference(gm.from_wkt(poly), gm.from_wkt(poly)).to_wkt() == 'POLYGON EMPTY'
    )
    assert (
        gm.symmetric_difference(gm.from_wkt(poly), gm.from_wkt(poly)).to_wkt()
        == 'POLYGON EMPTY'
    )
    assert (
        gm.intersection(
            gm.from_wkt(line), gm.from_wkt('LINESTRING (9 9, 10 10)')
        ).to_wkt()
        == 'LINESTRING EMPTY'
    )
    assert (
        gm.intersection(gm.from_wkt('POINT (0 0)'), gm.from_wkt(far)).to_wkt()
        == 'POINT EMPTY'
    )
    assert gm.from_wkt(far).clip_by_rect(0, 0, 2, 2).to_wkt() == 'POLYGON EMPTY'
    assert (
        gm.from_wkt('LINESTRING (9 9, 10 10)').clip_by_rect(0, 0, 2, 2).to_wkt()
        == 'LINESTRING EMPTY'
    )
    assert gm.from_wkt('POINT (9 9)').clip_by_rect(0, 0, 2, 2).to_wkt() == 'POINT EMPTY'
    assert gm.from_wkt('POLYGON EMPTY').boundary().to_wkt() == 'MULTILINESTRING EMPTY'
    assert gm.from_wkt('LINESTRING EMPTY').boundary().to_wkt() == 'MULTIPOINT EMPTY'
    assert gm.from_wkt('POINT EMPTY').boundary().to_wkt() == 'GEOMETRYCOLLECTION EMPTY'
    for empty in (
        'POLYGON EMPTY',
        'LINESTRING EMPTY',
        'POINT EMPTY',
        'GEOMETRYCOLLECTION EMPTY',
    ):
        assert gm.from_wkt(empty).envelope().to_wkt() == 'POLYGON EMPTY', empty
        assert (
            gm.from_wkt(empty).minimum_rotated_rectangle().to_wkt() == 'POLYGON EMPTY'
        ), empty
    for fn in (gm.intersection_all, gm.symmetric_difference_all, gm.union_all):
        with pytest.raises(
            gm.InvalidGeometryError, match='requires at least one geometry'
        ):
            fn([])


def test_validity_repeated_vertices_regression() -> None:
    """Repeated consecutive vertices are removable redundancy (valid/simple,
    while genuine defects remain invalid.
    """
    dup_poly = 'POLYGON ((0 0, 0 0, 4 0, 4 4, 0 4, 0 0))'
    clean_poly = 'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))'
    g, c = (gm.from_wkt(dup_poly), gm.from_wkt(clean_poly))
    assert g.is_valid and g.is_simple and gm.equals(g, c) and (g.area == c.area)
    assert shapely.from_wkt(dup_poly).is_valid
    assert gm.from_wkt('LINESTRING (0 0, 1 1, 1 1, 2 0)').is_simple
    assert (
        gm.from_wkt('LINESTRING (0 0, 1 1, 1 1, 2 0)').self_intersections().__len__()
        == 0
    )
    assert not gm.from_wkt('POLYGON ((0 0, 2 2, 2 0, 0 2, 0 0))').is_valid
    assert not gm.from_wkt('LINESTRING (0 0, 2 0, 1 0)').is_simple
    assert not gm.from_wkt('POLYGON ((0 0, 0 0, 0 0, 0 0))').is_valid
    assert not gm.from_wkt('POLYGON ((0 0, 4 0, 4 0, 0 0))').is_valid
    assert not gm.from_wkt('MULTIPOINT ((0 0), (0 0))').is_simple


def test_minimum_bounding_circle_polygon_regression() -> None:
    """minimum_bounding_circle returns a circle POLYGON with consistent
    handling for empty and coincident inputs.
    """
    circle = gm.from_wkt(
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))'
    ).minimum_bounding_circle()
    assert circle.geometry_type == 'Polygon' and circle.is_valid
    assert circle.area == pytest.approx(math.pi * 8, rel=0.01)
    assert (
        gm.from_wkt('POINT (3 4)').minimum_bounding_circle().to_wkt() == 'POINT (3 4)'
    )
    assert (
        gm.from_wkt('MULTIPOINT ((1 1), (1 1))').minimum_bounding_circle().to_wkt()
        == 'POINT (1 1)'
    )
    assert (
        gm.from_wkt('POLYGON EMPTY').minimum_bounding_circle().to_wkt()
        == 'POLYGON EMPTY'
    )


def test_simplify_never_yields_invalid_polygon_regression() -> None:
    """simplify(preserve_topology=False) must never leak a degenerate invalid
    polygon — a collapsed shell is POLYGON EMPTY.
    """
    sliver = 'POLYGON ((0 0, 4 0, 4 0.01, 0 0))'
    for method in ('dp', 'vw'):
        out = gm.from_wkt(sliver).simplify(1.0, method=method, preserve_topology=False)
        assert out.to_wkt() == 'POLYGON EMPTY' and out.is_valid, method
    assert shapely.simplify(
        shapely.from_wkt(sliver), 1.0, preserve_topology=False
    ).is_empty


def test_predicate_and_overlay_edge_cases_match_shapely_regression() -> None:
    """Predicate verdicts and overlay output TYPES on the classic edge cases
    agree with Shapely (boundary points, endpoints, touches, edge/point contact).
    """
    preds = (
        'contains',
        'covers',
        'within',
        'covered_by',
        'intersects',
        'touches',
        'disjoint',
        'equals',
    )
    cases = [
        ('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))', 'POINT (0 0)'),
        ('LINESTRING (0 0, 1 0)', 'POINT (0 0)'),
        ('LINESTRING (0 0, 1 0, 1 1, 0 0)', 'POINT (0 0)'),
        ('POINT EMPTY', 'POLYGON EMPTY'),
    ]
    for a, b in cases:
        ga, gb = (gm.from_wkt(a), gm.from_wkt(b))
        sa, sb = (shapely.from_wkt(a), shapely.from_wkt(b))
        for p in preds:
            assert getattr(gm, p)(ga, gb) == getattr(sa, p)(sb), f'{p} {a} {b}'
    collapsed = gm.from_wkt('LINESTRING (0 0, 0 0)')
    assert gm.intersects(collapsed, gm.Point(0, 0))
    assert not gm.intersects(collapsed, gm.Point(1, 0))
    a = gm.from_wkt('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))')
    assert (
        gm.intersection(
            a, gm.from_wkt('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))')
        ).geometry_type
        == 'LineString'
    )
    assert (
        gm.intersection(
            a, gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))')
        ).geometry_type
        == 'Point'
    )
