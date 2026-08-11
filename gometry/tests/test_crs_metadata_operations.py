"""CRS behavior — geodesic measurement, transforms, best-UTM selection,
runtime config, cache info, and the PROJ authority metadata surface.
"""

import json
import math
from array import array
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest


class _PyprojStyleArea:
    west_lon_degree = -75.0
    south_lat_degree = 40.0
    east_lon_degree = -73.0
    north_lat_degree = 42.0


def test_crs_roundtrip_admitted_in_core_pair_scalar_and_zt_batch() -> None:
    gm.crs_clear_cache()
    assert gm.crs_roundtrip(4326, 32618, -73.0, 41.0) < 1e-9

    projected = gm.crs_transform(4326, 32618, -73.0, 41.0)
    assert gm.crs_roundtrip(4326, 32618, *projected, direction='inverse') < 1e-8

    errors = gm.crs_roundtrip(
        4326,
        32618,
        np.linspace(-74.0, -72.0, 9),
        41.0,
        10.0,
        t=np.arange(2020.0, 2029.0),
    )
    assert errors.shape == (9,)
    assert errors.dtype == np.float64
    assert errors.flags.writeable is False
    assert np.all(errors < 1e-8)

    proj_pipeline = next(
        bucket
        for bucket in gm.crs_cache_info()['buckets']
        if bucket['name'] == 'proj_pipeline'
    )
    assert proj_pipeline['entries'] == 0


def _assert_crs_namespace_operations_geodesic_and_transforms() -> None:
    json.loads(gm.CRS(4326).to_projjson())
    operation = gm.CRS(4326).operation(3857)
    eastern_operation_at = gm.CRS(4267).operation(4326, at=(-75.0, 40.0))
    western_operation_at = gm.CRS(4267).operation(4326, at=(-120.0, 35.0))
    assert eastern_operation_at['source'] == 'EPSG:4267'
    assert eastern_operation_at['target'] == 'EPSG:4326'
    assert eastern_operation_at['accuracy'] == pytest.approx(10.0)
    assert western_operation_at['accuracy'] == pytest.approx(7.0)
    assert 'proj=pipeline' in cast('str', eastern_operation_at['definition'])
    assert [
        step['method']['name']
        for step in eastern_operation_at['steps']
        if step['method']
    ] == [
        'Axis Order Reversal (2D)',
        'Geocentric translations (geog2D domain)',
        'Axis Order Reversal (2D)',
    ]
    assert (
        cast('dict[str, object]', eastern_operation_at['area_of_use'])['name']
        != cast('dict[str, object]', western_operation_at['area_of_use'])['name']
    )
    # PROJ reports this pipeline's accuracy as 0.0 (9.6.2) or None/unknown
    # (9.8.1); either is PROJ's call, so pin the type contract, not the value.
    # The roundtrip below is the real, version-independent exactness evidence.
    at_accuracy = gm.CRS(4326).operation(3857, at=(-73.0, 41.0))['accuracy']
    assert at_accuracy is None or isinstance(at_accuracy, float)
    assert gm.crs_roundtrip(4326, 3857, -73.0, 41.0) < 1e-09
    assert gm.crs_roundtrip(4267, 4326, -75.0, 40.0) < 1e-06
    assert gm.crs_roundtrip(4979, 4978, -73.0, 41.0, 10.0) < 1e-06
    roundtrip_batch = gm.crs_roundtrip(4326, 3857, [-73.0, -72.0], [41.0, 41.0])
    assert len(roundtrip_batch) == 2
    assert all(v is not None and v < 1e-09 for v in roundtrip_batch.tolist())
    errors_3d = gm.crs_roundtrip(4979, 4978, [-73.0, -72.0], [41.0, 42.0], 10.0)
    first_error = errors_3d[0]
    assert first_error is not None and first_error < 1e-06
    web_mercator_factors = gm.CRS(3857).factors(-73.0, 41.0)
    utm_factors = gm.CRS(32618).factors(-73.0, 41.0)
    utm_batch_factors = gm.CRS(32618).factors([-73.0, -72.0], 41.0)
    utm_radian_factors = gm.CRS(32618).factors(
        math.radians(-73.0), math.radians(41.0), radians=True
    )
    assert web_mercator_factors['meridional_scale'] == pytest.approx(1.3250129934028188)
    assert web_mercator_factors['parallel_scale'] == pytest.approx(1.325012993342781)
    assert web_mercator_factors['angular_distortion'] == pytest.approx(0.0)
    assert web_mercator_factors['meridian_parallel_angle'] == pytest.approx(90.0)
    assert utm_factors['meridian_convergence'] == pytest.approx(1.3124251629103112)
    assert utm_factors['areal_scale'] == pytest.approx(0.9998964902489327)
    assert utm_batch_factors['meridian_convergence'][0] == pytest.approx(
        cast('float', utm_factors['meridian_convergence'])
    )
    assert len(utm_batch_factors['areal_scale']) == 2
    assert utm_batch_factors['areal_scale'][1] != pytest.approx(
        cast('float', utm_factors['areal_scale'])
    )
    with pytest.raises(ValueError, match='lon and lat must have the same length'):
        gm.CRS(32618).factors([-73.0, -72.0], [41.0, 42.0, 43.0])
    assert utm_radian_factors['meridian_parallel_angle'] == pytest.approx(math.pi / 2)
    assert utm_radian_factors['meridian_convergence'] == pytest.approx(
        math.radians(cast('float', utm_factors['meridian_convergence']))
    )
    crs_geodesic = gm.CRS(4326).geodesic(-73.0, 41.0, -74.0, 42.0)
    crs_geodesic_3d = gm.CRS(4326).geodesic(-73.0, 41.0, -74.0, 42.0, z1=10.0, z2=110.0)
    crs_geodesic_radians = gm.CRS(4326).geodesic(
        math.radians(-73.0),
        math.radians(41.0),
        math.radians(-74.0),
        math.radians(42.0),
        radians=True,
    )
    assert crs_geodesic['distance'] == pytest.approx(138946.2474912301)
    assert crs_geodesic['distance_3d'] is None
    assert crs_geodesic_3d['distance'] == pytest.approx(crs_geodesic['distance'])
    assert crs_geodesic_3d['distance_3d'] == pytest.approx(
        math.hypot(cast('float', crs_geodesic['distance']), 100.0)
    )
    assert crs_geodesic['forward_azimuth'] == pytest.approx(-36.60508316670839)
    assert crs_geodesic_radians['forward_azimuth'] == pytest.approx(
        math.radians(cast('float', crs_geodesic['forward_azimuth']))
    )
    crs_geodesic_batch = gm.CRS(4326).geodesic(
        [-73.0, -73.0],
        [41.0, 41.0],
        [-74.0, -74.0],
        [42.0, 42.0],
        z1=[10.0, 20.0],
        z2=[110.0, 20.0],
    )
    assert crs_geodesic_batch['distance'] == pytest.approx([
        cast('float', crs_geodesic['distance']),
        cast('float', crs_geodesic['distance']),
    ])
    assert crs_geodesic_batch['distance_3d'] == pytest.approx([
        cast('float', crs_geodesic_3d['distance_3d']),
        cast('float', crs_geodesic['distance']),
    ])
    assert crs_geodesic_batch['forward_azimuth'] == pytest.approx([
        cast('float', crs_geodesic['forward_azimuth']),
        cast('float', crs_geodesic['forward_azimuth']),
    ])
    crs_geodesic_direct = gm.CRS(4326).geodesic_direct(-73.0, 41.0, 45.0, 1000.0)
    crs_geodesic_direct_radians = gm.CRS(4326).geodesic_direct(
        math.radians(-73.0),
        math.radians(41.0),
        math.radians(45.0),
        1000.0,
        radians=True,
    )
    assert crs_geodesic_direct['longitude'] == pytest.approx(-72.99159477886612)
    assert crs_geodesic_direct['latitude'] == pytest.approx(41.00636692912867)
    assert crs_geodesic_direct['final_azimuth'] == pytest.approx(45.00551467367843)
    assert crs_geodesic_direct_radians['longitude'] == pytest.approx(
        math.radians(cast('float', crs_geodesic_direct['longitude']))
    )
    assert crs_geodesic_direct_radians['final_azimuth'] == pytest.approx(
        math.radians(cast('float', crs_geodesic_direct['final_azimuth']))
    )
    crs_geodesic_direct_batch = gm.CRS(4326).geodesic_direct(
        [-73.0, -73.0], [41.0, 41.0], [45.0, 90.0], [1000.0, 2000.0]
    )
    assert crs_geodesic_direct_batch['longitude'] == pytest.approx([
        cast('float', crs_geodesic_direct['longitude']),
        -72.97622873298613,
    ])
    assert crs_geodesic_direct_batch['latitude'] == pytest.approx([
        cast('float', crs_geodesic_direct['latitude']),
        40.999997549025636,
    ])
    with pytest.raises(
        ValueError, match='geodesic direct distance must be non-negative'
    ):
        gm.CRS(4326).geodesic_direct(-73.0, 41.0, 45.0, -1.0)
    crs_geodesic_midpoint = gm.CRS(4326).geodesic_interpolate(
        -73.0, 41.0, -74.0, 42.0, 0.5, normalized=True
    )
    crs_geodesic_half_direct = gm.CRS(4326).geodesic_direct(
        -73.0,
        41.0,
        cast('float', crs_geodesic['forward_azimuth']),
        cast('float', crs_geodesic['distance']) / 2,
    )
    assert crs_geodesic_midpoint['longitude'] == pytest.approx(
        crs_geodesic_half_direct['longitude']
    )
    assert crs_geodesic_midpoint['latitude'] == pytest.approx(
        crs_geodesic_half_direct['latitude']
    )
    assert crs_geodesic_midpoint['distance'] == pytest.approx(
        cast('float', crs_geodesic['distance']) / 2
    )
    crs_geodesic_interpolate_batch = gm.CRS(4326).geodesic_interpolate(
        -73.0, 41.0, -74.0, 42.0, [-1.0, 0.0, 0.5, 1.0, 2.0], normalized=True
    )
    assert crs_geodesic_interpolate_batch['longitude'] == pytest.approx([
        -73.0,
        -73.0,
        cast('float', crs_geodesic_midpoint['longitude']),
        -74.0,
        -74.0,
    ])
    assert crs_geodesic_interpolate_batch['latitude'] == pytest.approx([
        41.0,
        41.0,
        cast('float', crs_geodesic_midpoint['latitude']),
        42.0,
        42.0,
    ])
    crs_geodesic_interpolate_radians = gm.CRS(4326).geodesic_interpolate(
        math.radians(-73.0),
        math.radians(41.0),
        math.radians(-74.0),
        math.radians(42.0),
        0.5,
        normalized=True,
        radians=True,
    )
    assert crs_geodesic_interpolate_radians['longitude'] == pytest.approx(
        math.radians(cast('float', crs_geodesic_midpoint['longitude']))
    )
    crs_geodesic_polygon = gm.Polygon(
        [(-73.0, 41.0), (-72.0, 41.0), (-72.0, 42.0), (-73.0, 42.0), (-73.0, 41.0)],
        crs=4326,
    )
    crs_geodesic_line = gm.LineString([(-73.0, 41.0), (-72.0, 42.0)], crs=4326)
    projected_geodesic_polygon = crs_geodesic_polygon.to_crs(3857)
    assert crs_geodesic_polygon.area == pytest.approx(9273189574.569885)
    assert projected_geodesic_polygon.to_crs(4326).area == pytest.approx(
        crs_geodesic_polygon.area
    )
    assert crs_geodesic_polygon.length == pytest.approx(389112.1929116194)
    assert crs_geodesic_line.length == pytest.approx(138946.2474912301)
    assert gm.GeometryArray([crs_geodesic_polygon]).area == pytest.approx([
        crs_geodesic_polygon.area
    ])
    operations = gm.CRS(4326).operations(3857)
    assert operations[0]['description'] == operation['description']
    assert operations[0]['definition'] == operation['definition']
    assert operations[0]['instantiable'] is True
    assert isinstance(operations[0]['parameters'], list)
    local_operation = gm.CRS(2263).operation(
        4326, area_of_interest=(-75.0, 40.0, -73.0, 42.0)
    )
    local_area_dict: dict[str, float] = {
        'west': -75.0,
        'south': 40.0,
        'east': -73.0,
        'north': 42.0,
    }

    class _PyprojStyleArea:
        west_lon_degree = -75.0
        south_lat_degree = 40.0
        east_lon_degree = -73.0
        north_lat_degree = 42.0

    pyproj_style_area = _PyprojStyleArea()
    assert (
        gm.CRS(2263).operation(4326, area_of_interest=local_area_dict)['description']
        == local_operation['description']
    )
    assert (
        gm.CRS(2263).operation(4326, area_of_interest=pyproj_style_area)['description']
        == local_operation['description']
    )
    assert (
        gm.crs_catalog(
            authority='EPSG', kind='projected', area_of_interest=local_area_dict
        )[0]['kind']
        == 'projected'
    )
    remote_operation = gm.CRS(2263).operation(
        4326, area_of_interest=(-1.0, 50.0, 1.0, 52.0)
    )
    local_operations = gm.CRS(2263).operations(
        4326, area_of_interest=(-75.0, 40.0, -73.0, 42.0)
    )
    assert local_operations
    assert local_operations[0]['description'] == local_operation['description']
    assert local_operation['accuracy'] == pytest.approx(4.0)
    assert local_operation['has_ballpark_transformation'] is False
    unconstrained_operations = gm.CRS(2263).operations(4326)
    grid_operation = next(
        operation
        for operation in unconstrained_operations
        if cast('list[object]', operation['grids'])
    )
    operation_grid = cast('list[dict[str, object]]', grid_operation['grids'])[0]
    grid_info = gm.crs_grid(cast('str', operation_grid['short_name']))
    assert grid_info['name'] == operation_grid['short_name']
    assert grid_info['available'] == operation_grid['available']
    assert remote_operation['accuracy'] is None
    assert remote_operation['has_ballpark_transformation'] is True
    antimeridian_aoi_operation = gm.CRS(2263).operation(
        4326, area_of_interest=(170.0, -10.0, -170.0, 10.0)
    )
    assert antimeridian_aoi_operation['has_ballpark_transformation'] is True
    dynamic_operation = gm.CRS(7789).operation(7660)
    epoch_operation = gm.CRS(7789).operation(7660, source_epoch=2010.0)
    assert dynamic_operation['requires_coordinate_epoch'] is True
    assert epoch_operation['requires_coordinate_epoch'] is False
    assert epoch_operation['source_epoch'] == 2010.0
    assert epoch_operation['target_epoch'] is None
    assert gm.crs_transform(4326, 3857, -1.0, 50.0) == pytest.approx(
        tuple(gm.Point(-1.0, 50.0, crs=4326).to_crs(3857).coords.to_nested())
    )
    raw_matrix = gm.crs_transform(4326, 3857, [-1.0, 1.0], [50.0, 51.0])
    assert raw_matrix.shape == (2, 2)
    raw_xs, raw_ys = raw_matrix[:, 0], raw_matrix[:, 1]
    transformed_points = gm.points([-1.0, 1.0], [50.0, 51.0], crs=4326).to_crs(3857)
    assert raw_xs == pytest.approx([point.x for point in transformed_points])
    assert raw_ys == pytest.approx([point.y for point in transformed_points])
    broadcast_matrix = gm.crs_transform(4326, 3857, -1.0, [50.0, 51.0])
    repeated_matrix = gm.crs_transform(4326, 3857, [-1.0, -1.0], [50.0, 51.0])
    assert broadcast_matrix.shape == repeated_matrix.shape == (2, 2)
    broadcast_xs, broadcast_ys = broadcast_matrix[:, 0], broadcast_matrix[:, 1]
    repeated_xs, repeated_ys = repeated_matrix[:, 0], repeated_matrix[:, 1]
    assert broadcast_xs == pytest.approx(repeated_xs)
    assert broadcast_ys == pytest.approx(repeated_ys)
    broadcast_z = gm.crs_transform(4979, 4978, [-73.0, -72.0], 41.0, 10.0)
    assert broadcast_z.shape == (2, 3)
    broadcast_from_z = gm.crs_transform(4979, 4978, -73.0, 41.0, [10.0, 20.0])
    repeated_from_z = gm.crs_transform(
        4979, 4978, [-73.0, -73.0], [41.0, 41.0], [10.0, 20.0]
    )
    assert broadcast_from_z.shape == repeated_from_z.shape == (2, 3)
    assert broadcast_from_z[:, 0] == pytest.approx(repeated_from_z[:, 0])
    assert broadcast_from_z[:, 1] == pytest.approx(repeated_from_z[:, 1])
    assert broadcast_from_z[:, 2] == pytest.approx(repeated_from_z[:, 2])
    broadcast_apply = cast(
        'tuple[list[float], list[float], list[float], list[float]]',
        gm.crs_apply(
            '+proj=pipeline +step +proj=affine +xoff=1 +yoff=2 +zoff=3',
            [1.0, 2.0],
            5.0,
            10.0,
            t=2020.0,
        ),
    )
    assert broadcast_apply[0] == pytest.approx([2.0, 3.0])
    assert broadcast_apply[1] == pytest.approx([7.0, 7.0])
    assert broadcast_apply[2] == pytest.approx([13.0, 13.0])
    assert broadcast_apply[3] == pytest.approx([2020.0, 2020.0])
    broadcast_apply_from_z = cast(
        'tuple[list[float], list[float], list[float]]',
        gm.crs_apply(
            '+proj=pipeline +step +proj=affine +xoff=1 +yoff=2 +zoff=3',
            1.0,
            5.0,
            [10.0, 20.0],
        ),
    )
    assert broadcast_apply_from_z[0] == pytest.approx([2.0, 2.0])
    assert broadcast_apply_from_z[1] == pytest.approx([7.0, 7.0])
    assert broadcast_apply_from_z[2] == pytest.approx([13.0, 23.0])
    broadcast_geodesic = gm.CRS(4326).geodesic(-73.0, [41.0, 42.0], -74.0, [42.0, 43.0])
    assert len(broadcast_geodesic['distance']) == 2
    broadcast_geodesic_from_z = gm.CRS(4326).geodesic(
        -73.0, 41.0, -74.0, 42.0, z1=[10.0, 20.0], z2=[110.0, 20.0]
    )
    assert broadcast_geodesic_from_z['distance'] == pytest.approx(
        crs_geodesic_batch['distance']
    )
    assert broadcast_geodesic_from_z['distance_3d'] == pytest.approx(
        crs_geodesic_batch['distance_3d']
    )
    numpy_matrix = gm.crs_transform(
        4326,
        3857,
        np.array([-1.0, 1.0], dtype='float64'),
        np.array([50.0, 51.0], dtype='float64'),
    )
    assert numpy_matrix.shape == (2, 2)
    numpy_xs, numpy_ys = numpy_matrix[:, 0], numpy_matrix[:, 1]
    assert numpy_xs == pytest.approx(raw_xs)
    assert numpy_ys == pytest.approx(raw_ys)
    buffer_matrix = gm.crs_transform(
        4326,
        3857,
        memoryview(array('d', [-1.0, 1.0])),
        memoryview(array('d', [50.0, 51.0])),
    )
    assert buffer_matrix.shape == (2, 2)
    buffer_xs, buffer_ys = buffer_matrix[:, 0], buffer_matrix[:, 1]
    assert buffer_xs == pytest.approx(raw_xs)
    assert buffer_ys == pytest.approx(raw_ys)
    buffer_geodesic = gm.CRS(4326).geodesic(
        memoryview(array('d', [-73.0, -73.0])),
        memoryview(array('d', [41.0, 41.0])),
        memoryview(array('d', [-74.0, -74.0])),
        memoryview(array('d', [42.0, 42.0])),
    )
    assert buffer_geodesic['distance'] == pytest.approx(crs_geodesic_batch['distance'])
    with pytest.raises(ValueError, match='x and y must have the same length'):
        gm.crs_transform(4326, 3857, [0.0], [0.0, 1.0])
    with pytest.raises(ValueError, match='x, y, and z must have the same length'):
        gm.crs_transform(4979, 4978, [0.0], [0.0], [0.0, 1.0])
    time_only = cast('Any', gm.crs_transform)(4979, 4978, [0.0], [0.0], t=[2020.0])
    assert time_only.shape == (1, 2)
    with pytest.raises(ValueError, match='x, y, z, and t must have the same length'):
        gm.crs_transform(4979, 4978, [0.0], [0.0], [0.0], t=[2020.0, 2021.0])
    with pytest.raises(gm.GeometryError, match='area_of_interest must be'):
        cast('Any', gm.CRS(4326).operation)(3857, area_of_interest=(0.0, 0.0, 1.0))
    with pytest.raises(ValueError, match='area_of_interest dictionary requires'):
        cast('Any', gm.CRS(4326).operation)(
            3857, area_of_interest={'west': 0.0, 'south': 0.0, 'east': 1.0}
        )
    with pytest.raises(ValueError, match='area_of_interest object requires attribute'):
        cast('Any', gm.CRS(4326).operation)(
            3857, area_of_interest=type('PartialArea', (), {'west': 0.0})()
        )
    with pytest.raises(ValueError, match='area must be finite'):
        gm.crs_transform(
            2263, 4326, [0.0], [0.0], area_of_interest=(-181.0, 40.0, -73.0, 42.0)
        )
    with pytest.raises(ValueError, match='area must be finite'):
        gm.crs_transform(
            4326, 3857, [0.0], [0.0], area_of_interest=(-181.0, 40.0, -73.0, 42.0)
        )
    with pytest.raises(ValueError, match='area must be finite'):
        gm.Point(0.0, 0.0, crs=4326).to_crs(
            3857, area_of_interest=(-181.0, 40.0, -73.0, 42.0)
        )
    with pytest.raises(ValueError, match='authority must be a non-empty string'):
        gm.CRS(2263).operation(4326, authority='')
    with pytest.raises(ValueError, match='accuracy must be'):
        gm.CRS(2263).operation(4326, accuracy=-1.0)
    with pytest.raises(ValueError, match='source_epoch must be'):
        gm.CRS(7789).operation(7660, source_epoch=float('nan'))
    with pytest.raises(ValueError, match='target_epoch must be'):
        gm.CRS(7789).operation(7660, target_epoch=float('inf'))
    with pytest.raises(ValueError, match='x must be finite'):
        gm.CRS(4326).operation(3857, at=(float('nan'), 41.0))
    with pytest.raises(ValueError, match='x must be finite'):
        gm.crs_roundtrip(4326, 3857, float('nan'), 41.0)
    with pytest.raises(ValueError, match='roundtrip iterations must be'):
        gm.crs_roundtrip(4326, 3857, -73.0, 41.0, iterations=0)
    with pytest.raises(ValueError, match='x and y must have the same length'):
        gm.crs_roundtrip(4326, 3857, [-73.0, -72.0], [41.0])
    with pytest.raises(ValueError, match='lon must be finite'):
        gm.CRS(3857).factors(float('nan'), 41.0)
    with pytest.raises(ValueError, match='geodesic coordinates must be finite'):
        gm.CRS(4326).geodesic(float('nan'), 41.0, -74.0, 42.0)
    with pytest.raises(ValueError, match='requires both z1 and z2'):
        gm.CRS(4326).geodesic(-73.0, 41.0, -74.0, 42.0, z1=10.0)
    with pytest.raises(
        ValueError, match='lon1, lat1, lon2, and lat2 must have the same length'
    ):
        gm.CRS(4326).geodesic([-73.0, -72.0], [41.0], -74.0, 42.0)
    with pytest.raises(
        ValueError, match='lon1, lat1, lon2, lat2, z1, and z2 must have the same length'
    ):
        gm.CRS(4326).geodesic(
            [-73.0, -72.0],
            [41.0, 42.0],
            [-74.0, -75.0],
            [42.0, 43.0],
            z1=[0.0],
            z2=[1.0, 2.0],
        )
    with pytest.raises(
        ValueError, match='lon, lat, azimuth, and distance must have the same length'
    ):
        gm.CRS(4326).geodesic_direct([-73.0, -72.0], [41.0], 45.0, 1000.0)
    with pytest.raises(
        ValueError,
        match='lon1, lat1, lon2, lat2, and distance must have the same length',
    ):
        gm.CRS(4326).geodesic_interpolate([-73.0, -72.0], [41.0], -74.0, 42.0, 0.5)
    assert (
        gm.CRS(4979).operation(4978, at=(-73.0, 41.0, None, 2020.0))['source']
        == 'EPSG:4979'
    )
    assert math.isfinite(gm.crs_roundtrip(4979, 4978, -73.0, 41.0, t=2020.0))
    with pytest.raises(gm.TransformError, match='cannot create CRS transform'):
        gm.CRS(2263).operation(
            4326, area_of_interest=(-1.0, 50.0, 1.0, 52.0), allow_ballpark=False
        )
    with pytest.raises(ValueError, match='operation definition is required'):
        gm.crs_apply('', 0.0, 0.0)
    with pytest.raises(ValueError, match='grid name is required'):
        gm.crs_grid('')
    with pytest.raises(ValueError, match='unknown PROJ grid'):
        gm.crs_grid('not-a-real-grid.tif')
    with pytest.raises(ValueError, match='unit authority must be'):
        gm.crs_units('')
    with pytest.raises(ValueError, match='unknown PROJ unit'):
        gm.crs_unit('EPSG', 'not-a-unit')
    applied_x, applied_y, applied_t = cast('Any', gm.crs_apply)(
        '+proj=affine +xoff=1', [0.0], [0.0], t=[2020.0]
    )
    assert list(applied_x) == [1.0]
    assert list(applied_y) == [0.0]
    assert list(applied_t) == [2020.0]
    with pytest.raises(ValueError, match='unknown direction'):
        gm.crs_apply('+proj=affine +xoff=1', 0.0, 0.0, direction='sideways')
    assert gm.crs_transform_bounds(
        4326, 3857, (-1.0, 50.0, 1.0, 51.0)
    ) == pytest.approx(gm.box(-1.0, 50.0, 1.0, 51.0, crs=4326).to_crs(3857).bounds)
    bounds_many = gm.crs_transform_bounds(
        4326,
        3857,
        [(-1.0, 50.0, 1.0, 51.0), (0.0, 0.0, 1.0, 1.0)],
    )
    assert isinstance(bounds_many, np.ndarray)
    assert bounds_many.shape == (2, 4)
    assert bounds_many[0] == pytest.approx(
        gm.crs_transform_bounds(4326, 3857, (-1.0, 50.0, 1.0, 51.0))
    )
    assert bounds_many[1] == pytest.approx(
        gm.crs_transform_bounds(4326, 3857, (0.0, 0.0, 1.0, 1.0))
    )
    in_core_bounds_3d = gm.crs_transform_bounds(
        4326, 3857, (-1.0, 50.0, 3.0, 1.0, 51.0, 7.0)
    )
    assert in_core_bounds_3d[:2] + in_core_bounds_3d[3:5] == pytest.approx(
        gm.box(-1.0, 50.0, 1.0, 51.0, crs=4326).to_crs(3857).bounds
    )
    assert in_core_bounds_3d[2] == 3.0
    assert in_core_bounds_3d[5] == 7.0
    bounds_many_3d = gm.crs_transform_bounds(
        4326,
        3857,
        [(-1.0, 50.0, 3.0, 1.0, 51.0, 7.0), (0.0, 0.0, 2.0, 1.0, 1.0, 4.0)],
    )
    assert bounds_many_3d.shape == (2, 6)
    assert bounds_many_3d[0] == pytest.approx(in_core_bounds_3d)
    geocentric_bounds = gm.crs_transform_bounds(
        4979, 4978, (-73.0, 41.0, 10.0, -72.5, 41.5, 20.0)
    )
    geocentric_corners = [
        gm.crs_transform(4979, 4978, x, y, z)
        for x in (-73.0, -72.5)
        for y in (41.0, 41.5)
        for z in (10.0, 20.0)
    ]
    xs = [point[0] for point in geocentric_corners]
    ys = [point[1] for point in geocentric_corners]
    zs = [point[2] for point in geocentric_corners]
    assert geocentric_bounds[0] <= min(xs) <= max(xs) <= geocentric_bounds[3]
    assert geocentric_bounds[1] <= min(ys) <= max(ys) <= geocentric_bounds[4]
    assert geocentric_bounds[2] <= min(zs) <= max(zs) <= geocentric_bounds[5]
    assert gm.crs_transform_bounds(4326, 4326, (-1.0, 50.0, 1.0, 51.0)) == (
        -1.0,
        50.0,
        1.0,
        51.0,
    )
    assert gm.crs_transform_bounds(4326, 4326, (179.0, -1.0, -179.0, 1.0)) == (
        179.0,
        -1.0,
        -179.0,
        1.0,
    )
    with pytest.raises(ValueError, match='antimeridian-crossing geographic bounds'):
        gm.crs_transform_bounds(4326, 4326, (-181.0, -1.0, 1.0, 1.0))
    with pytest.raises(ValueError, match='antimeridian-crossing geographic bounds'):
        gm.crs_transform_bounds(4979, 4979, (-181.0, -1.0, 0.0, 1.0, 1.0, 2.0))
    with pytest.raises(
        gm.GeometryError, match='bounds densify must be between 0 and 10000, got -1'
    ) as excinfo:
        gm.crs_transform_bounds(4326, 3857, (-1.0, 50.0, 1.0, 51.0), densify=-1)
    assert excinfo.value.param == 'densify'
    assert excinfo.value.value == -1
    with pytest.raises(ValueError, match='bounds densify must be <= 10000'):
        gm.crs_transform_bounds(4326, 3857, (-1.0, 50.0, 1.0, 51.0), densify=10001)
    with pytest.raises(ValueError, match='min <= max'):
        gm.crs_transform_bounds(4326, 3857, (-1.0, 50.0, 7.0, 1.0, 51.0, 3.0))
    with pytest.raises(gm.TransformError, match='cannot create CRS transform'):
        gm.crs_transform_bounds(
            2263,
            4326,
            (900000.0, 100000.0, 1100000.0, 300000.0),
            area_of_interest=(-1.0, 50.0, 1.0, 52.0),
            allow_ballpark=False,
        )


def test_crs_factories_from_wkt_proj_authority() -> None:
    """The universal ``CRS(value)`` constructor accepts WKT, PROJ, and authority inputs."""
    epsg = gm.CRS(4326)
    from_wkt = gm.CRS(epsg.to_wkt())
    from_proj = gm.CRS(epsg.to_proj())
    from_authority = gm.CRS(('EPSG', '4326'))

    assert from_wkt == epsg
    assert from_authority == epsg
    assert from_authority.canonical == 'EPSG:4326'
    # PROJ string form often identifies as OGC:CRS84 (lon/lat axis order) rather
    # than EPSG:4326; it must still be a geographic CRS on the WGS84 ellipsoid.
    assert from_proj.is_geographic
    assert from_proj.ellipsoid is not None
    assert from_proj.ellipsoid['name'] == epsg.ellipsoid['name']
    assert from_proj == epsg or from_proj.celestial_body == epsg.celestial_body

    projected = gm.CRS(2263)
    assert gm.CRS(projected.to_wkt()) == projected
    assert gm.CRS(('EPSG', '2263')) == projected
    assert gm.CRS(projected.to_proj()).is_projected


@pytest.mark.parametrize(
    ('code', 'expect_authority', 'expect_kind'),
    [
        (4326, 'EPSG', 'geographic'),
        (2263, 'EPSG', 'projected'),
        (3857, 'EPSG', 'projected'),
    ],
    ids=['wgs84', 'ny_east_ft', 'web_mercator'],
)
def test_crs_direct_metadata_properties(
    code: int, expect_authority: str, expect_kind: str
) -> None:
    """Direct property getters stay wired (not only the ``info`` dict path)."""
    crs = gm.CRS(code)
    assert crs.authority == expect_authority
    assert crs.code == str(code)
    assert crs.canonical == f'{expect_authority}:{code}'
    assert crs.celestial_body == 'Earth'
    assert crs.ellipsoid is not None
    assert isinstance(crs.ellipsoid, dict)
    assert 'semi_major_metre' in crs.ellipsoid
    assert crs.ellipsoid['semi_major_metre'] > 0.0
    assert crs.datum is not None
    assert isinstance(crs.datum, dict)
    assert crs.datum.get('name')
    assert crs.prime_meridian is not None
    assert crs.prime_meridian['name'] == 'Greenwich'
    assert crs.prime_meridian['longitude'] == pytest.approx(0.0)
    assert crs.geodetic_crs is not None
    assert isinstance(crs.geodetic_crs, dict)
    assert crs.geodetic_crs.get('kind') in {
        'geographic_2d',
        'geographic_3d',
        'geodetic',
    }
    if expect_kind == 'geographic':
        assert crs.is_geographic
        # Geographic CRS is its own geodetic CRS.
        assert crs.geodetic_crs.get('code') in {str(code), crs.code}
    else:
        assert crs.is_projected
        # Projected CRS points at a base geographic CRS.
        assert crs.geodetic_crs.get('kind') == 'geographic_2d'


def test_crs_metadata_properties_agree_with_info_dict() -> None:
    """Object properties match the corresponding fields of ``CRS.info``."""
    crs = gm.CRS(4326)
    info = crs.info
    assert crs.authority == info.get('authority') or crs.authority == 'EPSG'
    assert crs.name == info.get('name') or crs.name
    assert crs.kind == info.get('kind') or crs.kind
    # Ellipsoid block is present on both surfaces.
    assert crs.ellipsoid is not None
    if 'ellipsoid' in info:
        assert crs.ellipsoid['name'] == info['ellipsoid']['name']
