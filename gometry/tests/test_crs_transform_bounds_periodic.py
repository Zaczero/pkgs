from __future__ import annotations

import math
from typing import Any

import gometry as gm
import pytest

WEB_RADIUS = 6_378_137.0
WEB_SEAM = math.pi * WEB_RADIUS
RADIAN_WGS84 = (
    'GEOGCS["WGS 84 rad",'
    'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["radian",1],'
    'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
)
NEAR_GREENWICH_WGS84 = (
    'GEOGCS["WGS 84 near Greenwich",'
    'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563],'
    'AUTHORITY["EPSG","6326"]],'
    'PRIMEM["Greenwich",0.00000000005],'
    'UNIT["degree",0.0174532925199433],'
    'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
)


def _web_y(latitude: float) -> float:
    radians = math.radians(latitude)
    return WEB_RADIUS * math.asinh(math.tan(radians))


@pytest.mark.parametrize(
    'options, engine', [({}, 'in_core'), ({'force_over': True}, 'proj')]
)
def test_wrapped_bounds_use_one_plan_under_each_backend(
    options: dict[str, Any], engine: str
) -> None:
    gm.crs_clear_cache()
    transformed = gm.crs_transform_bounds(
        4326,
        3857,
        (170.0, -10.0, -170.0, 10.0),
        densify=2,
        **options,
    )
    assert transformed == pytest.approx((
        -WEB_SEAM,
        _web_y(-10.0),
        WEB_SEAM,
        _web_y(10.0),
    ))
    assert gm.crs_cache_info()['last_transform_engine'] == engine


@pytest.mark.parametrize(
    'options, engine', [({}, 'in_core'), ({'force_over': True}, 'proj')]
)
def test_geographic_target_uses_minimal_wrapped_cover(
    options: dict[str, Any], engine: str
) -> None:
    west = WEB_RADIUS * math.radians(179.0)
    east = WEB_RADIUS * math.radians(181.0)
    gm.crs_clear_cache()
    transformed = gm.crs_transform_bounds(
        3857,
        4326,
        (west, _web_y(-1.0), east, _web_y(1.0)),
        densify=2,
        **options,
    )
    assert transformed == pytest.approx((179.0, -1.0, -179.0, 1.0))
    assert transformed[0] > transformed[2]
    assert gm.crs_cache_info()['last_transform_engine'] == engine


@pytest.mark.parametrize(
    'options, engine', [({}, 'in_core'), ({'authority': 'EPSG'}, 'proj')]
)
def test_web_mercator_bounds_exclude_unrepresentable_target_poles(
    options: dict[str, Any], engine: str
) -> None:
    half_extent = 100.0
    expected_latitude = math.degrees(math.atan(math.sinh(half_extent / WEB_RADIUS)))
    transformed = gm.crs_transform_bounds(
        3857,
        4326,
        (-half_extent, -half_extent, half_extent, half_extent),
        densify=2,
        **options,
    )
    assert transformed == pytest.approx(
        (
            -math.degrees(half_extent / WEB_RADIUS),
            -expected_latitude,
            math.degrees(half_extent / WEB_RADIUS),
            expected_latitude,
        ),
        abs=1e-12,
    )
    assert -90.0 < transformed[1] < transformed[3] < 90.0
    assert gm.crs_cache_info()['last_transform_engine'] == engine


@pytest.mark.parametrize(
    ('source', 'expected'),
    [
        (3413, (-180.0, 83.47926098750534, 180.0, 90.0)),
        (3031, (-180.0, -90.0, 180.0, -83.49873281319081)),
    ],
    ids=['north-polar', 'south-polar'],
)
@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_projected_bounds_containing_a_pole_use_full_target_longitude(
    source: int, expected: tuple[float, float, float, float], force_over: bool
) -> None:
    gm.crs_clear_cache()
    transformed = gm.crs_transform_bounds(
        source,
        4326,
        (-500_000.0, -500_000.0, 500_000.0, 500_000.0),
        densify=21,
        force_over=force_over,
    )
    assert transformed == pytest.approx(expected, abs=1e-12)
    assert transformed[0:3:2] == (-180.0, 180.0)
    assert gm.crs_cache_info()['last_transform_engine'] == (
        'proj' if force_over else 'in_core'
    )


def test_projected_pole_bounds_match_pyproj_when_available() -> None:
    pyproj = pytest.importorskip('pyproj')
    bounds = (-500_000.0, -500_000.0, 500_000.0, 500_000.0)
    for source in (3413, 3031):
        expected = pyproj.Transformer.from_crs(
            source, 4326, always_xy=True
        ).transform_bounds(*bounds, densify_pts=21)
        assert gm.crs_transform_bounds(
            source, 4326, bounds, densify=21
        ) == pytest.approx(expected, abs=1e-10)


@pytest.mark.parametrize('source', [3413, 3031], ids=['north-polar', 'south-polar'])
@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_pole_containment_in_three_dimensions_does_not_widen_z(
    source: int, force_over: bool
) -> None:
    transformed = gm.crs_transform_bounds(
        source,
        4326,
        (-500_000.0, -500_000.0, 7.0, 500_000.0, 500_000.0, 11.0),
        densify=21,
        force_over=force_over,
    )
    assert transformed[0] == pytest.approx(-180.0)
    assert transformed[2] == pytest.approx(7.0)
    assert transformed[3] == pytest.approx(180.0)
    assert transformed[4] == pytest.approx(
        90.0 if source == 3413 else -83.49873281319081
    )
    if source == 3031:
        assert transformed[1] == pytest.approx(-90.0)
    else:
        assert transformed[1] == pytest.approx(83.47926098750534, abs=1e-12)
    assert transformed[5] == pytest.approx(11.0)


@pytest.mark.parametrize('source', [3413, 3031], ids=['north-polar', 'south-polar'])
def test_pole_on_source_boundary_does_not_force_full_target_longitude(
    source: int,
) -> None:
    transformed = gm.crs_transform_bounds(
        source,
        4326,
        (0.0, 0.0, 500_000.0, 500_000.0),
        densify=21,
    )
    assert transformed[0:3:2] != (-180.0, 180.0)


def test_bulk_pole_containment_uses_the_same_probe_as_single_bounds() -> None:
    transformed = gm.crs_transform_bounds(
        3413,
        4326,
        [
            (-500_000.0, -500_000.0, 500_000.0, 500_000.0),
            (0.0, 0.0, 500_000.0, 500_000.0),
        ],
        densify=21,
    )
    assert transformed[0] == pytest.approx(
        (-180.0, 83.47926098750534, 180.0, 90.0), abs=1e-12
    )
    assert tuple(transformed[1][0:3:2]) != (-180.0, 180.0)


def test_pole_containment_uses_target_frame_units_and_prime_meridian() -> None:
    shifted_target = (
        'GEOGCS["WGS 84 shifted",'
        'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Rome",10,AUTHORITY["EPSG","8906"]],'
        'UNIT["degree",0.0174532925199433],'
        'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
    )
    transformed = gm.crs_transform_bounds(
        3413,
        shifted_target,
        (-500_000.0, -500_000.0, 500_000.0, 500_000.0),
        densify=21,
    )
    assert transformed == pytest.approx(
        (-180.0, 83.47926098750534, 180.0, 90.0), abs=1e-12
    )

    transformed_radians = gm.crs_transform_bounds(
        3413,
        RADIAN_WGS84,
        (-500_000.0, -500_000.0, 500_000.0, 500_000.0),
        densify=21,
    )
    assert transformed_radians[0] == pytest.approx(-math.pi)
    assert transformed_radians[2] == pytest.approx(math.pi)
    assert transformed_radians[3] == pytest.approx(math.pi / 2.0)


@pytest.mark.parametrize(
    'options, engine', [({}, 'in_core'), ({'force_over': True}, 'proj')]
)
def test_bulk_bounds_stream_mixed_periodic_rows_through_one_operation(
    options: dict[str, Any], engine: str
) -> None:
    rows = [(170.0, -10.0, -170.0, 10.0), (-20.0, -5.0, 30.0, 5.0)]
    gm.crs_clear_cache()
    transformed = gm.crs_transform_bounds(4326, 3857, rows, densify=2, **options)
    assert transformed[0] == pytest.approx((
        -WEB_SEAM,
        _web_y(-10.0),
        WEB_SEAM,
        _web_y(10.0),
    ))
    assert transformed[1] == pytest.approx((
        WEB_RADIUS * math.radians(-20.0),
        _web_y(-5.0),
        WEB_RADIUS * math.radians(30.0),
        _web_y(5.0),
    ))
    assert gm.crs_cache_info()['last_transform_engine'] == engine


def test_bulk_bounds_reject_before_publishing_partial_rows() -> None:
    rows = [(0.0, 0.0, 1.0, 1.0), (0.0, -91.0, 1.0, -89.0)]
    gm.crs_clear_cache()
    with pytest.raises(ValueError, match='bounds must be finite'):
        gm.crs_transform_bounds(4326, 3857, rows)
    assert gm.crs_cache_info()['last_transform_engine'] is None


def test_full_and_unwrapped_geographic_sources_retain_full_topology() -> None:
    expected = (-WEB_SEAM, _web_y(-10.0), WEB_SEAM, _web_y(10.0))
    assert gm.crs_transform_bounds(
        4326, 3857, (0.0, -10.0, 360.0, 10.0)
    ) == pytest.approx(expected)
    assert gm.crs_transform_bounds(
        4326, 3857, (-180.0, -10.0, 180.0, 10.0)
    ) == pytest.approx(expected)
    assert gm.crs_transform_bounds(4326, 4326, (0.0, -90.0, 360.0, 90.0)) == (
        -180.0,
        -90.0,
        180.0,
        90.0,
    )


def test_identity_bounds_use_canonical_geographic_and_projected_frames() -> None:
    assert gm.crs_transform_bounds(4326, 4326, (0.0, -10.0, 360.0, 10.0)) == (
        -180.0,
        -10.0,
        180.0,
        10.0,
    )
    assert gm.crs_transform_bounds(4326, 4326, (-180.0, -10.0, 180.0, 10.0)) == (
        -180.0,
        -10.0,
        180.0,
        10.0,
    )
    assert gm.crs_transform_bounds(4326, 4326, (179.0, -1.0, -179.0, 1.0)) == (
        179.0,
        -1.0,
        -179.0,
        1.0,
    )
    assert gm.crs_transform_bounds(3857, 3857, (-10.0, -20.0, 30.0, 40.0)) == (
        -10.0,
        -20.0,
        30.0,
        40.0,
    )
    assert gm.crs_transform_bounds(4979, 4979, (0.0, -10.0, 3.0, 360.0, 10.0, 4.0)) == (
        -180.0,
        -10.0,
        3.0,
        180.0,
        10.0,
        4.0,
    )


def test_native_radian_and_grad_frames_are_not_double_converted() -> None:
    radians = gm.crs_transform_bounds(
        RADIAN_WGS84,
        4326,
        (-1.0, -0.5, 1.0, 0.5),
        densify=2,
        force_over=True,
    )
    assert radians == pytest.approx(
        tuple(math.degrees(value) for value in (-1.0, -0.5, 1.0, 0.5))
    )

    pyproj = pytest.importorskip('pyproj')
    bounds = (-1.0, 49.0, 1.0, 51.0)
    oracle = pyproj.Transformer.from_crs(4807, 4326, always_xy=True).transform_bounds(
        *bounds, densify_pts=2
    )
    assert gm.crs_transform_bounds(4807, 4326, bounds, densify=2) == pytest.approx(
        oracle
    )


def test_in_core_prime_meridian_covers_scalar_shape_coordinate_and_bounds_lanes() -> (
    None
):
    prime_meridian = 0.00000000005
    source = NEAR_GREENWICH_WGS84
    expected_scalar = gm.crs_transform(
        4326,
        3857,
        10.0 + prime_meridian,
        50.0,
        force_over=True,
    )

    gm.crs_clear_cache()
    scalar = gm.crs_transform(source, 3857, 10.0, 50.0)
    assert scalar == pytest.approx(expected_scalar, abs=1e-6)
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'

    coordinate = gm.crs_transform(source, 3857, [10.0, 11.0], [50.0, 51.0])
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'
    expected_coordinate = gm.crs_transform(
        4326,
        3857,
        [10.0 + prime_meridian, 11.0 + prime_meridian],
        [50.0, 51.0],
        force_over=True,
    )
    assert coordinate == pytest.approx(expected_coordinate, abs=1e-6)

    gm.crs_clear_cache()
    shape = gm.LineString([(10.0, 50.0), (11.0, 51.0)], crs=source).to_crs(3857)
    assert all(
        actual == pytest.approx(expected, abs=1e-6)
        for actual, expected in zip(
            shape.coords.to_nested(), expected_coordinate.tolist(), strict=True
        )
    )
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'

    bounds = gm.crs_transform_bounds(
        source,
        3857,
        (10.0, 50.0, 11.0, 51.0),
        densify=2,
    )
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'
    expected_bounds = gm.crs_transform_bounds(
        4326,
        3857,
        (10.0 + prime_meridian, 50.0, 11.0 + prime_meridian, 51.0),
        densify=2,
        force_over=True,
    )
    assert bounds == pytest.approx(expected_bounds, abs=1e-6)

    bounds_3d = gm.crs_transform_bounds(
        source,
        3857,
        (10.0, 50.0, 3.0, 11.0, 51.0, 4.0),
        densify=2,
    )
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'
    expected_bounds_3d = gm.crs_transform_bounds(
        4326,
        3857,
        (10.0 + prime_meridian, 50.0, 3.0, 11.0 + prime_meridian, 51.0, 4.0),
        densify=2,
        force_over=True,
    )
    assert bounds_3d == pytest.approx(expected_bounds_3d, abs=1e-6)

    gm.crs_clear_cache()
    reverse_scalar = gm.crs_transform(3857, source, *expected_scalar)
    assert reverse_scalar == pytest.approx((10.0, 50.0), abs=1e-12)
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'

    gm.crs_clear_cache()
    reverse_coordinate = gm.crs_transform(
        3857,
        source,
        expected_coordinate[:, 0],
        expected_coordinate[:, 1],
    )
    assert all(
        actual == pytest.approx(expected, abs=1e-12)
        for actual, expected in zip(
            reverse_coordinate.tolist(), ((10.0, 50.0), (11.0, 51.0)), strict=True
        )
    )
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'

    gm.crs_clear_cache()
    reverse_shape = gm.LineString(expected_coordinate.tolist(), crs=3857).to_crs(source)
    assert all(
        actual == pytest.approx(expected, abs=1e-12)
        for actual, expected in zip(
            reverse_shape.coords.to_nested(), ((10.0, 50.0), (11.0, 51.0)), strict=True
        )
    )
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'

    gm.crs_clear_cache()
    reverse_bounds = gm.crs_transform_bounds(
        3857,
        source,
        expected_bounds,
        densify=2,
    )
    assert reverse_bounds == pytest.approx((10.0, 50.0, 11.0, 51.0), abs=1e-12)
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'

    gm.crs_clear_cache()
    reverse_bounds_3d = gm.crs_transform_bounds(
        3857,
        source,
        expected_bounds_3d,
        densify=2,
    )
    assert reverse_bounds_3d == pytest.approx(
        (10.0, 50.0, 3.0, 11.0, 51.0, 4.0), abs=1e-12
    )
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'


def test_xyz_epoch_bounds_use_the_same_proj_lane_plan() -> None:
    bounds = (
        1_000_000.0,
        2_000_000.0,
        3_000_000.0,
        1_000_001.0,
        2_000_001.0,
        3_000_001.0,
    )
    gm.crs_clear_cache()
    transformed = gm.crs_transform_bounds(
        7789,
        7789,
        bounds,
        densify=2,
        source_epoch=2010.0,
        target_epoch=2020.0,
    )
    corners = [
        gm.crs_transform(
            7789,
            7789,
            x,
            y,
            z,
            source_epoch=2010.0,
            target_epoch=2020.0,
        )
        for x in (bounds[0], bounds[3])
        for y in (bounds[1], bounds[4])
        for z in (bounds[2], bounds[5])
    ]
    for axis in range(3):
        values = [corner[axis] for corner in corners]
        assert transformed[axis] <= min(values)
        assert transformed[axis + 3] >= max(values)
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'


def test_bulk_xyz_bounds_broadcast_epoch_per_streamed_row() -> None:
    rows = [
        (1_000_000.0, 2_000_000.0, 3_000_000.0, 1_000_001.0, 2_000_001.0, 3_000_001.0),
        (2_000_000.0, 1_000_000.0, 4_000_000.0, 2_000_001.0, 1_000_001.0, 4_000_001.0),
    ]
    options = {'source_epoch': 2010.0, 'target_epoch': 2020.0}
    bulk = gm.crs_transform_bounds(7789, 7789, rows, densify=2, **options)
    for actual, row in zip(bulk, rows, strict=True):
        assert actual == pytest.approx(
            gm.crs_transform_bounds(7789, 7789, row, densify=2, **options)
        )
