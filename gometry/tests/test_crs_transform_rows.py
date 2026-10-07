"""GeometryArray CRS failures retain the first displayed logical row."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import gometry as gm
import pytest

if TYPE_CHECKING:
    from collections.abc import Callable


def _assert_row_error(operation: Callable[[], object], row: int) -> BaseException:
    with pytest.raises(gm.TransformError) as excinfo:
        operation()
    assert excinfo.value.__notes__ == [f'while processing array element {row}']
    return excinfo.value


def _assert_identity_row_error(
    operation: Callable[[], object], row: int
) -> BaseException:
    with pytest.raises(
        gm.InvalidGeometryError, match='invalid longitude/latitude'
    ) as excinfo:
        operation()
    assert excinfo.value.__notes__ == [f'while processing array element {row}']
    return excinfo.value


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_mixed_to_crs_arbitrates_xy_and_xyz_failures_by_logical_row(
    force_over: bool,
) -> None:
    values = gm.GeometryArray([
        gm.Point(-90.0, 0.0, z=1.0, crs=4326),
        gm.Point(-90.0, 0.0, crs=4326),
    ])

    error = _assert_row_error(
        lambda: values.to_crs(32660, force_over=force_over),
        0,
    )
    if force_over:
        assert error.source == 'EPSG:4326'
        assert error.target == 'EPSG:32660'


@pytest.mark.parametrize(
    ('selected', 'row'),
    [
        ('slice', 1),
        ('fancy', 0),
        ('reverse', 0),
        ('duplicate', 0),
    ],
)
@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_packed_point_to_crs_reports_displayed_selection_row(
    selected: str,
    row: int,
    force_over: bool,
) -> None:
    source = gm.points(
        [0.0, 1.0, -90.0, -90.0],
        [0.0, 0.0, 0.0, 0.0],
        crs=4326,
    )
    values = {
        'slice': source[1:4],
        'fancy': source[[3, 2, 0]],
        'reverse': source[::-1],
        'duplicate': source[[2, 2, 0]],
    }[selected]

    _assert_row_error(lambda: values.to_crs(32660, force_over=force_over), row)


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
@pytest.mark.parametrize('kind', ['lines', 'polygons'])
def test_packed_line_and_polygon_to_crs_reports_displayed_row(
    force_over: bool,
    kind: str,
) -> None:
    if kind == 'lines':
        source = gm.GeometryArray([
            gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
            gm.LineString([(0.0, 0.0), (-90.0, 0.0)], crs=4326),
            gm.LineString([(2.0, 2.0), (3.0, 3.0)], crs=4326),
        ])
    else:
        source = gm.GeometryArray([
            gm.box(0.0, 0.0, 1.0, 1.0, crs=4326),
            gm.Polygon(
                [(-90.0, 0.0), (-89.0, 0.0), (-89.0, 1.0), (-90.0, 0.0)],
                crs=4326,
            ),
            gm.box(2.0, 2.0, 3.0, 3.0, crs=4326),
        ])

    selected = source[[2, 1, 0]]
    _assert_row_error(lambda: selected.to_crs(32660, force_over=force_over), 1)


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_compact_nullable_packed_point_to_crs_reports_gathered_logical_row(
    force_over: bool,
) -> None:
    source = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        None,
        gm.Point(-90.0, 0.0, crs=4326),
    ])
    selected = source[[1, 2, 0]]

    # The point extension and present-only coordinate child are public
    # evidence for the packed nullable path; the mask stays logical.
    assert selected.to_arrow().type.extension_name == 'geoarrow.point'
    assert len(selected.coords.x) == 2

    _assert_row_error(lambda: selected.to_crs(32660, force_over=force_over), 1)


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_masked_mixed_to_crs_reports_gathered_logical_row(force_over: bool) -> None:
    """Mixed WKB storage keeps row-aligned missingness outside packed lanes."""
    source = gm.GeometryArray([
        None,
        gm.Point(-90.0, 0.0, crs=4326),
        gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
    ])
    selected = source[[2, 0, 1]]

    # A generic WKB extension is the public consequence of heterogeneous
    # storage; the missing row is still before the bad present row after gather.
    assert selected.to_arrow().type.extension_name == 'geoarrow.wkb'
    assert selected.geometry_type == ['LineString', None, 'Point']

    _assert_row_error(lambda: selected.to_crs(32660, force_over=force_over), 2)


@pytest.mark.parametrize('force_over', [False, True], ids=['in-core', 'proj'])
def test_multiple_bad_rows_report_first_logical_row(
    force_over: bool,
) -> None:
    values = gm.points(
        [-90.0, -90.0, 0.0],
        [0.0, 0.0, 0.0],
        crs=4326,
    )

    _assert_row_error(lambda: values.to_crs(32660, force_over=force_over), 0)


def test_array_setup_error_and_scalar_transform_error_have_no_row_note() -> None:
    values = gm.points([0.0], [0.0], crs=4326)
    with pytest.raises(gm.CRSError) as setup:
        values.to_crs('EPSG:999999')
    assert not hasattr(setup.value, '__notes__')

    with pytest.raises(gm.TransformError) as scalar:
        gm.Point(-90.0, 0.0, crs=4326).to_crs(32660)
    assert not hasattr(scalar.value, '__notes__')


def test_scalar_identity_to_crs_admits_source_and_retags_target_frame() -> None:
    with pytest.raises(
        gm.InvalidGeometryError, match=r'invalid longitude/latitude \(0, 91\)'
    ):
        gm.Point(0.0, 91.0, crs=4326).to_crs('OGC:CRS84')
    source = gm.Point(12.0, 34.0, crs=4326)
    transformed = source.to_crs('OGC:CRS84')
    assert str(transformed.crs) == 'OGC:CRS84'
    assert (transformed.x, transformed.y) == (source.x, source.y)


def test_packed_identity_to_crs_admits_selected_rows() -> None:
    packed = gm.points([0.0, 1.0], [0.0, 91.0], crs=4326)
    selected_valid = packed[[0]].to_crs('OGC:CRS84')
    assert str(selected_valid.crs) == 'OGC:CRS84'
    assert selected_valid.geometry_type == ['Point']
    _assert_identity_row_error(lambda: packed.to_crs('OGC:CRS84'), 1)
    _assert_identity_row_error(lambda: packed[[1, 0]].to_crs('OGC:CRS84'), 0)


@pytest.mark.parametrize('kind', ['lines', 'polygons'])
def test_packed_identity_to_crs_validates_selected_csr_rows(kind: str) -> None:
    if kind == 'lines':
        source = gm.GeometryArray([
            gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
            gm.LineString([(0.0, 0.0), (1.0, 91.0)], crs=4326),
            gm.LineString([(2.0, 2.0), (3.0, 3.0)], crs=4326),
        ])
        assert source.to_arrow().type.extension_name == 'geoarrow.linestring'
    else:
        source = gm.GeometryArray([
            gm.Polygon([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)], crs=4326),
            gm.Polygon([(0.0, 0.0), (1.0, 0.0), (0.0, 91.0), (0.0, 0.0)], crs=4326),
            gm.Polygon([(2.0, 2.0), (3.0, 2.0), (2.0, 3.0), (2.0, 2.0)], crs=4326),
        ])
        assert source.to_arrow().type.extension_name == 'geoarrow.polygon'
    _assert_identity_row_error(lambda: source[[2, 1, 0]].to_crs('OGC:CRS84'), 1)


def test_mixed_identity_to_crs_admits_present_rows_and_empty_payloads() -> None:
    mixed = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
        gm.Point(0.0, 91.0, crs=4326),
    ])
    _assert_identity_row_error(lambda: mixed.to_crs('OGC:CRS84'), 1)

    nullable_mixed = gm.GeometryArray([
        None,
        gm.Point(0.0, 91.0, crs=4326),
        gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
    ])
    _assert_identity_row_error(lambda: nullable_mixed.to_crs('OGC:CRS84'), 1)
    selected = nullable_mixed[[2, 0, 1]]
    _assert_identity_row_error(lambda: selected.to_crs('OGC:CRS84'), 2)
    assert selected[[0]].to_crs('OGC:CRS84').geometry_type == ['LineString']

    all_missing = gm.GeometryArray([None, None], crs=4326).to_crs('OGC:CRS84')
    assert str(all_missing.crs) == 'OGC:CRS84'
    assert list(all_missing) == [None, None]
    empty = gm.GeometryArray([], crs=4326).to_crs('OGC:CRS84')
    assert str(empty.crs) == 'OGC:CRS84'
    assert len(empty) == 0
    empty_geometry = gm.GeometryArray(
        [gm.LineString([], crs=4326)],
    ).to_crs('OGC:CRS84')
    assert empty_geometry.geometry_type == ['LineString']
    assert empty_geometry.is_empty.tolist() == [True]


def test_identity_to_crs_preserves_coordinate_bytes_and_epoch() -> None:
    source = gm.from_wkt(
        'LINESTRING ZM (180 -0 3 4, 540 0 5 6)',
        crs=4326,
    )
    transformed = source.to_crs('OGC:CRS84')
    assert str(transformed.crs) == 'OGC:CRS84'
    assert transformed.to_wkb() == source.to_wkb()
    assert transformed.to_wkt() == source.to_wkt()

    dynamic = (
        'GEOGCRS["xyzzy-dyn-geo",'
        'DYNAMIC[FRAMEEPOCH[2010.0]],'
        'DATUM["Xyzzy Dynamic Geodetic",'
        'ELLIPSOID["GRS 1980",6378137,298.257222101,LENGTHUNIT["metre",1]]],'
        'PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],'
        'CS[ellipsoidal,2],'
        'AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],'
        'AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]]]'
    )
    epoch_source = gm.Point(180.0, -0.0, crs=dynamic).set_epoch(2010.0)
    gm.crs_clear_cache()
    equal_epoch = epoch_source.to_crs(epoch_source.crs)
    assert equal_epoch.epoch == 2010.0
    assert equal_epoch.to_wkb(drop_epoch=True) == epoch_source.to_wkb(drop_epoch=True)
    assert gm.crs_cache_info()['last_transform_engine'] is None

    gm.crs_clear_cache()
    unequal_epoch = epoch_source.to_crs(dynamic, epoch=2020.0)
    assert unequal_epoch.epoch == 2020.0
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'


def test_numeric_geographic_transform_publishes_but_raw_paths_do_not() -> None:
    radius = 6_378_137.0
    east_half_period = radius * math.pi
    transformed = gm.crs_transform(
        3857,
        4326,
        east_half_period,
        -0.0,
        authority='EPSG',
    )
    assert transformed == (-180.0, 0.0)
    assert math.copysign(1.0, transformed[1]) < 0.0
    geometry = gm.Point(east_half_period, -0.0, crs=3857).to_crs(
        4326,
        authority='EPSG',
    )
    assert (geometry.x, geometry.y) == transformed
    assert math.copysign(1.0, geometry.y) < 0.0

    radians = (
        'GEOGCRS["WGS 84 radians",DATUM["World Geodetic System 1984",'
        'ELLIPSOID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.017453292519943295]],'
        'CS[ellipsoidal,2],AXIS["Longitude",east,ORDER[1]],'
        'AXIS["Latitude",north,ORDER[2]],ANGLEUNIT["radian",1]]'
    )
    native = gm.crs_transform(4326, radians, 180.0, -0.0, authority='EPSG')
    assert native == (-math.pi, 0.0)
    assert math.copysign(1.0, native[1]) < 0.0
    native_geometry = gm.Point(180.0, -0.0, crs=4326).to_crs(
        radians,
        authority='EPSG',
    )
    assert (native_geometry.x, native_geometry.y) == native
    zero = gm.crs_transform(4326, radians, -0.0, -0.0, authority='EPSG')
    assert zero[0] == 0.0
    assert math.copysign(1.0, zero[0]) > 0.0
    assert math.copysign(1.0, zero[1]) < 0.0

    raw_over = gm.crs_transform(
        3857,
        4326,
        east_half_period,
        0.0,
        force_over=True,
    )
    assert raw_over[0] == 180.0
    pipeline = (
        '+proj=pipeline +step +inv +proj=webmerc +a=6378137 +b=6378137 +over '
        '+step +proj=unitconvert +xy_in=rad +xy_out=deg'
    )
    assert gm.crs_apply(pipeline, radius * 2.0 * math.pi, 0.0) == (360.0, 0.0)


@pytest.mark.parametrize(
    ('longitude_factor', 'latitude_factor', 'longitude_half'),
    [
        pytest.param(
            math.tau / 400.0,
            math.tau / 400.0,
            200.0,
            id='uniform-grad',
        ),
        pytest.param(
            math.nextafter(math.tau / 400.0, math.inf),
            math.nextafter(math.tau / 400.0, math.inf),
            200.0,
            id='recognized-factor-above',
        ),
        pytest.param(
            math.tau / math.nextafter(400.0, math.inf),
            math.tau / math.nextafter(400.0, math.inf),
            200.0,
            id='recognized-period-below',
        ),
        pytest.param(
            1.0,
            math.nextafter(math.tau / 400.0, math.inf),
            math.pi,
            id='mixed-radian-grad',
        ),
    ],
)
def test_proj_default_units_preserve_grad_poles_with_mixed_axis_factors(
    longitude_factor: float,
    latitude_factor: float,
    longitude_half: float,
) -> None:
    gm.crs_clear_cache()
    source = (
        'GEOGCRS["conventional angular units",'
        'DATUM["World Geodetic System 1984",'
        'ELLIPSOID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.017453292519943295]],'
        'CS[ellipsoidal,2],'
        'AXIS["Latitude",north,ORDER[1],ANGLEUNIT["probe grad",'
        f'{latitude_factor!r}]],'
        'AXIS["Longitude",east,ORDER[2],ANGLEUNIT["probe longitude",'
        f'{longitude_factor!r}]]]'
    )
    info = gm.CRS(source)
    assert info.axes[0]['unit_conversion_factor'] == latitude_factor
    assert info.axes[1]['unit_conversion_factor'] == longitude_factor
    assert gm.crs_cache_info()['last_transform_engine'] is None
    listed = next(
        candidate
        for candidate in info.operations(4326)
        if any(
            step['method'] is not None and step['method']['code'] == '9619'
            for step in candidate['steps']
        )
    )
    assert listed['definition'] is None

    operation = info.operation(4326, only_best=True)
    assert operation['definition'] is None
    assert operation['source'] == source
    assert operation['target']
    assert any(
        step['definition'] is None
        and step['method'] is not None
        and step['method']['code'] == '9619'
        and len(step['parameters']) == 2
        for step in operation['steps']
    )

    north = gm.crs_transform(source, 4326, 0.0, 100.0, only_best=True)
    south = gm.crs_transform(source, 4326, 0.0, -100.0, only_best=True)
    east_seam = gm.crs_transform(source, 4326, longitude_half, 0.0, only_best=True)
    neighbor = gm.crs_transform(
        source, 4326, 0.0, math.nextafter(100.0, 0.0), only_best=True
    )
    assert north == (0.0, 90.0)
    assert south == (0.0, -90.0)
    assert east_seam == (-180.0, 0.0)
    assert neighbor[1] == 89.99999999999999
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'

    native_north = gm.crs_transform(4326, source, 0.0, 90.0, only_best=True)
    native_south = gm.crs_transform(4326, source, 0.0, -90.0, only_best=True)
    native_seam = gm.crs_transform(4326, source, 180.0, 0.0, only_best=True)
    assert native_north == (0.0, 100.0)
    assert native_south == (0.0, -100.0)
    assert native_seam == (-longitude_half, 0.0)
    assert gm.crs_cache_info()['last_transform_engine'] == 'proj'


def test_proj_noop_phase_uses_coalesced_original_prime_meridians() -> None:
    gm.crs_clear_cache()
    standard_grad = math.tau / 400.0
    next_pm = math.nextafter(2.0, 0.0)
    next_factor = math.nextafter(standard_grad, math.inf)

    def geographic(name: str, pm: float, factor: float) -> str:
        return (
            f'GEOGCRS["{name}",DATUM["World Geodetic System 1984",'
            'ELLIPSOID["WGS 84",6378137,298.257223563]],'
            f'PRIMEM["Probe PM",{pm!r},ANGLEUNIT["probe grad",{factor!r}]],'
            'CS[ellipsoidal,2],AXIS["Longitude",east,ORDER[1]],'
            'AXIS["Latitude",north,ORDER[2]],'
            'ANGLEUNIT["degree",0.017453292519943295]]'
        )

    source = geographic('Selected PM source', 2.0, standard_grad)
    target = geographic('Selected PM target', next_pm, next_factor)
    source_pm = 2.0 * standard_grad
    target_pm = next_pm * next_factor
    assert source_pm == target_pm

    listed = gm.CRS(source).operations(target)
    assert listed
    assert all(item['definition'] is None for item in listed)
    assert all(item['source'] == source and item['target'] == target for item in listed)

    operation = gm.CRS(source).operation(target, at=(0.0, 0.0), only_best=True)
    assert operation['definition'] is None
    assert operation['source'] == source
    assert operation['target'] == target
    eastward = gm.crs_transform(source, target, 0.0, 0.0, only_best=True)
    westward = gm.crs_transform(target, source, 0.0, 0.0, only_best=True)
    expected_shift = float.fromhex('0x1.ccccccccccccdp-53')
    assert eastward == (expected_shift, 0.0)
    assert westward == (-expected_shift, 0.0)

    # Explicit definitions and force-over use their original literal operation.
    literal_noop = 'proj=noop ellps=GRS80'
    assert gm.crs_apply(literal_noop, 0.0, 0.0) == (0.0, 0.0)
    assert gm.crs_transform(source, target, 0.0, 0.0, force_over=True) == (0.0, 0.0)
    assert gm.crs_transform(source, target, 0.0, 0.0, only_best=True) == eastward


def test_successful_homogeneous_packed_transform_stays_one_batch_invocation() -> None:
    gm.crs_clear_cache()
    values = gm.points([0.0, 1.0], [0.0, 1.0], crs=4326)

    transformed = values.to_crs(3857)

    assert transformed.geometry_type == ['Point', 'Point']
    assert gm.crs_cache_info()['transform_invocations'] == 1


def test_successful_mixed_xyz_and_xy_transform_preserves_known_values_and_engine() -> (
    None
):
    gm.crs_clear_cache()
    values = gm.GeometryArray([
        gm.Point(1.0, 2.0, z=7.0, crs=4326),
        gm.Point(-3.0, 4.0, crs=4326),
    ])

    transformed = values.to_crs(3857)

    assert transformed[0].x == pytest.approx(111319.49079327357)
    assert transformed[0].y == pytest.approx(222684.20850554405)
    assert transformed[0].z == pytest.approx(7.0)
    assert transformed[1].x == pytest.approx(-333958.4723798207)
    assert transformed[1].y == pytest.approx(445640.10965602624)
    assert gm.crs_cache_info()['last_transform_engine'] == 'in_core'
