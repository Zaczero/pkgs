"""Exact public I/O round trips for geographic coordinates and split drawings."""

from __future__ import annotations

import json
import struct
from typing import Any

import gometry as gm
import pytest

pytest.importorskip('pyarrow')

_CRS = 'EPSG:4326'
_CRS84 = 'OGC:CRS84'
_EPOCH = 2024.25
_AXIS_TAGS = {'XY': '', 'XYZ': ' Z', 'XYM': ' M', 'XYZM': ' ZM'}
_XY = (
    [(-0.0, 11.125), (12.25, -0.0), (-33.5, 44.75)],
    [(55.125, -12.5), (-66.75, 31.25), (77.5, -8.125)],
)
_Z = ([-0.0, 101.125, -202.5], [303.75, -404.125, 505.5])
_M = ([606.25, -0.0, 707.875], [-808.5, 909.125, -1001.75])


def _line(axes: str, variant: int = 0) -> gm.LineString:
    ordinates: dict[str, Any] = {}
    if 'Z' in axes:
        ordinates['z'] = _Z[variant]
    if 'M' in axes:
        ordinates['m'] = _M[variant]
    return gm.LineString(
        _XY[variant],
        **ordinates,
        crs=_CRS,
        epoch=_EPOCH,
    )


def _float_bits(value: float) -> int:
    return struct.unpack('<Q', struct.pack('<d', value))[0]


def _coordinate_bits(geometry: gm.Geometry) -> tuple[tuple[int, ...], ...]:
    return tuple(
        tuple(_float_bits(value) for value in coordinate)
        for coordinate in geometry.coords.select(geometry.coordinate_axes)
    )


def _assert_negative_zero_sentinels(
    coordinates: tuple[tuple[int, ...], ...],
    axes: str,
) -> None:
    negative_zero = _float_bits(-0.0)
    assert coordinates[0][0] == negative_zero
    assert coordinates[1][1] == negative_zero
    if 'Z' in axes:
        assert coordinates[0][axes.index('Z')] == negative_zero
    if 'M' in axes:
        assert coordinates[1][axes.index('M')] == negative_zero


def _assert_line(
    actual: gm.Geometry,
    expected_bits: tuple[tuple[int, ...], ...],
    axes: str,
) -> None:
    assert actual.coordinate_axes == axes
    assert _coordinate_bits(actual) == expected_bits


def _assert_frame(value: gm.GeometryArray) -> None:
    assert value.crs == gm.CRS(_CRS)
    assert value.epoch == _EPOCH
    assert value[0].crs == gm.CRS(_CRS)
    assert value[0].epoch == _EPOCH


@pytest.mark.parametrize('axes', ['XY', 'XYZ', 'XYM', 'XYZM'])
def test_geographic_wkb_and_arrow_preserve_scalar_and_array_bits(
    axes: str,
) -> None:
    source = _line(axes)
    expected_bits = _coordinate_bits(source)
    _assert_negative_zero_sentinels(expected_bits, axes)
    empty = gm.from_wkt(
        f'LINESTRING{_AXIS_TAGS[axes]} EMPTY',
        crs=_CRS,
        epoch=_EPOCH,
    )
    rows = gm.GeometryArray([source, None, empty], crs=_CRS, epoch=_EPOCH)

    for include_srid in (False, True):
        read_options: dict[str, Any] = {'epoch': _EPOCH}
        if not include_srid:
            read_options['crs'] = _CRS

        scalar = gm.from_wkb(
            source.to_wkb(include_srid=include_srid, drop_epoch=True),
            **read_options,
        )
        _assert_line(scalar, expected_bits, axes)
        assert scalar.crs == gm.CRS(_CRS)
        assert scalar.epoch == _EPOCH

        encoded = rows.to_wkb(include_srid=include_srid, drop_epoch=True)
        restored = gm.from_wkb(encoded, **read_options)
        assert restored.is_missing.tolist() == [False, True, False]
        _assert_frame(restored)
        _assert_line(restored[0], expected_bits, axes)
        assert restored[1] is None
        assert restored[2].is_empty
        assert restored[2].coordinate_axes == axes
        assert (
            restored[0].to_wkb(include_srid=include_srid, drop_epoch=True) == encoded[0]
        )

    for encoding in ('auto', 'wkb'):
        arrow = rows.to_arrow(encoding=encoding)
        scalar_arrow = source.to_arrow(encoding=encoding)
        scalar_restored = gm.from_arrow(scalar_arrow)
        _assert_frame(scalar_restored)
        _assert_line(scalar_restored[0], expected_bits, axes)

        restored = gm.from_arrow(arrow)
        assert restored.is_missing.tolist() == [False, True, False]
        _assert_frame(restored)
        _assert_line(restored[0], expected_bits, axes)
        assert restored[1] is None
        assert restored[2].is_empty
        assert restored[2].coordinate_axes == axes

        expected_encoding = (
            'geoarrow.linestring' if encoding == 'auto' else 'geoarrow.wkb'
        )
        assert arrow.type.extension_name == expected_encoding
        assert scalar_arrow.type.extension_name == expected_encoding
        if encoding == 'auto':
            assert arrow.storage.offsets.to_pylist() == [0, 3, 3, 3]
        assert arrow.storage.is_null().to_pylist() == [False, True, False]

        padded = gm.GeometryArray(
            [_line(axes, 1), source, None, empty, _line(axes, 1)],
            crs=_CRS,
            epoch=_EPOCH,
        ).to_arrow(encoding=encoding)
        sliced = padded.slice(1, 3)
        assert sliced.offset == 1
        sliced_restored = gm.from_arrow(sliced)
        assert sliced_restored.is_missing.tolist() == [False, True, False]
        _assert_frame(sliced_restored)
        _assert_line(sliced_restored[0], expected_bits, axes)
        assert sliced_restored[2].is_empty
        assert sliced_restored[2].coordinate_axes == axes

    native_line = _line(axes, 1)
    native_line_bits = _coordinate_bits(native_line)
    native_rows = gm.GeometryArray([source, None, native_line], crs=_CRS, epoch=_EPOCH)
    native_arrow = native_rows.to_arrow()
    assert native_arrow.type.extension_name == 'geoarrow.linestring'
    assert native_arrow.storage.offsets.to_pylist() == [0, 3, 3, 6]
    assert native_arrow.storage.is_null().to_pylist() == [False, True, False]
    native_restored = gm.from_arrow(native_arrow)
    assert native_restored.is_missing.tolist() == [False, True, False]
    _assert_frame(native_restored)
    _assert_line(native_restored[0], expected_bits, axes)
    _assert_line(native_restored[2], native_line_bits, axes)
    assert _coordinate_bits(native_line) == native_line_bits

    native_sliced = (
        gm
        .GeometryArray(
            [_line(axes, 1), source, _line(axes, 1)],
            crs=_CRS,
            epoch=_EPOCH,
        )
        .to_arrow()
        .slice(1, 1)
    )
    assert native_sliced.offset == 1
    native_sliced_restored = gm.from_arrow(native_sliced)
    _assert_frame(native_sliced_restored)
    _assert_line(native_sliced_restored[0], expected_bits, axes)
    assert _coordinate_bits(source) == expected_bits
    assert _coordinate_bits(rows[0]) == expected_bits


def _assert_multiline(
    actual: gm.Geometry,
    expected_bits: tuple[tuple[tuple[int, ...], ...], ...],
    axes: str,
) -> None:
    assert actual.geometry_type == 'MultiLineString'
    assert actual.coordinate_axes == axes
    parts = list(actual.parts)
    assert len(parts) == len(expected_bits)
    for part, line_bits in zip(parts, expected_bits, strict=True):
        _assert_line(part, line_bits, axes)


@pytest.mark.parametrize('axes', ['XY', 'XYZ', 'XYM', 'XYZM'])
def test_geographic_grouped_arrow_offsets_and_values_are_preserved(
    axes: str,
) -> None:
    lines = (_line(axes, 0), _line(axes, 1))
    expected_bits = tuple(_coordinate_bits(line) for line in lines)
    _assert_negative_zero_sentinels(expected_bits[0], axes)
    grouped = gm.MultiLineString(lines, crs=_CRS, epoch=_EPOCH)
    empty = gm.from_wkt(
        f'MULTILINESTRING{_AXIS_TAGS[axes]} EMPTY',
        crs=_CRS,
        epoch=_EPOCH,
    )
    values = gm.GeometryArray([grouped, None, empty], crs=_CRS, epoch=_EPOCH)

    parts = values.parts
    assert parts.offsets.tolist() == [0, 2, 2, 2]
    assert parts[1] is None
    empty_group = parts[2]
    assert empty_group is not None
    assert len(empty_group) == 0
    grouped_values = parts.values
    assert len(grouped_values) == 2
    for actual, line_bits in zip(grouped_values, expected_bits, strict=True):
        _assert_line(actual, line_bits, axes)

    grouped_wkb = gm.from_wkb(
        grouped_values.to_wkb(include_srid=True, drop_epoch=True),
        epoch=_EPOCH,
    )
    assert grouped_wkb.crs == gm.CRS(_CRS)
    assert grouped_wkb.epoch == _EPOCH
    for actual, line_bits in zip(grouped_wkb, expected_bits, strict=True):
        _assert_line(actual, line_bits, axes)

    grouped_arrow = gm.from_arrow(grouped_values.to_arrow())
    _assert_frame(grouped_arrow)
    for actual, line_bits in zip(grouped_arrow, expected_bits, strict=True):
        _assert_line(actual, line_bits, axes)

    for encoding in ('auto', 'wkb'):
        arrow = values.to_arrow(encoding=encoding)
        assert arrow.type.extension_name == (
            'geoarrow.multilinestring' if encoding == 'auto' else 'geoarrow.wkb'
        )
        if encoding == 'auto':
            assert arrow.storage.offsets.to_pylist() == [0, 2, 2, 2]
            assert arrow.storage.values.offsets.to_pylist() == [0, 3, 6]
        assert arrow.storage.is_null().to_pylist() == [False, True, False]

        restored = gm.from_arrow(arrow)
        assert restored.is_missing.tolist() == [False, True, False]
        _assert_frame(restored)
        _assert_multiline(restored[0], expected_bits, axes)
        assert restored[1] is None
        assert restored[2].is_empty
        assert restored[2].coordinate_axes == axes

        padded = gm.GeometryArray(
            [
                gm.MultiLineString([_line(axes, 1)], crs=_CRS, epoch=_EPOCH),
                grouped,
                None,
                empty,
                gm.MultiLineString([_line(axes, 1)], crs=_CRS, epoch=_EPOCH),
            ],
            crs=_CRS,
            epoch=_EPOCH,
        ).to_arrow(encoding=encoding)
        sliced = padded.slice(1, 3)
        assert sliced.offset == 1
        sliced_restored = gm.from_arrow(sliced)
        assert sliced_restored.is_missing.tolist() == [False, True, False]
        _assert_frame(sliced_restored)
        _assert_multiline(sliced_restored[0], expected_bits, axes)
        assert sliced_restored[2].is_empty
        assert sliced_restored[2].coordinate_axes == axes

    other = gm.MultiLineString([lines[1]], crs=_CRS, epoch=_EPOCH)
    native_values = gm.GeometryArray([grouped, other], crs=_CRS, epoch=_EPOCH)
    native_arrow = native_values.to_arrow()
    assert native_arrow.type.extension_name == 'geoarrow.multilinestring'
    assert native_arrow.storage.offsets.to_pylist() == [0, 2, 3]
    assert native_arrow.storage.values.offsets.to_pylist() == [0, 3, 6, 9]
    native_restored = gm.from_arrow(native_arrow)
    _assert_frame(native_restored)
    _assert_multiline(native_restored[0], expected_bits, axes)
    _assert_multiline(native_restored[1], (expected_bits[1],), axes)

    native_sliced = (
        gm
        .GeometryArray(
            [other, grouped, other],
            crs=_CRS,
            epoch=_EPOCH,
        )
        .to_arrow()
        .slice(1, 1)
    )
    assert native_sliced.offset == 1
    native_sliced_restored = gm.from_arrow(native_sliced)
    _assert_frame(native_sliced_restored)
    _assert_multiline(native_sliced_restored[0], expected_bits, axes)
    assert tuple(_coordinate_bits(line) for line in lines) == expected_bits
    assert tuple(_coordinate_bits(line) for line in grouped.parts) == expected_bits
    assert tuple(_coordinate_bits(line) for line in values[0].parts) == expected_bits


def _multiline_coordinate_bits(
    geometry: gm.Geometry,
) -> tuple[tuple[tuple[int, ...], ...], ...]:
    return tuple(_coordinate_bits(part) for part in geometry.parts)


def test_geojson_split_writers_share_the_metric_aware_drawing() -> None:
    source = gm.LineString(
        [(144.0, -40.0), (-144.0, 60.0)],
        z=[20.0, 120.0],
        crs=_CRS84,
    )
    expected = source.split_antimeridian()
    expected_parts = _multiline_coordinate_bits(expected)
    seam_z = [
        coordinate[2]
        for part in expected.parts
        for coordinate in part.coords
        if abs(coordinate[0]) == 180.0
    ]
    assert len(seam_z) == 2
    assert abs(seam_z[0] - 70.0) > 1.0e-3

    rows = gm.GeometryArray([source, None], crs=_CRS84)
    array_text = rows.to_geojson()
    feature = gm.to_feature(source)
    feature_collection = gm.to_feature_collection([source])
    array_feature_collection = gm.to_feature_collection(rows)
    array_interface = rows.__geo_interface__
    source_features = gm.from_geojson(array_feature_collection, as_features=True)
    features_feature_collection = gm.to_feature_collection(source_features)
    scalar_json = json.loads(source.to_geojson())
    assert array_text[1] is None
    assert array_interface['features'][1]['geometry'] is None
    assert array_feature_collection['features'][1]['geometry'] is None
    assert features_feature_collection == array_feature_collection

    serialized = [
        (
            'scalar string',
            gm.from_geojson(source.to_geojson()),
            scalar_json,
        ),
        (
            'array string',
            gm.from_geojson(array_text)[0],
            json.loads(array_text[0]),
        ),
        (
            'scalar interface',
            gm.from_geojson(source.__geo_interface__),
            source.__geo_interface__,
        ),
        (
            'array interface',
            gm.from_geojson(array_interface['features'][0]['geometry']),
            array_interface['features'][0]['geometry'],
        ),
        (
            'Feature',
            gm.from_geojson(feature, as_features=True).geometries[0],
            feature['geometry'],
        ),
        (
            'FeatureCollection',
            gm.from_geojson(feature_collection, as_features=True).geometries[0],
            feature_collection['features'][0]['geometry'],
        ),
        (
            'array FeatureCollection',
            gm.from_geojson(array_feature_collection, as_features=True).geometries[0],
            array_feature_collection['features'][0]['geometry'],
        ),
        (
            'Features FeatureCollection',
            gm.from_geojson(features_feature_collection, as_features=True).geometries[
                0
            ],
            features_feature_collection['features'][0]['geometry'],
        ),
    ]
    for writer, geometry, mapping in serialized:
        assert geometry.crs == gm.CRS(_CRS84), writer
        assert geometry.coordinate_axes == 'XYZ', writer
        assert _multiline_coordinate_bits(geometry) == expected_parts, writer
        assert mapping == scalar_json, writer


def test_geojson_feature_collection_keeps_crs84_and_rejects_m() -> None:
    measured = gm.LineString(
        [(1.0, 2.0), (3.0, 4.0)],
        m=[5.0, 6.0],
        crs=_CRS84,
    )
    with pytest.raises(gm.InvalidGeometryError, match='GeoJSON cannot represent M'):
        gm.to_feature(measured)
    for values in ([measured], gm.GeometryArray([measured], crs=_CRS84)):
        with pytest.raises(
            gm.InvalidGeometryError,
            match='GeoJSON cannot represent M',
        ):
            gm.to_feature_collection(values)

    projected = gm.Point(1.0, 2.0, crs=3857)
    with pytest.raises(gm.CRSError):
        gm.to_feature(projected)
    for values in ([projected], gm.GeometryArray([projected], crs=3857)):
        with pytest.raises(gm.CRSError):
            gm.to_feature_collection(values)
