"""Arrow/GeoArrow interop — packed columnar storage, sliced buffers,
extension metadata, IPC roundtrips, and decode strictness.
"""

from __future__ import annotations

import ctypes
import io
import json as _json
import math
import pickle
import struct
import subprocess
import sys
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest
from gometry._lib import CRS as _CRS

from tests._support import (
    _capsule_only_from_gometry,
    _capsule_only_from_pyarrow,
    _StreamOnly,
    arrow_geometries,
    canon,
    polygon_storage_twins,
    with_validity,
)

pa = pytest.importorskip('pyarrow', reason='pyarrow is an optional Arrow dependency')

_WGS84_PROJJSON_METADATA = _json.dumps(
    {
        'crs': _CRS(4326).to_projjson(as_dict=True),
        'crs_type': 'projjson',
        'gometry:crs_definition': 'EPSG:4326',
    },
    separators=(',', ':'),
).encode()


def _assert_same_crs(left: gm.CRS | None, right: gm.CRS | None) -> None:
    if left is None or right is None:
        assert left is right
    else:
        assert left.same_as(right, mode='exact')


@pytest.mark.parametrize(
    ('wkt', 'empty', 'encoding'),
    [
        ('POINT (7 13)', 'POINT EMPTY', 'point'),
        ('MULTIPOINT ((7 13))', 'MULTIPOINT EMPTY', 'multipoint'),
        ('LINESTRING (7 13, 11 17)', 'LINESTRING EMPTY', 'linestring'),
        ('MULTILINESTRING ((7 13, 11 17))', 'MULTILINESTRING EMPTY', 'multilinestring'),
        ('POLYGON ((0 0, 3 0, 0 3, 0 0))', 'POLYGON EMPTY', 'polygon'),
        ('MULTIPOLYGON (((0 0, 3 0, 0 3, 0 0)))', 'MULTIPOLYGON EMPTY', 'multipolygon'),
    ],
)
@pytest.mark.parametrize('frontend', ['pyarrow', 'cdata', 'stream'])
def test_homogeneous_native_layout_keeps_empty_and_missing_rows(
    wkt: str, empty: str, encoding: str, frontend: str
) -> None:
    source = gm.GeometryArray([gm.from_wkt(wkt), gm.from_wkt(empty), None], crs=4326)
    arrow = source.to_arrow()
    assert arrow.type.extension_name == f'geoarrow.{encoding}'
    if frontend == 'cdata':
        foreign = _capsule_only_from_pyarrow(arrow)
    elif frontend == 'stream':
        foreign = _StreamOnly(pa.table({'geometry': arrow}))
    else:
        foreign = arrow
    restored = arrow_geometries(foreign)
    assert restored.to_wkt() == [wkt, empty, None]
    assert restored.is_missing.tolist() == [False, False, True]
    assert restored.crs == source.crs


def test_native_empty_point_sentinel_retains_z_and_m_axes() -> None:
    source = gm.GeometryArray([
        gm.from_wkt('POINT ZM (7 13 19 23)'),
        gm.from_wkt('POINT ZM EMPTY'),
        None,
    ])
    arrow: Any = source.to_arrow()
    assert arrow.type.extension_name == 'geoarrow.point'
    assert arrow.storage.type.names == ['x', 'y', 'z', 'm']
    assert all(
        math.isnan(arrow.storage.field(axis)[1].as_py())
        for axis in ('x', 'y', 'z', 'm')
    )
    restored = arrow_geometries(arrow)
    assert restored.to_wkt() == ['POINT ZM (7 13 19 23)', 'POINT ZM EMPTY', None]


_ARROW_LAYOUT_WKTS: dict[str, tuple[str, str, str]] = {
    'points': ('POINT (0 0)', 'POINT (1 1)', 'POINT (2 2)'),
    'lines': (
        'LINESTRING (0 0, 1 1)',
        'LINESTRING (2 2, 3 3, 4 4)',
        'LINESTRING (5 5, 6 6)',
    ),
    'polygons': (
        'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
        'POLYGON ((2 2, 5 2, 5 5, 2 5, 2 2), (3 3, 4 3, 4 4, 3 3))',
        'POLYGON ((6 6, 7 6, 7 7, 6 7, 6 6))',
    ),
    'multipoint': (
        'MULTIPOINT ((0 0), (1 1))',
        'MULTIPOINT ((2 2))',
        'MULTIPOINT ((3 3), (4 4), (5 5))',
    ),
    'multiline': (
        'MULTILINESTRING ((0 0, 1 1))',
        'MULTILINESTRING ((2 2, 3 3), (4 4, 5 5))',
        'MULTILINESTRING ((6 6, 7 7))',
    ),
    'multipolygon': (
        'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))',
        'MULTIPOLYGON (((2 2, 3 2, 3 3, 2 3, 2 2)), ((4 4, 5 4, 5 5, 4 5, 4 4)))',
        'MULTIPOLYGON (((6 6, 7 6, 7 7, 6 7, 6 6)))',
    ),
    'mixed': (
        'POINT (0 0)',
        'LINESTRING (1 1, 2 2)',
        'POLYGON ((3 3, 4 3, 4 4, 3 4, 3 3))',
    ),
}


def test_format_readers_normalize_arrow_scalars_and_nulls() -> None:
    point = gm.Point(1, 2, crs=4326)
    wkts = pa.array(['POINT (1 2)', None])
    wkbs = pa.chunked_array([
        pa.array([point.to_wkb()]),
        pa.array([None], type=pa.binary()),
    ])
    geojson = pa.array([_json.dumps({'type': 'Point', 'coordinates': [1, 2]}), None])
    polylines = pa.chunked_array([
        pa.array([point.to_polyline()]),
        pa.array([None], type=pa.string()),
    ])

    assert gm.from_wkt(wkts).to_wkt() == ['POINT (1 2)', None]
    assert gm.from_wkb(wkbs).to_wkt() == ['POINT (1 2)', None]
    assert gm.from_geojson(geojson).to_wkt() == ['POINT (1 2)', None]
    assert gm.from_polyline(polylines).to_wkt() == ['POINT (1 2)', None]


@pytest.mark.parametrize('layout', list(_ARROW_LAYOUT_WKTS))
@pytest.mark.parametrize('masked', [False, True], ids=['dense', 'masked'])
@pytest.mark.parametrize('shape', ['whole', 'sliced', 'chunked'])
def test_arrow_conformance_matrix_preserves_validity_and_values(
    layout: str, masked: bool, shape: str
) -> None:
    payload = [gm.from_wkt(wkt) for wkt in _ARROW_LAYOUT_WKTS[layout]]
    expected = gm.GeometryArray(
        [payload[0], None, payload[2]] if masked else payload, crs=4326
    )
    if shape == 'whole':
        arrow_like = expected.to_arrow()
    elif shape == 'sliced':
        sentinels = [gm.from_wkt(wkt) for wkt in _ARROW_LAYOUT_WKTS[layout]]
        padded = gm.GeometryArray(
            [sentinels[-1], *list(expected), sentinels[0]], crs=4326
        )
        arrow_like = padded.to_arrow().slice(1, len(expected))
    elif shape == 'chunked':
        arrow = expected.to_arrow()
        arrow_like = pa.chunked_array([
            arrow.slice(0, 1),
            arrow.slice(1, 1),
            arrow.slice(2),
        ])
    else:
        raise AssertionError(shape)

    restored = cast('gm.GeometryArray', arrow_geometries(arrow_like))
    assert restored.is_missing.tolist() == expected.is_missing.tolist()
    assert restored.to_wkt() == expected.to_wkt()
    _assert_same_crs(restored.crs, expected.crs)


@pytest.mark.parametrize('layout', list(_ARROW_LAYOUT_WKTS))
@pytest.mark.parametrize('consumer', ['pyarrow', 'arro3'])
def test_exported_window_outlives_geometry_and_preserves_frame(
    layout: str, consumer: str
) -> None:
    payload = [gm.from_wkt(wkt) for wkt in _ARROW_LAYOUT_WKTS[layout]]
    source = gm.GeometryArray(
        [payload[0], None, payload[2], payload[1]], crs=4326, epoch=2020.5
    )
    window = source[1:4]
    expected = window.to_wkt(drop_epoch=True)
    if consumer == 'pyarrow':
        published = pa.array(window)
    else:
        arro3 = pytest.importorskip('arro3.core')
        published = arro3.Array.from_arrow(window)
    del source, window, payload

    restored = cast('gm.GeometryArray', arrow_geometries(published))
    assert restored.to_wkt(drop_epoch=True) == expected
    _assert_same_crs(restored.crs, gm.CRS(4326))
    assert restored.epoch == 2020.5


def test_point_window_exports_share_all_coordinate_buffers() -> None:
    points = gm.points([1.25, 9.0, 17.5], [-3.5, 4.5, 8.25], z=[2, 3, 4], m=[5, 6, 7])
    parent = pa.array(points).storage
    window = pa.array(points[1:]).storage

    for axis in ['x', 'y', 'z', 'm']:
        assert (
            parent.field(axis).buffers()[1].address
            == window.field(axis).buffers()[1].address
        )
        assert window.field(axis).offset == 1
    del points, parent
    assert window.to_pylist() == [
        {'x': 9.0, 'y': 4.5, 'z': 3.0, 'm': 6.0},
        {'x': 17.5, 'y': 8.25, 'z': 4.0, 'm': 7.0},
    ]


def test_line_window_exports_share_offsets_and_coordinate_buffers() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(1, 2), (3, 4)]),
        gm.LineString([(5, 6), (7, 8)]),
        gm.LineString([(9, 10), (11, 12)]),
    ])
    parent = pa.array(lines).storage
    window = pa.array(lines[1:]).storage

    assert window.offset == 1
    assert parent.buffers()[1].address == window.buffers()[1].address
    for axis in ['x', 'y']:
        assert (
            parent.values.field(axis).buffers()[1].address
            == window.values.field(axis).buffers()[1].address
        )
    del lines, parent
    assert window.to_pylist() == [
        [{'x': 5.0, 'y': 6.0}, {'x': 7.0, 'y': 8.0}],
        [{'x': 9.0, 'y': 10.0}, {'x': 11.0, 'y': 12.0}],
    ]


def test_z_m_point_arrays_and_lines_preserve_axes_through_arrow() -> None:
    values = gm.points([0, 1], [2, 3], z=[4, 5], m=[6, 7], crs=4326)
    xyz_values = gm.points([0], [1], z=[2], crs=4326)
    xym_values = gm.points([0], [1], m=[3], crs=4326)
    line = gm.LineString([(0, 0), (1, 1)], z=[10, 11], m=[20, 21], crs=4326)
    line_values = gm.GeometryArray([line])
    polygon_values = gm.GeometryArray([
        gm.Polygon([(0, 0, 1, 10), (2, 0, 2, 20), (0, 2, 3, 30)], crs=4326)
    ])
    mixed_axes = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1, z=1)])
    assert values.common_coordinate_axes == 'XYZM'
    assert [point.coords.to_nested() for point in list(values)] == [
        [0.0, 2.0, 4.0, 6.0],
        [1.0, 3.0, 5.0, 7.0],
    ]
    assert line.coordinate_axes == 'XYZM'
    assert (line).to_wkt() == 'LINESTRING ZM (0 0 10 20, 1 1 11 21)'
    assert gm.from_wkt((line).to_wkt()).coords.to_nested() == line.coords.to_nested()
    assert line.topological_dimension == 1
    assert (line).with_crs(None).length == pytest.approx(2**0.5)
    assert list(line.coords.select('XYZM')) == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]
    assert (line).quantize(0).coords.to_nested() == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]
    assert [
        point.coords.to_nested() for point in values.with_coordinates(z=None, m=None)
    ] == [[0.0, 2.0], [1.0, 3.0]]
    assert line.with_coordinates(z=None, m=None).coords.to_nested() == [
        (0.0, 0.0),
        (1.0, 1.0),
    ]
    assert gm.require(values, crs=4326, axes='XYZM').crs == gm.CRS('EPSG:4326')
    assert (
        gm.require(
            values.with_coordinates(z=None, m=None), axes='XY'
        ).common_coordinate_axes
        == 'XY'
    )
    values_arrow = values.to_arrow()
    xyz_arrow = xyz_values.to_arrow()
    xym_arrow = xym_values.to_arrow()
    line_arrow = line_values.to_arrow()
    polygon_arrow = polygon_values.to_arrow()
    mixed_arrow = mixed_axes.to_arrow()
    assert values_arrow.type.extension_name == 'geoarrow.point'
    assert values_arrow.type.storage_type == pa.struct([
        pa.field('x', pa.float64()),
        pa.field('y', pa.float64()),
        pa.field('z', pa.float64()),
        pa.field('m', pa.float64()),
    ])
    assert values_arrow.storage.field('z').to_pylist() == [4.0, 5.0]
    assert values_arrow.storage.field('m').to_pylist() == [6.0, 7.0]
    assert xyz_arrow.type.storage_type == pa.struct([
        pa.field('x', pa.float64()),
        pa.field('y', pa.float64()),
        pa.field('z', pa.float64()),
    ])
    assert xym_arrow.type.storage_type == pa.struct([
        pa.field('x', pa.float64()),
        pa.field('y', pa.float64()),
        pa.field('m', pa.float64()),
    ])
    assert line_arrow.type.extension_name == 'geoarrow.linestring'
    assert line_arrow.type.storage_type == pa.list_(values_arrow.type.storage_type)
    assert polygon_arrow.type.extension_name == 'geoarrow.polygon'
    assert polygon_arrow.type.storage_type == pa.list_(
        pa.list_(values_arrow.type.storage_type)
    )
    assert mixed_arrow.type.extension_name == 'geoarrow.wkb'
    assert [
        point.coords.to_nested()
        for point in cast('gm.GeometryArray', arrow_geometries(values_arrow))
    ] == [[0.0, 2.0, 4.0, 6.0], [1.0, 3.0, 5.0, 7.0]]
    line_back = arrow_geometries(line_arrow)
    assert isinstance(line_back, gm.GeometryArray)
    assert len(line_back) == 1
    assert line_back[0].coords.to_nested() == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]
    polygon_back = arrow_geometries(polygon_arrow)
    assert isinstance(polygon_back, gm.GeometryArray)
    assert len(polygon_back) == 1
    assert polygon_back[0].coords.to_nested() == [
        [
            (0.0, 0.0, 1.0, 10.0),
            (2.0, 0.0, 2.0, 20.0),
            (0.0, 2.0, 3.0, 30.0),
            (0.0, 0.0, 1.0, 10.0),
        ]
    ]
    with pytest.raises(ValueError, match='same length'):
        gm.points([0, 1], [0, 1], z=[1])
    with pytest.raises(ValueError, match='same length'):
        gm.LineString([(0, 0), (1, 1)], m=[1])
    with pytest.raises(ValueError, match='expected axes'):
        gm.require(values, axes='XY')
    with pytest.raises(ValueError, match='expected CRS'):
        gm.require(values, crs=3857)
    arrow_values = arrow_geometries(values.to_arrow())
    assert isinstance(arrow_values, gm.GeometryArray)
    assert [point.coords.to_nested() for point in list(arrow_values)] == [
        [0.0, 2.0, 4.0, 6.0],
        [1.0, 3.0, 5.0, 7.0],
    ]
    arrow_line = arrow_geometries(line.to_arrow())
    assert isinstance(arrow_line, gm.GeometryArray)
    assert len(arrow_line) == 1
    assert arrow_line[0].coords.to_nested() == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]
    mixed_arrow = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 2, z=3)]).to_arrow()
    arrow_mixed = arrow_geometries(mixed_arrow)
    assert isinstance(arrow_mixed, gm.GeometryArray)
    assert [point.coords.to_nested() for point in list(arrow_mixed)] == [
        [0.0, 0.0],
        [1.0, 2.0, 3.0],
    ]
    assert gm.from_wkb((line).to_wkb()).coords.to_nested() == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]


@pytest.mark.parametrize(
    ('wkt', 'axes', 'fields', 'depth', 'extension_name'),
    [
        ('MULTILINESTRING (EMPTY)', 'XY', ('x', 'y'), 2, 'geoarrow.multilinestring'),
        (
            'MULTILINESTRING Z (EMPTY)',
            'XYZ',
            ('x', 'y', 'z'),
            2,
            'geoarrow.multilinestring',
        ),
        (
            'MULTILINESTRING M (EMPTY)',
            'XYM',
            ('x', 'y', 'm'),
            2,
            'geoarrow.multilinestring',
        ),
        (
            'MULTILINESTRING ZM (EMPTY)',
            'XYZM',
            ('x', 'y', 'z', 'm'),
            2,
            'geoarrow.multilinestring',
        ),
        ('MULTIPOLYGON (EMPTY)', 'XY', ('x', 'y'), 3, 'geoarrow.multipolygon'),
        ('MULTIPOLYGON Z (EMPTY)', 'XYZ', ('x', 'y', 'z'), 3, 'geoarrow.multipolygon'),
        ('MULTIPOLYGON M (EMPTY)', 'XYM', ('x', 'y', 'm'), 3, 'geoarrow.multipolygon'),
        (
            'MULTIPOLYGON ZM (EMPTY)',
            'XYZM',
            ('x', 'y', 'z', 'm'),
            3,
            'geoarrow.multipolygon',
        ),
    ],
    ids=[
        'multilinestring-xy',
        'multilinestring-xyz',
        'multilinestring-xym',
        'multilinestring-xyzm',
        'multipolygon-xy',
        'multipolygon-xyz',
        'multipolygon-xym',
        'multipolygon-xyzm',
    ],
)
def test_explicit_empty_multipart_arrow_export_preserves_axes(
    wkt: str,
    axes: str,
    fields: tuple[str, ...],
    depth: int,
    extension_name: str,
) -> None:
    """Native Arrow keeps axes and one explicit empty multipart member."""
    geometry = gm.from_wkt(wkt)
    values = gm.GeometryArray([geometry])
    coordinate = pa.struct([pa.field(name, pa.float64()) for name in fields])
    storage_type = coordinate
    for _ in range(depth):
        storage_type = pa.list_(storage_type)

    for owner in (geometry, values):
        arrow = owner.to_arrow()
        assert arrow.type.extension_name == extension_name
        assert arrow.type.storage_type == storage_type
        assert arrow.storage.to_pylist() == [[[]]]
        restored = cast('gm.GeometryArray', arrow_geometries(arrow))
        assert restored.to_wkt() == [geometry.to_wkt()]
        assert restored[0].coordinate_axes == axes
        assert len(restored[0]) == 1

        # PyArrow's low-level import consumes the same public C schema and array
        # capsules that a foreign Arrow consumer receives.
        schema_capsule = owner.__arrow_c_schema__()
        _, array_capsule = owner.__arrow_c_array__()
        get_pointer = ctypes.pythonapi.PyCapsule_GetPointer
        get_pointer.argtypes = [ctypes.py_object, ctypes.c_char_p]
        get_pointer.restype = ctypes.c_void_p
        schema_pointer = get_pointer(schema_capsule, b'arrow_schema')
        array_pointer = get_pointer(array_capsule, b'arrow_array')
        c_array = pa.Array._import_from_c(array_pointer, schema_pointer)
        assert c_array.type.extension_name == extension_name
        assert c_array.type.storage_type == storage_type

        class ArrayOnly:
            def __init__(self, source: object) -> None:
                self.source = source

            def __arrow_c_array__(
                self, requested_schema: object | None = None
            ) -> object:
                return self.source.__arrow_c_array__(requested_schema)  # type: ignore[attr-defined]

        class StreamOnly:
            def __init__(self, source: object) -> None:
                self.source = source

            def __arrow_c_stream__(
                self, requested_schema: object | None = None
            ) -> object:
                return self.source.__arrow_c_stream__(requested_schema)  # type: ignore[attr-defined]

        c_restored = cast('gm.GeometryArray', arrow_geometries(ArrayOnly(owner)))
        assert c_restored.to_wkt() == [geometry.to_wkt()]
        assert c_restored[0].coordinate_axes == axes
        stream_restored = cast('gm.GeometryArray', arrow_geometries(StreamOnly(owner)))
        assert stream_restored.to_wkt() == [geometry.to_wkt()]
        assert stream_restored[0].coordinate_axes == axes


def test_explicit_empty_multipart_arrow_controls_preserve_other_contracts() -> None:
    """Typed empties, missing rows, nonempty members, and frames stay distinct."""
    for wkt, axes, native_encoding in (
        ('MULTILINESTRING Z EMPTY', 'XYZ', 'geoarrow.multilinestring'),
        ('MULTIPOLYGON ZM EMPTY', 'XYZM', 'geoarrow.multipolygon'),
    ):
        geometry = gm.from_wkt(wkt)
        for encoding in ('auto', 'wkb'):
            arrow = geometry.to_arrow(encoding=encoding)
            assert arrow.type.extension_name == (
                native_encoding if encoding == 'auto' else 'geoarrow.wkb'
            )
            restored = cast('gm.GeometryArray', arrow_geometries(arrow))
            assert restored.to_wkt() == [wkt]
            assert restored[0].coordinate_axes == axes
            assert len(restored[0]) == 0

    geometry = gm.from_wkt('MULTILINESTRING ZM (EMPTY)')
    missing = gm.GeometryArray([geometry, None])
    restored = cast('gm.GeometryArray', arrow_geometries(missing.to_arrow()))
    assert restored.is_missing.tolist() == [False, True]
    assert restored.to_wkt() == [geometry.to_wkt(), None]
    assert restored[0].coordinate_axes == 'XYZM'

    mixed = gm.from_wkt(
        'MULTIPOLYGON ZM  (EMPTY, ((0 0 1 2, 1 0 1 2, 0 1 1 2, 0 0 1 2)))'
    )
    mixed_arrow = mixed.to_arrow()
    assert mixed_arrow.type.storage_type == pa.list_(
        pa.list_(
            pa.list_(
                pa.struct([
                    pa.field('x', pa.float64()),
                    pa.field('y', pa.float64()),
                    pa.field('z', pa.float64()),
                    pa.field('m', pa.float64()),
                ])
            )
        )
    )
    mixed_restored = cast('gm.GeometryArray', arrow_geometries(mixed_arrow))
    assert mixed_restored.to_wkt() == [mixed.to_wkt()]
    assert mixed_restored[0].coordinate_axes == 'XYZM'
    assert len(mixed_restored[0]) == 2

    for wkts, axes in (
        (
            ('MULTILINESTRING Z (EMPTY)', 'MULTILINESTRING M (EMPTY)'),
            ('XYZ', 'XYM'),
        ),
        (
            ('MULTIPOLYGON Z (EMPTY)', 'MULTIPOLYGON ZM (EMPTY)'),
            ('XYZ', 'XYZM'),
        ),
    ):
        mixed_axes = gm.GeometryArray([gm.from_wkt(wkt) for wkt in wkts])
        mixed_axes_arrow = mixed_axes.to_arrow()
        assert mixed_axes_arrow.type.extension_name == 'geoarrow.wkb'
        mixed_axes_restored = cast(
            'gm.GeometryArray', arrow_geometries(mixed_axes_arrow)
        )
        assert mixed_axes_restored.to_wkt() == list(wkts)
        assert [value.coordinate_axes for value in mixed_axes_restored] == list(axes)

    framed = gm.from_wkt('MULTILINESTRING ZM (EMPTY)', crs=4326, epoch=2020.0)
    framed_restored = cast('gm.GeometryArray', arrow_geometries(framed.to_arrow()))
    assert framed_restored.to_wkt(drop_epoch=True) == [framed.to_wkt(drop_epoch=True)]
    assert framed_restored[0].coordinate_axes == 'XYZM'
    assert framed_restored.crs.to_epsg() == 4326
    assert framed_restored.epoch == 2020.0


def test_sliced_nullable_geometry_export_rebases_validity_bits() -> None:
    source = gm.GeometryArray([
        gm.Point(0.0, 10.0),
        None,
        gm.Point(2.0, 12.0),
        gm.Point(3.0, 13.0),
        None,
        gm.Point(5.0, 15.0),
    ])
    sliced = source[1:5]
    arrow = sliced.to_arrow()

    assert arrow.type.extension_name == 'geoarrow.point'
    validity = arrow.storage.buffers()[0]
    assert validity is not None
    assert validity.to_pybytes()[0] & 0b1111 == 0b0110
    assert arrow.is_null().to_pylist() == [True, False, False, True]

    restored = cast('gm.GeometryArray', arrow_geometries(arrow))
    assert restored.is_missing.tolist() == [True, False, False, True]
    assert restored.to_wkt() == [None, 'POINT (2 12)', 'POINT (3 13)', None]


def test_to_arrow_forced_wkb_encoding_roundtrips() -> None:
    values = gm.points([0.0, 1.0], [2.0, 3.0], crs=4326)
    arrow = values.to_arrow(encoding='wkb')
    assert arrow.type.extension_name == 'geoarrow.wkb'
    assert arrow.storage.type == pa.binary()
    restored = cast('gm.GeometryArray', arrow_geometries(arrow))
    assert canon(restored) == canon(values)
    _assert_same_crs(restored.crs, values.crs)

    scalar = gm.Point(4.0, 5.0, crs=4326)
    scalar_arrow = scalar.to_arrow(encoding='wkb')
    assert scalar_arrow.type.extension_name == 'geoarrow.wkb'
    scalar_restored = arrow_geometries(scalar_arrow)
    assert isinstance(scalar_restored, gm.GeometryArray)
    assert canon(scalar_restored) == canon(gm.GeometryArray([scalar]))


@pytest.mark.parametrize('import_order', ['cold', 'warm'])
def test_to_arrow_registers_extensions_before_capsule_materialization(
    import_order: str,
) -> None:
    preamble = (
        "import gometry as gm\nimport sys\nassert 'pyarrow' not in sys.modules\n"
        if import_order == 'cold'
        else 'import pyarrow\nimport gometry as gm\n'
    )
    code = f"""
{preamble}
point = gm.Point(1, 2, crs=4326)
native_scalar = point.to_arrow()
native_array = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326).to_arrow()
wkb_scalar = point.to_arrow(encoding='wkb')
wkb_array = gm.GeometryArray([point, None]).to_arrow(encoding='wkb')
for arrow, extension_name in (
    (native_scalar, 'geoarrow.point'),
    (native_array, 'geoarrow.point'),
    (wkb_scalar, 'geoarrow.wkb'),
    (wkb_array, 'geoarrow.wkb'),
):
    assert type(arrow.type).__module__ == 'gometry._arrow'
    assert arrow.type.extension_name == extension_name
assert gm.from_arrow(native_scalar).to_wkt() == ['POINT (1 2)']
assert gm.from_arrow(native_array).to_wkt() == ['POINT (1 3)', 'POINT (2 4)']
assert gm.from_arrow(wkb_scalar).to_wkt() == ['POINT (1 2)']
assert gm.from_arrow(wkb_array).to_wkt() == ['POINT (1 2)', None]
"""
    subprocess.run([sys.executable, '-c', code], check=True)


def test_nullable_pyarrow_import_preserves_explicit_frame() -> None:
    values = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
        gm.LineString([(2.0, 2.0), (3.0, 3.0)]),
    ])
    nullable = with_validity(values.to_arrow(), b'\x01')
    restored = cast(
        'gm.GeometryArray', arrow_geometries(nullable, crs=4326, epoch=2020.0)
    )
    assert restored.is_missing.tolist() == [False, True]
    assert restored.crs.to_epsg() == 4326
    assert restored.epoch == 2020.0
    assert restored[0].crs.to_epsg() == 4326
    assert restored[0].epoch == 2020.0


def test_from_arrow_epoch_accepts_numpy_scalar_and_index_protocol() -> None:
    class IndexEpoch:
        def __index__(self) -> int:
            return 2024

    arrow = gm.GeometryArray([gm.Point(1.0, 2.0)]).to_arrow()
    for epoch, expected in ((np.float64(2023.5), 2023.5), (IndexEpoch(), 2024.0)):
        restored = arrow_geometries(arrow, crs=4326, epoch=epoch)
        assert restored.epoch == expected


@pytest.mark.parametrize(
    ('rows', 'extension_name', 'present_payload_count'),
    [
        (
            [gm.Point(0, 0), None, gm.Point(2, 2)],
            'geoarrow.point',
            2,
        ),
        (
            [
                gm.LineString([(0, 0), (1, 1)]),
                None,
                gm.LineString([(2, 2), (3, 3)]),
            ],
            'geoarrow.linestring',
            4,
        ),
        (
            [gm.box(0, 0, 1, 1), None, gm.box(2, 2, 3, 3)],
            'geoarrow.polygon',
            10,
        ),
    ],
)
def test_nullable_arrow_c_import_keeps_compact_native_payload(
    rows: list[object], extension_name: str, present_payload_count: int
) -> None:
    values = gm.GeometryArray(rows, crs=4326).with_epoch(2020.0)
    source = values.to_arrow()

    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return source.__arrow_c_array__()

    restored = cast('gm.GeometryArray', arrow_geometries(CapsuleOnly()))
    assert restored.to_wkt(drop_epoch=True) == values.to_wkt(drop_epoch=True)
    assert restored.is_missing.tolist() == [False, True, False]
    assert restored.crs.to_epsg() == 4326
    assert restored.epoch == 2020.0
    native = restored.to_arrow()
    assert native.type.extension_name == extension_name
    if extension_name == 'geoarrow.point':
        assert len(native.storage.field('x')) == len(values)
    elif extension_name == 'geoarrow.linestring':
        assert len(native.storage.values.field('x')) == present_payload_count
    else:
        assert len(native.storage.values.values.field('x')) == present_payload_count


def test_nullable_wkb_arrow_omits_null_payload_bytes() -> None:
    values = gm.GeometryArray([
        gm.Point(0, 0),
        None,
        gm.Point(2, 2),
    ])
    arrow = values.to_arrow(encoding='wkb')
    offsets = struct.unpack('<4i', arrow.storage.buffers()[1].to_pybytes())
    first, last = values[[0, 2]].to_wkb()
    assert offsets == (0, len(first), len(first), len(first) + len(last))
    assert arrow.storage.buffers()[2].size == len(first) + len(last)


def test_from_arrow_deeply_nested_list_schema_no_sigsegv() -> None:
    """P01: deep list nesting must not stack-overflow; clean typed rejection.

    Depth is well above CPython's default recursion limit so a recursive type
    walk would die, but well below depths that crash pyarrow type teardown
    independently of gometry.
    """
    import subprocess
    import sys

    # ~2x sys.getrecursionlimit() - old recursive LargeList walk overflowed;
    # exact encoding classification rejects without deep recursion.
    depth = max(2000, sys.getrecursionlimit() * 2)
    code = f"""
import gometry as gm
import pyarrow as pa
import sys

t = pa.float64()
for _ in range({depth}):
    t = pa.list_(t)
field = pa.field(
    'geometry',
    t,
    metadata={{
        b'ARROW:extension:name': b'geoarrow.point',
        b'ARROW:extension:metadata': b'{{}}',
    }},
)
table = pa.Table.from_batches([], schema=pa.schema([field]))
try:
    gm.from_arrow(table)
except Exception as exc:
    assert not isinstance(exc, RecursionError), type(exc)
    print(type(exc).__name__, ':', exc)
    sys.exit(0)
raise SystemExit('deep nested schema was accepted')
"""
    completed = subprocess.run(
        [sys.executable, '-c', code],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert completed.returncode == 0, (
        f'exit={completed.returncode}\nstdout={completed.stdout}\nstderr={completed.stderr}'
    )
    assert 'TypeError' in completed.stdout or 'ParseError' in completed.stdout
    assert 'RecursionError' not in completed.stdout
    assert 'struct' in completed.stdout or 'list' in completed.stdout


def test_from_arrow_null_count_bitmap_mismatch_rejects_all_frontends() -> None:
    """P02: lying null_count vs validity bitmap must not panic; frontends agree."""
    import struct

    import pyarrow as pa
    import pytest

    wkb = gm.Point(1, 2).to_wkb()
    # Validity buffer bit0 set (row present) while null_count claims 1 null.
    lying = pa.Array.from_buffers(
        pa.binary(),
        1,
        [
            pa.py_buffer(b'\x01'),
            pa.py_buffer(struct.pack('<2i', 0, len(wkb))),
            pa.py_buffer(wkb),
        ],
        null_count=1,
    )
    assert lying.null_count == 1
    assert lying.is_valid().to_pylist() == [True]

    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return lying.__arrow_c_array__()

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.table({'g': lying}).__arrow_c_stream__()

    outcomes: list[tuple[str, type[BaseException], str]] = []
    for name, source in (
        ('direct', lying),
        ('capsule', CapsuleOnly()),
        ('stream', StreamOnly()),
    ):
        try:
            arrow_geometries(source)
        except BaseException as exc:
            # BaseException so a PanicException cannot slip past as "pass".
            outcomes.append((name, type(exc), str(exc)))
            continue
        pytest.fail(f'{name} imported instead of rejecting null_count mismatch')

    # No panic; typed ParseError with the same message on every frontend.
    for name, exc_type, message in outcomes:
        assert exc_type is gm.ParseError, f'{name}: {exc_type.__name__}: {message}'
        assert 'null_count' in message and 'validity bitmap' in message
    assert len({message for _, _, message in outcomes}) == 1


def test_from_arrow_consistent_null_positive_still_imports() -> None:
    """P02 positive: matching null_count + bitmap still yields a missing row."""
    values = gm.GeometryArray([gm.Point(1, 2), gm.Point(3, 4)])
    nullable = with_validity(values.to_arrow(encoding='wkb'), b'\x01')
    restored = cast('gm.GeometryArray', arrow_geometries(nullable))
    assert restored.is_missing.tolist() == [False, True]
    assert restored[0].to_wkt() == 'POINT (1 2)'


def test_sliced_geoarrow_import_decodes_only_the_visible_span() -> None:
    families = {
        'point': [gm.from_wkt(f'POINT ({i} {i + 1})') for i in range(6)],
        'point_z': [gm.from_wkt(f'POINT Z ({i} {i + 1} {i + 2})') for i in range(6)],
        'multipoint': [
            gm.from_wkt(f'MULTIPOINT (({i} {i}), ({i + 1} {i + 1}))') for i in range(6)
        ],
        'linestring': [
            gm.from_wkt(f'LINESTRING ({i} {i}, {i + 1} {i + 1}, {i + 2} {i + 2})')
            for i in range(6)
        ],
        'multilinestring': [
            gm.from_wkt(
                f'MULTILINESTRING (({i} {i}, {i + 1} {i + 1}), ({i + 2} {i + 2}, {i + 3} {i + 3}))'
            )
            for i in range(6)
        ],
        'polygon': [
            gm.from_wkt(f'POLYGON (({i} {i}, {i + 2} {i}, {i + 2} {i + 2}, {i} {i}))')
            for i in range(6)
        ],
        'multipolygon': [
            gm.from_wkt(
                f'MULTIPOLYGON ((({i} {i}, {i + 2} {i}, {i + 2} {i + 2}, {i} {i})), (({i + 3} {i + 3}, {i + 5} {i + 3}, {i + 5} {i + 5}, {i + 3} {i + 3})))'
            )
            for i in range(6)
        ],
    }
    slices = [(0, 6), (0, 2), (1, 2), (4, 2), (3, 1), (5, 1), (2, 0)]
    for name, geoms in families.items():
        arrow = gm.GeometryArray(geoms).to_arrow()
        for offset, length in slices:
            imported = arrow_geometries(arrow.slice(offset, length))
            assert isinstance(imported, gm.GeometryArray)
            got = list(imported)
            expected = geoms[offset : offset + length]
            assert len(got) == len(expected), f'{name} slice({offset}, {length}) length'
            assert all(
                (gm.equals_exact(g, e) for g, e in zip(got, expected, strict=True))
            ), f'{name} slice({offset}, {length}) coordinates'


def test_large_native_geoarrow_point_import_roundtrips_packed_coordinates() -> None:
    count = 100000
    xs = [float(i) for i in range(count)]
    ys = [float(-i) / 2.0 for i in range(count)]
    values = gm.points(xs, ys)
    restored = cast('gm.GeometryArray', arrow_geometries(values.to_arrow()))
    assert len(restored) == count
    assert restored.to_arrow().storage.field('x').to_pylist() == xs
    assert restored.to_arrow().storage.field('y').to_pylist() == ys


def test_sliced_geoarrow_point_import_respects_coordinate_child_offsets() -> None:
    values = gm.points([0.0, 1.0, 2.0, 3.0], [10.0, 11.0, 12.0, 13.0])
    sliced = values.to_arrow().slice(1, 2)
    storage = sliced.storage
    assert storage.field('x').offset == 1
    assert storage.field('y').offset == 1
    restored = cast('gm.GeometryArray', arrow_geometries(sliced))
    assert [point.coords.to_nested() for point in restored] == [
        [1.0, 11.0],
        [2.0, 12.0],
    ]


def test_from_arrow_wkb_rejects_ewkb_srid_conflicting_with_extension_crs() -> None:
    wkb_type = (
        gm
        .GeometryArray([gm.Point(0, 0, crs=3857), gm.box(0, 0, 1, 1, crs=3857)])
        .to_arrow()
        .type
    )
    assert wkb_type.extension_name == 'geoarrow.wkb'
    ewkb = gm.Point(1, 2, crs=4326).to_wkb(include_srid=True)
    arrow = pa.ExtensionArray.from_storage(wkb_type, pa.array([ewkb], pa.binary()))
    with pytest.raises(
        gm.CRSMismatchError, match='conflicts with the embedded EWKB SRID'
    ):
        arrow_geometries(arrow)


def test_geoarrow_wkb_roundtrip_for_scalar_and_array() -> None:
    point = gm.Point(1, 2, crs=4326)
    values = gm.GeometryArray([point, gm.box(0, 0, 1, 1, crs=4326)])
    wkb_values = (values).to_wkb()
    ewkb_values = (values).to_wkb(include_srid=True)
    arrow = (values).to_arrow()
    recovered = arrow_geometries(arrow)
    assert arrow.type.extension_name == 'geoarrow.wkb'
    assert _json.loads(arrow.type.__arrow_ext_serialize__()) == _json.loads(
        _WGS84_PROJJSON_METADATA
    )
    assert isinstance(recovered, gm.GeometryArray)
    assert recovered.crs.to_epsg() == 4326
    assert [(item).to_wkt() for item in list(recovered)] == [
        'POINT (1 2)',
        'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
    ]
    singleton = arrow_geometries((point).to_arrow())
    assert isinstance(singleton, gm.GeometryArray)
    assert len(singleton) == 1
    assert (singleton[0]).to_wkt() == 'POINT (1 2)'
    wkb_recovered = cast('gm.GeometryArray', gm.from_wkb(wkb_values))
    memoryview_recovered = cast(
        'gm.GeometryArray', gm.from_wkb([memoryview(value) for value in wkb_values])
    )
    ewkb_recovered = cast('gm.GeometryArray', gm.from_wkb(ewkb_values))
    assert [(item).to_wkt() for item in list(wkb_recovered)] == [
        (item).to_wkt() for item in values
    ]
    assert [(item).to_wkt() for item in list(memoryview_recovered)] == [
        (item).to_wkt() for item in values
    ]
    assert ewkb_recovered.crs.to_epsg() == 4326
    assert all(isinstance(value, bytes) for value in wkb_values)
    with pytest.raises(AttributeError, match='to_arrow'):
        (cast('Any', [point])).to_arrow()


def test_arrow_capsule_roundtrip_via_pyarrow_array_protocol() -> None:
    polygon_with_hole = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4)],
        holes=[[(1, 1), (2, 1), (2, 2), (1, 1)]],
        crs=4326,
        epoch=2020.0,
    )
    cases = [
        (gm.points([1, 3], [2, 4], crs=4326, epoch=2020.0), 'geoarrow.point'),
        (
            gm.GeometryArray([
                gm.LineString([(0, 0), (1, 1)], crs=4326, epoch=2020.0),
                gm.LineString([(2, 2), (3, 4)], crs=4326, epoch=2020.0),
            ]),
            'geoarrow.linestring',
        ),
        (gm.GeometryArray([polygon_with_hole]), 'geoarrow.polygon'),
        (
            gm.GeometryArray([
                gm.MultiPolygon([polygon_with_hole], crs=4326, epoch=2020.0)
            ]),
            'geoarrow.multipolygon',
        ),
    ]
    for values, extension_name in cases:
        arrow = pa.array(values)
        restored = arrow_geometries(arrow)
        assert arrow.type.extension_name == extension_name
        assert _json.loads(arrow.type.__arrow_ext_serialize__()) == {
            'crs': _CRS(4326).to_projjson(as_dict=True),
            'crs_type': 'projjson',
            'gometry:crs_definition': 'EPSG:4326',
            'epoch': 2020.0,
        }
        assert isinstance(restored, gm.GeometryArray)
        assert restored.crs.to_epsg() == 4326
        assert restored.epoch == 2020.0
        assert [str(g) for g in restored] == [str(g) for g in values]


def test_arrow_c_wkb_width_error_keeps_logical_row_note() -> None:
    shape = gm.Point(0.0, 0.0)
    for _ in range(128):
        shape = gm.GeometryCollection([shape, shape])
    values = gm.GeometryArray([None, shape, None])

    with pytest.raises(OverflowError) as error:
        values.to_arrow(encoding='wkb')
    assert error.value.__notes__ == ['while processing array element 1']


def test_arrow_capsules_can_be_consumed_without_pyarrow_materialization() -> None:
    values = gm.GeometryArray([
        gm.Point(0, 0, crs=4326, epoch=2015.5),
        gm.Point(1, 1, crs=4326, epoch=2015.5),
    ])
    schema_capsule, array_capsule = values.__arrow_c_array__()
    stream_capsule = values.__arrow_c_stream__()
    assert 'arrow_schema' in repr(schema_capsule)
    assert 'arrow_array' in repr(array_capsule)
    assert 'arrow_array_stream' in repr(stream_capsule)
    restored = cast('gm.GeometryArray', arrow_geometries(_CapsuleStreamOnly(values)))
    assert restored.crs.to_epsg() == 4326
    assert restored.epoch == 2015.5
    assert [str(g) for g in restored] == ['POINT (0 0)', 'POINT (1 1)']
    for export in (
        lambda: pa.array(values, type=pa.binary()),
        lambda: values[0].__arrow_c_array__(pa.binary()),
    ):
        with pytest.raises(TypeError, match='requested_schema does not match'):
            export()
    assert pa.array(values).type.extension_name == 'geoarrow.point'


def test_arrow_c_array_import_does_not_import_pyarrow() -> None:
    code = "\nimport importlib.abc\nimport sys\n\nimport gometry as gm\n\nclass _CapsuleArrayOnly:\n    def __init__(self, schema, array):\n        self._schema = schema\n        self._array = array\n\n    def __arrow_c_array__(self, requested_schema=None):\n        return (self._schema, self._array)\n\nclass BlockPyArrow(importlib.abc.MetaPathFinder):\n    def find_spec(self, fullname, path=None, target=None):\n        if fullname == 'pyarrow' or fullname.startswith('pyarrow.'):\n            raise AssertionError('pyarrow import attempted')\n        return None\n\nsys.meta_path.insert(0, BlockPyArrow())\nvalues = gm.points([0.0, 1.0], [2.0, 3.0], crs=4326, epoch=2020.0)\nschema, array = values.__arrow_c_array__()\nrestored = gm.from_arrow(_CapsuleArrayOnly(schema, array))\nassert 'pyarrow' not in sys.modules\nassert restored.crs.to_epsg() == 4326\nassert restored.epoch == 2020.0\nassert [str(value) for value in restored] == ['POINT (0 2)', 'POINT (1 3)']\n"
    subprocess.run([sys.executable, '-c', code], check=True)


def test_foreign_arrow_c_providers_with_shape_attributes_use_native_path() -> None:
    values = gm.points([0.0, 1.0], [2.0, 3.0], crs=4326, epoch=2020.0)

    class ForeignType:
        pass

    class ArrayProvider:
        type = ForeignType()

        def __arrow_c_array__(self, requested_schema: Any | None = None) -> Any:
            return values.__arrow_c_array__(requested_schema)

    class StreamProvider:
        column_names = ('geometry',)

        def __arrow_c_stream__(self, requested_schema: Any | None = None) -> Any:
            return values.__arrow_c_stream__(requested_schema)

    providers: list[Any] = [ArrayProvider(), StreamProvider()]
    try:
        import arro3.core as arro3
    except ModuleNotFoundError:
        pass
    else:
        providers.extend([
            arro3.Array.from_arrow(values.to_arrow()),
            arro3.Table.from_arrow(pa.table({'geometry': values.to_arrow()})),
        ])

    for provider in providers:
        restored = cast('gm.GeometryArray', arrow_geometries(provider))
        assert restored.to_wkt(drop_epoch=True) == values.to_wkt(drop_epoch=True)
        assert restored.crs.to_epsg() == 4326
        assert restored.epoch == 2020.0


def test_pyarrow_container_types_preserve_geometry_frame() -> None:
    values = gm.points([0.0, 1.0], [2.0, 3.0], crs=4326, epoch=2020.0)
    arrow = values.to_arrow()
    table = pa.table({'geometry': arrow})
    inputs: list[Any] = [
        arrow,
        pa.chunked_array([arrow]),
        table,
        pa.RecordBatch.from_arrays([arrow], names=['geometry']),
        pa.RecordBatchReader.from_batches(table.schema, table.to_batches()),
    ]

    for source in inputs:
        restored = cast('gm.GeometryArray', arrow_geometries(source))
        assert restored.to_wkt(drop_epoch=True) == values.to_wkt(drop_epoch=True)
        assert restored.crs.to_epsg() == 4326
        assert restored.epoch == 2020.0


def test_arrow_c_provider_shape_attributes_do_not_import_pyarrow() -> None:
    code = """
import importlib.abc

import gometry as gm

class BlockPyArrow(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pyarrow' or fullname.startswith('pyarrow.'):
            raise AssertionError('pyarrow import attempted')
        return None

class Provider:
    type = object()

    def __init__(self, source):
        self._source = source

    def __arrow_c_array__(self, requested_schema=None):
        return self._source.__arrow_c_array__(requested_schema)

import sys
sys.meta_path.insert(0, BlockPyArrow())
values = gm.points([0.0, 1.0], [2.0, 3.0], crs=4326, epoch=2020.0)
restored = gm.from_arrow(Provider(values))
assert restored.crs.to_epsg() == 4326
assert restored.epoch == 2020.0
assert [str(value) for value in restored] == ['POINT (0 2)', 'POINT (1 3)']
"""
    subprocess.run([sys.executable, '-c', code], check=True)


def test_gometry_arrow_provider_import_does_not_import_pyarrow() -> None:
    code = "\nimport importlib.abc\nimport sys\n\nimport gometry as gm\n\nclass BlockPyArrow(importlib.abc.MetaPathFinder):\n    def find_spec(self, fullname, path=None, target=None):\n        if fullname == 'pyarrow' or fullname.startswith('pyarrow.'):\n            raise AssertionError('pyarrow import attempted')\n        return None\n\nsys.meta_path.insert(0, BlockPyArrow())\nvalues = gm.points([0.0, 1.0], [2.0, 3.0], crs=4326, epoch=2020.0)\ntry:\n    gm.from_arrow(values)\nexcept TypeError as exc:\n    assert 'already decoded' in str(exc)\nelse:\n    raise AssertionError('from_arrow(GeometryArray) should raise TypeError')\nassert 'pyarrow' not in sys.modules\ntry:\n    gm.from_arrow(object())\nexcept TypeError as exc:\n    assert 'expected a GeoArrow-encoded Arrow' in str(exc)\nelse:\n    raise AssertionError('from_arrow(object()) should raise TypeError')\nassert 'pyarrow' not in sys.modules\n"
    subprocess.run([sys.executable, '-c', code], check=True)


class _CapsuleStreamOnly:
    def __init__(self, values: gm.GeometryArray) -> None:
        self._values = values

    def __arrow_c_stream__(self, requested_schema: Any | None = None) -> Any:
        return self._values.__arrow_c_stream__(requested_schema)


class _ArrayCapsulesOnly:
    def __init__(self, capsules: tuple[Any, Any]) -> None:
        self._capsules = capsules

    def __arrow_c_array__(self, requested_schema: Any | None = None) -> tuple[Any, Any]:
        return self._capsules


class _StreamCapsuleOnly:
    def __init__(self, capsule: Any) -> None:
        self._capsule = capsule

    def __arrow_c_stream__(self, requested_schema: Any | None = None) -> Any:
        return self._capsule


@pytest.mark.parametrize(
    'values',
    [
        gm.Point(0.0, 2.0, crs=4326, epoch=2020.0),
        gm.points([0.0, 1.0], [2.0, 3.0], crs=4326, epoch=2020.0),
    ],
    ids=['geometry', 'geometry-array'],
)
def test_identical_requested_arrow_schema_is_accepted_for_array_and_stream(
    values: gm.Geometry | gm.GeometryArray,
) -> None:
    expected = gm.GeometryArray([values]) if isinstance(values, gm.Geometry) else values
    schema, _ = values.__arrow_c_array__()

    array_capsules = values.__arrow_c_array__(schema)
    restored_array = cast(
        'gm.GeometryArray', arrow_geometries(_ArrayCapsulesOnly(array_capsules))
    )
    assert restored_array.to_wkt(drop_epoch=True) == expected.to_wkt(drop_epoch=True)
    assert restored_array.crs is not None and expected.crs is not None
    assert restored_array.crs.same_as(expected.crs, mode='exact')
    assert restored_array.epoch == expected.epoch

    stream = values.__arrow_c_stream__(schema)
    restored_stream = cast(
        'gm.GeometryArray', arrow_geometries(_StreamCapsuleOnly(stream))
    )
    assert restored_stream.to_wkt(drop_epoch=True) == expected.to_wkt(drop_epoch=True)
    assert restored_stream.crs is not None and expected.crs is not None
    assert restored_stream.crs.same_as(expected.crs, mode='exact')
    assert restored_stream.epoch == expected.epoch

    typed = pa.array(values, type=values.to_arrow().type)
    arrow = values.to_arrow()
    assert typed.type == arrow.type.storage_type
    assert typed.to_pylist() == arrow.storage.to_pylist()

    different_schema, _ = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)], crs=4326)
    ]).__arrow_c_array__()
    with pytest.raises(TypeError, match='requested_schema does not match'):
        values.__arrow_c_array__(different_schema)


class _ArrowCStreamOnly:
    def __init__(self, value: Any) -> None:
        self._value = value

    def __arrow_c_stream__(self, requested_schema: Any | None = None) -> Any:
        return self._value.__arrow_c_stream__(requested_schema)


def test_arrow_c_stream_import_consumes_all_batches() -> None:
    values = gm.points(
        [0.0, 1.0, 2.0, 3.0, 4.0],
        [10.0, 11.0, 12.0, 13.0, 14.0],
        crs=4326,
        epoch=2020.0,
    )
    arrow = values.to_arrow()
    chunked = pa.chunked_array([
        arrow.slice(0, 2),
        arrow.slice(2, 2),
        arrow.slice(4, 1),
    ])
    stream_only = _ArrowCStreamOnly(chunked)
    assert not hasattr(stream_only, 'chunks')
    restored = cast('gm.GeometryArray', arrow_geometries(stream_only))
    assert restored.crs.to_epsg() == 4326
    assert restored.epoch == 2020.0
    assert [str(value) for value in restored] == [
        'POINT (0 10)',
        'POINT (1 11)',
        'POINT (2 12)',
        'POINT (3 13)',
        'POINT (4 14)',
    ]


def test_geoarrow_point_array_uses_packed_coordinate_storage() -> None:
    values = gm.points([1, 3], [2, 4], crs=4326)
    arrow = values.to_arrow()
    recovered = arrow_geometries(arrow)
    assert arrow.type.extension_name == 'geoarrow.point'
    assert arrow.type.storage_type == pa.struct([
        pa.field('x', pa.float64()),
        pa.field('y', pa.float64()),
    ])
    assert arrow.storage.field('x').to_pylist() == [1.0, 3.0]
    assert arrow.storage.field('y').to_pylist() == [2.0, 4.0]
    assert _json.loads(arrow.type.__arrow_ext_serialize__()) == _json.loads(
        _WGS84_PROJJSON_METADATA
    )
    assert isinstance(recovered, gm.GeometryArray)
    assert recovered.crs.to_epsg() == 4326
    assert [(item).to_wkt() for item in list(recovered)] == [
        'POINT (1 2)',
        'POINT (3 4)',
    ]


def test_geoarrow_linestring_array_uses_packed_coordinate_storage() -> None:
    values = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)], crs=4326),
        gm.LineString([(2, 2), (3, 4), (5, 8)], crs=4326),
    ])
    arrow = values.to_arrow()
    recovered = arrow_geometries(arrow)
    assert arrow.type.extension_name == 'geoarrow.linestring'
    assert arrow.type.storage_type == pa.list_(
        pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
    )
    assert arrow.storage.to_pylist() == [
        [{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}],
        [{'x': 2.0, 'y': 2.0}, {'x': 3.0, 'y': 4.0}, {'x': 5.0, 'y': 8.0}],
    ]
    assert _json.loads(arrow.type.__arrow_ext_serialize__()) == _json.loads(
        _WGS84_PROJJSON_METADATA
    )
    assert isinstance(recovered, gm.GeometryArray)
    assert recovered.crs.to_epsg() == 4326
    assert [(item).to_wkt() for item in list(recovered)] == [
        'LINESTRING (0 0, 1 1)',
        'LINESTRING (2 2, 3 4, 5 8)',
    ]


def test_geoarrow_polygon_array_uses_packed_coordinate_storage() -> None:
    values = gm.GeometryArray([
        gm.Polygon([(0, 0), (4, 0), (4, 4), (0, 4)], crs=4326),
        gm.Polygon(
            [(10, 10), (14, 10), (14, 14), (10, 14)],
            [[(11, 11), (12, 11), (12, 12), (11, 12)]],
            crs=4326,
        ),
    ])
    arrow = values.to_arrow()
    recovered = arrow_geometries(arrow)
    assert arrow.type.extension_name == 'geoarrow.polygon'
    assert arrow.type.storage_type == pa.list_(
        pa.list_(pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())]))
    )
    assert arrow.storage.to_pylist() == [
        [
            [
                {'x': 0.0, 'y': 0.0},
                {'x': 4.0, 'y': 0.0},
                {'x': 4.0, 'y': 4.0},
                {'x': 0.0, 'y': 4.0},
                {'x': 0.0, 'y': 0.0},
            ]
        ],
        [
            [
                {'x': 10.0, 'y': 10.0},
                {'x': 14.0, 'y': 10.0},
                {'x': 14.0, 'y': 14.0},
                {'x': 10.0, 'y': 14.0},
                {'x': 10.0, 'y': 10.0},
            ],
            [
                {'x': 11.0, 'y': 11.0},
                {'x': 12.0, 'y': 11.0},
                {'x': 12.0, 'y': 12.0},
                {'x': 11.0, 'y': 12.0},
                {'x': 11.0, 'y': 11.0},
            ],
        ],
    ]
    assert _json.loads(arrow.type.__arrow_ext_serialize__()) == _json.loads(
        _WGS84_PROJJSON_METADATA
    )
    assert isinstance(recovered, gm.GeometryArray)
    assert recovered.crs.to_epsg() == 4326
    assert [(item).to_wkt() for item in list(recovered)] == [
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))',
        'POLYGON ((10 10, 14 10, 14 14, 10 14, 10 10), (11 11, 12 11, 12 12, 11 12, 11 11))',
    ]


def test_geoarrow_polygon_import_matches_packed_storage_twins() -> None:
    packed, _mixed = polygon_storage_twins()
    recovered = arrow_geometries(packed.to_arrow())
    assert isinstance(recovered, gm.GeometryArray)
    assert canon(recovered) == canon(packed)
    assert recovered.to_arrow().type.extension_name == 'geoarrow.polygon'


def test_geoarrow_polygon_chunked_import_concatenates_chunks() -> None:
    packed, _mixed = polygon_storage_twins()
    arrow = packed.to_arrow()
    chunks = pa.chunked_array([arrow.slice(0, 1), arrow.slice(1, 1)])
    recovered = arrow_geometries(chunks)
    assert isinstance(recovered, gm.GeometryArray)
    assert canon(recovered) == canon(packed)


def test_geoarrow_multi_and_polygon_arrays_use_packed_coordinate_storage() -> None:
    coordinate = pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
    cases = [
        (
            gm.GeometryArray([
                gm.MultiPoint([(0, 0), (1, 1)], crs=4326),
                gm.MultiPoint([(2, 2)], crs=4326),
            ]),
            'geoarrow.multipoint',
            pa.list_(coordinate),
            ['MULTIPOINT ((0 0), (1 1))', 'MULTIPOINT ((2 2))'],
        ),
        (
            gm.GeometryArray([
                gm.MultiLineString([[(0, 0), (1, 1)]], crs=4326),
                gm.MultiLineString([[(2, 2), (3, 3)], [(4, 4), (5, 5)]], crs=4326),
            ]),
            'geoarrow.multilinestring',
            pa.list_(pa.list_(coordinate)),
            [
                'MULTILINESTRING ((0 0, 1 1))',
                'MULTILINESTRING ((2 2, 3 3), (4 4, 5 5))',
            ],
        ),
        (
            gm.GeometryArray([
                gm.Polygon([(0, 0), (4, 0), (4, 4), (0, 4)], crs=4326),
                gm.Polygon(
                    [(10, 10), (14, 10), (14, 14), (10, 14)],
                    [[(11, 11), (12, 11), (12, 12), (11, 12)]],
                    crs=4326,
                ),
            ]),
            'geoarrow.polygon',
            pa.list_(pa.list_(coordinate)),
            [
                'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))',
                'POLYGON ((10 10, 14 10, 14 14, 10 14, 10 10), (11 11, 12 11, 12 12, 11 12, 11 11))',
            ],
        ),
        (
            gm.GeometryArray([
                gm.MultiPolygon(
                    [
                        [[(0, 0), (1, 0), (1, 1), (0, 1)]],
                        [[(2, 2), (3, 2), (3, 3), (2, 3)]],
                    ],
                    crs=4326,
                ),
                gm.MultiPolygon([[[(4, 4), (5, 4), (5, 5), (4, 5)]]], crs=4326),
            ]),
            'geoarrow.multipolygon',
            pa.list_(pa.list_(pa.list_(coordinate))),
            [
                'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))',
                'MULTIPOLYGON (((4 4, 5 4, 5 5, 4 5, 4 4)))',
            ],
        ),
    ]
    for values, extension_name, storage_type, expected_wkt in cases:
        arrow = values.to_arrow()
        recovered = arrow_geometries(arrow)
        assert arrow.type.extension_name == extension_name
        assert arrow.type.storage_type == storage_type
        assert _json.loads(arrow.type.__arrow_ext_serialize__()) == _json.loads(
            _WGS84_PROJJSON_METADATA
        )
        assert isinstance(recovered, gm.GeometryArray)
        assert recovered.crs.to_epsg() == 4326
        assert [(item).to_wkt() for item in recovered] == expected_wkt


def test_from_arrow_singleton_slice_yields_an_array() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)], crs=4326),
        gm.LineString([(2, 2), (3, 3), (4, 4)], crs=4326),
        gm.LineString([(5, 5), (6, 6)], crs=4326),
    ]).to_arrow()
    recovered = arrow_geometries(lines.slice(1, 1))
    assert isinstance(recovered, gm.GeometryArray)
    assert len(recovered) == 1
    assert recovered.crs.to_epsg() == 4326
    assert (recovered[0]).to_wkt() == 'LINESTRING (2 2, 3 3, 4 4)'
    epoch_lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)], crs=4326, epoch=2020.0),
        gm.LineString([(2, 2), (3, 3), (4, 4)], crs=4326, epoch=2020.0),
    ]).to_arrow()
    epoch_recovered = arrow_geometries(epoch_lines.slice(1, 1))
    assert isinstance(epoch_recovered, gm.GeometryArray)
    assert len(epoch_recovered) == 1
    assert epoch_recovered.crs.to_epsg() == 4326
    assert epoch_recovered.epoch == 2020.0
    assert epoch_recovered[0].epoch == 2020.0
    assert str(epoch_recovered[0]) == 'LINESTRING (2 2, 3 3, 4 4)'


def test_from_arrow_empty_chunked_and_table_preserve_frame_metadata() -> None:
    values = gm.points([0.0], [1.0], crs=4326, epoch=2020.0)
    empty_chunked = pa.chunked_array([], type=values.to_arrow().type)
    chunked_recovered = arrow_geometries(empty_chunked)
    table_recovered = arrow_geometries(pa.table({'geometry': empty_chunked}))
    assert isinstance(chunked_recovered, gm.GeometryArray)
    assert isinstance(table_recovered, gm.GeometryArray)
    assert len(chunked_recovered) == 0
    assert len(table_recovered) == 0
    assert chunked_recovered.crs.to_epsg() == 4326
    assert table_recovered.crs.to_epsg() == 4326
    assert chunked_recovered.epoch == 2020.0
    assert table_recovered.epoch == 2020.0


def test_from_arrow_zero_batch_reader_roundtrips_to_arrow() -> None:
    """D09: zero-batch framed import keeps CRS and re-exports without CRSMismatchError."""
    a = gm.points([1.0], [2.0], crs=4326).to_arrow()
    reader = pa.RecordBatchReader.from_batches(
        pa.schema([pa.field('geometry', a.type)]), []
    )
    out = arrow_geometries(reader)
    assert len(out) == 0
    assert out.crs.to_epsg() == 4326
    exported = out.to_arrow()
    back = arrow_geometries(exported)
    assert len(back) == 0
    assert back.crs.to_epsg() == 4326
    # Positive: empty array constructed with CRS also re-exports.
    empty = gm.GeometryArray([], crs=4326)
    assert len(empty) == 0
    assert arrow_geometries(empty.to_arrow()).crs.to_epsg() == 4326


def test_from_arrow_binary_view_prefix_and_inline_padding() -> None:
    """m02: BinaryView external prefix + PRESENT inline padding are validated.

    Corrupting an external view's 4-byte prefix to ``BAD!``, or setting unused
    inline-descriptor bytes nonzero, fails PyArrow ``validate(full=True)`` and
    must be rejected by gometry on every frontend. Valid external and inline
    BinaryView still import.
    """
    wkb0 = gm.Point(1.0, 2.0).to_wkb()
    wkb1 = gm.Point(3.0, 4.0).to_wkb()
    assert len(wkb0) > 12  # external (non-inline) views
    good = pa.array([wkb0, wkb1], type=pa.binary_view())
    good.validate(full=True)
    restored = cast('gm.GeometryArray', arrow_geometries(good))
    assert restored.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']

    bufs = good.buffers()
    views = bytearray(bufs[1])
    views[4:8] = b'BAD!'
    bad_prefix = pa.Array.from_buffers(
        pa.binary_view(), 2, [bufs[0], pa.py_buffer(bytes(views)), bufs[2]]
    )
    with pytest.raises(pa.ArrowInvalid):
        bad_prefix.validate(full=True)

    # Inline row with nonzero padding (length=5 "hello").
    inline = pa.array([b'hello'], type=pa.binary_view())
    inline_views = bytearray(inline.buffers()[1])
    inline_views[9:16] = b'\x01' * 7
    ibufs = inline.buffers()
    bad_pad = pa.Array.from_buffers(
        pa.binary_view(),
        1,
        [ibufs[0], pa.py_buffer(bytes(inline_views)), *ibufs[2:]],
    )
    with pytest.raises(pa.ArrowInvalid):
        bad_pad.validate(full=True)

    class CapsuleOnly:
        def __init__(self, arr: object) -> None:
            self._arr = arr

        def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
            return self._arr.__arrow_c_array__(requested_schema)

    class StreamOnly:
        def __init__(self, arr: object) -> None:
            self._table = pa.table({'g': arr})

        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            return self._table.__arrow_c_stream__(requested_schema)

    for source in (
        bad_prefix,
        CapsuleOnly(bad_prefix),
        StreamOnly(bad_prefix),
    ):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            arrow_geometries(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'prefix' in msg or 'binary-view' in msg or 'binary view' in msg

    for source in (bad_pad, CapsuleOnly(bad_pad), StreamOnly(bad_pad)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            arrow_geometries(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'padding' in msg or 'binary-view' in msg or 'binary view' in msg

    # Positive: short inline WKB is not a valid geometry but padding-zero
    # external round-trip of real WKB is already covered by `good` above.
    # Also accept a pure BinaryView of valid WKB via capsule/stream.
    for source in (good, CapsuleOnly(good), StreamOnly(good)):
        out = cast('gm.GeometryArray', arrow_geometries(source))
        assert out.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']


def test_from_arrow_non_monotonic_offsets_rejected_all_frontends() -> None:
    """m01: offset chains must stay ordered across null slots too.

    Exact repro: binary offsets ``[0, L, 0, L]`` with validity ``0x05``, and
    multipoint list offsets ``[0, 2, 0, 2]`` with a middle null — PyArrow
    ``validate(full=True)`` rejects both; gometry must reject on pyarrow /
    capsule / stream, while legitimate null rows with monotonic offsets still
    import.
    """
    wkb = gm.Point(1.0, 2.0).to_wkb()
    L = len(wkb)
    off_bin = pa.array([0, L, 0, L], type=pa.int32()).buffers()[1]
    validity = pa.py_buffer(bytes([0x05]))
    data = pa.py_buffer(wkb)
    bad_bin = pa.Array.from_buffers(
        pa.binary(), 3, [validity, off_bin, data], null_count=1
    )
    with pytest.raises(pa.ArrowInvalid):
        bad_bin.validate(full=True)

    coords = pa.StructArray.from_arrays(
        [
            pa.array([0.0, 1.0, 2.0, 3.0], type=pa.float64()),
            pa.array([0.0, 1.0, 2.0, 3.0], type=pa.float64()),
        ],
        names=['x', 'y'],
    )
    off_list = pa.array([0, 2, 0, 2], type=pa.int32()).buffers()[1]
    mp_type = (
        gm
        .GeometryArray([gm.MultiPoint([(0.0, 0.0), (1.0, 1.0)])], crs=4326)
        .to_arrow()
        .type
    )
    bad_list = pa.ListArray.from_buffers(
        mp_type.storage_type, 3, [validity, off_list], children=[coords]
    )
    with pytest.raises(pa.ArrowInvalid):
        bad_list.validate(full=True)
    bad_mp = pa.ExtensionArray.from_storage(mp_type, bad_list)

    class CapsuleOnly:
        def __init__(self, arr: object) -> None:
            self._arr = arr

        def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
            return self._arr.__arrow_c_array__(requested_schema)

    class StreamOnly:
        def __init__(self, arr: object) -> None:
            self._table = pa.table({'g': arr})

        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            return self._table.__arrow_c_stream__(requested_schema)

    for source in (bad_bin, CapsuleOnly(bad_bin), StreamOnly(bad_bin)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            arrow_geometries(source)
        assert 'PanicException' not in type(ei.value).__name__
        assert 'offset' in str(ei.value).lower() or 'ordered' in str(ei.value).lower()

    for source in (bad_mp, CapsuleOnly(bad_mp), StreamOnly(bad_mp)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            arrow_geometries(source)
        assert 'PanicException' not in type(ei.value).__name__
        assert 'offset' in str(ei.value).lower() or 'ordered' in str(ei.value).lower()

    # Positive: legitimate null row with monotonic offsets still imports.
    good = gm.GeometryArray([gm.Point(1, 2), None, gm.Point(3, 4)], crs=4326)
    for source in (
        good.to_arrow(),
        CapsuleOnly(good.to_arrow()),
        StreamOnly(good.to_arrow()),
    ):
        out = cast('gm.GeometryArray', arrow_geometries(source))
        assert out.is_missing.tolist() == [False, True, False]
        assert out.to_wkt()[0] == 'POINT (1 2)'
        assert out.to_wkt()[2] == 'POINT (3 4)'

    good_mp = gm.GeometryArray(
        [
            gm.MultiPoint([(0.0, 0.0), (1.0, 1.0)]),
            None,
            gm.MultiPoint([(2.0, 2.0), (3.0, 3.0)]),
        ],
        crs=4326,
    )
    restored_mp = cast('gm.GeometryArray', arrow_geometries(good_mp.to_arrow()))
    assert restored_mp.is_missing.tolist() == [False, True, False]
    assert restored_mp.to_wkt()[0] == 'MULTIPOINT ((0 0), (1 1))'


def test_from_arrow_empty_malformed_start_offset_rejected_all_frontends() -> None:
    """D18: length-0 binary still has one start offset that must be non-negative.

    Exact repro: empty Binary with offsets buffer ``[-1]``. PyArrow
    ``validate(full=True)`` rejects; gometry must reject on pyarrow / capsule /
    stream (stream emits an explicit zero-row batch — empty tables emit no
    batches and never expose offsets). Well-formed empty binary and the m09
    many-empty-chunks path still import as length 0.
    """

    class CapsuleOnly:
        def __init__(self, arr: object) -> None:
            self._arr = arr

        def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
            return self._arr.__arrow_c_array__(requested_schema)

    class StreamZeroRow:
        """Stream that yields one zero-row batch (offsets are visible)."""

        def __init__(self, arr: object) -> None:
            self._arr = arr

        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            schema = pa.schema([pa.field('geometry', self._arr.type)])
            batch = pa.RecordBatch.from_arrays([self._arr], schema=schema)
            reader = pa.RecordBatchReader.from_batches(schema, [batch])
            return reader.__arrow_c_stream__(requested_schema)

    bad = pa.Array.from_buffers(
        pa.binary(),
        0,
        [None, pa.array([-1], type=pa.int32()).buffers()[1], pa.py_buffer(b'')],
    )
    with pytest.raises(pa.ArrowInvalid):
        bad.validate(full=True)

    for source in (bad, CapsuleOnly(bad), StreamZeroRow(bad)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            arrow_geometries(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'offset' in msg or 'non-negative' in msg or 'ordered' in msg

    # Positive: well-formed empty binary (offsets == [0]) imports as empty.
    good = pa.array([], type=pa.binary())
    good.validate(full=True)
    for source in (good, CapsuleOnly(good), StreamZeroRow(good)):
        out = cast('gm.GeometryArray', arrow_geometries(source))
        assert len(out) == 0
        assert out.to_wkt() == []

    # Positive: empty large_binary and sliced-empty of a valid array.
    good_large = pa.array([], type=pa.large_binary())
    assert len(cast('gm.GeometryArray', arrow_geometries(good_large))) == 0
    nonempty = pa.array([gm.Point(1, 2).to_wkb()], type=pa.binary())
    sliced_empty = nonempty.slice(1, 0)
    sliced_empty.validate(full=True)
    assert len(cast('gm.GeometryArray', arrow_geometries(sliced_empty))) == 0


def test_from_arrow_nested_offsets_validate_selected_descendant_span() -> None:
    """D17: selected nested offsets reject; sliced-away payload is ignored.

    Ring offsets are ``[0,4,2,2,7,11]`` (4→2), outer offsets are
    ``[0,2,3,5]`` and validity is ``0x05``. The selected ``[0:2]`` parent
    span includes the malformed transition even though row 1 is null, so all
    frontends reject. The ``[1:3]`` slice starts after that transition and all
    frontends import it: a visible-span import must not retain or validate a
    physical child it cannot reach.
    """

    class CapsuleOnly:
        def __init__(self, arr: object) -> None:
            self._arr = arr

        def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
            return self._arr.__arrow_c_array__(requested_schema)

    class StreamOnly:
        def __init__(self, arr: object) -> None:
            self._table = pa.table({'g': arr})

        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            return self._table.__arrow_c_stream__(requested_schema)

    coords = pa.StructArray.from_arrays(
        [
            pa.array([float(i) for i in range(11)], type=pa.float64()),
            pa.array([float(i) for i in range(11)], type=pa.float64()),
        ],
        names=['x', 'y'],
    )
    inner_off = pa.array([0, 4, 2, 2, 7, 11], type=pa.int32()).buffers()[1]
    rings = pa.ListArray.from_buffers(
        pa.list_(pa.struct([('x', pa.float64()), ('y', pa.float64())])),
        5,
        [None, inner_off],
        children=[coords],
    )
    outer_off = pa.array([0, 2, 3, 5], type=pa.int32()).buffers()[1]
    validity = pa.py_buffer(bytes([0x05]))
    poly_type = (
        gm
        .GeometryArray(
            [gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)])],
            crs=4326,
        )
        .to_arrow()
        .type
    )
    poly_storage = pa.ListArray.from_buffers(
        poly_type.storage_type,
        3,
        [validity, outer_off],
        children=[rings],
        null_count=1,
    )
    with pytest.raises(pa.ArrowInvalid):
        poly_storage.validate(full=True)
    bad = pa.ExtensionArray.from_storage(poly_type, poly_storage)
    selected_bad = bad.slice(0, 2)
    with pytest.raises(pa.ArrowInvalid):
        selected_bad.validate(full=True)
    for source in (
        selected_bad,
        CapsuleOnly(selected_bad),
        StreamOnly(selected_bad),
    ):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            arrow_geometries(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'offset' in msg or 'ordered' in msg

    sliced = bad.slice(1, 2)
    with pytest.raises(pa.ArrowInvalid):
        sliced.validate(full=True)
    for source in (sliced, CapsuleOnly(sliced), StreamOnly(sliced)):
        out = cast('gm.GeometryArray', arrow_geometries(source))
        assert out.is_missing.tolist() == [True, False]
        assert out.to_wkt()[1] == (
            'POLYGON ((2 2, 3 3, 4 4, 5 5, 6 6, 2 2), (7 7, 8 8, 9 9, 10 10, 7 7))'
        )

    # Positive: valid 3-row polygon array sliced [1:3] imports correctly.
    good = gm.GeometryArray(
        [
            gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]),
            gm.Polygon([(2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 2.0)]),
            gm.Polygon([(4.0, 4.0), (5.0, 4.0), (5.0, 5.0), (4.0, 4.0)]),
        ],
        crs=4326,
    )
    good_slice = good.to_arrow().slice(1, 2)
    for source in (good_slice, CapsuleOnly(good_slice), StreamOnly(good_slice)):
        out = cast('gm.GeometryArray', arrow_geometries(source))
        assert out.to_wkt() == [
            'POLYGON ((2 2, 3 2, 3 3, 2 2))',
            'POLYGON ((4 4, 5 4, 5 5, 4 4))',
        ]
        assert out.crs.to_epsg() == 4326

    # Positive: nested multipolygon round-trip (all nesting levels).
    mp = gm.GeometryArray(
        [
            gm.MultiPolygon([
                gm.Polygon([(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 0.0)]),
                gm.Polygon([(3.0, 3.0), (4.0, 3.0), (4.0, 4.0), (3.0, 3.0)]),
            ]),
            gm.MultiPolygon([
                gm.Polygon([(5.0, 5.0), (6.0, 5.0), (6.0, 6.0), (5.0, 5.0)])
            ]),
        ],
        crs=4326,
    )
    for source in (
        mp.to_arrow(),
        CapsuleOnly(mp.to_arrow()),
        StreamOnly(mp.to_arrow()),
    ):
        restored = cast('gm.GeometryArray', arrow_geometries(source))
        assert restored.to_wkt() == mp.to_wkt()


def test_from_arrow_terminal_offset_validates_selected_descendant_span() -> None:
    """N2: selected terminal past child length is rejected on every frontend.

    Exact repro: MultiLineString list-of-list with inner offsets ``[0, 2, 100]``
    against only 4 coordinates. PyArrow ``validate(full=True)`` rejects
    (``terminal > child length``). Full input and the second-row slice select
    that terminal and must reject. The first-row slice does not reach it and
    imports: visible-span admission must not scan or copy sibling payload.
    """

    class CapsuleOnly:
        def __init__(self, arr: object) -> None:
            self._arr = arr

        def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
            return self._arr.__arrow_c_array__(requested_schema)

    class StreamOnly:
        """ChunkedArray stream — ``pa.table`` full-validates nested lists and
        would refuse to wrap the forged array; chunked export still streams.
        """

        def __init__(self, arr: object) -> None:
            self._chunks = pa.chunked_array([arr], type=arr.type)  # type: ignore[attr-defined]

        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            return self._chunks.__arrow_c_stream__(requested_schema)

    typ = (
        gm
        .GeometryArray([gm.MultiLineString([[(0.0, 0.0), (1.0, 1.0)]])])
        .to_arrow()
        .type
    )
    coord_type = typ.storage_type.value_type.value_type
    coords = pa.array(
        [
            {'x': 0.0, 'y': 0.0},
            {'x': 1.0, 'y': 1.0},
            {'x': 2.0, 'y': 2.0},
            {'x': 3.0, 'y': 3.0},
        ],
        type=coord_type,
    )
    inner = bytearray(struct.pack('<iii', 0, 2, 4))
    lines = pa.ListArray.from_buffers(
        typ.storage_type.value_type,
        2,
        [None, pa.py_buffer(inner)],
        children=[coords],
    )
    outer = pa.ListArray.from_buffers(
        typ.storage_type,
        2,
        [None, pa.py_buffer(struct.pack('<iii', 0, 1, 2))],
        children=[lines],
    )
    arrx = pa.ExtensionArray.from_storage(typ, outer)
    # Forge terminal past coords length 4 while leaving first line valid.
    inner[8:12] = struct.pack('<i', 100)
    with pytest.raises(pa.ArrowInvalid):
        arrx.storage.validate(full=True)
    selected_bad = arrx.slice(1, 1)
    with pytest.raises(pa.ArrowInvalid):
        selected_bad.storage.validate(full=True)
    for source in (
        arrx,
        CapsuleOnly(arrx),
        StreamOnly(arrx),
        selected_bad,
        CapsuleOnly(selected_bad),
        StreamOnly(selected_bad),
    ):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            arrow_geometries(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert (
            'offset' in msg
            or 'child' in msg
            or 'terminal' in msg
            or 'ordered' in msg
            or 'bound' in msg
        )

    sliced = arrx.slice(0, 1)
    with pytest.raises(pa.ArrowInvalid):
        sliced.storage.validate(full=True)
    for source in (sliced, CapsuleOnly(sliced), StreamOnly(sliced)):
        out = cast('gm.GeometryArray', arrow_geometries(source))
        assert out.to_wkt() == ['MULTILINESTRING ((0 0, 1 1))']

    # Positive: valid nested MultiLineString (full + sliced).
    good = gm.GeometryArray(
        [
            gm.MultiLineString([[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]]),
            gm.MultiLineString([[(4.0, 4.0), (5.0, 5.0)]]),
        ],
        crs=4326,
    )
    good_arr = good.to_arrow()
    for source in (good_arr, CapsuleOnly(good_arr), StreamOnly(good_arr)):
        out = cast('gm.GeometryArray', arrow_geometries(source))
        assert out.to_wkt() == good.to_wkt()
        assert out.crs.to_epsg() == 4326
    good_slice = good_arr.slice(1, 1)
    for source in (good_slice, CapsuleOnly(good_slice), StreamOnly(good_slice)):
        out = cast('gm.GeometryArray', arrow_geometries(source))
        assert out.to_wkt() == ['MULTILINESTRING ((4 4, 5 5))']

    # Positive: nested MultiPolygon round-trip still imports.
    mp = gm.GeometryArray(
        [
            gm.MultiPolygon([
                gm.Polygon([(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 0.0)]),
                gm.Polygon([(3.0, 3.0), (4.0, 3.0), (4.0, 4.0), (3.0, 3.0)]),
            ]),
        ],
        crs=4326,
    )
    restored = cast('gm.GeometryArray', arrow_geometries(mp.to_arrow()))
    assert restored.to_wkt() == mp.to_wkt()


def test_from_arrow_multi_empty_malformed_start_offset_rejected() -> None:
    """N1: multi-chunk total-len-0 must still run D18 per empty chunk.

    Exact repro: ChunkedArray of two empty Binary arrays with start offset
    ``-1``. Single-chunk D18 already rejects; multi-chunk must not bypass.
    Well-formed multi-empty chunks still import as length 0 on all frontends
    that surface chunks (ChunkedArray, ExtensionType empty chunks, table of
    empty geometry, RecordBatchReader.read_all()).
    """

    class CapsuleOnly:
        def __init__(self, arr: object) -> None:
            self._arr = arr

        def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
            return self._arr.__arrow_c_array__(requested_schema)

    class StreamZeroRow:
        def __init__(self, arr: object) -> None:
            self._arr = arr

        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            schema = pa.schema([pa.field('geometry', self._arr.type)])
            batch = pa.RecordBatch.from_arrays([self._arr], schema=schema)
            reader = pa.RecordBatchReader.from_batches(schema, [batch])
            return reader.__arrow_c_stream__(requested_schema)

    bad = pa.Array.from_buffers(
        pa.binary(),
        0,
        [None, pa.py_buffer(struct.pack('<i', -1)), pa.py_buffer(b'')],
    )
    chunks = pa.chunked_array([bad, bad], type=pa.binary())
    assert len(chunks) == 0 and chunks.num_chunks == 2

    for source in (chunks,):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            arrow_geometries(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'offset' in msg or 'non-negative' in msg or 'ordered' in msg

    # Mixed: first good empty, second bad — still reject.
    good_empty = pa.array([], type=pa.binary())
    mixed_bad = pa.chunked_array([good_empty, bad], type=pa.binary())
    with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)):
        arrow_geometries(mixed_bad)

    # ExtensionType multi-empty with forged binary storage under WKB extension.
    wkb_typ = (
        gm
        .GeometryArray([
            gm.Point(0.0, 0.0),
            gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
        ])
        .to_arrow()
        .type
    )
    assert wkb_typ.extension_name == 'geoarrow.wkb'
    assert pa.types.is_binary(wkb_typ.storage_type)
    bad_ext = pa.ExtensionArray.from_storage(wkb_typ, bad)
    ext_chunks = pa.chunked_array([bad_ext, bad_ext], type=wkb_typ)
    with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)):
        arrow_geometries(ext_chunks)

    # Nested MultiPolygon empty chunks with bad outer start offset.
    mp_typ = (
        gm
        .GeometryArray([
            gm.MultiPolygon([
                gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)])
            ])
        ])
        .to_arrow()
        .type
    )
    # Empty multipolygon list: length 0, one start offset.
    empty_mp_storage = pa.ListArray.from_buffers(
        mp_typ.storage_type,
        0,
        [None, pa.py_buffer(struct.pack('<i', -1))],
        children=[
            pa.array([], type=mp_typ.storage_type.value_type),
        ],
    )
    try:
        bad_mp = pa.ExtensionArray.from_storage(mp_typ, empty_mp_storage)
        bad_mp_chunks = pa.chunked_array([bad_mp, bad_mp], type=mp_typ)
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)):
            arrow_geometries(bad_mp_chunks)
    except Exception as exc:  # construction may already reject; either is fine
        if not (
            'offset' in str(exc).lower()
            or isinstance(
                exc, (gm.ParseError, gm.GeometryError, TypeError, pa.ArrowInvalid)
            )
        ):
            raise

    # Positive: well-formed multi-empty binary / extension imports as empty.
    good_chunks = pa.chunked_array([good_empty, good_empty], type=pa.binary())
    for source in (good_chunks,):
        out = cast('gm.GeometryArray', arrow_geometries(source))
        assert len(out) == 0
        assert out.to_wkt() == []

    good_pts = gm.points([1.0], [2.0], crs=4326).to_arrow()
    good_empty_pts = good_pts.slice(0, 0)
    multi_empty_pts = pa.chunked_array(
        [good_empty_pts, good_empty_pts, good_empty_pts], type=good_pts.type
    )
    out_pts = cast('gm.GeometryArray', arrow_geometries(multi_empty_pts))
    assert len(out_pts) == 0
    assert out_pts.to_wkt() == []
    assert out_pts.crs.to_epsg() == 4326

    # Table / read_all path: empty multi-chunk geometry column.
    table = pa.table({'geometry': multi_empty_pts})
    out_table = cast('gm.GeometryArray', arrow_geometries(table))
    assert len(out_table) == 0
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    out_reader = cast('gm.GeometryArray', arrow_geometries(reader))
    assert len(out_reader) == 0

    # Single empty still rejects via D18 (capsule/stream).
    for source in (bad, CapsuleOnly(bad), StreamZeroRow(bad)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)):
            arrow_geometries(source)


def test_from_arrow_pyarrow_discards_zero_length_chunks() -> None:
    """Empty chunks add no rows and preserve CRS around non-empty chunks."""
    arr = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326).to_arrow()
    empty = arr.slice(0, 0)

    multiple_empty = pa.chunked_array([empty, empty], type=arr.type)
    assert multiple_empty.num_chunks == 2
    empty_out = cast('gm.GeometryArray', arrow_geometries(multiple_empty))
    assert len(empty_out) == 0
    assert empty_out.crs.to_epsg() == 4326
    assert empty_out.to_wkt() == []

    mixed = pa.chunked_array([empty, arr, empty], type=arr.type)
    mixed_out = cast('gm.GeometryArray', arrow_geometries(mixed))
    assert mixed_out.to_wkt() == ['POINT (1 3)', 'POINT (2 4)']
    assert mixed_out.crs.to_epsg() == 4326


def test_from_arrow_stream_discards_zero_row_batches() -> None:
    """R04: pure Arrow-C stream drops zero-row batches; only merged result remains.

    Three empty WKB/geoarrow batches must yield one zero-row array (not retain
    three empty storages). Empty batches around a real batch must not appear as
    rows. A large empty generator must not OOM from O(batch-count) retention.
    """
    arr = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326).to_arrow()
    schema = pa.schema([pa.field('geometry', arr.type)])

    def empty_batch() -> object:
        return pa.RecordBatch.from_arrays([arr.slice(0, 0)], schema=schema)

    def pure_stream(batches: list[object]) -> object:
        reader = pa.RecordBatchReader.from_batches(schema, batches)

        class StreamOnly:
            def __arrow_c_stream__(
                self, requested_schema: object | None = None
            ) -> object:
                return reader.__arrow_c_stream__(requested_schema)

        return StreamOnly()

    # Exact repro: three empty batches → zero-row product with CRS.
    three_empty = pure_stream([empty_batch(), empty_batch(), empty_batch()])
    out = cast('gm.GeometryArray', arrow_geometries(three_empty))
    assert len(out) == 0
    assert out.crs.to_epsg() == 4326
    assert out.to_wkt() == []

    # Empty around real data: only the present rows land (empties discarded).
    mixed = pure_stream([
        empty_batch(),
        pa.RecordBatch.from_arrays([arr], schema=schema),
        empty_batch(),
        empty_batch(),
    ])
    mixed_out = cast('gm.GeometryArray', arrow_geometries(mixed))
    assert mixed_out.to_wkt() == ['POINT (1 3)', 'POINT (2 4)']
    assert mixed_out.crs.to_epsg() == 4326

    # Large finite empty generator: discard path stays O(1) retained state.
    # If every empty batch were retained as a native storage, this would allocate
    # tens of thousands of wrapper objects before producing zero rows.
    n_empty = 20_000
    produced = {'n': 0}

    def many_empty() -> object:
        def gen():  # type: ignore[no-untyped-def]
            for _ in range(n_empty):
                produced['n'] += 1
                yield empty_batch()

        reader = pa.RecordBatchReader.from_batches(schema, gen())

        class StreamOnly:
            def __arrow_c_stream__(
                self, requested_schema: object | None = None
            ) -> object:
                return reader.__arrow_c_stream__(requested_schema)

        return StreamOnly()

    large = cast('gm.GeometryArray', arrow_geometries(many_empty()))
    assert produced['n'] == n_empty
    assert len(large) == 0
    assert large.crs.to_epsg() == 4326


def test_geoarrow_outer_empty_polygon_is_polygon_empty() -> None:
    """D06: zero-ring outer list is valid POLYGON EMPTY (not a ring error)."""
    polygon_type = gm.GeometryArray([gm.box(0, 0, 1, 1)]).to_arrow().type
    shell = [
        {'x': 0.0, 'y': 0.0},
        {'x': 1.0, 'y': 0.0},
        {'x': 1.0, 'y': 1.0},
        {'x': 0.0, 'y': 1.0},
        {'x': 0.0, 'y': 0.0},
    ]
    empty = pa.ExtensionArray.from_storage(
        polygon_type,
        pa.array([[], None, [shell], []], type=polygon_type.storage_type),
    )
    recovered = arrow_geometries(empty)
    assert recovered.to_wkt() == [
        'POLYGON EMPTY',
        None,
        'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
        'POLYGON EMPTY',
    ]
    assert recovered.is_missing.tolist() == [False, True, False, False]
    assert recovered.is_simple.tolist() == [True, False, True, True]
    assert recovered.is_valid.tolist() == [True, False, True, True]
    assert recovered[0].coordinate_axes == 'XY'
    assert recovered[[3, 0, 3]].to_wkt() == [
        'POLYGON EMPTY',
        'POLYGON EMPTY',
        'POLYGON EMPTY',
    ]
    restored = pickle.loads(pickle.dumps(recovered, protocol=5))
    assert restored.to_wkt() == recovered.to_wkt()
    assert restored.is_missing.tolist() == [False, True, False, False]


def test_geoarrow_zero_ring_polygon_preserves_xyzm_axes() -> None:
    source = gm.GeometryArray([
        gm.from_wkt('POLYGON ZM ((0 0 1 10, 1 0 2 20, 1 1 3 30, 0 1 4 40, 0 0 1 10))')
    ])
    polygon_type = source.to_arrow().type
    empty = pa.ExtensionArray.from_storage(
        polygon_type,
        pa.array([[], None], type=polygon_type.storage_type),
    )

    recovered = cast('gm.GeometryArray', arrow_geometries(empty))

    assert recovered.to_wkt() == ['POLYGON ZM EMPTY', None]
    assert recovered[0].coordinate_axes == 'XYZM'
    assert recovered.is_missing.tolist() == [False, True]
    assert recovered.is_simple.tolist() == [True, False]
    assert recovered.is_valid.tolist() == [True, False]
    gathered = recovered[[0, 1, 0]]
    assert gathered.to_wkt() == ['POLYGON ZM EMPTY', None, 'POLYGON ZM EMPTY']
    assert gathered.is_missing.tolist() == [False, True, False]
    assert gathered.is_simple.tolist() == [True, False, True]
    assert gathered.is_valid.tolist() == [True, False, True]


def test_from_arrow_accepts_plain_binary_wkb_arrays() -> None:
    arrow = pa.array([(gm.Point(1, 2)).to_wkb()], type=pa.binary())
    recovered = arrow_geometries(arrow)
    assert isinstance(recovered, gm.GeometryArray)
    assert len(recovered) == 1
    assert (recovered[0]).to_wkt() == 'POINT (1 2)'


def test_from_arrow_accepts_table_and_record_batch_geometry_columns() -> None:
    values = gm.GeometryArray([gm.Point(1, 2), gm.Point(3, 4)], crs=4326)
    arrow = values.to_arrow()
    table = pa.table({'id': pa.array([1, 2]), 'geometry': arrow})
    record_batch = pa.record_batch([pa.array([1, 2]), arrow], names=['id', 'geometry'])
    unnamed_geometry_table = pa.table({'geom': arrow})
    ambiguous = pa.table({'left': arrow, 'right': (values).to_wkb()})
    table_recovered = arrow_geometries(table)
    batch_recovered = arrow_geometries(record_batch)
    unnamed_recovered = arrow_geometries(unnamed_geometry_table)
    assert isinstance(table_recovered, gm.GeometryArray)
    assert isinstance(batch_recovered, gm.GeometryArray)
    assert isinstance(unnamed_recovered, gm.GeometryArray)
    assert table_recovered.crs.to_epsg() == 4326
    assert [(item).to_wkt() for item in table_recovered] == [
        'POINT (1 2)',
        'POINT (3 4)',
    ]
    assert [(item).to_wkt() for item in batch_recovered] == [
        'POINT (1 2)',
        'POINT (3 4)',
    ]
    assert [(item).to_wkt() for item in unnamed_recovered] == [
        'POINT (1 2)',
        'POINT (3 4)',
    ]
    recovered = gm.from_arrow(ambiguous)
    assert isinstance(recovered, gm.Features)
    assert recovered.geometries.to_wkt() == values.to_wkt()
    with pytest.raises(ValueError, match='geometry column'):
        gm.from_arrow(pa.table({'left': arrow, 'right': arrow}))
    with pytest.raises(TypeError, match='geometry-like column'):
        arrow_geometries(pa.table({}))
    with pytest.raises(TypeError, match='geometry-like column'):
        arrow_geometries(pa.table({'id': pa.array([1, 2])}))


def test_from_arrow_rejects_duplicate_geometry_column_names() -> None:
    """D08: more than one exact 'geometry' field is ambiguous, never silent first-pick."""
    a = gm.points([1.0], [2.0]).to_arrow()
    b = gm.points([9.0], [8.0]).to_arrow()
    table = pa.Table.from_arrays([a, b], names=['geometry', 'geometry'])
    with pytest.raises(ValueError, match='unique column names'):
        arrow_geometries(table)
    # Positive: single geometry + differently named non-geometry still imports.
    ok = pa.Table.from_arrays(
        [pa.array([1]), a],
        names=['id', 'geometry'],
    )
    recovered = arrow_geometries(ok)
    assert recovered.to_wkt() == ['POINT (1 2)']
    # Positive: ordinary single-geometry table.
    single = pa.Table.from_arrays([a], names=['geometry'])
    assert arrow_geometries(single).to_wkt() == ['POINT (1 2)']


def test_from_arrow_geoarrow_empty_point_and_polygon_and_axes() -> None:
    """D06: NaN POINT EMPTY, outer-list POLYGON EMPTY, Z/M empty multiparts keep axes."""
    from gometry._arrow import _extension_type_from_storage

    xy = pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
    pt = pa.array([{'x': float('nan'), 'y': float('nan')}], type=xy)
    empty_pt = arrow_geometries(
        pa.ExtensionArray.from_storage(
            _extension_type_from_storage(pa, 'geoarrow.point', xy, None, None),
            pt,
        )
    )
    assert empty_pt.to_wkt() == ['POINT EMPTY']
    assert empty_pt[0].coordinate_axes == 'XY'

    # POINT Z EMPTY: all active ordinates NaN
    xyz = pa.struct([
        pa.field('x', pa.float64()),
        pa.field('y', pa.float64()),
        pa.field('z', pa.float64()),
    ])
    pt_z = pa.array(
        [{'x': float('nan'), 'y': float('nan'), 'z': float('nan')}], type=xyz
    )
    empty_pt_z = arrow_geometries(
        pa.ExtensionArray.from_storage(
            _extension_type_from_storage(pa, 'geoarrow.point', xyz, None, None),
            pt_z,
        )
    )
    assert empty_pt_z.to_wkt() == ['POINT Z EMPTY']
    assert empty_pt_z[0].coordinate_axes == 'XYZ'

    # Partial NaN / Inf still rejected
    partial = pa.array([{'x': float('nan'), 'y': 1.0}], type=xy)
    with pytest.raises(ValueError, match='coordinates must be finite'):
        arrow_geometries(
            pa.ExtensionArray.from_storage(
                _extension_type_from_storage(pa, 'geoarrow.point', xy, None, None),
                partial,
            )
        )
    inf_pt = pa.array([{'x': float('inf'), 'y': float('nan')}], type=xy)
    with pytest.raises(ValueError, match='coordinates must be finite'):
        arrow_geometries(
            pa.ExtensionArray.from_storage(
                _extension_type_from_storage(pa, 'geoarrow.point', xy, None, None),
                inf_pt,
            )
        )

    poly = pa.array([[]], type=pa.list_(pa.list_(xy)))
    empty_poly = arrow_geometries(
        pa.ExtensionArray.from_storage(
            _extension_type_from_storage(pa, 'geoarrow.polygon', poly.type, None, None),
            poly,
        )
    )
    assert empty_poly.to_wkt() == ['POLYGON EMPTY']

    # Empty MultiLineString / MultiPolygon keep declared Z/M/ZM axes
    mls = pa.array([[]], type=pa.list_(pa.list_(xyz)))
    empty_mls = arrow_geometries(
        pa.ExtensionArray.from_storage(
            _extension_type_from_storage(
                pa, 'geoarrow.multilinestring', mls.type, None, None
            ),
            mls,
        )
    )
    assert empty_mls.to_wkt() == ['MULTILINESTRING Z EMPTY']
    assert empty_mls[0].coordinate_axes == 'XYZ'

    xyzm = pa.struct([
        pa.field('x', pa.float64()),
        pa.field('y', pa.float64()),
        pa.field('z', pa.float64()),
        pa.field('m', pa.float64()),
    ])
    mp = pa.array([[]], type=pa.list_(pa.list_(pa.list_(xyzm))))
    empty_mp = arrow_geometries(
        pa.ExtensionArray.from_storage(
            _extension_type_from_storage(
                pa, 'geoarrow.multipolygon', mp.type, None, None
            ),
            mp,
        )
    )
    assert empty_mp.to_wkt() == ['MULTIPOLYGON ZM EMPTY']
    assert empty_mp[0].coordinate_axes == 'XYZM'

    # Positive: non-empty XY point still works
    ok = pa.array([{'x': 1.0, 'y': 2.0}], type=xy)
    assert arrow_geometries(
        pa.ExtensionArray.from_storage(
            _extension_type_from_storage(pa, 'geoarrow.point', xy, None, None),
            ok,
        )
    ).to_wkt() == ['POINT (1 2)']


def test_geoarrow_ipc_roundtrip_preserves_registered_extension_crs() -> None:
    cases = [
        (
            gm.points([1, 3], [2, 4], crs=4326),
            'geoarrow.point',
            ['POINT (1 2)', 'POINT (3 4)'],
        ),
        (
            gm.GeometryArray(
                [
                    gm.LineString([(0, 0), (1, 1)]),
                    gm.LineString([(2, 2), (3, 4), (5, 8)]),
                ],
                crs=4326,
            ),
            'geoarrow.linestring',
            ['LINESTRING (0 0, 1 1)', 'LINESTRING (2 2, 3 4, 5 8)'],
        ),
        (
            gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)], crs=4326),
            'geoarrow.polygon',
            [
                'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
                'POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))',
            ],
        ),
        (
            gm.points([1, 3], [2, 4], z=[5, 6], m=[7, 8], crs=4326),
            'geoarrow.point',
            ['POINT ZM (1 2 5 7)', 'POINT ZM (3 4 6 8)'],
        ),
    ]
    for values, extension_name, expected_wkt in cases:
        arrow = values.to_arrow()
        assert arrow.type.extension_name == extension_name
        table = pa.table({'geometry': arrow})
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        restored_table = pa.ipc.open_stream(pa.py_buffer(sink.getvalue())).read_all()
        restored = arrow_geometries(restored_table)
        assert isinstance(restored, gm.GeometryArray)
        assert restored.crs.to_epsg() == 4326
        assert [(item).to_wkt() for item in restored] == expected_wkt


def test_from_arrow_outer_null_parity_pyarrow_capsule_stream() -> None:
    """D04: the same outer-nullable GeoArrow column must behave identically
    through PyArrow object, direct capsule, and stream — missing row preserved,
    never native-only reject.
    """
    from gometry._arrow import _extension_type_from_storage

    c = pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
    storage = pa.array(
        [None, [{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}]],
        type=pa.list_(c),
    )
    typ = _extension_type_from_storage(
        pa, 'geoarrow.linestring', storage.type, None, None
    )
    ext = pa.ExtensionArray.from_storage(typ, storage)

    via_pyarrow = cast('gm.GeometryArray', arrow_geometries(ext))
    assert via_pyarrow.is_missing.tolist() == [True, False]
    assert via_pyarrow.to_wkt()[1] == 'LINESTRING (0 0, 1 1)'

    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return ext.__arrow_c_array__()

    via_capsule = cast('gm.GeometryArray', arrow_geometries(CapsuleOnly()))
    assert via_capsule.is_missing.tolist() == via_pyarrow.is_missing.tolist()
    assert via_capsule.to_wkt() == via_pyarrow.to_wkt()

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.table({'geometry': ext}).__arrow_c_stream__()

    via_stream = cast('gm.GeometryArray', arrow_geometries(StreamOnly()))
    assert via_stream.is_missing.tolist() == via_pyarrow.is_missing.tolist()
    assert via_stream.to_wkt() == via_pyarrow.to_wkt()


def _list_geom_with_hidden_nan_span(
    extension_name: str, *, nested: bool = False
) -> object:
    """Valid Arrow layout: offsets [0,2,4], row0 null with (NaN,NaN) child span,
    row1 present with (0,0),(1,1). Nested=True wraps one list level (MultiLineString).
    """
    from gometry._arrow import _extension_type_from_storage

    nan = float('nan')
    coords = pa.StructArray.from_arrays(
        [pa.array([nan, nan, 0.0, 1.0]), pa.array([nan, nan, 0.0, 1.0])],
        names=['x', 'y'],
    )
    offsets = pa.array([0, 2, 4], type=pa.int32())
    mask = pa.array([True, False])  # first outer row null
    if nested:
        # MultiLineString: outer list of lines; one null outer, one line of two verts.
        # Structure: offsets outer [0,1,2]; inner line offsets cover the same coords.
        # Simpler: outer list offsets [0,1,2] over inner list array with spans.
        inner = pa.ListArray.from_arrays(
            pa.array([0, 2, 4], type=pa.int32()),
            coords,
        )
        storage = pa.ListArray.from_arrays(
            pa.array([0, 1, 2], type=pa.int32()),
            inner,
            mask=mask,
        )
    else:
        storage = pa.ListArray.from_arrays(offsets, coords, mask=mask)
    typ = _extension_type_from_storage(pa, extension_name, storage.type, None, None)
    return pa.ExtensionArray.from_storage(typ, storage)


def _assert_arrow_three_frontend_parity(
    ext: object, expected_wkt: list[str | None]
) -> None:
    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return ext.__arrow_c_array__()  # type: ignore[attr-defined]

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.table({'geometry': ext}).__arrow_c_stream__()

    results = []
    for frontend in (ext, CapsuleOnly(), StreamOnly()):
        restored = cast('gm.GeometryArray', arrow_geometries(frontend))
        results.append(restored.to_wkt())
        assert restored.is_missing.tolist() == [True, False]
    assert results[0] == expected_wkt
    assert results[0] == results[1] == results[2]


def test_from_arrow_hidden_null_nan_span_parity_all_list_encodings() -> None:
    """R06: outer-null child spans with non-finite coords must not reject.

    Layout offsets [0,2,4] + validity [null, valid] + first span (NaN,NaN)*2.
    PyArrow-direct, pure capsule, and pure stream must agree.
    """
    _assert_arrow_three_frontend_parity(
        _list_geom_with_hidden_nan_span('geoarrow.linestring'),
        [None, 'LINESTRING (0 0, 1 1)'],
    )
    _assert_arrow_three_frontend_parity(
        _list_geom_with_hidden_nan_span('geoarrow.multipoint'),
        [None, 'MULTIPOINT ((0 0), (1 1))'],
    )
    _assert_arrow_three_frontend_parity(
        _list_geom_with_hidden_nan_span('geoarrow.multilinestring', nested=True),
        [None, 'MULTILINESTRING ((0 0, 1 1))'],
    )

    # Positive: NaNs in a *present* row still reject (not over-acceptance).
    nan = float('nan')
    coords = pa.StructArray.from_arrays(
        [pa.array([nan, nan]), pa.array([nan, nan])],
        names=['x', 'y'],
    )
    present_nan = pa.ListArray.from_arrays(pa.array([0, 2], type=pa.int32()), coords)
    from gometry._arrow import _extension_type_from_storage

    typ = _extension_type_from_storage(
        pa, 'geoarrow.linestring', present_nan.type, None, None
    )
    ext = pa.ExtensionArray.from_storage(typ, present_nan)
    with pytest.raises(gm.ParseError, match='coordinates must be finite'):
        arrow_geometries(ext)

    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return ext.__arrow_c_array__()

    with pytest.raises(gm.ParseError, match='coordinates must be finite'):
        arrow_geometries(CapsuleOnly())


def _sliced_nested_child_null_geometry(
    extension_name: str,
    nesting: int,
    *,
    large_list: bool,
    fixed_size_coordinates: bool,
    outer_null: bool,
    child_null: bool,
) -> object:
    """A nonzero parent slice whose first row may mask an invalid child span."""
    from gometry._arrow import _extension_type_from_storage

    points_per_geometry = 3 if 'polygon' in extension_name else 2
    values: list[float | None] = []
    for row in range(3):
        for point in range(points_per_geometry):
            if row == 1 and child_null:
                values.extend((None, None))
            else:
                coordinate = float(row * 10 + point)
                values.extend((coordinate, coordinate))
    if fixed_size_coordinates:
        coordinates = pa.FixedSizeListArray.from_arrays(
            pa.array(values, type=pa.float64()), 2
        )
    else:
        coordinates = pa.StructArray.from_arrays(
            [
                pa.array(values[::2], type=pa.float64()),
                pa.array(values[1::2], type=pa.float64()),
            ],
            names=['x', 'y'],
        )

    storage: object = coordinates
    array_type = pa.LargeListArray if large_list else pa.ListArray
    offset_type = pa.int64() if large_list else pa.int32()
    for depth in range(nesting):
        offsets = (
            pa.array(
                [
                    0,
                    points_per_geometry,
                    2 * points_per_geometry,
                    3 * points_per_geometry,
                ],
                type=offset_type,
            )
            if depth == 0
            else pa.array([0, 1, 2, 3], type=offset_type)
        )
        storage = array_type.from_arrays(
            offsets,
            storage,
            mask=pa.array([False, outer_null, False]) if depth == nesting - 1 else None,
        )
    typ = _extension_type_from_storage(pa, extension_name, storage.type, None, None)
    # ``slice(1, 2)`` is essential: parent-relative masks must propagate from
    # a nonzero physical offset, not only from an array that begins at zero.
    return pa.ExtensionArray.from_storage(typ, storage).slice(1, 2)


@pytest.mark.parametrize(
    ('extension_name', 'nesting'),
    [
        ('geoarrow.linestring', 1),
        ('geoarrow.multipoint', 1),
        ('geoarrow.multilinestring', 2),
        ('geoarrow.polygon', 2),
        ('geoarrow.multipolygon', 3),
    ],
)
@pytest.mark.parametrize('large_list', [False, True])
@pytest.mark.parametrize('fixed_size_coordinates', [False, True])
@pytest.mark.parametrize(
    ('outer_null', 'child_null'),
    [(False, False), (False, True), (True, False), (True, True)],
)
def test_from_arrow_ancestor_masks_cover_struct_list_large_list_and_fixed_size_list(
    extension_name: str,
    nesting: int,
    large_list: bool,
    fixed_size_coordinates: bool,
    outer_null: bool,
    child_null: bool,
) -> None:
    """Null validity is relative to every parent, consistently on all lanes."""
    ext = _sliced_nested_child_null_geometry(
        extension_name,
        nesting,
        large_list=large_list,
        fixed_size_coordinates=fixed_size_coordinates,
        outer_null=outer_null,
        child_null=child_null,
    )

    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return ext.__arrow_c_array__()

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.table({'geometry': ext}).__arrow_c_stream__()

    frontends = (ext, CapsuleOnly(), StreamOnly())
    if child_null and not outer_null:
        for frontend in frontends:
            with pytest.raises(
                gm.ParseError,
                match=r'(nested geometry values must not contain nulls|geometry arrays require non-null geometry values)',
            ):
                arrow_geometries(frontend)
        return

    restored = [
        cast('gm.GeometryArray', arrow_geometries(frontend)) for frontend in frontends
    ]
    expected_missing = [outer_null, False]
    assert [value.is_missing.tolist() for value in restored] == [expected_missing] * 3
    assert restored[0].to_wkt() == restored[1].to_wkt() == restored[2].to_wkt()


def test_from_arrow_outer_null_points_and_non_null_positive() -> None:
    """D04 positives: non-null linestrings still import; point outer nulls
    (native capsule) become missing without rejecting the batch.
    """
    from gometry._arrow import _extension_type_from_storage

    c = pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
    dense = pa.array(
        [[{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}]],
        type=pa.list_(c),
    )
    typ = _extension_type_from_storage(
        pa, 'geoarrow.linestring', dense.type, None, None
    )
    dense_ext = pa.ExtensionArray.from_storage(typ, dense)

    class CapsuleDense:
        def __arrow_c_array__(self, requested_schema=None):
            return dense_ext.__arrow_c_array__()

    ok = cast('gm.GeometryArray', arrow_geometries(CapsuleDense()))
    assert ok.is_missing.tolist() == [False]
    assert ok.to_wkt() == ['LINESTRING (0 0, 1 1)']

    # Point outer null via native capsule (packed path falls through to append).
    pts = gm.points([1.0, 9.0], [2.0, 8.0]).to_arrow()
    null_pts = pa.ExtensionArray.from_storage(
        pts.type,
        pa.array([{'x': 1.0, 'y': 2.0}, None], type=pts.type.storage_type),
    )

    class CapsulePts:
        def __arrow_c_array__(self, requested_schema=None):
            return null_pts.__arrow_c_array__()

    out = cast('gm.GeometryArray', arrow_geometries(CapsulePts()))
    assert out.is_missing.tolist() == [False, True]
    assert out.to_wkt()[0] == 'POINT (1 2)'


def test_from_arrow_inner_list_null_rejected_not_empty_member() -> None:
    """D05: GeoArrow permits nulls only at the outer geometry level. An inner
    list/ring null must raise a typed error — never decode as an empty member.
    Exact multilinestring repro from the ingress audit.
    """
    from gometry._arrow import _extension_type_from_storage

    c = pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
    storage = pa.array(
        [[None, [{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}]]],
        type=pa.list_(pa.list_(c)),
    )
    typ = _extension_type_from_storage(
        pa, 'geoarrow.multilinestring', storage.type, None, None
    )
    ext = pa.ExtensionArray.from_storage(typ, storage)
    with pytest.raises(
        gm.ParseError, match='nested geometry values must not contain nulls'
    ):
        arrow_geometries(ext)

    # Capsule frontend must agree (not resurrect via a different path).
    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return ext.__arrow_c_array__()

    with pytest.raises(
        gm.ParseError, match='nested geometry values must not contain nulls'
    ):
        arrow_geometries(CapsuleOnly())


def test_from_arrow_empty_inner_member_not_null_still_imports() -> None:
    """D05 positive: legitimately EMPTY (offset-equal, not null) inner members
    remain legal; outer-only nulls still map to missing rows.
    """
    from gometry._arrow import _extension_type_from_storage

    c = pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
    # Empty list member (not None) + a real line.
    storage = pa.array(
        [[[], [{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}]]],
        type=pa.list_(pa.list_(c)),
    )
    typ = _extension_type_from_storage(
        pa, 'geoarrow.multilinestring', storage.type, None, None
    )
    ext = pa.ExtensionArray.from_storage(typ, storage)
    out = cast('gm.GeometryArray', arrow_geometries(ext))
    assert out.is_missing.tolist() == [False]
    assert out.to_wkt() == ['MULTILINESTRING (EMPTY, (0 0, 1 1))'] or out.to_wkt()[
        0
    ].startswith('MULTILINESTRING')
    # Prefer exact WKT when the empty-member spelling is stable.
    wkt = out.to_wkt()[0]
    assert '0 0' in wkt and '1 1' in wkt
    assert 'EMPTY' in wkt or '()' in wkt

    # Outer geometry null still becomes a missing row (not an inner-null reject).
    outer_null = pa.array(
        [None, [[{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}]]],
        type=pa.list_(pa.list_(c)),
    )
    outer_ext = pa.ExtensionArray.from_storage(
        _extension_type_from_storage(
            pa, 'geoarrow.multilinestring', outer_null.type, None, None
        ),
        outer_null,
    )
    missing = cast('gm.GeometryArray', arrow_geometries(outer_ext))
    assert missing.is_missing.tolist() == [True, False]


def test_from_arrow_parent_struct_nulls_become_missing_capsule_and_stream() -> None:
    """D03: a null on a parent struct/table row must not resurrect the child
    geometry payload. Ancestor validity is OR'd into the selected child mask.
    Exact repro: capsule of a masked StructArray; stream of the same via
    RecordBatch (child already carries transferred nulls).
    """
    pts = gm.points([1.0, 9.0], [2.0, 8.0]).to_arrow()
    root = pa.StructArray.from_arrays(
        [pts],
        fields=[pa.field('geometry', pts.type)],
        mask=pa.array([False, True]),  # row 1 is NULL at the parent
    )

    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return root.__arrow_c_array__()

    out = cast('gm.GeometryArray', arrow_geometries(CapsuleOnly()))
    assert out.is_missing.tolist() == [False, True]
    assert out.to_wkt()[0] == 'POINT (1 2)'
    # Present row only — the deleted parent row must not come back as POINT (9 8).
    assert out[~out.is_missing].to_wkt() == ['POINT (1 2)']

    # Stream path: RecordBatch.from_struct_array transfers parent nulls onto the
    # geometry column; native import must preserve them as missing (not reject).
    batch = pa.RecordBatch.from_struct_array(root)
    assert batch.column(0).is_null().to_pylist() == [False, True]

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
            return reader.__arrow_c_stream__()

    streamed = cast('gm.GeometryArray', arrow_geometries(StreamOnly()))
    assert streamed.is_missing.tolist() == [False, True]
    assert streamed.to_wkt()[0] == 'POINT (1 2)'
    assert streamed[~streamed.is_missing].to_wkt() == ['POINT (1 2)']


def test_from_arrow_parent_struct_nulls_sliced_and_non_null_positive() -> None:
    """D03 positives: non-null parent structs, and sliced parents with nulls,
    still import correctly (no over-rejection).
    """
    pts = gm.points([1.0, 9.0, 3.0], [2.0, 8.0, 4.0]).to_arrow()
    # All-valid parent: full round-trip.
    full = pa.StructArray.from_arrays([pts], fields=[pa.field('geometry', pts.type)])

    class CapsuleFull:
        def __arrow_c_array__(self, requested_schema=None):
            return full.__arrow_c_array__()

    ok = cast('gm.GeometryArray', arrow_geometries(CapsuleFull()))
    assert ok.is_missing.tolist() == [False, False, False]
    assert ok.to_wkt() == ['POINT (1 2)', 'POINT (9 8)', 'POINT (3 4)']

    # Sliced parent with a null in the visible window.
    root = pa.StructArray.from_arrays(
        [pts],
        fields=[pa.field('geometry', pts.type)],
        mask=pa.array([False, True, False]),
    )
    sliced = root.slice(1, 2)  # rows [null, present]

    class CapsuleSliced:
        def __arrow_c_array__(self, requested_schema=None):
            return sliced.__arrow_c_array__()

    out = cast('gm.GeometryArray', arrow_geometries(CapsuleSliced()))
    assert out.is_missing.tolist() == [True, False]
    assert out[~out.is_missing].to_wkt() == ['POINT (3 4)']


def test_from_arrow_geometry_nulls_become_missing_rows() -> None:
    """Geometry-level Arrow validity maps onto first-class missing rows;
    VERTEX-level nulls stay rejected (a coordinate cannot be half-present).
    """
    arrow = pa.array(
        [(gm.Point(0, 0)).to_wkb(), None, (gm.Point(1, 1)).to_wkb()], type=pa.binary()
    )
    assert arrow_geometries(arrow).is_missing.tolist() == [False, True, False]
    chunks = pa.chunked_array([
        pa.array([(gm.Point(0, 0)).to_wkb()], type=pa.binary()),
        pa.array([None], type=pa.binary()),
    ])
    assert arrow_geometries(chunks).is_missing.tolist() == [False, True]
    linestring_type = (
        gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])]).to_arrow().type
    )
    linestring_arrow = pa.ExtensionArray.from_storage(
        linestring_type,
        pa.array(
            [None, [{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}]],
            type=linestring_type.storage_type,
        ),
    )
    assert arrow_geometries(linestring_arrow).is_missing.tolist() == [True, False]
    # vertex-level null: the struct row is valid but its x child is null
    point_type = gm.points([0, 1], [0, 1]).to_arrow().type
    point_arrow = pa.ExtensionArray.from_storage(
        point_type,
        pa.StructArray.from_arrays(
            [pa.array([0.0, None]), pa.array([0.0, 1.0])], names=['x', 'y']
        ),
    )
    with pytest.raises(ValueError, match='null at index 1'):
        arrow_geometries(point_arrow)


def test_from_arrow_rejects_non_finite_packed_coordinates() -> None:
    point_type = gm.points([0, 1], [0, 1]).to_arrow().type
    linestring_type = (
        gm.GeometryArray([gm.LineString([(0, 0), (1, 1)])]).to_arrow().type
    )
    polygon_type = gm.GeometryArray([gm.box(0, 0, 1, 1)]).to_arrow().type
    point_arrow = pa.ExtensionArray.from_storage(
        point_type,
        pa.StructArray.from_arrays(
            [pa.array([0.0, math.nan]), pa.array([0.0, 1.0])], names=['x', 'y']
        ),
    )
    linestring_arrow = pa.ExtensionArray.from_storage(
        linestring_type,
        pa.array(
            [[{'x': 0.0, 'y': 0.0}, {'x': math.inf, 'y': 1.0}]],
            type=linestring_type.storage_type,
        ),
    )
    polygon_arrow = pa.ExtensionArray.from_storage(
        polygon_type,
        pa.array(
            [
                [
                    [
                        {'x': 0.0, 'y': 0.0},
                        {'x': math.nan, 'y': 0.0},
                        {'x': 1.0, 'y': 1.0},
                        {'x': 0.0, 'y': 1.0},
                        {'x': 0.0, 'y': 0.0},
                    ]
                ]
            ],
            type=polygon_type.storage_type,
        ),
    )
    with pytest.raises(ValueError, match='coordinates must be finite'):
        arrow_geometries(point_arrow)
    with pytest.raises(ValueError, match='coordinates must be finite'):
        arrow_geometries(linestring_arrow)
    with pytest.raises(ValueError, match='coordinates must be finite'):
        arrow_geometries(polygon_arrow)


def test_arrow_roundtrip_preserves_coordinate_epoch() -> None:
    points = gm.points([0.0, 1.0], [0.0, 1.0], crs=4326, epoch=2020.0)
    back = cast('gm.GeometryArray', arrow_geometries(points.to_arrow()))
    assert back.crs.to_epsg() == 4326
    assert back.epoch == 2020.0
    assert back[0].epoch == 2020.0
    plain = cast(
        'gm.GeometryArray',
        arrow_geometries(gm.points([0.0], [0.0], crs=4326).to_arrow()),
    )
    assert plain.epoch is None
    mixed = gm.GeometryArray(
        [gm.Point(0, 0), gm.box(0, 0, 1, 1)], crs=4326, epoch=2010.5
    )
    wkb_back = cast('gm.GeometryArray', arrow_geometries(mixed.to_arrow()))
    assert wkb_back.epoch == 2010.5
    assert [item.epoch for item in wkb_back] == [2010.5, 2010.5]


def test_point_outputs_of_mixed_arrays_export_packed_geoarrow() -> None:
    mixed = gm.from_wkt([
        'POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))',
        'LINESTRING (0 0, 2 0)',
    ])
    assert isinstance(mixed, gm.GeometryArray)
    centroids = (mixed).centroid()
    arrow = centroids.to_arrow()
    assert arrow.type.extension_name == 'geoarrow.point'
    restored = cast('gm.GeometryArray', arrow_geometries(arrow))
    assert [(g).to_wkt() for g in restored] == [(g).to_wkt() for g in centroids]
    projected = gm.points([21.0, 22.0], [52.0, 53.0], crs=4326).to_crs(3857)
    assert projected.to_arrow().type.extension_name == 'geoarrow.point'
    assert (
        cast('gm.GeometryArray', arrow_geometries(projected.to_arrow())).crs.to_epsg()
        == 3857
    )


def test_geometry_collections_export_via_wkb_lane_and_chunked_import() -> None:
    collection = gm.GeometryCollection([gm.Point(1, 2), gm.box(0, 0, 1, 1)])
    values = gm.GeometryArray([collection, collection])
    arrow = values.to_arrow()
    assert arrow.type.extension_name == 'geoarrow.wkb'
    assert arrow.storage.to_pylist() == (values).to_wkb()
    chunks = pa.chunked_array([
        pa.array((values).to_wkb()[:1], type=pa.binary()),
        pa.array((values).to_wkb()[1:], type=pa.binary()),
    ])
    restored = cast('gm.GeometryArray', arrow_geometries(chunks))
    assert [(g).to_wkt() for g in restored] == (values).to_wkt()


def test_arrow_c_multirow_binary_and_large_binary_round_trip() -> None:
    wkbs = [gm.Point(i, i).to_wkb() for i in range(5)]
    for arrow_type in (pa.binary(), pa.large_binary()):
        arrow = pa.array(wkbs, type=arrow_type)
        restored = arrow_geometries(arrow)
        assert restored.to_wkt() == [f'POINT ({i} {i})' for i in range(5)]


def test_arrow_c_nonzero_offset_and_sliced_geoarrow_struct() -> None:
    values = gm.points([0, 1, 2, 3], [0, 1, 2, 3], crs=4326)
    arrow = values.to_arrow()
    sliced = arrow.slice(1, 2)
    restored = arrow_geometries(sliced)
    assert restored.to_wkt() == ['POINT (1 1)', 'POINT (2 2)']
    # Coordinate child fields of a sliced GeoArrow struct inherit parent offset
    # so the visible span matches the slice, not the full parent allocation.
    assert len(arrow_geometries(sliced)) == 2
    assert restored.crs is not None
    assert values.crs is not None
    assert restored.crs.same_as(values.crs, mode='exact')


def test_true_nonzero_offset_c_array_capsule_binary() -> None:
    """True nonzero-offset C array capsule (PyArrow slice retains offset;
    gometry re-export always writes offset=0 so must not be the only test).
    """
    wkbs = [gm.Point(i, i).to_wkb() for i in range(5)]
    full = pa.array(wkbs, type=pa.binary())
    sliced = full.slice(1, 3)  # offset=1, length=3 in the C ABI
    # Capsule-only forces owned native Arrow admission with a real offset.
    restored = arrow_geometries(_capsule_only_from_pyarrow(sliced))
    assert restored.to_wkt() == ['POINT (1 1)', 'POINT (2 2)', 'POINT (3 3)']
    # LargeBinary sibling.
    sliced_l = pa.array(wkbs, type=pa.large_binary()).slice(2, 2)
    restored_l = arrow_geometries(_capsule_only_from_pyarrow(sliced_l))
    assert restored_l.to_wkt() == ['POINT (2 2)', 'POINT (3 3)']


def test_arrow_c_null_count_unknown_and_excessive() -> None:
    # Known nulls still import as missing rows through pyarrow.
    # Layout edge cases (null_count=-1 / <-1 / known>length, overflow, struct
    # child bounds, Binary vs LargeBinary widths) are exercised by the pure
    # Rust battery `layout_validation_tests` in src/py/arrow_c/native.rs —
    # pyarrow normalizes those C-ABI fields before they reach from_arrow.
    binary = pa.array(
        [gm.Point(0, 0).to_wkb(), None, gm.Point(1, 1).to_wkb()],
        type=pa.binary(),
    )
    restored = arrow_geometries(binary)
    assert restored.is_missing.tolist() == [False, True, False]


def test_arrow_c_empty_array_with_unknown_null_count_imports() -> None:
    """Empty arrays with null_count=-1 must not require a validity bitmap."""
    empty = pa.array([], type=pa.binary())
    restored = arrow_geometries(empty)
    assert len(restored) == 0


def test_arrow_c_binary_view_wkb_round_trip() -> None:
    """BinaryView storage (geoarrow.wkb / pa.binary_view) through the real
    import path: schema tree + buffer iteration + final copy.
    """
    if not hasattr(pa, 'binary_view'):
        pytest.skip('pyarrow binary_view unavailable')
    wkbs = [gm.Point(i, i).to_wkb() for i in range(3)]
    arrow = pa.array(wkbs, type=pa.binary_view())
    restored = arrow_geometries(arrow)
    assert restored.to_wkt() == [f'POINT ({i} {i})' for i in range(3)]


def test_zero_chunk_binary_view_accepted() -> None:
    """B: zero-chunk BinaryView must share non-empty acceptance (empty GeometryArray)."""
    if not hasattr(pa, 'binary_view'):
        pytest.skip('pyarrow binary_view unavailable')
    empty = arrow_geometries(pa.chunked_array([], type=pa.binary_view()))
    assert len(empty) == 0
    # Capsule-only zero-chunk is not a ChunkedArray; empty BinaryView array
    # still exercises the same type frame acceptance.
    empty2 = arrow_geometries(pa.array([], type=pa.binary_view()))
    assert len(empty2) == 0


def test_zero_chunk_large_list_geoarrow_extension_admits_empty() -> None:
    """LargeList-backed GeoArrow accepts zero chunks as an empty column."""
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')

    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    storage_type = pa.large_list(xy)

    class _GeoarrowLinestring(pa.ExtensionType):
        def __arrow_ext_serialize__(self) -> bytes:
            return b'{}'

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return cls(storage_type)

    ext = _GeoarrowLinestring(storage_type, 'geoarrow.linestring')
    # Zero-chunk extension type (the C zero-chunk frame path).
    zero = pa.chunked_array([], type=ext)
    empty = arrow_geometries(zero)
    assert len(empty) == 0

    # Field-metadata LargeList + zero batches (Table) also admits empty.
    field = pa.field(
        'geometry',
        storage_type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    table = pa.Table.from_batches([], schema=pa.schema([field]))
    empty_t = arrow_geometries(table)
    assert len(empty_t) == 0


def test_geoarrow_closed_3_coord_ring_rejects_like_wkt_wkb() -> None:
    """G1/F2: closed 3-coordinate GeoArrow ring is rejected at admission.

    Shared ``admit_closed_ring`` policy: XY-closed rings need ≥4 vertices
    (MIN_VERTICES_CLOSED). GeoArrow must not admit a short ring WKT/WKB refuse.
    A legal 4-vertex ring still imports and pickles cleanly.
    """
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    # Degenerate closed 3-vertex ring: (0,0), (1,0), (0,0).
    points3 = pa.array([(0.0, 0.0), (1.0, 0.0), (0.0, 0.0)], type=xy)
    rings3 = pa.ListArray.from_arrays(pa.array([0, 3], type=pa.int32()), points3)
    polygons3 = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), rings3)
    field = pa.field(
        'geometry',
        polygons3.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.polygon',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch3 = pa.RecordBatch.from_arrays([polygons3], schema=pa.schema([field]))
    with pytest.raises(
        (gm.ParseError, gm.InvalidGeometryError, TypeError, ValueError),
        match=r'three coordinates|RingTooShort|require at least',
    ):
        arrow_geometries(batch3)

    # Legal triangle (4 verts closed) still round-trips through pickle.
    points4 = pa.array([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)], type=xy)
    rings4 = pa.ListArray.from_arrays(pa.array([0, 4], type=pa.int32()), points4)
    polygons4 = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), rings4)
    field4 = pa.field(
        'geometry',
        polygons4.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.polygon',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    arr4 = arrow_geometries(
        pa.RecordBatch.from_arrays([polygons4], schema=pa.schema([field4]))
    )
    restored = pickle.loads(pickle.dumps(arr4))
    assert len(restored) == 1
    _ = restored.is_valid
    _ = restored.area


def test_capsule_only_binary_large_binary_binary_view_and_empty() -> None:
    """Capsule-only imports for z / Z / vz / empty — must hit native.rs."""
    wkbs = [gm.Point(i, i).to_wkb() for i in range(3)]
    for arrow_type in (pa.binary(), pa.large_binary()):
        restored = arrow_geometries(
            _capsule_only_from_pyarrow(pa.array(wkbs, type=arrow_type))
        )
        assert restored.to_wkt() == [f'POINT ({i} {i})' for i in range(3)]

    if hasattr(pa, 'binary_view'):
        restored = arrow_geometries(
            _capsule_only_from_pyarrow(pa.array(wkbs, type=pa.binary_view()))
        )
        assert restored.to_wkt() == [f'POINT ({i} {i})' for i in range(3)]

    # Empty Binary / LargeBinary / BinaryView — zero-sized buffers may be null.
    for arrow_type in (pa.binary(), pa.large_binary()):
        empty = arrow_geometries(
            _capsule_only_from_pyarrow(pa.array([], type=arrow_type))
        )
        assert len(empty) == 0
    if hasattr(pa, 'binary_view'):
        empty_v = arrow_geometries(
            _capsule_only_from_pyarrow(pa.array([], type=pa.binary_view()))
        )
        assert len(empty_v) == 0


def test_capsule_only_geoarrow_struct_slice_nested_list() -> None:
    """Capsule-only +s (struct point) and nested +l (linestring) + slices."""
    points = gm.points([0, 1, 2, 3], [0, 1, 2, 3], crs=4326)
    restored = arrow_geometries(_capsule_only_from_gometry(points))
    assert restored.to_wkt() == points.to_wkt()
    assert restored.crs is not None
    assert points.crs is not None
    assert restored.crs.same_as(points.crs, mode='exact')

    # Empty WKB capsule path (zero-sized buffers may be null).
    empty_wkb = arrow_geometries(
        _capsule_only_from_pyarrow(pa.array([], type=pa.binary()))
    )
    assert len(empty_wkb) == 0

    sliced = points[1:3]
    restored_s = arrow_geometries(_capsule_only_from_gometry(sliced))
    assert restored_s.to_wkt() == ['POINT (1 1)', 'POINT (2 2)']

    lines = gm.GeometryArray(
        [
            gm.LineString([(0, 0), (1, 1)]),
            gm.LineString([(2, 2), (3, 3), (4, 4)]),
        ],
        crs=4326,
    )
    restored_l = arrow_geometries(_capsule_only_from_gometry(lines))
    assert restored_l.to_wkt() == lines.to_wkt()


def test_capsule_only_large_list_geometry_extension_admits() -> None:
    """Native capsule and PyArrow lanes agree on checked-i64 LargeList input.

    Bare LargeList without a geometry extension remains non-geometry storage.
    """
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')
    coords = pa.array([0.0, 0.0, 1.0, 1.0], type=pa.float64())
    large = pa.LargeListArray.from_arrays(pa.array([0, 2], type=pa.int64()), coords)
    # Bare LargeList (no geometry encoding) is not a geometry storage type.
    with pytest.raises((TypeError, gm.ParseError, ValueError)):
        arrow_geometries(large)

    # Both GeoArrow paths read checked i64 offsets; they must not reinterpret
    # [0, 2] as i32 cells and produce EMPTY/a short line.
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    points = pa.array([(0.0, 0.0), (1.0, 1.0)], type=xy)
    storage = pa.LargeListArray.from_arrays(pa.array([0, 2], type=pa.int64()), points)
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch = pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    admitted = arrow_geometries(batch)
    assert admitted.to_wkt() == ['LINESTRING (0 0, 1 1)']

    class _GeoArrowLinestringType(pa.ExtensionType):
        def __init__(self) -> None:
            super().__init__(storage.type, 'geoarrow.linestring')

        def __arrow_ext_serialize__(self) -> bytes:
            return b'{}'

        @classmethod
        def __arrow_ext_deserialize__(
            cls, storage_type: pa.DataType, serialized: bytes
        ) -> _GeoArrowLinestringType:
            del storage_type, serialized
            return cls()

    extension = _GeoArrowLinestringType()
    native = arrow_geometries(
        _capsule_only_from_pyarrow(pa.ExtensionArray.from_storage(extension, storage))
    )
    assert native.to_wkt() == admitted.to_wkt()


def test_pyarrow_large_list_geometry_extension_admits() -> None:
    """Geoarrow linestring stored as LargeList decodes with checked i64 offsets.

    Offsets [0, 2] as i64 must NOT be misread as i32 cells (which would yield
    EMPTY or a short line). Admission is the GeoArrow SHOULD for LargeList.
    """
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    points = pa.array([(0.0, 0.0), (1.0, 1.0)], type=xy)
    offsets = pa.array([0, 2], type=pa.int64())
    storage = pa.LargeListArray.from_arrays(offsets, points)
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    batch = pa.RecordBatch.from_arrays([storage], schema=pa.schema([field]))
    arr = arrow_geometries(batch)
    assert arr.to_wkt() == ['LINESTRING (0 0, 1 1)']


def test_empty_arrow_c_stream_large_list_geoarrow_admits() -> None:
    """Zero-batch native streams accept GeoArrow LargeList like the direct lane."""
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')
    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    storage_type = pa.large_list(xy)
    field = pa.field(
        'geometry',
        storage_type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    table = pa.Table.from_batches([], schema=pa.schema([field]))

    class _StreamOnly:
        """Force the native empty-stream path (no .type / pyarrow dispatch)."""

        __slots__ = ('_obj',)

        def __init__(self, obj: object) -> None:
            self._obj = obj

        def __arrow_c_stream__(self, requested_schema=None):
            return self._obj.__arrow_c_stream__(requested_schema)

    assert len(arrow_geometries(_StreamOnly(table))) == 0
    # i32 List remains equivalent.
    ok_field = pa.field(
        'geometry',
        pa.list_(xy),
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    ok_table = pa.Table.from_batches([], schema=pa.schema([ok_field]))
    empty = arrow_geometries(_StreamOnly(ok_table))
    assert len(empty) == 0
    empty_wkb = arrow_geometries(
        _StreamOnly(
            pa.Table.from_batches(
                [],
                schema=pa.schema([
                    pa.field(
                        'geometry',
                        pa.binary(),
                        metadata={
                            b'ARROW:extension:name': b'geoarrow.wkb',
                            b'ARROW:extension:metadata': b'{}',
                        },
                    )
                ]),
            )
        )
    )
    assert len(empty_wkb) == 0


def test_multichunk_point_import_matches_single_chunk() -> None:
    """Multi-chunk packed import must equal single-chunk (no chunking-dependent cap)."""
    n = 2_500
    xs = list(range(n))
    ys = [0.0] * n
    single = gm.points(xs, ys).to_arrow()
    mid = n // 2
    multi = pa.chunked_array([single.slice(0, mid), single.slice(mid, n - mid)])
    a = arrow_geometries(single)
    b = arrow_geometries(multi)
    assert a.to_wkt() == b.to_wkt()
    assert len(a) == n


def test_malformed_geoarrow_polygon_offsets_raise_not_panic() -> None:
    """Nested ring offsets that escape the loaded span must raise cleanly.

    polygon offsets [0,2], ring offsets [0,100,4] against 100 coords previously
    panicked in ArrowOrdinateValues::value (index out of bounds).
    """
    from gometry._arrow import GEOARROW_POLYGON, _extension_type_from_storage

    # 100 XY coordinate structs; visible polygon claims 2 rings with a runaway
    # first ring window [0, 100] then [100, 4] (non-monotonic / out of span).
    coords = pa.StructArray.from_arrays(
        [pa.array([float(i) for i in range(100)]), pa.array([0.0] * 100)],
        names=['x', 'y'],
    )
    # Ring offsets: [0, 100, 4] — first ring claims 100 vertices, second is inverted.
    ring_offsets = pa.array([0, 100, 4], type=pa.int32())
    rings = pa.ListArray.from_arrays(ring_offsets, coords)
    poly_offsets = pa.array([0, 2], type=pa.int32())
    polys = pa.ListArray.from_arrays(poly_offsets, rings)
    ext = _extension_type_from_storage(pa, GEOARROW_POLYGON, polys.type, None, None)
    arrow = pa.ExtensionArray.from_storage(ext, polys)
    with pytest.raises(
        (gm.ParseError, TypeError, ValueError, gm.GeometryError),
        match=r'offset|span|range|ordered|vertex|coordinate|buffer',
    ) as exc_info:
        arrow_geometries(arrow)
    # Must not be a PanicException / index out of bounds.
    assert 'PanicException' not in type(exc_info.value).__name__
    assert 'index out of bounds' not in str(exc_info.value).lower()


def test_empty_stream_rejects_bare_non_geometry_types() -> None:
    """Zero-chunk stream must reject the same bare types non-empty rejects."""
    for typ in [
        pa.int64(),
        pa.float64(),
        pa.string(),
        pa.list_(pa.float64()),
        pa.struct([('x', pa.float64()), ('y', pa.float64())]),
    ]:
        table = pa.Table.from_batches([], schema=pa.schema([pa.field('col', typ)]))
        with pytest.raises((TypeError, gm.ParseError, ValueError)):
            arrow_geometries(_StreamOnly(table))


def test_empty_stream_preserves_geometry_field_crs_epoch() -> None:
    """Empty stream CRS/epoch come from the geometry field, not the struct root."""
    meta = (
        gm
        .GeometryArray([gm.Point(0, 0, crs=4326)], epoch=2020.0)
        .to_arrow()
        .type.__arrow_ext_serialize__()
    )
    field = pa.field(
        'geometry',
        pa.binary(),
        metadata={
            b'ARROW:extension:name': b'geoarrow.wkb',
            b'ARROW:extension:metadata': meta,
        },
    )
    table = pa.Table.from_batches([], schema=pa.schema([field]))
    empty = arrow_geometries(_StreamOnly(table))
    assert len(empty) == 0
    assert empty.crs is not None
    assert empty.crs.to_authority() == ('EPSG', '4326')
    assert empty.epoch == 2020.0


def test_stream_table_with_large_list_sibling_imports() -> None:
    """Unrelated large_list sibling must not block geometry column import."""
    if not hasattr(pa, 'large_list'):
        pytest.skip('pyarrow large_list unavailable')
    pts = [gm.Point(0, 0).to_wkb(), gm.Point(1, 1).to_wkb()]
    table = pa.table({
        'geometry': pa.array(pts, type=pa.binary()),
        'extra': pa.array([[1], [2]], type=pa.large_list(pa.int32())),
    })
    direct = arrow_geometries(table)
    stream = arrow_geometries(_StreamOnly(table))
    assert direct.to_wkt() == stream.to_wkt() == ['POINT (0 0)', 'POINT (1 1)']


def test_stream_wrapper_nonempty_wkb_table_imports() -> None:
    """Valid non-empty one-column WKB table must import through stream-only wrapper."""
    pts = [gm.Point(i, i).to_wkb() for i in range(3)]
    table = pa.table({'geometry': pa.array(pts, type=pa.binary())})
    assert (
        arrow_geometries(table).to_wkt()
        == arrow_geometries(_StreamOnly(table)).to_wkt()
    )


def test_zero_chunk_tagged_int64_geoarrow_rejected() -> None:
    """geoarrow.linestring with int64 storage must reject empty and non-empty."""

    class _Ext(pa.ExtensionType):
        def __arrow_ext_serialize__(self) -> bytes:
            return b'{}'

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            return cls(storage_type)

    ext = _Ext(pa.int64(), 'geoarrow.linestring')
    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        arrow_geometries(pa.chunked_array([], type=ext))
    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        arrow_geometries(pa.array([1, 2, 3], type=ext))


class _ArrayOnly:
    """Force the Arrow-C array capsule path (no .type / pyarrow dispatch)."""

    __slots__ = ('_obj',)

    def __init__(self, obj: object) -> None:
        self._obj = obj

    def __arrow_c_array__(self, requested_schema=None):
        return self._obj.__arrow_c_array__(requested_schema)


def _geoarrow_point_field_array(struct_array: pa.StructArray) -> pa.Table:
    field = pa.field(
        'geometry',
        struct_array.type,
        metadata={b'ARROW:extension:name': b'geoarrow.point'},
    )
    return pa.Table.from_arrays([struct_array], schema=pa.schema([field]))


def _geoarrow_point_extension(struct_array: pa.StructArray) -> pa.ExtensionArray:
    from gometry._arrow import GEOARROW_POINT, _extension_type_from_storage

    ext = _extension_type_from_storage(
        pa, GEOARROW_POINT, struct_array.type, None, None
    )
    return pa.ExtensionArray.from_storage(ext, struct_array)


@pytest.mark.parametrize('frontend', ['direct', 'stream', 'array_capsule'])
@pytest.mark.parametrize('empty', [False, True])
@pytest.mark.parametrize('via', ['extension', 'field_metadata'])
def test_geoarrow_int64_point_storage_rejected_every_frontend(
    frontend: str, empty: bool, via: str
) -> None:
    """int64 geoarrow.point must never reinterpret bytes as f64.

    Exact kill: struct<x:int64,y:int64> row (1,2) must REJECT — not import as
    ~(5e-324, 1e-323).
    """
    if empty:
        typ = pa.struct([('x', pa.int64()), ('y', pa.int64())])
        storage = pa.array([], type=typ)
    else:
        storage = pa.StructArray.from_arrays(
            [pa.array([1], type=pa.int64()), pa.array([2], type=pa.int64())],
            names=['x', 'y'],
        )
    if via == 'extension':
        try:
            arrow = _geoarrow_point_extension(storage)
        except Exception:
            # ExtensionType construction may reject non-float storage types.
            return
        table = pa.table({'geometry': arrow})
    else:
        table = _geoarrow_point_field_array(storage)

    if frontend == 'direct':
        target: object = table if via == 'field_metadata' else table.column(0)
    elif frontend == 'stream':
        target = _StreamOnly(table)
    else:
        # Direct capsule on the extension/column array when available.
        col = table.column(0)
        if hasattr(col, 'combine_chunks'):
            col = col.combine_chunks()
        if not hasattr(col, '__arrow_c_array__'):
            pytest.skip('no __arrow_c_array__ on column')
        target = _ArrayOnly(col)

    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        arrow_geometries(target)


@pytest.mark.parametrize('frontend', ['direct', 'stream', 'array_capsule'])
@pytest.mark.parametrize('empty', [False, True])
def test_geoarrow_duplicate_x_point_storage_rejected_every_frontend(
    frontend: str, empty: bool
) -> None:
    """Duplicate ``x`` field must reject — never silently drop the second x.

    Exact kill: struct<x:f64,x:f64,y:f64> row (1,999,2) must not become POINT (1 2).
    """
    if empty:
        typ = pa.struct([('x', pa.float64()), ('x', pa.float64()), ('y', pa.float64())])
        storage = pa.array([], type=typ)
    else:
        storage = pa.StructArray.from_arrays(
            [pa.array([1.0]), pa.array([999.0]), pa.array([2.0])],
            names=['x', 'x', 'y'],
        )
    # Field-metadata path (ExtensionType may collapse duplicate names).
    table = _geoarrow_point_field_array(storage)

    if frontend == 'direct':
        target: object = table
    elif frontend == 'stream':
        target = _StreamOnly(table)
    else:
        col = table.column(0)
        if hasattr(col, 'combine_chunks'):
            col = col.combine_chunks()
        if not hasattr(col, '__arrow_c_array__'):
            pytest.skip('no __arrow_c_array__ on column')
        target = _ArrayOnly(col)

    with pytest.raises((
        TypeError,
        gm.ParseError,
        ValueError,
        KeyError,
        AttributeError,
    )):
        arrow_geometries(target)


@pytest.mark.parametrize('frontend', ['direct', 'stream'])
@pytest.mark.parametrize('empty', [False, True])
def test_unrecognized_extension_name_errors_every_frontend(
    frontend: str, empty: bool
) -> None:
    """If an extension NAME is present it must classify or ERROR — never fall through."""
    field = pa.field(
        'geometry',
        pa.binary(),
        metadata={b'ARROW:extension:name': b'not.geometry'},
    )
    if empty:
        table = pa.Table.from_batches([], schema=pa.schema([field]))
    else:
        wkb = gm.Point(1, 2).to_wkb()
        table = pa.Table.from_arrays(
            [pa.array([wkb], type=pa.binary())], schema=pa.schema([field])
        )
    target: object = table if frontend == 'direct' else _StreamOnly(table)
    with pytest.raises(
        (TypeError, gm.ParseError, ValueError), match=r'geoarrow|extension'
    ):
        arrow_geometries(target)


@pytest.mark.parametrize('frontend', ['direct', 'stream'])
@pytest.mark.parametrize('empty', [False, True])
def test_junk_extension_metadata_without_name_imports_as_bare_wkb(
    frontend: str, empty: bool
) -> None:
    """Bare binary WKB with junk ext-metadata but no name imports everywhere."""
    field = pa.field(
        'geometry',
        pa.binary(),
        metadata={b'ARROW:extension:metadata': b'not-json'},
    )
    if empty:
        table = pa.Table.from_batches([], schema=pa.schema([field]))
        target: object = table if frontend == 'direct' else _StreamOnly(table)
        out = arrow_geometries(target)
        assert len(out) == 0
        return
    wkb = gm.Point(1, 2).to_wkb()
    table = pa.Table.from_arrays(
        [pa.array([wkb], type=pa.binary())], schema=pa.schema([field])
    )
    target = table if frontend == 'direct' else _StreamOnly(table)
    assert arrow_geometries(target).to_wkt() == ['POINT (1 2)']


def test_geoarrow_nested_polygon_int64_coords_rejected() -> None:
    """Nested polygon geoarrow with int64 ordinate leaves must reject."""
    from gometry._arrow import GEOARROW_POLYGON, _extension_type_from_storage

    coords = pa.StructArray.from_arrays(
        [
            pa.array([0, 1, 1, 0, 0], type=pa.int64()),
            pa.array([0, 0, 1, 1, 0], type=pa.int64()),
        ],
        names=['x', 'y'],
    )
    ring_offsets = pa.array([0, 5], type=pa.int32())
    rings = pa.ListArray.from_arrays(ring_offsets, coords)
    poly_offsets = pa.array([0, 1], type=pa.int32())
    polys = pa.ListArray.from_arrays(poly_offsets, rings)
    try:
        ext = _extension_type_from_storage(pa, GEOARROW_POLYGON, polys.type, None, None)
        arrow = pa.ExtensionArray.from_storage(ext, polys)
    except Exception:
        # Construction rejected — also a pass for this invariant.
        return
    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        arrow_geometries(arrow)
    with pytest.raises((TypeError, gm.ParseError, ValueError, AttributeError)):
        arrow_geometries(_StreamOnly(pa.table({'geometry': arrow})))


def _geoarrow_polygon_array(
    points: list[tuple[float, ...]],
    *,
    axes: str = 'xyz',
) -> pa.Array:
    """Build a single-row geoarrow.polygon ListArray over a coordinate struct."""
    if axes == 'xyz':
        coord_type = pa.struct([
            ('x', pa.float64()),
            ('y', pa.float64()),
            ('z', pa.float64()),
        ])
    elif axes == 'xym':
        coord_type = pa.struct([
            ('x', pa.float64()),
            ('y', pa.float64()),
            ('m', pa.float64()),
        ])
    elif axes == 'xyzm':
        coord_type = pa.struct([
            ('x', pa.float64()),
            ('y', pa.float64()),
            ('z', pa.float64()),
            ('m', pa.float64()),
        ])
    else:
        coord_type = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    pts = pa.array(points, type=coord_type)
    rings = pa.ListArray.from_arrays(pa.array([0, len(points)], type=pa.int32()), pts)
    polys = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), rings)
    field = pa.field(
        'geometry',
        polys.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.polygon',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    return pa.RecordBatch.from_arrays([polys], schema=pa.schema([field]))


@pytest.mark.parametrize(
    ('points', 'axes'),
    [
        # Z-unclosed: XY closes (1 vs 9 on Z).
        (
            [(0.0, 0.0, 1.0), (1.0, 0.0, 2.0), (0.0, 1.0, 3.0), (0.0, 0.0, 9.0)],
            'xyz',
        ),
        # M-unclosed.
        (
            [(0.0, 0.0, 1.0), (1.0, 0.0, 2.0), (0.0, 1.0, 3.0), (0.0, 0.0, 9.0)],
            'xym',
        ),
        # ZM-unclosed on Z.
        (
            [
                (0.0, 0.0, 1.0, 4.0),
                (1.0, 0.0, 2.0, 4.0),
                (0.0, 1.0, 3.0, 4.0),
                (0.0, 0.0, 9.0, 4.0),
            ],
            'xyzm',
        ),
        # ZM-unclosed on M.
        (
            [
                (0.0, 0.0, 1.0, 4.0),
                (1.0, 0.0, 2.0, 4.0),
                (0.0, 1.0, 3.0, 4.0),
                (0.0, 0.0, 1.0, 9.0),
            ],
            'xyzm',
        ),
    ],
)
def test_geoarrow_polygon_rejects_active_ordinate_unclosed_rings(
    points: list[tuple[float, ...]],
    axes: str,
) -> None:
    """F4: Z/M/ZM-open GeoArrow rings must reject at import (D05 sister).

    Packed import used to check XY-only closure, admitting trusted state that
    pickle later rejected via same_active_position.
    """
    batch = _geoarrow_polygon_array(points, axes=axes)
    with pytest.raises(gm.ParseError, match=r'closed|active ordinate|ring'):
        arrow_geometries(batch)
    # Capsule frontend (native Arrow-C path).
    storage = batch.column(0)
    field = batch.schema.field(0)
    # Re-wrap as ExtensionArray-like via RecordBatch so metadata sticks;
    # capsule-only uses the same field metadata through table export.
    table = pa.Table.from_batches([batch])
    with pytest.raises(gm.ParseError, match=r'closed|active ordinate|ring'):
        arrow_geometries(_StreamOnly(table))
    del storage, field


def test_geoarrow_closed_3d_polygon_imports_and_pickles() -> None:
    """Genuinely closed 3D GeoArrow polygon still imports and pickle-round-trips."""
    closed = [
        (0.0, 0.0, 1.0),
        (1.0, 0.0, 2.0),
        (0.0, 1.0, 3.0),
        (0.0, 0.0, 1.0),
    ]
    arr = arrow_geometries(_geoarrow_polygon_array(closed, axes='xyz'))
    assert arr.to_wkt() == ['POLYGON Z ((0 0 1, 1 0 2, 0 1 3, 0 0 1))']
    restored = pickle.loads(pickle.dumps(arr))
    assert restored.to_wkt() == arr.to_wkt()
    assert list(restored[0].exterior.coords.z) == [1.0, 2.0, 3.0, 1.0]


def test_geoarrow_xy_closed_2d_polygon_unaffected() -> None:
    """XY 2D closed rings still import on the packed path."""
    closed_2d = [
        (0.0, 0.0),
        (1.0, 0.0),
        (0.0, 1.0),
        (0.0, 0.0),
    ]
    arr = arrow_geometries(_geoarrow_polygon_array(closed_2d, axes='xy'))
    assert arr.to_wkt() == ['POLYGON ((0 0, 1 0, 0 1, 0 0))']
    assert pickle.loads(pickle.dumps(arr)).to_wkt() == arr.to_wkt()
