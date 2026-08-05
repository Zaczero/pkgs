"""Arrow/GeoArrow interop — packed columnar storage, sliced buffers,
extension metadata, IPC roundtrips, and decode strictness.
"""

import io
import json as _json
import math
import struct
import subprocess
import sys
from typing import Any, cast

import gometry as gm
import pyarrow as pa
import pytest
from gometry._lib import CRS as _CRS

from tests._support import canon, polygon_storage_twins

_WGS84_PROJJSON_METADATA = _json.dumps(
    {'crs': _CRS(4326).to_projjson_dict(), 'crs_type': 'projjson'},
    separators=(',', ':'),
).encode()


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

    restored = cast('gm.GeometryArray', gm.from_arrow(arrow_like))
    assert restored.is_missing.tolist() == expected.is_missing.tolist()
    assert restored.to_wkt() == expected.to_wkt()
    assert restored.crs == expected.crs


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
    assert (line).set_crs(None).length == pytest.approx(2**0.5)
    assert list(line.coords.select('XYZM')) == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]
    assert (line).quantize(0).coords.to_nested() == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]
    assert [
        point.coords.to_nested() for point in ((values).set_z(None)).set_m(None)
    ] == [[0.0, 2.0], [1.0, 3.0]]
    assert ((line).set_z(None)).set_m(None).coords.to_nested() == [
        (0.0, 0.0),
        (1.0, 1.0),
    ]
    assert gm.require(values, crs=4326, axes='XYZM').crs == 'EPSG:4326'
    assert (
        gm.require(values.set_z(None).set_m(None), axes='XY').common_coordinate_axes
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
        for point in cast('gm.GeometryArray', gm.from_arrow(values_arrow))
    ] == [[0.0, 2.0, 4.0, 6.0], [1.0, 3.0, 5.0, 7.0]]
    line_back = gm.from_arrow(line_arrow)
    assert isinstance(line_back, gm.GeometryArray)
    assert len(line_back) == 1
    assert line_back[0].coords.to_nested() == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]
    polygon_back = gm.from_arrow(polygon_arrow)
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
    arrow_values = gm.from_arrow(values.to_arrow())
    assert isinstance(arrow_values, gm.GeometryArray)
    assert [point.coords.to_nested() for point in list(arrow_values)] == [
        [0.0, 2.0, 4.0, 6.0],
        [1.0, 3.0, 5.0, 7.0],
    ]
    arrow_line = gm.from_arrow(line.to_arrow())
    assert isinstance(arrow_line, gm.GeometryArray)
    assert len(arrow_line) == 1
    assert arrow_line[0].coords.to_nested() == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]
    mixed_arrow = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 2, z=3)]).to_arrow()
    arrow_mixed = gm.from_arrow(mixed_arrow)
    assert isinstance(arrow_mixed, gm.GeometryArray)
    assert [point.coords.to_nested() for point in list(arrow_mixed)] == [
        [0.0, 0.0],
        [1.0, 2.0, 3.0],
    ]
    assert gm.from_wkb((line).to_wkb()).coords.to_nested() == [
        (0.0, 0.0, 10.0, 20.0),
        (1.0, 1.0, 11.0, 21.0),
    ]


def test_to_arrow_forced_wkb_encoding_roundtrips() -> None:
    values = gm.points([0.0, 1.0], [2.0, 3.0], crs=4326)
    arrow = values.to_arrow(encoding='wkb')
    assert arrow.type.extension_name == 'geoarrow.wkb'
    assert arrow.storage.type == pa.binary()
    restored = cast('gm.GeometryArray', gm.from_arrow(arrow))
    assert canon(restored) == canon(values)
    assert restored.crs == values.crs

    scalar = gm.Point(4.0, 5.0, crs=4326)
    scalar_arrow = scalar.to_arrow(encoding='wkb')
    assert scalar_arrow.type.extension_name == 'geoarrow.wkb'
    scalar_restored = gm.from_arrow(scalar_arrow)
    assert isinstance(scalar_restored, gm.GeometryArray)
    assert canon(scalar_restored) == canon(gm.GeometryArray([scalar]))


def test_nullable_pyarrow_import_preserves_explicit_frame() -> None:
    from gometry._arrow import apply_missing

    values = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
        gm.LineString([(2.0, 2.0), (3.0, 3.0)]),
    ])
    nullable = apply_missing(values.to_arrow(), b'\x01')
    restored = cast('gm.GeometryArray', gm.from_arrow(nullable, crs=4326, epoch=2020.0))
    assert restored.is_missing.tolist() == [False, True]
    assert restored.crs == 'EPSG:4326'
    assert restored.epoch == 2020.0
    assert restored[0].crs == 'EPSG:4326'
    assert restored[0].epoch == 2020.0


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
            gm.from_arrow(source)
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
    from gometry._arrow import apply_missing

    values = gm.GeometryArray([gm.Point(1, 2), gm.Point(3, 4)])
    nullable = apply_missing(values.to_arrow(encoding='wkb'), b'\x01')
    restored = cast('gm.GeometryArray', gm.from_arrow(nullable))
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
            imported = gm.from_arrow(arrow.slice(offset, length))
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
    restored = cast('gm.GeometryArray', gm.from_arrow(values.to_arrow()))
    assert len(restored) == count
    assert restored.to_arrow().storage.field('x').to_pylist() == xs
    assert restored.to_arrow().storage.field('y').to_pylist() == ys


def test_sliced_geoarrow_point_import_respects_coordinate_child_offsets() -> None:
    values = gm.points([0.0, 1.0, 2.0, 3.0], [10.0, 11.0, 12.0, 13.0])
    sliced = values.to_arrow().slice(1, 2)
    storage = sliced.storage
    assert storage.field('x').offset == 1
    assert storage.field('y').offset == 1
    restored = cast('gm.GeometryArray', gm.from_arrow(sliced))
    assert [point.coords.to_nested() for point in restored] == [
        [1.0, 11.0],
        [2.0, 12.0],
    ]


def test_from_arrow_wkb_rejects_ewkb_srid_conflicting_with_extension_crs() -> None:
    from gometry._arrow import to_arrow_wkb

    ewkb = (gm.Point(1, 2, crs=4326)).to_wkb(include_srid=True)
    offsets = struct.pack('<ii', 0, len(ewkb))
    arrow = to_arrow_wkb(offsets, ewkb, 'EPSG:3857', None)
    with pytest.raises(
        gm.CRSMismatchError, match='conflicts with the embedded EWKB SRID'
    ):
        gm.from_arrow(arrow)


def test_geoarrow_wkb_roundtrip_for_scalar_and_array() -> None:
    point = gm.Point(1, 2, crs=4326)
    values = gm.GeometryArray([point, gm.box(0, 0, 1, 1, crs=4326)])
    wkb_values = (values).to_wkb()
    ewkb_values = (values).to_wkb(include_srid=True)
    arrow = (values).to_arrow()
    recovered = gm.from_arrow(arrow)
    assert arrow.type.extension_name == 'geoarrow.wkb'
    assert arrow.type.__arrow_ext_serialize__() == _WGS84_PROJJSON_METADATA
    assert isinstance(recovered, gm.GeometryArray)
    assert recovered.crs == 'EPSG:4326'
    assert [(item).to_wkt() for item in list(recovered)] == [
        'POINT (1 2)',
        'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
    ]
    singleton = gm.from_arrow((point).to_arrow())
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
    assert ewkb_recovered.crs == 'EPSG:4326'
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
        restored = gm.from_arrow(arrow)
        assert arrow.type.extension_name == extension_name
        assert (
            arrow.type.__arrow_ext_serialize__()
            == _json.dumps(
                {
                    'crs': _CRS(4326).to_projjson_dict(),
                    'crs_type': 'projjson',
                    'epoch': 2020.0,
                },
                separators=(',', ':'),
            ).encode()
        )
        assert isinstance(restored, gm.GeometryArray)
        assert restored.crs == 'EPSG:4326'
        assert restored.epoch == 2020.0
        assert [str(g) for g in restored] == [str(g) for g in values]


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
    restored = cast('gm.GeometryArray', gm.from_arrow(_CapsuleStreamOnly(values)))
    assert restored.crs == 'EPSG:4326'
    assert restored.epoch == 2015.5
    assert [str(g) for g in restored] == ['POINT (0 0)', 'POINT (1 1)']
    for export in (
        lambda: pa.array(values, type=pa.binary()),
        lambda: values[0].__arrow_c_array__(pa.binary()),
    ):
        with pytest.raises(TypeError, match='requested_schema is not supported'):
            export()
    assert pa.array(values).type.extension_name == 'geoarrow.point'


def test_arrow_c_array_import_does_not_import_pyarrow() -> None:
    code = "\nimport importlib.abc\nimport sys\n\nimport gometry as gm\n\nclass _CapsuleArrayOnly:\n    def __init__(self, schema, array):\n        self._schema = schema\n        self._array = array\n\n    def __arrow_c_array__(self, requested_schema=None):\n        return (self._schema, self._array)\n\nclass BlockPyArrow(importlib.abc.MetaPathFinder):\n    def find_spec(self, fullname, path=None, target=None):\n        if fullname == 'pyarrow' or fullname.startswith('pyarrow.'):\n            raise AssertionError('pyarrow import attempted')\n        return None\n\nsys.meta_path.insert(0, BlockPyArrow())\nvalues = gm.points([0.0, 1.0], [2.0, 3.0], crs=4326, epoch=2020.0)\nschema, array = values.__arrow_c_array__()\nrestored = gm.from_arrow(_CapsuleArrayOnly(schema, array))\nassert 'pyarrow' not in sys.modules\nassert restored.crs == 'EPSG:4326'\nassert restored.epoch == 2020.0\nassert [str(value) for value in restored] == ['POINT (0 2)', 'POINT (1 3)']\n"
    subprocess.run([sys.executable, '-c', code], check=True)


def test_gometry_arrow_provider_import_does_not_import_pyarrow() -> None:
    code = "\nimport importlib.abc\nimport sys\n\nimport gometry as gm\n\nclass BlockPyArrow(importlib.abc.MetaPathFinder):\n    def find_spec(self, fullname, path=None, target=None):\n        if fullname == 'pyarrow' or fullname.startswith('pyarrow.'):\n            raise AssertionError('pyarrow import attempted')\n        return None\n\nsys.meta_path.insert(0, BlockPyArrow())\nvalues = gm.points([0.0, 1.0], [2.0, 3.0], crs=4326, epoch=2020.0)\ntry:\n    gm.from_arrow(values)\nexcept TypeError as exc:\n    assert 'already decoded' in str(exc)\nelse:\n    raise AssertionError('from_arrow(GeometryArray) should raise TypeError')\nassert 'pyarrow' not in sys.modules\ntry:\n    gm.from_arrow(object())\nexcept TypeError as exc:\n    assert 'expected a GeoArrow-encoded Arrow' in str(exc)\nelse:\n    raise AssertionError('from_arrow(object()) should raise TypeError')\nassert 'pyarrow' not in sys.modules\n"
    subprocess.run([sys.executable, '-c', code], check=True)


class _CapsuleStreamOnly:
    def __init__(self, values: gm.GeometryArray) -> None:
        self._values = values

    def __arrow_c_stream__(self, requested_schema: Any | None = None) -> Any:
        return self._values.__arrow_c_stream__(requested_schema)


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
    restored = cast('gm.GeometryArray', gm.from_arrow(stream_only))
    assert restored.crs == 'EPSG:4326'
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
    recovered = gm.from_arrow(arrow)
    assert arrow.type.extension_name == 'geoarrow.point'
    assert arrow.type.storage_type == pa.struct([
        pa.field('x', pa.float64()),
        pa.field('y', pa.float64()),
    ])
    assert arrow.storage.field('x').to_pylist() == [1.0, 3.0]
    assert arrow.storage.field('y').to_pylist() == [2.0, 4.0]
    assert arrow.type.__arrow_ext_serialize__() == _WGS84_PROJJSON_METADATA
    assert isinstance(recovered, gm.GeometryArray)
    assert recovered.crs == 'EPSG:4326'
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
    recovered = gm.from_arrow(arrow)
    assert arrow.type.extension_name == 'geoarrow.linestring'
    assert arrow.type.storage_type == pa.list_(
        pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
    )
    assert arrow.storage.to_pylist() == [
        [{'x': 0.0, 'y': 0.0}, {'x': 1.0, 'y': 1.0}],
        [{'x': 2.0, 'y': 2.0}, {'x': 3.0, 'y': 4.0}, {'x': 5.0, 'y': 8.0}],
    ]
    assert arrow.type.__arrow_ext_serialize__() == _WGS84_PROJJSON_METADATA
    assert isinstance(recovered, gm.GeometryArray)
    assert recovered.crs == 'EPSG:4326'
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
    recovered = gm.from_arrow(arrow)
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
    assert arrow.type.__arrow_ext_serialize__() == _WGS84_PROJJSON_METADATA
    assert isinstance(recovered, gm.GeometryArray)
    assert recovered.crs == 'EPSG:4326'
    assert [(item).to_wkt() for item in list(recovered)] == [
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))',
        'POLYGON ((10 10, 14 10, 14 14, 10 14, 10 10), (11 11, 12 11, 12 12, 11 12, 11 11))',
    ]


def test_geoarrow_polygon_import_matches_packed_storage_twins() -> None:
    packed, _mixed = polygon_storage_twins()
    recovered = gm.from_arrow(packed.to_arrow())
    assert isinstance(recovered, gm.GeometryArray)
    assert canon(recovered) == canon(packed)
    assert recovered.to_arrow().type.extension_name == 'geoarrow.polygon'


def test_geoarrow_polygon_chunked_import_concatenates_chunks() -> None:
    packed, _mixed = polygon_storage_twins()
    arrow = packed.to_arrow()
    chunks = pa.chunked_array([arrow.slice(0, 1), arrow.slice(1, 1)])
    recovered = gm.from_arrow(chunks)
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
        recovered = gm.from_arrow(arrow)
        assert arrow.type.extension_name == extension_name
        assert arrow.type.storage_type == storage_type
        assert arrow.type.__arrow_ext_serialize__() == _WGS84_PROJJSON_METADATA
        assert isinstance(recovered, gm.GeometryArray)
        assert recovered.crs == 'EPSG:4326'
        assert [(item).to_wkt() for item in recovered] == expected_wkt


def test_from_arrow_singleton_slice_yields_an_array() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)], crs=4326),
        gm.LineString([(2, 2), (3, 3), (4, 4)], crs=4326),
        gm.LineString([(5, 5), (6, 6)], crs=4326),
    ]).to_arrow()
    recovered = gm.from_arrow(lines.slice(1, 1))
    assert isinstance(recovered, gm.GeometryArray)
    assert len(recovered) == 1
    assert recovered.crs == 'EPSG:4326'
    assert (recovered[0]).to_wkt() == 'LINESTRING (2 2, 3 3, 4 4)'
    epoch_lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)], crs=4326, epoch=2020.0),
        gm.LineString([(2, 2), (3, 3), (4, 4)], crs=4326, epoch=2020.0),
    ]).to_arrow()
    epoch_recovered = gm.from_arrow(epoch_lines.slice(1, 1))
    assert isinstance(epoch_recovered, gm.GeometryArray)
    assert len(epoch_recovered) == 1
    assert epoch_recovered.crs == 'EPSG:4326'
    assert epoch_recovered.epoch == 2020.0
    assert epoch_recovered[0].epoch == 2020.0
    assert str(epoch_recovered[0]) == 'LINESTRING (2 2, 3 3, 4 4)'


def test_from_arrow_empty_chunked_and_table_preserve_frame_metadata() -> None:
    values = gm.points([0.0], [1.0], crs=4326, epoch=2020.0)
    empty_chunked = pa.chunked_array([], type=values.to_arrow().type)
    chunked_recovered = gm.from_arrow(empty_chunked)
    table_recovered = gm.from_arrow(pa.table({'geometry': empty_chunked}))
    assert isinstance(chunked_recovered, gm.GeometryArray)
    assert isinstance(table_recovered, gm.GeometryArray)
    assert len(chunked_recovered) == 0
    assert len(table_recovered) == 0
    assert chunked_recovered.crs == 'EPSG:4326'
    assert table_recovered.crs == 'EPSG:4326'
    assert chunked_recovered.epoch == 2020.0
    assert table_recovered.epoch == 2020.0


def test_from_arrow_zero_batch_reader_roundtrips_to_arrow() -> None:
    """D09: zero-batch framed import keeps CRS and re-exports without CRSMismatchError."""
    a = gm.points([1.0], [2.0], crs=4326).to_arrow()
    reader = pa.RecordBatchReader.from_batches(
        pa.schema([pa.field('geometry', a.type)]), []
    )
    out = gm.from_arrow(reader)
    assert len(out) == 0
    assert out.crs == 'EPSG:4326'
    exported = out.to_arrow()
    back = gm.from_arrow(exported)
    assert len(back) == 0
    assert back.crs == 'EPSG:4326'
    # Positive: empty array constructed with CRS also re-exports.
    empty = gm.GeometryArray([], crs=4326)
    assert len(empty) == 0
    assert gm.from_arrow(empty.to_arrow()).crs == 'EPSG:4326'


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
    restored = cast('gm.GeometryArray', gm.from_arrow(good))
    assert restored.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']

    bufs = good.buffers()
    views = bytearray(bufs[1])
    views[4:8] = b'BAD!'
    bad_prefix = pa.Array.from_buffers(
        pa.binary_view(), 2, [bufs[0], pa.py_buffer(bytes(views)), bufs[2]]
    )
    with pytest.raises(Exception):
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
    with pytest.raises(Exception):
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
            gm.from_arrow(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'prefix' in msg or 'binary-view' in msg or 'binary view' in msg

    for source in (bad_pad, CapsuleOnly(bad_pad), StreamOnly(bad_pad)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            gm.from_arrow(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'padding' in msg or 'binary-view' in msg or 'binary view' in msg

    # Positive: short inline WKB is not a valid geometry but padding-zero
    # external round-trip of real WKB is already covered by `good` above.
    # Also accept a pure BinaryView of valid WKB via capsule/stream.
    for source in (good, CapsuleOnly(good), StreamOnly(good)):
        out = cast('gm.GeometryArray', gm.from_arrow(source))
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
    with pytest.raises(Exception):
        bad_bin.validate(full=True)

    coords = pa.StructArray.from_arrays(
        [
            pa.array([0.0, 1.0, 2.0, 3.0], type=pa.float64()),
            pa.array([0.0, 1.0, 2.0, 3.0], type=pa.float64()),
        ],
        names=['x', 'y'],
    )
    off_list = pa.array([0, 2, 0, 2], type=pa.int32()).buffers()[1]
    mp_type = gm.GeometryArray(
        [gm.MultiPoint([(0.0, 0.0), (1.0, 1.0)])], crs=4326
    ).to_arrow().type
    bad_list = pa.ListArray.from_buffers(
        mp_type.storage_type, 3, [validity, off_list], children=[coords]
    )
    with pytest.raises(Exception):
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
            gm.from_arrow(source)
        assert 'PanicException' not in type(ei.value).__name__
        assert 'offset' in str(ei.value).lower() or 'ordered' in str(ei.value).lower()

    for source in (bad_mp, CapsuleOnly(bad_mp), StreamOnly(bad_mp)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            gm.from_arrow(source)
        assert 'PanicException' not in type(ei.value).__name__
        assert 'offset' in str(ei.value).lower() or 'ordered' in str(ei.value).lower()

    # Positive: legitimate null row with monotonic offsets still imports.
    good = gm.GeometryArray(
        [gm.Point(1, 2), None, gm.Point(3, 4)], crs=4326
    )
    for source in (
        good.to_arrow(),
        CapsuleOnly(good.to_arrow()),
        StreamOnly(good.to_arrow()),
    ):
        out = cast('gm.GeometryArray', gm.from_arrow(source))
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
    restored_mp = cast('gm.GeometryArray', gm.from_arrow(good_mp.to_arrow()))
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
    with pytest.raises(Exception):
        bad.validate(full=True)

    for source in (bad, CapsuleOnly(bad), StreamZeroRow(bad)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            gm.from_arrow(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'offset' in msg or 'non-negative' in msg or 'ordered' in msg

    # Positive: well-formed empty binary (offsets == [0]) imports as empty.
    good = pa.array([], type=pa.binary())
    good.validate(full=True)
    for source in (good, CapsuleOnly(good), StreamZeroRow(good)):
        out = cast('gm.GeometryArray', gm.from_arrow(source))
        assert len(out) == 0
        assert out.to_wkt() == []

    # Positive: empty large_binary and sliced-empty of a valid array.
    good_large = pa.array([], type=pa.large_binary())
    assert len(cast('gm.GeometryArray', gm.from_arrow(good_large))) == 0
    nonempty = pa.array([gm.Point(1, 2).to_wkb()], type=pa.binary())
    sliced_empty = nonempty.slice(1, 0)
    sliced_empty.validate(full=True)
    assert len(cast('gm.GeometryArray', gm.from_arrow(sliced_empty))) == 0


def test_from_arrow_nested_nonmono_offsets_under_null_slice_rejected() -> None:
    """D17: non-monotonic inner offsets hidden by null/slice must still reject.

    Exact layout: ring offsets ``[0,4,2,2,7,11]`` (4→2), outer ``[0,2,3,5]``,
    validity ``0x05``, outer sliced ``[1:3]``. Direct PyArrow
    ``validate(full=True)`` rejects; capsule/stream/pyarrow frontends must too.
    A valid sliced polygon array still imports the correct two geometries.
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
    poly_type = gm.GeometryArray(
        [gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)])],
        crs=4326,
    ).to_arrow().type
    poly_storage = pa.ListArray.from_buffers(
        poly_type.storage_type,
        3,
        [validity, outer_off],
        children=[rings],
        null_count=1,
    )
    with pytest.raises(Exception):
        poly_storage.validate(full=True)
    bad = pa.ExtensionArray.from_storage(poly_type, poly_storage)
    sliced = bad.slice(1, 2)
    with pytest.raises(Exception):
        sliced.validate(full=True)

    for source in (sliced, CapsuleOnly(sliced), StreamOnly(sliced)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            gm.from_arrow(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'offset' in msg or 'ordered' in msg

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
        out = cast('gm.GeometryArray', gm.from_arrow(source))
        assert out.to_wkt() == [
            'POLYGON ((2 2, 3 2, 3 3, 2 2))',
            'POLYGON ((4 4, 5 4, 5 5, 4 4))',
        ]
        assert out.crs == 'EPSG:4326'

    # Positive: nested multipolygon round-trip (all nesting levels).
    mp = gm.GeometryArray(
        [
            gm.MultiPolygon(
                [
                    gm.Polygon([(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 0.0)]),
                    gm.Polygon([(3.0, 3.0), (4.0, 3.0), (4.0, 4.0), (3.0, 3.0)]),
                ]
            ),
            gm.MultiPolygon(
                [gm.Polygon([(5.0, 5.0), (6.0, 5.0), (6.0, 6.0), (5.0, 5.0)])]
            ),
        ],
        crs=4326,
    )
    for source in (mp.to_arrow(), CapsuleOnly(mp.to_arrow()), StreamOnly(mp.to_arrow())):
        restored = cast('gm.GeometryArray', gm.from_arrow(source))
        assert restored.to_wkt() == mp.to_wkt()


def test_from_arrow_terminal_offset_past_child_rejected_all_frontends() -> None:
    """N2: list terminal offset past child length is OOB-adjacent and must reject.

    Exact repro: MultiLineString list-of-list with inner offsets ``[0, 2, 100]``
    against only 4 coordinates. PyArrow ``validate(full=True)`` rejects
    (``terminal > child length``); gometry must reject on pyarrow / capsule /
    stream. A valid nested MultiLineString and a valid sliced nested array
    still import correctly; D17/D18 remain covered by sibling tests.
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

    typ = gm.GeometryArray(
        [gm.MultiLineString([[(0.0, 0.0), (1.0, 1.0)]])]
    ).to_arrow().type
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
    with pytest.raises(Exception):
        arrx.storage.validate(full=True)
    sliced = arrx.slice(0, 1)
    with pytest.raises(Exception):
        sliced.storage.validate(full=True)

    for source in (arrx, sliced, CapsuleOnly(sliced), StreamOnly(sliced)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)) as ei:
            gm.from_arrow(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert (
            'offset' in msg
            or 'child' in msg
            or 'terminal' in msg
            or 'ordered' in msg
            or 'bound' in msg
        )

    # Positive: valid nested MultiLineString (full + sliced).
    good = gm.GeometryArray(
        [
            gm.MultiLineString(
                [[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]]
            ),
            gm.MultiLineString([[(4.0, 4.0), (5.0, 5.0)]]),
        ],
        crs=4326,
    )
    good_arr = good.to_arrow()
    for source in (good_arr, CapsuleOnly(good_arr), StreamOnly(good_arr)):
        out = cast('gm.GeometryArray', gm.from_arrow(source))
        assert out.to_wkt() == good.to_wkt()
        assert out.crs == 'EPSG:4326'
    good_slice = good_arr.slice(1, 1)
    for source in (good_slice, CapsuleOnly(good_slice), StreamOnly(good_slice)):
        out = cast('gm.GeometryArray', gm.from_arrow(source))
        assert out.to_wkt() == ['MULTILINESTRING ((4 4, 5 5))']

    # Positive: nested MultiPolygon round-trip still imports.
    mp = gm.GeometryArray(
        [
            gm.MultiPolygon(
                [
                    gm.Polygon([(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 0.0)]),
                    gm.Polygon([(3.0, 3.0), (4.0, 3.0), (4.0, 4.0), (3.0, 3.0)]),
                ]
            ),
        ],
        crs=4326,
    )
    restored = cast('gm.GeometryArray', gm.from_arrow(mp.to_arrow()))
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
            gm.from_arrow(source)
        assert 'PanicException' not in type(ei.value).__name__
        msg = str(ei.value).lower()
        assert 'offset' in msg or 'non-negative' in msg or 'ordered' in msg

    # Mixed: first good empty, second bad — still reject.
    good_empty = pa.array([], type=pa.binary())
    mixed_bad = pa.chunked_array([good_empty, bad], type=pa.binary())
    with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)):
        gm.from_arrow(mixed_bad)

    # ExtensionType multi-empty with forged binary storage under WKB extension.
    wkb_typ = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
    ]).to_arrow().type
    assert wkb_typ.extension_name == 'geoarrow.wkb'
    assert pa.types.is_binary(wkb_typ.storage_type)
    bad_ext = pa.ExtensionArray.from_storage(wkb_typ, bad)
    ext_chunks = pa.chunked_array([bad_ext, bad_ext], type=wkb_typ)
    with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)):
        gm.from_arrow(ext_chunks)

    # Nested MultiPolygon empty chunks with bad outer start offset.
    mp_typ = gm.GeometryArray(
        [gm.MultiPolygon([gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)])])]
    ).to_arrow().type
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
            gm.from_arrow(bad_mp_chunks)
    except Exception as exc:  # construction may already reject; either is fine
        if not (
            'offset' in str(exc).lower()
            or isinstance(exc, (gm.ParseError, gm.GeometryError, TypeError, pa.ArrowInvalid))
        ):
            raise

    # Positive: well-formed multi-empty binary / extension imports as empty.
    good_chunks = pa.chunked_array([good_empty, good_empty], type=pa.binary())
    for source in (good_chunks,):
        out = cast('gm.GeometryArray', gm.from_arrow(source))
        assert len(out) == 0
        assert out.to_wkt() == []

    good_pts = gm.points([1.0], [2.0], crs=4326).to_arrow()
    good_empty_pts = good_pts.slice(0, 0)
    multi_empty_pts = pa.chunked_array(
        [good_empty_pts, good_empty_pts, good_empty_pts], type=good_pts.type
    )
    out_pts = cast('gm.GeometryArray', gm.from_arrow(multi_empty_pts))
    assert len(out_pts) == 0
    assert out_pts.to_wkt() == []
    assert out_pts.crs == 'EPSG:4326'

    # Table / read_all path: empty multi-chunk geometry column.
    table = pa.table({'geometry': multi_empty_pts})
    out_table = cast('gm.GeometryArray', gm.from_arrow(table))
    assert len(out_table) == 0
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    out_reader = cast('gm.GeometryArray', gm.from_arrow(reader))
    assert len(out_reader) == 0

    # Single empty still rejects via D18 (capsule/stream).
    for source in (bad, CapsuleOnly(bad), StreamZeroRow(bad)):
        with pytest.raises((gm.ParseError, gm.GeometryError, TypeError)):
            gm.from_arrow(source)


def test_from_arrow_pyarrow_discards_zero_length_chunks() -> None:
    """m09: PyArrow chunked import discards zero-length chunks (stream parity).

    Many empty chunks must yield a zero-row array without retaining per-empty
    storage. Point empties are offset-free so total ``len==0`` stays O(1) after
    type classification; offset-bearing empties still validate per chunk (N1)
    without retaining storage. Empty chunks around real data must not appear as
    rows; non-empty chunk import stays unchanged.
    """
    import resource
    import time

    arr = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326).to_arrow()
    empty = arr.slice(0, 0)

    # Exact repro shape: many empty chunks → zero rows + CRS preserved, O(1)
    # relative to chunk count (no per-empty retained storage / chunk Vec).
    n_empty = 250_000
    many_empty = pa.chunked_array([empty] * n_empty, type=arr.type)
    assert len(many_empty) == 0 and many_empty.num_chunks == n_empty
    max_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    t0 = time.perf_counter()
    out = cast('gm.GeometryArray', gm.from_arrow(many_empty))
    elapsed = time.perf_counter() - t0
    max_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert len(out) == 0
    assert out.crs == 'EPSG:4326'
    assert out.to_wkt() == []
    # len==0 short-circuit: no O(chunk) walk — must finish well under 0.1s and
    # not grow peak RSS by O(chunk) (~tens of MiB from retaining wrappers).
    assert elapsed < 0.1, f'all-empty from_arrow too slow: {elapsed:.3f}s'
    assert (max_after - max_before) < 4096, (
        f'from_arrow peak RSS grew {max_after - max_before} KiB (expected ~0)'
    )

    # Empty around real data: only present rows land.
    mixed = pa.chunked_array([empty, arr, empty, empty], type=arr.type)
    mixed_out = cast('gm.GeometryArray', gm.from_arrow(mixed))
    assert mixed_out.to_wkt() == ['POINT (1 3)', 'POINT (2 4)']
    assert mixed_out.crs == 'EPSG:4326'

    # Positive: non-empty multi-chunk import unchanged.
    half = arr.slice(0, 1)
    other = arr.slice(1, 1)
    non_empty = pa.chunked_array([half, other], type=arr.type)
    kept = cast('gm.GeometryArray', gm.from_arrow(non_empty))
    assert kept.to_wkt() == ['POINT (1 3)', 'POINT (2 4)']
    assert kept.crs == 'EPSG:4326'


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
            def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
                return reader.__arrow_c_stream__(requested_schema)

        return StreamOnly()

    # Exact repro: three empty batches → zero-row product with CRS.
    three_empty = pure_stream([empty_batch(), empty_batch(), empty_batch()])
    out = cast('gm.GeometryArray', gm.from_arrow(three_empty))
    assert len(out) == 0
    assert out.crs == 'EPSG:4326'
    assert out.to_wkt() == []

    # Empty around real data: only the present rows land (empties discarded).
    mixed = pure_stream([
        empty_batch(),
        pa.RecordBatch.from_arrays([arr], schema=schema),
        empty_batch(),
        empty_batch(),
    ])
    mixed_out = cast('gm.GeometryArray', gm.from_arrow(mixed))
    assert mixed_out.to_wkt() == ['POINT (1 3)', 'POINT (2 4)']
    assert mixed_out.crs == 'EPSG:4326'

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
            def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
                return reader.__arrow_c_stream__(requested_schema)

        return StreamOnly()

    large = cast('gm.GeometryArray', gm.from_arrow(many_empty()))
    assert produced['n'] == n_empty
    assert len(large) == 0
    assert large.crs == 'EPSG:4326'


def test_geoarrow_outer_empty_polygon_is_polygon_empty() -> None:
    """D06: zero-ring outer list is valid POLYGON EMPTY (not a ring error)."""
    polygon_type = gm.GeometryArray([gm.box(0, 0, 1, 1)]).to_arrow().type
    empty = pa.ExtensionArray.from_storage(
        polygon_type, pa.array([[]], type=polygon_type.storage_type)
    )
    recovered = gm.from_arrow(empty)
    assert recovered.to_wkt() == ['POLYGON EMPTY']
    assert recovered[0].coordinate_axes == 'XY'


def test_from_arrow_accepts_plain_binary_wkb_arrays() -> None:
    arrow = pa.array([(gm.Point(1, 2)).to_wkb()], type=pa.binary())
    recovered = gm.from_arrow(arrow)
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
    table_recovered = gm.from_arrow(table)
    batch_recovered = gm.from_arrow(record_batch)
    unnamed_recovered = gm.from_arrow(unnamed_geometry_table)
    assert isinstance(table_recovered, gm.GeometryArray)
    assert isinstance(batch_recovered, gm.GeometryArray)
    assert isinstance(unnamed_recovered, gm.GeometryArray)
    assert table_recovered.crs == 'EPSG:4326'
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
    with pytest.raises(TypeError, match='multiple geometry-like columns'):
        gm.from_arrow(ambiguous)
    with pytest.raises(TypeError, match='no columns'):
        gm.from_arrow(pa.table({}))
    with pytest.raises(TypeError, match='geometry-like column'):
        gm.from_arrow(pa.table({'id': pa.array([1, 2])}))


def test_from_arrow_rejects_duplicate_geometry_column_names() -> None:
    """D08: more than one exact 'geometry' field is ambiguous, never silent first-pick."""
    a = gm.points([1.0], [2.0]).to_arrow()
    b = gm.points([9.0], [8.0]).to_arrow()
    table = pa.Table.from_arrays([a, b], names=['geometry', 'geometry'])
    with pytest.raises(TypeError, match="multiple columns named 'geometry'"):
        gm.from_arrow(table)
    # Positive: single geometry + differently named non-geometry still imports.
    ok = pa.Table.from_arrays(
        [pa.array([1]), a],
        names=['id', 'geometry'],
    )
    recovered = gm.from_arrow(ok)
    assert recovered.to_wkt() == ['POINT (1 2)']
    # Positive: ordinary single-geometry table.
    single = pa.Table.from_arrays([a], names=['geometry'])
    assert gm.from_arrow(single).to_wkt() == ['POINT (1 2)']


def test_from_arrow_geoarrow_empty_point_and_polygon_and_axes() -> None:
    """D06: NaN POINT EMPTY, outer-list POLYGON EMPTY, Z/M empty multiparts keep axes."""
    from gometry._arrow import _extension_type_from_storage

    xy = pa.struct([pa.field('x', pa.float64()), pa.field('y', pa.float64())])
    pt = pa.array([{'x': float('nan'), 'y': float('nan')}], type=xy)
    empty_pt = gm.from_arrow(
        pa.ExtensionArray.from_storage(
            _extension_type_from_storage(pa, 'geoarrow.point', xy, None, None),
            pt,
        )
    )
    assert empty_pt.to_wkt() == ['POINT EMPTY']
    assert empty_pt[0].coordinate_axes == 'XY'

    # POINT Z EMPTY: all active ordinates NaN
    xyz = pa.struct(
        [
            pa.field('x', pa.float64()),
            pa.field('y', pa.float64()),
            pa.field('z', pa.float64()),
        ]
    )
    pt_z = pa.array(
        [{'x': float('nan'), 'y': float('nan'), 'z': float('nan')}], type=xyz
    )
    empty_pt_z = gm.from_arrow(
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
        gm.from_arrow(
            pa.ExtensionArray.from_storage(
                _extension_type_from_storage(pa, 'geoarrow.point', xy, None, None),
                partial,
            )
        )
    inf_pt = pa.array([{'x': float('inf'), 'y': float('nan')}], type=xy)
    with pytest.raises(ValueError, match='coordinates must be finite'):
        gm.from_arrow(
            pa.ExtensionArray.from_storage(
                _extension_type_from_storage(pa, 'geoarrow.point', xy, None, None),
                inf_pt,
            )
        )

    poly = pa.array([[]], type=pa.list_(pa.list_(xy)))
    empty_poly = gm.from_arrow(
        pa.ExtensionArray.from_storage(
            _extension_type_from_storage(pa, 'geoarrow.polygon', poly.type, None, None),
            poly,
        )
    )
    assert empty_poly.to_wkt() == ['POLYGON EMPTY']

    # Empty MultiLineString / MultiPolygon keep declared Z/M/ZM axes
    mls = pa.array([[]], type=pa.list_(pa.list_(xyz)))
    empty_mls = gm.from_arrow(
        pa.ExtensionArray.from_storage(
            _extension_type_from_storage(
                pa, 'geoarrow.multilinestring', mls.type, None, None
            ),
            mls,
        )
    )
    assert empty_mls.to_wkt() == ['MULTILINESTRING Z EMPTY']
    assert empty_mls[0].coordinate_axes == 'XYZ'

    xyzm = pa.struct(
        [
            pa.field('x', pa.float64()),
            pa.field('y', pa.float64()),
            pa.field('z', pa.float64()),
            pa.field('m', pa.float64()),
        ]
    )
    mp = pa.array([[]], type=pa.list_(pa.list_(pa.list_(xyzm))))
    empty_mp = gm.from_arrow(
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
    assert gm.from_arrow(
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
        restored = gm.from_arrow(restored_table)
        assert isinstance(restored, gm.GeometryArray)
        assert restored.crs == 'EPSG:4326'
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
    typ = _extension_type_from_storage(pa, 'geoarrow.linestring', storage.type, None, None)
    ext = pa.ExtensionArray.from_storage(typ, storage)

    via_pyarrow = cast('gm.GeometryArray', gm.from_arrow(ext))
    assert via_pyarrow.is_missing.tolist() == [True, False]
    assert via_pyarrow.to_wkt()[1] == 'LINESTRING (0 0, 1 1)'

    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return ext.__arrow_c_array__()

    via_capsule = cast('gm.GeometryArray', gm.from_arrow(CapsuleOnly()))
    assert via_capsule.is_missing.tolist() == via_pyarrow.is_missing.tolist()
    assert via_capsule.to_wkt() == via_pyarrow.to_wkt()

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.table({'geometry': ext}).__arrow_c_stream__()

    via_stream = cast('gm.GeometryArray', gm.from_arrow(StreamOnly()))
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
    typ = _extension_type_from_storage(
        pa, extension_name, storage.type, None, None
    )
    return pa.ExtensionArray.from_storage(typ, storage)


def _assert_arrow_three_frontend_parity(ext: object, expected_wkt: list[str | None]) -> None:
    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return ext.__arrow_c_array__()  # type: ignore[attr-defined]

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.table({'geometry': ext}).__arrow_c_stream__()

    results = []
    for frontend in (ext, CapsuleOnly(), StreamOnly()):
        restored = cast('gm.GeometryArray', gm.from_arrow(frontend))
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
    present_nan = pa.ListArray.from_arrays(
        pa.array([0, 2], type=pa.int32()), coords
    )
    from gometry._arrow import _extension_type_from_storage

    typ = _extension_type_from_storage(
        pa, 'geoarrow.linestring', present_nan.type, None, None
    )
    ext = pa.ExtensionArray.from_storage(typ, present_nan)
    with pytest.raises(gm.ParseError, match='coordinates must be finite'):
        gm.from_arrow(ext)

    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return ext.__arrow_c_array__()

    with pytest.raises(gm.ParseError, match='coordinates must be finite'):
        gm.from_arrow(CapsuleOnly())


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
    typ = _extension_type_from_storage(pa, 'geoarrow.linestring', dense.type, None, None)
    dense_ext = pa.ExtensionArray.from_storage(typ, dense)

    class CapsuleDense:
        def __arrow_c_array__(self, requested_schema=None):
            return dense_ext.__arrow_c_array__()

    ok = cast('gm.GeometryArray', gm.from_arrow(CapsuleDense()))
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

    out = cast('gm.GeometryArray', gm.from_arrow(CapsulePts()))
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
    with pytest.raises(gm.ParseError, match='nested geometry values must not contain nulls'):
        gm.from_arrow(ext)

    # Capsule frontend must agree (not resurrect via a different path).
    class CapsuleOnly:
        def __arrow_c_array__(self, requested_schema=None):
            return ext.__arrow_c_array__()

    with pytest.raises(gm.ParseError, match='nested geometry values must not contain nulls'):
        gm.from_arrow(CapsuleOnly())


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
    out = cast('gm.GeometryArray', gm.from_arrow(ext))
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
    missing = cast('gm.GeometryArray', gm.from_arrow(outer_ext))
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

    out = cast('gm.GeometryArray', gm.from_arrow(CapsuleOnly()))
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

    streamed = cast('gm.GeometryArray', gm.from_arrow(StreamOnly()))
    assert streamed.is_missing.tolist() == [False, True]
    assert streamed.to_wkt()[0] == 'POINT (1 2)'
    assert streamed[~streamed.is_missing].to_wkt() == ['POINT (1 2)']


def test_from_arrow_parent_struct_nulls_sliced_and_non_null_positive() -> None:
    """D03 positives: non-null parent structs, and sliced parents with nulls,
    still import correctly (no over-rejection).
    """
    pts = gm.points([1.0, 9.0, 3.0], [2.0, 8.0, 4.0]).to_arrow()
    # All-valid parent: full round-trip.
    full = pa.StructArray.from_arrays(
        [pts], fields=[pa.field('geometry', pts.type)]
    )

    class CapsuleFull:
        def __arrow_c_array__(self, requested_schema=None):
            return full.__arrow_c_array__()

    ok = cast('gm.GeometryArray', gm.from_arrow(CapsuleFull()))
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

    out = cast('gm.GeometryArray', gm.from_arrow(CapsuleSliced()))
    assert out.is_missing.tolist() == [True, False]
    assert out[~out.is_missing].to_wkt() == ['POINT (3 4)']


def test_from_arrow_geometry_nulls_become_missing_rows() -> None:
    """Geometry-level Arrow validity maps onto first-class missing rows;
    VERTEX-level nulls stay rejected (a coordinate cannot be half-present).
    """
    arrow = pa.array(
        [(gm.Point(0, 0)).to_wkb(), None, (gm.Point(1, 1)).to_wkb()], type=pa.binary()
    )
    assert gm.from_arrow(arrow).is_missing.tolist() == [False, True, False]
    chunks = pa.chunked_array([
        pa.array([(gm.Point(0, 0)).to_wkb()], type=pa.binary()),
        pa.array([None], type=pa.binary()),
    ])
    assert gm.from_arrow(chunks).is_missing.tolist() == [False, True]
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
    assert gm.from_arrow(linestring_arrow).is_missing.tolist() == [True, False]
    # vertex-level null: the struct row is valid but its x child is null
    point_type = gm.points([0, 1], [0, 1]).to_arrow().type
    point_arrow = pa.ExtensionArray.from_storage(
        point_type,
        pa.StructArray.from_arrays(
            [pa.array([0.0, None]), pa.array([0.0, 1.0])], names=['x', 'y']
        ),
    )
    with pytest.raises(ValueError, match='null at index 1'):
        gm.from_arrow(point_arrow)


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
        gm.from_arrow(point_arrow)
    with pytest.raises(ValueError, match='coordinates must be finite'):
        gm.from_arrow(linestring_arrow)
    with pytest.raises(ValueError, match='coordinates must be finite'):
        gm.from_arrow(polygon_arrow)


def test_arrow_roundtrip_preserves_coordinate_epoch() -> None:
    points = gm.points([0.0, 1.0], [0.0, 1.0], crs=4326, epoch=2020.0)
    back = cast('gm.GeometryArray', gm.from_arrow(points.to_arrow()))
    assert back.crs == 'EPSG:4326'
    assert back.epoch == 2020.0
    assert back[0].epoch == 2020.0
    plain = cast(
        'gm.GeometryArray',
        gm.from_arrow(gm.points([0.0], [0.0], crs=4326).to_arrow()),
    )
    assert plain.epoch is None
    mixed = gm.GeometryArray(
        [gm.Point(0, 0), gm.box(0, 0, 1, 1)], crs=4326, epoch=2010.5
    )
    wkb_back = cast('gm.GeometryArray', gm.from_arrow(mixed.to_arrow()))
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
    restored = cast('gm.GeometryArray', gm.from_arrow(arrow))
    assert [(g).to_wkt() for g in restored] == [(g).to_wkt() for g in centroids]
    projected = gm.points([21.0, 22.0], [52.0, 53.0], crs=4326).to_crs(3857)
    assert projected.to_arrow().type.extension_name == 'geoarrow.point'
    assert (
        cast('gm.GeometryArray', gm.from_arrow(projected.to_arrow())).crs == 'EPSG:3857'
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
    restored = cast('gm.GeometryArray', gm.from_arrow(chunks))
    assert [(g).to_wkt() for g in restored] == (values).to_wkt()
