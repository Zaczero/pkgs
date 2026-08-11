"""GeoArrow extension-metadata boundary rules: malformed metadata tagging,
crs_type/edges validation, epoch strictness, and the install-extra error.
"""

import json as _json
import math
import pickle
import subprocess
import sys
from typing import Any, cast

import gometry as gm
import pytest

pa = pytest.importorskip('pyarrow')


def test_geoarrow_extension_types_and_arrays_pickle_round_trip() -> None:
    values = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326, epoch=2020.0)
    arrow = values.to_arrow()
    restored_type = pickle.loads(pickle.dumps(arrow.type))
    restored_array = pickle.loads(pickle.dumps(arrow))

    assert restored_type == arrow.type
    assert restored_array == arrow
    assert restored_array.type == arrow.type
    assert gm.from_arrow(restored_array).crs == 'EPSG:4326'
    assert gm.from_arrow(restored_array).epoch == 2020.0


@pytest.mark.parametrize(
    ('metadata', 'match'),
    [
        pytest.param(b'{bad', 'invalid GeoArrow extension metadata', id='invalid-json'),
        pytest.param(
            b'{"crs": 123}', 'crs must be a string or object', id='wrong-crs-type'
        ),
        pytest.param(
            b'{"crs": "EPSG:NOPE"}', 'invalid GeoArrow extension metadata', id='bad-crs'
        ),
        pytest.param(
            b'{"epoch": "2020"}', 'epoch must be a number', id='wrong-epoch-type'
        ),
        pytest.param(b'[]', 'expected a JSON object', id='non-object-root'),
        # spherical on POINT is accepted (vacuous edges); use linestring shape below
        pytest.param(b'{"edges":"bogus"}', 'unknown edges value', id='unknown-edges'),
        pytest.param(
            b'{"crs_type":"bogus"}', 'unknown crs_type', id='unknown-crs-type'
        ),
        pytest.param(b'{"crs_type":null}', 'unknown crs_type', id='null-crs-type'),
        pytest.param(
            b'{"crs":"opaque","crs_type":"srid"}',
            'requires out-of-band CRS resolution',
            id='opaque-srid',
        ),
    ],
)
def test_from_arrow_malformed_geoarrow_metadata_is_tagged_parse_error(
    metadata: bytes, match: str
) -> None:
    storage = pa.StructArray.from_arrays(
        [pa.array([0.0]), pa.array([0.0])], names=['x', 'y']
    )
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': metadata,
        },
    )
    table = pa.Table.from_arrays([storage], schema=pa.schema([field]))
    with pytest.raises(gm.ParseError, match=match) as excinfo:
        gm.from_arrow(table)
    assert excinfo.value.format == 'geoarrow'


def test_geoarrow_spherical_edges_rejected_on_linestring_accepted_on_point() -> None:
    """Edge semantics are vacuous on points; spherical remains rejected for lines."""
    point_storage = pa.StructArray.from_arrays(
        [pa.array([0.0]), pa.array([0.0])], names=['x', 'y']
    )
    point_field = pa.field(
        'geometry',
        point_storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{"edges":"spherical"}',
        },
    )
    point_table = pa.Table.from_arrays([point_storage], schema=pa.schema([point_field]))
    assert gm.from_arrow(point_table).to_wkt() == ['POINT (0 0)']

    # linestring: list of point structs
    coord = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    line_storage = pa.array(
        [[(0.0, 0.0), (1.0, 1.0)]],
        type=pa.list_(coord),
    )
    line_field = pa.field(
        'geometry',
        line_storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{"edges":"spherical"}',
        },
    )
    line_table = pa.Table.from_arrays([line_storage], schema=pa.schema([line_field]))
    with pytest.raises(
        gm.ParseError, match=r'edges "spherical" are unsupported'
    ) as excinfo:
        gm.from_arrow(line_table)
    assert excinfo.value.format == 'geoarrow'


def test_reserved_extension_name_must_be_utf8_on_all_arrow_frontends() -> None:
    storage = pa.StructArray.from_arrays(
        [pa.array([0.0]), pa.array([0.0])], names=['x', 'y']
    )
    field = pa.field(
        'geometry', storage.type, metadata={b'ARROW:extension:name': b'\xff'}
    )
    table = pa.Table.from_arrays([storage], schema=pa.schema([field]))
    batch = table.to_batches()[0]
    for source in (table, _ArrowCStreamOnly(table), _ArrowCArrayOnly(batch)):
        with pytest.raises(
            gm.ParseError, match='extension name metadata is not UTF-8'
        ) as exc:
            gm.from_arrow(source)
        assert exc.value.format == 'geoarrow'


def test_python_arrow_metadata_frame_matches_rust_boundary_rules() -> None:
    from gometry import _arrow as gometry_arrow

    # Python adapter now delegates to the native GeoArrow metadata parser —
    # accept/reject and messages match the from_arrow boundary.
    assert gometry_arrow._metadata_frame(b'{"crs":"EPSG:4326"}') == ('EPSG:4326', None)
    assert gometry_arrow._metadata_frame(b'{"crs":"EPSG:4326","epoch":-0.0}') == (
        'EPSG:4326',
        0.0,
    )
    with pytest.raises(ValueError, match='crs must be a string or object'):
        gometry_arrow._metadata_frame(b'{"crs":123}')
    with pytest.raises(ValueError, match='invalid GeoArrow extension metadata'):
        # Non-standard Python ``Infinity`` token is not valid JSON for serde.
        gometry_arrow._metadata_frame(b'{"crs":"EPSG:4326","epoch":Infinity}')
    with pytest.raises(
        gm.ParseError,
        match=r'^invalid GeoArrow extension metadata: a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ) as exc:
        gometry_arrow._metadata_frame(b'{"epoch":2020.0}')
    assert exc.value.format == 'geoarrow'
    with pytest.raises(
        gm.ParseError,
        match=r'^invalid GeoArrow extension metadata: a coordinate epoch requires a dynamic CRS; EPSG:2180 is static\. Remove epoch= or transform to a dynamic CRS first$',
    ):
        gometry_arrow._metadata_frame(b'{"crs":"EPSG:2180","epoch":2020.0}')
    with pytest.raises(ValueError, match='JSON object'):
        gometry_arrow._metadata_frame(b'[]')
    with pytest.raises(ValueError, match=r'edges "spherical".*unsupported'):
        gometry_arrow._metadata_frame(b'{"edges":"spherical"}')
    with pytest.raises(ValueError, match='unknown crs_type'):
        gometry_arrow._metadata_frame(b'{"crs_type":"bogus"}')
    with pytest.raises(ValueError, match='unknown crs_type'):
        gometry_arrow._metadata_frame(b'{"crs_type":null}')
    with pytest.raises(ValueError, match='out-of-band CRS resolution'):
        gometry_arrow._metadata_frame(b'{"crs":"opaque","crs_type":"srid"}')


@pytest.mark.parametrize('epoch', [True, '2020'])
def test_python_arrow_metadata_frame_rejects_non_numeric_epoch(epoch: object) -> None:
    from gometry import _arrow as gometry_arrow

    metadata = _json.dumps({'crs': 'EPSG:4326', 'epoch': epoch}).encode()
    with pytest.raises(ValueError, match='epoch must be a number'):
        gometry_arrow._metadata_frame(metadata)


class _ArrowCStreamOnly:
    """Pure `__arrow_c_stream__` frontend — no type/chunks/column_names/read_all."""

    def __init__(self, value: object) -> None:
        self._value = value

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        return self._value.__arrow_c_stream__(requested_schema)  # type: ignore[attr-defined]


class _ArrowCArrayOnly:
    """Pure `__arrow_c_array__` frontend — forces capsule path."""

    def __init__(self, value: object) -> None:
        self._value = value

    def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
        return self._value.__arrow_c_array__(requested_schema)  # type: ignore[attr-defined]


def _conflicted_point_table() -> Any:
    """ExtensionType CRS=4326 + empty field extension metadata (semantic conflict)."""
    arr = gm.points([1.0], [2.0], crs=4326).to_arrow()
    field = pa.field(
        'geometry',
        arr.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    return pa.Table.from_arrays([arr], schema=pa.schema([field]))


def _pure_stream_freevar(carrier: object) -> object:
    """Objective-style pure protocol: free/global carrier, no wrapper attrs."""

    class S:
        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            return carrier.__arrow_c_stream__(requested_schema)  # type: ignore[attr-defined]

    return S()


def _pure_array_freevar(carrier: object) -> object:
    """Pure `__arrow_c_array__` with free/global carrier (no `_value` attr)."""

    class A:
        def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
            return carrier.__arrow_c_array__(requested_schema)  # type: ignore[attr-defined]

    return A()


def _arrow_frontends(table: Any) -> list[tuple[str, object]]:
    """PyArrow-direct, pure stream, pure array — the D07 three-frontend matrix."""
    batch = table.to_batches()[0]
    return [
        ('pyarrow-direct', table),
        ('arrow-c-stream-attr', _ArrowCStreamOnly(table)),
        ('arrow-c-stream-freevar', _pure_stream_freevar(table)),
        ('arrow-c-array-attr', _ArrowCArrayOnly(batch)),
        ('arrow-c-array-freevar', _pure_array_freevar(batch)),
    ]


def test_from_arrow_rejects_extension_type_vs_field_metadata_conflict_pyarrow_paths() -> (
    None
):
    """D07: dual-source conflict raises on PyArrow-direct / read_all (both carriers visible).

    Pure Arrow-C capsule/stream paths trust the exported schema after collapse
    (field wins) — dual-source reconcilation is not invented by walking wrapper
    freevars/globals (R05).
    """
    table = _conflicted_point_table()
    match = 'conflicting GeoArrow extension metadata'
    with pytest.raises(gm.ParseError, match=match) as direct:
        gm.from_arrow(table)
    assert direct.value.format == 'geoarrow'

    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    with pytest.raises(gm.ParseError, match=match):
        gm.from_arrow(reader)

    empty_reader = pa.RecordBatchReader.from_batches(table.schema, [])
    with pytest.raises(gm.ParseError, match=match):
        gm.from_arrow(empty_reader)

    arr = table.column(0).chunk(0)
    mismatch = pa.field(
        'geometry',
        arr.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{"crs":"EPSG:3857"}',
        },
    )
    mismatch_table = pa.Table.from_arrays([arr], schema=pa.schema([mismatch]))
    with pytest.raises(gm.ParseError, match=match):
        gm.from_arrow(mismatch_table)
    with pytest.raises(gm.ParseError, match=match):
        gm.from_arrow(
            pa.RecordBatchReader.from_batches(
                mismatch_table.schema, mismatch_table.to_batches()
            )
        )

    # Pure C of the same dual-source table trusts the collapsed export (field
    # wins: empty field metadata → CRS-free). Coordinates still import.
    batch = table.to_batches()[0]
    pure_c = gm.from_arrow(_ArrowCArrayOnly(batch))
    assert pure_c.crs is None
    assert pure_c.to_wkt() == ['POINT (1 2)']
    pure_stream = gm.from_arrow(_pure_stream_freevar(table))
    assert pure_stream.crs is None
    assert pure_stream.to_wkt() == ['POINT (1 2)']

    # Capsule of the bare ExtensionArray still has only ExtensionType (no field).
    bare = gm.points([1.0], [2.0], crs=4326).to_arrow()
    assert gm.from_arrow(bare).crs == 'EPSG:4326'
    assert gm.from_arrow(_ArrowCArrayOnly(bare)).crs == 'EPSG:4326'
    assert gm.from_arrow(_pure_array_freevar(bare)).crs == 'EPSG:4326'


def test_from_arrow_honest_capsule_ignores_unrelated_conflicted_globals() -> None:
    """R05: pure C export of GOOD must not inherit conflicts from BAD in scope.

    An honest provider returns only GOOD's capsules. An untaken branch merely
    *references* an unrelated dual-source-conflicted BAD table — that object is
    never exported and must not invent a ParseError.
    """
    good = gm.points([1.0], [2.0], crs=4326).to_arrow()
    bad = _conflicted_point_table()

    class HonestArrayProvider:
        def __arrow_c_array__(self, requested_schema: object | None = None) -> object:
            if False:  # untaken — still a co_names global/free reference to BAD
                return bad.__arrow_c_array__(requested_schema)  # type: ignore[attr-defined]
            return good.__arrow_c_array__(requested_schema)  # type: ignore[attr-defined]

    class HonestStreamProvider:
        def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
            if False:
                return bad.__arrow_c_stream__(requested_schema)  # type: ignore[attr-defined]
            return pa.table({'geometry': good}).__arrow_c_stream__(requested_schema)

    for provider in (HonestArrayProvider(), HonestStreamProvider()):
        restored = gm.from_arrow(provider)
        assert restored.crs == 'EPSG:4326'
        assert restored.to_wkt() == ['POINT (1 2)']

    # Positive: when BAD itself is the PyArrow carrier, dual-source still errors.
    with pytest.raises(
        gm.ParseError, match='conflicting GeoArrow extension metadata'
    ) as exc:
        gm.from_arrow(bad)
    assert exc.value.format == 'geoarrow'


def test_from_arrow_geoarrow_wkb_over_point_storage_same_verdict_all_frontends() -> (
    None
):
    """D07: geoarrow.wkb declared over point (struct) storage rejects on every frontend."""
    storage = pa.StructArray.from_arrays(
        [pa.array([1.0]), pa.array([2.0])], names=['x', 'y']
    )
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.wkb',
            b'ARROW:extension:metadata': b'{}',
        },
    )
    table = pa.Table.from_arrays([storage], schema=pa.schema([field]))
    match = 'geoarrow.wkb storage must be'
    errors: list[tuple[str, type[BaseException], str]] = []
    for frontend_id, frontend in _arrow_frontends(table):
        with pytest.raises(Exception) as excinfo:
            gm.from_arrow(frontend)
        errors.append((frontend_id, type(excinfo.value), str(excinfo.value)))
        assert match in str(excinfo.value), (frontend_id, excinfo.value)
    # Same exception class on every frontend (no silent accept / divergent type).
    classes = {cls for _, cls, _ in errors}
    assert len(classes) == 1, errors


def test_from_arrow_extension_only_and_field_only_crs_agree_across_frontends() -> None:
    """D07 positive: single-source CRS is preserved (no over-rejection)."""
    # ExtensionType-only (field metadata absent).
    arr = gm.points([1.0], [2.0], crs=4326).to_arrow()
    table = pa.table({'geometry': arr})
    assert gm.from_arrow(table).crs == 'EPSG:4326'
    assert gm.from_arrow(arr).crs == 'EPSG:4326'
    assert gm.from_arrow(_ArrowCArrayOnly(arr)).crs == 'EPSG:4326'
    assert gm.from_arrow(_ArrowCStreamOnly(table)).crs == 'EPSG:4326'
    assert gm.from_arrow(_pure_stream_freevar(table)).crs == 'EPSG:4326'
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    assert gm.from_arrow(reader).crs == 'EPSG:4326'

    # Field-metadata-only (no ExtensionType wrapper).
    storage = pa.StructArray.from_arrays(
        [pa.array([1.0]), pa.array([2.0])], names=['x', 'y']
    )
    field = pa.field(
        'geometry',
        storage.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{"crs":"EPSG:4326"}',
        },
    )
    field_table = pa.Table.from_arrays([storage], schema=pa.schema([field]))
    assert gm.from_arrow(field_table).crs == 'EPSG:4326'
    assert gm.from_arrow(_ArrowCStreamOnly(field_table)).crs == 'EPSG:4326'
    assert gm.from_arrow(_pure_stream_freevar(field_table)).crs == 'EPSG:4326'
    field_reader = pa.RecordBatchReader.from_batches(
        field_table.schema, field_table.to_batches()
    )
    assert gm.from_arrow(field_reader).crs == 'EPSG:4326'
    assert gm.from_arrow(field_table).to_wkt() == ['POINT (1 2)']

    # Agreeing dual sources (same CRS) must not reject on any frontend.
    agree = pa.field(
        'geometry',
        arr.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{"crs":"EPSG:4326"}',
        },
    )
    agree_table = pa.Table.from_arrays([arr], schema=pa.schema([agree]))
    assert gm.from_arrow(agree_table).crs == 'EPSG:4326'
    assert gm.from_arrow(_ArrowCStreamOnly(agree_table)).crs == 'EPSG:4326'
    assert gm.from_arrow(_pure_stream_freevar(agree_table)).crs == 'EPSG:4326'


def test_from_arrow_plain_stream_preserves_crs() -> None:
    """D07 positive: plain crs=4326 through pure stream keeps EPSG:4326."""
    table = pa.table({'geometry': gm.points([1.0], [2.0], crs=4326).to_arrow()})
    assert gm.from_arrow(_pure_stream_freevar(table)).crs == 'EPSG:4326'
    assert gm.from_arrow(_ArrowCStreamOnly(table)).crs == 'EPSG:4326'
    reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
    assert gm.from_arrow(reader).crs == 'EPSG:4326'


def test_from_arrow_empty_and_zero_batch_preserve_crs_epoch() -> None:
    """D07 positive: empty/zero-chunk framed readers keep CRS+epoch (no over-rejection)."""
    arr = gm.points([1.0], [2.0], crs=4326, epoch=2020.0).to_arrow()
    table = pa.table({'geometry': arr})
    full = gm.from_arrow(table)
    assert full.crs == 'EPSG:4326'
    assert full.epoch == 2020.0

    empty_reader = pa.RecordBatchReader.from_batches(table.schema, [])
    empty = gm.from_arrow(empty_reader)
    assert len(empty) == 0
    assert empty.crs == 'EPSG:4326'
    assert empty.epoch == 2020.0

    empty_table = table.slice(0, 0)
    via_stream = gm.from_arrow(_pure_stream_freevar(empty_table))
    assert len(via_stream) == 0
    assert via_stream.crs == 'EPSG:4326'
    assert via_stream.epoch == 2020.0

    empty_arr = arr.slice(0, 0)
    via_arr = gm.from_arrow(empty_arr)
    assert len(via_arr) == 0
    assert via_arr.crs == 'EPSG:4326'
    assert via_arr.epoch == 2020.0


def test_python_arrow_extension_type_rejects_epoch_without_crs() -> None:
    from gometry import _arrow as gometry_arrow

    with pytest.raises(
        ValueError,
        match=r'^invalid GeoArrow extension metadata: a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gometry_arrow._extension_type(cast('Any', pa), 'geoarrow.point', None, 2020.0)
    with pytest.raises(ValueError, match='epoch must be finite'):
        gometry_arrow._extension_type(
            cast('Any', pa), 'geoarrow.point', 'EPSG:4326', math.inf
        )


def test_arrow_missing_pyarrow_error_names_install_extra() -> None:
    code = """
import builtins
import gometry._arrow as arrow

real_import = builtins.__import__
def blocked_import(name, *args, **kwargs):
    if name == 'pyarrow' or name.startswith('pyarrow.'):
            raise ModuleNotFoundError("No module named 'pyarrow'", name='pyarrow')
    return real_import(name, *args, **kwargs)

builtins.__import__ = blocked_import
try:
    arrow._pyarrow()
except ModuleNotFoundError as error:
    assert "'gometry[arrow]'" in str(error)
else:
    raise AssertionError('missing pyarrow should fail')
"""
    subprocess.run([sys.executable, '-c', code], check=True)


@pytest.mark.parametrize(
    'values',
    [
        [gm.Point(float(i), float(-i)) for i in range(12)],
        [gm.LineString([(float(i), 0.0), (float(i + 1), 1.0)]) for i in range(12)],
        [gm.box(float(i), 0.0, float(i + 1), 1.0) for i in range(12)],
    ],
)
def test_multichunk_packed_arrow_import_preserves_all_rows(
    values: list[object],
) -> None:
    arrow = gm.GeometryArray(values).to_arrow()
    chunks = pa.chunked_array([arrow.slice(0, 3), arrow.slice(3, 4), arrow.slice(7, 5)])
    restored = gm.from_arrow(chunks, crs=None)
    assert restored.to_wkt() == gm.GeometryArray(values).to_wkt()


def test_binary_view_wkb_import_handles_inline_and_external_rows() -> None:
    if not hasattr(pa, 'binary_view'):
        pytest.skip('pyarrow has no binary_view type')
    # Empty GeometryCollection WKB is 9 bytes (< 12-byte BinaryView inline
    # threshold); Point/LineString WKB is external. Both paths must work.
    empty_gc = gm.from_wkt('GEOMETRYCOLLECTION EMPTY')
    assert len(empty_gc.to_wkb()) == 9
    values = gm.GeometryArray([
        empty_gc,
        gm.Point(1, 2),
        gm.LineString([(0, 0), (1, 1), (2, 2)]),
    ])
    from gometry._arrow import GEOARROW_WKB, _extension_type_from_storage

    storage = pa.array(values.to_wkb(), type=pa.binary_view())
    arrow = pa.ExtensionArray.from_storage(
        _extension_type_from_storage(pa, GEOARROW_WKB, storage.type, None, None),
        storage,
    )
    restored = gm.from_arrow(arrow, crs=None)
    assert restored.to_wkt() == values.to_wkt()
