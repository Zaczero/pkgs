"""Native Parquet geometry interoperability and frame admission."""

from __future__ import annotations

import base64
import json
import struct
from typing import TYPE_CHECKING

import gometry as gm
import pytest

if TYPE_CHECKING:
    from pathlib import Path

pa = pytest.importorskip('pyarrow', reason='native Parquet geometry requires pyarrow')
pq = pytest.importorskip(
    'pyarrow.parquet', reason='native Parquet geometry requires pyarrow'
)


class _WkbType(pa.ExtensionType):
    def __init__(self, metadata: dict[str, object]) -> None:
        self.metadata = metadata
        super().__init__(pa.binary(), 'geoarrow.wkb')

    def __arrow_ext_serialize__(self) -> bytes:
        return json.dumps(self.metadata).encode()

    @classmethod
    def __arrow_ext_deserialize__(
        cls, storage_type: object, serialized: bytes
    ) -> _WkbType:
        return cls(json.loads(serialized))


def _foreign_geometry(metadata: dict[str, object], x: float = 7.0) -> pa.ExtensionArray:
    return pa.ExtensionArray.from_storage(
        _WkbType(metadata), pa.array([struct.pack('<BIdd', 1, 1, x, 13.0)])
    )


def _ipc_geometry(column: pa.ExtensionArray) -> pa.ChunkedArray:
    table = pa.table({'geometry': column})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return pa.ipc.open_stream(sink.getvalue()).read_all()['geometry']


@pytest.mark.parametrize('crs', [None, 3857, 4326])
@pytest.mark.parametrize(
    'axes',
    [
        pytest.param({}, id='xy'),
        pytest.param({'z': 19.0}, id='xyz'),
        pytest.param({'m': 23.0}, id='xym'),
        pytest.param({'z': 19.0, 'm': 23.0}, id='xyzm'),
    ],
)
def test_native_geometry_roundtrip_and_pyarrow_cross_read(
    tmp_path: Path, crs: int | None, axes: dict[str, float]
) -> None:
    path = tmp_path / 'native.parquet'
    source = gm.GeometryArray([gm.Point(7, 13, crs=crs, **axes), None])
    source.to_geoparquet(path, encoding='geometry', attributes={'id': [31, 47]})
    restored, attributes = gm.from_geoparquet(path)
    assert restored.to_wkt() == source.to_wkt()
    assert restored.crs == source.crs
    assert attributes.to_pydict() == {'id': [31, 47]}
    table = pq.read_table(path)
    assert table['id'].to_pylist() == [31, 47]
    assert table['geometry'].null_count == 1
    file = pq.ParquetFile(path)
    assert (
        json.loads(file.schema.column(0).logical_type.to_json())['Type'] == 'Geometry'
    )
    if 'm' in axes:
        assert file.metadata.row_group(0).column(0).geo_statistics.mmin == 23.0


@pytest.mark.parametrize('rows', [[], [None], [gm.from_wkt('POINT M EMPTY')]])
def test_native_geometry_preserves_empty_and_missing_rows(
    tmp_path: Path, rows: list[gm.Geometry | None]
) -> None:
    path = tmp_path / 'empty.parquet'
    source = gm.GeometryArray(rows)
    source.to_geoparquet(path, encoding='geometry')
    restored, attrs = gm.from_geoparquet(path)
    assert restored.to_wkt() == source.to_wkt()
    assert attrs.num_rows == len(source)


@pytest.mark.parametrize('store_schema', [True, False])
@pytest.mark.parametrize('crs', ['EPSG:3857', 'srid:0'])
def test_plain_native_file_resolves_contextual_crs(
    tmp_path: Path, store_schema: bool, crs: str
) -> None:
    path = tmp_path / 'plain.parquet'
    pq.write_table(
        pa.table({'shape': _foreign_geometry({'crs': crs}), 'id': [53]}),
        path,
        store_schema=store_schema,
    )
    restored, attributes = gm.from_geoparquet(path)
    assert restored.to_wkt() == ['POINT (7 13)']
    assert (None if restored.crs is None else restored.crs.to_epsg()) == (
        None if crs == 'srid:0' else 3857
    )
    assert attributes.to_pydict() == {'id': [53]}


def test_plain_native_multiple_columns_require_explicit_selection(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'multiple.parquet'
    pq.write_table(
        pa.table({
            'start': _foreign_geometry({'crs': 'EPSG:3857'}),
            'end': _foreign_geometry({'crs': 'EPSG:3857'}, 17.0),
            'id': [59],
        }),
        path,
    )
    with pytest.raises(gm.ParseError, match='select one with geometry='):
        gm.from_geoparquet(path)
    restored, attributes = gm.from_geoparquet(path, geometry='end')
    assert restored.to_wkt() == ['POINT (17 13)']
    assert attributes.to_pydict() == {'id': [59]}


def test_native_crs_reference_resolves_file_metadata(tmp_path: Path) -> None:
    path = tmp_path / 'referenced.parquet'
    table = pa.table({'geometry': _foreign_geometry({'crs': 'projjson:frame'})})
    table = table.replace_schema_metadata({
        b'frame': gm.CRS(3857).to_projjson().encode(),
    })
    pq.write_table(table, path)
    restored, _ = gm.from_geoparquet(path)
    assert restored.to_wkt() == ['POINT (7 13)']
    assert restored.crs.to_epsg() == 3857


def test_arrow_deserialization_preserves_metadata_until_contextual_import() -> None:
    gm.points([1], [2]).to_arrow()
    declaration = {'crs': '0', 'crs_type': 'srid', 'producer:extra': [17, 29]}
    restored = _ipc_geometry(_foreign_geometry(declaration))
    assert json.loads(restored.type.__arrow_ext_serialize__()) == declaration
    with pytest.raises(gm.ParseError, match='out-of-band CRS resolution'):
        gm.from_arrow(restored)


def test_arrow_frame_types_remain_composable_after_reading_many_frames() -> None:
    gm.points([1], [2]).to_arrow()
    first = _ipc_geometry(_foreign_geometry({'crs': 'EPSG:3857'}))
    for index in range(300):
        _ipc_geometry(_foreign_geometry({'crs': 'EPSG:3395', 'producer:record': index}))
    second = _ipc_geometry(_foreign_geometry({'crs': 'EPSG:3857'}))
    combined = pa.concat_tables([
        pa.table({'geometry': first}),
        pa.table({'geometry': second}),
    ])
    assert gm.from_arrow(combined['geometry']).to_wkt() == [
        'POINT (7 13)',
        'POINT (7 13)',
    ]
    other_frame = _ipc_geometry(_foreign_geometry({'crs': 'EPSG:3395'}))
    with pytest.raises(pa.ArrowInvalid, match='Schema'):
        pa.concat_tables([
            pa.table({'geometry': first}),
            pa.table({'geometry': other_frame}),
        ])


def test_native_and_geo_crs_must_agree(tmp_path: Path) -> None:
    path = tmp_path / 'conflict.parquet'
    source = gm.points([7], [13], crs=3857)
    source.to_geoparquet(path, encoding='geometry')
    table = pq.read_table(path)
    metadata = dict(table.schema.metadata)
    geo = json.loads(metadata[b'geo'])
    geo['columns']['geometry']['crs'] = gm.CRS(3395).to_projjson(as_dict=True)
    geo['columns']['geometry'].pop('gometry:crs_definition', None)
    metadata[b'geo'] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(metadata), path)
    with pytest.raises(gm.ParseError, match='conflicts with native Parquet CRS'):
        gm.from_geoparquet(path)


@pytest.mark.parametrize(
    'field_metadata',
    [
        {
            b'ARROW:extension:name': b'geoarrow.linestring',
            b'ARROW:extension:metadata': b'{}',
        },
        {
            b'ARROW:extension:name': b'geoarrow.wkb',
            b'ARROW:extension:metadata': b'{"crs":"EPSG:3395"}',
        },
        {
            b'ARROW:extension:name': b'geoarrow.wkb',
            b'ARROW:extension:metadata': b'{"crs":null}',
        },
    ],
    ids=['extension-name', 'crs', 'unknown-crs'],
)
def test_native_stored_arrow_schema_must_agree(
    tmp_path: Path, field_metadata: dict[bytes, bytes]
) -> None:
    path = tmp_path / 'stored-schema.parquet'
    table = pa.table({'geometry': _foreign_geometry({'crs': 'EPSG:3857'})})
    contradictory = pa.schema([
        pa.field('geometry', pa.binary(), metadata=field_metadata),
    ])
    with pq.ParquetWriter(path, table.schema) as writer:
        writer.write_table(table)
        writer.add_key_value_metadata({
            'ARROW:schema': base64.b64encode(contradictory.serialize()).decode(),
        })
    with pytest.raises(gm.ParseError, match='Arrow'):
        gm.from_geoparquet(path)


def test_native_crs_survives_a_stored_schema_without_frame_metadata(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'stored-schema.parquet'
    table = pa.table({'geometry': _foreign_geometry({'crs': 'EPSG:3857'})})
    stored = pa.schema([pa.field('geometry', pa.binary())])
    with pq.ParquetWriter(path, table.schema) as writer:
        writer.write_table(table)
        writer.add_key_value_metadata({
            'ARROW:schema': base64.b64encode(stored.serialize()).decode(),
        })
    restored, attributes = gm.from_geoparquet(path)
    assert restored.crs is not None
    assert restored.crs.to_epsg() == 3857
    assert restored.to_wkt() == ['POINT (7 13)']
    assert attributes.num_rows == 1


def test_geography_requires_explicit_curve_conversion(tmp_path: Path) -> None:
    path = tmp_path / 'geography.parquet'
    pq.write_table(
        pa.table({'geometry': _foreign_geometry({'edges': 'spherical'})}), path
    )
    with pytest.raises(
        gm.ParseError, match=r'straight source-chart edges.*Explicitly convert'
    ):
        gm.from_geoparquet(path)


def test_native_bbox_prunes_geometry_decoding(tmp_path: Path) -> None:
    path = tmp_path / 'pruned.parquet'
    source = gm.points([7, 1007], [13, 1013])
    source.to_geoparquet(
        path,
        encoding='geometry',
        attributes={'id': [61, 67]},
        row_group_size=1,
        compression='NONE',
        use_dictionary=False,
    )
    payload = path.read_bytes()
    outside = struct.pack('<BIdd', 1, 1, 1007.0, 1013.0)
    assert payload.count(outside) == 1
    path.write_bytes(payload.replace(outside, b'\xff' + outside[1:]))
    restored, attributes = gm.from_geoparquet(
        path, bbox=(0, 0, 20, 20), columns=['id'], filters=[('id', '>', 60)]
    )
    assert restored.to_wkt() == ['POINT (7 13)']
    assert attributes.to_pydict() == {'id': [61]}
    with pytest.raises(gm.ParseError):
        gm.from_geoparquet(path)
