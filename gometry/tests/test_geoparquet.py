"""GeoParquet read/write round-trip and geopandas interop."""

from __future__ import annotations

import json
import os
import tempfile

import geopandas as gpd
import gometry as gm
import pytest
from shapely.geometry import LineString, Point, Polygon

from tests._support import canon


@pytest.fixture
def parquet_path() -> str:
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as handle:
        path = handle.name
    yield path
    os.unlink(path)


def _assert_roundtrip(original: gm.GeometryArray, restored: gm.GeometryArray) -> None:
    assert canon(restored) == canon(original)
    assert restored.crs == original.crs


def _read_geometry(path: str, **kwargs: object) -> gm.GeometryArray:
    geometries, _ = gm.from_geoparquet(path, **kwargs)
    return geometries


@pytest.mark.parametrize(
    ('geometries', 'label'),
    [
        (gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 2)], crs=4326), 'points_4326'),
        (gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)]), 'points_crs_free'),
        (
            gm.GeometryArray([
                gm.LineString([(0, 0), (1, 1)], crs=4326),
                gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)], crs=4326),
            ]),
            'mixed_lines_polygons',
        ),
        (
            gm.GeometryArray([
                gm.Point(0, 0, z=1, crs=4326),
                gm.LineString([(0, 0), (1, 1)], z=[1, 2], crs=4326),
            ]),
            'z_geometries',
        ),
        (gm.GeometryArray([]), 'empty'),
    ],
)
def test_to_geoparquet_from_geoparquet_roundtrip(
    geometries: gm.GeometryArray,
    label: str,
    parquet_path: str,
) -> None:
    del label
    geometries.to_geoparquet(parquet_path)
    restored = _read_geometry(parquet_path)
    _assert_roundtrip(geometries, restored)


def test_scalar_geometry_writes_single_row(parquet_path: str) -> None:
    point = gm.Point(2, 3, crs=4326)
    gm.GeometryArray([point]).to_geoparquet(parquet_path)
    restored = _read_geometry(parquet_path)
    assert len(restored) == 1
    _assert_roundtrip(gm.GeometryArray([point]), restored)


def test_geoparquet_readable_by_geopandas(parquet_path: str) -> None:
    geometries = gm.GeometryArray([
        gm.Point(1, 2, crs=4326),
        gm.LineString([(0, 0), (1, 1)], crs=4326),
        gm.box(0, 0, 1, 1, crs=4326),
    ])
    geometries.to_geoparquet(parquet_path)
    gdf = gpd.read_parquet(parquet_path)
    assert len(gdf) == 3
    assert gdf.crs is not None
    assert gdf.geometry.iloc[0].equals(Point(1, 2))
    assert gdf.geometry.iloc[1].equals(LineString([(0, 0), (1, 1)]))
    assert gdf.geometry.iloc[2].equals(
        Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    )


def test_from_geoparquet_reads_geopandas_written_file(parquet_path: str) -> None:
    gdf = gpd.GeoDataFrame(
        {'name': ['alpha', 'beta']},
        geometry=[Point(0, 0), Point(10, 20)],
        crs=4326,
    )
    gdf.to_parquet(parquet_path)
    restored = _read_geometry(parquet_path)
    assert restored.crs == 'EPSG:4326'
    assert canon(restored) == ['POINT (0 0)', 'POINT (10 20)']


def test_from_geoparquet_preserves_attribute_columns(parquet_path: str) -> None:
    gdf = gpd.GeoDataFrame(
        {'id': [1, 2], 'label': ['a', 'b']},
        geometry=[Point(1, 2), Point(3, 4)],
        crs=4326,
    )
    gdf.to_parquet(parquet_path)
    restored, attributes = gm.from_geoparquet(parquet_path)
    assert len(restored) == 2
    assert canon(restored) == ['POINT (1 2)', 'POINT (3 4)']
    assert attributes.column_names == ['id', 'label']
    assert attributes.to_pydict() == {'id': [1, 2], 'label': ['a', 'b']}
    assert b'geo' not in (attributes.schema.metadata or {})


def test_to_geoparquet_crs_kwarg_attaches_metadata(parquet_path: str) -> None:
    geometries = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    geometries.set_crs(4326).to_geoparquet(parquet_path)
    restored = _read_geometry(parquet_path)
    assert restored.crs == 'EPSG:4326'
    assert canon(restored) == ['POINT (0 0)', 'POINT (1 1)']


def test_geoparquet_geometry_types_labels(parquet_path: str) -> None:
    geometries = gm.GeometryArray([
        gm.Point(0, 0, z=1, crs=4326),
        gm.GeometryCollection([gm.Point(1, 1, crs=4326)], crs=4326),
    ])
    geometries.to_geoparquet(parquet_path)
    import pyarrow.parquet as pq

    metadata = json.loads(pq.read_schema(parquet_path).metadata[b'geo'].decode())
    labels = set(metadata['columns']['geometry']['geometry_types'])
    assert labels == {'Point Z', 'GeometryCollection'}


def test_geoparquet_writer_rejects_m_ordinates(parquet_path: str) -> None:
    values = gm.GeometryArray([gm.Point(0, 0, m=2, crs=4326)])
    with pytest.raises(gm.GeometryError, match='does not support M ordinates'):
        values.to_geoparquet(parquet_path)


def test_geoparquet_epoch_roundtrip(parquet_path: str) -> None:
    geometries = gm.GeometryArray([gm.Point(0, 0, crs=4326)], crs=4326, epoch=2020.5)
    geometries.to_geoparquet(parquet_path)
    restored = _read_geometry(parquet_path)
    assert restored.epoch == 2020.5


def test_from_geoparquet_rejects_native_geoarrow_geometrycollection_encoding(
    parquet_path: str,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({'geometry': pa.array([b''], type=pa.binary())})
    geo_metadata = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'geometrycollection',
                'geometry_types': ['GeometryCollection'],
            }
        },
    }
    metadata = {b'geo': json.dumps(geo_metadata).encode('utf-8')}
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, parquet_path)
    with pytest.raises(
        gm.GeometryError,
        match='native GeoArrow geometrycollection read is unsupported',
    ):
        gm.from_geoparquet(parquet_path)


def test_from_geoparquet_wkb_geometrycollection_roundtrips(parquet_path: str) -> None:
    geometries = gm.GeometryArray([
        gm.GeometryCollection([gm.Point(1, 2, crs=4326)], crs=4326)
    ])
    geometries.to_geoparquet(parquet_path)
    restored = _read_geometry(parquet_path)
    _assert_roundtrip(geometries, restored)


def test_from_geoparquet_geoarrow_crs_mismatch_raises(parquet_path: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    arr = gm.GeometryArray([gm.Point(0, 0, crs=3857)])
    table = pa.table({'geometry': arr.to_arrow()})
    geo_metadata = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'point',
                'geometry_types': ['Point'],
                'crs': gm.CRS(4326).to_projjson_dict(),
            }
        },
    }
    metadata = {b'geo': json.dumps(geo_metadata).encode('utf-8')}
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, parquet_path)
    with pytest.raises(gm.ParseError) as excinfo:
        gm.from_geoparquet(parquet_path)
    assert excinfo.value.format == 'geoparquet'


def test_missing_pyarrow_raises_install_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    real_import = builtins.__import__

    def blocked_import(name: str, *args: object, **kwargs: object) -> object:
        if name == 'pyarrow' or name.startswith('pyarrow.'):
            raise ModuleNotFoundError("No module named 'pyarrow'", name='pyarrow')
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', blocked_import)
    with pytest.raises(ModuleNotFoundError, match=r'gometry\[arrow\]'):
        gm.GeometryArray([gm.Point(0, 0)]).to_geoparquet('unused.parquet')


def test_unknown_geoparquet_encoding_names_expected_tokens(
    parquet_path: str,
) -> None:
    """Encoding rejections match the shared token template with alternatives."""
    with pytest.raises(gm.GeometryError) as excinfo:
        gm.points([0.0], [0.0]).to_geoparquet(parquet_path, encoding='zzz')
    assert (
        str(excinfo.value)
        == "unknown GeoParquet encoding 'zzz'; expected 'wkb' or 'native'"
    )


def _write_geo_table(
    parquet_path: str,
    column_metadata: dict[str, object],
    *,
    version: object = '1.1.0',
    attributes: dict[str, object] | None = None,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    values = gm.GeometryArray([gm.Point(1, 2)]).to_arrow(encoding='wkb').storage
    columns = {'geometry': values, **(attributes or {})}
    table = pa.table(columns)
    geo_metadata = {
        'version': version,
        'primary_column': 'geometry',
        'columns': {'geometry': column_metadata},
    }
    table = table.replace_schema_metadata({
        b'geo': json.dumps(geo_metadata).encode('utf-8')
    })
    pq.write_table(table, parquet_path)


def test_geoparquet_absent_and_null_crs_are_distinct(parquet_path: str) -> None:
    base = {'encoding': 'WKB', 'geometry_types': ['Point']}
    _write_geo_table(parquet_path, base)
    defaulted = _read_geometry(parquet_path)
    assert defaulted.crs == 'OGC:CRS84'

    _write_geo_table(parquet_path, {**base, 'crs': None})
    unframed = _read_geometry(parquet_path)
    assert unframed.crs is None


def test_geoparquet_native_point_roundtrip(parquet_path: str) -> None:
    values = gm.points([1.0, 3.0], [2.0, 4.0], crs=4326)
    values.to_geoparquet(parquet_path, encoding='native')
    restored = _read_geometry(parquet_path)
    assert canon(restored) == ['POINT (1 2)', 'POINT (3 4)']
    assert restored.crs == 'EPSG:4326'


def _write_native_list_of_points_parquet(
    path: str,
    *,
    encoding: str,
    geometry_types: list[str],
    field_extension: str | bytes | None,
    crs: object = None,
) -> None:
    """Write a depth-1 geoarrow list-of-points column with optional field extension."""
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq

    coords = pa.array(
        [(0.0, 0.0), (1.0, 1.0)],
        type=pa.struct([('x', pa.float64()), ('y', pa.float64())]),
    )
    storage = pa.ListArray.from_arrays(pa.array([0, 2], type=pa.int32()), coords)
    field_meta = None
    if field_extension is not None:
        field_meta = {
            b'ARROW:extension:name': (
                field_extension
                if isinstance(field_extension, bytes)
                else field_extension.encode('utf-8')
            ),
            b'ARROW:extension:metadata': b'{}',
        }
    field = pa.field('geometry', storage.type, metadata=field_meta)
    column_meta: dict[str, object] = {
        'encoding': encoding,
        'geometry_types': geometry_types,
        'crs': crs,
    }
    geo = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {'geometry': column_meta},
    }
    table = pa.table(
        [storage],
        schema=pa.schema([field], metadata={b'geo': json.dumps(geo).encode()}),
    )
    pq.write_table(table, path)


def test_geoparquet_rejects_non_utf8_reserved_arrow_extension_name(
    parquet_path: str,
) -> None:
    _write_native_list_of_points_parquet(
        parquet_path,
        encoding='linestring',
        geometry_types=['LineString'],
        field_extension=b'\xff',
    )
    with pytest.raises(gm.ParseError, match='extension name metadata is not UTF-8') as exc:
        _read_geometry(parquet_path)
    assert exc.value.format == 'geoparquet'


def test_geoparquet_field_extension_encoding_mismatch_rejected(
    parquet_path: str,
) -> None:
    """P20: multipoint field extension + linestring encoding is always rejected."""
    import subprocess
    import sys

    _write_native_list_of_points_parquet(
        parquet_path,
        encoding='linestring',
        geometry_types=['LineString'],
        field_extension='geoarrow.multipoint',
    )
    # Current process (may already have ExtensionTypes registered via prior tests).
    with pytest.raises(gm.GeometryError, match=r'encoding|extension|physical storage'):
        gm.from_geoparquet(parquet_path)
    # Fresh interpreter without prior registration — same verdict (order-independent).
    code = (
        'import gometry as gm\n'
        f'path = {parquet_path!r}\n'
        'try:\n'
        '    gm.from_geoparquet(path)\n'
        "    raise SystemExit('imported')\n"
        'except gm.GeometryError as exc:\n'
        "    assert 'encoding' in str(exc) or 'extension' in str(exc) "
        "or 'physical storage' in str(exc), exc\n"
    )
    completed = subprocess.run(
        [sys.executable, '-c', code],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_geoparquet_native_no_extension_linestring_still_decodes(
    parquet_path: str,
) -> None:
    """P20 / R09: native storage without a field extension remains valid."""
    _write_native_list_of_points_parquet(
        parquet_path,
        encoding='linestring',
        geometry_types=['LineString'],
        field_extension=None,
    )
    restored = _read_geometry(parquet_path)
    assert canon(restored) == ['LINESTRING (0 0, 1 1)']


def test_geoparquet_matching_field_extension_multipoint_decodes(
    parquet_path: str,
) -> None:
    """P20 positive: matching multipoint extension + multipoint encoding decodes."""
    _write_native_list_of_points_parquet(
        parquet_path,
        encoding='multipoint',
        geometry_types=['MultiPoint'],
        field_extension='geoarrow.multipoint',
        crs=None,
    )
    restored = _read_geometry(parquet_path)
    assert canon(restored) == ['MULTIPOINT ((0 0), (1 1))']


@pytest.mark.parametrize(
    'values',
    [
        gm.GeometryArray([gm.LineString([(0, 0), (1, 1)], crs=4326)]),
        gm.GeometryArray([gm.box(0, 0, 1, 1, crs=4326)]),
        gm.GeometryArray([gm.MultiPoint([(0, 0), (1, 1)], crs=4326)]),
        gm.GeometryArray([gm.MultiLineString([[(0, 0), (1, 1)]], crs=4326)]),
        gm.GeometryArray([gm.MultiPolygon([gm.box(0, 0, 1, 1)], crs=4326)]),
    ],
)
def test_geoparquet_native_separated_layouts_roundtrip(
    parquet_path: str, values: gm.GeometryArray
) -> None:
    values.to_geoparquet(parquet_path, encoding='native')
    restored = _read_geometry(parquet_path)
    _assert_roundtrip(values, restored)


def test_geoparquet_feature_table_roundtrip(parquet_path: str) -> None:
    import pyarrow as pa

    values = gm.points([1.0, 3.0], [2.0, 4.0], crs=4326)
    attributes = pa.table({'id': [10, 20], 'name': ['a', 'b']})
    values.to_geoparquet(parquet_path, attributes=attributes)
    restored, restored_attributes = gm.from_geoparquet(parquet_path)
    assert canon(restored) == canon(values)
    assert restored_attributes.equals(attributes)


def test_geoparquet_filters_geometry_and_attributes_together(parquet_path: str) -> None:
    values = gm.points([1.0, 3.0, 5.0], [2.0, 4.0, 6.0], crs=4326)
    values.to_geoparquet(
        parquet_path,
        attributes={'id': [1, 2, 3], 'name': ['a', 'b', 'c']},
    )
    restored, attributes = gm.from_geoparquet(parquet_path, filters=[('id', '>', 1)])
    assert canon(restored) == ['POINT (3 4)', 'POINT (5 6)']
    assert attributes.to_pydict() == {'id': [2, 3], 'name': ['b', 'c']}


def test_geoparquet_row_group_and_filesystem_read(parquet_path: str) -> None:
    from pyarrow import fs

    values = gm.points([1.0, 3.0], [2.0, 4.0], crs=4326)
    values.to_geoparquet(
        parquet_path,
        attributes={'id': [1, 2]},
        row_group_size=1,
    )
    restored, attributes = gm.from_geoparquet(
        parquet_path,
        row_groups=[1],
        filesystem=fs.LocalFileSystem(),
    )
    assert canon(restored) == ['POINT (3 4)']
    assert attributes.to_pydict() == {'id': [2]}


def test_geoparquet_read_projects_selected_columns(
    parquet_path: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import pyarrow.parquet as pq

    _write_geo_table(
        parquet_path,
        {'encoding': 'WKB', 'geometry_types': ['Point'], 'crs': None},
        attributes={'id': [1], 'payload': ['unused']},
    )
    calls: list[list[str] | None] = []
    real_read_table = pq.read_table

    def recording_read_table(*args: object, **kwargs: object) -> object:
        calls.append(kwargs.get('columns'))  # type: ignore[arg-type]
        return real_read_table(*args, **kwargs)

    monkeypatch.setattr(pq, 'read_table', recording_read_table)
    _, attributes = gm.from_geoparquet(parquet_path, columns=['id'])
    assert calls == [['geometry', 'id']]
    assert attributes.to_pydict() == {'id': [1]}


@pytest.mark.parametrize('version', [None, 1, '0.1.0', '999.0.0'])
def test_geoparquet_rejects_unsupported_version(
    parquet_path: str, version: object
) -> None:
    _write_geo_table(
        parquet_path,
        {'encoding': 'WKB', 'geometry_types': ['Point']},
        version=version,
    )
    with pytest.raises(gm.GeometryError, match='version'):
        gm.from_geoparquet(parquet_path)


@pytest.mark.parametrize('version', ['1.0.0', '1.99.0', '1.2.3-rc.1+build.7'])
def test_geoparquet_accepts_semver_1_x(parquet_path: str, version: str) -> None:
    _write_geo_table(
        parquet_path,
        {'encoding': 'WKB', 'geometry_types': ['Point'], 'crs': None},
        version=version,
    )
    assert canon(_read_geometry(parquet_path)) == ['POINT (1 2)']


@pytest.mark.parametrize('root', [None, [], 1])
def test_geoparquet_normalizes_malformed_metadata_root(
    parquet_path: str, root: object
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({'geometry': [b'']}).replace_schema_metadata({
        b'geo': json.dumps(root).encode('utf-8')
    })
    pq.write_table(table, parquet_path)
    with pytest.raises(gm.GeometryError, match='geo field'):
        gm.from_geoparquet(parquet_path)


def test_geoparquet_normalizes_invalid_utf8_metadata(parquet_path: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({'geometry': [b'']}).replace_schema_metadata({b'geo': b'\xff'})
    pq.write_table(table, parquet_path)
    with pytest.raises(gm.GeometryError, match='UTF-8 JSON'):
        gm.from_geoparquet(parquet_path)


@pytest.mark.parametrize('epoch', [True, '2020', float('nan')])
def test_geoparquet_rejects_invalid_epoch(parquet_path: str, epoch: object) -> None:
    _write_geo_table(
        parquet_path,
        {
            'encoding': 'WKB',
            'geometry_types': ['Point'],
            'crs': gm.CRS(4326).to_projjson_dict(),
            'epoch': epoch,
        },
    )
    with pytest.raises(gm.GeometryError, match='epoch'):
        gm.from_geoparquet(parquet_path)


def test_geoparquet_rejects_spherical_edges(parquet_path: str) -> None:
    _write_geo_table(
        parquet_path,
        {
            'encoding': 'WKB',
            'geometry_types': ['Point'],
            'edges': 'spherical',
        },
    )
    with pytest.raises(gm.GeometryError, match=r"edges 'spherical'.*unsupported"):
        gm.from_geoparquet(parquet_path)


def test_geoparquet_rejects_string_crs(parquet_path: str) -> None:
    _write_geo_table(
        parquet_path,
        {
            'encoding': 'WKB',
            'geometry_types': ['Point'],
            'crs': 'EPSG:4326',
        },
    )
    with pytest.raises(gm.GeometryError, match='PROJJSON object or null'):
        gm.from_geoparquet(parquet_path)


@pytest.mark.parametrize(
    ('update', 'match'),
    [
        ({'geometry_types': ['Point', 'Point']}, 'unique array'),
        ({'geometry_types': ['Point M']}, 'supported type names'),
        ({'orientation': 'clockwise'}, 'orientation'),
        ({'bbox': [0.0, 0.0, float('nan'), 1.0]}, 'bbox'),
        ({'edges': 'bogus'}, 'edges'),
    ],
)
def test_geoparquet_rejects_malformed_column_metadata(
    parquet_path: str, update: dict[str, object], match: str
) -> None:
    _write_geo_table(
        parquet_path,
        {'encoding': 'WKB', 'geometry_types': ['Point'], **update},
    )
    with pytest.raises(gm.GeometryError, match=match):
        gm.from_geoparquet(parquet_path)


def test_geoparquet_native_point_accepts_required_coordinate_fields(
    parquet_path: str,
) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    point_type = pa.struct([
        pa.field('x', pa.float64(), nullable=False),
        pa.field('y', pa.float64(), nullable=False),
    ])
    table = pa.table({'geometry': pa.array([{'x': 1.0, 'y': 2.0}], type=point_type)})
    metadata = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'point',
                'geometry_types': ['Point'],
                'crs': None,
            }
        },
    }
    table = table.replace_schema_metadata({b'geo': json.dumps(metadata).encode()})
    pq.write_table(table, parquet_path)
    assert canon(_read_geometry(parquet_path)) == ['POINT (1 2)']


def test_geoparquet_native_layout_errors_are_normalized(parquet_path: str) -> None:
    # _write_geo_table stores binary WKB; declaring native 'point' must fail closed
    # at physical-encoding match (typed GeometryError), not coerce or decode as WKB.
    _write_geo_table(
        parquet_path,
        {'encoding': 'point', 'geometry_types': ['Point'], 'crs': None},
    )
    with pytest.raises(gm.GeometryError, match='does not match physical storage'):
        gm.from_geoparquet(parquet_path)


# --- D11: declared WKB must match physical binary / geoarrow.wkb storage ---


def test_d11_wkb_metadata_rejects_native_geoarrow_storage() -> None:
    """Exact audit repro: WKB encoding over native point storage must not decode."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    point = gm.points([1.0], [2.0]).to_arrow()  # native geoarrow, not binary
    with pytest.raises(
        gm.GeometryError,
        match=r"encoding 'WKB' requires Binary",
    ):
        _decode_geometry_column(
            pa,
            pa.chunked_array([point]),
            {'geometry_types': ['Point']},
            'geometry',
            'WKB',
            None,
            None,
        )


def test_d11_wkb_physical_binary_still_decodes() -> None:
    """Positive: real Binary WKB and geoarrow.wkb still decode under WKB metadata."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    meta = {'geometry_types': ['Point']}
    wkb_ext = gm.points([1.0], [2.0]).to_arrow(encoding='wkb')
    out_ext = _decode_geometry_column(
        pa, pa.chunked_array([wkb_ext]), meta, 'geometry', 'WKB', None, None
    )
    assert out_ext.to_wkt() == ['POINT (1 2)']

    plain = wkb_ext.storage
    out_plain = _decode_geometry_column(
        pa, pa.chunked_array([plain]), meta, 'geometry', 'WKB', None, None
    )
    assert out_plain.to_wkt() == ['POINT (1 2)']


def test_d11_wkb_public_roundtrip_still_works(parquet_path: str) -> None:
    """Positive: public WKB GeoParquet write+read still works."""
    values = gm.points([1.0, 3.0], [2.0, 4.0], crs=4326)
    values.to_geoparquet(parquet_path, encoding='wkb')
    restored = _read_geometry(parquet_path)
    _assert_roundtrip(values, restored)


def test_r15_dictionary_encoded_wkb_geoparquet_accepted(parquet_path: str) -> None:
    """R15: dictionary<binary> WKB must decode (read_dictionary / dict writers).

    Exact public repro: valid WKB GeoParquet re-encoded as dictionary binary,
    then read with ``read_dictionary=['geometry']`` (and without). Previously
    ``_is_wkb_physical_type`` rejected dictionary storage while
    ``_READ_TABLE_OPTIONS`` permits ``read_dictionary``.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gometry._geoparquet import _decode_geometry_column

    values = gm.points([1.0, 3.0, 1.0], [2.0, 4.0, 2.0], crs=4326)
    values.to_geoparquet(parquet_path, encoding='wkb')
    table = pq.read_table(parquet_path)
    geom = table.column('geometry')
    # Materialize plain binary, then dictionary-encode for a real dict column.
    plain_chunks = [getattr(chunk, 'storage', chunk) for chunk in geom.chunks]
    plain = pa.chunked_array(plain_chunks)
    dict_chunks = [chunk.dictionary_encode() for chunk in plain.chunks]
    dict_column = pa.chunked_array(dict_chunks)
    assert pa.types.is_dictionary(dict_column.type)
    assert pa.types.is_binary(dict_column.type.value_type)

    # Unit path: decode helper accepts dictionary binary.
    unit = _decode_geometry_column(
        pa,
        dict_column,
        {'geometry_types': ['Point']},
        'geometry',
        'WKB',
        'EPSG:4326',
        None,
    )
    assert unit.to_wkt() == values.to_wkt()

    # Public path: rewrite file with dictionary geometry + same geo metadata.
    other_cols = [
        table.column(i)
        for i in range(table.num_columns)
        if table.schema.field(i).name != 'geometry'
    ]
    other_names = [
        table.schema.field(i).name
        for i in range(table.num_columns)
        if table.schema.field(i).name != 'geometry'
    ]
    rewritten = pa.Table.from_arrays(
        [dict_column, *other_cols],
        names=['geometry', *other_names],
        metadata=table.schema.metadata,
    )
    pq.write_table(rewritten, parquet_path)

    restored, _ = gm.from_geoparquet(parquet_path)
    assert restored.to_wkt() == values.to_wkt()
    assert restored.crs == values.crs

    # Exact audit option: read_dictionary=['geometry'].
    restored_dict, _ = gm.from_geoparquet(
        parquet_path, read_dictionary=['geometry']
    )
    assert restored_dict.to_wkt() == values.to_wkt()

    # Retained: non-dictionary WKB still works.
    values.to_geoparquet(parquet_path, encoding='wkb')
    plain_restored = _read_geometry(parquet_path)
    _assert_roundtrip(values, plain_restored)

    # Retained: non-binary dictionary still rejected.
    bad = pa.chunked_array([pa.array([1, 2, 1]).dictionary_encode()])
    with pytest.raises(gm.GeometryError, match=r"encoding 'WKB' requires Binary"):
        _decode_geometry_column(
            pa, bad, {'geometry_types': ['Point']}, 'geometry', 'WKB', None, None
        )


# --- D10: never silently relabel geometry kind via metadata ---


def test_d10_multipoint_cannot_be_relabeled_as_linestring() -> None:
    """Exact audit repro: MultiPoint storage + linestring encoding must error."""
    import pyarrow as pa
    from gometry._geoparquet import _native_geoarrow_column

    mp = gm.GeometryArray([gm.MultiPoint([(0, 0), (1, 1)])]).to_arrow()
    with pytest.raises(
        gm.GeometryError,
        match=r"encoding 'linestring' conflicts with embedded Arrow extension "
        r"'geoarrow\.multipoint'",
    ):
        _native_geoarrow_column(pa, pa.chunked_array([mp]), 'linestring', None, None)


def test_d10_polygon_multilinestring_relabel_rejected() -> None:
    """Same-depth kinds cannot be silently swapped via declared encoding."""
    import pyarrow as pa
    from gometry._geoparquet import _native_geoarrow_column

    poly = gm.GeometryArray([gm.box(0, 0, 1, 1)]).to_arrow()
    with pytest.raises(
        gm.GeometryError,
        match=r"encoding 'multilinestring' conflicts with embedded Arrow extension "
        r"'geoarrow\.polygon'",
    ):
        _native_geoarrow_column(
            pa, pa.chunked_array([poly]), 'multilinestring', None, None
        )

    mls = gm.GeometryArray([
        gm.MultiLineString([[(0, 0), (1, 1), (2, 0)]])
    ]).to_arrow()
    with pytest.raises(
        gm.GeometryError,
        match=r"encoding 'polygon' conflicts with embedded Arrow extension "
        r"'geoarrow\.multilinestring'",
    ):
        _native_geoarrow_column(pa, pa.chunked_array([mls]), 'polygon', None, None)


def test_d10_matching_native_extension_still_decodes() -> None:
    """Positive: matching embedded extension + declared encoding keeps kind."""
    import pyarrow as pa
    from gometry._geoparquet import _native_geoarrow_column

    mp = gm.GeometryArray([gm.MultiPoint([(0, 0), (1, 1)])]).to_arrow()
    labeled = _native_geoarrow_column(
        pa, pa.chunked_array([mp]), 'multipoint', None, None
    )
    assert gm.from_arrow(labeled).to_wkt() == ['MULTIPOINT ((0 0), (1 1))']


def test_d10_native_public_roundtrip_preserves_kinds(parquet_path: str) -> None:
    """Positive: public native GeoParquet preserves MultiPoint / LineString kinds."""
    values = gm.GeometryArray([
        gm.MultiPoint([(0, 0), (1, 1)], crs=4326),
    ])
    values.to_geoparquet(parquet_path, encoding='native')
    restored = _read_geometry(parquet_path)
    _assert_roundtrip(values, restored)
    assert restored.geometry_type[0] == 'MultiPoint'

    line = gm.GeometryArray([gm.LineString([(0, 0), (1, 1)], crs=4326)])
    line.to_geoparquet(parquet_path, encoding='native')
    restored_line = _read_geometry(parquet_path)
    _assert_roundtrip(line, restored_line)
    assert restored_line.geometry_type[0] == 'LineString'


def _forge_geo_column_metadata(
    parquet_path: str,
    *,
    encoding: str,
    geometry_types: list[str],
    strip_extension: bool = False,
) -> None:
    """Rewrite geo column encoding/types; optionally strip Arrow extension types."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(parquet_path)
    if strip_extension:
        columns = []
        fields = []
        for index in range(table.num_columns):
            column = table.column(index)
            field = table.schema.field(index)
            storage_type = getattr(field.type, 'storage_type', field.type)
            if getattr(field.type, 'extension_name', None) is not None:
                storage_chunks = [
                    getattr(chunk, 'storage', chunk) for chunk in column.chunks
                ]
                column = pa.chunked_array(storage_chunks, type=storage_type)
                field = pa.field(field.name, storage_type, nullable=field.nullable)
            columns.append(column)
            fields.append(field)
        table = pa.Table.from_arrays(columns, schema=pa.schema(fields, metadata=table.schema.metadata))
    meta = json.loads(table.schema.metadata[b'geo'])
    column = meta['columns']['geometry']
    column['encoding'] = encoding
    column['geometry_types'] = geometry_types
    table = table.replace_schema_metadata({
        b'geo': json.dumps(meta).encode('utf-8')
    })
    pq.write_table(table, parquet_path)


def test_d10_public_path_rejects_multipoint_relabeled_as_linestring(
    parquet_path: str,
) -> None:
    """Public native write+forge: MultiPoint must not become LineString."""
    values = gm.GeometryArray([gm.MultiPoint([(0, 0), (1, 1)], crs=4326)])
    values.to_geoparquet(parquet_path, encoding='native')
    _forge_geo_column_metadata(
        parquet_path, encoding='linestring', geometry_types=['LineString']
    )
    with pytest.raises(gm.GeometryError, match=r'linestring|multipoint|physical storage'):
        gm.from_geoparquet(parquet_path)


def test_d10_public_path_rejects_polygon_relabeled_as_multilinestring(
    parquet_path: str,
) -> None:
    """Public native write+forge: Polygon must not become MultiLineString."""
    values = gm.GeometryArray([gm.box(0, 0, 1, 1, crs=4326)])
    values.to_geoparquet(parquet_path, encoding='native')
    _forge_geo_column_metadata(
        parquet_path,
        encoding='multilinestring',
        geometry_types=['MultiLineString'],
    )
    with pytest.raises(
        gm.GeometryError, match=r'multilinestring|polygon|physical storage'
    ):
        gm.from_geoparquet(parquet_path)


def test_d10_plain_storage_rejects_encoding_geometry_types_mismatch(
    parquet_path: str,
) -> None:
    """Plain parquet storage (no extension): encoding must agree with geometry_types."""
    values = gm.GeometryArray([gm.MultiPoint([(0, 0), (1, 1)], crs=4326)])
    values.to_geoparquet(parquet_path, encoding='native')
    _forge_geo_column_metadata(
        parquet_path,
        encoding='linestring',
        geometry_types=['MultiPoint'],  # encoding-only lie; types still MultiPoint
        strip_extension=True,
    )
    with pytest.raises(
        gm.GeometryError,
        match=r'incompatible|geometry_types|do not match|ambiguous|physical storage|requires embedded',
    ):
        gm.from_geoparquet(parquet_path)


def test_d10_plain_storage_rejects_wrong_depth_encoding(
    parquet_path: str,
) -> None:
    """Plain storage without extension: depth-incompatible encoding is rejected."""
    values = gm.GeometryArray([gm.MultiPoint([(0, 0), (1, 1)], crs=4326)])
    values.to_geoparquet(parquet_path, encoding='native')
    _forge_geo_column_metadata(
        parquet_path,
        encoding='point',  # depth 0; multipoint storage is depth 1
        geometry_types=['Point'],
        strip_extension=True,
    )
    with pytest.raises(gm.GeometryError, match=r'physical storage|storage layout'):
        gm.from_geoparquet(parquet_path)


def test_d10_plain_storage_declared_encoding_resolves_depth(
    parquet_path: str,
) -> None:
    """R09: declared encoding + list depth resolve kind without ExtensionType.

    MultiPoint storage re-labeled as linestring (same list depth) is accepted
    as LineString — GeoParquet 1.1 encoding is authoritative. D10 still rejects
    when an *embedded* extension disagrees (see sister tests).
    """
    values = gm.GeometryArray([gm.MultiPoint([(0, 0), (1, 1)], crs=4326)])
    values.to_geoparquet(parquet_path, encoding='native')
    _forge_geo_column_metadata(
        parquet_path,
        encoding='linestring',
        geometry_types=['LineString'],
        strip_extension=True,
    )
    restored, _ = gm.from_geoparquet(parquet_path)
    assert list(restored.geometry_type) == ['LineString']


def test_d10_plain_storage_polygon_as_multilinestring_via_encoding(
    parquet_path: str,
) -> None:
    """Same-depth polygon storage + multilinestring encoding follows the declaration."""
    values = gm.GeometryArray([gm.box(0, 0, 1, 1, crs=4326)])
    values.to_geoparquet(parquet_path, encoding='native')
    _forge_geo_column_metadata(
        parquet_path,
        encoding='multilinestring',
        geometry_types=['MultiLineString'],
        strip_extension=True,
    )
    restored, _ = gm.from_geoparquet(parquet_path)
    assert list(restored.geometry_type) == ['MultiLineString']


def test_d10_depth_ambiguous_plain_storage_with_matching_encoding(
    parquet_path: str,
) -> None:
    """Bare multipoint storage + matching multipoint encoding is legal (R09)."""
    values = gm.GeometryArray([gm.MultiPoint([(0, 0), (1, 1)], crs=4326)])
    values.to_geoparquet(parquet_path, encoding='native')
    _forge_geo_column_metadata(
        parquet_path,
        encoding='multipoint',
        geometry_types=['MultiPoint'],
        strip_extension=True,
    )
    restored, _ = gm.from_geoparquet(parquet_path)
    assert list(restored.geometry_type) == ['MultiPoint']
    assert restored.to_wkt() == values.to_wkt()


def test_d10_unique_depth_plain_point_still_reads(parquet_path: str) -> None:
    """Point (depth 0) is unique — plain storage without extension still decodes."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    point_type = pa.struct([
        pa.field('x', pa.float64(), nullable=False),
        pa.field('y', pa.float64(), nullable=False),
    ])
    table = pa.table({'geometry': pa.array([{'x': 1.0, 'y': 2.0}], type=point_type)})
    metadata = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'point',
                'geometry_types': ['Point'],
                'crs': None,
            }
        },
    }
    table = table.replace_schema_metadata({b'geo': json.dumps(metadata).encode()})
    pq.write_table(table, parquet_path)
    assert canon(_read_geometry(parquet_path)) == ['POINT (1 2)']


# --- D13: every geo.columns entry must exist and match physical encoding ---


def test_d13_declared_columns_must_exist_and_match_encoding() -> None:
    """Exact audit repro: ghost names and non-geometry 'id' as WKB must error."""
    import pyarrow as pa
    from gometry._geoparquet import _validate_attribute_columns

    metadata = {
        'columns': {
            'geometry': {'encoding': 'WKB', 'geometry_types': ['Point']},
            'id': {'encoding': 'WKB', 'geometry_types': []},
            'ghost': {'encoding': 'WKB', 'geometry_types': []},
        }
    }
    schema = pa.schema([
        pa.field('geometry', pa.binary()),
        pa.field('id', pa.int64()),
    ])
    with pytest.raises(gm.GeometryError) as raised:
        _validate_attribute_columns(schema, metadata, None)
    message = str(raised.value)
    # Either contradiction must fail closed — never silently return [].
    assert (
        "encoding 'WKB' does not match physical storage" in message
        or 'declared in geo metadata is not present' in message
    )


def test_d13_ghost_column_raises() -> None:
    import pyarrow as pa
    from gometry._geoparquet import _validate_attribute_columns

    metadata = {
        'columns': {
            'geometry': {'encoding': 'WKB', 'geometry_types': ['Point']},
            'ghost': {'encoding': 'WKB', 'geometry_types': []},
        }
    }
    schema = pa.schema([
        pa.field('geometry', pa.binary()),
        pa.field('id', pa.int64()),
    ])
    with pytest.raises(
        gm.GeometryError,
        match=r"geometry column 'ghost' declared in geo metadata is not present",
    ):
        _validate_attribute_columns(schema, metadata, None)


def test_d13_int64_declared_wkb_raises_and_does_not_drop_attribute() -> None:
    import pyarrow as pa
    from gometry._geoparquet import _validate_attribute_columns

    metadata = {
        'columns': {
            'geometry': {'encoding': 'WKB', 'geometry_types': ['Point']},
            'id': {'encoding': 'WKB', 'geometry_types': []},
        }
    }
    schema = pa.schema([
        pa.field('geometry', pa.binary()),
        pa.field('id', pa.int64()),
    ])
    with pytest.raises(
        gm.GeometryError,
        match=r"column 'id' encoding 'WKB' does not match physical storage",
    ):
        _validate_attribute_columns(schema, metadata, None)

    # Real attribute path: id not listed under geo.columns remains selectable.
    clean = {
        'columns': {
            'geometry': {'encoding': 'WKB', 'geometry_types': ['Point']},
        }
    }
    attrs = _validate_attribute_columns(schema, clean, None)
    assert attrs == ['id']


def test_d13_public_roundtrip_preserves_attributes(parquet_path: str) -> None:
    """Positive: attributes survive public write+read for WKB and native."""
    import pyarrow as pa

    values = gm.points([1.0, 3.0], [2.0, 4.0], crs=4326)
    attributes = pa.table({'id': [10, 20], 'name': ['a', 'b']})
    values.to_geoparquet(parquet_path, attributes=attributes, encoding='wkb')
    restored, restored_attrs = gm.from_geoparquet(parquet_path)
    _assert_roundtrip(values, restored)
    assert restored_attrs.equals(attributes)
    assert restored.crs == 'EPSG:4326'

    values.to_geoparquet(parquet_path, attributes=attributes, encoding='native')
    restored_n, restored_attrs_n = gm.from_geoparquet(parquet_path)
    _assert_roundtrip(values, restored_n)
    assert restored_attrs_n.equals(attributes)
    assert restored_n.crs == 'EPSG:4326'


def test_geoparquet_sized_array_like_attributes_preserve_native_types(
    parquet_path: str,
) -> None:
    """Sized array-likes hand off to PyArrow natively (no islice materialization).

    Regression: ``list(islice(...))`` mangled numpy datetime64 (TypeError) and
    degraded pandas.Categorical to plain strings. Sized columns are proportional
    data — pass them through for date32 / dictionary encodings.
    """
    import datetime

    import numpy as np
    import pandas as pd
    import pyarrow as pa

    values = gm.points([1.0, 3.0], [2.0, 4.0], crs=4326)

    # numpy datetime64[D] → Arrow date32 (native conversion, not list of dates).
    when = np.array(['2020-01-01', '2020-01-02'], dtype='datetime64[D]')
    values.to_geoparquet(parquet_path, attributes={'when': when})
    restored, attrs = gm.from_geoparquet(parquet_path)
    _assert_roundtrip(values, restored)
    assert pa.types.is_date32(attrs.schema.field('when').type)
    assert attrs.column('when').to_pylist() == [
        datetime.date(2020, 1, 1),
        datetime.date(2020, 1, 2),
    ]

    # pandas.Categorical → dictionary-encoded Arrow (not plain strings).
    cat = pd.Categorical(['a', 'b'], categories=['a', 'b', 'c'])
    values.to_geoparquet(parquet_path, attributes={'label': cat})
    restored, attrs = gm.from_geoparquet(parquet_path)
    _assert_roundtrip(values, restored)
    assert pa.types.is_dictionary(attrs.schema.field('label').type)
    assert attrs.column('label').to_pylist() == ['a', 'b']

    # int64 numpy + plain list still work.
    ids = np.array([10, 20], dtype=np.int64)
    values.to_geoparquet(parquet_path, attributes={'id': ids, 'name': ['x', 'y']})
    restored, attrs = gm.from_geoparquet(parquet_path)
    _assert_roundtrip(values, restored)
    assert pa.types.is_int64(attrs.schema.field('id').type)
    assert attrs.to_pydict() == {'id': [10, 20], 'name': ['x', 'y']}


def test_geoparquet_unsized_attribute_generator_length_mismatch_rejects(
    parquet_path: str,
) -> None:
    """Unsized iterators stay bounded (row_count+1) and still reject wrong length."""
    import itertools

    values = gm.points([1.0], [2.0], crs=4326)
    with pytest.raises(gm.GeometryError, match='does not match geometry length'):
        values.to_geoparquet(
            parquet_path,
            attributes={'x': itertools.repeat(1)},
        )


# --- D12: filtered subset of declared geometry_types must decode ---


def test_d12_filtered_subset_of_declared_types_decodes() -> None:
    """Exact audit repro: actual Point under declared [Point, LineString] is valid."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    column = pa.chunked_array([
        pa.array([gm.Point(1, 2).to_wkb()], type=pa.binary())
    ])
    out = _decode_geometry_column(
        pa,
        column,
        {'geometry_types': ['Point', 'LineString']},
        'geometry',
        'WKB',
        None,
        None,
    )
    assert out.to_wkt() == ['POINT (1 2)']


def test_d12_actual_type_outside_declared_set_errors() -> None:
    """Actual type not in a nonempty declared inventory still raises."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    column = pa.chunked_array([
        pa.array([gm.Point(1, 2).to_wkb()], type=pa.binary())
    ])
    with pytest.raises(
        gm.GeometryError,
        match=r"geometry_types \['LineString'\] do not cover \['Point'\]",
    ):
        _decode_geometry_column(
            pa,
            column,
            {'geometry_types': ['LineString']},
            'geometry',
            'WKB',
            None,
            None,
        )


def test_d12_exact_match_and_empty_declaration_still_work() -> None:
    """Positive: exact inventory match and empty (unknown) declaration still decode."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    column = pa.chunked_array([
        pa.array([gm.Point(1, 2).to_wkb()], type=pa.binary())
    ])
    exact = _decode_geometry_column(
        pa, column, {'geometry_types': ['Point']}, 'geometry', 'WKB', None, None
    )
    assert exact.to_wkt() == ['POINT (1 2)']
    empty = _decode_geometry_column(
        pa, column, {'geometry_types': []}, 'geometry', 'WKB', None, None
    )
    assert empty.to_wkt() == ['POINT (1 2)']


# --- D14.1: M ordinates rejected independent of geometry_types ---


def test_d14_1_m_rejected_with_empty_geometry_types() -> None:
    """Exact audit repro: POINT M must not slip through geometry_types: []."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    p = gm.Point(1, 2, m=3)
    column = pa.chunked_array([pa.array([p.to_wkb()], type=pa.binary())])
    with pytest.raises(
        gm.GeometryError,
        match=r'geoparquet 1\.x does not support M ordinates',
    ):
        _decode_geometry_column(
            pa, column, {'geometry_types': []}, 'geometry', 'WKB', None, None
        )


def test_d14_1_m_rejected_with_nonempty_geometry_types() -> None:
    """M is rejected even when a type declaration is present."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    p = gm.Point(1, 2, m=3)
    column = pa.chunked_array([pa.array([p.to_wkb()], type=pa.binary())])
    with pytest.raises(
        gm.GeometryError,
        match=r'geoparquet 1\.x does not support M ordinates',
    ):
        _decode_geometry_column(
            pa,
            column,
            {'geometry_types': ['Point']},
            'geometry',
            'WKB',
            None,
            None,
        )


def test_d14_1_z_geometries_still_decode_and_roundtrip(parquet_path: str) -> None:
    """Positive: Z (supported by GeoParquet) still decodes and round-trips."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    p = gm.Point(1, 2, z=3)
    column = pa.chunked_array([pa.array([p.to_wkb()], type=pa.binary())])
    out = _decode_geometry_column(
        pa, column, {'geometry_types': ['Point Z']}, 'geometry', 'WKB', None, None
    )
    assert out.to_wkt() == ['POINT Z (1 2 3)']

    values = gm.GeometryArray([gm.Point(1, 2, z=3, crs=4326)])
    values.to_geoparquet(parquet_path, encoding='wkb')
    restored = _read_geometry(parquet_path)
    _assert_roundtrip(values, restored)
    assert restored[0].has_z
    assert not restored.any_has_m


# --- D19 / N6: empty/all-null declared-vs-storage axes reconciliation ---


def test_n6_empty_all_null_wkb_declared_z_imports() -> None:
    """N6: empty/all-null WKB trusts declared Point Z (binary storage is not XY).

    WKB encodes 3D inside the blob; inventing an XY inventory from binary
    storage over-rejected valid GeoParquet 1.1 column inventories.
    """
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    empty = pa.chunked_array([pa.array([], type=pa.binary())])
    empty_out = _decode_geometry_column(
        pa, empty, {'geometry_types': ['Point Z']}, 'geometry', 'WKB', None, None
    )
    assert len(empty_out) == 0

    all_null = pa.chunked_array([pa.array([None, None], type=pa.binary())])
    null_out = _decode_geometry_column(
        pa, all_null, {'geometry_types': ['Point Z']}, 'geometry', 'WKB', None, None
    )
    assert len(null_out) == 2
    assert all(null_out.is_missing)

    # Non-empty XY WKB + declared Z still rejects (decoded content is real).
    xy = pa.chunked_array([pa.array([gm.Point(1, 2).to_wkb()], type=pa.binary())])
    with pytest.raises(
        gm.GeometryError,
        match=r"geometry_types \['Point Z'\] do not cover \['Point'\]",
    ):
        _decode_geometry_column(
            pa, xy, {'geometry_types': ['Point Z']}, 'geometry', 'WKB', None, None
        )


def test_n6_filtered_all_null_wkb_row_group_declared_point_z(parquet_path: str) -> None:
    """Public path: row_groups=[0] all-null WKB declared Point Z imports 2 nulls."""
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq

    metadata = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'WKB',
                'geometry_types': ['Point Z'],
                'crs': None,
            }
        },
    }
    schema = pa.schema(
        [pa.field('geometry', pa.binary())],
        metadata={b'geo': json.dumps(metadata).encode()},
    )
    table = pa.Table.from_arrays(
        [pa.array([None, None, gm.Point(1, 2, z=3).to_wkb()])],
        schema=schema,
    )
    pq.write_table(table, parquet_path, row_group_size=2)

    full, _ = gm.from_geoparquet(parquet_path)
    assert len(full) == 3
    assert full.is_missing[0] and full.is_missing[1]
    assert full[2].to_wkt() == 'POINT Z (1 2 3)'

    # Filtered first row group is all-null binary — must not invent XY inventory.
    filtered, _ = gm.from_geoparquet(parquet_path, row_groups=[0])
    assert len(filtered) == 2
    assert all(filtered.is_missing)


def test_d19_empty_consistent_xy_imports() -> None:
    """Consistent empty declaration (Point on XY WKB) still imports empty."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    empty = pa.chunked_array([pa.array([], type=pa.binary())])
    out = _decode_geometry_column(
        pa, empty, {'geometry_types': ['Point']}, 'geometry', 'WKB', None, None
    )
    assert len(out) == 0
    assert out.common_coordinate_axes == 'XY'

    all_null = pa.chunked_array([pa.array([None], type=pa.binary())])
    null_out = _decode_geometry_column(
        pa, all_null, {'geometry_types': ['Point']}, 'geometry', 'WKB', None, None
    )
    assert len(null_out) == 1
    assert null_out.is_missing[0]


def test_d19_empty_native_xy_declared_z_still_rejects() -> None:
    """D19 intact: empty native XY storage + declared Point Z rejects.

    F6 rejects at the structural-axes check (declared axes exceed storage);
    either that message or the older actual⊆declared cover message is a reject.
    """
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    xy = pa.struct([
        ('x', pa.float64()),
        ('y', pa.float64()),
    ])
    empty = pa.chunked_array([pa.array([], type=xy)])
    with pytest.raises(
        gm.GeometryError,
        match=r"exceeds structural storage axes|do not cover",
    ):
        _decode_geometry_column(
            pa,
            empty,
            {'geometry_types': ['Point Z']},
            'geometry',
            'point',
            None,
            None,
        )


def test_d19_empty_native_xyz_declared_z_imports() -> None:
    """Native XYZ storage + declared Point Z is consistent and imports empty."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    xyz = pa.struct([
        ('x', pa.float64()),
        ('y', pa.float64()),
        ('z', pa.float64()),
    ])
    empty = pa.chunked_array([pa.array([], type=xyz)])
    out = _decode_geometry_column(
        pa, empty, {'geometry_types': ['Point Z']}, 'geometry', 'point', None, None
    )
    assert len(out) == 0

    # Declared XY on XYZ storage is still a mismatch (same ⊆ rule as non-empty).
    with pytest.raises(
        gm.GeometryError,
        match=r"geometry_types \['Point'\] do not cover \['Point Z'\]",
    ):
        _decode_geometry_column(
            pa, empty, {'geometry_types': ['Point']}, 'geometry', 'point', None, None
        )


def test_d19_empty_native_xym_storage_rejects_m() -> None:
    """Empty native storage with an M field rejects GeoParquet 1.x M schema-wide."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    xym = pa.struct([
        ('x', pa.float64()),
        ('y', pa.float64()),
        ('m', pa.float64()),
    ])
    empty = pa.chunked_array([pa.array([], type=xym)])
    with pytest.raises(
        gm.GeometryError,
        match=r'geoparquet 1\.x does not support M ordinates',
    ):
        _decode_geometry_column(
            pa, empty, {'geometry_types': ['Point']}, 'geometry', 'point', None, None
        )


# --- D14.2: bbox ordering + orientation assertion ---


def test_d14_2_bbox_ymin_gt_ymax_rejected() -> None:
    """Exact audit repro: bbox [0,5,10,4] (ymin > ymax) is malformed."""
    from gometry._geoparquet import _validate_column_metadata

    with pytest.raises(
        gm.GeometryError,
        match=r'bbox min ordinates must not exceed max ordinates',
    ):
        _validate_column_metadata(
            {
                'columns': {
                    'geometry': {
                        'encoding': 'WKB',
                        'geometry_types': [],
                        'bbox': [0, 5, 10, 4],
                    }
                }
            },
            'geometry',
        )


def test_d14_2_valid_bbox_4_and_6_accepted() -> None:
    """Positive: ordered 4- and 6-element finite bboxes pass metadata validation."""
    from gometry._geoparquet import _validate_column_metadata

    meta4, encoding4, _, _ = _validate_column_metadata(
        {
            'columns': {
                'geometry': {
                    'encoding': 'WKB',
                    'geometry_types': [],
                    'bbox': [0, 1, 10, 4],
                }
            }
        },
        'geometry',
    )
    assert encoding4 == 'WKB'
    assert meta4['bbox'] == [0, 1, 10, 4]

    meta6, encoding6, _, _ = _validate_column_metadata(
        {
            'columns': {
                'geometry': {
                    'encoding': 'WKB',
                    'geometry_types': [],
                    'bbox': [0, 1, 2, 10, 4, 5],
                }
            }
        },
        'geometry',
    )
    assert encoding6 == 'WKB'
    assert meta6['bbox'] == [0, 1, 2, 10, 4, 5]


def test_d14_2_bbox_z_and_x_ordering_rejected() -> None:
    """Z inversion always rejected; X inversion only for non-geographic CRS.

    Under default CRS84 / geographic CRS, west > east is the legal
    antimeridian wrap (R10). Projected CRS still requires xmin <= xmax.
    """
    from gometry._geoparquet import _validate_column_metadata

    # Geographic default (absent CRS → CRS84): antimeridian wrap accepted.
    meta, _, crs, _ = _validate_column_metadata(
        {
            'columns': {
                'geometry': {
                    'encoding': 'WKB',
                    'geometry_types': [],
                    'bbox': [10, 0, 0, 4],  # xmin > xmax, geographic wrap
                }
            }
        },
        'geometry',
    )
    assert crs == 'OGC:CRS84'
    assert meta['bbox'] == [10, 0, 0, 4]

    # Projected CRS: X inversion remains illegal.
    with pytest.raises(gm.GeometryError, match=r'bbox min ordinates'):
        _validate_column_metadata(
            {
                'columns': {
                    'geometry': {
                        'encoding': 'WKB',
                        'geometry_types': [],
                        'crs': gm.CRS(3857).to_projjson_dict(),
                        'bbox': [10, 0, 0, 4],
                    }
                }
            },
            'geometry',
        )
    with pytest.raises(gm.GeometryError, match=r'bbox min ordinates'):
        _validate_column_metadata(
            {
                'columns': {
                    'geometry': {
                        'encoding': 'WKB',
                        'geometry_types': [],
                        'bbox': [0, 0, 5, 1, 1, 2],  # zmin > zmax
                    }
                }
            },
            'geometry',
        )


def test_d14_2_orientation_assertion_rejects_clockwise_rings() -> None:
    """When orientation is counterclockwise, CW polygonal rings must error."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    cw = gm.Polygon([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)])
    column = pa.chunked_array([pa.array([cw.to_wkb()], type=pa.binary())])
    with pytest.raises(
        gm.GeometryError,
        match=r"asserts orientation 'counterclockwise'",
    ):
        _decode_geometry_column(
            pa,
            column,
            {
                'geometry_types': ['Polygon'],
                'orientation': 'counterclockwise',
            },
            'geometry',
            'WKB',
            None,
            None,
        )


def test_d14_2_orientation_assertion_accepts_ccw_rings() -> None:
    """Positive: CCW exteriors under orientation assertion still decode."""
    import pyarrow as pa
    from gometry._geoparquet import _decode_geometry_column

    ccw = gm.box(0, 0, 1, 1)
    column = pa.chunked_array([pa.array([ccw.to_wkb()], type=pa.binary())])
    out = _decode_geometry_column(
        pa,
        column,
        {
            'geometry_types': ['Polygon'],
            'orientation': 'counterclockwise',
        },
        'geometry',
        'WKB',
        None,
        None,
    )
    assert out.to_wkt() == ['POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))']
    assert out[0].exterior.is_ccw


# --- P19: declared-column validation must be O(schema + metadata) ---


def test_p19_declared_geometry_validation_is_subquadratic() -> None:
    """P19: no per-declared-column ``count``/``index`` over schema.names.

    A schema with N columns that are ALL declared as WKB geometries must
    finish in work linear in N. Under the old O(N²) ``list.count`` /
    ``list.index`` loop, membership probes alone scale as ~N²/2; with a
    single name→indices map they stay O(N).

    The test instruments a schema-names proxy that counts linear scans
    (``count`` / ``index`` / ``__contains__`` each cost one full pass) and
    rejects any super-linear budget. Wall-clock is not used.
    """
    import pyarrow as pa
    from gometry._geoparquet import _validate_declared_geometry_columns

    n = 2048
    names = [f'g{i}' for i in range(n)]

    class CountingNames(list):
        """List that counts every full-sequence membership/search scan."""

        def __init__(self, values: list[str]) -> None:
            super().__init__(values)
            self.linear_scans = 0

        def count(self, value: object) -> int:  # type: ignore[override]
            self.linear_scans += 1
            return super().count(value)

        def index(self, value: object, *args: object) -> int:  # type: ignore[override]
            self.linear_scans += 1
            return super().index(value, *args)  # type: ignore[arg-type]

        def __contains__(self, value: object) -> bool:
            self.linear_scans += 1
            return super().__contains__(value)

    counting_names = CountingNames(names)

    class SchemaProxy:
        def __init__(self, real: pa.Schema, name_list: CountingNames) -> None:
            self._real = real
            self.names = name_list

        def field(self, index: int) -> pa.Field:
            return self._real.field(index)

    real_schema = pa.schema([pa.field(name, pa.binary()) for name in names])
    schema = SchemaProxy(real_schema, counting_names)
    metadata = {
        'columns': {
            name: {'encoding': 'WKB', 'geometry_types': ['Point']} for name in names
        }
    }

    validated = _validate_declared_geometry_columns(
        pa, schema, metadata
    )
    assert validated == frozenset(names)
    # One materialization pass may walk names once (list(schema.names) does
    # not call count/index/contains). Super-linear would call count+index per
    # declared column → 2N scans. Budget: zero scans from those methods.
    assert counting_names.linear_scans == 0, (
        f'expected 0 count/index/contains scans after name→indices map, '
        f'got {counting_names.linear_scans} (quadratic path still live)'
    )

    # Positive selection path also stays set-based (no per-name schema scan).
    from gometry._geoparquet import _validate_attribute_columns

    attr_names = CountingNames([*names, 'attr'])
    real_attrs = pa.schema([
        *[pa.field(name, pa.binary()) for name in names],
        pa.field('attr', pa.int64()),
    ])
    attr_schema = SchemaProxy(real_attrs, attr_names)
    meta_one = {
        'columns': {
            'g0': {'encoding': 'WKB', 'geometry_types': ['Point']},
        }
    }
    # Fresh names list after materialization inside the helper.
    selected = _validate_attribute_columns(attr_schema, meta_one, ['attr'] * 1)
    assert selected == ['attr']
    # Materialize names once + set construction may iterate; must not do
    # per-selected-column membership against the live schema.names object.
    # With the set cache, contains on schema.names after materialization is 0.
    # (list(schema.names) copies without __contains__.)
    assert attr_names.linear_scans == 0


def test_p19_ghost_and_duplicate_still_rejected() -> None:
    """P19 behavior parity: ghost names and duplicate schema columns still error."""
    import pyarrow as pa
    from gometry._geoparquet import _validate_declared_geometry_columns

    schema = pa.schema([
        pa.field('geometry', pa.binary()),
        pa.field('id', pa.int64()),
    ])
    with pytest.raises(
        gm.GeometryError,
        match=r"geometry column 'ghost' declared in geo metadata is not present",
    ):
        _validate_declared_geometry_columns(
            pa,
            schema,
            {
                'columns': {
                    'geometry': {'encoding': 'WKB', 'geometry_types': []},
                    'ghost': {'encoding': 'WKB', 'geometry_types': []},
                }
            },
        )

    # Duplicate column name in schema (pyarrow allows via from_arrays path).
    dup_schema = pa.schema([
        pa.field('geometry', pa.binary()),
        pa.field('geometry', pa.binary()),
    ])
    with pytest.raises(
        gm.GeometryError,
        match=r"geometry column 'geometry' must appear exactly once",
    ):
        _validate_declared_geometry_columns(
            pa,
            dup_schema,
            {'columns': {'geometry': {'encoding': 'WKB', 'geometry_types': []}}},
        )


# --- D14.3: one-shot columns / row_groups materialization ---


def test_d14_3_generator_columns_honored_by_validator() -> None:
    """Exact audit repro: generator columns must not be consumed to empty."""
    import pyarrow as pa
    from gometry._geoparquet import _validate_attribute_columns

    schema = pa.schema([
        pa.field('geometry', pa.binary()),
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
    ])
    metadata = {
        'columns': {
            'geometry': {'encoding': 'WKB', 'geometry_types': []},
        }
    }
    selected = _validate_attribute_columns(
        schema, metadata, (x for x in ['id'])
    )
    assert selected == ['id']


def test_d14_3_generator_columns_and_row_groups_public_path(
    parquet_path: str,
) -> None:
    """Public from_geoparquet honors generator columns and row_groups."""
    import pyarrow as pa

    values = gm.points([1.0, 3.0, 5.0], [2.0, 4.0, 6.0], crs=4326)
    attributes = pa.table({
        'id': [10, 20, 30],
        'name': ['a', 'b', 'c'],
    })
    values.to_geoparquet(
        parquet_path,
        attributes=attributes,
        encoding='wkb',
        row_group_size=1,
    )
    restored, attrs = gm.from_geoparquet(
        parquet_path,
        columns=(name for name in ['id']),
        row_groups=(i for i in [0, 2]),
    )
    assert restored.to_wkt() == ['POINT (1 2)', 'POINT (5 6)']
    assert attrs.column_names == ['id']
    assert attrs['id'].to_pylist() == [10, 30]
    assert restored.crs == 'EPSG:4326'


# --- Cross-defect positives: WKB + native public round-trips stay green ---


def test_d12_d14_public_roundtrips_wkb_and_native(parquet_path: str) -> None:
    """No over-rejection: WKB and homogeneous native keep attrs + CRS/epoch/kinds."""
    import pyarrow as pa

    values = gm.GeometryArray([
        gm.Point(1.0, 2.0, crs=4326),
        gm.Point(3.0, 4.0, crs=4326),
    ])
    values = values.set_epoch(2020.5)
    attributes = pa.table({'id': [1, 2], 'label': ['x', 'y']})

    for encoding in ('wkb', 'native'):
        values.to_geoparquet(
            parquet_path, attributes=attributes, encoding=encoding
        )
        restored, restored_attrs = gm.from_geoparquet(parquet_path)
        _assert_roundtrip(values, restored)
        assert restored.epoch == 2020.5
        assert list(restored.geometry_type) == ['Point', 'Point']
        assert restored_attrs.equals(attributes)
        # Writer-emitted bbox/type declaration is accepted on the way back.
        import pyarrow.parquet as pq

        meta = json.loads(pq.read_schema(parquet_path).metadata[b'geo'])
        col = meta['columns']['geometry']
        assert col['geometry_types'] == ['Point']
        assert col['bbox'] == [1.0, 2.0, 3.0, 4.0]


def test_from_geoparquet_lying_len_columns_and_row_groups(parquet_path: str) -> None:
    """m08: lying ``__len__`` must not MemoryError before iterating columns/row_groups."""
    import sys

    import pyarrow as pa

    values = gm.GeometryArray([
        gm.Point(1.0, 2.0, crs=4326),
        gm.Point(3.0, 4.0, crs=4326),
        gm.Point(5.0, 6.0, crs=4326),
    ])
    attributes = pa.table({'id': [10, 20, 30]})
    values.to_geoparquet(
        parquet_path,
        attributes=attributes,
        encoding='wkb',
        row_group_size=1,
    )

    class _LieColumns:
        def __iter__(self):
            yield 'id'

        def __len__(self) -> int:
            return sys.maxsize

    class _LieRowGroups:
        def __iter__(self):
            yield 0
            yield 2

        def __len__(self) -> int:
            return sys.maxsize

    restored, attrs = gm.from_geoparquet(
        parquet_path,
        columns=_LieColumns(),
        row_groups=_LieRowGroups(),
    )
    assert restored.to_wkt() == ['POINT (1 2)', 'POINT (5 6)']
    assert attrs.column_names == ['id']
    assert attrs['id'].to_pylist() == [10, 30]

    # Positive: honest sequences still work.
    restored2, attrs2 = gm.from_geoparquet(
        parquet_path, columns=['id'], row_groups=[0, 2]
    )
    assert restored2.to_wkt() == restored.to_wkt()
    assert attrs2['id'].to_pylist() == [10, 30]


# ---------------------------------------------------------------------------
# F6 — native GeoParquet rejects impossible mixed-axes geometry_types
# ---------------------------------------------------------------------------


def test_f6_native_point_z_over_xy_storage_rejected(parquet_path: str) -> None:
    """F6: declared Point Z over fixed XY native struct storage must reject.

    Base-kind stripping + actual ⊆ declared let a matching 'Point' entry mask
    an impossible 'Point Z' inventory entry on structural XY storage.
    """
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq

    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    column = pa.array([{'x': 1.0, 'y': 2.0}], type=xy)
    md = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'point',
                'geometry_types': ['Point', 'Point Z'],
                'crs': None,
            }
        },
    }
    schema = pa.schema(
        [pa.field('geometry', xy)],
        metadata={b'geo': json.dumps(md).encode()},
    )
    pq.write_table(pa.table({'geometry': column}, schema=schema), parquet_path)
    with pytest.raises(gm.GeometryError, match=r'Point Z|exceeds|storage axes|geometry_types'):
        gm.from_geoparquet(parquet_path)


def test_f6_native_matching_axes_still_imports(parquet_path: str) -> None:
    """Native column whose declared types match structural axes still imports."""
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq

    xy = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    column = pa.array([{'x': 1.0, 'y': 2.0}], type=xy)
    md = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'point',
                'geometry_types': ['Point'],
                'crs': None,
            }
        },
    }
    schema = pa.schema(
        [pa.field('geometry', xy)],
        metadata={b'geo': json.dumps(md).encode()},
    )
    pq.write_table(pa.table({'geometry': column}, schema=schema), parquet_path)
    restored, _ = gm.from_geoparquet(parquet_path)
    assert restored.to_wkt() == ['POINT (1 2)']


def test_f6_wkb_point_z_declaration_still_trusted(parquet_path: str) -> None:
    """N6: WKB columns keep declared geometry_types trust (axes live in blob)."""
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq

    # WKB POINT (1 2) — 2D blob; declaration lists Point Z inventory (filtered
    # row-group style). N6: trust the declaration when storage is WKB.
    wkb_2d = gm.Point(1.0, 2.0).to_wkb()
    column = pa.array([wkb_2d], type=pa.binary())
    md = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'WKB',
                'geometry_types': ['Point', 'Point Z'],
                'crs': None,
            }
        },
    }
    schema = pa.schema(
        [pa.field('geometry', pa.binary())],
        metadata={b'geo': json.dumps(md).encode()},
    )
    pq.write_table(pa.table({'geometry': column}, schema=schema), parquet_path)
    restored, _ = gm.from_geoparquet(parquet_path)
    assert restored.to_wkt() == ['POINT (1 2)']
