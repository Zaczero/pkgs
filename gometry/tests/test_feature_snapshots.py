"""Feature values remain snapshots across import, export and caller mutation."""

import json
import pickle
import weakref
from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import gometry as gm
import numpy as np
import pytest


def test_feature_snapshot_recursively_detaches_broadcast_values() -> None:
    values = {'nested': [{'name': 'original'}]}
    rows = gm.Features(gm.points([1, 2], [3, 4]), properties=values)
    values['nested'][0]['name'] = 'changed'
    assert rows.properties[0]['nested'][0]['name'] == 'original'
    assert rows.properties[1]['nested'][0]['name'] == 'original'
    with pytest.raises(TypeError):
        rows.properties[0]['nested'][0]['name'] = 'mutated'


def test_property_projection_preserves_non_utf8_python_strings() -> None:
    name = '\ud800'
    rows = gm.Features([None], properties={name: 4})
    selected = rows.select_columns([name])
    assert selected.column(name)[:] == (4,)
    assert list(selected[0].properties) == [name]


def test_native_geojson_admission_snapshots_nested_values() -> None:
    properties = {'values': [1, {'value': 2}]}
    feature = {
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
        'properties': properties,
    }
    rows = gm.from_geojson(feature, as_features=True)
    properties['values'][1]['value'] = 9
    assert rows.properties[0]['values'][1]['value'] == 2


def test_geojson_export_and_pickle_do_not_alias_snapshot() -> None:
    rows = gm.Features(
        gm.GeometryArray([gm.Point(1, 2)]),
        properties={'nested': [{'value': 2}]},
        ids=[7],
    )
    exported = gm.to_feature_collection(rows)
    exported['features'][0]['properties']['nested'][0]['value'] = 9
    assert rows.properties[0]['nested'][0]['value'] == 2
    restored = pickle.loads(pickle.dumps(rows))
    assert restored.properties[0]['nested'][0]['value'] == 2
    assert restored.ids == (7,)


def test_cycles_are_rejected_with_property_path() -> None:
    cycle = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match=r'properties.*cyclic'):
        gm.Features(gm.GeometryArray([gm.Point(1, 2)]), properties={'cycle': cycle})


def test_cycles_crossing_a_mapping_provider_are_rejected() -> None:
    class MappingProvider:
        def keys(self):
            return ('parent',)

        def __getitem__(self, key):
            assert key == 'parent'
            return parent

    parent = {'provider': MappingProvider()}
    with pytest.raises(ValueError, match=r'properties.*cyclic'):
        gm.Features(gm.GeometryArray([gm.Point(1, 2)]), properties=parent)


def test_mapping_provider_is_consumed_once() -> None:
    class MappingProvider:
        def __init__(self):
            self.calls = 0

        def keys(self):
            self.calls += 1
            return iter(('items',))

        def __getitem__(self, key):
            assert key == 'items'
            return [1, 2]

    provider = MappingProvider()
    rows = gm.Features(gm.GeometryArray([gm.Point(1, 2)]), properties=provider)
    assert provider.calls == 1
    assert rows.properties[0]['items'] == (1, 2)


def test_feature_rows_do_not_consume_past_first_extra_row() -> None:
    def properties():
        yield {'a': 1}
        yield {'a': 2}
        raise AssertionError('read beyond first excess row')

    with pytest.raises(gm.GeometryError, match='properties length'):
        gm.Features(gm.GeometryArray([gm.Point(1, 2)]), properties=properties())


def test_nested_standard_scalars_are_preserved() -> None:
    values = {
        'day': date(2024, 2, 29),
        'instant': datetime(2024, 2, 29, tzinfo=UTC),
        'decimal': Decimal('0.000001'),
        'count': np.int64(42),
        'nested': (np.float32(1.25), None),
    }
    rows = gm.Features(gm.GeometryArray([gm.Point(1, 2)]), properties=values)
    observed = rows.properties[0]
    assert observed['day'] == date(2024, 2, 29)
    assert observed['instant'] == datetime(2024, 2, 29, tzinfo=UTC)
    assert observed['decimal'] == Decimal('0.000001')
    assert type(observed['count']) is int
    assert observed['nested'] == (1.25, None)


def test_unsupported_value_does_not_invoke_copy_hooks() -> None:
    class ApplicationObject:
        def __deepcopy__(self, memo):
            raise AssertionError('must not invoke application code')

    with pytest.raises(TypeError, match='unsupported property value'):
        gm.Features(
            gm.GeometryArray([gm.Point(1, 2)]),
            properties={'value': ApplicationObject()},
        )


def test_numpy_temporal_scalar_preserves_unit() -> None:
    value = np.datetime64('2024-02-29T00:00:00.000000001', 'ns')
    rows = gm.Features(gm.GeometryArray([gm.Point(1, 2)]), properties={'at': value})
    observed = rows.properties[0]['at']
    assert isinstance(observed, np.datetime64)
    assert observed.dtype == value.dtype
    assert observed == value


def test_arrow_temporal_scalar_preserves_submicrosecond_precision() -> None:
    pa = pytest.importorskip('pyarrow')
    scalar = pa.array([1], type=pa.timestamp('ns', tz='UTC'))[0]
    rows = gm.Features(gm.GeometryArray([gm.Point(1, 2)]), properties={'at': scalar})
    observed = rows.properties[0]['at']
    assert isinstance(observed, gm.ArrowValue)
    assert pa.array(observed).type == scalar.type
    assert observed.value == 1


def test_native_arrow_value_preserves_type_and_exact_nanoseconds() -> None:
    pa = pytest.importorskip('pyarrow')
    from gometry._lib import _arrow_value, _snapshot_arrow_array

    source = pa.array([1, 2000, None], type=pa.timestamp('ns', tz='America/New_York'))
    column = _snapshot_arrow_array(source)
    value = _arrow_value(column, 0)
    assert isinstance(value, gm.ArrowValue)
    assert value.value == 1
    assert 'America/New_York' in value.type
    with pytest.raises(ValueError, match='exactly'):
        value.as_py()
    first = pa.array(value)
    second = pa.array(value.materialize())
    assert first.type == source.type
    assert first[0].value == 1
    assert second[0].value == 1
    assert first.buffers()[1].address != second.buffers()[1].address
    restored = pickle.loads(pickle.dumps(value))
    assert restored == value
    assert pa.array(restored).type == source.type
    assert pa.array(restored)[0].value == 1
    assert _arrow_value(column, 1).as_py() == datetime(
        1969, 12, 31, 19, 0, 0, 2, tzinfo=ZoneInfo('America/New_York')
    )
    with pytest.raises(ValueError, match='null'):
        _arrow_value(column, 2)
    with pytest.raises(IndexError):
        _arrow_value(column, 3)


def test_native_arrow_value_temporal_exact_conversion() -> None:
    pa = pytest.importorskip('pyarrow')
    from datetime import time, timedelta

    from gometry._lib import _arrow_value, _snapshot_arrow_array

    duration = _snapshot_arrow_array(pa.array([-1000, -1], type=pa.duration('ns')))
    assert _arrow_value(duration, 0).as_py() == timedelta(microseconds=-1)
    with pytest.raises(ValueError, match='exactly'):
        _arrow_value(duration, 1).as_py()
    times = _snapshot_arrow_array(pa.array([3_723_004_005_000], type=pa.time64('ns')))
    assert _arrow_value(times, 0).as_py() == time(1, 2, 3, 4005)


def test_native_arrow_column_exposes_values_across_chunks_and_nested_types() -> None:
    pa = pytest.importorskip('pyarrow')
    from gometry._lib import _arrow_column, _snapshot_arrow_array

    values = pa.array(
        [
            {'name': 'west', 'scores': [1, None], 'at': 1},
            None,
            {'name': 'east', 'scores': [], 'at': 2000},
        ],
        type=pa.struct([
            ('name', pa.string()),
            ('scores', pa.list_(pa.int16())),
            ('at', pa.timestamp('ns', tz='UTC')),
        ]),
    )
    column = _arrow_column((
        _snapshot_arrow_array(values.slice(0, 1)),
        _snapshot_arrow_array(values.slice(1, 2)),
    ))
    assert len(column) == 3
    assert column[0]['name'] == 'west'
    assert column[0]['scores'] == (1, None)
    assert isinstance(column[0]['at'], gm.ArrowValue)
    assert pa.array(column[0]['at'])[0].value == 1
    assert column[1] is None
    assert column[2]['scores'] == ()
    assert pa.array(column[2]['at'])[0].value == 2000
    assert column[-1]['name'] == 'east'
    assert column[::-1][0]['name'] == 'east'


def test_arrow_c_table_preserves_attributes_and_explicit_id_role() -> None:
    arro3 = pytest.importorskip('arro3.core')
    geometry = gm.points([1, 2], [3, 4], crs=4326)
    table = arro3.Table.from_arrays(
        [
            arro3.Array.from_arrow(geometry),
            arro3.Array.from_numpy(np.array([0, 17], dtype=np.uint64)),
            arro3.Array.from_numpy(np.array([3, 5], dtype=np.int16)),
        ],
        names=['shape', 'fid', 'rank'],
    )
    table = table.with_schema(table.schema.with_metadata({'gometry:feature_id': 'fid'}))
    features = gm.from_arrow(table)
    assert isinstance(features, gm.Features)
    assert features.ids == (0, 17)
    assert features.column('rank')[:] == (3, 5)
    assert features.geometries.to_wkt() == ['POINT (1 3)', 'POINT (2 4)']
    without_ids = gm.from_arrow(table, id_column=None)
    assert without_ids.ids == (None, None)
    assert without_ids.column('fid')[:] == (0, 17)


def test_arrow_c_struct_parent_null_applies_to_all_feature_columns() -> None:
    pa = pytest.importorskip('pyarrow')
    geometry = gm.points([1, 2, 3], [4, 5, 6]).to_arrow()
    source = pa.StructArray.from_arrays(
        [geometry, pa.array([11, 13, 17])],
        names=['shape', 'rank'],
        mask=pa.array([False, True, False]),
    )

    class COnly:
        def __arrow_c_array__(self, requested_schema=None):
            return source.__arrow_c_array__(requested_schema)

    features = gm.from_arrow(COnly())
    assert isinstance(features, gm.Features)
    assert features.geometries.to_wkt() == ['POINT (1 4)', None, 'POINT (3 6)']
    assert features.column('rank')[:] == (11, None, 17)


def test_arrow_nested_scalar_does_not_expose_writable_snapshot_buffers() -> None:
    pa = pytest.importorskip('pyarrow')
    values = np.array([1, 2], dtype=np.int64)
    array = pa.ListArray.from_arrays(
        pa.array([0, 2], type=pa.int32()), pa.array(values)
    )
    rows = gm.Features(
        gm.GeometryArray([gm.Point(1, 2)]), properties={'items': array[0]}
    )
    values[0] = 99
    observed = rows.properties[0]['items']
    assert observed == (1, 2)


def test_arrow_table_snapshot_detaches_nested_dictionary_and_preserves_schema() -> None:
    pa = pytest.importorskip('pyarrow')
    from gometry._arrow_snapshot import snapshot_arrow_table

    source = bytearray(b'abc')
    dictionary = pa.Array.from_buffers(
        pa.string(),
        1,
        [None, pa.py_buffer(np.array([0, 3], dtype=np.int32)), pa.py_buffer(source)],
    )
    values = pa.DictionaryArray.from_arrays(pa.array([0, None]), dictionary)
    nested = pa.ListArray.from_arrays(pa.array([0, 2]), values)
    schema = pa.schema([pa.field('items', nested.type)], metadata={b'role': b'value'})
    table = pa.Table.from_arrays([nested], schema=schema)
    result = snapshot_arrow_table(pa, table)
    source[0] = ord('z')
    assert result.schema == schema
    assert result.column(0).to_pylist() == [['abc', None]]
    copied_dictionary = result.column(0).chunk(0).values.dictionary
    assert memoryview(copied_dictionary.buffers()[2]).readonly


def test_arrow_snapshot_preserves_binary_nested_field_metadata() -> None:
    pa = pytest.importorskip('pyarrow')
    metadata = {b'\xffkey': b'\xfe\x00value', b'text': b'ordinary'}
    field = pa.field('code', pa.int64(), nullable=False, metadata=metadata)
    source = pa.StructArray.from_arrays([pa.array([7, 11, 19])], fields=[field])
    features = gm.Features.from_columns(
        [None, None], columns={'nested': source.slice(1)}
    )
    exported = features.to_arrow().column('nested')
    assert exported.type.field('code').metadata == metadata
    assert not exported.type.field('code').nullable
    assert exported.to_pylist() == [{'code': 11}, {'code': 19}]


def test_native_arrow_snapshot_releases_producer_and_reexports_owned_buffers() -> None:
    pa = pytest.importorskip('pyarrow')
    from gometry._lib import _snapshot_arrow_array

    class Exporter:
        def __init__(self, array):
            self.array = array

        def __arrow_c_array__(self, requested_schema=None):
            return self.array.__arrow_c_array__(requested_schema)

    values = np.array([3, 5, 7], dtype=np.int64)
    source = pa.array(values)
    source_ref = weakref.ref(source)
    exporter = Exporter(source)
    owned = _snapshot_arrow_array(exporter)
    assert _snapshot_arrow_array(owned) is owned
    del source, exporter
    assert source_ref() is None
    values[:] = -1
    first = pa.array(owned)
    second = pa.array(owned)
    abandoned_capsules = owned.__arrow_c_array__()
    del abandoned_capsules, owned
    assert first.to_pylist() == [3, 5, 7]
    assert second.to_pylist() == [3, 5, 7]
    assert first.buffers()[1].address == second.buffers()[1].address
    assert memoryview(second.buffers()[1]).readonly


@pytest.mark.parametrize(
    'layout', ['list_view', 'large_list_view', 'string_view', 'run_end']
)
def test_feature_snapshot_preserves_sliced_arrow_layouts(layout) -> None:
    pa = pytest.importorskip('pyarrow')
    if layout == 'run_end':
        source = pa.RunEndEncodedArray.from_arrays(
            pa.array([2, 4], type=pa.int16()), pa.array(['north', 'south'])
        )
        expected = ['north', 'south']
    elif layout == 'string_view':
        source = pa.array(
            ['discard', 'a string stored outside the inline view', None, 'last'],
            type=pa.string_view(),
        )
        expected = ['a string stored outside the inline view', None]
    else:
        data_type = getattr(pa, layout)(pa.int64())
        source = pa.array([[2], [3, None], [], [11, 13]], type=data_type)
        expected = [[3, None], []]
    features = gm.Features.from_columns(
        [None, None], columns={'value': source.slice(1, 2)}
    )
    result = features.to_arrow().column('value')
    assert result.type == source.type
    assert result.to_pylist() == expected


def test_arrow_table_snapshot_preserves_zero_column_row_count() -> None:
    pa = pytest.importorskip('pyarrow')
    from gometry._arrow_snapshot import snapshot_arrow_table

    table = (
        pa
        .table({'temporary': [1, 2]})
        .replace_schema_metadata({b'role': b'value'})
        .drop(['temporary'])
    )
    assert table.num_rows == 2
    result = snapshot_arrow_table(pa, table)
    assert result.num_rows == 2
    assert result.schema == table.schema


@pytest.mark.parametrize('encoding', ['wkb', 'geoarrow', 'geometry'])
def test_geoparquet_features_keep_ids_attributes_and_geometry_columns(encoding) -> None:
    pytest.importorskip('pyarrow')
    import io

    geometries = gm.points([1, 2, 3], [4, 5, 6], crs=4326)
    features = gm.Features.from_columns(
        geometries,
        columns={'name': ['west', None, 'east'], 'other': geometries},
        ids=[0, None, 2**64 - 1],
    )
    stream = io.BytesIO()
    features.to_geoparquet(stream, encoding=encoding)
    assert not stream.closed
    stream.seek(0)
    restored = gm.from_geoparquet(stream)
    assert not stream.closed
    assert restored.ids == (0, None, 2**64 - 1)
    assert restored.column('name')[:] == ('west', None, 'east')
    assert list(restored.geometries.to_wkt()) == list(geometries.to_wkt())
    assert [value.to_wkt() for value in restored.column('other')] == list(
        geometries.to_wkt()
    )
    stream.seek(0)
    selected = gm.from_geoparquet(stream, columns=[])
    assert selected.ids == (0, None, 2**64 - 1)
    with pytest.raises(KeyError):
        selected.column('name')
    stream.seek(0)
    ids_as_properties = gm.from_geoparquet(stream, id_column=None, columns=['id'])
    assert ids_as_properties.ids == (None, None, None)
    assert ids_as_properties.column('id')[:] == (0, None, 2**64 - 1)


def test_geoparquet_explicit_id_role_survives_projection_and_spatial_filter() -> None:
    pytest.importorskip('pyarrow')
    import io

    features = gm.Features.from_columns(
        gm.points([1, 5, 9], [1, 5, 9]),
        columns={'id': ['a', 'b', 'c'], 'rank': [2, 4, 6]},
        ids=[0, 11, 17],
    )
    renamed = gm.from_arrow(features.to_arrow(geometry='shape', id_column='fid'))
    stream = io.BytesIO()
    renamed.to_geoparquet(stream)
    stream.seek(0)
    selected = gm.from_geoparquet(
        stream, columns=['id'], bbox=(0, 0, 6, 6), filters=[('rank', '>', 2)]
    )
    assert selected.geometries.to_wkt() == ['POINT (5 5)']
    assert selected.ids == (11,)
    assert selected.column('id')[:] == ('b',)
    assert selected.to_arrow().column_names == ['shape', 'id', 'fid']
    with pytest.raises(KeyError):
        selected.column('rank')
    stream.seek(0)
    ordinary_ids = gm.from_geoparquet(stream, id_column=None, columns=['fid'])
    assert ordinary_ids.ids == (None, None, None)
    assert ordinary_ids.column('fid')[:] == (0, 11, 17)


def test_feature_arrow_export_preserves_columns_and_explicit_id_role() -> None:
    pa = pytest.importorskip('pyarrow')
    source = np.array([1, 2], dtype=np.int64)
    times = pa.array([1, 2], type=pa.timestamp('ns', tz='UTC'))
    features = gm.Features.from_columns(
        gm.points([1, 2], [3, 4]),
        columns={'id': ['a', 'b'], 'value': source, 'time': times},
        ids=[0, 2**64 - 1],
    )
    source[0] = 99
    with pytest.raises(ValueError, match='ID column'):
        features.to_arrow()
    table = features.to_arrow(id_column='fid', geometry='shape')
    assert table.column_names == ['shape', 'id', 'value', 'time', 'fid']
    assert table.schema.metadata[b'gometry:feature_id'] == b'fid'
    assert table['fid'].type == pa.uint64()
    assert table['fid'].to_pylist() == [0, 2**64 - 1]
    assert table['value'].to_pylist() == [1, 2]
    assert table['time'].type == times.type
    assert table['time'][0].value == 1
    exported_values = table['value'].chunk(0).buffers()[1]
    if not memoryview(exported_values).readonly:
        memoryview(exported_values).cast('q')[0] = 17
        assert features.column('value')[0] == 1


def test_arrow_exports_share_owned_whole_and_window_columns() -> None:
    pa = pytest.importorskip('pyarrow')
    source = np.arange(6, dtype=np.int64)
    features = gm.Features.from_columns(
        [None] * 6,
        columns={'rank': source, 'label': pa.array(['a', 'b', 'c', 'd', 'e', 'f'])},
        ids=np.arange(10, 16, dtype=np.uint64),
    )
    first = features.to_arrow()
    source[:] = -1
    whole = features.to_arrow()
    window = features[2:5].to_arrow()
    for name in ('rank', 'label', 'id'):
        before = first.column(name).chunk(0)
        repeated = whole.column(name).chunk(0)
        selected = window.column(name).chunk(0)
        assert repeated.buffers()[1].address == before.buffers()[1].address
        if name == 'label':
            assert selected.buffers()[1].address == before.buffers()[1].address
            assert selected.offset == 2
        else:
            assert np.shares_memory(before.to_numpy(), selected.to_numpy())
    assert window.column('rank').to_pylist() == [2, 3, 4]
    assert window.column('id').to_pylist() == [12, 13, 14]
    assert features[[4, 1, 4]].to_arrow().column('rank').to_pylist() == [4, 1, 4]
    detached = features[2:5].materialize().to_arrow()
    assert (
        detached.column('rank').chunk(0).buffers()[1].address
        != first.column('rank').chunk(0).buffers()[1].address
    )


def test_mixed_arrow_exports_share_unchanged_owned_columns() -> None:
    pa = pytest.importorskip('pyarrow')
    features = gm.Features.from_columns(
        [None] * 3,
        columns={'rank': pa.array([1, 2, 3]), 'name': pa.array(['a', 'b', 'c'])},
    )
    first = features.to_arrow()
    changed = features.with_columns({'name': ['x', 'y', 'z']})
    exported = changed.to_arrow()
    assert (
        exported.column('rank').chunk(0).buffers()[1].address
        == first.column('rank').chunk(0).buffers()[1].address
    )
    assert exported.column('name').to_pylist() == ['x', 'y', 'z']
    assert features.column('name')[:] == ('a', 'b', 'c')


def test_feature_arrow_export_normalizes_sparse_rows_and_checks_collisions() -> None:
    pytest.importorskip('pyarrow')
    features = gm.Features(
        [gm.Point(1, 2), None, gm.Point(3, 4)],
        properties=[{'a': 1}, None, {'b': None}],
    )
    table = features.to_arrow()
    assert table.column_names == ['geometry', 'a', 'b']
    assert table['a'].to_pylist() == [1, None, None]
    assert table['b'].to_pylist() == [None, None, None]
    assert b'gometry:feature_id' not in (table.schema.metadata or {})
    with pytest.raises(ValueError, match='no ID'):
        features.to_arrow(id_column='fid')
    with pytest.raises(ValueError, match='geometry column'):
        features.to_arrow(geometry='a')


def test_feature_arrow_export_preserves_all_null_id_role() -> None:
    pa = pytest.importorskip('pyarrow')
    features = gm.Features.from_columns([None, None], columns={}, ids=[None, None])
    table = features.to_arrow(id_column='fid')
    assert table.schema.metadata[b'gometry:feature_id'] == b'fid'
    assert table['fid'].type == pa.null()
    assert table['fid'].to_pylist() == [None, None]


def test_feature_arrow_export_preserves_selected_mixed_column_types() -> None:
    pa = pytest.importorskip('pyarrow')
    features = gm.Features(
        [None, None], properties=[{'name': 'first', 'omit': 1}, None], ids=[3, 4]
    )
    values = pa.array([1, 2], type=pa.timestamp('ns'))
    result = (
        features
        .with_columns({'instant': values, 'discard': [9, 9]})
        .select_columns(['instant', 'name'])[[1, 0]]
        .to_arrow()
    )
    assert result.column_names == ['geometry', 'instant', 'name', 'id']
    assert result['instant'].type == values.type
    assert [scalar.value for scalar in result['instant']] == [2, 1]
    assert result['name'].to_pylist() == [None, 'first']
    assert result['id'].to_pylist() == [4, 3]


def test_arrow_feature_table_round_trip_preserves_names_metadata_and_ids() -> None:
    pa = pytest.importorskip('pyarrow')
    geometry = gm.points([1, 2], [3, 4], crs=4326).to_arrow()
    source = np.array([4, 8], dtype=np.int32)
    times = pa.array([1, 2], type=pa.timestamp('ns', tz='UTC'))
    fields = [
        pa.field('rank', pa.int32(), metadata={b'unit': b'priority'}),
        pa.field('shape', geometry.type, metadata={b'purpose': b'active'}),
        pa.field('fid', pa.uint64()),
        pa.field('id', pa.string()),
        pa.field('time', times.type),
    ]
    schema = pa.schema(
        fields, metadata={b'gometry:feature_id': b'fid', b'owner': b'test'}
    )
    table = pa.Table.from_arrays(
        [
            pa.array(source),
            geometry,
            pa.array([0, None], type=pa.uint64()),
            pa.array(['a', 'b']),
            times,
        ],
        schema=schema,
    )
    features = gm.from_arrow(table)
    assert isinstance(features, gm.Features)
    source[0] = 99
    assert features.column('rank')[:] == (4, 8)
    assert features.ids == (0, None)
    assert features[0].properties['id'] == 'a'
    assert features[0].properties['time'].value == 1
    restored = pickle.loads(pickle.dumps(features[[1, 0]]))
    exported = restored.to_arrow()
    assert exported.column_names == schema.names
    assert exported.schema.metadata[b'owner'] == b'test'
    assert exported.schema.metadata[b'gometry:feature_id'] == b'fid'
    assert exported.schema.field('rank').metadata == {b'unit': b'priority'}
    assert exported.schema.field('shape').metadata == {b'purpose': b'active'}
    assert exported['fid'].type == pa.uint64()
    assert exported['fid'].to_pylist() == [None, 0]
    assert exported['time'].type == times.type
    renamed = restored.to_arrow(geometry='geometry', id_column='feature_id')
    assert renamed.schema.metadata[b'gometry:feature_id'] == b'feature_id'
    assert 'geometry' in renamed.column_names


def test_arrow_feature_export_updates_geometry_metadata_after_edits() -> None:
    pa = pytest.importorskip('pyarrow')
    first = gm.points([1, 2], [3, 4], crs=4326)
    second = gm.points([5, 6], [7, 8], crs=4326)
    geo = {
        'version': '1.1.0',
        'primary_column': 'shape',
        'columns': {
            'shape': {'encoding': 'point', 'geometry_types': ['Point']},
            'secondary': {'encoding': 'point', 'geometry_types': ['Point']},
        },
    }
    source = pa.table({
        'shape': first.to_arrow(),
        'secondary': second.to_arrow(),
        'name': ['a', 'b'],
    }).replace_schema_metadata({b'geo': json.dumps(geo).encode(), b'other': b'kept'})
    features = gm.from_arrow(source).select_columns(['name'])
    features = features.with_geometries(gm.points([10, 20], [30, 40], crs=4326))
    exported = features.to_arrow(geometry='renamed')
    metadata = json.loads(exported.schema.metadata[b'geo'])
    assert metadata['primary_column'] == 'renamed'
    assert set(metadata['columns']) == {'renamed'}
    assert metadata['columns']['renamed']['bbox'] == [10, 30, 20, 40]
    assert exported.schema.metadata[b'other'] == b'kept'
    restored = gm.from_arrow(exported)
    assert restored.geometries.to_wkt() == ['POINT (10 30)', 'POINT (20 40)']


def test_arrow_table_id_role_is_explicit_and_never_guessed() -> None:
    pa = pytest.importorskip('pyarrow')
    geometry = gm.points([1], [2]).to_arrow()
    table = pa.table({'shape': geometry, 'id': [7]})
    ordinary = gm.from_arrow(table)
    assert ordinary.ids == (None,)
    assert ordinary.column('id')[:] == (7,)
    marked = table.replace_schema_metadata({b'gometry:feature_id': b'id'})
    assert gm.from_arrow(marked).ids == (7,)
    disabled = gm.from_arrow(marked, id_column=None)
    assert disabled.ids == (None,)
    assert disabled.column('id')[:] == (7,)
    assert b'gometry:feature_id' not in (disabled.to_arrow().schema.metadata or {})
    with pytest.raises(ValueError, match='ID column'):
        gm.from_arrow(
            table.replace_schema_metadata({b'gometry:feature_id': b'missing'})
        )


def test_arrow_feature_table_requires_unambiguous_active_geometry() -> None:
    pa = pytest.importorskip('pyarrow')
    first = gm.points([1], [2]).to_arrow()
    second = gm.points([3], [4]).to_arrow()
    table = pa.table({'first': first, 'second': second})
    with pytest.raises(ValueError, match='geometry column'):
        gm.from_arrow(table)
    features = gm.from_arrow(table, geometry='first')
    assert features.column('second')[0] == gm.Point(3, 4)
    exported = features.to_arrow()
    assert exported['second'].type.extension_name == 'geoarrow.point'


def test_arrow_binary_property_does_not_compete_with_geoarrow_geometry() -> None:
    pa = pytest.importorskip('pyarrow')
    table = pa.table({
        'shape': gm.points([1, 2], [3, 4]).to_arrow(),
        'payload': pa.array([b'first', b'second'], type=pa.binary()),
    })
    features = gm.from_arrow(table)
    assert features.geometries.to_wkt() == ['POINT (1 3)', 'POINT (2 4)']
    assert features.column('payload')[:] == (b'first', b'second')

    class COnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return table.__arrow_c_stream__(requested_schema)

    native = gm.from_arrow(COnly())
    assert native.column('payload')[:] == (b'first', b'second')


def test_unknown_arrow_extension_values_are_rejected_for_every_provider() -> None:
    pa = pytest.importorskip('pyarrow')

    class OpaqueType(pa.ExtensionType):
        def __init__(self):
            super().__init__(pa.int64(), 'gometry.test.opaque')

        def __arrow_ext_serialize__(self):
            return b'extension-metadata'

        @classmethod
        def __arrow_ext_deserialize__(cls, storage_type, serialized):
            assert storage_type == pa.int64()
            assert serialized == b'extension-metadata'
            return cls()

    extension = OpaqueType()
    pa.register_extension_type(extension)
    try:
        column = pa.ExtensionArray.from_storage(extension, pa.array([7, None]))
        with pytest.raises(TypeError, match=r'columns\.opaque'):
            gm.Features.from_columns([None, None], columns={'opaque': column})
        table = pa.table({
            'shape': gm.points([1, 2], [3, 4]).to_arrow(),
            'opaque': column,
        })

        class COnly:
            def __arrow_c_stream__(self, requested_schema=None):
                return table.__arrow_c_stream__(requested_schema)

        with pytest.raises(TypeError, match=r'columns\.opaque'):
            gm.from_arrow(COnly())
    finally:
        pa.unregister_extension_type('gometry.test.opaque')


def test_arrow_feature_batches_are_incremental_and_caller_owned() -> None:
    pa = pytest.importorskip('pyarrow')
    geometry = gm.points([1, 2], [3, 4]).to_arrow()
    table = pa.table({'shape': geometry, 'name': ['first', 'second']})
    reader = pa.RecordBatchReader.from_batches(
        table.schema, table.to_batches(max_chunksize=1)
    )
    batches = gm.from_arrow_batches(reader)
    first = next(batches)
    assert first.column('name')[:] == ('first',)
    batches.close()
    assert reader.read_next_batch()['name'].to_pylist() == ['second']


def test_arrow_c_feature_batches_are_incremental_and_closeable() -> None:
    pa = pytest.importorskip('pyarrow')
    geometry = gm.points([1, 2], [3, 4]).to_arrow()
    table = pa.table({'shape': geometry, 'name': ['first', 'second']})
    batches = table.to_batches(max_chunksize=1)
    produced = []

    def source():
        for index, batch in enumerate(batches):
            produced.append(index)
            yield batch

    reader = pa.RecordBatchReader.from_batches(table.schema, source())

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return reader.__arrow_c_stream__(requested_schema)

    result = gm.from_arrow_batches(StreamOnly())
    assert produced == []
    first = next(result)
    assert produced == [0]
    assert first.column('name')[:] == ('first',)
    result.close()
    assert first.column('name')[:] == ('first',)
    assert reader.read_next_batch()['name'].to_pylist() == ['second']


def test_arrow_c_batch_failure_preserves_previously_yielded_snapshot() -> None:
    pa = pytest.importorskip('pyarrow')
    table = pa.table({'shape': gm.points([1], [2]).to_arrow(), 'name': ['saved']})

    def source():
        yield table.to_batches()[0]
        raise RuntimeError('producer failed')

    reader = pa.RecordBatchReader.from_batches(table.schema, source())

    class StreamOnly:
        def __arrow_c_stream__(self, requested_schema=None):
            return reader.__arrow_c_stream__(requested_schema)

    batches = gm.from_arrow_batches(StreamOnly())
    first = next(batches)
    with pytest.raises((OSError, RuntimeError), match='producer failed'):
        next(batches)
    assert first.column('name')[:] == ('saved',)


def test_mapping_provider_duplicate_keys_are_not_overwritten() -> None:
    class MappingProvider:
        def keys(self):
            return iter(('name', 'name'))

        def __getitem__(self, key):
            return key

    with pytest.raises(gm.GeometryError, match='duplicate'):
        gm.Features(gm.GeometryArray([gm.Point(1, 2)]), properties=MappingProvider())


def test_mapping_provider_keys_must_be_an_iterable_of_keys() -> None:
    class MappingProvider:
        def keys(self):
            return 'name'

        def __getitem__(self, key):
            return key

    with pytest.raises(TypeError, match=r'keys\(\)'):
        gm.Features(gm.GeometryArray([gm.Point(1, 2)]), properties=MappingProvider())
