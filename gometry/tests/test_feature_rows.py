"""Aligned feature row construction and selection."""

import copy
import gc
import pickle
import weakref
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from operator import length_hint
from threading import Barrier

import gometry as gm
import numpy as np
import pytest


def test_feature_is_an_immutable_single_row() -> None:
    source = {'nested': [1, {'name': 'road'}]}
    feature = gm.Feature(gm.Point(1, 2), properties=source, id=7)
    source['nested'][1]['name'] = 'changed'
    assert feature.properties['nested'][1]['name'] == 'road'
    assert feature.id == 7
    with pytest.raises(AttributeError):
        feature.id = 8
    with pytest.raises(TypeError):
        feature.properties['nested'][1]['name'] = 'changed'
    restored = pickle.loads(pickle.dumps(feature))
    assert restored == feature


def test_feature_replace_admits_changed_values_and_shares_omitted_values() -> None:
    original = gm.Feature(gm.Point(1, 2), properties={'nested': [3]}, id=7)
    replacement = original.__replace__(id=9)
    assert replacement.geometry is original.geometry
    assert replacement.properties is original.properties
    assert replacement.id == 9
    assert original.id == 7
    changed = {'nested': [5]}
    updated = original.__replace__(geometry=None, properties=changed, id=None)
    changed['nested'].append(11)
    assert updated.geometry is None
    assert updated.id is None
    assert updated.properties == {'nested': (5,)}
    assert original.__replace__(properties=None).properties is None
    with pytest.raises(TypeError):
        original.__replace__(geometry='POINT (1 2)')
    with pytest.raises(TypeError):
        original.__replace__(properties=[])


def test_feature_copy_and_deepcopy_share_immutable_values() -> None:
    original = gm.Feature(gm.Point(1, 2), properties={'nested': [3]}, id=7)
    for copier in (copy.copy, copy.deepcopy):
        copied = copier(original)
        assert type(copied) is type(original)
        assert copied.properties is original.properties
        assert copied.geometry is original.geometry
        assert copied.id == original.id
    pair = copy.deepcopy([original, original])
    assert pair[0] is pair[1]


@pytest.mark.skipif(not hasattr(copy, 'replace'), reason='requires Python 3.13+')
def test_standard_copy_replace_supports_feature_and_features() -> None:
    row = gm.Feature(gm.Point(1, 2), properties={'name': 'a'}, id=7)
    replaced = copy.replace(row, id=11)
    assert type(replaced) is type(row)
    assert replaced.properties is row.properties
    assert replaced.id == 11
    rows = gm.Features.from_columns(
        [gm.Point(3, 4), None], columns={'value': np.array([5, 9])}, ids=[2, 8]
    )
    updated = copy.replace(rows, ids=[17, 19])
    assert type(updated) is type(rows)
    assert updated.properties is rows.properties
    assert updated.geometries is rows.geometries
    assert updated.ids == (17, 19)
    assert rows.ids == (2, 8)


def test_features_deepcopy_shares_typed_selected_columns_and_metadata() -> None:
    pa = pytest.importorskip('pyarrow')
    table = pa.table({
        'shape': gm.points([1, 2], [3, 4]).to_arrow(),
        'rank': pa.array([5, 7], type=pa.int16()),
        'fid': pa.array([9, 11], type=pa.uint64()),
    }).replace_schema_metadata({b'gometry:feature_id': b'fid'})
    source = gm.from_arrow(table)[[1, 0, 1]]
    copied = copy.deepcopy(source)
    assert copied.properties is source.properties
    assert copied.geometries is source.geometries
    assert copied.ids is source.ids
    assert copied.to_arrow().schema == source.to_arrow().schema


def test_selection_keeps_geometries_properties_and_ids_aligned() -> None:
    rows = gm.Features(
        [gm.Point(1, 1), None, gm.Point(3, 3)],
        properties=[{'name': 'first'}, None, {'name': 'last'}],
        ids=[3, None, 9],
    )
    assert len(rows) == 3
    selected = rows[[2, 0, 2]]
    assert [row.id for row in selected] == [9, 3, 9]
    assert [row.properties['name'] for row in selected] == ['last', 'first', 'last']
    assert [row.geometry.to_wkt() for row in selected] == [
        'POINT (3 3)',
        'POINT (1 1)',
        'POINT (3 3)',
    ]
    assert rows[-2].geometry is None
    assert rows[-2].properties is None


def test_selection_supports_slices_numpy_indices_and_masks() -> None:
    rows = gm.Features(
        gm.points([1, 2, 3], [4, 5, 6]),
        properties=[{'name': 'a'}, {'name': 'b'}, {'name': 'c'}],
        ids=[7, 8, 9],
    )
    assert rows[np.array(1, dtype=np.int64)].id == 8
    assert [row.id for row in rows[::-1]] == [9, 8, 7]
    assert [row.id for row in rows[np.array([True, False, True])]] == [7, 9]
    assert [row.id for row in rows[np.array([2, 0], dtype=np.int32)]] == [9, 7]
    with pytest.raises(TypeError, match='boolean scalar'):
        _ = rows[True]
    with pytest.raises(IndexError, match='mask length'):
        _ = rows[[True, False]]


def test_column_construction_keeps_rows_aligned_and_immutable() -> None:
    names = ['first', 'second', 'third']
    nested = [[1], [], [3]]
    rows = gm.Features.from_columns(
        gm.points([1, 2, 3], [1, 2, 3]),
        columns={'name': names, 'nested': nested},
        ids=[1, None, 3],
    )
    names[0] = 'changed'
    nested[0].append(9)
    selected = rows[[2, 0, 2]][::-1]
    assert selected.ids == (3, 1, 3)
    assert [row.properties['name'] for row in selected] == [
        'third',
        'first',
        'third',
    ]
    assert selected[1].properties['nested'] == (1,)
    assert list(selected.column('name')) == ['third', 'first', 'third']
    assert selected.column('nested')[::-1] == ((3,), (1,), (3,))
    assert pickle.loads(pickle.dumps(selected)) == selected
    assert gm.Features.from_columns([None, None], columns={}).properties == ({}, {})
    with pytest.raises(gm.GeometryError, match=r'column.*length'):
        gm.Features.from_columns([None], columns={'name': ['a', 'b']})


def test_arrow_column_snapshot_preserves_precision_and_detaches_buffers() -> None:
    pa = pytest.importorskip('pyarrow')
    values = np.array([1, 2], dtype=np.int64)
    times = pa.array([1, 2], type=pa.timestamp('ns', tz='UTC'))
    rows = gm.Features.from_columns(
        [None, None], columns={'value': pa.array(values), 'time': times}
    )
    values[0] = 42
    assert rows[0].properties['value'] == 1
    assert type(rows[0].properties['value']) is int
    assert isinstance(rows[0].properties['time'], gm.ArrowValue)
    assert pa.array(rows[0].properties['time']).type == times.type
    assert rows[0].properties['time'].value == 1
    assert rows[[1, 0]][1].properties['time'].value == 1
    restored = pickle.loads(pickle.dumps(rows[[1, 0, 1]]))
    saved_columns = restored.to_arrow()
    assert saved_columns['value'].type == pa.int64()
    assert saved_columns['time'].type == times.type
    assert [value.value for value in restored.column('time')] == [2, 1, 2]
    empty = pickle.loads(pickle.dumps(rows[:0]))
    assert len(empty) == 0
    assert empty.to_arrow()['time'].type == times.type


def test_arrow_chunked_dictionary_selection_materializes_and_pickles() -> None:
    pa = pytest.importorskip('pyarrow')
    first = pa.DictionaryArray.from_arrays(
        pa.array([0, 1], type=pa.int8()), pa.array(['north', 'south'])
    )
    second = pa.DictionaryArray.from_arrays(
        pa.array([0, 1], type=pa.int8()), pa.array(['east', 'west'])
    )
    values = pa.chunked_array([first, second])
    features = gm.Features.from_columns([None] * 4, columns={'name': values})
    selected = features[[0, 2, 1, 3, 0]]
    assert selected.column('name')[:] == ('north', 'east', 'south', 'west', 'north')
    for result in (selected.materialize(), pickle.loads(pickle.dumps(selected))):
        assert result.column('name')[:] == ('north', 'east', 'south', 'west', 'north')
        assert result.to_arrow()['name'].type == values.type


def test_dictionary_gather_materializes_one_chunk_per_source_run() -> None:
    pa = pytest.importorskip('pyarrow')
    values = pa.DictionaryArray.from_arrays(
        pa.array([0, 1, 1, 0, 0, 1], type=pa.int8()),
        pa.array(['north', 'south']),
    )
    features = gm.Features.from_columns([None] * 6, columns={'name': values})
    exported = features[[4, 2, 0]].materialize().to_arrow()['name']
    assert exported.to_pylist() == ['north', 'south', 'north']
    assert exported.num_chunks == 1


def test_arrow_nested_sliced_children_keep_values_after_materialization() -> None:
    pa = pytest.importorskip('pyarrow')
    union = pa.UnionArray.from_sparse(
        pa.array([0, 1, 0, 1], type=pa.int8()),
        [pa.array([11, None, 17, None]), pa.array([None, 'b', None, 'd'])],
        field_names=['number', 'text'],
        type_codes=[0, 1],
    )
    fixed = pa.array([[11, 13], None, [17, 19], [23, 29]], type=pa.list_(pa.int64(), 2))
    source = pa.StructArray.from_arrays(
        [union.slice(1, 3), fixed.slice(1, 3)], names=['choice', 'pair']
    ).slice(1, 2)
    features = gm.Features.from_columns([None, None], columns={'nested': source})
    detached = features[[1, 0]].materialize()
    assert detached.column('nested')[:] == (
        {'choice': 'd', 'pair': (23, 29)},
        {'choice': 17, 'pair': (17, 19)},
    )
    assert detached.to_arrow()['nested'].to_pylist() == [
        {'choice': 'd', 'pair': [23, 29]},
        {'choice': 17, 'pair': [17, 19]},
    ]


def test_numpy_column_is_a_detached_typed_snapshot() -> None:
    original = np.array([2, 7, 3, 9], dtype=np.int64)
    values = original[::2]
    times = np.array(['2024-01-01T00:00:00.000000001', 'NaT'], dtype='datetime64[ns]')
    rows = gm.Features.from_columns(
        [None, None], columns={'value': values, 'time': times}
    )
    original[0] = 99
    assert rows.column('value')[:] == (2, 3)
    assert type(rows[0].properties['value']) is int
    assert rows[0].properties['time'] == times[0]
    copied = rows._properties._column_snapshot()['value']
    assert isinstance(copied, np.ndarray)
    assert copied.dtype == values.dtype
    restored = pickle.loads(pickle.dumps(rows[[1, 0]]))
    assert restored.column('value')[:] == (3, 2)
    assert restored._properties._column_snapshot()['time'].dtype == times.dtype
    with pytest.raises(ValueError, match='one-dimensional'):
        gm.Features.from_columns([None], columns={'value': np.array([[1]])})


def test_additional_geometry_column_remains_typed_and_aligned() -> None:
    pytest.importorskip('pyarrow')
    main = gm.points([1, 2], [3, 4])
    secondary = gm.points([10, 20], [30, 40])
    rows = gm.Features.from_columns(main, columns={'secondary': secondary}, ids=[1, 2])
    selected = rows[[1, 0, 1]]
    assert selected.column('secondary')[0] == secondary[1]
    result = pickle.loads(pickle.dumps(selected))
    assert result[1].properties['secondary'] == secondary[0]
    table = result.to_arrow()
    assert table['secondary'].type.extension_name == 'geoarrow.point'
    assert table['secondary'].to_pylist()[0] == {'x': 20.0, 'y': 40.0}


def test_sparse_column_view_distinguishes_known_names_from_absent_values() -> None:
    rows = gm.Features([None, None, None], properties=[{'a': [1]}, None, {'b': 3}])
    assert list(rows.column('a')) == [(1,), None, None]
    assert list(rows.column('b')) == [None, None, 3]
    assert list(rows[[2, 0]].column('a')) == [None, (1,)]
    with pytest.raises(KeyError):
        rows.column('missing')
    with pytest.raises(KeyError):
        rows[[1]].column('a')
    with pytest.raises(IndexError):
        _ = rows.column('a')[3]


def test_select_columns_preserves_sparse_absence_and_null_rows() -> None:
    rows = gm.Features(
        [None, None, None],
        properties=[{'a': 1}, None, {'a': None, 'b': 3}],
        ids=[1, 2, 3],
    )
    selected = rows.select_columns(['b', 'a'])[[2, 1, 0]]
    assert list(selected.properties) == [{'b': 3, 'a': None}, None, {'a': 1}]
    assert selected.ids == (3, 2, 1)
    empty = rows.select_columns([])
    assert list(empty.properties) == [{}, None, {}]
    with pytest.raises(KeyError):
        rows.select_columns(['missing'])
    with pytest.raises(ValueError, match='duplicate'):
        rows.select_columns(['a', 'a'])
    empty_restored = pickle.loads(pickle.dumps(rows.select_columns(['a'])[:0]))
    assert empty_restored.column('a')[:] == ()


def test_select_columns_preserves_columnar_order_and_pickle_types() -> None:
    pa = pytest.importorskip('pyarrow')
    rows = gm.Features.from_columns(
        [None, None],
        columns={'first': pa.array([1, 2]), 'second': pa.array(['a', 'b'])},
    )
    selected = rows.select_columns(['second', 'first'])[[1, 0]]
    assert list(selected[0].properties) == ['second', 'first']
    restored = pickle.loads(pickle.dumps(selected))
    assert list(restored[0].properties) == ['second', 'first']
    assert restored.column('first')[:] == (2, 1)
    assert restored.to_arrow()['first'].type == pa.int64()
    with pytest.raises(KeyError):
        selected.select_columns(['missing'])


def test_column_edits_preserve_sparse_rows_and_align_after_selection() -> None:
    rows = gm.Features(
        [None, None, None],
        properties=[{'old': 1}, None, {'old': 3, 'keep': 'c'}],
        ids=[1, 2, 3],
    )
    selected = rows[[2, 1, 2, 0]]
    changed = selected.with_columns({
        'old': [30, 20, 31, 10],
        'new': ['a', 'b', 'c', 'd'],
    })
    assert list(changed.properties) == [
        {'old': 30, 'keep': 'c', 'new': 'a'},
        {'old': 20, 'new': 'b'},
        {'old': 31, 'keep': 'c', 'new': 'c'},
        {'old': 10, 'new': 'd'},
    ]
    assert changed.ids == (3, 2, 3, 1)
    assert rows.properties[1] is None
    assert list(selected.with_columns({}).properties)[1] is None
    reedited = changed[[2, 1, 2]].with_columns({'new': [4, 5, 6]})
    assert reedited.column('old')[:] == (31, 20, 31)
    assert reedited.column('new')[:] == (4, 5, 6)
    assert list(reedited.select_columns(['keep', 'new']).properties) == [
        {'keep': 'c', 'new': 4},
        {'new': 5},
        {'keep': 'c', 'new': 6},
    ]
    with pytest.raises(gm.GeometryError, match=r'column.*length'):
        selected.with_columns({'bad': [1]})


def test_column_replacements_release_superseded_buffers() -> None:
    original_column = gm.points([1, 2], [3, 4])
    original_ref = weakref.ref(original_column)
    original = gm.Features.from_columns(
        [None, None], columns={'shape': original_column, 'keep': [7, 9]}
    )
    current = original
    replaced_refs = []
    for index in range(20):
        column = gm.points([index, index + 1], [0, 1])
        replaced_refs.append(weakref.ref(column))
        current = current.with_columns({'shape': column})
    del column
    gc.collect()
    assert all(reference() is None for reference in replaced_refs[:-1])
    assert replaced_refs[-1]() is not None
    assert current.column('keep')[:] == (7, 9)
    assert original.column('shape')[0].to_wkt() == 'POINT (1 3)'
    del original, original_column
    gc.collect()
    assert original_ref() is None


def test_partial_column_replacement_composes_selection_and_projection() -> None:
    original = gm.Features.from_columns(
        [None] * 4,
        columns={'a': [0, 1, 2, 3], 'b': [4, 5, 6, 7], 'hidden': [8] * 4},
    )
    changed = original[[3, 1, 3]].with_columns({'a': [10, 11, 12], 'c': [20, 21, 22]})
    changed = changed.select_columns(['c', 'b', 'a'])[[2, 0]]
    changed = changed.with_columns({'a': [30, 31]})[[1, 0, 1]]
    assert list(changed.properties) == [
        {'c': 20, 'b': 7, 'a': 31},
        {'c': 22, 'b': 7, 'a': 30},
        {'c': 20, 'b': 7, 'a': 31},
    ]
    assert list(changed[0].properties) == ['c', 'b', 'a']
    assert list(changed.materialize().properties) == list(changed.properties)
    assert list(pickle.loads(pickle.dumps(changed)).properties) == list(
        changed.properties
    )
    assert original.column('a')[:] == (0, 1, 2, 3)


def test_mixed_column_pickle_preserves_typed_columns_and_projection() -> None:
    pa = pytest.importorskip('pyarrow')
    rows = gm.Features(
        [None, None], properties=[{'name': 'a', 'hidden': 1}, None], ids=[None, 0]
    )
    times = pa.array([1, 2], type=pa.timestamp('ns', tz='UTC'))
    selected = rows.with_columns({'at': times}).select_columns(['at', 'name'])[
        [1, 0, 1]
    ]
    restored = pickle.loads(pickle.dumps(selected))
    assert list(restored[0].properties) == ['at']
    assert restored[1].properties['name'] == 'a'
    assert [value.value for value in restored.column('at')] == [2, 1, 2]
    assert restored.ids == (0, None, 0)
    with pytest.raises(KeyError):
        restored.column('hidden')
    empty = pickle.loads(pickle.dumps(selected[:0]))
    assert empty.column('at')[:] == ()
    assert empty.column('name')[:] == ()


def test_materialize_detaches_selected_storage_and_preserves_values() -> None:
    pytest.importorskip('pyarrow')

    class Key(str):
        __slots__ = ('__weakref__',)

    key = Key('unselected')
    reference = weakref.ref(key)
    rows = gm.Features(
        gm.points(range(20), range(20)),
        properties=[{'kept': i} if i != 19 else {key: 19} for i in range(20)],
        ids=range(20),
    )
    selected = rows[3:5]
    before = selected.geometries.to_arrow().buffers()[2]
    detached = selected.materialize()
    after = detached.geometries.to_arrow().buffers()[2]
    assert before.address != after.address
    assert list(detached.properties) == [{'kept': 3}, {'kept': 4}]
    assert detached.ids == (3, 4)
    assert [point.to_wkt() for point in detached.geometries] == [
        'POINT (3 3)',
        'POINT (4 4)',
    ]
    del key, rows, selected, before
    gc.collect()
    assert reference() is None


@pytest.mark.parametrize('mixed', [False, True])
def test_materialized_numpy_columns_export_immutable_buffers(mixed: bool) -> None:
    pa = pytest.importorskip('pyarrow')
    rows = gm.Features.from_columns(
        gm.points(range(4), range(4)),
        columns={'rank': np.array([2, 7, 3, 9], dtype=np.int32)},
        ids=np.array([11, 13, 17, 19], dtype=np.uint64),
    )
    if mixed:
        rows = rows.with_columns({'other': np.array([23, 29, 31, 37])})
    detached = rows[[3, 1]].materialize()
    table = detached.to_arrow()
    assert table['rank'].type == pa.int32()
    assert table['id'].type == pa.uint64()
    assert detached.column('rank')[:] == (9, 7)
    assert detached.ids[:] == (19, 13)
    for name, dtype in [('rank', np.int32), ('id', np.uint64)]:
        buffer = table[name].chunk(0).buffers()[1]
        assert not buffer.is_mutable
        view = np.frombuffer(buffer, dtype=dtype)
        with pytest.raises(ValueError, match='WRITEABLE'):
            view.setflags(write=True)


def test_materialize_shared_typed_selection_across_threads() -> None:
    pa = pytest.importorskip('pyarrow')
    rows = gm.Features.from_columns(
        gm.points(range(4), range(4)),
        columns={
            'category': pa.array([
                'north',
                'south',
                'east',
                'west',
            ]).dictionary_encode(),
            'instant': pa.array([11, 23, 31, 47], type=pa.timestamp('ns')),
        },
        ids=np.array([11, 13, 17, 19], dtype=np.uint64),
    )
    selected = rows[[3, 1, 3]].with_columns({'rank': np.array([5, 7, 11])})
    barrier = Barrier(4)

    def materialize() -> None:
        barrier.wait(timeout=10)
        for _ in range(6):
            result = selected.materialize()
            table = result.to_arrow()
            assert table['category'].to_pylist() == ['west', 'south', 'west']
            assert table['rank'].to_pylist() == [5, 7, 11]
            assert result.ids[:] == (19, 13, 19)
            assert result[0].properties['instant'].materialize().value == 47

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(materialize) for _ in range(4)]
        for future in futures:
            future.result()


def test_materialize_detaches_mixed_columns_and_nullable_geometry() -> None:
    pa = pytest.importorskip('pyarrow')
    rows = gm.Features(
        [gm.Point(1, 2), None, gm.Point(3, 4)],
        properties=[{'keep': 1, 'unused': 2}, None, {'keep': 3}],
        ids=[0, None, 2],
    )
    times = pa.array([1, 2, 3], type=pa.timestamp('ns'))
    selected = rows.with_columns({'time': times, 'drop': [3, 4, 5]}).select_columns([
        'keep',
        'time',
    ])[[2, 1]]
    result = selected.materialize()
    assert result.ids == (2, None)
    assert result[1].geometry is None
    assert result[0].properties['keep'] == 3
    assert result[1].properties['time'].value == 2
    assert result.to_arrow()['time'].type == times.type
    with pytest.raises(KeyError):
        result.column('drop')


@pytest.mark.parametrize(
    'geometries',
    [
        [gm.LineString([(0, 0), (1, 1)]), gm.LineString([(2, 2), (3, 3)])],
        [
            gm.Polygon([(0, 0), (2, 0), (2, 2), (0, 0)]),
            gm.Polygon([(3, 3), (5, 3), (5, 5), (3, 3)]),
        ],
        [gm.Point(1, 2), gm.LineString([(0, 0), (1, 1)])],
    ],
)
def test_materialize_preserves_packed_and_mixed_geometry(geometries) -> None:
    rows = gm.Features(geometries, properties=[{'index': 0}, {'index': 1}])
    selected = rows[[1, 0, 1]]
    detached = selected.materialize()
    assert detached == selected
    assert detached.geometries is not selected.geometries


def test_materialize_detaches_geometry_views_inside_mixed_storage() -> None:
    source = gm.GeometryArray([
        gm.LineString([(1, 2), (3, 4)]),
        gm.LineString([(5, 6), (7, 8)]),
    ])
    rows = gm.Features([source[0], gm.Point(0, 0)])
    selected = rows[:1]
    original_x = selected[0].geometry.coords.x
    detached = selected.materialize()
    detached_x = detached[0].geometry.coords.x
    assert (
        original_x.__array_interface__['data'][0]
        != detached_x.__array_interface__['data'][0]
    )
    np.testing.assert_array_equal(detached_x, [1, 3])


def test_chained_property_selections_preserve_order_and_export() -> None:
    rows = gm.Features(
        gm.points([1, 2, 3, 4], [1, 2, 3, 4]),
        properties=[{'value': [1]}, None, {'value': [3]}, {'value': [4]}],
        ids=[1, 2, 3, 4],
    )
    selected = rows[[3, 0, 3, 1]][::-1][[2, 0, 2]]
    assert selected.ids == (1, 2, 1)
    assert list(selected.properties) == [{'value': (1,)}, None, {'value': (1,)}]
    assert pickle.loads(pickle.dumps(selected)) == selected
    assert gm.to_feature_collection(selected)['features'][0]['properties'] == {
        'value': [1]
    }
    with pytest.raises(IndexError):
        selected.properties._take((3,))


def test_feature_geojson_export_preserves_null_and_detaches_values() -> None:
    feature = gm.Feature(gm.Point(1, 2), properties={'nested': [1]}, id=0)
    exported = gm.to_feature(feature)
    assert exported['id'] == 0
    exported['properties']['nested'].append(2)
    assert feature.properties['nested'] == (1,)
    assert gm.to_feature(gm.Feature(None))['properties'] is None


def test_property_views_expose_read_only_mapping_protocols() -> None:
    rows = gm.Features([gm.Point(1, 2)], properties={'nested': {'value': 3}})
    assert isinstance(rows.properties, Sequence)
    properties = rows[0].properties
    assert isinstance(properties, Mapping)
    assert list(properties.keys()) == ['nested']
    nested = dict(properties.items())['nested']
    assert isinstance(nested, Mapping)
    with pytest.raises(TypeError):
        nested['value'] = 9
    assert rows[0].properties['nested']['value'] == 3


def test_property_rows_iterate_and_compare_as_a_sequence() -> None:
    rows = gm.Features(
        [gm.Point(1, 2), None],
        properties=[{'name': 'a'}, None],
    )
    assert list(rows.properties) == [{'name': 'a'}, None]
    assert rows.properties == ({'name': 'a'}, None)
    assert rows.properties != ({'name': 'b'}, None)
    assert rows.properties[:] == ({'name': 'a'}, None)


def test_replace_geometries_retains_aligned_snapshots() -> None:
    rows = gm.Features(
        [gm.Point(1, 2), None], properties=[{'name': 'a'}, None], ids=[0, 5]
    )
    replacement = rows.with_geometries([None, gm.Point(3, 4)])
    assert replacement.properties is rows.properties
    assert replacement.ids is rows.ids
    assert replacement[0].geometry is None
    assert replacement[1].geometry == gm.Point(3, 4)
    assert rows[0].geometry == gm.Point(1, 2)
    with pytest.raises(gm.GeometryError, match='geometries length'):
        rows.with_geometries([gm.Point(1, 2)])


def test_replace_ids_retains_aligned_snapshots() -> None:
    rows = gm.Features([gm.Point(1, 2), None], properties={'name': 'a'}, ids=[0, 5])
    replacement = rows.with_ids(iter([7, None]))
    assert replacement.geometries is rows.geometries
    assert replacement.properties is rows.properties
    assert replacement.ids == (7, None)
    assert rows.ids == (0, 5)
    assert rows.with_ids(None).ids == (None, None)
    with pytest.raises(gm.GeometryError, match='ids length'):
        rows.with_ids([7])
    with pytest.raises(TypeError):
        rows.with_ids([True, None])


def test_id_role_survives_selection_edits_and_pickle_even_when_all_null() -> None:
    no_role = gm.Features([None, None])
    explicit = gm.Features([None, None], ids=[None, None])
    named = gm.Features._from_property_rows(
        explicit.geometries, explicit._properties, explicit.ids, 'fid'
    )
    assert no_role._id_column is None
    assert explicit._id_column == 'id'
    assert named[[1, 0, 1]]._id_column == 'fid'
    assert named[:0]._id_column == 'fid'
    assert named.select_columns([])._id_column == 'fid'
    assert named.with_ids([0, None])._id_column == 'fid'
    assert named.with_ids(None)._id_column is None
    assert no_role.with_ids([None, None])._id_column == 'id'
    assert pickle.loads(pickle.dumps(named[:0]))._id_column == 'fid'
    assert pickle.loads(pickle.dumps(no_role))._id_column is None


def test_typed_numpy_ids_remain_columnar_through_selection_and_pickle() -> None:
    pa = pytest.importorskip('pyarrow')
    source = np.array([0, 2**64 - 1, 7], dtype=np.uint64)
    rows = gm.Features([None, None, None], ids=source)
    source[0] = 99
    selected = rows[[1, 0, 1]]
    assert selected.ids == (2**64 - 1, 0, 2**64 - 1)
    assert type(selected[0].id) is int
    assert selected.ids._snapshot().dtype == np.uint64
    assert selected.to_arrow()['id'].type == pa.uint64()
    restored = pickle.loads(pickle.dumps(selected))
    assert restored.ids == selected.ids
    assert restored.ids._snapshot().dtype == np.uint64
    assert restored.materialize().ids == selected.ids
    with pytest.raises(TypeError, match='boolean'):
        gm.Features([None], ids=np.array([True]))
    with pytest.raises(gm.GeometryError, match='finite'):
        gm.Features([None], ids=np.array([np.nan]))


def test_arrow_ids_preserve_type_nulls_and_snapshot_ownership() -> None:
    pa = pytest.importorskip('pyarrow')
    values = np.array([0, 5], dtype=np.uint64)
    ids = pa.array(values)
    rows = gm.Features([None, None], ids=ids)
    values[0] = 9
    assert rows.ids == (0, 5)
    assert rows[[1, 0]].to_arrow()['id'].type == pa.uint64()
    all_null = gm.Features([None], ids=pa.array([None], type=pa.uint64()))
    assert all_null.to_arrow()['id'].type == pa.uint64()
    with pytest.raises(TypeError, match='Arrow feature IDs'):
        gm.Features([None], ids=pa.array([True]))
    with pytest.raises(gm.GeometryError, match='finite'):
        gm.Features([None], ids=pa.array([float('inf')]))


def test_replace_shares_unchanged_columns_and_checks_alignment() -> None:
    rows = gm.Features([gm.Point(1, 2), None], properties={'name': 'a'}, ids=[0, 5])
    replacement = rows.__replace__(ids=[7, 8])
    assert replacement.geometries is rows.geometries
    assert replacement.properties is rows.properties
    assert replacement.ids == (7, 8)
    with pytest.raises(gm.GeometryError, match='properties length'):
        rows.__replace__(geometries=gm.GeometryArray([None]))
    resized = rows.__replace__(
        geometries=gm.GeometryArray([None]), properties=[{'name': 'new'}], ids=[9]
    )
    assert len(resized) == 1
    assert resized[0].properties == {'name': 'new'}
    assert resized.ids == (9,)


def test_feature_iterator_can_be_shared_across_threads() -> None:
    rows = gm.Features(gm.points(range(1000), range(1000)), ids=range(1000))
    iterator = iter(rows)
    assert length_hint(iterator) == 1000
    with ThreadPoolExecutor(max_workers=4) as executor:
        batches = list(executor.map(lambda _: [row.id for row in iterator], range(4)))
    ids = [id for batch in batches for id in batch]
    assert sorted(ids) == list(range(1000))
    assert length_hint(iterator) == 0


@pytest.mark.parametrize('storage', ['rows', 'columns', 'mixed'])
def test_property_and_iterator_cycles_are_collectable(storage: str) -> None:
    class Key(str):
        __slots__ = ('__weakref__', 'owner')

    key = Key('name')
    if storage == 'columns':
        rows = gm.Features.from_columns([gm.Point(1, 2)], columns={key: ['value']})
    elif storage == 'mixed':
        rows = gm.Features([gm.Point(1, 2)], properties={'base': 1}).with_columns({
            key: ['value'],
        })
    else:
        rows = gm.Features([gm.Point(1, 2)], properties={key: 'value'})
    iterator = iter(rows)
    key.owner = iterator
    reference = weakref.ref(key)
    del iterator, rows, key
    assert reference() is not None
    gc.collect()
    assert reference() is None
