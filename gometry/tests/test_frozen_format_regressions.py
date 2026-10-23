"""Regression coverage for v1 on-disk and cross-process formats."""

from __future__ import annotations

import json
import math
import pickle
from importlib import import_module

import gometry as gm
import pytest
from numpy.testing import assert_array_equal

# Protocol-4 reader fixtures for the intended first public wire format.
# Revise these with an intentional format change while the package is unreleased.
_V1_PICKLES = {
    'point': (
        '8004956b000000000000008c0c676f6d657472792e5f6c6962948c155f756e7069636b6c65'
        '5f67656f6d657472795f7631949394432501b90b0000000000000000f03f0000000000000040'
        '00000000000008400000000000001040948c09455053473a343332369447409f920000000000'
        '879452942e'
    ),
    'points': (
        '8004959b000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f7061636b65645f61727261795f7631949394284310000000000000f03f0000000000001440'
        '9443100000000000000040000000000000184094431000000000000008400000000000001c40'
        '9443100000000000001040000000000000204094298c09455053473a343332369447409f9500'
        '00000000430300010094749452942e'
    ),
    'lines': (
        '8004956f000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f7061636b65645f61727261795f7631949394284310000000000000f03f0000000000000840'
        '94431000000000000000400000000000001040944e4e430800000000020000009485944e4e'
        '4302000194749452942e'
    ),
    'polygons': (
        '8004959a000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f7061636b65645f61727261795f7631949394284320000000000000f03f0000000000000840'
        '0000000000000840000000000000f03f944320000000000000004000000000000000400000'
        '0000000010400000000000000040944e4e430800000000040000009443080000000001000000'
        '9486944e4e4302000194749452942e'
    ),
    'mixed': (
        '80049588000000000000008c0c676f6d657472792e5f6c6962948c1b5f756e7069636b6c65'
        '5f67656f6d657472795f61727261795f7631949394285d942843150101000000000000000000'
        'f03f000000000000004094432901020000000200000000000000000008400000000000001040'
        '0000000000001440000000000000184094430094654e4e430300000194749452942e'
    ),
    'groups': (
        '80049548000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f696e7436345f67726f7570735f76319493945d94284b074b0b4b0d655d94284b004b024b02'
        '4b0365869452942e'
    ),
    'crs': (
        '80049535000000000000008c0c676f6d657472792e5f6c6962948c105f756e7069636b6c65'
        '5f6372735f76319493948c09455053473a3338353794859452942e'
    ),
    'cell': (
        '80049539000000000000008c0c676f6d657472792e5f6c6962948c195f756e7069636b6c65'
        '5f67656f686173685f63656c6c5f76319493948c047533336494859452942e'
    ),
    'cells': (
        '80049549000000000000008c0c676f6d657472792e5f6c6962948c175f756e7069636b6c65'
        '5f63656c6c5f61727261795f76319493945d948c047533336494618c0767656f686173689443'
        '02000194879452942e'
    ),
    'index': (
        '8004958e000000000000008c0c676f6d657472792e5f6c6962948c1a5f756e7069636b6c65'
        '5f7370617469616c5f696e6465785f76319493942868008c195f756e7069636b6c655f7061636b'
        '65645f61727261795f7631949394284310000000000000f03f0000000000001440944310000000'
        '00000000400000000000001840944e4e294e4e4e749452944b004b0286944b0388749452942e'
    ),
}


def test_v1_geometry_pickle_preserves_frame_and_ordinates() -> None:
    point = pickle.loads(bytes.fromhex(_V1_PICKLES['point']))
    assert type(point) is gm.Point
    assert (point.x, point.y, point.z, point.m) == (1, 2, 3, 4)
    assert point.crs == gm.CRS(4326)
    assert point.epoch == 2020.5


@pytest.mark.parametrize(
    ('kind', 'expected'),
    [
        ('points', ['POINT ZM (1 2 3 4)', None, 'POINT ZM (5 6 7 8)']),
        ('lines', ['LINESTRING (1 2, 3 4)', None]),
        ('polygons', ['POLYGON ((1 2, 3 2, 3 4, 1 2))', None]),
        ('mixed', ['POINT (1 2)', 'LINESTRING (3 4, 5 6)', None]),
    ],
)
def test_v1_array_pickles_preserve_rows(kind: str, expected: list) -> None:
    values = pickle.loads(bytes.fromhex(_V1_PICKLES[kind]))
    assert values.to_wkt(drop_epoch=True) == expected
    if kind == 'points':
        assert values.crs == gm.CRS(4326)
        assert values.epoch == 2021.25
    else:
        assert values.crs is None
        assert values.epoch is None


def test_v1_groups_pickle_preserves_empty_groups() -> None:
    values = pickle.loads(bytes.fromhex(_V1_PICKLES['groups']))
    assert [row.tolist() for row in values] == [[7, 11], [], [13]]
    assert values.is_missing.tolist() == [False, False, False]


def test_v1_crs_and_cell_pickles_preserve_identity() -> None:
    crs = pickle.loads(bytes.fromhex(_V1_PICKLES['crs']))
    cell = pickle.loads(bytes.fromhex(_V1_PICKLES['cell']))
    cells = pickle.loads(bytes.fromhex(_V1_PICKLES['cells']))
    assert crs.definition == 'EPSG:3857'
    assert cell.token == 'u33d'
    assert cells.token == ['u33d', None]
    assert cells.is_missing.tolist() == [False, True]


def test_v1_index_pickle_preserves_removed_handles_and_next_handle() -> None:
    index = pickle.loads(bytes.fromhex(_V1_PICKLES['index']))
    assert list(index) == [0, 2]
    assert index[0] == gm.Point(1, 2)
    assert index[2] == gm.Point(5, 6)
    with pytest.raises(KeyError):
        index[1]
    assert index.insert(gm.Point(7, 8)) == 3
    assert index.query(gm.Point(5, 6), predicate='intersects').tolist() == [2]


def test_unknown_pickle_version_is_not_decoded_as_v1() -> None:
    payload = bytes.fromhex(_V1_PICKLES['point']).replace(b'_v1', b'_v9')
    with pytest.raises(AttributeError, match='_v9'):
        pickle.loads(payload)


def _restore(path: str, *args):
    """Apply a frozen reader identity and argument schema, without a current writer."""
    module, name = path.rsplit('.', 1)
    return getattr(import_module(module), name)(*args)


def test_v1_coordinate_and_parts_views_rebuild_from_their_owner() -> None:
    point = pickle.loads(bytes.fromhex(_V1_PICKLES['point']))
    coordinates = _restore('gometry._lib._unpickle_coordinates_v1', point, 'XY')
    assert_array_equal(coordinates, [[1, 2]])
    parts = _restore('builtins.getattr', point, 'parts')
    assert list(parts) == [point]


@pytest.mark.parametrize(
    ('reader', 'identifier', 'token'),
    [
        ('h3_cell', 617700169958293503, '8928308280fffff'),
        ('s2_cell', 9926584489608216576, '89c25'),
        ('tile', 927741523238322176, '032'),
        ('h3_vertex', 2491197614943633407, '229283082803ffff'),
        ('h3_edge', 1266218516299644927, '11928308280fffff'),
    ],
)
def test_v1_numeric_cell_readers(reader, identifier, token) -> None:
    value = _restore(f'gometry._lib._unpickle_{reader}_v1', identifier)
    assert value.id == identifier
    assert value.token == token


@pytest.mark.parametrize(
    ('reader', 'identifiers'),
    [
        ('h3_vertex_array', [2491197614943633407, 2563255208982085631]),
        ('h3_edge_array', [1266218516299644927, 1554448892451356671]),
    ],
)
def test_v1_h3_vertex_and_edge_arrays(reader, identifiers) -> None:
    values = _restore(f'gometry._lib._unpickle_{reader}_v1', identifiers)
    assert [value.id for value in values] == identifiers


@pytest.mark.parametrize('kind', ['int64', 'float64', 'geometry', 'cell'])
def test_v1_groups_keep_missing_and_empty_groups(kind) -> None:
    if kind == 'geometry':
        values = pickle.loads(bytes.fromhex(_V1_PICKLES['mixed']))
        offsets = [0, 1, 1, 3]
    elif kind == 'cell':
        values = pickle.loads(bytes.fromhex(_V1_PICKLES['cells']))
        offsets = [0, 1, 1, 2]
    else:
        values = [7, 11] if kind == 'int64' else [1.5, 2.5]
        offsets = [0, 1, 1, 2]
    groups = _restore(
        f'gometry._lib._unpickle_{kind}_groups_v1',
        values,
        offsets,
        [False, True, False],
    )
    assert groups[1] is None
    assert_array_equal(groups.is_missing, [False, True, False])
    assert groups[0][0] == values[0]
    assert list(groups[2]) == list(values[1:])


def test_v1_python_records_keep_field_meanings() -> None:
    points = pickle.loads(bytes.fromhex(_V1_PICKLES['points']))
    features = _restore(
        'gometry._types._restore_features_v1',
        points,
        ({'name': 'first'}, None, {'name': 'last'}),
        ('a', None, 23),
    )
    assert features.geometries == points
    assert features.properties == ({'name': 'first'}, None, {'name': 'last'})
    assert features.ids == ('a', None, 23)
    extremes = _restore('gometry._types._restore_extremes_v1', 1, 2, 3, 4)
    assert (extremes.west, extremes.south, extremes.east, extremes.north) == (
        1,
        2,
        3,
        4,
    )
    polygons = pickle.loads(bytes.fromhex(_V1_PICKLES['polygons']))
    lines = pickle.loads(bytes.fromhex(_V1_PICKLES['lines']))
    groups = _restore('gometry._lib._unpickle_geometry_groups_v1', lines, [0, 1, 2])
    result = _restore(
        'gometry._types._restore_polygonize_result_v1', polygons, groups, groups, groups
    )
    assert result.polygons == polygons
    assert result.cut_edges == groups
    assert result.dangles == groups
    assert result.invalid_rings == groups


def test_v1_validation_report_recomputes_the_verdict() -> None:
    point = pickle.loads(bytes.fromhex(_V1_PICKLES['point']))
    report = _restore('gometry._lib._unpickle_validation_report_v1', point)
    assert report.valid


def test_v1_pandas_reader_preserves_shared_views_and_readonly_state() -> None:
    pytest.importorskip('pandas')
    dtype = _restore('gometry._pandas._unpickle_geometry_dtype_v1')
    assert dtype.name == 'gometry.geometry'
    assert pickle.loads(pickle.dumps(dtype)) == dtype
    points = pickle.loads(bytes.fromhex(_V1_PICKLES['points']))
    state = [points]
    left = _restore(
        'gometry._pandas._unpickle_geometry_extension_array_v1', state, False
    )
    right = _restore(
        'gometry._pandas._unpickle_geometry_extension_array_v1', state, True
    )
    left[0] = points[2]
    assert right[0] == points[2]
    with pytest.raises(ValueError, match=r'read.only'):
        right[0] = points[0]


def test_v1_arrow_reader_preserves_storage_and_frame() -> None:
    pa = pytest.importorskip('pyarrow')
    storage = pa.struct([('x', pa.float64()), ('y', pa.float64())])
    metadata = b'{"crs":"EPSG:4326","crs_type":"authority_code","epoch":2020.5}'
    arrow_type = _restore(
        'gometry._arrow._restore_extension_type_v1', 'geoarrow.point', storage, metadata
    )
    array = pa.ExtensionArray.from_storage(
        arrow_type, pa.array([{'x': 1, 'y': 2}], type=storage)
    )
    restored = gm.from_arrow(array)
    assert restored.crs == gm.CRS(4326)
    assert restored.epoch == 2020.5
    assert restored[0].x == 1
    assert restored[0].y == 2


def test_v1_protocol_five_buffers_preserve_fixed_width_columns() -> None:
    # x=(1,5), y=(2,6), with one missing logical row between them.
    xs = bytes.fromhex('000000000000f03f0000000000001440')
    ys = bytes.fromhex('00000000000000400000000000001840')
    values = _restore(
        'gometry._lib._unpickle_packed_array_v1',
        pickle.PickleBuffer(xs),
        pickle.PickleBuffer(ys),
        None,
        None,
        (),
        None,
        None,
        b'\x00\x01\x00',
    )
    assert values.to_wkt() == ['POINT (1 2)', None, 'POINT (5 6)']


def test_v1_circle_reader_preserves_the_exact_disk() -> None:
    payload = (
        b'{"frame":"planar","disk":{"kind":"bounding","components":'
        b'[[false,[1],0],[false,[1],0],[false,[1],0],'
        b'[false,[1],1],[false,[1],0]],"source_center":null},'
        b'"scale":4607182418800017408,"crs":null,"metres":false,"geographic":false}'
    )
    circle = _restore('gometry._lib._unpickle_circle_v1', payload, None, None)
    expected = gm.LineString([(0, 0), (2, 2)]).minimum_bounding_circle()
    assert circle == expected
    assert circle.to_polygon() == expected.to_polygon()
    array = _restore(
        'gometry._lib._unpickle_circle_array_v1',
        b'[' + payload + b',null]',
        None,
        None,
    )
    assert array[0] == expected
    assert array[1] is None


@pytest.mark.parametrize(
    ('disk', 'center', 'radius'),
    [
        ({'kind': 'empty'}, 'POINT EMPTY', math.nan),
        (
            {
                'kind': 'source_point',
                'point': [
                    4607182418800017408,
                    4611686018427387904,
                    4619567317775286272,
                    4623507967449235456,
                ],
            },
            'POINT ZM (1 2 7 13)',
            0.0,
        ),
        ({'kind': 'zero', 'point': [0, 0, None, None]}, 'POINT (0 0)', 0.0),
        (
            {
                'kind': 'inscribed',
                'center': [
                    4607182418800017408,
                    4607182418800017408,
                    None,
                    None,
                ],
                'squared_radius': [[False, [1], 0], [False, [1], 0]],
            },
            'POINT (1 1)',
            1.0,
        ),
    ],
)
def test_v1_circle_solution_variants(disk, center, radius) -> None:
    state = {
        'frame': 'planar',
        'disk': disk,
        'scale': 4607182418800017408,
        'crs': None,
        'metres': False,
        'geographic': False,
    }
    circle = _restore(
        'gometry._lib._unpickle_circle_v1', json.dumps(state).encode(), None, None
    )
    assert circle.center.to_wkt() == center
    assert circle.radius.hex() == radius.hex()


def test_v1_ellipsoidal_circle_frame() -> None:
    state = {
        'frame': 'ellipsoidal',
        'center': [4607182418800017408, 4611686018427387904, None, None],
        'radius': 4652007308841189376,
        'crs': 'EPSG:4326',
    }
    circle = _restore(
        'gometry._lib._unpickle_circle_v1',
        json.dumps(state).encode(),
        'EPSG:4326',
        2020.5,
    )
    assert circle.center == gm.Point(1, 2, crs=4326, epoch=2020.5)
    assert circle.radius == 1000
    assert circle.unit == 'meters'
    assert circle.working_crs is None


def test_v1_local_circle_retains_its_polar_working_frame() -> None:
    working = (
        '+proj=stere +lat_0=90 +lat_ts=90 +lon_0=0 +datum=WGS84 +units=m +type=crs'
    )
    state = {
        'frame': 'local',
        'crs': 'EPSG:4326',
        'working_crs': working,
        'disk': {'kind': 'zero', 'point': [0, 0, None, None]},
        'custom': True,
        'projected_pole': [0, 0],
    }
    circle = _restore(
        'gometry._lib._unpickle_circle_v1',
        json.dumps(state).encode(),
        'EPSG:4326',
        2020.5,
    )
    assert circle.center.y == 90
    assert circle.center.epoch == 2020.5
    assert circle.radius == 0
    assert circle.unit == 'meters'
    assert circle.working_crs == working
    assert circle.to_polygon().y == 90


def test_nullable_homogeneous_ingress_stays_geoarrow_native() -> None:
    """D2: missing rows scatter after, rather than before, packed inference."""
    line = gm.LineString([(0, 0), (1, 1)])
    geojson_line = {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]}
    feature_collection = {
        'type': 'FeatureCollection',
        'features': [
            {'type': 'Feature', 'geometry': geojson_line, 'properties': {}},
            {'type': 'Feature', 'geometry': None, 'properties': {}},
        ],
    }
    arrays = (
        gm.GeometryArray([line, None]),
        gm.from_wkt([line.to_wkt(), None]),
        gm.from_wkb([line.to_wkb(), None]),
        gm.from_geojson([geojson_line, None]),
        gm.from_geojson(feature_collection),
        gm.from_geojson(feature_collection, features=True).geometries,
    )
    for values in arrays:
        assert values.is_missing.tolist() == [False, True]
        assert values.to_arrow().type.extension_name == 'geoarrow.linestring'
