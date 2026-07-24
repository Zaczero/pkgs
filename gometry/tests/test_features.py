"""GeoJSON Feature and FeatureCollection boundary behavior."""

import json
import math
import pickle
import types

import gometry as gm
import pytest


class _FeatureId(int):
    """Distinct int object used to prove object-lane identity is retained."""


def test_to_feature_uses_geom_keyword_only() -> None:
    point = gm.Point(1, 2)
    assert gm.to_feature(geom=point)['geometry'] == point.__geo_interface__
    with pytest.raises(TypeError, match="unexpected keyword argument 'geometry'"):
        gm.to_feature(geometry=point)


def test_feature_properties_are_shallow_copies_not_aliases() -> None:
    """Exact-dict properties must be distinct from the caller's mapping.

    Regression: the PyDict fast path in mapping_as_dict returned the same
    object (new reference, not a copy), so mutating feature properties
    mutated the caller's dict.
    """
    props = {'a': 1}
    feature = gm.to_feature(gm.Point(0, 0), properties=props)
    assert feature['properties'] is not props
    assert feature['properties'] == {'a': 1}
    feature['properties']['a'] = 9
    assert props == {'a': 1}

    collection = gm.to_feature_collection([gm.Point(0, 0)], properties=props)
    row = collection['features'][0]['properties']
    assert row is not props
    row['a'] = 9
    assert props == {'a': 1}

    features = gm.Features(gm.GeometryArray([gm.Point(0, 0)]), properties=props)
    assert features.properties[0] is not props
    features.properties[0]['a'] = 9
    assert props == {'a': 1}

    # Non-dict mapping still accepted and yields a real independent dict.
    proxy = types.MappingProxyType({'k': 2})
    feature = gm.to_feature(gm.Point(0, 0), properties=proxy)
    assert isinstance(feature['properties'], dict)
    assert feature['properties'] == {'k': 2}
    feature['properties']['k'] = 3
    assert proxy['k'] == 2


def test_feature_helpers_preserve_properties_round_trip() -> None:
    point = gm.Point(1, 2)
    box = gm.box(0, 0, 1, 1)
    feature = gm.to_feature(point, properties={'name': 'A'}, id=7)
    assert feature['type'] == 'Feature'
    assert feature['geometry'] == point.__geo_interface__
    assert feature['properties'] == {'name': 'A'} and feature.get('id') == 7
    fc = gm.to_feature_collection(
        [point, box], properties=[{'name': 'A'}, {'name': 'B'}], ids=[7, None]
    )
    assert fc['type'] == 'FeatureCollection' and len(fc['features']) == 2
    assert fc['features'][0].get('id') == 7 and 'id' not in fc['features'][1]
    bare = gm.to_feature_collection([point, box])
    assert [f['properties'] for f in bare['features']] == [{}, {}]
    assert all('id' not in f for f in bare['features'])
    projected = gm.Point(1_000_000, 1_000_000, crs=3857)
    with pytest.raises(gm.GeometryError, match=r'to_crs\(4326\)'):
        gm.to_feature(projected)
    with pytest.raises(gm.GeometryError, match=r'to_crs\(4326\)'):
        gm.to_feature_collection([point, projected])
    measured = gm.Point(1, 2, m=3)
    encoders = (
        lambda: gm.to_feature(measured),
        lambda: gm.to_feature_collection([measured]),
        lambda: gm.to_feature_collection(gm.GeometryArray([measured])),
    )
    for encode in encoders:
        with pytest.raises(gm.InvalidGeometryError, match='GeoJSON has no M'):
            encode()
    features = gm.from_features(json.dumps(fc))
    assert isinstance(features, gm.Features)
    geometries, properties, ids = features
    assert isinstance(geometries, gm.GeometryArray)
    assert geometries.crs == 'EPSG:4326'
    assert [g.to_wkt() for g in geometries] == [point.to_wkt(), box.to_wkt()]
    assert properties == ({'name': 'A'}, {'name': 'B'})
    assert ids == (7, None)
    assert gm.from_features(feature).properties == ({'name': 'A'},)
    assert gm.from_features([feature, feature]).ids == (7, 7)
    assert gm.from_features(fc).geometries[0].to_wkt() == point.to_wkt()
    assert gm.from_features(fc, crs=None).geometries.crs is None
    with pytest.raises(ValueError, match='does not match'):
        gm.to_feature_collection([point], properties=[{}, {}])
    with pytest.raises(gm.GeometryError, match='feature id'):
        gm.to_feature(point, id=object())
    with pytest.raises(gm.GeometryError, match='feature id'):
        gm.to_feature(point, id=math.inf)
    with pytest.raises(ValueError, match='must have a geometry'):
        gm.from_features([{'type': 'Feature', 'properties': {}}])
    with pytest.raises(ValueError, match='Feature, FeatureCollection'):
        gm.from_features({'type': 'Nonsense'})
    with pytest.raises(gm.ParseError, match='Feature, FeatureCollection'):
        gm.from_features(123)
    with pytest.raises(gm.ParseError, match='mapping'):
        gm.from_features([123])
    with pytest.raises(gm.ParseError, match='features must be iterable'):
        gm.from_features({'type': 'FeatureCollection', 'features': 123})
    with pytest.raises(gm.ParseError, match='type "Feature"'):
        gm.from_features([{'type': 'Point', 'coordinates': [1, 2]}])
    with pytest.raises(gm.ParseError, match='properties must be a mapping'):
        gm.from_features({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [1, 2]},
            'properties': 3,
        })
    with pytest.raises(gm.ParseError, match='feature id'):
        gm.from_features({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [1, 2]},
            'id': object(),
        })


def test_feature_null_geometry_mapping_matches_text_missing_rows() -> None:
    fc = gm.to_feature_collection(
        gm.GeometryArray([gm.Point(1, 2, crs=4326), None]),
        properties=[{'name': 'point'}, {'name': 'missing'}],
        ids=['a', 'b'],
    )
    mapping = gm.from_features(fc)
    text = gm.from_features(json.dumps(fc))
    geojson = gm.from_geojson(fc)
    assert (
        mapping.properties
        == text.properties
        == ({'name': 'point'}, {'name': 'missing'})
    )
    assert mapping.ids == text.ids == ('a', 'b')
    assert mapping.geometries.is_missing.tolist() == [False, True]
    assert text.geometries.is_missing.tolist() == [False, True]
    assert geojson.is_missing.tolist() == [False, True]
    # Scalar null Feature is rejected uniformly (mapping and text); bulk nulls
    # go through from_features / FeatureCollection only.
    null_feature = {'type': 'Feature', 'properties': {}, 'geometry': None}
    with pytest.raises(gm.ParseError, match='null'):
        gm.from_geojson(null_feature)
    with pytest.raises(gm.ParseError, match=r'null|geometry'):
        gm.from_geojson('{"type":"Feature","geometry":null,"properties":{}}')
    with pytest.raises(gm.ParseError, match='null'):
        gm.from_geojson(fc['features'][1])
    # Bulk FeatureCollection still admits null geometries as missing rows.
    collection = {
        'type': 'FeatureCollection',
        'features': [
            null_feature,
            {
                'type': 'Feature',
                'properties': {},
                'geometry': {'type': 'Point', 'coordinates': [1.0, 2.0]},
            },
        ],
    }
    arr = gm.from_geojson(collection)
    assert arr.is_missing.tolist() == [True, False]
    single = gm.from_features(fc['features'][1])
    assert single.geometries.is_missing.tolist() == [True]
    with pytest.raises(ValueError, match='must have a geometry'):
        gm.from_features([{'type': 'Feature', 'properties': {}}])


def test_from_features_native_object_lane_preserves_python_fidelity() -> None:
    assert gm.from_features.__module__ == 'gometry._lib'
    marker = object()
    huge = 1 << 100
    identifier = _FeatureId(17)
    properties = types.MappingProxyType({
        'marker': marker,
        'huge': huge,
        'tail': 'keeps insertion order',
    })
    feature = types.MappingProxyType({
        'type': 'Feature',
        'geometry': types.MappingProxyType({
            'type': 'Point',
            'coordinates': (1.0, 2.0),
        }),
        'properties': properties,
        'id': identifier,
    })
    collection = types.MappingProxyType({
        'type': 'FeatureCollection',
        'features': (feature,),
    })

    result = gm.from_features(collection, epoch=2020.5)
    assert isinstance(result, gm.Features)
    assert result.geometries.epoch == 2020.5
    assert [str(value) for value in result.geometries] == ['POINT (1 2)']
    assert list(result.properties[0]) == ['marker', 'huge', 'tail']
    assert result.properties[0]['marker'] is marker
    assert result.properties[0]['huge'] == huge
    assert result.ids[0] is identifier


def test_from_features_native_generator_and_json_sequence_match() -> None:
    rows = [
        {
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [1, 2]},
            'properties': {'row': 0},
            'id': 'a',
        },
        {
            'type': 'Feature',
            'geometry': None,
            'properties': None,
        },
    ]
    generated = gm.from_features(row for row in rows)
    text = gm.from_features(json.dumps(rows))
    assert generated.geometries.to_wkt() == text.geometries.to_wkt()
    assert generated.geometries.is_missing.tolist() == [False, True]
    assert generated.properties == text.properties == ({'row': 0}, None)
    assert generated.ids == text.ids == ('a', None)


def test_features_record_validates_alignment_and_has_bounded_repr() -> None:
    geometries = gm.GeometryArray([gm.Point(i, i) for i in range(10)])
    features = gm.Features(geometries, [{'row': 0}] * 10, [None] * 10)
    assert features.properties == ({'row': 0},) * 10
    assert isinstance(features.properties, tuple)
    assert isinstance(features.ids, tuple)
    with pytest.raises(TypeError, match='has no len'):
        len(features)  # type: ignore[arg-type]
    assert 'rows=10' in repr(features)
    assert len(repr(features)) < 200
    huge = gm.Features(
        gm.GeometryArray([gm.Point(0, 0)]),
        ({'payload': 'x' * 10_000},),
        (None,),
    )
    assert len(repr(huge)) < 300
    with pytest.raises(TypeError, match='GeometryArray'):
        gm.Features([gm.Point(0, 0)], ({}), (None))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match='properties length'):
        gm.Features(geometries, iter([{}]), (None,) * 10)
    with pytest.raises(ValueError, match='ids length'):
        gm.Features(geometries, ({},) * 10, ())
    mapped = gm.Features(
        geometries,
        (types.MappingProxyType({'row': row}) for row in range(10)),
        (None for _ in range(10)),
    )
    assert mapped.properties[3] == {'row': 3}
    assert isinstance(mapped.properties[3], dict)
    with pytest.raises(TypeError, match='ids'):
        gm.Features(geometries, ({},) * 10, (True,) + (None,) * 9)


def test_features_defaults_and_mapping_broadcast_are_independent() -> None:
    geometries = gm.points([0, 1], [2, 3])
    empty = gm.Features(geometries)
    assert empty.properties == (None, None)
    assert empty.ids == (None, None)

    nested: list[int] = []
    broadcast = gm.Features(geometries, {'nested': nested})
    assert broadcast.properties == ({'nested': nested}, {'nested': nested})
    assert broadcast.properties[0] is not broadcast.properties[1]
    assert broadcast.properties[0]['nested'] is nested
    assert broadcast.properties[1]['nested'] is nested
    broadcast.properties[0]['row'] = 0
    assert 'row' not in broadcast.properties[1]

    with pytest.raises(TypeError, match='ids must be an iterable'):
        gm.Features(geometries, ids='ab')  # type: ignore[arg-type]


def test_features_pickle_revalidates_parallel_columns() -> None:
    geometries = gm.GeometryArray([gm.Point(0, 0)])

    class InvalidFeatures:
        def __reduce__(self):
            return (gm.Features, (geometries, (), ()))

    with pytest.raises(ValueError, match='properties length'):
        pickle.loads(pickle.dumps(InvalidFeatures()))


def test_feature_collection_broadcasts_mapping_and_bounds_alignment() -> None:
    points = [gm.Point(0, 0), gm.Point(1, 1)]
    nested: list[int] = []
    collection = gm.to_feature_collection(
        points, properties=types.MappingProxyType({'nested': nested})
    )
    first, second = collection['features']
    assert first['properties'] == second['properties'] == {'nested': nested}
    assert first['properties'] is not second['properties']
    assert first['properties']['nested'] is second['properties']['nested'] is nested

    consumed = 0

    def too_many() -> object:
        nonlocal consumed
        while True:
            consumed += 1
            yield {}

    with pytest.raises(gm.GeometryError, match='does not match'):
        gm.to_feature_collection(points, properties=too_many())
    assert consumed == len(points) + 1

    for scalar in ('abc', b'abc', bytearray(b'abc'), 3):
        with pytest.raises((TypeError, gm.GeometryError), match=r'ids|iterable'):
            gm.to_feature_collection(points, ids=scalar)  # type: ignore[arg-type]


def test_feature_collection_accepts_one_geometry_or_null_geometry() -> None:
    point = gm.Point(1, 2)
    feature = gm.to_feature_collection(point, properties={'name': 'one'})['features'][0]
    assert feature['geometry'] == point.__geo_interface__
    assert feature['properties'] == {'name': 'one'}
    null_feature = gm.to_feature_collection(None)['features'][0]
    assert null_feature['geometry'] is None


def test_feature_properties_missing_and_explicit_null_are_distinct() -> None:
    base = {'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [1, 2]}}
    parsed = gm.from_features([base, {**base, 'properties': None}])
    assert parsed.properties == ({}, None)
    encoded = gm.to_feature_collection(parsed.geometries, properties=parsed.properties)
    assert encoded['features'][0]['properties'] == {}
    assert encoded['features'][1]['properties'] is None


def test_from_features_noncontiguous_utf8_memoryview() -> None:
    encoded = (
        b'{"type":"Feature","geometry":{"type":"Point",'
        b'"coordinates":[3,4]},"properties":{"name":"p"},"id":9}'
    )
    storage = bytearray(len(encoded) * 2)
    storage[::2] = encoded
    view = memoryview(storage)[::2]
    result = gm.from_features(view)
    assert result.geometries.to_wkt() == ['POINT (3 4)']
    assert result.properties == ({'name': 'p'},)
    assert result.ids == (9,)


def test_d16_from_features_accepts_signed_one_byte_buffers() -> None:
    """D16: signed itemsize-1 buffers parse like from_geojson (not BufferError)."""
    import array

    payload = (
        b'{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},'
        b'"properties":{"k":1},"id":7}'
    )
    signed = array.array('b', payload)
    # Exact repro: from_geojson already accepted signed; from_features must too.
    assert gm.from_geojson(signed).to_wkt() == 'POINT (1 2)'
    feats = gm.from_features(signed)
    assert feats.geometries.to_wkt() == ['POINT (1 2)']
    assert feats.properties == ({'k': 1},)
    assert feats.ids == (7,)

    # Unsigned / bytes / bytearray / memoryview stay accepted (no over-rejection).
    for data in (
        array.array('B', payload),
        payload,
        bytearray(payload),
        memoryview(payload),
    ):
        other = gm.from_features(data)
        assert other.geometries.to_wkt() == ['POINT (1 2)']
        assert other.properties == feats.properties
        assert other.ids == feats.ids


def test_from_features_text_and_object_reconcile_embedded_geometry_crs() -> None:
    feature = {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [1, 2],
            'crs': {'type': 'name', 'properties': {'name': 'EPSG:3857'}},
        },
        'properties': {},
    }
    encoded = json.dumps(feature)
    escaped_key = encoded.replace('"crs"', '"c\\u0072s"')
    for value in (feature, encoded, encoded.encode(), escaped_key):
        with pytest.raises(gm.ParseError, match='conflicts with crs=EPSG:4326'):
            gm.from_features(value, crs=4326)


@pytest.mark.parametrize('container_kind', ['Feature', 'FeatureCollection'])
def test_from_features_reconciles_feature_envelope_legacy_crs(
    container_kind: str,
) -> None:
    feature = {
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
        'properties': {},
    }
    value = (
        {**feature, 'crs': {'type': 'name', 'properties': {'name': 'EPSG:3857'}}}
        if container_kind == 'Feature'
        else {
            'type': 'FeatureCollection',
            'features': [feature],
            'crs': {'type': 'name', 'properties': {'name': 'EPSG:3857'}},
        }
    )
    encoded = json.dumps(value)
    escaped_key = encoded.replace('"crs"', '"c\\u0072s"')
    for input_value in (value, encoded, encoded.encode(), escaped_key):
        with pytest.raises(gm.ParseError, match='conflicts with crs=EPSG:4326'):
            gm.from_features(input_value, crs=4326)

    agreeing = dict(value)
    agreeing['crs'] = {'type': 'name', 'properties': {'name': 'EPSG:4326'}}
    assert gm.from_features(agreeing, crs=4326).geometries.to_wkt() == ['POINT (1 2)']


@pytest.mark.parametrize(
    'geometry',
    [
        gm.Point(1, 2),
        gm.MultiPoint([(1, 2), (3, 4)]),
        gm.LineString([(0, 0), (1, 1)]),
        gm.MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]),
        gm.Polygon([(0, 0), (2, 0), (2, 2), (0, 0)]),
        gm.MultiPolygon([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]),
        gm.GeometryCollection([gm.Point(1, 2), gm.LineString([(0, 0), (1, 1)])]),
    ],
)
@pytest.mark.parametrize('as_text', [False, True], ids=['object', 'text'])
def test_from_features_native_lanes_cover_every_geometry_kind(
    geometry: gm.Geometry,
    as_text: bool,
) -> None:
    feature = {
        'type': 'Feature',
        'geometry': geometry.__geo_interface__,
        'properties': {},
    }
    result = gm.from_features(json.dumps(feature) if as_text else feature)
    assert result.geometries[0].to_wkt() == geometry.to_wkt()


def test_to_feature_collection_accepts_features_without_restatement() -> None:
    source = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [1, 2]},
                'properties': {'name': 'A'},
                'id': 'a',
            },
            {
                'type': 'Feature',
                'geometry': None,
                'properties': None,
                'id': 2,
            },
        ],
    }
    features = gm.from_features(source)

    assert gm.to_feature_collection(features) == source
    with pytest.raises(TypeError, match='must be omitted when values is a Features'):
        gm.to_feature_collection(features, properties={'extra': True})
    with pytest.raises(TypeError, match='must be omitted when values is a Features'):
        gm.to_feature_collection(features, ids=['replacement'])
