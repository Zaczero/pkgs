"""GeoJSON decoding preserves the document's feature and geometry shape."""

import json

import gometry as gm
import pytest


def test_geojson_document_shape_preserves_feature_data() -> None:
    geometry = {'type': 'Point', 'coordinates': [1, 2]}
    feature = {
        'type': 'Feature',
        'geometry': geometry,
        'properties': {'name': 'station', 'optional': None},
        'id': 0,
    }
    collection = {'type': 'FeatureCollection', 'features': [feature]}
    for encode in (lambda value: value, json.dumps):
        point = gm.from_geojson(encode(geometry))
        row = gm.from_geojson(encode(feature))
        rows = gm.from_geojson(encode(collection))
        assert isinstance(point, gm.Point)
        assert isinstance(row, gm.Feature)
        assert row.geometry.to_wkt() == 'POINT (1 2)'
        assert row.properties == {'name': 'station', 'optional': None}
        assert row.id == 0
        assert isinstance(rows, gm.Features)
        assert rows[0] == row


def test_geojson_mixed_rows_preserve_alignment_and_presence() -> None:
    geometry = {'type': 'Point', 'coordinates': [1, 2]}
    feature = {
        'type': 'Feature',
        'geometry': None,
        'properties': {'name': None},
        'id': 0,
    }
    consumed = []

    def source():
        for value in (geometry, json.dumps(feature), None):
            consumed.append(value)
            yield value

    features = gm.from_geojson(source())
    assert len(consumed) == 3
    assert isinstance(features, gm.Features)
    assert features.geometries.to_wkt() == ['POINT (1 2)', None, None]
    assert features.properties == (None, {'name': None}, None)
    assert features.ids == (None, 0, None)
    with pytest.raises(gm.ParseError):
        gm.from_geojson([{'type': 'FeatureCollection', 'features': []}])


def test_geojson_explicit_features_normalizes_geometry_and_empty_rows() -> None:
    geometry = {'type': 'Point', 'coordinates': [1, 2]}
    features = gm.from_geojson(geometry, as_features=True)
    assert isinstance(features, gm.Features)
    assert features.geometries.to_wkt() == ['POINT (1 2)']
    assert features.properties == (None,)
    assert features.ids == (None,)
    assert isinstance(gm.from_geojson([]), gm.GeometryArray)
    assert isinstance(gm.from_geojson([None]), gm.GeometryArray)
    assert isinstance(gm.from_geojson([], as_features=True), gm.Features)
    assert gm.from_geojson([None], as_features=True).properties == (None,)
