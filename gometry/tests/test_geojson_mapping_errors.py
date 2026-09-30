"""GeoJSON content errors are independent of the Python carrier."""

from __future__ import annotations

import json
from typing import Any

import gometry as gm
import pytest


class _Unsupported:
    pass


def _feature(geometry: dict[str, Any], **members: Any) -> dict[str, Any]:
    return {
        'type': 'Feature',
        'geometry': geometry,
        'properties': {},
        **members,
    }


def _deep_coordinates(depth: int) -> tuple[list[Any], str]:
    coordinates: list[Any] | int = 0
    for _ in range(depth):
        coordinates = [coordinates]
    return coordinates, '[' * depth + '0' + ']' * depth


def _malformed_documents() -> list[Any]:
    cases: list[tuple[str, dict[Any, Any], str, bool]] = []

    def add(
        name: str,
        mapping: dict[Any, Any],
        *,
        text: str | None = None,
        same_details: bool = True,
    ) -> None:
        cases.append((
            name,
            mapping,
            json.dumps(mapping, allow_nan=True) if text is None else text,
            same_details,
        ))

    add(
        'non-finite coordinate',
        _feature({'type': 'Point', 'coordinates': [float('nan'), 0]}),
        same_details=True,
    )
    add(
        'integer outside JSON number range',
        _feature({'type': 'Point', 'coordinates': [10**1000, 0]}),
        same_details=True,
    )
    add(
        'wrong Feature type value',
        {'type': 7, 'geometry': {'type': 'Point', 'coordinates': [0, 0]}},
    )
    add(
        'wrong geometry type value',
        _feature({'type': 7, 'coordinates': [0, 0]}),
    )
    add(
        'missing Feature geometry',
        {'type': 'Feature', 'properties': {}},
    )
    add(
        'unsupported geometry type',
        _feature({'type': 'CircularString', 'coordinates': [0, 0]}),
    )
    add(
        'missing coordinates',
        _feature({'type': 'Point'}),
    )
    add(
        'wrong coordinate member type',
        _feature({'type': 'Point', 'coordinates': 'not coordinates'}),
    )
    add(
        'wrong coordinate nesting',
        _feature({'type': 'LineString', 'coordinates': [0, 0]}),
    )
    add(
        'empty polygon ring',
        _feature({'type': 'Polygon', 'coordinates': [[]]}),
    )
    add(
        'short polygon ring',
        _feature({
            'type': 'Polygon',
            'coordinates': [[[0, 0], [1, 0], [0, 0]]],
        }),
    )
    add(
        'non-finite bbox',
        _feature(
            {'type': 'Point', 'coordinates': [0, 0]},
            bbox=[float('nan'), 0, 1, 1],
        ),
        same_details=True,
    )
    add(
        'wrong bbox type',
        _feature({'type': 'Point', 'coordinates': [0, 0]}, bbox='bad'),
    )
    add(
        'wrong bbox length',
        _feature({'type': 'Point', 'coordinates': [0, 0]}, bbox=[0, 0, 1]),
    )
    add(
        'wrong FeatureCollection bbox type',
        {
            'type': 'FeatureCollection',
            'bbox': 'bad',
            'features': [_feature({'type': 'Point', 'coordinates': [0, 0]})],
        },
    )
    add(
        'unsupported legacy CRS',
        _feature({'type': 'Point', 'coordinates': [0, 0]}, crs={}),
    )
    add(
        'wrong properties type',
        _feature({'type': 'Point', 'coordinates': [0, 0]}, properties=[]),
    )
    add(
        'unsupported nested property value',
        _feature(
            {'type': 'Point', 'coordinates': [0, 0]},
            properties={'value': _Unsupported()},
        ),
        text='{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},'
        '"properties":{"value":undefined}}',
        same_details=False,
    )
    add(
        'wrong feature id type',
        _feature({'type': 'Point', 'coordinates': [0, 0]}, id=[]),
    )
    add(
        'non-finite feature id',
        _feature({'type': 'Point', 'coordinates': [0, 0]}, id=float('nan')),
        same_details=True,
    )
    add(
        'non-string object key',
        {**_feature({'type': 'Point', 'coordinates': [0, 0]}), 7: 'bad'},
        text='{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},'
        '"properties":{},7:"bad"}',
        same_details=False,
    )
    add(
        'unsupported coordinate value',
        _feature({
            'type': 'Point',
            'coordinates': [_Unsupported(), 0],
        }),
        text='{"type":"Feature","geometry":{"type":"Point","coordinates":'
        '["not a number",0]},"properties":{}}',
        same_details=False,
    )

    coordinates, coordinates_text = _deep_coordinates(280)
    add(
        'JSON nesting limit',
        _feature({'type': 'Point', 'coordinates': coordinates}),
        text='{"type":"Feature","geometry":{"type":"Point","coordinates":'
        + coordinates_text
        + '},"properties":{}}',
        same_details=True,
    )
    return [
        pytest.param(mapping, text, same_details, id=name)
        for name, mapping, text, same_details in cases
    ]


def _read(reader: str, value: object) -> object:
    if reader == 'from_geojson':
        return gm.from_geojson(value, crs=None)
    if reader == 'from_features':
        return gm.from_features(value, crs=None)
    if reader == 'require':
        return gm.require(value, crs=None)
    raise AssertionError(f'unexpected reader: {reader}')


@pytest.mark.parametrize(
    'reader',
    [
        pytest.param('from_geojson', id='from_geojson'),
        pytest.param('from_features', id='from_features'),
        pytest.param('require', id='require'),
    ],
)
@pytest.mark.parametrize(
    ('mapping', 'text', 'same_details'),
    _malformed_documents(),
)
def test_geojson_mapping_errors_match_text(
    reader: str,
    mapping: dict[Any, Any],
    text: str,
    same_details: bool,
) -> None:
    errors = []
    for value in (text, mapping):
        with pytest.raises(gm.ParseError) as exc_info:
            _read(reader, value)
        assert exc_info.value.format == 'geojson'
        errors.append(exc_info.value)

    assert type(errors[0]) is type(errors[1])
    if same_details:
        assert str(errors[0]) == str(errors[1])
        assert errors[0].path == errors[1].path
        assert errors[0].position == errors[1].position


@pytest.mark.parametrize(
    ('mapping', '_text', '_same_details'),
    _malformed_documents(),
)
def test_geometry_array_mapping_errors_are_geojson_parse_errors(
    mapping: dict[Any, Any],
    _text: str,
    _same_details: bool,
) -> None:
    with pytest.raises(gm.ParseError) as exc_info:
        gm.GeometryArray([mapping], crs=None)
    assert exc_info.value.format == 'geojson'


def test_require_keeps_geometric_validity_error_for_both_carriers() -> None:
    polygon = {
        'type': 'Polygon',
        'coordinates': [[[0, 0], [2, 2], [0, 2], [2, 0], [0, 0]]],
    }
    text = json.dumps(polygon)

    for value in (text, polygon):
        with pytest.raises(gm.InvalidGeometryError) as exc_info:
            gm.require(value)
        assert getattr(exc_info.value, 'format', None) is None


def test_from_features_property_depth_matches_text_and_mapping() -> None:
    nested: Any = 1
    for _ in range(130):
        nested = [nested]
    feature = _feature(
        {'type': 'Point', 'coordinates': [0, 0]},
        properties={'nested': nested},
    )
    text = json.dumps(feature)
    errors = []

    for value in (text, feature):
        with pytest.raises(gm.ParseError) as exc_info:
            gm.from_features(value, crs=None)
        assert exc_info.value.format == 'geojson'
        errors.append(exc_info.value)

    assert str(errors[0]) == str(errors[1])


def test_python_mapping_protocol_type_errors_remain_python_errors() -> None:
    class BrokenMapping:
        def keys(self) -> list[str]:
            raise TypeError('broken keys')

    with pytest.raises(TypeError, match='broken keys'):
        gm.from_geojson(BrokenMapping())
    with pytest.raises(TypeError):
        gm.from_geojson(object())
