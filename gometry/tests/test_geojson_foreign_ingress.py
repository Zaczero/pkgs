"""Revert-sensitive regressions for GeoJSON / foreign-geometry ingress D20-D23.

Each defect group pins the EXACT objective repros plus nearby positives so a
reverted fix fails and a decoder that over-rejects is caught.
"""

from __future__ import annotations

import json
import types
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from collections.abc import Callable

import gometry as gm
import pytest

# ---------------------------------------------------------------------------
# D23 — GeometryArray accepts Mapping rows (stub contract)
# ---------------------------------------------------------------------------


def test_d23_geometry_array_accepts_mapping_point() -> None:
    """EXACT repro: Mapping GeoJSON rows must construct, not TypeError."""
    arr = gm.GeometryArray([{'type': 'Point', 'coordinates': [1, 2]}])
    assert len(arr) == 1
    assert arr[0].to_wkt() == 'POINT (1 2)'
    assert arr[0].geometry_type == 'Point'


def test_d23_geometry_array_mapping_batch_and_proxy() -> None:
    """Positive: multi-row Mapping batch + MappingProxyType rows."""
    rows = [
        {'type': 'Point', 'coordinates': [1, 2]},
        types.MappingProxyType({'type': 'Point', 'coordinates': [3, 4]}),
        {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]},
    ]
    arr = gm.GeometryArray(rows)
    assert arr.to_wkt() == [
        'POINT (1 2)',
        'POINT (3 4)',
        'LINESTRING (0 0, 1 1)',
    ]


def test_d23_geometry_array_native_and_mapping_mixed() -> None:
    """Positive: native Geometry rows still work beside Mapping rows."""
    arr = gm.GeometryArray([
        gm.Point(0, 0),
        {'type': 'Point', 'coordinates': [1, 2]},
    ])
    assert arr.to_wkt() == ['POINT (0 0)', 'POINT (1 2)']


def test_d23_geometry_array_mapping_with_crs_kwarg() -> None:
    """Positive: explicit array crs= tags Mapping rows with no embedded crs."""
    arr = gm.GeometryArray(
        [{'type': 'Point', 'coordinates': [1, 2]}],
        crs=4326,
    )
    assert arr.crs == 'EPSG:4326'
    assert arr[0].to_wkt() == 'POINT (1 2)'


# ---------------------------------------------------------------------------
# D21 — nested legacy GeoJSON CRS reconciled on from_geojson
# ---------------------------------------------------------------------------

_LEGACY_3857 = {'type': 'name', 'properties': {'name': 'EPSG:3857'}}
_LEGACY_4258 = {'type': 'name', 'properties': {'name': 'EPSG:4258'}}
_LEGACY_4326 = {'type': 'name', 'properties': {'name': 'EPSG:4326'}}
_LEGACY_CRS84 = {'type': 'name', 'properties': {'name': 'OGC:CRS84'}}


def test_d21_from_geojson_rejects_nested_geometry_crs_conflict() -> None:
    """EXACT repro: nested geometry crs=3857 vs crs=4326 must reject like from_features."""
    feature = {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [1, 2],
            'crs': _LEGACY_3857,
        },
        'properties': {},
    }
    with pytest.raises(gm.ParseError, match='legacy GeoJSON CRS member') as exc_info:
        gm.from_geojson(feature, crs=4326)
    assert exc_info.value.format == 'geojson'
    with pytest.raises(gm.ParseError, match='legacy GeoJSON CRS member') as exc_info:
        gm.from_features(feature, crs=4326)
    assert exc_info.value.format == 'geojson'


def test_d21_from_geojson_rejects_per_feature_and_gc_child_crs() -> None:
    """Per-feature FeatureCollection crs and GeometryCollection child crs."""
    fc = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'crs': _LEGACY_3857,
                'geometry': {'type': 'Point', 'coordinates': [1, 2]},
                'properties': {},
            }
        ],
    }
    with pytest.raises(gm.ParseError, match='legacy GeoJSON CRS member'):
        gm.from_geojson(fc, crs=4326)

    gc = {
        'type': 'GeometryCollection',
        'geometries': [
            {
                'type': 'Point',
                'coordinates': [1, 2],
                'crs': _LEGACY_3857,
            }
        ],
    }
    with pytest.raises(gm.ParseError, match='legacy GeoJSON CRS member'):
        gm.from_geojson(gc, crs=4326)


def test_d21_from_geojson_nested_crs_all_frontends() -> None:
    """Dict / str / bytes all reject the same nested CRS conflict."""
    feature = {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [1, 2],
            'crs': _LEGACY_3857,
        },
        'properties': {},
    }
    text = json.dumps(feature)
    for data in (feature, text, text.encode()):
        with pytest.raises(
            gm.ParseError, match='legacy GeoJSON CRS member'
        ) as exc_info:
            gm.from_geojson(data, crs=4326)
        assert exc_info.value.format == 'geojson'


@pytest.mark.parametrize(
    'carrier',
    [
        pytest.param('mapping', id='mapping'),
        pytest.param('text', id='text'),
        pytest.param('bytes', id='bytes'),
    ],
)
def test_from_features_reports_envelope_crs_conflict_first(carrier: str) -> None:
    """Feature envelope CRS wins diagnostic precedence for every carrier."""
    feature = {
        'type': 'Feature',
        'crs': _LEGACY_3857,
        'geometry': {
            'type': 'Point',
            'coordinates': [1, 2],
            'crs': _LEGACY_4258,
        },
        'properties': {},
    }
    encoded = json.dumps(feature)
    data = {
        'mapping': feature,
        'text': encoded,
        'bytes': encoded.encode(),
    }[carrier]

    with pytest.raises(gm.ParseError, match=r'\(EPSG:3857\)') as exc_info:
        gm.from_features(data, crs=4326)
    assert exc_info.value.format == 'geojson'


def test_d21_from_geojson_matching_nested_crs_accepted() -> None:
    """Positive: nested EPSG:4326 agrees with default crs= and parses."""
    feature = {
        'type': 'Feature',
        'geometry': {
            'type': 'Point',
            'coordinates': [1, 2],
            'crs': _LEGACY_4326,
        },
        'properties': {},
    }
    g = gm.from_geojson(feature, crs=4326)
    assert g.to_wkt() == 'POINT (1 2)'
    assert g.crs == 'EPSG:4326'

    # No nested crs still works.
    plain = {
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [3, 4]},
        'properties': {},
    }
    assert gm.from_geojson(plain).to_wkt() == 'POINT (3 4)'


# ---------------------------------------------------------------------------
# D22 — foreign-geometry coerce: semantic CRS match (IgnoreAxisOrder)
# ---------------------------------------------------------------------------


class _Geo3857:
    __geo_interface__: ClassVar[dict[str, object]] = {
        'type': 'Point',
        'coordinates': [1, 2],
        'crs': _LEGACY_3857,
    }


class _Geo4326:
    __geo_interface__: ClassVar[dict[str, object]] = {
        'type': 'Point',
        'coordinates': [1, 2],
        'crs': _LEGACY_4326,
    }


class _GeoCrs84:
    __geo_interface__: ClassVar[dict[str, object]] = {
        'type': 'Point',
        'coordinates': [1, 2],
        'crs': _LEGACY_CRS84,
    }


class _GeoPlain:
    __geo_interface__: ClassVar[dict[str, object]] = {
        'type': 'Point',
        'coordinates': [1, 2],
    }


def _legacy_mapping(name: str) -> dict[str, object]:
    return {
        'type': 'Point',
        'coordinates': [1, 2],
        'crs': {'type': 'name', 'properties': {'name': name}},
    }


def _assert_geojson_parse_error(exc: BaseException) -> None:
    assert isinstance(exc, gm.ParseError)
    assert exc.format == 'geojson'
    assert 'legacy GeoJSON CRS member' in str(exc)


@pytest.mark.parametrize(
    'foreign',
    [
        _legacy_mapping('EPSG:3857'),
        _Geo3857(),
    ],
    ids=['mapping', 'geo_interface'],
)
def test_d22_geometry_array_crs_conflict_is_parse_error(foreign: object) -> None:
    """Embedded 3857 vs explicit crs=4326 → ParseError(format=GeoJSON)."""
    with pytest.raises(gm.ParseError) as exc_info:
        gm.GeometryArray([foreign], crs=4326)
    _assert_geojson_parse_error(exc_info.value)


@pytest.mark.parametrize(
    'foreign',
    [
        _legacy_mapping('EPSG:3857'),
        _Geo3857(),
    ],
    ids=['mapping', 'geo_interface'],
)
def test_d22_require_crs_conflict_is_parse_error(foreign: object) -> None:
    """Require conflict is ParseError, not silent mislabel / CRSMismatchError."""
    with pytest.raises(gm.ParseError) as exc_info:
        gm.require(foreign, crs=4326)
    _assert_geojson_parse_error(exc_info.value)


@pytest.mark.parametrize(
    ('foreign', 'expected_crs'),
    [
        (_legacy_mapping('EPSG:4326'), 'EPSG:4326'),
        (_Geo4326(), 'EPSG:4326'),
        (_legacy_mapping('OGC:CRS84'), 'EPSG:4326'),
        (_GeoCrs84(), 'EPSG:4326'),
        (
            {'type': 'Point', 'coordinates': [1, 2]},
            'EPSG:4326',
        ),
        (_GeoPlain(), 'EPSG:4326'),
    ],
    ids=[
        'mapping-4326',
        'iface-4326',
        'mapping-crs84',
        'iface-crs84',
        'mapping-plain',
        'iface-plain',
    ],
)
def test_d22_geometry_array_accepts_matching_and_plain(
    foreign: object,
    expected_crs: str,
) -> None:
    """Accept matching / CRS84 semantic match / absent under crs=4326."""
    arr = gm.GeometryArray([foreign], crs=4326)
    assert arr.crs == expected_crs
    assert arr[0].to_wkt() == 'POINT (1 2)'


@pytest.mark.parametrize(
    'foreign',
    [
        _legacy_mapping('EPSG:4326'),
        _Geo4326(),
        _legacy_mapping('OGC:CRS84'),
        _GeoCrs84(),
        {'type': 'Point', 'coordinates': [1, 2]},
        _GeoPlain(),
    ],
    ids=['m4326', 'i4326', 'mcrs84', 'icrs84', 'mplain', 'iplain'],
)
def test_d22_require_accepts_matching_crs84_and_plain(foreign: object) -> None:
    """Require accepts matching, CRS84 semantic match, and plain under crs=4326."""
    g = gm.require(foreign, crs=4326)
    assert g.crs == 'EPSG:4326'
    assert g.to_wkt() == 'POINT (1 2)'


def test_d22_geometry_array_adopts_embedded_when_crs_omitted() -> None:
    """Adopt path: embedded 3857 with omitted crs → EPSG:3857."""
    arr = gm.GeometryArray([_legacy_mapping('EPSG:3857')])
    assert arr.crs == 'EPSG:3857'
    assert arr[0].to_wkt() == 'POINT (1 2)'
    arr_iface = gm.GeometryArray([_Geo3857()])
    assert arr_iface.crs == 'EPSG:3857'

    req = gm.require(_legacy_mapping('EPSG:3857'))
    assert req.crs == 'EPSG:3857'


def test_geojson_malformed_geometry_has_one_path_across_text_mapping_interface() -> (
    None
):
    """All GeoJSON carriers use the same structured geometry path."""
    payload = {
        'type': 'GeometryCollection',
        'geometries': [
            {'type': 'LineString', 'coordinates': [[0, 0], [1, {'bad': True}]]}
        ],
    }

    class Interface:
        @property
        def __geo_interface__(self):
            return payload

    errors = []
    for value in (json.dumps(payload), payload, Interface()):
        with pytest.raises(gm.ParseError) as exc_info:
            gm.from_geojson(value, crs=None)
        errors.append(exc_info.value)

    assert [(error.format, error.path) for error in errors] == [
        ('geojson', '$.geometries[0].coordinates[1][1]')
    ] * 3

    with pytest.raises(gm.ParseError) as iterable_error:
        gm.from_geojson([payload], crs=None)
    assert iterable_error.value.path == '$[0].geometries[0].coordinates[1][1]'

    collection = {
        'type': 'FeatureCollection',
        'features': [
            {'type': 'Feature', 'geometry': payload, 'properties': {}},
        ],
    }
    with pytest.raises(gm.ParseError) as collection_error:
        gm.from_geojson(collection)
    assert collection_error.value.path == (
        '$.features[0].geometry.geometries[0].coordinates[1][1]'
    )

    feature = {'type': 'Feature', 'geometry': payload, 'properties': {}}
    for value in (feature, json.dumps(feature)):
        with pytest.raises(gm.ParseError) as feature_error:
            gm.from_features(value)
        assert feature_error.value.path == (
            '$.geometry.geometries[0].coordinates[1][1]'
        )


@pytest.mark.parametrize(
    'sequence_factory',
    [pytest.param(list, id='list'), pytest.param(tuple, id='tuple')],
)
def test_geojson_nested_sequence_conversion_stops_at_first_unsupported_child(
    sequence_factory: Callable[..., object],
) -> None:
    class FirstUnsupported:
        pass

    class LaterUnsupported:
        pass

    coordinates = sequence_factory((
        sequence_factory((FirstUnsupported(), 0)),
        sequence_factory((LaterUnsupported(), 0)),
    ))

    with pytest.raises(
        gm.ParseError, match='unsupported JSON value type FirstUnsupported'
    ) as exc_info:
        gm.from_geojson(
            {'type': 'LineString', 'coordinates': coordinates},
            crs=None,
        )
    assert exc_info.value.format == 'geojson'


@pytest.mark.parametrize(
    'sequence_factory',
    [pytest.param(list, id='list'), pytest.param(tuple, id='tuple')],
)
def test_geojson_nested_sequence_conversion_keeps_geometry_path(
    sequence_factory: Callable[..., object],
) -> None:
    coordinates = sequence_factory((
        sequence_factory((0, 0)),
        sequence_factory((1, 'not numeric')),
        sequence_factory((2, 0)),
    ))

    with pytest.raises(gm.ParseError) as exc_info:
        gm.from_geojson(
            {'type': 'LineString', 'coordinates': coordinates},
            crs=None,
        )

    assert exc_info.value.path == '$.coordinates[1][1]'


def test_geojson_multipolygon_invalid_ordinate_reports_nested_path() -> None:
    payload = {
        'type': 'MultiPolygon',
        'coordinates': [[[[0, 0], [1, {'bad': True}], [0, 1], [0, 0]]]],
    }

    with pytest.raises(gm.ParseError) as exc_info:
        gm.from_geojson(payload, crs=None)
    assert exc_info.value.path == '$.coordinates[0][0][1][1]'


def test_require_feature_projection_rejects_non_json_property_values() -> None:
    class Opaque:
        pass

    feature = {
        'type': 'Feature',
        'properties': {'opaque': Opaque()},
        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
    }
    with pytest.raises(
        gm.ParseError, match='unsupported JSON value type Opaque'
    ) as exc_info:
        gm.require(feature)
    assert exc_info.value.format == 'geojson'


def test_wkt_looking_text_keeps_wkt_format_after_auto_selection() -> None:
    with pytest.raises(gm.ParseError) as exc_info:
        gm.require('POINT (0)')
    assert exc_info.value.format == 'wkt'


def test_d22_geometry_array_crs_free_when_omitted() -> None:
    """Absent embedded + omitted crs → remain CRS-free."""
    arr = gm.GeometryArray([{'type': 'Point', 'coordinates': [1, 2]}])
    assert arr.crs is None
    assert arr[0].crs is None

    plain = gm.GeometryArray([_GeoPlain()])
    assert plain.crs is None

    req = gm.require({'type': 'Point', 'coordinates': [1, 2]})
    assert req.crs is None


def test_d22_require_frontend_parity_text_and_bytes() -> None:
    """Require Mapping / __geo_interface__ / JSON text / JSON bytes agree."""
    conflict = _legacy_mapping('EPSG:3857')
    match = _legacy_mapping('OGC:CRS84')
    text_conflict = json.dumps(conflict)
    text_match = json.dumps(match)

    for data in (conflict, _Geo3857(), text_conflict, text_conflict.encode()):
        with pytest.raises(gm.ParseError) as exc_info:
            gm.require(data, crs=4326)
        _assert_geojson_parse_error(exc_info.value)

    for data in (match, _GeoCrs84(), text_match, text_match.encode()):
        g = gm.require(data, crs=4326)
        assert g.crs == 'EPSG:4326'
        assert g.to_wkt() == 'POINT (1 2)'


def test_d22_geometry_array_epoch_with_adopted_embedded_crs() -> None:
    """Foreign 3857 + epoch= first adopts CRS then attaches epoch."""
    arr = gm.GeometryArray([_Geo3857()], epoch=2020.0)
    assert arr.crs == 'EPSG:3857'
    assert arr.epoch == 2020.0


def test_d22_mixed_native_and_foreign_same_embedded_crs() -> None:
    """Mixed native EPSG:3857 + foreign embedded 3857 under adopt."""
    arr = gm.GeometryArray([
        gm.Point(0, 0, crs=3857),
        _legacy_mapping('EPSG:3857'),
    ])
    assert arr.crs == 'EPSG:3857'
    assert arr.to_wkt() == ['POINT (0 0)', 'POINT (1 2)']


def test_d22_indirect_consumers_adopt_foreign_crs() -> None:
    """SpatialIndex / union_all inherit the shared adopt chokepoint."""
    assert gm.SpatialIndex([_Geo3857()])[0].crs == 'EPSG:3857'
    assert gm.union_all([_Geo3857()]).crs == 'EPSG:3857'


def test_d22_feature_valued_geo_interface_crs() -> None:
    """Top-level Feature-valued __geo_interface__ matching and conflict."""

    class FeatureMatch:
        __geo_interface__: ClassVar[dict[str, object]] = {
            'type': 'Feature',
            'properties': {},
            'geometry': {
                'type': 'Point',
                'coordinates': [1, 2],
                'crs': _LEGACY_CRS84,
            },
        }

    class FeatureConflict:
        __geo_interface__: ClassVar[dict[str, object]] = {
            'type': 'Feature',
            'properties': {},
            'geometry': {
                'type': 'Point',
                'coordinates': [1, 2],
                'crs': _LEGACY_3857,
            },
        }

    g = gm.require(FeatureMatch(), crs=4326)
    assert g.crs == 'EPSG:4326'
    assert g.to_wkt() == 'POINT (1 2)'

    arr = gm.GeometryArray([FeatureMatch()], crs=4326)
    assert arr.crs == 'EPSG:4326'

    with pytest.raises(gm.ParseError) as exc_info:
        gm.require(FeatureConflict(), crs=4326)
    _assert_geojson_parse_error(exc_info.value)


def test_d22_from_features_nested_feature_geometry_slot_parity() -> None:
    """Nested Feature in Feature.geometry is ParseError on Mapping/text/bytes."""
    nested = {
        'type': 'Feature',
        'properties': {},
        'geometry': {
            'type': 'Feature',
            'properties': {},
            'geometry': {'type': 'Point', 'coordinates': [1, 2]},
        },
    }
    text = json.dumps(nested)
    for data in (nested, text, text.encode()):
        with pytest.raises(gm.ParseError) as exc_info:
            gm.from_features(data)
        assert exc_info.value.format == 'geojson'


def test_d22_from_geojson_fixed_document_semantics() -> None:
    """Fixed policy: omitted = OGC:CRS84; CRS84 accepts; 3857 / crs=None reject."""
    plain = {'type': 'Point', 'coordinates': [1, 2]}
    assert gm.from_geojson(plain).crs == 'OGC:CRS84'
    assert (
        gm.from_features({
            'type': 'Feature',
            'geometry': plain,
            'properties': {},
        }).geometries.crs
        == 'OGC:CRS84'
    )

    crs84 = _legacy_mapping('OGC:CRS84')
    assert gm.from_geojson(crs84).crs == 'OGC:CRS84'
    assert (
        gm.from_features({
            'type': 'Feature',
            'geometry': crs84,
            'properties': {},
        }).geometries.crs
        == 'OGC:CRS84'
    )

    conflict = _legacy_mapping('EPSG:3857')
    with pytest.raises(gm.ParseError) as exc_info:
        gm.from_geojson(conflict)
    _assert_geojson_parse_error(exc_info.value)
    with pytest.raises(gm.ParseError) as exc_info:
        gm.from_features({
            'type': 'Feature',
            'geometry': conflict,
            'properties': {},
        })
    _assert_geojson_parse_error(exc_info.value)

    with pytest.raises(gm.ParseError, match='conflicts with crs=None') as exc_info:
        gm.from_geojson(crs84, crs=None)
    assert exc_info.value.format == 'geojson'

    with pytest.raises(gm.CRSError, match='WGS84'):
        gm.from_geojson(plain, crs=3857)


def test_d22_native_frame_mismatch_still_crs_mismatch_error() -> None:
    """Native / already-decoded mismatches remain CRSMismatchError."""
    with pytest.raises(gm.CRSMismatchError):
        gm.GeometryArray([gm.Point(1, 2, crs=3857)], crs=4326)
    with pytest.raises(gm.CRSMismatchError):
        gm.require(gm.Point(1, 2, crs=3857), crs=4326)


# ---------------------------------------------------------------------------
# D20 — GeoJSON object context: frontend parity + no Feature-as-geometry
# ---------------------------------------------------------------------------


def test_d20_feature_collection_non_feature_member_rejected_all_frontends() -> None:
    """EXACT repro: FC member that is a bare Point rejects for dict/str/bytes."""
    fc = {
        'type': 'FeatureCollection',
        'features': [{'type': 'Point', 'coordinates': [1, 2]}],
    }
    text = json.dumps(fc)
    for data in (fc, text, text.encode()):
        with pytest.raises(gm.ParseError, match='must be Features'):
            gm.from_geojson(data)


def test_d20_nested_feature_in_geometry_slot_rejected() -> None:
    """EXACT repro: a Feature nested in a geometry slot is not a Geometry."""
    nested = {
        'type': 'Feature',
        'properties': {},
        'geometry': {
            'type': 'Feature',
            'properties': {},
            'geometry': {'type': 'Point', 'coordinates': [1, 2]},
        },
    }
    with pytest.raises(gm.ParseError, match='not a Feature'):
        gm.from_geojson(nested)
    text = json.dumps(nested)
    with pytest.raises(gm.ParseError, match='not a Feature'):
        gm.from_geojson(text)
    with pytest.raises(gm.ParseError, match='not a Feature'):
        gm.from_geojson(text.encode())


def test_d20_valid_feature_collection_all_frontends() -> None:
    """Positive: valid FC parses identically for dict / str / bytes."""
    fc = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'properties': {},
                'geometry': {'type': 'Point', 'coordinates': [1, 2]},
            },
            {
                'type': 'Feature',
                'properties': {},
                'geometry': None,
            },
        ],
    }
    text = json.dumps(fc)
    for data in (fc, text, text.encode()):
        arr = gm.from_geojson(data)
        assert isinstance(arr, gm.GeometryArray)
        assert len(arr) == 2
        assert arr[0].to_wkt() == 'POINT (1 2)'
        assert arr.is_missing[1]


def test_d20_valid_geometry_corpus() -> None:
    """Positive: geometry types and nested GeometryCollection still parse."""
    cases = [
        ({'type': 'Point', 'coordinates': [1, 2]}, 'POINT (1 2)'),
        (
            {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]},
            'LINESTRING (0 0, 1 1)',
        ),
        (
            {
                'type': 'Polygon',
                'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 0]]],
            },
            'POLYGON ((0 0, 1 0, 1 1, 0 0))',
        ),
        (
            {
                'type': 'GeometryCollection',
                'geometries': [
                    {'type': 'Point', 'coordinates': [1, 2]},
                    {'type': 'Point', 'coordinates': [3, 4]},
                ],
            },
            'GEOMETRYCOLLECTION (POINT (1 2), POINT (3 4))',
        ),
        (
            {
                'type': 'Feature',
                'properties': {'k': 1},
                'geometry': {'type': 'Point', 'coordinates': [5, 6]},
            },
            'POINT (5 6)',
        ),
    ]
    for obj, wkt in cases:
        assert gm.from_geojson(obj).to_wkt() == wkt
        text = json.dumps(obj)
        assert gm.from_geojson(text).to_wkt() == wkt
        assert gm.from_geojson(text.encode()).to_wkt() == wkt


def test_d20_from_features_valid_feature() -> None:
    """Positive: from_features still accepts Feature / FeatureCollection."""
    feature = {
        'type': 'Feature',
        'properties': {'a': 1},
        'geometry': {'type': 'Point', 'coordinates': [1.0, 2.0]},
    }
    feats = gm.from_features(feature)
    assert feats.geometries.to_wkt() == ['POINT (1 2)']
    assert feats.properties == ({'a': 1},)


def _mixed_ring_polygon() -> dict[str, object]:
    return {
        'type': 'Polygon',
        'coordinates': [
            [[0, 0, 1], [4, 0, 1], [0, 4, 1], [0, 0, 1]],
            [[1, 1], [2, 1], [1, 2], [1, 1]],
        ],
    }


def _assert_mixed_ring_parse_error(call: Callable[[], object], path: str) -> None:
    with pytest.raises(
        gm.ParseError,
        match=r'Polygon rings must share one coordinate axes',
    ) as exc_info:
        call()
    assert exc_info.value.format == 'geojson'
    assert exc_info.value.path == path


def test_d24_mixed_polygon_ring_axes_reject_all_geojson_readers() -> None:
    """Mixed shell/hole axes reject with the reader's precise ring path."""
    geometry = _mixed_ring_polygon()
    encoded = [geometry, json.dumps(geometry), json.dumps(geometry).encode()]
    for data in encoded:
        _assert_mixed_ring_parse_error(
            lambda data=data: gm.from_geojson(data, crs=None),
            '$.coordinates[1]',
        )
        _assert_mixed_ring_parse_error(
            lambda data=data: gm.require(data, crs=None),
            '$.coordinates[1]',
        )

    feature = {'type': 'Feature', 'geometry': geometry, 'properties': {}}
    for data in [feature, json.dumps(feature), json.dumps(feature).encode()]:
        _assert_mixed_ring_parse_error(
            lambda data=data: gm.from_features(data, crs=None),
            '$.geometry.coordinates[1]',
        )

    collection = {'type': 'FeatureCollection', 'features': [feature]}
    for data in [collection, json.dumps(collection), json.dumps(collection).encode()]:
        _assert_mixed_ring_parse_error(
            lambda data=data: gm.from_geojson(data, crs=None),
            '$.features[0].geometry.coordinates[1]',
        )
        _assert_mixed_ring_parse_error(
            lambda data=data: gm.from_features(data, crs=None),
            '$.features[0].geometry.coordinates[1]',
        )


def test_d24_mixed_multipolygon_ring_axes_reject_all_geojson_readers() -> None:
    """The nested MultiPolygon ring path uses the same admission owner."""
    geometry = {
        'type': 'MultiPolygon',
        'coordinates': [
            [
                [[0, 0, 1], [4, 0, 1], [0, 4, 1], [0, 0, 1]],
                [[1, 1], [2, 1], [1, 2], [1, 1]],
            ]
        ],
    }
    encoded = [geometry, json.dumps(geometry), json.dumps(geometry).encode()]
    for data in encoded:
        _assert_mixed_ring_parse_error(
            lambda data=data: gm.from_geojson(data, crs=None),
            '$.coordinates[0][1]',
        )
        _assert_mixed_ring_parse_error(
            lambda data=data: gm.require(data, crs=None),
            '$.coordinates[0][1]',
        )

    feature = {'type': 'Feature', 'geometry': geometry, 'properties': {}}
    for data in [feature, json.dumps(feature), json.dumps(feature).encode()]:
        _assert_mixed_ring_parse_error(
            lambda data=data: gm.from_features(data, crs=None),
            '$.geometry.coordinates[0][1]',
        )
