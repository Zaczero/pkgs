"""WKT/WKB/GeoJSON IO — roundtrips, feature helpers, parser strictness,
EWKB/SRID handling, and direct WKB helpers.
"""

import json
import math
import struct
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest


def test_recursive_geometry_inputs_fail_at_a_bounded_depth() -> None:
    wkt = 'POINT (0 0)'
    wkb = gm.Point(0, 0).to_wkb()
    for _ in range(130):
        wkt = f'GEOMETRYCOLLECTION ({wkt})'
        wkb = b'\x01' + struct.pack('<II', 7, 1) + wkb

    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        gm.from_wkt(wkt)
    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        gm.from_wkb(wkb)

    coordinates: list[Any] = []
    coordinates.append(coordinates)
    with pytest.raises(gm.GeometryError, match='nesting exceeds'):
        gm.from_geojson({'type': 'Point', 'coordinates': coordinates})


def test_ingress_epoch_from_wkt_wkb_and_geojson() -> None:
    point = gm.Point(1, 2, crs=4326)
    wkt = gm.from_wkt('POINT (1 2)', crs=4326, epoch=2020.0)
    wkb = gm.from_wkb(point.to_wkb(), crs=4326, epoch=2020.0)
    geojson = gm.from_geojson(
        {'type': 'Point', 'coordinates': [1, 2]}, crs=4326, epoch=2020.0
    )
    assert wkt.epoch == 2020.0
    assert wkb.epoch == 2020.0
    assert geojson.epoch == 2020.0


def test_ingress_rejects_non_finite_epoch() -> None:
    with pytest.raises(gm.GeometryError, match='coordinate epoch must be a finite'):
        gm.from_wkt('POINT (1 2)', crs=4326, epoch=float('inf'))
    with pytest.raises(gm.GeometryError, match='coordinate epoch must be a finite'):
        gm.from_wkb(gm.Point(1, 2).to_wkb(), crs=4326, epoch=float('inf'))
    with pytest.raises(gm.GeometryError, match='coordinate epoch must be a finite'):
        gm.from_geojson(
            {'type': 'Point', 'coordinates': [1, 2]}, crs=4326, epoch=float('inf')
        )


def test_from_arrow_applies_crs_and_rejects_embedded_conflict() -> None:
    bare = gm.points([1.0, 2.0], [3.0, 4.0])
    recovered = gm.from_arrow(bare.to_arrow(), crs=4326)
    assert recovered.crs == 'EPSG:4326'
    tagged = gm.points([1.0], [2.0], crs=3857).to_arrow()
    with pytest.raises(gm.CRSMismatchError, match='conflicts with the embedded'):
        gm.from_arrow(tagged, crs=4326)


def test_from_wkb_crs_is_a_fallback_not_an_override() -> None:
    plain = gm.Point(1, 2).to_wkb()
    assert gm.from_wkb(plain, crs=4326).crs == 'EPSG:4326'
    assert gm.from_wkb(plain).crs is None
    ewkb = gm.Point(1, 2, crs=4326).to_wkb(include_srid=True)
    assert gm.from_wkb(ewkb, crs=4326).crs == 'EPSG:4326'
    assert gm.from_wkb(ewkb).crs == 'EPSG:4326'
    with pytest.raises(ValueError, match='conflicts with the embedded EWKB SRID'):
        gm.from_wkb(ewkb, crs=3857)
    with pytest.raises(ValueError, match='conflicts with the embedded EWKT SRID'):
        gm.from_wkt('SRID=4326;POINT (1 2)', crs=3857)


def test_batch_wkt_wkb_plain_rows_share_explicit_frame() -> None:
    wkts = ['POINT (1 2)', 'POINT (3 4)']
    from_wkt = gm.from_wkt(wkts, crs=4326, epoch=2020.0)
    assert isinstance(from_wkt, gm.GeometryArray)
    assert from_wkt.crs == 'EPSG:4326'
    assert from_wkt.epoch == 2020.0
    assert from_wkt.to_wkt(drop_epoch=True) == wkts
    assert [row.crs for row in from_wkt] == ['EPSG:4326', 'EPSG:4326']
    assert [row.epoch for row in from_wkt] == [2020.0, 2020.0]
    wkbs = [gm.Point(1, 2).to_wkb(), gm.Point(3, 4).to_wkb()]
    from_wkb = gm.from_wkb(wkbs, crs=4326, epoch=2020.0)
    assert isinstance(from_wkb, gm.GeometryArray)
    assert from_wkb.crs == 'EPSG:4326'
    assert from_wkb.epoch == 2020.0
    assert from_wkb.to_wkt(drop_epoch=True) == wkts
    assert [row.crs for row in from_wkb] == ['EPSG:4326', 'EPSG:4326']
    assert [row.epoch for row in from_wkb] == [2020.0, 2020.0]


def test_batch_wkt_wkb_embedded_srid_still_reconciles_and_conflicts() -> None:
    ewkts = ['SRID=4326;POINT (1 2)', 'SRID=4326;POINT (3 4)']
    ewkt_array = gm.from_wkt(ewkts, crs=4326, epoch=2020.0)
    assert isinstance(ewkt_array, gm.GeometryArray)
    assert ewkt_array.crs == 'EPSG:4326'
    assert ewkt_array.epoch == 2020.0
    assert ewkt_array.to_wkt(drop_epoch=True) == ['POINT (1 2)', 'POINT (3 4)']
    ewkbs = [
        gm.Point(1, 2, crs=4326).to_wkb(include_srid=True),
        gm.Point(3, 4, crs=4326).to_wkb(include_srid=True),
    ]
    ewkb_array = gm.from_wkb(ewkbs, crs=4326, epoch=2020.0)
    assert isinstance(ewkb_array, gm.GeometryArray)
    assert ewkb_array.crs == 'EPSG:4326'
    assert ewkb_array.epoch == 2020.0
    assert ewkb_array.to_wkt(drop_epoch=True) == ['POINT (1 2)', 'POINT (3 4)']
    with pytest.raises(
        gm.CRSMismatchError, match='conflicts with the embedded EWKT SRID'
    ) as ewkt_error:
        gm.from_wkt(['POINT (0 0)', 'SRID=3857;POINT (1 2)'], crs=4326)
    assert 'array element 1' in ''.join(ewkt_error.value.__notes__)
    with pytest.raises(
        gm.CRSMismatchError, match='conflicts with the embedded EWKB SRID'
    ) as ewkb_error:
        gm.from_wkb(
            [
                gm.Point(0, 0).to_wkb(),
                gm.Point(1, 2, crs=3857).to_wkb(include_srid=True),
            ],
            crs=4326,
        )
    assert 'array element 1' in ''.join(ewkb_error.value.__notes__)
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.from_wkt(['SRID=4326;POINT (0 0)', 'SRID=3857;POINT (1 2)'])
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.from_wkb([
            gm.Point(0, 0, crs=4326).to_wkb(include_srid=True),
            gm.Point(1, 2, crs=3857).to_wkb(include_srid=True),
        ])


def test_wkb_point_z_empty_requires_all_present_ordinates_nan() -> None:
    nan = float('nan')
    empty_z = struct.pack('<BI3d', 1, 1001, nan, nan, nan)
    assert gm.from_wkb(empty_z).is_empty
    not_empty_z = struct.pack('<BI3d', 1, 1001, nan, nan, 1.0)
    with pytest.raises(gm.ParseError, match='finite'):
        gm.from_wkb(not_empty_z)


def test_geojson_legacy_wgs84_crs_member_is_ignored() -> None:
    for name in ('EPSG:4326', 'urn:ogc:def:crs:OGC:1.3:CRS84'):
        payload = {
            'type': 'Point',
            'coordinates': [1, 2],
            'crs': {'type': 'name', 'properties': {'name': name}},
        }
        point = gm.from_geojson(payload)
        assert point.to_wkt() == 'POINT (1 2)'
        assert point.crs == 'EPSG:4326'
        assert gm.from_geojson(json.dumps(payload)).to_wkt() == 'POINT (1 2)'


def test_geojson_legacy_crs_member_conflicts_with_crs() -> None:
    payload = {
        'type': 'Point',
        'coordinates': [1, 2],
        'crs': {'type': 'name', 'properties': {'name': 'EPSG:4326'}},
    }
    with pytest.raises(gm.ParseError, match='conflicts with crs='):
        gm.from_geojson(payload, crs='EPSG:4979')


def test_geojson_reader_rejects_non_wgs84_crs() -> None:
    with pytest.raises(gm.CRSError, match='WGS84'):
        gm.from_geojson({'type': 'Point', 'coordinates': [1, 2]}, crs=3857)
    assert (
        gm.from_geojson({'type': 'Point', 'coordinates': [1, 2]}, crs=4326).crs
        == 'EPSG:4326'
    )
    assert gm.from_geojson({'type': 'Point', 'coordinates': [1, 2]}).crs == 'EPSG:4326'


def test_foreign_decoders_reject_already_decoded_geometries() -> None:
    point = gm.Point(1, 2)
    with pytest.raises(TypeError, match='already decoded'):
        gm.from_geojson(point)
    with pytest.raises(TypeError, match='already decoded'):
        gm.from_geojson([point])
    with pytest.raises(TypeError, match='already decoded'):
        gm.from_arrow(point)


def test_geojson_legacy_crs_member_conflicts_with_crs_none() -> None:
    payload = {
        'type': 'Point',
        'coordinates': [1, 2],
        'crs': {'type': 'name', 'properties': {'name': 'EPSG:4326'}},
    }
    with pytest.raises(gm.ParseError, match='conflicts with crs=None'):
        gm.from_geojson(payload, crs=None)


def test_geojson_rejects_invalid_polygon_rings() -> None:
    two_point = {'type': 'Polygon', 'coordinates': [[(0, 0), (1, 0)]]}
    unclosed = {'type': 'Polygon', 'coordinates': [[(0, 0), (1, 0), (1, 1), (0, 1)]]}
    with pytest.raises(gm.ParseError, match='requires at least 4 coordinates'):
        gm.from_geojson(two_point)
    with pytest.raises(gm.ParseError, match='explicitly closed'):
        gm.from_geojson(unclosed)


def test_m05_mixed_within_sequence_axes_rejected_uniform_constraint() -> None:
    """m05: within-sequence mixed XY/XYZ is cleanly rejected (no 0-elevation).

    Decision: DOCUMENT + reject, not promotion-with-NaN. gometry's CoordSeq is
    one set of columns with a finite-coordinate invariant — NaN is not a legal
    absent-Z sentinel, and filling 0 invents elevation. GeometryCollection
    members may still differ in axes (cross-member, not within-sequence).

    EXACT repro: ``{"type":"LineString","coordinates":[[0,0],[1,1,5]]}`` raises
    on dict/text/bytes/bulk/features. Uniform sequences still parse.
    """
    mixed = {'type': 'LineString', 'coordinates': [[0, 0], [1, 1, 5]]}
    text = json.dumps(mixed)
    match = r'dimensionally uniform|mixes XY and XYZ'
    for frontend in (mixed, text, text.encode()):
        with pytest.raises(gm.ParseError, match=match) as raised:
            gm.from_geojson(frontend)
        assert type(raised.value) is gm.ParseError
        # No silent acceptance that would invent Z=0.
        assert '0' not in str(raised.value) or 'mixes' in str(raised.value)

    # Bulk + features frontends.
    with pytest.raises(gm.ParseError, match=match):
        gm.from_geojson([mixed])
    feature = {
        'type': 'Feature',
        'geometry': mixed,
        'properties': {},
    }
    for frontend in (feature, json.dumps(feature), json.dumps(feature).encode()):
        with pytest.raises(gm.ParseError, match=match):
            gm.from_features(frontend)
        with pytest.raises(gm.ParseError, match=match):
            gm.from_geojson(frontend)

    # Positive: uniform XY and uniform XYZ sequences parse; no invented Z.
    xy = gm.from_geojson({
        'type': 'LineString',
        'coordinates': [[0, 0], [1, 1]],
    })
    assert xy.coordinate_axes == 'XY'
    assert xy.to_wkt() == 'LINESTRING (0 0, 1 1)'

    xyz = gm.from_geojson({
        'type': 'LineString',
        'coordinates': [[0, 0, 1], [1, 1, 5]],
    })
    assert xyz.coordinate_axes == 'XYZ'
    assert list(xyz.coords) == [(0.0, 0.0, 1.0), (1.0, 1.0, 5.0)]
    assert all(c[2] != 0.0 or c[2] == 1.0 for c in xyz.coords)  # no silent zeros

    # Cross-member (GeometryCollection) may still mix axes.
    gc = gm.from_geojson({
        'type': 'GeometryCollection',
        'geometries': [
            {'type': 'Point', 'coordinates': [0, 0]},
            {'type': 'Point', 'coordinates': [1, 1, 5]},
        ],
    })
    assert gc.coordinate_axes == 'XYZ'
    assert [p.coordinate_axes for p in gc.parts] == ['XY', 'XYZ']


def test_m04_geojson_ring_closure_compares_active_ordinates() -> None:
    """m04: RFC 7946 ring closure requires identical first/last positions.

    EXACT repro: first/last match in XY but differ in Z must be REJECTED.
    A properly closed XYZ (and XY) ring is still accepted. Covers dict/str/bytes.
    """
    z_mismatch = {
        'type': 'Polygon',
        'coordinates': [[[0, 0, 1], [1, 0, 1], [1, 1, 1], [0, 0, 2]]],
    }
    text = json.dumps(z_mismatch)
    for frontend in (z_mismatch, text, text.encode()):
        with pytest.raises(gm.ParseError, match='explicitly closed') as raised:
            gm.from_geojson(frontend)
        assert type(raised.value) is gm.ParseError

    # Positive: properly closed XYZ / XY rings still parse.
    closed_xyz = {
        'type': 'Polygon',
        'coordinates': [[[0, 0, 1], [1, 0, 1], [1, 1, 1], [0, 0, 1]]],
    }
    for frontend in (
        closed_xyz,
        json.dumps(closed_xyz),
        json.dumps(closed_xyz).encode(),
    ):
        g = gm.from_geojson(frontend)
        assert g.geometry_type == 'Polygon'
        assert g.coordinate_axes == 'XYZ'
        exterior = list(g.exterior.coords)
        assert exterior[0] == exterior[-1]

    closed_xy = {
        'type': 'Polygon',
        'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 0]]],
    }
    assert gm.from_geojson(closed_xy).geometry_type == 'Polygon'


def test_to_geojson_requires_a_wgs84_frame() -> None:
    projected = gm.Point(500000.0, 4649776.0, crs=32633)
    with pytest.raises(ValueError, match='RFC 7946'):
        projected.to_geojson()
    with pytest.raises(ValueError, match='RFC 7946'):
        gm.GeometryArray([projected]).to_geojson()
    assert json.loads(projected.to_crs(4326).to_geojson())['type'] == 'Point'
    assert json.loads(gm.Point(1, 2).to_geojson())['type'] == 'Point'


def test_ewkt_roundtrip_preserves_crs() -> None:
    point = gm.Point(1, 2, crs=4326)
    ewkt = point.to_wkt(include_srid=True)
    assert ewkt.startswith('SRID=4326;')
    recovered = gm.from_wkt(ewkt)
    assert recovered.crs == 'EPSG:4326'
    assert gm.equals_exact(recovered, point)
    with pytest.raises(gm.CRSError, match='EWKT SRID requires an EPSG-authority CRS'):
        gm.Point(0, 0, crs='OGC:CRS84').to_wkt(include_srid=True)


def test_wkt_wkb_and_geojson_roundtrip() -> None:
    polygon = gm.from_wkt('POLYGON ((0 0, 2 0, 2 1, 0 0))', crs=4326)
    wkb = polygon.to_wkb(include_srid=True)
    recovered = gm.from_wkb(wkb)
    assert recovered.crs == 'EPSG:4326'
    assert recovered.to_wkt() == 'POLYGON ((0 0, 2 0, 2 1, 0 0))'
    assert gm.from_geojson(polygon.to_geojson()).to_wkt() == polygon.to_wkt()
    parsed = gm.from_wkt(['POINT Z (1 2 3)', 'LINESTRING Z (0 0 1, 1 1 2)'], crs=4979)
    assert isinstance(parsed, gm.GeometryArray)
    assert parsed.crs == 'EPSG:4979'
    assert parsed.to_wkt(output_dimension=2) == ['POINT (1 2)', 'LINESTRING (0 0, 1 1)']
    geojson_texts = parsed.to_geojson(include_z=False)
    geojson_recovered = gm.from_geojson(geojson_texts, crs=4326)
    assert isinstance(geojson_recovered, gm.GeometryArray)
    assert geojson_recovered.crs == 'EPSG:4326'
    assert geojson_recovered.to_wkt() == ['POINT (1 2)', 'LINESTRING (0 0, 1 1)']
    geojson_mapping_recovered = gm.from_geojson(
        [
            {'type': 'Point', 'coordinates': [1, 2]},
            {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]},
        ],
        crs=4326,
    )
    mixed_geojson_recovered = gm.from_geojson(
        [
            {'type': 'Point', 'coordinates': [1, 2]},
            '{"type":"LineString","coordinates":[[0,0],[1,1]]}',
        ],
        crs=4326,
    )
    assert isinstance(geojson_mapping_recovered, gm.GeometryArray)
    assert isinstance(mixed_geojson_recovered, gm.GeometryArray)
    assert geojson_mapping_recovered.crs == 'EPSG:4326'
    assert geojson_mapping_recovered.to_wkt() == [
        'POINT (1 2)',
        'LINESTRING (0 0, 1 1)',
    ]
    assert mixed_geojson_recovered.to_wkt() == ['POINT (1 2)', 'LINESTRING (0 0, 1 1)']
    with pytest.raises(gm.InvalidGeometryError, match='GeoJSON has no M'):
        gm.GeometryArray([gm.Point(1, 2, m=3)]).to_geojson()


def test_geo_interface_strips_m_but_coords_preserves_m() -> None:
    point_m = gm.Point(1, 2, m=3)
    assert point_m.__geo_interface__ == {'type': 'Point', 'coordinates': [1.0, 2.0]}
    assert point_m.coords.to_nested() == [1.0, 2.0, 3.0]
    point_zm = gm.Point(1, 2, z=9, m=7)
    assert point_zm.__geo_interface__ == {
        'type': 'Point',
        'coordinates': [1.0, 2.0, 9.0],
    }
    assert point_zm.coords.to_nested() == [1.0, 2.0, 9.0, 7.0]


def test_geojson_feature_and_feature_collection_inputs() -> None:

    class GeoInterfaceObject:
        @property
        def __geo_interface__(self) -> dict[str, object]:
            return {'type': 'Point', 'coordinates': [1, 2]}

    feature = {
        'type': 'Feature',
        'properties': {'name': 'origin'},
        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
    }
    collection = {
        'type': 'FeatureCollection',
        'features': [
            feature,
            {
                'type': 'Feature',
                'properties': {},
                'geometry': {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]},
            },
        ],
    }
    assert gm.from_geojson(feature, crs=4326).to_wkt() == 'POINT (1 2)'
    assert gm.from_geojson(json.dumps(feature)).to_wkt() == 'POINT (1 2)'
    assert gm.from_geojson(feature).to_wkt() == 'POINT (1 2)'
    assert gm.from_geojson(GeoInterfaceObject()).to_wkt() == 'POINT (1 2)'
    with pytest.raises(TypeError, match='already decoded'):
        gm.from_geojson(gm.Point(1, 2))
    geo_interface_array = gm.from_geojson(
        [GeoInterfaceObject(), {'type': 'Point', 'coordinates': [3, 4]}], crs=4326
    )
    assert isinstance(geo_interface_array, gm.GeometryArray)
    assert geo_interface_array.crs == 'EPSG:4326'
    assert geo_interface_array.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']
    with pytest.raises(TypeError, match='array element 1'):
        gm.from_geojson([GeoInterfaceObject(), gm.Point(3, 4)], crs=4326)
    parsed = gm.from_geojson(collection)
    assert isinstance(parsed, gm.GeometryArray)
    assert parsed.crs == 'EPSG:4326'
    assert parsed.to_wkt() == ['POINT (1 2)', 'LINESTRING (0 0, 1 1)']
    with pytest.raises(ValueError, match='Feature requires geometry'):
        gm.from_geojson({'type': 'Feature'})
    with pytest.raises(ValueError, match='FeatureCollection requires features'):
        gm.from_geojson({'type': 'FeatureCollection'})


def test_from_geojson_accepts_bytes_bytearray_and_iterables() -> None:
    point = b'{"type":"Point","coordinates":[1,2]}'
    assert gm.from_geojson(point).to_wkt() == 'POINT (1 2)'
    assert gm.from_geojson(bytearray(point)).to_wkt() == 'POINT (1 2)'
    rows = [
        b'{"type":"Point","coordinates":[1,2]}',
        b'{"type":"LineString","coordinates":[[0,0],[1,1]]}',
    ]
    parsed = gm.from_geojson(rows, crs=4326)
    assert isinstance(parsed, gm.GeometryArray)
    assert parsed.crs == 'EPSG:4326'
    assert parsed.to_wkt() == ['POINT (1 2)', 'LINESTRING (0 0, 1 1)']
    parsed_bytearray = gm.from_geojson([bytearray(row) for row in rows], crs=4326)
    assert isinstance(parsed_bytearray, gm.GeometryArray)
    assert parsed_bytearray.to_wkt() == parsed.to_wkt()
    with pytest.raises(gm.ParseError, match='GeoJSON'):
        gm.from_geojson(b'\xff')


def test_r18_geojson_duplicate_key_last_wins_all_frontends() -> None:
    """R18: recognized-member last-value-wins is identical on dict/str/bytes.

    Exact audit repro: shadowed non-string ``type`` then string ``type`` must
    yield Point on every frontend (previously str rejected while bytes accepted).
    """
    s = '{"type":123,"type":"Point","coordinates":[1,2]}'
    expected = 'POINT (1 2)'
    assert gm.from_geojson(s).to_wkt() == expected
    assert gm.from_geojson(s.encode()).to_wkt() == expected
    # Python dict cannot carry duplicate keys; json.loads last-wins matches.
    import json

    assert gm.from_geojson(json.loads(s)).to_wkt() == expected
    assert gm.from_geojson([s]).to_wkt() == [expected]

    # Shadowed coordinates / features / geometries / geometry also last-wins.
    assert (
        gm.from_geojson(
            '{"type":"Point","coordinates":[0,0],"coordinates":[1,2]}'
        ).to_wkt()
        == expected
    )
    assert (
        gm.from_geojson(
            b'{"type":"Point","coordinates":[0,0],"coordinates":[1,2]}'
        ).to_wkt()
        == expected
    )
    feat = (
        '{"type":"Feature","geometry":null,'
        '"geometry":{"type":"Point","coordinates":[1,2]},"properties":{}}'
    )
    assert gm.from_geojson(feat).to_wkt() == expected
    assert gm.from_geojson(feat.encode()).to_wkt() == expected

    # Last type wins when final value is invalid → reject everywhere.
    bad = '{"type":"Point","type":123,"coordinates":[1,2]}'
    for v in (bad, bad.encode()):
        with pytest.raises(gm.ParseError, match=r'type|string'):
            gm.from_geojson(v)


def test_m03_from_features_rejects_dual_defining_members() -> None:
    """m03: from_features shares from_geojson's RFC §7.1 defining-member check.

    EXACT repro: a Feature carrying both defining ``coordinates`` and
    ``geometry`` must be REJECTED on dict/str/bytes — not accepted by ignoring
    coordinates. A normal Feature still parses.
    """
    import json

    dual = {
        'type': 'Feature',
        'coordinates': [9, 9],
        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
        'properties': {},
    }
    text = json.dumps(dual)
    for frontend in (dual, text, text.encode()):
        with pytest.raises(
            gm.ParseError,
            match=r'coordinates.*geometries|RFC 7946',
        ) as raised:
            gm.from_features(frontend)
        assert type(raised.value) is gm.ParseError
        # Parity with from_geojson.
        with pytest.raises(gm.ParseError, match=r'coordinates|RFC 7946'):
            gm.from_geojson(frontend)

    # Nested in a FeatureCollection (JSON path walks FeatureRowMeta).
    collection = {
        'type': 'FeatureCollection',
        'features': [dual],
    }
    for frontend in (collection, json.dumps(collection), json.dumps(collection).encode()):
        with pytest.raises(gm.ParseError, match=r'coordinates|RFC 7946'):
            gm.from_features(frontend)

    # Positive: a valid Feature still parses on all frontends.
    valid = {
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
        'properties': {'k': 1},
    }
    for frontend in (valid, json.dumps(valid), json.dumps(valid).encode()):
        feats = gm.from_features(frontend)
        assert feats.geometries.to_wkt() == ['POINT (1 2)']
        assert feats.properties == ({'k': 1},)


def test_r19_rfc7946_cross_type_defining_members_rejected() -> None:
    """R19: RFC 7946 §7.1 cross-type defining-member exclusions on all frontends."""
    import json

    cases = [
        {'type': 'FeatureCollection', 'features': [], 'coordinates': []},
        {
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [1, 2]},
            'properties': {},
            'geometries': [],
        },
        {'type': 'Point', 'coordinates': [1, 2], 'properties': {}},
        {'type': 'Point', 'coordinates': [1, 2], 'features': []},
        {'type': 'FeatureCollection', 'features': [], 'geometries': []},
        {'type': 'GeometryCollection', 'geometries': [], 'coordinates': []},
        {'type': 'Point', 'coordinates': [1, 2], 'geometry': None},
    ]
    for case in cases:
        text = json.dumps(case)
        for frontend in (case, text, text.encode()):
            with pytest.raises(gm.ParseError, match=r'RFC 7946|must not contain'):
                gm.from_geojson(frontend)

    # Arbitrary foreign members remain valid.
    foreign = {'type': 'Point', 'coordinates': [1, 2], 'title': 'ok', 'extra': 1}
    assert gm.from_geojson(foreign).to_wkt() == 'POINT (1 2)'
    assert gm.from_geojson(json.dumps(foreign)).to_wkt() == 'POINT (1 2)'
    assert gm.from_geojson(json.dumps(foreign).encode()).to_wkt() == 'POINT (1 2)'

    # Valid kinds still parse on all frontends.
    for kind, payload in [
        ('Point', {'type': 'Point', 'coordinates': [1, 2]}),
        (
            'Feature',
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [1, 2]},
                'properties': {'k': 1},
            },
        ),
        (
            'FeatureCollection',
            {
                'type': 'FeatureCollection',
                'features': [
                    {
                        'type': 'Feature',
                        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
                        'properties': {},
                    }
                ],
            },
        ),
        (
            'GeometryCollection',
            {
                'type': 'GeometryCollection',
                'geometries': [{'type': 'Point', 'coordinates': [1, 2]}],
            },
        ),
    ]:
        text = json.dumps(payload)
        for frontend in (payload, text, text.encode()):
            out = gm.from_geojson(frontend)
            if kind == 'FeatureCollection':
                assert out.to_wkt() == ['POINT (1 2)']
            elif kind == 'GeometryCollection':
                assert 'POINT (1 2)' in out.to_wkt()
            else:
                assert out.to_wkt() == 'POINT (1 2)'


def test_geojson_text_rows_use_direct_coordinate_decoder() -> None:
    rows = [
        '{"coordinates":[[0,0],[1,1]],"type":"LineString"}',
        '{"coordinates":[[[0,0],[2,0],[2,2],[0,0]]],"type":"Polygon"}',
        # A shadowed coordinate member is ignored just as serde map decoding
        # ignored it before the direct seeded decoder was introduced.
        '{"coordinates":"shadowed","type":"Point","coordinates":[3,4,5]}',
        '{"type":"Feature","geometry":null}',
    ]
    parsed = gm.from_geojson(rows, crs=None)
    assert parsed.to_wkt()[:3] == [
        'LINESTRING (0 0, 1 1)',
        'POLYGON ((0 0, 2 0, 2 2, 0 0))',
        'POINT Z (3 4 5)',
    ]
    assert parsed.is_missing.tolist() == [False, False, False, True]


def test_from_geojson_dict_feature_collection_skips_properties() -> None:

    class NotJson:
        pass

    feature = {
        'type': 'Feature',
        'properties': {'bad': NotJson()},
        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
    }
    collection = {
        'type': 'FeatureCollection',
        'features': [
            feature,
            {
                'type': 'Feature',
                'properties': {'bad': NotJson()},
                'geometry': {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]},
            },
        ],
    }
    assert gm.from_geojson(feature).to_wkt() == 'POINT (1 2)'
    parsed = gm.from_geojson(collection)
    assert isinstance(parsed, gm.GeometryArray)
    assert parsed.to_wkt() == ['POINT (1 2)', 'LINESTRING (0 0, 1 1)']


def test_mixed_geometry_and_bytes_batch_coerces_per_item() -> None:
    """Characterization: mixed Geometry + WKB batches succeed via per-item coerce.

    Pure geometry and pure-bytes batches take specialized lanes; a mixed batch
    falls through to per-item ``coerce_geometry`` (not an error). Pin both
    lead-item orders so a first-item dispatch refactor cannot change the lane.
    """
    wkb = gm.Point(1, 2).to_wkb()
    geom = gm.Point(3, 4)
    bytes_first = gm.GeometryArray([wkb, geom])
    assert bytes_first.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']
    geom_first = gm.GeometryArray([geom, wkb])
    assert geom_first.to_wkt() == ['POINT (3 4)', 'POINT (1 2)']


def test_geometry_array_and_geometry_item_coercion_accept_wkb_and_geo_interface() -> (
    None
):

    class GeoInterfaceObject:
        def __init__(self, x: float, y: float) -> None:
            self.x = x
            self.y = y

        @property
        def __geo_interface__(self) -> dict[str, object]:
            return {'type': 'Point', 'coordinates': [self.x, self.y]}

    wkbs = [gm.Point(1, 2).to_wkb(), gm.Point(3, 4).to_wkb()]
    array = gm.GeometryArray(wkbs)
    assert array.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']
    from_wkb = gm.from_wkb(wkbs)
    assert isinstance(from_wkb, gm.GeometryArray)
    assert array.to_wkt() == from_wkb.to_wkt()
    framed = gm.GeometryArray(wkbs, crs=4326, epoch=2020.0)
    assert framed.crs == 'EPSG:4326'
    assert framed.epoch == 2020.0
    assert [row.crs for row in framed] == ['EPSG:4326', 'EPSG:4326']
    assert [row.epoch for row in framed] == [2020.0, 2020.0]
    with pytest.raises(gm.ParseError, match='invalid WKB') as parse_error:
        gm.GeometryArray([wkbs[0], b'\x01\xff\xff\xff\xff'])
    assert not hasattr(parse_error.value, '__notes__')
    points = gm.GeometryArray([gm.Point(1, 2), gm.Point(3, 4)])
    assert points.to_wkt() == ['POINT (1 2)', 'POINT (3 4)']
    with pytest.raises(TypeError, match=r'GeometryArray\(\[geom\]\).*GeometryArray\(geom.parts\)'):
        gm.GeometryArray(gm.Point(1, 2))
    multipart = gm.MultiPoint([(0, 0), (1, 1), (2, 2)])
    with pytest.raises(TypeError, match=r'GeometryArray\(\[geom\]\).*GeometryArray\(geom.parts\)'):
        gm.GeometryArray(multipart)
    mixed = gm.GeometryArray([wkbs[0], gm.Point(3, 4), GeoInterfaceObject(5, 6)])
    assert mixed.to_wkt() == ['POINT (1 2)', 'POINT (3 4)', 'POINT (5 6)']
    inserted = gm.SpatialIndex()
    np.testing.assert_array_equal(
        inserted.insert([wkbs[0], gm.Point(3, 4), GeoInterfaceObject(5, 6)]),
        [0, 1, 2],
    )
    np.testing.assert_array_equal(gm.nearest(list(mixed), gm.Point(5, 6)), [2])
    left, right = gm.join(
        [wkbs[0], gm.Point(3, 4), GeoInterfaceObject(5, 6)],
        [gm.Point(1, 2)],
    )
    np.testing.assert_array_equal(left, [0])
    np.testing.assert_array_equal(right, [0])
    np.testing.assert_allclose(
        gm.bounds([wkbs[0], gm.Point(3, 4), GeoInterfaceObject(5, 6)]),
        [[1, 2, 1, 2], [3, 4, 3, 4], [5, 6, 5, 6]],
    )
    assert gm.get_coordinates([
        wkbs[0],
        gm.Point(3, 4),
        GeoInterfaceObject(5, 6),
    ]).tolist() == [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
    indexed = gm.SpatialIndex(wkbs)
    np.testing.assert_array_equal(indexed.query(gm.Point(1, 2)), [0])
    geo_index = gm.SpatialIndex([GeoInterfaceObject(1, 2), GeoInterfaceObject(3, 4)])
    np.testing.assert_array_equal(geo_index.query(gm.Point(3, 4)), [1])
    hetero_index = gm.SpatialIndex([GeoInterfaceObject(1, 2), gm.Point(3, 4)])
    np.testing.assert_array_equal(hetero_index.query(gm.Point(1, 2)), [0])
    ewkbs = [
        gm.Point(1, 2, crs=4326).to_wkb(include_srid=True),
        gm.Point(3, 4, crs=4326).to_wkb(include_srid=True),
    ]
    reconciled = gm.GeometryArray(ewkbs)
    expected = gm.from_wkb(ewkbs)
    assert isinstance(expected, gm.GeometryArray)
    assert reconciled.crs == expected.crs == 'EPSG:4326'
    assert reconciled.to_wkt() == expected.to_wkt()
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.GeometryArray([
            ewkbs[0],
            gm.Point(3, 4, crs=3857).to_wkb(include_srid=True),
        ])


def test_from_features_text_fast_path_preserves_geojson_payloads() -> None:
    payload = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [1, 2]},
                'properties': {
                    'name': 'A',
                    'nested': {'items': [1, 2.5, None, 'x']},
                    'flag': True,
                },
                'id': 'point-a',
            },
            {
                'type': 'Feature',
                'geometry': {'type': 'LineString', 'coordinates': [[0, 0], [1, 1]]},
                'properties': {},
                'id': 42,
            },
            {
                'type': 'Feature',
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[[0, 0], [1, 0], [1, 1], [0, 0]]],
                },
                'properties': None,
            },
        ],
    }
    parsed = gm.from_features(json.dumps(payload))
    assert parsed.geometries.to_wkt() == [
        'POINT (1 2)',
        'LINESTRING (0 0, 1 1)',
        'POLYGON ((0 0, 1 0, 1 1, 0 0))',
    ]
    assert parsed.properties == (
        payload['features'][0]['properties'],
        {},
        None,
    )
    assert parsed.ids == ('point-a', 42, None)
    assert (
        gm.from_features(json.dumps(payload).encode()).properties == parsed.properties
    )
    assert gm.from_features(bytearray(json.dumps(payload), 'utf-8')).ids == parsed.ids

    single = gm.from_features(json.dumps(payload['features'][0]))
    assert single.geometries.to_wkt() == ['POINT (1 2)']
    assert single.properties == (payload['features'][0]['properties'],)
    assert single.ids == ('point-a',)

    empty = gm.from_features('{"type":"FeatureCollection","features":[]}')
    assert len(empty.geometries) == 0
    assert empty.properties == ()
    assert empty.ids == ()


def test_from_features_text_fast_path_errors_match_python_path() -> None:
    missing_geometry = '{"type":"Feature","properties":{}}'
    with pytest.raises(
        gm.ParseError, match='each feature must have a geometry'
    ) as missing:
        gm.from_features(missing_geometry)
    assert missing.value.format == 'geojson'

    with pytest.raises(gm.ParseError, match='Feature, FeatureCollection') as bad_kind:
        gm.from_features('{"type":"Nonsense"}')
    assert bad_kind.value.format == 'geojson'

    with pytest.raises(
        gm.ParseError, match='feature collection features must be iterable'
    ):
        gm.from_features('{"type":"FeatureCollection","features":123}')
    with pytest.raises(gm.ParseError, match='properties must be a mapping'):
        gm.from_features(
            '{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},'
            '"properties":3}'
        )
    with pytest.raises(gm.ParseError, match='feature id'):
        gm.from_features(
            '{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},'
            '"id":true}'
        )
    with pytest.raises(
        gm.CRSError,
        match=r'^a coordinate epoch requires a CRS; attach one with crs= \(or set_crs\(\.\.\.\)\) before tagging an epoch$',
    ):
        gm.from_features(missing_geometry, crs=None, epoch=2020.0)
    with pytest.raises(gm.CRSError, match='WGS84'):
        gm.from_features(missing_geometry, crs='EPSG:3857')
    with pytest.raises(gm.GeometryError, match='coordinate epoch must be a finite'):
        gm.from_features(missing_geometry, epoch=math.inf)


@pytest.mark.parametrize(
    'payload',
    [
        '{"type":"FeatureCollection","c\\u0072s":{"type":"name","properties":{"name":"EPSG:4326"}},"features":[]}',
        '[{"type":"Feature","c\\u0072s":{"type":"name","properties":{"name":"EPSG:4326"}},"geometry":{"type":"Point","coordinates":[1,2]}}]',
        '[{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2],"c\\u0072s":{"type":"name","properties":{"name":"EPSG:4326"}}}}]',
    ],
)
def test_from_features_single_pass_captures_escaped_legacy_crs(payload: str) -> None:
    with pytest.raises(gm.ParseError, match='conflicts with crs=None'):
        gm.from_features(payload, crs=None)

    # Backslashes in ordinary feature side data no longer trigger a second
    # whole-document parse; they remain ordinary JSON strings.
    clean = (
        '{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},'
        '"properties":{"path":"C:\\\\tiles\\\\a"}}'
    )
    assert gm.from_features(clean).properties == ({'path': 'C:\\tiles\\a'},)


def test_require_parses_wkt_wkb_and_geojson() -> None:
    wkt_point = gm.require('POINT (3 4)', crs=4326, axes='XY')
    assert wkt_point.to_wkt() == 'POINT (3 4)'
    wkb_point = gm.require(gm.Point(5, 6).to_wkb(), crs=4326, axes='XY')
    assert wkb_point.to_wkt() == 'POINT (5 6)'
    geojson_point = gm.require(
        {'type': 'Point', 'coordinates': [7, 8]}, crs=4326, axes='XY'
    )
    assert geojson_point.to_wkt() == 'POINT (7 8)'
    geojson_bytes = b'  {"type":"Point","coordinates":[9,10]}'
    assert gm.require(geojson_bytes).to_wkt() == 'POINT (9 10)'
    assert gm.require(memoryview(geojson_bytes)).to_wkt() == 'POINT (9 10)'
    # Well-formed EWKT routes to the WKT parser (not GeoJSON).
    assert gm.require('SRID=4326;POINT(1 2)').to_wkt() == 'POINT (1 2)'
    # Malformed SRID= (missing ';') is EWKT, not a confusing GeoJSON error.
    with pytest.raises(gm.ParseError, match='EWKT') as malformed_ewkt:
        gm.require('SRID=4326POINT(1 2)')
    assert 'GeoJSON' not in str(malformed_ewkt.value)


def test_require_validates_arrays_and_general_iterables_atomically() -> None:
    points = gm.points([0, 1], [2, 3])
    required = gm.require(points, crs=4326, axes='XY')
    assert isinstance(required, gm.GeometryArray)
    assert required.crs == 'EPSG:4326'
    assert required.to_wkt() == ['POINT (0 2)', 'POINT (1 3)']

    values = (
        value
        for value in (
            'POINT (4 5)',
            gm.Point(6, 7).to_wkb(),
            {'type': 'Point', 'coordinates': [8, 9]},
            None,
        )
    )
    parsed = gm.require(values, crs=4326, axes='XY')
    assert isinstance(parsed, gm.GeometryArray)
    assert parsed.to_wkt() == ['POINT (4 5)', 'POINT (6 7)', 'POINT (8 9)', None]
    assert parsed.is_missing.tolist() == [False, False, False, True]

    source = [gm.Point(0, 0), gm.Polygon([(0, 0), (1, 1), (1, 0), (0, 1)])]
    with pytest.raises(gm.InvalidGeometryError, match='geometry 1 is invalid'):
        gm.require(source)
    assert source[0].crs is None


def test_validate_boundary_helper_parses_and_enforces_storage_contracts() -> None:
    feature = {
        'type': 'Feature',
        'properties': {},
        'geometry': {'type': 'Point', 'coordinates': [1, 2]},
    }
    point = gm.require(feature, crs=4326, axes='XY')
    assert point.to_wkt() == 'POINT (1 2)'
    assert point.crs == 'EPSG:4326'
    with pytest.raises(ValueError, match='expected CRS "EPSG:3857"'):
        gm.require(point, crs=3857)
    with pytest.raises(ValueError, match='expected axes'):
        gm.require(gm.Point(1, 2, z=3, crs=4326), crs=4326, axes='XY')
    with pytest.raises(ValueError, match='self-intersection'):
        gm.require(gm.Polygon([(0, 0), (1, 1), (1, 0), (0, 1)]), axes='XY')


def test_v1_api_convenience_and_ewkb_helpers() -> None:
    point = gm.Point(21.0, 52.0, crs=4326)
    values = gm.GeometryArray([point, gm.Point(22.0, 52.0, crs=4326)])
    assigned = gm.GeometryArray([gm.Point(1, 2), gm.Point(3, 4)], crs=4326)
    assert point.to_wkt() == 'POINT (21 52)'
    assert point.coordinate_axes == 'XY'
    assert not point.has_z
    assert not point.has_m
    assert values.common_coordinate_axes == 'XY'
    assert not values.any_has_z
    assert not values.any_has_m
    np.testing.assert_allclose(
        gm.bounds(values), [[21.0, 52.0, 21.0, 52.0], [22.0, 52.0, 22.0, 52.0]]
    )
    assert values.total_bounds == (21.0, 52.0, 22.0, 52.0)
    with pytest.raises(TypeError, match='bulk-only'):
        gm.bounds(point)
    assert point.bounds == (21.0, 52.0, 21.0, 52.0)
    assert gm.box(0, 0, 2, 2).area == 4
    np.testing.assert_allclose(
        gm.GeometryArray([gm.Point(21.0, 52.0), gm.Point(22.0, 52.0)]).area,
        [0.0, 0.0],
    )
    assert gm.LineString([(0, 0), (3, 4)]).length == 5
    np.testing.assert_allclose(
        gm.GeometryArray([gm.LineString([(0, 0), (3, 4)]), gm.Point(0, 0)]).length,
        [5.0, 0.0],
    )
    assert gm.distance(gm.Point(0, 0), gm.Point(3, 4)) == 5
    np.testing.assert_allclose(
        gm.distance(gm.Point(0, 0), gm.points([0, 3], [0, 4])), [0.0, 5.0]
    )
    np.testing.assert_allclose(
        gm.distance(gm.points([0, 3], [0, 4]), gm.Point(0, 0)), [0.0, 5.0]
    )
    np.testing.assert_allclose(
        gm.distance(gm.points([0, 3], [0, 4]), gm.points([0, 0], [0, 0])), [0.0, 5.0]
    )
    np.testing.assert_allclose(
        gm.distance(gm.points([0, 3], [0, 4]), gm.Point(0, 0)), [0.0, 5.0]
    )
    np.testing.assert_allclose(
        gm.distance(gm.points([0, 3], [0, 4]), gm.points([0, 0], [0, 0])), [0.0, 5.0]
    )
    np.testing.assert_array_equal(
        gm.dwithin(gm.Point(0, 0), gm.points([0, 3], [0, 4]), 5), [True, True]
    )
    np.testing.assert_array_equal(
        gm.dwithin(gm.points([0, 3], [0, 4]), gm.Point(0, 0), 5), [True, True]
    )
    np.testing.assert_array_equal(
        gm.dwithin(gm.points([0, 3], [0, 4]), gm.points([0, 0], [0, 0]), 4.99),
        [True, False],
    )
    np.testing.assert_array_equal(
        gm.dwithin(gm.points([0, 3], [0, 4]), gm.Point(0, 0), 5), [True, True]
    )
    np.testing.assert_array_equal(
        gm.dwithin(gm.points([0, 3], [0, 4]), gm.points([0, 0], [0, 0]), 4.99),
        [True, False],
    )
    assert gm.dwithin(gm.Point(0, 0), gm.Point(3, 4), 4.99) is False
    collection = gm.GeometryCollection([
        gm.LineString([(0, 0), (0, 1)]),
        gm.Point(10, 0),
    ])
    assert gm.distance(collection, gm.Point(9, 0)) == 1.0
    assert gm.dwithin(collection, gm.Point(9, 0), 1.0) is True
    assert gm.dwithin(collection, gm.Point(9, 0), 0.99) is False
    far_point = gm.Point(1e200, 0)
    far_line = gm.LineString([(0, 0), (0, 1)])
    assert gm.dwithin(far_line, far_point, 5e199) is False
    assert gm.dwithin(far_line, far_point, 1.5e200) is True
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(0, 0), (0, 2)]),
    ])
    targets = gm.GeometryArray([
        gm.LineString([(0, 1), (1, 1)]),
        gm.LineString([(1, 0), (1, 2)]),
    ])
    np.testing.assert_allclose(gm.hausdorff_distance(lines, targets), [1.0, 1.0])
    assert gm.frechet_distance(lines, targets) == pytest.approx([1.0, 1.0])
    with pytest.raises(ValueError, match='same length'):
        gm.distance(gm.points([0, 3], [0, 4]), gm.points([0], [0]))
    with pytest.raises(ValueError, match='same length'):
        gm.distance(gm.points([0, 3], [0, 4]), gm.points([0], [0]))
    with pytest.raises(ValueError, match='non-negative finite'):
        gm.dwithin(gm.points([0, 3], [0, 4]), gm.Point(0, 0), -1)
    assert point.area == 0.0
    np.testing.assert_allclose(values.area, [0.0, 0.0])
    assert gm.LineString([(21.0, 52.0), (22.0, 52.0)], crs=4326).length > 0.0
    assert gm.distance(point, gm.Point(22.0, 52.0, crs=4326)) == pytest.approx(
        68677.47478989759
    )
    assert gm.distance(point, gm.Point(22.0, 52.0, crs=4326)) > 60000
    np.testing.assert_array_equal(gm.dwithin(values, point, 100000.0), [True, True])
    np.testing.assert_array_equal(gm.dwithin(values, point, 1.0), [True, False])
    with pytest.raises(
        gm.CRSMismatchError, match='distance requires matching CRS metadata'
    ):
        gm.distance(gm.Point(0, 0, crs=3857), gm.Point(0, 0, crs=32634))
    with pytest.raises(
        gm.CRSMismatchError, match='dwithin requires matching CRS metadata'
    ):
        gm.dwithin(gm.Point(0, 0, crs=3857), gm.Point(0, 0, crs=32634), 1.0)
    assert gm.distance(point.set_crs(None), gm.Point(22.0, 52.0)) == 1.0
    assert gm.require(point, crs=4326, axes='XY').to_wkt() == point.to_wkt()
    with pytest.raises(ValueError, match='unknown axes'):
        gm.require(point, axes=cast('Any', '2D'))
    assert gm.from_wkb(point.to_wkb(include_srid=True)).crs == 'EPSG:4326'
    assert gm.from_wkb(point.to_wkb(include_srid=True)).to_wkt() == point.to_wkt()
    assert assigned.crs == 'EPSG:4326'
    assert assigned[0].crs == 'EPSG:4326'
    assert (
        gm.require(assigned, crs=4326).to_arrow().type.extension_name
        == 'geoarrow.point'
    )
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.GeometryArray([gm.Point(1, 2, crs=4326), gm.Point(1, 2, crs=3857)])
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.GeometryArray([gm.Point(1, 2, crs=4326), gm.Point(1, 2)])
    with pytest.raises(gm.CRSMismatchError, match='expected CRS'):
        gm.require(point, crs=3857)
    with pytest.raises(ValueError, match='expected axes'):
        gm.require(point, axes='XYZ')


def test_constructors_emit_dimensional_wkt() -> None:
    assert (
        gm.LineString([(0, 0, 1, 2), (1, 1, 3, 4)]).to_wkt()
        == 'LINESTRING ZM (0 0 1 2, 1 1 3 4)'
    )
    assert (
        gm.Polygon([(0, 0, 1), (1, 0, 1), (0, 1, 1)]).to_wkt()
        == 'POLYGON Z ((0 0 1, 1 0 1, 0 1 1, 0 0 1))'
    )
    assert (
        gm.MultiPolygon([[[(0, 0, 1, 2), (1, 0, 1, 2), (0, 1, 1, 2)]]]).to_wkt()
        == 'MULTIPOLYGON ZM (((0 0 1 2, 1 0 1 2, 0 1 1 2, 0 0 1 2)))'
    )


def test_empty_geometry_collection_wkt_roundtrips() -> None:
    empty = gm.from_wkt('GEOMETRYCOLLECTION EMPTY')
    assert empty.geometry_type == 'GeometryCollection'
    assert empty.to_wkt() == 'GEOMETRYCOLLECTION EMPTY'
    assert empty.bounds is None
    assert empty.area == 0
    assert empty.length == 0
    assert empty.validate().valid
    assert len(gm.parts(empty)) == 0
    with pytest.raises(TypeError, match='bulk-only'):
        gm.bounds(empty)


def test_multi_geometries_and_collections_roundtrip_through_public_io() -> None:
    multipoint = gm.MultiPoint([(0, 0), (2, 2)], crs=4326)
    multiline = gm.MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 2)]], crs=4326)
    multipolygon = gm.MultiPolygon(
        [gm.box(0, 0, 1, 1), [[(2, 2), (4, 2), (4, 4), (2, 4)]]], crs=4326
    )
    collection = gm.GeometryCollection([multipoint, multiline, multipolygon], crs=4326)
    assert multipoint.geometry_type == 'MultiPoint'
    assert multiline.geometry_type == 'MultiLineString'
    assert multipolygon.geometry_type == 'MultiPolygon'
    assert collection.geometry_type == 'GeometryCollection'
    assert multipolygon.bounds == (0.0, 0.0, 4.0, 4.0)
    assert multipolygon.set_crs(None).area == 5
    assert multiline.set_crs(None).length == pytest.approx(math.sqrt(2) + 1)
    assert multipolygon.area > 0
    assert multiline.length > 0
    assert gm.contains(multipoint, gm.Point(2, 2, crs=4326))
    assert gm.contains(multipolygon, gm.Point(3, 3, crs=4326))
    assert gm.intersects(collection, gm.Point(3, 3, crs=4326))
    assert [part.geometry_type for part in gm.parts(collection)] == [
        'MultiPoint',
        'MultiLineString',
        'MultiPolygon',
    ]
    assert len(collection.coords) == 16
    assert [ring.geometry_type for ring in list(gm.rings(multipolygon))] == [
        'LineString',
        'LineString',
    ]
    for geometry in (multipoint, multiline, multipolygon, collection):
        assert gm.from_wkt(geometry.to_wkt()).to_wkt() == geometry.to_wkt()
        assert gm.from_wkb(geometry.to_wkb()).to_wkt() == geometry.to_wkt()
        assert gm.from_wkb(geometry.to_wkb(include_srid=True)).crs == 'EPSG:4326'
        assert (
            gm.from_geojson(geometry.to_geojson(), crs=4326).to_wkt()
            == geometry.to_wkt()
        )


def test_wkb_parser_is_strict_and_accepts_big_endian_input() -> None:
    big_endian_point = struct.pack('>BIdd', 0, 1, 1.0, 2.0)
    big_endian_ewkb_point = struct.pack('>BII', 0, 536870913, 4326) + struct.pack(
        '>dd', 1.0, 2.0
    )
    assert gm.from_wkb(big_endian_point).to_wkt() == 'POINT (1 2)'
    recovered = gm.from_wkb(big_endian_ewkb_point)
    assert recovered.crs == 'EPSG:4326'
    assert recovered.to_wkt() == 'POINT (1 2)'
    with pytest.raises(ValueError, match='trailing bytes after WKB geometry'):
        gm.from_wkb(gm.Point(1, 2).to_wkb() + b'extra')
    nested_srid = struct.pack('<BIIBII', 1, 7, 1, 1, 536870913, 4326) + struct.pack(
        '<dd', 1.0, 2.0
    )
    # A member-only SRID under an SRID-less container ESTABLISHES the frame
    # (PostGIS payloads may stamp members only); sibling disagreement rejects.
    adopted = gm.from_wkb(nested_srid)
    assert adopted.crs == 'EPSG:4326'
    conflicting_members = (
        struct.pack('<BII', 1, 7, 2)
        + struct.pack('<BII', 1, 536870913, 4326)
        + struct.pack('<dd', 1.0, 2.0)
        + struct.pack('<BII', 1, 536870913, 3857)
        + struct.pack('<dd', 3.0, 4.0)
    )
    with pytest.raises(ValueError, match='conflicts with enclosing SRID'):
        gm.from_wkb(conflicting_members)
    with pytest.raises(
        ValueError, match='unsupported WKB geometry type CircularString'
    ):
        gm.from_wkb(struct.pack('<BI', 1, 8))
    with pytest.raises(
        ValueError, match='unsupported WKB geometry type TIN \\(16; raw 1016\\)'
    ):
        gm.from_wkb(struct.pack('<BI', 1, 1016))
    with pytest.raises(
        ValueError,
        match='unsupported WKB geometry type Triangle \\(17; raw 2147483665\\)',
    ):
        gm.from_wkb(struct.pack('<BI', 1, 2147483665))


def test_wkt_float_parse_bit_identical_to_std() -> None:
    import struct

    tokens = [
        '0',
        '-0.0',
        '1.2345678901234567',
        '-987654321.123456789',
        '1e308',
        '1e-308',
        '5.5e-17',
        '-2.5E+12',
        '3.141592653589793',
        '2.2250738585072014e-308',
    ]
    for token in tokens:
        std_bits = struct.unpack('>Q', struct.pack('>d', float(token)))[0]
        point = cast('gm.Point', gm.from_wkt(f'POINT Z ({token} {token} {token})'))
        assert struct.unpack('>Q', struct.pack('>d', point.x))[0] == std_bits, token
        assert struct.unpack('>Q', struct.pack('>d', point.y))[0] == std_bits, token
        assert struct.unpack('>Q', struct.pack('>d', point.z))[0] == std_bits, token


def test_wkt_parser_rejects_prefix_collisions_and_trailing_text() -> None:
    for value in ('POINTLESS (1 2)', 'POINT (1 2) trailing', 'POINT ZBAD (1 2 3)'):
        with pytest.raises(gm.ParseError, match='invalid WKT'):
            gm.from_wkt(value)
    for value, name in (
        ('CIRCULARSTRING (0 0, 1 1, 2 0)', 'CircularString'),
        ('COMPOUNDCURVE ((0 0, 1 1))', 'CompoundCurve'),
        ('CURVEPOLYGON ((0 0, 1 0, 0 1, 0 0))', 'CurvePolygon'),
        ('CURVEDPOLYGON ((0 0, 1 0, 0 1, 0 0))', 'CurvePolygon'),
        ('MULTICURVE ((0 0, 1 1))', 'MultiCurve'),
        ('MULTISURFACE (((0 0, 1 0, 0 1, 0 0)))', 'MultiSurface'),
        ('TRIANGLE ((0 0, 1 0, 0 1, 0 0))', 'Triangle'),
        ('TIN (((0 0, 1 0, 0 1, 0 0)))', 'TIN'),
        ('POLYHEDRALSURFACE (((0 0, 1 0, 0 1, 0 0)))', 'PolyhedralSurface'),
    ):
        with pytest.raises(ValueError, match=f'unsupported WKT geometry type {name}'):
            gm.from_wkt(value)


def test_serializer_precision_kwarg_rounds_output() -> None:
    assert (
        gm.Point(1.234, 2.345, crs=4326).to_wkt(include_srid=True, precision=1)
        == 'SRID=4326;POINT (1.2 2.3)'
    )


def test_serializer_precision_kwarg_matches_quantize() -> None:
    geom = gm.LineString([(1.23456, 2.34567), (7.65432, 8.76543)], crs=4326)
    array = gm.GeometryArray([geom, gm.Point(3.14159, 2.71828, crs=4326)])
    for precision in (1, 2, 5):
        assert geom.to_wkt(precision=precision) == geom.quantize(precision).to_wkt()
        assert geom.to_wkb(precision=precision) == geom.quantize(precision).to_wkb()
        assert array.to_wkt(precision=precision) == array.quantize(precision).to_wkt()
        assert array.to_wkb(precision=precision) == array.quantize(precision).to_wkb()


def test_serializer_precision_kwarg_rejects_overflow() -> None:
    with pytest.raises(gm.GeometryError, match='precision'):
        gm.Point(0, 0).to_wkt(precision=16)
    with pytest.raises(gm.GeometryError, match='precision'):
        gm.Point(0, 0).to_wkb(precision=16)


def test_serializer_methods_agree_across_scalar_and_array() -> None:
    geom = gm.LineString([(0, 0, 1), (3, 4, 5)], crs=4326)
    array = gm.GeometryArray([geom])
    assert geom.to_wkt() == 'LINESTRING Z (0 0 1, 3 4 5)'
    assert array.to_wkt() == [geom.to_wkt()]
    assert array.to_wkb() == [geom.to_wkb()]
    restored = gm.from_wkb(geom.to_wkb())
    assert isinstance(restored, gm.Geometry)
    assert restored.to_wkt() == geom.to_wkt()
    assert array.to_geojson() == [geom.to_geojson()]


def test_geojson_rejects_integer_coordinates_beyond_f64_exact_range() -> None:
    with pytest.raises(gm.ParseError, match='exceeds f64 exact integer range'):
        gm.from_geojson({'type': 'Point', 'coordinates': [9007199254740993, 0]})


def test_polyline_codec_round_trips_google_vectors() -> None:
    canonical = '_p~iF~ps|U_ulLnnqC_mqNvxq`@'
    line = gm.from_polyline(canonical)
    assert line.to_wkt() == 'LINESTRING (-120.2 38.5, -120.95 40.7, -126.453 43.252)'
    assert line.crs == 'EPSG:4326'
    assert line.geometry_type == 'LineString'
    assert line.to_polyline() == canonical
    assert line.to_polyline() == canonical
    route = gm.LineString([(-120.2, 38.5), (-120.95, 40.7)], crs=4326)
    assert gm.equals(
        gm.from_polyline(route.to_polyline(precision=6), precision=6), route
    )
    singleton = gm.from_polyline('_p~iF~ps|U')
    assert singleton.geometry_type == 'Point'
    assert singleton.to_wkt() == 'POINT (-120.2 38.5)'
    assert singleton.crs == 'EPSG:4326'
    assert singleton.to_polyline() == '_p~iF~ps|U'
    half = gm.LineString([(-5e-06, 5e-06), (0, 0)])
    assert list(gm.from_polyline(half.to_polyline()).coords) == [
        (0.0, 1e-05),
        (0.0, 0.0),
    ]
    arrays = gm.from_polyline(['_p~iF~ps|U', canonical])
    assert arrays.geometry_type == ['Point', 'LineString']
    assert arrays.to_polyline() == ['_p~iF~ps|U', canonical]
    masked = gm.from_polyline(['_p~iF~ps|U', None], epoch=2020.0)
    assert masked.is_missing.tolist() == [False, True]
    assert masked.epoch == 2020.0
    assert masked[0].epoch == 2020.0
    framed_route = gm.LineString(
        [(-120.2, 38.5), (-120.95, 40.7)], crs=4326, epoch=2020.0
    )
    with pytest.raises(gm.GeometryError, match='cannot encode coordinate epoch'):
        framed_route.to_polyline()
    restored_route = gm.from_polyline(
        framed_route.to_polyline(drop_epoch=True), epoch=2020.0
    )
    assert restored_route.crs == 'EPSG:4326'
    assert restored_route.epoch == 2020.0
    assert gm.equals(restored_route, framed_route)
    bare_route = gm.from_polyline(framed_route.to_polyline(drop_epoch=True), crs=None)
    assert bare_route.crs is None
    with pytest.raises(gm.CRSError, match='WGS84'):
        gm.from_polyline(framed_route.to_polyline(drop_epoch=True), crs=3857)
    with pytest.raises(gm.GeometryError, match='cannot encode coordinate epoch'):
        gm.GeometryArray([framed_route]).to_polyline()
    assert gm.GeometryArray([framed_route]).to_polyline(drop_epoch=True) == [
        framed_route.to_polyline(drop_epoch=True)
    ]
    assert gm.from_wkt('LINESTRING EMPTY').to_polyline() == ''
    assert gm.from_polyline('').is_empty
    measured = gm.from_wkt('LINESTRING Z (0 0 1, 1 1 2)')
    with pytest.raises(gm.InvalidGeometryError, match='Z/M'):
        measured.to_polyline()
    with pytest.raises(gm.InvalidGeometryError, match='Z/M'):
        gm.from_wkt('LINESTRING M (0 0 1, 1 1 2)').to_polyline()
    assert gm.from_polyline(measured.force_2d().to_polyline()).to_wkt() == (
        'LINESTRING (0 0, 1 1)'
    )
    with pytest.raises(gm.CRSError, match='requires EPSG:4326'):
        gm.LineString([(0, 0), (1, 1)], crs=3857).to_polyline()
    assert gm.Point(0, 0).to_polyline() == '??'
    with pytest.raises(gm.GeometryTypeError, match='requires a LineString or Point'):
        gm.box(0, 0, 1, 1).to_polyline()
    with pytest.raises(gm.GeometryError, match='between 0 and 11'):
        gm.from_wkt('LINESTRING EMPTY').to_polyline(precision=12)
    with pytest.raises(gm.ParseError, match='invalid polyline character'):
        gm.from_polyline('!!!')
    with pytest.raises(gm.ParseError) as poly_ctrl:
        gm.from_polyline('\x01')
    # Python-style escapes (not Rust Debug '\u{1}'); echo the offending input.
    assert str(poly_ctrl.value) == "invalid polyline character '\\x01' in \"\\x01\""
    assert r'\u{' not in str(poly_ctrl.value)
    with pytest.raises(gm.ParseError, match='mid-value'):
        gm.from_polyline('_p~iF~ps|')
    with pytest.raises(gm.ParseError, match='between a latitude'):
        gm.from_polyline('_p~iF')
    with pytest.raises(gm.ParseError, match='invalid polyline character') as info:
        gm.from_polyline(['_p~iF~ps|U', '!!!'])
    assert 'array element 1' in ''.join(info.value.__notes__)
    for hostile in ['}~~~~~~~~~~~N?', '____________O?', '}~~~~~~~~~~~F?' * 3]:
        with pytest.raises(gm.ParseError, match=r'overflows|domain'):
            gm.from_polyline(hostile, precision=0)
    with pytest.raises(gm.InvalidGeometryError, match='outside the longitude/latitude'):
        gm.LineString([(181, 0), (0, 0)], crs=4326).to_polyline()
    with pytest.raises(gm.InvalidGeometryError, match='outside the longitude/latitude'):
        gm.LineString([(0, 91), (1, 91)], crs=4326).to_polyline()
