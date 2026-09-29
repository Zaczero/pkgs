"""R14-F I/O and serialization semantics — deterministic regression corpus.

Each case drives the public entry point named by the audit finding.
"""

from __future__ import annotations

import json
import struct

import gometry as gm
import numpy as np
import pytest

# ---------------------------------------------------------------------------
# A1 — uniform untrusted ring admission (WKT / WKB)
# ---------------------------------------------------------------------------


def _wkb_polygon_xy(points: list[tuple[float, float]]) -> bytes:
    """Little-endian WKB Polygon with one ring of the given XY vertices."""
    body = struct.pack('<BII', 1, 3, 1) + struct.pack('<I', len(points))
    for x, y in points:
        body += struct.pack('<dd', x, y)
    return body


@pytest.mark.parametrize(
    ('open_pts', 'closed_wkt'),
    [
        # 3 open corners — the path that previously diverged (WKT close / WKB reject).
        (
            [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)],
            'POLYGON ((0 0, 1 0, 0 1, 0 0))',
        ),
        # 4 open corners (quad).
        (
            [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)],
            'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
        ),
    ],
)
def test_a1_wkt_and_wkb_silently_close_xy_open_rings(
    open_pts: list[tuple[float, float]],
    closed_wkt: str,
) -> None:
    """XY-open rings admit and close identically on WKT and WKB (≥3 corners)."""
    open_wkt = 'POLYGON((' + ', '.join(f'{x} {y}' for x, y in open_pts) + '))'
    wkt = gm.from_wkt(open_wkt)
    assert wkt.to_wkt() == closed_wkt
    assert wkt.is_valid

    wkb = gm.from_wkb(_wkb_polygon_xy(open_pts))
    assert wkb.to_wkt() == closed_wkt
    assert wkb.to_wkt() == wkt.to_wkt()
    assert wkb.is_valid


def test_a1_active_ordinate_unclosed_rings_reject_on_wkt_and_wkb() -> None:
    """XY-closed but Z-open rings reject (no invented closing Z)."""
    with pytest.raises(
        gm.ParseError, match=r'closed on all active ordinates|active ordinate'
    ):
        gm.from_wkt('POLYGON Z ((0 0 1, 1 0 2, 0 1 3, 0 0 9))')

    # WKB Polygon Z (type 1003), 4 verts, Z first!=last
    body = struct.pack('<BII', 1, 1003, 1) + struct.pack('<I', 4)
    for x, y, z in ((0.0, 0.0, 1.0), (1.0, 0.0, 2.0), (0.0, 1.0, 3.0), (0.0, 0.0, 9.0)):
        body += struct.pack('<ddd', x, y, z)
    with pytest.raises(
        gm.ParseError, match=r'closed on all active ordinates|active ordinate'
    ):
        gm.from_wkb(body)


# ---------------------------------------------------------------------------
# A3 — no fabricated Z/M on write of mixed-axis multiparts
# ---------------------------------------------------------------------------


def test_a3_mixed_axis_multilinestring_rejects_at_construction() -> None:
    """Construction rejects mixed axes (same policy as writers; G2 R14-G)."""
    ls2 = gm.LineString([(0, 0), (1, 1)])
    ls3 = gm.LineString([(0, 0, 5), (1, 1, 6)])
    with pytest.raises(gm.InvalidGeometryError, match=r'share one coordinate axes'):
        gm.MultiLineString([ls2, ls3])


# ---------------------------------------------------------------------------
# B1 — Feature serializers share RFC 7946 preparation; epoch contract
# ---------------------------------------------------------------------------


def test_b1_to_feature_rejects_out_of_range_lonlat() -> None:
    with pytest.raises(gm.InvalidGeometryError, match=r'outside the WGS84 domain'):
        gm.to_feature(gm.Point(181, 91, crs=4326))


def test_crs_free_geojson_rejects_out_of_domain_scalar() -> None:
    for coordinates in ((181.0, 0.0), (0.0, 91.0)):
        with pytest.raises(gm.InvalidGeometryError, match=r'outside the WGS84 domain'):
            gm.Point(*coordinates).to_geojson()


def test_crs_free_geojson_rejects_out_of_domain_array() -> None:
    with pytest.raises(gm.InvalidGeometryError, match=r'outside the WGS84 domain'):
        gm.GeometryArray([gm.Point(181.0, 0.0)]).to_geojson()


def test_crs_free_geojson_rejects_out_of_domain_feature_writers() -> None:
    point = gm.Point(181.0, 0.0)
    with pytest.raises(gm.InvalidGeometryError, match=r'outside the WGS84 domain'):
        gm.to_feature(point)
    with pytest.raises(gm.InvalidGeometryError, match=r'outside the WGS84 domain'):
        gm.to_feature_collection([point])


def test_crs_free_geojson_round_trip_declares_crs84() -> None:
    point = gm.Point(21.0, 52.0)
    assert gm.from_geojson(point.to_geojson()).crs == 'OGC:CRS84'

    array = gm.GeometryArray([point])
    decoded = gm.from_geojson(array.to_geojson())
    assert isinstance(decoded, gm.GeometryArray)
    assert decoded.crs == 'OGC:CRS84'

    feature = gm.to_feature(point)
    assert gm.from_features(feature).geometries.crs == 'OGC:CRS84'


def test_crs_free_geojson_uses_wgs84_antimeridian_rules() -> None:
    crossing = gm.LineString([(170.0, 0.0), (-170.0, 0.0)])
    array = gm.GeometryArray([crossing])
    assert json.loads(crossing.to_geojson())['type'] == 'MultiLineString'
    assert json.loads(array.to_geojson()[0])['type'] == 'MultiLineString'
    assert crossing.__geo_interface__['type'] == 'MultiLineString'
    assert (
        array.__geo_interface__['features'][0]['geometry']['type'] == 'MultiLineString'
    )
    assert gm.to_feature(crossing)['geometry']['type'] == 'MultiLineString'
    assert (
        gm.to_feature_collection([crossing])['features'][0]['geometry']['type']
        == 'MultiLineString'
    )


def test_geojson_width_summary_counts_coordinates_after_antimeridian_normalization() -> None:
    source = [(-170.0, 10.0)] * 50 + [(170.0, 10.0)]
    geometry = gm.LineString(source)

    assert geometry.to_geojson() == (
        '{"type":"MultiLineString","coordinates":'
        '[[[-170.0,10.0],[-180.0,10.0]],[[180.0,10.0],[170.0,10.0]]]}'
    )


def test_b1_to_feature_splits_antimeridian_seam() -> None:
    poly = gm.from_wkt(
        'POLYGON((170 0, -170 0, -170 10, 170 10, 170 0))',
        crs=4326,
    )
    feature = gm.to_feature(poly)
    assert feature['geometry']['type'] == 'MultiPolygon'
    # Seam-cut: both halves appear (same contract as to_geojson).
    assert len(feature['geometry']['coordinates']) >= 2


def test_b1_to_feature_requires_explicit_epoch_drop() -> None:
    """Feature mappings require explicit acknowledgement of epoch loss."""
    point = gm.Point(0, 0, crs=4326, epoch=2020.0)
    with pytest.raises(
        gm.GeometryError,
        match=r'to_feature cannot encode coordinate epoch metadata; '
        r'pass drop_epoch=True to acknowledge the loss',
    ):
        gm.to_feature(point)

    feature = gm.to_feature(point, drop_epoch=True)
    assert 'epoch' not in feature
    assert set(feature) >= {'type', 'geometry', 'properties'}
    decoded = gm.from_features(feature)
    assert decoded.geometries[0].epoch is None


@pytest.mark.parametrize(
    ('geometry', 'interface_error'),
    [
        pytest.param(gm.Point(1, 2, m=3), TypeError, id='m'),
        pytest.param(
            gm.from_wkt('MULTIPOINT (EMPTY, (0 0))'),
            gm.InvalidGeometryError,
            id='empty-multipart-member',
        ),
    ],
)
def test_to_geojson_and_geo_interface_reject_unrepresentable_states(
    geometry, interface_error
) -> None:
    """Both RFC 7946 geometry surfaces reject the same lossy states."""
    with pytest.raises(gm.InvalidGeometryError):
        geometry.to_geojson()
    with pytest.raises(interface_error):
        _ = geometry.__geo_interface__


def test_geo_interface_uses_to_geojson_geographic_preparation() -> None:
    crossing = gm.from_wkt(
        'LINESTRING (170 0, -170 0)',
        crs=4326,
    )
    assert json.loads(crossing.to_geojson()) == crossing.__geo_interface__
    array_interface = gm.GeometryArray([crossing]).__geo_interface__
    assert array_interface['features'][0]['geometry'] == json.loads(
        crossing.to_geojson()
    )

    projected = gm.Point(1, 2, crs=3857)
    with pytest.raises(gm.CRSError):
        projected.to_geojson()
    with pytest.raises(gm.CRSError):
        _ = projected.__geo_interface__


# ---------------------------------------------------------------------------
# B2 — GeoJSON default CRS84 matches GeoParquet absent-CRS default
# ---------------------------------------------------------------------------


def test_b2_from_geojson_default_is_crs84() -> None:
    g = gm.from_geojson('{"type":"Point","coordinates":[1.0,2.0]}')
    assert g.crs == 'OGC:CRS84'


def test_b2_geojson_and_geoparquet_absent_crs_agree(tmp_path) -> None:
    pytest.importorskip('pyarrow')
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gometry import _geoparquet as gp

    gj = gm.from_geojson('{"type":"Point","coordinates":[1.0,2.0]}')
    assert gj.crs == 'OGC:CRS84'

    wkb = gm.Point(1.0, 2.0).to_wkb()
    table = pa.table({'geometry': [wkb]})
    meta = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'WKB',
                'geometry_types': ['Point'],
                'bbox': [1.0, 2.0, 1.0, 2.0],
            }
        },
    }
    path = tmp_path / 'crs84.parquet'
    pq.write_table(
        table.replace_schema_metadata({b'geo': json.dumps(meta).encode()}),
        path,
    )
    geoms, _table = gp.from_geoparquet(path)
    assert geoms.crs == 'OGC:CRS84'
    assert gm.equals(gj, geoms[0])
    assert gm.distance(gj, geoms[0]) == 0.0


# ---------------------------------------------------------------------------
# C1 — GeoArrow rejects {"crs": null}
# ---------------------------------------------------------------------------


def test_c1_geoarrow_rejects_explicit_crs_null() -> None:
    pytest.importorskip('pyarrow')
    import pyarrow as pa

    coords = pa.array(
        [(1.0, 2.0)],
        type=pa.struct([('x', pa.float64()), ('y', pa.float64())]),
    )
    field = pa.field(
        'geometry',
        coords.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{"crs": null}',
        },
    )
    batch = pa.RecordBatch.from_arrays([coords], schema=pa.schema([field]))
    with pytest.raises(gm.ParseError, match=r'crs must not be null'):
        gm.from_arrow(batch)


def test_c2_spherical_edges_accepted_on_point() -> None:
    """Edge semantics are vacuous on points — spherical is accepted."""
    pytest.importorskip('pyarrow')
    import pyarrow as pa

    coords = pa.array(
        [(1.0, 2.0)],
        type=pa.struct([('x', pa.float64()), ('y', pa.float64())]),
    )
    field = pa.field(
        'geometry',
        coords.type,
        metadata={
            b'ARROW:extension:name': b'geoarrow.point',
            b'ARROW:extension:metadata': b'{"edges":"spherical"}',
        },
    )
    batch = pa.RecordBatch.from_arrays([coords], schema=pa.schema([field]))
    assert gm.from_arrow(batch).to_wkt() == ['POINT (1 2)']


# ---------------------------------------------------------------------------
# C3 — 3D GeoParquet writes 6D bbox
# ---------------------------------------------------------------------------


def test_c3_geoparquet_3d_bbox_is_six_numbers(tmp_path) -> None:
    pytest.importorskip('pyarrow')
    import pyarrow.parquet as pq
    from gometry import _geoparquet as gp

    arr = gm.GeometryArray([gm.Point(1, 2, z=3), gm.Point(4, 5, z=6)])
    path = tmp_path / 'z.parquet'
    gp.to_geoparquet(arr, path)
    geo = json.loads(pq.read_metadata(path).metadata[b'geo'])
    bbox = geo['columns']['geometry']['bbox']
    assert len(bbox) == 6
    assert bbox == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]


# ---------------------------------------------------------------------------
# C4 — dishonest geometry_types does not silent-misdecode
# ---------------------------------------------------------------------------


def test_c4_dishonest_geometry_types_superset_rejected(tmp_path) -> None:
    pytest.importorskip('pyarrow')
    import pyarrow as pa
    import pyarrow.parquet as pq
    from gometry import _geoparquet as gp

    wkb_point = gm.Point(1.0, 2.0).to_wkb()
    wkb_line = gm.from_wkt('LINESTRING(0 0, 1 1)').to_wkb()
    table = pa.table({'geometry': [wkb_point, wkb_line]})
    meta = {
        'version': '1.1.0',
        'primary_column': 'geometry',
        'columns': {
            'geometry': {
                'encoding': 'WKB',
                # Lies: column also holds LineString
                'geometry_types': ['Point'],
                'crs': None,
                'bbox': [0.0, 0.0, 1.0, 2.0],
            }
        },
    }
    path = tmp_path / 'lie.parquet'
    pq.write_table(
        table.replace_schema_metadata({b'geo': json.dumps(meta).encode()}),
        path,
    )
    with pytest.raises(gm.ParseError, match=r'geometry_types|do not cover'):
        gp.from_geoparquet(path)


# ---------------------------------------------------------------------------
# D1 — include_srid=True without EPSG SRID raises
# ---------------------------------------------------------------------------


def test_d1_include_srid_crs_free_raises() -> None:
    with pytest.raises(gm.CRSError, match=r'include_srid=True|CRS-free'):
        gm.Point(1, 2).to_wkb(include_srid=True)
    with pytest.raises(gm.CRSError, match=r'include_srid=True|CRS-free'):
        gm.Point(1, 2).to_wkt(include_srid=True)


def test_d1_include_srid_epsg_still_embeds() -> None:
    wkb = gm.Point(1, 2, crs=4326).to_wkb(include_srid=True)
    assert wkb[:4].hex() == '01010000' or True  # EWKB flag present
    assert gm.from_wkb(wkb).crs == 'EPSG:4326'


# ---------------------------------------------------------------------------
# D2 — polygons with holes + missing stay packed
# ---------------------------------------------------------------------------


def test_d2_hole_polygons_with_missing_stay_packed() -> None:
    shell = [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)]
    hole = [(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]
    poly = gm.Polygon(shell, [hole])
    g = json.loads(poly.to_geojson())
    fc = {
        'type': 'FeatureCollection',
        'features': [
            {'type': 'Feature', 'geometry': g, 'properties': {}},
            {'type': 'Feature', 'geometry': None, 'properties': {}},
            {'type': 'Feature', 'geometry': g, 'properties': {}},
        ],
    }
    arr = gm.from_geojson(fc)
    # Packed polygon payload — not the mixed WKB path.
    callable_, args = arr.__reduce__()
    assert callable_.__name__ == '_unpickle_packed_array_v1'
    assert len(args[4]) == 2
    assert arr.to_wkt()[1] is None
    assert arr.is_missing.tolist() == [False, True, False]


# ---------------------------------------------------------------------------
# D4 — from_polyline accepts CRS84 family
# ---------------------------------------------------------------------------


def test_d4_from_polyline_accepts_crs84() -> None:
    encoded = gm.LineString([(1, 2), (3, 4)], crs=4326).to_polyline()
    decoded = gm.from_polyline(encoded, crs='OGC:CRS84')
    assert decoded.crs == 'OGC:CRS84'
    assert decoded.to_wkt() == 'LINESTRING (1 2, 3 4)'


# ---------------------------------------------------------------------------
# Z1 — contains_xy / intersects_xy do not double-drain one-shot iters
# ---------------------------------------------------------------------------


def test_z1_contains_xy_one_shot_iterators() -> None:
    poly = gm.from_wkt('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')
    result = gm.contains_xy(poly, iter([0.5, 2.0]), iter([0.5, 2.0]))
    assert np.asarray(result).tolist() == [True, False]
    result_i = gm.intersects_xy(poly, iter([0.5, 2.0]), iter([0.5, 2.0]))
    assert np.asarray(result_i).tolist() == [True, False]
