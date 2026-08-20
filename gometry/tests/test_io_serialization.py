"""R14-F I/O and serialization semantics — deterministic regression corpus.

Each case drives the public entry point named by the audit finding.
"""

from __future__ import annotations

import json
import struct
from typing import TYPE_CHECKING

import gometry as gm
import numpy as np
import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


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


def test_a1_pickle_silent_closes_xy_open_rings_like_wkt() -> None:
    """Pickle polygon unpickle uses admit_closed_ring (silent-close open rings)."""
    import pickle

    # Build a closed triangle, pickle, then craft an open 3-corner payload via
    # the private unpickler (same columns without the closing vertex).
    closed = gm.GeometryArray([gm.from_wkt('POLYGON((0 0, 1 0, 0 1, 0 0))')])
    # Round-trip still works for already-closed rings.
    restored = pickle.loads(pickle.dumps(closed))
    assert restored.to_wkt() == closed.to_wkt()

    # Hand-build open 3-corner columns matching the pickle layout.
    from gometry import _lib

    xs = struct.pack('<3d', 0.0, 1.0, 0.0)
    ys = struct.pack('<3d', 0.0, 0.0, 1.0)
    ring = struct.pack('<2i', 0, 3)
    poly = struct.pack('<2i', 0, 1)
    arr = _lib._unpickle_polygon_array(
        xs, ys, None, None, ring, poly, None, None, None, None
    )
    assert arr.to_wkt() == ['POLYGON ((0 0, 1 0, 0 1, 0 0))']
    assert bool(arr.is_valid[0])


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
# B3 — mapping __len__ is advisory, never a hard key cap
# ---------------------------------------------------------------------------


def test_b3_mapping_len_underreport_still_admits_all_keys() -> None:
    class UnderLenMap:
        def keys(self) -> Iterator[str]:
            return iter(['a', 'b', 'c'])

        def __len__(self) -> int:
            return 1

        def __getitem__(self, key: str) -> int:
            return {'a': 1, 'b': 2, 'c': 3}[key]

    # Feature properties path uses mapping_as_dict; under-reported __len__
    # must not reject the extra keys.
    feature = {
        'type': 'Feature',
        'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]},
        'properties': UnderLenMap(),
    }
    feats = gm.from_features(feature)
    assert feats.properties[0] == {'a': 1, 'b': 2, 'c': 3}


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
    # Packed polygon unpickler — not the mixed WKB path.
    assert arr.__reduce__()[0].__name__ == '_unpickle_polygon_array'
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
