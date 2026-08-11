"""Valid-input corpus: accept legal ingress and round-trip self-output.

This module is the structural gate against over-rejection regressions (R03+).
It must fail when gometry rejects its own encoded output or well-formed legal
payloads, and stay seconds-fast (a handful of large shapes, not thousands of
tiny ones).
"""

from __future__ import annotations

import pickle
import struct
from typing import TYPE_CHECKING

import gometry as gm
import pytest

if TYPE_CHECKING:
    from pathlib import Path

pa = pytest.importorskip('pyarrow')
pytest.importorskip('pyarrow.parquet')


# ---------------------------------------------------------------------------
# Corpus builders
# ---------------------------------------------------------------------------


def _shell() -> list[tuple[float, float]]:
    return [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]


def _hole() -> list[tuple[float, float]]:
    return [(1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0), (1.0, 1.0)]


def _empty_heavy_multilinestring(n_empty: int = 2000) -> gm.Geometry:
    """R03 shape: many EMPTY line members plus one real segment."""
    members = ', '.join(['EMPTY'] * n_empty + ['(0 0, 1 1)'])
    return gm.from_wkt(f'MULTILINESTRING ({members})')


def _large_multipoint(n: int = 70_000) -> gm.Geometry:
    body = bytearray(b'\x01\x04\x00\x00\x00')
    body += struct.pack('<I', n)
    point = b'\x01\x01\x00\x00\x00' + struct.pack('<2d', 1.0, 2.0)
    body += point * n
    return gm.from_wkb(bytes(body))


def _large_holed_polygon(n_holes: int = 65_537) -> gm.Geometry:
    """One shell + (n_holes - 1) hole rings = n_holes total rings."""
    parts = [struct.pack('<BI', 1, 3), struct.pack('<I', n_holes)]
    shell = [(-1.0, -1.0), (256.0, -1.0), (256.0, 256.0), (-1.0, 256.0), (-1.0, -1.0)]
    parts.append(struct.pack('<I', 5))
    for x, y in shell:
        parts.append(struct.pack('<2d', x, y))
    for i in range(256):
        for j in range(256):
            sq = [
                (i + 0.2, j + 0.2),
                (i + 0.8, j + 0.2),
                (i + 0.8, j + 0.8),
                (i + 0.2, j + 0.8),
                (i + 0.2, j + 0.2),
            ]
            parts.append(struct.pack('<I', 5))
            for x, y in sq:
                parts.append(struct.pack('<2d', x, y))
    return gm.from_wkb(b''.join(parts))


def _corpus_geometries() -> list[tuple[str, gm.Geometry]]:
    """Handful of legal shapes covering kinds, axes, empties, extremes, CRS."""
    items: list[tuple[str, gm.Geometry]] = [
        ('point_xy', gm.Point(1.0, 2.0)),
        ('point_z', gm.Point(1.0, 2.0, z=3.0)),
        ('point_m', gm.Point(1.0, 2.0, m=4.0)),
        ('point_zm', gm.Point(1.0, 2.0, z=3.0, m=4.0)),
        ('point_negzero', gm.Point(-0.0, 0.0)),
        ('point_huge', gm.Point(1e300, -1e300)),
        ('point_tiny', gm.Point(1e-300, -1e-300)),
        ('point_crs4326', gm.Point(1.0, 2.0, crs=4326)),
        ('point_crs3857', gm.Point(1000.0, 2000.0, crs=3857)),
        ('point_epoch', gm.Point(1.0, 2.0, crs=4326, epoch=2010.5)),
        ('linestring', gm.LineString([(0, 0), (1, 1), (2, 0)])),
        ('linestring_z', gm.from_wkt('LINESTRING Z (0 0 1, 1 1 2)')),
        ('polygon', gm.Polygon(_shell())),
        ('polygon_holed', gm.Polygon(_shell(), [_hole()])),
        ('multipoint', gm.MultiPoint([(0, 0), (1, 1), (2, 2)])),
        ('multilinestring', gm.MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]])),
        (
            'multipolygon',
            gm.MultiPolygon([
                gm.Polygon(_shell()),
                gm.Polygon([(5, 5), (6, 5), (6, 6), (5, 6), (5, 5)]),
            ]),
        ),
        (
            'collection_nested',
            gm.GeometryCollection([
                gm.Point(0, 0),
                gm.LineString([(0, 0), (1, 1)]),
                gm.GeometryCollection([gm.Point(2, 2, z=1.0)]),
            ]),
        ),
        ('empty_point', gm.from_wkt('POINT EMPTY')),
        ('empty_point_z', gm.from_wkt('POINT Z EMPTY')),
        ('empty_linestring', gm.from_wkt('LINESTRING EMPTY')),
        ('empty_polygon', gm.from_wkt('POLYGON EMPTY')),
        ('empty_multipoint', gm.from_wkt('MULTIPOINT EMPTY')),
        ('empty_multilinestring', gm.from_wkt('MULTILINESTRING EMPTY')),
        ('empty_multipolygon', gm.from_wkt('MULTIPOLYGON EMPTY')),
        ('empty_collection', gm.from_wkt('GEOMETRYCOLLECTION EMPTY')),
        (
            'collection_with_empty_members',
            gm.from_wkt(
                'GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING EMPTY, '
                'POLYGON EMPTY, POINT (1 2))'
            ),
        ),
        (
            'multilinestring_empty_member',
            gm.from_wkt('MULTILINESTRING (EMPTY, (0 0, 1 1))'),
        ),
        ('empty_heavy_mls', _empty_heavy_multilinestring(2000)),
    ]
    return items


def _assert_same_shape(
    label: str, original: gm.Geometry, restored: gm.Geometry
) -> None:
    """Exact self-output identity: WKT body, coordinate axes, CRS, epoch."""
    assert str(restored) == str(original), f'{label}: wkt mismatch'
    assert restored.coordinate_axes == original.coordinate_axes, f'{label}: axes'
    assert restored.crs == original.crs, (
        f'{label}: crs {restored.crs!r} vs {original.crs!r}'
    )
    assert restored.epoch == original.epoch, f'{label}: epoch'


def _geojson_roundtrip_eligible(g: gm.Geometry) -> bool:
    """GeoJSON is WGS84 lon/lat + optional Z; skip M, projected, and out-of-domain."""
    if g.has_m:
        return False
    if g.crs is not None:
        try:
            if not gm.CRS(g.crs).is_geographic:
                return False
        except (gm.CRSError, gm.GeometryError, ValueError):
            return False
    # Extreme magnitudes are legal geometry but not legal GeoJSON positions.
    try:
        b = g.bounds
    except (gm.GeometryError, ValueError):
        return True
    if b is None:
        return True
    minx, miny, maxx, maxy = b
    domain = (-180.0, -90.0, 180.0, 90.0)
    return (
        domain[0] <= minx <= domain[2]
        and domain[0] <= maxx <= domain[2]
        and domain[1] <= miny <= domain[3]
        and domain[1] <= maxy <= domain[3]
    )


def _roundtrip_self_output(label: str, g: gm.Geometry) -> None:
    """Every codec that can carry the geometry must accept gometry's own output."""
    # Plain WKT never carries CRS/epoch — compare axes+WKT body only.
    wkt = g.to_wkt(drop_epoch=True)
    from_wkt = gm.from_wkt(wkt)
    assert from_wkt.to_wkt() == wkt, f'{label}/wkt body'
    assert from_wkt.coordinate_axes == g.coordinate_axes, f'{label}/wkt axes'

    # Plain WKB: shape+axes; CRS only when include_srid embeds an EPSG code.
    wkb = g.to_wkb(drop_epoch=True)
    from_wkb = gm.from_wkb(wkb)
    assert from_wkb.to_wkt() == wkt, f'{label}/wkb body'
    assert from_wkb.coordinate_axes == g.coordinate_axes, f'{label}/wkb axes'

    if g.crs is not None:
        try:
            ewkb = g.to_wkb(include_srid=True, drop_epoch=True)
        except (gm.GeometryError, gm.CRSError, ValueError):
            # Non-EPSG CRS cannot embed SRID — skip EWKB path.
            ewkb = None
        if ewkb is not None:
            from_ewkb = gm.from_wkb(ewkb)
            assert from_ewkb.to_wkt() == wkt, f'{label}/ewkb body'
            assert from_ewkb.coordinate_axes == g.coordinate_axes, f'{label}/ewkb axes'
            assert from_ewkb.crs == g.crs, f'{label}/ewkb crs'

    # GeoJSON: RFC 7946 WGS84 domain; skip ineligible sources rather than force it.
    # Accept self-output: decoder may attach default WGS84 CRS, reverse hole
    # winding (RFC 7946), or flatten dimensional empties — topological equals
    # with CRS cleared is the right identity check here.
    if _geojson_roundtrip_eligible(g):
        gj = gm.from_geojson(g.to_geojson(drop_epoch=True))
        left = g if g.crs is None else g.set_crs(None, overwrite=True)
        right = gj if gj.crs is None else gj.set_crs(None, overwrite=True)
        assert gm.equals(left, right), f'{label}/geojson'

    # Pickle and Arrow must reproduce exactly (including CRS + epoch).
    pickled = pickle.loads(pickle.dumps(g))
    _assert_same_shape(f'{label}/pickle', g, pickled)

    arr = gm.GeometryArray([g])
    from_arrow = gm.from_arrow(arr.to_arrow())
    assert len(from_arrow) == 1, f'{label}/arrow len'
    _assert_same_shape(f'{label}/arrow', g, from_arrow[0])


# ---------------------------------------------------------------------------
# Self-output round-trips
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ('label', 'geom'),
    _corpus_geometries(),
    ids=lambda x: x if isinstance(x, str) else '',
)
def test_corpus_self_output_roundtrips(label: str, geom: gm.Geometry) -> None:
    _roundtrip_self_output(label, geom)


def test_corpus_empty_heavy_multilinestring_self_output() -> None:
    """R03 exact shape: 1024+ EMPTY members must round-trip WKB/pickle/Arrow."""
    s = 'MULTILINESTRING (' + ', '.join(['EMPTY'] * 1024 + ['(0 0,1 1)']) + ')'
    g = gm.from_wkt(s)
    wkb = g.to_wkb()
    assert len(wkb) > 0
    back = gm.from_wkb(wkb)
    assert back.to_wkt() == g.to_wkt()
    assert pickle.loads(pickle.dumps(g)).to_wkt() == g.to_wkt()
    arr = gm.from_arrow(gm.GeometryArray([g]).to_arrow())
    assert arr[0].to_wkt() == g.to_wkt()


def test_corpus_large_multipoint_parses() -> None:
    """70k MultiPoint: type, length, first/last coords, WKB round-trip."""
    g = _large_multipoint(70_000)
    assert g.geometry_type == 'MultiPoint'
    assert len(g.coords) == 70_000
    assert g.coords[0] == (1.0, 2.0)
    assert g.coords[-1] == (1.0, 2.0)
    assert gm.from_wkb(g.to_wkb()).to_wkt() == g.to_wkt()


def test_corpus_large_holed_polygon_parses() -> None:
    """65_537-ring polygon: type, interior count, WKB round-trip."""
    g = _large_holed_polygon(65_537)
    assert g.geometry_type == 'Polygon'
    assert len(g.interiors) == 65_536
    back = gm.from_wkb(g.to_wkb())
    assert back.geometry_type == 'Polygon'
    assert len(back.interiors) == 65_536


# ---------------------------------------------------------------------------
# EWKT corpus (R08)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ('wkt', 'axes'),
    [
        ('POINTM(1 2 3)', 'XYM'),
        ('POINTZ(1 2 3)', 'XYZ'),
        ('POINTZM(1 2 3 4)', 'XYZM'),
        ('POINT(1 2 3)', 'XYZ'),
        ('POINT(1 2 3 4)', 'XYZM'),
        ('SRID=4326;MULTIPOINTM(0 0 0,1 2 1)', 'XYM'),
        ('LINESTRINGM(0 0 1, 1 1 2)', 'XYM'),
        ('MULTILINESTRINGZ((0 0 1, 1 1 2))', 'XYZ'),
    ],
)
def test_corpus_ewkt_canonical_forms(wkt: str, axes: str) -> None:
    g = gm.from_wkt(wkt)
    assert g.coordinate_axes == axes
    if wkt.upper().startswith('SRID='):
        assert g.crs == 'EPSG:4326'


def test_corpus_ewkt_is_wkt_routing() -> None:
    """require/is_wkt must recognize compact M suffixes (not GeoJSON)."""
    g = gm.require('POINTM(1 2 3)')
    assert g.coordinate_axes == 'XYM'
    assert g.m == 3.0


@pytest.mark.parametrize(
    'bad',
    [
        'POLYGON (garbage, ...)',
        'LINESTRING (0 0, 1 1,)',
        'LINESTRING (, 0 0)',
        'POINT (1)',
        'MULTIPOLYGON ((()))',
    ],
)
def test_corpus_malformed_wkt_still_rejected(bad: str) -> None:
    with pytest.raises((gm.ParseError, gm.InvalidGeometryError, gm.GeometryError)):
        gm.from_wkt(bad)


# ---------------------------------------------------------------------------
# GeoParquet corpus (R09 / R10)
# ---------------------------------------------------------------------------


def test_corpus_geoparquet_wkb_and_native_roundtrips(tmp_path: Path) -> None:
    values = gm.GeometryArray(
        [
            gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
            gm.LineString([(2.0, 2.0), (3.0, 3.0)], crs=4326),
        ],
        crs=4326,
    )
    attributes = {'id': [1, 2], 'name': ['a', 'b']}

    wkb_path = tmp_path / 'wkb.parquet'
    values.to_geoparquet(str(wkb_path), encoding='wkb', attributes=attributes)
    wkb_restored, wkb_attrs = gm.from_geoparquet(str(wkb_path))
    assert wkb_restored.to_wkt() == values.to_wkt()
    assert [int(v) for v in wkb_attrs['id']] == [1, 2]
    assert wkb_restored.crs in ('EPSG:4326', 'OGC:CRS84')

    native_path = tmp_path / 'native.parquet'
    values.to_geoparquet(str(native_path), encoding='native', attributes=attributes)
    native_restored, native_attrs = gm.from_geoparquet(str(native_path))
    assert native_restored.to_wkt() == values.to_wkt()
    assert [str(v) for v in native_attrs['name']] == ['a', 'b']


def test_corpus_geoparquet_native_plain_list_struct_linestring(tmp_path: Path) -> None:
    """R09: declared encoding resolves depth without embedded ExtensionType."""
    from gometry._geoparquet import _native_geoarrow_column

    coords = pa.StructArray.from_arrays(
        [
            pa.array([0.0, 1.0, 2.0, 3.0]),
            pa.array([0.0, 1.0, 2.0, 3.0]),
        ],
        names=['x', 'y'],
    )
    offsets = pa.array([0, 2, 4], type=pa.int32())
    storage = pa.ListArray.from_arrays(offsets, coords)
    chunked = pa.chunked_array([storage])
    # No extension type — plain list<struct<x,y>> with declared linestring encoding.
    out = _native_geoarrow_column(
        pa,
        chunked,
        encoding='linestring',
        crs='OGC:CRS84',
        epoch=None,
    )
    arr = gm.from_arrow(out, crs='OGC:CRS84')
    assert list(arr.geometry_type) == ['LineString', 'LineString']
    assert arr.to_wkt() == [
        'LINESTRING (0 0, 1 1)',
        'LINESTRING (2 2, 3 3)',
    ]


def test_corpus_geoparquet_extension_disagreement_still_rejected() -> None:
    """D10: embedded extension that disagrees with declared encoding is rejected."""
    from gometry._arrow import GEOARROW_POINT, _extension_type_from_storage
    from gometry._geoparquet import _native_geoarrow_column

    storage = pa.StructArray.from_arrays(
        [pa.array([1.0]), pa.array([2.0])],
        names=['x', 'y'],
    )
    ext_type = _extension_type_from_storage(
        pa, GEOARROW_POINT, storage.type, None, None
    )
    ext = pa.ExtensionArray.from_storage(ext_type, storage)
    chunked = pa.chunked_array([ext])
    with pytest.raises(gm.GeometryError, match=r'conflicts with.*extension|encoding'):
        _native_geoarrow_column(
            pa,
            chunked,
            encoding='linestring',
            crs=None,
            epoch=None,
        )


def test_corpus_geoparquet_antimeridian_bbox_absent_crs() -> None:
    """R10: west>east bbox is legal under default CRS84."""
    from gometry._geoparquet import _validate_column_metadata

    meta, encoding, crs, _ = _validate_column_metadata(
        {
            'columns': {
                'geometry': {
                    'encoding': 'WKB',
                    'geometry_types': [],
                    'bbox': [177.0, -20.0, -178.0, -16.0],
                }
            }
        },
        'geometry',
    )
    assert encoding == 'WKB'
    assert meta['bbox'] == [177.0, -20.0, -178.0, -16.0]
    assert crs == 'OGC:CRS84'


# ---------------------------------------------------------------------------
# Retained amplification protection (200k empty rings)
# ---------------------------------------------------------------------------


def test_corpus_empty_ring_wkb_still_rejected() -> None:
    """200k empty rings must fail structurally (~0 RSS growth), not by magic ratio."""
    n = 200_000
    wkb = b'\x01\x03\x00\x00\x00' + struct.pack('<I', n) + b'\x00\x00\x00\x00' * n
    with pytest.raises(
        (gm.ParseError, gm.InvalidGeometryError, gm.GeometryError),
        match=r'ring|vertices|too short|count|structure|budget',
    ):
        gm.from_wkb(wkb)
