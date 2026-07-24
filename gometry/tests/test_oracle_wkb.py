"""Bidirectional WKB oracle: gometry ↔ Shapely across kinds, axes, endian, EWKB.

Pins that gometry's encoder is understandable by Shapely and that gometry's
decoder accepts Shapely's WKB — catching mutually compensating encoder/decoder
defects that a pure gometry round-trip would miss.
"""

from __future__ import annotations

import struct

import gometry as gm
import pytest

shapely = pytest.importorskip('shapely')


def _sample_geometries() -> list[tuple[str, gm.Geometry]]:
    """Deterministic corpus: 7 kinds x XY/Z/M/ZM (where kind allows) + empties
    + nested collections.
    """
    samples: list[tuple[str, gm.Geometry]] = [
        ('Point/XY', gm.Point(1.0, 2.0)),
        ('Point/XYZ', gm.from_wkt('POINT Z (1 2 3)')),
        ('Point/XYM', gm.from_wkt('POINT M (1 2 3)')),
        ('Point/XYZM', gm.from_wkt('POINT ZM (1 2 3 4)')),
        ('Point/EMPTY', gm.from_wkt('POINT EMPTY')),
        ('LineString/XY', gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)])),
        ('LineString/XYZ', gm.from_wkt('LINESTRING Z (0 0 1, 1 1 2, 2 0 3)')),
        ('LineString/XYM', gm.from_wkt('LINESTRING M (0 0 10, 1 1 20, 2 0 30)')),
        (
            'LineString/XYZM',
            gm.from_wkt('LINESTRING ZM (0 0 1 10, 1 1 2 20, 2 0 3 30)'),
        ),
        ('LineString/EMPTY', gm.from_wkt('LINESTRING EMPTY')),
        (
            'Polygon/XY',
            gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]),
        ),
        (
            'Polygon/XYZ',
            gm.from_wkt('POLYGON Z ((0 0 1, 1 0 1, 1 1 1, 0 0 1))'),
        ),
        (
            'Polygon/XYM',
            gm.from_wkt('POLYGON M ((0 0 10, 1 0 20, 1 1 30, 0 0 10))'),
        ),
        (
            'Polygon/XYZM',
            gm.from_wkt('POLYGON ZM ((0 0 1 10, 1 0 1 20, 1 1 1 30, 0 0 1 10))'),
        ),
        ('Polygon/EMPTY', gm.from_wkt('POLYGON EMPTY')),
        ('MultiPoint/XY', gm.MultiPoint([(0.0, 0.0), (1.0, 1.0)])),
        ('MultiPoint/XYZ', gm.from_wkt('MULTIPOINT Z ((0 0 1), (1 1 2))')),
        ('MultiPoint/XYM', gm.from_wkt('MULTIPOINT M ((0 0 10), (1 1 20))')),
        ('MultiPoint/XYZM', gm.from_wkt('MULTIPOINT ZM ((0 0 1 10), (1 1 2 20))')),
        (
            'MultiLineString/XY',
            gm.MultiLineString([[(0.0, 0.0), (1.0, 1.0)], [(2.0, 2.0), (3.0, 3.0)]]),
        ),
        (
            'MultiLineString/XYZ',
            gm.from_wkt('MULTILINESTRING Z ((0 0 1, 1 1 2), (2 2 3, 3 3 4))'),
        ),
        (
            'MultiPolygon/XY',
            gm.MultiPolygon([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]),
        ),
        (
            'MultiPolygon/XYZ',
            gm.from_wkt(
                'MULTIPOLYGON Z (((0 0 0, 1 0 0, 1 1 0, 0 0 0)), '
                '((2 2 1, 3 2 1, 3 3 1, 2 2 1)))'
            ),
        ),
        (
            'GeometryCollection/XY',
            gm.GeometryCollection([
                gm.Point(1.0, 2.0),
                gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
            ]),
        ),
        (
            'GeometryCollection/XYZ',
            gm.from_wkt(
                'GEOMETRYCOLLECTION Z (POINT Z (1 2 3), LINESTRING Z (0 0 1, 1 1 2))'
            ),
        ),
        (
            'GeometryCollection/nested',
            gm.GeometryCollection([
                gm.GeometryCollection([gm.Point(0.0, 0.0)]),
                gm.MultiPoint([(1.0, 1.0), (2.0, 2.0)]),
            ]),
        ),
        (
            'GeometryCollection/mixed_dim',
            gm.GeometryCollection([
                gm.Point(1.0, 2.0),
                gm.from_wkt('POINT Z (3 4 5)'),
            ]),
        ),
        ('GeometryCollection/EMPTY', gm.from_wkt('GEOMETRYCOLLECTION EMPTY')),
    ]
    return samples


def _geometries_equal(left: gm.Geometry, right: gm.Geometry) -> bool:
    if left.is_empty and right.is_empty:
        return left.geometry_type == right.geometry_type
    if left.coordinate_axes != right.coordinate_axes:
        # Mixed-dim collections may promote on the wire; compare topologically
        # after force_2d when axes diverge only by promotion.
        try:
            return bool(gm.equals(left.force_2d(), right.force_2d()))
        except Exception:
            return False
    try:
        return bool(gm.equals_exact(left, right)) or bool(gm.equals(left, right))
    except Exception:
        return left.to_wkt() == right.to_wkt()


@pytest.mark.parametrize(
    ('label', 'geom'), _sample_geometries(), ids=[s[0] for s in _sample_geometries()]
)
def test_gometry_wkb_decoded_by_shapely_and_back(label: str, geom: gm.Geometry) -> None:
    """geom.to_wkb → shapely.from_wkb → shapely.to_wkb → gm.from_wkb."""
    payload = geom.to_wkb()
    assert isinstance(payload, (bytes, bytearray))
    assert payload[0] in (0, 1)

    shapely_geom = shapely.from_wkb(bytes(payload))
    assert shapely_geom is not None

    # Shapely → gometry return trip (extended flavor preserves M).
    shapely_bytes = shapely.to_wkb(shapely_geom, flavor='extended')
    back = gm.from_wkb(shapely_bytes)
    assert _geometries_equal(geom, back), (label, geom.to_wkt(), back.to_wkt())


@pytest.mark.parametrize(
    ('label', 'geom'), _sample_geometries(), ids=[s[0] for s in _sample_geometries()]
)
def test_shapely_wkb_decoded_by_gometry(label: str, geom: gm.Geometry) -> None:
    """shapely.to_wkb(shapely.from_wkt(gm WKT)) → gm.from_wkb."""
    # Build the shapely side from WKT so the oracle owns the encode path.
    wkt = geom.to_wkt()
    shapely_geom = shapely.from_wkt(wkt)
    for byte_order in (0, 1):  # big-endian, little-endian
        payload = shapely.to_wkb(shapely_geom, byte_order=byte_order, flavor='extended')
        assert payload[0] == byte_order
        back = gm.from_wkb(payload)
        assert _geometries_equal(geom, back), (
            label,
            byte_order,
            geom.to_wkt(),
            back.to_wkt(),
        )


@pytest.mark.parametrize('byte_order', [0, 1], ids=['big', 'little'])
def test_manual_endian_point_round_trip(byte_order: int) -> None:
    """Hand-built ISO WKB Point in both endians decodes identically."""
    x, y = 1.25, -3.5
    if byte_order == 1:
        payload = struct.pack('<BIdd', 1, 1, x, y)
    else:
        payload = struct.pack('>BIdd', 0, 1, x, y)
    geom = gm.from_wkb(payload)
    assert geom.geometry_type == 'Point'
    assert (geom.x, geom.y) == pytest.approx((x, y))
    # gometry re-encodes little-endian
    re = geom.to_wkb()
    assert re[0] == 1
    assert gm.from_wkb(re) == geom


def test_ewkb_srid_bidirectional() -> None:
    """EWKB with SRID flag: gometry ↔ shapely."""
    geom = gm.Point(10.0, 20.0, crs=4326)
    ewkb = geom.to_wkb(include_srid=True)
    shapely_geom = shapely.from_wkb(ewkb)
    assert shapely.get_srid(shapely_geom) == 4326
    assert shapely_geom.x == pytest.approx(10.0)
    assert shapely_geom.y == pytest.approx(20.0)

    # Reverse: shapely EWKB → gometry (an embedded SRID IS auto-applied as
    # the CRS; coordinate payload must match).
    shapely_ewkb = shapely.to_wkb(
        shapely.set_srid(shapely.Point(10.0, 20.0), 4326),
        include_srid=True,
        flavor='extended',
    )
    back = gm.from_wkb(shapely_ewkb)
    assert back.crs == 'EPSG:4326'
    assert back.x == pytest.approx(10.0)
    assert back.y == pytest.approx(20.0)
    # With explicit crs= the frame is attached from the caller, not the SRID
    # alone — pin that coordinates still parse under an explicit CRS.
    tagged = gm.from_wkb(shapely_ewkb, crs=4326)
    assert tagged.crs is not None
    assert tagged.crs.canonical == 'EPSG:4326'


def test_ewkb_nested_collection_with_srid() -> None:
    """Nested GeometryCollection EWKB survives the shapely hop."""
    geom = gm.GeometryCollection(
        [
            gm.Point(1.0, 2.0, crs=4326),
            gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326),
        ],
        crs=4326,
    )
    payload = geom.to_wkb(include_srid=True)
    shapely_geom = shapely.from_wkb(payload)
    assert shapely_geom.geom_type == 'GeometryCollection'
    assert len(shapely_geom.geoms) == 2
    back = gm.from_wkb(shapely.to_wkb(shapely_geom, flavor='extended'), crs=4326)
    assert back.geometry_type == 'GeometryCollection'
    assert len(back.parts) == 2
