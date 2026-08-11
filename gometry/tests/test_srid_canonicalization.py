"""D19: EWKT/EWKB SRIDs go through canonical CRS parsing.

SRID 0 is PostGIS unknown/unspecified → CRS-free (never a false EPSG:0).
Nonzero codes resolve via PROJ at parse time (invalid codes fail immediately).
"""

from __future__ import annotations

import struct

import gometry as gm
import pytest


def test_ewkt_srid_zero_is_crs_free() -> None:
    """Exact repro: SRID=0;POINT must not produce EPSG:0."""
    geom = gm.from_wkt('SRID=0;POINT (1 2)')
    assert geom.crs is None
    assert geom.to_wkt() == 'POINT (1 2)'
    # Case-insensitive prefix too.
    assert gm.from_wkt('srid=0;POINT (1 2)').crs is None
    assert gm.require('SRID=0;POINT (1 2)').crs is None


def test_ewkt_invalid_srid_rejected_at_parse() -> None:
    """Exact repro: SRID=999999 must fail at parse, not later."""
    with pytest.raises(gm.CRSError, match=r'999999|EPSG:999999|resolve'):
        gm.from_wkt('SRID=999999;POINT (1 2)')
    with pytest.raises(gm.CRSError):
        gm.require('SRID=999999;POINT (1 2)')


def test_ewkb_srid_zero_is_crs_free() -> None:
    """Exact repro: EWKB packet with SRID 0 → crs is None."""
    # little-endian point, EWKB SRID flag | type 1, srid=0, x=1, y=2
    ewkb = struct.pack('<BII2d', 1, 0x20000001, 0, 1.0, 2.0)
    geom = gm.from_wkb(ewkb)
    assert geom.crs is None
    assert geom.to_wkt() == 'POINT (1 2)'
    arr = gm.GeometryArray([ewkb])
    assert arr.crs is None
    assert arr[0].crs is None


def test_ewkb_invalid_srid_rejected_at_parse() -> None:
    ewkb = struct.pack('<BII2d', 1, 0x20000001, 999_999, 1.0, 2.0)
    with pytest.raises(gm.CRSError, match=r'999999|EPSG:999999|resolve'):
        gm.from_wkb(ewkb)


def test_valid_srid_positives_ewkt_ewkb() -> None:
    """No over-rejection: real SRIDs still attach on every lane."""
    geom = gm.from_wkt('SRID=4326;POINT (1 2)')
    assert geom.crs is not None
    assert geom.crs.to_authority() == ('EPSG', '4326')

    ewkb = struct.pack('<BII2d', 1, 0x20000001, 4326, 1.0, 2.0)
    from_ewkb = gm.from_wkb(ewkb)
    assert from_ewkb.crs is not None
    assert from_ewkb.crs.to_authority() == ('EPSG', '4326')
    assert from_ewkb.to_wkt() == 'POINT (1 2)'

    # Explicit crs= still fills SRID-less plain WKT/WKB.
    assert gm.from_wkt('POINT (0 0)', crs=4326).crs.to_authority() == ('EPSG', '4326')
    plain_wkb = gm.Point(0, 0).to_wkb()
    assert gm.from_wkb(plain_wkb, crs=4326).crs.to_authority() == ('EPSG', '4326')

    # Round-trip with include_srid keeps a valid EPSG.
    roundtrip = gm.from_wkb(gm.Point(1, 2, crs=4326).to_wkb(include_srid=True))
    assert roundtrip.crs.to_authority() == ('EPSG', '4326')


def test_arrow_wkb_inherits_srid_canonicalization() -> None:
    """Arrow / bulk WKB import uses the same reader (valid SRID + SRID 0)."""
    valid = gm.Point(1, 2, crs=4326).to_wkb(include_srid=True)
    zero = struct.pack('<BII2d', 1, 0x20000001, 0, 3.0, 4.0)
    # Homogeneous frames only — GeometryArray requires one shared CRS.
    from_valid = gm.from_wkb([valid, valid])
    assert from_valid.crs.to_authority() == ('EPSG', '4326')
    from_zero = gm.from_wkb([zero, zero])
    assert from_zero.crs is None
    assert from_zero[0].to_wkt() == 'POINT (3 4)'
    # Arrow capsule path: GeometryArray → to_arrow → from_arrow preserves CRS.
    native = gm.GeometryArray([gm.Point(1, 2, crs=4326)])
    back = gm.from_arrow(native.to_arrow())
    assert back.crs.to_authority() == ('EPSG', '4326')


def _ewkb_point(x: float, y: float, srid: int) -> bytes:
    """Little-endian EWKB Point with embedded SRID."""
    return struct.pack('<BII2d', 1, 0x20000001, srid, x, y)


def test_bulk_mixed_distinct_srids_require_shared_crs() -> None:
    """Two distinct embedded SRIDs in one batch → SharedCrs (EWKB and EWKT)."""
    a = _ewkb_point(1.0, 2.0, 32634)
    b = _ewkb_point(3.0, 4.0, 4326)
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.from_wkb([a, b])
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.from_wkt(['SRID=32634;POINT (1 2)', 'SRID=4326;POINT (3 4)'])


def test_bulk_partial_embedded_srid_requires_shared_crs() -> None:
    """SRID-bearing row mixed with plain/SRID-0 row fails without explicit crs=."""
    tagged = _ewkb_point(1.0, 2.0, 32634)
    plain = gm.Point(3.0, 4.0).to_wkb()
    zero = _ewkb_point(5.0, 6.0, 0)
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.from_wkb([tagged, plain])
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.from_wkb([tagged, zero])
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.from_wkt(['SRID=32634;POINT (1 2)', 'POINT (3 4)'])
    with pytest.raises(gm.CRSMismatchError, match='requires one shared CRS'):
        gm.from_wkt(['SRID=32634;POINT (1 2)', 'SRID=0;POINT (3 4)'])


def test_bulk_explicit_crs_agrees_and_conflicts_with_embedded() -> None:
    """Explicit crs= agrees with embedded SRID, conflicts raise CRSMismatchError."""
    a = _ewkb_point(1.0, 2.0, 32634)
    b = _ewkb_point(3.0, 4.0, 32634)
    ok = gm.from_wkb([a, b], crs=32634)
    assert ok.crs.to_authority() == ('EPSG', '32634')
    # Plain rows under agreeing explicit crs= share the frame.
    plain = gm.Point(0.0, 0.0).to_wkb()
    mixed = gm.from_wkb([plain, a], crs=32634)
    assert mixed.crs.to_authority() == ('EPSG', '32634')
    with pytest.raises(
        gm.CRSMismatchError, match='conflicts with the embedded EWKB SRID'
    ):
        gm.from_wkb([a, b], crs=4326)
    with pytest.raises(
        gm.CRSMismatchError, match='conflicts with the embedded EWKT SRID'
    ):
        gm.from_wkt(
            ['SRID=32634;POINT (1 2)', 'SRID=32634;POINT (3 4)'],
            crs=4326,
        )


def test_bulk_srid_plus_epoch() -> None:
    """epoch= attaches to a uniform embedded-SRID batch; epoch without CRS fails."""
    a = _ewkb_point(1.0, 2.0, 4326)
    b = _ewkb_point(3.0, 4.0, 4326)
    arr = gm.from_wkb([a, b], epoch=2020.0)
    assert arr.crs.to_authority() == ('EPSG', '4326')
    assert arr.epoch == 2020.0
    assert [row.epoch for row in arr] == [2020.0, 2020.0]
    ewkt = gm.from_wkt(
        ['SRID=4326;POINT (1 2)', 'SRID=4326;POINT (3 4)'],
        epoch=2015.5,
    )
    assert ewkt.epoch == 2015.5
    # SRID 0 → CRS-free; epoch requires a CRS.
    zero = _ewkb_point(1.0, 2.0, 0)
    with pytest.raises(gm.CRSError, match='coordinate epoch requires a CRS'):
        gm.from_wkb([zero, zero], epoch=2020.0)
    # Scalar path still wires epoch with embedded SRID.
    scalar = gm.from_wkb(a, epoch=2010.0)
    assert scalar.crs.to_authority() == ('EPSG', '4326')
    assert scalar.epoch == 2010.0
