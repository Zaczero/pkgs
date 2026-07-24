"""D24: buffer / EWKT coercion frontend parity.

One-byte buffers (signed and unsigned) and case-insensitive EWKT must be
accepted on every ingress that claims the bytes/WKT contract — not only the
scalar ``from_wkb`` / ``from_wkt`` paths.
"""

from __future__ import annotations

import array

import gometry as gm


def test_geometry_array_accepts_array_array_wkb() -> None:
    """Exact repro: GeometryArray rows may be array.array('B', wkb)."""
    wkb = gm.Point(1, 2).to_wkb()
    scalar = gm.from_wkb(array.array("B", wkb))
    arr = gm.GeometryArray([array.array("B", wkb)])
    assert len(arr) == 1
    assert gm.equals(arr[0], scalar)
    assert arr[0].to_wkt() == "POINT (1 2)"


def test_from_wkb_and_array_accept_signed_one_byte_memoryview() -> None:
    """Exact repro: memoryview cast to signed 'b' is still a one-byte Buffer."""
    wkb = gm.Point(1, 2).to_wkb()
    signed = memoryview(wkb).cast("b")
    assert gm.from_wkb(signed).to_wkt() == "POINT (1 2)"
    arr = gm.GeometryArray([signed])
    assert arr[0].to_wkt() == "POINT (1 2)"
    assert gm.require(signed).to_wkt() == "POINT (1 2)"


def test_require_case_insensitive_ewkt_not_geojson() -> None:
    """Exact repro: lowercase srid=… is EWKT, not a failed GeoJSON parse."""
    geom = gm.require("srid=4326;POINT (1 2)")
    assert geom.to_wkt() == "POINT (1 2)"
    assert geom.crs is not None
    assert geom.crs.to_authority() == ("EPSG", "4326")
    # Uppercase remains accepted (parity with from_wkt).
    upper = gm.require("SRID=4326;POINT (1 2)")
    assert upper.crs.to_authority() == ("EPSG", "4326")


def test_one_byte_buffer_positives_across_frontends() -> None:
    """No over-rejection: bytes / bytearray / unsigned memoryview / array.array."""
    wkb = gm.Point(3, 4).to_wkb()
    payloads = [
        wkb,
        bytearray(wkb),
        memoryview(wkb),
        array.array("B", wkb),
        memoryview(wkb).cast("B"),
    ]
    for payload in payloads:
        assert gm.from_wkb(payload).to_wkt() == "POINT (3 4)"
        assert gm.GeometryArray([payload])[0].to_wkt() == "POINT (3 4)"
        assert gm.require(payload).to_wkt() == "POINT (3 4)"


def test_from_wkt_ewkt_still_accepts_lowercase_srid() -> None:
    """from_wkt already had case-insensitive SRID; keep it green."""
    geom = gm.from_wkt("srid=4326;POINT (1 2)")
    assert geom.crs.to_authority() == ("EPSG", "4326")


def test_require_plain_wkt_and_geojson_still_work() -> None:
    assert gm.require("POINT (0 0)").to_wkt() == "POINT (0 0)"
    assert gm.require({"type": "Point", "coordinates": [0, 1]}).to_wkt() == "POINT (0 1)"
