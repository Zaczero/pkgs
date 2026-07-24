"""WKB/EWKB/EWKT spec conformance: mixed-axis multipart promotion, nested
EWKB SRID inheritance, and EWKT prefix tolerance.
"""

import struct

import gometry as gm
import pytest


def _iso_dim(type_code: int) -> int:
    """Ordinate count encoded by an ISO WKB geometry-type code (1000=Z,
    2000=M, 3000=ZM offsets).
    """
    base = type_code
    z = m = False
    for offset, has_z, has_m in (
        (3000, True, True),
        (2000, False, True),
        (1000, True, False),
    ):
        if base >= offset:
            z, m = has_z, has_m
            break
    return 2 + int(z) + int(m)


def _mls_type_codes(wkb: bytes) -> list[int]:
    """Outer + per-member ISO geometry-type codes of a little-endian
    MULTILINESTRING WKB.
    """
    assert wkb[0] == 1, 'little-endian byte order'
    off = 1
    outer = struct.unpack_from('<I', wkb, off)[0]
    off += 4
    count = struct.unpack_from('<I', wkb, off)[0]
    off += 4
    codes = [outer]
    for _ in range(count):
        assert wkb[off] == 1, 'little-endian member byte order'
        off += 1
        member_type = struct.unpack_from('<I', wkb, off)[0]
        off += 4
        codes.append(member_type)
        npts = struct.unpack_from('<I', wkb, off)[0]
        off += 4
        off += npts * _iso_dim(member_type) * 8
    return codes


def test_mixed_axis_multiline_promotes_members_to_union_axes() -> None:
    """A mixed XY/XYZ MULTILINESTRING must serialize with the union axes on the
    outer AND every child header (standards-valid WKB), filling promoted Z with
    0.0 rather than fabricating NaN in WKT.
    """
    mixed = gm.MultiLineString([[(0, 0), (1, 1)], [(2, 2, 3), (3, 3, 4)]])
    assert mixed.coordinate_axes == 'XYZ'

    # WKT: promoted ordinate is 0.0, never NaN.
    assert format(mixed) == 'MULTILINESTRING Z ((0 0 0, 1 1 0), (2 2 3, 3 3 4))'
    assert 'nan' not in format(mixed).lower()

    # WKB: outer and both children carry the ISO-Z type code (1005 / 1002).
    wkb = mixed.to_wkb()
    assert _mls_type_codes(wkb) == [1005, 1002, 1002]

    # Round-trips to the fully-promoted shape (first member gains z == 0.0).
    promoted = gm.MultiLineString([[(0, 0, 0), (1, 1, 0)], [(2, 2, 3), (3, 3, 4)]])
    assert gm.from_wkb(wkb) == promoted

    # Shapely parses the promoted bytes and sees both parts as 3D.
    import shapely

    parsed = shapely.from_wkb(wkb)
    assert parsed.geom_type == 'MultiLineString'
    assert shapely.has_z(parsed)
    assert all(shapely.has_z(part) for part in parsed.geoms)


def test_mixed_axis_multipolygon_promotes_members_to_union_axes() -> None:
    """MULTIPOLYGON mirrors MULTILINESTRING: XY members promote to the union
    XYZ, children carrying the ISO-Z type code (1006 / 1003).
    """
    mixed = gm.MultiPolygon([
        [[(0, 0), (1, 0), (1, 1), (0, 0)]],
        [[(2, 2, 1), (3, 2, 2), (3, 3, 3), (2, 2, 1)]],
    ])
    assert mixed.coordinate_axes == 'XYZ'
    wkb = mixed.to_wkb()
    assert wkb[0] == 1
    assert struct.unpack_from('<I', wkb, 1)[0] == 1006  # WKBMultiPolygonZ

    import shapely

    parsed = shapely.from_wkb(wkb)
    assert parsed.geom_type == 'MultiPolygon'
    assert all(shapely.has_z(part) for part in parsed.geoms)
    # First polygon's shell gained z == 0.0 on promotion.
    first = gm.from_wkb(wkb).parts[0]
    assert first.coordinate_axes == 'XYZ'


def _ewkb(type_base: int, srid: int | None, payload: bytes) -> bytes:
    """One little-endian EWKB element: byte order, type (with the SRID flag
    when ``srid`` is given), the optional SRID, then ``payload``.
    """
    flag = 0x2000_0000 if srid is not None else 0
    out = b'\x01' + struct.pack('<I', type_base | flag)
    if srid is not None:
        out += struct.pack('<I', srid)
    return out + payload


def test_nested_ewkb_srid_accepts_matching_and_rejects_conflict() -> None:
    """PostGIS may stamp the SRID flag on nested members: a nested SRID equal
    to the parent's is accepted; a genuine conflict is rejected with both codes
    and the member path.
    """
    point_body = struct.pack('<dd', 1.0, 2.0)

    # GEOMETRYCOLLECTION SRID=4326 containing POINT SRID=4326 — accepted.
    matching = _ewkb(7, 4326, struct.pack('<I', 1) + _ewkb(1, 4326, point_body))
    recovered = gm.from_wkb(matching)
    assert recovered.crs == 'EPSG:4326'
    assert recovered.to_wkt() == 'GEOMETRYCOLLECTION (POINT (1 2))'

    # A nested member with no SRID under an SRID parent is also accepted.
    inherit = _ewkb(7, 4326, struct.pack('<I', 1) + _ewkb(1, None, point_body))
    assert gm.from_wkb(inherit).crs == 'EPSG:4326'

    # Conflicting nested SRID — rejected, naming both codes and the path.
    conflict = _ewkb(7, 4326, struct.pack('<I', 1) + _ewkb(1, 3857, point_body))
    with pytest.raises(gm.ParseError) as excinfo:
        gm.from_wkb(conflict)
    message = str(excinfo.value)
    assert '4326' in message and '3857' in message
    assert 'member 0' in message
    assert not isinstance(excinfo.value, BaseException) or type(excinfo.value).__name__ != (
        'PanicException'
    )


def test_nested_ewkb_srid_zero_is_unknown_before_reconcile() -> None:
    """R07: SRID 0 is PostGIS unknown — normalize before nested reconcile.

    Outer 0 + child 4326, outer 4326 + child 0, and sibling 0/4326 must parse;
    a genuine nested conflict (child 3857 under outer 4326) still errors.
    """
    point_body = struct.pack('<dd', 1.0, 2.0)

    # Exact R07 shape: GeometryCollection SRID 0 + Point SRID 4326.
    outer0_child4326 = _ewkb(7, 0, struct.pack('<I', 1) + _ewkb(1, 4326, point_body))
    # Hand-built wire (SRID flag on outer type 7, srid=0, one member).
    assert outer0_child4326 == bytes.fromhex(
        '010700002000000000'
        '01000000'
        '0101000020e6100000'
        '000000000000f03f'
        '0000000000000040'
    )
    g = gm.from_wkb(outer0_child4326)
    assert g.crs == 'EPSG:4326'
    assert g.to_wkt() == 'GEOMETRYCOLLECTION (POINT (1 2))'

    outer4326_child0 = _ewkb(7, 4326, struct.pack('<I', 1) + _ewkb(1, 0, point_body))
    g2 = gm.from_wkb(outer4326_child0)
    assert g2.crs == 'EPSG:4326'
    assert g2.to_wkt() == 'GEOMETRYCOLLECTION (POINT (1 2))'

    # Sibling stamps: first member SRID 0 (unknown), second establishes 4326.
    sibling = _ewkb(
        7,
        None,
        struct.pack('<I', 2)
        + _ewkb(1, 0, point_body)
        + _ewkb(1, 4326, struct.pack('<dd', 3.0, 4.0)),
    )
    g3 = gm.from_wkb(sibling)
    assert g3.crs == 'EPSG:4326'
    assert g3.to_wkt() == 'GEOMETRYCOLLECTION (POINT (1 2), POINT (3 4))'

    # Genuine nested conflict still errors (typed ParseError, never panic).
    conflict = _ewkb(7, 4326, struct.pack('<I', 1) + _ewkb(1, 3857, point_body))
    with pytest.raises(gm.ParseError, match='conflicts with enclosing SRID') as excinfo:
        gm.from_wkb(conflict)
    assert '4326' in str(excinfo.value) and '3857' in str(excinfo.value)
    assert excinfo.value.format == 'wkb'


_EMPTY_KINDS = [
    'POINT',
    'LINESTRING',
    'POLYGON',
    'MULTIPOINT',
    'MULTILINESTRING',
    'MULTIPOLYGON',
    'GEOMETRYCOLLECTION',
]
_EMPTY_TAGS = [('', 'XY'), (' Z', 'XYZ'), (' M', 'XYM'), (' ZM', 'XYZM')]


@pytest.mark.parametrize('kind', _EMPTY_KINDS)
@pytest.mark.parametrize(('tag', 'axes'), _EMPTY_TAGS)
def test_empty_dimensionality_round_trips_wkt_and_wkb(
    kind: str, tag: str, axes: str
) -> None:
    """`POINT Z EMPTY` and friends keep their dimensional tag through both
    WKT and WKB, for every geometry kind and axes combination.
    """
    wkt = f'{kind}{tag} EMPTY'
    geom = gm.from_wkt(wkt)
    assert geom.is_empty
    assert geom.geometry_type.upper() == kind
    assert geom.coordinate_axes == axes
    assert geom.has_z is ('Z' in axes)
    assert geom.has_m is ('M' in axes)
    assert geom.to_wkt() == wkt

    back = gm.from_wkb(geom.to_wkb())
    assert back == geom
    assert back.to_wkt() == wkt
    assert back.coordinate_axes == axes


def test_empty_point_z_wkb_is_iso_1001_with_three_nan() -> None:
    """The writer is symmetric with the reader's NaN-sentinel convention:
    `POINT Z EMPTY` is ISO type 1001 with NaN for each of the 3 ordinates.
    """
    wkb = gm.from_wkt('POINT Z EMPTY').to_wkb()
    assert wkb[0] == 1
    assert struct.unpack_from('<I', wkb, 1)[0] == 1001
    assert len(wkb) == 1 + 4 + 3 * 8
    assert all(x != x for x in struct.unpack_from('<3d', wkb, 5))  # noqa: PLR0124

    zm = gm.from_wkt('POINT ZM EMPTY').to_wkb()
    assert struct.unpack_from('<I', zm, 1)[0] == 3001
    assert len(zm) == 1 + 4 + 4 * 8


def test_empty_container_z_wkb_is_axes_typed_zero_count() -> None:
    """Dimensional empty containers write the axes-typed code with count 0."""
    for kind, code in (
        ('MULTIPOINT', 1004),
        ('MULTILINESTRING', 1005),
        ('MULTIPOLYGON', 1006),
        ('GEOMETRYCOLLECTION', 1007),
    ):
        wkb = gm.from_wkt(f'{kind} Z EMPTY').to_wkb()
        assert struct.unpack_from('<I', wkb, 1)[0] == code, kind
        assert struct.unpack_from('<I', wkb, 5)[0] == 0, kind
    # POLYGON Z EMPTY is the zero-ring body under the Z type code.
    wkb = gm.from_wkt('POLYGON Z EMPTY').to_wkb()
    assert struct.unpack_from('<I', wkb, 1)[0] == 1003
    assert struct.unpack_from('<I', wkb, 5)[0] == 0


def test_dimensional_empties_are_typed_leaves() -> None:
    """A dimensional empty parses to the matching typed leaf class."""
    for wkt, leaf in (
        ('POINT Z EMPTY', gm.Point),
        ('LINESTRING Z EMPTY', gm.LineString),
        ('POLYGON Z EMPTY', gm.Polygon),
        ('MULTIPOINT Z EMPTY', gm.MultiPoint),
        ('MULTILINESTRING Z EMPTY', gm.MultiLineString),
        ('MULTIPOLYGON Z EMPTY', gm.MultiPolygon),
        ('GEOMETRYCOLLECTION Z EMPTY', gm.GeometryCollection),
    ):
        geom = gm.from_wkt(wkt)
        assert isinstance(geom, leaf), wkt
    for wkt in ('MULTILINESTRING ZM EMPTY', 'MULTIPOLYGON M EMPTY'):
        geom = gm.from_wkt(wkt)
        assert len(geom.parts) == 0
        assert list(geom) == []


def test_ewkt_prefix_is_case_insensitive_and_whitespace_tolerant() -> None:
    """The EWKT ``SRID=<code>;`` prefix is recognized ASCII-case-insensitively
    and after leading whitespace, not only as exact uppercase at byte zero.
    """
    for text in (
        'srid=4326;POINT (1 2)',
        'Srid=4326;POINT (1 2)',
        '  SRID=4326;POINT (1 2)',
        '\t sRiD=4326; POINT (1 2)',
    ):
        parsed = gm.from_wkt(text)
        assert parsed.crs == 'EPSG:4326', text
        assert parsed.to_wkt() == 'POINT (1 2)', text

    # A bare WKT body (no prefix) still parses, unaffected by the trim.
    assert gm.from_wkt('  POINT (3 4)').to_wkt() == 'POINT (3 4)'


def test_malformed_wkb_raises_clean_errors_never_panics() -> None:
    malformed = [
        b'',
        b'\x01',
        b'\x01\x01\x00\x00\x00\x00\x00',
        b'\x01\x03\x00\x00\x00\xff\xff\xff\x7f',
        b'\x01c\x00\x00\x00',
        b'\x07\x01\x00\x00\x00',
    ]
    for payload in malformed:
        with pytest.raises(ValueError, match='WKB'):
            gm.from_wkb(payload)


def test_wkb_rejects_oversized_coordinate_count_before_allocation() -> None:
    """Hostile vertex counts must fail against remaining bytes, not reserve GB."""
    linestring = b'\x01' + struct.pack('<I', 2) + struct.pack('<I', 4294967295)
    multipoint = b'\x01' + struct.pack('<I', 4) + struct.pack('<I', 4294967295)
    multipolygon = (
        b'\x01'
        + struct.pack('<I', 6)
        + struct.pack('<I', 4294967295)
        + struct.pack('<I', 1)
        + struct.pack('<I', 0)
    )
    collection = b'\x01' + struct.pack('<I', 7) + struct.pack('<I', 4294967295)
    for payload in (linestring, multipoint, multipolygon, collection):
        with pytest.raises(gm.ParseError, match=r'exceeds remaining input|too large'):
            gm.from_wkb(payload)
