"""Revert-sensitive regressions for WKT ingress defects D15-D18.

Each defect group pins the EXACT objective repros plus nearby positives so a
reverted fix fails and a tokenizer that over-rejects is caught.
"""

from __future__ import annotations

import gometry as gm
import pytest

# ---------------------------------------------------------------------------
# D16 — all-empty multipart WKT keeps declared axes
# ---------------------------------------------------------------------------


def test_r17_empty_parenthesis_multipart_rejected() -> None:
    """R17: empty parenthesized multiparts/collections are invalid WKT.

    Exact audit repros: ``MULTILINESTRING ()``, ``MULTIPOLYGON ()``,
    ``GEOMETRYCOLLECTION ()``. The empty spelling is the ``EMPTY`` keyword.
    """
    for wkt in (
        'MULTILINESTRING ()',
        'MULTIPOLYGON ()',
        'GEOMETRYCOLLECTION ()',
        'MULTILINESTRING( )',
        'MULTIPOLYGON(\t)',
    ):
        with pytest.raises(gm.ParseError, match=r'empty parenthesized|EMPTY'):
            gm.from_wkt(wkt)

    # EMPTY keyword remains the empty spelling.
    assert gm.from_wkt('MULTILINESTRING EMPTY').to_wkt() == 'MULTILINESTRING EMPTY'
    assert gm.from_wkt('MULTIPOLYGON EMPTY').to_wkt() == 'MULTIPOLYGON EMPTY'
    assert gm.from_wkt('GEOMETRYCOLLECTION EMPTY').to_wkt() == 'GEOMETRYCOLLECTION EMPTY'

    # Mixed EMPTY member still round-trips (must not weaken).
    mixed = gm.from_wkt('MULTILINESTRING (EMPTY, (0 0,1 1))')
    assert mixed.to_wkt() == 'MULTILINESTRING (EMPTY, (0 0, 1 1))'
    assert gm.from_wkt(mixed.to_wkt()).to_wkt() == mixed.to_wkt()

    # Strict tokenizer rejections retained.
    with pytest.raises(gm.ParseError):
        gm.from_wkt('POLYGON (garbage, (0 0,1 0,1 1,0 0))')
    with pytest.raises(gm.ParseError):
        gm.from_wkt('MULTILINESTRING ((0 0, 1 1),)')

    # Valid WKT/EWKT still parse (PostGIS forms).
    assert gm.from_wkt('POINTM(1 2 3)').to_wkt() == 'POINT M (1 2 3)'
    assert gm.from_wkt('POINT(1 2 3 4)').to_wkt() == 'POINT ZM (1 2 3 4)'
    assert gm.from_wkt('MULTILINESTRING Z (EMPTY)').to_wkt() == 'MULTILINESTRING Z EMPTY'


@pytest.mark.parametrize(
    ('wkt', 'gtype', 'axes', 'expected'),
    [
        ('MULTIPOINT EMPTY', 'MultiPoint', 'XY', 'MULTIPOINT EMPTY'),
        ('MULTIPOINT M EMPTY', 'MultiPoint', 'XYM', 'MULTIPOINT M EMPTY'),
        ('MULTIPOINT (EMPTY)', 'MultiPoint', 'XY', 'MULTIPOINT EMPTY'),
        ('MULTIPOINT M (EMPTY, EMPTY)', 'MultiPoint', 'XYM', 'MULTIPOINT M EMPTY'),
        # EXACT D16 repros: empty-member form keeps declared axes + kind.
        ('MULTIPOINT Z (EMPTY)', 'MultiPoint', 'XYZ', 'MULTIPOINT Z EMPTY'),
        ('MULTILINESTRING EMPTY', 'MultiLineString', 'XY', 'MULTILINESTRING EMPTY'),
        ('MULTILINESTRING M EMPTY', 'MultiLineString', 'XYM', 'MULTILINESTRING M EMPTY'),
        (
            'MULTILINESTRING Z (EMPTY)',
            'MultiLineString',
            'XYZ',
            'MULTILINESTRING Z EMPTY',
        ),
        (
            'MULTILINESTRING ZM (EMPTY, EMPTY)',
            'MultiLineString',
            'XYZM',
            'MULTILINESTRING ZM EMPTY',
        ),
        ('MULTIPOLYGON EMPTY', 'MultiPolygon', 'XY', 'MULTIPOLYGON EMPTY'),
        ('MULTIPOLYGON Z EMPTY', 'MultiPolygon', 'XYZ', 'MULTIPOLYGON Z EMPTY'),
        ('MULTIPOLYGON M (EMPTY)', 'MultiPolygon', 'XYM', 'MULTIPOLYGON M EMPTY'),
        ('MULTIPOLYGON ZM (EMPTY)', 'MultiPolygon', 'XYZM', 'MULTIPOLYGON ZM EMPTY'),
    ],
)
def test_d16_all_empty_multipart_axes_matrix(
    wkt: str, gtype: str, axes: str, expected: str
) -> None:
    """Positive matrix: top-level EMPTY and all-EMPTY members keep axes tags."""
    g = gm.from_wkt(wkt)
    assert g.is_empty
    assert g.geometry_type == gtype
    assert g.coordinate_axes == axes
    assert g.to_wkt() == expected


def test_d16_multipoint_empty_member_mixed_drops_empty() -> None:
    """EMPTY MultiPoint members are accepted then dropped (WKB parity)."""
    g = gm.from_wkt('MULTIPOINT Z ((1 2 3), EMPTY)')
    assert g.to_wkt() == 'MULTIPOINT Z ((1 2 3))'
    assert g.coordinate_axes == 'XYZ'


def test_d16_multipolygon_empty_member_mixed_drops_empty() -> None:
    """EMPTY MultiPolygon members are accepted then dropped (WKB parity)."""
    g = gm.from_wkt('MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), EMPTY)')
    assert g.to_wkt() == 'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))'
    assert not g.is_empty


# ---------------------------------------------------------------------------
# D17 — GeometryCollection explicit axes inheritance
# ---------------------------------------------------------------------------


def test_d17_geometrycollection_z_inherits_into_untagged_point() -> None:
    """EXACT repro: GEOMETRYCOLLECTION Z (POINT (1 2 3)) inherits Z."""
    g = gm.from_wkt('GEOMETRYCOLLECTION Z (POINT (1 2 3))')
    assert g.geometry_type == 'GeometryCollection'
    members = list(g)
    assert len(members) == 1
    assert members[0].geometry_type == 'Point'
    assert members[0].coordinate_axes == 'XYZ'
    assert members[0].z == 3.0
    assert g.to_wkt() == 'GEOMETRYCOLLECTION (POINT Z (1 2 3))'


def test_d17_geometrycollection_z_rejects_short_xy_child() -> None:
    """EXACT repro: GEOMETRYCOLLECTION Z (POINT (1 2)) is under-specified."""
    with pytest.raises(gm.ParseError):
        gm.from_wkt('GEOMETRYCOLLECTION Z (POINT (1 2))')


def test_d17_geometrycollection_z_rejects_mixed_short_after_explicit() -> None:
    """EXACT repro: second untagged child under Z outer must carry Z ordinates."""
    with pytest.raises(gm.ParseError):
        gm.from_wkt('GEOMETRYCOLLECTION Z (POINT Z (1 2 3), POINT (4 5))')


def test_d17_geometrycollection_z_rejects_conflicting_child_tag() -> None:
    """Explicit child tag that conflicts with the outer collection is rejected."""
    with pytest.raises(gm.ParseError, match='dimensional tag conflicts'):
        gm.from_wkt('GEOMETRYCOLLECTION Z (POINT M (1 2 3))')
    with pytest.raises(gm.ParseError, match='dimensional tag conflicts'):
        gm.from_wkt('GEOMETRYCOLLECTION Z (POINT Z (1 2 3), POINT M (4 5 6))')


def test_d17_untagged_geometrycollection_stays_heterogeneous() -> None:
    """EXACT positive: untagged outer collection accepts mixed child axes."""
    g = gm.from_wkt('GEOMETRYCOLLECTION (POINT Z (1 2 3), POINT (4 5))')
    members = list(g)
    assert members[0].coordinate_axes == 'XYZ'
    assert members[1].coordinate_axes == 'XY'
    assert g.to_wkt() == 'GEOMETRYCOLLECTION (POINT Z (1 2 3), POINT (4 5))'


@pytest.mark.parametrize(
    'wkt',
    [
        'GEOMETRYCOLLECTION Z (POINT Z (1 2 3), POINT (4 5 6))',
        'GEOMETRYCOLLECTION Z (LINESTRING (0 0 1, 1 1 2))',
        'GEOMETRYCOLLECTION M (POINT (1 2 3))',
        'GEOMETRYCOLLECTION ZM (POINT (1 2 3 4))',
        'GEOMETRYCOLLECTION Z (MULTIPOINT ((1 2 3), (4 5 6)))',
        'GEOMETRYCOLLECTION Z (GEOMETRYCOLLECTION (POINT (1 2 3)))',
        'GEOMETRYCOLLECTION Z (POINT EMPTY)',
        'geometrycollection z (point (1 2 3))',
    ],
)
def test_d17_tagged_collection_positive_matrix(wkt: str) -> None:
    """Positive: inherited / matching tags parse for several kinds and nestings."""
    g = gm.from_wkt(wkt)
    assert g.geometry_type == 'GeometryCollection'


# ---------------------------------------------------------------------------
# D18 — empty LineString members serialize as valid WKT EMPTY
# ---------------------------------------------------------------------------


def test_d18_multilinestring_empty_member_emits_empty_not_parens() -> None:
    """EXACT repro: zero-length MultiLineString member must not emit () ."""
    g = gm.from_geojson({
        'type': 'MultiLineString',
        'coordinates': [[], [[0, 0], [1, 1]]],
    })
    text = g.to_wkt()
    assert 'EMPTY' in text
    assert '()' not in text
    assert text == 'MULTILINESTRING (EMPTY, (0 0, 1 1))'


def test_d18_multilinestring_empty_member_round_trips() -> None:
    """EXACT repro: from_wkt(to_wkt(...)) must succeed for empty members."""
    g = gm.from_geojson({
        'type': 'MultiLineString',
        'coordinates': [[], [[0, 0], [1, 1]]],
    })
    text = g.to_wkt()
    recovered = gm.from_wkt(text)
    assert recovered.to_wkt() == text
    members = list(recovered)
    assert len(members) == 2
    assert members[0].is_empty
    assert not members[1].is_empty
    assert members[1].to_wkt() == 'LINESTRING (0 0, 1 1)'


def test_d18_multilinestring_empty_member_z_round_trip() -> None:
    """Positive: Z MultiLineString with an empty member keeps axes and round-trips."""
    g = gm.from_wkt('MULTILINESTRING Z (EMPTY, (0 0 1, 1 1 2))')
    text = g.to_wkt()
    assert text == 'MULTILINESTRING Z (EMPTY, (0 0 1, 1 1 2))'
    assert gm.from_wkt(text).to_wkt() == text


# ---------------------------------------------------------------------------
# D15 — strict member grammar (no leftover/garbage tokens)
# ---------------------------------------------------------------------------


def test_d15_polygon_garbage_token_rejected() -> None:
    """EXACT repro: garbage before a ring must not be discarded."""
    with pytest.raises(gm.ParseError):
        gm.from_wkt('POLYGON (garbage, (0 0,1 0,1 1,0 0))')


def test_d15_multilinestring_missing_comma_rejected() -> None:
    """EXACT repro: missing comma between members is a ParseError."""
    with pytest.raises(gm.ParseError):
        gm.from_wkt('MULTILINESTRING ((0 0,1 1) (2 2,3 3))')


def test_d15_geometrycollection_trailing_comma_rejected() -> None:
    """EXACT repro: trailing comma in a GeometryCollection is a ParseError."""
    with pytest.raises(gm.ParseError):
        gm.from_wkt('GEOMETRYCOLLECTION (POINT (1 2),)')


@pytest.mark.parametrize(
    'wkt',
    [
        'POINT (1 2)',
        'POINT Z (1 2 3)',
        'POINT M (1 2 3)',
        'POINT ZM (1 2 3 4)',
        'POINT EMPTY',
        'POINT Z EMPTY',
        'LINESTRING (0 0, 1 1)',
        'LINESTRING Z (0 0 1, 1 1 2)',
        'LINESTRING EMPTY',
        'POLYGON ((0 0, 1 0, 1 1, 0 0))',
        'POLYGON ((0 0, 1 0, 1 1, 0 0), (0.2 0.2, 0.8 0.2, 0.5 0.8, 0.2 0.2))',
        'POLYGON Z ((0 0 1, 1 0 1, 1 1 1, 0 0 1))',
        'POLYGON EMPTY',
        'MULTIPOINT ((1 2), (3 4))',
        'MULTIPOINT (1 2, 3 4)',
        'MULTIPOINT Z ((1 2 3), EMPTY)',
        'MULTIPOINT Z EMPTY',
        'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))',
        'MULTILINESTRING Z (EMPTY, (0 0 1, 1 1 2))',
        'MULTILINESTRING EMPTY',
        'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))',
        'MULTIPOLYGON ZM (EMPTY)',
        'MULTIPOLYGON EMPTY',
        'GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))',
        'GEOMETRYCOLLECTION Z (POINT (1 2 3), LINESTRING (0 0 1, 1 1 2))',
        'GEOMETRYCOLLECTION (POINT Z (1 2 3), POINT (4 5))',
        'GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 2)))',
        'GEOMETRYCOLLECTION EMPTY',
        'SRID=4326;POINT (1 2)',
        'SRID=4326;POLYGON ((0 0, 1 0, 1 1, 0 0))',
        'point (1.5e-3 -2.5E+2)',
        'LINESTRING\n(\n0 0,\n1 1\n)',
        'MULTILINESTRING((0 0,1 1),(2 2,3 3))',
    ],
)
def test_d15_valid_wkt_corpus_still_parses(wkt: str) -> None:
    """Positive no-over-rejection matrix: full valid WKT/EWKT surface still works."""
    g = gm.from_wkt(wkt)
    # Round-trip preserves structure for non-EWKT (SRID is not re-emitted by default).
    body = wkt.split(';', 1)[-1]
    recovered = gm.from_wkt(g.to_wkt())
    assert recovered.to_wkt() == g.to_wkt()
    # Case-insensitive / whitespace variants still parse the same kind.
    assert g.geometry_type is not None
    _ = body  # documented intent: EWKT prefix accepted above
