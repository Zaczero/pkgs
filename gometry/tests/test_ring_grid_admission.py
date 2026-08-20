"""R15-I: ring admission equivalence + grid bounding residuals + MLS empty axes.

Deterministic fixtures only. Guards:
- A: constructor / WKT / WKB / pickle ring admission are one policy
- B1/C14: h3_bounding_cell proves the full antimeridian-aware rectangle
- B2/C15: s2_bounding_cell scalar / collection / array share one seam owner
- B3 residual: covered-point tokens ⊆ visible cover tokens; collinear invariance
- C17: typed empty MultiLineString members participate in axis homogeneity
"""

from __future__ import annotations

import pickle
import struct
from typing import Any

import gometry as gm
import pytest

# ---------------------------------------------------------------------------
# A — ring admission matrix (constructor, WKT, WKB, pickle)
# ---------------------------------------------------------------------------


def _ring_payloads() -> list[tuple[str, list[tuple[float, ...]], str | None]]:
    """(name, coords, expected_outcome).

    ``expected_outcome`` is None for reject, or a WKT substring for accept.
    """
    return [
        ('2-corner', [(0.0, 0.0), (1.0, 0.0)], None),
        (
            'open-3',
            [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)],
            'POLYGON ((0 0, 1 0, 0 1, 0 0))',
        ),
        (
            'closed-4',
            [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)],
            'POLYGON ((0 0, 1 0, 1 1, 0 0))',
        ),
        (
            'XY-closed-Z-open',
            [(0.0, 0.0, 1.0), (1.0, 0.0, 2.0), (1.0, 1.0, 3.0), (0.0, 0.0, 9.0)],
            None,
        ),
        (
            'XY-closed-M-open',
            # Point(x,y,z,m) via 4-tuple with Z None is awkward; use M via WKT
            # for serialized paths. Constructor uses (x, y, m) is not standard —
            # gometry Point is (x,y,z=None,m=None). Build M via set_m after.
            # Represent as XYZM with Z equal and M open:
            [
                (0.0, 0.0, 0.0, 1.0),
                (1.0, 0.0, 0.0, 2.0),
                (1.0, 1.0, 0.0, 3.0),
                (0.0, 0.0, 0.0, 9.0),
            ],
            None,
        ),
        (
            'degenerate-closed-3',
            [(0.0, 0.0), (1.0, 0.0), (0.0, 0.0)],
            None,
        ),
        (
            'closed-4-Z',
            [(0.0, 0.0, 1.0), (1.0, 0.0, 1.0), (1.0, 1.0, 1.0), (0.0, 0.0, 1.0)],
            'POLYGON Z ((0 0 1, 1 0 1, 1 1 1, 0 0 1))',
        ),
    ]


def _coords_to_wkt_ring(coords: list[tuple[float, ...]]) -> str:
    parts = []
    for c in coords:
        if len(c) == 2:
            parts.append(f'{c[0]} {c[1]}')
        elif len(c) == 3:
            parts.append(f'{c[0]} {c[1]} {c[2]}')
        else:
            parts.append(f'{c[0]} {c[1]} {c[2]} {c[3]}')
    body = ', '.join(parts)
    if len(coords[0]) == 4:
        return f'POLYGON ZM(({body}))'
    if len(coords[0]) == 3:
        return f'POLYGON Z(({body}))'
    return f'POLYGON(({body}))'


def _try_construct(coords: list[tuple[float, ...]]) -> tuple[str, Any]:
    try:
        g = gm.Polygon(coords)
    except Exception as exc:
        return type(exc).__name__, exc
    else:
        return 'ok', g


def _try_wkt(coords: list[tuple[float, ...]]) -> tuple[str, Any]:
    try:
        g = gm.from_wkt(_coords_to_wkt_ring(coords))
    except Exception as exc:
        return type(exc).__name__, exc
    else:
        return 'ok', g


def _hand_wkb_polygon(coords: list[tuple[float, ...]]) -> bytes:
    """ISO WKB polygon with one ring — admits open / Z-open payloads shapely rejects."""
    ndim = len(coords[0])
    # ISO type codes: XY=3, XYZ=1003, XYM=2003, XYZM=3003
    type_code = {2: 3, 3: 1003, 4: 3003}[ndim]
    body = struct.pack('<BII', 1, type_code, 1)  # LE, type, 1 ring
    body += struct.pack('<I', len(coords))
    fmt = {2: '<dd', 3: '<ddd', 4: '<dddd'}[ndim]
    for c in coords:
        body += struct.pack(fmt, *c)
    return body


def _try_wkb(coords: list[tuple[float, ...]]) -> tuple[str, Any]:
    try:
        g = gm.from_wkb(_hand_wkb_polygon(coords))
    except Exception as exc:
        return type(exc).__name__, exc
    else:
        return 'ok', g


def _try_pickle(coords: list[tuple[float, ...]]) -> tuple[str, Any]:
    """Pickle pack admission for a single-ring polygon column."""
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    zs = [c[2] for c in coords] if len(coords[0]) >= 3 else None
    ms = [c[3] for c in coords] if len(coords[0]) >= 4 else None
    # For 3-tuples that are XYZ (not XYM): Z only.
    if len(coords[0]) == 3:
        ms = None

    def pack_f(vals: list[float] | None) -> bytes | None:
        if vals is None:
            return None
        return b''.join(struct.pack('<d', v) for v in vals)

    def pack_i(vals: list[int]) -> bytes:
        return b''.join(struct.pack('<i', v) for v in vals)

    n = len(coords)
    try:
        arr = gm._lib._unpickle_polygon_array(  # type: ignore[attr-defined]
            pack_f(xs),
            pack_f(ys),
            pack_f(zs),
            pack_f(ms),
            pack_i([0, n]),
            pack_i([0, 1]),
            None,
            None,
            None,
            None,
        )
    except Exception as exc:
        return type(exc).__name__, exc
    else:
        return 'ok', arr[0]


@pytest.mark.parametrize('name,coords,expected_wkt', _ring_payloads())
def test_r15i_ring_admission_matrix_four_ingresses(
    name: str,
    coords: list[tuple[float, ...]],
    expected_wkt: str | None,
) -> None:
    """Constructor, WKT, WKB, pickle agree on accept/reject + normalized form."""
    results = {
        'ctor': _try_construct(coords),
        'wkt': _try_wkt(coords),
        'wkb': _try_wkb(coords),
        'pickle': _try_pickle(coords),
    }
    accepts = {k: (v[0] == 'ok') for k, v in results.items()}
    # All four must accept, or all four must reject.
    assert len(set(accepts.values())) == 1, (
        f'{name}: ingress disagreement { {k: (results[k][0], str(results[k][1])[:80]) for k in results} }'
    )
    accepted = next(iter(accepts.values()))
    if expected_wkt is None:
        assert not accepted, f'{name}: expected reject, got accept {results}'
        # Domain: short rings vs active-ordinate mismatch.
        messages = ' | '.join(str(results[k][1]) for k in results)
        if 'Z-open' in name or 'M-open' in name:
            assert (
                'closed on all active ordinates' in messages
                or 'closed' in messages.lower()
            )
        else:
            assert (
                'three coordinates' in messages
                or 'RingTooShort' in messages
                or 'require at least' in messages
            )
        return

    assert accepted, f'{name}: expected accept, got {results}'
    wkts = {k: results[k][1].to_wkt() for k in results}
    # Normalized geometry is identical across ingresses.
    assert len(set(wkts.values())) == 1, f'{name}: WKT diverge {wkts}'
    assert next(iter(wkts.values())) == expected_wkt


def test_r15i_multipolygon_ctor_rejects_z_open_ring() -> None:
    coords = [(0.0, 0.0, 1.0), (1.0, 0.0, 2.0), (1.0, 1.0, 3.0), (0.0, 0.0, 9.0)]
    with pytest.raises(gm.GeometryError, match='closed on all active ordinates'):
        gm.MultiPolygon([coords])
    with pytest.raises(gm.ParseError, match='closed on all active ordinates'):
        gm.from_wkt('MULTIPOLYGON Z(((0 0 1, 1 0 2, 1 1 3, 0 0 9)))')


# ---------------------------------------------------------------------------
# B1 / C14 — h3_bounding_cell full rectangle
# ---------------------------------------------------------------------------


def test_r15i_h3_bounding_cell_multipoint_proves_rectangle_not_vertices() -> None:
    """C14: diagonal MultiPoint vertices must not under-prove the bbox region."""
    p1 = (-4.0263, 11.4529)
    p2 = (-13.6235, 6.2313)
    mp = gm.MultiPoint([p1, p2], crs=4326)
    bounds = [
        min(p1[0], p2[0]),
        min(p1[1], p2[1]),
        max(p1[0], p2[0]),
        max(p1[1], p2[1]),
    ]
    # Raw bounds path is the oracle for the rectangle contract.
    with pytest.raises(gm.GeometryError, match='no single H3 cell'):
        gm.h3_bounding_cell(bounds)
    with pytest.raises(gm.GeometryError, match='no single H3 cell'):
        gm.h3_bounding_cell(mp)
    # Array of the same two points must agree.
    arr = gm.GeometryArray([gm.Point(*p1, crs=4326), gm.Point(*p2, crs=4326)])
    with pytest.raises(gm.GeometryError, match='no single H3 cell'):
        gm.h3_bounding_cell(arr)


def test_r15i_h3_bounding_cell_seam_line_still_works() -> None:
    line = gm.LineString([(179.9, 0.0), (-179.9, 0.0)], crs=4326)
    cell = gm.h3_bounding_cell(line)
    poly = cell.polygon
    if poly.crs is None:
        poly = poly.set_crs(4326)
    assert gm.covers(poly, gm.Point(179.9, 0.0, crs=4326))
    assert gm.covers(poly, gm.Point(-179.9, 0.0, crs=4326))


# ---------------------------------------------------------------------------
# B2 / C15 — s2_bounding_cell one seam-aware owner
# ---------------------------------------------------------------------------


def test_r15i_s2_bounding_cell_array_matches_collection_seam_token_7() -> None:
    line1 = gm.LineString([(179.0, 0.0), (-179.0, 0.0)], crs=4326)
    line2 = gm.LineString([(179.0, 1.0), (-179.0, 1.0)], crs=4326)
    scalar_tokens = {gm.s2_bounding_cell(line1).token, gm.s2_bounding_cell(line2).token}
    assert scalar_tokens == {'7'}
    coll = gm.GeometryCollection([line1, line2])
    assert gm.s2_bounding_cell(coll).token == '7'
    arr = gm.GeometryArray([line1, line2])
    assert gm.s2_bounding_cell(arr).token == '7'


# ---------------------------------------------------------------------------
# B3 residual — cover superset + collinear invariance
# ---------------------------------------------------------------------------


def _cover_tokens(cover: Any) -> set[str]:
    return {c.token for c in cover}


def test_r15i_h3_s2_covered_point_tokens_subset_of_visible() -> None:
    """Covered-point tokens must be a subset of visible cover tokens."""
    corpus = [
        gm.LineString([(-50.0, -40.0), (50.0, -40.0)], crs=4326),  # long edge
        gm.LineString([(179.5, 0.0), (-179.5, 0.0)], crs=4326),  # seam
        gm.Polygon(
            [(-10.0, 80.0), (10.0, 80.0), (10.0, 85.0), (-10.0, 85.0), (-10.0, 80.0)],
            crs=4326,
        ),  # polar
        gm.LineString([(13.3, 52.4), (13.5, 52.6)], crs=4326),  # ordinary
    ]
    for geom in corpus:
        coords = gm.get_coordinates(geom)
        for res in (3, 5):
            h3 = gm.h3_cover(geom, resolution=res, cell_rule='overlap')
            visible = _cover_tokens(h3)
            for row in coords:
                lon, lat = float(row[0]), float(row[1])
                pt = gm.Point(lon, lat, crs=4326)
                if gm.covers(geom, pt):
                    cell = gm.H3Cell(lon, lat, resolution=res)
                    assert cell.token in visible, (
                        f'H3 r{res}: covered point {pt.to_wkt()} cell {cell.token} '
                        f'not in visible {sorted(visible)[:12]}...'
                    )
        for level in (4, 6):
            s2 = gm.s2_cover(geom, level=level, cell_rule='overlap')
            visible = _cover_tokens(s2)
            for row in coords:
                lon, lat = float(row[0]), float(row[1])
                pt = gm.Point(lon, lat, crs=4326)
                if gm.covers(geom, pt):
                    cell = gm.S2Cell(lon, lat, level=level)
                    assert cell.token in visible, (
                        f'S2 L{level}: covered point cell {cell.token} not in visible'
                    )


@pytest.mark.parametrize('case_name', ['ordinary', 'seam', 'full_world'])
def test_r15i_collinear_source_vertex_invariance_all_rules(case_name: str) -> None:
    # Exact affine collinearity is checked in the continuous source lift. The
    # seam and written full-world cases ensure a valid coalesce cannot erase
    # a sheet/direction identity while simplifying an ordinary midpoint.
    cases = [
        (
            'ordinary',
            gm.LineString([(10.0, 20.0), (30.0, 20.0)], crs=4326),
            gm.LineString([(10.0, 20.0), (20.0, 20.0), (30.0, 20.0)], crs=4326),
            (4, 6),
            (5, 8),
        ),
        (
            'seam',
            gm.LineString([(170.0, -10.0), (-170.0, 10.0)], crs=4326),
            gm.LineString([(170.0, -10.0), (180.0, 0.0), (-170.0, 10.0)], crs=4326),
            (4,),
            (5, 8),
        ),
        (
            'full_world',
            gm.LineString([(-180.0, 0.0), (180.0, 0.0)], crs=4326),
            gm.LineString([(-180.0, 0.0), (0.0, 0.0), (180.0, 0.0)], crs=4326),
            (1, 2),
            (2, 4),
        ),
    ]
    name, base, denser, h3_resolutions, s2_levels = next(
        case for case in cases if case[0] == case_name
    )
    for res in h3_resolutions:
        for rule in ('center', 'within', 'overlap', 'bbox'):
            a = _cover_tokens(gm.h3_cover(base, resolution=res, cell_rule=rule))
            b = _cover_tokens(gm.h3_cover(denser, resolution=res, cell_rule=rule))
            assert a == b, (
                f'{name} H3 r{res} {rule}: collinear vertex changed cells {a ^ b}'
            )
    for level in s2_levels:
        for rule in ('center', 'within', 'overlap', 'bbox'):
            a = _cover_tokens(gm.s2_cover(base, level=level, cell_rule=rule))
            b = _cover_tokens(gm.s2_cover(denser, level=level, cell_rule=rule))
            assert a == b, (
                f'{name} S2 L{level} {rule}: collinear vertex changed cells {a ^ b}'
            )


# ---------------------------------------------------------------------------
# C — multipart empty / mixed-axis admission
# ---------------------------------------------------------------------------


def test_r15i_multiline_xy_plus_z_empty_rejects_at_construction() -> None:
    """C17: typed empty members participate in axis homogeneity."""
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'MultiLineString members must share one coordinate axes',
    ):
        gm.MultiLineString([
            gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
            gm.from_wkt('LINESTRING Z EMPTY'),
        ])


def test_r15i_multiline_homogeneous_empty_still_round_trips() -> None:
    """XY + EMPTY and Z + Z-EMPTY remain legal and serialize cleanly."""
    xy = gm.MultiLineString([gm.LineString([(0.0, 0.0), (1.0, 1.0)]), gm.LineString()])
    assert xy.to_wkt() == 'MULTILINESTRING ((0 0, 1 1), EMPTY)'
    assert gm.from_wkb(xy.to_wkb()).to_wkt() == xy.to_wkt()
    assert pickle.loads(pickle.dumps(xy)).to_wkt() == xy.to_wkt()

    z = gm.from_wkt('MULTILINESTRING Z ((0 0 1, 1 1 2), EMPTY)')
    assert z.coordinate_axes == 'XYZ'
    assert gm.from_wkb(z.to_wkb()).to_wkt() == z.to_wkt()


def test_r15i_polygon_mixed_ring_axes_message_matches_writer() -> None:
    """Polygon constructor and serialize reject with the same message family."""
    shell = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]
    hole_z = [(0.2, 0.2, 1.0), (0.4, 0.2, 1.0), (0.4, 0.4, 1.0), (0.2, 0.2, 1.0)]
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'Polygon rings must share one coordinate axes layout',
    ):
        gm.Polygon(shell, holes=[hole_z])


def test_r15i_multipolygon_empty_members_dropped_documented() -> None:
    """STOP-AND-REPORT pin: MultiPolygon EMPTY members are dropped at ingress.

    ``Shape::MultiPolygon`` is ``Vec<Polygon>`` with no empty-slot representation
    (typed empties are ``Shape::Empty(EmptyKind::Polygon, axes)``). Preserving
    empty members would require a model change; keep the drop + axes on the
    remaining members, and pin that WKT drops them rather than inventing.
    """
    g = gm.from_wkt('MULTIPOLYGON (((0 0,1 0,1 1,0 0)), EMPTY)')
    assert g.to_wkt() == 'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))'
    # Constructor rejects empty polygon objects as members (not Polygon-shaped).
    with pytest.raises(TypeError, match='MultiPolygon expected Polygon'):
        gm.MultiPolygon([
            gm.Polygon([(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]),
            gm.from_wkt('POLYGON EMPTY'),
        ])
