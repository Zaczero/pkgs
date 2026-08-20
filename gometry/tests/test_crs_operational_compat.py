"""Axis-order-agnostic operational CRS compatibility.

One rule decides whether two CRS labels name the same frame: PROJ equivalence
after visualization normalization (EPSG:4326 ↔ OGC:CRS84, N/E ↔ E/N projected,
UPS polar axes). Every lane uses it — predicates, metrics, overlay, LRS, and
the collection seams (array construction, concat, fill/replace, index build and
insert, ``require``) — and each seam keeps the receiver's or first item's label.

Structural identity is a different question and stays exact: ``CRS(4326) !=
CRS('OGC:CRS84')`` as objects, and geometry ``__eq__``/``equals_identical``
compare stored labels literally.

Counterexamples that share datum/ellipsoid/units but differ in the deriving
conversion must keep raising, at every one of those seams.
"""

from __future__ import annotations

import json

import gometry as gm
import pytest
from gometry import CRSMismatchError

# ---------------------------------------------------------------------------
# Positives — bidirectional operational compatibility
# ---------------------------------------------------------------------------


def test_crs84_4326_predicates_metrics_overlay_lrs_prepared() -> None:
    a = gm.Point(10.0, 50.0, crs=4326)
    b = gm.Point(10.0, 50.0, crs='OGC:CRS84')
    assert gm.equals(a, b) is True
    assert gm.equals(b, a) is True
    assert gm.distance(a, b) == 0.0
    assert gm.distance(b, a) == 0.0
    assert gm.dwithin(a, b, 1.0) is True

    poly_a = gm.box(0, 0, 2, 2, crs=4326)
    poly_b = gm.box(1, 1, 3, 3, crs='OGC:CRS84')
    inter = gm.intersection(poly_a, poly_b)
    assert inter.crs == 'EPSG:4326'  # left-biased label
    rev = gm.intersection(poly_b, poly_a)
    assert rev.crs == 'OGC:CRS84'
    assert gm.equals(inter, rev)  # coordinate result commutative

    line = gm.LineString([(0, 0), (1, 1)], crs=4326)
    pt = gm.Point(0.5, 0.5, crs='OGC:CRS84')
    # LRS across axis-order aliases must not raise CRSMismatchError.
    assert 0.0 < line.line_locate(pt, normalized=True) < 1.0

    prepared = poly_a.prepare()
    assert gm.contains(prepared, gm.Point(1, 1, crs='OGC:CRS84')) is True


def test_crs84h_4979_operational() -> None:
    a = gm.Point(10.0, 50.0, z=0.0, crs=4979)
    b = gm.Point(10.0, 50.0, z=0.0, crs='OGC:CRS84h')
    assert gm.equals(a, b) is True
    assert gm.distance(a, b) == 0.0


def test_same_as_ignore_axis_order_matches_operations() -> None:
    assert gm.CRS(4326).same_as('OGC:CRS84', mode='ignore_axis_order') is True
    assert gm.CRS('OGC:CRS84').same_as(4326, mode='ignore_axis_order') is True
    assert gm.CRS(4326).same_as('OGC:CRS84', mode='exact') is False
    # Object equality stays exact.
    assert gm.CRS(4326) != gm.CRS('OGC:CRS84')
    assert gm.CRS(4326) != 'OGC:CRS84'


def test_array_broadcast_and_index_query_accept_axis_aliases() -> None:
    arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)], crs=4326)
    other = gm.Point(0, 0, crs='OGC:CRS84')
    mask = gm.equals(arr, other)
    assert mask.tolist() == [True, False]

    idx = gm.SpatialIndex(arr)
    hits = idx.query(other, predicate='intersects')
    assert hits.tolist() == [0]
    nearest = idx.nearest(other)
    assert nearest.tolist() == [0]


def test_projected_axis_order_swap_operational() -> None:
    """Same projected definition with top-level N/E vs E/N axes is compatible."""
    # EPSG:2180 is N/E (northing, easting). Build an E/N twin via PROJJSON.
    base = json.loads(gm.CRS(2180).to_projjson())
    axes = base['coordinate_system']['axis']
    assert axes[0]['direction'] == 'north'
    assert axes[1]['direction'] == 'east'
    swapped = dict(base)
    swapped['coordinate_system'] = {
        **base['coordinate_system'],
        'axis': [axes[1], axes[0]],
    }
    # Drop authority id so PROJ keeps our axis order rather than re-identifying.
    swapped.pop('id', None)
    en_crs = gm.CRS(json.dumps(swapped))
    assert en_crs.axes[0]['direction'] == 'east'
    assert en_crs.axes[1]['direction'] == 'north'

    # gometry always stores lon/lat / (x,y) in storage order; both frames hold
    # the same projected easting/northing numbers under visualization order.
    p_ne = gm.Point(637000.0, 486000.0, crs=2180)
    p_en = gm.Point(637000.0, 486000.0, crs=en_crs)
    assert gm.CRS(2180).same_as(en_crs, mode='ignore_axis_order') is True
    assert gm.equals(p_ne, p_en) is True
    assert gm.distance(p_ne, p_en) == 0.0


def test_ups_polar_axes_operational() -> None:
    """UPS polar projected CRS with meridian-qualified axes stay self-compatible
    across the ignore-axis-order path (and accept an axis-normalized twin).
    """
    # EPSG:32661 = WGS 84 / UPS North (E,N). Compare against itself via
    # same_as and a geometry op after a round-trip through ignore_axis_order.
    ups = gm.CRS(32661)
    assert ups.same_as(32661, mode='ignore_axis_order') is True
    a = gm.Point(2_000_000.0, 2_000_000.0, crs=32661)
    b = gm.Point(2_000_000.0, 2_000_000.0, crs=32661)
    assert gm.equals(a, b) is True
    # Axis-normalized form (visualization) must still compare operationally.
    assert ups.same_as(ups, mode='ignore_axis_order') is True


def test_comparison_symmetry_and_cache_invalidation() -> None:
    left = 'EPSG:4326'
    right = 'OGC:CRS84'
    assert gm.CRS(left).same_as(right, mode='ignore_axis_order') is True
    assert gm.CRS(right).same_as(left, mode='ignore_axis_order') is True
    # Warm the comparison cache, then clear — results must remain correct.
    assert gm.equals(gm.Point(1, 2, crs=left), gm.Point(1, 2, crs=right))
    gm.crs_clear_cache()
    assert gm.equals(gm.Point(1, 2, crs=left), gm.Point(1, 2, crs=right))
    assert gm.CRS(left).same_as(right, mode='ignore_axis_order') is True


# ---------------------------------------------------------------------------
# Collection seams — one frame rule everywhere, the receiver's label wins
# ---------------------------------------------------------------------------


def test_collection_seams_accept_equivalent_crs_and_keep_the_receiver_label() -> None:
    """A collection admits exactly what its own operations already accept.

    These seams once demanded string-exact CRS because they select one stored
    label. They still select one — the receiver's, or the first item's — but a
    value naming the same frame under another spelling is admitted rather than
    refused. The practical effect of the old split was that an array could
    measure and compare a pair of geometries it then refused to hold.
    """
    arr = gm.GeometryArray([gm.Point(0, 0), None], crs=4326)
    alias = gm.Point(1, 1, crs='OGC:CRS84')

    filled = arr.fill_missing(alias)
    assert filled.crs == 'EPSG:4326'  # the column's label, not the fill's
    assert filled[1].to_wkt() == 'POINT (1 1)'

    replaced = arr._replace_at([0], [gm.Point(9, 9, crs='OGC:CRS84')])
    assert replaced.crs == 'EPSG:4326'
    assert replaced[0].to_wkt() == 'POINT (9 9)'

    joined = gm.GeometryArray([gm.Point(0, 0, crs=4326)]).concat(
        gm.GeometryArray([alias])
    )
    assert joined.crs == 'EPSG:4326'
    assert len(joined) == 2

    # Construction is first-biased, in both directions.
    assert gm.GeometryArray([alias, gm.Point(0, 0, crs=4326)]).crs == 'OGC:CRS84'
    assert gm.GeometryArray([gm.Point(0, 0, crs=4326), alias]).crs == 'EPSG:4326'
    # An explicit crs= is the requested output label.
    assert gm.GeometryArray([alias], crs=4326).crs == 'EPSG:4326'


def test_index_insert_and_query_both_accept_equivalent_crs() -> None:
    idx = gm.SpatialIndex([gm.Point(0, 0, crs=4326)])
    idx.insert(gm.Point(1, 1, crs='OGC:CRS84'))
    assert len(idx) == 2
    hits = idx.query(gm.Point(0, 0, crs='OGC:CRS84'), predicate='intersects')
    assert hits.tolist() == [0]


def test_require_retags_an_equivalent_label_to_the_requested_one() -> None:
    """``require(crs=)`` attaches rather than merely asserting, so a value
    already in the requested frame is relabelled to what the caller asked for.
    """
    g = gm.from_geojson('{"type":"Point","coordinates":[10,50]}')
    assert g.crs == 'OGC:CRS84'
    assert gm.require(g, crs=4326).crs == 'EPSG:4326'
    assert gm.require(gm.GeometryArray([g]), crs=4326).crs == 'EPSG:4326'


def test_equals_identical_and_object_eq_stay_exact() -> None:
    a = gm.Point(1, 2, crs=4326)
    b = gm.Point(1, 2, crs='OGC:CRS84')
    assert gm.equals(a, b) is True  # topological / operational
    assert gm.equals_identical(a, b) is False  # value identity includes CRS string
    assert (a == b) is False


def test_geojson_default_stays_crs84() -> None:
    g = gm.from_geojson('{"type":"Point","coordinates":[10,50]}')
    assert g.crs == 'OGC:CRS84'
    # And it interoperates with EPSG:4326 points operationally.
    assert gm.equals(g, gm.Point(10, 50, crs=4326)) is True


# ---------------------------------------------------------------------------
# Negatives — load-bearing counterexamples
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ('left', 'right'),
    [
        (4326, 4258),  # WGS84 vs ETRS89
        (2180, 2177),  # same datum, different projection (the checklist killer)
        (3857, 3395),  # Web Mercator vs World Mercator (~32 km Y)
        (4326, 4979),  # 2D vs 3D geographic
    ],
)
def test_required_negatives_keep_raising(left: int, right: int) -> None:
    a = gm.Point(1.0, 1.0, crs=left)
    b = gm.Point(1.0, 1.0, crs=right)
    with pytest.raises(CRSMismatchError):
        gm.equals(a, b)
    with pytest.raises(CRSMismatchError):
        gm.distance(a, b)
    assert gm.CRS(left).same_as(right, mode='ignore_axis_order') is False


@pytest.mark.parametrize(
    ('left', 'right'),
    [
        (4326, 4258),
        (2180, 2177),
        (3857, 3395),
        (4326, 4979),
    ],
)
def test_collection_seams_keep_rejecting_the_negatives(left: int, right: int) -> None:
    """Relaxing WHERE the rule applies must not relax WHAT it admits.

    Every seam that now accepts an axis-order alias must still refuse these
    pairs — each would be unified by an attribute checklist (same datum,
    ellipsoid, prime meridian, units, dimension) yet places the same raw
    coordinate somewhere entirely different.
    """
    a = gm.Point(1.0, 1.0, crs=left)
    b = gm.Point(1.0, 1.0, crs=right)
    with pytest.raises(CRSMismatchError):
        gm.GeometryArray([a, b])
    with pytest.raises(CRSMismatchError):
        gm.GeometryArray([a]).concat(gm.GeometryArray([b]))
    with pytest.raises(CRSMismatchError):
        gm.GeometryArray([a, None]).fill_missing(b)
    with pytest.raises(CRSMismatchError):
        gm.GeometryArray([a, None])._replace_at([0], [b])
    with pytest.raises(CRSMismatchError):
        gm.SpatialIndex([a]).insert(b)
    with pytest.raises(CRSMismatchError):
        gm.require(a, crs=right)
    with pytest.raises(CRSMismatchError):
        gm.GeometryCollection([a, b])


def test_mixed_epoch_and_presence_still_raise() -> None:
    a = gm.Point(1, 2, crs=4326).set_epoch(2020.0)
    b = gm.Point(1, 2, crs='OGC:CRS84').set_epoch(2021.0)
    with pytest.raises(CRSMismatchError):
        gm.equals(a, b)
    bare = gm.Point(1, 2)
    tagged = gm.Point(1, 2, crs=4326)
    with pytest.raises(CRSMismatchError):
        gm.equals(bare, tagged)


def test_west_positive_and_radian_axes_incompatible() -> None:
    # West-positive longitude is a substantive direction change, not mere order.
    west = (
        'GEOGCS["WGS 84 west",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],'
        'AXIS["Longitude",WEST],AXIS["Latitude",NORTH]]'
    )
    with pytest.raises(CRSMismatchError):
        gm.equals(gm.Point(1, 0.5, crs=4326), gm.Point(1, 0.5, crs=west))
    assert gm.CRS(4326).same_as(west, mode='ignore_axis_order') is False

    # Radian axes reinterpret the numbers — must stay incompatible.
    rad = (
        'GEOGCS["WGS 84 rad",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0],UNIT["radian",1],'
        'AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]'
    )
    with pytest.raises(CRSMismatchError):
        gm.equals(gm.Point(1, 0.5, crs=4326), gm.Point(1, 0.5, crs=rad))
    assert gm.CRS(4326).same_as(rad, mode='ignore_axis_order') is False


def test_ewkb_identity_loss_and_alias_restore() -> None:
    g = gm.Point(10, 50, crs='OGC:CRS84')
    ewkb = g.to_wkb(include_srid=True)
    lost = gm.from_wkb(ewkb)
    assert lost.crs == 'EPSG:4326'
    restored = gm.from_wkb(ewkb, crs='OGC:CRS84')
    assert restored.crs == 'OGC:CRS84'
    assert gm.equals(restored, g) is True
    with pytest.raises(CRSMismatchError):
        gm.from_wkb(ewkb, crs=3857)
