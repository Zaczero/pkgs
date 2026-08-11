"""Declared-kind constant gates for crosses / overlaps / equals (R4-L1).

Locks the OGC algebra that must key off declared kind (`Point` vs
`MultiPoint`), never topological dimension alone — MultiPoint can cross an
area while a single Point never can. Also covers array / missing / prepared /
spatial-index surfaces and the mask-aware packed unary bool path.
"""

from __future__ import annotations

import gometry as gm
import pytest

from tests._support import bools


def _poly() -> gm.Polygon:
    return gm.Polygon([(0, 0), (1000, 0), (1000, 1000), (0, 1000), (0, 0)])


def _line_through() -> gm.LineString:
    return gm.LineString([(0, 500), (2000, 500)])


# ---------------------------------------------------------------------------
# Scalar mandatory corpus
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ('point',),
    [
        (gm.Point(500, 500),),  # interior
        (gm.Point(0, 0),),  # boundary
        (gm.Point(2000, 2000),),  # exterior
    ],
)
def test_point_area_crosses_and_overlaps_always_false(point: gm.Point) -> None:
    poly = _poly()
    assert gm.crosses(point, poly) is False
    assert gm.crosses(poly, point) is False
    assert gm.overlaps(point, poly) is False
    assert gm.overlaps(poly, point) is False


def test_multipoint_some_in_some_out_crosses_area() -> None:
    """The load-bearing counterexample: dim-only gates would wrongly constant-false."""
    poly = _poly()
    mp = gm.MultiPoint([(500, 500), (2000, 2000)])
    assert gm.crosses(mp, poly) is True
    assert gm.crosses(poly, mp) is True


def test_multipoint_all_in_does_not_cross_area() -> None:
    poly = _poly()
    mp = gm.MultiPoint([(100, 100), (200, 200), (300, 300)])
    assert gm.crosses(mp, poly) is False
    assert gm.crosses(poly, mp) is False


def test_point_point_equals_true_overlaps_crosses_false() -> None:
    a = gm.Point(1, 2)
    b = gm.Point(1, 2)
    assert gm.equals(a, b) is True
    assert gm.overlaps(a, b) is False
    assert gm.crosses(a, b) is False


def test_empty_empty_equals_across_kinds() -> None:
    empty_pt = gm.from_wkt('POINT EMPTY')
    empty_poly = gm.from_wkt('POLYGON EMPTY')
    assert gm.equals(empty_pt, empty_poly) is True
    assert gm.equals(empty_pt, empty_pt) is True
    assert gm.equals(empty_poly, empty_poly) is True


def test_empty_vs_nonempty_equals_false() -> None:
    empty_pt = gm.from_wkt('POINT EMPTY')
    empty_poly = gm.from_wkt('POLYGON EMPTY')
    poly = _poly()
    pt = gm.Point(1, 1)
    assert gm.equals(empty_pt, poly) is False
    assert gm.equals(poly, empty_pt) is False
    assert gm.equals(empty_poly, pt) is False
    assert gm.equals(pt, empty_poly) is False


def test_poly_poly_crosses_false_overlaps_real() -> None:
    a = _poly()
    b = gm.Polygon([(500, 500), (1500, 500), (1500, 1500), (500, 1500), (500, 500)])
    assert gm.crosses(a, b) is False
    assert gm.overlaps(a, b) is True


def test_line_line_crossing_and_collinear_overlap() -> None:
    a = gm.LineString([(0, 0), (10, 10)])
    crossing = gm.LineString([(0, 10), (10, 0)])
    collinear = gm.LineString([(5, 5), (15, 15)])
    assert gm.crosses(a, crossing) is True
    assert gm.overlaps(a, collinear) is True
    assert gm.crosses(a, collinear) is False


def test_geometry_collection_is_not_constant_gated() -> None:
    """GC either side must fall through; answers match the real path."""
    poly = _poly()
    gc = gm.GeometryCollection([gm.Point(500, 500), gm.LineString([(0, 0), (10, 10)])])
    # Mixed GC that truly crosses via a line part.
    gc_cross = gm.GeometryCollection([_line_through()])
    assert gm.crosses(gc_cross, poly) is True
    assert gm.equals(gc, poly) is False
    # Overlaps/crosses with a GC of only an interior point stay false.
    gc_pt = gm.GeometryCollection([gm.Point(500, 500)])
    assert gm.crosses(gc_pt, poly) is False
    assert gm.overlaps(gc_pt, poly) is False


# ---------------------------------------------------------------------------
# Array forms (scalarxarray, arrayxarray, missing)
# ---------------------------------------------------------------------------


def test_point_array_vs_polygon_crosses_overlaps_all_false() -> None:
    poly = _poly()
    pts = gm.GeometryArray([
        gm.Point(500, 500),
        gm.Point(0, 0),
        gm.Point(2000, 2000),
        None,
    ])
    assert bools(gm.crosses(pts, poly)) == [False, False, False, False]
    assert bools(gm.crosses(poly, pts)) == [False, False, False, False]
    assert bools(gm.overlaps(pts, poly)) == [False, False, False, False]
    assert bools(gm.overlaps(poly, pts)) == [False, False, False, False]


def test_multipoint_array_crosses_area_real_path() -> None:
    poly = _poly()
    arr = gm.GeometryArray([
        gm.MultiPoint([(500, 500), (2000, 2000)]),  # crosses
        gm.MultiPoint([(100, 100), (200, 200)]),  # all in
        None,
    ])
    assert bools(gm.crosses(arr, poly)) == [True, False, False]
    assert bools(gm.crosses(poly, arr)) == [True, False, False]


def test_poly_array_crosses_all_false_overlaps_pairwise() -> None:
    a = _poly()
    b = gm.Polygon([(500, 500), (1500, 500), (1500, 1500), (500, 1500), (500, 500)])
    c = gm.Polygon([
        (2000, 2000),
        (3000, 2000),
        (3000, 3000),
        (2000, 3000),
        (2000, 2000),
    ])
    left = gm.GeometryArray([a, a, None])
    right = gm.GeometryArray([b, c, b])
    assert bools(gm.crosses(left, right)) == [False, False, False]
    assert bools(gm.overlaps(left, right)) == [True, False, False]


def test_point_point_array_equals_and_false_predicates() -> None:
    left = gm.GeometryArray([gm.Point(1, 2), gm.Point(1, 2), None])
    right = gm.GeometryArray([gm.Point(1, 2), gm.Point(3, 4), gm.Point(1, 2)])
    assert bools(gm.equals(left, right)) == [True, False, False]
    assert bools(gm.crosses(left, right)) == [False, False, False]
    assert bools(gm.overlaps(left, right)) == [False, False, False]


def test_empty_array_equals() -> None:
    empties = gm.GeometryArray([
        gm.from_wkt('POINT EMPTY'),
        gm.from_wkt('POLYGON EMPTY'),
        None,
    ])
    other = gm.GeometryArray([
        gm.from_wkt('POLYGON EMPTY'),
        gm.from_wkt('POINT EMPTY'),
        gm.from_wkt('POINT EMPTY'),
    ])
    assert bools(gm.equals(empties, other)) == [True, True, False]


# ---------------------------------------------------------------------------
# Surfaces: free fn, PreparedGeometry, spatial index
# ---------------------------------------------------------------------------


def test_surfaces_share_point_area_constant_false() -> None:
    poly = _poly()
    pt = gm.Point(500, 500)
    pts = gm.GeometryArray([pt, gm.Point(2000, 2000)])

    # Free function (scalar + array)
    assert gm.crosses(pt, poly) is False
    assert bools(gm.crosses(pts, poly)) == [False, False]
    assert bools(gm.crosses(poly, pts)) == [False, False]

    # PreparedGeometry (shares scalar_vs_shapes)
    prepared = poly.prepare()
    assert prepared.crosses(pt) is False
    assert bools(prepared.crosses(pts)) == [False, False]

    # Spatial index refine
    idx = gm.SpatialIndex(pts)
    assert list(idx.query(poly, predicate='crosses')) == []


def test_surfaces_share_multipoint_area_true_crosses() -> None:
    poly = _poly()
    mp = gm.MultiPoint([(500, 500), (2000, 2000)])
    arr = gm.GeometryArray([mp])

    assert gm.crosses(mp, poly) is True
    assert bools(gm.crosses(arr, poly)) == [True]
    assert bools(gm.crosses(poly, arr)) == [True]
    assert poly.prepare().crosses(mp) is True
    assert bools(poly.prepare().crosses(arr)) == [True]

    idx = gm.SpatialIndex([mp])
    assert list(idx.query(poly, predicate='crosses')) == [0]


# ---------------------------------------------------------------------------
# Mask-aware packed unary bool (missing sentinel = false)
# ---------------------------------------------------------------------------


def test_nullable_is_valid_is_simple_match_dense_present_rows() -> None:
    dense = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1), gm.Point(2, 2)])
    nullable = gm.GeometryArray([gm.Point(0, 0), None, gm.Point(2, 2)])

    assert bools(dense.is_valid) == [True, True, True]
    assert bools(dense.is_simple) == [True, True, True]
    # Missing rows are the false sentinel; present rows match dense siblings.
    assert bools(nullable.is_valid) == [True, False, True]
    assert bools(nullable.is_simple) == [True, False, True]
