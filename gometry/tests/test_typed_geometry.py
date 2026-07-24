"""Typed geometry hierarchy: subclasses, precise returns, value semantics,
part accessors, and the GeometryArray sequence protocol.
"""

from __future__ import annotations

import gometry as gm
import pytest

LEAVES = [
    gm.Point,
    gm.MultiPoint,
    gm.LineString,
    gm.MultiLineString,
    gm.Polygon,
    gm.MultiPolygon,
    gm.GeometryCollection,
]


@pytest.mark.parametrize('leaf', LEAVES)
def test_leaves_are_geometry_subclasses(leaf: type) -> None:
    assert issubclass(leaf, gm.Geometry)
    assert leaf is not gm.Geometry


def test_scalar_factories_are_removed() -> None:
    for name in (
        'point',
        'lonlat',
        'line_string',
        'polygon',
        'multi_point',
        'multi_line_string',
        'multi_polygon',
        'geometry_collection',
    ):
        assert name not in gm.__all__
        assert not hasattr(gm, name)


def test_points_scalar_args_direct_to_point() -> None:
    with pytest.raises(gm.GeometryError, match='use Point\\(x, y\\)'):
        gm.points(1, 2)


def test_geometry_base_is_not_constructible() -> None:
    with pytest.raises(TypeError, match='abstract'):
        gm.Geometry()


def test_constructors_return_typed_instances() -> None:
    assert type(gm.Point(1, 2)) is gm.Point
    assert type(gm.Point(1, 2, z=3, m=4)) is gm.Point
    assert type(gm.Point(1, 2, crs=4326)) is gm.Point
    assert type(gm.LineString([(0, 0), (1, 1)])) is gm.LineString
    assert type(gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])) is gm.Polygon
    assert type(gm.box(0, 0, 1, 1)) is gm.Polygon
    assert type(gm.MultiPoint([(0, 0), (1, 1)])) is gm.MultiPoint
    assert type(gm.GeometryCollection([gm.Point(0, 0)])) is gm.GeometryCollection
    assert isinstance(gm.Point(1, 2), gm.Point)


def test_derived_geometries_are_typed() -> None:
    poly = gm.box(0, 0, 4, 4)
    assert type(poly.centroid()) is gm.Point
    assert type(poly.point_on_surface()) is gm.Point
    assert isinstance(poly.buffer(1.0), (gm.Polygon, gm.MultiPolygon))
    assert type(gm.from_wkt('LINESTRING(0 0, 1 1)')) is gm.LineString
    assert type(gm.from_wkb(poly.to_wkb())) is gm.Polygon
    left, right = gm.nearest_points(poly, gm.Point(9, 9))
    assert type(left) is gm.Point and type(right) is gm.Point


def test_point_ordinate_accessors() -> None:
    p = gm.Point(1.0, 2.0, z=3.0, m=4.0)
    assert (p.x, p.y, p.z, p.m) == (1.0, 2.0, 3.0, 4.0)


def test_point_only_members_absent_on_others() -> None:
    line = gm.LineString([(0, 0), (1, 1)])
    with pytest.raises(AttributeError):
        _ = line.x
    with pytest.raises(TypeError):
        _ = gm.Point(1, 2).z


def test_polygon_exterior_and_interiors() -> None:
    poly = gm.from_wkt('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))')
    assert isinstance(poly, gm.Polygon)
    assert type(poly.exterior) is gm.LineString
    assert poly.exterior.coords[0] == (0.0, 0.0)
    assert len(poly.interiors) == 1
    assert all(type(ring) is gm.LineString for ring in poly.interiors)
    assert gm.box(0, 0, 1, 1).interiors == []


def test_multipart_geoms_and_sequence_protocol() -> None:
    point = gm.Point(1, 2)
    assert [part.to_wkt() for part in point.parts] == ['POINT (1 2)']
    assert [part.to_wkt() for part in point.parts] == [
        part.to_wkt() for part in gm.parts(point)
    ]
    mp = gm.from_wkt('MULTIPOINT(0 0, 1 1, 2 2)')
    assert isinstance(mp, gm.MultiPoint)
    parts = mp.parts
    assert type(parts).__name__ == 'GeometryParts'
    assert not isinstance(parts, list)
    assert len(parts) == 3
    assert [type(g) for g in parts] == [gm.Point] * 3
    assert len(mp) == 3
    assert mp[1].x == 1.0
    assert mp[-1].y == 2.0
    assert [g.x for g in mp] == [0.0, 1.0, 2.0]
    iterator = iter(parts)
    assert type(iterator).__name__ == 'GeometryPartsIterator'
    assert iterator.__length_hint__() == 3
    assert [g.x for g in iterator] == [0.0, 1.0, 2.0]
    assert list(iter(mp)) == list(parts)
    with pytest.raises(IndexError):
        _ = mp[5]
    with pytest.raises(IndexError):
        _ = mp[-4]


def test_vertex_traversal_recurses_nested_collections() -> None:
    nested = gm.from_wkt(
        'GEOMETRYCOLLECTION (POINT (0 0), GEOMETRYCOLLECTION (LINESTRING Z (0 0 4, 1 1 9), POINT (2 2)))'
    )
    assert nested.has_z is True
    assert nested.has_m is False
    assert nested.coordinate_axes == 'XYZ'
    assert nested.min_z == 4.0
    assert nested.max_z == 9.0
    flat = gm.from_wkt('MULTIPOINT (0 0, 1 1)')
    assert flat.has_z is False
    assert flat.coordinate_axes == 'XY'
    assert flat.min_z is None


def test_structural_equality_and_hash() -> None:
    a = gm.LineString([(0, 0), (1, 1)])
    b = gm.LineString([(0, 0), (1, 1)])
    reversed_ = gm.LineString([(1, 1), (0, 0)])
    assert a == b
    assert a != reversed_
    assert a != gm.Point(0, 0)
    assert (a == 5) is False
    assert hash(a) == hash(b)
    assert len({a, b, reversed_}) == 2
    assert {a: 'x'}[b] == 'x'


def test_equality_considers_crs_and_zm() -> None:
    assert gm.Point(1, 2, crs=3857) != gm.Point(1, 2, crs=4326)
    assert gm.Point(1, 2, z=3) != gm.Point(1, 2)


def test_geometry_array_slicing_and_typed_elements() -> None:
    arr = gm.GeometryArray([gm.Point(i, i) for i in range(5)])
    assert type(arr[0]) is gm.Point
    sliced = arr[1:4]
    assert type(sliced) is gm.GeometryArray
    assert len(sliced) == 3
    assert [g.x for g in arr[::2]] == [0.0, 2.0, 4.0]
    assert [type(g) for g in list(arr)] == [gm.Point] * 5


def test_orient_polygons_ccw() -> None:
    cw = gm.from_wkt('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')
    assert cw.orient_polygons().to_wkt() == 'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'
    assert cw.orient_polygons(ccw=False).to_wkt() == cw.to_wkt()


def test_empty_polygon_shell_kwarg_spelling() -> None:
    """Unique empty-Polygon spelling: shell=[] with CRS (matrices cover bare ctors)."""
    assert gm.Polygon(shell=[], crs=4326).to_wkt() == 'POLYGON EMPTY'
    assert gm.Polygon(shell=[], crs=4326).crs == 'EPSG:4326'


_EMPTY_CTORS = [
    (gm.Point, 'Point', 'POINT EMPTY'),
    (gm.LineString, 'LineString', 'LINESTRING EMPTY'),
    (gm.Polygon, 'Polygon', 'POLYGON EMPTY'),
    (gm.MultiPoint, 'MultiPoint', 'MULTIPOINT EMPTY'),
    (gm.MultiLineString, 'MultiLineString', 'MULTILINESTRING EMPTY'),
    (gm.MultiPolygon, 'MultiPolygon', 'MULTIPOLYGON EMPTY'),
    (gm.GeometryCollection, 'GeometryCollection', 'GEOMETRYCOLLECTION EMPTY'),
]


@pytest.mark.parametrize(('ctor', 'gtype', 'wkt'), _EMPTY_CTORS)
def test_every_class_no_args_builds_its_typed_empty(
    ctor: type, gtype: str, wkt: str
) -> None:
    geom = ctor()
    assert type(geom) is ctor
    assert geom.geometry_type == gtype
    assert geom.is_empty
    assert geom.to_wkt() == wkt
    assert gm.from_wkt(wkt).to_wkt() == wkt
    assert gm.equals(geom, gm.from_wkt(wkt))


@pytest.mark.parametrize(('ctor', 'gtype', 'wkt'), _EMPTY_CTORS)
def test_empty_constructors_carry_crs_and_epoch(
    ctor: type, gtype: str, wkt: str
) -> None:
    geom = ctor(crs=4326, epoch=2020.0)
    assert geom.geometry_type == gtype
    assert geom.is_empty
    assert geom.crs == 'EPSG:4326'
    assert geom.epoch == pytest.approx(2020.0)


def test_empty_collection_constructors_wkb_round_trip() -> None:
    for wkt in (
        'LINESTRING EMPTY',
        'MULTIPOINT EMPTY',
        'MULTILINESTRING EMPTY',
        'MULTIPOLYGON EMPTY',
        'GEOMETRYCOLLECTION EMPTY',
    ):
        geom = gm.from_wkt(wkt)
        assert gm.from_wkb(geom.to_wkb()).to_wkt() == wkt


def test_empty_constructor_argument_errors() -> None:
    with pytest.raises(ValueError):
        gm.Point(1)
    with pytest.raises(ValueError):
        gm.Point(z=3)
    with pytest.raises(ValueError):
        gm.Polygon([], holes=[[(0, 0), (1, 0), (1, 1)]])


@pytest.mark.parametrize('wkt', ['POINT EMPTY', 'POLYGON EMPTY'])
def test_empty_format_round_trips(wkt: str) -> None:
    geom = gm.from_wkt(wkt)
    assert geom.to_wkt() == wkt
    assert gm.from_wkb(geom.to_wkb()).to_wkt() == wkt
    assert gm.from_geojson(geom.__geo_interface__).to_wkt() == wkt


def test_empty_wkb_matches_ogc_convention() -> None:
    assert gm.Point().to_wkb().hex() == '0101000000000000000000f87f000000000000f87f'
    assert gm.Polygon().to_wkb().hex() == '010300000000000000'


def test_empty_geojson_coordinates_are_empty() -> None:
    assert gm.Point().__geo_interface__ == {'type': 'Point', 'coordinates': []}
    assert gm.Polygon().__geo_interface__ == {'type': 'Polygon', 'coordinates': []}


def test_empty_point_ordinate_access_raises() -> None:
    # AttributeError so `match Point(x, y)` patterns FAIL per the match
    # protocol instead of raising mid-match.
    pe = gm.Point()
    for attr in ('x', 'y', 'z', 'm'):
        with pytest.raises(AttributeError, match='empty point'):
            getattr(pe, attr)
    assert len(pe.coords) == 0


def test_empty_predicates_are_all_false_against_a_point() -> None:
    pe = gm.Point()
    other = gm.Point(0, 0)
    for predicate in (
        'intersects',
        'contains',
        'within',
        'covers',
        'covered_by',
        'touches',
        'crosses',
        'overlaps',
    ):
        assert getattr(gm, predicate)(pe, other) is False
    assert gm.disjoint(pe, other) is True


def test_empty_equality_semantics() -> None:
    pe, pe2 = (gm.Point(), gm.from_wkt('POINT EMPTY'))
    pg = gm.Polygon()
    assert gm.equals(pe, pe2) is True
    assert gm.equals(pe, pg) is True
    assert gm.equals_exact(pe, pe2) is True
    assert gm.equals_exact(pe, pg) is False
    assert pe == pe2
    assert pe != pg
    assert hash(pe) == hash(pe2)


def test_empty_metrics_and_accessors() -> None:
    pe, pg = (gm.Point(), gm.Polygon())
    assert pg.area == 0.0
    assert pg.length == 0.0
    assert pe.bounds is None
    assert pg.bounds is None
    assert pg.exterior.to_wkt() == 'LINESTRING EMPTY'
    assert list(gm.rings(pg)) == []
    assert pe.centroid().to_wkt() == 'POINT EMPTY'
    assert pg.centroid().to_wkt() == 'POINT EMPTY'
    assert pe.buffer(1).to_wkt() == 'POLYGON EMPTY'
    assert pg.convex_hull().to_wkt() == 'GEOMETRYCOLLECTION EMPTY'


def test_empty_overlay_short_circuits() -> None:
    pe = gm.Point()
    other = gm.Point(0, 0)
    assert gm.equals(gm.union(pe, other), other)
    assert gm.intersection(pe, other).is_empty
    assert gm.difference(pe, other).is_empty
    assert gm.equals(gm.difference(other, pe), other)


def test_empty_overlay_is_dimension_typed() -> None:
    poly = gm.from_wkt('POLYGON((0 0,4 0,4 4,0 4,0 0))')
    poly_far = gm.from_wkt('POLYGON((9 9,10 9,10 10,9 10,9 9))')
    poly_in = gm.from_wkt('POLYGON((1 1,3 1,3 3,1 3,1 1))')
    line = gm.from_wkt('LINESTRING(5 5,6 6)')
    point = gm.from_wkt('POINT(9 9)')
    assert gm.intersection(poly, poly_far).to_wkt() == 'POLYGON EMPTY'
    assert gm.intersection(line, poly).to_wkt() == 'LINESTRING EMPTY'
    assert gm.intersection(point, poly).to_wkt() == 'POINT EMPTY'
    assert gm.difference(poly_in, poly).to_wkt() == 'POLYGON EMPTY'
    assert gm.symmetric_difference(poly, poly).to_wkt() == 'POLYGON EMPTY'
    assert gm.symmetric_difference(line, line).to_wkt() == 'LINESTRING EMPTY'
    assert gm.union(gm.Polygon(), gm.Polygon()).to_wkt() == 'POLYGON EMPTY'
    left = gm.GeometryArray([poly, poly])
    right = gm.GeometryArray([poly_in, poly_far])
    out = gm.intersection(left, right)
    assert [out[i].geometry_type for i in range(2)] == ['Polygon', 'Polygon']
    assert out[1].to_wkt() == 'POLYGON EMPTY'
