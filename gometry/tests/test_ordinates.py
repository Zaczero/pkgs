"""Z/M preservation and fixed dimensional behavior across derived operations."""

import gometry as gm
import pytest


def test_derived_and_preserving_operation_axes() -> None:
    line_z = gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)], z=[0.0, 1.0, 2.0])
    line_xy = gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 0.0)])
    assert (line_z).buffer(1.0).coordinate_axes == 'XY'
    assert (gm.GeometryArray([line_z])).buffer(1.0)[0].coordinate_axes == 'XY'
    assert (line_z).simplify(0.1).coordinate_axes == 'XYZ'
    assert (line_z).convex_hull().coordinate_axes == 'XYZ'
    assert (line_z).concave_hull().coordinate_axes == 'XYZ'
    assert (gm.GeometryArray([line_z])).simplify(0.1)[0].coordinate_axes == 'XYZ'
    kept = (line_z).simplify(0.1)
    assert next(iter(kept.coords)) == (0.0, 0.0, 0.0)
    assert (line_xy).buffer(1.0).coordinate_axes == 'XY'
    assert (line_xy).simplify(0.1).coordinate_axes == 'XY'


def test_clip_by_rect_resolves_zm_where_derivable() -> None:
    """clip_by_rect restores Z/M on kept vertices and interpolates boundary
    crossings; only a clip-rectangle corner entering the output (underivable)
    trips the default policy.
    """
    line_z = gm.from_wkt('LINESTRING Z (0 0 1, 1 0.001 2, 2 0 3)')
    clipped = (line_z).clip_by_rect(0.5, -1.0, 3.0, 1.0)
    assert clipped.coordinate_axes == 'XYZ'
    assert next(iter(clipped.coords)) == (0.5, 0.0005, 1.5)
    z_polygon = _z_polygon()
    dropped = (z_polygon).clip_by_rect(1, 1, 3, 3)
    assert dropped.coordinate_axes == 'XY'
    derivable = (z_polygon).clip_by_rect(-1, -1, 2, 5)
    assert derivable.coordinate_axes == 'XYZ'


_ZSQUARE_WKT = 'POLYGON Z ((0 0 1, 4 0 2, 4 4 3, 0 4 4, 0 0 1))'
_XYSQUARE_WKT = 'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))'


def _z_polygon() -> gm.Geometry:
    return gm.from_wkt(_ZSQUARE_WKT)


def _xy_polygon() -> gm.Geometry:
    return gm.from_wkt(_XYSQUARE_WKT)


@pytest.mark.parametrize(
    'op', ['centroid', 'envelope', 'minimum_rotated_rectangle', 'polylabel']
)
def test_computed_point_ops_are_2d(op: str) -> None:
    """Invented vertices have no source Z/M, so computed results are 2D."""
    zsquare = _z_polygon()
    assert zsquare.coordinate_axes == 'XYZ'
    assert getattr(zsquare, op)().coordinate_axes == 'XY'
    assert getattr(_xy_polygon(), op)().coordinate_axes == 'XY'


@pytest.mark.parametrize('op', ['voronoi_polygons', 'voronoi_edges'])
def test_voronoi_results_are_2d(op: str) -> None:
    """Voronoi cells are synthesized and therefore have no source Z/M."""
    zsquare = _z_polygon()
    assert all(part.coordinate_axes == 'XY' for part in getattr(zsquare, op)())
    assert getattr(_xy_polygon(), op)() is not None


@pytest.mark.parametrize('method', ['delaunay', 'constrained', 'earcut'])
def test_triangulation_preserves_ordinates(method: str) -> None:
    """Triangle corners coincide with input vertices, so Z/M is always
    preserved — there is no ordinate policy to configure.
    """
    zsquare = _z_polygon()
    carried = zsquare.triangulate(method=method)
    assert carried
    assert all(part.coordinate_axes == 'XYZ' for part in carried)
    assert _xy_polygon().triangulate(method=method) is not None


def test_point_on_surface_is_interior_xy_point() -> None:
    """point_on_surface returns an interior computed point, not a boundary vertex.

    The representative point does not coincide with an input vertex, so (like
    centroid) Z/M cannot be carried, so it is 2D.
    """
    assert (_z_polygon()).point_on_surface().coordinate_axes == 'XY'
    result = (_z_polygon()).point_on_surface()
    assert result.coordinate_axes == 'XY'
    assert gm.within(result, _xy_polygon())
    assert (result).to_wkt() == 'POINT (2 2)'
    assert ((_xy_polygon()).point_on_surface()).to_wkt() == 'POINT (2 2)'


def test_witness_lines_keep_only_shared_ordinates() -> None:
    """A witness line's axes are the INTERSECTION of its endpoints' axes.

    Witness kernels mix vertex copies, interpolated feet, and computed
    centers; a sequence is axis-homogeneous, so the pair keeps Z/M only
    when BOTH sides resolve them. A 2D side never gets fabricated zeros —
    a mixed pair once tripped the debug mixed-axes assert and silently
    invented ``Z 0`` ordinates in release builds.
    """
    lifted = gm.from_wkt('POINT Z (0 0 5)')
    flat = gm.Point(3, 4)
    line = gm.shortest_line(lifted, flat)
    assert (line).to_wkt() == 'LINESTRING (0 0, 3 4)'
    assert not line.has_z
    start, end = gm.nearest_points(lifted, flat)
    assert ((start).to_wkt(), (end).to_wkt()) == ('POINT (0 0)', 'POINT (3 4)')
    zline = gm.from_wkt('LINESTRING Z (0 0 0, 4 0 8)')
    zpoint = gm.from_wkt('POINT Z (1 3 7)')
    assert (gm.shortest_line(zline, zpoint)).to_wkt() == 'LINESTRING Z (1 0 2, 1 3 7)'
    poly = gm.from_wkt('POLYGON Z ((0 0 1, 4 0 1, 4 4 1, 0 4 1, 0 0 1))')
    for witness in (
        (poly).minimum_bounding_circle(),
        (poly).maximum_inscribed_circle(),
    ):
        assert not witness.has_z
        assert not witness.has_m
    assert ((poly).minimum_clearance_line()).to_wkt() == 'LINESTRING Z (0 0 1, 4 0 1)'
    geo_lifted = gm.from_wkt('POINT Z (0 0 5)', crs=4326)
    geo_flat = gm.Point(1, 0, crs=4326)
    assert not gm.shortest_line(geo_lifted, geo_flat).has_z


def test_mixed_zm_overlay_axes_follow_operands() -> None:
    xyz = gm.from_wkt('POLYGON Z ((0 0 1, 4 0 2, 4 4 3, 0 4 4, 0 0 1))')
    xym = gm.from_wkt('POLYGON M ((1 1 10, 3 1 20, 3 3 30, 1 3 40, 1 1 10))')
    assert gm.union(xyz, xym).coordinate_axes == 'XYZ'
    assert gm.intersection(xyz, xym).coordinate_axes == 'XYM'


def test_xyzm_m_linear_referencing_on_measured_line() -> None:
    route = gm.from_wkt('LINESTRING ZM (0 0 0 0, 10 0 0 100)')
    interpolated = route.line_interpolate(50.0, basis='m')
    assert (interpolated).to_wkt() == 'POINT ZM (5 0 0 50)'
    assert route.line_locate(gm.Point(5, 0), basis='m') == pytest.approx(50.0)
    substring = route.line_substring(20.0, 80.0, basis='m')
    assert (substring).to_wkt() == 'LINESTRING ZM (2 0 0 20, 8 0 0 80)'


_EMPTY_TYPES = [
    'POINT',
    'LINESTRING',
    'POLYGON',
    'MULTIPOINT',
    'MULTILINESTRING',
    'MULTIPOLYGON',
    'GEOMETRYCOLLECTION',
]


def test_force_3d_on_empties_has_uniform_dimension() -> None:
    """Axes are real state on empties: force_3d tags Z on every empty kind
    (uniformly), and force_2d flattens back to the XY empty.
    """
    for t in _EMPTY_TYPES:
        lifted = gm.from_wkt(f'{t} EMPTY').force_3d()
        assert lifted.has_z, t
        assert lifted.to_wkt() == f'{t} Z EMPTY'
        assert not lifted.force_2d().has_z, t
        assert lifted.force_2d() == gm.from_wkt(f'{t} EMPTY'), t
    assert (gm.LineString([(0, 0), (1, 1)])).force_3d().has_z


def test_set_z_and_set_m_retag_empties() -> None:
    """set_z/set_m on an empty flip the declared axes (no vertices to fill);
    ``None`` clears the ordinate. Collections retag nested empties too.
    """
    for t in _EMPTY_TYPES:
        empty = gm.from_wkt(f'{t} EMPTY')
        assert empty.set_z(5.0).coordinate_axes == 'XYZ', t
        assert empty.set_m(7.0).coordinate_axes == 'XYM', t
        assert empty.set_z(5.0).set_m(7.0).coordinate_axes == 'XYZM', t
        assert empty.set_z(5.0).set_z(None).coordinate_axes == 'XY', t
        assert empty.set_z(5.0).is_empty, t
    nested = gm.from_wkt('GEOMETRYCOLLECTION (POINT EMPTY)').force_3d()
    assert nested.to_wkt() == 'GEOMETRYCOLLECTION (POINT Z EMPTY)'


def test_empty_equality_is_axes_sensitive() -> None:
    """Value equality compares the declared axes of empties (mirroring
    equals_identical): an XY empty and a Z empty are distinct values with
    consistent hashes; topological equals() still treats them as equal.
    """
    for t in _EMPTY_TYPES:
        xy = gm.from_wkt(f'{t} EMPTY')
        z = gm.from_wkt(f'{t} Z EMPTY')
        assert xy != z, t
        assert z == gm.from_wkt(f'{t} Z EMPTY'), t
        assert hash(z) == hash(gm.from_wkt(f'{t} Z EMPTY')), t
        assert gm.equals(xy, z), t
        assert not gm.equals_exact(xy, z), t
        assert gm.equals_exact(xy, z, include_z=False), t
    assert gm.Point() == gm.from_wkt('POINT EMPTY')


def test_empty_polygon_exterior_carries_axes() -> None:
    assert gm.from_wkt('POLYGON Z EMPTY').exterior.coordinate_axes == 'XYZ'
    assert gm.from_wkt('POLYGON EMPTY').exterior.coordinate_axes == 'XY'


def test_dimensional_empties_behave_like_their_xy_siblings() -> None:
    """Beyond the carried axes, a Z-tagged empty is behaviorally identical to
    its XY sibling: same predicates, relate matrix, and overlay results.
    """
    probe = gm.Point(0, 0)
    for t in _EMPTY_TYPES:
        xy = gm.from_wkt(f'{t} EMPTY')
        z = gm.from_wkt(f'{t} Z EMPTY')
        for prop in ('is_valid', 'is_simple', 'is_closed', 'is_empty'):
            assert getattr(xy, prop) == getattr(z, prop), (t, prop)
        assert gm.relate(xy, probe) == gm.relate(z, probe), t
        assert gm.union(xy, probe) == gm.union(z, probe), t
        assert xy.buffer(1.0) == z.buffer(1.0), t


def test_polygon_centroid_normalizes_negative_zero() -> None:
    import math

    import gometry as gm

    x, y = next(iter((gm.box(-1, -1, 1, 1)).centroid().coords))
    assert math.copysign(1.0, x) == 1.0 and math.copysign(1.0, y) == 1.0
