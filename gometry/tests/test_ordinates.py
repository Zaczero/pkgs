"""Z/M preservation and fixed dimensional behavior across derived operations."""

import math

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


@pytest.mark.parametrize('method', ['set_z', 'set_m', 'force_3d'])
@pytest.mark.parametrize('value', [float('nan'), float('inf'), -float('inf')])
def test_ordinate_setters_reject_nonfinite_before_empty_or_noop(
    method: str, value: float
) -> None:
    geometries = [
        gm.Point(1, 2),
        gm.from_wkt('POINT Z (1 2 3)'),
        gm.from_wkt('POINT Z EMPTY'),
        gm.from_wkt('MULTIPOINT (EMPTY, (1 2))'),
    ]
    for geometry in geometries:
        with pytest.raises(gm.InvalidGeometryError):
            getattr(geometry, method)(value)


@pytest.mark.parametrize('method', ['set_z', 'set_m', 'force_3d'])
@pytest.mark.parametrize('value', [float('nan'), float('inf'), -float('inf')])
def test_array_ordinate_setters_reject_nonfinite_before_missing_or_noop(
    method: str, value: float
) -> None:
    arrays = [
        gm.GeometryArray([gm.Point(1, 2), None]),
        gm.GeometryArray([gm.Point(1, 2, z=3)]),
        gm.GeometryArray([gm.Point(1, 2), gm.Point(3, 4, z=5)]),
        gm.GeometryArray([]),
    ]
    for array in arrays:
        with pytest.raises(gm.InvalidGeometryError):
            getattr(array, method)(value)


def test_ordinate_updates_preserve_signed_zero_and_full_zm() -> None:
    source = gm.from_wkt('POINT ZM (-0 0 -0 0)')
    z_updated = next(iter(source.set_z(7.0).coords))
    m_updated = next(iter(source.set_m(8.0).coords))
    both_updated = next(iter(source.set_z(7.0).set_m(8.0).coords))

    assert z_updated == (-0.0, 0.0, 7.0, 0.0)
    assert m_updated == (-0.0, 0.0, -0.0, 8.0)
    assert both_updated == (-0.0, 0.0, 7.0, 8.0)
    assert math.copysign(1.0, z_updated[0]) == -1.0
    assert math.copysign(1.0, z_updated[1]) == 1.0
    assert math.copysign(1.0, m_updated[2]) == -1.0


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


def _origin_coord(geom: gm.Geometry) -> tuple[float, ...]:
    for coord in geom.coords:
        if coord[0] == 0.0 and coord[1] == 0.0:
            return coord
    raise AssertionError(f'no origin vertex on {geom.to_wkt()}')


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
def test_hull_copies_first_seen_z_at_duplicate_xy(op: str) -> None:
    points = gm.MultiPoint([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)], z=[10.0, 20.0, 30.0, 99.0])
    hull = getattr(points, op)()
    assert hull.coordinate_axes == 'XYZ'
    origin = _origin_coord(hull)
    assert origin == (0.0, 0.0, 10.0)
    assert math.copysign(1.0, origin[0]) == 1.0


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
def test_hull_copies_first_seen_m_at_duplicate_xy(op: str) -> None:
    points = gm.MultiPoint([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)], m=[1.0, 2.0, 3.0, 9.0])
    hull = getattr(points, op)()
    assert hull.coordinate_axes == 'XYM'
    assert _origin_coord(hull) == (0.0, 0.0, 1.0)


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
def test_hull_copies_first_seen_xyzm_when_z_and_m_conflict(op: str) -> None:
    points = gm.MultiPoint(
        [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)],
        z=[10.0, 20.0, 30.0, 99.0],
        m=[1.0, 2.0, 3.0, 8.0],
    )
    hull = getattr(points, op)()
    assert hull.coordinate_axes == 'XYZM'
    assert _origin_coord(hull) == (0.0, 0.0, 10.0, 1.0)


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
def test_hull_closing_vertex_conflict_keeps_first_seen(op: str) -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)], z=[10.0, 20.0, 30.0, 99.0])
    hull = getattr(line, op)()
    assert hull.coordinate_axes == 'XYZ'
    assert _origin_coord(hull) == (0.0, 0.0, 10.0)


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
def test_hull_all_coincident_keeps_first_seen_z(op: str) -> None:
    points = gm.MultiPoint([(1.0, 2.0), (1.0, 2.0), (1.0, 2.0)], z=[4.0, 5.0, 6.0])
    hull = getattr(points, op)()
    assert hull.geometry_type == 'Point'
    assert hull.coordinate_axes == 'XYZ'
    assert next(iter(hull.coords)) == (1.0, 2.0, 4.0)


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
def test_hull_collinear_keeps_endpoint_ordinates(op: str) -> None:
    points = gm.MultiPoint([(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)], z=[1.0, 2.0, 3.0])
    hull = getattr(points, op)()
    assert hull.geometry_type == 'LineString'
    assert hull.coordinate_axes == 'XYZ'
    assert list(hull.coords) == [(0.0, 0.0, 1.0), (2.0, 2.0, 3.0)]


def test_convex_hull_mixed_signed_zero_rectangle_drops_collinear_midpoint() -> None:
    cloud = gm.MultiPoint([(-0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 2.0), (-0.0, 2.0)])
    hull = cloud.convex_hull()
    open_coords = list(hull.exterior.coords)[:-1]
    assert len(open_coords) == 4
    assert (0.0, 1.0) not in {(coord[0], coord[1]) for coord in open_coords}
    origin = next(coord for coord in open_coords if coord[1] == 0.0 and coord[0] == 0.0)
    assert math.copysign(1.0, origin[0]) == 1.0
    top_left = next(coord for coord in open_coords if coord[1] == 2.0 and coord[0] == 0.0)
    assert math.copysign(1.0, top_left[0]) == -1.0


def test_concave_hull_uniform_zero_rectangle_keeps_collinear_edge_site() -> None:
    cloud = gm.MultiPoint(
        [(0.0, 0.0), (1.0, 0.0), (1.0, 2.0), (0.0, 2.0), (0.0, 1.0)],
        z=[10.0, 20.0, 30.0, 40.0, 50.0],
    )
    hull = cloud.concave_hull(concavity=10.0)
    assert hull.coordinate_axes == 'XYZ'
    assert (0.0, 1.0, 50.0) in list(hull.exterior.coords)


def test_concave_hull_mixed_signed_zero_rectangle_keeps_collinear_edge_site() -> None:
    cloud = gm.MultiPoint(
        [(-0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 2.0), (-0.0, 2.0)],
        z=[50.0, 10.0, 20.0, 30.0, 40.0],
        m=[5.0, 1.0, 2.0, 3.0, 4.0],
    )
    hull = cloud.concave_hull(concavity=10.0)
    assert hull.coordinate_axes == 'XYZM'
    edge = next(coord for coord in hull.exterior.coords if coord[1] == 1.0)
    assert edge[2] == 50.0
    assert edge[3] == 5.0
    assert math.copysign(1.0, edge[0]) == -1.0


def test_convex_hull_transposed_signed_zero_collinear_drops_midpoint() -> None:
    cloud = gm.MultiPoint([(0.0, -0.0), (0.0, 1.0), (2.0, 1.0), (2.0, 0.0), (1.0, -0.0)])
    hull = cloud.convex_hull()
    open_coords = list(hull.exterior.coords)[:-1]
    assert len(open_coords) == 4
    assert (1.0, 0.0) not in {(coord[0], coord[1]) for coord in open_coords}


def test_hull_mixed_signed_zero_keeps_first_seen_zm() -> None:
    cloud = gm.MultiPoint(
        [(-0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 2.0), (-0.0, 2.0)],
        z=[50.0, 10.0, 20.0, 30.0, 40.0],
        m=[5.0, 1.0, 2.0, 3.0, 4.0],
    )
    hull = cloud.convex_hull()
    assert hull.coordinate_axes == 'XYZM'
    origin = _origin_coord(hull)
    assert origin[2] == 10.0
    assert origin[3] == 1.0
    assert math.copysign(1.0, origin[0]) == 1.0


def test_minimum_rotated_rectangle_mixed_signed_zero_has_rectangle_area() -> None:
    cloud = gm.MultiPoint([(-0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 2.0), (-0.0, 2.0)])
    rectangle = cloud.minimum_rotated_rectangle()
    assert rectangle.area == pytest.approx(2.0)


def test_convex_hull_keeps_first_seen_signed_zero_spelling() -> None:
    points = gm.MultiPoint(
        [(0.0, 0.0), (-0.0, 0.0), (1.0, 0.0), (0.0, 1.0)],
        z=[10.0, 20.0, 30.0, 40.0],
    )
    hull = points.convex_hull()
    origin = _origin_coord(hull)
    assert hull.coordinate_axes == 'XYZ'
    assert origin[2] == 10.0
    assert math.copysign(1.0, origin[0]) == 1.0


def test_convex_hull_ignores_unselected_interior_ordinate_collision() -> None:
    points = gm.MultiPoint(
        [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (2.0, 1.0), (2.0, 1.0)],
        z=[1.0, 2.0, 3.0, 4.0, 5.0, 99.0],
    )
    hull = points.convex_hull()
    assert hull.coordinate_axes == 'XYZ'
    assert _origin_coord(hull) == (0.0, 0.0, 1.0)


def test_concave_hull_keeps_first_seen_on_selected_interior_collision() -> None:
    points = gm.MultiPoint(
        [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (2.0, 1.0), (2.0, 1.0)],
        z=[1.0, 2.0, 3.0, 4.0, 5.0, 99.0],
    )
    hull = points.concave_hull(concavity=1.0)
    assert hull.coordinate_axes == 'XYZ'
    interior = [coord for coord in hull.coords if coord[:2] == (2.0, 1.0)]
    assert interior == [(2.0, 1.0, 5.0)]


def test_hull_first_seen_ordinates_on_scalar_and_array() -> None:
    conflict = gm.MultiPoint([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)], z=[10.0, 20.0, 30.0, 99.0])
    unique = gm.MultiPoint([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)], z=[10.0, 20.0, 30.0])
    array = gm.GeometryArray([conflict, unique])
    convex = array.convex_hull()
    concave = array.concave_hull()
    for hulls, scalar in ((convex, conflict.convex_hull()), (concave, conflict.concave_hull())):
        assert hulls[0].coordinate_axes == 'XYZ'
        assert _origin_coord(hulls[0]) == (0.0, 0.0, 10.0)
        assert gm.equals_exact(hulls[0], scalar)
        assert hulls[1].coordinate_axes == 'XYZ'


def _run_hull(value: object, op: str, length_threshold: float = 0.0) -> object:
    if op == 'convex_hull':
        return getattr(value, op)()
    return getattr(value, op)(concavity=0.0, length_threshold=length_threshold)


def _mixed_selected_hull(vertex_count: int) -> gm.Geometry:
    if vertex_count == 2:
        points = [gm.Point(0.0, 0.0, z=10.0), gm.Point(1.0, 1.0)]
    else:
        points = [
            gm.Point(0.0, 0.0, z=10.0),
            gm.Point(1.0, 0.0),
            gm.Point(0.0, 1.0),
        ]
    return gm.GeometryCollection(points)


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
@pytest.mark.parametrize('vertex_count', [2, 3], ids=['two-selected', 'three-selected'])
def test_hull_rejects_mixed_selected_axes_scalar(op: str, vertex_count: int) -> None:
    with pytest.raises(gm.InvalidGeometryError):
        _run_hull(_mixed_selected_hull(vertex_count), op)


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
@pytest.mark.parametrize('vertex_count', [2, 3], ids=['two-selected', 'three-selected'])
def test_hull_rejects_mixed_selected_axes_array(op: str, vertex_count: int) -> None:
    values = gm.GeometryArray([_mixed_selected_hull(vertex_count)])
    with pytest.raises(gm.InvalidGeometryError) as error:
        _run_hull(values, op)
    assert error.value.__notes__ == ['while processing array element 0']


def _homogeneous_selected_mixed_interior() -> gm.Geometry:
    return gm.GeometryCollection([
        gm.Point(0.0, 0.0, z=10.0),
        gm.Point(4.0, 0.0, z=20.0),
        gm.Point(4.0, 4.0, z=30.0),
        gm.Point(0.0, 4.0, z=40.0),
        gm.Point(2.0, 2.0),
    ])


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
@pytest.mark.parametrize('as_array', [False, True], ids=['scalar', 'array'])
def test_hull_ignores_mixed_axes_on_unselected_interior(
    op: str, as_array: bool
) -> None:
    source = _homogeneous_selected_mixed_interior()
    value = gm.GeometryArray([source]) if as_array else source
    result = _run_hull(value, op, length_threshold=100.0)
    hull = result[0] if as_array else result
    expected = {
        (0.0, 0.0, 10.0),
        (4.0, 0.0, 20.0),
        (4.0, 4.0, 30.0),
        (0.0, 4.0, 40.0),
    }
    coordinates = list(hull.coords)
    assert hull.coordinate_axes == 'XYZ'
    assert coordinates[0] == coordinates[-1]
    assert set(coordinates[:-1]) == expected


@pytest.mark.parametrize('op', ['convex_hull', 'concave_hull'])
def test_hull_missing_rows_keep_alignment_without_nan_conversion(op: str) -> None:
    values = gm.GeometryArray([_homogeneous_selected_mixed_interior(), None])
    result = _run_hull(values, op, length_threshold=100.0)
    assert result.is_missing.tolist() == [False, True]
    assert result[1] is None
    assert result[0].coordinate_axes == 'XYZ'


def test_convex_hull_akl_filter_keeps_known_hull_vertices() -> None:
    corners = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0)]
    interior = [(1.0 + (index % 16) * 0.5, 1.0 + (index // 16) * 0.5) for index in range(256)]
    coords = corners + interior + [(0.0, 0.0)]
    z = [1.0, 2.0, 3.0, 4.0] + [100.0 + index for index in range(256)] + [99.0]
    hull = gm.MultiPoint(coords, z=z).convex_hull()
    assert hull.coordinate_axes == 'XYZ'
    selected = {(coord[0], coord[1], coord[2]) for coord in list(hull.exterior.coords)[:-1]}
    assert selected == {(0.0, 0.0, 1.0), (10.0, 0.0, 2.0), (10.0, 10.0, 3.0), (0.0, 10.0, 4.0)}
