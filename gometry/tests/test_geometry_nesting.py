"""Geometry collection nesting is bounded at the Shape admission owner."""

from __future__ import annotations

import json
import math
import pickle
import struct
import subprocess
import sys

import gometry as gm
import pytest


def nested_constructor(depth: int) -> gm.Geometry:
    shape: gm.Geometry = gm.Point(0, 0)
    for _ in range(depth):
        shape = gm.GeometryCollection([shape])
    return shape


def nested_wkt(depth: int) -> str:
    return 'GEOMETRYCOLLECTION (' * depth + 'POINT (0 0)' + ')' * depth


def nested_wkb(depth: int) -> bytes:
    value = struct.pack('<BIdd', 1, 1, 0.0, 0.0)
    for _ in range(depth):
        value = struct.pack('<BII', 1, 7, 1) + value
    return value


def nested_geojson(depth: int) -> str:
    return (
        '{"type":"GeometryCollection","geometries":[' * depth
        + '{"type":"Point","coordinates":[0,0]}'
        + ']}' * depth
    )


def test_constructor_accepts_127_and_128_and_rejects_129() -> None:
    assert nested_constructor(127).geometry_type == 'GeometryCollection'
    assert nested_constructor(128).geometry_type == 'GeometryCollection'
    with pytest.raises(gm.InvalidGeometryError, match='nesting exceeds'):
        nested_constructor(129)


@pytest.mark.parametrize('depth', [127, 128])
def test_wkt_accepts_boundary(depth: int) -> None:
    assert gm.from_wkt(nested_wkt(depth)).geometry_type == 'GeometryCollection'


def test_wkt_rejects_129() -> None:
    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        gm.from_wkt(nested_wkt(129))


@pytest.mark.parametrize('depth', [127, 128])
def test_wkb_accepts_boundary(depth: int) -> None:
    assert gm.from_wkb(nested_wkb(depth)).geometry_type == 'GeometryCollection'


def test_wkb_rejects_129() -> None:
    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        gm.from_wkb(nested_wkb(129))


@pytest.mark.parametrize('depth', [127, 128])
def test_pickle_accepts_boundary(depth: int) -> None:
    shape = nested_constructor(depth)
    restored = pickle.loads(pickle.dumps(shape))
    assert restored.to_wkt() == shape.to_wkt()


@pytest.mark.parametrize('depth', [127, 128])
@pytest.mark.parametrize('as_bytes', [False, True], ids=['text', 'bytes'])
def test_geojson_accepts_boundary(depth: int, as_bytes: bool) -> None:
    shape = nested_constructor(depth)
    text = shape.to_geojson()
    restored = gm.from_geojson(text.encode() if as_bytes else text)
    assert restored.to_wkt() == shape.to_wkt()


def test_geojson_rejects_129() -> None:
    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        gm.from_geojson(nested_geojson(129))


def test_geojson_structural_limit_precedes_default_serde_recursion() -> None:
    nested = '[' * 273 + '0' + ']' * 273
    feature = json.dumps({
        'type': 'Feature',
        'geometry': None,
        'properties': json.loads(nested),
    })
    for reader, payload in ((gm.from_geojson, nested), (gm.from_features, feature)):
        with pytest.raises(gm.ParseError, match='structural limit of 272'):
            reader(payload)


@pytest.mark.parametrize('depth', [127, 128])
@pytest.mark.parametrize('as_bytes', [False, True], ids=['text', 'bytes'])
def test_feature_geometry_accepts_boundary(depth: int, as_bytes: bool) -> None:
    shape = nested_constructor(depth)
    feature = {
        'type': 'Feature',
        'geometry': json.loads(shape.to_geojson()),
        'properties': {},
    }
    text = json.dumps(feature)
    restored = gm.from_features(text.encode() if as_bytes else text).geometries[0]
    assert restored.to_wkt() == shape.to_wkt()


@pytest.mark.parametrize('as_bytes', [False, True], ids=['text', 'bytes'])
def test_feature_geometry_rejects_129(as_bytes: bool) -> None:
    feature = {
        'type': 'Feature',
        'geometry': json.loads(nested_geojson(129)),
        'properties': {},
    }
    text = json.dumps(feature)
    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        gm.from_features(text.encode() if as_bytes else text)


def test_structure_preserving_operations_accept_boundary() -> None:
    shape = nested_constructor(128)
    results = [
        shape.affine_transform((1.0, 0.0, 0.0, 1.0, 1.0, 1.0)),
        shape.force_2d(),
        shape.normalize(),
        shape.reverse(),
        shape.snap_to_grid((1.0, 1.0)),
        shape.orient_polygons(),
        shape.with_coordinates(z=None),
        shape.boundary(),
    ]
    assert all(result.geometry_type == 'GeometryCollection' for result in results)
    assert gm.from_wkt(shape.to_wkt(), crs=4326).to_crs(3857).geometry_type == (
        'GeometryCollection'
    )


def test_pickle_reconstruction_rejects_129() -> None:
    import gometry._lib as native

    with pytest.raises(gm.ParseError, match='nesting exceeds'):
        native._unpickle_geometry_v1(nested_wkb(129), None, None)


def test_constructor_rejects_50000_without_process_crash() -> None:
    script = """
import gometry as gm

shape = gm.Point(0, 0)
try:
    for _ in range(50_000):
        shape = gm.GeometryCollection([shape])
except gm.InvalidGeometryError as error:
    assert 'nesting exceeds' in str(error)
    print('typed nesting error')
else:
    raise AssertionError('50,000 nested constructors were accepted')
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'typed nesting error'


def test_shared_depth_128_point_set_bounds_extrema_and_axes_in_subprocess() -> None:
    script = r"""
import math
import sys
import gometry as gm

shape = gm.Point(0.0, -0.0, z=7.0, m=29.0, crs=4326)
for _ in range(128):
    shape = gm.GeometryCollection([shape, shape])

unique = shape.unique_points()
assert unique.geometry_type == 'MultiPoint'
assert unique.coordinate_axes == 'XYZM'
[(x, y, z, m)] = list(unique.coords)
assert (z, m) == (7.0, 29.0)
assert math.copysign(1.0, x) == 1.0
assert math.copysign(1.0, y) == -1.0
assert shape.coordinate_axes == 'XYZM'
assert shape.topological_dimension == 0
assert shape.has_z and shape.has_m and not shape.is_empty
assert shape.min_z == 7.0
assert shape.max_m == 29.0
assert shape.minimum_bounding_circle().geometry_type == 'Point'
bounds = shape.bounds
assert all(math.copysign(1.0, bounds[i]) == 1.0 for i in (0, 2))
assert all(math.copysign(1.0, bounds[i]) == -1.0 for i in (1, 3))
assert shape.estimate_local_crs() == 'EPSG:32631'
print(sys.argv[1])
"""
    result = subprocess.run(
        [sys.executable, '-c', script, 'depth-128-ok'],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'depth-128-ok'


@pytest.mark.parametrize(
    'operation',
    [
        'buffer',
        'offset_curve',
        'polylabel',
        'maximum_inscribed_circle',
        'maximum_inscribed_radius',
        'minimum_clearance',
        'minimum_clearance_line',
        'minimum_bounding_circle',
        'minimum_bounding_radius',
        'concave_hull',
        'estimate_local_crs',
    ],
)
def test_shared_depth_128_local_frame_consumers_use_first_members(
    operation: str,
) -> None:
    script = r"""
import math
import sys
import gometry as gm
import pytest

operation = sys.argv[1]
source = (
    gm.LineString([(2.9, 39.0), (3.1, 39.0)], crs=4326)
    if operation == 'offset_curve'
    else gm.box(2.9, 39.0, 3.1, 39.2, crs=4326)
)
shape = source
for _ in range(128):
    shape = gm.GeometryCollection([shape, shape])

def run(value):
    method = getattr(value, operation)
    if operation in ('buffer', 'offset_curve'):
        return method(100.0)
    if operation in ('polylabel', 'maximum_inscribed_circle', 'maximum_inscribed_radius'):
        return method(tolerance=1.0)
    if operation == 'concave_hull':
        return method(concavity=10.0)
    return method()

expected = run(source)
if operation in ('polylabel', 'maximum_inscribed_circle', 'maximum_inscribed_radius'):
    # These operations admit Polygon/MultiPolygon, not collections; shared
    # preparation must reach the type error without occurrence expansion.
    with pytest.raises(gm.GeometryTypeError, match='a Polygon or MultiPolygon is required'):
        run(shape)
    sys.exit(0)
actual = run(shape)
if isinstance(actual, float):
    assert math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-8), (actual, expected)
elif operation == 'estimate_local_crs':
    assert actual == expected
else:
    assert actual.crs == 'EPSG:4326'
    assert gm.equals(actual, expected), (actual, expected)
"""
    result = subprocess.run(
        [sys.executable, '-c', script, operation],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_shared_depth_128_delaunay_uses_first_full_xy_site() -> None:
    script = r"""
import gometry as gm

source = gm.MultiPoint(
    [(0.0, 0.0), (0.0, -0.0), (4.0, 0.0), (0.0, 4.0)],
    z=[10.0, 99.0, 20.0, 30.0],
    m=[100.0, 999.0, 200.0, 300.0],
)
shape = source
for _ in range(128):
    shape = gm.GeometryCollection([shape, shape])

triangles = shape.triangulate(method='delaunay')
assert len(triangles) == 1
triangle = triangles[0]
assert triangle.coordinate_axes == 'XYZM'
vertices = list(triangle.exterior.coords)
assert vertices[0] == vertices[-1]
assert set(vertices[:-1]) == {
    (0.0, 0.0, 10.0, 100.0),
    (4.0, 0.0, 20.0, 200.0),
    (0.0, 4.0, 30.0, 300.0),
}
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_shared_depth_128_geojson_preflight_and_tile_admission_keep_first_error() -> (
    None
):
    script = r"""
import gometry as gm

operation = __import__('sys').argv[1]
if operation == 'geojson':
    shape = gm.GeometryCollection([
        gm.Point(181.0, 0.0, crs=4326),
        gm.Point(0.0, 91.0, crs=4326),
    ])
    for _ in range(127):
        shape = gm.GeometryCollection([shape, shape])
    shape.to_geojson()
else:
    shape = gm.GeometryCollection([
        gm.Point(0.0, 86.0, crs=4326),
        gm.Point(0.0, 89.0, crs=4326),
    ])
    for _ in range(127):
        shape = gm.GeometryCollection([shape, shape])
    gm.tile_cover(shape, zoom=0)
"""
    for operation, message in [
        ('geojson', 'longitude 181'),
        ('tile', 'latitude 86'),
    ]:
        result = subprocess.run(
            [sys.executable, '-c', script, operation],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        assert result.returncode != 0
        assert message in result.stderr


def test_shared_depth_128_serializers_reject_width_but_repr_stays_bounded() -> None:
    script = r"""
import gometry as gm

point = gm.Point(0.0, 0.0)
for _ in range(58):
    point = gm.GeometryCollection([point, point])
for serialize in (point.to_wkt, point.to_geojson):
    try:
        serialize()
    except OverflowError as error:
        assert str(error) == 'resource size arithmetic overflowed'
    else:
        raise AssertionError('depth-58 doubled Point output exceeded Vec capacity')

shape = gm.LineString([(170.0, 0.0), (-170.0, 0.0)])
for _ in range(128):
    shape = gm.GeometryCollection([shape, shape])

for serialize in (shape.to_wkb, shape.to_wkt, shape.to_geojson):
    try:
        serialize()
    except OverflowError as error:
        assert str(error) == 'resource size arithmetic overflowed'
    else:
        raise AssertionError('full serialization accepted an unrepresentable width')

rendered = repr(shape)
assert rendered.startswith('<GEOMETRYCOLLECTION (')
assert len(rendered[1:121]) == 120
assert rendered[121:124] == '...'
assert rendered.endswith('>')
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_shared_depth_8_geojson_output_matches_expanded_occurrences() -> None:
    def expanded(depth: int) -> gm.Geometry:
        if depth == 0:
            return gm.Point(13.0, 52.0, crs=4326)
        return gm.GeometryCollection(
            [expanded(depth - 1), expanded(depth - 1)],
            crs=4326,
        )

    shared: gm.Geometry = gm.Point(13.0, 52.0, crs=4326)
    for _ in range(8):
        shared = gm.GeometryCollection([shared, shared], crs=4326)

    assert shared.to_geojson() == expanded(8).to_geojson()


@pytest.mark.parametrize(
    ('name', 'wkt', 'x', 'y'),
    [
        ('polygon', 'POLYGON ((0 0,2 0,2 2,0 2,0 0))', 1.0, 1.0),
        ('line', 'LINESTRING (0 0,2 0,2 1)', 0.0, 0.0),
        ('point', 'POINT (1 1)', 1.0, 1.0),
        (
            'polygon with hole',
            'POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))',
            0.5,
            0.5,
        ),
        (
            'Z/M points sharing XY',
            'GEOMETRYCOLLECTION (POINT ZM (0 0 1 10), POINT ZM (0 0 2 20), POINT ZM (1 1 3 30))',
            0.0,
            0.0,
        ),
        (
            'mixed collection',
            'GEOMETRYCOLLECTION (POLYGON ((0 0,2 0,2 2,0 2,0 0)), LINESTRING (-1 1,3 1), POINT (1 1))',
            1.0,
            1.0,
        ),
    ],
    ids=[
        'polygon',
        'line',
        'point',
        'polygon-with-hole',
        'zm-points-sharing-xy',
        'mixed-collection',
    ],
)
@pytest.mark.parametrize('depth', [6, 10], ids=['depth-6', 'depth-10'])
def test_shared_set_operations_match_unshared_wkb_expansion(
    name: str,
    wkt: str,
    x: float,
    y: float,
    depth: int,
) -> None:
    shared = gm.from_wkt(wkt)
    for _ in range(depth):
        shared = gm.GeometryCollection([shared, shared])
    expanded = gm.from_wkb(shared.to_wkb())
    probe = gm.Point(x, y)

    shared_parts = [part.to_wkb() for part in shared.subdivide(max_vertices=8)]
    expanded_parts = [part.to_wkb() for part in expanded.subdivide(max_vertices=8)]
    assert shared_parts == expanded_parts, f'{name}, depth {depth}'
    assert gm.union_all([shared]).to_wkb() == gm.union_all([expanded]).to_wkb(), (
        name,
        depth,
    )
    for predicate in (gm.intersects, gm.disjoint, gm.covers):
        assert predicate(shared, probe) == predicate(expanded, probe), (name, depth)
    assert gm.covered_by(probe, shared) == gm.covered_by(probe, expanded), (name, depth)
    assert gm.intersects_xy(shared, x, y) == gm.intersects_xy(expanded, x, y), (
        name,
        depth,
    )


def test_depth_128_shared_subdivide_union_and_intersects() -> None:
    shared = gm.box(0.0, 0.0, 1.0, 1.0)
    for _ in range(128):
        shared = gm.GeometryCollection([shared, shared])

    parts = shared.subdivide(max_vertices=8)
    assert len(parts) > 0
    assert gm.union_all([shared]).area == 1.0
    assert gm.intersects(shared, gm.Point(0.5, 0.5))


def test_nonadjacent_shared_bounds_and_extrema_keep_signed_zero_bits() -> None:
    positive = gm.GeometryCollection([
        gm.Point(0.0, 0.0, z=0.0, m=0.0),
    ])
    negative = gm.Point(-0.0, 0.0, z=-0.0, m=-0.0)
    shape = gm.GeometryCollection([positive, negative, positive])

    bounds = shape.bounds

    assert [math.copysign(1.0, bounds[index]) for index in (0, 2)] == [1.0, 1.0]
    assert [math.copysign(1.0, bounds[index]) for index in (1, 3)] == [1.0, 1.0]
    assert math.copysign(1.0, shape.min_z) == 1.0
    assert math.copysign(1.0, shape.max_z) == 1.0
    assert math.copysign(1.0, shape.min_m) == 1.0
    assert math.copysign(1.0, shape.max_m) == 1.0


def test_geographic_crossing_bounds_replay_shared_run_extrema() -> None:
    line = gm.LineString([(170.0, 0.0), (-170.0, 0.0)], crs=4326)
    middle = gm.Point(0.0, -0.0, crs=4326)
    shape = gm.GeometryCollection([line, middle, line], crs=4326)

    bounds = shape.bounds

    assert bounds[0] > bounds[2]
    assert math.copysign(1.0, bounds[1]) == 1.0
    assert math.copysign(1.0, bounds[3]) == 1.0


def test_shared_packed_array_windows_remain_distinct_in_point_sets() -> None:
    rows = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)], z=[1, 2], m=[101, 102]),
        gm.LineString([(10, 0), (11, 0)], z=[3, 4], m=[201, 202]),
    ])
    shape = gm.GeometryCollection([rows[0], rows[1]])

    result = shape.unique_points()

    assert list(result.coords) == [
        (0, 0, 1, 101),
        (1, 0, 2, 102),
        (10, 0, 3, 201),
        (11, 0, 4, 202),
    ]
    assert shape.bounds == (0, 0, 11, 0)
    assert shape.min_z == 1
    assert shape.max_z == 4
    assert shape.min_m == 101
    assert shape.max_m == 202


def test_geodesic_concave_hull_keeps_first_source_ordinates_in_depth_128() -> None:
    script = r"""
import math
import gometry as gm

shape = gm.MultiPoint(
    [(-0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 2.0), (-0.0, 2.0)],
    z=[50.0, 10.0, 20.0, 30.0, 40.0],
    m=[5.0, 1.0, 2.0, 3.0, 4.0],
    crs=4326,
)
for _ in range(128):
    shape = gm.GeometryCollection([shape, shape])

hull = shape.concave_hull(concavity=10.0)
assert hull.coordinate_axes == 'XYZM'
assert hull.crs == 'EPSG:4326'
edge = next(coord for coord in hull.exterior.coords if coord[1] == 1.0)
assert (edge[2], edge[3]) == (50.0, 5.0)
assert math.copysign(1.0, edge[0]) == 1.0
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_shared_depth_128_geodesic_concave_hull_preserves_domain_error() -> None:
    script = r"""
import gometry as gm

source = gm.MultiPoint([(10.0, 20.0), (0.0, 91.0)], crs=4326)

def error(shape):
    try:
        shape.concave_hull(concavity=1.0)
    except gm.CRSError as caught:
        return type(caught).__name__, str(caught)
    raise AssertionError('out-of-domain latitude was accepted')

expected = (
    'CRSError',
    'local CRS estimation requires finite longitude/latitude coordinates '
    'within the geographic domain',
)
assert error(source) == expected

shared = source
for _ in range(128):
    shared = gm.GeometryCollection([shared, shared])
assert error(shared) == expected
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_packed_nullable_gathered_lines_keep_row_windows_in_unique_points() -> None:
    rows = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)], z=[1, 2], m=[101, 102]),
        None,
        gm.LineString([(10, 0), (11, 0)], z=[3, 4], m=[201, 202]),
    ])
    gathered = rows[[2, 1, 0]]

    result = gathered.unique_points()

    assert [None if row is None else list(row.coords) for row in result] == [
        [(10, 0, 3, 201), (11, 0, 4, 202)],
        None,
        [(0, 0, 1, 101), (1, 0, 2, 102)],
    ]


def test_shared_depth_128_point_sets_keep_planar_distance_and_area_overlap() -> None:
    script = r"""
import gometry as gm

source = gm.Point(0.0, 0.0)
for _ in range(128):
    source = gm.GeometryCollection([source, source])

target = gm.Point(3.0, 4.0)
assert gm.distance(source, target) == 5.0
assert gm.hausdorff_distance(source, target, max_error=1e-12) == 5.0
cover = gm.from_wkt('POLYGON ((-1 -1, 1 -1, 1 1, -1 1, -1 -1))')
assert gm.distance(source, cover) == 0.0
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_shared_depth_128_line_sets_keep_distance_and_hausdorff() -> None:
    script = r"""
import gometry as gm

source = gm.LineString([(0.0, 0.0), (10.0, 0.0)])
for _ in range(128):
    source = gm.GeometryCollection([source, source])

target = gm.LineString([(0.0, 2.0), (10.0, 2.0)])
assert gm.distance(source, target) == 2.0
assert gm.hausdorff_distance(source, target, max_error=1e-12) == 2.0
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_shared_depth_128_geodesic_set_operations_use_one_owner() -> None:
    script = r"""
import gometry as gm

for coordinates in (
    [(0.0, 0.0), (1.0, 0.0)],
    [(179.0, 0.0), (-179.0, 0.0)],
):
    source = gm.LineString(coordinates, crs=4326)
    for _ in range(128):
        source = gm.GeometryCollection([source, source], crs=4326)

    target = gm.LineString(coordinates, crs=4326)
    assert gm.distance(source, target) == 0.0
    assert gm.dwithin(source, target, 0.0)
    left, right = gm.nearest_points(source, target)
    assert gm.distance(left, right) == 0.0
    assert gm.shortest_line(source, target).length == 0.0
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_shared_depth_128_point_set_distance_3d_is_multiplicity_insensitive() -> None:
    script = r"""
import gometry as gm

source = gm.Point(0.0, 0.0, z=0.0)
for _ in range(128):
    source = gm.GeometryCollection([source, source])

assert gm.distance_3d(source, gm.Point(3.0, 4.0, z=0.0)) == 5.0

line = gm.LineString([(0.0, 0.0), (10.0, 0.0)], z=[0.0, 0.0])
for _ in range(128):
    line = gm.GeometryCollection([line, line])
assert gm.distance_3d(line, gm.Point(5.0, 2.0, z=0.0)) == 2.0
"""
    result = subprocess.run(
        [sys.executable, '-c', script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
