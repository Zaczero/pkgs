"""CDT mesh-refinement controls on ``triangulate(method='constrained')``."""

from __future__ import annotations

import math
import random
from fractions import Fraction
from itertools import combinations
from typing import cast

import gometry as gm
import pytest
from gometry import GeometryError


def _sliver_polygon() -> gm.Polygon:
    """Dyadic sliver whose exact boundary midpoints are representable."""
    return gm.Polygon([(0, 0), (20, 0), (20, 0.25), (0.125, 0.0625)])


def _triangle_coords(triangle: gm.Polygon) -> list[tuple[float, float]]:
    return [(x, y) for x, y, *_ in triangle.exterior.coords[:3]]


def _interior_angles_deg(triangle: gm.Polygon) -> list[float]:
    coords = _triangle_coords(triangle)

    def angle_at(vertex: int) -> float:
        prev_pt = coords[vertex - 1]
        curr = coords[vertex]
        next_pt = coords[(vertex + 1) % 3]
        v1 = (prev_pt[0] - curr[0], prev_pt[1] - curr[1])
        v2 = (next_pt[0] - curr[0], next_pt[1] - curr[1])
        dot = v1[0] * v2[0] + v1[1] * v2[1]
        len1 = math.hypot(*v1)
        len2 = math.hypot(*v2)
        cos_angle = max(-1.0, min(1.0, dot / (len1 * len2)))
        return math.degrees(math.acos(cos_angle))

    return [angle_at(i) for i in range(3)]


def _min_interior_angle_deg(triangles: gm.GeometryArray[gm.Polygon]) -> float:
    return min(
        min(_interior_angles_deg(cast('gm.Polygon', triangle)))
        for triangle in triangles
    )


def _z_polygon() -> gm.Polygon:
    return gm.Polygon(
        [(0, 0, 1), (10, 0, 2), (10, 10, 3), (0, 10, 4)],
        holes=[[(3, 3, 5), (7, 3, 6), (7, 7, 7), (3, 7, 8)]],
    )


def _source_parameter(
    point: tuple[float, float],
    start: tuple[float, float],
    end: tuple[float, float],
) -> tuple[Fraction, Fraction] | None:
    lower, upper = Fraction(0), Fraction(1)
    for coordinate, source_start, source_end in zip(point, start, end, strict=True):
        value = Fraction(coordinate)
        previous = Fraction(math.nextafter(coordinate, -math.inf))
        following = Fraction(math.nextafter(coordinate, math.inf))
        rounded_low = (previous + value) / 2
        rounded_high = (value + following) / 2
        origin = Fraction(source_start)
        delta = Fraction(source_end) - origin
        if delta == 0:
            if not rounded_low <= origin <= rounded_high:
                return None
            continue
        first = (rounded_low - origin) / delta
        second = (rounded_high - origin) / delta
        lower = max(lower, min(first, second))
        upper = min(upper, max(first, second))
        if lower > upper:
            return None
    return lower, upper


def _exactly_on_segment(
    point: tuple[float, float],
    start: tuple[float, float],
    end: tuple[float, float],
) -> bool:
    px, py = map(Fraction, point)
    sx, sy = map(Fraction, start)
    ex, ey = map(Fraction, end)
    return (ex - sx) * (py - sy) == (ey - sy) * (px - sx)


def _constraint_has_emitted_chain(
    boundary_edges: list[tuple[tuple[float, float], tuple[float, float]]],
    start: tuple[float, float],
    end: tuple[float, float],
) -> bool:
    intervals: list[tuple[Fraction, Fraction]] = []
    for left, right in boundary_edges:
        left_parameter = _source_parameter(left, start, end)
        right_parameter = _source_parameter(right, start, end)
        if left_parameter is not None and right_parameter is not None:
            intervals.append((
                min(left_parameter[0], right_parameter[0]),
                max(left_parameter[1], right_parameter[1]),
            ))
    cursor = Fraction(0)
    for lo, hi in sorted(intervals):
        if lo > cursor:
            return False
        cursor = max(cursor, hi)
        if cursor >= 1:
            return True
    return False


def _boundary_edges(
    triangles: gm.GeometryArray[gm.Polygon],
) -> list[tuple[tuple[float, float], tuple[float, float]]]:
    edge_counts: dict[tuple[tuple[float, float], tuple[float, float]], int] = {}
    for triangle in triangles:
        coordinates = _triangle_coords(cast('gm.Polygon', triangle))
        for left, right in zip(
            coordinates, coordinates[1:] + coordinates[:1], strict=True
        ):
            edge = tuple(sorted((left, right)))
            edge_counts[edge] = edge_counts.get(edge, 0) + 1
    return [edge for edge, count in edge_counts.items() if count == 1]


def _rounding_half_ulp(value: float) -> Fraction:
    exact = Fraction(value)
    previous = Fraction(math.nextafter(value, -math.inf))
    following = Fraction(math.nextafter(value, math.inf))
    return max(exact - previous, following - exact) / 2


def _exact_ring_area(ring: list[tuple[float, float]]) -> Fraction:
    return (
        abs(
            sum(
                Fraction(left[0]) * Fraction(right[1])
                - Fraction(left[1]) * Fraction(right[0])
                for left, right in zip(ring, ring[1:] + ring[:1], strict=True)
            )
        )
        / 2
    )


def _assert_mesh_properties(
    triangles: gm.GeometryArray[gm.Polygon],
    polygon: gm.Polygon,
    ring: list[tuple[float, float]],
    *,
    min_angle: float | None = None,
    max_area: float | None = None,
    require_rounded_boundary: bool = False,
) -> None:
    assert triangles
    triangle_area = Fraction(0)
    for triangle in triangles:
        coordinates = _triangle_coords(cast('gm.Polygon', triangle))
        assert triangle.area > 0
        exact_coordinates = [(Fraction(x), Fraction(y)) for x, y in coordinates]
        signed_area = (
            sum(
                left[0] * right[1] - left[1] * right[0]
                for left, right in zip(
                    exact_coordinates,
                    exact_coordinates[1:] + exact_coordinates[:1],
                    strict=True,
                )
            )
            / 2
        )
        assert signed_area > 0
        triangle_area += signed_area
        if min_angle is not None:
            assert (
                min(_interior_angles_deg(cast('gm.Polygon', triangle))) + 1e-7
                >= min_angle
            )
        if max_area is not None:
            assert triangle.area <= max_area * (1 + 1e-9)
    for left, right in combinations(triangles, 2):
        assert gm.relate_pattern(left, right, 'F********'), gm.relate(left, right)
    boundary_edges = _boundary_edges(triangles)
    area_error_bound = Fraction(0)
    for left, right in boundary_edges:
        matching = [
            (start, end)
            for start, end in zip(ring, ring[1:] + ring[:1], strict=True)
            if _source_parameter(left, start, end) is not None
            and _source_parameter(right, start, end) is not None
        ]
        assert len(matching) == 1, (left, right, matching)
        left_error = (_rounding_half_ulp(left[0]), _rounding_half_ulp(left[1]))
        right_error = (_rounding_half_ulp(right[0]), _rounding_half_ulp(right[1]))
        edge_x = abs(Fraction(right[0]) - Fraction(left[0]))
        edge_y = abs(Fraction(right[1]) - Fraction(left[1]))
        true_edge_x = edge_x + left_error[0] + right_error[0]
        true_edge_y = edge_y + left_error[1] + right_error[1]
        # The shoelace difference for each boundary quadrilateral is bounded
        # by endpoint half-ULP errors times its incident source-edge lengths.
        area_error_bound += (
            (left_error[1] + right_error[1]) * true_edge_x
            + (left_error[0] + right_error[0]) * true_edge_y
            + left_error[0] * right_error[1]
            + left_error[1] * right_error[0]
        ) / 2
    boundary_vertices = {point for edge in boundary_edges for point in edge}
    assert set(ring) <= boundary_vertices
    source_area = _exact_ring_area(ring)
    assert abs(triangle_area - source_area) <= area_error_bound
    for start, end in zip(ring, ring[1:] + ring[:1], strict=True):
        assert _constraint_has_emitted_chain(boundary_edges, start, end), (start, end)
    source_vertices = set(ring)
    for point in {point for edge in boundary_edges for point in edge} - source_vertices:
        matches = [
            (start, end)
            for start, end in zip(ring, ring[1:] + ring[:1], strict=True)
            if _source_parameter(point, start, end) is not None
        ]
        assert len(matches) == 1, (point, matches)
    if require_rounded_boundary:
        assert any(
            not _exactly_on_segment(point, start, end)
            for point in boundary_vertices - source_vertices
            for start, end in zip(ring, ring[1:] + ring[:1], strict=True)
            if _source_parameter(point, start, end) is not None
        )


@pytest.fixture
def sliver() -> gm.Polygon:
    return _sliver_polygon()


def test_refine_raises_minimum_angle_and_adds_triangles(sliver: gm.Polygon) -> None:
    base = sliver.triangulate(method='constrained')
    refined = sliver.triangulate(method='constrained', min_angle=25.0)
    assert len(refined) > len(base)
    assert _min_interior_angle_deg(refined) > _min_interior_angle_deg(base)


def test_max_area_caps_triangle_areas(sliver: gm.Polygon) -> None:
    max_area = 2.0
    refined = sliver.triangulate(method='constrained', max_area=max_area)
    eps = max_area * 1e-09 + 1e-12
    for triangle in refined:
        assert triangle.area <= max_area + eps


def test_geographic_constrained_chart_is_shared_by_scalar_and_array() -> None:
    seam_hole = gm.Polygon(
        [(170, -10), (-170, -10), (-170, 10), (170, 10)],
        holes=[[(175, -2), (-175, -2), (-175, 2), (175, 2)]],
        crs=4326,
    )
    assert seam_hole.is_valid
    scalar = seam_hole.triangulate(method='constrained')
    array = gm.GeometryArray([seam_hole]).triangulate(method='constrained')[0]
    assert len(scalar) == 8
    assert len(array) == 8

    seam_trapezoid = gm.Polygon(
        [(170, 0), (-170, 0), (-170, 10), (170, 8)],
        crs=4326,
    )
    assert len(seam_trapezoid.triangulate(method='constrained', max_area=1000)) == 2
    assert (
        len(
            gm.GeometryArray([seam_trapezoid]).triangulate(
                method='constrained', max_area=1000
            )[0]
        )
        == 2
    )


def test_geographic_constrained_cdt_keeps_zero_turn_world_band() -> None:
    ring = [
        (0.0, 20.0),
        (120.0, 20.0),
        (240.0, 20.0),
        (360.0, 20.0),
        (360.0, 10.0),
        (240.0, 10.0),
        (120.0, 10.0),
        (0.0, 10.0),
    ]
    polygon = gm.Polygon(ring, crs=4326)
    scalar = polygon.triangulate(method='constrained')
    packed = gm.GeometryArray([polygon]).triangulate(method='constrained')[0]

    assert len(scalar) == len(packed) == 6
    assert [triangle.to_wkb() for triangle in scalar] == [
        triangle.to_wkb() for triangle in packed
    ]
    assert all(triangle.area > 0 for triangle in scalar)
    for left, right in combinations(scalar, 2):
        assert gm.relate_pattern(left, right, 'F********'), gm.relate(left, right)
    assert gm.equals(gm.union_all(scalar), polygon)


def test_geographic_constrained_cdt_polar_cap_uses_receipted_storage() -> None:
    cap = gm.Polygon(
        [(0.0, -80.0), (90.0, -80.0), (180.0, -80.0), (-90.0, -80.0)],
        crs=4326,
    )
    scalar = cap.triangulate(method='constrained')
    groups = gm.GeometryArray([cap, None], crs=4326).triangulate(method='constrained')
    grouped = groups[0]

    assert len(scalar) == len(grouped) == 5
    assert groups[1] is None
    assert groups.is_missing.tolist() == [False, True]
    assert [triangle.to_wkb() for triangle in scalar] == [
        triangle.to_wkb() for triangle in grouped
    ]
    assert gm.equals(gm.union_all(scalar), cap)


@pytest.mark.parametrize(
    'coords',
    [
        [(1e16, 1.0), (2.0, 0.0), (3.0, 1e-8)],
        [(2.0, 0.0), (3.0, 1e-8), (1e16, 1.0)],
        [(3.0, 1e-8), (1e16, 1.0), (2.0, 0.0)],
    ],
    ids=['A-first', 'B-first', 'C-first'],
)
def test_max_area_named_triangle_keeps_limit_across_ring_rotations(
    coords: list[tuple[float, float]],
) -> None:
    """A representable source midpoint keeps the exact area admission stable."""
    max_area = 49999999.249974996
    poly = gm.Polygon(coords)
    assert poly.area > max_area
    refined = poly.triangulate(method='constrained', max_area=max_area)
    assert len(refined) > 0
    slack = max_area * (1.0 + 1.0e-12)
    for triangle in refined:
        assert triangle.area <= slack


@pytest.mark.parametrize(
    ('kwargs', 'pattern'),
    [
        ({'min_angle': 0}, 'min_angle must be a positive finite number'),
        ({'min_angle': -5}, 'min_angle must be a positive finite number'),
        ({'min_angle': 31}, 'min_angle must be at most 30 degrees'),
        ({'min_angle': float('nan')}, 'min_angle must be finite'),
        ({'max_area': 0}, 'max_area must be a positive finite number'),
        ({'max_area': -1}, 'max_area must be a positive finite number'),
        ({'max_area': float('inf')}, 'max_area must be finite'),
    ],
)
def test_param_validation(
    sliver: gm.Polygon, kwargs: dict[str, bool | float], pattern: str
) -> None:
    with pytest.raises(GeometryError, match=pattern):
        sliver.triangulate(method='constrained', **kwargs)


@pytest.mark.parametrize('kwargs', [{'min_angle': 25.0}, {'max_area': 1.0}])
def test_quality_constraint_implies_refine(
    sliver: gm.Polygon, kwargs: dict[str, float]
) -> None:
    base = sliver.triangulate(method='constrained')
    implied = sliver.triangulate(method='constrained', **kwargs)
    assert len(implied) > len(base)


def test_refine_drops_zm_preserves_without(sliver: gm.Polygon) -> None:
    zpoly = _z_polygon()
    preserved = zpoly.triangulate(method='constrained')
    assert preserved
    assert all(t.coordinate_axes == 'XYZ' for t in preserved)
    refined = zpoly.triangulate(method='constrained', max_area=2.0)
    assert refined
    assert all(t.coordinate_axes == 'XY' for t in refined)
    assert all(
        t.coordinate_axes == 'XY' for t in sliver.triangulate(method='constrained')
    )


def test_array_refinement_drops_zm_while_plain_mesh_preserves_it() -> None:
    zpoly = _z_polygon()
    array = gm.GeometryArray([zpoly])
    assert all(
        t.coordinate_axes == 'XYZ' for t in array.triangulate(method='constrained')[0]
    )
    assert all(
        t.coordinate_axes == 'XY'
        for t in array.triangulate(method='constrained', max_area=2.0)[0]
    )
    rows = gm.GeometryArray([gm.box(0, 0, 4, 4), gm.box(0, 0, 4, 4)])
    varied = rows.triangulate(method='constrained', max_area=[4.0, 1.0])
    assert len(varied[1]) > len(varied[0])


def test_scalar_and_array_surfaces_match(sliver: gm.Polygon) -> None:
    kwargs = {'min_angle': 25.0}
    scalar = sliver.triangulate(method='constrained', **kwargs)
    # Array is per-row Groups; row 0 is this single geometry's triangles.
    array = gm.GeometryArray([sliver]).triangulate(method='constrained', **kwargs)[0]
    assert len(scalar) == len(array)
    assert [t.to_wkt() for t in scalar] == [t.to_wkt() for t in array]


def test_rounded_source_midpoint_is_an_unrepresentable_output() -> None:
    next_x = math.nextafter(1.0, math.inf)
    polygon = gm.Polygon([(1.0, 0.0), (next_x, 1.0), (1.0, 0.5)])
    before = polygon.to_wkt()

    with pytest.raises(GeometryError) as raised:
        polygon.triangulate(method='constrained', max_area=1.0e-20)

    assert raised.value.operation == 'triangulate'
    assert polygon.to_wkt() == before


@pytest.mark.parametrize(
    ('first_z', 'first_m'),
    [(1.0, 10.0), (2.0, 20.0)],
)
def test_constrained_duplicate_xy_uses_first_source_point(
    first_z: float, first_m: float
) -> None:
    second_z = 3.0 - first_z
    second_m = 30.0 - first_m
    source = gm.from_wkt(
        'POLYGON ZM (('
        f'0 0 {first_z:g} {first_m:g}, '
        f'0 0 {second_z:g} {second_m:g}, '
        '4 0 4 40, 0 4 5 50, '
        f'0 0 {first_z:g} {first_m:g}))'
    )
    scalar = source.triangulate(method='constrained')
    assert scalar
    assert all(triangle.coordinate_axes == 'XYZM' for triangle in scalar)
    assert all(
        f'0 0 {first_z:g} {first_m:g}' in triangle.to_wkt() for triangle in scalar
    )

    array = gm.GeometryArray([source]).triangulate(method='constrained')[0]
    assert [triangle.to_wkt() for triangle in array] == [
        triangle.to_wkt() for triangle in scalar
    ]
    refined = source.triangulate(method='constrained', max_area=2.0)
    assert refined
    assert all(triangle.coordinate_axes == 'XY' for triangle in refined)


def test_constrained_array_mixed_missing_and_empty_rows_match_scalar() -> None:
    zm = gm.from_wkt('POLYGON ZM ((0 0 1 10, 4 0 2 20, 0 4 3 30, 0 0 1 10))')
    xy = gm.from_wkt('POLYGON ((0 0, 4 0, 0 4, 0 0))')
    rows = gm.GeometryArray([zm, xy, gm.from_wkt('POLYGON EMPTY'), None])
    groups = rows.triangulate(method='constrained')

    assert [triangle.to_wkt() for triangle in groups[0]] == [
        triangle.to_wkt() for triangle in zm.triangulate(method='constrained')
    ]
    assert [triangle.to_wkt() for triangle in groups[1]] == [
        triangle.to_wkt() for triangle in xy.triangulate(method='constrained')
    ]
    assert len(groups[2]) == 0
    assert groups[3] is None
    assert groups.is_missing.tolist() == [False, False, False, True]


def test_delaunay_duplicate_xy_uses_the_same_source_site_for_scalar_and_array() -> None:
    source = gm.from_wkt(
        'MULTIPOINT ZM (0 0 1 10, 0 0 2 20, 4 0 3 30, 0 4 4 40, 4 4 5 50)'
    )
    scalar = source.triangulate(method='delaunay')
    array = gm.GeometryArray([source]).triangulate(method='delaunay')[0]

    assert scalar
    assert [triangle.to_wkt() for triangle in array] == [
        triangle.to_wkt() for triangle in scalar
    ]
    assert all(triangle.coordinate_axes == 'XYZM' for triangle in scalar)
    origin_corners = [
        tuple(coordinate)
        for triangle in scalar
        for coordinate in triangle.exterior.coords
        if coordinate[0] == 0 and coordinate[1] == 0
    ]
    assert origin_corners
    assert set(origin_corners) == {(0.0, 0.0, 1.0, 10.0)}


def test_constrained_delaunay_records_flipped_source_constraints() -> None:
    ring = [(4.0, 3.0), (5.0, 1.0), (1.0, 4.0), (1.0, 0.0), (6.0, 1.0)]
    polygon = gm.Polygon(ring)
    assert polygon.is_valid

    triangles = polygon.triangulate(method='constrained')

    _assert_mesh_properties(triangles, polygon, ring)


@pytest.mark.parametrize(
    'ring',
    [
        pytest.param(
            [(7.0, 1.0), (2.0, 8.0), (8.0, 3.0), (1.0, 2.0), (2.0, 7.0)],
            id='pentagram-edge-flip',
        ),
        pytest.param(
            [
                (8.0, 8.0),
                (3.0, 0.0),
                (1.0, 6.0),
                (5.0, 0.0),
                (6.0, 7.0),
                (4.0, 1.0),
            ],
            id='pentagram-flip-cycle',
        ),
        pytest.param(
            [
                (8.0, 8.0),
                (1.0, 2.0),
                (2.0, 4.0),
                (8.0, 5.0),
                (3.0, 3.0),
                (8.0, 1.0),
            ],
            id='pentagram-stale-halfedge',
        ),
    ],
)
def test_invalid_constraints_return_invalid_geometry_error(
    ring: list[tuple[float, float]],
) -> None:
    polygon = gm.Polygon(ring)
    assert not polygon.is_valid

    with pytest.raises(gm.InvalidGeometryError):
        polygon.triangulate(method='constrained')


@pytest.mark.parametrize(
    ('method', 'message'),
    [
        (
            'constrained',
            'triangulation failed: polygon hole touches or crosses the shell',
        ),
        (
            'earcut',
            'triangulation failed: polygon hole touches or crosses the shell',
        ),
    ],
    ids=['constrained', 'earcut'],
)
def test_triangulation_names_hole_shell_edge_overlap(
    method: str,
    message: str,
) -> None:
    polygon = gm.from_wkt(
        'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (0 2, 4 2, 4 6, 0 6, 0 2))'
    )

    with pytest.raises(gm.InvalidGeometryError) as raised:
        polygon.triangulate(method=method)

    assert str(raised.value) == message


@pytest.mark.parametrize(
    ('option', 'value'),
    [('min_angle', 20.0), ('max_area', 50.0)],
)
def test_reported_polygon_refines_with_exact_source_chains(
    option: str,
    value: float,
) -> None:
    ring = [
        (15.28, 53.13),
        (21.72, 36.65),
        (22.67, 41.11),
        (40.29, 16.93),
        (48.68, 39.6),
    ]
    polygon = gm.Polygon(ring)
    assert polygon.is_valid

    triangles = polygon.triangulate(method='constrained', **{option: value})

    _assert_mesh_properties(
        triangles,
        polygon,
        ring,
        min_angle=value if option == 'min_angle' else None,
        max_area=value if option == 'max_area' else None,
    )


def test_refined_acute_source_corner_uses_nearest_binary64_boundary_nodes() -> None:
    ring = [
        (83.61, 61.84),
        (74.31, 79.69),
        (56.51, 68.12),
        (47.38, 70.5),
        (41.3, 91.42),
        (41.79, 83.06),
        (25.5, 21.18),
        (43.42, 14.3),
        (33.98, 15.0),
        (34.84, 9.08),
        (45.46, 2.96),
        (53.31, 26.0),
        (61.72, 35.4),
        (75.05, 35.75),
        (99.45, 46.32),
    ]
    polygon = gm.Polygon(ring)
    assert polygon.is_valid

    triangles = polygon.triangulate(method='constrained', min_angle=20.0)

    _assert_mesh_properties(
        triangles,
        polygon,
        ring,
        min_angle=20.0,
        require_rounded_boundary=True,
    )


def test_seeded_constrained_triangulation_properties_against_other_methods() -> None:
    seed = 0xCD7
    random_source = random.Random(seed)
    for case in range(200):
        vertex_count = random_source.randint(4, 8)
        step = math.tau / vertex_count
        offset = random_source.random() * math.tau
        ring = []
        for index in range(vertex_count):
            angle = offset + index * step + random_source.uniform(-0.15, 0.15) * step
            radius = random_source.randint(20, 80)
            ring.append((
                float(round(radius * math.cos(angle))),
                float(round(radius * math.sin(angle))),
            ))

        polygon = gm.Polygon(ring)
        assert polygon.is_valid, (seed, case, ring)
        earcut = polygon.triangulate(method='earcut')
        delaunay = polygon.triangulate(method='delaunay')
        constrained = polygon.triangulate(method='constrained')

        assert math.isclose(
            math.fsum(triangle.area for triangle in earcut),
            polygon.area,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ), (seed, case, 'earcut')
        assert math.isclose(
            math.fsum(triangle.area for triangle in delaunay),
            polygon.convex_hull().area,
            rel_tol=1e-12,
            abs_tol=1e-12,
        ), (seed, case, 'delaunay')
        _assert_mesh_properties(constrained, polygon, ring)

        if case < 6:
            angle_mesh = polygon.triangulate(method='constrained', min_angle=20.0)
            _assert_mesh_properties(angle_mesh, polygon, ring, min_angle=20.0)
            area_limit = polygon.area / 8
            area_mesh = polygon.triangulate(method='constrained', max_area=area_limit)
            _assert_mesh_properties(area_mesh, polygon, ring, max_area=area_limit)
