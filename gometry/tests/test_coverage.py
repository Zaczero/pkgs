"""Polygonal-coverage suite — validation, topology-preserving simplification,
and cleaning (the GEOS/PostGIS coverage model over a GeometryArray).
"""

import math

import gometry as gm
import pytest


def grid_rows() -> list[gm.Polygon]:
    return [
        gm.box(0, 0, 1, 1),
        gm.box(1, 0, 2, 1),
        gm.box(0, 1, 1, 2),
        gm.box(1, 1, 2, 2),
    ]


def test_coverage_is_valid_verdicts() -> None:
    grid = gm.GeometryArray(grid_rows())
    assert gm.coverage_is_valid(grid)
    assert all(g.is_empty for g in gm.coverage_invalid_edges(grid))
    overlap = gm.GeometryArray([gm.box(0, 0, 1.1, 1), gm.box(1, 0, 2, 1)])
    assert not gm.coverage_is_valid(overlap)
    assert not any(g.is_empty for g in gm.coverage_invalid_edges(overlap))
    tjoin = gm.GeometryArray([
        gm.box(0, 0, 1, 2),
        gm.from_wkt('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'),
        gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'),
    ])
    assert not gm.coverage_is_valid(tjoin)
    gap = gm.GeometryArray([gm.box(0, 0, 0.999, 1), gm.box(1, 0, 2, 1)])
    assert gm.coverage_is_valid(gap)
    assert not gm.coverage_is_valid(gap, gap_width=0.01)
    assert gm.coverage_is_valid(grid_rows())
    assert gm.coverage_is_valid(gm.box(0, 0, 1, 1))
    assert len(gm.coverage_invalid_edges(gm.box(0, 0, 1, 1))) == 1
    assert len(gm.coverage_clean(gm.box(0, 0, 1, 1))) == 1
    assert len(gm.coverage_simplify(gm.box(0, 0, 1, 1), 0.0)) == 1
    assert gm.coverage_union(gm.box(0, 0, 1, 1)).geometry_type == 'Polygon'
    edges = gm.coverage_invalid_edges([
        gm.box(0, 0, 1.1, 1, crs=3857),
        gm.box(1, 0, 2, 1, crs=3857),
    ])
    assert edges.crs == 'EPSG:3857'
    with pytest.raises(gm.GeometryTypeError, match='Polygon or MultiPolygon'):
        gm.coverage_is_valid([gm.Point(0, 0)])
    with pytest.raises(gm.GeometryError, match='gap_width'):
        gm.coverage_is_valid(grid, gap_width=-1)


def test_coverage_is_valid_keeps_subnormal_gap_width_in_distance_space() -> None:
    scale = 1.0e-162
    rows = gm.GeometryArray([
        gm.box(0.0, 0.0, scale, scale),
        gm.box(2.0 * scale, 0.0, 3.0 * scale, scale),
    ])

    assert gm.coverage_is_valid(rows)
    assert not gm.coverage_is_valid(rows, gap_width=1.1 * scale)


def test_coverage_free_functions_accept_geometry_array() -> None:
    grid = gm.GeometryArray(grid_rows())
    assert gm.coverage_is_valid(grid)
    assert gm.coverage_invalid_edges(grid)[0].is_empty
    assert gm.coverage_is_valid(gm.coverage_simplify(grid, 0.0))
    assert gm.coverage_is_valid(gm.coverage_clean(grid, grid_size=0.0))
    assert gm.coverage_union(grid).geometry_type == 'Polygon'
    assert gm.coverage_is_valid(grid)
    assert gm.coverage_invalid_edges(grid)[0].is_empty
    assert gm.coverage_is_valid(gm.coverage_simplify(grid, 0.0))
    assert gm.coverage_is_valid(gm.coverage_clean(grid, grid_size=0.0))
    assert gm.coverage_union(grid).geometry_type == 'Polygon'


def test_coverage_union_normalize_is_stable() -> None:
    """Canonical presentation lives on normalize(); raw ring start may vary."""
    tiles = [gm.box(0, 0, 1, 1), gm.box(1, 0, 2, 1)]
    union = gm.coverage_union(tiles)
    assert union.area == pytest.approx(2.0)
    canonical = {union.normalize().to_wkt() for _ in range(20)}
    assert len(canonical) == 1
    assert canonical.pop() == 'POLYGON ((0 0, 1 0, 2 0, 2 1, 1 1, 0 1, 0 0))'


def test_coverage_union_dissolves_shared_edges() -> None:
    grid = [gm.box(i, j, i + 1, j + 1) for i in range(3) for j in range(3)]
    union = gm.coverage_union(grid)
    assert union.geometry_type == 'Polygon'
    assert union.area == pytest.approx(9.0)
    ring = [
        gm.box(i, j, i + 1, j + 1)
        for i in range(3)
        for j in range(3)
        if not (i == 1 and j == 1)
    ]
    holed = gm.coverage_union(ring)
    assert holed.area == pytest.approx(8.0)
    disjoint = gm.coverage_union([gm.box(0, 0, 1, 1), gm.box(5, 5, 6, 6)])
    assert disjoint.geometry_type == 'MultiPolygon'
    assert disjoint.area == pytest.approx(2.0)
    assert gm.coverage_union([gm.box(0, 0, 1, 1, crs=3857)]).crs == 'EPSG:3857'
    with pytest.raises(gm.GeometryTypeError, match='Polygon or MultiPolygon'):
        gm.coverage_union([gm.Point(0, 0)])
    with pytest.raises(gm.InvalidGeometryError):
        gm.coverage_union([])


def test_coverage_union_keeps_point_touching_parts_separate() -> None:
    """An all-exterior edge inventory may bypass arrangement without merging a touch."""
    touching = [gm.box(0, 0, 1, 1), gm.box(1, 1, 2, 2)]
    union = gm.coverage_union(touching)
    assert union.geometry_type == 'MultiPolygon'
    assert union.area == pytest.approx(2.0)


def test_coverage_union_mixed_axes_stays_homogeneous_in_sparse_path() -> None:
    xy = gm.box(0, 0, 1, 1)
    xyz = gm.box(3, 0, 4, 1).set_z(7.0)
    xym = gm.box(6, 0, 7, 1).set_m(9.0)
    xyzm = gm.box(9, 0, 10, 1).set_z(11.0).set_m(12.0)
    xyz_second = gm.box(12, 0, 13, 1).set_z(8.0)
    explicit_empty_xy = gm.from_wkt('MULTIPOLYGON (EMPTY, ((0 0, 1 0, 1 1, 0 1, 0 0)))')
    explicit_empty_xyz = gm.from_wkt(
        'MULTIPOLYGON Z (EMPTY, ((3 0 7, 4 0 8, 4 1 9, 3 1 10, 3 0 7)))'
    )

    cases = [
        (
            'xy_xyz',
            xy,
            xyz,
            'XY',
            [
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
                (3.0, 0.0),
                (4.0, 0.0),
                (4.0, 1.0),
                (3.0, 1.0),
                (3.0, 0.0),
            ],
        ),
        (
            'xyz_xym',
            xyz,
            xym,
            'XY',
            [
                (3.0, 0.0),
                (4.0, 0.0),
                (4.0, 1.0),
                (3.0, 1.0),
                (3.0, 0.0),
                (6.0, 0.0),
                (7.0, 0.0),
                (7.0, 1.0),
                (6.0, 1.0),
                (6.0, 0.0),
            ],
        ),
        (
            'xyz_xyzm',
            xyz,
            xyzm,
            'XYZ',
            [
                (3.0, 0.0, 7.0),
                (4.0, 0.0, 7.0),
                (4.0, 1.0, 7.0),
                (3.0, 1.0, 7.0),
                (3.0, 0.0, 7.0),
                (9.0, 0.0, 11.0),
                (10.0, 0.0, 11.0),
                (10.0, 1.0, 11.0),
                (9.0, 1.0, 11.0),
                (9.0, 0.0, 11.0),
            ],
        ),
        (
            'xyz_xyz',
            xyz,
            xyz_second,
            'XYZ',
            [
                (3.0, 0.0, 7.0),
                (4.0, 0.0, 7.0),
                (4.0, 1.0, 7.0),
                (3.0, 1.0, 7.0),
                (3.0, 0.0, 7.0),
                (12.0, 0.0, 8.0),
                (13.0, 0.0, 8.0),
                (13.0, 1.0, 8.0),
                (12.0, 1.0, 8.0),
                (12.0, 0.0, 8.0),
            ],
        ),
        (
            'explicit_empty_xy_xyz',
            explicit_empty_xy,
            explicit_empty_xyz,
            'XY',
            [
                (0.0, 0.0),
                (1.0, 0.0),
                (1.0, 1.0),
                (0.0, 1.0),
                (0.0, 0.0),
                (3.0, 0.0),
                (4.0, 0.0),
                (4.0, 1.0),
                (3.0, 1.0),
                (3.0, 0.0),
            ],
        ),
    ]
    for name, left, right, axes, expected in cases:
        for order, rows in (('forward', (left, right)), ('reverse', (right, left))):
            label = f'{name}/{order}'
            result = gm.coverage_union(list(rows))
            assert result.geometry_type == 'MultiPolygon', label
            assert result.coordinate_axes == axes, label
            assert result.area == pytest.approx(2.0), label
            assert all(part.coordinate_axes == axes for part in result.parts), label
            actual = [
                coordinate
                for polygon in result.coords.to_nested()
                for ring in polygon
                for coordinate in ring
            ]
            assert sorted(actual) == sorted(expected), label


def test_coverage_union_all_empty_members_use_declared_union_axes() -> None:
    cases = [
        (
            gm.from_wkt('MULTIPOLYGON (EMPTY)'),
            gm.from_wkt('MULTIPOLYGON Z (EMPTY)'),
            'XYZ',
        ),
        (
            gm.from_wkt('MULTIPOLYGON Z (EMPTY)'),
            gm.from_wkt('MULTIPOLYGON M (EMPTY)'),
            'XYZM',
        ),
    ]
    for left, right, axes in cases:
        for rows in ((left, right), (right, left)):
            result = gm.coverage_union(list(rows))
            assert result.geometry_type == 'Polygon'
            assert result.is_empty
            assert result.coordinate_axes == axes
            assert result.area == 0.0


def test_coverage_union_uses_exact_closed_support_for_invalid_polygon_rows() -> None:
    """Exact coverage dissolve uses a polygon's closed shell support."""
    bowtie = gm.from_wkt('POLYGON ((0 0, 2 2, 0 2, 2 0, 0 0))')
    disjoint = gm.box(5, 0, 6, 1)
    assert gm.coverage_is_valid([bowtie, disjoint])
    result = gm.coverage_union([bowtie, disjoint])
    assert result.geometry_type == 'MultiPolygon'
    assert result.area == pytest.approx(3.0)


def test_coverage_union_matches_shapely_oracle() -> None:
    import shapely

    cases = [
        [gm.box(i, j, i + 1, j + 1) for i in range(4) for j in range(4)],
        [
            gm.box(i, j, i + 1, j + 1)
            for i in range(3)
            for j in range(3)
            if not (i == 1 and j == 1)
        ],
        [gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)],
    ]
    for rows in cases:
        ours = gm.coverage_union(rows)
        theirs = shapely.coverage_union_all(
            shapely.from_wkt([g.to_wkt() for g in rows])
        )
        assert ours.area == pytest.approx(shapely.area(theirs)), [
            g.to_wkt() for g in rows
        ]


def test_coverage_validity_matches_shapely_oracle() -> None:
    import shapely

    cases = [
        grid_rows(),
        [gm.box(0, 0, 1.05, 1), gm.box(1, 0, 2, 1)],
        [
            gm.box(0, 0, 1, 2),
            gm.from_wkt('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'),
            gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'),
        ],
        [gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)],
    ]
    for rows in cases:
        ours = gm.coverage_is_valid(rows)
        theirs = bool(
            shapely.coverage_is_valid([shapely.from_wkt(g.to_wkt()) for g in rows])
        )
        assert ours == theirs, [g.to_wkt() for g in rows]


def test_three_polygons_sharing_one_edge_is_invalid() -> None:
    """An interface must be matched by exactly two rows; a third claimant is invalid."""
    triple = gm.GeometryArray([
        gm.box(0, 0, 1, 1),
        gm.box(1, 0, 2, 1),
        gm.box(1, 0, 1.5, 1),
    ])
    assert not gm.coverage_is_valid(triple)
    assert not any(g.is_empty for g in gm.coverage_invalid_edges(triple))


@pytest.mark.parametrize('copies', [2, 3, 5])
def test_coincident_rows_are_invalid_coverage(copies: int) -> None:
    """Repeated boundaries enclose overlap, not a shared interface."""
    first = gm.box(0, 0, 1, 1)
    # Equivalent presentation with a different ring start and orientation.
    second = gm.from_wkt('POLYGON ((1 1, 1 0, 0 0, 0 1, 1 1))')
    rows = gm.GeometryArray([first, *[second] * (copies - 1)])
    assert not gm.coverage_is_valid(rows)
    assert all(not edge.is_empty for edge in gm.coverage_invalid_edges(rows))
    for operation in (
        lambda: gm.coverage_union(rows),
        lambda: gm.coverage_simplify(rows, 0.0),
    ):
        with pytest.raises(gm.InvalidGeometryError, match='valid polygonal coverage'):
            operation()


def test_adjacent_coverage_rows_can_have_identical_bounds() -> None:
    rows = gm.from_wkt([
        'POLYGON ((0 0, 1 0, 1 1, 0 0))',
        'POLYGON ((0 0, 1 1, 0 1, 0 0))',
    ])
    assert gm.coverage_is_valid(rows)
    assert all(edge.is_empty for edge in gm.coverage_invalid_edges(rows))
    assert gm.equals(gm.coverage_union(rows), gm.box(0, 0, 1, 1))


def test_check_and_do_coverage_operations_reject_invalid_input() -> None:
    overlap = gm.GeometryArray([gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)])
    for operation in (
        lambda: gm.coverage_union(overlap),
        lambda: gm.coverage_simplify(overlap, 0.1),
        lambda: gm.coverage_union(list(overlap)),
        lambda: gm.coverage_simplify(list(overlap), 0.1),
    ):
        with pytest.raises(
            gm.InvalidGeometryError,
            match=r'valid polygonal coverage.*coverage_invalid_edges.*coverage_clean',
        ) as exc_info:
            operation()
        assert type(exc_info.value) is gm.InvalidGeometryError
        assert exc_info.value.operation in {'coverage_union', 'coverage_simplify'}


def test_coverage_union_keeps_validity_gate_distinct_from_union_all() -> None:
    """The coverage fast path must not silently become a general dissolve."""
    overlap = [gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)]
    assert gm.union_all(overlap).area == pytest.approx(2.0)
    with pytest.raises(gm.InvalidGeometryError, match='valid polygonal coverage'):
        gm.coverage_union(overlap)


def test_coverage_union_rejects_tjunction() -> None:
    """T-junction coverages fail the precondition through coverage_union too."""
    tjoin = gm.GeometryArray([
        gm.box(0, 0, 1, 2),
        gm.from_wkt('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'),
        gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'),
    ])
    assert not gm.coverage_is_valid(tjoin)
    for operation in (
        lambda: gm.coverage_union(tjoin),
        lambda: gm.coverage_union(list(tjoin)),
    ):
        with pytest.raises(
            gm.InvalidGeometryError,
            match=r'valid polygonal coverage.*coverage_invalid_edges.*coverage_clean',
        ) as exc_info:
            operation()
        assert exc_info.value.operation == 'coverage_union'


def test_coverage_union_rejects_gap_under_width() -> None:
    """gap_width is a coverage_is_valid concern; union always uses exact match.

    A narrow gap is still a *valid* exact coverage (no shared edge required
    across a gap), so coverage_union dissolves the disjoint parts. Document
    that union does not apply gap_width — rejection for gap is only via
    coverage_is_valid(gap_width=...).
    """
    gap = gm.GeometryArray([gm.box(0, 0, 0.999, 1), gm.box(1, 0, 2, 1)])
    assert gm.coverage_is_valid(gap)
    assert not gm.coverage_is_valid(gap, gap_width=0.01)
    # Exact-match union still accepts (two components); not a rejection class.
    union = gm.coverage_union(gap)
    assert union.geometry_type == 'MultiPolygon'
    assert union.area == pytest.approx(0.999 + 1.0)


def test_coverage_simplify_preserves_topology() -> None:
    left = gm.from_wkt(
        'POLYGON ((0 0, 1 0, 1.05 0.25, 0.95 0.5, 1.05 0.75, 1 1, 0 1, 0 0))'
    )
    right = gm.from_wkt(
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1.05 0.75, 0.95 0.5, 1.05 0.25, 1 0))'
    )
    arr = gm.GeometryArray([left, right], crs=3857)
    assert gm.coverage_is_valid(arr)
    out = gm.coverage_simplify(arr, 0.5)
    assert gm.coverage_is_valid(out)
    assert [g.to_wkt() for g in out] == [
        'POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))',
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))',
    ]
    assert out.crs == 'EPSG:3857'
    assert sum(g.area for g in out) == pytest.approx(sum(g.area for g in arr), rel=0.1)
    pinned = gm.coverage_simplify(arr, 0.5, simplify_boundary=False)
    assert gm.coverage_is_valid(pinned)
    measured = gm.GeometryArray([g.set_z(1.0) for g in grid_rows()])
    carried = gm.coverage_simplify(measured, 0.1)
    assert all(g.has_z for g in carried)
    assert all(z == 1.0 for geom in carried for z in geom.coords.z)


def test_coverage_simplify_remains_valid_across_aggressive_tolerances() -> None:
    """One exact chain decision stays shared across multipart rows."""
    left = gm.from_wkt(
        'POLYGON ((0 0, 1 0, 1.05 0.25, 0.95 0.5, 1.05 0.75, 1 1, 0 1, 0 0))'
    )
    right = gm.from_wkt(
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1.05 0.75, 0.95 0.5, 1.05 0.25, 1 0))'
    )
    rows = [
        gm.MultiPolygon([left, gm.box(10, 0, 11, 1)]),
        right,
        gm.box(11, 0, 12, 1),
    ]
    assert gm.coverage_is_valid(rows)
    for tolerance in (0.0, 0.01, 0.1, 0.5, 1.0, 1.1, 2.0, 10.0, 100.0):
        simplified = gm.coverage_simplify(rows, tolerance)
        assert gm.coverage_is_valid(simplified), (
            tolerance,
            [geometry.to_wkt() for geometry in simplified],
        )


def test_coverage_dp_keeps_vertex_when_shortcut_projection_clamps() -> None:
    """DP measures a removable vertex to the finite shortcut segment."""
    polygon = gm.from_wkt('POLYGON ((0 0, 6 3, 5 0, 25 -5, 0 -5, 0 0))')
    vertex = gm.Point(6, 3)
    tolerance = 3.1

    # The projection of (6, 3) onto the supporting line of (0, 0)-(5, 0)
    # is (6, 0), past the shortcut's endpoint. Its finite-segment distance
    # is therefore the witnessed endpoint distance sqrt(10), above tolerance.
    assert math.sqrt(10.0) > tolerance
    simplified = gm.coverage_simplify([polygon], tolerance, method='dp')[0]

    assert gm.covers(simplified.boundary(), vertex)


def test_coverage_simplify_matches_shapely_oracle() -> None:
    import shapely

    left = gm.from_wkt(
        'POLYGON ((0 0, 1 0, 1.05 0.25, 0.95 0.5, 1.05 0.75, 1 1, 0 1, 0 0))'
    )
    right = gm.from_wkt(
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1.05 0.75, 0.95 0.5, 1.05 0.25, 1 0))'
    )
    ours = gm.coverage_simplify([left, right], 0.5)
    theirs = shapely.coverage_simplify(
        [shapely.from_wkt(g.to_wkt()) for g in (left, right)], 0.5
    )
    for got, want in zip(ours, theirs, strict=True):
        assert shapely.equals(shapely.from_wkt(got.to_wkt()), want), (
            got.to_wkt(),
            want.wkt,
        )


def test_coverage_clean_overlap_rules_and_gaps() -> None:
    overlap = gm.GeometryArray([gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)])
    assert not gm.coverage_is_valid(overlap)
    expected: dict[str, list[float]] = {
        'longest_border': [1.2, 0.8],
        'max_area': [1.2, 0.8],
        'min_area': [1.0, 1.0],
        'min_index': [1.2, 0.8],
    }
    for rule, areas in expected.items():
        out = gm.coverage_clean(overlap, grid_size=0, overlap_rule=rule)
        assert [g.area for g in out] == pytest.approx(areas)
        assert gm.coverage_is_valid(out)
    reversed_overlap = gm.GeometryArray(list(reversed(overlap)))
    reversed_cleaned = gm.coverage_clean(
        reversed_overlap,
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert [g.area for g in reversed_cleaned] == pytest.approx([1.0, 1.0])
    outer = gm.from_wkt(
        'POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))'
    )
    inner = gm.box(1.005, 1.005, 1.995, 1.995)
    ring = gm.GeometryArray([outer, inner])
    merged = gm.coverage_clean(ring, grid_size=0, gap_width=0.05)
    assert sum(g.area for g in merged) == pytest.approx(9.0)
    assert gm.coverage_is_valid(merged)
    kept = gm.coverage_clean(ring, grid_size=0)
    assert sum(g.area for g in kept) == pytest.approx(8.9801)
    near = gm.GeometryArray([gm.box(0, 0, 1.0000001, 1), gm.box(1, 0, 2, 1)])
    assert not gm.coverage_is_valid(near)
    unchanged_default = gm.coverage_clean(near)
    assert gm.coverage_is_valid(unchanged_default)
    coordinates = gm.get_coordinates(unchanged_default)
    assert sorted(set(coordinates[:, 0])) == [
        0.0,
        1.0,
        1.0000001,
        2.0,
    ]
    healed = gm.coverage_clean(near, grid_size=1e-6)
    assert gm.coverage_is_valid(healed)
    grid = gm.GeometryArray(grid_rows())
    identity = gm.coverage_clean(grid, grid_size=0)
    assert gm.coverage_is_valid(identity)
    assert [g.area for g in identity] == pytest.approx([1.0] * 4)
    assert identity.to_wkb() == grid.to_wkb()
    twice = gm.coverage_clean(identity)
    assert twice.to_wkb() == identity.to_wkb()


def test_coverage_clean_longest_border_orders_individual_length_overflow() -> None:
    rows = gm.GeometryArray([
        gm.box(-1.6e308, -1.7e308, 1.6e308, 1.7e308),
        gm.box(-1.7e308, 0.0, 1.7e308, 1.0e308),
    ])
    overlap = gm.Point(0.0, 0.5e308)
    output = gm.coverage_clean(rows, grid_size=0, overlap_rule='longest_border')
    assert not gm.contains(output[0], overlap)
    assert gm.contains(output[1], overlap)

    reversed_output = gm.coverage_clean(
        gm.GeometryArray(list(reversed(rows))),
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert gm.contains(reversed_output[0], overlap)
    assert not gm.contains(reversed_output[1], overlap)

    scaled_rows = gm.GeometryArray([
        gm.box(-1.6, -1.7, 1.6, 1.7),
        gm.box(-1.7, 0.0, 1.7, 1.0),
    ])
    scaled_output = gm.coverage_clean(
        scaled_rows,
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert not gm.contains(scaled_output[0], gm.Point(0.0, 0.5))
    assert gm.contains(scaled_output[1], gm.Point(0.0, 0.5))


def test_coverage_clean_longest_border_orders_overflowed_sums_with_finite_edges() -> None:
    rows = gm.GeometryArray([
        gm.box(0.0, -1.7e308, 1.5e308, 1.7e308),
        gm.box(-1.7e308, 0.0, 1.7e308, 1.0e308),
    ])
    output = gm.coverage_clean(rows, grid_size=0, overlap_rule='longest_border')
    assert not gm.contains(output[0], gm.Point(0.75e308, 0.5e308))
    assert gm.contains(output[1], gm.Point(0.75e308, 0.5e308))

    scaled_rows = gm.GeometryArray([
        gm.box(0.0, -0.017, 0.015, 0.017),
        gm.box(-0.017, 0.0, 0.017, 0.01),
    ])
    scaled_output = gm.coverage_clean(
        scaled_rows,
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert not gm.contains(scaled_output[0], gm.Point(0.0075, 0.005))
    assert gm.contains(scaled_output[1], gm.Point(0.0075, 0.005))


def test_coverage_clean_longest_border_keeps_overflowed_tie_order() -> None:
    rows = gm.GeometryArray([
        gm.box(-1.7e308, 0.0, 1.7e308, 1.5e308),
        gm.box(0.0, -1.7e308, 1.5e308, 1.7e308),
    ])
    overlap = gm.Point(0.75e308, 0.75e308)
    output = gm.coverage_clean(rows, grid_size=0, overlap_rule='longest_border')
    assert gm.contains(output[0], overlap)
    assert not gm.contains(output[1], overlap)

    reversed_output = gm.coverage_clean(
        gm.GeometryArray(list(reversed(rows))),
        grid_size=0,
        overlap_rule='longest_border',
    )
    assert gm.contains(reversed_output[0], overlap)
    assert not gm.contains(reversed_output[1], overlap)


def test_coverage_clean_publishes_shared_bits_and_source_ordinates() -> None:
    import numpy as np

    rows = gm.GeometryArray([
        gm.box(0, 0, 1.2, 1).set_z(7.0).set_m(11.0),
        gm.box(1, 0, 2, 1).set_z(7.0).set_m(11.0),
    ])
    out = gm.coverage_clean(rows)
    assert all(geometry.coordinate_axes == 'XYZM' for geometry in out)
    assert all(z == 7.0 for geometry in out for z in geometry.coords.z)
    assert all(m == 11.0 for geometry in out for m in geometry.coords.m)

    expected = np.asarray([[1.2, 0.0], [1.2, 1.0]], dtype=np.float64)
    expected_bits = {tuple(row) for row in expected.view(np.uint64)}
    shared_bits = []
    for geometry in out:
        coordinates = gm.get_coordinates(geometry, axes='XY')
        selected = coordinates[
            (coordinates[:, 0] == 1.2)
            & ((coordinates[:, 1] == 0.0) | (coordinates[:, 1] == 1.0))
        ]
        shared_bits.append({tuple(row) for row in selected.view(np.uint64)})
    assert shared_bits == [expected_bits, expected_bits]

    mixed = gm.coverage_clean(
        gm.GeometryArray([
            gm.box(0, 0, 1.2, 1).set_z(5.0),
            gm.box(1, 0, 2, 1),
        ])
    )
    assert [geometry.coordinate_axes for geometry in mixed] == ['XYZ', 'XY']


def test_coverage_clean_deterministic_jitter_corpus() -> None:
    """Pinned near-coverages stay valid and conserve their union area."""
    jitters = (
        (
            (0.012, -0.009),
            (-0.018, 0.021),
            (0.0, 0.015),
            (0.027, -0.024),
            (-0.006, 0.003),
            (0.019, -0.011),
        ),
        (
            (-0.03, 0.03),
            (0.03, -0.03),
            (-0.02, 0.01),
            (0.01, -0.02),
            (0.025, 0.005),
            (-0.005, -0.025),
        ),
        (
            (0.0, 0.0),
            (1e-9, -1e-9),
            (-1e-7, 1e-7),
            (0.02, 0.02),
            (-0.02, -0.02),
            (0.01, -0.01),
        ),
    )
    for case in jitters:
        rows = []
        for index, (x_jitter, y_jitter) in enumerate(case):
            i, j = divmod(index, 2)
            rows.append(gm.box(i, j, i + 1 + x_jitter, j + 1 + y_jitter))
        arr = gm.GeometryArray(rows)
        out = gm.coverage_clean(arr, grid_size=0)
        assert gm.coverage_is_valid(out)
        union_in = gm.union_all(list(arr)).area
        union_out = gm.union_all([g for g in out if not g.is_empty]).area
        assert union_out == pytest.approx(union_in, rel=1e-06)
