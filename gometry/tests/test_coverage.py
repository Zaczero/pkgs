"""Polygonal-coverage suite — validation, topology-preserving simplification,
and cleaning (the GEOS/PostGIS coverage model over a GeometryArray).
"""

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
    assert grid.coverage_is_valid()
    assert all(g.is_empty for g in grid.coverage_invalid_edges())
    overlap = gm.GeometryArray([gm.box(0, 0, 1.1, 1), gm.box(1, 0, 2, 1)])
    assert not overlap.coverage_is_valid()
    assert not any(g.is_empty for g in overlap.coverage_invalid_edges())
    tjoin = gm.GeometryArray([
        gm.box(0, 0, 1, 2),
        gm.from_wkt('POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))'),
        gm.from_wkt('POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))'),
    ])
    assert not tjoin.coverage_is_valid()
    gap = gm.GeometryArray([gm.box(0, 0, 0.999, 1), gm.box(1, 0, 2, 1)])
    assert gap.coverage_is_valid()
    assert not gap.coverage_is_valid(gap_width=0.01)
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
        grid.coverage_is_valid(gap_width=-1)


def test_coverage_free_functions_accept_geometry_array() -> None:
    grid = gm.GeometryArray(grid_rows())
    assert grid.coverage_is_valid()
    assert grid.coverage_invalid_edges()[0].is_empty
    assert grid.coverage_simplify(0.0).coverage_is_valid()
    assert grid.coverage_clean(grid_size=0.0).coverage_is_valid()
    assert grid.coverage_union().geometry_type == 'Polygon'
    assert gm.coverage_is_valid(grid)
    assert gm.coverage_invalid_edges(grid)[0].is_empty
    assert gm.coverage_simplify(grid, 0.0).coverage_is_valid()
    assert gm.coverage_clean(grid, grid_size=0.0).coverage_is_valid()
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
    assert not triple.coverage_is_valid()
    assert not any(g.is_empty for g in triple.coverage_invalid_edges())


def test_coincident_rows_are_invalid_coverage() -> None:
    """Two identical boundaries enclose overlap, not a shared interface."""
    first = gm.box(0, 0, 1, 1)
    # Equivalent presentation with a different ring start and orientation.
    second = gm.from_wkt('POLYGON ((1 1, 1 0, 0 0, 0 1, 1 1))')
    rows = gm.GeometryArray([first, second])
    assert not rows.coverage_is_valid()
    assert all(not edge.is_empty for edge in rows.coverage_invalid_edges())
    for operation in (rows.coverage_union, lambda: rows.coverage_simplify(0.0)):
        with pytest.raises(gm.InvalidGeometryError, match='valid polygonal coverage'):
            operation()


def test_check_and_do_coverage_operations_reject_invalid_input() -> None:
    overlap = gm.GeometryArray([gm.box(0, 0, 1.2, 1), gm.box(1, 0, 2, 1)])
    for operation in (
        overlap.coverage_union,
        lambda: overlap.coverage_simplify(0.1),
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


def test_coverage_simplify_preserves_topology() -> None:
    left = gm.from_wkt(
        'POLYGON ((0 0, 1 0, 1.05 0.25, 0.95 0.5, 1.05 0.75, 1 1, 0 1, 0 0))'
    )
    right = gm.from_wkt(
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1.05 0.75, 0.95 0.5, 1.05 0.25, 1 0))'
    )
    arr = gm.GeometryArray([left, right], crs=3857)
    assert arr.coverage_is_valid()
    out = arr.coverage_simplify(0.5)
    assert out.coverage_is_valid()
    assert [g.to_wkt() for g in out] == [
        'POLYGON ((1 0, 1 1, 0 1, 0 0, 1 0))',
        'POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))',
    ]
    assert out.crs == 'EPSG:3857'
    assert sum(g.area for g in out) == pytest.approx(sum(g.area for g in arr), rel=0.1)
    pinned = arr.coverage_simplify(0.5, simplify_boundary=False)
    assert pinned.coverage_is_valid()
    measured = gm.GeometryArray([g.set_z(1.0) for g in grid_rows()])
    carried = measured.coverage_simplify(0.1)
    assert all(g.has_z for g in carried)
    assert all(z == 1.0 for geom in carried for z in geom.coords.z)


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
    assert not overlap.coverage_is_valid()
    expected: dict[str, list[float]] = {
        'longest_border': [1.2, 0.8],
        'max_area': [1.2, 0.8],
        'min_area': [1.0, 1.0],
        'min_index': [1.2, 0.8],
    }
    for rule, areas in expected.items():
        out = overlap.coverage_clean(grid_size=0, overlap_rule=rule)
        assert [g.area for g in out] == pytest.approx(areas)
        assert out.coverage_is_valid()
    outer = gm.from_wkt(
        'POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))'
    )
    inner = gm.box(1.005, 1.005, 1.995, 1.995)
    ring = gm.GeometryArray([outer, inner])
    merged = ring.coverage_clean(grid_size=0, gap_width=0.05)
    assert sum(g.area for g in merged) == pytest.approx(9.0)
    assert merged.coverage_is_valid()
    kept = ring.coverage_clean(grid_size=0)
    assert sum(g.area for g in kept) == pytest.approx(8.9801)
    near = gm.GeometryArray([gm.box(0, 0, 1.0000001, 1), gm.box(1, 0, 2, 1)])
    assert not near.coverage_is_valid()
    unchanged_default = near.coverage_clean()
    assert unchanged_default.coverage_is_valid()
    assert sorted(set(gm.get_coordinates(unchanged_default)[:, 0])) == [
        0.0,
        1.0,
        1.0000001,
        2.0,
    ]
    healed = near.coverage_clean(grid_size=1e-6)
    assert healed.coverage_is_valid()
    grid = gm.GeometryArray(grid_rows())
    identity = grid.coverage_clean(grid_size=0)
    assert identity.coverage_is_valid()
    assert [g.area for g in identity] == pytest.approx([1.0] * 4)
    assert identity.to_wkb() == grid.to_wkb()
    twice = identity.coverage_clean()
    assert twice.to_wkb() == identity.to_wkb()


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
        out = arr.coverage_clean(grid_size=0)
        assert out.coverage_is_valid()
        union_in = gm.union_all(list(arr)).area
        union_out = gm.union_all([g for g in out if not g.is_empty]).area
        assert union_out == pytest.approx(union_in, rel=1e-06)
