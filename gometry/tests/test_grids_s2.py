"""H3 and S2 discrete global grids — cells, boundaries, coverage,
exact membership predicates, compaction, and antimeridian-aware bounds.
"""

import math
import operator

import gometry as gm
import numpy as np
import pytest


def _assert_seam_within_matches_linear_source(source: gm.Geometry) -> None:
    """``within`` is the exact raw lon/lat source relation, not S2's loop API.

    This is the independently enumerated finite L8 universe of true S2 cells
    wholly inside the stored 179°→181°, -1°→1° linear source. It is pinned by
    token rather than derived from the coverer, sampled points, or a cell's
    four-corner chord proxy; it includes the sub-ULP retained seam sheet.
    """
    overlap = gm.s2_cover(source, level=8, cell_rule='overlap')
    within = gm.s2_cover(source, level=8, cell_rule='within')
    expected = {
        '65545',
        '6554d',
        '6554f',
        '65551',
        '65553',
        '65555',
        '65557',
        '65559',
        '6555b',
        '6ffe3',
        '6ffe5',
        '6ffef',
        '6fff1',
        '6fff7',
        '6fff9',
        '6fffb',
        '6fffd',
        '6ffff',
        '70001',
        '70003',
        '70005',
        '70007',
        '70009',
        '7000f',
        '70011',
        '7001b',
        '7001d',
        '7aaa5',
        '7aaa7',
        '7aaa9',
        '7aaab',
        '7aaad',
        '7aaaf',
        '7aab1',
        '7aab3',
        '7aabb',
    }
    assert len(expected) == 36
    assert expected <= {cell.token for cell in overlap}
    assert {cell.token for cell in within} == expected


def test_s2_point_cell_boundary_and_parent() -> None:
    cell = gm.S2Cell(21.0, 52.0, level=12)
    geometry_cell = gm.S2Cell(gm.Point(21.0, 52.0, crs=4326), level=12)
    projected_cell = gm.S2Cell(gm.Point(21.0, 52.0, crs=4326).to_crs(32634), level=12)
    cells = gm.s2_cells([21.0, 22.0], [52.0, 53.0], level=12)
    projected_cells = gm.s2_cells([21.0, 22.0], [52.0, 53.0], level=12)
    boundary = cell.polygon
    assert cell.level == 12
    assert int(cell) == cell.id
    assert operator.index(cell) == cell.id
    assert geometry_cell.token == cell.token
    assert projected_cell.token == cell.token
    assert isinstance(cells, gm.CellArray)
    assert [value.token for value in projected_cells] == [
        value.token for value in cells
    ]
    assert len(cells) == 2
    assert all(isinstance(value, gm.S2Cell) for value in cells)
    assert str(cell) == cell.token
    parent = cell.parent(10)
    children = cell.parent(11).children()
    assert parent.level == 10
    assert len(children) == 4
    assert all(value.level == 12 for value in children)
    assert any(value == cell for value in children)
    assert parent.contains(cell)
    assert parent.contains(cell.token)
    assert parent.contains(cell.id)
    assert cell.intersects(parent)
    assert {cell, geometry_cell, parent} == {cell, parent}
    assert hash(cell) == hash(geometry_cell)
    assert cell.center.crs == 'OGC:CRS84'
    assert boundary.geometry_type == 'Polygon'
    assert boundary.crs == 'OGC:CRS84'
    assert (
        gm.CellArray([cell.token], type=gm.S2Cell).polygon[0].to_wkt()
        == boundary.to_wkt()
    )
    assert (
        gm.CellArray([cell.id], type=gm.S2Cell).polygon[0].to_wkt() == boundary.to_wkt()
    )
    with pytest.raises(ValueError):
        cell.parent(13)
    with pytest.raises(TypeError, match='lat must not be provided'):
        gm.S2Cell(gm.Point(21.0, 52.0, crs=4326), 52.0, level=12)


def test_s2_bounds_coverage_membership() -> None:
    polygon = gm.box(20.99, 51.99, 21.01, 52.01, crs=4326)
    coverage = gm.s2_cover(polygon, level=12)
    projected_coverage = gm.s2_cover(polygon.to_crs(32634), level=12)
    assert coverage
    assert [value.token for value in projected_coverage] == [
        value.token for value in coverage
    ]
    assert all(cell.level == 12 for cell in coverage)
    assert coverage[0] in coverage
    assert int(coverage[0]) in coverage
    assert coverage[0].token in coverage
    assert gm.S2Cell(30.0, 52.0, level=12) not in coverage
    assert coverage.polygon.crs == 'OGC:CRS84'
    np.testing.assert_array_equal(
        gm.contains(polygon, gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)),
        [True, False],
    )
    np.testing.assert_array_equal(
        gm.contains(
            polygon.to_crs(32634),
            gm.points([21.0, 30.0], [52.0, 52.0], crs=4326).to_crs(32634),
        ),
        [True, False],
    )
    assert not gm.contains(polygon, gm.Point(30.0, 52.0, crs=4326))
    with pytest.raises(ValueError):
        gm.s2_cover(gm.box(0, 0, 1, 1), max_cells=0)


def test_s2_membership_is_exact_against_geometry_not_bounds() -> None:
    triangle = gm.Polygon([(0, 0), (10, 0), (0, 10)], crs=4326)
    inside = gm.Point(1.0, 1.0, crs=4326)
    bbox_only = gm.Point(8.0, 8.0, crs=4326)
    assert gm.covers(triangle, inside) is True
    assert gm.covers(triangle, bbox_only) is False
    np.testing.assert_array_equal(
        gm.covers(triangle, gm.GeometryArray([inside, bbox_only])), [True, False]
    )
    edge = gm.Point(5.0, 0.0, crs=4326)
    assert gm.covers(triangle, edge) is True
    assert not gm.contains(triangle, edge)
    assert gm.contains(triangle, inside) is True


def test_s2_cell_rule_overlap_is_exact_and_bbox_is_loose() -> None:
    triangle = gm.Polygon([(0, 0), (10, 0), (0, 10)], crs=4326)
    overlap = gm.s2_cover(triangle, level=8, cell_rule='overlap')
    bbox = gm.s2_cover(triangle, level=8, cell_rule='bbox')
    bbox_only = gm.S2Cell(8.0, 8.0, level=8)
    assert bbox_only not in overlap
    assert bbox_only in bbox
    assert {cell.token for cell in overlap} < {cell.token for cell in bbox}


def test_s2_coverage_compact_uncompact_round_trip() -> None:
    polygon = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    coverage = gm.s2_cover(polygon, level=10)
    compacted = coverage.compact()
    assert len(compacted) <= len(coverage)
    assert max(c.level for c in compacted) <= max(c.level for c in coverage)
    expanded = compacted.uncompact(10)
    assert {c.token for c in coverage} <= {c.token for c in expanded}
    with pytest.raises(ValueError, match='uncompact level'):
        coverage.uncompact(2)
    assert gm.contains(polygon, gm.Point(21.0, 52.0, crs=4326)) is True
    assert gm.contains(polygon, gm.Point(30.0, 52.0, crs=4326)) is False


def test_s2_cover_long_horizontal_includes_containing_cells() -> None:
    """Planar lon/lat source edges must not omit cells that intersect them.

    Face 0's true spherical footprint reaches lat -45° at lon 0 while the
    four-corner chord proxy has a southern edge at ≈-35.26°. Spans past
    2*acos(tan(40°)) ≈ 65.91° send both endpoints off face 0 and used to prune
    the entire face-0 subtree via an unsound proxy-negative certificate.
    """
    probe = gm.Point(0.0, -40.0, crs=4326)
    for span in (0.01, 1.0, 20.0, 65.9, 66.0, 100.0):
        half = span / 2.0
        source = gm.LineString([(-half, -40.0), (half, -40.0)], crs=4326)
        coverage = gm.s2_cover(source, level=10)
        source_tokens = {c.token for c in coverage}
        point_tokens = {c.token for c in gm.s2_cover(probe, level=10)}
        assert gm.contains(source, probe), f'span={span} contains(probe)'
        assert point_tokens <= source_tokens, (
            f'span={span}: probe cells {point_tokens} not subset of cover'
        )
    # Explicit witnesses at the face-0 midpoint cell.
    line100 = gm.LineString([(-50.0, -40.0), (50.0, -40.0)], crs=4326)
    assert '1d5' in {c.token for c in gm.s2_cover(line100, level=4)}
    assert '1d4aab' in {c.token for c in gm.s2_cover(line100, level=10)}
    # Collapse was independent of target level.
    for level in range(11):
        cov = gm.s2_cover(line100, level=level)
        tokens = {c.token for c in cov}
        pt = {c.token for c in gm.s2_cover(probe, level=level)}
        assert gm.contains(line100, probe), f'level={level} contains'
        assert pt <= tokens, f'level={level}: {pt} not in cover'


def test_s2_cover_collinear_midpoint_is_invariant() -> None:
    """Adding a collinear midpoint must not change the planar cover.

    The pre-fix proxy path treated the two-vertex and densified lines as
    different geometries (414 vs 1274 cells at L10) solely because the extra
    discrete vertex prevented a bad parent-face rejection.
    """
    sparse = gm.LineString([(-50.0, -40.0), (50.0, -40.0)], crs=4326)
    dense = gm.LineString([(-50.0, -40.0), (0.0, -40.0), (50.0, -40.0)], crs=4326)
    visible: dict[str, set[str]] = {}
    for rule in ('center', 'within', 'overlap', 'bbox'):
        sparse_cells = {
            cell.token for cell in gm.s2_cover(sparse, level=10, cell_rule=rule)
        }
        dense_cells = {
            cell.token for cell in gm.s2_cover(dense, level=10, cell_rule=rule)
        }
        assert sparse_cells == dense_cells, rule
        visible[rule] = sparse_cells
    assert len(visible['overlap']) == 1274


def test_s2_pole_touching_sources_keep_every_pole_owner() -> None:
    """Pole closure may not be undone by non-pole vertex longitude windows."""
    sources = [
        (
            gm.LineString([(-60.0, 80.0), (-60.0, 90.0)], crs=4326),
            gm.Point(-60.0, 90.0, crs=4326),
        ),
        (
            gm.Polygon(
                [(-60.0, 80.0), (0.0, 90.0), (60.0, 80.0), (-60.0, 80.0)],
                crs=4326,
            ),
            gm.Point(0.0, 90.0, crs=4326),
        ),
    ]
    for source, pole in sources:
        assert gm.covers(source, pole)
        for level in (0, 4, 8):
            coverage = gm.s2_cover(source, level=level)
            owners = {cell.token for cell in gm.s2_cover(pole, level=level)}
            cells = {cell.token for cell in coverage}
            assert gm.covers(source, pole), f'{source.geometry_type} level={level}'
            assert owners <= cells, (
                f'{source.geometry_type} level={level}: {owners - cells}'
            )


def _s2_pole_shape_family(latitude: float, longitude: float) -> dict[str, gm.Geometry]:
    """Every grid carrier of one physical pole, including one-member wrappers."""
    near = 80.0 if latitude > 0.0 else -80.0
    line = [(longitude - 10.0, latitude), (longitude + 10.0, latitude)]
    cap = [
        (longitude - 10.0, near),
        (longitude + 10.0, near),
        (longitude + 10.0, latitude),
        (longitude - 10.0, latitude),
    ]
    point = gm.Point(longitude, latitude, crs=4326)
    single_line = gm.LineString(line, crs=4326)
    polygon = gm.Polygon(cap, crs=4326)
    return {
        'point': point,
        'line': single_line,
        'multi_point': gm.MultiPoint([(longitude, latitude)], crs=4326),
        'multi_line': gm.MultiLineString([line], crs=4326),
        'polygon': polygon,
        'multi_polygon': gm.MultiPolygon([cap], crs=4326),
        'collection': gm.GeometryCollection([polygon], crs=4326),
    }


def test_s2_pole_normalization_and_one_member_decomposition_are_closed() -> None:
    """Every accepted pole spelling shares one canonical source everywhere.

    The outer-ULP carrier is the regression: before construction, certificates,
    and membership all used the same normalized source, S2 could emit no cells
    while the resulting empty coverage claimed to cover its input.  The matrix
    includes all source kinds, every visible rule, both hemispheres, both ULP
    neighbours, and a depth sweep; one-member aggregate rows must be identical
    to their atomic counterpart rather than merely approximately equivalent.
    """
    atomic = {
        'point': 'point',
        'line': 'line',
        'multi_point': 'point',
        'multi_line': 'line',
        'polygon': 'polygon',
        'multi_polygon': 'polygon',
        'collection': 'polygon',
    }
    for pole in (90.0, -90.0):
        for latitude in (
            math.nextafter(pole, 0.0),
            math.nextafter(pole, math.copysign(math.inf, pole)),
        ):
            actual = _s2_pole_shape_family(latitude, 0.0)
            for name, source in actual.items():
                # The inward neighbour is a distinct stored double; only the
                # representation must disappear.  Compare every container to
                # its atomic component at that *same* latitude rather than
                # accidentally testing a clamp to the exact pole.
                reference = actual[atomic[name]]
                for level in range(5):
                    for rule in ('overlap', 'bbox', 'center', 'within'):
                        coverage = gm.s2_cover(source, level=level, cell_rule=rule)
                        assert {cell.token for cell in coverage} == {
                            cell.token
                            for cell in gm.s2_cover(
                                reference, level=level, cell_rule=rule
                            )
                        }, (pole, latitude, name, level, rule)


def test_s2_bbox_uses_the_same_atomic_union_and_global_budget() -> None:
    """Separated aggregate carriers cannot manufacture their gap's bbox cells."""
    components = (
        gm.Point(-120.0, 20.0, crs=4326),
        gm.Point(120.0, 20.0, crs=4326),
    )
    aggregates = (
        gm.MultiPoint(components, crs=4326),
        gm.MultiLineString(
            [[(-121.0, 20.0), (-120.0, 20.0)], [(120.0, 20.0), (121.0, 20.0)]],
            crs=4326,
        ),
        gm.GeometryCollection(components, crs=4326),
    )
    for aggregate in aggregates:
        if isinstance(aggregate, gm.MultiLineString):
            atomic = tuple(
                gm.LineString(line, crs=4326)
                for line in [
                    [(-121.0, 20.0), (-120.0, 20.0)],
                    [(120.0, 20.0), (121.0, 20.0)],
                ]
            )
        else:
            atomic = components
        expected = {
            cell.token
            for component in atomic
            for cell in gm.s2_cover(component, level=4, cell_rule='bbox')
        }
        covered = gm.s2_cover(
            aggregate, level=4, cell_rule='bbox', max_cells=len(expected)
        )
        assert {cell.token for cell in covered} == expected
        with pytest.raises(gm.GeometryError, match='max_cells'):
            gm.s2_cover(
                aggregate, level=4, cell_rule='bbox', max_cells=len(expected) - 1
            )


def test_s2_cover_superset_invariant_over_handwritten_corpus() -> None:
    """Every source point must lie in some returned cell.

    The source predicate can hold even when a discrete cell set omits the
    containing cell; this test pins that defect class.
    """
    level = 8
    corpus: list[tuple[gm.Geometry, list[gm.Point]]] = [
        (
            gm.LineString([(10.0, 20.0), (12.0, 20.0)], crs=4326),
            [
                gm.Point(10.0, 20.0, crs=4326),
                gm.Point(11.0, 20.0, crs=4326),
                gm.Point(12.0, 20.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(-50.0, -40.0), (50.0, -40.0)], crs=4326),
            [
                gm.Point(-50.0, -40.0, crs=4326),
                gm.Point(-25.0, -40.0, crs=4326),
                gm.Point(0.0, -40.0, crs=4326),
                gm.Point(25.0, -40.0, crs=4326),
                gm.Point(50.0, -40.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(-30.0, -10.0), (40.0, 25.0)], crs=4326),
            [
                gm.Point(-30.0, -10.0, crs=4326),
                gm.Point(5.0, 7.5, crs=4326),
                gm.Point(40.0, 25.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(179.0, 0.0), (-179.0, 0.0)], crs=4326),
            [
                gm.Point(179.0, 0.0, crs=4326),
                gm.Point(180.0, 0.0, crs=4326),
                gm.Point(-180.0, 0.0, crs=4326),
                gm.Point(-179.0, 0.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(0.0, 80.0), (10.0, 85.0)], crs=4326),
            [
                gm.Point(0.0, 80.0, crs=4326),
                gm.Point(5.0, 82.5, crs=4326),
                gm.Point(10.0, 85.0, crs=4326),
            ],
        ),
        (
            gm.Polygon(
                [
                    (-10.0, -50.0),
                    (10.0, -50.0),
                    (10.0, -30.0),
                    (-10.0, -30.0),
                    (-10.0, -50.0),
                ],
                crs=4326,
            ),
            [
                gm.Point(0.0, -40.0, crs=4326),
                gm.Point(-10.0, -50.0, crs=4326),
                gm.Point(10.0, -30.0, crs=4326),
            ],
        ),
    ]
    for source, probes in corpus:
        coverage = gm.s2_cover(source, level=level)
        for probe in probes:
            assert gm.covers(source, probe), f'{source!r} covers {probe!r}'
            # Construct the closed S2 owner independently
            # from the stored probe and demand an emitted ancestor; removing
            # that owner leaves the source predicate true but fails here.
            owner = gm.S2Cell(probe, level=level)
            assert any(cell.contains(owner) for cell in coverage), (
                f'{source!r} has no emitted level-{level} owner for {probe!r}'
            )


@pytest.mark.parametrize(
    ('longitude', 'latitude', 'expected_level_2'),
    [
        (0.0, 0.0, {'05', '0f', '11', '1b'}),
        (90.0, 0.0, {'25', '2f', '31', '3b'}),
        (-90.0, 0.0, {'85', '8f', '91', '9b'}),
        (180.0, 0.0, {'65', '6f', '71', '7b'}),
        (37.0, 90.0, {'45', '4f', '51', '5b'}),
        (-37.0, -90.0, {'a5', 'af', 'b1', 'bb'}),
    ],
)
def test_s2_closed_point_owners_are_independent_of_source_representation(
    longitude: float,
    latitude: float,
    expected_level_2: set[str],
) -> None:
    """Hard-coded cube-seam/pole owners; never ask the Point coverer for them.

    The literal L2 owner sets are the independent oracle; the same equality
    is checked at neighbouring levels and through every one-member public
    carrier.  Whether an implementation shares an internal point owner is
    deliberately irrelevant to this public oracle.
    """
    point = gm.Point(longitude, latitude, crs=4326)
    sources = [
        point,
        gm.MultiPoint([point], crs=4326),
        gm.GeometryCollection([point], crs=4326),
        gm.LineString([(longitude, latitude), (longitude, latitude)], crs=4326),
    ]
    for level in (1, 2, 5):
        for rule in ('overlap', 'bbox'):
            token_sets = [
                {
                    cell.token
                    for cell in gm.s2_cover(source, level=level, cell_rule=rule)
                }
                for source in sources
            ]
            assert all(tokens == token_sets[0] for tokens in token_sets[1:])
            if level == 2:
                assert token_sets[0] == expected_level_2


@pytest.mark.parametrize(
    ('longitude', 'latitude', 'expected'),
    [
        (
            37.0,
            math.nextafter(90.0, 0.0),
            {1: {'44'}, 2: {'45'}, 5: {'4554'}},
        ),
        (
            37.0,
            math.nextafter(90.0, math.inf),
            {
                1: {'44', '4c', '54', '5c'},
                2: {'45', '4f', '51', '5b'},
                5: {'4554', '4ffc', '5004', '5aac'},
            },
        ),
        (
            -37.0,
            math.nextafter(-90.0, 0.0),
            {1: {'bc'}, 2: {'bb'}, 5: {'baac'}},
        ),
        (
            -37.0,
            math.nextafter(-90.0, -math.inf),
            {
                1: {'a4', 'ac', 'b4', 'bc'},
                2: {'a5', 'af', 'b1', 'bb'},
                5: {'a554', 'affc', 'b004', 'baac'},
            },
        ),
    ],
)
def test_s2_pole_ulp_point_owners_match_literal_closed_cell_oracles(
    longitude: float,
    latitude: float,
    expected: dict[int, set[str]],
) -> None:
    """ULP-neighbour ownership is literal, not another call into the coverer.

    Both signs of the inward and outward ULP are material: the stored double
    on one side is a single interior owner, while the other is normalized to
    the closed pole and owns four face cells.  `center` and `within` correctly
    remain empty for all point-like carriers; overlap/bbox must emit exactly
    the hard-coded S2 tokens at each level.
    """
    point = gm.Point(longitude, latitude, crs=4326)
    sources = (
        point,
        gm.MultiPoint([point], crs=4326),
        gm.GeometryCollection([point], crs=4326),
        gm.LineString([(longitude, latitude), (longitude, latitude)], crs=4326),
    )
    for source in sources:
        for level, overlap_bbox in expected.items():
            for rule in ('overlap', 'bbox', 'center', 'within'):
                actual = {
                    cell.token
                    for cell in gm.s2_cover(source, level=level, cell_rule=rule)
                }
                assert actual == (
                    overlap_bbox if rule in {'overlap', 'bbox'} else set()
                )


def test_s2_coverage_matches_planar_semantics_at_the_seam() -> None:
    line = gm.LineString([(179.0, -1.0), (-179.0, 1.0)], crs=4326)
    coverage = gm.s2_cover(line, level=6, max_cells=16)
    _ = gm.s2_cover(gm.box(-200, -10, 200, 10, crs=4326, wrap='split'), level=6)
    _ = gm.s2_cover(gm.box(-180, -10, 180, 10, crs=4326), level=6)
    assert line.crosses_antimeridian
    assert gm.contains(line, gm.Point(0.0, 0.0056, crs=4326)) == gm.intersects(
        line, gm.Point(0.0, 0.0056, crs=4326)
    )
    lons = sorted(cell.center.x for cell in coverage)
    assert lons[0] < -170 and lons[-1] > 170
    seam = gm.box(179.5, -1.0, -179.5, 1.0, crs=4326, wrap='split')
    seam_lons = [cell.center.x for cell in gm.s2_cover(seam, level=6)]
    assert any(lon > 170 for lon in seam_lons)
    assert any(lon < -170 for lon in seam_lons)
    assert all(abs(lon) > 170 for lon in seam_lons)
    np.testing.assert_array_equal(
        gm.covers(
            gm.box(-200, -10, 200, 10, crs=4326, wrap='split'),
            gm.points([0.0, 179.5, -179.5], [0.0, 0.0, 0.0], crs=4326),
        ),
        [True, True, True],
    )
    np.testing.assert_array_equal(
        gm.covers(
            gm.box(-180, -10, 180, 10, crs=4326),
            gm.points([0.0, 179.5, -179.5], [0.0, 0.0, 0.0], crs=4326),
        ),
        [True, True, True],
    )


def test_s2_split_antimeridian_box_covers_the_seam_not_the_world() -> None:
    narrow = gm.box(170, -10, -170, 10, crs=4326, wrap='split')
    world = gm.box(-180, -10, 180, 10, crs=4326)
    narrow_cells = gm.s2_cover(narrow, level=4)
    world_cells = gm.s2_cover(world, level=4)
    assert {c.id for c in narrow_cells} != {c.id for c in world_cells}
    assert all(abs(c.center.x) > 160 for c in narrow_cells)


def test_s2_cover_normalizes_raw_crossing_polygon_like_s2sphere() -> None:
    # Unsplit seam rectangle: planar cover is the false-middle world band.
    # Geographic auto-split yields the 64-cell L8 covering (s2sphere oracle).
    seam = gm.Polygon(
        [(179.0, -1.0), (-179.0, -1.0), (-179.0, 1.0), (179.0, 1.0)],
        crs=4326,
    )
    coverage = gm.s2_cover(seam, level=8)
    assert len(coverage) == 64
    assert all(abs(cell.center.x) > 170 for cell in coverage)
    assert gm.contains(seam, gm.Point(179.5, 0.0, crs=4326))
    assert not gm.contains(seam, gm.Point(0.0, 0.0, crs=4326))


def test_s2_bounding_cell_point_aggregates_use_bbox_path() -> None:
    """Multi-point aggregates share the R18 bbox path (not leaf-LCA).

    Oracle repros: leaf-LCA was non-containing / over-rejecting; bbox path
    matches bounds/box and contains inset envelope probes.
    """
    # Repro 1: leaf-LCA 'a8eb4'/L7 missed the inset bbox point; bbox → L6.
    mp1 = gm.MultiPoint([(170.0, -60.0), (170.2, -59.8)], crs=4326)
    box1 = gm.box(170.0, -60.0, 170.2, -59.8, crs=4326)
    cell1 = gm.s2_bounding_cell(mp1)
    assert cell1 == gm.s2_bounding_cell(box1)
    assert cell1.token == 'a8eb'
    assert cell1.level == 6
    assert cell1 == gm.s2_bounding_cell(
        gm.GeometryCollection(
            [gm.Point(170.0, -60.0, crs=4326), gm.Point(170.2, -59.8, crs=4326)],
            crs=4326,
        )
    )
    assert cell1 == gm.s2_bounding_cell(
        gm.GeometryArray([
            gm.Point(170.0, -60.0, crs=4326),
            gm.Point(170.2, -59.8, crs=4326),
        ])
    )
    inset = gm.Point(170.02, -59.82, crs=4326)
    assert cell1.contains(gm.S2Cell(inset.x, inset.y, level=30)) or gm.covers(
        cell1.polygon, inset
    )

    # Repro 2: leaf-LCA multi-face raise; face root '3' closed-contains bbox.
    mp2 = gm.MultiPoint([(45.0, -20.0), (45.2, -19.8)], crs=4326)
    box2 = gm.box(45.0, -20.0, 45.2, -19.8, crs=4326)
    cell2 = gm.s2_bounding_cell(mp2)
    assert cell2 == gm.s2_bounding_cell(box2)
    assert cell2.token == '3'
    assert cell2.level == 0

    # Single point still exact L30 leaf.
    pt = gm.Point(13.4, 52.5, crs=4326)
    leaf = gm.s2_bounding_cell(pt)
    assert leaf.level == 30
    assert leaf == gm.S2Cell(13.4, 52.5, level=30)

    # Soundness matrix: multipoint ≡ box; inset center contained when same face.
    for minx, miny, size in (
        (0.0, 0.0, 0.2),
        (13.4, 52.5, 0.01),
        (170.0, -60.0, 0.2),
        (45.0, -20.0, 0.2),
        (-40.0, 10.0, 1.0),
    ):
        maxx, maxy = minx + size, miny + size
        mp = gm.MultiPoint([(minx, miny), (maxx, maxy)], crs=4326)
        bx = gm.box(minx, miny, maxx, maxy, crs=4326)
        got = gm.s2_bounding_cell(mp)
        assert got == gm.s2_bounding_cell(bx), (minx, miny, size, got.token)
        cx, cy = (minx + maxx) / 2.0, (miny + maxy) / 2.0
        probe = gm.S2Cell(cx, cy, level=30)
        if probe.token[0] == got.token[0] or got.level == 0:
            assert got.contains(probe) or gm.covers(
                got.polygon, gm.Point(cx, cy, crs=4326)
            ), (got.token, cx, cy)


def test_s2_bounding_cell_cube_vertex_microbox_never_non_containing() -> None:
    """Cube-vertex 1e-4° boxes: containing cell or multi-face raise (never non-contain).

    Oracle repro: absolute closed-halfspace EPS false-accepted face root ``7``
    for ``[-180,-45,-179.9999,-44.9999]`` while an interior probe mapped to
    another face. Relative halfspace slack rejects that face; multi-face raise
    is sound. Sweep the eight cube-edge midpoints at ±45° lat.
    """
    # Repro: multi-face raise (no single face closed-contains the envelope).
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell([-180.0, -45.0, -179.9999, -44.9999])

    non_containing = 0
    raised = 0
    ok = 0
    for lon in (-180.0, -90.0, 0.0, 90.0):
        for lat in (-45.0, 45.0):
            minx, miny = lon, lat
            maxx, maxy = lon + 1e-4, lat + 1e-4
            if maxx > 180.0:
                minx, maxx = lon - 1e-4, lon
            if maxy > 90.0:
                miny, maxy = lat - 1e-4, lat
            try:
                cell = gm.s2_bounding_cell([minx, miny, maxx, maxy])
            except gm.GeometryError as exc:
                if 'no single S2 cell' not in str(exc):
                    raise
                raised += 1
                continue
            miss = 0
            for i in range(1, 10):
                for j in range(1, 10):
                    x = minx + (maxx - minx) * i / 10.0
                    y = miny + (maxy - miny) * j / 10.0
                    leaf = gm.S2Cell(x, y, level=30)
                    if not (
                        cell.contains(leaf)
                        or gm.covers(cell.polygon, gm.Point(x, y, crs=4326))
                    ):
                        miss += 1
            if miss:
                non_containing += 1
            else:
                ok += 1
    assert non_containing == 0, (ok, raised, non_containing)
    assert ok + raised == 8
    # Point path stays exact L30.
    assert gm.s2_bounding_cell(gm.Point(13.4, 52.5, crs=4326)).level == 30


def test_s2_bounding_cell_signed_zero_bbox_is_level30_leaf() -> None:
    """Signed-zero point bbox uses plain ==, not to_bits (regression).

    ``to_bits`` treated ``-0.0`` ≠ ``+0.0``, so ``[-0.0,0,0,0]`` missed the
    point-degenerate path and returned a face root (level 0) while
    ``[0,0,0,0]`` correctly returned the level-30 leaf.
    """
    pos = gm.s2_bounding_cell([0.0, 0.0, 0.0, 0.0])
    neg = gm.s2_bounding_cell([-0.0, 0.0, 0.0, 0.0])
    assert pos == neg
    assert pos.level == 30
    assert neg.level == 30
    assert pos == gm.S2Cell(0.0, 0.0, level=30)

    # Point-degenerate with a -0.0 ordinate on one axis only.
    mixed = gm.s2_bounding_cell([-0.0, 1.0, 0.0, 1.0])
    plain = gm.s2_bounding_cell([0.0, 1.0, 0.0, 1.0])
    assert mixed == plain
    assert mixed.level == 30
    assert mixed == gm.S2Cell(0.0, 1.0, level=30)


def test_s2_cover_partial_polar_overlap_fails_open_without_vertex_negative() -> None:
    """Partial-lon polar bounds retain ambiguous closed polar wedges.

    A full-longitude ``rect_bound`` is a sound outer bound, while four non-pole
    vertices are not a negative certificate. The ambiguous level-4 wedges stay
    in the overlap cover rather than being silently pruned.

    Antimeridian-touching partial polar boxes must still cover cells that meet
    the shared ±180 meridian (east spelling ``box(170,80,180,85)`` includes L4
    parents of corners at lon=180: ``507``/``501``), matching the west spelling.
    """
    partial = gm.box(0.0, 80.0, 10.0, 85.0, crs=4326)
    tokens = {c.token for c in gm.s2_cover(partial, level=4, cell_rule='overlap')}
    assert tokens == {'455', '457', '4f9', '4ff', '501', '5ab'}

    # East vs west antimeridian spellings: shared ±180 must keep seam cells.
    east = gm.box(170.0, 80.0, 180.0, 85.0, crs=4326)
    west = gm.box(-180.0, 80.0, -170.0, 85.0, crs=4326)
    east_tok = {c.token for c in gm.s2_cover(east, level=4, cell_rule='overlap')}
    west_tok = {c.token for c in gm.s2_cover(west, level=4, cell_rule='overlap')}
    # L4 parents of corners (180,80)/(180,85) == (-180,80)/(-180,85).
    for lon, lat in ((180.0, 80.0), (180.0, 85.0), (-180.0, 80.0), (-180.0, 85.0)):
        parent = gm.S2Cell(lon, lat, level=30).parent(4)
        assert parent.token in east_tok, (lon, lat, parent.token, east_tok)
        assert parent.token in west_tok, (lon, lat, parent.token, west_tok)
    assert {'501', '507'} <= east_tok
    assert {'501', '507'} <= west_tok

    cap = gm.box(-180.0, 80.0, 180.0, 90.0, crs=4326)
    within_l4 = gm.s2_cover(cap, level=4, cell_rule='within')
    assert {c.token for c in within_l4} == {'455', '4ff', '501', '5ab'}
    assert len(gm.s2_cover(cap, level=4, cell_rule='overlap')) == 16
    assert len(gm.s2_cover(cap, level=8, cell_rule='within')) == 2848

    seam = gm.Polygon(
        [(179.0, -1.0), (-179.0, -1.0), (-179.0, 1.0), (179.0, 1.0)],
        crs=4326,
    )
    assert len(gm.s2_cover(seam, level=8, cell_rule='overlap')) == 64
    _assert_seam_within_matches_linear_source(seam)

    # Non-polar within: interior ⊆ overlap (Berlin box has interior cells at L12).
    berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    b_within = {c.token for c in gm.s2_cover(berlin, level=12, cell_rule='within')}
    b_overlap = {c.token for c in gm.s2_cover(berlin, level=12, cell_rule='overlap')}
    assert b_within
    assert b_within <= b_overlap


def test_s2_cover_within_polar_and_antimeridian_honors_source_geometry() -> None:
    """Within preserves polar interiors and raw antimeridian source edges.

    Outer/overlap fixed-level covers stay at the prior oracles (polar L4 = 16,
    antimeridian L8 = 64).
    """
    cap = gm.box(-180.0, 80.0, 180.0, 90.0, crs=4326)
    within_l4 = gm.s2_cover(cap, level=4, cell_rule='within')
    assert {c.token for c in within_l4} == {'455', '4ff', '501', '5ab'}
    assert len(gm.s2_cover(cap, level=4, cell_rule='overlap')) == 16
    assert len(gm.s2_cover(cap, level=8, cell_rule='within')) == 2848

    seam = gm.Polygon(
        [(179.0, -1.0), (-179.0, -1.0), (-179.0, 1.0), (179.0, 1.0)],
        crs=4326,
    )
    assert len(gm.s2_cover(seam, level=8, cell_rule='overlap')) == 64
    _assert_seam_within_matches_linear_source(seam)

    # Non-polar within: interior ⊆ overlap (Berlin box has interior cells at L12).
    berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    b_within = {c.token for c in gm.s2_cover(berlin, level=12, cell_rule='within')}
    b_overlap = {c.token for c in gm.s2_cover(berlin, level=12, cell_rule='overlap')}
    assert b_within
    assert b_within <= b_overlap


def test_s2_set_utilities_mirror_h3() -> None:
    parent = gm.S2Cell(21.0, 52.0, level=8)
    child = parent.children(10)[0]
    children = gm.CellArray([parent], type=gm.S2Cell).uncompact(10)
    assert len(children) == 16
    assert all(c.level == 10 for c in children)
    assert len(gm.CellArray([parent, child], type=gm.S2Cell).uncompact(10)) == 16
    assert list(children.compact()) == [parent]
    floored = children.compact(9)
    assert len(floored) == 4
    assert all(c.level == 9 for c in floored)
    assert len(children[1:].compact()) > 1
    with pytest.raises(ValueError, match='level'):
        children.uncompact(8)


def test_s2_cell_set_algebra_is_hierarchy_aware() -> None:
    cell = gm.S2Cell(13.4, 52.5, level=10)
    children = cell.children()
    assert list(gm.s2_union([cell], children[:2])) == [cell]
    assert list(gm.s2_union(children[:2], children[2:])) == [cell]
    assert [c.level for c in gm.s2_intersection([cell], children[:2])] == [11, 11]
    difference = gm.s2_difference([cell], children[:1])
    assert sorted(int(c) for c in difference) == sorted(int(c) for c in children[1:])
    assert list(gm.s2_difference(children[:1], children[1:])) == [children[0]]


def test_s2_to_polygon_handles_the_antimeridian() -> None:
    coverage = gm.s2_cover(gm.Point(180, 0, crs=4326), level=5)
    assert len(coverage) > 0
    outline = coverage.to_polygon()
    assert outline.is_valid
    assert outline.geometry_type in ('Polygon', 'MultiPolygon')
    for part in gm.parts(outline):
        bounds = part.bounds
        assert bounds is not None
        minx, _, maxx, _ = bounds
        assert minx >= 170.0 or maxx <= -170.0


def test_s2_to_polygon_topology_dissolve_conserves_area() -> None:
    cases = [
        gm.s2_cover(gm.box(-122.55, 37.7, -122.35, 37.85, crs=4326), level=14),
        gm.s2_cover(gm.box(43, 33, 47, 37, crs=4326), level=9),
        gm.s2_cover(
            gm.from_wkt(
                'POLYGON((-120 35,-110 35,-110 45,-120 45,-120 35),(-117 38,-113 38,-113 42,-117 42,-117 38))',
                crs=4326,
            ),
            level=8,
        ),
        gm.s2_cover(
            gm.from_wkt(
                'MULTIPOLYGON(((-120 35,-118 35,-118 37,-120 37,-120 35)),((-100 40,-98 40,-98 42,-100 42,-100 40)))',
                crs=4326,
            ),
            level=9,
        ),
    ]
    for coverage in cases:
        outline = coverage.to_polygon()
        assert outline.is_valid
        cell_area = sum(cell.area for cell in coverage)
        assert abs(outline.area - cell_area) / cell_area < 0.01


def test_dissolve_is_order_independent_for_all_live_grid_dissolvers() -> None:
    cases = [
        (gm.h3_cover(gm.box(0, 0, 2, 2, crs=4326), resolution=4), gm.H3Cell),
        (gm.s2_cover(gm.box(0, 0, 2, 2, crs=4326), level=8), gm.S2Cell),
        (gm.geohash_cover(gm.box(0, 0, 2, 2, crs=4326), precision=4), gm.GeohashCell),
        (gm.tile_cover(gm.box(0, 0, 2, 2, crs=4326), zoom=8), gm.Tile),
    ]
    for coverage, cell_type in cases:
        values = list(coverage)
        shuffled = list(reversed(values)) + values[:1]
        canonical = gm.CellArray(values, type=cell_type).to_polygon()
        rebuilt = gm.CellArray(shuffled, type=cell_type).to_polygon()
        assert gm.equals(rebuilt, canonical)


def test_s2_to_polygon_pole_cap_conserves_area() -> None:
    for box in (
        gm.box(-180, -90, 180, -85, crs=4326),
        gm.box(-180, 85, 180, 90, crs=4326),
        gm.box(-180, 70, 180, 90, crs=4326),
    ):
        for level in (3, 5):
            coverage = gm.s2_cover(box, level=level)
            outline = coverage.to_polygon()
            assert outline.is_valid
            cell_area = sum(cell.area for cell in coverage)
            assert abs(outline.area - cell_area) / cell_area < 0.02


def test_rect_to_polygon_topology_dissolve_conserves_area() -> None:
    cases = [
        gm.geohash_cover(gm.box(13.2, 52.4, 13.6, 52.6, crs=4326), precision=6),
        gm.tile_cover(gm.box(13.2, 52.4, 13.6, 52.6, crs=4326), zoom=13),
        gm.geohash_cover(
            gm.from_wkt(
                'POLYGON((-5 -5,5 -5,5 5,-5 5,-5 -5),(-2 -2,2 -2,2 2,-2 2,-2 -2))',
                crs=4326,
            ),
            precision=5,
        ),
        gm.tile_cover(
            gm.from_wkt(
                'MULTIPOLYGON(((10 10,12 10,12 12,10 12,10 10)),((20 20,22 20,22 22,20 22,20 20)))',
                crs=4326,
            ),
            zoom=10,
        ),
        gm.geohash_cover(gm.box(-180, 86, 180, 90, crs=4326), precision=3),
        gm.geohash_cover(gm.box(-180, -90, 180, -86, crs=4326), precision=3),
    ]
    for coverage in cases:
        outline = coverage.to_polygon()
        assert outline.is_valid
        cell_area = sum(cell.area for cell in coverage)
        assert abs(outline.area - cell_area) / cell_area < 0.02
        assert (coverage.compact().to_polygon() ^ outline).area <= 1e-06


def test_rect_cover_compact_uncompact() -> None:
    box = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
    cases = [
        ('geohash', gm.geohash_cover(box, precision=7), 7),
        ('tiles', gm.tile_cover(box, zoom=13), 13),
    ]
    for _name, coverage, depth in cases:
        compacted = coverage.compact()
        assert len(compacted) <= len(coverage)
        assert (compacted.to_polygon() ^ coverage.to_polygon()).area <= 1e-09
        restored = compacted.uncompact(depth)
        assert set(restored) == set(coverage)
    coarse = gm.geohash_cover(box, precision=5)
    with pytest.raises(gm.GeometryError, match='must be >='):
        coarse.uncompact(4)


def test_s2_coverage_closed_cell_and_partition_properties() -> None:
    seam_point = gm.Point(180, 0, crs=4326)
    tokens = {cell.token for cell in gm.s2_cover(seam_point, level=5)}
    # A cube-seam/grid-vertex point has four closed S2 owners.  These are
    # literal independent identities, not derived through the Point coverer.
    assert tokens == {'6554', '6ffc', '7004', '7aac'}
    polygon = gm.Polygon([(20.2, 51.2), (21.8, 51.4), (20.9, 52.8)], crs=4326)
    coverage = gm.s2_cover(polygon, level=8)
    outer = {cell.token for cell in coverage}
    assert outer


def test_geohash_and_tile_coverage_classify_exactly() -> None:
    box = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
    for coverage, cover_fn, depth_attr in [
        (gm.geohash_cover(box, precision=5), gm.geohash_cover, 'precision'),
        (gm.tile_cover(box, zoom=10), gm.tile_cover, 'zoom'),
    ]:
        depth = {depth_attr: getattr(coverage[0], depth_attr)}
        inter = cover_fn(box, cell_rule='overlap', **depth)
        center = cover_fn(box, cell_rule='center', **depth)
        contain = cover_fn(box, cell_rule='within', **depth)
        assert len(inter) >= len(center) >= len(contain)
        assert all(gm.intersects(box, cell.polygon) for cell in inter)
        assert gm.contains(box, gm.Point(13.4, 52.5, crs=4326))
        assert not gm.contains(box, gm.Point(0, 0, crs=4326))
        assert gm.contains_xy(box, 13.4, 52.5)
        np.testing.assert_array_equal(
            gm.contains_xy(box, [13.4, 0.0], [52.5, 0.0]), [True, False]
        )
        assert gm.intersects_xy(box, 13.2, 52.4)
        polys = coverage.polygon
        assert len(polys) == len(coverage)
        assert all(p.geometry_type == 'Polygon' for p in polys)
        assert coverage[0] in coverage
        assert next(iter(coverage)) == coverage[0]
        if len(coverage) >= 2:
            sliced = coverage[0:2]
            assert isinstance(sliced, gm.CellArray)
            assert len(sliced) == 2
    box4 = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
    for coverage in (
        gm.h3_cover(box4, resolution=6),
        gm.s2_cover(box4, level=10),
        gm.geohash_cover(box4, precision=5),
        gm.tile_cover(box4, zoom=10),
    ):
        assert isinstance(coverage, gm.CellArray)
    coarse = gm.geohash_cover(box, precision=5, cell_rule='within')
    assert coarse
    assert all(gm.within(cell.polygon, box) for cell in coarse)
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.h3_cover(gm.from_wkt('POLYGON EMPTY', crs=4326), resolution=5)
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.s2_cover(gm.from_wkt('POLYGON EMPTY', crs=4326), level=10)
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.geohash_cover(gm.from_wkt('POLYGON EMPTY'), precision=5)
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.tile_cover(gm.from_wkt('POLYGON EMPTY', crs=4326), zoom=10)
    with pytest.raises(gm.GeometryError, match='precision'):
        gm.geohash_cover(box, precision=13)
    with pytest.raises(gm.GeometryError, match='zoom'):
        gm.tile_cover(box, zoom=30)
