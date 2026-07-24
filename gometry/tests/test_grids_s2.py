"""H3 and S2 discrete global grids — cells, boundaries, coverage,
exact membership predicates, compaction, and antimeridian-aware bounds.
"""

import operator

import gometry as gm
import numpy as np
import pytest


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
    assert cell.center.crs == 'EPSG:4326'
    assert boundary.geometry_type == 'Polygon'
    assert boundary.crs == 'EPSG:4326'
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
    assert [value.token for value in projected_coverage.cells] == [
        value.token for value in coverage.cells
    ]
    assert coverage.min_level == 12
    assert coverage.max_level == 12
    assert coverage.level == 12
    assert all(cell.level == 12 for cell in coverage)
    assert list(coverage) == list(coverage.cells)
    assert coverage.cells[0] in coverage
    assert int(coverage.cells[0]) in coverage
    assert coverage.cells[0].token in coverage
    assert gm.S2Cell(30.0, 52.0, level=12) not in coverage
    assert coverage.cells.polygon.crs == 'EPSG:4326'
    with_parents = coverage.with_parents(min_level=10)
    assert set(map(str, coverage.cells)) <= set(map(str, with_parents.cells))
    assert len(set(map(str, with_parents.cells))) == len(with_parents)
    assert {cell.level for cell in with_parents.cells} <= {10, 11, 12}
    assert any(cell.level == 10 for cell in with_parents.cells)
    assert any(cell.level == 11 for cell in with_parents.cells)
    np.testing.assert_array_equal(
        coverage.covers(gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)), [True, False]
    )
    np.testing.assert_array_equal(
        projected_coverage.covers(
            gm.points([21.0, 30.0], [52.0, 52.0], crs=4326).to_crs(32634)
        ),
        [True, False],
    )
    assert not coverage.with_parents(min_level=0).covers(gm.Point(30.0, 52.0, crs=4326))
    with pytest.raises(ValueError):
        gm.s2_cover(gm.box(0, 0, 1, 1), max_cells=0)
    with pytest.raises(ValueError, match='S2 min_level'):
        coverage.with_parents(min_level=31)


def test_s2_level_budget_is_explicit() -> None:
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    fixed = gm.s2_cover(area, level=8)
    assert fixed.level == 8
    assert fixed.min_level == fixed.max_level == 8
    adaptive = gm.s2_cover(area, min_level=6, max_level=10, target_cells=16)
    assert adaptive.level is None
    assert adaptive.min_level == 6
    assert adaptive.max_level == 10
    assert adaptive.max_cells == 1_000_000
    assert adaptive.target_cells == 16
    with pytest.raises(gm.GeometryError, match='level cannot be combined'):
        gm.s2_cover(area, level=8, min_level=6)
    with pytest.raises(gm.GeometryError, match='level cannot be combined'):
        gm.s2_cover(area, level=8, max_level=10)


def test_s2_coverage_level_mod_propagation() -> None:
    """``S2Coverage.level_mod`` is stored, survives compact/uncompact/pickle."""
    import pickle

    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)

    default = gm.s2_cover(area, level=8)
    assert default.level_mod == 1

    fixed = gm.s2_cover(area, level=8, level_mod=2)
    assert fixed.level_mod == 2
    assert fixed.level == 8
    assert fixed.compact().level_mod == 2
    # uncompact back to the cover level keeps the mod
    assert fixed.compact().uncompact(8).level_mod == 2
    assert pickle.loads(pickle.dumps(fixed)).level_mod == 2

    adaptive = gm.s2_cover(area, min_level=6, max_level=10, target_cells=16, level_mod=3)
    assert adaptive.level_mod == 3
    assert adaptive.level is None
    assert adaptive.compact().level_mod == 3
    assert pickle.loads(pickle.dumps(adaptive)).level_mod == 3


def test_s2_membership_is_exact_against_geometry_not_bounds() -> None:
    triangle = gm.Polygon([(0, 0), (10, 0), (0, 10)], crs=4326)
    coverage = gm.s2_cover(triangle, level=8)
    inside = gm.Point(1.0, 1.0, crs=4326)
    bbox_only = gm.Point(8.0, 8.0, crs=4326)
    assert coverage.covers(inside) is True
    assert coverage.covers(bbox_only) is False
    np.testing.assert_array_equal(
        coverage.covers(gm.GeometryArray([inside, bbox_only])), [True, False]
    )
    edge = gm.Point(5.0, 0.0, crs=4326)
    assert coverage.covers(edge) is True
    assert coverage.contains(edge) is False
    assert coverage.contains(inside) is True


def test_s2_cell_rule_overlap_is_exact_and_bbox_is_loose() -> None:
    triangle = gm.Polygon([(0, 0), (10, 0), (0, 10)], crs=4326)
    overlap = gm.s2_cover(triangle, level=8, cell_rule='overlap')
    bbox = gm.s2_cover(triangle, level=8, cell_rule='bbox')
    bbox_only = gm.S2Cell(8.0, 8.0, level=8)
    assert bbox_only not in overlap
    assert bbox_only in bbox
    assert {cell.token for cell in overlap.cells} < {cell.token for cell in bbox.cells}
    probe = gm.Point(8.0, 8.0, crs=4326)
    assert overlap.covers(probe) is False
    assert bbox.covers(probe) is False


def test_s2_explain_names_visible_cells_and_exact_membership() -> None:
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    coverage = gm.s2_cover(area, level=8, cell_rule='bbox')
    plan = coverage.explain()
    assert plan[0].startswith('s2 coverage: level 8, cell_rule bbox, ')
    assert 'coverage partition:' in plan[1]
    assert 'exact source-geometry predicates' in plan[2]


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
    assert compacted.covers(gm.Point(21.0, 52.0, crs=4326)) is True
    assert compacted.covers(gm.Point(30.0, 52.0, crs=4326)) is False


def test_s2_coverage_matches_planar_semantics_at_the_seam() -> None:
    line = gm.LineString([(179.0, -1.0), (-179.0, 1.0)], crs=4326)
    coverage = gm.s2_cover(line, level=6, max_cells=16)
    full_longitude = gm.s2_cover(
        gm.box(-200, -10, 200, 10, crs=4326, wrap='split'), level=6
    )
    exact_full_longitude = gm.s2_cover(gm.box(-180, -10, 180, 10, crs=4326), level=6)
    assert line.crosses_antimeridian
    assert coverage.covers(gm.Point(0.0, 0.0056, crs=4326)) == coverage.intersects(
        gm.Point(0.0, 0.0056, crs=4326)
    )
    lons = sorted(cell.center.x for cell in coverage.cells)
    assert lons[0] < -170 and lons[-1] > 170
    seam = gm.box(179.5, -1.0, -179.5, 1.0, crs=4326, wrap='split')
    seam_lons = [cell.center.x for cell in gm.s2_cover(seam, level=6).cells]
    assert any(lon > 170 for lon in seam_lons)
    assert any(lon < -170 for lon in seam_lons)
    assert all(abs(lon) > 170 for lon in seam_lons)
    np.testing.assert_array_equal(
        full_longitude.covers(
            gm.points([0.0, 179.5, -179.5], [0.0, 0.0, 0.0], crs=4326)
        ),
        [True, True, True],
    )
    np.testing.assert_array_equal(
        exact_full_longitude.covers(
            gm.points([0.0, 179.5, -179.5], [0.0, 0.0, 0.0], crs=4326)
        ),
        [True, True, True],
    )


def test_s2_split_antimeridian_box_covers_the_seam_not_the_world() -> None:
    narrow = gm.box(170, -10, -170, 10, crs=4326, wrap='split')
    world = gm.box(-180, -10, 180, 10, crs=4326)
    narrow_cells = gm.s2_cover(narrow, level=4).cells
    world_cells = gm.s2_cover(world, level=4).cells
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
    assert len(coverage.cells) == 64
    assert all(abs(cell.center.x) > 170 for cell in coverage.cells)
    assert coverage.covers(gm.Point(179.5, 0.0, crs=4326))
    assert not coverage.covers(gm.Point(0.0, 0.0, crs=4326))


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


def test_s2_cover_partial_polar_overlap_excludes_opposite_wedges() -> None:
    """Partial-lon polar box: overlap must not force-include opposite polar wedges.

    Oracle: ``box(0,80,10,85)`` L4 overlap == ``{455,457,4f9,4ff}`` (no ``501``/
    ``5ab``). Full-longitude caps and antimeridian R19 cases stay intact.

    Antimeridian-touching partial polar boxes must still cover cells that meet
    the shared ±180 meridian (east spelling ``box(170,80,180,85)`` includes L4
    parents of corners at lon=180: ``507``/``501``), matching the west spelling.
    """
    partial = gm.box(0.0, 80.0, 10.0, 85.0, crs=4326)
    tokens = {c.token for c in gm.s2_cover(partial, level=4, cell_rule='overlap').cells}
    assert tokens == {'455', '457', '4f9', '4ff'}
    assert '501' not in tokens and '5ab' not in tokens

    # East vs west antimeridian spellings: shared ±180 must keep seam cells.
    east = gm.box(170.0, 80.0, 180.0, 85.0, crs=4326)
    west = gm.box(-180.0, 80.0, -170.0, 85.0, crs=4326)
    east_tok = {c.token for c in gm.s2_cover(east, level=4, cell_rule='overlap').cells}
    west_tok = {c.token for c in gm.s2_cover(west, level=4, cell_rule='overlap').cells}
    # L4 parents of corners (180,80)/(180,85) == (-180,80)/(-180,85).
    for lon, lat in ((180.0, 80.0), (180.0, 85.0), (-180.0, 80.0), (-180.0, 85.0)):
        parent = gm.S2Cell(lon, lat, level=30).parent(4)
        assert parent.token in east_tok, (lon, lat, parent.token, east_tok)
        assert parent.token in west_tok, (lon, lat, parent.token, west_tok)
    assert {'501', '507'} <= east_tok
    assert {'501', '507'} <= west_tok

    cap = gm.box(-180.0, 80.0, 180.0, 90.0, crs=4326)
    within_l4 = gm.s2_cover(cap, level=4, cell_rule='within')
    assert {c.token for c in within_l4.cells} == {'455', '4ff', '501', '5ab'}
    assert len(gm.s2_cover(cap, level=4, cell_rule='overlap').cells) == 16
    assert len(gm.s2_cover(cap, level=8, cell_rule='within').cells) == 2848

    seam = gm.Polygon(
        [(179.0, -1.0), (-179.0, -1.0), (-179.0, 1.0), (179.0, 1.0)],
        crs=4326,
    )
    assert len(gm.s2_cover(seam, level=8, cell_rule='overlap').cells) == 64
    assert len(gm.s2_cover(seam, level=8, cell_rule='within').cells) == 36

    # Non-polar within: interior ⊆ overlap (Berlin box has interior cells at L12).
    berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    b_within = {c.token for c in gm.s2_cover(berlin, level=12, cell_rule='within').cells}
    b_overlap = {c.token for c in gm.s2_cover(berlin, level=12, cell_rule='overlap').cells}
    assert b_within
    assert b_within <= b_overlap


def test_s2_cover_within_polar_and_antimeridian_match_s2sphere() -> None:
    """Within must not under-select full-longitude polar / seam interior cells.

    Outer/overlap fixed-level covers stay at the prior oracles (polar L4 = 16,
    antimeridian L8 = 64).
    """
    cap = gm.box(-180.0, 80.0, 180.0, 90.0, crs=4326)
    within_l4 = gm.s2_cover(cap, level=4, cell_rule='within')
    assert {c.token for c in within_l4.cells} == {'455', '4ff', '501', '5ab'}
    assert len(gm.s2_cover(cap, level=4, cell_rule='overlap').cells) == 16
    assert len(gm.s2_cover(cap, level=8, cell_rule='within').cells) == 2848

    seam = gm.Polygon(
        [(179.0, -1.0), (-179.0, -1.0), (-179.0, 1.0), (179.0, 1.0)],
        crs=4326,
    )
    assert len(gm.s2_cover(seam, level=8, cell_rule='overlap').cells) == 64
    assert len(gm.s2_cover(seam, level=8, cell_rule='within').cells) == 36

    # Non-polar within: interior ⊆ overlap (Berlin box has interior cells at L12).
    berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    b_within = {c.token for c in gm.s2_cover(berlin, level=12, cell_rule='within').cells}
    b_overlap = {c.token for c in gm.s2_cover(berlin, level=12, cell_rule='overlap').cells}
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
    assert len(coverage.cells) > 0
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
        cell_area = sum(cell.area for cell in coverage.cells)
        assert abs(outline.area - cell_area) / cell_area < 0.01


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
            cell_area = sum(cell.area for cell in coverage.cells)
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
        cell_area = sum(cell.area for cell in coverage.cells)
        assert abs(outline.area - cell_area) / cell_area < 0.02
        assert (coverage.compact().to_polygon() ^ outline).area <= 1e-06


def test_rect_coverage_compact_uncompact_with_parents() -> None:
    box = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
    probes = [box.centroid(), gm.Point(13.3, 52.45, crs=4326), gm.Point(0, 0, crs=4326)]
    cases = [
        ('geohash', gm.geohash_cover(box, precision=7), 7),
        ('tiles', gm.tile_cover(box, zoom=13), 13),
    ]
    for _name, coverage, depth in cases:
        compacted = coverage.compact()
        assert len(compacted.cells) <= len(coverage.cells)
        assert (compacted.to_polygon() ^ coverage.to_polygon()).area <= 1e-09
        restored = compacted.uncompact(depth)
        assert set(restored.cells) == set(coverage.cells)
        parented = coverage.with_parents()
        assert set(coverage.cells) <= set(parented.cells)
        for probe in probes:
            answers = {
                cov.contains(probe) for cov in (coverage, compacted, restored, parented)
            }
            assert len(answers) == 1
            answers = {cov.intersects(probe) for cov in (coverage, compacted, parented)}
            assert len(answers) == 1
    coarse = gm.geohash_cover(box, precision=5)
    with pytest.raises(gm.GeometryError, match='must be >='):
        coarse.uncompact(4)


def test_s2_coverage_closed_cell_and_partition_properties() -> None:
    seam_point = gm.Point(180, 0, crs=4326)
    tokens = {cell.token for cell in gm.s2_cover(seam_point, level=5).cells}
    assert tokens == {'6554', '6ffc'}
    polygon = gm.Polygon([(20.2, 51.2), (21.8, 51.4), (20.9, 52.8)], crs=4326)
    coverage = gm.s2_cover(polygon, level=8)
    outer = {cell.token for cell in coverage.cells}
    interior = {cell.token for cell in coverage.interior_cells}
    boundary = {cell.token for cell in coverage.boundary_cells}
    assert interior | boundary == outer
    assert not interior & boundary
    for cell in coverage.interior_cells:
        assert gm.covers(polygon, cell.center)


def test_geohash_and_tile_coverage_classify_exactly() -> None:
    box = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
    for coverage, cover_fn, depth_attr in [
        (gm.geohash_cover(box, precision=5), gm.geohash_cover, 'precision'),
        (gm.tile_cover(box, zoom=10), gm.tile_cover, 'zoom'),
    ]:
        depth = {depth_attr: getattr(coverage, depth_attr)}
        inter = cover_fn(box, cell_rule='overlap', **depth)
        center = cover_fn(box, cell_rule='center', **depth)
        contain = cover_fn(box, cell_rule='within', **depth)
        assert len(inter) >= len(center) >= len(contain)
        assert len(inter.interior_cells) + len(inter.boundary_cells) == len(inter)
        interior_tokens = {c.token for c in inter.interior_cells}
        boundary_tokens = {c.token for c in inter.boundary_cells}
        assert interior_tokens.isdisjoint(boundary_tokens)
        assert interior_tokens | boundary_tokens == {c.token for c in inter}
        for cell in inter:
            assert gm.intersects(box, cell.polygon)
        for cell in inter.interior_cells:
            assert gm.covers(box, cell.polygon)
        for cell in inter.boundary_cells:
            assert not gm.covers(box, cell.polygon)
        assert coverage.covers(gm.Point(13.4, 52.5, crs=4326))
        assert not coverage.covers(gm.Point(0, 0, crs=4326))
        assert coverage.contains_xy(13.4, 52.5)
        np.testing.assert_array_equal(
            coverage.contains_xy([13.4, 0.0], [52.5, 0.0]), [True, False]
        )
        assert coverage.intersects_xy(13.2, 52.4)
        polys = coverage.cells.polygon
        assert len(polys) == len(coverage)
        assert all(p.geometry_type == 'Polygon' for p in polys)
        assert coverage[0] in coverage
        assert next(iter(coverage)) == coverage[0]
        if len(coverage) >= 2:
            sliced = coverage[0:2]
            assert isinstance(sliced, gm.CellArray)
            assert len(sliced) == 2
        assert 'cell_rule' in coverage.explain()[0]
    box4 = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
    for coverage in (
        gm.h3_cover(box4, resolution=6),
        gm.s2_cover(box4, level=10),
        gm.geohash_cover(box4, precision=5),
        gm.tile_cover(box4, zoom=10),
    ):
        assert type(coverage).__match_args__ == ('cells',)
        match coverage:
            case (
                gm.GeohashCoverage(cells)
                | gm.TileCoverage(cells)
                | gm.H3Coverage(cells)
                | gm.S2Coverage(cells)
            ):
                assert len(cells) == len(coverage)
    coarse = gm.geohash_cover(box, precision=3, cell_rule='within')
    assert coarse.intersects(box)
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


def test_uncompact_rejects_over_budget() -> None:
    with pytest.raises(gm.GeometryError, match='exceeding the limit'):
        gm.CellArray([gm.H3Cell(0.0, 0.0, resolution=0)], type=gm.H3Cell).uncompact(8)
    with pytest.raises(gm.GeometryError, match='exceeding the limit'):
        gm.CellArray([gm.S2Cell(0.0, 0.0, level=0)], type=gm.S2Cell).uncompact(15)
    with pytest.raises(gm.GeometryError, match='exceeding the limit'):
        gm.CellArray(['u'], type=gm.GeohashCell).uncompact(5)
    with pytest.raises(gm.GeometryError, match='exceeding the limit'):
        gm.CellArray([gm.Tile('')], type=gm.Tile).uncompact(10)
    with pytest.raises(ValueError, match='exceeding the limit'):
        gm.CellArray([gm.S2Cell(0.0, 0.0, level=0)], type=gm.S2Cell).uncompact(15)
