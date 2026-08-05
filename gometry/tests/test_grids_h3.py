"""H3 and S2 discrete global grids — cells, boundaries, coverage,
exact membership predicates, compaction, and antimeridian-aware bounds.
"""

import copy
import operator
import pickle
from typing import Any, cast

import gometry as gm
import numpy as np
import pytest


def test_h3_point_cell_and_boundary() -> None:
    cell = gm.H3Cell(21.0, 52.0, resolution=7)
    geometry_cell = gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), resolution=7)
    projected_cell = gm.H3Cell(
        gm.Point(21.0, 52.0, crs=4326).to_crs(32634), resolution=7
    )
    cells = gm.h3_cells([21.0, 22.0], [52.0, 53.0], resolution=7)
    projected_cells = gm.h3_cells([21.0, 22.0], [52.0, 53.0], resolution=7)
    boundary = cell.polygon
    assert cell.resolution == 7
    assert cell.id > 0
    assert int(cell) == cell.id
    assert operator.index(cell) == cell.id
    assert str(geometry_cell) == str(cell)
    assert str(projected_cell) == str(cell)
    assert isinstance(cells, gm.CellArray)
    assert [str(value) for value in projected_cells] == [str(value) for value in cells]
    assert len(cells) == 2
    assert all(isinstance(value, gm.H3Cell) for value in cells)
    assert str(cell)
    assert boundary.geometry_type == 'Polygon'
    assert boundary.crs == 'EPSG:4326'
    assert cell.parent(6).resolution == 6
    assert len(cell.parent(6).children(7)) == 7
    disk = cell.grid_disk(0)
    assert isinstance(disk, gm.CellArray)
    assert cell in disk
    ring = cell.grid_ring(1)
    assert isinstance(ring, gm.CellArray)
    assert len(ring) == 6
    assert cell.grid_distance(ring[0]) == 1
    assert cell.grid_distance(ring[0].id) == 1
    assert cell.grid_distance(str(ring[0])) == 1
    assert list(cell.grid_path(ring[0])) == [cell, ring[0]]
    assert list(cell.grid_path(ring[0].id)) == [cell, ring[0]]
    assert list(cell.grid_path(str(ring[0]))) == [cell, ring[0]]
    assert {cell, geometry_cell, ring[0]} == {cell, ring[0]}
    assert hash(cell) == hash(geometry_cell)
    assert (
        gm.CellArray([str(cell)], type=gm.H3Cell).polygon[0].to_wkt()
        == boundary.to_wkt()
    )
    assert (
        gm.CellArray([cell.id], type=gm.H3Cell).polygon[0].to_wkt() == boundary.to_wkt()
    )
    np.testing.assert_array_equal(np.asarray(cells, copy=False), cells.to_numpy())
    np.testing.assert_array_equal(np.asarray(cells, copy=True), cells.to_numpy())
    with pytest.raises(ValueError, match='without copying'):
        np.asarray(cells[[1, 0]], copy=False)
    with pytest.raises(ValueError, match='without copying'):
        np.asarray(cells, dtype=object, copy=False)
    with pytest.raises(TypeError, match='lat must not be provided'):
        gm.H3Cell(gm.Point(21.0, 52.0, crs=4326), 52.0, resolution=7)


def test_h3_polygon_coverage() -> None:
    polygon = gm.box(20.99, 51.99, 21.01, 52.01, crs=4326)
    coverage = gm.h3_cover(polygon, resolution=7)
    projected = polygon.to_crs(32634)
    projected_coverage = gm.h3_cover(projected, resolution=7)
    assert coverage
    assert set(map(str, projected_coverage.cells)) == set(map(str, coverage.cells))
    assert coverage.cell_rule == 'overlap'
    assert coverage.resolution == 7
    assert all(cell.resolution == 7 for cell in coverage)
    assert list(coverage) == list(coverage.cells)
    assert coverage.cells[0] in coverage
    assert int(coverage.cells[0]) in coverage
    assert str(coverage.cells[0]) in coverage
    assert gm.H3Cell(30.0, 52.0, resolution=7) not in coverage
    assert coverage.cells.polygon.crs == 'EPSG:4326'
    assert coverage.compact().uncompact(7).cells.polygon.crs == 'EPSG:4326'
    assert isinstance(np.asarray(coverage.cells, dtype=object)[0], gm.H3Cell)
    with pytest.raises(TypeError, match=r'ufunc|does not support'):
        np.add(coverage, coverage)
    with_parents = coverage.with_parents(min_resolution=5)
    assert set(map(str, coverage.cells)) <= set(map(str, with_parents.cells))
    assert len(set(map(str, with_parents.cells))) == len(with_parents)
    assert {cell.resolution for cell in with_parents.cells} <= {5, 6, 7}
    assert any(cell.resolution == 5 for cell in with_parents.cells)
    assert any(cell.resolution == 6 for cell in with_parents.cells)
    with pytest.raises(ValueError, match='H3 min_resolution'):
        coverage.with_parents(min_resolution=16)
    center = gm.h3_cover(polygon, resolution=7, cell_rule='center')
    within = gm.h3_cover(polygon, resolution=7, cell_rule='within')
    bbox = gm.h3_cover(polygon, resolution=7, cell_rule='bbox')
    explicit_overlap = gm.h3_cover(polygon, resolution=7, cell_rule='overlap')
    assert [cell.id for cell in coverage.cells] == [
        cell.id for cell in explicit_overlap.cells
    ]
    assert center.cell_rule == 'center'
    assert within.cell_rule == 'within'
    assert bbox.cell_rule == 'bbox'
    assert set(map(str, within.cells)) <= set(map(str, center.cells))
    assert set(map(str, center.cells)) <= set(map(str, coverage.cells))
    assert set(map(str, coverage.cells)) <= set(map(str, bbox.cells))
    for unknown in ('intersects', 'contains', 'bbox_overlap'):
        with pytest.raises(ValueError, match='cell_rule'):
            gm.h3_cover(polygon, resolution=7, cell_rule=cast('Any', unknown))
    points = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
    np.testing.assert_array_equal(coverage.covers(points), [True, False])
    np.testing.assert_array_equal(center.covers(points), [True, False])
    np.testing.assert_array_equal(
        projected_coverage.covers(points.to_crs(32634)), [True, False]
    )
    membership_points = gm.points(
        [21.0, 21.005, 21.02, 30.0], [52.0, 51.995, 52.0, 52.0], crs=4326
    )
    expected_contains = gm.contains(polygon, membership_points)
    for cover in (coverage, center, within, bbox):
        np.testing.assert_array_equal(
            cover.contains(membership_points), expected_contains
        )
    crossing = gm.LineString([(20.98, 52.0), (21.02, 52.0)], crs=4326)
    assert not coverage.contains(crossing)
    assert coverage.intersects(crossing)
    other = gm.box(21.99, 51.99, 22.01, 52.01, crs=4326)
    multipolygon = gm.MultiPolygon([polygon, other], crs=4326)
    multi_coverage = gm.h3_cover(multipolygon, resolution=7)
    single_cells = set(map(str, coverage.cells)) | set(
        map(str, gm.h3_cover(other, resolution=7).cells)
    )
    assert set(map(str, multi_coverage.cells)) == single_cells
    np.testing.assert_array_equal(
        multi_coverage.covers(
            gm.points([21.0, 22.0, 21.5], [52.0, 52.0, 52.0], crs=4326)
        ),
        [True, True, False],
    )


def test_h3_point_and_line_coverage_matches_other_grids() -> None:
    point = gm.Point(13.4, 52.5, crs=4326)
    line = gm.LineString([(13.4, 52.5), (13.55, 52.62)], crs=4326)
    for geom in (point, line):
        assert len(gm.h3_cover(geom, resolution=9, cell_rule='center').cells) == 0
        assert len(gm.h3_cover(geom, resolution=9, cell_rule='within').cells) == 0
        overlap = gm.h3_cover(geom, resolution=9, cell_rule='overlap')
        assert len(overlap.cells) > 0
        assert len(gm.h3_cover(geom, resolution=9, cell_rule='bbox').cells) >= len(
            overlap.cells
        )
        assert len(overlap.interior_cells) == 0
        assert overlap.cells[0] in overlap
    assert list(gm.h3_cover(point, resolution=9).cells) == [
        gm.H3Cell(point, resolution=9)
    ]
    assert gm.h3_cover(point, resolution=9).covers(point)
    line_cov = gm.h3_cover(line, resolution=9)
    assert line_cov.covers(gm.Point(13.4, 52.5, crs=4326))
    assert line_cov.to_polygon().geometry_type in ('Polygon', 'MultiPolygon')
    assert set(map(str, gm.h3_cover(line.to_crs(32634), resolution=9).cells)) == set(
        map(str, line_cov.cells)
    )


def test_h3_set_utilities_compact_uncompact_children_count() -> None:
    parent = gm.H3Cell(21.0, 52.0, resolution=5)
    children = parent.children(6)
    assert len(gm.CellArray([parent, parent], type=gm.H3Cell).compact()) == 1
    assert list(children.compact()) == [parent]
    mixed = [f'{children[0].id:x}', children[1].id, *children[2:]]
    assert list(gm.CellArray(mixed, type=gm.H3Cell).compact()) == [parent]
    assert sorted(c.id for c in children[1:].compact()) == sorted(
        c.id for c in children[1:]
    )
    assert sorted(
        c.id for c in gm.CellArray([parent], type=gm.H3Cell).uncompact(6)
    ) == sorted(c.id for c in children)
    with pytest.raises(ValueError, match='resolution'):
        children.uncompact(5)
    assert parent.children_count(6) == len(children) == 7
    assert parent.children_count(8) == len(parent.children(8))


def test_h3_coverage_uncompact_is_not_recapped() -> None:
    """Coverage transforms are explicit user ops and are not re-capped."""
    coverage = gm.h3_cover(gm.Point(0.0, 0.0, crs=4326), resolution=0)
    expanded = coverage.uncompact(8)
    assert len(expanded) > len(coverage)
    # Free cell-array uncompact still applies the shared uncompact budget.
    cells = coverage.cells
    with pytest.raises(gm.GeometryError, match=r'uncompact would produce .* exceeding the limit'):
        cells.uncompact(12)


def test_h3_depth_accessors_and_local_ij() -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=9)
    neighbor = cell.neighbors[0]
    assert cell.is_neighbor(neighbor) and (not neighbor.is_neighbor(neighbor))
    i, j = neighbor.local_ij(cell)
    assert cell.cell_from_local_ij(i, j) == neighbor
    assert not hasattr(gm.H3Cell, 'from_local_ij')
    assert 0 <= cell.base_cell <= 121
    assert cell.child_position(8) is not None
    assert cell.child_position(15) is None
    assert len(gm.h3_pentagons(3)) == 12
    assert len(gm.h3_base_cells()) == 122


def test_h3_vertices_canonical_identity() -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    vertices = cell.vertices
    assert isinstance(vertices, gm.H3VertexArray)
    assert not isinstance(vertices, gm.CellArray)
    assert len(vertices) == 6
    assert len(gm.h3_pentagons(4)[0].vertices) == 5
    np.testing.assert_array_equal(vertices.values, vertices.to_numpy())
    assert isinstance(np.asarray(vertices, dtype=object)[0], gm.H3Vertex)
    np.testing.assert_array_equal(np.asarray(vertices, copy=False), vertices.values)
    np.testing.assert_array_equal(np.asarray(vertices, copy=True), vertices.values)
    with pytest.raises(ValueError, match='without copying'):
        np.asarray(vertices[[1, 0]], copy=False)
    with pytest.raises(ValueError, match='without copying'):
        np.asarray(vertices, dtype=object, copy=False)
    assert vertices.token == [v.token for v in vertices]
    assert vertices.point.to_wkt() == [v.point.to_wkt() for v in vertices]
    assert list(vertices[1:3]) == list(vertices)[1:3]
    assert next(reversed(vertices)) == vertices[-1]
    assert vertices.count(vertices[0]) == 1
    assert vertices.index(vertices[0]) == 0
    assert vertices[0] in vertices
    assert pickle.loads(pickle.dumps(vertices)) == vertices
    assert copy.copy(vertices) is vertices
    neighbor = cell.neighbors[0]
    shared = set(vertices) & set(neighbor.vertices)
    assert len(shared) == 2
    vertex = vertices[0]
    assert int(vertex) == vertex.id
    with pytest.raises(gm.ParseError):
        gm.H3Cell(vertex.token)
    assert vertex.point.crs == 'EPSG:4326'
    assert pickle.loads(pickle.dumps(vertex)) == vertex
    assert copy.copy(vertex) is vertex
    assert copy.deepcopy(vertex) is vertex
    match vertex:
        case gm.H3Vertex(vertex_id):
            assert vertex_id == vertex.id
        case _:
            pytest.fail('H3Vertex did not destructure')


def test_h3_set_ops_match_uncompacted_flat_algebra() -> None:
    """The hierarchy-aware H3 set ops agree with plain set algebra after
    expanding both sides to a common resolution (the id-algebra contract).
    """
    box = gm.box(13.3, 52.4, 13.45, 52.55, crs=4326)
    other = gm.box(13.4, 52.5, 13.55, 52.65, crs=4326)
    left = gm.h3_cover(box, resolution=6).compact().cells
    right = gm.h3_cover(other, resolution=6).cells
    flat = 7
    left_flat = set(left.uncompact(flat))
    right_flat = set(right.uncompact(flat))
    for ours, want in [
        (gm.h3_union(left, right), left_flat | right_flat),
        (gm.h3_intersection(left, right), left_flat & right_flat),
        (gm.h3_difference(left, right), left_flat - right_flat),
    ]:
        assert set(ours.uncompact(flat)) == want


def test_h3_directed_edges() -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    neighbor = cell.neighbors[0]
    edge = cell.edge(neighbor)
    assert edge.origin == cell
    assert edge.destination == neighbor
    assert edge.cells == (cell, neighbor)
    assert edge.reverse().origin == neighbor
    assert edge.reverse().reverse() == edge
    assert int(edge) == edge.id
    assert str(edge) == edge.token
    assert gm.H3Edge(edge.token) == edge
    assert gm.H3Edge(edge.id) == edge
    boundary = edge.line
    assert isinstance(boundary, gm.LineString)
    assert boundary.crs == 'EPSG:4326'
    assert 1000 < edge.length < 3000
    edges = cell.edges
    assert isinstance(edges, gm.H3EdgeArray)
    assert not isinstance(edges, gm.CellArray)
    assert len(edges) == 6
    assert {e.destination for e in edges} == set(cell.neighbors)
    np.testing.assert_array_equal(edges.values, edges.to_numpy())
    assert isinstance(np.asarray(edges, dtype=object)[0], gm.H3Edge)
    np.testing.assert_array_equal(np.asarray(edges, copy=False), edges.values)
    np.testing.assert_array_equal(np.asarray(edges, copy=True), edges.values)
    with pytest.raises(ValueError, match='without copying'):
        np.asarray(edges[[1, 0]], copy=False)
    with pytest.raises(ValueError, match='without copying'):
        np.asarray(edges, dtype=object, copy=False)
    assert edges.token == [e.token for e in edges]
    assert list(edges.origin) == [e.origin for e in edges]
    assert list(edges.destination) == [e.destination for e in edges]
    assert not hasattr(edges, 'cells')
    assert list(edges.reverse()) == [e.reverse() for e in edges]
    assert edges.line.to_wkt() == [e.line.to_wkt() for e in edges]
    np.testing.assert_allclose(edges.length, [e.length for e in edges])
    assert list(edges[1:3]) == list(edges)[1:3]
    assert next(reversed(edges)) == edges[-1]
    assert edges.count(edges[0]) == 1
    assert edges.index(edges[0]) == 0
    counted, counts = gm.H3EdgeArray([edges[0], edges[1], edges[0]]).value_counts()
    assert list(counted) == [edges[0], edges[1]]
    np.testing.assert_array_equal(counts, [2, 1])
    assert pickle.loads(pickle.dumps(edges)) == edges
    assert copy.copy(edges) is edges
    assert pickle.loads(pickle.dumps(edge)) == edge
    assert copy.copy(edge) is edge
    match edge:
        case gm.H3Edge(edge_id):
            assert edge_id == edge.id
        case _:
            pytest.fail('H3Edge did not destructure')
    with pytest.raises(gm.GeometryError, match='not a neighbor'):
        cell.edge(cell)
    with pytest.raises(gm.ParseError, match='invalid H3 edge'):
        gm.H3Edge('zzz')


def test_h3_index_scalar_and_bulk_parsing_share_validation() -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    vertex = cell.vertices[0]
    edge = cell.edges[0]

    assert gm.H3Cell(cell.id) == gm.H3Cell(cell.token) == cell
    assert gm.H3Vertex(vertex.id) == gm.H3Vertex(vertex.token) == vertex
    assert str(vertex) == vertex.token
    assert gm.H3Edge(edge.id) == gm.H3Edge(edge.token) == edge
    assert list(gm.CellArray(np.array([cell.id], dtype=np.uint64), type=gm.H3Cell)) == [
        cell
    ]
    assert list(gm.H3VertexArray(np.array([vertex.id], dtype=np.uint64))) == [vertex]
    assert list(gm.H3EdgeArray(np.array([edge.id], dtype=np.uint64))) == [edge]

    with pytest.raises(gm.ParseError, match='invalid H3 cell id 0'):
        gm.CellArray(np.array([0], dtype=np.uint64), type=gm.H3Cell)
    with pytest.raises(gm.ParseError, match='invalid H3 vertex id 0'):
        gm.H3VertexArray(np.array([0], dtype=np.uint64))
    with pytest.raises(gm.ParseError, match='invalid H3 edge id 0'):
        gm.H3EdgeArray(np.array([0], dtype=np.uint64))


@pytest.mark.parametrize(
    'call',
    [
        lambda: gm.H3Cell(13.4, 52.5, resolution=16),
        lambda: gm.h3_pentagons(16),
        lambda: gm.h3_cover(gm.Point(13.4, 52.5, crs=4326), resolution=16),
    ],
    ids=['cell', 'pentagons', 'cover'],
)
def test_h3_resolution_boundaries_share_the_typed_parser(call: Any) -> None:
    with pytest.raises(
        gm.GeometryError, match='H3 resolution must be between 0 and 15, got 16'
    ):
        call()


def test_h3_child_at_inverts_child_position() -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    for child in (cell.children(9)[0], cell.children(9)[37]):
        position = child.child_position(7)
        assert position is not None
        assert cell.child_at(position, 9) == child
    assert cell.center_child(9).parent(7) == cell
    assert cell.center_child(7) == cell
    with pytest.raises(gm.GeometryError, match='child position must be between'):
        cell.child_at(10**9, 8)
    with pytest.raises(gm.GeometryError, match='child position must be between'):
        cell.child_at(-1, 8)
    with pytest.raises(TypeError, match='child position must be an integer'):
        cell.child_at(1.5, 8)
    with pytest.raises(gm.GeometryError, match='children resolution'):
        cell.center_child(5)


def test_h3_to_polygon_dissolves_shared_edges() -> None:
    coverage = gm.h3_cover(gm.box(13.2, 52.4, 13.6, 52.6, crs=4326), resolution=6)
    outline = coverage.to_polygon()
    assert outline.geometry_type == 'Polygon'
    assert outline.crs == 'EPSG:4326'
    assert outline.is_valid
    union = coverage.cells.polygon.union_all()
    assert (outline ^ union).area <= 1e-06 * outline.area
    assert gm.equals(coverage.cells.compact().to_polygon(), outline)
    assert gm.equals(coverage.compact().to_polygon(), outline)
    cells = list(coverage.cells)
    assert gm.equals(gm.CellArray(cells + cells, type=gm.H3Cell).to_polygon(), outline)
    empty = gm.h3_cover(
        gm.Point(13.4, 52.5, crs=4326).buffer(1), resolution=5, cell_rule='within'
    )
    assert len(empty) == 0
    with pytest.raises(gm.GeometryError, match='at least one cell'):
        empty.to_polygon()
    cell = gm.H3Cell(13.4, 52.5, resolution=5)
    rebuilt = cell.children(7).to_polygon()
    hexagon = cell.polygon
    assert rebuilt.geometry_type == 'Polygon'
    assert abs(rebuilt.area - hexagon.area) <= 0.02 * hexagon.area
    assert gm.contains(rebuilt, cell.center)
    with pytest.raises(gm.GeometryError, match='at least one cell'):
        gm.CellArray([], type=gm.H3Cell).to_polygon()


def test_h3_to_polygon_rejects_mixed_resolution_expansion_over_budget() -> None:
    cells = [
        gm.H3Cell(0.0, 0.0, resolution=0),
        gm.H3Cell(0.0, 0.0, resolution=15),
    ]
    with pytest.raises(gm.GeometryError, match='exceeding the limit'):
        gm.CellArray(cells, type=gm.H3Cell).to_polygon()


# ---------------------------------------------------------------------------
# Polar / full-longitude / seam H3 cover: exact universe equality
# ---------------------------------------------------------------------------


def _reference_bboxes(universe: gm.CellArray) -> gm.GeometryArray:
    """Independent geographic cell bboxes — NOT the production helper.

    Polar cells (cover POINT(0, ±90)) → full-longitude box extended to the
    pole. Otherwise sort normalized longitudes, drop the largest circular
    gap, and use the complementary minimum circular interval; split the box
    when west > east.
    """
    north_pole = gm.Point(0.0, 90.0, crs=4326)
    south_pole = gm.Point(0.0, -90.0, crs=4326)
    boxes: list[gm.Geometry] = []
    for cell in universe:
        poly = cell.polygon
        if gm.covers(poly, north_pole) or gm.covers(poly, south_pole):
            # Polar cell: full longitude, lat extended to the enclosed pole.
            lats = [c[1] for c in poly.exterior.coords]
            min_y = min(lats)
            max_y = max(lats)
            if gm.covers(poly, north_pole):
                max_y = 90.0
            if gm.covers(poly, south_pole):
                min_y = -90.0
            boxes.append(gm.box(-180.0, min_y, 180.0, max_y, crs=4326))
            continue
        lons = [c[0] for c in poly.exterior.coords[:-1]]  # drop closing vertex
        lats = [c[1] for c in poly.exterior.coords[:-1]]
        if not lons:
            boxes.append(poly.bounds)  # type: ignore[arg-type]
            continue
        # Minimum circular interval: sort lon, find largest gap on the circle.
        sorted_lons = sorted(lons)
        n = len(sorted_lons)
        # Gaps between consecutive sorted lons, plus wrap gap.
        max_gap = -1.0
        max_gap_i = 0
        for i in range(n - 1):
            gap = sorted_lons[i + 1] - sorted_lons[i]
            if gap > max_gap:
                max_gap = gap
                max_gap_i = i
        wrap_gap = (sorted_lons[0] + 360.0) - sorted_lons[-1]
        if wrap_gap > max_gap:
            # Ordinary interval [first, last] — no wrap.
            west, east = sorted_lons[0], sorted_lons[-1]
        else:
            # Interval is the complement of the largest gap at max_gap_i.
            # After dropping gap between sorted_lons[max_gap_i] and
            # sorted_lons[max_gap_i+1], the interval runs from
            # sorted_lons[max_gap_i+1] to sorted_lons[max_gap_i] (wrapping).
            west = sorted_lons[max_gap_i + 1]
            east = sorted_lons[max_gap_i]
        south, north = min(lats), max(lats)
        if west <= east:
            boxes.append(gm.box(west, south, east, north, crs=4326))
        else:
            boxes.append(gm.box(west, south, east, north, crs=4326, wrap='split'))
    return gm.GeometryArray(boxes, crs=4326)


def _expected_h3_rule_ids(
    source: gm.Geometry, resolution: int
) -> dict[str, set[int]]:
    """Independent oracle: universe equality target for every cell_rule."""
    universe = gm.h3_base_cells().uncompact(resolution)
    polygons = universe.polygon
    centers = universe.center
    ids = [int(c) for c in universe]
    overlap_mask = gm.intersects(source, polygons)
    within_mask = gm.covers(source, polygons)
    center_mask = gm.covers(source, centers)
    bbox_mask = gm.intersects(source, _reference_bboxes(universe))
    return {
        'overlap': {ids[i] for i, ok in enumerate(overlap_mask) if ok},
        'within': {ids[i] for i, ok in enumerate(within_mask) if ok},
        'center': {ids[i] for i, ok in enumerate(center_mask) if ok},
        'bbox': {ids[i] for i, ok in enumerate(bbox_mask) if ok},
    }


def _actual_h3_rule_ids(
    source: gm.Geometry, resolution: int
) -> dict[str, set[int]]:
    return {
        rule: {int(c) for c in gm.h3_cover(source, resolution, cell_rule=rule).cells}
        for rule in ('overlap', 'within', 'center', 'bbox')
    }


def _assert_h3_cover_universe_equal(
    source: gm.Geometry, resolution: int, *, expected_counts: tuple[int, int, int, int] | None = None
) -> dict[str, set[int]]:
    expected = _expected_h3_rule_ids(source, resolution)
    actual = _actual_h3_rule_ids(source, resolution)
    for rule in ('overlap', 'within', 'center', 'bbox'):
        assert actual[rule] == expected[rule], (
            f'{rule}: extra={actual[rule] - expected[rule]!r} '
            f'missing={expected[rule] - actual[rule]!r} '
            f'(|actual|={len(actual[rule])}, |expected|={len(expected[rule])})'
        )
    if expected_counts is not None:
        counts = tuple(len(actual[r]) for r in ('overlap', 'within', 'center', 'bbox'))
        assert counts == expected_counts, f'counts {counts} != {expected_counts}'
    # Overlap partition: interior == within, boundary == overlap - within.
    overlap_cov = gm.h3_cover(source, resolution, cell_rule='overlap')
    assert {int(c) for c in overlap_cov.interior_cells} == expected['within']
    assert {int(c) for c in overlap_cov.boundary_cells} == (
        expected['overlap'] - expected['within']
    )
    return actual


def test_h3_cover_north_polar_cap_exact() -> None:
    source = gm.box(-180.0, 70.0, 180.0, 90.0, crs=4326)
    actual = _assert_h3_cover_universe_equal(source, 3)
    assert len(actual['overlap']) == 1274


def test_h3_cover_partial_north_cap_exact() -> None:
    source = gm.box(0.0, 70.0, 60.0, 90.0, crs=4326)
    actual = _assert_h3_cover_universe_equal(source, 3)
    assert len(actual['overlap']) == 267


def test_h3_cover_source_gate_counterexample_barrier_gate() -> None:
    """box(0,60,60,88) r2: PolePosition Exterior but outline enters polar cell."""
    source = gm.box(0.0, 60.0, 60.0, 88.0, crs=4326)
    _assert_h3_cover_universe_equal(source, 2, expected_counts=(104, 56, 77, 105))


def test_h3_cover_south_polar_symmetry() -> None:
    full = gm.box(-180.0, -90.0, 180.0, -70.0, crs=4326)
    partial = gm.box(-60.0, -90.0, 0.0, -70.0, crs=4326)
    _assert_h3_cover_universe_equal(full, 3)
    _assert_h3_cover_universe_equal(partial, 3)
    # South full cap mirrors north cap size at the same resolution.
    north = gm.box(-180.0, 70.0, 180.0, 90.0, crs=4326)
    assert len(gm.h3_cover(full, 3).cells) == len(gm.h3_cover(north, 3).cells)


def test_h3_cover_full_longitude_nonpolar_band() -> None:
    source = gm.box(-180.0, -10.0, 180.0, 10.0, crs=4326)
    _assert_h3_cover_universe_equal(source, 2, expected_counts=(1208, 888, 1038, 1208))


def test_h3_cover_raw_antimeridian_polygon() -> None:
    source = gm.Polygon(
        [(179.0, -5.0), (-179.0, -5.0), (-179.0, 5.0), (179.0, 5.0)],
        crs=4326,
    )
    _assert_h3_cover_universe_equal(source, 2, expected_counts=(9, 0, 4, 10))
    split = source.split_antimeridian()
    for rule in ('overlap', 'within', 'center', 'bbox'):
        a = {int(c) for c in gm.h3_cover(source, 2, cell_rule=rule).cells}
        b = {int(c) for c in gm.h3_cover(split, 2, cell_rule=rule).cells}
        assert a == b, f'{rule}: raw antimeridian != split_antimeridian()'


def test_h3_cover_polar_annulus() -> None:
    """Lat-60 polar shell with a lat-80 polar hole — excluded pole/hole respected."""

    def lat_ring(lat: float, *, n: int = 72) -> list[tuple[float, float]]:
        lons = np.linspace(-180.0, 180.0, n, endpoint=False)
        coords = [(float(lon), lat) for lon in lons]
        coords.append(coords[0])
        return coords

    # Densified constant-latitude rings: shell winds the pole; same-sense hole
    # at lat 80 removes the polar cap (covers 70, not 85/90).
    source = gm.Polygon(lat_ring(60.0), [lat_ring(80.0)], crs=4326)
    assert gm.covers(source, gm.Point(0.0, 70.0, crs=4326))
    assert not gm.covers(source, gm.Point(0.0, 85.0, crs=4326))
    assert not gm.covers(source, gm.Point(0.0, 90.0, crs=4326))
    actual = _assert_h3_cover_universe_equal(source, 2)
    north_pole = gm.Point(0.0, 90.0, crs=4326)
    for cell in gm.h3_cover(source, 2, cell_rule='within').cells:
        assert not gm.covers(cell.polygon, north_pole)
    assert len(actual['overlap']) > 0


def test_h3_cover_multipart_polar_plus_midlatitude() -> None:
    polar = gm.box(0.0, 70.0, 60.0, 90.0, crs=4326)
    mid = gm.box(10.0, 0.0, 20.0, 10.0, crs=4326)
    source = gm.MultiPolygon([polar, mid], crs=4326)
    _assert_h3_cover_universe_equal(source, 2)
    # Global overlap == union of parts covered separately.
    union_ids = (
        {int(c) for c in gm.h3_cover(polar, 2).cells}
        | {int(c) for c in gm.h3_cover(mid, 2).cells}
    )
    assert {int(c) for c in gm.h3_cover(source, 2).cells} == union_ids


def test_h3_cover_pentagon_neighborhood() -> None:
    source = gm.box(5.0, 60.0, 15.0, 69.0, crs=4326)
    pent = gm.H3Cell(0x820807FFFFFFFFF)
    assert pent.resolution == 2
    ring = pent.grid_ring(1)
    assert len(ring) == 5  # pentagon has five neighbors
    _assert_h3_cover_universe_equal(source, 2, expected_counts=(17, 1, 9, 17))
    assert pent in gm.h3_cover(source, 2).cells


def test_h3_cover_fast_path_controls_unchanged() -> None:
    """Ordinary mid-latitude sources must keep the unchecked flood (exact ids).

    Resolution stays coarse enough that the independent universe oracle
    (``h3_base_cells().uncompact``) is practical; the unchecked path is
    resolution-independent in structure.
    """
    cases: list[gm.Geometry] = [
        gm.box(20.0, 51.0, 22.0, 53.0, crs=4326),
        gm.Polygon(
            [
                (21.0, 52.0),
                (21.5, 52.0),
                (21.5, 52.5),
                (21.0, 52.5),
                (21.0, 52.0),
            ],
            [
                [
                    (21.1, 52.1),
                    (21.4, 52.1),
                    (21.4, 52.4),
                    (21.1, 52.4),
                    (21.1, 52.1),
                ]
            ],
            crs=4326,
        ),
        # Near antimeridian but NON-crossing.
        gm.box(170.0, -5.0, 175.0, 5.0, crs=4326),
    ]
    for source in cases:
        _assert_h3_cover_universe_equal(source, 3)


def test_h3_cover_polar_pickle_roundtrip() -> None:
    source = gm.box(-180.0, 70.0, 180.0, 90.0, crs=4326)
    for rule in ('overlap', 'within', 'center', 'bbox'):
        cov = gm.h3_cover(source, 3, cell_rule=rule)
        restored = pickle.loads(pickle.dumps(cov))
        assert {int(c) for c in restored.cells} == {int(c) for c in cov.cells}
        assert {int(c) for c in restored.interior_cells} == {
            int(c) for c in cov.interior_cells
        }
        assert {int(c) for c in restored.boundary_cells} == {
            int(c) for c in cov.boundary_cells
        }
        assert restored.cell_rule == rule
