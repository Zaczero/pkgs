"""H3 and S2 discrete global grids — cells, boundaries, coverage,
exact membership predicates, compaction, and antimeridian-aware bounds.
"""

import copy
import hashlib
import math
import operator
import pickle
from itertools import pairwise
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
    assert boundary.crs == 'OGC:CRS84'
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
    assert coverage.cells.polygon.crs == 'OGC:CRS84'
    assert coverage.compact().uncompact(7).cells.polygon.crs == 'OGC:CRS84'
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


def test_h3_cover_line_endpoint_owners_named_regressions() -> None:
    """Line covers must include the H3 cell that owns each endpoint.

    Pre-fix: terminal-exclusive edge sampling + chord-proxy filtering omitted
    the owner of a line endpoint while ``covers(endpoint)`` stayed True
    (membership answers against the source).
    """
    # Mid-latitude long edge at res 2: terminal endpoint (10, -40).
    mid = gm.LineString([(-10.0, -40.0), (10.0, -40.0)], crs=4326)
    mid_tokens = {c.token for c in gm.h3_cover(mid, resolution=2).cells}
    assert '82d10ffffffffff' in mid_tokens
    assert gm.H3Cell(10.0, -40.0, resolution=2).token == '82d10ffffffffff'

    # Polar long edge at res 0: start endpoint (-50, 85) — chord proxy rejects
    # the spherical owner while ``to_cell`` assigns it.
    polar = gm.LineString([(-50.0, 85.0), (50.0, 85.0)], crs=4326)
    polar_tokens = {c.token for c in gm.h3_cover(polar, resolution=0).cells}
    assert '8003fffffffffff' in polar_tokens
    assert gm.H3Cell(-50.0, 85.0, resolution=0).token == '8003fffffffffff'
    assert '8001fffffffffff' in polar_tokens


def test_h3_bbox_certified_root_descent_and_native_arc_corpus() -> None:
    """BBox uses all-root certified windows, never an owner-ring padding rule."""
    point = gm.Point(-170.0, 74.0, crs=4326)
    point_bbox = {
        cell.token for cell in gm.h3_cover(point, resolution=0, cell_rule='bbox').cells
    }
    assert point_bbox == {
        '8001fffffffffff',
        '8003fffffffffff',
        '8005fffffffffff',
        '800dfffffffffff',
    }
    assert gm.h3_cover(point, resolution=0, cell_rule='bbox').covers(point)

    polar = gm.LineString([(-90.0, 85.0), (90.0, 85.0)], crs=4326)
    polar_bbox = {
        cell.token for cell in gm.h3_cover(polar, resolution=2, cell_rule='bbox').cells
    }
    assert polar_bbox == {
        '820047fffffffff',
        '82004ffffffffff',
        '820057fffffffff',
        '82005ffffffffff',
        '8200c7fffffffff',
        '8200e7fffffffff',
        '82030ffffffffff',
        '82032ffffffffff',
        '820377fffffffff',
    }
    polar_overlap = {
        cell.token
        for cell in gm.h3_cover(polar, resolution=2, cell_rule='overlap').cells
    }
    assert polar_overlap == polar_bbox
    assert '820377fffffffff' in polar_overlap
    # Eager visible overlap and delayed inspection have one certified owner.
    # The old lazy proxy tile returned eight cells here, silently disagreeing
    # with the routed relation and omitting the arc-latitude extremum owner.
    polar_overlap_coverage = gm.h3_cover(polar, resolution=2, cell_rule='overlap')
    partition_tokens = {
        cell.token
        for cell in (
            list(polar_overlap_coverage.interior_cells)
            + list(polar_overlap_coverage.boundary_cells)
        )
    }
    assert partition_tokens == polar_overlap

    endpoint = gm.LineString([(-10.0, -40.0), (10.0, -40.0)], crs=4326)
    endpoint_bbox = {
        cell.token
        for cell in gm.h3_cover(endpoint, resolution=2, cell_rule='bbox').cells
    }
    assert '82d10ffffffffff' in endpoint_bbox


def test_h3_cover_collinear_vertex_is_invariant_for_every_visible_rule() -> None:
    """Exact lift canonicalization may not change any public H3 rule set."""
    sparse = gm.LineString([(-90.0, 85.0), (90.0, 85.0)], crs=4326)
    dense = gm.LineString([(-90.0, 85.0), (0.0, 85.0), (90.0, 85.0)], crs=4326)
    for rule in ('center', 'within', 'overlap', 'bbox'):
        sparse_cells = {
            cell.token
            for cell in gm.h3_cover(sparse, resolution=2, cell_rule=rule).cells
        }
        dense_cells = {
            cell.token
            for cell in gm.h3_cover(dense, resolution=2, cell_rule=rule).cells
        }
        assert sparse_cells == dense_cells, rule
    assert len(gm.h3_cover(sparse, resolution=2, cell_rule='overlap').cells) == 9


def test_h3_antimeridian_overlap_parity_has_no_fail_open_additions() -> None:
    source = gm.LineString([(170.0, -10.0), (-170.0, 10.0)], crs=4326)
    tokens = sorted(cell.token for cell in gm.h3_cover(source, resolution=7).cells)
    assert len(tokens) == 1_937
    assert hashlib.sha256(('\n'.join(tokens) + '\n').encode()).hexdigest() == (
        '56be706d44be0ac75a12295d208f0eac4d3b9e2acdd92a0bd047da360d799698'
    )


def test_h3_cover_superset_invariant_over_handwritten_corpus() -> None:
    """Every point the coverage covers() must lie in some returned cell.

    Membership answers against the retained source, so covers(probe) can hold
    even when the discrete cell set omits the containing cell — that is the
    defect class this suite pins (mirror of the S2 superset invariant).
    Only the documented visible supersets, ``overlap`` and ``bbox``, are
    required to retain every covered-point owner. Probes always include every
    source vertex and both line endpoints.
    """
    corpus: list[tuple[gm.Geometry, int, list[gm.Point]]] = [
        (
            gm.LineString([(-10.0, -40.0), (10.0, -40.0)], crs=4326),
            2,
            [
                gm.Point(-10.0, -40.0, crs=4326),
                gm.Point(0.0, -40.0, crs=4326),
                gm.Point(10.0, -40.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(-50.0, 85.0), (50.0, 85.0)], crs=4326),
            0,
            [
                gm.Point(-50.0, 85.0, crs=4326),
                gm.Point(0.0, 85.0, crs=4326),
                gm.Point(50.0, 85.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(10.0, 20.0), (12.0, 20.0)], crs=4326),
            5,
            [
                gm.Point(10.0, 20.0, crs=4326),
                gm.Point(11.0, 20.0, crs=4326),
                gm.Point(12.0, 20.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(-30.0, -10.0), (40.0, 25.0)], crs=4326),
            3,
            [
                gm.Point(-30.0, -10.0, crs=4326),
                gm.Point(5.0, 7.5, crs=4326),
                gm.Point(40.0, 25.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(179.0, 0.0), (-179.0, 0.0)], crs=4326),
            3,
            [
                gm.Point(179.0, 0.0, crs=4326),
                gm.Point(180.0, 0.0, crs=4326),
                gm.Point(-180.0, 0.0, crs=4326),
                gm.Point(-179.0, 0.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(0.0, 80.0), (10.0, 85.0)], crs=4326),
            2,
            [
                gm.Point(0.0, 80.0, crs=4326),
                gm.Point(5.0, 82.5, crs=4326),
                gm.Point(10.0, 85.0, crs=4326),
            ],
        ),
        (
            gm.LineString([(0.0, -50.0), (0.0, 50.0)], crs=4326),
            2,
            [
                gm.Point(0.0, -50.0, crs=4326),
                gm.Point(0.0, 0.0, crs=4326),
                gm.Point(0.0, 50.0, crs=4326),
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
            3,
            [
                gm.Point(0.0, -40.0, crs=4326),
                gm.Point(-10.0, -50.0, crs=4326),
                gm.Point(10.0, -30.0, crs=4326),
                gm.Point(10.0, -50.0, crs=4326),
                gm.Point(-10.0, -30.0, crs=4326),
            ],
        ),
        (
            gm.Polygon(
                [
                    (-50.0, 70.0),
                    (50.0, 70.0),
                    (50.0, 85.0),
                    (-50.0, 85.0),
                    (-50.0, 70.0),
                ],
                crs=4326,
            ),
            1,
            [
                gm.Point(-50.0, 85.0, crs=4326),
                gm.Point(50.0, 85.0, crs=4326),
                gm.Point(0.0, 77.5, crs=4326),
            ],
        ),
    ]
    for source, resolution, probes in corpus:
        for rule in ('overlap', 'bbox'):
            coverage = gm.h3_cover(source, resolution=resolution, cell_rule=rule)
            source_tokens = {c.token for c in coverage.cells}
            for probe in probes:
                assert coverage.covers(probe), f'{source!r} covers {probe!r}'
                owner = gm.H3Cell(probe.x, probe.y, resolution=resolution)
                assert owner.token in source_tokens, (
                    f'{rule}: {probe!r} owner {owner.token} not in cover for {source!r} '
                    f'res={resolution}; cover={sorted(source_tokens)}'
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
    with pytest.raises(
        gm.GeometryError, match=r'uncompact would produce .* exceeding the limit'
    ):
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
    assert vertex.point.crs == 'OGC:CRS84'
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
    assert boundary.crs == 'OGC:CRS84'
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
    assert outline.crs == 'OGC:CRS84'
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
# Independent finite-universe H3 coverage oracle
# ---------------------------------------------------------------------------


def _reference_bboxes(universe: gm.CellArray) -> gm.GeometryArray:
    """Independent geographic cell bboxes — not the production helper.

    Polar cells (cover POINT(0, ±90)) use a full-longitude box extended to the
    pole. Otherwise the complementary minimum circular longitude interval is
    split at the antimeridian when needed.
    """
    north_pole = gm.Point(0.0, 90.0, crs=4326)
    south_pole = gm.Point(0.0, -90.0, crs=4326)
    boxes: list[gm.Geometry] = []
    for cell in universe:
        poly = cell.polygon
        if gm.covers(poly, north_pole) or gm.covers(poly, south_pole):
            lats = [coord[1] for coord in poly.exterior.coords]
            south, north = min(lats), max(lats)
            if gm.covers(poly, north_pole):
                north = 90.0
            if gm.covers(poly, south_pole):
                south = -90.0
            boxes.append(gm.box(-180.0, south, 180.0, north, crs=4326))
            continue

        coordinates = poly.exterior.coords[:-1]
        longitudes = [coord[0] for coord in coordinates]
        latitudes = [coord[1] for coord in coordinates]
        if not longitudes:
            boxes.append(poly.bounds)  # type: ignore[arg-type]
            continue
        ordered = sorted(longitudes)
        gaps = [right - left for left, right in pairwise(ordered)]
        gaps.append(ordered[0] + 360.0 - ordered[-1])
        largest_gap = max(range(len(gaps)), key=gaps.__getitem__)
        if largest_gap == len(ordered) - 1:
            west, east = ordered[0], ordered[-1]
        else:
            west, east = ordered[largest_gap + 1], ordered[largest_gap]
        south, north = min(latitudes), max(latitudes)
        if west <= east:
            boxes.append(gm.box(west, south, east, north, crs=4326))
        else:
            boxes.append(gm.box(west, south, east, north, crs=4326, wrap='split'))
    return gm.GeometryArray(boxes, crs=4326)


def _expected_h3_rule_ids(source: gm.Geometry, resolution: int) -> dict[str, set[int]]:
    """Finite-universe targets for rules with planar point/box semantics."""
    universe = gm.h3_base_cells().uncompact(resolution)
    ids = [int(cell) for cell in universe]
    return {
        # Exact spherical overlap is pinned by literal analytic boundary cases
        # below. A planar presentation polygon may witness contact, but may not
        # establish a negative for a true H3 arc.
        'overlap': set(),
        # Exact `within` is checked separately with native spherical-arc
        # witnesses. A planar cell polygon is not an independent oracle for
        # the universal claim that the true spherical cell stays inside.
        'within': set(),
        'center': {
            ids[index]
            for index, included in enumerate(gm.covers(source, universe.center))
            if included
        },
        'bbox': {
            ids[index]
            for index, included in enumerate(
                gm.intersects(source, _reference_bboxes(universe))
            )
            if included
        },
    }


def _actual_h3_rule_ids(source: gm.Geometry, resolution: int) -> dict[str, set[int]]:
    return {
        rule: {
            int(cell) for cell in gm.h3_cover(source, resolution, cell_rule=rule).cells
        }
        for rule in ('overlap', 'within', 'center', 'bbox')
    }


def _assert_h3_cover_universe_equal(
    source: gm.Geometry,
    resolution: int,
    *,
    expected_counts: tuple[int, int, int, int] | None = None,
) -> dict[str, set[int]]:
    expected = _expected_h3_rule_ids(source, resolution)
    actual = _actual_h3_rule_ids(source, resolution)
    for rule in ('center', 'bbox'):
        assert actual[rule] == expected[rule], (
            f'{rule}: extra={actual[rule] - expected[rule]!r} '
            f'missing={expected[rule] - actual[rule]!r} '
            f'(|actual|={len(actual[rule])}, |expected|={len(expected[rule])})'
        )
    if expected_counts is not None:
        counts = tuple(
            len(actual[rule]) for rule in ('overlap', 'within', 'center', 'bbox')
        )
        assert counts == expected_counts, f'counts {counts} != {expected_counts}'

    overlap = gm.h3_cover(source, resolution, cell_rule='overlap')
    interior = {int(cell) for cell in overlap.interior_cells}
    boundary = {int(cell) for cell in overlap.boundary_cells}
    # Certified interior is a sufficient core, not a duplicate spelling of
    # ``within``: a failed containment proof must remain Boundary.
    assert interior.isdisjoint(boundary)
    assert interior | boundary == actual['overlap']
    return actual


def _minor_great_circle_midpoint(
    left: tuple[float, float], right: tuple[float, float]
) -> tuple[float, float]:
    """Closed minor-arc midpoint from independent unit-Cartesian geometry."""

    def unit(latitude: float, longitude: float) -> tuple[float, float, float]:
        lat = math.radians(latitude)
        lon = math.radians(longitude)
        cos_lat = math.cos(lat)
        return cos_lat * math.cos(lon), cos_lat * math.sin(lon), math.sin(lat)

    a = unit(*left)
    b = unit(*right)
    summed = tuple(x + y for x, y in zip(a, b, strict=True))
    norm = math.sqrt(sum(value * value for value in summed))
    x, y, z = (value / norm for value in summed)
    return math.degrees(math.atan2(z, math.hypot(x, y))), math.degrees(math.atan2(y, x))


def _minor_arc_latitude_extrema(
    left: tuple[float, float], right: tuple[float, float]
) -> tuple[float, float]:
    """Analytic latitude extrema of one native H3 minor great-circle arc."""

    def unit(point: tuple[float, float]) -> tuple[float, float, float]:
        latitude, longitude = map(math.radians, point)
        cosine = math.cos(latitude)
        return (
            cosine * math.cos(longitude),
            cosine * math.sin(longitude),
            math.sin(latitude),
        )

    def dot(a: tuple[float, ...], b: tuple[float, ...]) -> float:
        return sum(x * y for x, y in zip(a, b, strict=True))

    def angle(a: tuple[float, ...], b: tuple[float, ...]) -> float:
        return math.acos(max(-1.0, min(1.0, dot(a, b))))

    a, b = unit(left), unit(right)
    normal = (
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    )
    normal_sq = dot(normal, normal)
    projected = (
        -normal[2] * normal[0] / normal_sq,
        -normal[2] * normal[1] / normal_sq,
        1.0 - normal[2] * normal[2] / normal_sq,
    )
    norm = math.sqrt(dot(projected, projected))
    north = tuple(value / norm for value in projected)
    candidates = [a, b]
    total = angle(a, b)
    candidates.extend(
        candidate
        for candidate in (north, tuple(-value for value in north))
        if abs(angle(a, candidate) + angle(candidate, b) - total) <= 2e-14
    )
    latitudes = [math.degrees(math.asin(point[2])) for point in candidates]
    return min(latitudes), max(latitudes)


def _analytic_h3_within_box(
    resolution: int, west: float, south: float, east: float, north: float
) -> set[str]:
    """Native vertices plus analytic arc latitudes for an axis-aligned box."""
    h3 = pytest.importorskip('h3')
    expected = set()
    for base in h3.get_res0_cells():
        for token in h3.cell_to_children(base, resolution):
            boundary = h3.cell_to_boundary(token)
            if not all(west <= longitude <= east for _, longitude in boundary):
                continue
            # Vertex-latitude prefilter.  `_minor_arc_latitude_extrema` seeds
            # `candidates` with both endpoints and only ever *adds* to them, so
            # every arc's minimum is <= both its vertex latitudes and its
            # maximum is >= both.  A cell with a vertex outside [south, north]
            # therefore always fails the exact test below, and skipping it here
            # cannot change the result — this is a necessary condition, not an
            # approximation.  It matters because the polar box spans the full
            # longitude range, so the longitude prefilter above rejects nothing
            # and all 288k res-4 descendants would otherwise pay the arc math.
            latitudes = [latitude for latitude, _ in boundary]
            if min(latitudes) < south or max(latitudes) > north:
                continue
            extrema = [
                _minor_arc_latitude_extrema(left, right)
                for left, right in pairwise((*boundary, boundary[0]))
            ]
            if (
                min(low for low, _ in extrema) >= south
                and max(high for _, high in extrema) <= north
            ):
                expected.add(token)
    return expected


@pytest.mark.parametrize(('resolution', 'count'), [(2, 145), (3, 1_116), (4, 8_189)])
def test_h3_within_polar_box_matches_independent_arc_extrema(
    resolution: int, count: int
) -> None:
    source = gm.box(-180.0, 70.0, 180.0, 90.0, crs=4326)
    actual = {
        cell.token for cell in gm.h3_cover(source, resolution, cell_rule='within').cells
    }
    expected = _analytic_h3_within_box(resolution, -180.0, 70.0, 180.0, 90.0)
    assert len(expected) == count
    assert actual == expected
    if resolution == 2:
        assert {'820297fffffffff', '820b6ffffffffff'} <= actual


@pytest.mark.parametrize(('resolution', 'count'), [(2, 53), (3, 453), (4, 3_369)])
def test_h3_within_central_box_is_analytic_strict_overlap_subset(
    resolution: int, count: int
) -> None:
    source = gm.box(-10.0, -10.0, 10.0, 10.0, crs=4326)
    within = {
        cell.token for cell in gm.h3_cover(source, resolution, cell_rule='within').cells
    }
    overlap = {
        cell.token
        for cell in gm.h3_cover(source, resolution, cell_rule='overlap').cells
    }
    assert within == _analytic_h3_within_box(resolution, -10.0, -10.0, 10.0, 10.0)
    assert len(within) == count
    assert within < overlap


def test_h3_overlap_keeps_both_closed_owners_at_native_arc_midpoint() -> None:
    """Literal independent great-circle midpoint, not a chord proxy oracle."""
    point = gm.Point(1.5835173670280838, -0.9613796866574875, crs=4326)
    assert {
        cell.token for cell in gm.h3_cover(point, 2, cell_rule='overlap').cells
    } == {
        '82754ffffffffff',
        '82825ffffffffff',
    }


def test_h3_within_never_uses_the_chord_proxy_as_a_containment_certificate() -> None:
    """The true first H3 arc exits its own planar presentation polygon."""
    h3 = pytest.importorskip('h3')
    shapely = pytest.importorskip('shapely')

    cell = gm.H3Cell(0x82754FFFFFFFFFF)
    source = cell.polygon
    boundary = h3.cell_to_boundary(cell.token)
    midpoint_lat, midpoint_lon = _minor_great_circle_midpoint(boundary[0], boundary[1])
    assert midpoint_lon == pytest.approx(1.5835173670280838, abs=5e-15)
    assert midpoint_lat == pytest.approx(-0.9613796866574875, abs=5e-15)

    source_ref = shapely.from_wkb(source.to_wkb())
    assert not source_ref.covers(shapely.Point(midpoint_lon, midpoint_lat))
    # Complete finite-universe oracle for this source: every r2 cell has an
    # independently derived native vertex or exact minor-arc midpoint outside
    # the planar source. Each is a constructive non-containment certificate,
    # so the exact expected `within` set is empty without consulting a gometry
    # cell polygon or predicate.
    expected: set[str] = set()
    for candidate in h3.get_res0_cells():
        for token in h3.cell_to_children(candidate, 2):
            native = h3.cell_to_boundary(token)
            probes = list(native)
            probes.extend(
                _minor_great_circle_midpoint(left, right)
                for left, right in pairwise((*native, native[0]))
            )
            if all(
                source_ref.covers(shapely.Point(longitude, latitude))
                for latitude, longitude in probes
            ):
                expected.add(token)
    assert expected == set()
    within = gm.h3_cover(source, 2, cell_rule='within')
    assert {candidate.token for candidate in within.cells} == expected
    assert not gm.covers(source, gm.Point(midpoint_lon, midpoint_lat, crs=4326))


def test_h3_within_certifies_against_the_complete_component_union() -> None:
    """Two touching components jointly, but not individually, contain a cell."""
    cell = gm.H3Cell(0x82754FFFFFFFFFF)
    split = cell.center.x
    left = gm.box(-10.0, -10.0, split, 10.0, crs=4326)
    right = gm.box(split, -10.0, 10.0, 10.0, crs=4326)
    source = gm.MultiPolygon([left, right], crs=4326)

    assert cell not in gm.h3_cover(left, 2, cell_rule='within').cells
    assert cell not in gm.h3_cover(right, 2, cell_rule='within').cells
    assert cell in gm.h3_cover(source, 2, cell_rule='within').cells


@pytest.mark.parametrize(
    'token',
    [
        '82754ffffffffff',  # named ordinary Class II cell
        '83754efffffffff',  # Class III child neighbourhood
        '820807fffffffff',  # pentagon
        '820327fffffffff',  # north-polar owner
        '827eb7fffffffff',  # antimeridian neighbourhood
    ],
)
def test_h3_within_emits_no_cell_with_an_independent_spherical_arc_witness(
    token: str,
) -> None:
    """Vertices and exact arc midpoints red-team nearby/special-cell outputs."""
    h3 = pytest.importorskip('h3')
    shapely = pytest.importorskip('shapely')

    owner = gm.H3Cell(token)
    source = owner.polygon
    source_ref = shapely.from_wkb(source.to_wkb())
    coverage = gm.h3_cover(source, owner.resolution, cell_rule='within')
    for emitted in coverage.cells:
        boundary = h3.cell_to_boundary(emitted.token)
        probes = list(boundary)
        probes.extend(
            _minor_great_circle_midpoint(left, right)
            for left, right in pairwise((*boundary, boundary[0]))
        )
        assert all(
            source_ref.covers(shapely.Point(longitude, latitude))
            for latitude, longitude in probes
        ), emitted.token


def test_h3_cover_north_polar_cap_exact() -> None:
    source = gm.box(-180.0, 70.0, 180.0, 90.0, crs=4326)
    actual = _assert_h3_cover_universe_equal(source, 3)
    assert len(actual['overlap']) == 1274


@pytest.mark.parametrize(
    ('longitude', 'latitude'),
    [(0.0, 90.0), (0.0, -90.0), (0.0, 89.9)],
)
def test_h3_public_pole_points_reach_the_leaf_traversal(
    longitude: float, latitude: float
) -> None:
    """Public overlap/bbox traversal, not just the leaf classifier, retains poles.

    The finite r2 universe is the analytic oracle. This fails if a parent or
    leaf bbox prunes the signed full-turn pole before the exact arc relation.
    """
    point = gm.Point(longitude, latitude, crs=4326)
    actual = _assert_h3_cover_universe_equal(point, 2)
    assert len(actual['overlap']) == len(actual['bbox']) == 1
    if latitude > 0.0:
        assert int(gm.H3Cell('820327fffffffff')) in actual['overlap']
        assert int(gm.H3Cell('820327fffffffff')) in actual['bbox']
    for resolution in (0, 2, 5):
        for rule in ('overlap', 'bbox'):
            coverage = gm.h3_cover(point, resolution, cell_rule=rule)
            assert len(coverage.cells) > 0
            assert coverage.covers(point)


@pytest.mark.parametrize(
    ('pole', 'owner'),
    [(90.0, '820327fffffffff'), (-90.0, '82f297fffffffff')],
)
def test_h3_exact_pole_lines_are_point_carriers_through_neighbourhood(
    pole: float, owner: str
) -> None:
    """Exact poles and both stored-double sides keep their one H3 owner.

    The source is linear in lon/lat except at the exact physical pole, where
    longitude is structurally irrelevant. The nearest stored-double values
    below each pole are deliberately retained as real line sources so a
    boundary special case cannot hide a one-ULP strip-selection regression.
    """
    latitudes = [pole]
    for _ in range(2):
        latitudes.append(math.nextafter(latitudes[-1], 0.0))
    latitudes.append(math.nextafter(pole, math.copysign(math.inf, pole)))
    for latitude in latitudes:
        source = gm.LineString([(-10.0, latitude), (10.0, latitude)], crs=4326)
        for rule in ('overlap', 'bbox'):
            coverage = gm.h3_cover(source, resolution=2, cell_rule=rule, max_cells=1)
            assert {cell.token for cell in coverage.cells} == {owner}, (
                pole,
                latitude,
                rule,
            )


def _pole_neighbourhood_shapes(latitude: float, longitude: float) -> dict[str, Any]:
    near = 80.0 if latitude > 0.0 else -80.0
    cap = [
        (longitude - 10.0, near),
        (longitude + 10.0, near),
        (longitude + 10.0, latitude),
        (longitude - 10.0, latitude),
    ]
    line = [(longitude - 10.0, latitude), (longitude + 10.0, latitude)]
    return {
        'point': gm.Point(longitude, latitude, crs=4326),
        'line': gm.LineString(line, crs=4326),
        'multi_point': gm.MultiPoint(line, crs=4326),
        'multi_line': gm.MultiLineString(
            [line, [(longitude - 30.0, latitude), (longitude - 20.0, latitude)]],
            crs=4326,
        ),
        'polygon': gm.Polygon(cap, crs=4326),
        'multi_polygon': gm.MultiPolygon([cap], crs=4326),
        'collection': gm.GeometryCollection(
            [
                gm.Point(longitude, latitude, crs=4326),
                gm.LineString(line, crs=4326),
                gm.Polygon(cap, crs=4326),
            ],
            crs=4326,
        ),
    }


def test_h3_pole_normalization_closes_every_shape_and_rule_neighbourhood() -> None:
    """Accepted exterior pole rounding has the exact-pole cover, everywhere.

    The reference is the exact same stored-double geometry with its pole
    coordinate exact.  This exercises the complete H3 topology ingress — not
    a point-only carrier — through every public shape family and visible rule.
    """
    for pole in (90.0, -90.0):
        neighbours = (
            math.nextafter(pole, 0.0),
            math.nextafter(pole, math.copysign(math.inf, pole)),
        )
        for longitude in (0.0, 170.0):
            expected = _pole_neighbourhood_shapes(pole, longitude)
            for latitude in neighbours:
                actual = _pole_neighbourhood_shapes(latitude, longitude)
                for name, exact in expected.items():
                    for rule in ('overlap', 'bbox', 'center', 'within'):
                        exact_tokens = {
                            cell.token
                            for cell in gm.h3_cover(exact, 2, cell_rule=rule).cells
                        }
                        actual_tokens = {
                            cell.token
                            for cell in gm.h3_cover(
                                actual[name], 2, cell_rule=rule
                            ).cells
                        }
                        assert actual_tokens == exact_tokens, (
                            pole,
                            latitude,
                            longitude,
                            name,
                            rule,
                        )


def test_h3_pole_component_decomposition_merges_before_its_global_budget() -> None:
    """An inward-ULP pole component cannot poison an ordinary sibling.

    This must traverse the same atomic-decomposition rule as every aggregate,
    then charge `max_cells` only after the deduplicated union is known.
    """
    polar_latitude = math.nextafter(90.0, 0.0)
    polar = gm.LineString([(-45.0, polar_latitude), (45.0, polar_latitude)], crs=4326)
    ordinary = gm.LineString([(0.0, 0.0), (1.0, 1.0)], crs=4326)
    for aggregate in (
        gm.MultiLineString([polar, ordinary], crs=4326),
        gm.GeometryCollection([polar, ordinary], crs=4326),
    ):
        for rule in ('overlap', 'bbox', 'center', 'within'):
            component_union = {
                cell.token
                for component in (polar, ordinary)
                for cell in gm.h3_cover(component, 2, cell_rule=rule).cells
            }
            kwargs = {'max_cells': len(component_union)} if component_union else {}
            covered = gm.h3_cover(aggregate, 2, cell_rule=rule, **kwargs)
            assert {cell.token for cell in covered.cells} == component_union
            assert covered.covers(aggregate)
            if component_union:
                with pytest.raises(gm.GeometryError, match='max_cells'):
                    gm.h3_cover(
                        aggregate,
                        2,
                        cell_rule=rule,
                        max_cells=len(component_union) - 1,
                    )


def test_h3_cover_partial_north_cap_exact() -> None:
    source = gm.box(0.0, 70.0, 60.0, 90.0, crs=4326)
    actual = _assert_h3_cover_universe_equal(source, 3)
    assert len(actual['overlap']) == 267


def test_h3_cover_source_gate_counterexample_barrier_gate() -> None:
    """box(0,60,60,88) r2: the outline reaches the polar cell."""
    source = gm.box(0.0, 60.0, 60.0, 88.0, crs=4326)
    actual = _assert_h3_cover_universe_equal(
        source, 2, expected_counts=(104, 56, 77, 105)
    )
    assert {
        gm.H3Cell(value).token for value in actual['within']
    } == _analytic_h3_within_box(2, 0.0, 60.0, 60.0, 88.0)


def test_h3_cover_south_polar_symmetry() -> None:
    full = gm.box(-180.0, -90.0, 180.0, -70.0, crs=4326)
    partial = gm.box(-60.0, -90.0, 0.0, -70.0, crs=4326)
    _assert_h3_cover_universe_equal(full, 3)
    _assert_h3_cover_universe_equal(partial, 3)
    north = gm.box(-180.0, 70.0, 180.0, 90.0, crs=4326)
    assert len(gm.h3_cover(full, 3).cells) == len(gm.h3_cover(north, 3).cells)


def test_h3_cover_full_longitude_nonpolar_band() -> None:
    source = gm.box(-180.0, -10.0, 180.0, 10.0, crs=4326)
    actual = _assert_h3_cover_universe_equal(
        source, 2, expected_counts=(1208, 888, 1038, 1208)
    )
    assert {
        gm.H3Cell(value).token for value in actual['within']
    } == _analytic_h3_within_box(2, -180.0, -10.0, 180.0, 10.0)


def test_h3_cover_raw_antimeridian_polygon() -> None:
    """The exact carrier owns the raw seam source, not a historical split."""
    source = gm.Polygon(
        [(179.0, -5.0), (-179.0, -5.0), (-179.0, 5.0), (179.0, 5.0)],
        crs=4326,
    )
    _assert_h3_cover_universe_equal(source, 2, expected_counts=(9, 0, 4, 10))
    extra_bbox_only = gm.H3Cell(0x827FB7FFFFFFFFF)
    assert extra_bbox_only not in gm.h3_cover(source, 2, cell_rule='overlap').cells
    assert extra_bbox_only in gm.h3_cover(source, 2, cell_rule='bbox').cells
    # `split_antimeridian()` materializes different straight lon/lat edges.
    # It is a planar working representation, not source authority, so raw and
    # split polygons are deliberately not required to select the same H3 set.


def test_h3_cover_polar_annulus() -> None:
    """Lat-60 polar shell with a lat-80 polar hole; the pole is excluded."""

    def lat_ring(latitude: float, *, count: int = 72) -> list[tuple[float, float]]:
        longitudes = np.linspace(-180.0, 180.0, count, endpoint=False)
        coordinates = [(float(longitude), latitude) for longitude in longitudes]
        coordinates.append(coordinates[0])
        return coordinates

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
    actual = _assert_h3_cover_universe_equal(source, 2)
    union_ids = {int(cell) for cell in gm.h3_cover(polar, 2).cells} | {
        int(cell) for cell in gm.h3_cover(mid, 2).cells
    }
    assert actual['overlap'] == union_ids


def test_h3_multipart_global_uncertainty_keeps_independent_component_caps() -> None:
    """A polar component must not poison a disjoint rectangular sibling's cap.

    This is a finite analytic union: each component is covered independently
    from its exact source, then the multipart cover is required to be exactly
    that union. The frozen r2 universe is 202 polar cells plus 19 middle
    cells, with no overlap.
    """
    polar = gm.box(-180.0, 70.0, 180.0, 90.0, crs=4326)
    middle = gm.box(10.0, 0.0, 18.0, 8.0, crs=4326)
    multipart = gm.MultiPolygon([polar, middle], crs=4326)
    polar_ids = {cell.token for cell in gm.h3_cover(polar, 2, cell_rule='bbox').cells}
    middle_ids = {cell.token for cell in gm.h3_cover(middle, 2, cell_rule='bbox').cells}
    combined_ids = {
        cell.token for cell in gm.h3_cover(multipart, 2, cell_rule='bbox').cells
    }
    assert len(polar_ids) == 202
    assert len(middle_ids) == 19
    assert polar_ids.isdisjoint(middle_ids)
    assert combined_ids == polar_ids | middle_ids


def test_h3_multipart_cap_charges_the_incremental_union() -> None:
    """Multipart r2 bbox has an exact 202 + 19 disjoint-cell union."""
    polar = gm.box(-180.0, 70.0, 180.0, 90.0, crs=4326)
    middle = gm.box(10.0, 0.0, 18.0, 8.0, crs=4326)
    multipart = gm.MultiPolygon([polar, middle], crs=4326)
    with pytest.raises(gm.GeometryError, match='max_cells'):
        gm.h3_cover(multipart, 2, cell_rule='bbox', max_cells=220)
    coverage = gm.h3_cover(multipart, 2, cell_rule='bbox', max_cells=221)
    assert len(coverage.cells) == 221


def test_h3_cover_pentagon_neighborhood() -> None:
    source = gm.box(5.0, 60.0, 15.0, 69.0, crs=4326)
    pentagon = gm.H3Cell(0x820807FFFFFFFFF)
    assert pentagon.resolution == 2
    assert len(pentagon.grid_ring(1)) == 5
    _assert_h3_cover_universe_equal(source, 2, expected_counts=(17, 1, 9, 17))
    assert pentagon in gm.h3_cover(source, 2).cells


def test_h3_cover_fast_path_controls_unchanged() -> None:
    """Ordinary mid-latitude sources retain exact visible rule sets."""
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
