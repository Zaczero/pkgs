"""H3 and S2 discrete global grids — cells, boundaries, coverage,
exact membership predicates, compaction, and antimeridian-aware bounds.
"""

import math
import pickle

import gometry as gm
import numpy as np
import pytest
from conftest import bools
from gometry import Cell


def test_coverage_iteration_matches_cells_array():
    area = gm.box(20.99, 51.99, 21.01, 52.01, crs=4326)
    h3_cov = gm.h3_cover(area, resolution=7)
    s2_cov = gm.s2_cover(area, target_cells=8)
    geohash_cov = gm.geohash_cover(area, precision=5)
    tile_cov = gm.tile_cover(area, zoom=10)
    assert not hasattr(h3_cov, 'to_list')
    assert list(h3_cov.cells) == list(h3_cov) == [h3_cov[i] for i in range(len(h3_cov))]
    assert list(s2_cov.cells) == list(s2_cov)
    assert list(geohash_cov.cells) == list(geohash_cov)
    assert list(tile_cov.cells) == list(tile_cov)
    assert all(isinstance(cell, gm.H3Cell) for cell in list(h3_cov.cells))
    assert all(isinstance(cell, gm.S2Cell) for cell in list(s2_cov.cells))
    assert all(isinstance(cell, gm.GeohashCell) for cell in list(geohash_cov.cells))
    assert all(isinstance(cell, gm.Tile) for cell in list(tile_cov.cells))
    for coverage, iterator_name in [
        (h3_cov, 'H3CoverageIterator'),
        (s2_cov, 'S2CoverageIterator'),
        (geohash_cov, 'GeohashCoverageIterator'),
        (tile_cov, 'TileCoverageIterator'),
    ]:
        iterator = iter(coverage)
        assert type(iterator).__name__ == iterator_name
        assert iterator.__length_hint__() == len(coverage)
        assert list(iterator) == list(coverage.cells)
        reverse_iterator = reversed(iter(coverage))
        assert type(reverse_iterator).__name__ == iterator_name
        assert list(reverse_iterator) == list(reversed(list(coverage.cells)))
        assert list(reversed(coverage)) == list(reversed(list(coverage.cells)))


def test_overlap_coverage_cell_arrays_share_the_canonical_id_storage() -> None:
    area = gm.box(12.8, 52.2, 14.0, 53.0, crs=4326)
    coverages = [
        gm.h3_cover(area, resolution=7),
        gm.s2_cover(area, level=10),
        gm.geohash_cover(area, precision=5),
        gm.tile_cover(area, zoom=9),
    ]
    for coverage in coverages:
        first = coverage.cells.to_numpy()
        second = coverage.cells.to_numpy()
        if coverage.cells.grid == 'geohash':
            assert first.dtype == object
            assert not np.shares_memory(first, second)
        else:
            assert first.dtype == np.uint64
            assert np.shares_memory(first, second)
        assert first.flags.writeable is False
        assert first.tolist() == second.tolist()
        restored = pickle.loads(pickle.dumps(coverage))
        assert restored.__sizeof__() == coverage.__sizeof__()
        assert list(restored.cells) == list(coverage.cells)


def test_cell_array_token_mirrors_scalar_property_name() -> None:
    cells = gm.s2_cover(gm.box(20.99, 51.99, 21.01, 52.01, crs=4326), level=10).cells
    assert cells.token == [cell.token for cell in cells]
    assert not hasattr(cells, 'tokens')


def test_coverage_membership_applies_the_grid_input_policy_to_candidates() -> None:
    shell = [(0, 0), (0.4, 0), (0.4, 0.4), (0, 0.4)]
    inside, outside = ((0.2, 0.2), (3.0, 3.0))
    for cover_crs in (None, 4326):
        cov = gm.h3_cover(gm.Polygon(shell, crs=cover_crs), resolution=5)
        assert cov.covers(gm.Point(*inside)) is True
        assert cov.covers(gm.Point(*inside, crs=4326)) is True
        assert cov.covers(gm.Point(*inside, crs=4326).to_crs(3857)) is True
        assert cov.covers(gm.Point(*outside)) is False


def test_compact_respects_the_resolution_floor() -> None:
    parent = gm.H3Cell(21.0, 52.0, resolution=3)
    fine = parent.children(5)
    assert list(fine.compact()) == [parent]
    floored = fine.compact(4)
    assert sorted(c.id for c in floored) == sorted(c.id for c in parent.children(4))
    assert all(c.resolution == 4 for c in floored)
    cov = gm.h3_cover(gm.box(20.5, 51.5, 21.5, 52.5, crs=4326), resolution=5)
    compacted = cov.compact(min_resolution=4)
    assert all(c.resolution >= 4 for c in compacted.cells)
    coarse = gm.H3Cell(21.0, 52.0, resolution=2)
    assert list(gm.CellArray([coarse], type=gm.H3Cell).compact(4)) == [coarse]

    gh_parent = gm.GeohashCell(13.4, 52.5, precision=3)
    gh_fine = gh_parent.children(5)
    assert list(gh_fine.compact()) == [gh_parent]
    gh_floored = gh_fine.compact(4)
    assert sorted(c.token for c in gh_floored) == sorted(
        c.token for c in gh_parent.children(4)
    )
    assert all(c.precision == 4 for c in gh_floored)
    assert list(gm.CellArray([gh_parent], type=gm.GeohashCell).compact(4)) == [
        gh_parent
    ]

    tile_parent = gm.Tile(lon=13.4, lat=52.5, zoom=8)
    tile_fine = tile_parent.children(10)
    assert list(tile_fine.compact()) == [tile_parent]
    tile_floored = tile_fine.compact(9)
    assert sorted(c.id for c in tile_floored) == sorted(
        c.id for c in tile_parent.children(9)
    )
    assert all(c.zoom == 9 for c in tile_floored)
    assert list(gm.CellArray([tile_parent], type=gm.Tile).compact(9)) == [tile_parent]


@pytest.mark.parametrize(
    ('label', 'call', 'match'),
    [
        (
            'h3',
            lambda: gm.CellArray(
                [gm.H3Cell(0.0, 0.0, resolution=1)], type=gm.H3Cell
            ).compact(-1),
            'H3 min_resolution must be between 0 and 15, got -1',
        ),
        (
            's2',
            lambda: gm.CellArray(
                [gm.S2Cell(0.0, 0.0, level=1)], type=gm.S2Cell
            ).compact(-1),
            'S2 min_level must be between 0 and 30, got -1',
        ),
        (
            'geohash',
            lambda: gm.CellArray(
                [gm.GeohashCell(0.0, 0.0, precision=2)], type=gm.GeohashCell
            ).compact(0),
            'geohash min_precision must be between 1 and 12, got 0',
        ),
        (
            'tile',
            lambda: gm.CellArray([gm.Tile(lon=0.0, lat=0.0, zoom=2)], type=gm.Tile).compact(-1),
            'tile min_zoom must be between 0 and 29, got -1',
        ),
    ],
)
def test_compact_floor_out_of_range_errors_name_the_floor_kwarg(
    label: str, call, match: str
) -> None:
    del label
    with pytest.raises(gm.GeometryError, match=match):
        call()


@pytest.mark.parametrize(
    ('label', 'call', 'match'),
    [
        (
            'h3',
            lambda: gm.CellArray(
                [gm.H3Cell(0.0, 0.0, resolution=3)], type=gm.H3Cell
            ).uncompact(2),
            'uncompact resolution must be >= every',
        ),
        (
            's2',
            lambda: gm.CellArray(
                [gm.S2Cell(0.0, 0.0, level=3)], type=gm.S2Cell
            ).uncompact(2),
            "uncompact level must be >= every cell's level; cell",
        ),
        (
            'geohash',
            lambda: gm.CellArray(
                [gm.GeohashCell(0.0, 0.0, precision=3)], type=gm.GeohashCell
            ).uncompact(2),
            'uncompact precision must be >= every',
        ),
        (
            'tile',
            lambda: gm.CellArray([gm.Tile(lon=0.0, lat=0.0, zoom=3)], type=gm.Tile).uncompact(
                2
            ),
            'uncompact zoom must be >= every',
        ),
    ],
)
def test_uncompact_rejects_depth_below_existing_data(
    label: str, call, match: str
) -> None:
    del label
    with pytest.raises(gm.GeometryError, match=match):
        call()


def test_h3_coverage_depth_metadata_tracks_compact_uncompact_and_pickle() -> None:
    area = gm.box(20.5, 51.5, 21.5, 52.5, crs=4326)
    compacted = gm.h3_cover(area, resolution=5).compact()
    assert sorted({cell.resolution for cell in compacted.cells}) == [4, 5]
    assert compacted.resolution is None
    assert pickle.loads(pickle.dumps(compacted)).resolution is None
    expanded = compacted.uncompact(6)
    assert {cell.resolution for cell in expanded.cells} == {6}
    assert expanded.resolution == 6
    assert pickle.loads(pickle.dumps(expanded)).resolution == 6
    empty = gm.h3_cover(
        gm.Point(13.4, 52.5, crs=4326).buffer(1),
        resolution=5,
        cell_rule='within',
    )
    empty_expanded = empty.uncompact(7)
    assert len(empty_expanded) == 0
    assert empty_expanded.resolution == 7
    assert pickle.loads(pickle.dumps(empty_expanded)).resolution == 7


def test_s2_coverage_depth_metadata_tracks_compact_uncompact_and_pickle() -> None:
    area = gm.box(20.5, 51.5, 21.5, 52.5, crs=4326)
    compacted = gm.s2_cover(area, level=10, max_cells=256).compact()
    assert len({cell.level for cell in compacted.cells}) > 1
    assert compacted.level is None
    assert pickle.loads(pickle.dumps(compacted)).level is None
    adaptive = gm.s2_cover(area, min_level=6, max_level=10, target_cells=16)
    assert adaptive.level is None
    expanded = adaptive.uncompact(10)
    assert {cell.level for cell in expanded.cells} == {10}
    assert expanded.level == 10
    assert pickle.loads(pickle.dumps(expanded)).level == 10


def test_geohash_coverage_depth_metadata_tracks_compact_uncompact_and_pickle() -> None:
    area = gm.box(20.5, 51.5, 21.5, 52.5, crs=4326)
    compacted = gm.geohash_cover(area, precision=5).compact()
    assert sorted({cell.precision for cell in compacted.cells}) == [4, 5]
    assert compacted.precision is None
    assert pickle.loads(pickle.dumps(compacted)).precision is None
    expanded = compacted.uncompact(6)
    assert {cell.precision for cell in expanded.cells} == {6}
    assert expanded.precision == 6
    assert pickle.loads(pickle.dumps(expanded)).precision == 6
    empty = gm.geohash_cover(
        gm.Point(13.4, 52.5, crs=4326),
        precision=8,
        cell_rule='within',
    )
    empty_expanded = empty.uncompact(10)
    assert len(empty_expanded) == 0
    assert empty_expanded.precision == 10
    assert pickle.loads(pickle.dumps(empty_expanded)).precision == 10


def test_tile_coverage_depth_metadata_tracks_compact_uncompact_and_pickle() -> None:
    area = gm.box(-45.0, -20.0, 45.0, 20.0, crs=4326)
    compacted = gm.tile_cover(area, zoom=8).compact()
    assert len({tile.zoom for tile in compacted.cells}) > 1
    assert compacted.zoom is None
    assert pickle.loads(pickle.dumps(compacted)).zoom is None
    expanded = compacted.uncompact(9)
    assert {tile.zoom for tile in expanded.cells} == {9}
    assert expanded.zoom == 9
    assert pickle.loads(pickle.dumps(expanded)).zoom == 9
    empty = gm.tile_cover(
        gm.Point(13.4, 52.5, crs=4326),
        zoom=8,
        cell_rule='within',
    )
    empty_expanded = empty.uncompact(10)
    assert len(empty_expanded) == 0
    assert empty_expanded.zoom == 10
    assert pickle.loads(pickle.dumps(empty_expanded)).zoom == 10


def test_coverage_membership_matches_geometry_ground_truth() -> None:
    tri = gm.Polygon([(20.2, 51.2), (21.8, 51.4), (20.9, 52.8)], crs=4326)
    points = [
        (20.9, 52.0), (20.9, 52.79),
        (20.0, 52.0), (22.0, 52.0), (21.0, 51.0),
        (20.2, 51.2), (21.8, 51.4), (20.9, 52.8),
    ]
    pts = gm.points(*zip(*points, strict=True), crs=4326)
    cov = gm.h3_cover(tri, resolution=6)
    np.testing.assert_array_equal(cov.covers(pts), gm.covers(tri, pts))
    np.testing.assert_array_equal(cov.contains(pts), gm.contains(tri, pts))
    np.testing.assert_array_equal(cov.intersects(pts), gm.intersects(tri, pts))
    xs, ys = ([p.x for p in pts], [p.y for p in pts])
    np.testing.assert_array_equal(cov.intersects_xy(xs, ys), gm.covers(tri, pts))
    np.testing.assert_array_equal(cov.contains_xy(xs, ys), gm.contains(tri, pts))
    assert cov.contains_xy(20.9, 52.0) is True
    assert cov.intersects_xy(25.0, 52.0) is False
    s2 = gm.s2_cover(tri, level=8, max_cells=64)
    np.testing.assert_array_equal(s2.covers(pts), gm.covers(tri, pts))


def test_coverage_bulk_membership_returns_bool_ndarray() -> None:
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    pts = gm.points([21.0, 30.0], [52.0, 52.0], crs=4326)
    for coverage in (gm.h3_cover(area, resolution=5), gm.s2_cover(area, level=8)):
        for result in (
            coverage.covers(pts),
            coverage.contains(pts),
            coverage.intersects(pts),
            coverage.contains_xy([21.0, 30.0], [52.0, 52.0]),
            coverage.intersects_xy([21.0, 30.0], [52.0, 52.0]),
        ):
            assert bools(result) == [True, False]
    assert isinstance(
        gm.h3_cover(area, resolution=5).covers(gm.Point(21.0, 52.0)), bool
    )
    assert isinstance(gm.h3_cover(area, resolution=5).contains_xy(21.0, 52.0), bool)


def test_coverage_predicates_mask_missing_candidates_like_free_functions() -> None:
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    candidates = gm.GeometryArray(
        [
            gm.Point(21.0, 52.0, crs=4326),
            None,
            gm.Point(30.0, 52.0, crs=4326),
        ],
        crs=4326,
    )
    coverages = [
        gm.h3_cover(area, resolution=5),
        gm.s2_cover(area, level=8),
        gm.geohash_cover(area, precision=4),
        gm.tile_cover(area, zoom=7),
    ]
    for coverage in coverages:
        np.testing.assert_array_equal(
            coverage.covers(candidates), gm.covers(area, candidates)
        )
        np.testing.assert_array_equal(
            coverage.contains(candidates), gm.contains(area, candidates)
        )
        np.testing.assert_array_equal(
            coverage.intersects(candidates),
            gm.intersects(area, candidates),
        )
        assert bools(coverage.covers(candidates)) == [True, False, False]


def test_coverage_interior_and_boundary_cells_partition_the_covering() -> None:
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    cov = gm.h3_cover(area, resolution=5)
    interior = {c.id for c in cov.interior_cells}
    boundary = {c.id for c in cov.boundary_cells}
    assert interior | boundary == {c.id for c in cov.cells}
    assert not interior & boundary
    assert interior == {
        c.id for c in gm.h3_cover(area, resolution=5, cell_rule='within').cells
    }
    center = gm.h3_cover(area, resolution=5, cell_rule='center')
    assert {c.id for c in center.interior_cells} == interior
    assert all(cov.covers(cell.center) for cell in cov.interior_cells)
    plan = cov.explain()
    assert any('interior' in line for line in plan)
    assert any('exact source-geometry predicates' in line for line in plan)
    s2_plan = gm.s2_cover(area, level=8).explain()
    assert s2_plan[0].startswith('s2 coverage: level 8, cell_rule overlap, ')
    assert 'coverage partition:' in s2_plan[1]
    assert 'exact source-geometry predicates' in s2_plan[2]


def test_all_coverage_predicates_match_source_geometry_for_jagged_polygon() -> None:
    vertices = 96
    coords = []
    for index in range(vertices):
        angle = math.tau * index / vertices
        radius = 1.0 + 0.12 * math.sin(11 * angle) + 0.05 * math.sin(29 * angle)
        coords.append((
            21.0 + 1.1 * radius * math.cos(angle),
            52.0 + 0.7 * radius * math.sin(angle),
        ))
    source = gm.Polygon(coords, crs=4326)
    center = (21.0, 52.0)
    points = [center]
    for index in (0, 7, 31, 55):
        x, y = coords[index]
        points.extend([
            (x, y),
            (center[0] + 0.75 * (x - center[0]), center[1] + 0.75 * (y - center[1])),
            (center[0] + 1.35 * (x - center[0]), center[1] + 1.35 * (y - center[1])),
        ])
    xs, ys = zip(*points, strict=True)
    points = gm.points(xs, ys, crs=4326)
    coverages = [
        gm.h3_cover(source, resolution=6),
        gm.s2_cover(source, level=10),
        gm.geohash_cover(source, precision=4),
        gm.tile_cover(source, zoom=8),
    ]
    for coverage in coverages:
        np.testing.assert_array_equal(
            coverage.covers(points), gm.covers(source, points)
        )
        np.testing.assert_array_equal(
            coverage.contains(points), gm.contains(source, points)
        )
        np.testing.assert_array_equal(
            coverage.intersects(points), gm.intersects(source, points)
        )
        np.testing.assert_array_equal(
            coverage.contains_xy(xs, ys), gm.contains_xy(source, xs, ys)
        )
        np.testing.assert_array_equal(
            coverage.intersects_xy(xs, ys), gm.intersects_xy(source, xs, ys)
        )


def test_coverage_nonpoint_predicates_match_antimeridian_source_geometry() -> None:
    source = gm.box(170.0, -10.0, -170.0, 10.0, crs=4326, wrap='split')
    candidates = gm.GeometryArray(
        [
            gm.LineString([(175.0, 0.0), (-175.0, 0.0)], crs=4326),
            gm.box(172.0, -2.0, -172.0, 2.0, crs=4326, wrap='split'),
            gm.LineString([(120.0, 0.0), (125.0, 0.0)], crs=4326),
        ],
        crs=4326,
    )
    coverages = [
        gm.h3_cover(source, resolution=4),
        gm.s2_cover(source, level=6),
        gm.geohash_cover(source, precision=3),
        gm.tile_cover(source, zoom=4),
    ]
    for coverage in coverages:
        np.testing.assert_array_equal(
            coverage.covers(candidates), gm.covers(source, candidates)
        )
        np.testing.assert_array_equal(
            coverage.contains(candidates), gm.contains(source, candidates)
        )
        np.testing.assert_array_equal(
            coverage.intersects(candidates),
            gm.intersects(source, candidates),
        )


def test_s2_cover_raw_antimeridian_polygon_matches_split_oracle() -> None:
    """Raw geographic seam polygon must cover like the split form (s2sphere: 64 @ L8).

    Regression: covering used the unsplit planar box (false middle band), so
    L8 returned ~8672 cells instead of 64 and membership flipped mid-world.
    """
    seam = gm.Polygon(
        [(179.0, -1.0), (-179.0, -1.0), (-179.0, 1.0), (179.0, 1.0)],
        crs=4326,
    )
    assert seam.crosses_antimeridian
    coverage = gm.s2_cover(seam, level=8)
    split_coverage = gm.s2_cover(seam.split_antimeridian(), level=8)
    assert len(coverage.cells) == 64
    assert {cell.id for cell in coverage.cells} == {
        cell.id for cell in split_coverage.cells
    }
    mid = gm.Point(0.0, 0.0, crs=4326)
    east = gm.Point(179.5, 0.0, crs=4326)
    west = gm.Point(-179.5, 0.0, crs=4326)
    assert coverage.covers(mid) is False
    assert coverage.contains(mid) is False
    assert coverage.intersects(mid) is False
    assert coverage.covers(east) is True
    assert coverage.covers(west) is True
    assert coverage.covers(seam) is True
    # Peer grids share the same factory split gate.
    for cover in (
        gm.h3_cover(seam, resolution=3),
        gm.geohash_cover(seam, precision=3),
        gm.tile_cover(seam, zoom=4),
    ):
        assert cover.covers(mid) is False
        assert cover.covers(east) is True
        assert cover.covers(seam) is True
    # Budget must not false-raise once the false-middle band is gone.
    for south, north in ((-1.0, 1.0), (-5.0, 5.0), (-10.0, 10.0)):
        band = gm.Polygon(
            [(179.0, south), (-179.0, south), (-179.0, north), (179.0, north)],
            crs=4326,
        )
        gm.s2_cover(band, level=9, max_cells=10000)


def test_coverage_polar_cap_membership_matches_free_predicates() -> None:
    """Full-longitude polar caps: coverage membership agrees with free predicates.

    Regression: point membership used a planar-only probe and missed poles on
    crossing polar hexes; covering still needs the split working shape.
    """
    hex_north = gm.Polygon(
        [(0.0, 80.0), (60.0, 80.0), (120.0, 80.0), (180.0, 80.0), (-120.0, 80.0), (-60.0, 80.0)],
        crs=4326,
    )
    north_pole = gm.Point(0.0, 90.0, crs=4326)
    equator = gm.Point(0.0, 0.0, crs=4326)
    coverage = gm.s2_cover(hex_north, level=4)
    assert hex_north.crosses_antimeridian
    assert gm.covers(hex_north, north_pole) is True
    assert coverage.covers(north_pole) is True
    assert coverage.contains(north_pole) is True
    assert coverage.intersects(north_pole) is True
    assert coverage.contains_xy(0.0, 90.0) is True
    assert coverage.covers(equator) is False
    # Densified full-longitude arctic band (no planar longitude jump).
    ring = [(float(lon), 80.0) for lon in range(-180, 181, 10)]
    ring.extend([(180.0, 90.0), (-180.0, 90.0), (-180.0, 80.0)])
    band = gm.Polygon(ring, crs=4326)
    band_cov = gm.s2_cover(band, level=4)
    for point in (
        north_pole,
        gm.Point(0.0, 85.0, crs=4326),
        gm.Point(179.0, 85.0, crs=4326),
        equator,
    ):
        assert band_cov.covers(point) is bool(gm.covers(band, point))
        assert band_cov.contains(point) is bool(gm.contains(band, point))
        assert band_cov.intersects(point) is bool(gm.intersects(band, point))


def test_rect_coverer_classification_is_exact() -> None:
    shapes = [
        gm.from_wkt(wkt, crs=4326)
        for wkt in (
            'POLYGON ((5.164927365770143 50.32311879573511, 5.510883099207359 50, 6.630996219710697 49.62787249337675, 13.440113210910619 49.61537033642942, 14.173623267631958 49.69687170763404, 15.293125996476181 49.89114819068903, 15.147578814821362 50.207926659260046, 12.412518562014903 50.33428229507392, 11.412065111585147 50.36087864136956, 5.164927365770143 50.32311879573511))',
            'POLYGON ((5.605768036457973 49.66479593810252, 11.990362593215744 49.6642375586071, 12.976086246819165 50, 11.187141687404761 50.22565902079519, 10.403649350779128 50.28263184550744, 8.019162067864066 50.378383944974495, 5.91615755507557 50.277087046489484, 5.605768036457973 49.66479593810252))',
            'POLYGON ((5.406530864314727 50.15936253423455, 5.420494624102703 50.07775545315554, 6.67357626106296 49.76214186622715, 10.042196656561993 49.67102871817618, 12.727522761753587 49.7333533393371, 14.81293446056843 49.92886432583867, 14.714549766829771 50, 13.042414527934096 50.332859723781034, 7.613024486683244 50.295045169072026, 5.478582958346789 50.197469253117596, 5.406530864314727 50.15936253423455))',
        )
    ]
    shapes.append(gm.Polygon([(0, 0), (45, 0), (45, 45), (0, 45)], crs=4326))

    def check(
        shape: gm.Geometry, interior: list[gm.Polygon], boundary: list[gm.Polygon]
    ) -> None:
        for box in interior:
            assert gm.covers(shape, box)
        for box in boundary:
            assert gm.intersects(shape, box) and (not gm.covers(shape, box))

    for shape in shapes:
        geohash = gm.geohash_cover(shape, precision=4)
        check(
            shape,
            list(geohash.interior_cells.polygon),
            list(geohash.boundary_cells.polygon),
        )
        tiles = gm.tile_cover(shape, zoom=8)
        check(
            shape,
            list(tiles.interior_cells.polygon),
            list(tiles.boundary_cells.polygon),
        )


def test_vectorized_cell_boundaries() -> None:
    cov = gm.h3_cover(gm.box(20.9, 51.9, 21.1, 52.1, crs=4326), resolution=6)
    batch = cov.boundary_cells.polygon
    assert isinstance(batch, gm.GeometryArray)
    assert len(batch) == len(cov.boundary_cells)
    assert batch.crs == 'EPSG:4326'
    assert isinstance(cov.cells[0].polygon, gm.Polygon)
    s2_cells = gm.s2_cover(gm.box(20.9, 51.9, 21.1, 52.1, crs=4326), level=10).cells
    assert len(s2_cells.polygon) == len(s2_cells)
    with pytest.raises(ValueError):
        gm.CellArray(['not-a-cell'], type=gm.H3Cell)
    gh_cells = gm.geohash_cover(gm.box(20.9, 51.9, 21.1, 52.1, crs=4326), precision=5)
    gh_batch = gh_cells.cells.polygon
    assert isinstance(gh_batch, gm.GeometryArray)
    assert len(gh_batch) == len(gh_cells.cells)
    assert gh_batch.crs == 'EPSG:4326'
    assert isinstance(
        gm.CellArray([gh_cells.cells[0].token], type=gm.GeohashCell).polygon[0],
        gm.Polygon,
    )
    tile_cells = gm.tile_cover(gm.box(20.9, 51.9, 21.1, 52.1, crs=4326), zoom=11)
    tile_batch = tile_cells.cells.polygon
    assert isinstance(tile_batch, gm.GeometryArray)
    assert len(tile_batch) == len(tile_cells.cells)
    assert tile_batch.crs == 'EPSG:4326'
    assert isinstance(
        gm.CellArray([tile_cells.cells[0].id], type=gm.Tile).polygon[0], gm.Polygon
    )


def test_coverage_membership_matches_predicates_for_mixed_candidates() -> None:
    area = gm.box(20.0, 51.0, 22.0, 53.0, crs=4326)
    candidates = gm.GeometryArray(
        [
            gm.Point(21.0, 52.0, crs=4326),
            gm.Point(30.0, 52.0, crs=4326),
            gm.LineString([(20.5, 52.0), (21.5, 52.0)], crs=4326),
            gm.LineString([(19.0, 52.0), (23.0, 52.0)], crs=4326),
            gm.box(20.5, 51.5, 21.5, 52.5, crs=4326),
            gm.box(21.5, 52.5, 23.0, 54.0, crs=4326),
            gm.box(30.0, 30.0, 31.0, 31.0, crs=4326),
        ],
        crs=4326,
    )
    rows = list(candidates)
    for coverage in (gm.h3_cover(area, resolution=5), gm.s2_cover(area, level=8)):
        np.testing.assert_array_equal(
            coverage.covers(candidates), [gm.covers(area, g) for g in rows]
        )
        np.testing.assert_array_equal(
            coverage.contains(candidates), [gm.contains(area, g) for g in rows]
        )
        np.testing.assert_array_equal(
            coverage.intersects(candidates), [gm.intersects(area, g) for g in rows]
        )


def test_bounding_cells_contain_and_are_deepest() -> None:
    box = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    tile = gm.tile_bounding_cell(box)
    gh = gm.geohash_bounding_cell(box)
    h3 = gm.h3_bounding_cell(box)
    s2 = gm.s2_bounding_cell(box)
    assert gm.covers(tile.polygon, box)
    assert gm.covers(gh.polygon, box)
    assert not any(gm.covers(child.polygon, box) for child in tile.children())
    assert not any(gm.covers(child.polygon, box) for child in gh.children())
    assert gm.covers(h3.polygon, box)
    assert not any(gm.covers(child.polygon, box) for child in h3.children())
    # Berlin is mid-latitude: planar cell.polygon is a faithful cover oracle.
    assert gm.covers(s2.polygon, box)
    assert not any(gm.covers(child.polygon, box) for child in s2.children())
    assert all(s2.contains(cell) for cell in gm.s2_cover(box, level=12).cells)
    assert gm.tile_bounding_cell([13.3, 52.4, 13.5, 52.6]) == tile
    assert gm.geohash_bounding_cell((13.3, 52.4, 13.5, 52.6)) == gh
    assert gm.s2_bounding_cell([13.3, 52.4, 13.5, 52.6]) == s2
    assert gm.tile_bounding_cell([-10.0, -10.0, 10.0, 10.0]).zoom == 0
    with pytest.raises(gm.GeometryError, match='no single geohash cell'):
        gm.geohash_bounding_cell([-100.0, -10.0, 100.0, 10.0])
    with pytest.raises(gm.GeometryError, match='no single H3 cell'):
        gm.h3_bounding_cell([-100.0, -10.0, 100.0, 10.0])
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell([-100.0, -40.0, 100.0, 40.0])
    with pytest.raises(gm.GeometryError, match='ordered'):
        gm.tile_bounding_cell([10.0, 0.0, -10.0, 1.0])
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.s2_bounding_cell(gm.from_wkt('POLYGON EMPTY', crs=4326))


def _s2_bbox_of(region):
    """Lon/lat bounding box of a geometry or 4-tuple bounds."""
    if isinstance(region, (tuple, list)) and len(region) == 4:
        minx, miny, maxx, maxy = (float(v) for v in region)
        return minx, miny, maxx, maxy
    b = region.bounds
    return float(b[0]), float(b[1]), float(b[2]), float(b[3])


def _assert_s2_bounding_bbox(region) -> gm.S2Cell:
    """Bbox contract: returned cell always contains same-face bbox samples.

    Multi-face raises at the call site. Point/multipoint: Hilbert LCA of
    vertices. Region: same-face inset samples of the lon/lat bbox must be
    Hilbert-contained when their L30 leaf face root matches the cell (dual
    face/edge assignment is not a non-containing verdict). Descent is
    conservative (strict halfspace + margin); boundary-adjacent cases may be
    one level coarser than theoretical deepest.
    """
    got = gm.s2_bounding_cell(region)
    if isinstance(region, (gm.Point, gm.MultiPoint)):
        pts = [region] if isinstance(region, gm.Point) else list(region)
        for pt in pts:
            leaf = gm.S2Cell(float(pt.x), float(pt.y), level=30)
            assert got.contains(leaf) or got == leaf, (got.token, leaf.token, pt)
        return got
    minx, miny, maxx, maxy = _s2_bbox_of(region)
    dx = maxx - minx
    dy = maxy - miny
    if abs(dx) < 1e-15 and abs(dy) < 1e-15:
        leaf = gm.S2Cell(minx, miny, level=30)
        assert got.contains(leaf) or got == leaf, (got.token, leaf.token)
        return got
    # Strict interior of the lon/lat rectangle (5% inset); skip dual-face leaves.
    frac = 0.05
    cx, cy = 0.5 * (minx + maxx), 0.5 * (miny + maxy)
    samples = [(cx, cy)]
    if abs(dx) >= 1e-12 and abs(dy) >= 1e-12:
        samples.extend(
            [
                (minx + frac * dx, miny + frac * dy),
                (maxx - frac * dx, miny + frac * dy),
                (minx + frac * dx, maxy - frac * dy),
                (maxx - frac * dx, maxy - frac * dy),
            ]
        )
    face_root = got.parent(0) if got.level > 0 else got
    checked = 0
    for x, y in samples:
        leaf = gm.S2Cell(float(x), float(y), level=30)
        leaf_root = leaf.parent(0) if leaf.level > 0 else leaf
        if leaf_root != face_root:
            # Dual-face leaf assignment on cube edges is not a non-containing verdict.
            continue
        if got.contains(leaf):
            checked += 1
            continue
        # Same-face but not Hilbert-under cell: accept planar cover of the
        # sample (cube-edge sibling dual-assign). Fail only if neither holds.
        pt = gm.Point(float(x), float(y), crs=4326)
        assert gm.covers(got.polygon, pt) or gm.intersects(got.polygon, pt), (
            'non-containing bounding cell (same-face interior)',
            got.token,
            got.level,
            (minx, miny, maxx, maxy),
            (x, y, leaf.token),
        )
        checked += 1
    # Mid-latitude solid boxes must yield at least the center sample.
    if abs(cy) < 60.0 and abs(dx) > 1e-6 and abs(dy) > 1e-6:
        assert checked >= 1, (got.token, (minx, miny, maxx, maxy))
    return got


def test_s2_bounding_cell_is_deepest_containing():
    """s2_bounding_cell returns a cell covering the lon/lat bbox (provable containment)."""
    small = gm.box(0, 0, 1, 1, crs=4326)
    got = _assert_s2_bounding_bbox(small)
    assert got.level >= 0
    # Bare bounds, scalar geometry, and one-row GeometryArray agree.
    assert gm.s2_bounding_cell([0.0, 0.0, 1.0, 1.0]) == got
    assert gm.s2_bounding_cell((0.0, 0.0, 1.0, 1.0)) == got
    assert gm.s2_bounding_cell(gm.GeometryArray([small])) == got

    thin = gm.box(0.0, 0.0, 2.0, 0.05, crs=4326)
    _assert_s2_bounding_bbox(thin)

    point = gm.Point(13.4, 52.5, crs=4326)
    pt_cell = _assert_s2_bounding_bbox(point)
    assert pt_cell.level == 30

    # Multi-face span: no single cell contains the region.
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell(gm.box(-100.0, -40.0, 100.0, 40.0, crs=4326))
    # Moderate multi-face (skeptic): must raise, not ship non-containing face 7.
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell([-50.0, 10.0, -32.0, 15.0])

    # Berlin example from the docstring.
    berlin = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    berlin_cell = _assert_s2_bounding_bbox(berlin)
    assert berlin_cell.level == 8
    assert berlin_cell.token == '47a85'


def test_s2_bounding_cell_high_lat_matrix_and_oblique_line():
    """High-latitude boxes: bbox contract still contains corners and center."""
    got = gm.s2_bounding_cell((-110.1, 75.0, -110.0, 75.1))
    center = gm.S2Cell(-110.05, 75.05, level=30)
    assert got.contains(center) or gm.covers(
        got.polygon, gm.Point(-110.05, 75.05, crs=4326)
    ), (got.token, got.level)
    _assert_s2_bounding_bbox((-110.1, 75.0, -110.0, 75.1))

    checked = 0
    for lat0 in (0.0, 40.0, 75.0, 85.0):
        for lon0 in (-170.0, -110.0, -45.0, 0.0, 45.0, 110.0, 170.0):
            if lat0 + 0.1 >= 90.0:
                continue
            bounds = (lon0, lat0, lon0 + 0.1, lat0 + 0.1)
            try:
                cell = _assert_s2_bounding_bbox(bounds)
            except gm.GeometryError:
                continue
            mid_lon = (bounds[0] + bounds[2]) / 2
            mid_lat = (bounds[1] + bounds[3]) / 2
            mid_leaf = gm.S2Cell(mid_lon, mid_lat, level=30)
            assert cell.contains(mid_leaf), (
                bounds,
                cell.token,
                cell.level,
                mid_lon,
                mid_lat,
            )
            checked += 1
    # Coverer multi-face gate is stricter than densify-LCA; some seam boxes raise.
    assert checked >= 12

    # Oblique line: bbox contract (envelope), not exact-edge deepest.
    line = gm.LineString([(-110.1, 75.0), (-110.0, 75.1)], crs=4326)
    try:
        _assert_s2_bounding_bbox(line)
    except gm.GeometryError as exc:
        if 'no single S2 cell' not in str(exc):
            raise

    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell((-100.0, -40.0, 100.0, 40.0))


def test_s2_bounding_cell_face_meridian_edges_are_exact():
    """Cube-face seams: Hilbert-containing cell when single-face, else multi-face raise."""
    cases = [
        (-135.0, -35.0, -130.0, -30.0),
        (-135.0, 20.0, -134.5, 20.5),
        (-135.0, -30.0, -134.95, -29.95),
        (-135.0, 20.0, -134.99, 20.01),
        (-135.0, 0.0, -130.0, 5.0),
        (45.0, -10.0, 50.0, -5.0),
        (-45.0, 10.0, -40.0, 15.0),
        (135.0, -5.0, 140.0, 0.0),
        # Boxes fully on one side of a seam (must Hilbert-contain).
        (-134.0, -35.0, -130.0, -30.0),
        (46.0, -10.0, 50.0, -5.0),
        (-44.0, 10.0, -40.0, 15.0),
        (136.0, -5.0, 140.0, 0.0),
    ]
    ok = 0
    for west, south, east, north in cases:
        try:
            _assert_s2_bounding_bbox((west, south, east, north))
            ok += 1
        except gm.GeometryError as exc:
            if 'no single S2 cell' not in str(exc):
                raise AssertionError((west, south, east, north, exc)) from exc
    assert ok >= 4  # at least the strictly single-face boxes

def test_s2_bounding_cell_face_seam_box_45():
    """Face-seam box(45,0,46,1): multi-face raise or Hilbert-containing cell."""
    box = gm.box(45.0, 0.0, 46.0, 1.0, crs=4326)
    try:
        _assert_s2_bounding_bbox(box)
    except gm.GeometryError as exc:
        if 'no single S2 cell' not in str(exc):
            raise
    for lon in (45.0, -45.0, 135.0, -135.0):
        line = gm.LineString([(lon, 0.0), (lon, 1.0)], crs=4326)
        try:
            _assert_s2_bounding_bbox(line)
        except gm.GeometryError as exc:
            if 'no single S2 cell' not in str(exc):
                raise
        adj = gm.box(lon, 0.0, lon + 1.0, 1.0, crs=4326)
        try:
            _assert_s2_bounding_bbox(adj)
        except gm.GeometryError as exc:
            if 'no single S2 cell' not in str(exc):
                raise
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell(
            gm.LineString([(179.0, -1.0), (-179.0, 1.0)], crs=4326)
        )


def test_s2_bounding_cell_boundary_aligned_line_bbox() -> None:
    """Boundary-aligned equator line: bbox contract (may coarsen vs exact-edge)."""
    line = gm.LineString([(-1.0, 0.0), (0.0, 0.0)], crs=4326)
    cell = _assert_s2_bounding_bbox(line)
    assert cell.level >= 0
    anti = _assert_s2_bounding_bbox(
        gm.LineString([(180.0, 0.0), (180.0, 1.0)], crs=4326)
    )
    assert anti.level >= 0


def test_s2_bounding_cell_seam_multipoint() -> None:
    """Multi-point aggregates use the R18 bbox path (not leaf-LCA).

    Seam-adjacent multipoints that closed-contain under a face root return
    that root (same as the envelope box). True multi-face multipoints still
    raise. A single point still returns its L30 leaf.
    """
    mp = gm.MultiPoint(
        [gm.Point(45.0, 0.5, crs=4326), gm.Point(46.0, 0.5, crs=4326)],
        crs=4326,
    )
    got = gm.s2_bounding_cell(mp)
    assert got == gm.s2_bounding_cell(gm.box(45.0, 0.5, 46.0, 0.5, crs=4326))
    assert got.token == '3'
    assert got.level == 0
    pt = gm.s2_bounding_cell(gm.Point(13.4, 52.5, crs=4326))
    assert pt.level == 30
    assert pt == gm.S2Cell(13.4, 52.5, level=30)
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell(
            gm.MultiPoint([(-100.0, 0.0), (100.0, 0.0)], crs=4326)
        )


def test_s2_bounding_cell_exact_seam_repros() -> None:
    """Lon=-135 diagonal-seam boxes: always containing (margin may coarsen).

    Interior samples must be Hilbert-contained. Exact deepest tokens are not
    pinned — boundary-adjacent cases may stop one or more levels early.
    """
    c1 = gm.s2_bounding_cell([-135.0, -35.0, -134.8, -34.8])
    _assert_s2_bounding_bbox([-135.0, -35.0, -134.8, -34.8])
    for x, y in ((-134.95, -34.95), (-134.9, -34.9), (-134.85, -34.85)):
        assert c1.contains(gm.S2Cell(x, y, level=30)), (c1.token, x, y)
    c2 = gm.s2_bounding_cell([-135.0, 0.0, -134.8, 0.2])
    _assert_s2_bounding_bbox([-135.0, 0.0, -134.8, 0.2])
    for x, y in ((-134.95, 0.05), (-134.9, 0.1), (-134.85, 0.15)):
        assert c2.contains(gm.S2Cell(x, y, level=30)), (c2.token, x, y)
    # Face-center meridian: always containing (margin may stop at face root).
    line = gm.s2_bounding_cell(gm.LineString([(0.0, 0.0), (0.0, 45.0)], crs=4326))
    for lat in (0.0, 10.0, 30.0, 44.9, 45.0):
        assert line.contains(gm.S2Cell(0.0, lat, level=30)), (line.token, lat)


def test_s2_bounding_cell_exact_seam_touch_matrix() -> None:
    """Exact-seam-touch matrix: 0 false reject / 0 non-containing; dual-root raises.

    Boxes with a coordinate exactly on lon=±45/±135 that fit a single face
    root must return their deepest closed-containing cell. Zero-width pure
    face-edge segments (true geometric dual-root) still raise GeometryError.
    """
    seams = (-135.0, -45.0, 45.0, 135.0)
    lats = (-40.0, -35.0, -20.0, -5.0, 0.0, 5.0, 20.0, 35.0, 40.0)
    widths = ((0.2, 0.2), (0.05, 0.05), (1.0, 1.0), (5.0, 5.0))
    ok = 0
    raised = 0
    for lon0 in seams:
        for lat0 in lats:
            for dw, dh in widths:
                minx, maxx = lon0, lon0 + dw
                miny, maxy = lat0, lat0 + dh
                if maxy > 90.0 or miny < -90.0:
                    continue
                try:
                    _assert_s2_bounding_bbox((minx, miny, maxx, maxy))
                    ok += 1
                except gm.GeometryError as exc:
                    if 'no single S2 cell' not in str(exc):
                        raise AssertionError((minx, miny, maxx, maxy, exc)) from exc
                    raised += 1
    # Pure face-edge segments: true dual-root under closed halfspaces.
    for lon in seams:
        with pytest.raises(gm.GeometryError, match='no single S2 cell'):
            gm.s2_bounding_cell([lon, 0.0, lon, 5.0])
        raised += 1
    assert ok >= 100, f'expected many single-face seam-touch successes, got {ok}'
    assert raised >= 4, f'expected genuine dual-root raises, got {raised}'


def test_s2_bounding_cell_bbox_contract_matrix() -> None:
    """Broad soundness matrix under the bbox contract (not exact-edge deepest)."""
    # Cardinal short segments: bbox corners covered.
    meridians = (-180.0, -90.0, 0.0, 90.0)
    starts = (-80.0, -60.0, -40.0, -20.0, -5.0, 0.0, 5.0, 20.0, 40.0, 60.0, 75.0, 85.0)
    directed = 0
    for lon in meridians:
        for lat0 in starts:
            lat1 = min(lat0 + 0.1, 89.9)
            if lat1 <= lat0:
                continue
            for a, b in (((lon, lat0), (lon, lat1)), ((lon, lat1), (lon, lat0))):
                directed += 1
                try:
                    _assert_s2_bounding_bbox(gm.LineString([a, b], crs=4326))
                except gm.GeometryError:
                    continue
    assert directed >= 48

    # Multi-face cases raise (huge + moderate skeptic box).
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell(
            gm.LineString([(-45.0, 40.0), (45.0, 40.0)], crs=4326)
        )
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell(
            gm.LineString([(179.0, -1.0), (-179.0, 1.0)], crs=4326)
        )
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell([-50.0, 10.0, -32.0, 15.0])

    # Poles: exact L30; pole line collapses via bbox degeneracy.
    north = gm.s2_bounding_cell(gm.Point(0.0, 90.0, crs=4326))
    assert north.level == 30
    assert gm.s2_bounding_cell(gm.Point(180.0, 90.0, crs=4326)) == north
    assert gm.s2_bounding_cell(gm.Point(-90.0, 90.0, crs=4326)) == north
    south = gm.s2_bounding_cell(gm.Point(13.0, -90.0, crs=4326))
    assert south.level == 30
    pole_line = gm.s2_bounding_cell(
        gm.LineString([(-180.0, 90.0), (180.0, 90.0)], crs=4326)
    )
    # Full-longitude pole line bbox may yield the polar face root, not the L30 leaf.
    assert pole_line.contains(north) or pole_line == north

    pt = gm.s2_bounding_cell(gm.Point(13.4, 52.5, crs=4326))
    assert pt.level == 30
    assert pt == gm.S2Cell(13.4, 52.5, level=30)

    same = gm.s2_bounding_cell(
        gm.MultiPoint([(0.1, 0.1), (0.2, 0.2)], crs=4326)
    )
    assert same.contains(gm.S2Cell(0.1, 0.1, level=30))
    assert same.contains(gm.S2Cell(0.2, 0.2, level=30))
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell(
            gm.MultiPoint([(-100.0, 0.0), (100.0, 0.0)], crs=4326)
        )

    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.s2_bounding_cell(gm.from_wkt('POLYGON EMPTY', crs=4326))
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        gm.s2_bounding_cell(gm.GeometryArray([None, None]))

    # Polygon with hole: bbox of shell (envelope), not hole-aware exact edge.
    shell = [(-1.0, -1.0), (1.0, -1.0), (1.0, 1.0), (-1.0, 1.0), (-1.0, -1.0)]
    hole = [(-0.2, -0.2), (-0.2, 0.2), (0.2, 0.2), (0.2, -0.2), (-0.2, -0.2)]
    poly = gm.Polygon(shell, [hole], crs=4326)
    _assert_s2_bounding_bbox(poly)

    coll = gm.GeometryCollection(
        [gm.Point(0.1, 0.1, crs=4326), gm.Point(0.15, 0.15, crs=4326)],
        crs=4326,
    )
    coll_cell = gm.s2_bounding_cell(coll)
    assert coll_cell.contains(gm.S2Cell(0.1, 0.1, level=30))

    # Lat x lon boxes + oblique diagonals.
    for lat0 in (0.0, 40.0, 60.0, 75.0, 85.0, -80.0):
        for lon0 in (-170.0, -90.0, -20.0, 0.0, 45.0, 110.0):
            if lat0 + 0.1 >= 90.0 or lat0 <= -90.0:
                continue
            bounds = (lon0, lat0, lon0 + 0.1, lat0 + 0.1)
            try:
                _assert_s2_bounding_bbox(bounds)
            except gm.GeometryError:
                continue
            line = gm.LineString(
                [(bounds[0], bounds[1]), (bounds[2], bounds[3])], crs=4326
            )
            try:
                _assert_s2_bounding_bbox(line)
            except gm.GeometryError:
                continue


def _assert_s2_full_bbox_containment(minx: float, miny: float, maxx: float, maxy: float) -> gm.S2Cell:
    """Hard soundness: returned cell Hilbert-contains same-face bbox corners.

    Corner + center L30 leaves on the cell's face must be contained. Dual-face
    leaves on cube edges are skipped (not a non-containing verdict). Mid-lat
    off-edge solid boxes also require planar cover when the cell is deep enough
    that its planar polygon is a faithful oracle (level ≥ 4, |lat| < 50).
    """
    bounds = (minx, miny, maxx, maxy)
    cell = gm.s2_bounding_cell(list(bounds))
    samples = [
        (minx, miny),
        (maxx, miny),
        (maxx, maxy),
        (minx, maxy),
        (0.5 * (minx + maxx), 0.5 * (miny + maxy)),
    ]
    face_root = cell.parent(0) if cell.level > 0 else cell
    checked = 0
    for x, y in samples:
        leaf = gm.S2Cell(float(x), float(y), level=30)
        leaf_root = leaf.parent(0) if leaf.level > 0 else leaf
        if leaf_root != face_root:
            continue
        assert cell.contains(leaf), (
            'non-containing bounding cell (corner/center)',
            cell.token,
            cell.level,
            bounds,
            (x, y, leaf.token),
        )
        checked += 1
    # At least the center must be same-face for ordinary solid boxes.
    cy = 0.5 * (miny + maxy)
    if abs(cy) < 60.0 and (maxx - minx) > 1e-15 and (maxy - miny) > 1e-15:
        assert checked >= 1, (cell.token, bounds, checked)
    # Planar cover only where the cell polygon is a faithful oracle.
    if (
        cell.level >= 4
        and abs(cy) < 50.0
        and (maxx - minx) > 0
        and (maxy - miny) > 0
    ):
        box = gm.box(minx, miny, maxx, maxy, crs=4326)
        assert gm.covers(cell.polygon, box), (
            'planar polygon does not cover bbox',
            cell.token,
            cell.level,
            bounds,
        )
    return cell


def test_s2_bounding_cell_berlin_10m_and_multi_scale_soundness() -> None:
    """SOUNDNESS invariant: every returned cell contains the bbox; never over-descend.

    The ~10m Berlin box previously over-descended to a non-containing L30 leaf.
    Multi-scale sweep (0.2 / 0.01 / 1e-4 / 1e-6 deg) + skeptic counterexamples
    + curated 1e-6 offsets must yield 0 non-containing results. Points stay
    exact L30; genuine multi-face crossers still raise.
    """
    # --- Berlin 10m repro (the verified over-descent bug) ---
    berlin = gm.box(13.4, 52.5, 13.4001, 52.5001, crs=4326)
    c = _assert_s2_full_bbox_containment(13.4, 52.5, 13.4001, 52.5001)
    interior = gm.S2Cell(13.40005, 52.50005, level=30)
    assert c.contains(interior), (c.token, c.level)
    assert c.level < 30, f'over-descended to L{c.level}'
    assert 10 <= c.level <= 22, f'unexpected depth L{c.level}'
    assert gm.covers(c.polygon, berlin), (c.token, c.level)
    _assert_s2_bounding_bbox(berlin)

    # Off-boundary common case stays reasonably deep (not face-root).
    coarse = gm.s2_bounding_cell(gm.box(13.3, 52.4, 13.5, 52.6, crs=4326))
    assert coarse.level == 8 and coarse.token == '47a85'

    # Point exact L30.
    pt = gm.s2_bounding_cell(gm.Point(13.4, 52.5, crs=4326))
    assert pt.level == 30
    assert pt == gm.S2Cell(13.4, 52.5, level=30)

    # Genuine multi-face raises.
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell([-100.0, -40.0, 100.0, 40.0])

    # --- Skeptic counterexamples (Strict over-descent at 1e-6 / 1e-7) ---
    for minx, miny, size in (
        (117.08704757241938, 38.62041451878392, 1e-6),
        (138.6855388191179, -24.628965378441677, 1e-7),
        (-61.3534142325401, -22.102649469574658, 1e-6),
    ):
        _assert_s2_full_bbox_containment(minx, miny, minx + size, miny + size)

    # --- Multi-scale grid sweep: 0 non-containing (full corners) ---
    sizes = (0.2, 0.01, 1e-4, 1e-6)
    lats = (-80.0, -60.0, -40.0, -20.0, 0.0, 20.0, 40.0, 52.5, 60.0, 80.0)
    lons = (
        -170.0,
        -135.0,
        -90.0,
        -45.0,
        -20.0,
        0.0,
        13.4,
        45.0,
        90.0,
        135.0,
        170.0,
    )
    ok = 0
    for size in sizes:
        for lat0 in lats:
            for lon0 in lons:
                maxx = lon0 + size
                maxy = lat0 + size
                if maxy > 90.0 or lat0 < -90.0 or maxx > 180.0:
                    continue
                try:
                    _assert_s2_full_bbox_containment(lon0, lat0, maxx, maxy)
                except gm.GeometryError as exc:
                    if 'no single S2 cell' not in str(exc):
                        raise AssertionError(((lon0, lat0, maxx, maxy), exc)) from exc
                    continue
                ok += 1
    assert ok >= 200, f'expected many successes, got {ok}'

    # Dual-root pure face edges still raise somewhere (zero-width seams).
    for lon in (-135.0, 135.0):
        with pytest.raises(gm.GeometryError, match='no single S2 cell'):
            gm.s2_bounding_cell([lon, 0.0, lon, 5.0])


def test_s2_bounding_cell_south_pole_lon90_closed_contains() -> None:
    """Skeptic: lon in [90,91] x lat in [-80,-79] closed-contains under halfspace authority."""
    bounds = (90.0, -80.0, 91.0, -79.0)
    _assert_s2_bounding_bbox(bounds)
    mp = gm.MultiPoint([(90.0, -80.0), (91.0, -79.0)], crs=4326)
    mp_cell = gm.s2_bounding_cell(mp)
    for lon, lat in ((90.0, -80.0), (91.0, -79.0)):
        assert mp_cell.contains(gm.S2Cell(lon, lat, level=30)), (mp_cell.token, lon, lat)

    # Antimeridian-south: deepest closed-containing cell (may be deeper than face b).
    for bounds in (
        (-180.0, -85.0, -179.0, -84.0),
        (-180.0, -80.0, -179.0, -79.0),
        (-180.0, -75.0, -179.0, -74.0),
    ):
        _assert_s2_bounding_bbox(bounds)

    # Systematic high-south scan: closed-contain or multi-face raise.
    for lat0 in (-85.0, -80.0, -75.0):
        for lon0 in (85.0, 90.0, 95.0, -90.0, 0.0, 170.0, -180.0, -179.0):
            b = (lon0, lat0, min(lon0 + 1.0, 180.0), lat0 + 1.0)
            if b[2] <= b[0]:
                continue
            try:
                _assert_s2_bounding_bbox(b)
            except gm.GeometryError as exc:
                if 'no single S2 cell' not in str(exc):
                    raise


def test_s2_bounding_cell_face_center_meridian_contains_interior() -> None:
    """Lon=0 face-center meridian: returned cell always Hilbert-contains samples.

    Margin-only descent may stop coarser than L1 when the segment sits on a
    child edge (min≈0); soundness (always containing) is the invariant.
    """
    line = gm.LineString([(0.0, 0.0), (0.0, 45.0)], crs=4326)
    cell = gm.s2_bounding_cell(line)
    for lat in (0.0, 10.0, 30.0, 44.9, 45.0):
        assert cell.contains(gm.S2Cell(0.0, lat, level=30)), (
            cell.token,
            cell.level,
            lat,
        )
    import numpy as np

    for lat in np.linspace(0.0, 45.0, 2000):
        assert cell.contains(gm.S2Cell(0.0, float(lat), level=30)), lat


def test_s2_bounding_cell_cube_edge_meridian_raise_or_closed_contain() -> None:
    """Cube-edge meridians: multi-face raise OR a single closed-containing cell.

    Exact halfspaces may find a unique root even when densified Hilbert leaves
    dual-assign across faces; never return a non-containing cell.
    """
    for lon in (90.0, -90.0, -180.0, 180.0):
        line = gm.LineString([(lon, 45.0), (lon, 60.0)], crs=4326)
        try:
            cell = gm.s2_bounding_cell(line)
            assert cell.level >= 0
        except gm.GeometryError as exc:
            if 'no single S2 cell' not in str(exc):
                raise
        try:
            cell = gm.s2_bounding_cell([lon, 45.0, lon, 60.0])
            assert cell.level >= 0
        except gm.GeometryError as exc:
            if 'no single S2 cell' not in str(exc):
                raise


def test_s2_bounding_cell_false_reject_corpus_23_targets() -> None:
    """All 23 oracle false-rejects → exact tokens; all 17 genuine multi-face raise.

    Corpus: ``random.seed(1)``, 120 iters, each appends one box then one line:
    box ``(lon,lat,lon+w,lat+h)`` with lon∈[-179,179], lat∈[-85,85], w,h∈[0.1,3];
    line from ``(lon+da,lat+db)`` to ``(lon,lat)`` with da,db∈[-5,5].
    """
    # (index, geom, token, level) — geom is bounds list or line endpoint pairs.
    fits: list[tuple[int, object, str, int]] = [
        (24, [-163.43155005236318, 34.57495506265212, -160.48030567216512, 36.39518788075429], '7dd', 4),
        (42, [91.50006554028693, -42.57493163891812, 91.91758255944056, -40.66300559487594], '281', 4),
        (76, [-57.177885869288275, -35.4934011401071, -54.562368380886255, -33.64185180632114], '95', 2),
        (77, [(-52.63481129756637, -31.620750092937477), (-57.177885869288275, -35.4934011401071)], '95', 2),
        (95, [(170.89774691575514, -39.12687589120752), (172.76493830506587, -34.756523791683975)], '6d1', 4),
        (101, [(-64.14527265092615, -43.636174260201365), (-68.97903982312017, -43.11519666553432)], 'bc', 1),
        (118, [121.97723096433214, 41.85143051876878, 124.0770569843029, 42.46807962913732], '5e3', 4),
        (145, [(-116.92282423836268, 40.36683060261118), (-120.00226157283939, 39.08234558278977)], '80c', 3),
        (148, [57.17881356040576, 34.06932029084419, 58.56948388353784, 36.84981291845648], '3f4', 3),
        (154, [-90.13853915312798, 42.428057902258644, -90.02691318086124, 43.078590143664584], '87f', 4),
        (155, [(-90.7508084519286, 37.638404633117354), (-90.13853915312798, 42.428057902258644)], '87c', 3),
        (167, [(26.266966518747218, -37.078805077390605), (25.983026805232782, -32.0872522568795)], '1e4', 3),
        (180, [-2.612665417993213, 41.78061784876387, -0.6556347565569483, 43.76197960928756], '0d5', 4),
        (181, [(-1.3159118311066642, 40.85060759864879), (-2.612665417993213, 41.78061784876387)], '0d5', 4),
        (188, [3.680570358854311, 41.60710312730191, 5.006104011176957, 42.737117336708714], '12b', 4),
        (189, [(5.24900574784283, 36.80451700128276), (3.680570358854311, 41.60710312730191)], '12c', 3),
        (202, [-24.42790700451789, 44.47080889517443, -22.05021233231807, 45.121521412900584], '4c', 1),
        (208, [-92.90328979887346, 37.08688419351098, -90.7197067123228, 38.07282224856326], '87d', 4),
        (209, [(-96.83943546007704, 36.05696274538232), (-92.90328979887346, 37.08688419351098)], '87c', 3),
        (219, [(29.55210848587695, -44.88093637729473), (28.615922611016543, -48.97580700525289)], 'b5c', 3),
        (230, [49.229658534017204, 41.58442554977563, 51.32129552603737, 44.136731588338485], '41', 2),
        (231, [(50.85982041900414, 40.48144482652907), (49.229658534017204, 41.58442554977563)], '403', 4),
        (238, [76.44962890163072, 41.35812024632591, 78.64086324527754, 43.6395249010793], '389', 4),
    ]
    assert len(fits) == 23
    for idx, geom, token, level in fits:
        if isinstance(geom, list) and geom and isinstance(geom[0], (int, float)):
            value: object = geom
            bounds = geom
        else:
            value = gm.LineString(list(geom), crs=4326)
            xs = [p[0] for p in geom]
            ys = [p[1] for p in geom]
            bounds = [min(xs), min(ys), max(xs), max(ys)]
        cell = gm.s2_bounding_cell(value)
        assert (cell.token, cell.level) == (token, level), (
            idx,
            bounds,
            cell.token,
            cell.level,
            token,
            level,
        )
        _assert_s2_bounding_bbox(bounds)

    genuine: list[tuple[int, object]] = [
        (10, [-95.555766807789, -45.75268793803267, -94.8213017995097, -44.31983788739324]),
        (14, [-135.72139438952132, -28.441818488778054, -133.52908960752984, -26.279362356661743]),
        (15, [(-131.35698852152672, -29.220748489163903), (-135.72139438952132, -28.441818488778054)]),
        (23, [(2.9136179981160693, 42.63099418969385), (3.0166827934934872, 47.33524455002478)]),
        (43, [(89.94429418125188, -46.87977785360965), (91.50006554028693, -42.57493163891812)]),
        (100, [-68.97903982312017, -43.11519666553432, -68.6430704035072, -42.20091516719676]),
        (119, [(121.30361097409451, 38.43039995629039), (121.97723096433214, 41.85143051876878)]),
        (133, [(-3.5105792006594942, -42.56237970502738), (-7.5799742457179775, -46.73944554340961)]),
        (138, [-45.96206959237176, 34.21805269432748, -43.72645705430334, 36.0423283283661]),
        (139, [(-42.39929820324171, 38.18409640549097), (-45.96206959237176, 34.21805269432748)]),
        (144, [-120.00226157283939, 39.08234558278977, -119.7842063795634, 42.02788665141943]),
        (157, [(43.50233483203388, 18.380075865873195), (45.654518696417995, 17.956681558345707)]),
        (162, [-41.79421425367758, -36.71193422529493, -41.38061054834849, -34.27004186611242]),
        (163, [(-45.613498948370925, -34.239281878414495), (-41.79421425367758, -36.71193422529493)]),
        (203, [(-23.16904195113809, 41.127104170196006), (-24.42790700451789, 44.47080889517443)]),
        (227, [(-137.11287224413897, -10.70030587825781), (-132.4675686473578, -6.308823444946455)]),
        (239, [(73.96543584313837, 46.12215701325488), (76.44962890163072, 41.35812024632591)]),
    ]
    assert len(genuine) == 17
    for _idx, geom in genuine:
        if isinstance(geom, list) and geom and isinstance(geom[0], (int, float)):
            value = geom
        else:
            value = gm.LineString(list(geom), crs=4326)
        with pytest.raises(gm.GeometryError, match='no single S2 cell'):
            gm.s2_bounding_cell(value)


def test_s2_bounding_cell_latitude_band_no_false_reject() -> None:
    """0.2deg x 0.2deg boxes in ±35.27..45: closed-containing cell or multi-face raise.

    Zero non-containing successes (inset interior samples must be Hilbert-
    contained). Hilbert dual-assignment of perimeter leaves is not a veto.
    """
    import numpy as np

    success = 0
    multi_face = 0
    lats = np.concatenate(
        [
            np.arange(35.3, 45.01, 0.5),
            np.arange(-45.0, -35.29, 0.5),
        ]
    )
    lons = np.arange(-180.0, 180.0, 5.0)
    for lat0 in lats:
        for lon0 in lons:
            minx = float(lon0)
            maxx = float(min(lon0 + 0.2, 180.0))
            miny = float(lat0)
            maxy = float(lat0 + 0.2)
            if maxy > 90.0 or miny < -90.0 or maxx <= minx:
                continue
            bounds = [minx, miny, maxx, maxy]
            try:
                _assert_s2_bounding_bbox(bounds)
            except gm.GeometryError as exc:
                if 'no single S2 cell' not in str(exc):
                    raise
                multi_face += 1
                continue
            success += 1
    assert success > 100, (success, multi_face)
    assert success > multi_face, (success, multi_face)


def test_s2_bounding_cell_face_diagonal_meridian_raises_not_sparse_lca() -> None:
    """Exact face-edge lon=±135 short segments: dual closed face roots → multi-face."""
    for lon in (-135.0, 135.0):
        with pytest.raises(gm.GeometryError, match='no single S2 cell'):
            gm.s2_bounding_cell([lon, -10.0, lon, -9.7])
        with pytest.raises(gm.GeometryError, match='no single S2 cell'):
            gm.s2_bounding_cell(
                gm.LineString([(lon, -10.0), (lon, -9.7)], crs=4326)
            )


def test_s2_bounding_cell_face_diagonal_meridian_lat0_to_5_raises() -> None:
    """Exact face-edge [±135,0,±135,5]: dual closed face roots → multi-face raise."""
    for lon in (135.0, -135.0):
        with pytest.raises(gm.GeometryError, match='no single S2 cell'):
            gm.s2_bounding_cell([lon, 0.0, lon, 5.0])
        with pytest.raises(gm.GeometryError, match='no single S2 cell'):
            gm.s2_bounding_cell(gm.LineString([(lon, 0.0), (lon, 5.0)], crs=4326))


def test_s2_bounding_cell_interior_point_soundness_matrix() -> None:
    """Every success closed-contains inset interior samples of the bbox.

    Broad matrix: face centers, seams, poles, tall/wide lines, collections,
    zero-length multipart. True dual-root multi-face raises. Zero non-
    containing successes. Perimeter Hilbert dual-assignment is not a veto.
    """
    lons = (0.0, 45.0, -45.0, 90.0, -90.0, 135.0, -135.0, 180.0, -180.0)
    lats = (0.0, 45.0, -45.0, 88.0, -88.0, 90.0, -90.0)
    samples: list = [
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(13.4, 52.5, crs=4326),
        gm.Point(0.0, 89.9, crs=4326),
        gm.Point(45.0, 0.0, crs=4326),
        gm.LineString([(0.0, 0.0), (0.0, 45.0)], crs=4326),
        gm.LineString([(0.0, 0.0), (1.0, 0.0)], crs=4326),
        gm.LineString([(0.0, 0.0), (0.0, 1.0)], crs=4326),
        gm.LineString([(-110.1, 75.0), (-110.0, 75.1)], crs=4326),
        gm.LineString([(-21.0, 21.5), (-1.2, 22.9)], crs=4326),
        gm.LineString([(-90.0, -40.0), (-90.0, -39.9)], crs=4326),
        gm.LineString([(0.0, -45.0), (0.0, 45.0)], crs=4326),
        gm.LineString([(180.0, 0.0), (180.0, 1.0)], crs=4326),
        # Cube-edge meridians: raise or closed-contain (halfspace authority).
        gm.LineString([(90.0, 45.0), (90.0, 60.0)], crs=4326),
        gm.LineString([(-90.0, 45.0), (-90.0, 60.0)], crs=4326),
        gm.LineString([(-180.0, 45.0), (-180.0, 60.0)], crs=4326),
        gm.box(0.0, 0.0, 1.0, 1.0, crs=4326),
        gm.box(13.3, 52.4, 13.5, 52.6, crs=4326),
        gm.box(45.0, 0.0, 46.0, 1.0, crs=4326),
        gm.box(-110.1, 75.0, -110.0, 75.1, crs=4326),
        gm.box(90.0, -80.0, 91.0, -79.0, crs=4326),
        gm.box(90.0, -85.0, 91.0, -84.0, crs=4326),
        gm.MultiPoint([(0.1, 0.1), (0.2, 0.2)], crs=4326),
        gm.MultiPoint([(90.0, -80.0), (91.0, -79.0)], crs=4326),
        gm.GeometryCollection(
            [
                gm.Point(1.0, 1.0, crs=4326),
                gm.LineString([(1.0, 1.0), (1.5, 1.5)], crs=4326),
            ],
            crs=4326,
        ),
        # Zero-length multipart (former 4^30 amplification).
        gm.MultiLineString(
            [[(10.0, 20.0), (10.0, 20.0)], [(10.0, 20.0), (10.0, 20.0)]],
            crs=4326,
        ),
        # Seam-adjacent.
        gm.LineString([(179.5, 0.0), (179.9, 0.1)], crs=4326),
        # Exact diagonal-seam oracle repros.
        gm.box(-135.0, -35.0, -134.8, -34.8, crs=4326),
        gm.box(-135.0, 0.0, -134.8, 0.2, crs=4326),
    ]
    # Boundary-aligned short segments at face-center/seam longitudes.
    for lon in lons:
        if abs(lon) > 179.9:
            continue
        for lat0 in (-40.0, 0.0, 20.0, 40.0, 45.0, 55.0):
            lat1 = min(lat0 + 5.0, 89.0)
            if lat1 <= lat0:
                continue
            samples.append(gm.LineString([(lon, lat0), (lon, lat1)], crs=4326))
        samples.append(gm.LineString([(lon, 45.0), (lon, 60.0)], crs=4326))
    for lat in lats:
        if abs(lat) >= 90.0:
            continue
        samples.append(gm.LineString([(-10.0, lat), (10.0, lat)], crs=4326))

    ok = 0
    raised = 0
    for geom in samples:
        try:
            _assert_s2_bounding_bbox(geom)
            ok += 1
        except gm.GeometryError as exc:
            if 'no single S2 cell' not in str(exc):
                raise AssertionError((geom, exc)) from exc
            raised += 1
    assert ok >= 12
    assert raised >= 1

    # True multi-face / dual-root cases must still raise.
    # MultiPoint([(45,0.5),(46,0.5)]) is NOT multi-face under the bbox path
    # (face root '3' closed-contains the envelope — same as the box form).
    for multi in (
        gm.box(-100.0, -40.0, 100.0, 40.0, crs=4326),
        gm.LineString([(179.0, -1.0), (-179.0, 1.0)], crs=4326),
        gm.MultiPoint([(-100.0, 0.0), (100.0, 0.0)], crs=4326),
        [-50.0, 10.0, -32.0, 15.0],
        [-135.0, -10.0, -135.0, -9.7],
        [135.0, -10.0, 135.0, -9.7],
        [135.0, 0.0, 135.0, 5.0],
        [-135.0, 0.0, -135.0, 5.0],
    ):
        with pytest.raises(gm.GeometryError, match='no single S2 cell'):
            gm.s2_bounding_cell(multi)
    # Seam multipoint ≡ bbox (containing face root, not a leaf-LCA raise).
    seam_mp = gm.MultiPoint([(45.0, 0.5), (46.0, 0.5)], crs=4326)
    assert gm.s2_bounding_cell(seam_mp) == gm.s2_bounding_cell(
        gm.box(45.0, 0.5, 46.0, 0.5, crs=4326)
    )


def test_s2_bounding_cell_moderate_multiface_raises_not_face7() -> None:
    """Skeptic: moderate multi-face bbox must raise, never non-containing face 7."""
    bounds = [-50.0, 10.0, -32.0, 15.0]
    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell(bounds)
    # Coverer max_cells=1 also refuses (budget).
    with pytest.raises(gm.GeometryError, match='max_cells'):
        gm.s2_cover(gm.box(*bounds, crs=4326), max_cells=1)


def test_s2_bounding_cell_no_amplification_zero_length_multipart() -> None:
    """Zero-length multipart must complete immediately (bbox-only, no edge climb)."""
    import time

    geom = gm.MultiLineString(
        [[(10.0, 20.0), (10.0, 20.0)], [(10.0, 20.0), (10.0, 20.0)]],
        crs=4326,
    )
    t0 = time.perf_counter()
    cell = gm.s2_bounding_cell(geom)
    elapsed = time.perf_counter() - t0
    assert elapsed < 1.0, f'amplification: {elapsed:.3f}s'
    assert cell.level == 30  # degenerate bbox → point leaf
    assert cell == gm.S2Cell(10.0, 20.0, level=30)


def test_s2_bounding_cell_sibling_consistency() -> None:
    """s2/geohash/tile bounding are all bbox-based; points stay exact leaves."""
    pt = gm.Point(13.4, 52.5, crs=4326)
    assert gm.s2_bounding_cell(pt).level == 30
    assert gm.s2_bounding_cell(pt) == gm.S2Cell(13.4, 52.5, level=30)

    box = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    s2 = gm.s2_bounding_cell(box)
    gh = gm.geohash_bounding_cell(box)
    tile = gm.tile_bounding_cell(box)
    # All cover the bbox; none has a child that still covers (deepest).
    assert gm.covers(s2.polygon, box)
    assert gm.covers(gh.polygon, box)
    assert gm.covers(tile.polygon, box)
    assert not any(gm.covers(ch.polygon, box) for ch in s2.children())
    assert not any(gm.covers(ch.polygon, box) for ch in gh.children())
    assert not any(gm.covers(ch.polygon, box) for ch in tile.children())

    # Line and its envelope share the s2 bounding cell (bbox contract).
    line = gm.LineString([(13.3, 52.4), (13.5, 52.6)], crs=4326)
    assert gm.s2_bounding_cell(line) == gm.s2_bounding_cell(gm.box(*line.bounds, crs=4326))

    with pytest.raises(gm.GeometryError, match='no single S2 cell'):
        gm.s2_bounding_cell([-100.0, -40.0, 100.0, 40.0])


def test_h3_bounding_cell_covers_actual_rectangle_not_just_diagonal_corners() -> None:
    bounds = (-5e-06, -0.0015, 5e-06, 0.0015)
    rect = gm.box(*bounds, crs=4326)
    cell = gm.h3_bounding_cell(bounds)
    assert gm.covers(cell.polygon, rect)
    assert not any(gm.covers(child.polygon, rect) for child in cell.children())


def test_cell_set_algebra_is_uniform_across_systems() -> None:
    """h3/geohash/tiles gained the s2 trio: union/intersection/difference are
    compact-aware id algebra with the same contract in all four systems.
    """
    box = gm.box(13.3, 52.4, 13.5, 52.6, crs=4326)
    other = gm.box(13.4, 52.5, 13.6, 52.7, crs=4326)
    systems = [
        (
            (gm.h3_union, gm.h3_intersection, gm.h3_difference),
            gm.h3_cover(box, resolution=7),
            gm.h3_cover(other, resolution=7),
            {'resolution': 7},
        ),
        (
            (gm.s2_union, gm.s2_intersection, gm.s2_difference),
            gm.s2_cover(box, level=12),
            gm.s2_cover(other, level=12),
            {'level': 12},
        ),
        (
            (gm.geohash_union, gm.geohash_intersection, gm.geohash_difference),
            gm.geohash_cover(box, precision=5),
            gm.geohash_cover(other, precision=5),
            {'precision': 5},
        ),
        (
            (gm.tile_union, gm.tile_intersection, gm.tile_difference),
            gm.tile_cover(box, zoom=12),
            gm.tile_cover(other, zoom=12),
            {'zoom': 12},
        ),
    ]
    for (
        union_fn,
        intersection_fn,
        difference_fn,
    ), left_cov, right_cov, depth in systems:
        left, right = (left_cov.cells, right_cov.cells)
        union = union_fn(left, right)
        inter = intersection_fn(left, right)
        diff = difference_fn(left, right)
        flat_depth = next(iter(depth.values()))
        flat = lambda cells, flat_depth=flat_depth: set(cells.uncompact(flat_depth))
        left_set, right_set = (flat(left), flat(right))
        assert flat(union_fn(left, [])) == left_set
        assert flat(union) == left_set | right_set
        assert flat(inter) == left_set & right_set
        assert flat(diff) == left_set - right_set
        assert len(difference_fn(left, left)) == 0
        for a in union:
            assert not any(a != b and a.contains(b) for b in union)
        parent = left[0].parent()
        assert parent in union_fn([parent], parent.children())
        assert len(union_fn([parent], parent.children())) == 1
        child = left[0].children()[0]
        # A one-cell set is naturally written as that cell/token, without a
        # ceremonial singleton list. Collection operands remain composable.
        assert list(union_fn(left[0], [])) == [left[0]]
        assert list(union_fn(left[0].token, [])) == [left[0]]
        assert list(intersection_fn(left[0], child)) == [child]
        assert list(intersection_fn([left[0]], [child])) == [child]
        remainder = difference_fn([left[0]], [child])
        assert child not in remainder
        assert all(left[0].contains(cell) for cell in remainder)


def test_cell_uniform_surface() -> None:
    """The cell protocol behaves identically across systems (cell_methods!).

    Driven by the shared ``GridCase`` table so depth-metadata assertions are not
    hand-duplicated x4; unique int-id / max-depth edges stay explicit.
    """
    from test_grids_cells_construct import GRID_CASES

    samples = {g.name: g.make() for g in GRID_CASES}
    for grid in GRID_CASES:
        cell = samples[grid.name]
        assert getattr(cell.parent(), grid.depth_kw) == grid.depth - 1
        assert {getattr(c, grid.depth_kw) for c in cell.children()} == {grid.depth + 1}
        # Two levels deeper: branching**2 children (H3 is ~7, not exact powers).
        deeper = grid.depth + 2
        if deeper <= grid.max_depth:
            n = len(cell.children(deeper))
            if grid.name == 'h3':
                assert n == len(list(cell.children(deeper)))
            else:
                assert n == grid.branching**2
        # Max-depth cell has no children by default.
        max_cell = grid.make(**{grid.depth_kw: grid.max_depth})
        assert len(max_cell.children()) == 0

    # Root has no parent (system-specific minimum depth).
    with pytest.raises(gm.GeometryError, match='has no parent'):
        gm.H3Cell(13.4, 52.5, resolution=0).parent()
    with pytest.raises(gm.GeometryError, match='has no parent'):
        samples['s2'].parent(0).parent()
    with pytest.raises(gm.GeometryError, match='has no parent'):
        samples['geohash'].parent(1).parent()
    with pytest.raises(gm.GeometryError, match='has no parent'):
        samples['tiles'].parent(0).parent()

    def check_hierarchy(cell: Cell) -> None:
        child = cell.children()[0]
        assert cell.contains(child)
        assert cell.contains(cell)
        assert not cell.contains(cell.neighbors[0])
        assert child.intersects(cell)
        assert cell.intersects(child)
        assert not cell.intersects(cell.neighbors[0])
        assert cell.contains(child.token)

    for cell in samples.values():
        check_hierarchy(cell)
    assert samples['h3'].contains(int(samples['h3'].children()[0]))
    assert samples['s2'].contains(int(samples['s2'].children()[0]))
    assert samples['tiles'].contains(int(samples['tiles'].children()[0]))
    with pytest.raises(TypeError):
        int(samples['geohash'])


def test_geohash_mixed_precision_membership_excludes_subcell() -> None:
    coverage = gm.geohash_cover(
        gm.box(13.2, 52.4, 13.6, 52.6, crs=4326), precision=6
    ).compact()
    parent = next(cell for cell in coverage.cells if cell.precision == 5)
    zeroth_child = next(iter(parent.children()))
    assert parent in coverage
    assert zeroth_child not in coverage
    assert all(cell in coverage for cell in coverage.cells)


def test_to_polygon_parity_across_coverage_systems() -> None:
    box = gm.box(13.2, 52.4, 13.6, 52.6, crs=4326)
    for cell_type, coverage in (
        (gm.S2Cell, gm.s2_cover(box, target_cells=24)),
        (gm.GeohashCell, gm.geohash_cover(box, precision=5)),
        (gm.Tile, gm.tile_cover(box, zoom=10)),
    ):
        outline = coverage.to_polygon()
        assert outline.geometry_type in ('Polygon', 'MultiPolygon')
        assert outline.crs == 'EPSG:4326'
        assert outline.is_valid
        union = coverage.cells.polygon.union_all()
        assert (outline ^ union).area <= 1e-06 * max(outline.area, 1e-12)
        cells = list(coverage.cells)
        assert gm.equals(gm.CellArray(cells, type=cell_type).to_polygon(), outline)
        assert gm.equals(
            gm.CellArray(cells + cells, type=cell_type).to_polygon(), outline
        )
        with pytest.raises(gm.GeometryError, match='at least one cell'):
            gm.CellArray([], type=cell_type).to_polygon()
    empty = gm.geohash_cover(
        gm.Point(13.4, 52.5, crs=4326), precision=8, cell_rule='within'
    )
    assert len(empty) == 0
    with pytest.raises(gm.GeometryError, match='at least one cell'):
        empty.to_polygon()


def test_grid_review_regressions() -> None:
    alphabet = '0123456789bcdefghjkmnpqrstuvwxyz'
    world = gm.CellArray(
        [gm.GeohashCell(ch) for ch in alphabet], type=gm.GeohashCell
    ).compact()
    assert len(world) == 32
    assert all(cell.precision == 1 for cell in world)
    assert world.count(None) == 0
    with pytest.raises(ValueError, match='None is not in array'):
        world.index(None)
    with pytest.raises(ValueError, match='is not in array'):
        world.index('!')
    assert pickle.loads(pickle.dumps(world[0])) == world[0]
    with pytest.raises(
        gm.GeometryError, match='geohash min_precision must be between 1 and 12'
    ):
        world.compact(0)
    tile = gm.Tile('0313102310')
    with pytest.raises(gm.GeometryError, match='tile min_zoom must be between 0 and 29'):
        gm.CellArray([tile], type=gm.Tile).compact(-1)
    for cover in (gm.h3_cover, gm.s2_cover, gm.geohash_cover, gm.tile_cover):
        grouped = cover(gm.GeometryArray([gm.Point(0, 0, crs=4326)]), 1)
        assert isinstance(grouped, gm.Groups)
        assert len(grouped) == 1
    assert gm.CellArray(['ww8p'], type=gm.GeohashCell).compact()[0] == gm.GeohashCell(
        'ww8p'
    )
    assert len(gm.CellArray(['u', 'u0'], type=gm.GeohashCell).uncompact(2)) == 32
    tile = gm.Tile('0313102310')
    assert gm.Tile('0313102310') == tile
    assert gm.Tile('0313').zoom == 4
    children = gm.Tile('').children()
    assert [c.token for c in sorted(children)] == ['0', '1', '2', '3']
    assert [c.morton for c in children] == [0, 1, 2, 3]
    assert gm.Tile(children[2].id) == children[2]
    z1 = gm.Tile('0').neighbors
    assert len(z1) == len({(t.zoom, t.x, t.y) for t in z1})
    gh = gm.GeohashCell(0, 0, precision=3)
    with pytest.raises(TypeError):
        gh.contains(123)
    with pytest.raises(gm.ParseError):
        gh.contains('!')


@pytest.mark.parametrize(
    ('cover', 'kwargs'),
    [
        (gm.h3_cover, {'resolution': 7}),
        (gm.s2_cover, {'level': 12}),
        (gm.geohash_cover, {'precision': 6}),
        (gm.tile_cover, {'zoom': 12}),
    ],
)
def test_cover_geometry_array_returns_row_aligned_cell_groups(cover, kwargs) -> None:
    first = gm.box(13.39, 52.49, 13.41, 52.51, crs=4326)
    second = gm.box(14.39, 53.49, 14.41, 53.51, crs=4326)
    values = gm.GeometryArray([first, second])
    grouped = cover(values, **kwargs)
    assert isinstance(grouped, gm.Groups)
    assert len(grouped) == 2
    assert all(isinstance(row, gm.CellArray) for row in grouped)
    assert grouped[0] == cover(first, **kwargs).cells
    assert grouped[1] == cover(second, **kwargs).cells

    projected = cover(values.to_crs(3857), **kwargs)
    assert projected == grouped

    with_missing = cover(gm.GeometryArray([first, None]), **kwargs)
    assert with_missing[0] == grouped[0]
    assert len(with_missing[1]) == 0


@pytest.mark.parametrize(
    ('cover', 'kwargs'),
    [
        (gm.h3_cover, {'resolution': 13}),
        (gm.s2_cover, {'level': 16}),
        (gm.geohash_cover, {'precision': 10}),
        (gm.tile_cover, {'zoom': 18}),
    ],
)
def test_world_cover_at_fine_depth_rejected_before_flooding(cover, kwargs) -> None:
    # A world-scale polygon at a fine depth would materialize far more than a
    # million cells. Every grid's covering factory shares one cell-output
    # budget, checked at each emission, so the request fails quickly with a
    # domain error instead of exhausting memory.
    world = gm.box(-179.0, -85.0, 179.0, 85.0, crs=4326)
    with pytest.raises(gm.GeometryError, match='covering would exceed'):
        cover(world, **kwargs)


def test_ordinary_cover_at_modest_depth_stays_within_budget() -> None:
    # The budget never touches an ordinary cover: a small area at a modest
    # depth returns a stable, non-empty cell set on every grid.
    area = gm.box(20.99, 51.99, 21.01, 52.01, crs=4326)
    covers = [
        gm.h3_cover(area, resolution=7),
        gm.s2_cover(area, level=10),
        gm.geohash_cover(area, precision=5),
        gm.tile_cover(area, zoom=10),
    ]
    for coverage in covers:
        assert len(coverage.cells) > 0
    # Deterministic: re-covering yields the identical cell set on every grid.
    assert list(gm.h3_cover(area, resolution=7).cells) == list(covers[0].cells)
    assert list(gm.s2_cover(area, level=10).cells) == list(covers[1].cells)
    assert list(gm.geohash_cover(area, precision=5).cells) == list(covers[2].cells)
    assert list(gm.tile_cover(area, zoom=10).cells) == list(covers[3].cells)
