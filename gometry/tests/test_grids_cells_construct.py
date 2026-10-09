"""Callable cell classes: construction, round-trip, and plural builders.

Shared contracts use a ``GridCase`` table (constructor, factory, depth keyword,
max depth, token/id capability, branching factor) so the x4 depth-metadata and
construct duplication collapses into one parametrized surface — system-specific
assertions stay in the H3/S2/geohash/tile oracle modules.
"""

from __future__ import annotations

import pickle
import re
from dataclasses import dataclass

import gometry as gm
import numpy as np
import pytest
from gometry import _lib


@dataclass(frozen=True)
class GridCase:
    """Uniform grid metadata for parametrized construct/hierarchy tests."""

    name: str
    cell_type: type
    factory: object
    depth_kw: str
    depth: int
    max_depth: int
    branching: int
    has_int_id: bool
    sample_lon: float = 13.4
    sample_lat: float = 52.5

    def make(
        self, lon: float | None = None, lat: float | None = None, **depth_override: int
    ):
        kwargs = {self.depth_kw: depth_override.get(self.depth_kw, self.depth)}
        lon = lon if lon is not None else self.sample_lon
        lat = lat if lat is not None else self.sample_lat
        if self.cell_type is gm.Tile:
            return self.cell_type(lon=lon, lat=lat, **kwargs)
        return self.cell_type(
            lon,
            lat,
            **kwargs,
        )

    def depth_of(self, cell: object) -> int:
        return int(getattr(cell, self.depth_kw))


GRID_CASES: tuple[GridCase, ...] = (
    GridCase('h3', gm.H3Cell, gm.h3_cells, 'resolution', 7, 15, 7, True),
    GridCase('s2', gm.S2Cell, gm.s2_cells, 'level', 12, 30, 4, True),
    GridCase(
        'geohash', gm.GeohashCell, gm.geohash_cells, 'precision', 6, 12, 32, False
    ),
    GridCase('tiles', gm.Tile, gm.tile_cells, 'zoom', 12, 29, 4, True),
)


@pytest.mark.parametrize('grid', GRID_CASES, ids=[g.name for g in GRID_CASES])
def test_cell_construct_and_round_trip(grid: GridCase) -> None:
    """Lon/lat construct, token round-trip, optional id round-trip, Point ingest."""
    cell = grid.make()
    assert isinstance(cell, grid.cell_type)
    assert grid.depth_of(cell) == grid.depth
    assert grid.cell_type(cell.token) == cell
    if grid.has_int_id:
        assert grid.cell_type(cell.id) == cell
        assert int(cell) == cell.id
    else:
        with pytest.raises(TypeError):
            int(cell)
    # Same-type re-wrap
    assert grid.cell_type(cell) == cell
    point = gm.Point(grid.sample_lon, grid.sample_lat, crs=4326)
    assert grid.cell_type(point, **{grid.depth_kw: grid.depth}) == cell
    # Projected ingest matches geographic
    projected = point.to_crs(3857)
    assert grid.cell_type(projected, **{grid.depth_kw: grid.depth}) == cell


@pytest.mark.parametrize('grid', GRID_CASES, ids=[g.name for g in GRID_CASES])
def test_cell_construct_canonicalizes_periodic_seam_aliases(grid: GridCase) -> None:
    owner = grid.make(-180.0, 23.0)
    assert grid.make(180.0, 23.0) == owner
    assert grid.make(540.0, 23.0) == owner
    assert grid.make(-540.0, 23.0) == owner
    assert grid.make(np.nextafter(-180.0, 0.0), 23.0) == owner
    east_neighbor = grid.make(np.nextafter(180.0, 0.0), 23.0)
    if grid.name != 'h3':
        assert east_neighbor != owner

    cells = grid.factory(
        [-180.0, 180.0, 540.0, -540.0],
        [23.0, 23.0, 23.0, 23.0],
        **{grid.depth_kw: grid.depth},
    )
    assert cells.token == [owner.token] * 4


@pytest.mark.parametrize('grid', GRID_CASES[:3], ids=[g.name for g in GRID_CASES[:3]])
@pytest.mark.parametrize('latitude', [-90.0, 90.0], ids=['south', 'north'])
def test_cell_construct_canonicalizes_pole_longitude(
    grid: GridCase, latitude: float
) -> None:
    owner = grid.make(-180.0, latitude)
    for longitude in (-179.0, 0.0, 73.0, 180.0, 540.0):
        assert grid.make(longitude, latitude) == owner


def test_geohash_cell_array_uses_tokens_for_input_and_pickle() -> None:
    """Geohash arrays never expose their private packed identity key."""
    import pickle

    cells = gm.CellArray([gm.GeohashCell('u33d')])
    callable_, args = cells.__reduce__()
    assert callable_.__name__ == '_unpickle_cell_array_v1'
    assert args == (['u33d'], 'geohash', None)
    assert pickle.loads(pickle.dumps(cells)).token == ['u33d']
    with pytest.raises(TypeError):
        gm.CellArray([15043922711510253572], type=gm.GeohashCell)
    packed = type('Packed', (), {'id': 15043922711510253572})()
    with pytest.raises(TypeError):
        gm.CellArray([packed], type=gm.GeohashCell)
    with pytest.raises(TypeError, match='constructed from tokens'):
        gm.CellArray(
            np.array([15043922711510253572], dtype=np.uint64), type=gm.GeohashCell
        )
    with pytest.raises(TypeError, match='constructed from tokens'):
        gm.CellArray(
            memoryview(np.array([15043922711510253572], dtype=np.uint64)),
            type=gm.GeohashCell,
        )
    with pytest.raises(TypeError, match='constructed from tokens'):
        gm.CellArray([15043922711510253572], type=gm.GeohashCell)
    with pytest.raises(TypeError, match=r'integers.*not floats'):
        gm.CellArray(np.array([1.0], dtype=np.float64), type=gm.H3Cell)


@pytest.mark.parametrize(
    ('cell_type', 'roots', 'depth', 'expected_count'),
    [
        (
            gm.S2Cell,
            lambda: [gm.S2Cell(-120, 30, level=5), gm.S2Cell(120, 30, level=5)],
            7,
            32,
        ),
        (gm.Tile, lambda: [gm.Tile('00000'), gm.Tile('11111')], 7, 32),
        (gm.GeohashCell, lambda: [gm.GeohashCell('0'), gm.GeohashCell('z')], 3, 2048),
    ],
    ids=('s2', 'tile', 'geohash'),
)
def test_public_uncompact_preserves_exact_range_order(
    cell_type: type, roots: object, depth: int, expected_count: int
) -> None:
    canonical_roots = roots()
    first = canonical_roots[0]
    second = canonical_roots[1]
    overlapping = first.children()[0]
    unsorted = gm.CellArray(
        [second, first, first, overlapping, second],
        type=cell_type,
    )
    canonical = gm.CellArray(canonical_roots, type=cell_type)

    normalized_output = unsorted.uncompact(depth)
    canonical_output = canonical.uncompact(depth)

    assert len(normalized_output) == expected_count
    assert normalized_output.token == canonical_output.token


@pytest.mark.parametrize('grid', GRID_CASES, ids=[g.name for g in GRID_CASES])
def test_cell_array_rejects_ambiguous_bare_token_text(grid: GridCase) -> None:
    cell = grid.make()
    with pytest.raises(
        TypeError,
        match=rf'^bare token text is ambiguous; use \["{cell.token}"\] for one cell or an explicit iterable for many$',
    ):
        gm.CellArray(cell.token, type=grid.cell_type)
    assert gm.CellArray([cell.token], type=grid.cell_type).token == [cell.token]

    # The empty quadkey is a valid atomic Tile root, not an empty collection.
    assert gm.Tile('').token == ''
    assert gm.CellArray([''], type=gm.Tile).token == ['']


def test_h3_cell_match_args_destructure() -> None:
    """H3-specific ``__match_args__`` destructure (kept out of the uniform table)."""
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    match cell:
        case gm.H3Cell(cell_id):
            assert cell_id == int(cell)
        case _:
            pytest.fail('H3Cell did not destructure')


def test_tile_construct_keywords_disambiguate_coordinate_frames() -> None:
    """Tile coordinates always name their geographic or XYZ frame."""
    lonlat = gm.Tile(lon=13.4, lat=52.5, zoom=12)
    xyz = gm.Tile(x=105, y=201, zoom=9)
    assert lonlat.zoom == 12
    assert gm.Tile(gm.Point(13.4, 52.5, crs=4326), zoom=12) == lonlat
    assert xyz == gm.Tile(xyz.id)
    assert xyz == gm.Tile(xyz.token)
    assert gm.Tile(lon=13, lat=52, zoom=12) == gm.Tile(lon=13.0, lat=52.0, zoom=12)
    with pytest.raises(TypeError, match=r'lon=.*, lat=.*, zoom=.*x=.*, y=.*, zoom='):
        gm.Tile(1, 2, zoom=3)
    with pytest.raises(TypeError, match='x=, y=, and zoom='):
        gm.Tile(x=105, y=201)
    with pytest.raises(TypeError, match='not both'):
        gm.Tile(13.4, lon=13.4, lat=52.5, zoom=12, x=105, y=201)


def test_tile_boundary_owner_is_shared_by_scalar_and_array_construction() -> None:
    cases = [
        (4, -180.0, 79.171_334_640_819_44, 0, 2),
        (4, -180.0, 79.171_334_640_819_45, 0, 2),
        (4, -180.0, 79.171_334_640_819_47, 0, 1),
        (4, -180.0, -74.019_543_311_502_28, 0, 13),
        (29, -180.0, 83.731_647_374_545_96, 0, 20_227_569),
        (29, -180.0, 83.731_647_374_545_97, 0, 20_227_569),
        (29, -180.0, 83.731_647_374_545_99, 0, 20_227_568),
        (29, -180.0, -80.579_680_698_441_98, 0, 481_729_089),
        (1, np.nextafter(0.0, -np.inf), 1.0, 0, 0),
        (1, 0.0, 1.0, 1, 0),
    ]
    for zoom, lon, lat, expected_x, expected_y in cases:
        scalar = gm.Tile(lon=lon, lat=lat, zoom=zoom)
        assert (scalar.x, scalar.y) == (expected_x, expected_y)
        assert gm.Tile(gm.Point(lon, lat, crs=4326), zoom=zoom) == scalar

    cells = gm.tile_cells(
        [case[1] for case in cases],
        [case[2] for case in cases],
        zoom=[case[0] for case in cases],
    )
    assert [(cell.zoom, cell.x, cell.y) for cell in cells] == [
        (zoom, expected_x, expected_y) for zoom, _, _, expected_x, expected_y in cases
    ]


def test_world_tile_polygon_uses_geographic_rectangle_for_scalar_and_array() -> None:
    world = gm.Tile(x=0, y=0, zoom=0)
    expected = (-180.0, -85.0511287798066, 180.0, 85.0511287798066)
    polygon = world.polygon
    assert polygon.bounds == expected
    assert polygon.to_wkt() == gm.box(*expected, crs=4326).to_wkt()
    assert len(polygon.exterior.coords) == 9
    assert gm.contains(polygon, gm.Point(0.0, 0.0, crs=4326))

    polygons = gm.CellArray([world], type=gm.Tile).polygon
    np.testing.assert_array_equal(polygons.bounds, [expected])
    assert polygons.total_bounds == expected
    assert gm.contains(polygons[0], gm.Point(0.0, 0.0, crs=4326))

    hemisphere = gm.Tile(x=1, y=1, zoom=1).polygon
    minx, miny, maxx, maxy = hemisphere.bounds
    assert (minx, maxx) == (0.0, -180.0)
    midpoint = 90.0
    assert np.asarray(hemisphere.coords, dtype=np.float64).tolist() == [
        [minx, miny],
        [midpoint, miny],
        [-180.0, miny],
        [-180.0, maxy],
        [midpoint, maxy],
        [minx, maxy],
        [minx, miny],
    ]

    ordinary = gm.Tile(x=1, y=1, zoom=2).polygon
    minx, miny, maxx, maxy = ordinary.bounds
    four_corner_polygon = gm.Polygon(
        [(minx, miny), (maxx, miny), (maxx, maxy), (minx, maxy), (minx, miny)],
        crs=4326,
    )
    assert ordinary.to_wkt() == four_corner_polygon.to_wkt()


def test_world_tile_dissolve_keeps_the_complete_band() -> None:
    world = gm.Tile(x=0, y=0, zoom=0)
    for cells in ([world], list(world.children(1))):
        outline = gm.CellArray(cells, type=gm.Tile).to_polygon()
        assert outline.geometry_type == 'Polygon'
        assert outline.is_valid
        assert outline.bounds == (-180.0, -85.0511287798066, 180.0, 85.0511287798066)
        for longitude in (-179.0, 0.0, 179.0):
            assert gm.contains(outline, gm.Point(longitude, 0.0, crs=4326))


@pytest.mark.parametrize('zoom', [0, 4, 29])
def test_tile_outer_latitude_edges_keep_admission_contract(zoom: int) -> None:
    cells = 1 << zoom
    assert gm.Tile(lon=0.0, lat=85.051_128_779_806_6, zoom=zoom).y == 0
    assert gm.Tile(lon=0.0, lat=-85.051_128_779_806_6, zoom=zoom).y == cells - 1
    for lat in (-90.0, 90.0, -86.0, 86.0):
        with pytest.raises(gm.GeometryError):
            gm.Tile(lon=0.0, lat=lat, zoom=zoom)
        with pytest.raises(gm.GeometryError):
            gm.tile_cells([0.0], [lat], zoom=zoom)


@pytest.mark.parametrize('grid', GRID_CASES, ids=[g.name for g in GRID_CASES])
def test_children_count_parity_across_grids(grid: GridCase) -> None:
    """`children_count` is uniform: equals materialized children, default = +1,
    rejects a coarser target.
    """
    # Parent one step coarser than the sample depth when possible.
    parent_depth = max(grid.depth - 2, 0 if grid.name != 'geohash' else 1)
    if grid.name == 'geohash' and parent_depth < 1:
        parent_depth = 1
    cell = grid.make(**{grid.depth_kw: parent_depth})
    finer = parent_depth + 1
    min_depth = 1 if grid.name == 'geohash' else 0
    coarser = parent_depth - 1
    assert cell.children_count(finer) == len(cell.children(finer))
    assert cell.children_count() == len(cell.children())
    if coarser >= min_depth:
        with pytest.raises(ValueError, match='must be >='):
            cell.children_count(coarser)


def test_children_count_closed_form_and_edges() -> None:
    """Closed-form counts match the grid branching factor, and a maximum-depth
    cell reports zero descendants by default.
    """
    # S2 and tiles fan out 4x/level, geohash 32x/level, H3 ~7x/level.
    assert gm.S2Cell(0.0, 0.0, level=5).children_count(8) == 4**3
    assert gm.Tile(x=1, y=1, zoom=5).children_count(8) == 4**3
    assert gm.GeohashCell('u33d').children_count(6) == 32**2
    assert gm.H3Cell(21.0, 52.0, resolution=5).children_count(5) == 1
    assert gm.S2Cell(0.0, 0.0, level=30).children_count() == 0


@pytest.mark.parametrize(
    'cell',
    [
        gm.GeohashCell('zzzzzzzzzzzz'),
        gm.Tile(x=536_870_911, y=536_870_911, zoom=29),
    ],
    ids=['geohash', 'tile'],
)
def test_max_depth_rect_cells_have_no_default_children(cell) -> None:
    assert len(cell.children()) == 0
    assert cell.children_count() == 0


@pytest.mark.parametrize(
    ('cells', 'depth', 'child_n'),
    [
        (
            gm.CellArray(
                [
                    gm.H3Cell(0.0, 0.0, resolution=5),
                    gm.H3Cell(10.0, 10.0, resolution=5),
                ],
                type=gm.H3Cell,
            ),
            7,
            49,
        ),
        (gm.CellArray([gm.S2Cell(0.0, 0.0, level=5)], type=gm.S2Cell), 7, 16),
        (gm.CellArray([gm.GeohashCell('u33d')], type=gm.GeohashCell), 5, 32),
        (gm.CellArray([gm.Tile(x=4, y=5, zoom=5)], type=gm.Tile), 7, 16),
    ],
)
def test_cell_array_neighbors_children_groups(cells, depth: int, child_n: int) -> None:
    """`CellArray.neighbors`/`children` return a ragged `Groups` of `CellArray`
    rows — one row per input cell, aligned to the scalar cell surface.
    """
    nb = cells.neighbors
    assert isinstance(nb, gm.Groups) and len(nb) == len(cells)
    # each row is a CellArray equal to that scalar cell's own neighbors
    for i in range(len(cells)):
        assert isinstance(nb[i], gm.CellArray)
        assert set(map(str, nb[i])) == set(map(str, cells[i].neighbors))
    # children: one row per cell, matching the scalar child count
    ch = cells.children(depth)
    assert isinstance(ch, gm.Groups) and len(ch) == len(cells)
    assert len(ch[0]) == cells[0].children_count(depth) == child_n
    # default depth = one finer
    assert len(cells.children()[0]) == cells[0].children_count()
    # nested to_list yields cell objects
    listed = ch.to_list()
    assert isinstance(listed[0], list) and type(listed[0][0]) is type(cells[0])
    # pickle round-trips, equality holds, slicing narrows
    import pickle

    assert pickle.loads(pickle.dumps(nb)) == nb
    assert len(nb[0:1]) == 1


def test_empty_cell_array_neighbors_children_are_empty_groups() -> None:
    cells = gm.CellArray([], type=gm.H3Cell)
    neighbors = cells.neighbors
    children = cells.children()
    assert isinstance(neighbors, gm.Groups) and len(neighbors) == 0
    assert isinstance(children, gm.Groups) and len(children) == 0


def test_cell_array_outputs_preserve_nullable_reversed_duplicate_gathers() -> None:
    source = gm.CellArray(
        [
            gm.H3Cell(37.7749, -122.4194, resolution=7),
            None,
            gm.H3Cell(37.7749, -122.4194, resolution=8),
            gm.H3Cell(37.7749, -122.4194, resolution=9),
        ],
        type=gm.H3Cell,
    )
    cells = source[::-1][[0, 1, 2, 3, 1]]
    expected = list(cells)

    centers = cells.center
    assert centers.crs == 'OGC:CRS84'
    assert not centers.coords.x.flags.writeable
    assert not centers.coords.y.flags.writeable
    assert centers.to_wkt() == [
        None if cell is None else cell.center.to_wkt() for cell in expected
    ]
    polygons = cells.polygon
    assert polygons.crs == 'OGC:CRS84'
    assert polygons.to_wkt() == [
        None if cell is None else cell.polygon.to_wkt() for cell in expected
    ]

    parents = cells.parent()
    assert list(parents) == [
        None if cell is None else cell.parent() for cell in expected
    ]
    target = 10
    counts = cells.children_count(target)
    expected_counts = np.array([
        np.nan if cell is None else cell.children_count(target) for cell in expected
    ])
    np.testing.assert_array_equal(counts, expected_counts)


def test_cell_array_parent_and_count_parse_depth_once_for_present_rows() -> None:
    class CountingIndex:
        def __init__(self, value: int) -> None:
            self.value = value
            self.calls = 0

        def __index__(self) -> int:
            self.calls += 1
            return self.value

    cells = gm.CellArray(
        [
            gm.H3Cell(37.7749, -122.4194, resolution=7),
            None,
            gm.H3Cell(37.7749, -122.4194, resolution=8),
        ],
        type=gm.H3Cell,
    )[::-1]
    target = CountingIndex(6)
    assert list(cells.parent(target)) == [
        cells[0].parent(target.value),
        None,
        cells[2].parent(target.value),
    ]
    assert target.calls == 1

    target = CountingIndex(10)
    counts = cells.children_count(target)
    assert target.calls == 1
    np.testing.assert_array_equal(counts, [49.0, np.nan, 343.0])

    target = CountingIndex(6)
    empty = gm.CellArray([], type=gm.H3Cell)
    missing = gm.CellArray([None, None], type=gm.H3Cell)
    assert len(empty.parent(target)) == 0
    assert np.isnan(empty.children_count(target)).size == 0
    assert list(missing.parent(target)) == [None, None]
    assert np.isnan(missing.children_count(target)).all()
    assert target.calls == 0


def test_cell_array_parent_and_count_keep_root_and_max_depth_errors() -> None:
    root = gm.CellArray([gm.H3Cell(37.7749, -122.4194, resolution=0)], type=gm.H3Cell)
    with pytest.raises(ValueError, match='minimum-resolution cell has no parent'):
        root.parent()

    maximum = gm.CellArray(
        [gm.H3Cell(37.7749, -122.4194, resolution=15)], type=gm.H3Cell
    )
    np.testing.assert_array_equal(maximum.children_count(), [0.0])
    with pytest.raises(ValueError, match='must be >='):
        maximum.children_count(14)


def test_max_depth_cell_array_children_are_empty_rows() -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=15)
    groups = gm.CellArray([cell]).children()
    assert len(groups) == 1
    assert len(groups[0]) == 0
    assert len(cell.children()) == 0


def test_cell_array_children_mixed_depth_matches_scalar_when_target_covers_all() -> (
    None
):
    coarse = gm.H3Cell(13.4, 52.5, resolution=5)
    fine = gm.H3Cell(13.4, 52.5, resolution=6)
    cells = gm.CellArray([coarse, fine])
    target = 7
    groups = cells.children(target)
    assert [len(row) for row in groups] == [
        len(coarse.children(target)),
        len(fine.children(target)),
    ]
    assert list(groups[0]) == list(coarse.children(target))
    assert list(groups[1]) == list(fine.children(target))


@pytest.mark.parametrize('reverse', [False, True])
def test_cell_array_children_rejects_target_below_any_present_depth(reverse) -> None:
    cells = [gm.H3Cell(13.4, 52.5, resolution=5), gm.H3Cell(13.4, 52.5, resolution=7)]
    if reverse:
        cells.reverse()
    array = _lib._unpickle_cell_array_v1(
        [cell.token for cell in cells], 'h3', bytes([1, 0, 0])
    )
    with pytest.raises(
        gm.GeometryError, match='children resolution must be >= cell resolution'
    ):
        array.children(5)


def test_h3_edge_vertex_arrays_do_not_expose_cell_hierarchy() -> None:
    """Edge/vertex arrays have no cell hierarchy, so those attributes do not exist."""
    cell = gm.H3Cell(13.4, 52.5, resolution=6)
    vertices = cell.vertices
    edges = cell.edges
    assert isinstance(vertices, gm.H3VertexArray)
    assert isinstance(edges, gm.H3EdgeArray)
    for values in (vertices, edges):
        assert not isinstance(values, gm.CellArray)
        for name in (
            'area',
            'polygon',
            'parent',
            'children',
            'compact',
            'uncompact',
            'to_polygon',
        ):
            assert not hasattr(values, name)


def test_scalar_cell_factories_removed() -> None:
    for prefix in ('h3', 's2', 'geohash', 'tile'):
        assert not hasattr(gm, f'{prefix}_cell')
        assert callable(getattr(gm, f'{prefix}_cells'))


def test_cells_plural_rejects_all_scalar() -> None:
    with pytest.raises(gm.GeometryError, match='use H3Cell'):
        gm.h3_cells(13.4, 52.5, resolution=7)
    with pytest.raises(gm.GeometryError, match='use S2Cell'):
        gm.s2_cells(13.4, 52.5, level=12)
    with pytest.raises(gm.GeometryError, match='use GeohashCell'):
        gm.geohash_cells(13.4, 52.5, precision=6)
    with pytest.raises(gm.GeometryError, match='use Tile'):
        gm.tile_cells(13.4, 52.5, zoom=12)


def test_cells_columnar_and_broadcast() -> None:
    h3_cells = gm.h3_cells([13.4, 14.4], [52.5, 53.5], resolution=7)
    assert isinstance(h3_cells, gm.CellArray)
    assert len(h3_cells) == 2
    assert list(h3_cells) == [
        gm.H3Cell(13.4, 52.5, resolution=7),
        gm.H3Cell(14.4, 53.5, resolution=7),
    ]
    assert list(gm.h3_cells(13.4, [52.5, 53.5], resolution=7)) == [
        gm.H3Cell(13.4, 52.5, resolution=7),
        gm.H3Cell(13.4, 53.5, resolution=7),
    ]

    s2_cells = gm.s2_cells([13.4, 14.4], [52.5, 53.5], level=12)
    assert isinstance(s2_cells, gm.CellArray)
    assert len(s2_cells) == 2
    assert list(gm.s2_cells(13.4, [52.5, 53.5], level=12)) == [
        gm.S2Cell(13.4, 52.5, level=12),
        gm.S2Cell(13.4, 53.5, level=12),
    ]

    geohash_cells = gm.geohash_cells([13.4, 14.4], [52.5, 53.5], precision=6)
    assert isinstance(geohash_cells, gm.CellArray)
    assert len(geohash_cells) == 2
    assert list(gm.geohash_cells(13.4, [52.5, 53.5], precision=6)) == [
        gm.GeohashCell(13.4, 52.5, precision=6),
        gm.GeohashCell(13.4, 53.5, precision=6),
    ]

    tiles = gm.tile_cells([13.4, 14.4], [52.5, 53.5], zoom=12)
    assert isinstance(tiles, gm.CellArray)
    assert len(tiles) == 2
    assert list(gm.tile_cells(13.4, [52.5, 53.5], zoom=12)) == [
        gm.Tile(lon=13.4, lat=52.5, zoom=12),
        gm.Tile(lon=13.4, lat=53.5, zoom=12),
    ]


@pytest.mark.parametrize('dtype', [np.int64, np.uint64], ids=['signed', 'unsigned'])
@pytest.mark.parametrize('grid', GRID_CASES, ids=[grid.name for grid in GRID_CASES])
def test_cells_accept_numpy_scalar_and_zero_d_depths(grid: GridCase, dtype) -> None:
    for depth in (dtype(grid.depth), np.array(grid.depth, dtype=dtype)):
        cells = grid.factory(
            [grid.sample_lon, grid.sample_lon + 1.0],
            [grid.sample_lat, grid.sample_lat + 1.0],
            **{grid.depth_kw: depth},
        )
        assert [grid.depth_of(cell) for cell in cells] == [grid.depth, grid.depth]


@pytest.mark.parametrize('dtype', [np.int64, np.uint64], ids=['signed', 'unsigned'])
@pytest.mark.parametrize('grid', GRID_CASES, ids=[grid.name for grid in GRID_CASES])
def test_cells_accept_rank_one_numpy_depths_per_row(grid: GridCase, dtype) -> None:
    """Grid depth arrays are parsed per row for every cell factory.

    Mutation: removing the rank classification makes this positive rank-one
    case fail while NumPy is probed as a scalar index.
    """
    depths = np.array([grid.depth, grid.depth + 1], dtype=dtype)
    cells = grid.factory(
        [grid.sample_lon, grid.sample_lon + 1.0],
        [grid.sample_lat, grid.sample_lat + 1.0],
        **{grid.depth_kw: depths},
    )
    assert [grid.depth_of(cell) for cell in cells] == [grid.depth, grid.depth + 1]


def test_geohash_bulk_tokens_keep_precision_alphabet_and_missing_rows() -> None:
    lon = [-120.6623, 112.5584]
    lat = [35.3003, 37.8324]
    expected = {
        1: ['9', 'w'],
        6: ['9q60y6', 'ww8p1r'],
        12: ['9q60y60rhsgg', 'ww8p1r4t8yd0'],
    }
    for precision, tokens in expected.items():
        assert gm.geohash_cells(lon, lat, precision=precision).token == tokens

    masked = gm.GeometryArray([
        gm.Point(-120.6623, 35.3003),
        None,
        gm.Point(112.5584, 37.8324),
    ])
    assert gm.geohash_cells(masked, precision=6).token == [
        '9q60y6',
        None,
        'ww8p1r',
    ]


@pytest.mark.parametrize('grid', GRID_CASES, ids=[g.name for g in GRID_CASES])
def test_cells_accept_packed_point_arrays(grid: GridCase) -> None:
    points = gm.points([13.4, 14.4], [52.5, 53.5], crs=4326)
    kwargs = {grid.depth_kw: grid.depth}
    expected = grid.factory([13.4, 14.4], [52.5, 53.5], **kwargs)
    assert grid.factory(points, **kwargs) == expected
    assert grid.factory(points.to_crs(3857), **kwargs) == expected
    selected = points[[1, 0, 1]]._with_missing([False, True, False])
    for source in [selected, selected.to_crs(3857)]:
        assert list(grid.factory(source, **kwargs)) == [expected[1], None, expected[1]]


@pytest.mark.parametrize('grid', GRID_CASES, ids=[g.name for g in GRID_CASES])
def test_cell_admission_error_keeps_logical_row_after_missing(grid: GridCase) -> None:
    points = gm.points([0, 0], [0, 91], crs=4326)._with_missing([True, False])
    with pytest.raises(
        gm.InvalidGeometryError, match='invalid longitude/latitude'
    ) as error:
        grid.factory(points, **{grid.depth_kw: grid.depth})
    assert error.value.__notes__ == ['while processing array element 1']


@pytest.mark.parametrize('grid', GRID_CASES, ids=[grid.name for grid in GRID_CASES])
def test_cell_array_admits_later_gathered_invalid_coordinate_before_malformed_depth(
    grid: GridCase,
) -> None:
    source = gm.points([13.4, 14.4, 15.4], [52.5, 91.0, 54.5])
    points = source[[1, 0, 1]]._with_missing([True, False, False])
    with pytest.raises(
        gm.InvalidGeometryError,
        match='invalid longitude/latitude',
    ) as error:
        grid.factory(points, **{grid.depth_kw: 'not-an-integer'})
    assert error.value.__notes__ == ['while processing array element 2']


@pytest.mark.parametrize('grid', GRID_CASES, ids=[g.name for g in GRID_CASES])
def test_cell_array_hierarchical_predicate_broadcast(grid: GridCase) -> None:
    parent_depth = 5 if grid.name != 'geohash' else 4
    child_depth = parent_depth + 2
    parent = grid.make(**{grid.depth_kw: parent_depth})
    child = grid.make(**{grid.depth_kw: child_depth})
    values = gm.CellArray([parent, child], type=grid.cell_type)
    paired = gm.CellArray([child, parent], type=grid.cell_type)
    np.testing.assert_array_equal(values.contains(child), [True, True])
    np.testing.assert_array_equal(values.contains(paired), [True, False])
    np.testing.assert_array_equal(values.intersects(parent), [True, True])
    np.testing.assert_array_equal(values.intersects(paired), [True, True])
    assert not values.contains(child).flags.writeable
    with pytest.raises(gm.GeometryError, match='equal lengths'):
        values.contains(paired[:1])


@pytest.mark.parametrize('grid', GRID_CASES, ids=[g.name for g in GRID_CASES])
def test_cell_array_hierarchical_predicates_reject_other_grids(grid: GridCase) -> None:
    grid_index = GRID_CASES.index(grid)
    other = GRID_CASES[(grid_index + 1) % len(GRID_CASES)]
    values = gm.CellArray([grid.make()], type=grid.cell_type)
    other_cell = other.make()
    other_values = gm.CellArray([other_cell], type=other.cell_type)
    match = (
        rf'\Acell types must match, got '
        rf'{re.escape(grid.cell_type.__name__)} and '
        rf'{re.escape(other.cell_type.__name__)}\Z'
    )
    for operand in (other_cell, other_values):
        for predicate in (values.contains, values.intersects):
            with pytest.raises(gm.GeometryError, match=match):
                predicate(operand)

    grid_token = 'tile' if grid.name == 'tiles' else grid.name
    masked = _lib._unpickle_cell_array_v1([], grid_token, b'\x01')
    incompatible_masked = _lib._unpickle_cell_array_v1(
        [],
        'tile' if other.name == 'tiles' else other.name,
        b'\x01',
    )
    for operand in (other_cell, incompatible_masked):
        for predicate in (masked.contains, masked.intersects):
            with pytest.raises(gm.GeometryError, match=match):
                predicate(operand)


@pytest.mark.parametrize('grid', GRID_CASES, ids=[g.name for g in GRID_CASES])
def test_masked_cell_array_row_preserving_operations(grid: GridCase) -> None:
    """Missing rows remain aligned through every bulk row-preserving surface."""
    a = grid.make()
    grid_token = 'tile' if grid.name == 'tiles' else grid.name
    values = _lib._unpickle_cell_array_v1(
        [a.token, a.token], grid_token, bytes([0, 1, 0])
    )
    other = _lib._unpickle_cell_array_v1(
        [a.token, a.token], grid_token, bytes([0, 1, 0])
    )

    assert values.is_missing.tolist() == [False, True, False]
    np.testing.assert_array_equal(values.contains(other), [True, False, True])
    assert values.center.is_missing.tolist() == [False, True, False]
    assert values.polygon.is_missing.tolist() == [False, True, False]
    assert np.isnan(values.area[1])
    assert np.isnan(values.children_count()[1])
    assert values.parent().is_missing.tolist() == [False, True, False]
    assert [len(row) for row in values.neighbors] == [
        len(a.neighbors),
        0,
        len(a.neighbors),
    ]
    assert [len(row) for row in values.children()] == [
        len(a.children()),
        0,
        len(a.children()),
    ]
    assert values.token == [a.token, None, a.token]
    codes, unique = values.factorize()
    np.testing.assert_array_equal(codes, [0, -1, 0])
    assert list(unique) == [a]
    counted, counts = values.value_counts()
    assert list(counted) == [a]
    np.testing.assert_array_equal(counts, [2])


def test_masked_cell_array_views_keep_mask_and_pickle_polarity() -> None:
    import pickle

    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    values = _lib._unpickle_cell_array_v1(
        [cell.token, cell.token], 'h3', bytes([0, 1, 0, 1])
    )
    for view, expected_mask in (
        (values[1:], [True, False, True]),
        (values[::-1], [True, False, True, False]),
        (values[[3, 0, 2]], [True, False, False]),
    ):
        assert view.is_missing.tolist() == expected_mask
        assert pickle.loads(pickle.dumps(view)) == view


def test_cell_array_value_counts_and_factorize() -> None:
    a = gm.H3Cell(21.0, 52.0, resolution=7)
    b = gm.H3Cell(22.0, 52.0, resolution=7)
    c = gm.H3Cell(23.0, 52.0, resolution=7)
    cells = gm.CellArray([a, b, a, c, b, b], type=gm.H3Cell)
    codes, uniques = cells.factorize()
    assert isinstance(uniques, gm.CellArray)
    assert list(uniques) == [a, b, c]
    np.testing.assert_array_equal(codes, [0, 1, 0, 2, 1, 1])
    assert not codes.flags.writeable

    counted, counts = cells.value_counts()
    assert isinstance(counted, gm.CellArray)
    assert list(counted) == [b, a, c]
    np.testing.assert_array_equal(counts, [3, 2, 1])
    assert not counts.flags.writeable

    tied = gm.CellArray([c, a, b, a, b, c], type=gm.H3Cell)
    tied_counted, tied_counts = tied.value_counts()
    assert list(tied_counted) == [c, a, b]
    np.testing.assert_array_equal(tied_counts, [2, 2, 2])

    empty = gm.CellArray([], type=gm.H3Cell)
    empty_codes, empty_uniques = empty.factorize()
    empty_counted, empty_counts = empty.value_counts()
    assert isinstance(empty_uniques, gm.CellArray)
    assert isinstance(empty_counted, gm.CellArray)
    assert len(empty_uniques) == len(empty_counted) == 0
    np.testing.assert_array_equal(empty_codes, [])
    np.testing.assert_array_equal(empty_counts, [])


def test_cell_array_infers_only_homogeneous_typed_cells() -> None:
    a = gm.H3Cell(21.0, 52.0, resolution=7)
    b = gm.H3Cell(22.0, 52.0, resolution=7)
    inferred = gm.CellArray(cell for cell in [a, b, a])
    assert inferred.grid == 'h3'
    assert list(inferred) == [a, b, a]
    assert list(gm.CellArray(np.array([a, b], dtype=object))) == [a, b]

    with pytest.raises(TypeError, match='type is required for an empty input'):
        gm.CellArray([])
    with pytest.raises(TypeError, match='type is required for raw'):
        gm.CellArray([a.id])
    with pytest.raises(TypeError, match='type is required for raw'):
        gm.CellArray([a.token])
    with pytest.raises(TypeError, match='one cell type'):
        gm.CellArray([a, gm.S2Cell(21.0, 52.0, level=7)])
    with pytest.raises(TypeError, match='does not match S2Cell value'):
        gm.CellArray([gm.S2Cell(21.0, 52.0, level=7)], type=gm.H3Cell)
    assert list(gm.CellArray([a], type=gm.H3Cell)) == [a]
    with pytest.raises(TypeError, match="unexpected keyword argument 'cell_type'"):
        gm.CellArray([a], cell_type=gm.H3Cell)  # type: ignore[call-arg]


def test_cell_array_infers_nullable_typed_cells_and_preserves_missing_rows() -> None:
    a = gm.H3Cell(21.0, 52.0, resolution=7)
    b = gm.H3Cell(22.0, 52.0, resolution=7)

    for values in ([a, None, b], [None, a, None, b]):
        cells = gm.CellArray(values)
        assert cells.grid == 'h3'
        assert list(cells) == values
        assert cells.is_missing.tolist() == [value is None for value in values]
        parents = cells.parent()
        assert parents.is_missing.tolist() == cells.is_missing.tolist()
        for index, value in enumerate(values):
            if value is None:
                assert cells[index] is None
                assert parents[index] is None
            else:
                assert cells[index] == value
                assert parents[index] == value.parent()

    explicit = gm.CellArray([None, a, None], type=gm.H3Cell)
    assert list(explicit) == [None, a, None]
    assert explicit.is_missing.tolist() == [True, False, True]
    assert list(gm.CellArray([a.id, None], type=gm.H3Cell)) == [a, None]
    assert list(gm.CellArray([None, None], type=gm.H3Cell)) == [None, None]
    assert list(gm.CellArray([a.id, None, a])) == [a, None, a]
    assert list(gm.CellArray([a.token, None, a])) == [a, None, a]

    with pytest.raises(TypeError, match='type is required'):
        gm.CellArray([None, None])


def test_cell_array_object_and_token_ndarrays_fall_through() -> None:
    """D31: object/token ndarrays must not hard-reject as non-uint64 columns."""
    cell = next(iter(gm.h3_cover(gm.box(0, 0, 1, 1, crs=4326), resolution=1)))
    from_list = gm.CellArray([cell])
    from_cells = gm.CellArray(np.array([cell], dtype=object))
    from_tokens = gm.CellArray(np.array([cell.token], dtype=object), type=gm.H3Cell)
    from_token_list = gm.CellArray([cell.token], type=gm.H3Cell)
    assert list(from_cells) == list(from_list) == [cell]
    assert list(from_tokens) == list(from_token_list) == [cell]
    assert from_cells[0].token == cell.token
    assert from_tokens[0].id == cell.id
    # Packed integer columns still take the fast path.
    from_ids = gm.CellArray(np.array([cell.id], dtype=np.uint64), type=gm.H3Cell)
    assert list(from_ids) == [cell]
    # H3Vertex/H3Edge arrays: object ndarrays of typed values fall through too.
    v0 = cell.vertices[0]
    assert list(gm.H3VertexArray(np.array([v0], dtype=object))) == [v0]
    e0 = cell.edges[0]
    assert list(gm.H3EdgeArray(np.array([e0], dtype=object))) == [e0]


def test_cell_array_ndarray_and_memoryview_rank_parity() -> None:
    """m07: 2-D uint64 ndarray and memoryview of it reject identically (0/1-D)."""
    cell = gm.H3Cell(0.0, 0.0, resolution=5)
    ids_2d = np.array([[cell.id, cell.id]], dtype=np.uint64)
    assert ids_2d.ndim == 2
    with pytest.raises(TypeError, match='zero- or one-dimensional'):
        gm.CellArray(ids_2d, type=gm.H3Cell)
    with pytest.raises(TypeError, match='zero- or one-dimensional'):
        gm.CellArray(memoryview(ids_2d), type=gm.H3Cell)
    # Positive: 1-D ndarray and its memoryview both accept.
    ids_1d = ids_2d.reshape(-1)
    from_nd = gm.CellArray(ids_1d, type=gm.H3Cell)
    from_mv = gm.CellArray(memoryview(ids_1d), type=gm.H3Cell)
    assert list(from_nd) == list(from_mv) == [cell, cell]
    # H3Vertex/H3Edge share the same buffer rank rule.
    v0 = cell.vertices[0]
    v2d = np.array([[v0.id]], dtype=np.uint64)
    with pytest.raises(TypeError, match='zero- or one-dimensional'):
        gm.H3VertexArray(v2d)
    with pytest.raises(TypeError, match='zero- or one-dimensional'):
        gm.H3VertexArray(memoryview(v2d))


def test_h3_numeric_array_protocol_honors_endian_dtype_and_copy_contract() -> None:
    cell = gm.H3Cell(13.4, 52.5, resolution=7)
    vertices = cell.vertices
    edges = cell.edges
    arrays = (
        (gm.CellArray([cell], type=gm.H3Cell), [cell.id]),
        (vertices, [value.id for value in vertices]),
        (edges, [value.id for value in edges]),
    )
    byteorder = '>' if np.little_endian else '<'
    requested = np.dtype(f'{byteorder}u8')

    for array, values in arrays:
        expected = np.array(values, dtype=requested)
        for dtype in (None, np.dtype(np.uint64)):
            result = array.__array__(dtype=dtype)
            assert result.dtype == np.dtype(np.uint64)
            assert result.flags.writeable is False
            np.testing.assert_array_equal(result, expected)

        native = array.__array__(dtype=np.dtype(np.uint64), copy=False)
        assert native.flags.writeable is False
        assert np.shares_memory(native, array.to_numpy())

        for copy in (None, True):
            direct = array.__array__(dtype=requested, copy=copy)
            via_numpy = np.asarray(array, dtype=requested)
            for result in (direct, via_numpy):
                assert result.dtype == requested
                assert result.dtype.isnative is False
                assert result.flags.writeable is False
                np.testing.assert_array_equal(result, expected)

        objects = array.__array__(dtype=object)
        assert objects.dtype == np.dtype(object)
        assert objects.flags.writeable is False
        assert objects[0] == array[0]

        with pytest.raises(gm.GeometryError, match='dtype must be uint64'):
            array.__array__(dtype=np.float64)
        with pytest.raises(gm.GeometryError, match='dtype must be uint64'):
            np.asarray(array, dtype=np.float64)
        with pytest.raises(ValueError, match='without copying'):
            array.__array__(dtype=requested, copy=False)
        with pytest.raises(ValueError, match='without copying'):
            np.asarray(array, dtype=requested, copy=False)


def test_cell_collections_reject_byte_payloads_before_numeric_iteration() -> None:
    s2 = gm.S2Cell(13.4, 52.5, level=12)
    h3 = gm.H3Cell(13.4, 52.5, resolution=7)
    vertex = h3.vertices[0]
    edge = h3.edges[0]
    payloads = (
        b'104',
        bytearray(b'104'),
        memoryview(b'104'),
        memoryview(b'104').cast('c'),
    )
    for payload in payloads:
        with pytest.raises(TypeError, match=r'payload\.decode\(\).*list/uint64 array'):
            gm.s2_union(payload, [s2])
        with pytest.raises(TypeError, match=r'payload\.decode\(\).*list/uint64 array'):
            gm.CellArray(payload)
        with pytest.raises(TypeError, match=r'payload\.decode\(\).*list/uint64 array'):
            gm.CellArray(payload, type=gm.S2Cell)
        with pytest.raises(TypeError, match=r'payload\.decode\(\).*list/uint64 array'):
            gm.H3VertexArray(payload)
        with pytest.raises(TypeError, match=r'payload\.decode\(\).*list/uint64 array'):
            gm.H3EdgeArray(payload)
    assert list(gm.CellArray(np.array([s2.id], dtype=np.uint64), type=gm.S2Cell)) == [
        s2
    ]
    assert list(gm.H3VertexArray(np.array([vertex.id], dtype=np.uint64))) == [vertex]
    assert list(gm.H3EdgeArray(np.array([edge.id], dtype=np.uint64))) == [edge]


def test_geohash_numpy_docstrings_describe_its_object_identity() -> None:
    to_numpy_doc = gm.CellArray.to_numpy.__doc__ or ''
    array_doc = gm.CellArray.__array__.__doc__ or ''
    token_doc = gm.CellArray.token.__doc__ or ''
    assert 'Geohash' in to_numpy_doc and 'object array' in to_numpy_doc
    assert 'Geohash' in array_doc and 'dtype=uint64' in array_doc
    assert 'Geohash' in token_doc and 'public string identity' in token_doc


def test_grid_set_algebra_accepts_atomic_identities_and_iterables_consistently() -> (
    None
):
    cases = [
        (
            (gm.h3_union, gm.h3_intersection, gm.h3_difference),
            gm.H3Cell(21.0, 52.0, resolution=5),
            True,
        ),
        (
            (gm.s2_union, gm.s2_intersection, gm.s2_difference),
            gm.S2Cell(21.0, 52.0, level=5),
            True,
        ),
        (
            (gm.geohash_union, gm.geohash_intersection, gm.geohash_difference),
            gm.GeohashCell('u33d'),
            False,
        ),
        (
            (gm.tile_union, gm.tile_intersection, gm.tile_difference),
            gm.Tile(x=4, y=5, zoom=5),
            True,
        ),
    ]
    for (union_fn, intersection_fn, difference_fn), cell, has_numeric_id in cases:
        identities = [cell, cell.token]
        if has_numeric_id:
            identities.append(cell.id)
        for identity in identities:
            assert list(union_fn(identity, identity)) == [cell]
            assert list(intersection_fn(identity, identity)) == [cell]
            assert list(difference_fn(identity, identity)) == []
        assert list(union_fn((x for x in [cell]), [cell])) == [cell]


def test_grid_set_algebra_preserves_real_iterator_errors() -> None:
    class BrokenIterable:
        def __iter__(self):
            raise RuntimeError('broken cell source')

    cell = gm.H3Cell(21.0, 52.0, resolution=5)
    with pytest.raises(RuntimeError, match='broken cell source'):
        gm.h3_union(BrokenIterable(), cell)


@pytest.mark.parametrize(
    'cells',
    [
        [gm.H3Cell(21.0, 52.0, resolution=7), gm.H3Cell(22.0, 52.0, resolution=7)],
        [gm.S2Cell(21.0, 52.0, level=7), gm.S2Cell(22.0, 52.0, level=7)],
        [gm.GeohashCell('u33d'), gm.GeohashCell('u33e')],
        [gm.Tile(x=4, y=5, zoom=5), gm.Tile(x=5, y=5, zoom=5)],
    ],
    ids=['h3', 's2', 'geohash', 'tiles'],
)
def test_cell_array_inference_preserves_all_grid_sequences(
    cells: list[gm.Cell],
) -> None:
    expected = [cells[0], cells[1], cells[0]]
    for values in (
        expected,
        (cell for cell in expected),
        np.asarray(expected, dtype=object),
    ):
        inferred = gm.CellArray(values)
        assert list(inferred) == expected
        assert inferred.grid == gm.CellArray(expected, type=type(cells[0])).grid


@pytest.mark.parametrize('typed', [False, True], ids=['inferred', 'typed'])
def test_cell_array_iterable_reserves_exact_length_once(typed: bool) -> None:
    cell = gm.H3Cell(21.0, 52.0, resolution=7)

    class ExactRows:
        def __init__(self) -> None:
            self.length_calls = 0

        def __iter__(self):
            return iter([cell, cell])

        def __len__(self) -> int:
            self.length_calls += 1
            return 2

    rows = ExactRows()
    result = gm.CellArray(rows, type=gm.H3Cell) if typed else gm.CellArray(rows)

    assert list(result) == [cell, cell]
    assert rows.length_calls == 1


def test_cells_plural_returns_cell_array_round_trip() -> None:
    """Each plural grid constructor returns a typed CellArray that round-trips
    its ids back to the scalar cell objects (the 'plural noun → typed array'
    rule).
    """
    h3 = gm.h3_cells([21.0, 22.0], [52.0, 52.5], resolution=7)
    assert isinstance(h3, gm.CellArray)
    assert h3.grid == 'h3'
    assert h3[0] == gm.H3Cell(21.0, 52.0, resolution=7)
    assert h3[1] == gm.H3Cell(22.0, 52.5, resolution=7)

    s2 = gm.s2_cells([21.0, 22.0], [52.0, 52.5], level=12)
    assert isinstance(s2, gm.CellArray)
    assert s2.grid == 's2'
    assert s2[0] == gm.S2Cell(21.0, 52.0, level=12)
    assert s2[1] == gm.S2Cell(22.0, 52.5, level=12)

    geohash = gm.geohash_cells([21.0, 22.0], [52.0, 52.5], precision=6)
    assert isinstance(geohash, gm.CellArray)
    assert geohash.grid == 'geohash'
    assert geohash[0] == gm.GeohashCell(21.0, 52.0, precision=6)
    assert geohash[1] == gm.GeohashCell(22.0, 52.5, precision=6)

    tiles = gm.tile_cells([21.0, 22.0], [52.0, 52.5], zoom=12)
    assert isinstance(tiles, gm.CellArray)
    assert tiles.grid == 'tile'
    assert tiles[0] == gm.Tile(lon=21.0, lat=52.0, zoom=12)
    assert tiles[1] == gm.Tile(lon=22.0, lat=52.5, zoom=12)


def test_cell_array_iteration_returns_cells_in_row_order() -> None:
    cells = gm.h3_cells([21.0, 22.0], [52.0, 52.5], resolution=7)
    listed = list(cells)
    assert listed == [cells[0], cells[1]]
    assert all(isinstance(cell, gm.H3Cell) for cell in listed)


def test_cell_array_type_requires_native_cell_class_identity() -> None:
    H3Cell = type('H3Cell', (), {})
    with pytest.raises(TypeError, match='native H3Cell'):
        gm.CellArray([], type=H3Cell)


def test_cells_reject_crs_kwarg() -> None:
    with pytest.raises(TypeError, match='crs'):
        gm.H3Cell(0, 0, resolution=5, crs=4326)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match='crs'):
        gm.S2Cell(0, 0, level=5, crs=4326)  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ('builder', 'keyword', 'cls'),
    [
        (gm.h3_cells, 'resolution', 'H3Cell'),
        (gm.s2_cells, 'level', 'S2Cell'),
        (gm.geohash_cells, 'precision', 'GeohashCell'),
        (gm.tile_cells, 'zoom', 'Tile'),
    ],
)
def test_plural_builders_point_a_single_point_to_the_cell_class(
    builder: object, keyword: str, cls: str
) -> None:
    with pytest.raises(
        gm.GeometryError, match=rf'use {cls}\(point, {keyword}=\.\.\.\)'
    ):
        builder(gm.Point(10, 50, crs=4326), **{keyword: 3})  # type: ignore[operator]


def _roundtrip_exact(cov):
    out = pickle.loads(pickle.dumps(cov))
    assert len(out) == len(cov)
    np.testing.assert_array_equal(out.to_numpy(), cov.to_numpy())
    if len(cov) > 0:
        assert out.to_polygon().bounds == cov.to_polygon().bounds
    return out


@pytest.mark.parametrize(
    'label,build',
    [
        (
            'h3',
            lambda: gm.h3_cover(gm.box(-1, -1, 1, 1, crs=4326), 4, cell_rule='center'),
        ),
        (
            's2',
            lambda: gm.s2_cover(
                gm.box(-1, -1, 1, 1, crs=4326),
                level=6,
                max_cells=128,
                cell_rule='center',
            ),
        ),
        (
            'geohash',
            lambda: gm.geohash_cover(
                gm.box(-1, -1, 1, 1, crs=4326), 4, cell_rule='center'
            ),
        ),
        (
            'tile',
            lambda: gm.tile_cover(
                gm.box(-1, -1, 1, 1, crs=4326), 6, cell_rule='center'
            ),
        ),
    ],
)
def test_cell_grid_transforms_pickle_roundtrip(label, build):
    base = build()
    uncompact_depth = {
        'h3': 5,
        's2': 8,
        'geohash': 5,
        'tile': 7,
    }[label]
    for _form, cov in (
        ('base', base),
        ('compact', base.compact()),
        ('uncompact', base.uncompact(uncompact_depth)),
    ):
        _roundtrip_exact(cov)


def test_empty_h3_and_geohash_center_uncompact_pickle():
    """Empty center cover uncompacted to a finer depth round-trips (the P23 repro)."""
    source = gm.box(-0.1, -0.1, 0.1, 0.1, crs=4326)
    # H3: center@4 is empty; uncompact(5) stays empty; must not recompute as center@5.
    cov = gm.h3_cover(source, 4, cell_rule='center').uncompact(5)
    assert len(cov) == 0
    _ = _roundtrip_exact(cov)

    gh = gm.geohash_cover(source, 2, cell_rule='center').uncompact(5)
    assert len(gh) == 0
    _ = _roundtrip_exact(gh)


def test_uncompact_past_million_pickle_roundtrips():
    """Uncompact past one million cells must still pickle-round-trip.

    Explicit expansion is not capped; pickle reconstruction must use the same
    path without introducing a second limit.
    """
    # One world tile at z0 → 4^10 = 1_048_576 cells at z10 (>1M).
    cov = gm.tile_cover(gm.Point(10, 10, crs=4326), zoom=0).uncompact(10)
    assert len(cov) > 1_000_000
    # Must not raise "exceed the uncompact budget" during reconstruction.
    out = pickle.loads(pickle.dumps(cov))
    assert len(out) == len(cov)
    np.testing.assert_array_equal(out.to_numpy(), cov.to_numpy())


def test_cell_set_difference_past_million_cells_is_not_capped():
    """Set algebra accepts a canonical, non-mergeable result over one million."""
    leaves = gm.CellArray([gm.Tile('')], type=gm.Tile).uncompact(11)
    left = leaves[::2]
    result = gm.tile_difference(left, [])

    assert len(result) == len(left) > 1_000_000
    assert result[0] == left[0]
    assert result[-1] == left[-1]


def test_projected_source_roundtrips():
    """Projected sources (D26a false positive) still round-trip."""
    proj = gm.box(0, 0, 1000, 1000, crs=3857)
    for cov in (
        gm.h3_cover(proj, 3).uncompact(4),
        gm.tile_cover(proj, 4),
        gm.s2_cover(proj, level=5, max_cells=64).compact(),
        gm.geohash_cover(proj, 3).compact(),
    ):
        _roundtrip_exact(cov)


def test_h3_center_cover_pickle_roundtrips_empty_and_nonempty():
    """Empty/nonempty parity: empty factory stays empty; nonempty stays nonempty."""
    src = gm.box(-0.1, -0.1, 0.1, 0.1, crs=4326)
    empty = gm.h3_cover(src, 4, cell_rule='center')
    assert len(empty) == 0
    _roundtrip_exact(empty)
    nonempty = gm.h3_cover(src, 5, cell_rule='center')
    assert len(nonempty) > 0
    _roundtrip_exact(nonempty)
