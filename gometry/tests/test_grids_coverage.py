"""Grid cover functions return CellArray or Groups."""

import gometry as gm
import pytest


def test_scalar_covers_return_cell_arrays() -> None:
    polygon = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)
    cases = [
        (gm.h3_cover(polygon, resolution=7), gm.H3Cell),
        (gm.s2_cover(polygon, level=10), gm.S2Cell),
        (gm.geohash_cover(polygon, precision=6), gm.GeohashCell),
        (gm.tile_cover(polygon, zoom=10), gm.Tile),
    ]
    for cells, cell_type in cases:
        assert isinstance(cells, gm.CellArray)
        assert len(cells) > 0
        assert all(isinstance(cell, cell_type) for cell in cells)


def test_array_covers_return_groups() -> None:
    polygons = gm.GeometryArray(
        [
            gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326),
            None,
        ],
        crs=4326,
    )
    cases = [
        gm.h3_cover(polygons, resolution=7),
        gm.s2_cover(polygons, level=10),
        gm.geohash_cover(polygons, precision=6),
        gm.tile_cover(polygons, zoom=10),
    ]
    for groups in cases:
        assert isinstance(groups, gm.Groups)
        assert len(groups) == 2
        assert all(isinstance(row, gm.CellArray) for row in groups)
        assert len(groups[0]) > 0
        assert len(groups[1]) == 0


@pytest.mark.parametrize(
    'cover, depth, bounds',
    [
        (gm.h3_cover, {'resolution': 3}, (-10, -10, 10, 10)),
        (gm.geohash_cover, {'precision': 3}, (-170, -70, 170, 70)),
        (gm.tile_cover, {'zoom': 3}, (-170, -70, 170, 70)),
        (gm.s2_cover, {'level': 3}, (-170, -70, 170, 70)),
    ],
    ids=['h3', 'geohash', 'tile', 's2'],
)
def test_public_grid_order_budget_and_array_parity(cover, depth, bounds) -> None:
    polygon = gm.box(*bounds, crs=4326)
    for rule in ('overlap', 'bbox', 'center', 'within'):
        cells = cover(polygon, cell_rule=rule, max_cells=None, **depth)
        keys = [cell.token if hasattr(cell, 'token') else cell.id for cell in cells]
        assert keys == sorted(set(keys))
        assert keys
        with pytest.raises(gm.GeometryError):
            cover(polygon, cell_rule=rule, max_cells=len(keys) - 1, **depth)

        groups = cover(
            gm.GeometryArray([polygon, polygon, None], crs=4326),
            cell_rule=rule,
            max_cells=None,
            **depth,
        )
        assert len(groups) == 3
        assert list(groups[0]) == list(cells)
        assert list(groups[1]) == list(cells)
        assert len(groups[2]) == 0


def test_cover_cells_use_free_geometry_predicate() -> None:
    source = gm.box(0, 0, 1, 1)
    gm.h3_cover(source, resolution=5)
    assert gm.contains(source, gm.Point(0.5, 0.5))
    assert gm.disjoint(source, gm.Point(2, 2))


@pytest.mark.parametrize('name', ['h3', 's2', 'geohash', 'tile'])
def test_bounding_cells_prove_the_full_region_not_only_vertices(name) -> None:
    first = (-4.0263, 11.4529)
    second = (-13.6235, 6.2313)
    points = gm.MultiPoint([first, second], crs=4326)
    bounds = gm.box(
        min(first[0], second[0]),
        min(first[1], second[1]),
        max(first[0], second[0]),
        max(first[1], second[1]),
        crs=4326,
    )
    factory = getattr(gm, f'{name}_bounding_cell')
    try:
        point_result = factory(points)
    except gm.GeometryError:
        with pytest.raises(gm.GeometryError):
            factory(bounds)
    else:
        assert point_result == factory(bounds)


def test_bounding_cells_use_family_seam_contracts() -> None:
    line = gm.LineString([(179.9, 0.0), (-179.9, 0.0)], crs=4326)
    assert gm.h3_bounding_cell(line) is not None
    assert gm.s2_bounding_cell(line) is not None
    assert gm.tile_bounding_cell(line).zoom == 0
    with pytest.raises(gm.GeometryError, match='antimeridian'):
        gm.geohash_bounding_cell(line)


def test_cell_identity_arguments_match_grid_families() -> None:
    cases = [
        (gm.H3Cell(21.0, 52.0, resolution=7),),
        (gm.S2Cell(21.0, 52.0, level=12),),
        (gm.Tile(lon=21.0, lat=52.0, zoom=10),),
    ]
    for (cell,) in cases:
        assert cell.contains(cell.id)
        assert cell.intersects(cell.id)

    geohash = gm.GeohashCell(21.0, 52.0, precision=6)
    with pytest.raises(TypeError):
        geohash.contains(1)
    with pytest.raises(TypeError):
        geohash.intersects(1)
