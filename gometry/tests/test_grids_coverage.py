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
