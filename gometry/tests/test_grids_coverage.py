"""Grid cover functions return CellArray or Groups."""

import pickle
from collections.abc import Iterable
from typing import Any

import gometry as gm
import numpy as np
import pytest

COVER_CASES = [
    (gm.h3_cover, {'resolution': 2}, 'H3'),
    (gm.s2_cover, {'level': 3}, 'S2'),
    (
        gm.s2_cover,
        {'min_level': 3, 'max_level': 3, 'target_cells': 8},
        'S2',
    ),
    (gm.geohash_cover, {'precision': 3}, 'geohash'),
    (gm.tile_cover, {'zoom': 3}, 'Tile'),
]


def _cell_keys(cells: Iterable[Any]) -> list[Any]:
    return [cell.token if hasattr(cell, 'token') else cell.id for cell in cells]


def test_empty_source_cover_requires_non_empty_geometry() -> None:
    empty = gm.from_wkt('POINT EMPTY', crs=4326)
    with pytest.raises(
        gm.InvalidGeometryError,
        match='H3 coverage requires a non-empty geometry',
    ):
        gm.h3_cover(empty, resolution=5)


@pytest.mark.parametrize(
    'cover, depth, label',
    COVER_CASES,
    ids=['h3', 's2-fixed', 's2-adaptive', 'geohash', 'tile'],
)
def test_scalar_empty_errors_and_array_empty_is_a_group(cover, depth, label) -> None:
    empty = gm.from_wkt('POLYGON EMPTY', crs=4326)
    with pytest.raises(
        gm.InvalidGeometryError,
        match=f'{label} coverage requires a non-empty geometry',
    ):
        cover(empty, **depth)
    groups = cover(gm.GeometryArray([empty], crs=4326), **depth)
    assert len(groups) == 1
    assert len(groups[0]) == 0


@pytest.mark.parametrize(
    'cover, depth',
    [(cover, depth) for cover, depth, _label in COVER_CASES],
    ids=['h3', 's2-fixed', 's2-adaptive', 'geohash', 'tile'],
)
def test_array_first_row_budget_beats_later_invalid_latitude(cover, depth) -> None:
    first = gm.box(-2, -2, 2, 2, crs=4326)
    later = gm.Point(0, 95, crs=4326)
    with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
        cover(gm.GeometryArray([first, later], crs=4326), max_cells=1, **depth)
    assert excinfo.value.param == 'max_cells'
    assert excinfo.value.value == 1


@pytest.mark.parametrize(
    'cover, depth',
    [(cover, depth) for cover, depth, _label in COVER_CASES],
    ids=['h3', 's2-fixed', 's2-adaptive', 'geohash', 'tile'],
)
def test_array_later_row_invalid_latitude_after_successful_row(cover, depth) -> None:
    first = gm.box(-1, -1, 1, 1, crs=4326)
    later = gm.Point(0, 95, crs=4326)
    with pytest.raises(
        gm.InvalidGeometryError, match=r'invalid longitude/latitude \(0, 95\)'
    ):
        cover(gm.GeometryArray([first, later], crs=4326), max_cells=None, **depth)


def test_tile_cover_rejects_latitude_outside_square_web_mercator_grid() -> None:
    point = gm.Point(0, 86, crs=4326)
    with pytest.raises(gm.InvalidGeometryError, match='square Web Mercator tile grid'):
        gm.tile_cover(point, zoom=3)


def test_scalar_covers_return_cell_arrays() -> None:
    polygon = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)
    cases = [
        (gm.h3_cover(polygon, resolution=7), gm.H3Cell),
        (gm.s2_cover(polygon, level=10), gm.S2Cell),
        (
            gm.s2_cover(polygon, min_level=8, max_level=10, target_cells=64),
            gm.S2Cell,
        ),
        (gm.geohash_cover(polygon, precision=6), gm.GeohashCell),
        (gm.tile_cover(polygon, zoom=10), gm.Tile),
    ]
    for cells, cell_type in cases:
        assert isinstance(cells, gm.CellArray)
        assert len(cells) > 0
        assert all(isinstance(cell, cell_type) for cell in cells)


def test_s2_cover_combines_fixed_and_adaptive_refinement() -> None:
    area = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)
    fixed = gm.s2_cover(area, level=10)
    adaptive = gm.s2_cover(area)
    one_level = gm.s2_cover(
        gm.Point(0.0, 0.0, crs=4326),
        target_cells=1,
        min_level=10,
        max_level=10,
    )
    assert isinstance(fixed, gm.CellArray)
    assert isinstance(adaptive, gm.CellArray)
    assert {cell.level for cell in one_level if cell is not None} == {10}
    assert fixed == gm.s2_cover(area, level=10, max_cells=1_000_000)
    assert adaptive == gm.s2_cover(
        area,
        target_cells=8,
        min_level=0,
        max_level=30,
        level_mod=1,
    )

    with pytest.raises(
        gm.GeometryError,
        match='fixed S2 level cannot be combined with adaptive cover controls',
    ):
        gm.s2_cover(area, level=10, min_level=8)


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
        gm.s2_cover(polygons, min_level=8, max_level=10, target_cells=64),
        gm.geohash_cover(polygons, precision=6),
        gm.tile_cover(polygons, zoom=10),
    ]
    for groups in cases:
        assert isinstance(groups, gm.Groups)
        assert len(groups) == 2
        assert isinstance(groups[0], gm.CellArray)
        assert len(groups[0]) > 0
        assert groups[1] is None


@pytest.mark.parametrize(
    ('cover', 'options'),
    [
        pytest.param(gm.h3_cover, {'resolution': 7}, id='h3'),
        pytest.param(gm.s2_cover, {'level': 10}, id='s2-fixed'),
        pytest.param(
            gm.s2_cover,
            {'min_level': 8, 'max_level': 10, 'target_cells': 64},
            id='s2-adaptive',
        ),
        pytest.param(gm.geohash_cover, {'precision': 6}, id='geohash'),
        pytest.param(gm.tile_cover, {'zoom': 10}, id='tiles'),
    ],
)
def test_cover_accepts_geometry_iterables_and_keeps_missing_groups(
    cover, options
) -> None:
    first = gm.box(-122.5, 37.7, -122.3, 37.85, crs=4326)
    second = gm.box(-73.99, 40.72, -73.97, 40.74, crs=4326)
    empty = gm.from_wkt('POLYGON EMPTY', crs=4326)
    values = (geometry for geometry in (first, None, empty, second))
    groups = cover(values, **options)
    assert isinstance(groups, gm.Groups)
    assert len(groups) == 4
    assert list(groups[0]) == list(cover(first, **options))
    assert groups[1] is None
    assert len(groups[2]) == 0
    assert list(groups[3]) == list(cover(second, **options))


def test_interleaved_missing_cover_rows_keep_logical_alignment() -> None:
    first = gm.box(-3, -3, -2, -2, crs=4326)
    second = gm.box(2, 2, 3, 3, crs=4326)
    cases = [
        (gm.h3_cover, {'resolution': 2}),
        (gm.s2_cover, {'level': 3}),
        (
            gm.s2_cover,
            {'min_level': 3, 'max_level': 3, 'target_cells': 8},
        ),
        (gm.geohash_cover, {'precision': 3}),
        (gm.tile_cover, {'zoom': 3}),
    ]
    values = gm.GeometryArray([first, None, second, None], crs=4326)
    for cover, depth in cases:
        groups = cover(values, max_cells=None, **depth)
        assert len(groups) == 4
        assert list(groups[0]) == list(cover(first, max_cells=None, **depth))
        assert groups[1] is None
        assert list(groups[2]) == list(cover(second, max_cells=None, **depth))
        assert groups[3] is None


@pytest.mark.parametrize(
    'cover, depth, bounds',
    [
        (gm.h3_cover, {'resolution': 2}, (-10, -10, 10, 10)),
        (gm.geohash_cover, {'precision': 3}, (-2, -2, 2, 2)),
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
        assert len(keys) > 1
        at_limit = cover(polygon, cell_rule=rule, max_cells=len(keys), **depth)
        assert [
            cell.token if hasattr(cell, 'token') else cell.id for cell in at_limit
        ] == keys
        with pytest.raises(gm.GeometryError):
            cover(polygon, cell_rule=rule, max_cells=len(keys) - 1, **depth)

        groups = cover(
            gm.GeometryArray([polygon, None], crs=4326),
            cell_rule=rule,
            max_cells=None,
            **depth,
        )
        assert len(groups) == 2
        assert list(groups[0]) == list(cells)
        assert groups[1] is None


@pytest.mark.parametrize(
    'cover, depth, cell_rule',
    [
        (gm.geohash_cover, {'precision': 1}, 'overlap'),
        (gm.geohash_cover, {'precision': 1}, 'bbox'),
        (gm.geohash_cover, {'precision': 1}, 'center'),
        (gm.geohash_cover, {'precision': 1}, 'within'),
        (gm.s2_cover, {'level': 1}, 'overlap'),
        (gm.s2_cover, {'level': 1}, 'bbox'),
        (gm.s2_cover, {'level': 1}, 'center'),
        (gm.s2_cover, {'level': 1}, 'within'),
    ],
    ids=[
        'geohash-overlap',
        'geohash-bbox',
        'geohash-center',
        'geohash-within',
        's2-overlap',
        's2-bbox',
        's2-center',
        's2-within',
    ],
)
def test_rectangular_covers_use_xy_membership_for_mixed_ordinate_sources(
    cover, depth, cell_rule
) -> None:
    xyz = gm.box(-80, -60, 80, 60, crs=4326).set_z(1.0)
    xy = gm.box(-40, -30, 60, 50, crs=4326)
    source = gm.GeometryCollection([xyz, xy], crs=4326)
    original_wkb = source.to_wkb()
    original_axes = source.coordinate_axes
    xy_reference = source.force_2d()

    mixed_cells = cover(source, cell_rule=cell_rule, max_cells=None, **depth)
    reference_cells = cover(xy_reference, cell_rule=cell_rule, max_cells=None, **depth)

    assert _cell_keys(mixed_cells) == _cell_keys(reference_cells)
    assert _cell_keys(mixed_cells)
    assert source.coordinate_axes == original_axes == 'XYZ'
    assert source.to_wkb() == original_wkb


@pytest.mark.parametrize(
    'cover, depth',
    [
        (gm.h3_cover, {'resolution': 1}),
        (gm.tile_cover, {'zoom': 1}),
    ],
    ids=['h3', 'tile'],
)
def test_h3_and_tile_covers_use_xy_membership_for_mixed_ordinate_sources(
    cover, depth
) -> None:
    xyz = gm.box(0, 0, 2, 2, crs=4326).set_z(1.0)
    xy = gm.box(1, 1, 3, 3, crs=4326)
    source = gm.GeometryCollection([xyz, xy], crs=4326)
    original_wkb = source.to_wkb()
    original_axes = source.coordinate_axes
    xy_reference = source.force_2d()

    mixed_cells = cover(source, cell_rule='overlap', max_cells=None, **depth)
    reference_cells = cover(xy_reference, cell_rule='overlap', max_cells=None, **depth)

    assert _cell_keys(mixed_cells) == _cell_keys(reference_cells)
    assert _cell_keys(mixed_cells)
    assert source.coordinate_axes == original_axes == 'XYZ'
    assert source.to_wkb() == original_wkb


@pytest.mark.parametrize('crs', [None, 4326], ids=['crs-free', 'epsg4326'])
def test_rectangular_within_retains_closed_source_edges(crs: int | None) -> None:
    kwargs = {} if crs is None else {'crs': crs}
    source = gm.Polygon(
        [(-50, 0), (50, 0), (50, 50), (0, 60), (-50, 50), (-50, 0)],
        **kwargs,
    )
    cell = gm.Polygon([(-45, 0), (0, 0), (0, 45), (-45, 45), (-45, 0)], **kwargs)

    assert gm.within(cell, source)
    assert gm.covers(source, cell)
    assert {
        str(candidate)
        for candidate in gm.geohash_cover(source, precision=1, cell_rule='within')
    } == {
        'e',
        's',
    }
    assert {
        str(candidate)
        for candidate in gm.tile_cover(source, zoom=3, cell_rule='within')
    } == {
        '033',
        '122',
    }


@pytest.mark.parametrize('crs', [None, 4326], ids=['crs-free', 'epsg4326'])
def test_rectangular_within_excludes_an_exact_hole_cell(crs: int | None) -> None:
    kwargs = {} if crs is None else {'crs': crs}
    shell = [(-45, -45), (45, -45), (45, 45), (-45, 45), (-45, -45)]
    hole = [(-45, -45), (0, -45), (0, 0), (-45, 0), (-45, -45)]
    source = gm.Polygon(shell, holes=[hole], **kwargs)
    hole_cell = gm.Polygon(hole, **kwargs)

    assert not gm.within(hole_cell, source)
    assert not gm.covers(source, hole_cell)
    assert {
        str(candidate)
        for candidate in gm.geohash_cover(source, precision=1, cell_rule='within')
    } == {
        'e',
        'k',
        's',
    }


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


def test_bounding_array_projected_and_late_invalid() -> None:
    match = r'invalid longitude/latitude \(0, 95\)'
    good = gm.Point(13.4, 52.5, crs=4326)
    bad = gm.Point(0.0, 95.0, crs=4326)
    late = gm.GeometryArray([good, bad])
    with pytest.raises(gm.InvalidGeometryError, match=match):
        gm.h3_bounding_cell(late)
    with pytest.raises(gm.InvalidGeometryError, match=match):
        gm.s2_bounding_cell(late)
    projected = gm.GeometryArray([good.to_crs(3857)])
    assert gm.h3_bounding_cell(projected) == gm.h3_bounding_cell(good)
    assert gm.s2_bounding_cell(projected) == gm.s2_bounding_cell(good)


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


def _tile_tokens(cells) -> set[str]:
    return {cell.token for cell in cells}


def _tile_rectangle(tile: gm.Tile, *, crs: int | None) -> gm.Polygon:
    polygon = tile.polygon
    if crs == 4326:
        return polygon

    _, miny, _, maxy = polygon.bounds
    cells = 2**tile.zoom
    minx = tile.x / cells * 360.0 - 180.0
    maxx = (tile.x + 1) / cells * 360.0 - 180.0
    return gm.box(minx, miny, maxx, maxy)


def _tile_rectangles(tiles, *, crs: int | None):
    return [_tile_rectangle(tile, crs=crs) for tile in tiles]


@pytest.mark.parametrize('crs', [None, 4326], ids=['crs-free', 'epsg4326'])
@pytest.mark.parametrize('south', [False, True], ids=['north', 'south'])
def test_tile_cover_polar_cap_matches_exact_rectangular_relations(
    crs: int | None, south: bool
) -> None:
    min_lat, max_lat = (-90, -80) if south else (80, 90)
    source = gm.box(-60, min_lat, 60, max_lat, **({} if crs is None else {'crs': crs}))
    expected_bounds = (
        (-60.0, -90.0, 60.0, -80.0) if south else (-60.0, 80.0, 60.0, 90.0)
    )
    assert source.bounds == expected_bounds

    tiles = [gm.Tile(x=x, y=y, zoom=4) for y in range(16) for x in range(16)]
    rectangles = _tile_rectangles(tiles, crs=crs)
    expected_overlap = {
        tile.token
        for tile, rectangle in zip(tiles, rectangles, strict=True)
        if gm.intersects(source, rectangle)
    }
    expected_within = {
        tile.token
        for tile, rectangle in zip(tiles, rectangles, strict=True)
        if gm.covers(source, rectangle)
    }
    if not south:
        assert expected_overlap == {
            '0101',
            '0103',
            '0110',
            '0112',
            '0111',
            '0113',
            '1000',
            '1002',
            '1001',
            '1003',
            '1010',
            '1012',
        }
        assert expected_within == {'0110', '0111', '1000', '1001'}

    overlap = gm.tile_cover(source, zoom=4, cell_rule='overlap', max_cells=None)
    bbox = gm.tile_cover(source, zoom=4, cell_rule='bbox', max_cells=None)
    within = gm.tile_cover(source, zoom=4, cell_rule='within', max_cells=None)
    assert _tile_tokens(overlap) == expected_overlap
    assert _tile_tokens(bbox) == expected_overlap
    assert _tile_tokens(within) == expected_within
    assert all(
        gm.intersects(source, rectangle)
        for rectangle in _tile_rectangles(overlap, crs=crs)
    )
    assert all(
        gm.covers(source, rectangle) for rectangle in _tile_rectangles(within, crs=crs)
    )

    with pytest.raises(gm.GeometryError, match='max_cells'):
        gm.tile_cover(source, zoom=4, cell_rule='overlap', max_cells=11)
    assert len(gm.tile_cover(source, zoom=4, cell_rule='overlap', max_cells=12)) == 12
    with pytest.raises(gm.GeometryError, match='max_cells'):
        gm.tile_cover(source, zoom=4, cell_rule='within', max_cells=3)
    assert len(gm.tile_cover(source, zoom=4, cell_rule='within', max_cells=4)) == 4


@pytest.mark.parametrize('crs', [None, 4326], ids=['crs-free', 'epsg4326'])
@pytest.mark.parametrize('south', [False, True], ids=['north', 'south'])
def test_tile_cover_pole_point_is_empty(crs: int | None, south: bool) -> None:
    kwargs = {} if crs is None else {'crs': crs}
    point = gm.Point(0, -90 if south else 90, **kwargs)
    assert len(gm.tile_cover(point, zoom=4, max_cells=None)) == 0


@pytest.mark.parametrize('crs', [None, 4326], ids=['crs-free', 'epsg4326'])
def test_tile_cover_pole_line_reaches_top_row(crs: int | None) -> None:
    kwargs = {} if crs is None else {'crs': crs}
    source = gm.LineString([(-60, 80), (0, 90)], **kwargs)
    overlap = gm.tile_cover(source, zoom=4, cell_rule='overlap', max_cells=None)
    within = gm.tile_cover(source, zoom=4, cell_rule='within', max_cells=None)
    assert _tile_tokens(overlap) == {'0103', '0110', '0112'}
    assert not within
    assert all(
        gm.intersects(source, _tile_rectangle(tile, crs=crs)) for tile in overlap
    )


@pytest.mark.parametrize('crs', [None, 4326], ids=['crs-free', 'epsg4326'])
def test_tile_cover_seam_polar_cap_keeps_both_antimeridian_sides(
    crs: int | None,
) -> None:
    kwargs = {} if crs is None else {'crs': crs}
    source = gm.Polygon(
        [(170, 80), (-170, 80), (-170, 90), (170, 90)],
        **kwargs,
    )
    overlap = gm.tile_cover(source, zoom=4, cell_rule='overlap', max_cells=None)
    bbox = gm.tile_cover(source, zoom=4, cell_rule='bbox', max_cells=None)
    within = gm.tile_cover(source, zoom=4, cell_rule='within', max_cells=None)
    assert _tile_tokens(overlap) == {'0000', '0002', '1111', '1113'}
    assert _tile_tokens(bbox) == _tile_tokens(overlap)
    assert not within
    assert all(
        gm.intersects(source, _tile_rectangle(tile, crs=crs)) for tile in overlap
    )


@pytest.mark.parametrize('crs', [None, 4326], ids=['crs-free', 'epsg4326'])
def test_tile_cover_pole_touching_hole_removes_middle_within_tiles(
    crs: int | None,
) -> None:
    kwargs = {} if crs is None else {'crs': crs}
    source = gm.Polygon(
        [(-60, 80), (60, 80), (60, 90), (-60, 90), (-60, 80)],
        holes=[[(-10, 80), (10, 80), (10, 90), (-10, 90), (-10, 80)]],
        **kwargs,
    )
    within = gm.tile_cover(source, zoom=4, cell_rule='within', max_cells=None)
    assert _tile_tokens(within) == {'0110', '1001'}
    assert all(gm.covers(source, _tile_rectangle(tile, crs=crs)) for tile in within)


@pytest.mark.parametrize(
    'bounds',
    [
        (-10, 84, 10, 85.1),
        (-10, 84, 10, 89.9),
        (-10, -89.9, 10, -84),
        (-10, -85.1, 10, -84),
    ],
    ids=['north-edge', 'north-poleward', 'south-poleward', 'south-edge'],
)
@pytest.mark.parametrize('crs', [None, 4326], ids=['crs-free', 'epsg4326'])
def test_tile_cover_still_rejects_nonpolar_mercator_overflow(
    bounds: tuple[int, int | float, int, int | float], crs: int | None
) -> None:
    kwargs = {} if crs is None else {'crs': crs}
    source = gm.box(*bounds, **kwargs)
    with pytest.raises(gm.InvalidGeometryError, match='square Web Mercator tile grid'):
        gm.tile_cover(source, zoom=4)


_COVERS = {
    'h3': lambda source, rule: gm.h3_cover(source, 5, cell_rule=rule),
    's2': lambda source, rule: gm.s2_cover(source, 9, cell_rule=rule),
    'geohash': lambda source, rule: gm.geohash_cover(source, 4, cell_rule=rule),
    'tile': lambda source, rule: gm.tile_cover(source, zoom=8, cell_rule=rule),
}


@pytest.mark.parametrize('grid', sorted(_COVERS))
@pytest.mark.parametrize(
    'source',
    [
        gm.Polygon([(0, -2), (4, 2), (2, 4), (-2, 0), (0, -2)], crs=4326),
        gm.Polygon([(179, -5), (-179, -5), (-179, 5), (179, 5), (179, -5)], crs=4326),
    ],
    ids=['rotated', 'seam'],
)
def test_bbox_rule_covers_the_envelope_windows_on_every_grid(
    grid: str, source: gm.Geometry
) -> None:
    cover = _COVERS[grid]
    windows = gm.GeometryCollection([
        part.envelope() for part in source.split_antimeridian().parts
    ])
    bbox = set(cover(source, 'bbox').to_numpy().tolist())
    assert bbox == set(cover(windows, 'overlap').to_numpy().tolist())
    assert set(cover(source, 'overlap').to_numpy().tolist()) <= bbox


@pytest.mark.parametrize(
    'factory',
    [
        gm.h3_bounding_cell,
        gm.s2_bounding_cell,
        gm.geohash_bounding_cell,
        gm.tile_bounding_cell,
    ],
    ids=['h3', 's2', 'geohash', 'tile'],
)
def test_bounding_cell_skips_missing_rows(factory) -> None:
    arr = gm.GeometryArray([gm.Point(13.4, 52.5, crs=4326), None])
    cell = factory(arr)
    assert cell is not None
    # Present nonempty row folds; all-missing still errors.
    with pytest.raises(gm.InvalidGeometryError, match='non-empty'):
        factory(gm.GeometryArray([None, None], crs=4326))


def _assert_not_panic(exc: BaseException) -> None:
    assert type(exc).__name__ != 'PanicException', exc


def test_default_max_cells_names_knob_on_runaway():
    """Default 1_000_000 still rejects a runaway and names max_cells."""
    # Full-longitude world band (not a |Δlon|>180 seam edge — those normalize
    # to a narrow strip and no longer runaway under geographic covering).
    src = gm.box(-180.0, -85.0, 180.0, 85.0, crs=4326)
    with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
        gm.tile_cover(src, zoom=12, cell_rule='bbox')
    _assert_not_panic(excinfo.value)
    assert '1000000' in str(excinfo.value) or '1_000_000' in str(excinfo.value)


def test_max_cells_none_allows_past_old_wall():
    """max_cells=None (or a large value) succeeds where the hard wall used to fire."""
    # A covering that sits just above 1M cells with a tight budget must fail,
    # then succeed with None / a raised budget.
    src = gm.box(-10.0, -10.0, 10.0, 10.0, crs=4326)
    with pytest.raises(gm.GeometryError, match='max_cells'):
        gm.tile_cover(src, zoom=10, cell_rule='bbox', max_cells=100)
    cov = gm.tile_cover(src, zoom=10, cell_rule='bbox', max_cells=None)
    assert len(cov) > 100
    cov2 = gm.tile_cover(src, zoom=10, cell_rule='bbox', max_cells=1_000_000)
    assert len(cov2) == len(cov)


@pytest.mark.parametrize(
    'factory',
    [
        lambda src, **kw: gm.h3_cover(src, 3, **kw),
        lambda src, **kw: gm.s2_cover(src, level=6, **kw),
        lambda src, **kw: gm.geohash_cover(src, 4, **kw),
        lambda src, **kw: gm.tile_cover(src, 6, **kw),
    ],
    ids=['h3', 's2', 'geohash', 'tile'],
)
def test_max_cells_nonpositive_typed_error(factory):
    src = gm.box(0, 0, 1, 1, crs=4326)
    for bad in (0, -1):
        with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
            factory(src, max_cells=bad)
        _assert_not_panic(excinfo.value)


def test_s2_fixed_level_max_cells_is_a_hard_cap_like_other_grids():
    """Every fixed-level coverer raises when its hard cap is too small."""
    area = gm.box(0, 0, 2, 2, crs=4326)
    factories = (
        lambda: gm.h3_cover(area, resolution=6, max_cells=1),
        lambda: gm.s2_cover(area, level=10, max_cells=1),
        lambda: gm.geohash_cover(area, precision=5, max_cells=1),
        lambda: gm.tile_cover(area, zoom=8, max_cells=1),
    )
    for factory in factories:
        with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
            factory()
        _assert_not_panic(excinfo.value)


@pytest.mark.parametrize(
    'factory',
    [
        lambda src, **kw: gm.geohash_cover(src, precision=4, **kw),
        lambda src, **kw: gm.tile_cover(src, zoom=7, **kw),
        lambda src, **kw: gm.s2_cover(src, level=8, **kw),
    ],
    ids=['geohash', 'tile', 's2'],
)
def test_within_budget_charges_only_visible_cells(factory):
    source = gm.box(-20, -20, 20, 20, crs=4326)
    visible = factory(source, cell_rule='within', max_cells=None)
    overlap = factory(source, cell_rule='overlap', max_cells=None)
    assert 0 < len(visible) < len(overlap)

    assert len(factory(source, cell_rule='within', max_cells=len(visible))) == len(
        visible
    )
    with pytest.raises(gm.GeometryError, match='max_cells'):
        factory(source, cell_rule='within', max_cells=len(visible) - 1)


@pytest.mark.parametrize('rule', ['within', 'center'])
def test_s2_filtered_rules_budget_visible_emissions_only(rule):
    source = gm.box(0, 0, 2, 2, crs=4326)
    overlap = gm.s2_cover(source, level=8, cell_rule='overlap', max_cells=None)
    visible = gm.s2_cover(source, level=8, cell_rule=rule, max_cells=None)
    assert 0 < len(visible) < len(overlap)

    exact = gm.s2_cover(source, level=8, cell_rule=rule, max_cells=len(visible))
    assert list(exact) == list(visible)
    with pytest.raises(gm.GeometryError, match='max_cells'):
        gm.s2_cover(source, level=8, cell_rule=rule, max_cells=len(visible) - 1)


@pytest.mark.parametrize('rule', ['within', 'center'])
def test_s2_adaptive_filtered_rules_coarsen_against_visible_cap(rule):
    source = gm.box(-20, -20, 20, 20, crs=4326)
    unlimited = gm.s2_cover(
        source,
        cell_rule=rule,
        min_level=1,
        max_level=12,
        target_cells=1000,
        max_cells=None,
    )
    capped = gm.s2_cover(
        source,
        cell_rule=rule,
        min_level=1,
        max_level=12,
        target_cells=1000,
        max_cells=16,
    )
    assert len(unlimited) == 1000
    assert len(capped) <= 16
    assert max(cell.level for cell in capped) < max(cell.level for cell in unlimited)


@pytest.mark.parametrize('rule', ['within', 'center'])
def test_s2_min_level_rejects_unavoidable_visible_overflow(rule):
    source = gm.box(0, 0, 2, 2, crs=4326)
    floor = gm.s2_cover(source, level=8, cell_rule=rule, max_cells=None)
    with pytest.raises(gm.GeometryError, match='max_cells'):
        gm.s2_cover(
            source,
            cell_rule=rule,
            min_level=8,
            max_level=12,
            target_cells=1000,
            max_cells=len(floor) - 1,
        )


@pytest.mark.parametrize(
    'factory',
    [
        lambda src, **kw: gm.geohash_cover(src, precision=3, **kw),
        lambda src, **kw: gm.tile_cover(src, zoom=5, **kw),
        lambda src, **kw: gm.h3_cover(src, resolution=3, **kw),
        lambda src, **kw: gm.s2_cover(src, level=5, **kw),
    ],
    ids=['geohash', 'tile', 'h3', 's2'],
)
def test_grouped_cover_budget_is_cumulative(factory):
    source = gm.box(-5, -5, 5, 5, crs=4326)
    one = factory(source, max_cells=None)
    grouped = gm.GeometryArray([source, source], crs=4326)
    total = 2 * len(one)

    result = factory(grouped, max_cells=total)
    assert sum(len(group) for group in result) == total
    with pytest.raises(gm.GeometryError, match='max_cells'):
        factory(grouped, max_cells=total - 1)


def test_s2_adaptive_array_cap_is_cumulative_for_point_rows() -> None:
    points = gm.GeometryArray([
        gm.Point(0.25, 0.25, crs=4326),
        gm.Point(120.25, 0.25, crs=4326),
        gm.Point(-120.25, -0.25, crs=4326),
    ])
    options = {
        'min_level': 0,
        'max_level': 4,
        'target_cells': 8,
    }

    groups = gm.s2_cover(points, max_cells=3, **options)
    assert [len(group) for group in groups] == [1, 1, 1]
    with pytest.raises(gm.GeometryError, match='max_cells'):
        gm.s2_cover(points, max_cells=2, **options)


def test_s2_target_cells_quality_target_and_max_cells_ceiling():
    """target_cells seeks quality; max_cells is the output ceiling."""
    area = gm.box(0, 0, 2, 2, crs=4326)
    adaptive = gm.s2_cover(area, min_level=4, max_level=8, target_cells=64)
    assert 0 < len(adaptive) <= 64
    fixed = gm.s2_cover(area, level=6, max_cells=10_000)
    assert len(fixed) > 0
    unlimited = gm.s2_cover(area, level=6, max_cells=None)
    assert len(unlimited) == len(fixed)


def test_s2_adaptive_max_cells_caps_output():
    """Adaptive max_cells coarsens a detailed cover to the requested ceiling."""
    line = gm.LineString([(-75.0, 40.0), (-74.0, 40.5), (-73.0, 41.2)], crs=4326)
    unlimited = gm.s2_cover(
        line, min_level=1, max_level=14, target_cells=1_000_000, max_cells=None
    )
    capped = gm.s2_cover(
        line, min_level=1, max_level=14, target_cells=1_000_000, max_cells=64
    )
    assert len(unlimited) == 640
    assert len(capped) == 64


def test_s2_adaptive_high_min_level_max_cells_rejects_typed():
    """A high-min-level interior subtree rejects rather than reserving huge space."""
    source = gm.box(-10.0, -10.0, 10.0, 10.0, crs=4326)
    with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
        gm.s2_cover(
            source,
            min_level=20,
            max_level=21,
            target_cells=8,
            max_cells=8,
        )
    _assert_not_panic(excinfo.value)


def test_s2_cover_budget_equals_unlimited_fit_threshold():
    """Revert-sensitive B2: max_cells charges proven emissions, not DFS frontier.

    Short line unlimited L10 cover is exactly one cell; budgets 1..K+2 must
    succeed with the same tokens (previously 1..4 false-rejected because the
    transient fail-open frontier was charged against max_cells).
    """
    line = gm.LineString([(-75.0, 40.0), (-74.99, 40.01)], crs=4326)
    unlimited = gm.s2_cover(line, level=10, max_cells=None)
    tokens = [c.token for c in unlimited]
    k = len(tokens)
    assert k == 1
    assert tokens == ['89c6b5']
    for m in range(1, k + 3):
        got = gm.s2_cover(line, level=10, max_cells=m)
        assert [c.token for c in got] == tokens, m
    # Matrix: for several short geoms, M>=K succeeds equal; M<K raises.
    samples = [
        gm.Point(-75.0, 40.0, crs=4326),
        gm.LineString([(-75.0, 40.0), (-74.99, 40.01)], crs=4326),
        gm.LineString([(-75.0, 40.0), (-74.5, 40.2)], crs=4326),
        gm.box(-75.0, 40.0, -74.9, 40.1, crs=4326),
    ]
    for geom in samples:
        unlim = gm.s2_cover(geom, level=10, max_cells=None)
        kk = len(unlim)
        assert kk >= 1
        unlim_ids = {c.id for c in unlim}
        for m in range(kk, kk + 3):
            got = gm.s2_cover(geom, level=10, max_cells=m)
            assert {c.id for c in got} == unlim_ids
            assert len(got) <= m
        if kk > 1:
            with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
                gm.s2_cover(geom, level=10, max_cells=kk - 1)
            _assert_not_panic(excinfo.value)
            assert isinstance(excinfo.value, gm.GeometryError)


def test_h3_within_budget_counts_only_emitted_cells():
    """Discarded outline probes cannot consume H3's public output budget.

    The independent oracle is the unlimited *visible* token set.  At exactly
    that cardinality scalar and packed covers must succeed unchanged; one less
    must fail when there is more than one emitted cell.  The two polygons and
    their MultiPolygon exercise the single and aggregate owners that used to
    reject at outline counts 14/15 despite emitting 3/6/9 cells.
    """
    first = gm.box(10.0, 40.0, 20.0, 50.0, crs=4326)
    second = gm.box(-80.0, -40.0, -70.0, -30.0, crs=4326)
    for source in (first, second, gm.MultiPolygon([first, second], crs=4326)):
        unlimited = gm.h3_cover(source, 2, cell_rule='within', max_cells=None)
        expected = {cell.token for cell in unlimited}
        assert expected
        cap = len(expected)
        for cells in (
            gm.h3_cover(source, 2, cell_rule='within', max_cells=cap),
            gm.h3_cover(
                gm.GeometryArray([source]), 2, cell_rule='within', max_cells=cap
            )[0],
        ):
            actual = {cell.token for cell in cells}
            assert actual == expected, (
                'discarded outline owners changed the emitted H3 set'
            )
            assert len(actual) <= cap
        if cap > 1:
            with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
                gm.h3_cover(source, 2, cell_rule='within', max_cells=cap - 1)
            _assert_not_panic(excinfo.value)


@pytest.mark.parametrize('cell_rule', ['center', 'within'])
def test_h3_polygon_budget_stops_in_the_streamed_outline_owner(cell_rule: str) -> None:
    """A capped high-resolution polygon never first materializes its trace.

    The 180-degree, one-degree-tall source has multiple distinct resolution-15
    center cells, so the independent public contract is a typed output-budget
    failure at one cell.  Both polygon outline owners (center and within) are
    exercised, not the line helper; there is no timing assertion.
    """
    source = gm.Polygon(
        [(-90.0, 0.0), (90.0, 0.0), (90.0, 1.0), (-90.0, 1.0)], crs=4326
    )
    with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
        gm.h3_cover(source, 15, cell_rule=cell_rule, max_cells=1)
    _assert_not_panic(excinfo.value)


def test_s2_adaptive_target_cells_matches_requested_detail():
    """Adaptive target_cells charges proven-productive cells only.

    box(-75,40,-74.9,40.1) at target 4 is exactly 4 L10 cells; previously
    small targets false-rejected because fail-open Boundary queue
    entries were charged before deeper pruning to Outside.
    """
    area = gm.box(-75.0, 40.0, -74.9, 40.1, crs=4326)
    detailed = gm.s2_cover(area, min_level=1, max_level=10, target_cells=4)
    tokens = [c.token for c in detailed]
    k = len(tokens)
    assert k == 4
    assert tokens == ['89c14b', '89c14d', '89c6b3', '89c6b5']
    assert {c.level for c in detailed} == {10}
    # A target at or above K preserves the detailed covering.
    for m in range(k, k + 3):
        got = gm.s2_cover(area, min_level=1, max_level=10, target_cells=m)
        assert [c.token for c in got] == tokens, m
        assert len(got) <= m
    # Smaller targets coarsen only inside the requested adaptive range.
    for m in range(1, k):
        got = gm.s2_cover(area, min_level=1, max_level=10, target_cells=m)
        assert 0 < len(got) <= m, m
        assert all(c.level >= 1 for c in got)
    coarse = gm.s2_cover(area, min_level=0, max_level=10, target_cells=1)
    assert len(coarse) <= 1


def test_s2_fixed_level_soft_budget_never_emits_below_min_level():
    """F7: max_cells soft budget must not emit coarser than a fixed level.

    Regression: after N9, s2_cover(box, level=10, max_cells=256) mixed in
    level-9 interior cells even though the true level-10 cover (225 cells)
    fits in 256. Fixed-level purity requires every cell at exactly L when
    the level-L cover fits.
    """
    src = gm.box(0, 0, 1, 1, crs=4326)
    # Repro: level-10 unlimited is 225 cells — must match max_cells=256.
    unlimited = gm.s2_cover(src, level=10, max_cells=None)
    capped = gm.s2_cover(src, level=10, max_cells=256)
    assert len(unlimited) == 225
    assert len(capped) == len(unlimited)
    assert {c.level for c in capped} == {10}
    assert {c.id for c in capped} == {c.id for c in unlimited}
    # Pickle round-trip preserves pure level-10 set.
    restored = pickle.loads(pickle.dumps(capped))
    assert {c.level for c in restored} == {10}
    np.testing.assert_array_equal(restored.to_numpy(), capped.to_numpy())


@pytest.mark.parametrize('level', [4, 8, 10, 14])
@pytest.mark.parametrize('cell_rule', ['overlap', 'within', 'center', 'bbox'])
def test_s2_fixed_level_matrix_only_emits_level_l(level, cell_rule):
    """F7 matrix: when the level-L cover fits in M, every cell is level L."""
    src = gm.box(0, 0, 1, 1, crs=4326)
    unlimited = gm.s2_cover(src, level=level, cell_rule=cell_rule, max_cells=None)
    # max_cells budgets the exact coverer before cell_rule filtering, so size
    # M from the underlying overlap cover (within/center are subsets).
    overlap_fit = len(
        gm.s2_cover(src, level=level, cell_rule='overlap', max_cells=None)
    )
    assert all(c.level == level for c in unlimited)
    # Finite M above the coverer fit — must not coarsen below level L.
    budget = max(overlap_fit, 1) + 64
    capped = gm.s2_cover(src, level=level, cell_rule=cell_rule, max_cells=budget)
    assert len(capped) == len(unlimited)
    assert all(c.level == level for c in capped)
    assert {c.id for c in capped} == {c.id for c in unlimited}
    # Array path agrees with scalar for one-row GeometryArray (Groups of cells).
    arr = gm.GeometryArray([src])
    arr_cov = gm.s2_cover(arr, level=level, cell_rule=cell_rule, max_cells=budget)
    assert isinstance(arr_cov, gm.Groups)
    assert len(arr_cov) == 1
    row = arr_cov[0]
    assert {c.id for c in row} == {c.id for c in unlimited}
    assert all(c.level == level for c in row)


def test_s2_target_cells_respects_min_level_and_max_cells_stays_hard():
    """Adaptive targeting stays in range; max_cells remains a ceiling."""
    src = gm.box(0, 0, 1, 1, crs=4326)
    tight = gm.s2_cover(src, min_level=0, max_level=12, target_cells=1)
    assert len(tight) <= 1
    # Adaptive with min_level floor: every emitted cell is >= min_level.
    adaptive = gm.s2_cover(src, min_level=4, max_level=12, target_cells=64)
    assert 0 < len(adaptive) <= 64
    levels = {c.level for c in adaptive}
    assert levels
    assert min(levels) >= 4
    assert max(levels) <= 12
    # Fixed-level that cannot fit raises a typed GeometryError naming max_cells.
    with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
        gm.s2_cover(src, level=14, max_cells=1)
    _assert_not_panic(excinfo.value)


def test_s2_level30_tiny_budget_rejects_without_frontier_flood():
    """Defect 2: level=30 + max_cells=1 must reject without BFS flood.

    The native coverer test observes the bounded pending frontier directly.
    This Python test keeps the public semantic regression: the request must
    reject with a typed max_cells error rather than building a result.
    """
    line = gm.LineString([(0.0, 0.0), (0.1, 0.0)], crs=4326)
    # Warm classifier / PROJ paths so the measured call is cover work.
    gm.s2_cover(gm.Point(0.0, 0.0, crs=4326), level=4, max_cells=8)

    with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
        gm.s2_cover(line, level=30, max_cells=1)

    _assert_not_panic(excinfo.value)


def test_s2_world_line_l20_tiny_budget_rejects():
    """World-spanning line at L20 + tiny budget raises max_cells (no flood)."""
    line = gm.LineString([(-179.0, 0.0), (179.0, 0.0)], crs=4326)
    with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
        gm.s2_cover(line, level=20, max_cells=1)
    _assert_not_panic(excinfo.value)


def test_s2_point_level30_max_cells_one_succeeds():
    """Ordinary point at level=30 with max_cells=1 is a single cell."""
    pt = gm.Point(13.4, 52.5, crs=4326)
    cov = gm.s2_cover(pt, level=30, max_cells=1)
    assert len(cov) == 1
    assert cov[0].level == 30


def test_s2_adaptive_max_level30_target_one_returns_coarse():
    """Adaptive min=0/max=30/target_cells=1 returns one coarse cell."""
    area = gm.box(0, 0, 1, 1, crs=4326)
    cov = gm.s2_cover(area, min_level=0, max_level=30, target_cells=1)
    assert len(cov) <= 1


def test_s2_fixed_level_threshold_n_identical_n_minus_one_raises():
    """F7 threshold: budget=N matches unlimited; budget=N-1 raises."""
    src = gm.box(0, 0, 1, 1, crs=4326)
    unlimited = gm.s2_cover(src, level=10, max_cells=None)
    n = len(unlimited)
    assert n > 1
    exact = gm.s2_cover(src, level=10, max_cells=n)
    assert len(exact) == n
    assert {c.id for c in exact} == {c.id for c in unlimited}
    assert all(c.level == 10 for c in exact)
    with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
        gm.s2_cover(src, level=10, max_cells=n - 1)
    _assert_not_panic(excinfo.value)


@pytest.mark.parametrize('level_mod', [1, 2, 3])
def test_s2_level_mod_target_cells_conforming_and_bounded(level_mod):
    """level_mod fanout uses actual K; outputs stay conforming and ≤ target."""
    src = gm.box(0, 0, 2, 2, crs=4326)
    target = 32
    cov = gm.s2_cover(
        src,
        min_level=4,
        max_level=12,
        level_mod=level_mod,
        target_cells=target,
    )
    assert 0 < len(cov) <= target
    for cell in cov:
        assert cell.level >= 4
        assert cell.level <= 12
        assert (cell.level - 4) % level_mod == 0


def test_s2_interior_subtree_rejects_before_enumeration():
    """Interior 4^Δ preflight rejects a huge fixed-level areal cover early."""
    # Large box + fine min_level: interior faces expand to 4^Δ terminals.
    src = gm.box(-60.0, -40.0, 60.0, 40.0, crs=4326)
    with pytest.raises(gm.GeometryError, match='max_cells') as excinfo:
        gm.s2_cover(src, level=16, max_cells=100)
    _assert_not_panic(excinfo.value)
