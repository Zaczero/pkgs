"""Grid cover functions return CellArray or Groups."""

import gometry as gm
import pytest

COVER_CASES = [
    (gm.h3_cover, {'resolution': 2}, 'H3'),
    (gm.s2_cover, {'level': 3}, 'S2'),
    (gm.geohash_cover, {'precision': 3}, 'geohash'),
    (gm.tile_cover, {'zoom': 3}, 'Tile'),
]


def test_empty_source_cover_requires_non_empty_geometry() -> None:
    empty = gm.from_wkt('POINT EMPTY', crs=4326)
    with pytest.raises(
        gm.InvalidGeometryError,
        match='H3 coverage requires a non-empty geometry',
    ):
        gm.h3_cover(empty, resolution=5)


@pytest.mark.parametrize(
    'cover, depth, label', COVER_CASES, ids=['h3', 's2', 'geohash', 'tile']
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
    ids=['h3', 's2', 'geohash', 'tile'],
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
    ids=['h3', 's2', 'geohash', 'tile'],
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


def test_interleaved_missing_cover_rows_keep_logical_alignment() -> None:
    first = gm.box(-3, -3, -2, -2, crs=4326)
    second = gm.box(2, 2, 3, 3, crs=4326)
    cases = [
        (gm.h3_cover, {'resolution': 2}),
        (gm.s2_cover, {'level': 3}),
        (gm.geohash_cover, {'precision': 3}),
        (gm.tile_cover, {'zoom': 3}),
    ]
    values = gm.GeometryArray([first, None, second, None], crs=4326)
    for cover, depth in cases:
        groups = cover(values, max_cells=None, **depth)
        assert len(groups) == 4
        assert list(groups[0]) == list(cover(first, max_cells=None, **depth))
        assert len(groups[1]) == 0
        assert list(groups[2]) == list(cover(second, max_cells=None, **depth))
        assert len(groups[3]) == 0


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
        assert len(groups[1]) == 0


@pytest.mark.parametrize(
    'cover, depth',
    [
        (gm.geohash_cover, {'precision': 1}),
        (gm.s2_cover, {'level': 1}),
    ],
    ids=['geohash', 's2'],
)
def test_cover_propagates_dissolved_area_error(cover, depth) -> None:
    xyz = gm.Polygon(
        [(0, 0, 1), (2, 0, 1), (2, 2, 1), (0, 2, 1), (0, 0, 1)],
        crs=4326,
    )
    xy = gm.box(1, 1, 3, 3, crs=4326)
    source = gm.GeometryCollection([xyz, xy], crs=4326)

    with pytest.raises(gm.InvalidGeometryError) as raised:
        cover(source, **depth)

    assert type(raised.value) is gm.InvalidGeometryError
    assert type(raised.value).__name__ != 'PanicException'


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


def _tile_rectangles(tiles, *, crs: int | None):
    kwargs = {} if crs is None else {'crs': crs}
    return [gm.box(*tile.polygon.bounds, **kwargs) for tile in tiles]


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
        gm.intersects(source, gm.box(*tile.polygon.bounds, **kwargs))
        for tile in overlap
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
        gm.intersects(source, gm.box(*tile.polygon.bounds, **kwargs))
        for tile in overlap
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
    assert all(
        gm.covers(source, gm.box(*tile.polygon.bounds, **kwargs)) for tile in within
    )


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
