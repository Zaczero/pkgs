"""Prepared source, collapse and incremental tile contracts."""

import math
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import gometry as gm
import pytest


def test_collapse_policy_distinguishes_grid_loss_from_spatial_exclusion() -> None:
    frame = gm.TileFrame((0, 0, 16, 16), crs=None)
    collapsed = gm.Features([gm.LineString([(1.1, 15), (1.4, 15)])])
    assert (
        len(gm.mvt_prepare(collapsed, frame=frame, extent=16, buffer=0).features) == 0
    )
    with pytest.raises(gm.GeometryError, match='collapsed'):
        gm.mvt_prepare(collapsed, frame=frame, extent=16, buffer=0, on_collapse='raise')
    outside = gm.Features([gm.LineString([(20.1, 15), (20.4, 15)])])
    result = gm.mvt_prepare(
        outside, frame=frame, extent=16, buffer=0, on_collapse='raise'
    )
    assert len(result.features) == 0


def test_collapse_policy_ignores_a_hole_outside_the_clipping_rectangle() -> None:
    frame = gm.TileFrame((0, 0, 16, 16), crs=None)
    polygon = gm.from_wkt(
        'POLYGON ((-3 19,19 19,19 -3,-3 -3,-3 19),'
        '(16.1 15,16.1 14.8,16.4 14.8,16.4 15,16.1 15))'
    )
    result = gm.mvt_prepare(
        gm.Features([polygon]), frame=frame, extent=16, buffer=0, on_collapse='raise'
    )
    assert result.features.geometries[0].area == 256


def test_large_buffer_does_not_reject_geometry_excluded_by_the_final_clip() -> None:
    boundary = 1 << 54
    polygon = gm.Polygon([
        (boundary - 2, -boundary - 4),
        (boundary + 4, -boundary + 2),
        (boundary + 4, -boundary - 4),
        (boundary - 2, -boundary - 4),
    ])
    frame = gm.TileFrame((0, -1, 1, 0), crs=None)
    layer = gm.mvt_prepare(
        gm.Features([polygon]), frame=frame, extent=1, buffer=boundary
    )
    assert len(layer.features) == 0


def test_z_loss_requires_permission() -> None:
    frame = gm.TileFrame((0, 0, 8, 8), crs=None)
    features = gm.Features([gm.Point(2, 3, z=7)], ids=[0])
    with pytest.raises(gm.InvalidGeometryError):
        gm.mvt_prepare(features, frame=frame)
    result = gm.mvt_prepare(features, frame=frame, drop_zm=True)
    assert result.features.ids == (0,)
    assert not result.features.geometries.has_z.any()


def test_grid_merging_polygon_components_keeps_one_valid_feature() -> None:
    frame = gm.TileFrame((0, 0, 8, 8), crs=None)
    geometry = gm.from_wkt(
        'MULTIPOLYGON (((0.1 0.1,2.1 0.1,2.1 2.1,0.1 2.1,0.1 0.1)),'
        '((2.2 0.1,4.2 0.1,4.2 2.1,2.2 2.1,2.2 0.1)))'
    )
    source = gm.Features([geometry], properties=[{'owner': 'one'}], ids=[7])
    result = gm.mvt_prepare(source, frame=frame, extent=8, buffer=0)
    assert result.features.ids == (7,)
    assert result.features[0].properties == {'owner': 'one'}
    assert result.features.geometries[0].is_valid
    assert result.features.geometries[0].area == 8


@pytest.mark.parametrize('method', ['vw', 'dp'])
def test_simplification_preserves_the_entire_boundary_trace(method: str) -> None:
    frame = gm.TileFrame((0, 0, 16, 16), crs=None)
    features = gm.Features([gm.LineString([(16, 12), (10, 10), (10, 6), (16, 4)])])
    layer = gm.mvt_prepare(
        features, frame=frame, extent=16, buffer=0, tolerance=100, method=method
    )
    edge = gm.LineString([(16, 0), (16, 16)])
    contacts = gm.intersection(layer.features.geometries[0], edge)
    assert contacts.length == 0
    assert gm.covers(contacts, gm.Point(16, 4))
    assert gm.covers(contacts, gm.Point(16, 12))


def test_reusable_source_and_explicit_tile_iteration_preserve_order() -> None:
    features = gm.Features.from_columns(
        gm.points([0, 1_000], [0, 1_000], crs=3857),
        columns={'name': ['first', 'second']},
        ids=[0, 7],
    )
    source = gm.MVTSource(features)
    tile = gm.Tile(x=1, y=1, zoom=1)
    expected = gm.to_mvt({'places': source}, tile=tile)
    results = gm.mvt_tiles({'places': source}, tiles=[tile, tile])
    first, second = list(results)
    assert first == second == (tile, expected)
    decoded = gm.from_mvt(expected, layer='places')
    assert decoded.features.ids == (0, 7)
    assert decoded.features.column('name')[:] == ('first', 'second')


def test_shared_arrow_features_and_source_support_concurrent_tiles() -> None:
    pa = pytest.importorskip('pyarrow')
    features = gm.Features.from_columns(
        gm.points(range(32), range(32), crs=3857),
        columns={'name': pa.array([f'row-{i}' for i in range(32)])},
        ids=pa.array(range(32), type=pa.uint64()),
    )
    source = gm.MVTSource(features)
    tile = gm.Tile(x=1, y=1, zoom=1)
    barrier = Barrier(4)

    def run(_: int) -> None:
        barrier.wait()
        for _ in range(20):
            selected = features[[7, 3]]
            assert selected.to_arrow()['name'].to_pylist() == ['row-7', 'row-3']
            decoded = gm.from_mvt(
                gm.to_mvt(source, layer='points', tile=tile), layer='points'
            )
            assert decoded.features.ids == tuple(range(32))
            assert decoded.features.column('name')[:] == tuple(
                f'row-{i}' for i in range(32)
            )

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(run, range(4)))


def test_tile_iterator_snapshots_layer_mapping_and_can_close_before_advance() -> None:
    source = gm.MVTSource(gm.Features(gm.points([0], [0], crs=3857)))
    tile = gm.Tile(x=1, y=1, zoom=1)
    layers = {'original': source}
    tiles = gm.mvt_tiles(layers, tiles=[tile])
    layers.clear()
    _, payload = next(tiles)
    assert list(gm.from_mvt(payload)) == ['original']
    tiles.close()
    unadvanced = gm.mvt_tiles({'original': source}, tiles=[tile])
    unadvanced.close()


def test_automatic_tile_traversal_emits_only_nonempty_tiles_in_order() -> None:
    points = gm.points([-10_000_000, 10_000_000], [-10_000_000, 10_000_000], crs=3857)
    source = gm.MVTSource(gm.Features(points), buffer=0)
    tiles = list(gm.mvt_tiles({'points': source}, min_zoom=6, max_zoom=6))
    addresses = [(tile.zoom, tile.x, tile.y) for tile, _ in tiles]
    assert len(addresses) == 2
    assert addresses == sorted(addresses)
    assert all(
        len(gm.from_mvt(payload, layer='points').features) for _, payload in tiles
    )


def test_xyz_wrap_keeps_one_feature_for_periodic_images() -> None:
    world = math.pi * 6_378_137
    features = gm.Features(
        [gm.Point(world - 10_000, 0, crs=3857)],
        properties=[{'name': 'seam'}],
        ids=[7],
    )
    west = gm.Tile(x=0, y=0, zoom=1)
    layer = gm.mvt_prepare(features, tile=west, extent=4096, buffer=64)
    assert layer.features.ids == (7,)
    assert layer.features[0].properties == {'name': 'seam'}
    assert -64 <= layer.features.geometries[0].x < 0


def test_large_buffer_assembles_periodic_points_in_one_feature() -> None:
    features = gm.Features([gm.Point(0, 0, crs=3857)], ids=[7])
    world = gm.Tile(x=0, y=0, zoom=0)
    layer = gm.mvt_prepare(features, tile=world, extent=4096, buffer=4096)
    assert layer.features.ids == (7,)
    assert layer.features.geometries[0].to_wkt() == (
        'MULTIPOINT ((-2048 2048), (2048 2048), (6144 2048))'
    )
