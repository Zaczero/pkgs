"""Coverage tiles keep feature ownership and shared grid events."""

import gometry as gm
import pytest


def test_coverage_preparation_routes_shared_border_jointly() -> None:
    geometries = gm.GeometryArray([
        gm.from_wkt('POLYGON ((0 8,8 8,8 4,0 8))'),
        gm.from_wkt('POLYGON ((0 8,8 4,0 4,0 8),(3 6.4,3 5.5,4 5.5,3 6.4))'),
    ])
    assert gm.coverage_is_valid(geometries)
    features = gm.Features(
        geometries, properties=[{'owner': 'a'}, {'owner': 'b'}], ids=[0, 1]
    )
    source = gm.MVTSource(features, crs=None, coverage=True, extent=8, buffer=0)
    frame = gm.TileFrame((0, 0, 8, 8), crs=None)
    layer = gm.mvt_prepare(source, frame=frame)
    assert layer.features.ids == (0, 1)
    assert layer.features.column('owner')[:] == ('a', 'b')
    assert gm.union_all(layer.features.geometries).area == 31.5
    assert (
        gm.intersection(layer.features.geometries[0], layer.features.geometries[1]).area
        == 0
    )


@pytest.mark.parametrize('method', ['vw', 'dp'])
def test_coverage_simplification_replays_one_shared_chain(method: str) -> None:
    left = gm.Polygon([
        (0, 0),
        (8, 0),
        (9, 4),
        (8, 8),
        (9, 12),
        (8, 16),
        (0, 16),
        (0, 0),
    ])
    right = gm.Polygon([
        (8, 0),
        (16, 0),
        (16, 16),
        (8, 16),
        (9, 12),
        (8, 8),
        (9, 4),
        (8, 0),
    ])
    geometries = gm.GeometryArray([left, right])
    assert gm.coverage_is_valid(geometries)
    frame = gm.TileFrame((0, 0, 16, 16), crs=None)
    source = gm.MVTSource(
        gm.Features(geometries, ids=[0, 1]),
        crs=None,
        coverage=True,
        extent=16,
        buffer=0,
        tolerance=0,
    )
    baseline = gm.mvt_prepare(source, frame=frame).features.geometries
    simplified = gm.mvt_prepare(
        source.with_options(tolerance=4, method=method), frame=frame
    ).features.geometries
    assert gm.coverage_is_valid(simplified)
    assert gm.union_all(simplified).area == 256
    assert sum(simplified.num_coordinates) < sum(baseline.num_coordinates)
