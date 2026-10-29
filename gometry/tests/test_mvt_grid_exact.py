"""Grid clipping keeps only published, representable coordinates."""

import math

import gometry as gm


def test_point_clip_uses_half_open_rounding_edges() -> None:
    xs = [
        math.nextafter(-1.5, -math.inf),
        -1.5,
        math.nextafter(-1.5, math.inf),
        math.nextafter(5.5, -math.inf),
        5.5,
        math.nextafter(5.5, math.inf),
    ]
    result = gm.mvt_prepare(
        gm.Features(gm.points(xs, -2), ids=range(6)),
        frame=gm.TileFrame((0, -4, 4, 0), crs=None),
        extent=4,
        buffer=1,
    )
    assert result.features.ids == (1, 2, 3)
    assert [feature.geometry.x for feature in result.features] == [-1, -1, 5]
    assert [feature.geometry.y for feature in result.features] == [2, 2, 2]


def test_excluded_extreme_component_does_not_hide_surviving_polygon() -> None:
    boundary = 1 << 54
    outside = gm.Polygon([
        (boundary - 2, -boundary - 4),
        (boundary + 4, -boundary + 2),
        (boundary + 4, -boundary - 4),
        (boundary - 2, -boundary - 4),
    ])
    inside = gm.Polygon([(0, -1), (2, -1), (2, -3), (0, -3), (0, -1)])
    features = gm.Features([gm.MultiPolygon([inside, outside])], ids=[9])
    result = gm.mvt_prepare(
        features,
        frame=gm.TileFrame((0, -1, 1, 0), crs=None),
        extent=1,
        buffer=boundary,
    )
    assert result.features.ids == (9,)
    assert result.features.geometries[0].is_valid
    assert result.features.geometries[0].area == 4


def test_buffer_larger_than_machine_integer_preserves_nearby_polygon() -> None:
    polygon = gm.Polygon([(0, -1), (2, -1), (2, -3), (0, -3), (0, -1)])
    result = gm.mvt_prepare(
        gm.Features([polygon]),
        frame=gm.TileFrame((0, -1, 1, 0), crs=None),
        extent=1,
        buffer=1 << 100,
    )
    assert result.features.geometries[0].area == 4


def test_excluded_component_can_cross_machine_integer_grid_range() -> None:
    step = 2.0**-60
    boundary = 16.0
    delta = math.ulp(boundary)
    outside = gm.Polygon([
        (boundary - delta, -boundary - 2 * delta),
        (boundary + 2 * delta, -boundary + delta),
        (boundary + 2 * delta, -boundary - 2 * delta),
        (boundary - delta, -boundary - 2 * delta),
    ])
    inside = gm.Polygon([(0, 0), (step, 0), (step, -step), (0, -step), (0, 0)])
    result = gm.mvt_prepare(
        gm.Features([gm.MultiPolygon([inside, outside])], ids=[9]),
        frame=gm.TileFrame((0, -step, step, 0), crs=None),
        extent=1,
        buffer=1 << 64,
    )
    assert result.features.ids == (9,)
    assert result.features.geometries[0].area == 1
