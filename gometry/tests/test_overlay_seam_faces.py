"""Thin antimeridian faces keep their source-side attribution."""

from __future__ import annotations

import pytest

import gometry as gm


def _thin_seam_polygon(west: float, east: float, height: float) -> gm.Polygon:
    return gm.Polygon(
        [
            (west, 0.0),
            (east, 0.0),
            (east, height),
            (west, height),
            (west, 0.0),
        ],
        crs=4326,
    )


@pytest.mark.parametrize(
    ("west", "east", "height"),
    [
        pytest.param(179.0, -179.0, 1e-8, id="two-degree-below-probe-step"),
        pytest.param(179.0, -179.0, 1e-9, id="two-degree-well-below-probe-step"),
        pytest.param(170.0, -170.0, 1e-6, id="twenty-degree-at-probe-step"),
        pytest.param(170.0, -170.0, 1e-7, id="twenty-degree-at-half-step"),
        pytest.param(170.0, -170.0, 1e-8, id="twenty-degree-below-probe-step"),
    ],
)
def test_thin_seam_faces_conserve_area_below_probe_step(
    west: float, east: float, height: float
) -> None:
    polygon = _thin_seam_polygon(west, east, height)
    collection = gm.GeometryCollection([polygon], crs=4326)

    assert collection.area == pytest.approx(polygon.area, rel=1e-4)


def test_thin_seam_union_keeps_exclusions_and_disjoint_area() -> None:
    thin = _thin_seam_polygon(170.0, -170.0, 1e-6)
    far = gm.box(0.0, 40.0, 1.0, 41.0, crs=4326)
    union = gm.union_all([thin, far])

    assert union.area - far.area == pytest.approx(thin.area, rel=1e-4)
    assert not gm.contains(union, gm.Point(0.5, 0.5, crs=4326))

    holed = gm.Polygon(
        [
            (170.0, -1.0),
            (-170.0, -1.0),
            (-170.0, 1.0),
            (170.0, 1.0),
            (170.0, -1.0),
        ],
        holes=[
            [
                (175.0, -0.5),
                (-175.0, -0.5),
                (-175.0, 0.5),
                (175.0, 0.5),
                (175.0, -0.5),
            ]
        ],
        crs=4326,
    )
    holed_union = gm.union_all([holed])
    assert gm.contains(holed_union, gm.Point(179.0, 0.75, crs=4326))
    assert not gm.contains(holed_union, gm.Point(179.0, 0.0, crs=4326))

    degenerate = gm.Polygon(
        [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0), (1.0, 0.0), (0.0, 0.0)],
        crs=4326,
    )
    assert gm.union_all([degenerate]).area == 0.0

    ordinary = gm.Polygon(
        [(0.0, 0.0), (2.0, 0.0), (2.0, 1e-8), (0.0, 1e-8), (0.0, 0.0)],
        crs=4326,
    )
    assert gm.GeometryCollection([ordinary], crs=4326).area == pytest.approx(ordinary.area)
