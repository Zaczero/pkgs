"""Public Voronoi polygon output for many shell-tangent clip holes."""

from __future__ import annotations

import gometry as gm
import pytest


@pytest.mark.parametrize(
    ('hole_count', 'polygon_count', 'output_hole_count'),
    [
        (667, 4, 666),
        (1334, 5, 1332),
    ],
    ids=['667-tangent-holes', '1334-tangent-holes'],
)
def test_noncollinear_tangent_hole_clip_preserves_public_output(
    hole_count: int, polygon_count: int, output_hole_count: int
) -> None:
    holes = []
    for index in range(hole_count):
        x = 1.0 + index * 1.4
        holes.append([(x, 10.0), (x + 0.25, 9.5), (x - 0.25, 9.5), (x, 10.0)])
    clip = gm.Polygon(
        [(0.0, 0.0), (2000.0, 0.0), (2000.0, 10.0), (0.0, 10.0), (0.0, 0.0)],
        holes=holes,
    )
    sites = gm.MultiPoint([(500.0, 5.0), (1500.0, 5.0), (1000.0, 7.0)])

    cells = sites.voronoi_polygons(clip=clip)

    assert len(cells) == polygon_count
    assert all(cell.is_valid for cell in cells)
    assert sum(len(cell.interiors) for cell in cells) == output_hole_count
    assert sum(cell.area for cell in cells) == pytest.approx(clip.area, abs=1e-9)
