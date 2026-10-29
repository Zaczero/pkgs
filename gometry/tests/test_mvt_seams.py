"""Independent shared-boundary witnesses for polygon tile preparation."""

import gometry as gm
import pytest


@pytest.mark.parametrize(
    ('wkt', 'expected'),
    [
        (
            'POLYGON ((-5 15.8,40 8.2,40 2,-5 2,-5 15.8))',
            [4, 14],
        ),
        (
            'POLYGON ((-5 15.8,40 8.2,40 2,-5 2,-5 15.8),(14 8,14 6,18 6,18 8,14 8))',
            [4, 8, 10, 14],
        ),
        (
            'POLYGON ((10 10,22 10,16.49 -0.49,10 10))',
            [6, 16],
        ),
    ],
)
@pytest.mark.parametrize('buffer', [0, 1, 2])
def test_adjacent_polygon_tiles_share_visible_boundary_trace(
    wkt: str, expected: list[int], buffer: int
) -> None:
    source = gm.Features([gm.from_wkt(wkt)], properties=[{'kind': 'polygon'}], ids=[7])
    frames = (
        gm.TileFrame((0, 0, 16, 16), crs=None),
        gm.TileFrame((16, 0, 32, 16), crs=None),
    )
    for frame, edge_x in zip(frames, (16, 0), strict=True):
        layer = gm.mvt_prepare(source, frame=frame, extent=16, buffer=buffer)
        assert layer.features.ids == (7,)
        shape = layer.features.geometries[0]
        assert shape.is_valid
        edge = gm.LineString([(edge_x, 0), (edge_x, 16)])
        intersection = gm.intersection(shape, edge)
        assert intersection.length == sum(
            end - start
            for start, end in zip(expected[::2], expected[1::2], strict=True)
        )
        assert sorted(y for _, y in intersection.coords) == expected
