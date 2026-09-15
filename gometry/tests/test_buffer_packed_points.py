"""Packed point-buffer behavior and fallback counter-cases."""

import gometry as gm
import numpy as np
import pytest


@pytest.mark.parametrize('cap_style', ['round', 'flat', 'square'])
@pytest.mark.parametrize('join_style', ['round', 'miter', 'bevel'])
def test_packed_point_buffer_matches_scalar_and_preserves_frame(
    cap_style: str, join_style: str
) -> None:
    points = gm.GeometryArray(
        [gm.Point(0.0, 0.0), gm.Point(10.0, 20.0), gm.Point(-4.0, 8.0)],
        crs=3857,
        epoch=2020.5,
    )
    buffered = points.buffer(
        2.0,
        cap_style=cap_style,
        join_style=join_style,
        quadrant_segments=4,
    )

    assert buffered.crs == 'EPSG:3857'
    assert buffered.epoch == pytest.approx(2020.5)
    assert [row.to_wkt(drop_epoch=True) for row in buffered] == [
        point.buffer(
            2.0,
            cap_style=cap_style,
            join_style=join_style,
            quadrant_segments=4,
        ).to_wkt(drop_epoch=True)
        for point in points
    ]


def test_packed_point_buffer_preserves_missing_and_empty_rows() -> None:
    points = gm.GeometryArray(
        [gm.Point(0.0, 0.0), None, gm.Point(2.0, 3.0)],
        crs=3857,
        epoch=2020.5,
    )
    buffered = points.buffer(1.0, quadrant_segments=2)

    assert buffered.is_missing.tolist() == [False, True, False]
    assert buffered.to_wkt(drop_epoch=True)[1] is None
    np.testing.assert_allclose(
        [buffered[0].area, buffered[2].area],
        [
            points[0].buffer(1.0, quadrant_segments=2).area,
            points[2].buffer(1.0, quadrant_segments=2).area,
        ],
    )

    empty = gm.GeometryArray(
        [gm.Point(0.0, 0.0), gm.from_wkt('POINT EMPTY')],
        crs=3857,
    ).buffer(1.0, quadrant_segments=2)
    assert empty[1].is_empty


def test_buffer_point_fast_path_does_not_drop_polygon_holes() -> None:
    holed = gm.from_wkt(
        'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 3 7, 7 7, 7 3, 3 3))',
        crs=3857,
    )
    result = gm.GeometryArray([holed]).buffer(1.0, quadrant_segments=4)

    assert result[0].area == pytest.approx(holed.buffer(1.0, quadrant_segments=4).area)
