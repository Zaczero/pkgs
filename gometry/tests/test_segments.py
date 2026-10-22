"""Stored vertex pairs, without implicit connections across geometry boundaries."""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest


def test_segments_keep_closure_holes_parts_and_zero_length_edges() -> None:
    source = gm.from_wkt(
        'GEOMETRYCOLLECTION ('
        'LINESTRING (7 8, 7 8, 9 10), '
        'POLYGON ((0 0, 4 0, 4 4, 0 0), (1 1, 2 1, 2 2, 1 1)), '
        'MULTILINESTRING ((20 21, 22 23), (30 31, 32 33)), POINT (90 91))'
    )
    assert source.segments().to_wkt() == [
        'LINESTRING (7 8, 7 8)',
        'LINESTRING (7 8, 9 10)',
        'LINESTRING (0 0, 4 0)',
        'LINESTRING (4 0, 4 4)',
        'LINESTRING (4 4, 0 0)',
        'LINESTRING (1 1, 2 1)',
        'LINESTRING (2 1, 2 2)',
        'LINESTRING (2 2, 1 1)',
        'LINESTRING (20 21, 22 23)',
        'LINESTRING (30 31, 32 33)',
    ]


@pytest.mark.parametrize(
    'wkt', ['POINT (1 2)', 'MULTIPOINT ((1 2), (3 4))', 'POLYGON EMPTY']
)
def test_puntal_and_empty_geometries_have_no_segments(wkt: str) -> None:
    assert len(gm.from_wkt(wkt).segments()) == 0


def test_segments_preserve_all_ordinates_and_frame() -> None:
    source = gm.from_wkt('LINESTRING ZM (1 2 3 4, 5 6 7 8, 9 10 11 12)', crs=4326)
    segments = source.segments()
    assert segments.crs == source.crs
    assert segments.to_wkt() == [
        'LINESTRING ZM (1 2 3 4, 5 6 7 8)',
        'LINESTRING ZM (5 6 7 8, 9 10 11 12)',
    ]


def test_array_segments_preserve_nullable_selected_row_alignment() -> None:
    source = gm.GeometryArray([
        gm.LineString([(1, 2), (3, 4), (5, 6)]),
        None,
        gm.Point(8, 9),
        gm.from_wkt('LINESTRING EMPTY'),
    ])[::-1]
    groups = source.segments()
    np.testing.assert_array_equal(groups.offsets, [0, 0, 0, 0, 2])
    np.testing.assert_array_equal(groups.is_missing, [False, False, True, False])
    assert groups[2] is None
    assert groups[3].to_wkt() == ['LINESTRING (1 2, 3 4)', 'LINESTRING (3 4, 5 6)']


def test_empty_segments_retain_declared_ordinate_layout() -> None:
    source = gm.from_wkt('LINESTRING ZM EMPTY', crs=4326)
    assert source.segments().common_coordinate_axes == 'XYZM'
    groups = gm.GeometryArray([None, source]).segments()
    assert groups.values.common_coordinate_axes == 'XYZM'
    assert groups[1].common_coordinate_axes == 'XYZM'
