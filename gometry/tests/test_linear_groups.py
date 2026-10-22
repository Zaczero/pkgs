"""Explicit ragged stations use the shared Groups container."""

from __future__ import annotations

import pickle

import gometry as gm
import numpy as np
import pytest


def test_numeric_groups_copy_rows_and_preserve_csr_selection_and_pickle() -> None:
    row = np.array([0.25, 1.5, 2.75])
    groups = gm.Groups([row, None, [], [7.25, 8.5]])
    row[:] = 999
    np.testing.assert_array_equal(groups.values, [0.25, 1.5, 2.75, 7.25, 8.5])
    assert not groups.values.flags.writeable
    assert not groups[0].flags.writeable
    assert groups.to_list() == [[0.25, 1.5, 2.75], None, [], [7.25, 8.5]]
    selected = groups[::-1][1:]
    assert selected.to_list() == [[], None, [0.25, 1.5, 2.75]]
    np.testing.assert_array_equal(selected.offsets, [0, 0, 0, 3])
    assert pickle.loads(pickle.dumps(selected)) == selected
    assert [0.25, 1.5, 2.75] in selected
    assert selected.index(None) == 1


def test_array_routes_accept_distinct_explicit_station_groups() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (10, 0)]),
        gm.LineString([(20, 0), (24, 0)]),
    ])
    result = lines.line_interpolate(gm.Groups([[0, 3, 10], [1.5]]))
    assert result[0].to_wkt() == ['POINT (0 0)', 'POINT (3 0)', 'POINT (10 0)']
    assert result[1].to_wkt() == ['POINT (21.5 0)']


def test_ragged_stations_follow_nullable_gathered_routes_and_empty_alignment() -> None:
    routes = gm.GeometryArray([
        gm.LineString([(1, 0), (11, 0)]),
        None,
        gm.from_wkt('LINESTRING EMPTY'),
        gm.LineString([(20, 0), (30, 0)]),
        gm.LineString([(40, 0), (50, 0)]),
    ])[::-1]
    groups = routes.line_interpolate(gm.Groups([None, [], [1, 3], [2], [0, 10]]))
    np.testing.assert_array_equal(groups.is_missing, [True, False, False, True, False])
    np.testing.assert_array_equal(groups.offsets, [0, 0, 0, 2, 2, 4])
    assert groups[2].to_wkt() == ['POINT EMPTY', 'POINT EMPTY']
    assert groups[4].to_wkt() == ['POINT (1 0)', 'POINT (11 0)']


def test_ragged_normalized_stations_preserve_zm_and_frame() -> None:
    routes = gm.from_wkt(
        [
            'LINESTRING ZM (0 0 10 100, 10 0 20 200)',
            'LINESTRING ZM (20 0 30 300, 24 0 34 340)',
        ],
        crs=3857,
    )
    result = routes.line_interpolate(gm.Groups([[0.25, 0.75], [0.5]]), normalized=True)
    assert result.values.crs == routes.crs
    assert result[0].to_wkt() == [
        'POINT ZM (2.5 0 12.5 125)',
        'POINT ZM (7.5 0 17.5 175)',
    ]
    assert result[1].to_wkt() == ['POINT ZM (22 0 32 320)']


def test_ragged_m_stations_share_scalar_interpolation_rules() -> None:
    route = gm.from_wkt('LINESTRING ZM (0 0 10 100, 10 0 20 200)')
    result = gm.GeometryArray([route, route]).line_interpolate(
        gm.Groups([[125, 175], [150]]), basis='m'
    )
    assert result[0].to_wkt() == [
        'POINT ZM (2.5 0 12.5 125)',
        'POINT ZM (7.5 0 17.5 175)',
    ]
    assert result[1].to_wkt() == ['POINT ZM (5 0 15 150)']


def test_explicit_scalar_list_and_row_aligned_numeric_at_keep_their_meaning() -> None:
    route = gm.LineString([(0, 0), (10, 0)])
    assert route.line_interpolate([2, 7]).to_wkt() == ['POINT (2 0)', 'POINT (7 0)']
    assert gm.GeometryArray([route, route]).line_interpolate([2, 7]).to_wkt() == [
        'POINT (2 0)',
        'POINT (7 0)',
    ]


def test_ragged_station_validation_names_shape_and_row_errors() -> None:
    routes = gm.GeometryArray([gm.LineString([(0, 0), (10, 0)])])
    with pytest.raises(gm.GeometryError, match='1 groups'):
        routes.line_interpolate(gm.Groups([[], []]))
    with pytest.raises(gm.GeometryError, match='finite') as caught:
        routes.line_interpolate(gm.Groups([[float('nan')]]))
    assert any('0' in note for note in caught.value.__notes__)
    with pytest.raises(gm.GeometryError, match='normalized'):
        routes.line_interpolate(gm.Groups([[0.5]]), basis='m', normalized=True)
