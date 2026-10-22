"""Per-sequence mapping admits variable vertex counts without flattening topology."""

from __future__ import annotations

import operator

import gometry as gm
import numpy as np
import pytest


@pytest.mark.parametrize(
    'wkt',
    [
        'LINESTRING (0 0, 1 2, 3 4)',
        'LINESTRING Z (0 0 1, 1 2 3, 3 4 5)',
        'LINESTRING M (0 0 10, 1 2 20, 3 4 30)',
        'LINESTRING ZM (0 0 1 10, 1 2 3 20, 3 4 5 30)',
    ],
)
def test_sequence_mapping_changes_count_preserving_schema_and_frame(wkt: str) -> None:
    source = gm.from_wkt(wkt, crs=4326)
    result = source.map_coordinate_sequences(operator.itemgetter(slice(None, None, 2)))
    assert len(result.coords) == 2
    assert result.coordinate_axes == source.coordinate_axes
    assert result.crs == source.crs
    np.testing.assert_array_equal(
        np.asarray(result.coords), np.asarray(source.coords)[::2]
    )


def test_sequence_callback_arrays_are_readonly_owned_snapshots() -> None:
    saved: list[np.ndarray] = []

    def retain(coords: np.ndarray) -> np.ndarray:
        assert not coords.flags.writeable
        saved.append(coords)
        return coords[::-1]

    source = gm.LineString([(3, 4), (5, 6), (7, 8)])
    result = source.map_coordinate_sequences(retain)
    del source
    np.testing.assert_array_equal(saved[0], [[3, 4], [5, 6], [7, 8]])
    assert result.to_wkt() == 'LINESTRING (7 8, 5 6, 3 4)'


def test_sequence_mapping_preserves_rings_parts_and_empty_members() -> None:
    source = gm.from_wkt(
        'GEOMETRYCOLLECTION ('
        'MULTILINESTRING (EMPTY, (0 0, 1 1, 2 2)), '
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 1)))'
    )
    seen: list[int] = []

    def resize(coords: np.ndarray) -> np.ndarray:
        seen.append(len(coords))
        return coords if len(coords) != 3 else coords[[0, -1]]

    result = source.map_coordinate_sequences(resize)
    assert seen == [3, 5, 4]
    assert result.to_wkt() == (
        'GEOMETRYCOLLECTION (MULTILINESTRING (EMPTY, (0 0, 2 2)), '
        'POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 1)))'
    )


@pytest.mark.parametrize(
    ('wkt', 'expected'),
    [
        ('POINT ZM (1 2 3 4)', 'POINT ZM EMPTY'),
        ('LINESTRING M (1 2 3, 4 5 6)', 'LINESTRING M EMPTY'),
        ('POLYGON ((0 0, 4 0, 4 4, 0 0))', 'POLYGON EMPTY'),
        ('MULTILINESTRING (EMPTY, (1 2, 3 4))', 'MULTILINESTRING (EMPTY, EMPTY)'),
        ('MULTIPOINT (EMPTY, (1 2), (3 4))', 'MULTIPOINT EMPTY'),
    ],
)
def test_sequence_mapping_can_empty_components(wkt: str, expected: str) -> None:
    result = gm.from_wkt(wkt).map_coordinate_sequences(operator.itemgetter(slice(0)))
    assert result.to_wkt() == expected


def test_sequence_mapping_admits_open_ring_and_drops_empty_hole() -> None:
    source = gm.from_wkt('POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0), (1 1, 2 1, 2 2, 1 1))')
    result = source.map_coordinate_sequences(
        lambda coords: coords[:3] if len(coords) == 5 else coords[:0]
    )
    assert result.to_wkt() == 'POLYGON ((0 0, 5 0, 5 5, 0 0))'


def test_sequence_mapping_resizes_multipoint_as_one_coordinate_sequence() -> None:
    source = gm.from_wkt('MULTIPOINT M (EMPTY, (1 2 3), (4 5 6), (7 8 9))')
    seen: list[tuple[int, ...]] = []

    def resize(coords: np.ndarray) -> np.ndarray:
        seen.append(coords.shape)
        return coords[::2]

    assert (
        source.map_coordinate_sequences(resize).to_wkt()
        == 'MULTIPOINT M ((1 2 3), (7 8 9))'
    )
    assert seen == [(3, 3)]


@pytest.mark.parametrize(
    ('wkt', 'callback', 'message'),
    [
        ('LINESTRING (0 0, 1 1)', operator.itemgetter(slice(1)), 'at least two'),
        ('POLYGON ((0 0, 2 0, 2 2, 0 0))', operator.itemgetter(slice(2)), 'ring'),
        ('POINT (1 2)', lambda coords: np.repeat(coords, 2, axis=0), 'zero or one'),
        (
            'LINESTRING Z (0 0 1, 1 1 2)',
            operator.itemgetter((slice(None), slice(2))),
            'width 3',
        ),
        ('LINESTRING (0 0, 1 1)', lambda coords: coords * np.nan, 'finite'),
    ],
)
def test_sequence_mapping_rejects_domain_errors(wkt, callback, message) -> None:
    with pytest.raises(gm.InvalidGeometryError, match=message):
        gm.from_wkt(wkt).map_coordinate_sequences(callback)


def test_sequence_mapping_does_not_discard_holes_with_empty_shell() -> None:
    source = gm.from_wkt('POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0), (1 1, 2 1, 2 2, 1 1))')
    with pytest.raises(gm.InvalidGeometryError, match='empty shell'):
        source.map_coordinate_sequences(
            lambda coords: coords[:0] if len(coords) == 5 else coords
        )


def test_sequence_mapping_skips_missing_empty_and_keeps_native_member_axes() -> None:
    source = gm.GeometryArray([
        None,
        gm.from_wkt('LINESTRING EMPTY'),
        gm.from_wkt('LINESTRING M (1 2 3, 4 5 6)'),
        gm.Point(7, 8),
    ])
    seen: list[tuple[int, ...]] = []

    def record(coords: np.ndarray) -> np.ndarray:
        seen.append(coords.shape)
        return coords + 1

    result = source.map_coordinate_sequences(record)
    assert seen == [(2, 3), (1, 2)]
    assert result.to_wkt() == [
        None,
        'LINESTRING EMPTY',
        'LINESTRING M (2 3 4, 5 6 7)',
        'POINT (8 9)',
    ]


def test_sequence_mapping_preserves_callback_exception_and_adds_row_note() -> None:
    source = gm.GeometryArray([None, gm.LineString([(0, 0), (1, 1)])])
    failure = LookupError('callback failed')

    def fail(_coords: np.ndarray) -> np.ndarray:
        raise failure

    with pytest.raises(LookupError, match='callback failed') as caught:
        source.map_coordinate_sequences(fail)
    assert caught.value is failure
    assert any('1' in note for note in caught.value.__notes__)


def test_all_missing_sequence_mapping_retains_declared_schema_without_callback() -> (
    None
):
    source = gm.GeometryArray([gm.Point(1, 2, m=3, crs=4326), None])[1:]

    def fail(_coords: np.ndarray) -> np.ndarray:
        pytest.fail('missing rows must not invoke the callback')

    result = source.map_coordinate_sequences(fail)
    assert result.to_wkt() == [None]
    assert result.common_coordinate_axes == source.common_coordinate_axes == 'XYM'
    assert result.crs == source.crs
