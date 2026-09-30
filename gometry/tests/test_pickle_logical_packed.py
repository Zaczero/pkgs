from __future__ import annotations

import pickle
import struct

import gometry as gm
import pytest

GEOMETRIES = {
    'point': 'POINT {tag}({coords})',
    'line': 'LINESTRING {tag}({coords}, {coords2})',
    'polygon': 'POLYGON {tag}(({coords}, {coords2}, {coords3}, {coords}))',
}

AXES = {
    'XY': ('', '{x} 2', '{x2} 4', '{x3} 6'),
    'XYZ': ('Z', '{x} 2 11', '{x2} 4 12', '{x3} 6 13'),
    'XYM': ('M', '{x} 2 21', '{x2} 4 22', '{x3} 6 23'),
    'XYZM': ('ZM', '{x} 2 11 21', '{x2} 4 12 22', '{x3} 6 13 23'),
}


def _packed_rows(kind: str, axes: str, count: int = 4) -> gm.GeometryArray:
    tag, coords, coords2, coords3 = AXES[axes]
    text = GEOMETRIES[kind].format(
        tag=tag, coords=coords, coords2=coords2, coords3=coords3
    )
    rows = gm.from_wkt([
        text.format(x=10 * row + 1, x2=10 * row + 3, x3=10 * row + 5)
        for row in range(count)
    ])
    callable_, args = rows.__reduce_ex__(5)
    assert callable_.__name__ == '_unpickle_packed_array_v1'
    assert len(args[4]) == {'point': 0, 'line': 1, 'polygon': 2}[kind]
    return rows


@pytest.mark.parametrize('kind', GEOMETRIES)
@pytest.mark.parametrize('axes', AXES)
def test_packed_pickles_preserve_selected_row_order_and_axes(
    kind: str, axes: str
) -> None:
    base = _packed_rows(kind, axes)
    selections = [
        base,
        base[1:3],
        base[[3, 1, 1, 0]],
        base[[3, 1, 0]][[2, 0, 0]],
        base[[]],
    ]

    for selected in selections:
        restored = pickle.loads(pickle.dumps(selected, protocol=5))
        assert restored.to_wkt() == selected.to_wkt()
        assert restored.coordinate_axes == selected.coordinate_axes


@pytest.mark.parametrize('kind', GEOMETRIES)
def test_packed_pickle_size_tracks_selection_not_parent(kind: str) -> None:
    small = _packed_rows(kind, 'XY', 1)
    large = _packed_rows(kind, 'XY', 64)
    selected = large[[63]]

    small_payload = pickle.dumps(small, protocol=5)
    selected_payload = pickle.dumps(selected, protocol=5)
    assert len(selected_payload) == len(small_payload)


@pytest.mark.parametrize(
    ('rows', 'selected_wkt'),
    [
        (
            ['POINT (1234567.25 8)', 'POINT (1 2)'],
            'POINT (1 2)',
        ),
        (
            [
                'LINESTRING (1234567.25 8, 9 10)',
                'LINESTRING (1 2, 3 4)',
            ],
            'LINESTRING (1 2, 3 4)',
        ),
        (
            [
                'POLYGON ((1234567.25 8, 9 8, 9 10, 1234567.25 8))',
                'POLYGON ((1 2, 3 2, 3 4, 1 2))',
            ],
            'POLYGON ((1 2, 3 2, 3 4, 1 2))',
        ),
    ],
)
def test_packed_payload_omits_unselected_parent_coordinates(
    rows: list[str], selected_wkt: str
) -> None:
    selected = gm.from_wkt(rows)[[1]]
    _, args = selected.__reduce_ex__(5)
    coordinate_payload = b''.join(value for value in args[:4] if value is not None)

    assert struct.pack('<d', 1234567.25) not in coordinate_payload
    assert selected.to_wkt() == [selected_wkt]


@pytest.mark.parametrize(
    'rows',
    [
        [gm.Point(0, 0), None, gm.Point(2, 2)],
        [
            gm.LineString([(0, 0), (1, 1)]),
            None,
            gm.LineString([(2, 2), (3, 3)]),
        ],
        [gm.box(0, 0, 1, 1), None, gm.box(2, 2, 3, 3)],
    ],
)
def test_nullable_packed_pickle_keeps_present_payload_and_logical_mask(
    rows: list[object],
) -> None:
    values = gm.GeometryArray(rows, crs=4326).set_epoch(2020.0)
    _, args = values.__reduce_ex__(5)
    assert args[-1] == b'\x00\x01\x00'
    restored = pickle.loads(pickle.dumps(values, protocol=5))
    assert restored.to_wkt(drop_epoch=True) == values.to_wkt(drop_epoch=True)
    assert restored.is_missing.tolist() == [False, True, False]
    assert restored.crs == values.crs
    assert restored.epoch == values.epoch
    assert (
        restored.to_arrow().type.extension_name == values.to_arrow().type.extension_name
    )


@pytest.mark.parametrize(
    'base',
    [
        gm.points([0, 1], [2, 3]),
        gm.GeometryArray([
            gm.LineString([(0, 0), (1, 1)]),
            gm.LineString([(2, 2), (3, 3)]),
        ]),
        gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]),
    ],
)
def test_all_missing_compact_pickle_preserves_declared_native_kind(
    base: gm.GeometryArray,
) -> None:
    values = base._with_missing([True, True])
    restored = pickle.loads(pickle.dumps(values, protocol=5))
    assert restored.to_wkt() == [None, None]
    assert (
        restored.to_arrow().type.extension_name == values.to_arrow().type.extension_name
    )
    assert restored.coordinate_axes == values.coordinate_axes
