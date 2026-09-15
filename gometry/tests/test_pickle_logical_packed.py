from __future__ import annotations

import pickle
import struct

import gometry as gm
import pytest
from gometry import _lib

GEOMETRIES = {
    'point': 'POINT {tag}({coords})',
    'line': 'LINESTRING {tag}({coords}, {coords2})',
    'polygon': 'POLYGON {tag}(({coords}, {coords2}, {coords3}, {coords}))',
}

AXES = {
    'XY': ('', '1 2', '3 4', '5 6'),
    'XYZ': ('Z', '1 2 11', '3 4 12', '5 6 13'),
    'XYM': ('M', '1 2 21', '3 4 22', '5 6 23'),
    'XYZM': ('ZM', '1 2 11 21', '3 4 12 22', '5 6 13 23'),
}


def _packed_rows(kind: str, axes: str, count: int = 4) -> gm.GeometryArray:
    tag, coords, coords2, coords3 = AXES[axes]
    text = GEOMETRIES[kind].format(
        tag=tag, coords=coords, coords2=coords2, coords3=coords3
    )
    rows = gm.from_wkt([text] * count)
    assert rows.__reduce_ex__(5)[0].__name__ == f'_unpickle_{kind}_array'
    return rows


@pytest.mark.parametrize('kind', GEOMETRIES)
@pytest.mark.parametrize('axes', AXES)
def test_new_packed_pickles_store_logical_rows_without_row_map(
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
        _, args = selected.__reduce_ex__(5)
        assert args[-2] is None
        restored = pickle.loads(pickle.dumps(selected, protocol=5))
        assert restored.to_wkt(output_dimension=4) == selected.to_wkt(
            output_dimension=4
        )
        assert restored.coordinate_axes == selected.coordinate_axes


@pytest.mark.parametrize('kind', GEOMETRIES)
def test_new_packed_pickle_size_tracks_selection_not_parent(kind: str) -> None:
    small = _packed_rows(kind, 'XY', 1)
    large = _packed_rows(kind, 'XY', 20_000)
    selected = large[[19_999]]

    small_payload = pickle.dumps(small, protocol=5)
    selected_payload = pickle.dumps(selected, protocol=5)
    assert len(selected_payload) <= len(small_payload) + 32
    assert len(selected_payload) < len(pickle.dumps(large, protocol=5)) // 1_000
    assert selected.__reduce_ex__(5)[1][-2] is None


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
def test_new_packed_payload_omits_unselected_parent_coordinates(
    rows: list[str], selected_wkt: str
) -> None:
    selected = gm.from_wkt(rows)[[1]]
    _, args = selected.__reduce_ex__(5)
    coordinate_payload = b''.join(value for value in args[:4] if value is not None)

    assert struct.pack('<d', 1234567.25) not in coordinate_payload
    assert selected.to_wkt() == [selected_wkt]
    assert args[-2] is None


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
    assert args[-2] is None
    assert args[-1] == b'\x00\x01\x00'
    restored = pickle.loads(pickle.dumps(values, protocol=5))
    assert restored.to_wkt(drop_epoch=True) == values.to_wkt(drop_epoch=True)
    assert restored.is_missing.tolist() == [False, True, False]
    assert restored.crs == values.crs
    assert restored.epoch == values.epoch


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


def test_old_row_map_payloads_remain_readable_and_validated() -> None:
    doubles = lambda values: struct.pack(f'<{len(values)}d', *values)
    offsets = lambda values: struct.pack(f'<{len(values)}i', *values)
    row_map = struct.pack('<2Q', 2, 0)

    point = _lib._unpickle_point_array(
        doubles([1, 2, 3]), doubles([11, 12, 13]), None, None, None, None, row_map, None
    )
    line = _lib._unpickle_line_array(
        doubles([0, 1, 2, 3, 4, 5]),
        doubles([10, 11, 12, 13, 14, 15]),
        None,
        None,
        offsets([0, 2, 4, 6]),
        None,
        None,
        row_map,
        None,
    )
    polygon = _lib._unpickle_polygon_array(
        doubles([0, 1, 0, 0, 2, 3, 2, 2, 4, 5, 4, 4]),
        doubles([0, 0, 1, 0, 2, 2, 3, 2, 4, 4, 5, 4]),
        None,
        None,
        offsets([0, 4, 8, 12]),
        offsets([0, 1, 2, 3]),
        None,
        None,
        row_map,
        None,
    )

    assert point.to_wkt() == ['POINT (3 13)', 'POINT (1 11)']
    assert line.to_wkt() == ['LINESTRING (4 14, 5 15)', 'LINESTRING (0 10, 1 11)']
    assert polygon.to_wkt() == [
        'POLYGON ((4 4, 5 4, 4 5, 4 4))',
        'POLYGON ((0 0, 1 0, 0 1, 0 0))',
    ]

    invalid = struct.pack('<Q', 3)
    with pytest.raises(
        gm.GeometryError, match='row_map references a nonexistent physical row'
    ) as excinfo:
        _lib._unpickle_point_array(
            doubles([1, 2, 3]),
            doubles([11, 12, 13]),
            None,
            None,
            None,
            None,
            invalid,
            None,
        )
    assert type(excinfo.value) is gm.GeometryError
    assert (
        str(excinfo.value)
        == 'point-array pickle row_map references a nonexistent physical row'
    )
    with pytest.raises(
        gm.GeometryError, match='row_map references a nonexistent physical row'
    ) as excinfo:
        _lib._unpickle_line_array(
            doubles([0, 1, 2, 3, 4, 5]),
            doubles([10, 11, 12, 13, 14, 15]),
            None,
            None,
            offsets([0, 2, 4, 6]),
            None,
            None,
            invalid,
            None,
        )
    assert type(excinfo.value) is gm.GeometryError
    with pytest.raises(
        gm.GeometryError, match='row_map references a nonexistent physical row'
    ) as excinfo:
        _lib._unpickle_polygon_array(
            doubles([0, 1, 0, 0, 2, 3, 2, 2, 4, 5, 4, 4]),
            doubles([0, 0, 1, 0, 2, 2, 3, 2, 4, 4, 5, 4]),
            None,
            None,
            offsets([0, 4, 8, 12]),
            offsets([0, 1, 2, 3]),
            None,
            None,
            invalid,
            None,
        )
    assert type(excinfo.value) is gm.GeometryError
