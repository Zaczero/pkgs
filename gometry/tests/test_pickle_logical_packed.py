from __future__ import annotations

import pickle
import struct

import gometry as gm
import numpy as np
import pytest
from gometry import _lib

from tests._support import (
    _nullable_packed_linestring_arrow,
    _nullable_packed_polygon_arrow,
)

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


def test_pickle_empty_packed_selection_and_permutation() -> None:
    points = gm.points([0, 1, 2], [0, 1, 2])
    empty = points[[]]
    restored_empty = pickle.loads(pickle.dumps(empty))
    assert len(restored_empty) == 0
    assert restored_empty.to_wkt() == []

    permuted = points[[2, 0, 1]]
    restored = pickle.loads(pickle.dumps(permuted))
    assert restored.to_wkt() == permuted.to_wkt()

    sub = points[1:3]
    assert pickle.loads(pickle.dumps(sub)).to_wkt() == sub.to_wkt()


def test_packed_pickle_rejects_payloads_that_would_panic() -> None:
    """Packed rows are read through asserting constructors; corrupt payloads
    must fail at unpickle with a GeometryError instead.
    """
    doubles = lambda *values: struct.pack(f'<{len(values)}d', *values)
    offsets = lambda *values: struct.pack(f'<{len(values)}i', *values)
    square_xs = doubles(0.0, 1.0, 1.0, 0.0)
    square_ys = doubles(0.0, 0.0, 1.0, 0.0)
    rejected = {
        'mixed mask length': lambda: _lib._unpickle_geometry_array_v1(
            [gm.Point(0, 0).to_wkb()], None, None, bytes([0, 0])
        ),
        'one-vertex line': lambda: _lib._unpickle_packed_array_v1(
            square_xs, square_ys, None, None, (offsets(0, 1, 4),), None, None, None
        ),
        'short ring': lambda: _lib._unpickle_packed_array_v1(
            doubles(0.0, 1.0),
            doubles(0.0, 0.0),
            None,
            None,
            (offsets(0, 2), offsets(0, 1)),
            None,
            None,
            None,
        ),
        'open ring': lambda: _lib._unpickle_packed_array_v1(
            doubles(0.0, 1.0, 1.0, 0.5),
            doubles(0.0, 0.0, 1.0, 1.0),
            None,
            None,
            (offsets(0, 4), offsets(0, 1)),
            None,
            None,
            None,
        ),
        'ring past columns': lambda: _lib._unpickle_packed_array_v1(
            square_xs,
            square_ys,
            None,
            None,
            (offsets(0, 99), offsets(0, 1)),
            None,
            None,
            None,
        ),
        'empty ring': lambda: _lib._unpickle_packed_array_v1(
            b'', b'', None, None, (offsets(0, 0), offsets(0, 1)), None, None, None
        ),
        'descending polygon offsets': lambda: _lib._unpickle_packed_array_v1(
            square_xs,
            square_ys,
            None,
            None,
            (offsets(0, 4), offsets(0, 1, 0)),
            None,
            None,
            None,
        ),
    }
    for name, unpickle in rejected.items():
        with pytest.raises(gm.GeometryError) as excinfo:
            unpickle()
        assert type(excinfo.value).__name__ != 'PanicException', name

    # Zero-row packed polygon remains valid: [0]/[0] offsets, empty columns.
    empty = _lib._unpickle_packed_array_v1(
        b'', b'', None, None, (offsets(0), offsets(0)), None, None, None
    )
    assert len(empty) == 0


def test_packed_pickle_accepts_present_zero_ring_polygon() -> None:
    empty = _lib._unpickle_packed_array_v1(
        b'',
        b'',
        None,
        None,
        (struct.pack('<i', 0), struct.pack('<2i', 0, 0)),
        None,
        None,
        None,
    )

    assert empty.to_wkt() == ['POLYGON EMPTY']


def test_polygon_pickle_rejects_active_ordinate_unclosed_rings() -> None:
    """D05: pack-admission ring closure must cover every active ordinate.

    XY-closed rings whose Z and/or M first!=last previously entered trusted
    packed storage because pickle validated with XY-only ``same_point``.
    Admission must match ``ring_seq_is_packable`` / ``same_active_position``.
    """
    f = lambda xs: b''.join(struct.pack('<d', x) for x in xs)
    i = lambda xs: b''.join(struct.pack('<i', x) for x in xs)
    # Triangle closed in XY: (0,0)→(1,0)→(0,1)→(0,0).
    xs = f([0.0, 1.0, 0.0, 0.0])
    ys = f([0.0, 0.0, 1.0, 0.0])
    ring = i([0, 4])
    poly = i([0, 1])

    # Z closes in XY but not Z (1 != 2) — the audit repro.
    with pytest.raises(gm.GeometryError, match=r'closed|ring') as excinfo:
        _lib._unpickle_packed_array_v1(
            xs, ys, f([1.0, 1.0, 1.0, 2.0]), None, (ring, poly), None, None, None
        )
    assert type(excinfo.value).__name__ != 'PanicException'

    # M-only unclosed (first M != last M).
    with pytest.raises(gm.GeometryError, match=r'closed|ring') as excinfo:
        _lib._unpickle_packed_array_v1(
            xs, ys, None, f([1.0, 1.0, 1.0, 2.0]), (ring, poly), None, None, None
        )
    assert type(excinfo.value).__name__ != 'PanicException'

    # ZM unclosed on both extra ordinates.
    with pytest.raises(gm.GeometryError, match=r'closed|ring') as excinfo:
        _lib._unpickle_packed_array_v1(
            xs,
            ys,
            f([1.0, 1.0, 1.0, 2.0]),
            f([3.0, 3.0, 3.0, 4.0]),
            (ring, poly),
            None,
            None,
            None,
        )
    assert type(excinfo.value).__name__ != 'PanicException'

    # Genuinely closed 3D ring still unpickles into packed storage.
    zs_closed = f([1.0, 1.0, 1.0, 1.0])
    ok_z = _lib._unpickle_packed_array_v1(
        xs, ys, zs_closed, None, (ring, poly), None, None, None
    )
    assert len(ok_z) == 1
    assert list(ok_z[0].exterior.coords.z) == [1.0, 1.0, 1.0, 1.0]
    restored_z = pickle.loads(pickle.dumps(ok_z))
    assert list(restored_z[0].exterior.coords.z) == [1.0, 1.0, 1.0, 1.0]

    # Native 2D closed ring still accepts.
    ok_2d = _lib._unpickle_packed_array_v1(
        xs, ys, None, None, (ring, poly), None, None, None
    )
    assert len(ok_2d) == 1
    assert ok_2d[0].to_wkt().startswith('POLYGON')


def test_pickle_drop_missing_and_present_only_gather_round_trip() -> None:
    """drop_missing / fancy gather / bool-filter leave orphan NaNs in
    physical columns but clear the mask — must still round-trip.
    """
    base = gm.GeometryArray([gm.Point(0, 0), None, gm.Point(1, 1)])

    dropped = base.drop_missing()
    restored = pickle.loads(pickle.dumps(dropped))
    assert restored.to_wkt() == ['POINT (0 0)', 'POINT (1 1)']
    assert not restored.is_missing.any()

    gathered = base[[2, 0]]  # present-only fancy gather
    restored_g = pickle.loads(pickle.dumps(gathered))
    assert restored_g.to_wkt() == ['POINT (1 1)', 'POINT (0 0)']

    filtered = base[np.array([True, False, True])]
    restored_f = pickle.loads(pickle.dumps(filtered))
    assert restored_f.to_wkt() == ['POINT (0 0)', 'POINT (1 1)']


def _assert_packed_mask_pickle_round_trips(
    base: gm.GeometryArray, expected_dropped: list[str]
) -> None:
    """drop_missing / present-only gather / bool-filter / empty-selection pickle."""
    dropped = base.drop_missing()
    restored = pickle.loads(pickle.dumps(dropped))
    assert restored.to_wkt() == expected_dropped
    assert not restored.is_missing.any()

    # Present-only fancy gather (physical NaN orphans from mask scatter cleared).
    present_ids = [i for i, m in enumerate(base.is_missing.tolist()) if not m]
    gathered = base[present_ids[::-1]]
    restored_g = pickle.loads(pickle.dumps(gathered))
    assert restored_g.to_wkt() == list(reversed(expected_dropped))

    mask = np.array([not m for m in base.is_missing.tolist()])
    filtered = base[mask]
    restored_f = pickle.loads(pickle.dumps(filtered))
    assert restored_f.to_wkt() == expected_dropped

    # Transformed-mask path (to_crs keeps packed lines/polys + missing mask).
    if base.crs is not None:
        transformed = base.to_crs(3857).drop_missing()
        restored_t = pickle.loads(pickle.dumps(transformed))
        assert len(restored_t) == len(expected_dropped)
        assert not restored_t.is_missing.any()

    empty_sel = base[[]]
    restored_e = pickle.loads(pickle.dumps(empty_sel))
    assert len(restored_e) == 0


def test_pickle_packed_line_drop_missing_gather_filter_round_trip() -> None:
    """Packed LINES (not Mixed) must densify NaN placeholders on pickle."""
    base = _nullable_packed_linestring_arrow()
    assert base.is_missing.tolist() == [False, True, False]
    _assert_packed_mask_pickle_round_trips(
        base,
        ['LINESTRING (0 0, 1 1)', 'LINESTRING (2 2, 3 3)'],
    )


def test_pickle_packed_polygon_drop_missing_gather_filter_round_trip() -> None:
    """Packed POLYGONS (not Mixed) must densify NaN placeholders on pickle."""
    base = _nullable_packed_polygon_arrow()
    assert base.is_missing.tolist() == [False, True, False]
    _assert_packed_mask_pickle_round_trips(
        base,
        [
            'POLYGON ((0 0, 1 0, 1 1, 0 0))',
            'POLYGON ((2 2, 3 2, 3 3, 2 2))',
        ],
    )


def test_pickle_malformed_wkb_polygon_stays_mixed_not_packed() -> None:
    """The mixed WKB unpickler must not opportunistically pack.

    Uses a *valid, pack-admissible* polygon WKB so swapping
    ``mixed`` for ``pack_or_mixed`` would produce packed storage and fail
    the reduce-name check (malformed WKB demotes under pack_admission and
    would not detect that swap).
    """
    # Closed 4-vertex polygon — would pack under pack_or_mixed.
    wkb = gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]).to_wkb()
    arr = _lib._unpickle_geometry_array_v1([wkb], None, None, None)
    assert len(arr) == 1
    assert arr[0] is not None
    assert arr[0].is_valid
    # Unpickler must force Mixed: reduce stays on the mixed WKB lane.
    assert arr.__reduce_ex__(5)[0].__name__ == '_unpickle_geometry_array_v1'
    restored = pickle.loads(pickle.dumps(arr))
    assert restored.__reduce_ex__(5)[0].__name__ == '_unpickle_geometry_array_v1'
    assert restored[0].is_valid
