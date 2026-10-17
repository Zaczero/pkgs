"""The `Coordinates` view — flat columnar access, lazy iterators, XY-column
constructors, and parts/rings extraction helpers.
"""

import operator

import gometry as gm
import numpy as np
import pytest


def test_coords_sequence_is_flat_and_columnar() -> None:
    line = gm.LineString([(0, 0), (1, 2), (3, 4)])
    seq = line.coords
    assert len(seq) == 3
    assert list(seq) == [(0.0, 0.0), (1.0, 2.0), (3.0, 4.0)]
    assert seq[1] == (1.0, 2.0)
    assert seq[-1] == (3.0, 4.0)
    assert seq.coordinate_axes == 'XY'
    assert list(seq.x) == [0.0, 1.0, 3.0]
    assert list(seq.y) == [0.0, 2.0, 4.0]
    assert seq.z.dtype == np.float64 and seq.m.dtype == np.float64
    assert seq.z.flags.writeable is False and seq.m.flags.writeable is False
    assert np.isnan(seq.z).all() and np.isnan(seq.m).all()
    assert len(seq.z) == len(seq) == len(seq.m)
    np.testing.assert_array_equal(seq.row_index, [0, 0, 0])
    assert seq.x.dtype == np.float64
    assert seq.x.flags.writeable is False
    assert seq.x.tolist() == [0.0, 1.0, 3.0]
    assert seq[:] == [(0.0, 0.0), (1.0, 2.0), (3.0, 4.0)]
    assert seq[1:] == [(1.0, 2.0), (3.0, 4.0)]
    assert seq[::-1] == [(3.0, 4.0), (1.0, 2.0), (0.0, 0.0)]
    np.testing.assert_array_equal(seq.x[1:], [1.0, 3.0])
    with pytest.raises(IndexError):
        _ = seq[3]
    p = gm.Point(1, 2, z=3, m=4)
    assert list(p.coords) == [(1.0, 2.0, 3.0, 4.0)]
    assert p.coords.coordinate_axes == 'XYZM'
    z_col, m_col = (p.coords.z, p.coords.m)
    assert z_col is not None and m_col is not None
    assert z_col.tolist() == [3.0]
    assert m_col.tolist() == [4.0]
    poly = gm.Polygon([(0, 0), (4, 0), (4, 4), (0, 4)])
    assert poly.coords[0] == (0.0, 0.0)
    assert len(poly.coords) == 5
    collection = gm.GeometryCollection([
        gm.Point(9, 9),
        gm.LineString([(0, 0), (1, 1)]),
    ])
    assert list(collection.coords) == [(9.0, 9.0), (0.0, 0.0), (1.0, 1.0)]
    arr = gm.GeometryArray([
        gm.Point(0, 0),
        gm.LineString([(1, 1), (2, 2)]),
        gm.Point(3, 3),
    ])
    assert list(arr.coords) == [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)]
    np.testing.assert_array_equal(arr.coords.row_index, [0, 1, 1, 2])
    assert list(arr.coords.x) == [0.0, 1.0, 2.0, 3.0]
    columns = arr.coords.to_dict(index=True)
    np.testing.assert_array_equal(columns['x'], [0.0, 1.0, 2.0, 3.0])
    np.testing.assert_array_equal(columns['y'], [0.0, 1.0, 2.0, 3.0])
    np.testing.assert_array_equal(columns['index'], [0, 1, 1, 2])
    assert 'z' not in columns and 'm' not in columns
    assert all(not column.flags.writeable for column in columns.values())
    mat = np.asarray(line.coords)
    assert mat.shape == (3, 2) and mat.dtype == np.float64
    assert mat.tolist() == [[0.0, 0.0], [1.0, 2.0], [3.0, 4.0]]
    assert mat.flags.writeable is False
    forced = np.asarray(line.coords.select('XYZ'))
    assert forced.shape == (3, 3) and np.isnan(forced[:, 2]).all()
    ragged = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 2, z=9)])
    assert list(ragged.coords) == [(0.0, 0.0), (1.0, 2.0, 9.0)]
    ragged_matrix = np.asarray(ragged.coords)
    assert ragged_matrix.shape == (2, 3)
    np.testing.assert_allclose(ragged_matrix[:, 2], [np.nan, 9.0], equal_nan=True)
    assert list(ragged.coords.select('XYZ')) == [
        (0.0, 0.0, None),
        (1.0, 2.0, 9.0),
    ]
    assert ragged.coords.select('XYZ').to_nested() == [
        [0.0, 0.0, None],
        [1.0, 2.0, 9.0],
    ]
    selected = ragged.coords.select('XYZ').to_dict()
    np.testing.assert_array_equal(selected['x'], [0.0, 1.0])
    np.testing.assert_array_equal(selected['y'], [0.0, 2.0])
    np.testing.assert_allclose(selected['z'], [np.nan, 9.0], equal_nan=True)
    assert all(not column.flags.writeable for column in selected.values())
    assert list(gm.LineString([(0, 0), (1, 1)], z=[5, 6]).coords.select('XY')) == [
        (0.0, 0.0),
        (1.0, 1.0),
    ]


@pytest.mark.parametrize(
    'dtype',
    [
        np.dtype(np.float64),
        np.float64,
        'float64',
        'f8',
        'd',
        np.dtype(np.float32),
        np.float32,
        'float32',
        'f4',
        np.dtype(np.float16),
        np.float16,
        'float16',
        'f2',
        'e',
    ],
    ids=[
        'float64-dtype',
        'float64-type',
        'float64-name',
        'float64-f8',
        'float64-d',
        'float32-dtype',
        'float32-type',
        'float32-name',
        'float32-f4',
        'float16-dtype',
        'float16-type',
        'float16-name',
        'float16-f2',
        'float16-e',
    ],
)
def test_coordinates_numpy_protocol_accepts_float_dtype_spellings(
    dtype: object,
) -> None:
    coords = gm.LineString([(0, 0), (1, 2)]).coords

    direct = coords.__array__(dtype=dtype)
    via_numpy = np.asarray(coords, dtype=dtype)
    for result in (direct, via_numpy):
        assert result.dtype == np.dtype(dtype)
        np.testing.assert_array_equal(result, [[0.0, 0.0], [1.0, 2.0]])


@pytest.mark.parametrize(
    'dtype',
    [np.int64, 'int64', np.bool_, 'bool', object, 'O'],
    ids=['int64-type', 'int64-name', 'bool-type', 'bool-name', 'object-type', 'O'],
)
def test_coordinates_numpy_protocol_rejects_nonfloating_dtype(dtype: object) -> None:
    coords = gm.LineString([(0, 0), (1, 2)]).coords
    message = 'dtype must be a floating dtype or None'
    with pytest.raises(gm.GeometryError, match=message):
        coords.__array__(dtype=dtype)
    with pytest.raises(gm.GeometryError, match=message):
        np.asarray(coords, dtype=dtype)


def test_coordinates_numpy_protocol_honors_direct_opposite_endian_dtype() -> None:
    coords = gm.LineString([(0, 0), (1, 2)]).coords
    byteorder = '>' if np.little_endian else '<'
    requested = np.dtype(f'{byteorder}f8')
    expected = np.array([[0.0, 0.0], [1.0, 2.0]], dtype=requested)

    for dtype in (None, np.dtype(np.float64)):
        direct = coords.__array__(dtype=dtype)
        assert direct.dtype == np.dtype(np.float64)
        assert direct.flags.writeable is False
        np.testing.assert_array_equal(direct, expected)

    for copy in (None, True):
        direct = coords.__array__(dtype=requested, copy=copy)
        via_numpy = np.asarray(coords, dtype=requested)
        for result in (direct, via_numpy):
            assert result.dtype == requested
            assert result.dtype.isnative is False
            assert result.flags.writeable is False
            np.testing.assert_array_equal(result, expected)

    with pytest.raises(ValueError, match='without copying'):
        coords.__array__(dtype=requested, copy=False)
    with pytest.raises(ValueError, match='without copying'):
        np.asarray(coords, dtype=requested, copy=False)


def test_coordinates_equality_tracks_selected_layouts() -> None:
    xy = gm.LineString([(0, 0), (1, 1)]).coords
    assert xy == gm.LineString([(0, 0), (1, 1)]).coords
    assert xy == [(0, 0), (1, 1)]
    assert xy != [(0, 0)]
    assert xy != [(0, 0), (1, 1), (2, 2)]
    assert xy != [(0, 0), (1, 2)]
    assert xy == xy.select('XY')

    left_native = gm.LineString([(0, 0), (1, 1)], z=[5, 6]).coords
    right_native = gm.LineString([(0, 0), (1, 1)], z=[8, 9]).coords
    assert left_native != right_native

    left = left_native.select('XY')
    right = right_native.select('XY')
    assert left == right
    assert left == list(left)
    assert list(left) == left

    padded = xy.select('XYZ')
    assert padded == [(0, 0, None), (1, 1, None)]
    assert padded == list(padded)
    assert padded != xy


def test_coordinates_equality_uses_python_float_equality_for_signed_zero() -> None:
    positive = gm.points([0.0, 1.0], [0.0, 2.0]).coords
    negative = gm.points([-0.0, 1.0], [0.0, 2.0]).coords

    assert positive == [(0.0, 0.0), (1.0, 2.0)]
    assert negative == positive
    assert positive == negative
    assert negative == [(0.0, 0.0), (1.0, 2.0)]


def test_coordinates_support_numeric_numpy_ufuncs() -> None:
    coords = gm.LineString([(0.0, 1.0), (2.0, 3.0)]).coords

    np.testing.assert_array_equal(np.isfinite(coords), [[True, True], [True, True]])
    result = np.add(coords, 1.0)
    np.testing.assert_array_equal(result, [[1.0, 2.0], [3.0, 4.0]])
    assert result.flags.writeable is True
    np.testing.assert_array_equal(np.add.reduce(coords, axis=0), [2.0, 4.0])

    matrix = np.asarray(coords)
    assert matrix.flags.writeable is False
    with pytest.raises(ValueError, match='read-only'):
        np.add(coords, 1.0, out=matrix)


def test_coordinates_equality_propagates_type_errors_after_iterator_admission() -> None:
    coords = gm.Point(1.0, 2.0).coords
    iteration_error = TypeError('coordinates provider failed after one row')

    class OneShotRows:
        def __init__(self) -> None:
            self.iter_calls = 0

        def __iter__(self):
            self.iter_calls += 1
            if self.iter_calls > 1:
                raise AssertionError('one-shot provider was reacquired')
            yield (1.0, 2.0)
            raise iteration_error

    rows = OneShotRows()
    with pytest.raises(
        TypeError, match='coordinates provider failed after one row'
    ) as caught:
        coords.__eq__(rows)
    assert caught.value is iteration_error
    assert rows.iter_calls == 1

    comparison_error = TypeError('coordinate item comparison failed')

    class RaisingComparison:
        __hash__ = None

        def __eq__(self, other: object) -> bool:
            raise comparison_error

    with pytest.raises(TypeError, match='coordinate item comparison failed') as caught:
        coords.__eq__([RaisingComparison()])
    assert caught.value is comparison_error

    assert coords.__eq__(42) is NotImplemented


def test_coords_and_column_iterators_are_lazy() -> None:
    line = gm.LineString([(0, 0), (2, 3), (5, 6)])
    it = iter(line.coords)
    assert type(it).__name__ == 'CoordinatesIterator'
    assert next(it) == (0.0, 0.0)
    assert next(it) == (2.0, 3.0)
    assert list(it) == [(5.0, 6.0)]
    with pytest.raises(StopIteration):
        next(it)
    assert list(iter(line.coords)) == list(line.coords)


def test_line_and_multipoint_accept_xy_columns_round_tripping_coords() -> None:
    line = gm.LineString([(0, 0), (1, 2), (3, 4)])
    made = gm.LineString(x=[0, 1, 3], y=[0, 2, 4])
    assert gm.equals_exact(made, line)
    assert made.coords.x.tolist() == [0.0, 1.0, 3.0]
    assert made.coords.y.tolist() == [0.0, 2.0, 4.0]
    spatial = gm.LineString(x=[0, 1], y=[0, 1], z=[5, 6])
    assert spatial.coords.coordinate_axes == 'XYZ'
    assert gm.equals_exact(spatial, gm.LineString([(0, 0, 5), (1, 1, 6)]))
    pts = gm.MultiPoint(x=[0, 1], y=[2, 3])
    assert gm.equals_exact(pts, gm.MultiPoint([(0, 2), (1, 3)]))
    for bad in (
        lambda: gm.LineString([(0, 0), (1, 1)], x=[0, 1], y=[0, 1]),
        lambda: gm.LineString(x=[0, 1]),
        lambda: gm.MultiPoint(x=[0]),
    ):
        with pytest.raises((ValueError, TypeError)):
            bad()


def test_coordinate_extraction_parts_and_rings_helpers() -> None:
    polygon = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4)],
        holes=[[(1, 1), (3, 1), (3, 3), (1, 3)]],
        crs=4326,
    )
    values = gm.GeometryArray([gm.Point(9, 9, crs=4326), polygon])
    coordinates = list(values.coords.select('XY'))
    indexes = values.coords.row_index
    rings = polygon.boundary().parts
    assert list(gm.LineString([(0, 0), (1, 1)]).coords.select('XY')) == [
        (0.0, 0.0),
        (1.0, 1.0),
    ]
    assert gm.Point(1, 2, z=3).coords.select('XY').to_nested() == [1.0, 2.0]
    assert gm.Point(1, 2).coords.select('XYZ').to_nested() == [1.0, 2.0, None]
    assert coordinates[:3] == [(9.0, 9.0), (0.0, 0.0), (4.0, 0.0)]
    np.testing.assert_array_equal(indexes, [0] + [1] * 10)
    parts = values.parts
    assert isinstance(parts, gm.Groups)
    assert parts.offsets.tolist() == [0, 1, 2]
    assert [row.to_wkt() for row in parts[0]] == ['POINT (9 9)']
    assert [row.to_wkt() for row in parts[1]] == [values[1].to_wkt()]
    assert [ring.crs for ring in rings] == [gm.CRS('EPSG:4326')] * 2
    assert [ring.to_wkt() for ring in list(rings)] == [
        'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)',
        'LINESTRING (1 1, 3 1, 3 3, 1 3, 1 1)',
    ]
    assert next(iter(polygon.coords.select('XYZ'))) == (0.0, 0.0, None)


def test_parts_and_rings_collections_keep_groups_and_flat_values() -> None:
    polygon = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4)],
        holes=[[(1, 1), (3, 1), (3, 3), (1, 3)]],
    )
    sources = [
        gm.MultiPoint([(0, 0), (1, 1)]),
        gm.Point(2, 2),
        gm.from_wkt('POINT EMPTY'),
        gm.from_wkt('GEOMETRYCOLLECTION EMPTY'),
        None,
    ]

    parts = gm.GeometryArray(iter(sources)).parts
    assert isinstance(parts, gm.Groups)
    assert len(parts) == 5
    assert parts.offsets.tolist() == [0, 2, 3, 4, 4, 4]
    assert parts.values.to_wkt() == [
        'POINT (0 0)',
        'POINT (1 1)',
        'POINT (2 2)',
        'POINT EMPTY',
    ]
    assert parts[3].to_wkt() == []
    assert parts[4] is None
    assert parts.is_missing.tolist() == [False, False, False, False, True]

    rings = (
        gm
        .GeometryArray(iter([polygon, gm.from_wkt('POLYGON EMPTY'), None]))
        .boundary()
        .parts
    )
    assert isinstance(rings, gm.Groups)
    assert len(rings) == 3
    assert rings.offsets.tolist() == [0, 2, 2, 2]
    assert rings.values.to_wkt() == [
        'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)',
        'LINESTRING (1 1, 3 1, 3 3, 1 3, 1 1)',
    ]


def test_array_ragged_helpers_retain_empty_missing_rows_and_views() -> None:
    polygons = gm.GeometryArray([
        gm.Polygon([(0, 0), (1, 0), (0, 1), (0, 0)]),
        gm.from_wkt('POLYGON EMPTY'),
        None,
        gm.Polygon(
            [(2, 0), (3, 0), (2, 1), (2, 0)],
            holes=[[(2.2, 0.2), (2.4, 0.2), (2.2, 0.4), (2.2, 0.2)]],
        ),
    ])
    rings = polygons.boundary().parts
    assert isinstance(rings, gm.Groups)
    assert rings.offsets.tolist() == [0, 1, 1, 1, 3]

    view = polygons.coords
    coords = np.asarray(view, dtype=np.float64)
    indexes = view.row_index
    assert coords.shape == (12, 2)
    np.testing.assert_array_equal(indexes, [0] * 4 + [3] * 8)

    selected = polygons[[3, 1, 2]]
    selected_rings = selected.boundary().parts
    assert selected_rings.offsets.tolist() == [0, 2, 2, 2]
    selected_view = selected.coords
    selected_coords = np.asarray(selected_view, dtype=np.float64)
    selected_indexes = selected_view.row_index
    assert selected_coords.shape == (8, 2)
    np.testing.assert_array_equal(selected_indexes, [0] * 8)


def test_coordinate_matrix_is_readonly() -> None:
    matrix = np.asarray(
        gm.GeometryArray([gm.Point(1, 2), None]).coords, dtype=np.float64
    )

    assert matrix.flags.writeable is False
    with pytest.raises(ValueError, match='read-only'):
        matrix[0, 0] = 9.0


def test_coordinate_columns_are_readonly_ndarrays() -> None:
    line = gm.LineString([(0, 0), (1, 1)], z=[5, 6])
    xs = line.coords.x
    assert xs.dtype == np.float64
    assert xs.flags.writeable is False
    assert xs.tolist() == [0.0, 1.0]
    z = line.coords.z
    assert z.dtype == np.float64
    assert z.flags.writeable is False
    assert z.tolist() == [5.0, 6.0]
    mixed = gm.from_wkt(['POINT (0 0)', 'POINT Z (1 1 9)'])
    assert isinstance(mixed, gm.GeometryArray)
    nullable_z = mixed.coords.z
    assert nullable_z.dtype == np.float64
    assert np.isnan(nullable_z[0])
    assert nullable_z[1] == 9.0
    # Absent Z/M are NaN-filled float64 ndarrays of view length, never None.
    bare = gm.Point(1, 2, crs=4326).coords
    assert bare.z.dtype == np.float64 and bare.m.dtype == np.float64
    assert bare.z.flags.writeable is False and bare.m.flags.writeable is False
    assert np.isnan(bare.z).all() and np.isnan(bare.m).all()
    assert len(bare.z) == 1 == len(bare.m)


@pytest.mark.parametrize(
    'middle', [None, gm.from_wkt('POINT EMPTY')], ids=['missing', 'empty']
)
def test_coords_preserves_empty_rows_and_all_carried_axes(middle):
    array = gm.GeometryArray([
        gm.from_wkt('POINT Z (1 2 3)'),
        middle,
        gm.from_wkt('POINT M (5 6 7)'),
    ])
    view = array.coords
    matrix = np.asarray(view, dtype=np.float64)
    indexes = view.row_index
    assert matrix.shape == (2, 4)
    np.testing.assert_allclose(
        matrix,
        [[1, 2, 3, np.nan], [5, 6, np.nan, 7]],
        equal_nan=True,
    )
    np.testing.assert_array_equal(indexes, [0, 2])


def test_coords_matrix_and_row_index() -> None:
    line = gm.LineString([(0, 0), (1, 2), (3, 4)])
    view = line.coords
    coords = np.asarray(view, dtype=np.float64)
    assert coords.shape == (3, 2)
    assert coords.dtype == np.float64
    assert coords.tolist() == [[0.0, 0.0], [1.0, 2.0], [3.0, 4.0]]
    scalar_coords = np.asarray(view, dtype=np.float64)
    scalar_indexes = view.row_index
    np.testing.assert_array_equal(scalar_coords, coords)
    np.testing.assert_array_equal(scalar_indexes, [0, 0, 0])
    arr = gm.GeometryArray([gm.Point(0, 0), gm.LineString([(1, 1), (2, 2)])])
    with_z_arr = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1, z=9)])
    with_z = np.asarray(with_z_arr.coords, dtype=np.float64)
    assert with_z.shape == (2, 3)
    np.testing.assert_allclose(with_z[:, 2], [np.nan, 9], equal_nan=True)
    np.testing.assert_array_equal(with_z[:, :2], [[0, 0], [1, 1]])
    arr_view = arr.coords
    grouped = np.asarray(arr_view, dtype=np.float64)
    indexes = arr_view.row_index
    np.testing.assert_array_equal(grouped, [[0, 0], [1, 1], [2, 2]])
    assert indexes.dtype == np.int64
    assert indexes.flags.writeable is False
    np.testing.assert_array_equal(indexes, [0, 1, 1])
    iterable_view = gm.GeometryArray([gm.Point(5, 6), None, gm.Point(7, 8)]).coords
    iterable = np.asarray(iterable_view, dtype=np.float64)
    iterable_indexes = iterable_view.row_index
    np.testing.assert_array_equal(iterable, [[5.0, 6.0], [7.0, 8.0]])
    np.testing.assert_array_equal(iterable_indexes, [0, 2])


def test_coords_collections_are_flat_and_one_shot() -> None:
    rows = [gm.Point(5, 6), gm.from_wkt('POINT EMPTY'), None, gm.Point(7, 8)]

    class Once:
        def __init__(self, values):
            self.values = iter(values)

        def __iter__(self):
            values = self.values
            self.values = None
            if values is None:
                raise AssertionError('iterator was consumed more than once')
            return values

    for values in (rows, Once(rows), gm.GeometryArray(rows)):
        view = (
            values.coords
            if isinstance(values, gm.GeometryArray)
            else gm.GeometryArray(values).coords
        )
        matrix = np.asarray(view, dtype=np.float64)
        indexes = view.row_index
        np.testing.assert_array_equal(matrix, [[5.0, 6.0], [7.0, 8.0]])
        np.testing.assert_array_equal(indexes, [0, 3])


def test_coords_collection_axes_match_across_input_representations() -> None:
    rows = [
        gm.from_wkt('POINT Z (1 2 3)'),
        gm.from_wkt('POINT EMPTY'),
        None,
        gm.from_wkt('POINT M (5 6 7)'),
    ]
    for source_factory in (
        lambda: rows,
        lambda: iter(rows),
        lambda: gm.GeometryArray(rows),
    ):
        value = source_factory()
        view = (
            value.coords
            if isinstance(value, gm.GeometryArray)
            else gm.GeometryArray(value).coords
        )
        matrix = np.asarray(view, dtype=np.float64)
        assert matrix.shape == (2, 4)
        np.testing.assert_allclose(
            matrix,
            [[1, 2, 3, np.nan], [5, 6, np.nan, 7]],
            equal_nan=True,
        )
        value = source_factory()
        view = (
            value.coords
            if isinstance(value, gm.GeometryArray)
            else gm.GeometryArray(value).coords
        )
        matrix = np.asarray(view, dtype=np.float64)
        indexes = view.row_index
        np.testing.assert_allclose(
            matrix,
            [[1, 2, 3, np.nan], [5, 6, np.nan, 7]],
            equal_nan=True,
        )
        np.testing.assert_array_equal(indexes, [0, 3])


def test_coords_array_values_reconstruct_row_indices() -> None:
    arr = gm.GeometryArray([
        gm.Point(1, 2),
        gm.from_wkt('LINESTRING EMPTY'),
        None,
        gm.LineString([(3, 4), (5, 6)]),
    ])
    view = arr.coords
    coordinates = np.asarray(view, dtype=np.float64)
    indexes = view.row_index

    np.testing.assert_array_equal(
        coordinates,
        [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
    )
    np.testing.assert_array_equal(
        indexes,
        [0, 3, 3],
    )


def test_coords_row_index_packed_lines() -> None:
    """Packed identity lines: matrix + CSR-derived logical row index."""
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(2, 0), (3, 0), (4, 0)]),
        gm.LineString([(5, 0), (6, 0)]),
    ])
    view = lines.coords
    matrix = np.asarray(view, dtype=np.float64)
    indexes = view.row_index
    assert matrix.shape == (7, 2)
    np.testing.assert_array_equal(
        matrix,
        [[0, 0], [1, 0], [2, 0], [3, 0], [4, 0], [5, 0], [6, 0]],
    )
    np.testing.assert_array_equal(indexes, [0, 0, 1, 1, 1, 2, 2])


def test_coords_row_index_polygons_with_holes() -> None:
    """Polygon shell+holes flatten depth-first; index spans every ring vertex."""
    poly = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)],
        holes=[[(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]],
    )
    other = gm.Polygon([(10, 10), (11, 10), (11, 11), (10, 10)])
    arr = gm.GeometryArray([poly, other])
    view = arr.coords
    matrix = np.asarray(view, dtype=np.float64)
    indexes = view.row_index
    assert matrix.shape == (14, 2)
    np.testing.assert_array_equal(indexes, [0] * 10 + [1] * 4)
    # Shell first vertex, then hole first vertex, then second polygon.
    np.testing.assert_array_equal(matrix[0], [0.0, 0.0])
    np.testing.assert_array_equal(matrix[5], [1.0, 1.0])
    np.testing.assert_array_equal(matrix[10], [10.0, 10.0])


def test_coords_row_index_mixed_array() -> None:
    mixed = gm.GeometryArray([
        gm.Point(0, 0),
        gm.LineString([(1, 1), (2, 2)]),
        gm.Polygon([(3, 3), (4, 3), (4, 4), (3, 3)]),
    ])
    view = mixed.coords
    matrix = np.asarray(view, dtype=np.float64)
    indexes = view.row_index
    assert matrix.shape == (7, 2)
    np.testing.assert_array_equal(indexes, [0, 1, 1, 2, 2, 2, 2])
    np.testing.assert_array_equal(matrix[0], [0.0, 0.0])
    np.testing.assert_array_equal(matrix[1:3], [[1.0, 1.0], [2.0, 2.0]])
    np.testing.assert_array_equal(matrix[3], [3.0, 3.0])


def test_coords_row_index_gathered_selection() -> None:
    """Gather preserves logical (not physical) row numbers in the index."""
    base = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(2, 0), (3, 0), (4, 0)]),
        gm.LineString([(5, 0), (6, 0)]),
    ])
    gathered = base[[2, 0]]
    view = gathered.coords
    matrix = np.asarray(view, dtype=np.float64)
    indexes = view.row_index
    np.testing.assert_array_equal(
        matrix, [[5.0, 0.0], [6.0, 0.0], [0.0, 0.0], [1.0, 0.0]]
    )
    np.testing.assert_array_equal(indexes, [0, 0, 1, 1])
    # Window slice also keeps logical ids starting at 0.
    window = base[1:]
    window_indexes = window.coords.row_index
    np.testing.assert_array_equal(window_indexes, [0, 0, 0, 1, 1])


@pytest.mark.parametrize(
    ('missing', 'expected_rows'),
    [
        pytest.param([False, True, False], [0, 2], id='middle-null'),
        pytest.param([True, False, False], [1, 2], id='leading-null'),
        pytest.param([False, False, True], [0, 1], id='trailing-null'),
    ],
)
def test_compact_nullable_point_coordinate_indices(
    missing: list[bool], expected_rows: list[int]
) -> None:
    points = [
        gm.Point(10.0, 20.0, z=30.0, m=40.0),
        gm.Point(11.0, 21.0, z=31.0, m=41.0),
        gm.Point(12.0, 22.0, z=32.0, m=42.0),
    ]
    values = [
        point if not is_missing else None
        for point, is_missing in zip(points, missing, strict=True)
    ]
    array = gm.GeometryArray(values)
    expected_points = [points[row] for row in expected_rows]

    assert len(array.coords) == len(expected_points)
    assert array.coords.coordinate_axes == 'XYZM'
    np.testing.assert_array_equal(array.coords.row_index, expected_rows)
    columns = array.coords.to_dict(index=True)
    np.testing.assert_array_equal(columns['index'], expected_rows)
    np.testing.assert_array_equal(columns['x'], [point.x for point in expected_points])
    np.testing.assert_array_equal(columns['z'], [point.z for point in expected_points])

    view = gm.GeometryArray(values).coords
    matrix = np.asarray(view, dtype=np.float64)
    indexes = view.row_index
    np.testing.assert_array_equal(indexes, expected_rows)
    np.testing.assert_array_equal(
        matrix,
        [[point.x, point.y, point.z, point.m] for point in expected_points],
    )


def test_compact_nullable_point_coordinate_indices_preserve_slices_and_edges() -> None:
    array = gm.GeometryArray([
        gm.Point(10.0, 20.0, z=30.0),
        None,
        gm.Point(12.0, 22.0, z=32.0),
    ])
    reversed_array = array[::-1]
    assert len(reversed_array.coords) == 2
    np.testing.assert_array_equal(reversed_array.coords.row_index, [0, 2])
    np.testing.assert_array_equal(
        reversed_array.coords.to_dict(index=True)['index'], [0, 2]
    )
    window = array[1:]
    assert len(window.coords) == 1
    np.testing.assert_array_equal(window.coords.row_index, [1])
    np.testing.assert_array_equal(window.coords.to_dict(index=True)['index'], [1])

    all_missing = array[[1]]
    assert len(all_missing.coords) == 0
    np.testing.assert_array_equal(all_missing.coords.row_index, [])
    np.testing.assert_array_equal(all_missing.coords.to_dict(index=True)['index'], [])

    empty = gm.from_wkt('POINT ZM EMPTY')
    assert len(empty.coords) == 0
    assert empty.coords.coordinate_axes == 'XYZM'
    np.testing.assert_array_equal(empty.coords.row_index, [])
    np.testing.assert_array_equal(empty.coords.to_dict(index=True)['index'], [])
    view = empty.coords
    matrix = np.asarray(view, dtype=np.float64)
    indexes = view.row_index
    assert matrix.shape == (0, 4)
    np.testing.assert_array_equal(indexes, [])


def _coordinate_roundtrip_geometries() -> list[gm.Geometry]:
    return [
        gm.Point(1, 2),
        gm.from_wkt('POINT Z (1 2 3)'),
        gm.from_wkt('POINT M (1 2 4)'),
        gm.from_wkt('POINT ZM (1 2 3 4)'),
        gm.LineString([(0, 0), (1, 2), (3, 4)]),
        gm.from_wkt('LINESTRING Z (0 0 1, 1 2 3, 3 4 5)'),
        gm.from_wkt('LINESTRING M (0 0 10, 1 2 20, 3 4 30)'),
        gm.from_wkt('LINESTRING ZM (0 0 1 10, 1 2 3 20, 3 4 5 30)'),
        gm.Polygon(
            [(0, 0), (4, 0), (4, 4), (0, 0)],
            holes=[[(1, 1), (2, 1), (1, 2), (1, 1)]],
        ),
        gm.MultiPoint([(0, 0), (1, 1)]),
        gm.MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]),
        gm.MultiPolygon([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)]),
        gm.GeometryCollection([gm.Point(0, 0), gm.LineString([(1, 1), (2, 2)])]),
        # D22: heterogeneous GeometryCollection member axes — identity round-trip.
        # (MultiLineString / MultiPolygon mixed axes are rejected at construction.)
        gm.GeometryCollection([gm.Point(1, 2, z=3), gm.Point(4, 5)]),
    ]


@pytest.mark.parametrize('geom', _coordinate_roundtrip_geometries())
def test_with_coordinates_round_trips_coordinates_view(geom: gm.Geometry) -> None:
    out = geom.with_coordinates(geom.coords)
    assert gm.equals_exact(out, geom)
    assert out.crs == geom.crs
    assert out.epoch == geom.epoch


@pytest.mark.parametrize(
    ('wkt', 'axes', 'width'),
    [
        ('POINT Z EMPTY', 'XYZ', 3),
        ('POINT M EMPTY', 'XYM', 3),
        ('POINT ZM EMPTY', 'XYZM', 4),
        ('POLYGON Z EMPTY', 'XYZ', 3),
        ('POLYGON M EMPTY', 'XYM', 3),
        ('POLYGON ZM EMPTY', 'XYZM', 4),
        ('MULTILINESTRING Z EMPTY', 'XYZ', 3),
        ('MULTILINESTRING M EMPTY', 'XYM', 3),
        ('MULTILINESTRING ZM EMPTY', 'XYZM', 4),
        ('MULTIPOLYGON Z EMPTY', 'XYZ', 3),
        ('MULTIPOLYGON M EMPTY', 'XYM', 3),
        ('MULTIPOLYGON ZM EMPTY', 'XYZM', 4),
        ('GEOMETRYCOLLECTION Z EMPTY', 'XYZ', 3),
        ('GEOMETRYCOLLECTION M EMPTY', 'XYM', 3),
        ('GEOMETRYCOLLECTION ZM EMPTY', 'XYZM', 4),
        # Seq-backed empties already carried axes; pin them too.
        ('LINESTRING Z EMPTY', 'XYZ', 3),
        ('MULTIPOINT Z EMPTY', 'XYZ', 3),
        ('POINT EMPTY', 'XY', 2),
    ],
)
def test_m06_empty_coords_view_uses_declared_axes(
    wkt: str, axes: str, width: int
) -> None:
    """m06: empty geometries expose coords width from declared empty axes.

    EXACT repro: ``np.asarray(from_wkt('POINT Z EMPTY').coords).shape`` is
    ``(0, 3)`` (not ``(0, 2)``), and ``g.with_coordinates(g.coords)`` round-trips.
    """
    g = gm.from_wkt(wkt)
    assert g.coordinate_axes == axes
    assert g.coords.coordinate_axes == axes
    matrix = np.asarray(g.coords)
    assert matrix.shape == (0, width)
    assert matrix.dtype == np.float64

    out = g.with_coordinates(g.coords)
    assert out.coordinate_axes == axes
    assert out.geometry_type == g.geometry_type
    assert out.is_empty
    assert out.to_wkt() == g.to_wkt()
    assert np.asarray(out.coords).shape == (0, width)


def test_with_coordinates_array_mask_and_row_map_round_trip() -> None:
    arr = gm.GeometryArray([gm.Point(1, 2), None, gm.Point(3, 4)])
    out = arr.with_coordinates(arr.coords)
    assert list(out)[1] is None
    assert gm.equals_exact(out[0], gm.Point(1, 2))
    assert gm.equals_exact(out[2], gm.Point(3, 4))
    np.testing.assert_array_equal(out.num_coordinates, [1, 0, 1])
    assert int(out.num_coordinates.sum()) == len(out.coords)

    selected = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1), gm.Point(2, 2)])[::-1]
    selected_out = selected.with_coordinates(selected.coords)
    assert selected_out.to_wkt() == selected.to_wkt()


def test_with_coordinates_validation_errors() -> None:
    line = gm.LineString([(0, 0), (1, 1)])
    with pytest.raises(gm.InvalidGeometryError, match='two-dimensional'):
        line.with_coordinates(np.array([0.0, 0.0]))
    with pytest.raises(gm.InvalidGeometryError, match='two-dimensional'):
        line.with_coordinates(np.zeros((1, 2, 1)))
    with pytest.raises(gm.InvalidGeometryError, match='length 2'):
        line.with_coordinates(np.array([[0.0, 0.0]]))
    with pytest.raises(gm.InvalidGeometryError, match='width 2'):
        line.with_coordinates(np.zeros((2, 3)))
    with pytest.raises(gm.InvalidGeometryError, match='finite'):
        line.with_coordinates(np.array([[0.0, 0.0], [np.nan, 1.0]]))
    with pytest.raises(gm.GeometryError, match='cannot mix'):
        line.with_coordinates(np.asarray(line.coords), x=[0.0, 1.0], y=[0.0, 1.0])
    with pytest.raises(TypeError, match='unexpected keyword'):
        line.with_coordinates(coordinates=np.asarray(line.coords))
    xyz = line.with_coordinates(x=[0.0, 1.0], y=[0.0, 1.0], z=[1.0, 2.0])
    assert xyz.to_wkt() == 'LINESTRING Z (0 0 1, 1 1 2)'

    poly = gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])
    opened = np.asarray(poly.coords).copy()
    opened[-1] = [9.0, 9.0]
    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        poly.with_coordinates(opened)


@pytest.mark.parametrize(
    ('axis', 'kind'),
    [
        pytest.param('z', 'point', id='point-z'),
        pytest.param('m', 'point', id='point-m'),
        pytest.param('z', 'line', id='line-z'),
        pytest.param('m', 'line', id='line-m'),
    ],
)
def test_with_coordinates_column_adds_an_ordinate_to_every_collection_member(
    axis: str,
    kind: str,
) -> None:
    if kind == 'point':
        with_axis = gm.Point(0.0, 1.0, **{axis: 2.0})
        without_axis = gm.Point(3.0, 4.0)
    else:
        ordinate = 'Z' if axis == 'z' else 'M'
        with_axis = gm.from_wkt(f'LINESTRING {ordinate} (0 1 2, 3 4 5)')
        without_axis = gm.LineString([(6.0, 7.0), (8.0, 9.0)])
    geometry = gm.GeometryCollection([with_axis, without_axis])
    count = with_axis.num_coordinates + without_axis.num_coordinates
    replacement = {
        axis: [30.0 + index for index in range(count)],
    }

    for source in (geometry, gm.GeometryArray([geometry])):
        updated = source.with_coordinates(**replacement)
        row = updated if isinstance(updated, gm.Geometry) else updated[0]
        axes = 'XYZ' if axis == 'z' else 'XYM'
        assert [part.coordinate_axes for part in row.parts] == [axes, axes]
        ordinates = [
            value
            for part in row.parts
            for value in (part.coords.z if axis == 'z' else part.coords.m)
        ]
        assert ordinates == [30.0 + index for index in range(count)]


def test_with_coordinates_broadcasts_scalars_preserves_omissions_and_drops_axes() -> (
    None
):
    line = gm.from_wkt('LINESTRING ZM (0 0 1 10, 1 1 2 20)')

    assert line.with_coordinates() == line
    assert line.with_coordinates(None) == line
    assert line.with_coordinates(None, x=5.0) == line.with_coordinates(x=5.0)
    changed = line.with_coordinates(x=5.0, z=7.0)
    assert changed.to_wkt() == 'LINESTRING ZM (5 0 7 10, 5 1 7 20)'
    assert changed.with_coordinates(z=None).to_wkt() == 'LINESTRING M (5 0 10, 5 1 20)'
    assert changed.with_coordinates(m=None).to_wkt() == 'LINESTRING Z (5 0 7, 5 1 7)'

    added = gm.LineString([(0.0, 0.0), (1.0, 1.0)]).with_coordinates(z=3.0)
    assert added.to_wkt() == 'LINESTRING Z (0 0 3, 1 1 3)'

    with pytest.raises(
        gm.InvalidGeometryError, match='X and Y ordinates cannot be removed'
    ):
        line.with_coordinates(x=None)


def test_with_coordinates_columns_follow_flattened_present_array_vertices() -> None:
    values = gm.GeometryArray([gm.LineString([(0, 1), (2, 3)]), None, gm.Point(4, 5)])
    updated = values.with_coordinates(m=[10.0, 20.0, 30.0])

    assert updated.to_wkt() == [
        'LINESTRING M (0 1 10, 2 3 20)',
        None,
        'POINT M (4 5 30)',
    ]


def test_with_coordinates_retags_empty_collection_members() -> None:
    geometry = gm.GeometryCollection([
        gm.Point(),
        gm.LineString([(0.0, 1.0), (2.0, 3.0)]),
    ])
    updated = geometry.with_coordinates(z=[10.0, 20.0])

    assert [part.coordinate_axes for part in updated.parts] == ['XYZ', 'XYZ']
    assert [part.to_wkt() for part in updated.parts] == [
        'POINT Z EMPTY',
        'LINESTRING Z (0 1 10, 2 3 20)',
    ]

    nullable = gm.GeometryArray([geometry, None]).with_coordinates(m=[30.0, 40.0])
    assert nullable.is_missing.tolist() == [False, True]
    assert [part.coordinate_axes for part in nullable[0].parts] == ['XYM', 'XYM']


def test_points_reject_mismatched_z_and_m_column_lengths() -> None:
    with pytest.raises(gm.InvalidGeometryError, match='same length'):
        gm.points([0.0, 1.0], [0.0, 1.0], z=[2.0])
    with pytest.raises(gm.InvalidGeometryError, match='same length'):
        gm.points([0.0, 1.0], [0.0, 1.0], m=[3.0])


def test_with_coordinates_rejects_mismatched_z_and_m_column_lengths() -> None:
    line = gm.LineString([(0.0, 0.0, 1.0, 10.0), (1.0, 1.0, 2.0, 20.0)])
    with pytest.raises(gm.InvalidGeometryError, match='z must have length 2'):
        line.with_coordinates(x=[3.0, 4.0], y=[5.0, 6.0], z=[7.0], m=[30.0, 40.0])
    with pytest.raises(gm.InvalidGeometryError, match='m must have length 2'):
        line.with_coordinates(x=[3.0, 4.0], y=[5.0, 6.0], z=[7.0, 8.0], m=[30.0])


def test_with_coordinates_accepts_positive_negative_and_zero_strides() -> None:
    line = gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)])
    storage = np.array([[10.0, 20.0], [11.0, 21.0], [12.0, 22.0]])

    positive = line.with_coordinates(x=storage[:, 0], y=storage[:, 1])
    negative = line.with_coordinates(x=storage[::-1, 0], y=storage[::-1, 1])
    zero = line.with_coordinates(
        x=np.broadcast_to(30.0, 3),
        y=np.broadcast_to(40.0, 3),
    )
    assert list(positive.coords) == [(10.0, 20.0), (11.0, 21.0), (12.0, 22.0)]
    assert list(negative.coords) == [(12.0, 22.0), (11.0, 21.0), (10.0, 20.0)]
    assert list(zero.coords) == [(30.0, 40.0)] * 3


def test_numpy_scalar_coordinates_and_float32_matrix_are_admitted() -> None:
    point = gm.Point(np.float32(1.0), np.int64(2), z=np.float64(3.0))
    assert point.to_wkt() == 'POINT Z (1 2 3)'

    line = gm.LineString([(0.0, 0.0), (1.0, 1.0)])
    replacement = np.array([[5.0, 6.0], [7.0, 8.0]], dtype=np.float32)
    out = line.with_coordinates(replacement)
    assert out.to_wkt() == 'LINESTRING (5 6, 7 8)'
    assert out.coords.x.dtype == np.float64
    assert out.coords.y.dtype == np.float64

    points = gm.points(replacement[:, 0], replacement[:, 1])
    assert points.coords.x.dtype == np.float64
    assert not np.shares_memory(points.coords.x, replacement[:, 0])


@pytest.mark.parametrize(
    'wkt',
    [
        'POLYGON Z ((0 0 1, 1 0 1, 0 1 1, 0 0 1))',
        'POLYGON M ((0 0 1, 1 0 1, 0 1 1, 0 0 1))',
        'POLYGON ZM ((0 0 1 2, 1 0 1 2, 0 1 1 2, 0 0 1 2))',
    ],
)
def test_with_coordinates_rejects_active_ordinate_open_ring(wkt: str) -> None:
    """N5: Z/M-only open rings must fail with_coordinates (pack/pickle parity).

    XY-only closure used to admit these into trusted storage; the unpickler
    (same_active_position) then rejected them — self-inconsistency.
    """
    import pickle

    poly = gm.from_wkt(wkt)
    # Scalar path
    coords = np.asarray(poly.coords).copy()
    # Break only the last vertex's first measure ordinate (Z or M column).
    # Width is 3 for Z/M, 4 for ZM — always mutate the last column index >= 2.
    coords[-1, 2] = 9.0
    with pytest.raises(gm.InvalidGeometryError) as error:
        poly.with_coordinates(coords)
    assert str(error.value) == 'polygon ring must be closed'

    # Packed-array path (uniform polygon column)
    arr = gm.GeometryArray([poly])
    arr_coords = np.asarray(arr.coords).copy()
    arr_coords[-1, 2] = 9.0
    with pytest.raises(gm.InvalidGeometryError) as error:
        arr.with_coordinates(arr_coords)
    assert str(error.value) == 'polygon ring must be closed'

    # map_coordinates opens the same way
    def open_measure(c: np.ndarray) -> np.ndarray:
        out = np.array(c, copy=True)
        out[-1, 2] = 9.0
        return out

    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        poly.map_coordinates(open_measure)
    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        arr.map_coordinates(open_measure)

    # Closure-preserving: set first and last measure equal → accepted + pickles
    keep = np.asarray(poly.coords).copy()
    keep[0, 2] = 7.0
    keep[-1, 2] = 7.0
    kept = poly.with_coordinates(keep)
    assert pickle.loads(pickle.dumps(kept)) == kept
    arr_keep = np.asarray(arr.coords).copy()
    arr_keep[0, 2] = 7.0
    arr_keep[-1, 2] = 7.0
    kept_arr = arr.with_coordinates(arr_keep)
    assert pickle.loads(pickle.dumps(kept_arr)) == kept_arr

    # Interior-only change still succeeds
    interior = np.asarray(poly.coords).copy()
    interior[1, 0] = 1.5
    moved = poly.with_coordinates(interior)
    assert pickle.loads(pickle.dumps(moved)) == moved


def test_with_coordinates_xy_ring_unchanged() -> None:
    """2D rings still reject XY-open and accept closed replacements."""
    import pickle

    poly = gm.Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])
    opened = np.asarray(poly.coords).copy()
    opened[-1] = [9.0, 9.0]
    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        poly.with_coordinates(opened)
    kept = poly.with_coordinates(np.asarray(poly.coords).copy())
    assert pickle.loads(pickle.dumps(kept)) == kept
    arr = gm.GeometryArray([poly])
    kept_arr = arr.with_coordinates(np.asarray(arr.coords).copy())
    assert pickle.loads(pickle.dumps(kept_arr)) == kept_arr


@pytest.mark.parametrize(
    'select',
    [
        # Window / slice (non-identity row_map → replace_packed_coords_detached)
        operator.itemgetter(slice(1, None)),
        operator.itemgetter(slice(0, 1)),
        # Fancy gather
        operator.itemgetter([1]),
        operator.itemgetter([0, 2]),
    ],
    ids=['slice_tail', 'slice_head', 'gather_one', 'gather_two'],
)
def test_with_coordinates_selection_rejects_z_open_ring(select) -> None:
    """N5: sliced/gathered packed arrays use the detached replacement path.

    That path had a parallel XY-only ring check (packed_columns) that still
    admitted Z-open rings — same pickle self-inconsistency as the dense path.
    """
    import pickle

    poly = gm.from_wkt('POLYGON Z ((0 0 1, 1 0 1, 0 1 1, 0 0 1))')
    # Two extra closed rings so slices/gathers are non-trivial and non-identity.
    other = gm.from_wkt('POLYGON Z ((2 2 3, 3 2 3, 2 3 3, 2 2 3))')
    third = gm.from_wkt('POLYGON Z ((4 4 5, 5 4 5, 4 5 5, 4 4 5))')
    base = gm.GeometryArray([poly, other, third])
    view = select(base)
    assert len(view) >= 1

    opened = np.asarray(view.coords).copy()
    # Break Z on the last vertex of the view's coordinate matrix.
    opened[-1, 2] = 9.0
    with pytest.raises(gm.InvalidGeometryError) as error:
        view.with_coordinates(opened)
    assert str(error.value) == 'polygon ring must be closed'

    def open_measure(c: np.ndarray) -> np.ndarray:
        out = np.array(c, copy=True)
        out[-1, 2] = 9.0
        return out

    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        view.map_coordinates(open_measure)

    # Closure-preserving: mutate only an interior vertex, then pickle-round-trip.
    keep = np.asarray(view.coords).copy()
    keep[1, 2] = 8.0  # second vertex is always interior on a 4-vertex ring
    kept = view.with_coordinates(keep)
    assert pickle.loads(pickle.dumps(kept)) == kept


def test_with_coordinates_nullable_polygon_error_uses_logical_row() -> None:
    polygon = gm.from_wkt('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')
    array = gm.GeometryArray([None, polygon, None, polygon])
    opened = np.asarray(array.coords).copy()
    opened[-1, 0] = -1.0

    with pytest.raises(
        gm.InvalidGeometryError, match='polygon ring must be closed'
    ) as error:
        array.with_coordinates(opened)

    assert error.value.__notes__ == ['while processing array element 3']


@pytest.mark.parametrize(
    ('selection', 'expected_row'),
    [
        pytest.param('identity', 3, id='identity-row-3'),
        pytest.param('gather', 2, id='gather-row-2'),
        pytest.param('reverse', 0, id='reverse-row-0'),
    ],
)
@pytest.mark.parametrize(
    ('ring_end', 'ordinate'),
    [
        pytest.param(3, 0, id='shell-x'),
        pytest.param(7, 0, id='hole-x'),
        pytest.param(3, 2, id='shell-m'),
        pytest.param(7, 2, id='hole-m'),
    ],
)
@pytest.mark.parametrize(
    'operation',
    ['with_coordinates', 'map_coordinates'],
    ids=['replacement', 'callback'],
)
def test_with_coordinates_mixed_nullable_present_empty_replacement_reports_gathered_row(
    selection: str, expected_row: int, ring_end: int, ordinate: int, operation: str
) -> None:
    polygon = gm.from_wkt(
        'POLYGON M ((0 0 11, 4 0 12, 4 4 13, 0 0 11), (1 1 21, 2 1 22, 2 2 23, 1 1 21))'
    )
    source = gm.GeometryArray([
        None,
        gm.from_wkt('POLYGON M EMPTY'),
        None,
        polygon,
        polygon,
    ])
    match selection:
        case 'identity':
            selected = source
        case 'gather':
            selected = source[[0, 1, 4, 3]]
        case 'reverse':
            selected = source[::-1]

    opened = np.asarray(selected.coords).copy()
    opened[ring_end, ordinate] += 0.5

    def replace() -> gm.GeometryArray:
        if operation == 'with_coordinates':
            return selected.with_coordinates(opened)
        return selected.map_coordinates(lambda _coordinates: opened.copy())

    with pytest.raises(gm.InvalidGeometryError) as error:
        replace()

    assert str(error.value) == 'polygon ring must be closed'
    assert error.value.__notes__ == [f'while processing array element {expected_row}']


def test_mixed_coordinate_replacement_length_error_has_no_row_note() -> None:
    polygon = gm.from_wkt(
        'POLYGON M ((0 0 11, 4 0 12, 4 4 13, 0 0 11), (1 1 21, 2 1 22, 2 2 23, 1 1 21))'
    )
    array = gm.GeometryArray([None, gm.from_wkt('POLYGON M EMPTY'), polygon])
    coordinates = np.asarray(array.coords)

    with pytest.raises(
        gm.InvalidGeometryError, match='coordinates must have length'
    ) as error:
        array.with_coordinates(coordinates[:-1])

    assert not getattr(error.value, '__notes__', [])


def test_with_coordinates_nullable_polygon_carries_measure_columns() -> None:
    first = gm.from_wkt('POLYGON M ((0 0 10, 1 0 20, 1 1 30, 0 1 40, 0 0 10))')
    second = gm.from_wkt('POLYGON M ((2 2 50, 3 2 60, 3 3 70, 2 3 80, 2 2 50))')
    array = gm.GeometryArray([None, first, None, second])
    coordinates = np.asarray(array.coords)
    xs = coordinates[:, 0].copy()
    ys = coordinates[:, 1].copy()
    xs[1] = 1.5
    xs[6] = 3.5

    out = array.with_coordinates(x=xs, y=ys)

    assert out.is_missing.tolist() == [True, False, True, False]
    np.testing.assert_array_equal(
        out.coords.x, [0.0, 1.5, 1.0, 0.0, 0.0, 2.0, 3.5, 3.0, 2.0, 2.0]
    )
    np.testing.assert_array_equal(
        out.coords.m, [10, 20, 30, 40, 10, 50, 60, 70, 80, 50]
    )


def test_with_coordinates_per_axis_and_num_coordinates() -> None:
    line = gm.from_wkt('LINESTRING ZM (0 0 5 50, 1 1 6 60)')
    moved = line.with_coordinates(x=[10.0, 11.0], y=[20.0, 21.0])
    assert moved.to_wkt() == 'LINESTRING ZM (10 20 5 50, 11 21 6 60)'
    changed = line.with_coordinates(
        x=[10.0, 11.0], y=[20.0, 21.0], z=[7.0, 8.0], m=[70.0, 80.0]
    )
    assert changed.to_wkt() == 'LINESTRING ZM (10 20 7 70, 11 21 8 80)'
    assert line.num_coordinates == len(line.coords) == 2
    arr = gm.GeometryArray([line, None, gm.Point(0, 0)])
    np.testing.assert_array_equal(arr.num_coordinates, [2, 0, 1])
    assert int(arr.num_coordinates.sum()) == len(arr.coords)


def test_exact_count_and_byte_metadata_promote_for_shared_collections() -> None:
    geometry = gm.GeometryCollection([gm.Point(1, 2), gm.Point(3, 4)])
    for _ in range(127):
        geometry = gm.GeometryCollection([geometry, geometry])

    coordinate_count = 1 << 128
    coordinate_bytes = 1 << 132
    assert geometry.num_coordinates == coordinate_count
    assert geometry.nbytes == coordinate_bytes

    coords = geometry.coords
    assert coords.nbytes == coordinate_bytes
    assert repr(coords) == f'<Coordinates XY len={coordinate_count}>'
    assert coords[0] == (1.0, 2.0)
    iterator = iter(coords)
    assert next(iterator) == (1.0, 2.0)
    reverse_iterator = reversed(coords)
    assert next(reverse_iterator) == (3.0, 4.0)
    with pytest.raises(OverflowError, match='maximum Python sequence length'):
        len(coords)

    values = gm.GeometryArray([geometry, None, geometry])
    counts = values.num_coordinates
    assert counts.dtype == object
    assert counts.tolist() == [coordinate_count, 0, coordinate_count]
    assert values.nbytes == 2 * coordinate_bytes
    assert values.coords.nbytes == 2 * coordinate_bytes

    duplicated_points = gm.points([0.0, 1.0], [2.0, 3.0])[[1, 1]]
    assert duplicated_points.nbytes == 2 * 16
    assert duplicated_points.coords.nbytes == 2 * 16


def test_map_coordinates_matches_shapely_transform() -> None:
    shapely = pytest.importorskip('shapely')
    from shapely import transform as shapely_transform

    geom = gm.LineString([(0, 0), (1, 2), (3, 4)])
    mapped = geom.map_coordinates(lambda coords: coords + np.array([10.0, 20.0]))
    oracle = shapely_transform(
        shapely.from_wkt(geom.to_wkt()),
        lambda coords: coords + np.array([10.0, 20.0]),
    )
    assert mapped.to_wkt() == shapely.to_wkt(oracle, trim=True)


def test_with_coordinates_matches_shapely_result() -> None:
    shapely = pytest.importorskip('shapely')
    geom = gm.Polygon([(0, 0), (2, 0), (1, 1), (0, 0)])
    coords = np.asarray(geom.coords).copy()
    coords[:, 0] += 5.0
    coords[:, 1] += 7.0
    out = geom.with_coordinates(coords)
    oracle = shapely.set_coordinates(shapely.from_wkt(geom.to_wkt()), coords)
    assert out.to_wkt() == shapely.to_wkt(oracle, trim=True)


def test_heterogeneous_gc_coordinate_identity_is_noop() -> None:
    """D22: mixed-axes GeometryCollection set/map identity is an exact no-op.

    EXACT repro: ``GeometryCollection([Point Z, Point XY]).with_coordinates(g.coords)``
    and ``map_coordinates(lambda x: x)`` must preserve each member's axes
    (not force union-layout NaN padding into finite validation).
    """
    g = gm.GeometryCollection([gm.Point(1, 2, z=3), gm.Point(4, 5)])
    assert g.coordinate_axes == 'XYZ'
    assert [part.coordinate_axes for part in g.parts] == ['XYZ', 'XY']

    set_out = g.with_coordinates(g.coords)
    assert set_out == g
    assert [part.coordinate_axes for part in set_out.parts] == ['XYZ', 'XY']

    map_out = g.map_coordinates(lambda coords: coords)
    assert map_out == g
    assert [part.coordinate_axes for part in map_out.parts] == ['XYZ', 'XY']

    arr = gm.GeometryArray([g])
    assert arr.with_coordinates(arr.coords)[0] == g
    assert arr.map_coordinates(lambda coords: coords)[0] == g

    # Multi-row array with a heterogeneous GC row (exact D22 array gap repro).
    multi = gm.GeometryArray([g, gm.Point(0, 0)])
    map_multi = multi.map_coordinates(lambda coords: coords)
    assert map_multi[0] == g
    assert map_multi[1] == gm.Point(0, 0)
    assert [part.coordinate_axes for part in map_multi[0].parts] == ['XYZ', 'XY']
    set_multi = multi.with_coordinates(multi.coords)
    assert set_multi[0] == g
    assert set_multi[1] == gm.Point(0, 0)
    assert [part.coordinate_axes for part in set_multi[0].parts] == ['XYZ', 'XY']

    # Uniform geometries remain unchanged.
    uniform = gm.GeometryCollection([gm.Point(1, 2, z=3), gm.Point(4, 5, z=6)])
    assert uniform.with_coordinates(uniform.coords) == uniform
    assert uniform.map_coordinates(lambda coords: coords) == uniform

    # Uniform-axes array columnar path still applies non-trivial transforms.
    all_xy = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
        gm.Point(2.0, 3.0),
    ])
    scaled = all_xy.map_coordinates(lambda a: a * 2)
    assert scaled[0] == gm.LineString([(0.0, 0.0), (2.0, 2.0)])
    assert scaled[1] == gm.Point(4.0, 6.0)


def test_geometry_array_to_numpy_and_array_round_trip() -> None:
    pts = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    obj = pts.to_numpy()
    assert obj.dtype == object
    assert len(obj) == 2
    assert isinstance(obj[0], gm.Point)
    assert isinstance(obj[1], gm.Point)
    with pytest.raises(TypeError):
        pts.to_numpy(np.float64)
    round_trip = gm.GeometryArray(obj)
    assert len(round_trip) == 2
    assert gm.equals_exact(round_trip[0], pts[0])
    assert gm.equals_exact(round_trip[1], pts[1])


def test_array_accepts_shapely_via_geo_interface() -> None:
    pytest.importorskip('shapely')
    from shapely.geometry import Point as ShapelyPoint

    geoms = [ShapelyPoint(0, 0), ShapelyPoint(1, 1)]
    arr = gm.GeometryArray(geoms)
    assert len(arr) == 2
    assert gm.equals_exact(arr[0], gm.Point(0, 0))
    assert gm.equals_exact(arr[1], gm.Point(1, 1))


def test_coords_z_m_always_readonly_float64_nan_when_absent() -> None:
    coords = gm.Point(1, 2, crs=4326).coords
    for col in (coords.z, coords.m):
        assert isinstance(col, np.ndarray)
        assert col.dtype == np.float64
        assert col.flags.writeable is False
        assert col.shape == (1,)
        assert np.isnan(col).all()


def test_coordinate_exports_follow_carried_ordinates() -> None:
    line = gm.LineString([(0, 0), (1, 1)], z=[5, 6])
    assert list(line.coords) == [(0.0, 0.0, 5.0), (1.0, 1.0, 6.0)]
    assert np.asarray(line.coords).shape == (2, 3)
    assert set(line.coords.to_dict()) == {'x', 'y', 'z'}
    assert np.asarray(line.coords, dtype=np.float64).shape == (2, 3)

    flat = line.force_2d()
    assert list(flat.coords) == [(0.0, 0.0), (1.0, 1.0)]
    assert np.asarray(flat.coords).shape == (2, 2)
    assert set(flat.coords.to_dict()) == {'x', 'y'}
    assert np.asarray(flat.coords, dtype=np.float64).shape == (2, 2)


def _line_array(n: int, *, distinct: bool = False) -> gm.GeometryArray:
    """Packed lines — the storage shape whose flat index is CSR (log rows).

    When ``distinct`` is True each row has a unique offset so window/gather
    slices are value-unequal when their row sets differ.
    """
    if distinct:
        return gm.GeometryArray([
            gm.LineString([(float(i + r), float((i + r) % 5)) for i in range(16)])
            for r in range(n)
        ])
    line = gm.LineString([(float(i), float(i % 5)) for i in range(16)])
    return gm.GeometryArray([line] * n)


def test_coordinates_index_stops_at_first_hit_on_gathered_lines():
    """Gathered packed lines: first-hit index must not re-walk the tail."""
    # Distinct per-row geometry so a gather drops coordinates that only
    # lived on the excluded rows.
    arr = gm.GeometryArray([
        gm.LineString([(float(i), 0.0), (float(i), 1.0)]) for i in range(500)
    ])
    gathered = arr[::2].coords  # even rows only
    assert gathered.index((0.0, 0.0)) == 0
    assert gathered.index((2.0, 0.0)) == 2  # second even row, first vertex
    with pytest.raises(ValueError, match='not in Coordinates'):
        gathered.index((1.0, 0.0))  # odd row dropped by gather
    # Large gather: first hit is still position 0.
    big = gm.GeometryArray([
        gm.LineString([(float(i), 0.0), (float(i), 1.0)]) for i in range(4000)
    ])[::2].coords
    assert big.index(big[0]) == 0


def test_coordinates_equality_and_count_across_storage_shapes():
    """Equal-value equality / count across identity/window/gather/mixed/masked.

    Pins the bulk path, not a packed-points-only shortcut: windowed and
    gathered line coords, mixed multiparts, and missing masks must agree.
    """
    lines = _line_array(200, distinct=True)
    window = lines[10:50].coords
    assert window == lines[10:50].coords
    assert window != lines[11:51].coords  # first-mismatch (distinct rows)

    gather = lines[::2].coords
    assert gather == lines[::2].coords
    assert len(gather) == 100 * 16
    assert gather.count(gather[0]) >= 1

    # Mixed multiparts
    mixed = gm.GeometryArray([
        gm.Point(float(i), 0.0)
        if i % 2 == 0
        else gm.LineString([(0.0, 0.0), (1.0, 1.0)])
        for i in range(40)
    ])
    assert mixed.coords == mixed.coords
    assert mixed.coords != lines[:20].coords

    # Masked: missing rows contribute no coordinates
    masked = gm.GeometryArray([
        None if i % 3 == 0 else gm.Point(float(i), 1.0) for i in range(30)
    ])
    dense = gm.GeometryArray([gm.Point(float(i), 1.0) for i in range(30) if i % 3 != 0])
    assert list(masked.coords) == list(dense.coords)
    assert masked.coords == dense.coords

    # Z/M equality (finite ordinates — constructors reject non-finite)
    zm = gm.LineString([(0.0, 0.0), (1.0, 1.0)], z=[1.5, 2.0], m=[3.0, 4.0])
    zm2 = gm.LineString([(0.0, 0.0), (1.0, 1.0)], z=[1.5, 2.0], m=[3.0, 4.0])
    assert zm.coords == zm2.coords
    assert (
        zm.coords
        != gm.LineString([(0.0, 0.0), (1.0, 1.0)], z=[1.5, 9.0], m=[3.0, 4.0]).coords
    )

    # Reversed / slice stay consistent with the flattened order
    rev = list(reversed(list(window)))
    assert list(window)[::-1] == rev
    assert window[:5] == list(window)[:5]
    assert list(window)[:16] == list(lines[10:11].coords)


def test_coordinates_equality_cross_row_map_storage():
    """Value-equal Coordinates must compare equal across row-map shapes.

    A bulk path that answers False solely because RowSelection maps differ
    (identity vs full window, window/gather vs rebuilt identity) is wrong:
    flattened list equality is the oracle.
    """
    lines = _line_array(80, distinct=True)
    # Identity vs full-span slice (often Window {0, n} rather than Identity).
    assert list(lines.coords) == list(lines[:].coords)
    assert lines.coords == lines[:].coords

    window = lines[10:40].coords
    rebuilt_window = gm.GeometryArray(list(lines[10:40])).coords
    assert list(window) == list(rebuilt_window)
    assert window == rebuilt_window

    gather = lines[::2].coords
    rebuilt_gather = gm.GeometryArray(list(lines[::2])).coords
    assert list(gather) == list(rebuilt_gather)
    assert gather == rebuilt_gather

    # Packed points: same cross-map contract.
    pts = gm.GeometryArray([gm.Point(float(i), float(i % 3)) for i in range(60)])
    assert list(pts.coords) == list(pts[:].coords)
    assert pts.coords == pts[:].coords
    g_pts = pts[::2].coords
    rebuilt_pts = gm.GeometryArray(list(pts[::2])).coords
    assert list(g_pts) == list(rebuilt_pts)
    assert g_pts == rebuilt_pts

    # Polygons: window vs rebuild.
    polys = gm.GeometryArray([
        gm.box(float(i), 0.0, float(i) + 1.0, 1.0) for i in range(40)
    ])
    pw = polys[5:25].coords
    pr = gm.GeometryArray(list(polys[5:25])).coords
    assert list(pw) == list(pr)
    assert pw == pr

    # Unequal content still False across maps (distinct rows required).
    assert lines[10:40].coords != lines[11:41].coords
    assert lines[::2].coords != lines[1::2].coords


def test_coordinates_equality_ignores_csr_absolute_offsets():
    """Flattened equality must not require matching CSR absolute offsets.

    Different row partitions (or different window prefixes) can share the same
    visible vertex stream. A bulk path that compares absolute offset tables
    returns a false negative against ``list(coords)`` as the oracle.
    """
    # Identity: one 4-vertex line vs two 2-vertex lines — same verts in order.
    one = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0), (2.0, 2.0), (3.0, 3.0)])
    ])
    two = gm.GeometryArray([
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
        gm.LineString([(2.0, 2.0), (3.0, 3.0)]),
    ])
    assert list(one.coords) == list(two.coords)
    assert one.coords == two.coords

    # Windows with different prefix lengths → different absolute CSR bases.
    left = gm.GeometryArray([
        gm.LineString([(9.0, 9.0), (9.0, 8.0)]),  # 2-vert prefix
        gm.LineString([(1.0, 0.0), (2.0, 0.0), (3.0, 0.0)]),
        gm.LineString([(4.0, 0.0), (5.0, 0.0)]),
    ])
    right = gm.GeometryArray([
        gm.LineString([(8.0, 8.0), (8.0, 7.0), (8.0, 6.0)]),  # 3-vert prefix
        gm.LineString([(1.0, 0.0), (2.0, 0.0), (3.0, 0.0)]),
        gm.LineString([(4.0, 0.0), (5.0, 0.0)]),
    ])
    lw, rw = left[1:].coords, right[1:].coords
    assert list(lw) == list(rw)
    assert lw == rw

    # Distinct trailing content still compares unequal.
    other = gm.GeometryArray([
        gm.LineString([(8.0, 8.0), (8.0, 7.0), (8.0, 6.0)]),
        gm.LineString([(1.0, 0.0), (2.0, 0.0), (9.0, 9.0)]),  # last vertex differs
        gm.LineString([(4.0, 0.0), (5.0, 0.0)]),
    ])
    assert list(left[1:].coords) != list(other[1:].coords)
    assert left[1:].coords != other[1:].coords

    # Polygons: unequal prefix vertex counts, same visible window.
    left_polys = gm.GeometryArray([
        gm.Polygon([(9.0, 9.0), (9.0, 8.0), (8.0, 9.0), (9.0, 9.0)]),
        gm.box(1.0, 0.0, 3.0, 2.0),
        gm.box(4.0, 0.0, 5.0, 1.0),
    ])
    right_polys = gm.GeometryArray([
        gm.box(8.0, 8.0, 11.0, 11.0),
        gm.box(1.0, 0.0, 3.0, 2.0),
        gm.box(4.0, 0.0, 5.0, 1.0),
    ])
    lp, rp = left_polys[1:].coords, right_polys[1:].coords
    assert list(lp) == list(rp)
    assert lp == rp
    other_polys = gm.GeometryArray([
        gm.box(8.0, 8.0, 11.0, 11.0),
        gm.box(1.0, 0.0, 3.0, 2.0),
        gm.box(4.0, 0.0, 9.0, 1.0),
    ])
    assert list(left_polys[1:].coords) != list(other_polys[1:].coords)
    assert left_polys[1:].coords != other_polys[1:].coords


def test_coordinates_random_access_and_lazy_iter() -> None:
    """Random access crosses line boundaries and each iterator starts anew."""
    coords = gm.line_strings([
        [(13.0, 17.0), (19.0, 23.0)],
        [(29.0, 31.0), (37.0, 41.0)],
        [(43.0, 47.0), (53.0, 59.0)],
    ]).coords

    assert len(coords) == 6
    assert coords[0] == (13.0, 17.0)
    assert coords[3] == (37.0, 41.0)
    assert coords[-1] == (53.0, 59.0)
    iterator = iter(coords)
    assert next(iterator) == (13.0, 17.0)
    assert next(iterator) == (19.0, 23.0)
    assert next(iter(coords)) == (13.0, 17.0)


@pytest.mark.parametrize(
    ('wkt', 'expected'),
    [
        pytest.param(
            'LINESTRING (1 11, 2 12)',
            [(1.0, 11.0), (2.0, 12.0)],
            id='XY',
        ),
        pytest.param(
            'LINESTRING Z (1 11 21, 2 12 22)',
            [(1.0, 11.0, 21.0), (2.0, 12.0, 22.0)],
            id='XYZ',
        ),
        pytest.param(
            'LINESTRING M (1 11 31, 2 12 32)',
            [(1.0, 11.0, 31.0), (2.0, 12.0, 32.0)],
            id='XYM',
        ),
        pytest.param(
            'LINESTRING ZM (1 11 21 31, 2 12 22 32)',
            [(1.0, 11.0, 21.0, 31.0), (2.0, 12.0, 22.0, 32.0)],
            id='XYZM',
        ),
    ],
)
def test_to_nested_preserves_scalar_and_dense_line_axes(
    wkt: str, expected: list[tuple[float, ...]]
) -> None:
    line = gm.from_wkt(wkt)
    assert line.coords.to_nested() == expected

    dense = gm.GeometryArray([line, line])
    assert dense.coords.to_nested() == [expected, expected]


def test_to_nested_preserves_gather_order_and_skips_nullable_line_rows() -> None:
    lines = [
        gm.from_wkt('LINESTRING ZM (1 11 21 31, 2 12 22 32)'),
        gm.from_wkt('LINESTRING ZM (3 13 23 33, 4 14 24 34)'),
        gm.from_wkt('LINESTRING ZM (5 15 25 35, 6 16 26 36)'),
        gm.from_wkt('LINESTRING ZM (7 17 27 37, 8 18 28 38)'),
    ]
    expected = [
        [(1.0, 11.0, 21.0, 31.0), (2.0, 12.0, 22.0, 32.0)],
        [(3.0, 13.0, 23.0, 33.0), (4.0, 14.0, 24.0, 34.0)],
        [(5.0, 15.0, 25.0, 35.0), (6.0, 16.0, 26.0, 36.0)],
        [(7.0, 17.0, 27.0, 37.0), (8.0, 18.0, 28.0, 38.0)],
    ]
    source = gm.GeometryArray(lines)
    assert source.coords.to_nested() == expected
    assert source[[3, 0, 3, 1]].coords.to_nested() == [
        expected[3],
        expected[0],
        expected[3],
        expected[1],
    ]

    nullable = gm.GeometryArray([None, lines[1], None, lines[0], lines[3], None])
    assert nullable.coords.to_nested() == [expected[1], expected[0], expected[3]]
