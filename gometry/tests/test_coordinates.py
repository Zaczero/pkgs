"""The `Coordinates` view — flat columnar access, lazy iterators, XY-column
constructors, and parts/rings extraction helpers.
"""

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
    assert all(not column.flags.writeable for column in columns.values())
    mat = np.asarray(line.coords)
    assert mat.shape == (3, 2) and mat.dtype == np.float64
    assert mat.tolist() == [[0.0, 0.0], [1.0, 2.0], [3.0, 4.0]]
    assert mat.flags.writeable is False
    forced = np.asarray(line.coords.select('XYZ'))
    assert forced.shape == (3, 3) and np.isnan(forced[:, 2]).all()
    ragged = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 2, z=9)])
    assert list(ragged.coords) == [(0.0, 0.0), (1.0, 2.0, 9.0)]
    assert list(ragged.coords.select('XYZ')) == [(0.0, 0.0, None), (1.0, 2.0, 9.0)]
    selected = ragged.coords.select('XYZ').to_dict()
    np.testing.assert_array_equal(selected['x'], [0.0, 1.0])
    np.testing.assert_array_equal(selected['y'], [0.0, 2.0])
    np.testing.assert_allclose(selected['z'], [np.nan, 9.0], equal_nan=True)
    assert all(not column.flags.writeable for column in selected.values())
    assert list(gm.LineString([(0, 0), (1, 1)], z=[5, 6]).coords.select('XY')) == [
        (0.0, 0.0),
        (1.0, 1.0),
    ]


def test_coordinates_equality_compares_visible_values() -> None:
    xy = gm.LineString([(0, 0), (1, 1)]).coords
    assert xy == gm.LineString([(0, 0), (1, 1)]).coords
    assert xy == [(0, 0), (1, 1)]
    assert xy == xy.select('XY')
    assert xy != [(0, 0)]
    assert xy != [(0, 0), (1, 1), (2, 2)]
    assert xy != [(0, 0), (1, 2)]

    left = gm.LineString([(0, 0), (1, 1)], z=[5, 6]).coords.select('XY')
    right = gm.LineString([(0, 0), (1, 1)], z=[8, 9]).coords.select('XY')
    assert left == right
    assert left == list(left)
    assert list(left) == left

    padded = xy.select('XYZ')
    assert padded == [(0, 0, None), (1, 1, None)]
    assert padded == list(padded)
    assert padded != xy


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
    rings = gm.rings(polygon)
    assert list(gm.LineString([(0, 0), (1, 1)]).coords.select('XY')) == [
        (0.0, 0.0),
        (1.0, 1.0),
    ]
    assert coordinates[:3] == [(9.0, 9.0), (0.0, 0.0), (4.0, 0.0)]
    np.testing.assert_array_equal(indexes, [0] + [1] * 10)
    assert [part.to_wkt() for part in gm.parts(values)] == [
        item.to_wkt() for item in list(values)
    ]
    assert rings.crs == 'EPSG:4326'
    assert [ring.to_wkt() for ring in list(rings)] == [
        'LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)',
        'LINESTRING (1 1, 3 1, 3 3, 1 3, 1 1)',
    ]
    assert next(iter(polygon.coords.select('XYZ'))) == (0.0, 0.0, None)
    with pytest.raises(TypeError):
        gm.rings(gm.Point(0, 0))


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


def test_get_coordinates() -> None:
    line = gm.LineString([(0, 0), (1, 2), (3, 4)])
    coords = gm.get_coordinates(line)
    assert coords.shape == (3, 2)
    assert coords.dtype == np.float64
    assert coords.tolist() == [[0.0, 0.0], [1.0, 2.0], [3.0, 4.0]]
    arr = gm.GeometryArray([gm.Point(0, 0), gm.LineString([(1, 1), (2, 2)])])
    with_z = gm.get_coordinates(arr, axes='XYZ')
    assert with_z.shape == (3, 3)
    assert np.isnan(with_z).sum() == 3
    matrix, index = gm.get_coordinates(arr, return_index=True)
    assert matrix.shape == (3, 2)
    assert index.dtype == np.int64
    np.testing.assert_array_equal(index, [0, 1, 1])
    iterable = gm.get_coordinates([gm.Point(5, 6), None, gm.Point(7, 8)])
    np.testing.assert_array_equal(iterable, [[5.0, 6.0], [7.0, 8.0]])


def test_get_coordinates_return_index_packed_lines() -> None:
    """Packed identity lines: matrix + CSR-derived logical row index."""
    lines = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(2, 0), (3, 0), (4, 0)]),
        gm.LineString([(5, 0), (6, 0)]),
    ])
    matrix, index = gm.get_coordinates(lines, return_index=True)
    assert matrix.shape == (7, 2)
    assert matrix.dtype == np.float64
    assert matrix.flags.writeable is False
    assert index.dtype == np.int64
    assert index.flags.writeable is False
    np.testing.assert_array_equal(
        matrix,
        [[0, 0], [1, 0], [2, 0], [3, 0], [4, 0], [5, 0], [6, 0]],
    )
    np.testing.assert_array_equal(index, [0, 0, 1, 1, 1, 2, 2])
    np.testing.assert_array_equal(lines.coords.row_index, index)


def test_get_coordinates_return_index_polygons_with_holes() -> None:
    """Polygon shell+holes flatten depth-first; index spans every ring vertex."""
    poly = gm.Polygon(
        [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)],
        holes=[[(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]],
    )
    other = gm.Polygon([(10, 10), (11, 10), (11, 11), (10, 10)])
    arr = gm.GeometryArray([poly, other])
    matrix, index = gm.get_coordinates(arr, return_index=True)
    assert matrix.shape == (5 + 5 + 4, 2)
    np.testing.assert_array_equal(index, [0] * 10 + [1] * 4)
    np.testing.assert_array_equal(arr.coords.row_index, index)
    # Shell first vertex, then hole first vertex, then second polygon.
    np.testing.assert_array_equal(matrix[0], [0.0, 0.0])
    np.testing.assert_array_equal(matrix[5], [1.0, 1.0])
    np.testing.assert_array_equal(matrix[10], [10.0, 10.0])


def test_get_coordinates_return_index_mixed_array() -> None:
    mixed = gm.GeometryArray([
        gm.Point(0, 0),
        gm.LineString([(1, 1), (2, 2)]),
        gm.Polygon([(3, 3), (4, 3), (4, 4), (3, 3)]),
    ])
    matrix, index = gm.get_coordinates(mixed, return_index=True)
    assert matrix.shape == (1 + 2 + 4, 2)
    np.testing.assert_array_equal(index, [0, 1, 1, 2, 2, 2, 2])
    np.testing.assert_array_equal(matrix[0], [0.0, 0.0])
    np.testing.assert_array_equal(matrix[1], [1.0, 1.0])
    np.testing.assert_array_equal(matrix[3], [3.0, 3.0])


def test_get_coordinates_return_index_gathered_selection() -> None:
    """Gather preserves logical (not physical) row numbers in the index."""
    base = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 0)]),
        gm.LineString([(2, 0), (3, 0), (4, 0)]),
        gm.LineString([(5, 0), (6, 0)]),
    ])
    gathered = base[[2, 0]]
    matrix, index = gm.get_coordinates(gathered, return_index=True)
    assert matrix.tolist() == [[5.0, 0.0], [6.0, 0.0], [0.0, 0.0], [1.0, 0.0]]
    # Logical rows of the gathered view are 0 then 1 — not physical 2, 0.
    np.testing.assert_array_equal(index, [0, 0, 1, 1])
    np.testing.assert_array_equal(gathered.coords.row_index, index)
    # Window slice also keeps logical ids starting at 0.
    window = base[1:]
    _, win_index = gm.get_coordinates(window, return_index=True)
    np.testing.assert_array_equal(win_index, [0, 0, 0, 1, 1])


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
def test_set_coordinates_round_trips_coordinates_view(geom: gm.Geometry) -> None:
    out = geom.set_coordinates(geom.coords)
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
    ``(0, 3)`` (not ``(0, 2)``), and ``g.set_coordinates(g.coords)`` round-trips.
    """
    g = gm.from_wkt(wkt)
    assert g.coordinate_axes == axes
    assert g.coords.coordinate_axes == axes
    matrix = np.asarray(g.coords)
    assert matrix.shape == (0, width)
    assert matrix.dtype == np.float64

    out = g.set_coordinates(g.coords)
    assert out.coordinate_axes == axes
    assert out.geometry_type == g.geometry_type
    assert out.is_empty
    assert out.to_wkt() == g.to_wkt()
    assert np.asarray(out.coords).shape == (0, width)


def test_set_coordinates_array_mask_and_row_map_round_trip() -> None:
    arr = gm.GeometryArray([gm.Point(1, 2), None, gm.Point(3, 4)])
    out = arr.set_coordinates(arr.coords)
    assert list(out)[1] is None
    assert gm.equals_exact(out[0], gm.Point(1, 2))
    assert gm.equals_exact(out[2], gm.Point(3, 4))
    np.testing.assert_array_equal(out.num_coordinates, [1, 0, 1])
    assert int(out.num_coordinates.sum()) == len(out.coords)

    selected = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1), gm.Point(2, 2)])[::-1]
    selected_out = selected.set_coordinates(selected.coords)
    assert selected_out.to_wkt() == selected.to_wkt()


def test_set_coordinates_validation_errors() -> None:
    line = gm.LineString([(0, 0), (1, 1)])
    with pytest.raises(gm.InvalidGeometryError, match='length 2'):
        line.set_coordinates(np.array([[0.0, 0.0]]))
    with pytest.raises(gm.InvalidGeometryError, match='width 2'):
        line.set_coordinates(np.zeros((2, 3)))
    with pytest.raises(gm.InvalidGeometryError, match='finite'):
        line.set_coordinates(np.array([[0.0, 0.0], [np.nan, 1.0]]))
    with pytest.raises(gm.GeometryError, match='cannot mix'):
        line.set_coordinates(np.asarray(line.coords), x=[0.0, 1.0], y=[0.0, 1.0])
    with pytest.raises(gm.InvalidGeometryError, match='z coordinates require'):
        line.set_coordinates(x=[0.0, 1.0], y=[0.0, 1.0], z=[1.0, 2.0])

    poly = gm.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])
    opened = np.asarray(poly.coords).copy()
    opened[-1] = [9.0, 9.0]
    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        poly.set_coordinates(opened)


@pytest.mark.parametrize(
    'wkt',
    [
        'POLYGON Z ((0 0 1, 1 0 1, 0 1 1, 0 0 1))',
        'POLYGON M ((0 0 1, 1 0 1, 0 1 1, 0 0 1))',
        'POLYGON ZM ((0 0 1 2, 1 0 1 2, 0 1 1 2, 0 0 1 2))',
    ],
)
def test_n5_set_coordinates_rejects_active_ordinate_open_ring(wkt: str) -> None:
    """N5: Z/M-only open rings must fail set_coordinates (pack/pickle parity).

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
    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        poly.set_coordinates(coords)

    # Packed-array path (uniform polygon column)
    arr = gm.GeometryArray([poly])
    arr_coords = np.asarray(arr.coords).copy()
    arr_coords[-1, 2] = 9.0
    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        arr.set_coordinates(arr_coords)

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
    kept = poly.set_coordinates(keep)
    assert pickle.loads(pickle.dumps(kept)) == kept
    arr_keep = np.asarray(arr.coords).copy()
    arr_keep[0, 2] = 7.0
    arr_keep[-1, 2] = 7.0
    kept_arr = arr.set_coordinates(arr_keep)
    assert pickle.loads(pickle.dumps(kept_arr)) == kept_arr

    # Interior-only change still succeeds
    interior = np.asarray(poly.coords).copy()
    interior[1, 0] = 1.5
    moved = poly.set_coordinates(interior)
    assert pickle.loads(pickle.dumps(moved)) == moved


def test_n5_set_coordinates_xy_ring_unchanged() -> None:
    """2D rings still reject XY-open and accept closed replacements."""
    import pickle

    poly = gm.Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])
    opened = np.asarray(poly.coords).copy()
    opened[-1] = [9.0, 9.0]
    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        poly.set_coordinates(opened)
    kept = poly.set_coordinates(np.asarray(poly.coords).copy())
    assert pickle.loads(pickle.dumps(kept)) == kept
    arr = gm.GeometryArray([poly])
    kept_arr = arr.set_coordinates(np.asarray(arr.coords).copy())
    assert pickle.loads(pickle.dumps(kept_arr)) == kept_arr


@pytest.mark.parametrize(
    'select',
    [
        # Window / slice (non-identity row_map → replace_packed_coords_detached)
        lambda a: a[1:],
        lambda a: a[0:1],
        # Fancy gather
        lambda a: a[[1]],
        lambda a: a[[0, 2]],
    ],
    ids=['slice_tail', 'slice_head', 'gather_one', 'gather_two'],
)
def test_n5_set_coordinates_selection_rejects_z_open_ring(select) -> None:
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
    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        view.set_coordinates(opened)

    def open_measure(c: np.ndarray) -> np.ndarray:
        out = np.array(c, copy=True)
        out[-1, 2] = 9.0
        return out

    with pytest.raises(gm.InvalidGeometryError, match='ring must be closed'):
        view.map_coordinates(open_measure)

    # Closure-preserving: mutate only an interior vertex, then pickle-round-trip.
    keep = np.asarray(view.coords).copy()
    keep[1, 2] = 8.0  # second vertex is always interior on a 4-vertex ring
    kept = view.set_coordinates(keep)
    assert pickle.loads(pickle.dumps(kept)) == kept


def test_set_coordinates_per_axis_and_num_coordinates() -> None:
    line = gm.from_wkt('LINESTRING ZM (0 0 5 50, 1 1 6 60)')
    moved = line.set_coordinates(x=[10.0, 11.0], y=[20.0, 21.0])
    assert moved.to_wkt() == 'LINESTRING ZM (10 20 5 50, 11 21 6 60)'
    changed = line.set_coordinates(
        x=[10.0, 11.0], y=[20.0, 21.0], z=[7.0, 8.0], m=[70.0, 80.0]
    )
    assert changed.to_wkt() == 'LINESTRING ZM (10 20 7 70, 11 21 8 80)'
    assert line.num_coordinates == len(line.coords) == 2
    arr = gm.GeometryArray([line, None, gm.Point(0, 0)])
    np.testing.assert_array_equal(arr.num_coordinates, [2, 0, 1])
    assert int(arr.num_coordinates.sum()) == len(arr.coords)


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


def test_set_coordinates_matches_shapely_set_coordinates() -> None:
    shapely = pytest.importorskip('shapely')
    geom = gm.Polygon([(0, 0), (2, 0), (1, 1), (0, 0)])
    coords = np.asarray(geom.coords).copy()
    coords[:, 0] += 5.0
    coords[:, 1] += 7.0
    out = geom.set_coordinates(coords)
    oracle = shapely.set_coordinates(shapely.from_wkt(geom.to_wkt()), coords)
    assert out.to_wkt() == shapely.to_wkt(oracle, trim=True)


def test_d22_heterogeneous_gc_coordinate_identity_is_noop() -> None:
    """D22: mixed-axes GeometryCollection set/map identity is an exact no-op.

    EXACT repro: ``GeometryCollection([Point Z, Point XY]).set_coordinates(g.coords)``
    and ``map_coordinates(lambda x: x)`` must preserve each member's axes
    (not force union-layout NaN padding into finite validation).
    """
    g = gm.GeometryCollection([gm.Point(1, 2, z=3), gm.Point(4, 5)])
    assert g.coordinate_axes == 'XYZ'
    assert [part.coordinate_axes for part in g.parts] == ['XYZ', 'XY']

    set_out = g.set_coordinates(g.coords)
    assert set_out == g
    assert [part.coordinate_axes for part in set_out.parts] == ['XYZ', 'XY']

    map_out = g.map_coordinates(lambda coords: coords)
    assert map_out == g
    assert [part.coordinate_axes for part in map_out.parts] == ['XYZ', 'XY']

    arr = gm.GeometryArray([g])
    assert arr.set_coordinates(arr.coords)[0] == g
    assert arr.map_coordinates(lambda coords: coords)[0] == g

    # Multi-row array with a heterogeneous GC row (exact D22 array gap repro).
    multi = gm.GeometryArray([g, gm.Point(0, 0)])
    map_multi = multi.map_coordinates(lambda coords: coords)
    assert map_multi[0] == g
    assert map_multi[1] == gm.Point(0, 0)
    assert [part.coordinate_axes for part in map_multi[0].parts] == ['XYZ', 'XY']
    set_multi = multi.set_coordinates(multi.coords)
    assert set_multi[0] == g
    assert set_multi[1] == gm.Point(0, 0)
    assert [part.coordinate_axes for part in set_multi[0].parts] == ['XYZ', 'XY']

    # Uniform geometries remain unchanged.
    uniform = gm.GeometryCollection([gm.Point(1, 2, z=3), gm.Point(4, 5, z=6)])
    assert uniform.set_coordinates(uniform.coords) == uniform
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
