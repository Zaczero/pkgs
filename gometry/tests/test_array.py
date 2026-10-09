"""`GeometryArray` semantics — packed vs mixed parity, selection ops,
3D accessors, and method/function form parity.
"""

import sys

import gometry as gm
import numpy as np
import pytest


def assert_optional_float_array(actual: object, expected: list[float | None]) -> None:
    array = np.asarray(actual)
    want = np.array(
        [np.nan if value is None else value for value in expected], dtype=np.float64
    )
    assert array.dtype == np.float64
    assert array.flags.writeable is False
    np.testing.assert_allclose(array, want, equal_nan=True)


def test_packed_point_array_values_and_auto_packing() -> None:
    packed = gm.points([0, 1, 2], [10, 11, 12], z=[5, 6, 7], crs=4326)
    assert len(packed) == 3
    assert (packed[1]).to_wkt() == 'POINT Z (1 11 6)' and packed[1].crs == 'EPSG:4326'
    assert [(g).to_wkt() for g in packed[::2]] == [
        'POINT Z (0 10 5)',
        'POINT Z (2 12 7)',
    ]
    assert packed.total_bounds == (0.0, 10.0, 2.0, 12.0)
    np.testing.assert_allclose(
        packed.bounds,
        [(0.0, 10.0, 0.0, 10.0), (1.0, 11.0, 1.0, 11.0), (2.0, 12.0, 2.0, 12.0)],
    )
    assert packed.coords.coordinate_axes == 'XYZ'
    np.testing.assert_array_equal(packed.coords.row_index, [0, 1, 2])
    assert list(packed.coords.x) == [0.0, 1.0, 2.0]
    assert_optional_float_array(packed.min_z, [5.0, 6.0, 7.0])
    assert packed.coords.to_nested() == [
        [0.0, 10.0, 5.0],
        [1.0, 11.0, 6.0],
        [2.0, 12.0, 7.0],
    ]
    assert next(iter(packed[[2, 0]])).to_wkt() == 'POINT Z (2 12 7)'
    with pytest.raises(ValueError, match='finite'):
        gm.points([0], [0], z=[float('inf')])
    from_array = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1), gm.Point(2, 2)])
    np.testing.assert_array_equal(from_array.coords.row_index, [0, 1, 2])
    centroids = (gm.GeometryArray([gm.box(0, 0, 2, 2), gm.box(4, 4, 6, 6)])).centroid()
    assert [(g).to_wkt() for g in centroids] == ['POINT (1 1)', 'POINT (5 5)']
    assert [
        (g).to_wkt() for g in gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1, z=9)])
    ] == ['POINT (0 0)', 'POINT Z (1 1 9)']
    with pytest.raises(ValueError, match='matching CRS'):
        gm.distance(gm.points([0], [0], crs=4326), gm.points([0], [0], crs=3857))


def test_mixed_coordinate_axes_report_each_row() -> None:
    array = gm.GeometryArray([
        gm.Point(1, 2, z=3),
        gm.LineString([(4, 5), (6, 7)]),
        gm.box(8, 9, 10, 11),
        gm.Point(12, 13),
    ])
    assert array.has_z.tolist() == [True, False, False, False]


def test_array_bounds_with_empty_rows_and_empty_arrays() -> None:
    arr = gm.GeometryArray([gm.from_wkt('POINT EMPTY'), gm.box(0, 0, 2, 3)])
    np.testing.assert_allclose(
        arr.bounds,
        [[np.nan, np.nan, np.nan, np.nan], [0.0, 0.0, 2.0, 3.0]],
        equal_nan=True,
    )
    assert arr.bounds.dtype == np.float64
    assert arr.bounds.flags.writeable is False
    assert arr.total_bounds == (0.0, 0.0, 2.0, 3.0)
    empty = gm.GeometryArray([])
    assert empty.bounds.shape == (0, 4)
    assert empty.total_bounds is None
    assert empty.geometry_type == []


@pytest.mark.parametrize(
    'dtype',
    [np.dtype(object), object, 'O', 'object'],
    ids=['dtype', 'type', 'O', 'object'],
)
def test_geometry_array_numpy_protocol_accepts_object_dtype_spellings(
    dtype: object,
) -> None:
    array = gm.GeometryArray([gm.Point(0, 1)])

    direct = array.__array__(dtype=dtype)
    via_numpy = np.asarray(array, dtype=dtype)
    for result in (direct, via_numpy):
        assert result.dtype == np.dtype(object)
        assert result[0].to_wkt() == 'POINT (0 1)'

    with pytest.raises(gm.GeometryError, match='dtype must be object or None'):
        array.__array__(dtype=np.float64)
    with pytest.raises(gm.GeometryError, match='dtype must be object or None'):
        np.asarray(array, dtype='int64')


def test_array_with_crs_strips_and_relabels_the_whole_frame() -> None:
    pts = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326)
    plain = (pts).with_crs(None)
    assert plain.crs is None and plain[0].crs is None
    with pytest.raises(ValueError, match='overwrite=True'):
        pts.with_crs(('EPSG', 3857))
    relabelled = pts.with_crs(('EPSG', 3857), overwrite=True)
    assert relabelled.crs == 'EPSG:3857'
    assert [p.crs for p in relabelled] == ['EPSG:3857', 'EPSG:3857']
    assert relabelled.to_arrow().type.extension_name == 'geoarrow.point'


def test_mixed_axes_arrays_summarize_honestly() -> None:
    mixed = gm.GeometryArray([
        gm.from_wkt('POINT Z (1 2 3)'),
        gm.from_wkt('POINT M (1 2 9)'),
    ])
    assert mixed.coordinate_axes == ['XYZ', 'XYM']
    assert mixed.common_coordinate_axes is None
    np.testing.assert_array_equal(mixed.has_z, [True, False])
    np.testing.assert_array_equal(mixed.has_m, [False, True])
    assert not mixed.has_z.flags.writeable
    assert not mixed.has_m.flags.writeable
    with pytest.raises(ValueError, match='got mixed \\(XYZ, XYM\\)'):
        gm.require(mixed, axes='XYZM')
    with pytest.raises(gm.InvalidGeometryError) as mismatch:
        gm.require(mixed, axes='XYZ')
    assert (mismatch.value.row, mismatch.value.code, mismatch.value.path) == (
        1,
        'coordinate_axes_mismatch',
        '$',
    )
    uniform = gm.GeometryArray([gm.from_wkt('POINT Z (1 2 3)')])
    assert uniform.coordinate_axes == ['XYZ']
    assert uniform.common_coordinate_axes == 'XYZ'
    assert gm.GeometryArray([]).coordinate_axes == []
    assert gm.GeometryArray([]).common_coordinate_axes == 'XY'


def test_require_preserves_missing_rows_in_a_geometry_array() -> None:
    rows = gm.GeometryArray([gm.Point(1.0, 2.0), None])

    required = gm.require(rows)

    assert required.is_missing.tolist() == [False, True]
    assert required[1] is None


@pytest.mark.parametrize(
    'source_wkts',
    [
        pytest.param(
            ['POINT Z (0 0 1)', 'POINT Z (1 1 2)'],
            id='points',
        ),
        pytest.param(
            ['LINESTRING Z (0 0 1, 1 1 2)', 'LINESTRING Z (2 2 3, 3 3 4)'],
            id='lines',
        ),
        pytest.param(
            [
                'POLYGON Z ((0 0 1, 1 0 2, 1 1 3, 0 0 1))',
                'POLYGON Z ((2 2 4, 3 2 5, 3 3 6, 2 2 4))',
            ],
            id='polygons',
        ),
    ],
)
def test_require_axes_preserves_logical_rows_for_null_leading_storage(
    source_wkts: list[str],
) -> None:
    source = gm.from_wkt(source_wkts)
    missing = gm.GeometryArray([None, source[0], source[1]])
    with pytest.raises(gm.InvalidGeometryError) as missing_error:
        gm.require(missing, axes='XY')
    assert (
        missing_error.value.row,
        missing_error.value.code,
        missing_error.value.path,
    ) == (
        1,
        'coordinate_axes_mismatch',
        '$',
    )

    gathered = source[[1, 0]]
    with pytest.raises(gm.InvalidGeometryError) as gathered_error:
        gm.require(gathered, axes='XY')
    assert (
        gathered_error.value.row,
        gathered_error.value.code,
        gathered_error.value.path,
    ) == (
        0,
        'coordinate_axes_mismatch',
        '$',
    )


def test_require_axes_preserves_logical_row_for_sliced_groups() -> None:
    groups = gm.GeometryArray([
        gm.MultiPoint([(0, 0), (1, 1)]),
        gm.MultiPoint([(2, 2), (3, 3)]),
    ]).parts[1:]
    with pytest.raises(gm.InvalidGeometryError) as groups_error:
        gm.require(groups, axes='XYZ')
    assert (
        groups_error.value.row,
        groups_error.value.code,
        groups_error.value.path,
    ) == (
        0,
        'coordinate_axes_mismatch',
        '$',
    )


def test_require_axes_reports_typed_empty_axes_mismatch() -> None:
    typed_empty = gm.GeometryArray([gm.from_wkt('POINT Z EMPTY')])
    with pytest.raises(gm.InvalidGeometryError) as empty_error:
        gm.require(typed_empty, axes='XY')
    assert (empty_error.value.row, empty_error.value.code, empty_error.value.path) == (
        0,
        'coordinate_axes_mismatch',
        '$',
    )


def test_require_axes_rejects_axes_before_validity() -> None:
    invalid = gm.Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])
    mixed = gm.GeometryArray([gm.Point(0, 0, z=1), invalid])
    with pytest.raises(gm.InvalidGeometryError) as axes_error:
        gm.require(mixed, axes='XYZ')
    assert (axes_error.value.row, axes_error.value.code, axes_error.value.path) == (
        1,
        'coordinate_axes_mismatch',
        '$',
    )


def test_require_axes_rejects_crs_before_axes() -> None:
    mixed = gm.GeometryArray([
        gm.Point(0, 0, z=1, crs=4326),
        gm.Point(1, 1, crs=4326),
    ])
    with pytest.raises(gm.CRSMismatchError):
        gm.require(mixed, crs=3857, axes='XYZ')


def test_packed_axes_survive_selection_and_missing_rows_for_every_storage_kind() -> (
    None
):
    sources = [
        gm.GeometryArray([
            gm.from_wkt('POINT Z (0 0 1)'),
            gm.from_wkt('POINT Z (1 1 2)'),
        ]),
        gm.GeometryArray([
            gm.from_wkt('LINESTRING Z (0 0 1, 1 1 2)'),
            gm.from_wkt('LINESTRING Z (2 2 3, 3 3 4)'),
        ]),
        gm.GeometryArray([
            gm.from_wkt('POLYGON Z ((0 0 1, 1 0 2, 1 1 3, 0 0 1))'),
            gm.from_wkt('POLYGON Z ((2 2 4, 3 2 5, 3 3 6, 2 2 4))'),
        ]),
    ]
    for source in sources:
        assert source[[1, 0]].common_coordinate_axes == 'XYZ'
        assert gm.GeometryArray([source[1], None, source[0]]).coordinate_axes == [
            'XYZ',
            None,
            'XYZ',
        ]
        all_missing = gm.GeometryArray([None, source[0]])[[0]]
        assert all_missing.coordinate_axes == [None]
        assert all_missing.common_coordinate_axes == source.common_coordinate_axes
        assert not all_missing.has_z.any()


def test_fancy_index_preserves_nullable_axes_and_frame_metadata() -> None:
    array = gm.GeometryArray(
        [
            gm.Point(1, 2, z=3, m=4, crs=4326, epoch=2020.0),
            None,
            gm.Point(5, 6, z=7, m=8, crs=4326, epoch=2020.0),
        ],
        crs=4326,
        epoch=2020.0,
    )

    selected = array[[2, 1, 0]]
    assert [None if row is None else tuple(row.coords[0]) for row in selected] == [
        (5.0, 6.0, 7.0, 8.0),
        None,
        (1.0, 2.0, 3.0, 4.0),
    ]
    assert selected.coordinate_axes == ['XYZM', None, 'XYZM']
    assert selected.is_missing.tolist() == [False, True, False]
    assert selected.crs is not None and selected.crs.to_epsg() == 4326
    assert selected.epoch == 2020.0

    masked = array[[False, True, True]]
    assert [None if row is None else tuple(row.coords[0]) for row in masked] == [
        None,
        (5.0, 6.0, 7.0, 8.0),
    ]
    assert masked.coordinate_axes == [None, 'XYZM']
    assert masked.is_missing.tolist() == [True, False]


def test_mixed_axes_delaunay_triangles_matches_rowwise() -> None:
    sites_xy = gm.MultiPoint([(0, 0), (4, 0), (4, 4), (0, 4), (2, 2)])
    sites_z = gm.from_wkt('MULTIPOINT Z (0 0 0, 4 0 0, 4 4 0, 0 4 0, 2 2 0)')
    mixed = gm.GeometryArray([sites_z, sites_xy])
    assert mixed.common_coordinate_axes is None
    # Array tessellation is per-row Groups: each row's triangles match the
    # scalar op on that row (parity), preserving mixed-axis layout per row.
    groups = mixed.triangulate(method='delaunay')
    assert groups[0] == sites_z.triangulate(method='delaunay')
    assert groups[1] == sites_xy.triangulate(method='delaunay')


def test_array_parts_preserve_source_row_grouping() -> None:
    rows = gm.GeometryArray([
        gm.MultiPoint([(0, 0), (1, 1)]),
        gm.Point(2, 2),
        gm.from_wkt('GEOMETRYCOLLECTION EMPTY'),
        None,
    ])

    groups = rows.parts

    assert len(groups) == 4
    assert groups.counts.tolist() == [2, 1, 0, 0]
    assert groups[0].to_wkt() == ['POINT (0 0)', 'POINT (1 1)']
    assert groups[1].to_wkt() == ['POINT (2 2)']
    assert len(groups[2]) == len(groups[3]) == 0
    assert groups.offsets.tolist() == [0, 2, 3, 3, 3]


def test_groups_counts_dtype_values_and_readonly() -> None:
    """``Groups.counts`` is a read-only int64 ndarray aligned with row lengths."""
    rows = gm.GeometryArray([
        gm.MultiPoint([(0, 0), (1, 1), (2, 2)]),
        gm.Point(3, 3),
        gm.from_wkt('GEOMETRYCOLLECTION EMPTY'),
        None,
        gm.MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]),
    ])
    groups = rows.parts
    counts = groups.counts
    assert isinstance(counts, np.ndarray)
    assert counts.dtype == np.dtype('int64')
    assert counts.shape == (len(groups),)
    assert counts.tolist() == [3, 1, 0, 0, 2]
    assert not counts.flags.writeable
    # counts[i] equals len(groups[i]) for every row.
    for i, count in enumerate(counts.tolist()):
        assert len(groups[i]) == count
    # Cell hierarchy Groups share the same contract.
    cells = gm.CellArray(
        [gm.H3Cell(0.0, 0.0, resolution=5), gm.H3Cell(10.0, 10.0, resolution=5)],
        type=gm.H3Cell,
    )
    child_groups = cells.children(7)
    assert child_groups.counts.dtype == np.dtype('int64')
    assert not child_groups.counts.flags.writeable
    assert child_groups.counts.tolist() == [
        cells[0].children_count(7),
        cells[1].children_count(7),
    ]


def test_mixed_axis_geometry_collection_delaunay_triangles_does_not_panic() -> None:
    sites = gm.GeometryCollection([
        gm.Point(0, 0),
        gm.Point(1, 0, z=1),
        gm.Point(0, 1),
        gm.Point(1, 1, z=2),
    ])
    triangles = sites.triangulate(method='delaunay')
    assert len(triangles) > 0
    assert all(triangle.geometry_type == 'Polygon' for triangle in triangles)


def test_require_crs_reports_frames_without_debug_formatting() -> None:
    assert gm.require(gm.GeometryArray([]), crs=4326).crs == 'EPSG:4326'
    tagged = gm.GeometryArray([], crs=('EPSG', 4326))
    assert gm.require(tagged, crs=gm.CRS(4326)).crs == 'EPSG:4326'
    with pytest.raises(ValueError, match='expected CRS "EPSG:3857", got "EPSG:4326"'):
        gm.require(gm.Point(0, 0, crs=4326), crs=3857)


def test_array_iterator_is_a_lazy_iterator_object() -> None:
    iterator = iter(gm.points([0, 1, 2], [0, 1, 2]))
    assert type(iterator).__name__ == 'GeometryArrayIterator'
    assert iter(iterator) is iterator
    assert (next(iterator)).to_wkt() == 'POINT (0 0)'
    assert [(g).to_wkt() for g in iterator] == ['POINT (1 1)', 'POINT (2 2)']
    with pytest.raises(StopIteration):
        next(iterator)


def test_geometry_array_selection_take_filter_concat_is_valid() -> None:
    array = gm.GeometryArray(
        [gm.Point(0, 0), gm.LineString([(0, 0), (1, 1)]), gm.box(0, 0, 1, 1)], crs=4326
    )
    np.testing.assert_array_equal(array.is_valid, [True, True, True])
    taken = array[[2, 0, -1]]
    assert [g.geometry_type for g in taken] == ['Polygon', 'Point', 'Polygon']
    assert taken.crs == 'EPSG:4326'
    kept = array[[True, False, True]]
    assert [g.geometry_type for g in kept] == ['Point', 'Polygon']
    with pytest.raises(ValueError, match='mask length'):
        array[[True]]
    more = gm.GeometryArray([gm.Point(5, 5)], crs=4326)
    joined = array.concat(more)
    assert len(joined) == 4 and joined.crs == 'EPSG:4326'
    with pytest.raises(ValueError, match='shared CRS'):
        array.concat(gm.GeometryArray([gm.Point(9, 9)]))
    with pytest.raises(IndexError, match='out of range'):
        array[[99]]
    bowtie = gm.from_wkt('POLYGON ((0 0, 1 1, 1 0, 0 1, 0 0))')
    np.testing.assert_array_equal(
        gm.GeometryArray([gm.Point(0, 0), bowtie]).is_valid, [True, False]
    )


def test_geometry_array_fancy_getitem_routes_to_take_and_filter() -> None:
    array = gm.GeometryArray([
        gm.Point(0, 0),
        gm.GeometryCollection([]),
        gm.Point(2, 2),
    ])
    assert [tuple(geom.coords[0]) for geom in array[[2, 0, -1]]] == [
        (2.0, 2.0),
        (0.0, 0.0),
        (2.0, 2.0),
    ]
    assert [tuple(geom.coords[0]) for geom in array[[True, False, True]]] == [
        (0.0, 0.0),
        (2.0, 2.0),
    ]
    empty = array[array.is_empty]
    assert [geom.geometry_type for geom in empty] == ['GeometryCollection']
    assert empty.is_empty.tolist() == [True]
    assert [
        tuple(geom.coords[0]) for geom in array[np.array([2, 0], dtype=np.int32)]
    ] == [
        (2.0, 2.0),
        (0.0, 0.0),
    ]
    assert [
        tuple(geom.coords[0])
        for geom in array[np.array([0, 1, 2], dtype=np.uint64)[::2]]
    ] == [(0.0, 0.0), (2.0, 2.0)]
    assert tuple(array[np.array(2, dtype=np.int64)].coords[0]) == (2.0, 2.0)
    assert tuple(array[np.int64(2)].coords[0]) == (2.0, 2.0)
    strided_mask = np.array([True, False, False, True, True, False])[::2]
    assert [tuple(geom.coords[0]) for geom in array[strided_mask]] == [
        (0.0, 0.0),
        (2.0, 2.0),
    ]
    non_empty = gm.GeometryArray([gm.Point(0, 0), gm.Point(2, 2)])
    ids = gm.SpatialIndex(non_empty).query(gm.box(-1, -1, 0.5, 0.5))
    assert isinstance(ids, np.ndarray)
    assert ids.dtype == np.int64
    assert not ids.flags.writeable
    assert list(non_empty[ids]) == [non_empty[0]]
    assert len(array[[]]) == 0
    with pytest.raises(TypeError, match='boolean scalar'):
        array[True]
    with pytest.raises(TypeError, match='boolean scalar'):
        array[np.bool_(True)]
    with pytest.raises(TypeError, match='boolean scalar'):
        array[np.array(True)]
    with pytest.raises(TypeError, match='integer or boolean dtype'):
        array[np.array([1.0])]
    with pytest.raises(TypeError, match='zero- or one-dimensional'):
        array[np.array([[0, 1]])]
    with pytest.raises(ValueError, match='mask length'):
        array[[True]]


def _indexing_receivers() -> list[tuple[object, object]]:
    cell = gm.H3Cell('8928308280fffff')
    cells = gm.CellArray([cell, cell.parent(resolution=8), cell], type=gm.H3Cell)
    edges = gm.H3EdgeArray(list(cell.edges)[:3])
    vertices = gm.H3VertexArray(list(cell.vertices)[:3])
    return [
        (
            gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1), gm.Point(2, 2)]),
            lambda value: tuple(value.coords[0]),
        ),
        (cells, lambda value: value.id),
        (edges, lambda value: value.id),
        (vertices, lambda value: value.id),
    ]


@pytest.mark.parametrize(
    'receiver', range(4), ids=['geometry', 'cells', 'edges', 'vertices']
)
@pytest.mark.parametrize(
    'selector',
    [
        slice(1, None, sys.maxsize),
        slice(2, None, sys.maxsize),
        slice(None, None, sys.maxsize),
        slice(None, None, -sys.maxsize - 1),
        slice(1, None, 1 << 100),
        slice(1, 1, sys.maxsize),
    ],
)
def test_slice_with_extreme_step_matches_python(receiver: int, selector: slice) -> None:
    array, identity = _indexing_receivers()[receiver]
    for selected in (array, array[[2, 0, 1]], array[1:]):
        expected = [identity(value) for value in list(selected)[selector]]
        assert [identity(value) for value in selected[selector]] == expected


def test_cell_array_slices_compose_logical_rows_with_physical_ids() -> None:
    source_cell = gm.H3Cell(13.4, 52.5, resolution=5)
    values = list(source_cell.grid_ring(1))[:5]
    cells = gm.CellArray(values, type=gm.H3Cell)

    selected = cells[[4, 1]]
    assert [value.id for value in selected[0:1]] == [cells[4].id]

    masked = cells[np.array([False, True, False, False, True])]
    assert [value.id for value in masked[0:1]] == [cells[1].id]

    assert list(cells[5:]) == []
    assert list(cells[5:5]) == []
    assert [value.id for value in cells[4:5]] == [cells[4].id]


@pytest.mark.parametrize(
    'dtype',
    ['i1', 'i2', 'i4', 'i8', 'u1', 'u2', 'u4', 'u8'],
    ids=['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64'],
)
def test_opposite_endian_numpy_integer_indices_cover_all_receivers(dtype: str) -> None:
    byteorder = '>' if sys.byteorder == 'little' else '<'
    dtype = f'{byteorder}{dtype}'
    receivers = _indexing_receivers()

    # The strided view is deliberately non-contiguous; its visible values are [2, 0].
    selector = np.array([2, 99, 0], dtype=dtype)[::2]
    for array, identity in receivers:
        assert [identity(value) for value in array[selector]] == [
            identity(array[2]),
            identity(array[0]),
        ]


def test_opposite_endian_numpy_scalar_and_negative_indices_cover_all_receivers() -> (
    None
):
    byteorder = '>' if sys.byteorder == 'little' else '<'
    receivers = _indexing_receivers()
    for array, identity in receivers:
        scalar = np.array(2, dtype=f'{byteorder}i8')
        assert identity(array[scalar]) == identity(array[2])
        selected = array[np.array([-1, 0], dtype=f'{byteorder}i8')]
        assert [identity(value) for value in selected] == [
            identity(array[-1]),
            identity(array[0]),
        ]


def test_numpy_integer_index_overflow_is_an_index_error_for_all_receivers() -> None:
    byteorder = '>' if sys.byteorder == 'little' else '<'
    maximum = np.iinfo(np.uint64).max
    selector = np.array([maximum], dtype=f'{byteorder}u8')
    scalar = np.array(maximum, dtype=f'{byteorder}u8')
    for array, _identity in _indexing_receivers():
        with pytest.raises(IndexError) as excinfo:
            array[selector]
        assert type(excinfo.value) is IndexError
        with pytest.raises(IndexError) as excinfo:
            array[scalar]
        assert type(excinfo.value) is IndexError


@pytest.mark.parametrize('index', [10**100, -(10**100)], ids=['positive', 'negative'])
def test_scalar_python_index_overflow_is_an_index_error(index: int) -> None:
    receivers = _indexing_receivers()
    for array, _identity in receivers:
        with pytest.raises(IndexError):
            array[index]

    point = gm.Point(0, 0)
    collection = gm.GeometryCollection([point])
    view = gm.GeometryArray([point, gm.Point(1, 1)]).coords
    coordinates = np.asarray(view, dtype=np.float64)
    indexes = view.row_index
    assert point.coords[0] == point.coords[-1] == (0.0, 0.0)
    assert collection.parts[0] == collection.parts[-1]
    assert coordinates.tolist() == [[0.0, 0.0], [1.0, 1.0]]
    assert indexes.tolist() == [0, 1]
    for view in (point.coords, collection.parts, coordinates, indexes):
        with pytest.raises(IndexError):
            view[index]


def test_scalar_tuple_list_and_replace_at_index_boundaries() -> None:
    array = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    assert tuple(array[0].coords[0]) == (0.0, 0.0)
    assert tuple(array[-1].coords[0]) == (1.0, 1.0)

    for index in (10**100, -(10**100)):
        with pytest.raises(IndexError):
            array[(index,)]
        with pytest.raises(IndexError):
            array[[index]]
        with pytest.raises(IndexError):
            array._replace_at([index], [None])

    with pytest.raises(TypeError):
        array[1.5]
    with pytest.raises(TypeError):
        array[True]
    with pytest.raises(TypeError):
        array._replace_at([1.5], [None])


def test_empty_replace_at_preserves_nullable_zm_values_and_frame() -> None:
    array = gm.GeometryArray(
        [
            gm.Point(1, 2, z=3, m=4, crs=4326, epoch=2020.0),
            None,
        ],
        crs=4326,
        epoch=2020.0,
    )
    result = array._replace_at([], [])

    assert list(result.coords) == [(1.0, 2.0, 3.0, 4.0)]
    assert list(array.coords) == [(1.0, 2.0, 3.0, 4.0)]
    assert result.coordinate_axes == array.coordinate_axes == ['XYZM', None]
    assert result.is_missing.tolist() == array.is_missing.tolist() == [False, True]
    assert result.crs is not None and result.crs.to_epsg() == 4326
    assert result.epoch == 2020.0


def test_dense_geometry_replace_does_not_change_source() -> None:
    array = gm.points([0.0, 1.0, 2.0], [10.0, 11.0, 12.0], crs=4326)
    before = [(0.0, 10.0), (1.0, 11.0), (2.0, 12.0)]

    result = array._replace_at([1], [gm.Point(9.0, 19.0, crs=4326)])

    assert list(result.coords) == [(0.0, 10.0), (9.0, 19.0), (2.0, 12.0)]
    assert result.is_missing.tolist() == [False, False, False]
    assert list(array.coords) == before
    assert array.coordinate_axes == ['XY', 'XY', 'XY']
    assert result.crs == array.crs


def test_duplicate_replace_at_positions_apply_last_missing_state() -> None:
    array = gm.points([0.0, 1.0, 2.0], [10.0, 11.0, 12.0])

    present = array._replace_at([1, 1], [None, gm.Point(9.0, 19.0)])
    missing = array._replace_at([1, 1], [gm.Point(9.0, 19.0), None])

    assert [None if row is None else row.coords[0] for row in present] == [
        (0.0, 10.0),
        (9.0, 19.0),
        (2.0, 12.0),
    ]
    assert present.is_missing.tolist() == [False, False, False]
    assert [None if row is None else row.coords[0] for row in missing] == [
        (0.0, 10.0),
        None,
        (2.0, 12.0),
    ]
    assert missing.is_missing.tolist() == [False, True, False]
    assert missing.coordinate_axes == ['XY', None, 'XY']
    assert list(array.coords) == [(0.0, 10.0), (1.0, 11.0), (2.0, 12.0)]


def test_strided_numpy_boolean_masks_cover_all_receivers() -> None:
    selector = np.array([True, False, True, False, False])[::2]
    for array, identity in _indexing_receivers():
        assert [identity(value) for value in array[selector]] == [
            identity(array[0]),
            identity(array[1]),
        ]


def test_numpy_integer_index_rejects_rank_two() -> None:
    array = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1)])
    with pytest.raises(TypeError, match='zero- or one-dimensional'):
        array[np.array([[0, 1]], dtype='>i8' if sys.byteorder == 'little' else '<i8')]


def test_numpy_index_ignores_an_ndarray_subclass_tobytes_override() -> None:
    class ForgedBytes(np.ndarray):
        def tobytes(self, *args: object, **kwargs: object) -> bytes:
            del args, kwargs
            return np.array([1, 1], dtype=np.int64).tobytes()

    selector = np.array([2, 0], dtype=np.int64).view(ForgedBytes)
    for array, identity in _indexing_receivers():
        assert [identity(value) for value in array[selector]] == [
            identity(array[2]),
            identity(array[0]),
        ]


def test_geometry_array_3d_accessors_match_scalar() -> None:
    arr = gm.GeometryArray(
        [
            gm.from_wkt('POINT Z (0 0 5)'),
            gm.from_wkt('LINESTRING Z (0 0 1, 3 4 5)'),
            gm.Point(9, 9),
        ],
        crs=3857,
    )
    assert_optional_float_array(arr.min_z, [g.min_z for g in arr])
    assert_optional_float_array(arr.max_z, [g.max_z for g in arr])
    np.testing.assert_allclose((arr).length_3d, [(g).length_3d for g in arr])
    assert_optional_float_array(arr.min_z, [5.0, 1.0, None])
    assert (arr).length_3d[1] == pytest.approx(6.4031242374328485)


def test_packed_z_accessors_use_column_paths() -> None:
    points = gm.points([0, 1], [0, 1], z=[5, 9], crs=4326)
    assert_optional_float_array(points.min_z, [5.0, 9.0])
    assert_optional_float_array(points.max_z, [5.0, 9.0])
    lines = gm.GeometryArray(
        [
            gm.from_wkt('LINESTRING Z (0 0 1, 3 4 5)'),
            gm.from_wkt('LINESTRING Z (0 0 8, 1 0 2)'),
            gm.from_wkt('LINESTRING EMPTY'),
        ],
        crs=4326,
    )
    assert_optional_float_array(lines.min_z, [g.min_z for g in lines])
    assert_optional_float_array(lines.max_z, [g.max_z for g in lines])
    assert_optional_float_array(lines.min_z, [1.0, 2.0, None])
    lines_for_length = gm.GeometryArray(
        [
            gm.from_wkt('LINESTRING Z (0 0 1, 3 4 5)'),
            gm.from_wkt('LINESTRING Z (0 0 8, 1 0 2)'),
            gm.from_wkt('LINESTRING EMPTY'),
        ],
        crs=3857,
    )
    np.testing.assert_allclose(
        (lines_for_length).length_3d, [(g).length_3d for g in lines_for_length]
    )
    assert (lines_for_length).length_3d[0] == pytest.approx(6.4031242374328485)
    shell_only = gm.from_wkt('POLYGON Z ((0 0 10, 4 0 10, 4 4 10, 0 4 10, 0 0 10))')
    with_hole = gm.from_wkt(
        'POLYGON Z ((0 0 10, 4 0 10, 4 4 10, 0 4 10, 0 0 10), (1 1 1, 2 1 1, 2 2 1, 1 2 1, 1 1 1))'
    )
    polygons = gm.GeometryArray(
        [shell_only, with_hole, gm.from_wkt('POLYGON EMPTY')], crs=4326
    )
    assert_optional_float_array(polygons.min_z, [g.min_z for g in polygons])
    assert_optional_float_array(polygons.max_z, [g.max_z for g in polygons])
    assert_optional_float_array(polygons.min_z, [10.0, 1.0, None])
    polygons_for_length = gm.GeometryArray(
        [shell_only, with_hole, gm.from_wkt('POLYGON EMPTY')], crs=3857
    )
    np.testing.assert_allclose(
        (polygons_for_length).length_3d, [(g).length_3d for g in polygons_for_length]
    )
    xy_lines = gm.GeometryArray(
        [gm.LineString([(0, 0), (1, 0)]), gm.LineString([(0, 0), (3, 4)])], crs=3857
    )
    assert np.isnan((xy_lines).length_3d).all()
    with pytest.raises(ValueError, match='requires a Z ordinate'):
        _ = (xy_lines[0]).length_3d


def test_geometry_array_pickle_round_trip_equality() -> None:
    """GeometryArray pickle/unpickle round-trips equal across storage kinds."""
    import pickle

    packed_points = gm.points([0.0, 1.0, 2.0], [3.0, 4.0, 5.0], crs=4326)
    lines = gm.GeometryArray(
        [gm.LineString([(0, 0), (1, 1)]), gm.LineString([(2, 2), (3, 3)])], crs=4326
    )
    polygons = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3)], crs=4326)
    mixed = gm.GeometryArray([gm.box(0, 0, 1, 1), gm.Point(2, 3)], crs=4326)
    sliced = packed_points[::2]
    for arr in (packed_points, lines, polygons, mixed, sliced):
        assert pickle.loads(pickle.dumps(arr)) == arr


def test_m_aggregate_family_scalar_and_array() -> None:
    """min_m/max_m mirror the Z-aggregate family: scalar None / array
    nan where a geometry carries no M; values otherwise.
    """
    g = gm.from_wkt('LINESTRING M (0 0 5, 1 1 9)')
    assert (g.min_m, g.max_m) == (5.0, 9.0)
    flat = gm.from_wkt('LINESTRING (0 0, 1 1)')
    assert flat.min_m is None and flat.max_m is None
    arr = gm.GeometryArray([g, flat])
    assert_optional_float_array(arr.min_m, [g.min_m for g in arr])
    assert_optional_float_array(arr.max_m, [g.max_m for g in arr])


def test_total_bounds_is_memoized_and_refreshes_on_frame_retag() -> None:
    """Array-level total_bounds cache: warm re-reads agree; retag does not share."""
    arr = gm.GeometryArray(
        [gm.box(0, 0, 1, 1), gm.box(2, 2, 3, 3), gm.Point(4, 5)],
        crs=3857,
    )
    first = arr.total_bounds
    second = arr.total_bounds
    assert first == second == (0.0, 0.0, 4.0, 5.0)

    # Geographic crossing keeps the west>east convention, and warm re-reads agree.
    seam = gm.GeometryArray([
        gm.Polygon(
            [(170, 40), (-170, 40), (-170, 50), (170, 50), (170, 40)],
            crs=4326,
        )
    ])
    geo_first = seam.total_bounds
    geo_second = seam.total_bounds
    assert geo_first == geo_second
    assert geo_first is not None
    minx, miny, maxx, maxy = geo_first
    assert minx > maxx  # west>east for a ±180 crossing
    assert miny == pytest.approx(40.0)
    assert maxy == pytest.approx(50.0)

    # with_crs retags storage: cache must not inherit a stale geographic fold.
    plain = seam.with_crs(None, overwrite=True)
    plain_bounds = plain.total_bounds
    fresh_plain = gm.GeometryArray([
        gm.Polygon([(170, 40), (-170, 40), (-170, 50), (170, 50), (170, 40)])
    ])
    assert plain_bounds == fresh_plain.total_bounds
    # Without a geographic frame the planar fold reports the raw lon min/max
    # (east of west numerically for a wrapping ring).
    assert plain_bounds is not None
    assert plain_bounds[0] < plain_bounds[2]


def test_total_bounds_reuses_warm_planar_element_bounds() -> None:
    """A planar aggregate agrees after the per-row bounds product is warmed."""
    arr = gm.GeometryArray([
        gm.box(0, 0, 1, 1),
        gm.LineString([(4, -2), (6, 3)]),
        gm.GeometryCollection(),
    ])

    np.testing.assert_allclose(
        arr.bounds,
        [[0.0, 0.0, 1.0, 1.0], [4.0, -2.0, 6.0, 3.0], [np.nan] * 4],
        equal_nan=True,
    )
    assert arr.total_bounds == (0.0, -2.0, 6.0, 3.0)
