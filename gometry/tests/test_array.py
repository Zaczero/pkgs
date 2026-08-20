"""`GeometryArray` semantics — packed vs mixed parity, selection ops,
3D accessors, and method/function form parity.
"""

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


def assert_bounds3d_array(
    actual: object,
    expected: list[tuple[float, float, float, float, float, float] | None],
) -> None:
    array = np.asarray(actual)
    want = np.array(
        [[np.nan] * 6 if row is None else list(row) for row in expected],
        dtype=np.float64,
    )
    assert array.dtype == np.float64
    assert array.shape == want.shape
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
    assert gm.bounds(empty).shape == (0, 4)
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


def test_array_set_crs_strips_and_relabels_the_whole_frame() -> None:
    pts = gm.points([1.0, 2.0], [3.0, 4.0], crs=4326)
    plain = (pts).set_crs(None)
    assert plain.crs is None and plain[0].crs is None
    with pytest.raises(ValueError, match='overwrite=True'):
        pts.set_crs(('EPSG', 3857))
    relabelled = pts.set_crs(('EPSG', 3857), overwrite=True)
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
    assert mixed.any_has_z and mixed.any_has_m
    assert not mixed.has_z.flags.writeable
    assert not mixed.has_m.flags.writeable
    with pytest.raises(ValueError, match='got mixed \\(XYZ, XYM\\)'):
        gm.require(mixed, axes='XYZM')
    uniform = gm.GeometryArray([gm.from_wkt('POINT Z (1 2 3)')])
    assert uniform.coordinate_axes == ['XYZ']
    assert uniform.common_coordinate_axes == 'XYZ'
    assert gm.GeometryArray([]).coordinate_axes == []
    assert gm.GeometryArray([]).common_coordinate_axes == 'XY'


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
        assert all_missing.common_coordinate_axes == 'XY'
        assert not all_missing.any_has_z


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
    assert gm.parts(rows).to_wkt() == ['POINT (0 0)', 'POINT (1 1)', 'POINT (2 2)']


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
    assert [(geom).to_wkt() for geom in array[[2, 0, -1]]] == [
        'POINT (2 2)',
        'POINT (0 0)',
        'POINT (2 2)',
    ]
    assert [(geom).to_wkt() for geom in array[[True, False, True]]] == [
        'POINT (0 0)',
        'POINT (2 2)',
    ]
    assert [(geom).to_wkt() for geom in array[array.is_empty]] == [
        'GEOMETRYCOLLECTION EMPTY'
    ]
    assert [(geom).to_wkt() for geom in array[np.array([2, 0], dtype=np.int32)]] == [
        'POINT (2 2)',
        'POINT (0 0)',
    ]
    assert [
        (geom).to_wkt() for geom in array[np.array([0, 1, 2], dtype=np.uint64)[::2]]
    ] == ['POINT (0 0)', 'POINT (2 2)']
    assert (array[np.array(2, dtype=np.int64)]).to_wkt() == 'POINT (2 2)'
    strided_mask = np.array([True, False, False, True, True, False])[::2]
    assert [(geom).to_wkt() for geom in array[strided_mask]] == [
        'POINT (0 0)',
        'POINT (2 2)',
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
    assert_optional_float_array(arr.z_range, [g.z_range for g in arr])
    assert_bounds3d_array(arr.bounds_3d, [g.bounds_3d for g in arr])
    np.testing.assert_allclose((arr).length_3d, [(g).length_3d for g in arr])
    assert_optional_float_array(arr.min_z, [5.0, 1.0, None])
    assert (arr).length_3d[1] == pytest.approx(6.4031242374328485)


def test_packed_z_accessors_use_column_paths() -> None:
    points = gm.points([0, 1], [0, 1], z=[5, 9], crs=4326)
    assert_optional_float_array(points.min_z, [5.0, 9.0])
    assert_optional_float_array(points.max_z, [5.0, 9.0])
    assert_optional_float_array(points.z_range, [0.0, 0.0])
    assert_bounds3d_array(points.bounds_3d, [g.bounds_3d for g in points])
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
    assert_optional_float_array(lines.z_range, [g.z_range for g in lines])
    assert_bounds3d_array(lines.bounds_3d, [g.bounds_3d for g in lines])
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
    assert_optional_float_array(polygons.z_range, [g.z_range for g in polygons])
    assert_bounds3d_array(polygons.bounds_3d, [g.bounds_3d for g in polygons])
    assert_optional_float_array(polygons.min_z, [10.0, 1.0, None])
    polygons_for_length = gm.GeometryArray(
        [shell_only, with_hole, gm.from_wkt('POLYGON EMPTY')], crs=3857
    )
    np.testing.assert_allclose(
        (polygons_for_length).length_3d, [(g).length_3d for g in polygons_for_length]
    )
    assert_optional_float_array(polygons.z_range, [0.0, 9.0, None])
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
    """min_m/max_m/m_range mirror the Z-aggregate family: scalar None / array
    nan where a geometry carries no M; values otherwise.
    """
    g = gm.from_wkt('LINESTRING M (0 0 5, 1 1 9)')
    assert (g.min_m, g.max_m, g.m_range) == (5.0, 9.0, 4.0)
    flat = gm.from_wkt('LINESTRING (0 0, 1 1)')
    assert flat.min_m is None and flat.max_m is None and (flat.m_range is None)
    arr = gm.GeometryArray([g, flat])
    assert_optional_float_array(arr.min_m, [g.min_m for g in arr])
    assert_optional_float_array(arr.max_m, [g.max_m for g in arr])
    assert_optional_float_array(arr.m_range, [g.m_range for g in arr])


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

    # set_crs retags storage: cache must not inherit a stale geographic fold.
    plain = seam.set_crs(None, overwrite=True)
    plain_bounds = plain.total_bounds
    fresh_plain = gm.GeometryArray([
        gm.Polygon([(170, 40), (-170, 40), (-170, 50), (170, 50), (170, 40)])
    ])
    assert plain_bounds == fresh_plain.total_bounds
    # Without a geographic frame the planar fold reports the raw lon min/max
    # (east of west numerically for a wrapping ring).
    assert plain_bounds is not None
    assert plain_bounds[0] < plain_bounds[2]
