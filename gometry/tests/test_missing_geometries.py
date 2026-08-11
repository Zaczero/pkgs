"""First-class missing rows: semantics matrix + mask-survival characterization.

The doctrine (user-locked 2026-07-03): predicates -> False, measures -> NaN,
geometry results propagate missing, exports emit None, aggregates skip;
`arr[i]` is None; is_missing / drop_missing / fill_missing vocabulary; the
dense representation is byte-identical to pre-missing arrays (zero overhead).
"""

from __future__ import annotations

import math
import pickle

import gometry as gm
import numpy as np
import pytest


@pytest.fixture
def masked() -> gm.GeometryArray:
    return gm.GeometryArray([gm.Point(0.0, 0.0), None, gm.Point(2.0, 2.0)])


def test_constructor_access_and_vocabulary(masked: gm.GeometryArray) -> None:
    assert len(masked) == 3
    assert masked[1] is None and masked[-2] is None
    assert masked[0] == gm.Point(0.0, 0.0)
    assert masked.is_missing.tolist() == [False, True, False]
    assert list(masked)[1] is None
    assert None in masked
    assert masked.count(None) == 1
    assert masked.index(None) == 1
    assert masked.count(42) == 0
    with pytest.raises(ValueError, match='geometry is not in array'):
        masked.index(gm.Point(9.0, 9.0))
    assert masked.count(object()) == 0
    with pytest.raises(ValueError, match='value is not in array'):
        masked.index(object())
    assert masked.drop_missing().to_wkt() == ['POINT (0 0)', 'POINT (2 2)']
    filled = masked.fill_missing(gm.Point(9.0, 9.0))
    assert filled.is_missing.tolist() == [False, False, False]
    assert filled.to_wkt()[1] == 'POINT (9 9)'
    dense = gm.GeometryArray([gm.Point(0.0, 0.0)])
    assert None not in dense
    assert dense.count(None) == 0
    with pytest.raises(ValueError, match='None is not in array'):
        dense.index(None)
    assert dense.is_missing.tolist() == [False]
    assert dense.drop_missing() == dense
    with pytest.raises(TypeError, match='expected Geometry or GeometryArray'):
        dense.fill_missing(object())


def test_missing_mask_is_counted_in_retained_size(masked: gm.GeometryArray) -> None:
    dense = gm.GeometryArray([gm.Point(0, 0), gm.Point(1, 1), gm.Point(2, 2)])
    assert masked.nbytes == dense.nbytes
    assert masked.__sizeof__() == dense.__sizeof__() + len(masked)


def test_fill_missing_accepts_row_aligned_arrays(masked: gm.GeometryArray) -> None:
    fill = gm.GeometryArray([
        gm.Point(100.0, 100.0),
        gm.Point(9.0, 9.0),
        gm.Point(200.0, 200.0),
    ])
    filled = masked.fill_missing(fill)
    assert filled.is_missing.tolist() == [False, False, False]
    assert filled.to_wkt() == ['POINT (0 0)', 'POINT (9 9)', 'POINT (2 2)']

    # Only masked receiver rows are consumed from the fill array.
    fill_with_unused_missing = gm.GeometryArray([None, gm.Point(8.0, 8.0), None])
    assert masked.fill_missing(fill_with_unused_missing).to_wkt() == [
        'POINT (0 0)',
        'POINT (8 8)',
        'POINT (2 2)',
    ]

    dense = gm.GeometryArray([gm.Point(0.0, 0.0)])
    assert dense.fill_missing(gm.GeometryArray([gm.Point(1.0, 1.0)])) == dense

    with pytest.raises(ValueError, match='fill array length 1 does not match'):
        masked.fill_missing(gm.GeometryArray([gm.Point(1.0, 1.0)]))
    with pytest.raises(gm.CRSMismatchError):
        masked.set_crs(4326).fill_missing(
            gm.GeometryArray([
                gm.Point(0.0, 0.0, crs=3857),
                gm.Point(1.0, 1.0, crs=3857),
                gm.Point(2.0, 2.0, crs=3857),
            ])
        )
    with pytest.raises(
        gm.GeometryError, match='fill array contains missing geometries'
    ):
        masked.fill_missing(
            gm.GeometryArray([gm.Point(0.0, 0.0), None, gm.Point(2.0, 2.0)])
        )


def test_multiway_concat_preserves_packed_rows_order_and_missing_mask() -> None:
    chunks = [
        gm.GeometryArray([
            gm.Point(float(index), float(index)),
            None if index % 2 == 0 else gm.Point(float(index) + 0.5, 1.0),
        ])
        for index in range(8)
    ]

    joined = chunks[0].concat(*chunks[1:])

    assert joined.to_wkt() == [value for chunk in chunks for value in chunk.to_wkt()]
    assert joined.is_missing.tolist() == [
        missing for chunk in chunks for missing in chunk.is_missing.tolist()
    ]
    assert joined.concat() == joined
    with pytest.raises(gm.CRSMismatchError):
        chunks[0].concat(
            gm.GeometryArray([gm.Point(0.0, 0.0, crs=4326)]),
            object(),
        )

    lines = [
        gm.GeometryArray([
            gm.LineString([(float(index), 0.0), (float(index), 1.0)]),
            gm.LineString([(float(index), 2.0), (float(index), 3.0)]),
        ])[::-1]
        for index in range(8)
    ]
    line_joined = lines[0].concat(*lines[1:])
    assert line_joined.to_wkt() == [
        value for chunk in lines for value in chunk.to_wkt()
    ]

    polygons = [
        gm.GeometryArray([
            gm.box(float(index), 0.0, float(index) + 1.0, 1.0),
            gm.box(float(index), 2.0, float(index) + 1.0, 3.0),
        ])[::-1]
        for index in range(8)
    ]
    polygon_joined = polygons[0].concat(*polygons[1:])
    assert polygon_joined.to_wkt() == [
        value for chunk in polygons for value in chunk.to_wkt()
    ]


def test_geometry_item_consumers_handle_missing_rows_by_contract() -> None:
    geom = gm.Point(0.0, 0.0)
    message = 'contains missing geometries'

    # GeometryArray keeps list-None ingest as the first-class masked-array path.
    masked = gm.GeometryArray([geom, None])
    assert masked.is_missing.tolist() == [False, True]

    with pytest.raises(gm.GeometryError, match=message):
        gm.GeometryCollection([geom, None])
    # The spatial index SKIPS missing rows while preserving original row ids
    # (a missing row simply has no index entry) — same model as building from
    # a masked GeometryArray.
    sparse = gm.SpatialIndex([geom, None])
    assert len(sparse) == 1
    assert list(sparse) == [0]
    assert gm.equals_exact(gm.union_all([geom, None]), gm.union_all([geom]))


def test_p18_geometry_collection_rejects_none_member_fail_fast() -> None:
    """P18: None is not a GeometryCollection member; fail on first None.

    EXACT repro: ``itertools.repeat(None)`` must raise immediately (the old
    path counted missing items after full iteration and hung forever).
    Finite ``[pt, None]`` must raise, not silently skip. Valid multi-member
    collections still construct.
    """
    import itertools
    import signal

    message = 'contains missing geometries'
    pt = gm.Point(0.0, 0.0)

    with pytest.raises(gm.GeometryError, match=message) as finite:
        gm.GeometryCollection([pt, None])
    assert type(finite.value) is gm.GeometryError

    with pytest.raises(gm.GeometryError, match=message) as only_none:
        gm.GeometryCollection([None])
    assert type(only_none.value) is gm.GeometryError

    # Guard against a hang: fail-fast must fire before the alarm.
    def _alarm_handler(_signum: int, _frame: object) -> None:
        raise TimeoutError('GeometryCollection(itertools.repeat(None)) hung')

    previous = signal.signal(signal.SIGALRM, _alarm_handler)
    try:
        signal.setitimer(signal.ITIMER_REAL, 0.5)
        with pytest.raises(gm.GeometryError, match=message) as infinite:
            gm.GeometryCollection(itertools.repeat(None))
        assert type(infinite.value) is gm.GeometryError
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous)

    # Positive: multi-member and empty still construct.
    gc = gm.GeometryCollection([pt, gm.Point(1.0, 1.0)])
    assert gc.geometry_type == 'GeometryCollection'
    assert len(gc.parts) == 2
    assert gm.GeometryCollection().is_empty


def test_with_missing_length_mismatch_uses_value_error() -> None:
    arr = gm.GeometryArray([gm.Point(0.0, 0.0), gm.Point(1.0, 1.0)])
    with pytest.raises(ValueError, match='mask length 1 does not match array length 2'):
        arr._with_missing([True])


def _masked_semantics_array(layout: str) -> gm.GeometryArray:
    if layout == 'points':
        return gm.points([0.0, 9.0, 2.0], [0.0, 9.0, 2.0])._with_missing([
            False,
            True,
            False,
        ])
    if layout == 'lines':
        return gm.GeometryArray([
            gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
            None,
            gm.LineString([(5.0, 5.0), (6.0, 6.0)]),
        ])
    if layout == 'polygons-with-holes':
        return gm.GeometryArray([
            gm.Polygon(
                [(0, 0), (3, 0), (3, 3), (0, 3)],
                holes=[[(1, 1), (2, 1), (2, 2), (1, 1)]],
            ),
            None,
            gm.box(5, 5, 6, 6),
        ])
    if layout == 'mixed':
        return gm.GeometryArray([
            gm.Point(0.0, 0.0),
            None,
            gm.LineString([(5.0, 5.0), (6.0, 6.0)]),
        ])
    raise AssertionError(layout)


@pytest.mark.parametrize('layout', ['points', 'lines', 'polygons-with-holes', 'mixed'])
def test_semantics_matrix(layout: str) -> None:
    masked = _masked_semantics_array(layout)
    # measures -> NaN
    assert math.isnan(masked.area[1])
    assert math.isnan(gm.distance(masked, gm.Point(0.0, 0.0))[1])
    # predicates -> False
    assert gm.intersects(masked, gm.Point(0.0, 0.0)).tolist() == [True, False, False]
    assert masked.is_empty.tolist()[1] is False
    # geometry ops propagate
    assert masked.buffer(0.5).is_missing.tolist() == [False, True, False]
    assert masked.centroid().is_missing.tolist() == [False, True, False]
    assert gm.union(masked, gm.Point(5.0, 5.0)).is_missing.tolist() == [
        False,
        True,
        False,
    ]
    # pairwise: union of masks
    other = gm.GeometryArray([gm.Point(0.0, 0.0), gm.Point(1.0, 1.0), None])
    assert math.isnan(gm.distance(masked, other)[1])
    assert math.isnan(gm.distance(masked, other)[2])
    # exports emit None
    assert masked.to_wkt()[1] is None
    assert masked.to_wkb()[1] is None
    assert masked.to_geojson()[1] is None


def test_predicate_fast_paths_force_missing_rows_false(
    masked: gm.GeometryArray,
) -> None:
    assert gm.intersects(masked, gm.Point(0.0, 0.0)).tolist() == [True, False, False]
    assert gm.disjoint(masked, gm.Point(0.0, 0.0)).tolist() == [False, False, True]

    right = gm.GeometryArray([gm.Point(0.0, 0.0), gm.Point(9.0, 9.0), None])
    assert gm.intersects(masked, right).tolist() == [True, False, False]
    assert gm.disjoint(masked, right).tolist() == [False, False, False]


def test_scalar_polygon_predicates_skip_missing_packed_polygon_rows() -> None:
    rows = gm.GeometryArray([
        gm.box(0.0, 0.0, 1.0, 1.0),
        None,
        gm.box(2.0, 2.0, 3.0, 3.0),
    ])
    window = gm.box(-1.0, -1.0, 2.0, 2.0)

    assert gm.contains(window, rows).tolist() == [True, False, False]
    assert gm.intersects(window, rows).tolist() == [True, False, True]


def test_prepared_predicates_mask_missing_rows_like_free_functions() -> None:
    probe = gm.box(-0.5, -0.5, 0.5, 0.5)
    masked = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        None,
        gm.Point(2.0, 2.0),
    ])
    prepared = probe.prepare()

    for name in (
        'contains',
        'intersects',
        'within',
        'covers',
        'covered_by',
        'touches',
        'crosses',
        'overlaps',
        'equals',
        'disjoint',
    ):
        prepared_values = getattr(prepared, name)(masked)
        free_values = getattr(gm, name)(probe, masked)
        np.testing.assert_array_equal(prepared_values, free_values, err_msg=name)
        assert prepared_values[1] == np.False_

    prepared_dwithin = prepared.dwithin(masked, 0.25)
    free_dwithin = gm.dwithin(probe, masked, 0.25)
    np.testing.assert_array_equal(prepared_dwithin, free_dwithin)
    assert free_dwithin.tolist() == [True, False, False]


def test_length_3d_and_ordinate_extremes_degrade_missing_rows_to_nan() -> None:
    line = gm.LineString(
        [(0.0, 0.0), (3.0, 0.0)],
        z=[4.0, 8.0],
        m=[10.0, 16.0],
    )
    rows = gm.GeometryArray([line, None, line])

    assert rows.length_3d[0] == pytest.approx(5.0)
    assert math.isnan(rows.length_3d[1])
    assert rows.length[0] == pytest.approx(3.0)
    assert math.isnan(rows.length[1])

    for values in (
        rows.min_z,
        rows.max_z,
        rows.z_range,
        rows.min_m,
        rows.max_m,
        rows.m_range,
    ):
        assert math.isnan(values[1])
    assert rows.min_z[[0, 2]].tolist() == [4.0, 4.0]
    assert rows.max_z[[0, 2]].tolist() == [8.0, 8.0]
    assert rows.z_range[[0, 2]].tolist() == [4.0, 4.0]
    assert rows.min_m[[0, 2]].tolist() == [10.0, 10.0]
    assert rows.max_m[[0, 2]].tolist() == [16.0, 16.0]
    assert rows.m_range[[0, 2]].tolist() == [6.0, 6.0]
    assert np.isnan(rows.bounds_3d[1]).all()


def test_geographic_metric_fast_paths_skip_missing_placeholders() -> None:
    left = gm.points([0.0, 999.0, 1.0], [0.0, 999.0, 0.0], crs=4326)._with_missing([
        False,
        True,
        False,
    ])
    right = gm.points([0.0, 999.0, 1.0], [0.0, 999.0, 0.0], crs=4326)._with_missing([
        False,
        True,
        False,
    ])
    target = gm.Point(0.0, 0.0, crs=4326)
    line = gm.LineString([(0.0, -1.0), (0.0, 1.0)], crs=4326)

    scalar_distances = gm.distance(left, target)
    assert scalar_distances[0] == pytest.approx(0.0)
    assert math.isnan(scalar_distances[1])

    pair_distances = gm.distance(left, right)
    assert pair_distances[0] == pytest.approx(0.0)
    assert math.isnan(pair_distances[1])

    shape_distances = gm.distance(left, line)
    assert shape_distances[0] == pytest.approx(0.0)
    assert math.isnan(shape_distances[1])

    assert gm.dwithin(left, target, 1.0).tolist() == [True, False, False]
    assert gm.dwithin(left, right, 1.0).tolist() == [True, False, True]
    assert gm.dwithin(left, target, [1.0, 1.0, 1.0]).tolist() == [
        True,
        False,
        False,
    ]
    assert gm.dwithin(left, right, [1.0, 1.0, 1.0]).tolist() == [True, False, True]


def test_similarity_metrics_degrade_missing_rows_to_nan() -> None:
    line0 = gm.LineString([(0.0, 0.0), (1.0, 0.0)])
    line1 = gm.LineString([(2.0, 0.0), (3.0, 0.0)])
    masked = gm.GeometryArray([line0, None, line1])
    dense = gm.GeometryArray([line0, line1, line1])
    right_missing = gm.GeometryArray([line0, line1, None])

    for metric in (gm.hausdorff_distance, gm.frechet_distance):
        scalar = metric(masked, line0)
        assert scalar[0] == pytest.approx(0.0)
        assert math.isnan(scalar[1])

        same_storage = metric(masked, masked)
        assert same_storage[0] == pytest.approx(0.0)
        assert math.isnan(same_storage[1])
        assert same_storage[2] == pytest.approx(0.0)

        pair = metric(masked, right_missing)
        assert pair[0] == pytest.approx(0.0)
        assert math.isnan(pair[1])
        assert math.isnan(pair[2])

        densified = metric(masked, dense, densify=[0.5, 0.5, 0.5])
        assert densified[0] == pytest.approx(0.0)
        assert math.isnan(densified[1])
        assert densified[2] == pytest.approx(0.0)


def test_packed_equals_exact_treats_missing_rows_as_false() -> None:
    left = gm.points([0.0, 9.0, 2.0], [0.0, 9.0, 2.0])._with_missing([
        False,
        True,
        False,
    ])
    right = gm.points([0.0, 9.0, 3.0], [0.0, 9.0, 3.0])._with_missing([
        False,
        True,
        False,
    ])

    assert gm.equals_exact(left, right).tolist() == [True, False, False]


def test_masked_repair_preserves_missing_polygon_rows() -> None:
    rows = gm.GeometryArray([gm.box(0, 0, 1, 1), None, gm.box(2, 2, 3, 3)])
    repaired = rows.repair()

    assert repaired.is_missing.tolist() == [False, True, False]
    assert repaired.to_wkt() == rows.to_wkt()


def test_masked_split_skips_missing_line_rows() -> None:
    line = gm.LineString([(0, 0), (2, 0)])
    rows = gm.GeometryArray([line, None, line])

    pieces = gm.split(rows, gm.Point(1, 0))

    assert pieces.to_wkt() == [
        'LINESTRING (0 0, 1 0)',
        'LINESTRING (1 0, 2 0)',
        'LINESTRING (0 0, 1 0)',
        'LINESTRING (1 0, 2 0)',
    ]

    pairwise_pieces = gm.split(
        rows,
        gm.GeometryArray([gm.Point(1, 0), gm.Point(1, 0), None]),
    )
    assert pairwise_pieces.to_wkt() == [
        'LINESTRING (0 0, 1 0)',
        'LINESTRING (1 0, 2 0)',
    ]


def test_masked_splitter_array_skips_missing_points() -> None:
    line = gm.LineString([(0, 0), (4, 0)])
    masked_splitters = gm.GeometryArray([
        gm.Point(1, 0),
        None,
        gm.Point(3, 0),
    ])
    dense_splitters = gm.GeometryArray([gm.Point(1, 0), gm.Point(3, 0)])

    assert (
        gm.split(line, masked_splitters).to_wkt()
        == gm.split(
            line,
            dense_splitters,
        ).to_wkt()
    )


def test_spatial_index_point_fast_paths_skip_missing_query_rows() -> None:
    values = gm.points([0.0, 2.0], [0.0, 2.0])
    queries = gm.GeometryArray([gm.Point(0.0, 0.0), None, gm.Point(9.0, 9.0)])
    idx = gm.SpatialIndex(values)

    assert idx.candidates(queries).to_list() == [[0], [], []]
    assert idx.query(queries).to_list() == [[0], [], []]
    assert idx.nearest(queries).to_list() == [[0], [], [1]]

    left, right = gm.join(queries, values, predicate='intersects')
    assert list(zip(left.tolist(), right.tolist(), strict=True)) == [(0, 0)]


def test_linref_array_lanes_degrade_missing_rows_to_nan() -> None:
    line = gm.LineString([(0.0, 0.0), (10.0, 0.0)])
    lines = gm.GeometryArray([line, None, line])
    located = lines.line_locate(gm.Point(5.0, 0.0))
    assert located[0] == pytest.approx(5.0)
    assert math.isnan(located[1])
    assert located[2] == pytest.approx(5.0)

    points = gm.GeometryArray([gm.Point(5.0, 0.0), None, gm.Point(7.0, 0.0)])
    located_points = lines.line_locate(points)
    assert located_points[0] == pytest.approx(5.0)
    assert math.isnan(located_points[1])
    assert located_points[2] == pytest.approx(7.0)

    measured = gm.LineString([(0.0, 0.0), (10.0, 0.0)], m=[0.0, 100.0])
    measured_rows = gm.GeometryArray([measured, None])
    located_m = measured_rows.line_locate(gm.Point(5.0, 0.0), basis='m')
    assert located_m[0] == pytest.approx(50.0)
    assert math.isnan(located_m[1])


def test_point_nav_float_array_lanes_degrade_missing_rows_to_nan() -> None:
    points = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        None,
        gm.Point(2.0, 2.0, crs=4326),
    ])
    dense = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(2.0, 2.0, crs=4326),
    ])
    target = gm.Point(1.0, 1.0, crs=4326)
    targets = gm.GeometryArray([
        gm.Point(1.0, 1.0, crs=4326),
        gm.Point(9.0, 9.0, crs=4326),
        None,
    ])

    for op in (
        gm.bearing,
        gm.rhumb_distance,
        lambda left, right: gm.bearing(left, right, path='rhumb'),
    ):
        left_array = op(points, target)
        assert np.isfinite(left_array[[0, 2]]).all()
        assert math.isnan(left_array[1])

        right_array = op(target, points)
        assert np.isfinite(right_array[[0, 2]]).all()
        assert math.isnan(right_array[1])

        both_arrays = op(points, targets)
        assert np.isfinite(both_arrays[0])
        assert math.isnan(both_arrays[1])
        assert math.isnan(both_arrays[2])

        dense_values = op(dense, target)
        assert np.isfinite(dense_values).all()

    start = gm.Point(0.0, 0.0, crs=4326)
    end = gm.Point(1.0, 0.0, crs=4326)
    cross_track = gm.cross_track_distance(points, start, end)
    assert np.isfinite(cross_track[[0, 2]]).all()
    assert math.isnan(cross_track[1])

    starts = gm.GeometryArray([start, None, start])
    ends = gm.GeometryArray([end, end, end])
    cross_track_arrays = gm.cross_track_distance(points, starts, ends)
    assert np.isfinite(cross_track_arrays[[0, 2]]).all()
    assert math.isnan(cross_track_arrays[1])

    cross_track_start_array = gm.cross_track_distance(target, starts, end)
    assert np.isfinite(cross_track_start_array[[0, 2]]).all()
    assert math.isnan(cross_track_start_array[1])

    ends_with_missing = gm.GeometryArray([end, None, end])
    cross_track_end_array = gm.cross_track_distance(target, start, ends_with_missing)
    assert np.isfinite(cross_track_end_array[[0, 2]]).all()
    assert math.isnan(cross_track_end_array[1])

    dense_cross_track = gm.cross_track_distance(dense, start, end)
    assert np.isfinite(dense_cross_track).all()


def test_point_nav_geometry_array_lanes_propagate_missing_rows() -> None:
    points = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        None,
        gm.Point(2.0, 2.0, crs=4326),
    ])
    dense = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(2.0, 2.0, crs=4326),
    ])
    target = gm.Point(1.0, 1.0, crs=4326)
    targets = gm.GeometryArray([
        gm.Point(1.0, 1.0, crs=4326),
        gm.Point(9.0, 9.0, crs=4326),
        None,
    ])

    destination = gm.destination(points, 90.0, 1000.0)
    assert destination.is_missing.tolist() == [False, True, False]
    assert destination[1] is None

    destination_arrays = gm.destination(
        points,
        [90.0, 180.0, 270.0],
        [1000.0, 1000.0, 1000.0],
    )
    assert destination_arrays.is_missing.tolist() == [False, True, False]
    assert destination_arrays[1] is None

    between = gm.point_between(points, target, 1000.0)
    assert between.is_missing.tolist() == [False, True, False]
    assert between[1] is None

    between_right_array = gm.point_between(target, points, 1000.0)
    assert between_right_array.is_missing.tolist() == [False, True, False]
    assert between_right_array[1] is None

    between_arrays = gm.point_between(points, targets, 1000.0)
    assert between_arrays.is_missing.tolist() == [False, True, True]
    assert between_arrays[1] is None
    assert between_arrays[2] is None

    rhumb_result = gm.destination(points, 90.0, 1000.0, path='rhumb')
    assert rhumb_result.is_missing.tolist() == [False, True, False]
    assert rhumb_result[1] is None

    assert not gm.destination(dense, 90.0, 1000.0).is_missing.any()
    assert not gm.point_between(dense, target, 1000.0).is_missing.any()
    assert not gm.destination(dense, 90.0, 1000.0, path='rhumb').is_missing.any()


def test_geojson_iterable_legacy_crs_conflict_matches_scalar_lane() -> None:
    payload = {
        'type': 'Point',
        'coordinates': [1, 2],
        'crs': {'type': 'name', 'properties': {'name': 'EPSG:4326'}},
    }
    with pytest.raises(gm.ParseError, match='conflicts with crs=') as excinfo:
        gm.from_geojson([payload], crs='EPSG:4979')
    assert 'array element 0' in ''.join(excinfo.value.__notes__)


def test_geojson_feature_null_geometry_dict_lanes_become_missing_rows() -> None:
    missing_feature = {'type': 'Feature', 'properties': {}, 'geometry': None}
    point_feature = {
        'type': 'Feature',
        'properties': {},
        'geometry': {'type': 'Point', 'coordinates': [1.0, 2.0]},
    }
    collection = {
        'type': 'FeatureCollection',
        'features': [missing_feature, point_feature],
    }

    parsed_collection = gm.from_geojson(collection)
    assert parsed_collection.is_missing.tolist() == [True, False]
    assert parsed_collection.to_wkt() == [None, 'POINT (1 2)']

    parsed_iterable = gm.from_geojson([missing_feature, point_feature])
    assert parsed_iterable.is_missing.tolist() == [True, False]
    assert parsed_iterable.to_wkt() == [None, 'POINT (1 2)']


def test_mask_survives_row_aligned_ops(masked: gm.GeometryArray) -> None:
    expected = [False, True, False]
    survivors = [
        masked.set_crs(4326),
        masked.set_crs(4326).set_epoch(2020.0),
        masked.set_crs(4326).to_crs(3857),
        masked.set_z(1.0),
        masked.force_3d(),
        masked.reverse(),
        masked.swap_xy(),
        masked.affine_transform([1.0, 0.0, 0.0, 1.0, 10.0, 10.0]),
        masked.buffer(0.1),
        masked.simplify(0.1),
        masked[:],
        masked[[0, 1, 2]],
        pickle.loads(pickle.dumps(masked)),
    ]
    for result in survivors:
        assert result.is_missing.tolist() == expected, repr(result)
    assert masked[1:].is_missing.tolist() == [True, False]
    assert masked[[False, True, True]].is_missing.tolist() == [True, False]


def test_frame_semantics_with_missing() -> None:
    tagged = gm.GeometryArray([gm.Point(0.0, 0.0, crs=4326), None])
    assert str(tagged.crs) == 'EPSG:4326'
    assert tagged.is_missing.tolist() == [False, True]
    reprojected = tagged.to_crs(3857)
    assert reprojected.is_missing.tolist() == [False, True]
    assert str(reprojected.crs) == 'EPSG:3857'
    with pytest.raises(gm.CRSMismatchError):
        tagged.fill_missing(gm.Point(1.0, 1.0, crs=3857))


def test_container_equality_and_pickle(masked: gm.GeometryArray) -> None:
    clone = pickle.loads(pickle.dumps(masked))
    assert clone == masked
    assert hash(clone) == hash(masked)
    placeholderish = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        gm.from_wkt('POINT EMPTY'),
        gm.Point(2.0, 2.0),
    ])
    assert masked != placeholderish
    mixed = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        None,
        gm.LineString([(0.0, 0.0), (1.0, 1.0)]),
    ])
    assert pickle.loads(pickle.dumps(mixed)) == mixed

    from gometry._arrow import apply_missing

    line = gm.LineString([(0.0, 0.0), (1.0, 1.0)])
    line_masked = gm.GeometryArray([line, None])
    arrow_masked = gm.from_arrow(
        apply_missing(gm.GeometryArray([line, line]).to_arrow(), b'\x01')
    )
    assert arrow_masked == line_masked
    assert hash(arrow_masked) == hash(line_masked)


def test_masked_sort_keeps_mask_and_moves_missing_to_tail() -> None:
    arr = gm.GeometryArray([gm.Point(10, 10), None, gm.Point(0, 0)])
    sorted_arr = arr.sort_by_spatial_key(curve='hilbert', bounds=(0, 0, 10, 10))
    assert sorted_arr.is_missing.tolist() == [False, False, True]
    assert sorted_arr.to_wkt() == ['POINT (0 0)', 'POINT (10 10)', None]
    assert sorted_arr[2] is None


def test_masked_flatten_exports_do_not_leak_placeholder(
    masked: gm.GeometryArray,
) -> None:
    parts = gm.parts(masked)
    assert parts.to_wkt() == ['POINT (0 0)', 'POINT (2 2)']
    obj = np.asarray(masked)
    assert obj.tolist()[1] is None
    interface = masked.__geo_interface__
    assert interface['features'][1]['geometry'] is None
    assert masked.geometry_type == ['Point', None, 'Point']
    assert 'missing' in masked._repr_html_()
    assert 'NaN' not in masked._repr_html_()


def test_masked_repr_ignores_placeholder_geometry_type() -> None:
    rows = gm.GeometryArray([gm.box(0, 0, 1, 1), None, gm.box(2, 2, 3, 3)])

    assert repr(rows) == '<GeometryArray[Polygon] len=3 missing=1>'


def test_geographic_packed_bounds_preserve_missing_and_antimeridian_rows() -> None:
    lines = gm.GeometryArray([
        gm.LineString([(170.0, 10.0), (-170.0, 20.0)], crs='OGC:CRS84'),
        None,
        gm.LineString([(1.0, 2.0), (3.0, 4.0)], crs='OGC:CRS84'),
    ])
    polygons = gm.GeometryArray([
        gm.Polygon(
            [(170.0, 0.0), (-170.0, 0.0), (-170.0, 5.0), (170.0, 0.0)],
            crs='OGC:CRS84',
        ),
        None,
        gm.box(1.0, 2.0, 3.0, 4.0, crs='OGC:CRS84'),
    ])

    for values in (lines, polygons):
        expected = [row.bounds if row is not None else None for row in values]
        actual = values.bounds.tolist()
        assert actual[0] == list(expected[0])
        assert all(np.isnan(value) for value in actual[1])
        assert actual[2] == list(expected[2])


def test_arrow_c_export_preserves_missing_rows_as_nulls() -> None:
    pa = pytest.importorskip('pyarrow')
    rows = gm.GeometryArray([gm.Point(0, 0), None, gm.Point(2, 2)])

    arrow = pa.array(rows)

    assert arrow.null_count == 1
    assert arrow[1].as_py() is None
    restored = gm.from_arrow(arrow)
    assert restored.is_missing.tolist() == [False, True, False]
    assert restored.to_wkt() == ['POINT (0 0)', None, 'POINT (2 2)']


def test_masked_report_surfaces_hide_placeholder_rows() -> None:
    row = gm.LineString([(0, 0), (2, 1)])
    rows = gm.GeometryArray([row, None, gm.box(0, 0, 1, 1)])

    extremes = rows.extremes()
    assert extremes.west[0].to_wkt() == 'POINT (0 0)'
    assert all(column[1] is None for column in extremes)
    assert extremes.east[2].to_wkt() == 'POINT (1 0)'

    reports = rows.validate()
    assert bool(reports[0])
    assert reports[1] is None
    assert bool(reports[2])


def test_masked_integer_facts_use_none_geometry_sentinels() -> None:
    rows = gm.GeometryArray([
        gm.Point(0, 0),
        None,
        gm.GeometryCollection([gm.Point(1, 1), gm.Point(2, 2)]),
    ])

    assert rows.num_geometries.tolist() == [1, 0, 2]
    assert rows.topological_dimension.tolist() == [0, -1, 0]


def test_masked_self_intersections_missing_row_is_empty_group() -> None:
    bowtie = gm.LineString([(0, 0), (1, 1), (1, 0), (0, 1)])
    rows = gm.GeometryArray([bowtie, None])

    intersections = rows.self_intersections()

    assert [(point.x, point.y) for point in intersections[0]] == [(0.5, 0.5)]
    assert len(intersections[1]) == 0


def test_masked_polyline_returns_none_instead_of_type_error() -> None:
    arr = gm.GeometryArray([
        gm.LineString([(0, 0), (1, 1)], crs=4326),
        None,
        gm.LineString([(2, 2), (3, 3)], crs=4326),
    ])
    encoded = arr.to_polyline()
    assert encoded[0] == gm.LineString([(0, 0), (1, 1)], crs=4326).to_polyline()
    assert encoded[1] is None
    assert encoded[2] == gm.LineString([(2, 2), (3, 3)], crs=4326).to_polyline()


def test_masked_sample_points_missing_row_is_empty_group() -> None:
    arr = gm.GeometryArray([gm.box(0, 0, 1, 1), None, gm.box(1, 1, 2, 2)])
    samples = arr.sample_points(3, seed=7)
    assert isinstance(samples, gm.Groups)
    assert len(samples) == 3
    assert [len(row) for row in samples] == [3, 0, 3]
    assert all(
        point.x == point.x and point.y == point.y for row in samples for point in row
    )


def test_masked_line_interpolate_points_missing_row_is_empty_group() -> None:
    line = gm.LineString([(0.0, 0.0), (10.0, 0.0)])
    arr = gm.GeometryArray([line, None, line])
    points = arr.line_interpolate(count=3)
    assert isinstance(points, gm.Groups)
    assert len(points) == 3
    assert [[point.x for point in row] for row in points] == [
        [0.0, 5.0, 10.0],
        [],
        [0.0, 5.0, 10.0],
    ]


def test_masked_voronoi_skips_missing_placeholder() -> None:
    arr = gm.GeometryArray([gm.Point(0, 0), None, gm.Point(1, 0), gm.Point(0, 1)])
    # Per-row Groups: the missing row becomes an empty group (no NaN placeholder).
    edges = arr.voronoi_edges()
    assert isinstance(edges, gm.Groups) and len(edges) == 4
    assert len(edges[1]) == 0
    for row in list(edges):
        assert all('NaN' not in geom.to_wkt() for geom in row)


def test_masked_coordinates_skip_missing_rows(masked: gm.GeometryArray) -> None:
    assert len(masked.coords) == 2
    coords, index = gm.get_coordinates(masked, return_index=True)
    assert coords.tolist() == [[0.0, 0.0], [2.0, 2.0]]
    assert index.tolist() == [0, 2]
    assert masked.coords.x.tolist() == [0.0, 2.0]
    assert masked.coords.to_nested() == [[0.0, 0.0], [2.0, 2.0]]


def test_masked_nearest_geometry_outputs_propagate_missing_rows() -> None:
    rows = gm.GeometryArray([
        gm.LineString([(0, 0), (2, 0)]),
        None,
        gm.LineString([(0, 2), (2, 2)]),
    ])
    probe = gm.Point(1, 1)

    left, right = gm.nearest_points(probe, rows)
    assert left.is_missing.tolist() == [False, True, False]
    assert right.is_missing.tolist() == [False, True, False]
    assert left[1] is None and right[1] is None

    left_rows, right_rows = gm.nearest_points(rows, rows)
    assert left_rows.is_missing.tolist() == [False, True, False]
    assert right_rows.is_missing.tolist() == [False, True, False]

    shortest = gm.shortest_line(probe, rows)
    assert shortest.is_missing.tolist() == [False, True, False]
    assert shortest[1] is None

    shortest_left_array = gm.shortest_line(rows, probe)
    assert shortest_left_array.is_missing.tolist() == [False, True, False]
    assert shortest_left_array[1] is None


def test_geographic_masked_nearest_and_shortest_line_skip_placeholders() -> None:
    rows = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        None,
        gm.Point(2.0, 2.0, crs=4326),
    ])
    probe = gm.Point(1.0, 1.0, crs=4326)

    left, right = gm.nearest_points(rows, probe)
    assert left.is_missing.tolist() == [False, True, False]
    assert right.is_missing.tolist() == [False, True, False]
    assert left[1] is None and right[1] is None

    shortest = gm.shortest_line(rows, probe)
    assert shortest.is_missing.tolist() == [False, True, False]
    assert shortest[1] is None


def test_masked_string_and_xy_predicate_surfaces() -> None:
    rows = gm.GeometryArray([gm.Point(0, 0), None, gm.Point(2, 2)])
    probe = gm.box(-1, -1, 1, 1)

    assert gm.relate(rows, probe)[1] is None
    assert gm.relate(probe, rows)[1] is None
    np.testing.assert_array_equal(
        gm.contains_xy(probe, [0.0, 9.0, 2.0], [0.0, 9.0, 2.0]),
        [True, False, False],
    )
    np.testing.assert_array_equal(
        gm.intersects_xy(probe, [0.0, 9.0, 2.0], [0.0, 9.0, 2.0]),
        [True, False, False],
    )


def test_masked_overlay_operators_propagate_missing_rows() -> None:
    rows = gm.GeometryArray([gm.box(0, 0, 2, 2), None, gm.box(2, 2, 4, 4)])
    clip = gm.box(1, 1, 3, 3)

    for result in (rows & clip, rows | clip, rows - clip, rows ^ clip):
        assert result.is_missing.tolist() == [False, True, False]
        assert result[1] is None


def _scatter_dense_values(dense: object, missing: object) -> object:
    if isinstance(dense, gm.GeometryArray):
        present = iter(list(dense))
        return [None if flag else next(present) for flag in missing]
    if isinstance(dense, np.ndarray):
        if dense.dtype == np.bool_:
            present = iter(dense.tolist())
            return [False if flag else next(present) for flag in missing]
        present = iter(dense.tolist())
        return [math.nan if flag else next(present) for flag in missing]
    present = iter(dense)
    return [None if flag else next(present) for flag in missing]


def _assert_masked_matches_dense_scatter(
    masked_result: object, dense_result: object
) -> None:
    missing = [False, True, False]
    expected = _scatter_dense_values(dense_result, missing)
    if isinstance(masked_result, gm.GeometryArray):
        assert masked_result.is_missing.tolist() == missing
        assert masked_result.to_wkt() == [
            None if value is None else value.to_wkt() for value in expected
        ]
    elif isinstance(masked_result, np.ndarray) and masked_result.dtype == np.bool_:
        assert masked_result.tolist() == expected
    elif isinstance(masked_result, np.ndarray):
        for got, want in zip(masked_result.tolist(), expected, strict=True):
            if want is None or (isinstance(want, float) and math.isnan(want)):
                assert math.isnan(got)
            else:
                assert got == pytest.approx(want)
    else:
        assert masked_result == expected


def test_masked_vs_dense_parity_sweep() -> None:
    g0 = gm.LineString([(0.0, 0.0), (1.0, 0.0)], z=[0.0, 1.0], m=[10.0, 11.0])
    g1 = gm.LineString([(2.0, 0.0), (3.0, 0.0)], z=[2.0, 3.0], m=[12.0, 13.0])
    masked = gm.GeometryArray([g0, None, g1])
    dense = gm.GeometryArray([g0, g1])
    probe = gm.Point(0.5, 0.0)

    cases = [
        (masked.buffer(0.1), dense.buffer(0.1)),
        (masked.centroid(), dense.centroid()),
        (masked.reverse(), dense.reverse()),
        (gm.intersects(masked, probe), gm.intersects(dense, probe)),
        (gm.distance(masked, probe), gm.distance(dense, probe)),
        (masked.length, dense.length),
        (masked.length_3d, dense.length_3d),
        (masked.min_z, dense.min_z),
        (masked.min_m, dense.min_m),
        (gm.relate(masked, probe), gm.relate(dense, probe)),
        (masked.to_wkt(), dense.to_wkt()),
    ]
    for masked_result, dense_result in cases:
        _assert_masked_matches_dense_scatter(masked_result, dense_result)


def test_masked_coverage_methods_skip_and_scatter_missing() -> None:
    coverage = gm.GeometryArray([gm.box(0, 0, 1, 1), None, gm.box(1, 0, 2, 1)])
    assert coverage.coverage_is_valid()
    invalid_edges = coverage.coverage_invalid_edges()
    assert invalid_edges.is_missing.tolist() == [False, True, False]
    assert invalid_edges[1] is None
    simplified = coverage.coverage_simplify(0.0)
    assert simplified.is_missing.tolist() == [False, True, False]
    cleaned = coverage.coverage_clean(grid_size=0.0)
    assert cleaned.is_missing.tolist() == [False, True, False]
    method_cleaned = coverage.coverage_clean(grid_size=0.0)
    assert method_cleaned.is_missing.tolist() == [False, True, False]
    assert coverage.coverage_union().geometry_type == 'Polygon'
    with pytest.raises(gm.InvalidGeometryError):
        gm.GeometryArray([None, None]).coverage_union()


def test_free_coverage_functions_preserve_missing_iterable_rows() -> None:
    values = [gm.box(0, 0, 1, 1), None, gm.box(1, 0, 2, 1)]
    array = gm.GeometryArray(values)

    assert gm.coverage_is_valid(values) == array.coverage_is_valid()
    assert gm.coverage_union(values) == array.coverage_union()
    for free, method in (
        (gm.coverage_invalid_edges(values), array.coverage_invalid_edges()),
        (gm.coverage_simplify(values, 0.0), array.coverage_simplify(0.0)),
        (gm.coverage_clean(values), array.coverage_clean()),
    ):
        assert free.is_missing.tolist() == [False, True, False]
        assert free.to_wkt() == method.to_wkt()


def test_pooled_reductions_skip_missing_iterable_rows() -> None:
    values = [gm.box(0, 0, 2, 2), None, gm.box(1, 1, 3, 3)]
    present = [value for value in values if value is not None]

    for operation in (gm.union_all, gm.intersection_all, gm.symmetric_difference_all):
        assert gm.equals_exact(operation(values), operation(present))

    ring = gm.LineString([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    pooled = gm.polygonize([ring, None])
    dense = gm.polygonize([ring])
    assert gm.equals_exact(pooled, dense).all()
    assert gm.equals_exact(
        gm.polygonize_full([ring, None]).polygons,
        gm.polygonize_full([ring]).polygons,
    ).all()


def test_repeated_ops_on_a_fancy_selection_stay_consistent() -> None:
    # The gathered-storage memo must be invisible: every op on one selection
    # object returns the same values as a fresh selection, and mutating-style
    # ops (which build new arrays) never see stale columns.
    polys = gm.GeometryArray([gm.box(i, 0, i + 1, 2) for i in range(12)])
    mask = np.array([i % 3 == 0 for i in range(12)])
    sel = polys[mask]
    first_area = sel.area
    np.testing.assert_array_equal(sel.area, first_area)
    np.testing.assert_array_equal(polys[mask].area, first_area)
    np.testing.assert_array_equal(sel.length, polys[mask].length)
    np.testing.assert_array_equal(sel.bounds, polys[mask].bounds)
    shifted = sel.translate(10.0, 0.0)
    np.testing.assert_array_equal(shifted.area, first_area)
    assert shifted.bounds[0][0] == sel.bounds[0][0] + 10.0
