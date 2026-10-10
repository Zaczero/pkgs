"""Point navigation methods (bearing, destination, interpolate, rhumb)."""

from __future__ import annotations

from typing import Any

import gometry as gm
import numpy as np
import pytest


def _wkb_rows(values: gm.GeometryArray) -> list[bytes]:
    return [bytes(value) for value in values.to_wkb()]


def test_bearing_scalar_and_array() -> None:
    a = gm.Point(0, 0, crs=4326)
    b = gm.Point(1, 0, crs=4326)
    assert gm.bearing(a, b) == pytest.approx(90.0)
    points = gm.GeometryArray([gm.Point(0, 0, crs=4326), gm.Point(0, 1, crs=4326)])
    bearings = gm.bearing(points, b)
    assert isinstance(bearings, np.ndarray)
    assert np.isfinite(bearings).all()
    assert bearings[0] == pytest.approx(90.0)


def test_destination_scalar_and_array() -> None:
    start = gm.Point(0, 0, crs=4326)
    end = start.destination(90, 111_000)
    assert end.x == pytest.approx(1.0, abs=0.01)
    assert end.y == pytest.approx(0.0, abs=0.01)
    points = gm.GeometryArray([start, gm.Point(0, 1, crs=4326)])
    outs = points.destination(90, 111_000)
    assert isinstance(outs, gm.GeometryArray)
    assert outs[0].x == pytest.approx(1.0, abs=0.01)
    vector_outs = points.destination([90.0, 0.0], [111_000.0, 110_600.0])
    assert isinstance(vector_outs, gm.GeometryArray)
    assert _wkb_rows(vector_outs) == [
        points[0].destination(90.0, 111_000.0).to_wkb(),
        points[1].destination(0.0, 110_600.0).to_wkb(),
    ]
    scalar_point_vector_outs = start.destination([90.0, 0.0], [111_000.0, 110_600.0])
    assert isinstance(scalar_point_vector_outs, gm.GeometryArray)
    assert _wkb_rows(scalar_point_vector_outs) == [
        start.destination(90.0, 111_000.0).to_wkb(),
        start.destination(0.0, 110_600.0).to_wkb(),
    ]


def test_destination_masked_mixed_axis_points_falls_back_rowwise() -> None:
    points = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        None,
        gm.Point(1.0, 1.0, z=5.0, crs=4326),
    ])
    out = points.destination(90.0, 1000.0)
    assert out.is_missing.tolist() == [False, True, False]
    assert out[1] is None
    assert out[0].coordinate_axes == 'XY'
    assert out[2].coordinate_axes == 'XYZ'
    assert out[2].z == 5.0

    target = gm.Point(2.0, 2.0, z=9.0, crs=4326)
    between = gm.point_between(points, target, 0.5, normalized=True)
    assert between.is_missing.tolist() == [False, True, False]
    assert between[1] is None
    assert between[0].coordinate_axes == 'XY'
    assert between[2].coordinate_axes == 'XYZ'
    assert between[2].z == pytest.approx(7.0)


def test_point_between_scalar() -> None:
    a = gm.Point(0, 0, crs=4326)
    b = gm.Point(1, 0, crs=4326)
    mid = gm.point_between(a, b, 0.5, normalized=True)
    assert mid.x == pytest.approx(0.5, abs=0.01)


def test_point_nav_array_parity_is_bit_identical_to_scalar_loops() -> None:
    left = [
        gm.Point(-73.8, 40.6, z=1.0, m=10.0, crs=4326),
        gm.Point(-10.0, 45.0, z=2.0, m=20.0, crs=4326),
        gm.Point(12.0, -33.0, z=3.0, m=30.0, crs=4326),
    ]
    right = [
        gm.Point(-0.5, 51.6, z=4.0, m=40.0, crs=4326),
        gm.Point(2.0, 45.0, z=5.0, m=50.0, crs=4326),
        gm.Point(28.0, -20.0, z=6.0, m=60.0, crs=4326),
    ]
    lefts = gm.GeometryArray(left)
    rights = gm.GeometryArray(right)

    assert np.array_equal(
        gm.bearing(lefts, rights),
        np.array([gm.bearing(a, b) for a, b in zip(left, right, strict=True)]),
    )
    assert np.array_equal(
        gm.bearing(left[0], rights),
        np.array([gm.bearing(left[0], point) for point in right]),
    )
    assert np.array_equal(
        gm.bearing(lefts, right[0]),
        np.array([gm.bearing(point, right[0]) for point in left]),
    )
    assert np.array_equal(
        gm.distance(lefts, rights, path='rhumb'),
        np.array([
            gm.distance(a, b, path='rhumb') for a, b in zip(left, right, strict=True)
        ]),
    )
    assert np.array_equal(
        gm.distance(left[0], rights, path='rhumb'),
        np.array([gm.distance(left[0], point, path='rhumb') for point in right]),
    )
    assert np.array_equal(
        gm.distance(lefts, right[0], path='rhumb'),
        np.array([gm.distance(point, right[0], path='rhumb') for point in left]),
    )
    assert np.array_equal(
        gm.bearing(lefts, rights, path='rhumb'),
        np.array([
            gm.bearing(a, b, path='rhumb') for a, b in zip(left, right, strict=True)
        ]),
    )
    assert np.array_equal(
        gm.bearing(left[0], rights, path='rhumb'),
        np.array([gm.bearing(left[0], point, path='rhumb') for point in right]),
    )
    assert np.array_equal(
        gm.bearing(lefts, right[0], path='rhumb'),
        np.array([gm.bearing(point, right[0], path='rhumb') for point in left]),
    )

    assert _wkb_rows(lefts.destination(33.0, 12_345.0)) == [
        point.destination(33.0, 12_345.0).to_wkb() for point in left
    ]
    bearings = [33.0, 34.0, 35.0]
    distances = [12_345.0, 23_456.0, 34_567.0]
    assert _wkb_rows(lefts.destination(bearings, distances)) == [
        point.destination(bearing, distance).to_wkb()
        for point, bearing, distance in zip(left, bearings, distances, strict=True)
    ]
    assert _wkb_rows(lefts.destination(91.0, 234_567.0, path='rhumb')) == [
        point.destination(91.0, 234_567.0, path='rhumb').to_wkb() for point in left
    ]
    assert _wkb_rows(gm.point_between(lefts, rights, 0.375, normalized=True)) == [
        gm.point_between(a, b, 0.375, normalized=True).to_wkb()
        for a, b in zip(left, right, strict=True)
    ]
    assert _wkb_rows(gm.point_between(lefts, rights, 50_000.0)) == [
        gm.point_between(a, b, 50_000.0).to_wkb()
        for a, b in zip(left, right, strict=True)
    ]
    assert _wkb_rows(gm.point_between(left[0], rights, 0.5, normalized=True)) == [
        gm.point_between(left[0], point, 0.5, normalized=True).to_wkb()
        for point in right
    ]
    assert _wkb_rows(gm.point_between(lefts, right[0], 0.5, normalized=True)) == [
        gm.point_between(point, right[0], 0.5, normalized=True).to_wkb()
        for point in left
    ]

    probes = gm.GeometryArray([
        gm.Point(0.5, 0.1, crs=4326),
        gm.Point(0.5, -0.1, crs=4326),
        gm.Point(10.5, 1.0, crs=4326),
    ])
    starts = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(10.0, 0.0, crs=4326),
    ])
    ends = gm.GeometryArray([
        gm.Point(1.0, 0.0, crs=4326),
        gm.Point(1.0, 0.0, crs=4326),
        gm.Point(11.0, 0.0, crs=4326),
    ])
    assert np.array_equal(
        gm.cross_track_distance(probes, starts, ends),
        np.array([
            gm.cross_track_distance(p, s, e)
            for p, s, e in zip(probes, starts, ends, strict=True)
        ]),
    )
    assert np.array_equal(
        gm.cross_track_distance(probes[0], starts, ends),
        np.array([
            gm.cross_track_distance(probes[0], s, e)
            for s, e in zip(starts, ends, strict=True)
        ]),
    )
    assert np.array_equal(
        gm.cross_track_distance(probes, starts[0], ends),
        np.array([
            gm.cross_track_distance(p, starts[0], e)
            for p, e in zip(probes, ends, strict=True)
        ]),
    )
    assert np.array_equal(
        gm.cross_track_distance(probes, starts, ends[0]),
        np.array([
            gm.cross_track_distance(p, s, ends[0])
            for p, s in zip(probes, starts, strict=True)
        ]),
    )


def test_point_binary_plan_packed_planar_orientations_match_scalar() -> None:
    left = [
        gm.Point(0.0, 0.0, z=1.0, m=10.0, crs=3857),
        gm.Point(10.0, 5.0, z=2.0, m=20.0, crs=3857),
        gm.Point(-4.0, 8.0, z=3.0, m=30.0, crs=3857),
    ]
    right = [
        gm.Point(3.0, 4.0, z=4.0, m=40.0, crs=3857),
        gm.Point(8.0, 12.0, z=5.0, m=50.0, crs=3857),
        gm.Point(2.0, 1.0, z=6.0, m=60.0, crs=3857),
    ]
    lefts = gm.points(
        [point.x for point in left],
        [point.y for point in left],
        z=[point.z for point in left],
        m=[point.m for point in left],
        crs=3857,
    )
    rights = gm.points(
        [point.x for point in right],
        [point.y for point in right],
        z=[point.z for point in right],
        m=[point.m for point in right],
        crs=3857,
    )

    assert gm.bearing(left[0], right[0]) == gm.bearing(lefts[0], rights[0])
    assert np.array_equal(
        gm.bearing(left[0], rights),
        np.array([gm.bearing(left[0], point) for point in right]),
    )
    assert np.array_equal(
        gm.bearing(lefts, right[0]),
        np.array([gm.bearing(point, right[0]) for point in left]),
    )
    assert np.array_equal(
        gm.bearing(lefts, rights),
        np.array([gm.bearing(a, b) for a, b in zip(left, right, strict=True)]),
    )

    paired = gm.point_between(lefts, rights, 0.25, normalized=True)
    scalar_left = gm.point_between(left[0], rights, 0.25, normalized=True)
    scalar_right = gm.point_between(lefts, right[0], 0.25, normalized=True)
    assert _wkb_rows(paired) == [
        gm.point_between(a, b, 0.25, normalized=True).to_wkb()
        for a, b in zip(left, right, strict=True)
    ]
    assert _wkb_rows(scalar_left) == [
        gm.point_between(left[0], point, 0.25, normalized=True).to_wkb()
        for point in right
    ]
    assert _wkb_rows(scalar_right) == [
        gm.point_between(point, right[0], 0.25, normalized=True).to_wkb()
        for point in left
    ]
    assert paired.common_coordinate_axes == 'XYZM'
    assert paired[0].z == pytest.approx(1.75)
    assert paired[0].m == pytest.approx(17.5)


def test_point_binary_plan_mixed_axes_and_masks_preserve_rows() -> None:
    left = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=3857),
        None,
        gm.Point(10.0, 0.0, z=2.0, m=20.0, crs=3857),
    ])
    right = gm.GeometryArray([
        gm.Point(0.0, 10.0, crs=3857),
        gm.Point(5.0, 5.0, z=4.0, m=40.0, crs=3857),
        gm.Point(20.0, 10.0, z=6.0, m=60.0, crs=3857),
    ])
    scalar = gm.Point(30.0, 20.0, z=10.0, m=100.0, crs=3857)

    for actual, expected in (
        (
            gm.bearing(left, right),
            [gm.bearing(left[0], right[0]), np.nan, gm.bearing(left[2], right[2])],
        ),
        (
            gm.bearing(left, scalar),
            [gm.bearing(left[0], scalar), np.nan, gm.bearing(left[2], scalar)],
        ),
        (
            gm.bearing(scalar, left),
            [gm.bearing(scalar, left[0]), np.nan, gm.bearing(scalar, left[2])],
        ),
    ):
        np.testing.assert_array_equal(actual, expected)

    for actual, expected in (
        (
            gm.point_between(left, right, 0.5, normalized=True),
            [
                gm.point_between(left[0], right[0], 0.5, normalized=True),
                None,
                gm.point_between(left[2], right[2], 0.5, normalized=True),
            ],
        ),
        (
            gm.point_between(left, scalar, 0.5, normalized=True),
            [
                gm.point_between(left[0], scalar, 0.5, normalized=True),
                None,
                gm.point_between(left[2], scalar, 0.5, normalized=True),
            ],
        ),
        (
            gm.point_between(scalar, left, 0.5, normalized=True),
            [
                gm.point_between(scalar, left[0], 0.5, normalized=True),
                None,
                gm.point_between(scalar, left[2], 0.5, normalized=True),
            ],
        ),
    ):
        assert actual.is_missing.tolist() == [False, True, False]
        for got, want in zip(actual, expected, strict=True):
            if want is None:
                assert got is None
            else:
                assert got.to_wkb() == want.to_wkb()
        assert actual[0].coordinate_axes == 'XY'
        assert actual[2].coordinate_axes == 'XYZM'


def test_point_between_compact_nullable_pair_masks_preserve_rows() -> None:
    indices = np.arange(12, dtype=np.float64)
    left_points = gm.points(indices + 0.25, indices + 0.5)
    right_points = gm.points(indices + 3.25, indices + 4.5)
    left = gm.GeometryArray([
        None if row % 3 == 0 else left_points[row] for row in range(12)
    ])
    right = gm.GeometryArray([
        None if row % 4 == 0 else right_points[row] for row in range(12)
    ])

    result = gm.point_between(left, right, 0.5, normalized=True, unit='planar')

    expected_missing = [
        True,
        False,
        False,
        True,
        True,
        False,
        True,
        False,
        True,
        True,
        False,
        False,
    ]
    assert result.is_missing.tolist() == expected_missing
    expected_points = [
        None if missing else gm.Point(row + 1.75, row + 2.5).to_wkb()
        for row, missing in enumerate(expected_missing)
    ]
    actual = [None if geometry is None else geometry.to_wkb() for geometry in result]
    assert actual == expected_points
    assert result[1].to_wkb() == gm.Point(2.75, 3.5).to_wkb()


def test_point_between_all_missing_compact_rows_stay_missing() -> None:
    points = gm.GeometryArray([
        gm.Point(0.0, 0.0),
        None,
        gm.Point(2.0, 0.0),
        None,
    ])
    all_missing = points[[1, 3]]
    other = points[[0, 2]]

    result = gm.point_between(all_missing, other, 0.5, normalized=True)

    assert result.is_missing.tolist() == [True, True]
    assert result[0] is None
    assert result[1] is None


def test_point_nav_array_errors_keep_row_notes() -> None:
    origin = gm.Point(0.0, 0.0, crs=4326)
    targets = gm.GeometryArray([
        gm.Point(1.0, 0.0, crs=4326),
        gm.Point(0.0, 95.0, crs=4326),
    ])
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data',
    ) as domain_error:
        gm.bearing(origin, targets)
    assert domain_error.value.__notes__ == ['while processing array element 1']

    starts = gm.GeometryArray([origin, gm.Point(0.0, 95.0, crs=4326)])
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data',
    ) as destination_error:
        starts.destination(90.0, 1_000.0)
    assert destination_error.value.__notes__ == ['while processing array element 1']

    pole_crossers = gm.GeometryArray([origin, gm.Point(0.0, 89.0, crs=4326)])
    with pytest.raises(gm.InvalidGeometryError) as rhumb_destination_error:
        pole_crossers.destination(0.0, 200_000.0, path='rhumb')
    assert rhumb_destination_error.value.__notes__ == [
        'while processing array element 1'
    ]

    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data',
    ) as point_between_error:
        gm.point_between(origin, targets, 0.5, normalized=True)
    assert point_between_error.value.__notes__ == ['while processing array element 1']

    origins = gm.GeometryArray([origin, gm.Point(0.0, 95.0, crs=4326)])
    valid_targets = gm.GeometryArray([
        gm.Point(1.0, 0.0, crs=4326),
        gm.Point(1.0, 0.0, crs=4326),
    ])
    for operation in (
        lambda: gm.bearing(origins, targets[0]),
        lambda: gm.bearing(origins, valid_targets),
        lambda: gm.point_between(origins, targets[0], 0.5, normalized=True),
        lambda: gm.point_between(origins, valid_targets, 0.5, normalized=True),
    ):
        with pytest.raises(
            gm.InvalidGeometryError,
            match=r'invalid longitude/latitude \(0, 95\); coordinates are \(x, y\) = \(lon, lat\) — use swap_xy\(\) for latitude-first data',
        ) as row_error:
            operation()
        assert row_error.value.__notes__ == ['while processing array element 1']

    probes = gm.GeometryArray([
        gm.Point(0.5, 0.1, crs=4326),
        gm.Point(0.0, 0.1, crs=4326),
    ])
    starts = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(0.0, 0.0, crs=4326),
    ])
    ends = gm.GeometryArray([
        gm.Point(1.0, 0.0, crs=4326),
        gm.Point(0.0, 0.0, crs=4326),
    ])
    with pytest.raises(gm.InvalidGeometryError) as distinct_error:
        gm.cross_track_distance(probes, starts, ends)
    assert distinct_error.value.__notes__ == ['while processing array element 1']


def test_cross_track_array_broadcast_gates_frames_and_lengths() -> None:
    probes = gm.GeometryArray([
        gm.Point(0.5, 0.1, crs=4326),
        gm.Point(0.5, -0.1, crs=4326),
    ])
    starts = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(0.0, 0.0, crs=4326),
    ])
    mismatched_starts = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=3857),
        gm.Point(0.0, 0.0, crs=3857),
    ])
    ends = gm.GeometryArray([
        gm.Point(1.0, 0.0, crs=4326),
        gm.Point(1.0, 0.0, crs=4326),
    ])

    with pytest.raises(gm.CRSMismatchError, match='cross_track_distance'):
        gm.cross_track_distance(probes, mismatched_starts, ends)

    short_ends = gm.GeometryArray([gm.Point(1.0, 0.0, crs=4326)])
    with pytest.raises(gm.GeometryError, match='same length'):
        gm.cross_track_distance(probes, starts, short_ends)


def test_point_binary_plan_keeps_length_before_frame_validation() -> None:
    left = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        gm.Point(1.0, 0.0, crs=4326),
    ])
    short_mismatched = gm.GeometryArray([gm.Point(0.0, 0.0, crs=3857)])
    for operation in (
        lambda: gm.bearing(left, short_mismatched),
        lambda: gm.point_between(left, short_mismatched, 0.5, normalized=True),
    ):
        with pytest.raises(gm.GeometryError, match='same length'):
            operation()


def test_cross_track_distance_scalar() -> None:
    a, b = gm.Point(0, 0, crs=4326), gm.Point(1, 0, crs=4326)
    probe = gm.Point(0.5, 0.1, crs=4326)
    assert gm.cross_track_distance(probe, a, b) / 1000 == pytest.approx(11.1, abs=0.2)


@pytest.mark.parametrize(
    ('start', 'end'),
    [
        (gm.Point(0, 0, crs=4326), gm.Point(360, 0, crs=4326)),
        (gm.Point(0, 90, crs=4326), gm.Point(20, 90, crs=4326)),
        (gm.Point(0, 0, crs=4326), gm.Point(180, 0, crs=4326)),
        (gm.Point(0, 90, crs=4326), gm.Point(20, -90, crs=4326)),
    ],
)
def test_cross_track_rejects_coincident_and_antipodal_endpoints(
    start: gm.Point, end: gm.Point
) -> None:
    probe = gm.Point(5, 10, crs=4326)
    with pytest.raises(
        gm.InvalidGeometryError,
        match=r'unique great circle.*coincident and antipodal',
    ):
        gm.cross_track_distance(probe, start, end)


def test_cross_track_accepts_exactly_nonantipodal_nearby_endpoints() -> None:
    probe = gm.Point(5, 10, crs=4326)
    start = gm.Point(0, 0, crs=4326)
    end = gm.Point(180, 1e-10, crs=4326)
    assert np.isfinite(gm.cross_track_distance(probe, start, end))


def test_distance_rhumb_and_bearing_path() -> None:
    jfk, lhr = gm.Point(-73.8, 40.6, crs=4326), gm.Point(-0.5, 51.6, crs=4326)
    rhumb = gm.distance(jfk, lhr, path='rhumb')
    assert rhumb / 1000 == pytest.approx(5771.1, abs=1.0)
    assert rhumb > gm.distance(jfk, lhr)
    assert gm.bearing(jfk, lhr, path='rhumb') == pytest.approx(77.768, abs=0.01)
    with pytest.raises(
        gm.GeometryError, match="does not accept unit when path='rhumb'"
    ):
        gm.distance(jfk, lhr, path='rhumb', unit='meters')
    with pytest.raises(
        gm.CRSError, match="distance\\(path='rhumb'\\) requires a geographic CRS"
    ):
        gm.distance(gm.Point(0, 0), gm.Point(1, 1), path='rhumb')


def test_rhumb_destination_path_scalar() -> None:
    end = gm.Point(-10, 45, crs=4326).destination(90, 1_000_000, path='rhumb')
    assert (round(end.x, 3), round(end.y, 3)) == (2.683, 45.0)


def test_rhumb_destination_mixed_axis_points_preserve_rows() -> None:
    # GeographicLib RhumbSolve -p 10 destinations already pinned in
    # test_rhumb_trio_matches_rhumbsolve_oracles: due east on the 45° parallel
    # and due north from 45°S. The missing-row bearing/distance are unused.
    points = gm.GeometryArray([
        gm.Point(-10.0, 45.0, crs=4326, epoch=2020.0),
        None,
        gm.Point(170.0, -45.0, z=12.0, m=4.0, crs=4326, epoch=2020.0),
    ])
    out = points.destination(
        [90.0, 270.0, 0.0],
        [1_000_000.0, 250_000.0, 1_000_000.0],
        path='rhumb',
    )
    assert out.is_missing.tolist() == [False, True, False]
    assert out[1] is None
    assert out.crs.to_epsg() == 4326
    assert out.epoch == 2020.0
    assert out[0].coordinate_axes == 'XY'
    assert (out[0].x, out[0].y) == (
        pytest.approx(2.682817246984, abs=1e-08, rel=0.0),
        pytest.approx(45.0, abs=1e-12, rel=0.0),
    )
    assert out[0].crs.to_epsg() == 4326
    assert out[0].epoch == 2020.0
    assert out[2].coordinate_axes == 'XYZM'
    assert out[2].x == 170.0
    assert out[2].y == pytest.approx(-35.994607984102956, abs=1e-08, rel=0.0)
    assert (out[2].z, out[2].m) == (12.0, 4.0)
    assert out[2].crs.to_epsg() == 4326
    assert out[2].epoch == 2020.0


def test_rhumb_destination_nonpoint_array_keeps_row_type_error() -> None:
    rows = gm.GeometryArray([
        gm.Point(-5.0, 10.0, crs=4326),
        None,
        gm.LineString([(-5.0, 10.0), (0.0, 12.0)], crs=4326),
    ])
    with pytest.raises(gm.GeometryTypeError) as err:
        rows.destination(
            [90.0, 45.0, 0.0],
            [1000.0, 250_000.0, 1000.0],
            path='rhumb',
        )
    assert err.value.__notes__ == ['while processing array element 2']


def test_point_between_rhumb_path_matches_inverse_direct_and_array_lanes() -> None:
    start = gm.Point(-73.8, 40.6, z=2.0, m=10.0, crs=4326)
    end = gm.Point(-0.5, 51.6, z=6.0, m=30.0, crs=4326)
    total = gm.distance(start, end, path='rhumb')
    bearing = gm.bearing(start, end, path='rhumb')

    midpoint = gm.point_between(start, end, 0.5, normalized=True, path='rhumb')
    by_distance = gm.point_between(start, end, total / 2.0, path='rhumb')
    direct = start.destination(bearing, total / 2.0, path='rhumb')

    assert midpoint.to_wkb() == by_distance.to_wkb()
    assert midpoint.x == pytest.approx(direct.x, abs=1e-09)
    assert midpoint.y == pytest.approx(direct.y, abs=1e-09)
    assert gm.distance(start, midpoint, path='rhumb') == pytest.approx(
        total / 2.0, abs=1e-06
    )
    assert gm.bearing(start, midpoint, path='rhumb') == pytest.approx(
        bearing, abs=1e-10
    )
    assert (midpoint.z, midpoint.m) == (4.0, 20.0)

    assert (
        gm.point_between(start, end, 0.0, normalized=True, path='rhumb').to_wkb()
        == start.to_wkb()
    )
    assert (
        gm.point_between(start, end, 1.0, normalized=True, path='rhumb').to_wkb()
        == end.to_wkb()
    )

    starts = gm.GeometryArray([start, end])
    ends = gm.GeometryArray([end, start])
    midpoints = gm.point_between(starts, ends, 0.5, normalized=True, path='rhumb')
    assert _wkb_rows(midpoints) == [
        gm.point_between(a, b, 0.5, normalized=True, path='rhumb').to_wkb()
        for a, b in zip((start, end), (end, start), strict=True)
    ]

    with pytest.raises(
        gm.GeometryError, match="does not accept unit when path='rhumb'"
    ):
        gm.point_between(start, end, total / 2.0, path='rhumb', unit='meters')
    with pytest.raises(gm.CRSError, match='requires a geographic CRS'):
        gm.point_between(
            gm.Point(0, 0), gm.Point(1, 0), 0.5, normalized=True, path='rhumb'
        )


def test_geodesic_and_rhumb_between_have_distinct_known_witnesses() -> None:
    start = gm.Point(-73.8, 40.6, crs=4326)
    end = gm.Point(-0.5, 51.6, crs=4326)

    geodesic = gm.point_between(start, end, 0.5, normalized=True)
    rhumb = gm.point_between(start, end, 0.5, normalized=True, path='rhumb')

    assert (geodesic.x, geodesic.y) == pytest.approx(
        (-41.39507594238808, 52.27369979507356), abs=1e-12
    )
    assert (rhumb.x, rhumb.y) == pytest.approx(
        (-38.97943298625387, 46.10264990687271), abs=1e-12
    )
    assert (geodesic.x, geodesic.y) != pytest.approx((rhumb.x, rhumb.y), abs=1e-6)


def test_point_between_and_rhumb_destination_path_broadcast_float_lanes() -> None:
    left = gm.Point(0, 0)
    right = gm.Point(10, 0)
    between = gm.point_between(left, right, [0.25, 0.75], normalized=True)
    assert between.to_wkt() == ['POINT (2.5 0)', 'POINT (7.5 0)']

    lefts = gm.GeometryArray([left, left])
    rights = gm.GeometryArray([right, gm.Point(0, 10)])
    between_rows = gm.point_between(lefts, rights, [0.25, 0.5], normalized=True)
    assert between_rows.to_wkt() == ['POINT (2.5 0)', 'POINT (0 5)']

    origin = gm.Point(-10, 45, crs=4326)
    rhumb = origin.destination([90, 90], [0, 1_000_000], path='rhumb')
    assert rhumb[0] == origin
    assert (round(rhumb[1].x, 3), round(rhumb[1].y, 3)) == (2.683, 45.0)

    origins = gm.GeometryArray([origin, origin])
    rhumb_rows = origins.destination([0, 90], [1000, 1_000_000], path='rhumb')
    expected = [
        origins[row].destination([0, 90][row], [1000, 1_000_000][row], path='rhumb')
        for row in range(2)
    ]
    assert rhumb_rows.to_wkb() == [value.to_wkb() for value in expected]

    with pytest.raises(gm.InvalidGeometryError, match='same length'):
        gm.point_between(lefts, rights, [0.25], normalized=True)
    with pytest.raises(gm.InvalidGeometryError, match='same length'):
        origins.destination([0, 90, 180], 1000, path='rhumb')


def test_point_navigation_uses_binary_free_functions_and_receiver_destination() -> None:
    pt = gm.Point(0, 0, crs=4326)
    points = gm.GeometryArray([pt])
    assert not hasattr(gm, 'destination')
    assert hasattr(pt, 'destination')
    assert hasattr(points, 'destination')
    assert not hasattr(gm.LineString([(0, 0), (1, 0)]), 'destination')
    assert not hasattr(gm.box(0, 0, 1, 1), 'destination')
    with pytest.raises(AttributeError, match="object has no attribute 'bearing'"):
        pt.bearing(gm.Point(1, 0, crs=4326))
    with pytest.raises(AttributeError, match="object has no attribute 'point_between'"):
        pt.point_between(gm.Point(1, 0, crs=4326), 0.5)
    assert gm.bearing(pt, gm.Point(1, 0, crs=4326)) == pytest.approx(90.0)
    assert gm.point_between(
        pt, gm.Point(1, 0, crs=4326), 0.5, normalized=True
    ).x == pytest.approx(0.5)


def _empty() -> gm.Geometry:
    return gm.from_wkt('POINT EMPTY', crs=4326)


@pytest.mark.parametrize(
    ('call', 'expected'),
    [
        (gm.bearing, 'nan'),
        (lambda a, b: gm.bearing(a, b, path='rhumb'), 'nan'),
        (lambda a, b: gm.distance(a, b, path='rhumb'), 'inf'),
        (lambda a, b: gm.cross_track_distance(a, b, gm.Point(2, 0, crs=4326)), 'nan'),
        (lambda a, b: gm.point_between(a, b, 10.0), 'POINT EMPTY'),
        (lambda a, b: gm.point_between(a, b, 10.0, path='rhumb'), 'POINT EMPTY'),
    ],
    ids=[
        'bearing',
        'rhumb-bearing',
        'distance-rhumb',
        'cross-track',
        'point-between',
        'rhumb-point-between',
    ],
)
def test_navigation_answers_an_empty_point(call: Any, expected: str) -> None:
    point = gm.Point(1, 1, crs=4326)

    def answer(value: Any) -> str:
        return value.to_wkt() if isinstance(value, gm.Geometry) else repr(float(value))

    assert answer(call(_empty(), point)) == expected
    assert answer(call(point, _empty())) == expected
    rows = call(gm.GeometryArray([point, _empty()]), point)
    assert answer(rows[1]) == expected
    with pytest.raises(gm.GeometryTypeError, match='requires a Point'):
        call(gm.box(0, 0, 1, 1, crs=4326), point)


@pytest.mark.parametrize('path', ['geodesic', 'rhumb'])
def test_destination_from_an_empty_point_is_empty(path: str) -> None:
    assert _empty().destination(90, 10, path=path).to_wkt() == 'POINT EMPTY'
    assert _empty().destination([0, 90], 10, path=path).to_wkt() == ['POINT EMPTY'] * 2
    rows = gm.GeometryArray([gm.Point(0, 0, crs=4326), _empty()])
    assert rows.destination(90, 10, path=path).to_wkt()[1] == 'POINT EMPTY'


def test_cardinal_destinations_are_exact() -> None:
    planar = gm.Point(0, 0).destination([0, 90, 180, 270], 1)
    assert planar.to_wkt() == [
        'POINT (0 1)',
        'POINT (1 0)',
        'POINT (0 -1)',
        'POINT (-1 0)',
    ]
    east = gm.Point(10, 50, crs=4326).destination(90, 1000, path='rhumb')
    assert east.y == 50.0
    north = gm.Point(10, 50, crs=4326).destination(0, 1000, path='rhumb')
    assert north.x == 10.0
