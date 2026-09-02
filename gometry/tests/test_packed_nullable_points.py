"""Columnar nullable point transforms and bounds."""

from __future__ import annotations

import gometry as gm
import numpy as np
import pytest


def _assert_point_rows(
    actual: gm.GeometryArray, expected: list[gm.Geometry | None]
) -> None:
    assert actual.is_missing.tolist() == [row is None for row in expected]
    for got, want in zip(actual, expected, strict=True):
        if want is None:
            assert got is None
            continue
        assert got is not None
        assert (got.x, got.y) == pytest.approx((want.x, want.y))
        if want.has_z:
            assert got.z == pytest.approx(want.z)
        if want.has_m:
            assert got.m == pytest.approx(want.m)


def test_sparse_nullable_points_to_crs_and_bounds_match_dense_rows() -> None:
    count = 1_000
    dense = gm.points(
        [float(index) / 10.0 for index in range(count)],
        [40.0 + float(index) / 100.0 for index in range(count)],
        crs=4326,
    )
    nullable = gm.GeometryArray([
        None if index == 0 else row for index, row in enumerate(dense)
    ])

    projected = nullable.to_crs(3857)
    expected = [None, *dense[1:].to_crs(3857)]
    _assert_point_rows(projected, expected)
    expected_bounds = np.asarray([
        [np.nan] * 4 if row is None else row.bounds for row in expected
    ])
    np.testing.assert_allclose(
        np.asarray(projected.bounds),
        expected_bounds,
        equal_nan=True,
    )


@pytest.mark.parametrize('measured', [False, True])
@pytest.mark.parametrize('with_z', [False, True])
def test_nullable_point_to_crs_preserves_zm_axes(
    measured: bool,
    with_z: bool,
) -> None:
    def point(x: float, y: float, value: float) -> gm.Geometry:
        return gm.Point(
            x,
            y,
            z=value if with_z else None,
            m=value + 10.0 if measured else None,
            crs=4326,
        )

    values = [point(-1.0, 2.0, 3.0), None, point(5.0, 6.0, 7.0)]
    actual = gm.GeometryArray(values).to_crs(3857)
    expected = [values[0].to_crs(3857), None, values[2].to_crs(3857)]  # type: ignore[union-attr]
    _assert_point_rows(actual, expected)


def test_nullable_point_gather_preserves_reversed_and_duplicate_logical_order() -> None:
    source = gm.GeometryArray([
        gm.Point(0.0, 0.0, crs=4326),
        None,
        gm.Point(2.0, 2.0, crs=4326),
        gm.Point(4.0, 4.0, crs=4326),
    ])
    rows = [3, 0, 1, 3, 2, 0]
    selected = source[rows]
    expected = [source[row] for row in rows]

    actual = selected.to_crs(3857)
    _assert_point_rows(
        actual, [None if row is None else row.to_crs(3857) for row in expected]
    )
    np.testing.assert_array_equal(
        gm.equals_exact(selected, selected, 0.0),
        [True, True, False, True, True, True],
    )
    assert np.asarray(actual.bounds).shape == (len(rows), 4)
    assert np.isnan(np.asarray(actual.bounds)[2]).all()


def test_all_missing_compact_points_transform_to_typed_empty_payload() -> None:
    source = gm.GeometryArray([gm.Point(1.0, 2.0, crs=4326), None])
    all_missing = source[[1, 1, 1]]

    assert all_missing.nbytes == 0
    projected = all_missing.to_crs(3857)
    assert projected.is_missing.tolist() == [True, True, True]
    assert projected.nbytes == 0
    assert np.asarray(projected.coords.x).size == 0
    assert np.isnan(np.asarray(projected.bounds)).all()


def test_geographic_nullable_point_bounds_keep_logical_rows() -> None:
    values = gm.GeometryArray([
        gm.Point(170.0, 10.0, crs='OGC:CRS84'),
        None,
        gm.Point(-3.0, 4.0, crs='OGC:CRS84'),
    ])

    np.testing.assert_allclose(
        np.asarray(values.bounds),
        [[170.0, 10.0, 170.0, 10.0], [np.nan] * 4, [-3.0, 4.0, -3.0, 4.0]],
        equal_nan=True,
    )
