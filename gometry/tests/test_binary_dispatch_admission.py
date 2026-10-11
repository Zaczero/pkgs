"""Binary metric admission keeps operand order and row sentinels stable."""

from __future__ import annotations

from typing import TYPE_CHECKING

import gometry as gm
import numpy as np
import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


def test_scalar_array_metric_directions_preserve_missing_and_empty_rows() -> None:
    scalar = gm.Point(0.0, 0.0)
    values = gm.GeometryArray([
        gm.Point(3.0, 0.0),
        None,
        gm.from_wkt('POINT EMPTY'),
    ])

    for operands in ((scalar, values), (values, scalar)):
        np.testing.assert_allclose(
            gm.distance(*operands),
            [3.0, np.nan, np.inf],
            equal_nan=True,
        )
        np.testing.assert_array_equal(
            gm.dwithin(*operands, 3.0),
            [True, False, False],
        )


def test_three_d_no_fast_scalar_array_lane_preserves_direction_and_missing() -> None:
    scalar = gm.Point(0.0, 0.0, z=0.0, crs=3857)
    values = gm.GeometryArray([
        gm.Point(0.0, 0.0, z=3.0, crs=3857),
        None,
    ])

    for operands in ((scalar, values), (values, scalar)):
        np.testing.assert_allclose(
            gm.distance_3d(*operands),
            [3.0, np.nan],
            equal_nan=True,
        )


def test_similarity_and_shortest_line_normalize_nullable_iterable_operands() -> None:
    point = gm.Point(1.0, 1.0)
    reference = gm.Point(3.0, 1.0)
    line = gm.LineString([(0.0, 0.0), (2.0, 0.0)])

    def batches(values):
        return (
            lambda: values,
            lambda: iter(values),
            lambda: np.asarray(values, dtype=object),
        )

    for batch in batches([point, None]):
        for reverse in (False, True):
            left = (lambda: reference) if reverse else batch
            right = batch if reverse else (lambda: reference)
            result = gm.shortest_line(left(), right())
            assert result.to_wkt() == [
                'LINESTRING (3 1, 1 1)' if reverse else 'LINESTRING (1 1, 3 1)',
                None,
            ]
            np.testing.assert_allclose(
                gm.hausdorff_distance(left(), right()),
                [2.0, np.nan],
                equal_nan=True,
            )

    for batch in batches([line, None]):
        for reverse in (False, True):
            left = (lambda: line) if reverse else batch
            right = batch if reverse else (lambda: line)
            np.testing.assert_allclose(
                gm.frechet_distance(left(), right()),
                [0.0, np.nan],
                equal_nan=True,
            )


@pytest.mark.parametrize(
    'operation',
    [
        pytest.param(gm.hausdorff_distance, id='hausdorff'),
        pytest.param(gm.frechet_distance, id='frechet'),
        pytest.param(gm.shortest_line, id='shortest-line'),
    ],
)
def test_metric_batch_normalization_preserves_provider_iteration_errors(
    operation: Callable[..., object],
) -> None:
    failure = RuntimeError('row source unavailable')

    class FailingRows:
        def __iter__(self) -> Iterator[object]:
            raise failure

    line = gm.LineString([(0.0, 0.0), (2.0, 0.0)])
    with pytest.raises(RuntimeError) as exc_info:
        operation(FailingRows(), line)
    assert exc_info.value is failure


@pytest.mark.parametrize(
    'operation',
    [
        pytest.param(gm.hausdorff_distance, id='hausdorff'),
        pytest.param(gm.frechet_distance, id='frechet'),
        pytest.param(gm.shortest_line, id='shortest-line'),
    ],
)
def test_metric_batch_normalization_classifies_non_iterables(
    operation: Callable[..., object],
) -> None:
    line = gm.LineString([(0.0, 0.0), (2.0, 0.0)])

    with pytest.raises(TypeError):
        operation(object(), line)


def test_geographic_filled_metric_results_are_order_independent() -> None:
    polygon = gm.box(0.0, 0.0, 1.0, 1.0, crs=4326)
    point = gm.Point(0.5, 0.5, crs=4326)

    distances = [
        gm.distance(*operands) for operands in ((polygon, point), (point, polygon))
    ]
    within = [
        gm.dwithin(*operands, 0.0) for operands in ((polygon, point), (point, polygon))
    ]

    assert distances == [0.0, 0.0]
    assert within == [True, True]
