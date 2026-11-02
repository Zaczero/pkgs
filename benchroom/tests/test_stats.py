import re

import pytest

from benchroom import bootstrap_interval, median_interval, percentile


def test_percentile_interpolates():
    assert percentile([8, 2, 4], 0.25) == 3


def test_percentile_interpolates_across_extreme_finite_values():
    assert percentile([-1e308, 1e308], 0.25) == -5e307
    assert percentile([-1e308, 1e308], 0.5) == 0
    assert percentile([-1e308, 1e308], 0.75) == 5e307


def test_constant_runs_have_constant_interval():
    assert median_interval([7, 7, 7]) == (7, 7)


@pytest.mark.parametrize('size', [2, 9, 10])
def test_median_interval_keeps_extreme_finite_observations(size):
    assert median_interval([1e308] * size) == (1e308, 1e308)


def test_sampled_median_interval_spans_extreme_signed_observations():
    assert median_interval([-1e308] * 10 + [1e308] * 10) == (-1e308, 1e308)


def test_exact_median_interval_retains_subnormal_midpoints():
    smallest = float.fromhex('0x0.0000000000001p-1022')
    assert median_interval([smallest, smallest]) == (smallest, smallest)


def test_sampled_extreme_interval_retains_subnormal_majority():
    smallest = float.fromhex('0x0.0000000000001p-1022')
    assert median_interval([smallest] * 9 + [1e308]) == (smallest, smallest)


def test_sampled_median_preserves_constant_observations():
    assert median_interval([7] * 20) == (7, 7)


def test_sampled_median_interval_contains_the_center():
    values = [1, 3, 7, 9, 10, 12, 20, 21, 25]
    low, high = median_interval(values, seed=42)
    assert 1 <= low < 10
    assert 10 < high <= 25


def test_sampled_bootstrap_preserves_constant_statistic():
    assert bootstrap_interval(20, lambda _indexes: 12.5, samples=100) == (12.5, 12.5)


def test_exact_two_run_bootstrap():
    # The four equally likely resampled medians are 2, 5, 5, and 8.
    assert median_interval([2, 8]) == pytest.approx((2.225, 7.775))


def test_exact_bootstrap_interpolates_extreme_signed_medians():
    assert median_interval([-1e308, 1e308]) == pytest.approx((-9.25e307, 9.25e307))


def test_paired_resampling_keeps_shared_variation():
    before = [2, 20, 200]
    after = [4, 40, 400]
    assert bootstrap_interval(
        3,
        lambda indexes: (
            sum(after[i] for i in indexes) / sum(before[i] for i in indexes)
        ),
    ) == (2, 2)


@pytest.mark.parametrize(
    ('values', 'fraction', 'message'),
    [
        pytest.param([], 0.5, 'values must not be empty', id='empty-values'),
        pytest.param(
            [1, 2],
            -0.5,
            'fraction must be between 0 and 1, got -0.5',
            id='fraction-below-range',
        ),
        pytest.param(
            [1, 2],
            1.5,
            'fraction must be between 0 and 1, got 1.5',
            id='fraction-above-range',
        ),
    ],
)
def test_invalid_percentile_names_the_input(values, fraction, message):
    with pytest.raises(ValueError, match=re.escape(message)) as caught:
        percentile(values, fraction)
    assert str(caught.value) == message


def test_empty_median_interval_names_values():
    with pytest.raises(ValueError, match='values must not be empty'):
        median_interval([])


@pytest.mark.parametrize(
    ('size', 'samples', 'message'),
    [
        pytest.param(0, 5, 'size must be at least 1, got 0', id='empty-size'),
        pytest.param(3, 0, 'samples must be at least 1, got 0', id='empty-samples'),
    ],
)
def test_invalid_bootstrap_names_the_parameter(size, samples, message):
    with pytest.raises(ValueError, match=re.escape(message)) as caught:
        bootstrap_interval(size, lambda _indexes: 1.0, samples=samples)
    assert str(caught.value) == message
