import pytest

from benchroom import bootstrap_interval, median_interval, percentile


def test_percentile_interpolates():
    assert percentile([8, 2, 4], 0.25) == 3


def test_constant_runs_have_constant_interval():
    assert median_interval([7, 7, 7]) == (7, 7)


def test_sampled_bootstrap_preserves_constant_statistic():
    assert bootstrap_interval(20, lambda _indexes: 12.5, samples=100) == (12.5, 12.5)


def test_exact_two_run_bootstrap():
    # The four equally likely resampled medians are 2, 5, 5, and 8.
    assert median_interval([2, 8]) == pytest.approx((2.225, 7.775))


def test_paired_resampling_keeps_shared_variation():
    before = [2, 20, 200]
    after = [4, 40, 400]
    assert bootstrap_interval(
        3,
        lambda indexes: (
            sum(after[i] for i in indexes) / sum(before[i] for i in indexes)
        ),
    ) == (2, 2)


@pytest.mark.parametrize('values', [[], [1, 2]])
def test_invalid_percentile_rejected(values):
    with pytest.raises(ValueError, match=r'at least one value|between zero and one'):
        percentile(values, 1.5)
