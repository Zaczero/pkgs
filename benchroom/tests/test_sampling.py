import itertools

import pytest

from benchroom.sampling import (
    SamplingPolicy,
    assess_ratios,
    fits_budget,
    next_batch_size,
    rates_are_stable,
)
from benchroom.stats import median_order_interval


@pytest.mark.parametrize(
    ('size', 'error', 'orders', 'coverage'),
    [
        (6, 1 / 32, (1, 6), 62 / 64),
        (12, 3 / 160, (2, 11), 4070 / 4096),
    ],
)
def test_exact_binomial_order_indices(size, error, orders, coverage):
    interval = median_order_interval(list(range(1, size + 1)), error_probability=error)

    assert (interval.lower_order, interval.upper_order) == orders
    assert interval.interval == orders
    assert interval.achieved_coverage == coverage


def test_too_few_blocks_cannot_supply_a_finite_95_percent_median_interval():
    interval = median_order_interval([1.0] * 5, error_probability=0.05)

    assert interval.interval is None
    assert interval.lower_order is None


def test_both_allowed_looks_respect_the_across_look_error_budget():
    misses = 0
    for signs in itertools.product((-1.0, 1.0), repeat=12):
        first = median_order_interval(signs[:6], error_probability=1 / 32)
        last = median_order_interval(signs, error_probability=3 / 160)
        assert first.interval is not None
        assert last.interval is not None
        misses += any(
            interval[0] > 0 or interval[1] < 0
            for interval in (first.interval, last.interval)
        )

    assert misses == 140
    assert misses / 4096 <= 1 / 32 + 3 / 160
    assert SamplingPolicy().confidence == 0.95


def decision(values):
    return assess_ratios(values, block_ids=list(range(1, len(values) + 1)))


def test_consistent_direction_stops_at_the_first_look():
    result = decision([1.2, 1.4, 1.3, 1.5, 1.6, 1.7])

    assert result.complete
    assert result.reason == 'direction-resolved'
    assert result.direction == 'above'
    assert result.estimate == 1.45


def test_precision_stop_does_not_claim_equivalence_or_a_direction():
    result = decision([0.98, 1.01, 0.99, 1.02, 1.03, 1.0])

    assert result.complete
    assert result.reason == 'precision-met'
    assert result.direction == 'unresolved'


def test_unresolved_first_look_requests_twelve_then_finishes_inconclusive():
    result = decision([0.5, 2.0] * 3)
    final = decision([0.5, 2.0] * 6)

    assert not result.complete
    assert result.next_look == 12
    assert final.complete
    assert final.reason == 'maximum-look'
    assert final.direction == 'unresolved'


def test_ratio_estimate_preserves_the_ordinary_even_sample_median():
    result = decision([1.0, 2.0, 3.0, 9.0, 10.0, 11.0])

    assert result.estimate == 6.0


@pytest.mark.parametrize(
    ('values', 'ids', 'message'),
    [
        ([1.0] * 6, [1, 2, 3, 4, 5], 'identity'),
        ([1.0] * 6, [1, 2, 3, 4, 5, 5], 'identity'),
        ([1.0] * 5, [1, 2, 3, 4, 5], 'prespecified'),
        ([1.0] * 7, list(range(7)), 'prespecified'),
        ([1.0] * 5 + [float('nan')], list(range(6)), 'finite'),
        ([1.0] * 5 + [0.0], list(range(6)), 'positive'),
    ],
)
def test_invalid_or_pseudoreplicated_observations_are_rejected(values, ids, message):
    with pytest.raises(ValueError, match=message):
        assess_ratios(values, block_ids=ids)


def test_optional_work_protects_required_matrix_and_finish_reserve():
    assert fits_budget(
        remaining_seconds=100,
        required_seconds=60,
        optional_seconds=20,
        reserve_seconds=20,
    )
    assert not fits_budget(
        remaining_seconds=99,
        required_seconds=60,
        optional_seconds=20,
        reserve_seconds=20,
    )
    assert not fits_budget(remaining_seconds=-1, required_seconds=0, optional_seconds=1)


def test_zero_or_disagreeing_rate_windows_do_not_settle_calibration():
    assert not rates_are_stable(0, 0)
    assert not rates_are_stable(100, 130)
    assert rates_are_stable(100, 108)


def test_batch_calibration_doubles_until_its_minimum_exposure():
    assert next_batch_size(4, 0.0005, minimum_seconds=0.001) == 8
    assert next_batch_size(8, 0.001, minimum_seconds=0.001) == 8


@pytest.mark.parametrize(
    'policy',
    [
        {'looks': (6, 6)},
        {'looks': (12, 6)},
        {'error_probabilities': (0.5, 0.5)},
        {'precision_ratio': 0.9},
    ],
)
def test_malformed_policy_is_rejected(policy):
    with pytest.raises(ValueError, match=r'looks|error|precision'):
        SamplingPolicy(**policy)
