"""Adaptive benchmark calibration, planning, and median uncertainty."""

import math
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench.sampling import (
    CalibrationPoint,
    ScenarioSamples,
    allocate_production_blocks,
    calibration_duration,
    calibration_status,
    common_duration,
    median_order_interval,
    paired_log_ratio_scatter,
)


def point(duration: float, rate: float) -> CalibrationPoint:
    return CalibrationPoint(duration, rate, duration + 0.25, int(rate * duration))


def test_calibration_stops_at_the_first_stable_adjacent_windows():
    windows = [point(0.5, 100), point(1.0, 108)]

    assert calibration_duration([], fixed_count=False) == 0.5
    assert calibration_duration(windows[:1], fixed_count=False) == 1.0
    assert calibration_duration(windows, fixed_count=False) is None
    assert calibration_status(windows, fixed_count=False) == ('stable', 1.0)


def test_calibration_extends_after_disagreement_and_records_exhaustion():
    windows = [point(0.5, 100), point(1.0, 130), point(2.0, 108)]

    assert calibration_duration(windows[:2], fixed_count=False) == 2.0
    assert calibration_duration(windows, fixed_count=False) == 4.0
    assert calibration_status([*windows, point(4.0, 140)], fixed_count=False) == (
        'calibration-limited',
        4.0,
    )


def test_common_duration_gives_each_competitor_equal_exposure():
    duration, limited = common_duration(
        {
            'h2corn': {'status': 'stable', 'selected_duration_seconds': 0.5},
            'hypercorn': {'status': 'stable', 'selected_duration_seconds': 2.0},
            'gunicorn': {
                'status': 'calibration-limited',
                'selected_duration_seconds': 4.0,
            },
        },
        fixed_count=False,
    )

    assert duration == 4.0
    assert limited


def test_fixed_count_calibration_uses_the_complete_workload():
    points = [CalibrationPoint(None, 240.0, 6.5, 16)]

    assert calibration_duration(points, fixed_count=True) is None
    assert calibration_status(points, fixed_count=True) == ('fixed-count-cost', None)
    assert common_duration(
        {'h2corn': {'status': 'fixed-count-cost'}},
        fixed_count=True,
    ) == (None, False)


def test_pilot_scatter_uses_only_common_paired_blocks():
    assert paired_log_ratio_scatter({
        'h2corn': [100.0, 200.0, 300.0],
        'hypercorn': [50.0, 100.0],
    }) == pytest.approx(0.0)
    assert paired_log_ratio_scatter({
        'h2corn': [100.0, 200.0],
        'hypercorn': [50.0, 140.0],
    }) == pytest.approx(abs(math.log(2) - math.log(10 / 7)) / math.sqrt(2))


def test_noisy_scenarios_get_nine_blocks_only_when_the_target_fits():
    fit = allocate_production_blocks(
        scenario_costs={'steady': 10.0, 'noisy': 10.0},
        pilot_scatter={'steady': 0.01, 'noisy': 0.20},
        elapsed_seconds=900.0,
        target_seconds=1_150.0,
        hard_seconds=2_700.0,
        report_reserve_seconds=30.0,
        cleanup_reserve_seconds=30.0,
    )
    no_fit = allocate_production_blocks(
        scenario_costs={'steady': 10.0, 'noisy': 10.0},
        pilot_scatter={'steady': 0.01, 'noisy': 0.20},
        elapsed_seconds=1_000.0,
        target_seconds=1_100.0,
        hard_seconds=2_700.0,
        report_reserve_seconds=30.0,
        cleanup_reserve_seconds=30.0,
    )

    assert fit['blocks'] == {'steady': 6, 'noisy': 9}
    assert fit['target_met']
    assert no_fit['blocks'] == {'steady': 6, 'noisy': 6}
    assert not no_fit['target_met']


def test_production_blocks_do_not_depend_on_a_pilot_winner():
    quiet_scatter = paired_log_ratio_scatter({
        'h2corn': [200.0, 400.0],
        'hypercorn': [100.0, 200.0],
    })
    winning_scatter = paired_log_ratio_scatter({
        'h2corn': [20_000.0, 40_000.0],
        'hypercorn': [100.0, 200.0],
    })
    plan = allocate_production_blocks(
        scenario_costs={'quiet': 10.0, 'winning': 10.0},
        pilot_scatter={
            'quiet': quiet_scatter,
            'winning': winning_scatter,
        },
        elapsed_seconds=0,
        target_seconds=1_000.0,
        hard_seconds=2_700.0,
        report_reserve_seconds=30.0,
        cleanup_reserve_seconds=30.0,
    )

    assert plan['blocks'] == {'quiet': 6, 'winning': 6}


def test_six_and_nine_paired_blocks_get_exact_conservative_median_intervals():
    six = median_order_interval([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    nine = median_order_interval([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])

    assert six['interval'] == [1.0, 6.0]
    assert six['achieved_coverage'] == 62 / 64
    assert (six['lower_order'], six['upper_order']) == (1, 6)
    assert nine['interval'] == [2.0, 8.0]
    assert nine['achieved_coverage'] == 492 / 512
    assert (nine['lower_order'], nine['upper_order']) == (2, 8)
    assert 'independent paired blocks' in six['method']


def test_median_interval_rejects_too_few_or_nonfinite_blocks():
    with pytest.raises(ValueError, match='at least six'):
        median_order_interval([1.0, 2.0, 3.0, 4.0, 5.0])
    with pytest.raises(ValueError, match='finite'):
        median_order_interval([1.0, float('nan'), 3.0, 4.0, 5.0, 6.0])


def test_six_block_minimum_must_fit_the_hard_budget():
    with pytest.raises(ValueError, match='minimum six-block'):
        allocate_production_blocks(
            scenario_costs={'http': 100.0, 'grpc': 100.0},
            pilot_scatter={'http': 0.5, 'grpc': 0.5},
            elapsed_seconds=2_000.0,
            target_seconds=1_800.0,
            hard_seconds=2_700.0,
            report_reserve_seconds=200.0,
            cleanup_reserve_seconds=200.0,
        )


def test_failed_cell_retains_prior_blocks_but_is_not_a_successful_cell():
    samples = ScenarioSamples(('h2corn', 'gunicorn'))
    samples.add('h2corn', 1, {'rps': 100.0})
    samples.add('h2corn', 2, {'rps': 105.0})
    samples.add('gunicorn', 1, {'rps': 45.0})
    samples.add('gunicorn', 2, {'rps': 46.0})
    samples.fail(
        'gunicorn',
        {
            'phase': 'correctness-after',
            'block': 3,
            'status': 'response_validation_failed',
        },
    )

    assert samples.values()['gunicorn'] == [{'rps': 45.0}, {'rps': 46.0}]
    assert samples.block_ids()['gunicorn'] == [1, 2]
    assert 'gunicorn' not in samples.successful_values()
    assert samples.failures['gunicorn']['block'] == 3


def test_failed_cell_cannot_accept_later_success():
    samples = ScenarioSamples(('h2corn',))
    samples.fail('h2corn', {'block': 1, 'status': 'failed'})

    with pytest.raises(ValueError, match='already terminal'):
        samples.add('h2corn', 2, {'rps': 42.0})
