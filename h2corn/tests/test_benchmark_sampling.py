"""Adaptive benchmark calibration, planning, and median uncertainty."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench.sampling import (
    CalibrationPoint,
    ScenarioSamples,
    allocate_production_blocks,
    calibration_production_cost,
    calibration_settled,
    calibration_status,
    calibration_windows,
    common_duration,
    median_order_interval,
    production_warmup_seconds,
)


def point(duration: float, rate: float) -> CalibrationPoint:
    return CalibrationPoint(duration, rate, duration + 0.25, int(rate * duration))


def test_calibration_stops_at_the_first_stable_adjacent_windows():
    windows = [point(0.5, 100), point(1.0, 108)]

    assert not calibration_settled(windows[:1])
    assert calibration_settled(windows)
    assert calibration_status(windows) == ('stable', 1.0)


def test_calibration_extends_after_disagreement_and_records_exhaustion():
    windows = [point(0.5, 100), point(1.0, 130), point(2.0, 108)]

    assert not calibration_settled(windows[:2])
    assert not calibration_settled(windows)
    assert calibration_status([*windows, point(4.0, 140)]) == (
        'calibration-limited',
        4.0,
    )


def test_calibration_windows_respect_the_load_driver_minimum():
    assert calibration_windows(0.0) == (0.5, 1.0, 2.0, 4.0)
    assert calibration_windows(1.0) == (1.0, 2.0, 4.0)
    assert production_warmup_seconds(0.0) == 0.25
    assert production_warmup_seconds(1.0) == 1.0


def test_window_without_completions_extends_calibration():
    windows = [point(0.5, 0.0), point(1.0, 900)]

    assert not calibration_settled(windows)


def test_common_duration_gives_each_competitor_equal_exposure():
    duration, limited = common_duration({
        'h2corn': {'status': 'stable', 'selected_duration_seconds': 0.5},
        'hypercorn': {'status': 'stable', 'selected_duration_seconds': 2.0},
        'gunicorn': {
            'status': 'calibration-limited',
            'selected_duration_seconds': 4.0,
        },
    })

    assert duration == 4.0
    assert limited


def test_production_cost_replaces_calibration_windows_with_selected_duration():
    calibration = {
        'elapsed_seconds': 5.5,
        'windows': [
            {'duration_seconds': 0.5, 'elapsed_seconds': 0.6},
            {'duration_seconds': 1.0, 'elapsed_seconds': 1.1},
        ],
    }

    assert calibration_production_cost(
        calibration,
        duration_seconds=2.0,
        warmup_seconds=0.25,
    ) == pytest.approx(6.25)


def test_production_plan_assigns_six_blocks_to_every_scenario():
    plan = allocate_production_blocks(
        scenario_costs={'h1': 10.0, 'grpc': 20.0},
        elapsed_seconds=100.0,
        target_seconds=1_000.0,
        hard_seconds=2_700.0,
        report_reserve_seconds=30.0,
        cleanup_reserve_seconds=30.0,
    )

    assert plan['blocks'] == {'h1': 6, 'grpc': 6}
    assert plan['production_estimate_seconds'] == 180.0
    assert plan['target_met']


def test_six_paired_blocks_get_the_fixed_conservative_median_interval():
    six = median_order_interval([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])

    assert six['interval'] == [1.0, 6.0]
    assert six['achieved_coverage'] == 62 / 64
    assert (six['lower_order'], six['upper_order']) == (1, 6)
    assert 'independent paired blocks' in six['method']


def test_median_interval_rejects_non_six_or_nonfinite_pairs():
    with pytest.raises(ValueError, match='exactly 6'):
        median_order_interval([1.0, 2.0, 3.0, 4.0, 5.0])
    with pytest.raises(ValueError, match='exactly 6'):
        median_order_interval([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0])
    with pytest.raises(ValueError, match='finite'):
        median_order_interval([1.0, float('nan'), 3.0, 4.0, 5.0, 6.0])


def test_six_block_minimum_must_fit_the_hard_budget():
    with pytest.raises(ValueError, match='minimum six-block'):
        allocate_production_blocks(
            scenario_costs={'http': 100.0, 'grpc': 100.0},
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
