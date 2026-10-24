"""Adaptive benchmark calibration, planning, and median uncertainty."""

import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bench import bench
from bench._core import BenchmarkError, ResponseError, Scenario, write_json
from bench.adaptive import (
    ProfileRun,
    _calibrate_rpc_qualification,
    _freeze_suite_plan,
    _production_profile,
)
from bench.sampling import (
    CalibrationPoint,
    ScenarioSamples,
    allocate_production_blocks,
    calibration_production_cost,
    calibration_settled,
    calibration_status,
    calibration_windows,
    common_duration,
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


@pytest.mark.parametrize('block', [0, -1, True])
def test_production_block_identity_must_name_a_real_block(block):
    samples = ScenarioSamples(('h2corn',))

    with pytest.raises(ValueError, match='positive integer'):
        samples.add('h2corn', block, {'rps': 42.0})


@pytest.fixture
def collect_scenario(monkeypatch, tmp_path):
    """Supply the external cell-measurement boundary with chosen observations."""
    if sys.platform == 'win32':
        pytest.skip('benchmark collection deadlines require POSIX interval timers')

    def collect(ratios, *, required=0, limited=False, fail=None, later_cost=0):
        scenario = Scenario('Paired collection', 1, 'h1')
        scenarios = (
            (scenario, Scenario('Later required collection', 1, 'h1'))
            if later_cost
            else (scenario,)
        )
        root = tmp_path / 'loopback'
        calls = []

        def measured_cell(
            name, scenario, duration, warmup, *, evidence_directory, **kwargs
        ):
            block = int(evidence_directory.parent.name.removeprefix('block-'))
            calls.append((name, block))
            if (name, block) == fail:
                write_json(evidence_directory / 'trial.json', {'status': 'failed'})
                try:
                    raise ResponseError('invalid response body')
                except ResponseError as error:
                    raise bench.TrialError('correctness-after', str(error)) from error
            ratio = ratios[block - 1] if scenario.name == 'Paired collection' else 1.2
            metrics = {
                'rps': 100 * ratio if name == 'h2corn' else 100.0,
                'completed': 100,
                'peak_memory_bytes': 1234,
                'elapsed_seconds': 1.0,
                'latency_percentiles': {'p50': 0.001, 'p99': 0.002},
            }
            write_json(evidence_directory / 'trial.json', {'status': 'completed'})
            return metrics

        monkeypatch.setattr(bench, 'measure_cell', measured_cell)
        plan = {
            'family': 'http',
            'profile': 'loopback',
            'slug': scenario.slug(),
            'minimum_blocks': 6,
            'maximum_blocks': 12,
            'calibration_limited': limited,
            'duration_seconds': 1.0,
            'scenario_cost_seconds': 2.0,
        }
        profile = ProfileRun(
            family='http',
            name='loopback',
            scenarios=scenarios,
            servers=('h2corn', 'uvicorn'),
            directory=root,
            identity={},
            hypercorn_config=root / 'unused.toml',
            system_summary='test host',
            package_versions={},
            calibrations={
                item.slug(): {
                    'servers': {},
                    'supported_servers': ['h2corn', 'uvicorn'],
                    'calibration_limited': limited,
                }
                for item in scenarios
            },
            scenario_plans={
                item.slug(): {
                    **plan,
                    'slug': item.slug(),
                    'scenario_cost_seconds': 2.0 if item is scenario else later_cost,
                }
                for item in scenarios
            },
        )
        started = time.monotonic()
        _freeze_suite_plan(
            [profile],
            {},
            started=started,
            started_at=datetime.now(UTC),
            suite_directory=tmp_path,
        )
        _production_profile(
            bench,
            None,
            profile,
            deadline=started + 100,
            started=started,
            required_seconds_other_profiles=required,
        )
        record = json.loads((root / 'raw' / f'{scenario.slug()}.json').read_text())
        write_json(root / 'identity.json', {'build': {}})
        write_json(
            root / 'profile.json',
            {
                'profile': 'loopback',
                'benchmark_family': 'http',
                'status': 'completed',
            },
        )
        return record, root, calls

    return collect


@pytest.mark.parametrize(
    ('ratios', 'reason', 'verdict'),
    [
        ([1.2] * 6, 'direction-resolved', 'slower'),
        ([0.98, 1.01, 0.99, 1.02, 1.03, 1.0], 'precision-met', 'inconclusive'),
    ],
)
def test_scenario_stops_at_six_for_direction_or_reporting_precision(
    collect_scenario, ratios, reason, verdict
):
    record, root, calls = collect_scenario(ratios)

    assert record['stopped'] == reason
    assert record['planned_trials'] == 6
    assert len(calls) == 12
    assert record['paired_contrasts']['uvicorn']['decision'] == verdict
    bench.assert_publishable([root])


def test_scenario_extends_only_an_unresolved_comparison_to_twelve(collect_scenario):
    record, root, calls = collect_scenario([0.5, 2.0] * 6)

    assert record['stopped'] == 'maximum-look'
    assert record['planned_trials'] == 12
    assert len(calls) == 24
    assert [look['decision'] for look in record['look_history']] == [
        'continue',
        'maximum-look',
    ]
    assert record['paired_contrasts']['uvicorn']['decision'] == 'inconclusive'
    assert record['paired_contrasts']['uvicorn']['interval'] == [0.5, 2.0]
    bench.assert_publishable([root])


def test_optional_budget_exhaustion_is_complete_first_look_collection(collect_scenario):
    record, root, calls = collect_scenario([0.5, 2.0] * 3, required=99)

    assert record['stopped'] == 'budget-limited'
    assert record['status'] == 'completed'
    assert record['planned_trials'] == 6
    assert len(calls) == 12
    assert record['paired_contrasts']['uvicorn']['decision'] == 'inconclusive'
    bench.assert_publishable([root])


def test_optional_extension_protects_later_scenarios_in_the_same_profile(
    collect_scenario,
):
    record, root, calls = collect_scenario([0.5, 2.0] * 6, later_cost=16.5)

    assert record['stopped'] == 'budget-limited'
    assert record['look_history'][0]['budget']['required_seconds'] == 99
    assert len(calls) == 24
    bench.assert_publishable([root])


def test_calibration_limited_exposure_remains_inconclusive(collect_scenario):
    record, root, _ = collect_scenario([2.0] * 6, limited=True)

    assert record['stopped'] == 'calibration-limited'
    assert (
        record['paired_contrasts']['uvicorn']['decision_reason']
        == 'calibration-limited'
    )
    bench.assert_publishable([root])


def test_response_failure_retains_evidence_without_numeric_ranking(collect_scenario):
    record, root, calls = collect_scenario([2.0] * 6, fail=('uvicorn', 2))

    assert record['status'] == 'attempted_with_failures'
    assert record['failures']['uvicorn']['block'] == 2
    assert 'uvicorn' not in record['aggregate']
    assert ('uvicorn', 3) not in calls
    bench.assert_publishable([root])


def test_optional_look_response_failure_preserves_the_original_look(collect_scenario):
    record, root, _ = collect_scenario([0.5, 2.0] * 6, fail=('uvicorn', 8))

    assert record['status'] == 'attempted_with_failures'
    assert record['failures']['uvicorn']['block'] == 8
    assert record['look_history'][0]['contrasts']['uvicorn']['next_look'] == 12
    assert 'uvicorn' not in record['aggregate']
    bench.assert_publishable([root])


def test_publication_rejects_a_missing_required_process_block(collect_scenario):
    _, root, _ = collect_scenario([2.0] * 6)
    slug = next((root / 'raw').glob('*.json')).stem
    (root / 'evidence' / slug / 'block-06' / 'h2corn' / 'trial.json').unlink()

    with pytest.raises(BenchmarkError, match='incomplete h2corn block 6'):
        bench.assert_publishable([root])


def test_publication_rejects_a_fabricated_precision_stop(collect_scenario):
    record, root, _ = collect_scenario([0.5, 2.0] * 3, required=99)
    record['stopped'] = 'precision-met'
    path = next((root / 'raw').glob('*.json'))
    path.write_text(json.dumps(record))

    with pytest.raises(BenchmarkError, match='stop reason differs'):
        bench.assert_publishable([root])


def test_failed_framework_oracle_is_retained_for_reporting(tmp_path):
    class FailedFramework:
        @staticmethod
        async def qualify_framework():
            return [{'case': 'reserved-flag', 'status': 'failed'}]

    profile = ProfileRun(
        family='grpc',
        name='grpc',
        scenarios=(),
        servers=(),
        directory=tmp_path,
        identity={
            'rpc_artifacts': {'connectrpc': {'kind': 'release', 'sha256': 'app'}}
        },
        hypercorn_config=tmp_path / 'unused.toml',
        system_summary='test host',
        package_versions={},
    )

    _calibrate_rpc_qualification(
        profile, FailedFramework, deadline=time.monotonic() + 100
    )

    retained = json.loads((tmp_path / 'qualification.json').read_text())
    assert retained['framework'][0]['status'] == 'failed'
    assert profile.qualification == retained
