"""Typed native A/B protocol and blocked-sampling contracts."""

from __future__ import annotations

import importlib.util
import json
import math
import sys
from copy import deepcopy
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

GOMETRY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(GOMETRY_ROOT / 'benches' / 'support'))
from benchmark_observation import make_observation, validate_observation  # noqa: E402


def _module():
    path = GOMETRY_ROOT / 'benches' / 'drivers' / 'bench_ab.py'
    name = 'gometry_bench_ab_typed_test'
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path.pop(0)


def _observation(bench_ab, metric: str, value: float) -> dict[str, Any]:
    return make_observation(
        metric=metric,
        raw_values=[value],
        useful_work=(
            {
                'numerator': {'count': 10, 'unit': 'large'},
                'denominator': {'count': 1, 'unit': 'small'},
            }
            if metric == 'scaling_ratio'
            else {'count': 10, 'unit': 'calls'}
        ),
        oracle_identity='test:native-observation-v1',
        warmup_iterations=1,
        calibration_iterations=1,
    )


def _arm(
    bench_ab,
    value: float,
    *,
    metric: str = 'duration',
    identity: str = 'A',
    started: int = 10,
    ended: int = 11,
) -> dict[str, Any]:
    return {
        'observation': _observation(bench_ab, metric, value),
        'started_monotonic_ns': started,
        'ended_monotonic_ns': ended,
        'provenance_before': {'extension': identity},
        'provenance_after': {'extension': identity},
    }


def test_blocked_orders_are_seeded_balanced_and_alternate_leaders() -> None:
    bench_ab = _module()
    orders = bench_ab.blocked_orders(11, 20_260_709)
    assert orders == bench_ab.blocked_orders(11, 20_260_709)
    assert set(orders) == {('A', 'B'), ('B', 'A')}
    assert all(left[0] != right[0] for left, right in pairwise(orders))
    assert (
        abs(
            sum(order[0] == 'A' for order in orders)
            - sum(order[0] == 'B' for order in orders)
        )
        == 1
    )


def test_run_blocks_records_real_typed_pairs_and_excludes_warmups() -> None:
    bench_ab = _module()
    calls: list[str] = []
    tick = 0

    def clock() -> int:
        nonlocal tick
        tick += 1
        return tick

    def runner(python: str, _case: Path, _extra: list[str]) -> dict[str, Any]:
        calls.append(python)
        value = 1.1 if python == 'python-a' else 1.0
        started = clock()
        ended = clock()
        return _arm(bench_ab, value, identity=python, started=started, ended=ended)

    result = bench_ab.run_blocks(
        {'A': 'python-a', 'B': 'python-b'},
        GOMETRY_ROOT / 'benches' / 'cases' / 'case_import_wkb.py',
        [],
        warmup=2,
        rounds=9,
        seed=20_260_709,
        runner=runner,
        clock_ns=clock,
    )
    assert len(calls) == 22
    assert len(result['blocks']) == 11
    assert sum(block['measured'] for block in result['blocks']) == 9
    assert (
        result['blocks'][0]['arms']['A']['observation']['measurement']['unit']
        == 'seconds'
    )
    assert len(result['samples']['A']) == len(result['samples']['B']) == 9


@pytest.mark.parametrize('metric', ['duration', 'scaling_ratio'])
def test_stdout_parser_accepts_only_typed_metric_payload(metric: str) -> None:
    bench_ab = _module()
    observation = _observation(bench_ab, metric, 1.5)
    parsed = bench_ab._parse_observation_stdout(
        Path('case.py'), json.dumps({'observation': observation}) + '\n', ''
    )
    assert parsed['measurement']['metric'] == metric


def test_stdout_parser_rejects_float_and_ratio_as_seconds() -> None:
    bench_ab = _module()
    with pytest.raises(SystemExit, match='typed observation JSON'):
        bench_ab._parse_observation_stdout(Path('case.py'), '1.5\n', '')
    ratio = _observation(bench_ab, 'scaling_ratio', 1.5)
    ratio['measurement']['unit'] = 'seconds'
    with pytest.raises(ValueError, match='unit'):
        validate_observation(ratio)


def test_smooth_common_drift_is_inconclusive_for_a_a_blocks() -> None:
    bench_ab = _module()
    tick = 0
    call_index = 0

    def clock() -> int:
        nonlocal tick
        tick += 1
        return tick

    def runner(_side: str, _case: Path, _extra: list[str]) -> dict[str, Any]:
        nonlocal call_index
        started = clock()
        value = math.exp(call_index * 0.5)
        call_index += 1
        ended = clock()
        return _arm(bench_ab, value, started=started, ended=ended)

    run = bench_ab.run_blocks(
        {'A': 'same-a', 'B': 'same-b'},
        Path('synthetic-a-a.py'),
        [],
        warmup=1,
        rounds=6,
        seed=0,
        runner=runner,
        clock_ns=clock,
    )
    result = bench_ab.compare_blocks(
        run['blocks'], 123, metric='duration', unit='seconds'
    )
    measured = [block for block in run['blocks'] if block['measured']]
    unpaired_a = [
        bench_ab._observation_estimate(block['arms']['A']['observation'])
        for block in measured
    ]
    unpaired_b = [
        bench_ab._observation_estimate(block['arms']['B']['observation'])
        for block in measured
    ]
    assert bench_ab.statistics.median(unpaired_a) / bench_ab.statistics.median(
        unpaired_b
    ) != pytest.approx(1.0)
    assert result['relative_ratio_a_over_b'] == pytest.approx(1.0)
    assert result['significant'] is False
    broken_schedule = deepcopy(run['blocks'])
    for block in broken_schedule:
        if block['measured']:
            block['lead_order'] = ['A', 'B']
    with pytest.raises(SystemExit, match='both lead orders'):
        bench_ab.compare_blocks(broken_schedule, 123, metric='duration', unit='seconds')
    swapped_order = deepcopy(run['blocks'])
    for block in swapped_order:
        if block['measured']:
            block['lead_order'].reverse()
    with pytest.raises(SystemExit, match='timestamps'):
        bench_ab.compare_blocks(swapped_order, 123, metric='duration', unit='seconds')


@pytest.mark.parametrize(('seed', 'first_lead'), [(0, 'A'), (1, 'B')])
def test_constant_ten_percent_cost_is_recovered_for_both_leads(
    seed: int, first_lead: str
) -> None:
    bench_ab = _module()
    tick = 0

    def clock() -> int:
        nonlocal tick
        tick += 1
        return tick

    def runner(side: str, _case: Path, _extra: list[str]) -> dict[str, Any]:
        started = clock()
        ended = clock()
        value = 1.1 if side == 'python-a' else 1.0
        return _arm(bench_ab, value, identity=side, started=started, ended=ended)

    run = bench_ab.run_blocks(
        {'A': 'python-a', 'B': 'python-b'},
        Path('synthetic-cost.py'),
        [],
        warmup=1,
        rounds=4,
        seed=seed,
        runner=runner,
        clock_ns=clock,
    )
    measured = [block for block in run['blocks'] if block['measured']]
    assert run['blocks'][0]['lead_order'][0] == first_lead
    assert len(measured) >= 3
    result = bench_ab.compare_blocks(
        run['blocks'], 123, metric='duration', unit='seconds'
    )
    assert result['relative_ratio_a_over_b'] == pytest.approx(1.1)
    assert result['relative_ratio_ci95'] == pytest.approx((1.1, 1.1))


@pytest.mark.parametrize(
    ('ratio', 'expected'),
    [
        (1.25, 'Candidate B is 1.250x faster than A'),
        (0.8, 'Candidate B is 1.250x slower than A'),
        (1.0, 'Candidate B matches A'),
    ],
)
def test_directional_verdict_names_candidate_direction(
    ratio: float, expected: str
) -> None:
    bench_ab = _module()
    assert expected in bench_ab.format_directional_verdict(
        ratio, metric='duration', unit='seconds'
    )


def test_observation_requires_oracle_identity() -> None:
    bench_ab = _module()
    observation = _observation(bench_ab, 'duration', 1.0)
    del observation['oracle']['identity']
    with pytest.raises(ValueError, match='oracle'):
        validate_observation(observation)


def test_pair_semantics_reject_mismatched_metric_before_summary() -> None:
    bench_ab = _module()
    arms = {
        'A': _arm(bench_ab, 1.0, metric='duration'),
        'B': _arm(bench_ab, 1.0, metric='scaling_ratio'),
    }
    with pytest.raises(SystemExit, match='mismatched typed metrics'):
        bench_ab._validate_pair_arms(arms, Path('case.py'))


def test_perf_event_expansion_and_parser_retain_counter_identity() -> None:
    bench_ab = _module()
    assert bench_ab.expand_perf_events(['instructions, cycles', 'branches']) == (
        'instructions',
        'cycles',
        'branches',
    )
    parsed = bench_ab.parse_perf_output(
        '1234,,instructions:u,1000,100.00,,\n2000,,cycles,1000,50.00,,\n',
        ['instructions', 'cycles'],
    )
    assert parsed['instructions']['raw_value'] == 1234
    assert parsed['instructions']['coverage'] == 1.0
    assert parsed['cycles']['status'] == 'multiplexed'
    with pytest.raises(SystemExit, match='not comparable'):
        bench_ab._validate_counters(parsed, ('instructions', 'cycles'))


@pytest.mark.parametrize(
    'entry',
    [
        {'status': 'ok', 'raw_value': 0, 'coverage': 1.0},
        {'status': 'unsupported', 'raw_value': None, 'coverage': None},
        {'status': 'not-counted', 'raw_value': None, 'coverage': 0.0},
        {'status': 'multiplexed', 'raw_value': 10, 'coverage': 0.5},
    ],
)
def test_perf_positive_control_rejects_nonmoving_or_unusable_event(
    entry: dict[str, object],
) -> None:
    bench_ab = _module()
    failures = bench_ab.validate_positive_control(
        {'instructions': entry}, ('instructions',)
    )
    assert failures['instructions']


def test_perf_positive_control_accepts_supported_moving_event() -> None:
    bench_ab = _module()
    assert (
        bench_ab.validate_positive_control(
            {'instructions': {'status': 'ok', 'raw_value': 100, 'coverage': 1.0}},
            ('instructions',),
        )
        == {}
    )


def test_perf_capability_splits_at_failed_group_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bench_ab = _module()
    monkeypatch.setattr(
        bench_ab, 'perf_version', lambda _binary='perf': {'version': 'test'}
    )

    def control(events, **_kwargs):
        if len(events) > 2:
            return {'status': 'rejected', 'events': list(events)}
        return {
            'status': 'ok',
            'events': list(events),
            'counters': {
                event: {'status': 'ok', 'raw_value': 1, 'coverage': 1.0}
                for event in events
            },
        }

    monkeypatch.setattr(bench_ab, '_run_perf_control', control)
    result = bench_ab.check_perf_capability(('instructions', 'cycles', 'branches'))
    assert result['status'] == 'available'
    assert result['event_groups'] == [['instructions', 'cycles'], ['branches']]
    assert result['usable_group_capacity'] == 2


def test_split_perf_groups_execute_separate_case_invocations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bench_ab = _module()
    calls: list[tuple[str, ...]] = []
    observation = _observation(bench_ab, 'duration', 1.0)
    provenance = {'build': 'same'}

    def fake_group(_python, _case, _extra, events, **_kwargs):
        group = tuple(events)
        calls.append(group)
        return {
            'observation': observation,
            'provenance_before': provenance,
            'provenance_after': provenance,
            'counters': {
                event: {
                    'status': 'ok',
                    'raw_value': 10,
                    'coverage': 1.0,
                }
                for event in group
            },
            'command': ['perf', 'stat', '-e', ','.join(group)],
        }

    monkeypatch.setattr(bench_ab, '_run_case_with_perf_group', fake_group)
    result = bench_ab.run_case_with_perf(
        'python',
        Path('case.py'),
        [],
        ('instructions', 'cycles'),
        event_groups=(('instructions',), ('cycles',)),
    )
    assert calls == [('instructions',), ('cycles',)]
    assert result['commands'][0][-1] == 'instructions'
    assert result['commands'][1][-1] == 'cycles'
    assert set(result['counters']) == {'instructions', 'cycles'}
