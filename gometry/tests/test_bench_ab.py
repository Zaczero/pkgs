"""Statistical and interleaving contracts for the native A/B harness."""

from __future__ import annotations

import json
from itertools import pairwise
from typing import TYPE_CHECKING, Any

import pytest

from tests._support import GOMETRY_ROOT, load_tool

if TYPE_CHECKING:
    from types import ModuleType


def _module() -> ModuleType:
    path = GOMETRY_ROOT / 'benches' / 'drivers' / 'bench_ab.py'
    return load_tool(
        'gometry_bench_ab_test',
        path,
        path.parent,
    )


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


def test_run_blocks_obeys_balanced_order_and_keeps_only_measured_values() -> None:
    bench_ab = _module()
    calls: list[str] = []

    def runner(python: str, _case, _extra) -> float:
        calls.append(python)
        return float(len(calls))

    samples = bench_ab.run_blocks(
        {'A': 'python-a', 'B': 'python-b'},
        GOMETRY_ROOT / 'benches' / 'cases' / 'case_import_wkb.py',
        [],
        warmup=2,
        rounds=9,
        seed=20_260_709,
        runner=runner,
    )
    assert len(calls) == 22
    assert len(samples['A']) == len(samples['B']) == 9


def test_summary_reports_block_distribution_and_deterministic_confidence_interval() -> (
    None
):
    bench_ab = _module()
    values = [1.0, 1.1, 0.9, 1.05, 0.95, 1.02, 0.98, 1.01, 0.99]
    summary = bench_ab.summarize(values, 123)
    assert summary == bench_ab.summarize(values, 123)
    assert summary['median_seconds'] == 1.0
    assert summary['max_block_seconds'] >= summary['p50_seconds']
    assert 'p99_seconds' not in summary
    low, high = summary['median_ci95_seconds']
    assert low <= summary['median_seconds'] <= high


def test_bootstrap_ratio_ci_separates_large_explained_effect() -> None:
    bench_ab = _module()
    # Absolute runtime drifts across blocks, but each paired observation has
    # exactly the same 2x effect. A paired bootstrap must retain that signal.
    low, high = bench_ab.bootstrap_ratio_ci(
        [2.0 * value for value in range(1, 10)],
        [float(value) for value in range(1, 10)],
        123,
        samples=200,
    )
    assert (low, high) == (2.0, 2.0)


def test_bootstrap_ratio_ci_requires_block_pairs() -> None:
    bench_ab = _module()
    with pytest.raises(ValueError, match='equal sample counts'):
        bench_ab.bootstrap_ratio_ci([1.0], [1.0, 2.0], 123)


def test_perf_event_expansion_uses_a_stable_default_and_accepts_csv_groups() -> None:
    bench_ab = _module()
    assert bench_ab.expand_perf_events() == (
        'instructions',
        'cycles',
        'cache-references',
        'cache-misses',
        'branches',
        'branch-misses',
    )
    assert bench_ab.expand_perf_events(['instructions, cycles', 'branches']) == (
        'instructions',
        'cycles',
        'branches',
    )
    with pytest.raises(ValueError, match='duplicate'):
        bench_ab.expand_perf_events(['cycles', 'cycles'])


def test_perf_csv_parser_retains_value_identity_runtime_and_coverage() -> None:
    bench_ab = _module()
    parsed = bench_ab.parse_perf_output(
        '1234,,instructions:u,1000,100.00,,\n'
        '2000,,cycles,1000,50.00,,\n',
        ['instructions', 'cycles'],
    )
    assert parsed['instructions']['event'] == 'instructions:u'
    assert parsed['instructions']['raw_value'] == 1234
    assert parsed['instructions']['raw_value_text'] == '1234'
    assert parsed['instructions']['runtime'] == 1000
    assert parsed['instructions']['coverage'] == 1.0
    assert parsed['instructions']['scaling'] == 1.0
    assert parsed['cycles']['status'] == 'multiplexed'
    assert parsed['cycles']['coverage'] == 0.5
    assert parsed['cycles']['raw_line'].startswith('2000')


def test_perf_parser_handles_human_text_and_explicit_failure_states() -> None:
    bench_ab = _module()
    parsed = bench_ab.parse_perf_output(
        '1,234 cycles:u # 1.2 GHz\n'
        '<not supported> cache-misses\n'
        '<not counted> branches\n',
        ['cycles', 'cache-misses', 'branches', 'instructions'],
    )
    assert parsed['cycles']['status'] == 'ok'
    assert parsed['cycles']['raw_value'] == 1234
    assert parsed['cycles']['coverage'] is None
    assert parsed['cache-misses']['status'] == 'unsupported'
    assert parsed['branches']['status'] == 'not-counted'
    assert parsed['instructions']['status'] == 'missing'
    with pytest.raises(SystemExit, match='not comparable'):
        bench_ab._validate_counters(parsed, tuple(parsed))


def test_perf_block_path_preserves_ab_ba_association_and_excludes_warmups() -> None:
    bench_ab = _module()
    calls: list[tuple[str, tuple[str, ...]]] = []
    events = ('instructions', 'cycles')

    def runner(python: str, _case, _extra, requested) -> dict[str, Any]:
        calls.append((python, tuple(requested)))
        value = 10.0 if python == 'python-a' else 5.0
        return {
            'elapsed_seconds': value,
            'counters': {
                event: {
                    'value': value * (2 if event == 'instructions' else 3),
                    'status': 'ok',
                    'coverage': 1.0,
                }
                for event in requested
            },
            'command': ['perf', 'stat', '--', python],
            'perf_output': 'raw perf output\n',
            'case_stdout': f'{value}\n',
            'perf_stderr': '',
        }

    result = bench_ab.run_blocks_with_perf(
        {'A': 'python-a', 'B': 'python-b'},
        GOMETRY_ROOT / 'benches' / 'cases' / 'case_import_wkb.py',
        [],
        events=events,
        warmup=2,
        rounds=9,
        seed=20_260_709,
        runner=runner,
    )
    assert [python for python, _ in calls] == [
        side
        for order in bench_ab.blocked_orders(11, 20_260_709)
        for side in (('python-a' if order_side == 'A' else 'python-b') for order_side in order)
    ]
    assert all(requested == events for _, requested in calls)
    assert len(result['blocks']) == 11
    assert [block['lead_order'] for block in result['blocks']] == [
        list(order) for order in bench_ab.blocked_orders(11, 20_260_709)
    ]
    assert sum(block['measured'] for block in result['blocks']) == 9
    assert len(result['samples']['A']) == len(result['samples']['B']) == 9
    assert all(block['counter_ratios']['instructions'] == 2.0 for block in result['blocks'])
    encoded = json.dumps(result)
    assert 'counter_ratios' in encoded


def test_counter_ratio_summary_is_paired_and_primary_event_is_instructions() -> None:
    bench_ab = _module()
    blocks = [
        {
            'measured': index != 0,
            'A': {'counters': {'instructions': {'value': 20.0 + index}}},
            'B': {'counters': {'instructions': {'value': 10.0 + index}}},
        }
        for index in range(3)
    ]
    summary = bench_ab.summarize_counter_ratios(blocks, ('instructions',), 123)
    assert summary['primary_event'] == 'instructions'
    assert summary['A']['instructions']['samples'] == [21.0, 22.0]
    assert summary['B']['instructions']['samples'] == [11.0, 12.0]
    assert summary['ratios']['instructions']['status'] == 'ok'
    assert summary['ratios']['instructions']['a_over_b'] == 21.5 / 11.5


def test_counter_ratio_summary_reports_zero_denominators_without_infinity() -> None:
    bench_ab = _module()
    blocks = [
        {
            'measured': True,
            'A': {'counters': {'instructions': {'value': 1.0}}},
            'B': {'counters': {'instructions': {'value': 0.0}}},
        }
    ]
    summary = bench_ab.summarize_counter_ratios(blocks, ('instructions',), 123)
    assert summary['ratios']['instructions'] == {
        'status': 'undefined-zero-denominator',
        'a_over_b': None,
        'ci95': None,
    }
