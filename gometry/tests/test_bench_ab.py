"""Statistical and interleaving contracts for the native A/B harness."""

from __future__ import annotations

from itertools import pairwise
from typing import TYPE_CHECKING

import pytest
from conftest import GOMETRY_ROOT, load_tool

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
