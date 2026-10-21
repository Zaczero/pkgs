"""Adaptive sampling policy shared by the HTTP and RPC benchmark families."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

CALIBRATION_WINDOWS_SECONDS = (0.5, 1.0, 2.0, 4.0)
CALIBRATION_RELATIVE_TOLERANCE = 0.10
PILOT_BLOCKS = 2
BASE_PRODUCTION_BLOCKS = 6
NOISY_PRODUCTION_BLOCKS = 9
NOISY_LOG_RATIO_SCATTER = math.log1p(CALIBRATION_RELATIVE_TOLERANCE)
TARGET_TOTAL_SECONDS = 30 * 60
HARD_TOTAL_SECONDS = 45 * 60
REPORT_RESERVE_SECONDS = 90.0
CLEANUP_RESERVE_SECONDS = 60.0
MEDIAN_INTERVAL_COVERAGE = 0.95


class BudgetError(ValueError):
    """The minimum complete production plan cannot fit its hard budget."""


@dataclass(frozen=True, slots=True)
class CalibrationPoint:
    duration_seconds: float | None
    rate: float
    elapsed_seconds: float
    completed: int


@dataclass(slots=True)
class ScenarioSamples:
    """Own per-cell block identity, successful samples, and terminal failures."""

    servers: tuple[str, ...]
    _samples: dict[str, dict[int, Mapping[str, Any]]] = field(init=False)
    failures: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.servers or len(set(self.servers)) != len(self.servers):
            raise ValueError('scenario sample servers must be nonempty and unique')
        self._samples = {server: {} for server in self.servers}

    def add(self, server: str, block: int, metrics: Mapping[str, Any]) -> None:
        samples = self._samples[server]
        if server in self.failures or block in samples:
            raise ValueError(f'{server} block {block} is already terminal')
        samples[block] = metrics

    def fail(self, server: str, failure: dict[str, Any]) -> None:
        if server in self.failures:
            raise ValueError(f'{server} already has a terminal failure')
        self.failures[server] = failure

    def remaining(self) -> list[str]:
        return [server for server in self.servers if server not in self.failures]

    def values(self) -> dict[str, list[Mapping[str, Any]]]:
        return {
            server: [samples[block] for block in sorted(samples)]
            for server, samples in self._samples.items()
        }

    def block_ids(self) -> dict[str, list[int]]:
        return {server: sorted(samples) for server, samples in self._samples.items()}

    def successful_values(self) -> dict[str, list[Mapping[str, Any]]]:
        return {
            server: values
            for server, values in self.values().items()
            if server not in self.failures
        }

    def successful_block_ids(self) -> dict[str, list[int]]:
        return {
            server: block_ids
            for server, block_ids in self.block_ids().items()
            if server not in self.failures
        }


def calibration_duration(
    points: Sequence[CalibrationPoint],
    *,
    fixed_count: bool,
) -> float | None:
    """Choose the next same-process calibration window, or finish sampling."""
    if fixed_count:
        return None if points else 0.0
    if not points:
        return CALIBRATION_WINDOWS_SECONDS[0]
    if len(points) == 1:
        return CALIBRATION_WINDOWS_SECONDS[1]
    if rates_are_stable(points[-2].rate, points[-1].rate):
        return None
    next_index = len(points)
    if next_index < len(CALIBRATION_WINDOWS_SECONDS):
        return CALIBRATION_WINDOWS_SECONDS[next_index]
    return None


def calibration_status(
    points: Sequence[CalibrationPoint],
    *,
    fixed_count: bool,
) -> tuple[str, float | None]:
    if not points:
        raise ValueError('calibration requires at least one completed window')
    if fixed_count:
        if len(points) != 1 or points[0].duration_seconds is not None:
            raise ValueError('fixed-count calibration must retain one full-work sample')
        return 'fixed-count-cost', None
    if any(point.duration_seconds is None for point in points):
        raise ValueError('time-window calibration needs a duration for every window')
    stable = len(points) >= 2 and rates_are_stable(points[-2].rate, points[-1].rate)
    selected = points[-1].duration_seconds
    assert selected is not None
    return ('stable', selected) if stable else ('calibration-limited', selected)


def rates_are_stable(
    previous: float,
    current: float,
    tolerance: float = CALIBRATION_RELATIVE_TOLERANCE,
) -> bool:
    if (
        not math.isfinite(previous)
        or not math.isfinite(current)
        or previous <= 0
        or current <= 0
    ):
        return False
    return max(previous, current) / min(previous, current) - 1 <= tolerance


def common_duration(
    calibrations: Mapping[str, Mapping[str, Any]],
    *,
    fixed_count: bool,
) -> tuple[float | None, bool]:
    """Use equal production exposure, bounded by the slowest-settling server."""
    usable = [item for item in calibrations.values() if item.get('status') != 'failed']
    if not usable:
        raise ValueError('scenario has no successfully calibrated server')
    if fixed_count:
        if any(item.get('status') != 'fixed-count-cost' for item in usable):
            raise ValueError('fixed-count scenario has a time-window calibration')
        return None, False
    if any(item.get('status') == 'fixed-count-cost' for item in usable):
        raise ValueError('time-window scenario has a fixed-count calibration')
    durations = []
    for item in usable:
        value = item.get('selected_duration_seconds')
        if not isinstance(value, int | float) or value <= 0:
            raise ValueError('calibration is missing a selected duration')
        durations.append(float(value))
    return max(durations), any(
        item.get('status') == 'calibration-limited' for item in usable
    )


def paired_log_ratio_scatter(
    samples: Mapping[str, Sequence[float]],
    reference: str = 'h2corn',
    sample_blocks: Mapping[str, Sequence[int]] | None = None,
) -> float | None:
    """Return the largest sample SD of paired log-rate ratios across competitors."""
    if reference not in samples:
        return None
    blocks = sample_blocks or {
        name: list(range(len(values))) for name, values in samples.items()
    }
    baseline = dict(zip(blocks[reference], samples[reference], strict=True))
    scatters = []
    for name, rates in samples.items():
        if name == reference:
            continue
        competitor = dict(zip(blocks[name], rates, strict=True))
        common = sorted(baseline.keys() & competitor.keys())
        if len(common) < 2:
            continue
        ratios = []
        for block in common:
            control = baseline[block]
            candidate = competitor[block]
            if (
                not math.isfinite(control)
                or not math.isfinite(candidate)
                or control <= 0
                or candidate <= 0
            ):
                continue
            ratios.append(math.log(control / candidate))
        if len(ratios) >= 2:
            scatters.append(statistics.stdev(ratios))
    return max(scatters, default=None)


def is_noisy(scatter: float | None) -> bool:
    return scatter is not None and scatter > NOISY_LOG_RATIO_SCATTER


def allocate_production_blocks(
    *,
    scenario_costs: Mapping[str, float],
    pilot_scatter: Mapping[str, float | None],
    elapsed_seconds: float,
    target_seconds: float = TARGET_TOTAL_SECONDS,
    hard_seconds: float = HARD_TOTAL_SECONDS,
    setup_reserve_seconds: float = 0.0,
    report_reserve_seconds: float = REPORT_RESERVE_SECONDS,
    cleanup_reserve_seconds: float = CLEANUP_RESERVE_SECONDS,
) -> dict[str, Any]:
    """Freeze a full six-block plan, spending target headroom on noisy cases."""
    if not scenario_costs:
        raise ValueError('production planning requires at least one scenario')
    if elapsed_seconds < 0 or any(
        not math.isfinite(cost) or cost <= 0 for cost in scenario_costs.values()
    ):
        raise ValueError('production planning needs finite positive observed costs')
    blocks = dict.fromkeys(scenario_costs, BASE_PRODUCTION_BLOCKS)
    base_cost = sum(scenario_costs[name] * count for name, count in blocks.items())
    fixed_reserve = (
        setup_reserve_seconds + report_reserve_seconds + cleanup_reserve_seconds
    )
    base_forecast = elapsed_seconds + base_cost + fixed_reserve
    if base_forecast > hard_seconds:
        raise BudgetError(
            'minimum six-block matrix forecast exceeds the hard total budget '
            f'({base_forecast:.1f}s > {hard_seconds:.1f}s)'
        )

    extras = [
        (
            (pilot_scatter.get(name) or 0.0) / (scenario_costs[name] * 3),
            name,
        )
        for name in scenario_costs
        if is_noisy(pilot_scatter.get(name))
    ]
    extra_cost = 0.0
    for _, scenario in sorted(extras, key=lambda item: (-item[0], item[1])):
        added = scenario_costs[scenario] * (
            NOISY_PRODUCTION_BLOCKS - BASE_PRODUCTION_BLOCKS
        )
        if (
            elapsed_seconds + base_cost + extra_cost + added + fixed_reserve
            > target_seconds
        ):
            continue
        blocks[scenario] = NOISY_PRODUCTION_BLOCKS
        extra_cost += added

    forecast = elapsed_seconds + base_cost + extra_cost + fixed_reserve
    return {
        'blocks': blocks,
        'elapsed_before_production_seconds': elapsed_seconds,
        'production_estimate_seconds': base_cost + extra_cost,
        'setup_reserve_seconds': setup_reserve_seconds,
        'report_reserve_seconds': report_reserve_seconds,
        'cleanup_reserve_seconds': cleanup_reserve_seconds,
        'forecast_total_seconds': forecast,
        'target_seconds': target_seconds,
        'hard_seconds': hard_seconds,
        'target_met': forecast <= target_seconds,
        'minimum_six_block_forecast_seconds': base_forecast,
        'noisy_scenarios': [
            name for _, name in sorted(extras, key=lambda item: (-item[0], item[1]))
        ],
    }


def median_order_interval(values: Sequence[float]) -> dict[str, Any]:
    """Distribution-free common-median interval with at least 95% coverage."""
    if any(not math.isfinite(value) for value in values):
        raise ValueError('median interval values must be finite')
    ordered = sorted(values)
    count = len(ordered)
    selected = None
    for rank in range(1, (count + 1) // 2 + 1):
        coverage = (
            sum(
                math.comb(count, successes)
                for successes in range(rank, count - rank + 1)
            )
            / 2**count
        )
        if coverage >= MEDIAN_INTERVAL_COVERAGE:
            selected = rank, coverage
        else:
            break
    if selected is None:
        raise ValueError(
            'at least six independent paired blocks are required for a '
            'distribution-free 95% median interval'
        )
    rank, coverage = selected
    return {
        'interval': [ordered[rank - 1], ordered[count - rank]],
        'achieved_coverage': coverage,
        'method': (
            'distribution-free order-statistic interval for the common median; '
            'independent paired blocks'
        ),
        'sample_size': count,
        'lower_order': rank,
        'upper_order': count - rank + 1,
    }


def calibration_record(points: Sequence[CalibrationPoint]) -> list[dict[str, Any]]:
    return [
        {
            'duration_seconds': point.duration_seconds,
            'rate': point.rate,
            'elapsed_seconds': point.elapsed_seconds,
            'completed': point.completed,
        }
        for point in points
    ]
