"""Adaptive sampling policy shared by the HTTP and RPC benchmark families."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from benchroom.sampling import SamplingPolicy, rates_are_stable

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

CALIBRATION_WINDOWS_SECONDS = (0.5, 1.0, 2.0, 4.0)
SAMPLING_POLICY = SamplingPolicy()
PRODUCTION_WARMUP_SECONDS = 0.25
TARGET_TOTAL_SECONDS = 30 * 60
HARD_TOTAL_SECONDS = 45 * 60
REPORT_RESERVE_SECONDS = 90.0
CLEANUP_RESERVE_SECONDS = 60.0


class BudgetError(ValueError):
    """The minimum complete production plan cannot fit its hard budget."""


def calibration_windows(minimum_seconds: float) -> tuple[float, ...]:
    """Calibration windows the scenario's load driver can run."""
    return tuple(
        window for window in CALIBRATION_WINDOWS_SECONDS if window >= minimum_seconds
    )


def production_warmup_seconds(minimum_seconds: float) -> float:
    """Production warmup the scenario's load driver can run."""
    return max(PRODUCTION_WARMUP_SECONDS, minimum_seconds)


@dataclass(frozen=True, slots=True)
class CalibrationPoint:
    duration_seconds: float
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
        if type(block) is not int or block < 1:
            raise ValueError('production block identity must be a positive integer')
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


def calibration_settled(points: Sequence[CalibrationPoint]) -> bool:
    """Whether the two latest same-process windows agree within tolerance."""
    return len(points) >= 2 and rates_are_stable(points[-2].rate, points[-1].rate)


def calibration_status(points: Sequence[CalibrationPoint]) -> tuple[str, float]:
    if not points:
        raise ValueError('calibration requires at least one completed window')
    selected = points[-1].duration_seconds
    if calibration_settled(points):
        return 'stable', selected
    return 'calibration-limited', selected


def common_duration(
    calibrations: Mapping[str, Mapping[str, Any]],
) -> tuple[float, bool]:
    """Use equal production exposure, bounded by the slowest-settling server."""
    usable = [item for item in calibrations.values() if item.get('status') != 'failed']
    if not usable:
        raise ValueError('scenario has no successfully calibrated server')
    durations = []
    for item in usable:
        value = item.get('selected_duration_seconds')
        if not isinstance(value, int | float) or value <= 0:
            raise ValueError('calibration is missing a selected duration')
        durations.append(float(value))
    return max(durations), any(
        item.get('status') == 'calibration-limited' for item in usable
    )


def calibration_production_cost(
    calibration: Mapping[str, Any],
    *,
    duration_seconds: float,
    warmup_seconds: float,
) -> float:
    """Estimate one production cell by replacing its calibration load windows."""
    calibration_seconds = float(calibration['elapsed_seconds'])
    if duration_seconds <= 0 or not math.isfinite(duration_seconds):
        raise ValueError('production duration must be finite and positive')
    # Subtract requested load time, retaining per-window launch and teardown cost.
    window_seconds = sum(
        float(window['duration_seconds']) for window in calibration['windows']
    )
    if not math.isfinite(calibration_seconds) or calibration_seconds <= 0:
        raise ValueError('calibration elapsed time must be finite and positive')
    if not math.isfinite(window_seconds) or window_seconds <= 0:
        raise ValueError('calibration windows must have positive elapsed time')
    if calibration_seconds < window_seconds:
        raise ValueError('calibration elapsed time is shorter than its load windows')
    if warmup_seconds < 0 or not math.isfinite(warmup_seconds):
        raise ValueError('production warmup must be finite and nonnegative')
    return calibration_seconds - window_seconds + duration_seconds + warmup_seconds


def allocate_production_blocks(
    *,
    scenario_costs: Mapping[str, float],
    elapsed_seconds: float,
    target_seconds: float = TARGET_TOTAL_SECONDS,
    hard_seconds: float = HARD_TOTAL_SECONDS,
    report_reserve_seconds: float = REPORT_RESERVE_SECONDS,
    cleanup_reserve_seconds: float = CLEANUP_RESERVE_SECONDS,
) -> dict[str, Any]:
    """Protect the complete first-look matrix before optional second looks."""
    if not scenario_costs:
        raise ValueError('production planning requires at least one scenario')
    if elapsed_seconds < 0 or any(
        not math.isfinite(cost) or cost <= 0 for cost in scenario_costs.values()
    ):
        raise ValueError('production planning needs finite positive observed costs')
    blocks = dict.fromkeys(scenario_costs, SAMPLING_POLICY.looks[0])
    production_cost = sum(
        scenario_costs[name] * count for name, count in blocks.items()
    )
    fixed_reserve = report_reserve_seconds + cleanup_reserve_seconds
    forecast = elapsed_seconds + production_cost + fixed_reserve
    if forecast > hard_seconds:
        raise BudgetError(
            'minimum six-block matrix forecast exceeds the hard total budget '
            f'({forecast:.1f}s > {hard_seconds:.1f}s)'
        )

    return {
        'blocks': blocks,
        'elapsed_before_production_seconds': elapsed_seconds,
        'production_estimate_seconds': production_cost,
        'report_reserve_seconds': report_reserve_seconds,
        'cleanup_reserve_seconds': cleanup_reserve_seconds,
        'forecast_total_seconds': forecast,
        'target_seconds': target_seconds,
        'hard_seconds': hard_seconds,
        'target_met': forecast <= target_seconds,
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
