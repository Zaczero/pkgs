"""Bounded calibration and prespecified inference over independent paired blocks."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from benchroom.stats import OrderInterval, median_order_interval

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True, slots=True)
class SamplingPolicy:
    """Allowed looks, their two-sided error budgets, and reporting precision.

    Stop only at a listed look. Summing the error allocations bounds the chance
    that any reported interval misses the common median. Independent blocks
    are required; repeated readings inside one process remain one block.
    ``precision_ratio`` bounds interval width, not practical equivalence.
    """

    looks: tuple[int, ...] = (6, 12)
    error_probabilities: tuple[float, ...] = (1 / 32, 3 / 160)
    precision_ratio: float = 1.10

    def __post_init__(self) -> None:
        if (
            not self.looks
            or any(type(look) is not int or look < 1 for look in self.looks)
            or tuple(sorted(set(self.looks))) != self.looks
            or len(self.looks) != len(self.error_probabilities)
        ):
            raise ValueError('looks must be strictly increasing positive block counts')
        if any(not 0 < error < 1 for error in self.error_probabilities) or (
            sum(self.error_probabilities) >= 1
        ):
            raise ValueError('look error probabilities must sum to less than one')
        if not math.isfinite(self.precision_ratio) or self.precision_ratio < 1:
            raise ValueError('precision ratio must be finite and at least one')

    @property
    def confidence(self) -> float:
        """Across-look coverage lower bound, per comparison."""
        return 1 - sum(self.error_probabilities)


@dataclass(frozen=True, slots=True)
class SamplingDecision:
    """One allowed-look result; ratio direction is numerator over denominator."""

    estimate: float
    uncertainty: OrderInterval
    confidence: float
    error_probability: float
    direction: Literal['above', 'below', 'unresolved']
    reason: Literal['direction-resolved', 'precision-met', 'continue', 'maximum-look']
    next_look: int | None

    @property
    def complete(self) -> bool:
        """Whether the policy requests no further independent blocks."""
        return self.next_look is None


def assess_ratios(
    ratios: Sequence[float],
    *,
    block_ids: Sequence[str | int],
    policy: SamplingPolicy = SamplingPolicy(),
) -> SamplingDecision:
    """Assess positive ratios once at a permitted independent-block count.

    IDs must identify distinct process blocks and match the ratio count.
    Calibration observations must be excluded by the caller. The point estimate
    is the ordinary median of ratios, not an exponentiated median of logs.
    """
    if len(block_ids) != len(ratios) or len(set(block_ids)) != len(block_ids):
        raise ValueError('ratios require one distinct block identity per observation')
    if any(not math.isfinite(value) or value <= 0 for value in ratios):
        raise ValueError('paired ratios must be finite and positive')
    try:
        look = policy.looks.index(len(ratios))
    except ValueError as error:
        raise ValueError(
            'inference is only permitted at a prespecified look'
        ) from error
    error_probability = policy.error_probabilities[look]
    uncertainty = median_order_interval(ratios, error_probability=error_probability)
    direction: Literal['above', 'below', 'unresolved'] = 'unresolved'
    precise = False
    if uncertainty.interval is not None:
        lower, upper = uncertainty.interval
        direction = 'above' if lower > 1 else 'below' if upper < 1 else 'unresolved'
        precise = upper / lower <= policy.precision_ratio
    reason: Literal['direction-resolved', 'precision-met', 'continue', 'maximum-look']
    next_look = None
    if direction != 'unresolved':
        reason = 'direction-resolved'
    elif precise:
        reason = 'precision-met'
    elif look == len(policy.looks) - 1:
        reason = 'maximum-look'
    else:
        reason = 'continue'
        next_look = policy.looks[look + 1]
    return SamplingDecision(
        statistics.median(ratios),
        uncertainty,
        policy.confidence,
        error_probability,
        direction,
        reason,
        next_look,
    )


def rates_are_stable(
    previous: float, current: float, *, tolerance: float = 0.10
) -> bool:
    """Compare positive adjacent calibration rates; this is not inference."""
    if not math.isfinite(tolerance) or tolerance < 0:
        raise ValueError('relative tolerance must be finite and nonnegative')
    return (
        math.isfinite(previous)
        and math.isfinite(current)
        and previous > 0
        and current > 0
        and max(previous, current) / min(previous, current) - 1 <= tolerance
    )


def next_batch_size(
    iterations: int, elapsed_seconds: float, *, minimum_seconds: float
) -> int:
    """Keep a sufficiently long timing batch, otherwise double its iterations.

    The caller owns the maximum number of calibration attempts and the deadline.
    Calibration calls are excluded from the measured independent-block summary.
    """
    if type(iterations) is not int or iterations < 1:
        raise ValueError('batch iterations must be a positive integer')
    if not math.isfinite(elapsed_seconds) or elapsed_seconds < 0:
        raise ValueError('batch elapsed time must be finite and nonnegative')
    if not math.isfinite(minimum_seconds) or minimum_seconds <= 0:
        raise ValueError('minimum batch time must be finite and positive')
    return iterations if elapsed_seconds >= minimum_seconds else 2 * iterations


def fits_budget(
    *,
    remaining_seconds: float,
    required_seconds: float,
    optional_seconds: float,
    reserve_seconds: float = 0,
) -> bool:
    """Whether optional work fits after protecting required work and finishing."""
    if any(
        not math.isfinite(value) or value < 0
        for value in (required_seconds, optional_seconds, reserve_seconds)
    ) or not math.isfinite(remaining_seconds):
        raise ValueError('budget costs must be finite and nonnegative')
    return remaining_seconds >= required_seconds + optional_seconds + reserve_seconds
