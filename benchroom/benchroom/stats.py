"""Statistics over independent runs or paired blocks."""

from __future__ import annotations

import itertools
import math
import random
import statistics
from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


def percentile(values: Sequence[float], fraction: float) -> float:
    """Return a linearly interpolated percentile; fraction lies in [0, 1]."""
    if not values:
        raise ValueError('percentile requires at least one value')
    if not 0 <= fraction <= 1:
        raise ValueError('fraction must lie between zero and one')
    ordered = sorted(values)
    position = fraction * (len(ordered) - 1)
    lower = math.floor(position)
    remainder = position - lower
    low = ordered[lower]
    return low if remainder == 0 else low + (ordered[lower + 1] - low) * remainder


def bootstrap_interval(
    size: int,
    statistic: Callable[[Sequence[int]], float],
    *,
    seed: int = 0,
    samples: int = 10_000,
) -> tuple[float, float]:
    """Return a 95% percentile bootstrap interval over independent units.

    ``statistic`` receives resampled indexes. For paired measurements, use each
    index for both arms of its pair. Repeated readings within one run must stay
    together. A single unit produces a degenerate interval, not evidence of
    precision; callers presenting uncertainty should require repeated runs.

    Small distributions are enumerated with multinomial weights; larger ones
    use ``samples`` seeded draws. Empty input and nonpositive samples raise
    ValueError.
    """
    if size < 1 or samples < 1:
        raise ValueError('bootstrap requires positive size and samples')
    if math.comb(2 * size - 1, size) <= samples:
        orderings = math.factorial(size)
        weighted = [
            (
                statistic(indexes),
                orderings
                // math.prod(
                    math.factorial(count) for count in Counter(indexes).values()
                ),
            )
            for indexes in itertools.combinations_with_replacement(range(size), size)
        ]
    else:
        rng = random.Random(seed)
        weighted = [
            (statistic([rng.randrange(size) for _ in range(size)]), 1)
            for _ in range(samples)
        ]
    weighted.sort()
    total = sum(weight for _, weight in weighted)
    return (
        _weighted_percentile(weighted, total, 0.025),
        _weighted_percentile(weighted, total, 0.975),
    )


def median_interval(values: Sequence[float], *, seed: int = 0) -> tuple[float, float]:
    """Return a 95% percentile bootstrap interval for independent-run medians."""
    return bootstrap_interval(
        len(values),
        lambda indexes: statistics.median(values[index] for index in indexes),
        seed=seed,
    )


def _weighted_percentile(
    ordered: list[tuple[float, int]], total: int, fraction: float
) -> float:
    position = fraction * (total - 1)
    lower = math.floor(position)
    remainder = position - lower

    def at(index: int) -> float:
        seen = 0
        for value, weight in ordered:
            seen += weight
            if index < seen:
                return value
        raise AssertionError('percentile index is within the total weight')

    low = at(lower)
    return low if remainder == 0 else low + (at(lower + 1) - low) * remainder
