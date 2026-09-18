"""Percentile bootstrap intervals over independent benchmark blocks."""

from __future__ import annotations

import itertools
import math
import random
import statistics
from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

BOOTSTRAP_SAMPLES = 10_000


def percentile(values: Sequence[float], quantile: float) -> float:
    """Linearly interpolated percentile of unweighted values."""
    return _weighted_percentile([(value, 1) for value in values], quantile)


def bootstrap_interval(
    size: int,
    statistic: Callable[[Sequence[int]], float],
    *,
    seed: int,
    samples: int = BOOTSTRAP_SAMPLES,
) -> tuple[float, float]:
    """95% percentile interval of ``statistic`` over resampled block indexes.

    A resample is a multiset of ``size`` indexes. When there are no more
    distinct multisets than ``samples``, each one is evaluated once with its
    multinomial weight, which is the bootstrap distribution itself rather than
    a Monte Carlo estimate of it; otherwise ``samples`` seeded draws estimate it.
    """
    if size < 1:
        raise ValueError('bootstrap requires at least one block')
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
    return _weighted_percentile(weighted, 0.025), _weighted_percentile(weighted, 0.975)


def bootstrap_median_interval(
    values: Sequence[float], *, seed: int
) -> tuple[float, float]:
    """95% percentile bootstrap interval of the median of ``values``."""
    return bootstrap_interval(
        len(values),
        lambda indexes: statistics.median(values[index] for index in indexes),
        seed=seed,
    )


def _weighted_percentile(weighted: list[tuple[float, int]], quantile: float) -> float:
    """Interpolated percentile of the values repeated by their weights."""
    if not weighted:
        raise ValueError('percentile requires values')
    ordered = sorted(weighted)
    position = quantile * (sum(weight for _, weight in ordered) - 1)
    lower = math.floor(position)
    fraction = position - lower

    def at(index: int) -> float:
        seen = 0
        for value, weight in ordered:
            seen += weight
            if index < seen:
                return value
        raise AssertionError('percentile index is within the total weight')

    low = at(lower)
    return low if fraction == 0 else low + (at(lower + 1) - low) * fraction
