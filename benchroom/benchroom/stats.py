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

_BOOTSTRAP_SAMPLES = 10_000


def _exact_distribution_fits(size: int, samples: int) -> bool:
    # C(2n-1, n) >= 2**(n-1); avoid constructing a huge integer just to reject it.
    return size <= samples.bit_length() and math.comb(2 * size - 1, size) <= samples


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
    samples: int = _BOOTSTRAP_SAMPLES,
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
    if _exact_distribution_fits(size, samples):
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
    size = len(values)
    if not size or _exact_distribution_fits(size, _BOOTSTRAP_SAMPLES):
        return bootstrap_interval(
            size,
            lambda indexes: statistics.median(values[index] for index in indexes),
            seed=seed,
        )

    import numpy as np

    data = np.asarray(values, dtype=np.float64)
    rng = np.random.default_rng(seed)
    medians = np.empty(_BOOTSTRAP_SAMPLES)
    batch = max(1, (1024 * 1024) // (data.itemsize * size))
    for start in range(0, _BOOTSTRAP_SAMPLES, batch):
        end = min(start + batch, _BOOTSTRAP_SAMPLES)
        # NumPy's generic overloads include types unavailable at the Python floor.
        draws = rng.choice(data, size=(end - start, size))  # pyright: ignore[reportUnknownMemberType]
        medians[start:end] = np.median(draws, axis=1, overwrite_input=True)
    low, high = np.quantile(medians, (0.025, 0.975))  # pyright: ignore[reportUnknownMemberType]
    return float(low), float(high)


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
