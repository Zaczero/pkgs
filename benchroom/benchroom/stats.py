"""Statistics over independent runs or paired blocks."""

from __future__ import annotations

import itertools
import math
import random
import sys
from collections import Counter
from dataclasses import dataclass
from fractions import Fraction
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

_BOOTSTRAP_SAMPLES = 10_000


@dataclass(frozen=True, slots=True)
class OrderInterval:
    """Binomial order-statistic interval for a common population median.

    ``interval`` is None when the sample cannot supply a finite interval at the
    requested error probability. Coverage assumes independent observations with
    a common median; atoms at the median make the interval conservative.
    """

    interval: tuple[float, float] | None
    achieved_coverage: float
    sample_size: int
    lower_order: int | None
    upper_order: int | None


def median_order_interval(
    values: Sequence[float], *, error_probability: float
) -> OrderInterval:
    """Return the narrowest central median interval within the error budget.

    The two-sided miss probability for order k is
    ``2 * sum(comb(n, j) for j in range(k)) / 2**n``. This is a fixed-look
    interval: repeated inspection needs a separately allocated across-look
    error budget, as provided by ``benchroom.sampling.SamplingPolicy``.
    """
    if not values or any(not math.isfinite(value) for value in values):
        raise ValueError('median observations must be nonempty and finite')
    if not 0 < error_probability < 1:
        raise ValueError('error probability must lie strictly between zero and one')
    size = len(values)
    budget = Fraction(error_probability)
    tail = 0
    selected = 0
    miss = Fraction(0)
    for order in range(1, (size + 1) // 2 + 1):
        tail += math.comb(size, order - 1)
        candidate = Fraction(2 * tail, 2**size)
        if candidate > budget:
            break
        selected, miss = order, candidate
    if not selected:
        return OrderInterval(None, 1.0, size, None, None)
    ordered = sorted(values)
    return OrderInterval(
        (ordered[selected - 1], ordered[size - selected]),
        float(1 - miss),
        size,
        selected,
        size - selected + 1,
    )


def _exact_distribution_fits(size: int, samples: int) -> bool:
    # C(2n-1, n) >= 2**(n-1); avoid constructing a huge integer just to reject it.
    return size <= samples.bit_length() and math.comb(2 * size - 1, size) <= samples


def _midpoint(left: float, right: float) -> float:
    total = left + right
    if math.isinf(total) and math.isfinite(left) and math.isfinite(right):
        return left / 2 + right / 2
    return total / 2


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return _midpoint(ordered[middle - 1], ordered[middle])


def _interpolate(low: float, high: float, fraction: float) -> float:
    span = high - low
    if math.isinf(span) and math.isfinite(low) and math.isfinite(high):
        return low * (1 - fraction) + high * fraction
    return low + span * fraction


def percentile(values: Sequence[float], fraction: float) -> float:
    """Return a linearly interpolated percentile; fraction lies in [0, 1]."""
    if not values:
        raise ValueError('values must not be empty')
    if not 0 <= fraction <= 1:
        raise ValueError(f'fraction must be between 0 and 1, got {fraction}')
    ordered = sorted(values)
    position = fraction * (len(ordered) - 1)
    lower = math.floor(position)
    remainder = position - lower
    low = ordered[lower]
    return low if remainder == 0 else _interpolate(low, ordered[lower + 1], remainder)


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
    if size < 1:
        raise ValueError(f'size must be at least 1, got {size}')
    if samples < 1:
        raise ValueError(f'samples must be at least 1, got {samples}')
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
    if not size:
        raise ValueError('values must not be empty')
    if _exact_distribution_fits(size, _BOOTSTRAP_SAMPLES):
        return bootstrap_interval(
            size,
            lambda indexes: _median([values[index] for index in indexes]),
            seed=seed,
        )

    import numpy as np

    data = np.asarray(values, dtype=np.float64)
    rng = np.random.default_rng(seed)
    medians = np.empty(_BOOTSTRAP_SAMPLES)
    batch = max(1, (1024 * 1024) // (data.itemsize * size))
    extreme = any(abs(value) > sys.float_info.max / 2 for value in values)
    # Keep the previous batch until its replacement is allocated: releasing it
    # first can trigger heap trimming and page faults on every iteration.
    for start in range(0, _BOOTSTRAP_SAMPLES, batch):
        end = min(start + batch, _BOOTSTRAP_SAMPLES)
        # NumPy's generic overloads include types unavailable at the Python floor.
        draws = rng.choice(data, size=(end - start, size))  # pyright: ignore[reportUnknownMemberType]
        if extreme:
            middle = size // 2
            draws.partition((middle - 1, middle) if size % 2 == 0 else middle, axis=1)
            if size % 2:
                medians[start:end] = draws[:, middle]
            else:
                left, right = draws[:, middle - 1], draws[:, middle]
                with np.errstate(over='ignore'):
                    total = left + right
                    medians[start:end] = np.where(  # pyright: ignore[reportUnknownMemberType]
                        np.isfinite(total), total / 2, left / 2 + right / 2
                    )
        else:
            medians[start:end] = np.median(draws, axis=1, overwrite_input=True)
    if extreme:
        values = medians.tolist()
        return percentile(values, 0.025), percentile(values, 0.975)
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
    return low if remainder == 0 else _interpolate(low, at(lower + 1), remainder)
