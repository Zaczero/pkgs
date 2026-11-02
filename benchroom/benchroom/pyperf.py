"""Read pyperf results without treating readings in one worker as separate runs."""

from __future__ import annotations

import statistics
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyperf  # pyright: ignore[reportMissingTypeStubs]

from benchroom.report import Series

if TYPE_CHECKING:
    from collections.abc import Sequence


def _run_mean(values: Sequence[float]) -> float:
    try:
        return statistics.fmean(values)
    except OverflowError:
        # The exact-ratio mean avoids a floating sum that overflows even
        # though the average of finite measurements remains representable.
        return float(statistics.mean(values))


def load(path: str | Path, *, name: str | None = None) -> tuple[Series, ...]:
    """Read timing results as seconds per operation, averaging within each run.

    Warmup-only runs are omitted. Non-time benchmarks raise ValueError rather
    than mixing units. ``name`` overrides the implementation metadata, falling
    back to the file stem when neither is supplied.
    """
    # pyperf does not publish type information for its result objects.
    suite: Any = pyperf.BenchmarkSuite.load(str(path))  # pyright: ignore[reportUnknownMemberType]
    result: list[Series] = []
    for benchmark in suite:
        unit = benchmark.get_unit()
        if unit != 'second':
            raise ValueError(f"benchmark unit must be 'second', got {unit!r} in {path}")
        values = tuple(
            _run_mean(run.values) for run in benchmark.get_runs() if run.values
        )
        label = name or benchmark.get_metadata().get('implementation', Path(path).stem)
        result.append(Series(benchmark.get_name(), label, values))
    return tuple(result)
