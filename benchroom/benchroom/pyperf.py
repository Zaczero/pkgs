"""Read pyperf results without treating readings in one worker as separate runs."""

from __future__ import annotations

import statistics
from pathlib import Path

import pyperf

from benchroom.report import Series


def load(path: str | Path, *, name: str | None = None) -> tuple[Series, ...]:
    """Read timing results as seconds per operation, averaging within each run.

    Warmup-only runs are omitted. Non-time benchmarks raise ValueError rather
    than mixing units. ``name`` overrides the implementation metadata, falling
    back to the file stem when neither is supplied.
    """
    suite = pyperf.BenchmarkSuite.load(str(path))
    result = []
    for benchmark in suite:
        if benchmark.get_unit() != 'second':
            raise ValueError('pyperf timing import requires seconds')
        values = tuple(
            statistics.fmean(run.values) for run in benchmark.get_runs() if run.values
        )
        label = name or benchmark.get_metadata().get('implementation', Path(path).stem)
        result.append(Series(benchmark.get_name(), label, values))
    return tuple(result)
