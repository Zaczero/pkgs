"""Shared pyperf setup for the three benchmark suites."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pyperf

_SUPPORT = Path(__file__).resolve().parents[1] / 'support'
if str(_SUPPORT) not in sys.path:
    sys.path.insert(0, str(_SUPPORT))

from _bench_registry import PROFILES

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any


def selected_benchmarks(suite: str) -> tuple[str, ...]:
    """Rows selected by the driver, or the smoke manifest for a direct run."""
    explicit = os.environ.get('GOMETRY_BENCH_FILTER')
    if explicit is not None:
        return tuple(row.strip() for row in explicit.split(',') if row.strip())
    profile = os.environ.get('GOMETRY_BENCH_PROFILE', 'smoke')
    if profile != 'smoke' and os.environ.get('GOMETRY_BENCH_ORCHESTRATED') != '1':
        raise SystemExit('release benchmarks run through benches/drivers/bench.py')
    return PROFILES[profile].rows(suite)


def queue_selected_benchmarks(runner: pyperf.Runner, suite: str) -> Callable[[], None]:
    """Collect registrations, then execute them in driver-supplied order."""
    selected = selected_benchmarks(suite)
    selected_set = set(selected)
    pending = set(selected)
    bench_func = runner.bench_func
    registrations: dict[
        str,
        tuple[object, tuple[object, ...], dict[str, Any]],
    ] = {}

    def queue(name: str, func: object, *args: object, **kwargs: Any) -> None:
        if name in selected_set:
            pending.discard(name)
            registrations[name] = (func, args, kwargs)

    runner.bench_func = queue

    def flush() -> None:
        if pending and '--worker' not in sys.argv:
            missing = ', '.join(sorted(pending))
            raise SystemExit(
                f'GOMETRY_BENCH_FILTER selected unregistered benchmarks: {missing}'
            )
        runner.bench_func = bench_func
        for name in selected:
            registration = registrations.get(name)
            if registration is None:
                continue
            func, args, kwargs = registration
            bench_func(name, func, *args, **kwargs)

    return flush


def runner() -> pyperf.Runner:
    """Create the runner; the driver supplies release sampling arguments."""
    explicit = {
        '--debug-single-value',
        '--fast',
        '--rigorous',
        '--processes',
        '--values',
        '--warmups',
        '--min-time',
        '--loops',
    }
    if any(arg in explicit or arg.startswith('--min-time=') for arg in sys.argv[1:]):
        return pyperf.Runner()
    return pyperf.Runner(processes=1, values=1, warmups=1, min_time=0.01)
