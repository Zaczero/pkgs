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

from _bench_registry import PROFILES, RELEASE_OPERATIONS

from typing import Any

if TYPE_CHECKING:
    from collections.abc import Callable


def selected_benchmarks(suite: str) -> tuple[str, ...]:
    """Rows selected by the driver, or the smoke manifest for a direct run."""
    explicit = os.environ.get('GOMETRY_BENCH_FILTER')
    if explicit is not None:
        selected = tuple(row.strip() for row in explicit.split(',') if row.strip())
        if not selected:
            raise SystemExit(
                'GOMETRY_BENCH_FILTER must select at least one benchmark; '
                'use the driver without --filter for the intentional full selection'
            )
        return selected
    profile = os.environ.get('GOMETRY_BENCH_PROFILE', 'smoke')
    if profile != 'smoke' and os.environ.get('GOMETRY_BENCH_ORCHESTRATED') != '1':
        raise SystemExit('release benchmarks run through benches/drivers/bench.py')
    return PROFILES[profile].rows(suite)


def _require_oracle_gate() -> None:
    """Orchestrated timing workers must inherit the driver's oracle OK flag."""
    if os.environ.get('GOMETRY_BENCH_ORCHESTRATED') != '1':
        return
    if os.environ.get('GOMETRY_BENCH_ORACLE_OK') != '1':
        raise SystemExit(
            'orchestrated timing requires GOMETRY_BENCH_ORACLE_OK=1 '
            '(run the oracle via benches/drivers/bench.py first)'
        )


def queue_selected_benchmarks(runner: pyperf.Runner, suite: str) -> Callable[[], None]:
    """Collect registrations, then execute them in driver-supplied order.

    Duplicate registration of a selected name is fatal. Missing selected names
    (including public RELEASE rows) are fatal before any timing starts.

    Resolve the selected name set **before** suite code builds fixtures so a
    single-row filter does not materialize the entire public catalogue.
    """
    _require_oracle_gate()
    selected = selected_benchmarks(suite)
    selected_set = set(selected)
    pending = set(selected)
    public_names = {
        name for op in RELEASE_OPERATIONS if op.suite == suite for name in op.rows
    }
    bench_func = runner.bench_func
    registrations: dict[
        str,
        tuple[object, tuple[object, ...], dict[str, Any]],
    ] = {}

    def queue(name: str, func: object, *args: object, **kwargs: Any) -> None:
        if name in registrations:
            raise SystemExit(f'duplicate benchmark registration: {name}')
        if name in selected_set:
            pending.discard(name)
            registrations[name] = (func, args, kwargs)

    runner.bench_func = queue

    def flush() -> None:
        if pending and '--worker' not in sys.argv:
            missing = ', '.join(sorted(pending))
            public_missing = sorted(pending & public_names)
            if public_missing:
                raise SystemExit(
                    'missing public benchmark registration (fatal before timing): '
                    + ', '.join(public_missing)
                )
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


def register_selected_public_release_ops(runner: Any, suite: str) -> None:
    """Register only the public RELEASE rows selected for this worker.

    Resolves selected names first, then builds each needed builder lazily so a
    one-row filter never materializes the full fixture catalogue (~34 MB vs
    hundreds of MB for every fixture).
    """
    from _bench_public_cases import BUILDERS, PUBLIC_TIMED

    selected = set(selected_benchmarks(suite))
    if not selected:
        return
    for op in RELEASE_OPERATIONS:
        if op.suite != suite:
            continue
        needed = [name for name in op.rows if name in selected]
        if not needed:
            continue
        # Builder materializes fixtures and registers timed closures for this op.
        BUILDERS[op.gometry]()
        for name in needed:
            fn = PUBLIC_TIMED.get(name)
            if fn is None:
                raise SystemExit(f'missing public timed callable: {name}')
            runner.bench_func(name, fn)


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
