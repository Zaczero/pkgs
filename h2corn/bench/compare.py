"""Paired A/B benchmark for two h2corn builds or two h2corn configurations.

This is the tool that decides whether a code change helped. Each round starts
both variants cold, in alternating order, and measures them back to back; the
statistic is the *paired* per-round delta, so slow drift on a shared host
cancels instead of being read as a code change. Rounds continue until the
delta's bootstrap confidence interval is narrow enough to act on, or the time
budget runs out — in which case the result is reported as inconclusive rather
than rounded into a claim.

    # two builds, same scenario
    uv run python bench/compare.py \
        --control 'main=/path/to/main/.venv/bin/h2corn' \
        --candidate 'head=.venv/bin/h2corn' --scenario h1 --workers 4

    # one build, two configurations
    uv run python bench/compare.py \
        --control 'log=' --candidate 'nolog=--no-access-log' --scenario h1
"""

from __future__ import annotations

import argparse
import shlex
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    from bench._core import (
        RESPONSE_CONTRACTS,
        RESULTS_DIRECTORY,
        BenchmarkError,
        Metrics,
        Scenario,
        check_response,
        duration_seconds,
        ensure_static_file_payload,
        paired_comparison,
        run_load,
        running_server,
        write_json,
    )
except ModuleNotFoundError:  # Direct ``python bench/compare.py`` execution.
    from _core import (  # type: ignore[import-not-found, no-redef]
        RESPONSE_CONTRACTS,
        RESULTS_DIRECTORY,
        BenchmarkError,
        Metrics,
        Scenario,
        check_response,
        duration_seconds,
        ensure_static_file_payload,
        paired_comparison,
        run_load,
        running_server,
        write_json,
    )

if TYPE_CHECKING:
    from collections.abc import Sequence

DURATION = '10s'
WARMUP_DURATION = '2s'
CONCURRENCY = 100
MIN_ROUNDS = 4
MAX_ROUNDS = 20
#: Stop once the paired delta is resolved to better than this, in percent.
TARGET_HALF_WIDTH_PERCENT = 1.0
TIME_BUDGET_SECONDS = 900.0
SEED = 20_260_726


class Variant:
    """One side of the comparison: a binary plus configuration arguments."""

    __slots__ = ('args', 'executable', 'name')

    def __init__(self, name: str, executable: str, args: Sequence[str]) -> None:
        self.name = name
        self.executable = executable
        self.args = list(args)

    def command(self, scenario: Scenario, app: str) -> list[str]:
        socket_path = scenario.socket_path
        target = f'unix:{socket_path}' if socket_path is not None else '127.0.0.1:8000'
        return [
            self.executable,
            app,
            '-b',
            target,
            '-w',
            str(scenario.workers),
            '--backlog',
            '2048',
            *self.args,
        ]


def parse_variant(value: str, executable: str) -> Variant:
    name, separator, arguments = value.partition('=')
    if not name:
        raise argparse.ArgumentTypeError('a variant needs a NAME[=BIN|ARGS] form')
    if not separator:
        return Variant(name, executable, [])
    words = shlex.split(arguments)
    # A single bare path is the variant's own binary; anything else is config.
    if len(words) == 1 and not words[0].startswith('-'):
        return Variant(name, words[0], [])
    return Variant(name, executable, words)


def measure(
    variant: Variant, scenario: Scenario, app: str, duration: str, warmup: str
) -> tuple[Metrics, int]:
    concurrency = scenario.concurrency or CONCURRENCY
    with running_server(
        variant.command(scenario, app),
        workers=scenario.workers,
        socket_path=scenario.socket_path,
    ) as server:
        check_response(scenario)
        run_load(scenario, duration=warmup, concurrency=concurrency)
        metrics = run_load(scenario, duration=duration, concurrency=concurrency)
        check_response(scenario)
        return metrics, server.memory_bytes()


def compare(
    control: Variant,
    candidate: Variant,
    scenario: Scenario,
    *,
    app: str,
    duration: str,
    warmup: str,
    max_rounds: int,
    deadline: float,
) -> dict[str, Any]:
    rates: dict[str, list[float]] = {control.name: [], candidate.name: []}
    memory: dict[str, list[int]] = {control.name: [], candidate.name: []}
    latency: dict[str, list[dict[str, float]]] = {control.name: [], candidate.name: []}
    stopped = 'max-rounds'
    rounds = 0

    for round_index in range(1, max_rounds + 1):
        # Alternate AB / BA so a monotonic host trend cannot accumulate into
        # one variant's samples.
        order = (control, candidate) if round_index % 2 else (candidate, control)
        for variant in order:
            metrics, resident = measure(variant, scenario, app, duration, warmup)
            rates[variant.name].append(metrics['rps'])
            latency[variant.name].append(metrics['latency_percentiles'])
            memory[variant.name].append(resident)
        rounds = round_index

        result = paired_comparison(rates[control.name], rates[candidate.name], SEED)
        low, high = result.ci_percent
        print(f'  round {round_index}: {result.describe()}')
        # Only consider stopping on a fixed schedule. Re-examining the interval
        # after every single round is many chances to catch a momentarily narrow
        # one, which biases both the stopping point and the final verdict.
        may_stop = round_index >= MIN_ROUNDS and (round_index - MIN_ROUNDS) % 4 == 0
        if may_stop and (high - low) / 2 <= TARGET_HALF_WIDTH_PERCENT:
            stopped = 'resolved'
            break
        if time.monotonic() > deadline:
            stopped = 'time-budget'
            break

    result = paired_comparison(rates[control.name], rates[candidate.name], SEED)
    return {
        'scenario': scenario.name,
        'type': scenario.type,
        'workers': scenario.workers,
        'duration': duration,
        'rounds': rounds,
        'stopped': stopped,
        'control': control.name,
        'candidate': candidate.name,
        'commands': {
            control.name: control.command(scenario, app),
            candidate.name: candidate.command(scenario, app),
        },
        'rps': rates,
        'memory_bytes': memory,
        'latency_percentiles': latency,
        'comparison': {
            'control_median': result.control_median,
            'candidate_median': result.candidate_median,
            'delta_percent': result.delta_percent,
            'ci_percent': list(result.ci_percent),
            'significant': result.significant,
        },
        'memory_median_bytes': {
            name: statistics.median(values) for name, values in memory.items()
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description='paired A/B benchmark for h2corn builds and configurations',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--control', required=True, metavar='NAME[=BIN|ARGS]')
    parser.add_argument('--candidate', required=True, metavar='NAME[=BIN|ARGS]')
    parser.add_argument('--executable', default='h2corn', help='default variant binary')
    parser.add_argument('--app', default='bench.bench_app:app')
    parser.add_argument('--scenario', default='h1', choices=list(RESPONSE_CONTRACTS))
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--concurrency', type=int)
    parser.add_argument('--http2-parallelism', type=int, default=1)
    parser.add_argument('--duration', default=DURATION)
    parser.add_argument('--warmup-duration', default=WARMUP_DURATION)
    parser.add_argument('--max-rounds', type=int, default=MAX_ROUNDS)
    parser.add_argument('--time-budget', type=float, default=TIME_BUDGET_SECONDS)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()

    if args.workers < 1:
        parser.error('--workers must be positive')
    if args.max_rounds < MIN_ROUNDS:
        parser.error(f'--max-rounds must be at least {MIN_ROUNDS}')
    duration_seconds(args.duration)
    duration_seconds(args.warmup_duration)

    control = parse_variant(args.control, args.executable)
    candidate = parse_variant(args.candidate, args.executable)
    plural = 's' if args.workers > 1 else ''
    scenario = Scenario(
        f'{args.scenario} ({args.workers} worker{plural})',
        args.workers,
        args.scenario,
        args.concurrency,
        args.http2_parallelism,
    )

    ensure_static_file_payload()
    print(f'=== {control.name} vs {candidate.name}: {scenario.name} ===')
    try:
        record = compare(
            control,
            candidate,
            scenario,
            app=args.app,
            duration=args.duration,
            warmup=args.warmup_duration,
            max_rounds=args.max_rounds,
            deadline=time.monotonic() + args.time_budget,
        )
    except BenchmarkError as error:
        print(f'comparison failed: {error}', file=sys.stderr)
        return 1

    stamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
    output = args.output or (
        RESULTS_DIRECTORY / 'compare' / f'{stamp}-{control.name}-{candidate.name}.json'
    )
    write_json(output, record)

    comparison = record['comparison']
    low, high = comparison['ci_percent']
    memory = record['memory_median_bytes']
    print()
    print(f'{control.name:>16}: {comparison["control_median"]:>12,.0f} RPS')
    print(f'{candidate.name:>16}: {comparison["candidate_median"]:>12,.0f} RPS')
    print(
        f'{"delta":>16}: {comparison["delta_percent"]:>+11.2f}% '
        f'(95% CI {low:+.2f}%..{high:+.2f}%)'
    )
    print(
        f'{"memory (PSS)":>16}: {memory[control.name] / 1e6:,.1f} MB -> '
        f'{memory[candidate.name] / 1e6:,.1f} MB'
    )
    if record['stopped'] == 'time-budget':
        print('INCONCLUSIVE within the time budget — the interval is still wide.')
    elif comparison['significant']:
        print('SIGNIFICANT: the interval excludes zero.')
    else:
        print('No significant difference: the interval spans zero.')
    print(f'Record written to {output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
