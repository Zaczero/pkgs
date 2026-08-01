"""Paired instructions-per-request benchmark for h2corn builds.

Wall-clock RPS is the right measure for deployment-level comparisons, but it
cannot resolve the small server-side changes that a busy development host
buries in scheduler noise.  This runner attaches ``perf stat`` to every
server worker and divides the resulting hardware counters by an exact number
of successful raw-ASGI requests.  As in :mod:`bench.compare`, cold starts are
rotation-balanced and the decision is the paired bootstrap interval, not a
single favourable run.

    uv run --no-sync python bench/instr.py \
        --control 'main=/path/to/main/.venv/bin/h2corn' \
        --candidate 'head=.venv/bin/h2corn'
"""

from __future__ import annotations

import argparse
import json
import math
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    from bench._core import (
        RESULTS_DIRECTORY,
        BenchmarkError,
        Scenario,
        _run,
        bootstrap_trimmed_mean_ci,
        check_response,
        ensure_static_file_payload,
        paired_comparison,
        rotations,
        running_server,
        terminate_process_group,
        trimmed_mean,
        write_json,
    )
    from bench.compare import Variant, parse_variant
except ModuleNotFoundError:  # Direct ``python bench/instr.py`` execution.
    from _core import (  # type: ignore[import-not-found, no-redef]
        RESULTS_DIRECTORY,
        BenchmarkError,
        Scenario,
        _run,
        bootstrap_trimmed_mean_ci,
        check_response,
        ensure_static_file_payload,
        paired_comparison,
        rotations,
        running_server,
        terminate_process_group,
        trimmed_mean,
        write_json,
    )
    from compare import (  # type: ignore[import-not-found, no-redef]
        Variant,
        parse_variant,
    )

if TYPE_CHECKING:
    from collections.abc import Sequence

APP = 'bench.raw_app:app'
CONCURRENCY = 100
REQUESTS = 200_000
# Measured, not guessed: at 5,000 warmup requests this harness reports a
# SIGNIFICANT -0.24% difference between a binary and *itself* — the first
# measurement of a session is a cold outlier that four rounds cannot dilute.
# At 20,000 the same null lands at -0.096% (95% CI -0.274%..+0.085%), spanning
# zero. Do not lower this without re-running the self-comparison; a harness
# that resolves a quarter of a percent out of thin air is worse than no harness,
# because most changes worth measuring here are that size.
WARMUP_REQUESTS = 20_000
MIN_ROUNDS = 4
MAX_ROUNDS = 20
# This is a percentage-point half-width on the paired relative delta: ±0.3%
# of the control's instructions/request, not 0.3% of a possibly-zero delta.
TARGET_HALF_WIDTH_PERCENT = 0.3
TIME_BUDGET_SECONDS = 900.0
LOAD_TIMEOUT_SECONDS = 120.0
PERF_ATTACH_SETTLE_SECONDS = 0.05
SEED = 20_260_730
COUNTERS = ('instructions', 'cycles', 'branch-misses', 'cache-misses')
SCENARIOS = (
    'h1',
    'h1_uds',
    'h2',
    'h1_download',
    'h2_download',
    'h1_stream',
    'h2_stream',
    'h1_file',
    'h2_file',
    'h2_upload',
)


def oha_command(scenario: Scenario, requests: int) -> list[str]:
    """Build the fixed-count raw-app load command for one supported scenario."""
    contract = scenario.contract
    if contract.driver != 'oha':
        raise BenchmarkError(f'{scenario.type} does not use the fixed-count oha driver')
    concurrency = scenario.concurrency or CONCURRENCY
    command = [
        'oha',
        '-n',
        str(requests),
        '-c',
        str(concurrency),
        '-p',
        str(scenario.http2_parallelism),
        '--output-format',
        'json',
        '--http-version',
        '2' if contract.protocol == '2' else '1.1',
        '-m',
        contract.method,
    ]
    if scenario.socket_path is not None:
        command += ['--unix-socket', str(scenario.socket_path)]
    command.append(contract.load_url())
    return command


def run_fixed_requests(scenario: Scenario, requests: int) -> int:
    """Run exactly ``requests`` successful responses and return their count."""
    output = _run(oha_command(scenario, requests), LOAD_TIMEOUT_SECONDS)
    try:
        raw = json.loads(output)
    except json.JSONDecodeError as error:
        raise BenchmarkError('oha returned invalid JSON') from error

    errors = raw.get('errorDistribution', {})
    if not isinstance(errors, dict) or any(errors.values()):
        raise BenchmarkError(f'oha reported request errors: {errors!r}')
    statuses = raw.get('statusCodeDistribution', {})
    if not isinstance(statuses, dict):
        raise BenchmarkError(f'oha reported invalid statuses: {statuses!r}')
    completed = sum(value for value in statuses.values() if isinstance(value, int))
    if statuses != {'200': requests} or completed != requests:
        raise BenchmarkError(
            f'oha completed {completed}/{requests} successful requests: {statuses!r}'
        )
    return completed


def start_perf(worker_pids: Sequence[int]) -> subprocess.Popen[str]:
    """Attach user-space hardware counters to every worker process."""
    command = [
        'perf',
        'stat',
        '--no-big-num',
        '--field-separator',
        ',',
        '--event',
        ','.join(f'{counter}:u' for counter in COUNTERS),
        '--pid',
        ','.join(map(str, worker_pids)),
    ]
    try:
        return subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except OSError as error:
        raise BenchmarkError(f'failed to start perf: {error}') from error


def perf_totals(process: subprocess.Popen[str]) -> dict[str, float]:
    """Stop ``perf stat`` and parse its machine-readable counter output."""
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
    try:
        _, output = process.communicate(timeout=10)
    except subprocess.TimeoutExpired as error:
        terminate_process_group(process)
        raise BenchmarkError('perf did not stop after the fixed request run') from error
    if process.returncode not in (0, -signal.SIGINT):
        raise BenchmarkError(f'perf failed: {output.strip()}')

    totals: dict[str, float] = {}
    for line in output.splitlines():
        fields = line.split(',')
        if len(fields) < 3:
            continue
        event = fields[2].strip().removesuffix(':u')
        if event not in COUNTERS:
            continue
        value = fields[0].strip()
        if value.startswith('<'):
            raise BenchmarkError(f'perf could not count {event}: {value}')
        try:
            count = float(value)
        except ValueError as error:
            raise BenchmarkError(
                f'perf returned an invalid {event} count: {value!r}'
            ) from error
        if not math.isfinite(count) or count < 0:
            raise BenchmarkError(f'perf returned an invalid {event} count: {value!r}')
        if len(fields) > 4 and fields[4].strip():
            try:
                running_percent = float(fields[4])
            except ValueError as error:
                raise BenchmarkError(
                    f'perf returned invalid {event} runtime: {fields[4]!r}'
                ) from error
            if running_percent < 100.0:
                raise BenchmarkError(
                    f'perf multiplexed {event} ({running_percent:.2f}% running); retry later'
                )
        totals[event] = count
    missing = set(COUNTERS) - totals.keys()
    if missing:
        raise BenchmarkError(
            f'perf returned no counts for: {", ".join(sorted(missing))}'
        )
    return totals


def measure(
    variant: Variant, scenario: Scenario, requests: int, warmup_requests: int
) -> dict[str, float]:
    """Return the worker-only counters per successful raw-ASGI request."""
    with running_server(
        variant.command(scenario, APP),
        workers=scenario.workers,
        socket_path=scenario.socket_path,
    ) as server:
        check_response(scenario)
        run_fixed_requests(scenario, warmup_requests)
        perf = start_perf(server.worker_pids)
        try:
            # perf has no readiness protocol after attaching to an existing PID.
            # Its event setup completes before this small, fixed settle interval.
            time.sleep(PERF_ATTACH_SETTLE_SECONDS)
            completed = run_fixed_requests(scenario, requests)
        except BaseException:
            terminate_process_group(perf)
            raise
        totals = perf_totals(perf)
        check_response(scenario)

    per_request = {counter: totals[counter] / completed for counter in COUNTERS}
    per_request['ipc'] = totals['instructions'] / totals['cycles']
    return per_request


def metric_comparison(
    control: Sequence[float], candidate: Sequence[float]
) -> dict[str, float | bool | list[float]]:
    """Use the shared paired estimator for one per-request counter metric."""
    paired = paired_comparison(control, candidate, SEED)
    deltas = [
        (after - before) / before * 100.0
        for before, after in zip(control, candidate, strict=True)
    ]
    # Keep the estimator components explicit here: this makes every record
    # self-describing while retaining _core.py as the one implementation.
    delta_percent = trimmed_mean(deltas)
    low, high = bootstrap_trimmed_mean_ci(deltas, SEED)
    return {
        'control_median': paired.control_median,
        'candidate_median': paired.candidate_median,
        'delta_percent': delta_percent,
        'ci_percent': [low, high],
        'significant': paired.significant,
    }


def compare(
    control: Variant,
    candidate: Variant,
    scenario: Scenario,
    *,
    requests: int,
    warmup_requests: int,
    max_rounds: int,
    deadline: float,
) -> dict[str, Any]:
    """Run rotation-balanced pairs until instructions/request is resolved."""
    variants = {control.name: control, candidate.name: candidate}
    samples = {
        control.name: {metric: [] for metric in (*COUNTERS, 'ipc')},
        candidate.name: {metric: [] for metric in (*COUNTERS, 'ipc')},
    }
    stopped = 'max-rounds'
    rounds = 0

    for round_index, order in enumerate(
        rotations([control.name, candidate.name], max_rounds, SEED), start=1
    ):
        for name in order:
            sample = measure(variants[name], scenario, requests, warmup_requests)
            for metric, value in sample.items():
                samples[name][metric].append(value)
        rounds = round_index

        instructions = paired_comparison(
            samples[control.name]['instructions'],
            samples[candidate.name]['instructions'],
            SEED,
        )
        low, high = instructions.ci_percent
        print(f'  round {round_index}: {instructions.describe("instructions/request")}')
        may_stop = round_index >= MIN_ROUNDS and (round_index - MIN_ROUNDS) % 4 == 0
        if may_stop and (high - low) / 2 <= TARGET_HALF_WIDTH_PERCENT:
            stopped = 'resolved'
            break
        if time.monotonic() > deadline:
            stopped = 'time-budget'
            break

    comparisons = {
        metric: metric_comparison(
            samples[control.name][metric], samples[candidate.name][metric]
        )
        for metric in (*COUNTERS, 'ipc')
    }
    return {
        'app': APP,
        'scenario': scenario.name,
        'type': scenario.type,
        'workers': scenario.workers,
        'requests_per_sample': requests,
        'warmup_requests': warmup_requests,
        'rounds': rounds,
        'stopped': stopped,
        'target_half_width_percent': TARGET_HALF_WIDTH_PERCENT,
        'control': control.name,
        'candidate': candidate.name,
        'commands': {
            control.name: control.command(scenario, APP),
            candidate.name: candidate.command(scenario, APP),
        },
        'per_request': samples,
        'comparison': comparisons,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description='paired perf-stat instructions/request benchmark for h2corn builds',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--control', required=True, metavar='NAME[=BIN|ARGS]')
    parser.add_argument('--candidate', required=True, metavar='NAME[=BIN|ARGS]')
    parser.add_argument('--executable', default='h2corn', help='default variant binary')
    parser.add_argument('--scenario', default='h1', choices=SCENARIOS)
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--concurrency', type=int)
    parser.add_argument('--http2-parallelism', type=int, default=1)
    parser.add_argument('--requests', type=int, default=REQUESTS)
    parser.add_argument('--warmup-requests', type=int, default=WARMUP_REQUESTS)
    parser.add_argument('--max-rounds', type=int, default=MAX_ROUNDS)
    parser.add_argument('--time-budget', type=float, default=TIME_BUDGET_SECONDS)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()

    if args.workers < 1:
        parser.error('--workers must be positive')
    if args.requests < 1 or args.warmup_requests < 1:
        parser.error('--requests and --warmup-requests must be positive')
    if args.max_rounds < MIN_ROUNDS:
        parser.error(f'--max-rounds must be at least {MIN_ROUNDS}')
    if args.time_budget <= 0:
        parser.error('--time-budget must be positive')

    control = parse_variant(args.control, args.executable)
    candidate = parse_variant(args.candidate, args.executable)
    if control.name == candidate.name:
        parser.error('--control and --candidate need distinct names')
    plural = 's' if args.workers > 1 else ''
    scenario = Scenario(
        f'{args.scenario} ({args.workers} worker{plural})',
        args.workers,
        args.scenario,
        args.concurrency,
        args.http2_parallelism,
    )

    ensure_static_file_payload()
    print(f'=== {control.name} vs {candidate.name}: {scenario.name}, {APP} ===')
    try:
        record = compare(
            control,
            candidate,
            scenario,
            requests=args.requests,
            warmup_requests=args.warmup_requests,
            max_rounds=args.max_rounds,
            deadline=time.monotonic() + args.time_budget,
        )
    except BenchmarkError as error:
        print(f'comparison failed: {error}', file=sys.stderr)
        return 1

    stamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
    output = args.output or (
        RESULTS_DIRECTORY / 'instr' / f'{stamp}-{control.name}-{candidate.name}.json'
    )
    write_json(output, record)

    print()
    for metric in ('instructions', 'cycles', 'ipc', 'branch-misses', 'cache-misses'):
        comparison = record['comparison'][metric]
        low, high = comparison['ci_percent']
        unit = '' if metric == 'ipc' else '/request'
        print(
            f'{metric + unit:>24}: '
            f'{comparison["control_median"]:>12,.2f} -> '
            f'{comparison["candidate_median"]:>12,.2f} '
            f'({comparison["delta_percent"]:+.3f}%, 95% CI {low:+.3f}%..{high:+.3f}%)'
        )
    instructions = record['comparison']['instructions']
    low, high = instructions['ci_percent']
    if record['stopped'] != 'resolved':
        print(
            'INCONCLUSIVE within the budget — instructions/request remains wider than '
            f'±{TARGET_HALF_WIDTH_PERCENT:.1f}% (observed ±{(high - low) / 2:.3f}%).'
        )
    elif instructions['significant']:
        print('SIGNIFICANT: the instructions/request interval excludes zero.')
    else:
        print(
            'No significant difference: the instructions/request interval spans zero.'
        )
    print('Lower instructions, cycles, branch misses, and cache misses are better.')
    print(f'Record written to {output}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
