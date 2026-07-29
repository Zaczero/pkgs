"""Cross-server benchmark suite: runs every scenario against every server and plots it.

Servers and load generators run unpinned, exactly as a deployment runs them.
Trials are rotation-balanced cold starts (3-9 per scenario). Sampling stops
when the published claim is resolved — the leader is ahead of the runner-up
and the leader's own interval is within the target half-width — or when the
scenario's 240 s ceiling (suite 7200 s) is hit. There is no separate headroom
phase.

    uv run python bench/bench.py            # stage a run under bench/results/runs/
    uv run python bench/bench.py --publish  # also replace the canonical plots
"""

from __future__ import annotations

import argparse
import importlib.metadata
import platform
import secrets
import shutil
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import matplotlib as mpl

mpl.use('Agg')
import matplotlib.pyplot as plt

try:
    from bench._core import (
        RESPONSE_CONTRACTS,
        RESULTS_DIRECTORY,
        BenchmarkError,
        Metrics,
        Scenario,
        check_response,
        comparison_is_settled,
        duration_seconds,
        ensure_static_file_payload,
        leader_is_separated,
        relative_half_width,
        rotations,
        run_load,
        running_server,
        write_json,
    )
except ModuleNotFoundError:  # Direct ``python bench/bench.py`` execution.
    from _core import (  # type: ignore[import-not-found, no-redef]
        RESPONSE_CONTRACTS,
        RESULTS_DIRECTORY,
        BenchmarkError,
        Metrics,
        Scenario,
        check_response,
        comparison_is_settled,
        duration_seconds,
        ensure_static_file_payload,
        leader_is_separated,
        relative_half_width,
        rotations,
        run_load,
        running_server,
        write_json,
    )

if TYPE_CHECKING:
    from collections.abc import Sequence

DURATION = '10s'
WARMUP_DURATION = '2s'
CONCURRENCY = 100
STREAMING_CONCURRENCY = 1000
#: Rotation-balanced cold starts per server. Sampling stops when the leader is
#: separated and its interval is within TARGET_HALF_WIDTH, so a quiet host
#: finishes in MIN_TRIALS and a noisy one spends its budget where it is needed.
MIN_TRIALS = 3
MAX_TRIALS = 9
TARGET_HALF_WIDTH = 0.03
#: Hard wall-clock ceiling per scenario and for the whole suite. Trial duration
#: is never shortened to fit — the suite reports what it measured and says which
#: scenarios stopped on time rather than on precision. The suite ceiling has to
#: cover every scenario spending its own, or a full run dies partway with
#: nothing published; `main` refuses a combination that cannot fit.
SCENARIO_BUDGET_SECONDS = 240.0
SUITE_BUDGET_SECONDS = 7200.0
SEED = 20_260_726

RUNS_DIRECTORY = RESULTS_DIRECTORY / 'runs'
CANONICAL_RAW_DIRECTORY = RESULTS_DIRECTORY / 'raw'
CANONICAL_PLOT_DIRECTORY = RESULTS_DIRECTORY / 'plots'

# Every server runs with access logging ENABLED — that is how these servers are
# deployed, and request-log construction is part of the work a real deployment
# does. h2corn and uvicorn log by default; hypercorn and gunicorn need an
# explicit flag. Server output goes to /dev/null (see _core.running_server), so
# no server pays for a log sink the others avoid.
SERVERS: dict[str, list[str]] = {
    'h2corn': ['h2corn', 'bench.bench_app:app', '--backlog', '2048'],
    'uvicorn': [
        'uvicorn',
        'bench.bench_app:app',
        '--loop',
        'asyncio',
        '--http',
        'h11',
        '--no-proxy-headers',
        '--backlog',
        '2048',
    ],
    'hypercorn': [
        'hypercorn',
        'bench.bench_app:app',
        '-k',
        'uvloop',
        '--access-logfile',
        '-',
        '--backlog',
        '2048',
    ],
    'gunicorn': [
        'gunicorn',
        'bench.bench_app:app',
        '-k',
        'asgi',
        '--asgi-loop',
        'uvloop',
        '--http-parser',
        'python',
        '--no-control-socket',
        '--access-logfile',
        '-',
        '--backlog',
        '2048',
    ],
}

SERVER_COLORS = {
    'h2corn': '#4477AA',
    'gunicorn': '#228833',
    'hypercorn': '#CC3311',
    'uvicorn': '#B39B00',
}
SERVER_MARKERS = {'h2corn': 'o', 'gunicorn': 's', 'hypercorn': 'D', 'uvicorn': '^'}
SERVER_LINESTYLES = {'h2corn': '-', 'gunicorn': ':', 'hypercorn': '-.', 'uvicorn': '--'}
FALLBACK_COLOR = '#4C4C4C'


def benchmark_scenarios() -> tuple[Scenario, ...]:
    return (
        Scenario('HTTP/1 GET (1 Worker)', 1, 'h1'),
        Scenario('HTTP/1 GET (4 Workers)', 4, 'h1'),
        Scenario('HTTP/1 GET over UDS (1 Worker)', 1, 'h1_uds'),
        Scenario('HTTP/1 GET over UDS (4 Workers)', 4, 'h1_uds'),
        Scenario('HTTP/2 GET (1 Worker)', 1, 'h2'),
        Scenario('HTTP/2 GET (4 Workers)', 4, 'h2'),
        Scenario(
            'HTTP/2 GET multiplexed (1 Worker)',
            1,
            'h2',
            concurrency=10,
            http2_parallelism=10,
        ),
        Scenario('HTTP/1 Static file (1 Worker)', 1, 'h1_file'),
        Scenario('HTTP/1 Static file (4 Workers)', 4, 'h1_file'),
        Scenario('HTTP/2 Static file (1 Worker)', 1, 'h2_file'),
        Scenario('HTTP/2 Static file (4 Workers)', 4, 'h2_file'),
        Scenario('HTTP/1 Portable streaming download (1 Worker)', 1, 'h1_download'),
        Scenario('HTTP/1 Portable streaming download (4 Workers)', 4, 'h1_download'),
        Scenario('HTTP/2 Portable streaming download (1 Worker)', 1, 'h2_download'),
        Scenario('HTTP/2 Portable streaming download (4 Workers)', 4, 'h2_download'),
        Scenario(
            'HTTP/1 Streaming POST (1 Worker)', 1, 'h1_stream', STREAMING_CONCURRENCY
        ),
        Scenario(
            'HTTP/1 Streaming POST (4 Workers)', 4, 'h1_stream', STREAMING_CONCURRENCY
        ),
        Scenario(
            'HTTP/2 Streaming POST (1 Worker)', 1, 'h2_stream', STREAMING_CONCURRENCY
        ),
        Scenario(
            'HTTP/2 Streaming POST (4 Workers)', 4, 'h2_stream', STREAMING_CONCURRENCY
        ),
        Scenario('HTTP/1 WebSocket (1 Worker)', 1, 'ws'),
        Scenario('HTTP/1 WebSocket (4 Workers)', 4, 'ws'),
    )


def eligible_servers(scenario: Scenario, servers: Sequence[str]) -> list[str]:
    """Only h2corn and hypercorn speak HTTP/2 directly."""
    if scenario.contract.protocol == '2':
        return [name for name in servers if name not in {'uvicorn', 'gunicorn'}]
    return list(servers)


def server_command(name: str, scenario: Scenario) -> list[str]:
    command = SERVERS[name].copy()
    socket_path = scenario.socket_path
    if name == 'uvicorn':
        command += ['--ws', 'websockets' if scenario.type == 'ws' else 'none']
        command += (
            ['--uds', str(socket_path)]
            if socket_path is not None
            else ['--host', '127.0.0.1', '--port', '8000']
        )
    else:
        target = f'unix:{socket_path}' if socket_path is not None else '127.0.0.1:8000'
        command += ['-b', target]
    # uvicorn and hypercorn only spell it long; h2corn and gunicorn take both.
    workers_flag = '--workers' if name in {'hypercorn', 'uvicorn'} else '-w'
    command += [workers_flag, str(scenario.workers)]
    return command


def package_versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for package in SERVERS:
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def system_summary() -> str:
    kernel = f'{platform.system()} {platform.release()}'.strip()
    summary = f'Python {platform.python_version()} | {kernel} | {platform.machine()}'
    model = None
    cpuinfo = Path('/proc/cpuinfo')
    if cpuinfo.exists():
        for line in cpuinfo.read_text(errors='ignore').splitlines():
            if line.startswith('model name'):
                model = line.partition(':')[2].strip()
                break
    elif shutil.which('sysctl'):
        result = subprocess.run(
            ['sysctl', '-n', 'machdep.cpu.brand_string'],
            capture_output=True,
            text=True,
            check=False,
        )
        model = result.stdout.strip() or None
    if model is None:
        raise BenchmarkError('failed to determine the CPU model for plot metadata')
    return f'{summary}\nCPU: {model}'


def git_head() -> str | None:
    result = subprocess.run(
        ['git', 'rev-parse', 'HEAD'], capture_output=True, text=True, check=False
    )
    return result.stdout.strip() or None


def aggregate(samples: Sequence[Metrics]) -> dict[str, Any]:
    rates = [sample['rps'] for sample in samples]
    percentile_names = sorted(
        {name for sample in samples for name in sample['latency_percentiles']},
        key=lambda name: float(name.removeprefix('p')),
    )
    return {
        'rps': statistics.median(rates),
        'rps_samples': rates,
        'rps_range': [min(rates), max(rates)],
        'relative_half_width': relative_half_width(rates, SEED),
        'latency_percentiles': {
            name: statistics.median([
                sample['latency_percentiles'][name]
                for sample in samples
                if name in sample['latency_percentiles']
            ])
            for name in percentile_names
        },
    }


def measure_cell(name: str, scenario: Scenario, duration: str, warmup: str) -> Metrics:
    """One cold-start trial: start, prove correctness, warm up, measure, prove again."""
    concurrency = scenario.concurrency or CONCURRENCY
    command = server_command(name, scenario)
    with running_server(
        command, workers=scenario.workers, socket_path=scenario.socket_path
    ):
        check_response(scenario)
        run_load(scenario, duration=warmup, concurrency=concurrency)
        metrics = run_load(scenario, duration=duration, concurrency=concurrency)
        check_response(scenario)
    return metrics


def run_scenario(
    scenario: Scenario,
    servers: Sequence[str],
    *,
    duration: str,
    warmup: str,
    max_trials: int,
    deadline: float,
) -> dict[str, Any]:
    names = eligible_servers(scenario, servers)
    samples: dict[str, list[Metrics]] = {name: [] for name in names}
    excluded: dict[str, str] = {}
    stopped = 'max-trials'
    trials = 0
    for trial, order in enumerate(rotations(names, max_trials, SEED), start=1):
        for name in order:
            if name in excluded:
                continue
            try:
                samples[name].append(measure_cell(name, scenario, duration, warmup))
            except BenchmarkError as error:
                # h2corn failing is our bug and must stop the run. A competitor
                # that cannot serve this workload is a fact about that server:
                # drop it from this scenario, say so loudly, and keep the rest
                # of the comparison rather than losing the whole suite.
                if name == 'h2corn':
                    raise
                excluded[name] = str(error)
                samples.pop(name)
                print(f'  EXCLUDED {name} from {scenario.name}: {error}', flush=True)
        remaining = [name for name in names if name in samples]
        if not remaining:
            raise BenchmarkError(f'every server failed {scenario.name}')
        trials = trial
        rates = [[sample['rps'] for sample in samples[name]] for name in remaining]
        leader = relative_half_width(max(rates, key=statistics.median), SEED)
        medians = ', '.join(
            f'{name} {statistics.median([s["rps"] for s in samples[name]]):,.0f}'
            for name in remaining
        )
        print(
            f'  after trial {trial}: median {medians} (leader CI ±{leader:.1%})',
            flush=True,
        )
        if trial >= MIN_TRIALS and comparison_is_settled(
            rates, SEED, TARGET_HALF_WIDTH
        ):
            stopped = 'resolved'
            break
        if time.monotonic() > deadline:
            stopped = 'time-budget'
            break
    final_rates = [
        [sample['rps'] for sample in samples[name]] for name in names if name in samples
    ]
    leader_half_width = relative_half_width(
        max(final_rates, key=statistics.median), SEED
    )
    return {
        'leader_separated': leader_is_separated(final_rates, SEED),
        'leader_half_width': leader_half_width,
        'scenario': scenario.name,
        'type': scenario.type,
        'workers': scenario.workers,
        'trials': trials,
        'stopped': stopped,
        'excluded': excluded,
        'aggregate': {name: aggregate(values) for name, values in samples.items()},
    }


def alt_text(title: str, results: dict[str, Any], unit: str) -> str:
    ordered = sorted(results.items(), key=lambda item: item[1]['rps'], reverse=True)
    parts = [
        f'{name} {result["rps"]:,.0f} {unit}'
        + (
            f' p99 {result["latency_percentiles"]["p99"] * 1000:.1f}ms'
            if 'p99' in result['latency_percentiles']
            else ''
        )
        for name, result in ordered
    ]
    return f'{title}. ' + ', '.join(parts) + '.'


def plot_results(
    results: dict[str, Any],
    title: str,
    path: Path,
    *,
    summary: str,
    websocket: bool = False,
    excluded: dict[str, str] | None = None,
    leader_half_width: float | None = None,
) -> None:
    figure, axes = plt.subplots(1, 2, figsize=(15, 6))
    figure.suptitle(title, fontsize=16)

    names = list(results)
    versions = package_versions()
    labels = [f'{name}\n{versions.get(name)}' for name in names]
    rates = [results[name]['rps'] for name in names]
    colors = [SERVER_COLORS.get(name, FALLBACK_COLOR) for name in names]

    throughput = axes[0]
    bars = throughput.bar(
        labels, rates, color=colors, edgecolor='#222222', linewidth=1.0
    )
    for bar, name in zip(bars, names, strict=True):
        low, high = results[name]['rps_range']
        median = results[name]['rps']
        if high > low:
            throughput.errorbar(
                bar.get_x() + bar.get_width() / 2,
                median,
                yerr=[[median - low], [high - median]],
                fmt='none',
                color='#222222',
                capsize=4,
            )
    unit = 'sessions/s' if websocket else 'RPS'
    throughput.set_title(
        ('WebSocket Sessions Per Second' if websocket else 'Requests Per Second')
        + ' (Higher is better)'
    )
    throughput.set_ylabel(unit)
    for index, value in enumerate(rates):
        throughput.text(index, value, f'{value:,.0f}', ha='center', va='bottom')

    latency = axes[1]
    plotted = False
    for name, label in zip(names, labels, strict=True):
        percentiles = results[name]['latency_percentiles']
        if not percentiles:
            continue
        points = sorted(
            percentiles.items(), key=lambda item: float(item[0].removeprefix('p'))
        )
        latency.plot(
            [float(key.removeprefix('p')) for key, _ in points],
            [value * 1000 for _, value in points],
            color=SERVER_COLORS.get(name, FALLBACK_COLOR),
            linestyle=SERVER_LINESTYLES.get(name, '-'),
            marker=SERVER_MARKERS.get(name, 'o'),
            label=label,
            linewidth=2,
            markersize=6,
            markeredgecolor='#222222',
            markeredgewidth=1.0,
        )
        plotted = True
    if plotted:
        latency.set_title(
            ('Session duration' if websocket else 'Saturation latency')
            + ' Distribution (Lower is better)'
        )
        latency.set_xlabel('Percentile')
        latency.set_ylabel('Latency (ms)')
        latency.legend()
        latency.grid(True, linestyle='--', alpha=0.7)
    else:
        latency.set_title('Latency data unavailable')

    footer = summary
    if leader_half_width is not None and leader_half_width > TARGET_HALF_WIDTH:
        # The ordering is resolved, the bar height is not: say so rather than
        # let a crisply drawn bar imply a precision the run did not reach.
        footer += (
            f'\nLeading bar height resolved only to ±{leader_half_width:.0%} '
            f'(target ±{TARGET_HALF_WIDTH:.0%}); whiskers show the observed range'
        )
    if excluded:
        # A missing bar has to explain itself, or the plot silently implies the
        # server was never in the comparison.
        footer += '\nExcluded: ' + '; '.join(
            f'{name} ({reason})' for name, reason in sorted(excluded.items())
        )
    plt.figtext(
        0.02,
        0.02,
        footer,
        fontsize=9,
        verticalalignment='bottom',
        bbox={'boxstyle': 'round', 'facecolor': 'whitesmoke', 'alpha': 0.75},
    )
    plt.tight_layout(rect=(0.0, 0.05, 1.0, 0.95))
    plt.savefig(path, format='svg')
    plt.close()
    path.write_text(
        '\n'.join(line.rstrip() for line in path.read_text().splitlines()) + '\n'
    )
    print(f'Saved plot to {path}')
    print(alt_text(title, results, unit))


def publish(run_directory: Path) -> None:
    for kind, suffix, canonical in (
        ('raw', '.json', CANONICAL_RAW_DIRECTORY),
        ('plots', '.svg', CANONICAL_PLOT_DIRECTORY),
    ):
        canonical.mkdir(parents=True, exist_ok=True)
        for stale in canonical.glob(f'*{suffix}'):
            stale.unlink()
        for artifact in sorted((run_directory / kind).glob(f'*{suffix}')):
            shutil.copy2(artifact, canonical / artifact.name)
    print(
        f'Published {run_directory} to {CANONICAL_RAW_DIRECTORY} and {CANONICAL_PLOT_DIRECTORY}'
    )


def run_suite(
    *,
    servers: Sequence[str],
    scenarios: Sequence[Scenario],
    duration: str,
    warmup: str,
    max_trials: int,
    scenario_budget: float,
    suite_budget: float,
    should_publish: bool,
) -> Path:
    ensure_static_file_payload()
    for tool in {'oha'} | ({'k6'} if any(s.type == 'ws' for s in scenarios) else set()):
        if shutil.which(tool) is None:
            raise BenchmarkError(f'{tool} is required but not installed')

    run_id = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ') + f'-{secrets.token_hex(3)}'
    run_directory = RUNS_DIRECTORY / run_id
    (run_directory / 'raw').mkdir(parents=True)
    (run_directory / 'plots').mkdir()

    summary = system_summary()
    write_json(
        run_directory / 'identity.json',
        {
            'run_id': run_id,
            'git_head': git_head(),
            'versions': package_versions(),
            'system': summary,
            'duration': duration,
            'warmup': warmup,
            'max_trials': max_trials,
            'target_half_width': TARGET_HALF_WIDTH,
            'commands': {
                f'{name}:{scenario.type}:w{scenario.workers}': server_command(
                    name, scenario
                )
                for scenario in scenarios
                for name in eligible_servers(scenario, servers)
            },
        },
    )

    suite_deadline = time.monotonic() + suite_budget
    records = []
    for scenario in scenarios:
        print(f'\n=== {scenario.name} ===', flush=True)
        record = run_scenario(
            scenario,
            servers,
            duration=duration,
            warmup=warmup,
            max_trials=max_trials,
            deadline=min(time.monotonic() + scenario_budget, suite_deadline),
        )
        records.append(record)
        write_json(run_directory / 'raw' / f'{scenario.slug()}.json', record)
        plot_results(
            record['aggregate'],
            scenario.name,
            run_directory / 'plots' / f'{scenario.slug()}.svg',
            summary=summary,
            websocket=scenario.type == 'ws',
            excluded=record['excluded'],
            leader_half_width=record['leader_half_width'],
        )
        if time.monotonic() > suite_deadline:
            raise BenchmarkError(
                f'suite budget exhausted after {scenario.name}; '
                'raise --suite-budget or select fewer scenarios'
            )

    imprecise = [
        f'{r["scenario"]} (leader ±{r["leader_half_width"]:.0%})'
        for r in records
        if r['leader_half_width'] > TARGET_HALF_WIDTH
    ]
    if imprecise:
        print(f'\nBar heights not resolved to ±{TARGET_HALF_WIDTH:.0%}:')
        for entry in imprecise:
            print(f'  {entry}')
    if should_publish:
        # The ordering is the claim a bar chart cannot walk back, and it is the
        # one that survives a noisy host; a wide leader interval makes the bar
        # height approximate, which the whisker and the footer both say. An
        # unestablished ordering would make the chart simply wrong.
        undecided = [r['scenario'] for r in records if not r['leader_separated']]
        if undecided:
            raise BenchmarkError(
                f'refusing to publish {len(undecided)} scenario(s) whose winner '
                f'is not established: {", ".join(undecided)}; '
                'raise --scenario-budget or --max-trials and re-run'
            )
        publish(run_directory)
    return run_directory


def main() -> int:
    parser = argparse.ArgumentParser(
        description='h2corn cross-server benchmark suite',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--servers', nargs='+', choices=list(SERVERS))
    parser.add_argument('--types', nargs='+', choices=list(RESPONSE_CONTRACTS))
    parser.add_argument('--duration', default=DURATION)
    parser.add_argument('--warmup-duration', default=WARMUP_DURATION)
    parser.add_argument('--max-trials', type=int, default=MAX_TRIALS)
    parser.add_argument(
        '--scenario-budget', type=float, default=SCENARIO_BUDGET_SECONDS
    )
    parser.add_argument('--suite-budget', type=float, default=SUITE_BUDGET_SECONDS)
    parser.add_argument(
        '--publish',
        action='store_true',
        help='replace the canonical plots and raw records with this run',
    )
    args = parser.parse_args()

    if args.max_trials < MIN_TRIALS:
        parser.error(f'--max-trials must be at least {MIN_TRIALS}')
    duration_seconds(args.duration)
    duration_seconds(args.warmup_duration)

    scenarios = [
        scenario
        for scenario in benchmark_scenarios()
        if not args.types or scenario.type in args.types
    ]
    servers = args.servers or list(SERVERS)
    if args.publish and (args.servers or args.types):
        parser.error('--publish requires the complete server and scenario suite')
    if not scenarios:
        parser.error('scenario selection is empty')
    required = len(scenarios) * args.scenario_budget
    if args.suite_budget < required:
        parser.error(
            f'--suite-budget {args.suite_budget:.0f}s cannot cover {len(scenarios)} '
            f'scenarios at --scenario-budget {args.scenario_budget:.0f}s '
            f'(needs {required:.0f}s)'
        )

    try:
        run_suite(
            servers=servers,
            scenarios=scenarios,
            duration=args.duration,
            warmup=args.warmup_duration,
            max_trials=args.max_trials,
            scenario_budget=args.scenario_budget,
            suite_budget=args.suite_budget,
            should_publish=args.publish,
        )
    except BenchmarkError as error:
        print(f'benchmark failed: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
