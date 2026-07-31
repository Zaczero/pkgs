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
import json
import os
import platform
import secrets
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

import matplotlib as mpl
from matplotlib import colors

mpl.use('Agg')
import matplotlib.pyplot as plt

try:
    from bench._core import (
        PSS_SAMPLE_INTERVAL_SECONDS,
        RESPONSE_CONTRACTS,
        RESULTS_DIRECTORY,
        BenchmarkError,
        MeasuredMetrics,
        Scenario,
        check_response,
        comparison_is_settled,
        duration_seconds,
        ensure_large_upload_payload,
        ensure_static_file_payload,
        leader_is_separated,
        measure_peak_memory,
        relative_half_width,
        rotations,
        run_load,
        running_server,
        write_json,
    )
except ModuleNotFoundError:  # Direct ``python bench/bench.py`` execution.
    from _core import (  # type: ignore[import-not-found, no-redef]
        PSS_SAMPLE_INTERVAL_SECONDS,
        RESPONSE_CONTRACTS,
        RESULTS_DIRECTORY,
        BenchmarkError,
        MeasuredMetrics,
        Scenario,
        check_response,
        comparison_is_settled,
        duration_seconds,
        ensure_large_upload_payload,
        ensure_static_file_payload,
        leader_is_separated,
        measure_peak_memory,
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

NETEM_DELAY_MS = 25.0
NETEM_RTT_MS = NETEM_DELAY_MS * 2
NETEM_RATE = '1gbit'
NETEM_RTT_TOLERANCE_MS = 15.0
NETEM_RTT_SAMPLES = 10
NETEM_NAMESPACE_ENV = 'H2CORN_BENCH_NETEM_NAMESPACE'
TUNING_MATERIALITY_THRESHOLD = 0.10

CANONICAL_RAW_DIRECTORY = RESULTS_DIRECTORY / 'raw'
CANONICAL_PLOT_DIRECTORY = RESULTS_DIRECTORY / 'plots'

# Every server runs with access logging ENABLED — that is how these servers are
# deployed, and request-log construction is part of the work a real deployment
# does. h2corn and uvicorn log by default; hypercorn and gunicorn need an
# explicit flag. Server output goes to /dev/null (see _core.running_server), so
# no server pays for a log sink the others avoid.
SERVERS: dict[str, list[str]] = {
    'h2corn': [
        'h2corn',
        'bench.bench_app:app',
        '--backlog',
        '2048',
        '--loop',
        'uvloop',
        '--timeout-keep-alive',
        '120',
        '--server-header',
        'off',
        '--date-header',
        '--no-proxy-headers',
        '--max-concurrent-streams',
        '256',
        '--h2-max-header-list-size',
        '1048576',
        '--h2-max-inbound-frame-size',
        '65536',
    ],
    'uvicorn': [
        'uvicorn',
        'bench.bench_app:app',
        '--loop',
        'asyncio',
        '--http',
        'h11',
        '--no-proxy-headers',
        '--timeout-keep-alive',
        '120',
        '--no-server-header',
        '--date-header',
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
        '--keep-alive',
        '120',
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
MEMORY_LABEL_LIGHT = '#FFFFFF'
MEMORY_LABEL_DARK = '#111111'


@dataclass(frozen=True, slots=True)
class NetworkProfile:
    name: str
    description: str
    target_rtt_ms: float | None = None


class RTT50Netem(TypedDict):
    """Verified shape of the isolated netem profile written to each run."""

    name: str
    description: str
    status: str
    target_rtt_ms: float
    measured_rtt_ms: float
    one_way_delay_ms: float
    rate: str
    qdisc: str


LOOPBACK_PROFILE = NetworkProfile('loopback', 'loopback')
RTT50_PROFILE = NetworkProfile(
    'rtt50', f'{NETEM_RTT_MS:.0f} ms RTT, {NETEM_RATE} netem', NETEM_RTT_MS
)


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


def rtt50_scenarios() -> tuple[Scenario, ...]:
    """WAN-shaped cells where bytes in flight or HTTP/2 windows matter.

    Small requests are intentionally absent: at 50 ms RTT their rate is a
    concurrency/RTT property of the generator, not a server comparison. Four
    workers are the deployment headline shape, except for the existing HTTP/2
    multiplexing cell whose ten streams per connection are the point of the
    workload.
    """
    return (
        Scenario(
            'HTTP/1 Portable streaming download (4 Workers, 50 ms RTT)',
            4,
            'h1_download',
        ),
        Scenario(
            'HTTP/2 Portable streaming download (4 Workers, 50 ms RTT)',
            4,
            'h2_download',
        ),
        Scenario(
            'HTTP/1 Streaming POST (4 Workers, 50 ms RTT)',
            4,
            'h1_stream',
            STREAMING_CONCURRENCY,
        ),
        Scenario(
            'HTTP/2 Streaming POST (4 Workers, 50 ms RTT)',
            4,
            'h2_stream',
            STREAMING_CONCURRENCY,
        ),
        Scenario('HTTP/2 8 MiB upload (4 Workers, 50 ms RTT)', 4, 'h2_upload', 8),
        Scenario(
            'HTTP/2 GET multiplexed (1 Worker, 50 ms RTT)',
            1,
            'h2',
            concurrency=10,
            http2_parallelism=10,
        ),
    )


def _run_checked(command: list[str]) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as error:
        raise BenchmarkError(f'failed to run {command[0]!r}: {error}') from error
    if result.returncode:
        detail = (
            result.stderr.strip() or result.stdout.strip() or 'no diagnostic output'
        )
        raise BenchmarkError(f'{" ".join(command)} failed: {detail}')
    return result.stdout


def loopback_rtt_ms() -> float:
    """Measure TCP round trips through loopback without a benchmark server."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('127.0.0.1', 0))
    listener.listen(1)
    address = listener.getsockname()
    failures: list[BaseException] = []

    def echo() -> None:
        try:
            with listener.accept()[0] as connection:
                connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                for _ in range(NETEM_RTT_SAMPLES):
                    payload = connection.recv(1)
                    if not payload:
                        raise OSError('RTT client closed before completing probes')
                    connection.sendall(payload)
        except BaseException as error:  # pragma: no cover - reported to the caller
            failures.append(error)

    thread = threading.Thread(target=echo, name='h2corn-bench-rtt', daemon=True)
    thread.start()
    samples = []
    try:
        with socket.create_connection(address, timeout=5) as connection:
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            for _ in range(NETEM_RTT_SAMPLES):
                started = time.perf_counter()
                connection.sendall(b'x')
                if connection.recv(1) != b'x':
                    raise BenchmarkError('RTT echo returned the wrong payload')
                samples.append((time.perf_counter() - started) * 1000)
    finally:
        listener.close()
        thread.join(timeout=5)
    if thread.is_alive():
        raise BenchmarkError('RTT echo server did not stop')
    if failures:
        raise BenchmarkError(f'RTT echo failed: {failures[0]}')
    return statistics.median(samples)


def configure_rtt50_profile() -> RTT50Netem:
    """Install and prove the isolated 50 ms RTT profile before any cell runs."""
    for tool in ('ip', 'tc'):
        if shutil.which(tool) is None:
            raise BenchmarkError(f'{tool} is unavailable')
    _run_checked(['ip', 'link', 'set', 'lo', 'up'])
    _run_checked([
        'tc',
        'qdisc',
        'replace',
        'dev',
        'lo',
        'root',
        'netem',
        'delay',
        f'{NETEM_DELAY_MS:g}ms',
        'rate',
        NETEM_RATE,
    ])
    qdisc = _run_checked(['tc', 'qdisc', 'show', 'dev', 'lo']).strip()
    if 'netem' not in qdisc:
        raise BenchmarkError(f'netem did not attach to loopback: {qdisc or "no qdisc"}')
    measured_rtt = loopback_rtt_ms()
    if abs(measured_rtt - NETEM_RTT_MS) > NETEM_RTT_TOLERANCE_MS:
        raise BenchmarkError(
            f'netem RTT was {measured_rtt:.1f} ms, expected '
            f'{NETEM_RTT_MS:.0f}±{NETEM_RTT_TOLERANCE_MS:.0f} ms'
        )
    print(
        f'  verified {measured_rtt:.1f} ms loopback RTT '
        f'(target {NETEM_RTT_MS:.0f} ms)',
        flush=True,
    )
    return {
        'name': RTT50_PROFILE.name,
        'description': RTT50_PROFILE.description,
        'status': 'ready',
        'target_rtt_ms': NETEM_RTT_MS,
        'measured_rtt_ms': measured_rtt,
        'one_way_delay_ms': NETEM_DELAY_MS,
        'rate': NETEM_RATE,
        'qdisc': qdisc,
    }


def eligible_servers(scenario: Scenario, servers: Sequence[str]) -> list[str]:
    """Only h2corn and hypercorn speak HTTP/2 directly."""
    if scenario.contract.protocol == '2':
        return [name for name in servers if name not in {'uvicorn', 'gunicorn'}]
    return list(servers)


def write_hypercorn_config(path: Path) -> None:
    """Write the exposed Hypercorn counterparts to h2corn's chosen settings."""
    path.write_text(
        '# Generated by bench.py; retained with the run record.\n'
        'h2_max_concurrent_streams = 256\n'
        'h2_max_header_list_size = 1048576\n'
        'h2_max_inbound_frame_size = 65536\n'
        'keep_alive_timeout = 120\n'
        'include_server_header = false\n'
        'include_date_header = true\n'
    )


def server_command(
    name: str, scenario: Scenario, *, hypercorn_config: Path
) -> list[str]:
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
    if name == 'hypercorn':
        command += ['--config', str(hypercorn_config)]
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


def aligned_setting(name: str, value: object, default: object) -> dict[str, object]:
    return {'setting': name, 'value': value, 'default': default}


def no_equivalent(
    h2corn_setting: str, h2corn_value: object, reason: str
) -> dict[str, object]:
    return {
        'h2corn_setting': h2corn_setting,
        'h2corn_value': h2corn_value,
        'reason': reason,
    }


def server_profiles(scenario: Scenario) -> dict[str, dict[str, object]]:
    """The aligned knobs and deliberately unmatched settings for one cell.

    Values are data, not prose: a reader can see the value passed to every
    competitor alongside its upstream default without making a plot carry
    configuration archaeology.
    """
    workers = scenario.workers
    h2corn_http2 = [
        aligned_setting('max_concurrent_streams', 256, 256),
        aligned_setting('h2_max_header_list_size', 1_048_576, 1_048_576),
        aligned_setting('h2_max_inbound_frame_size', 65_536, 65_536),
    ]
    return {
        'h2corn': {
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 1_024),
                aligned_setting('loop', 'uvloop', 'auto'),
                aligned_setting('timeout_keep_alive', 120, 120),
                aligned_setting('server_header', 'off', 'off'),
                aligned_setting('date_header', True, True),
                aligned_setting('proxy_headers', False, True),
                *h2corn_http2,
            ],
            'no_equivalent': [],
        },
        'hypercorn': {
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 100),
                aligned_setting('keep_alive_timeout', 120, 5),
                aligned_setting('include_server_header', False, True),
                aligned_setting('include_date_header', True, True),
                aligned_setting('h2_max_concurrent_streams', 256, 100),
                aligned_setting('h2_max_header_list_size', 1_048_576, 65_536),
                aligned_setting('h2_max_inbound_frame_size', 65_536, 16_384),
            ],
            'no_equivalent': [
                no_equivalent(
                    'proxy_headers',
                    False,
                    'Hypercorn exposes no server-level proxy-header parsing setting.',
                ),
                no_equivalent(
                    'h2_initial_stream_window_size',
                    8_388_608,
                    "Hypercorn exposes no HTTP/2 flow-control window; it uses "
                    "h2's 65,535-byte default.",
                ),
                no_equivalent(
                    'h2_initial_connection_window_size',
                    8_388_608,
                    'Hypercorn exposes no HTTP/2 connection receive-window setting.',
                ),
                no_equivalent(
                    'h2_max_header_block_size',
                    1_048_576,
                    'Hypercorn exposes no compressed HTTP/2 header-block limit.',
                ),
            ],
        },
        'uvicorn': {
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 2_048),
                aligned_setting('timeout_keep_alive', 120, 5),
                aligned_setting('server_header', False, True),
                aligned_setting('date_header', True, True),
                aligned_setting('proxy_headers', False, True),
            ],
            'profile_choice': [
                aligned_setting(
                    'loop',
                    'asyncio',
                    'auto',
                )
            ],
            'no_equivalent': [
                no_equivalent(
                    'http2',
                    'h2corn direct HTTP/2',
                    'Uvicorn has no direct HTTP/2 server mode.',
                ),
            ],
        },
        'gunicorn': {
            'aligned': [
                aligned_setting('workers', workers, 1),
                aligned_setting('backlog', 2_048, 2_048),
                aligned_setting('keep_alive', 120, 2),
            ],
            'profile_choice': [aligned_setting('asgi_loop', 'uvloop', 'auto')],
            'no_equivalent': [
                no_equivalent(
                    'server_header',
                    'off',
                    'Gunicorn exposes no response Server-header setting.',
                ),
                no_equivalent(
                    'date_header',
                    True,
                    'Gunicorn exposes no response Date-header setting.',
                ),
                no_equivalent(
                    'proxy_headers',
                    False,
                    'Gunicorn has no ASGI proxy-header parsing counterpart.',
                ),
                no_equivalent(
                    'http2',
                    'h2corn direct HTTP/2',
                    'Gunicorn ASGI has no direct HTTP/2 server mode.',
                ),
            ],
        },
    }


def aggregate(samples: Sequence[MeasuredMetrics]) -> dict[str, Any]:
    rates = [sample['rps'] for sample in samples]
    peak_memory = [sample['peak_memory_bytes'] for sample in samples]
    percentile_names = sorted(
        {name for sample in samples for name in sample['latency_percentiles']},
        key=lambda name: float(name.removeprefix('p')),
    )
    return {
        'rps': statistics.median(rates),
        'rps_samples': rates,
        'rps_range': [min(rates), max(rates)],
        'relative_half_width': relative_half_width(rates, SEED),
        # Throughput is the median trial, but deployment capacity must cover
        # the largest sampled PSS high-water mark from every measured trial.
        'peak_memory_bytes': max(peak_memory),
        'latency_percentiles': {
            name: statistics.median([
                sample['latency_percentiles'][name]
                for sample in samples
                if name in sample['latency_percentiles']
            ])
            for name in percentile_names
        },
    }


def measure_cell(
    name: str,
    scenario: Scenario,
    duration: str,
    warmup: str,
    *,
    hypercorn_config: Path,
) -> MeasuredMetrics:
    """One cold-start trial: start, prove correctness, warm up, measure, prove again."""
    concurrency = scenario.concurrency or CONCURRENCY
    command = server_command(name, scenario, hypercorn_config=hypercorn_config)
    with running_server(
        command, workers=scenario.workers, socket_path=scenario.socket_path
    ) as server:
        check_response(scenario)
        run_load(scenario, duration=warmup, concurrency=concurrency)
        metrics, peak_memory_bytes = measure_peak_memory(
            server,
            lambda: run_load(scenario, duration=duration, concurrency=concurrency),
        )
        check_response(scenario)
    return {**metrics, 'peak_memory_bytes': peak_memory_bytes}


def run_scenario(
    scenario: Scenario,
    servers: Sequence[str],
    *,
    duration: str,
    warmup: str,
    max_trials: int,
    deadline: float,
    hypercorn_config: Path,
) -> dict[str, Any]:
    names = eligible_servers(scenario, servers)
    samples: dict[str, list[MeasuredMetrics]] = {name: [] for name in names}
    excluded: dict[str, str] = {}
    stopped = 'max-trials'
    trials = 0
    for trial, order in enumerate(rotations(names, max_trials, SEED), start=1):
        for name in order:
            if name in excluded:
                continue
            try:
                samples[name].append(
                    measure_cell(
                        name,
                        scenario,
                        duration,
                        warmup,
                        hypercorn_config=hypercorn_config,
                    )
                )
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
        'server_profiles': server_profiles(scenario),
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
        + (
            f' peak memory (PSS) {result["peak_memory_bytes"] / 1e6:.1f} MB'
            if 'peak_memory_bytes' in result
            else ''
        )
        for name, result in ordered
    ]
    return f'{title}. ' + ', '.join(parts) + '.'


def relative_luminance(color: str) -> float:
    """Return the WCAG relative luminance for an sRGB colour."""
    red, green, blue, _ = colors.to_rgba(color)

    def linear(value: float) -> float:
        return value / 12.92 if value <= 0.04045 else ((value + 0.055) / 1.055) ** 2.4

    return 0.2126 * linear(red) + 0.7152 * linear(green) + 0.0722 * linear(blue)


def contrast_ratio(first: str, second: str) -> float:
    """Return the WCAG contrast ratio between two opaque sRGB colours."""
    first_luminance = relative_luminance(first)
    second_luminance = relative_luminance(second)
    light, dark = max(first_luminance, second_luminance), min(
        first_luminance, second_luminance
    )
    return (light + 0.05) / (dark + 0.05)


def bar_label_color(color: str) -> str:
    """Choose the memory-label colour with the strongest WCAG contrast."""
    return max(
        (MEMORY_LABEL_LIGHT, MEMORY_LABEL_DARK),
        key=lambda candidate: contrast_ratio(color, candidate),
    )


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
        + ' (Higher is better; peak memory (PSS) labels)'
    )
    throughput.set_ylabel(unit)
    # Leave physical headroom for the two labels even when a short bar cannot
    # contain its memory annotation.
    throughput.margins(y=0.16)
    figure.canvas.draw()
    baseline = throughput.transData.transform((0, 0))[1]
    for bar, name, color in zip(bars, names, colors, strict=True):
        _, high = results[name]['rps_range']
        value = results[name]['rps']
        center = bar.get_x() + bar.get_width() / 2
        top = max(value, high)
        height_pixels = throughput.transData.transform((0, value))[1] - baseline
        throughput.annotate(
            f'{value:,.0f}',
            (center, top),
            xytext=(0, 4 if height_pixels >= 28 else 20),
            textcoords='offset points',
            ha='center',
            va='bottom',
            fontsize=10,
        )
        memory = results[name].get('peak_memory_bytes')
        if memory is None:
            continue
        memory_label = f'{memory / 1e6:.1f} MB'
        if height_pixels >= 28:
            # A fixed physical offset seats every in-bar label on solid fill
            # near the baseline, rather than letting a tall leader's label
            # float halfway up its otherwise empty bar.
            throughput.annotate(
                memory_label,
                (center, 0),
                xytext=(0, 8),
                textcoords='offset points',
                ha='center',
                va='bottom',
                color=bar_label_color(color),
                fontsize=8.5,
                fontweight='light',
            )
        else:
            throughput.annotate(
                memory_label,
                (center, top),
                xytext=(0, 4),
                textcoords='offset points',
                ha='center',
                va='bottom',
                fontsize=8.5,
                fontweight='light',
            )

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


def profile_record(
    profile: NetworkProfile,
    status: str,
    *,
    reason: str | None = None,
    measured_rtt_ms: float | None = None,
    netem: RTT50Netem | None = None,
) -> dict[str, Any]:
    record = {
        'profile': profile.name,
        'description': profile.description,
        'target_rtt_ms': profile.target_rtt_ms,
        'measured_rtt_ms': measured_rtt_ms,
        'status': status,
        'reason': reason,
    }
    if netem is not None:
        record['netem'] = netem
    return record


def record_skipped_profile(
    run_directory: Path,
    profile: NetworkProfile,
    scenarios: Sequence[Scenario],
    reason: str,
) -> None:
    raw_directory = run_directory / 'raw'
    raw_directory.mkdir(parents=True, exist_ok=True)
    write_json(
        run_directory / 'profile.json',
        profile_record(profile, 'skipped', reason=reason),
    )
    for scenario in scenarios:
        print(
            f'  EXCLUDED {profile.description} from {scenario.name}: {reason}',
            flush=True,
        )
        write_json(
            raw_directory / f'{scenario.slug()}.json',
            {
                'scenario': scenario.name,
                'type': scenario.type,
                'workers': scenario.workers,
                'network_profile': profile.name,
                'target_rtt_ms': profile.target_rtt_ms,
                'measured_rtt_ms': None,
                'excluded': {profile.description: reason},
                'server_profiles': server_profiles(scenario),
                'status': 'skipped',
            },
        )


def namespace_arguments(run_directory: Path) -> list[str]:
    """Forward the user arguments once, replacing profile-local switches."""
    forwarded = []
    index = 1
    while index < len(sys.argv):
        argument = sys.argv[index]
        if argument in {'--network-profile', '--run-directory'}:
            index += 2
        elif argument.startswith(('--network-profile=', '--run-directory=')):
            index += 1
        elif argument == '--publish':
            # The parent publishes both staged profiles together. Passing this
            # through would make the shaped child reject its partial profile.
            index += 1
        else:
            forwarded.append(argument)
            index += 1
    return [
        *forwarded,
        '--network-profile',
        RTT50_PROFILE.name,
        '--run-directory',
        str(run_directory),
    ]


def run_rtt50_namespace(run_directory: Path) -> tuple[int | None, str | None]:
    """Start the 50 ms profile in a throwaway unprivileged network namespace."""
    try:
        result = subprocess.run(
            [
                'unshare',
                '-rn',
                '--',
                sys.executable,
                sys.argv[0],
                *namespace_arguments(run_directory),
            ],
            env={**os.environ, NETEM_NAMESPACE_ENV: '1'},
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
    except OSError as error:
        return None, str(error)
    detail = result.stderr.strip() or None
    if result.returncode and detail:
        print(detail, file=sys.stderr)
    return result.returncode, detail


def publish(run_directories: Sequence[Path]) -> None:
    for kind, suffix, canonical in (
        ('raw', '.json', CANONICAL_RAW_DIRECTORY),
        ('plots', '.svg', CANONICAL_PLOT_DIRECTORY),
    ):
        canonical.mkdir(parents=True, exist_ok=True)
        for stale in canonical.glob(f'*{suffix}'):
            stale.unlink()
        for run_directory in run_directories:
            for artifact in sorted((run_directory / kind).glob(f'*{suffix}')):
                shutil.copy2(artifact, canonical / artifact.name)
    print(
        f'Published {len(run_directories)} profile(s) to {CANONICAL_RAW_DIRECTORY} '
        f'and {CANONICAL_PLOT_DIRECTORY}'
    )


def assert_publishable(run_directories: Sequence[Path]) -> None:
    undecided = []
    for run_directory in run_directories:
        for record_path in sorted((run_directory / 'raw').glob('*.json')):
            record = json.loads(record_path.read_text())
            if record.get('status') == 'skipped':
                continue
            if not record.get('leader_separated'):
                undecided.append(record['scenario'])
    if undecided:
        raise BenchmarkError(
            f'refusing to publish {len(undecided)} scenario(s) whose winner '
            f'is not established: {", ".join(undecided)}; '
            'raise --scenario-budget or --max-trials and re-run'
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
    run_directory: Path,
    profile: NetworkProfile,
    measured_rtt_ms: float | None = None,
    netem: RTT50Netem | None = None,
) -> Path:
    ensure_static_file_payload()
    if any(scenario.type == 'h2_upload' for scenario in scenarios):
        ensure_large_upload_payload()
    for tool in {'oha'} | ({'k6'} if any(s.type == 'ws' for s in scenarios) else set()):
        if shutil.which(tool) is None:
            raise BenchmarkError(f'{tool} is required but not installed')

    if run_directory.exists():
        raise BenchmarkError(f'refusing to overwrite existing run {run_directory}')
    (run_directory / 'raw').mkdir(parents=True)
    (run_directory / 'plots').mkdir()
    hypercorn_config = run_directory / 'hypercorn.toml'
    write_hypercorn_config(hypercorn_config)

    summary = system_summary()
    active_profile = profile_record(
        profile,
        'running',
        measured_rtt_ms=measured_rtt_ms,
        netem=netem,
    )
    write_json(run_directory / 'profile.json', active_profile)
    write_json(
        run_directory / 'identity.json',
        {
            'run_id': run_directory.name,
            'git_head': git_head(),
            'versions': package_versions(),
            'system': summary,
            'duration': duration,
            'warmup': warmup,
            'max_trials': max_trials,
            'target_half_width': TARGET_HALF_WIDTH,
            'network_profile': active_profile,
            'peak_memory': {
                'metric': 'peak memory (PSS)',
                'sample_interval_seconds': PSS_SAMPLE_INTERVAL_SECONDS,
            },
            'tuning_materiality_threshold': TUNING_MATERIALITY_THRESHOLD,
            'server_profiles': {
                scenario.name: server_profiles(scenario) for scenario in scenarios
            },
            'commands': {
                f'{name}:{scenario.type}:w{scenario.workers}': server_command(
                    name, scenario, hypercorn_config=hypercorn_config
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
            hypercorn_config=hypercorn_config,
        )
        records.append(record)
        record['network_profile'] = profile.name
        record['target_rtt_ms'] = profile.target_rtt_ms
        record['measured_rtt_ms'] = measured_rtt_ms
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
    write_json(
        run_directory / 'profile.json',
        profile_record(
            profile,
            'completed',
            measured_rtt_ms=measured_rtt_ms,
            netem=netem,
        ),
    )
    return run_directory


def main() -> int:
    parser = argparse.ArgumentParser(
        description='h2corn cross-server benchmark suite',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument('--servers', nargs='+', choices=list(SERVERS))
    parser.add_argument('--types', nargs='+', choices=list(RESPONSE_CONTRACTS))
    parser.add_argument(
        '--network-profile', choices=('all', 'loopback', 'rtt50'), default='all'
    )
    parser.add_argument(
        '--output-directory',
        type=Path,
        default=RESULTS_DIRECTORY,
        help='directory containing staged run artifacts',
    )
    parser.add_argument('--run-directory', type=Path, help=argparse.SUPPRESS)
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

    loopback_scenarios = [
        scenario
        for scenario in benchmark_scenarios()
        if not args.types or scenario.type in args.types
    ]
    shaped_scenarios = [
        scenario
        for scenario in rtt50_scenarios()
        if not args.types or scenario.type in args.types
    ]
    servers = args.servers or list(SERVERS)
    if args.publish and (args.servers or args.types or args.network_profile != 'all'):
        parser.error('--publish requires every server and both network profiles')
    selected_count = {
        'all': len(loopback_scenarios) + len(shaped_scenarios),
        'loopback': len(loopback_scenarios),
        'rtt50': len(shaped_scenarios),
    }[args.network_profile]
    if not selected_count:
        parser.error('scenario selection is empty')
    required = selected_count * args.scenario_budget
    if args.suite_budget < required:
        parser.error(
            f'--suite-budget {args.suite_budget:.0f}s cannot cover {selected_count} '
            f'scenarios at --scenario-budget {args.scenario_budget:.0f}s '
            f'(needs {required:.0f}s)'
        )

    try:
        output_directory = args.output_directory.resolve()
        if args.network_profile == 'all':
            suite_id = (
                datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
                + f'-{secrets.token_hex(3)}'
            )
            suite_directory = output_directory / 'runs' / suite_id
            complete = []
            if loopback_scenarios:
                complete.append(
                    run_suite(
                        servers=servers,
                        scenarios=loopback_scenarios,
                        duration=args.duration,
                        warmup=args.warmup_duration,
                        max_trials=args.max_trials,
                        scenario_budget=args.scenario_budget,
                        suite_budget=args.suite_budget,
                        run_directory=suite_directory / LOOPBACK_PROFILE.name,
                        profile=LOOPBACK_PROFILE,
                    )
                )
            if shaped_scenarios:
                shaped_directory = suite_directory / RTT50_PROFILE.name
                result, namespace_reason = run_rtt50_namespace(shaped_directory)
                profile_path = shaped_directory / 'profile.json'
                status = (
                    json.loads(profile_path.read_text()).get('status')
                    if profile_path.exists()
                    else None
                )
                if result not in (0, None) and status != 'skipped':
                    detail = f': {namespace_reason}' if namespace_reason else ''
                    raise BenchmarkError(f'50 ms RTT profile exited with {result}{detail}')
                if status == 'completed':
                    complete.append(shaped_directory)
                elif status == 'skipped':
                    # Preserve the explicit skip record in published raw data
                    # while emitting no unshaped plot for this profile.
                    complete.append(shaped_directory)
                elif status != 'skipped':
                    reason = (
                        f'unshare is unavailable: {namespace_reason}'
                        if result is None
                        else namespace_reason
                        or 'unshare failed before the netem profile could start'
                    )
                    print(
                        f'  SKIPPED {RTT50_PROFILE.description}: {reason}', flush=True
                    )
                    record_skipped_profile(
                        shaped_directory, RTT50_PROFILE, shaped_scenarios, reason
                    )
                    complete.append(shaped_directory)
            if args.publish:
                assert_publishable(complete)
                publish(complete)
        else:
            profile = (
                LOOPBACK_PROFILE
                if args.network_profile == 'loopback'
                else RTT50_PROFILE
            )
            scenarios = (
                loopback_scenarios if profile is LOOPBACK_PROFILE else shaped_scenarios
            )
            run_directory = args.run_directory or (
                output_directory
                / 'runs'
                / (
                    datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
                    + f'-{secrets.token_hex(3)}'
                )
            )
            if profile is RTT50_PROFILE:
                if os.environ.get(NETEM_NAMESPACE_ENV) != '1':
                    result, namespace_reason = run_rtt50_namespace(run_directory)
                    profile_path = run_directory / 'profile.json'
                    status = (
                        json.loads(profile_path.read_text()).get('status')
                        if profile_path.exists()
                        else None
                    )
                    if result == 0 or status == 'skipped':
                        return 0
                    if status is not None:
                        return result or 1
                    reason = (
                        f'unshare is unavailable: {namespace_reason}'
                        if result is None
                        else namespace_reason
                        or 'unshare failed before the netem profile could start'
                    )
                    print(
                        f'  SKIPPED {RTT50_PROFILE.description}: {reason}', flush=True
                    )
                    record_skipped_profile(run_directory, profile, scenarios, reason)
                    return 0
                try:
                    netem = configure_rtt50_profile()
                    measured_rtt_ms = netem['measured_rtt_ms']
                except BenchmarkError as error:
                    print(f'  SKIPPED {RTT50_PROFILE.description}: {error}', flush=True)
                    record_skipped_profile(
                        run_directory, profile, scenarios, str(error)
                    )
                    return 0
            else:
                measured_rtt_ms = None
                netem = None
            completed = run_suite(
                servers=servers,
                scenarios=scenarios,
                duration=args.duration,
                warmup=args.warmup_duration,
                max_trials=args.max_trials,
                scenario_budget=args.scenario_budget,
                suite_budget=args.suite_budget,
                run_directory=run_directory,
                profile=profile,
                measured_rtt_ms=measured_rtt_ms,
                netem=netem,
            )
            if args.publish:
                assert_publishable([completed])
                publish([completed])
    except BenchmarkError as error:
        print(f'benchmark failed: {error}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
